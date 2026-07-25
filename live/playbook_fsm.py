"""
Executor 状态机（§5）+ 仓位 / b2act 计算（§7）+ level 校验（§21 C 类）。

只负责 WAITING 阶段的**纯判断**（基于已收线 bar），不碰真实下单。
ACTIVATED / TP1_HIT 阶段靠订单状态查询推进，在 §8 manage_open_position。

逐行对齐 replay/scorer.py：
  primary_touch:  side=low → bar.low<=level ; side=high → bar.high>=level
  activation:     close 穿越（above:close>level, below:close<level）
  cancel:         同 close 穿越；**同 bar cancel 先于 activate**（scorer 顺序）
  b2act = (activated_open_time - primary_open_time) / 15min ≥ 2

primary touch 那根 bar 当根不判 activation（step 一次处理一根、只走一个分支），
最早 activation 在 primary 的下一根（b2act=1），b2act≥2 即排除紧邻下一根 —— 与 scorer 一致。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd

from live import exec_config as cfg


# ── 状态 ───────────────────────────────────────────────────────────────────────

class PBStatus(str, Enum):
    WAITING_FOR_PRIMARY_TOUCH = "WAITING_FOR_PRIMARY_TOUCH"
    WAITING_FOR_ACTIVATION    = "WAITING_FOR_ACTIVATION"
    OPENING                   = "OPENING"          # 已分配 slot、真实下单前持久化（崩溃恢复锚点 §18）
    ACTIVATED                 = "ACTIVATED"
    TP1_HIT                   = "TP1_HIT"
    DONE_CANCELLED            = "DONE_CANCELLED"
    DONE_SL                   = "DONE_SL"
    DONE_BE                   = "DONE_BE"
    DONE_TP2                  = "DONE_TP2"
    DONE_UNKNOWN              = "DONE_UNKNOWN"      # 无敞口但状态对不上 → 安全终态（§21）


TERMINAL = {
    PBStatus.DONE_CANCELLED, PBStatus.DONE_SL,
    PBStatus.DONE_BE, PBStatus.DONE_TP2, PBStatus.DONE_UNKNOWN,
}


class WaitEvent(str, Enum):
    NONE          = "NONE"            # 无变化
    PRIMARY_TOUCH = "PRIMARY_TOUCH"   # 刚触 primary
    ACTIVATE      = "ACTIVATE"        # 激活，应开仓（b2act 已通过）
    CANCEL        = "CANCEL"          # 取消（cancel level 先穿）
    SKIP_B2ACT    = "SKIP_B2ACT"      # 激活但 b2act<2，弃


@dataclass
class Bar:
    open_time: pd.Timestamp
    open: float
    high: float
    low: float
    close: float


@dataclass
class Sizing:
    notional: float      # USDT
    qty: float           # 币
    margin: float        # USDT
    leverage: int


# ── 价格穿越 / 触及（对齐 scorer）─────────────────────────────────────────────

def touch(bar: Bar, level: float, side: str) -> bool:
    if side == "low":
        return bar.low <= level
    if side == "high":
        return bar.high >= level
    return False


def close_cross(close: float, level: float, direction: str) -> bool:
    if direction == "above":
        return close > level
    if direction == "below":
        return close < level
    return False


# ── WAITING 阶段状态机 ────────────────────────────────────────────────────────

def step_waiting(pb: dict, bar: Bar) -> WaitEvent:
    """对一个 playbook（state.json 里的 dict）喂一根已收线 bar，原地推进 WAITING 阶段。

    返回 WaitEvent。ACTIVATE 时**不**改 status（留给 §8 开仓成功后置 ACTIVATED），
    其余事件直接更新 status。
    """
    status = pb["status"]

    if status == PBStatus.WAITING_FOR_PRIMARY_TOUCH.value:
        pt = pb["primary_touch"]
        if pt and pt.get("level") is not None and touch(bar, pt["level"], pt["side"]):
            pb["status"] = PBStatus.WAITING_FOR_ACTIVATION.value
            pb["primary_touched_at"] = bar.open_time.isoformat()
            return WaitEvent.PRIMARY_TOUCH
        return WaitEvent.NONE

    if status == PBStatus.WAITING_FOR_ACTIVATION.value:
        can = pb.get("cancels_if") or {}
        act = pb.get("activates_if") or {}
        # cancel 先判（scorer 顺序：同 bar cancel 赢）
        if can.get("level") is not None and close_cross(bar.close, can["level"], can["dir"]):
            pb["status"] = PBStatus.DONE_CANCELLED.value
            pb["result"] = "cancelled"
            pb["done_at"] = bar.open_time.isoformat()
            return WaitEvent.CANCEL
        if act.get("level") is not None and close_cross(bar.close, act["level"], act["dir"]):
            primary_t = pd.Timestamp(pb["primary_touched_at"])
            b2act = round((bar.open_time - primary_t) / pd.Timedelta(minutes=15))
            if b2act < 2:
                pb["status"] = PBStatus.DONE_CANCELLED.value
                pb["result"] = "skipped_b2act"
                pb["done_at"] = bar.open_time.isoformat()
                return WaitEvent.SKIP_B2ACT
            pb["activated_at"] = bar.open_time.isoformat()
            pb["b2act"] = int(b2act)
            return WaitEvent.ACTIVATE
        return WaitEvent.NONE

    return WaitEvent.NONE


# ── 仓位 / 杠杆（§7）──────────────────────────────────────────────────────────

def compute_notional(r_dist_pct: float) -> float:
    """notional = 1000 / r_dist_pct（锁定名义风险 = 1R = 10u）。仓位只由 1R 决定，与杠杆无关。"""
    return (cfg.ONE_R_USDT * 100) / r_dist_pct


def max_leverage(symbol: str, exchange: Optional[str] = None) -> int:
    """品种上限与交易所上限取小。exchange=None（或未列出的所）只受品种上限约束。"""
    lev = cfg.SYMBOL_MAX_LEV[symbol]
    if exchange is not None:
        lev = min(lev, cfg.EXCHANGE_MAX_LEV.get(exchange, lev))
    return max(1, int(lev))


def required_margin(symbol: str, r_dist_pct: float, exchange: Optional[str] = None) -> float:
    """本笔实际要占的逐仓保证金：SYMBOL_MARGIN 只是下限，杠杆封顶时按 notional/上限 反推。
    币安子账户 5x 封顶时，358u 名义要占 71.6u —— 按 SYMBOL_MARGIN 记 5u 会让 slot C1 超分配。"""
    notional = compute_notional(r_dist_pct)
    return max(cfg.SYMBOL_MARGIN[symbol], notional / max_leverage(symbol, exchange))


def compute_sizing(symbol: str, r_dist_pct: float, entry_price: float,
                   exchange: Optional[str] = None,
                   margin: Optional[float] = None) -> Sizing:
    """margin 由调用方（slot 分配时已按 exchange 算过）传入，不传则现算。"""
    notional = compute_notional(r_dist_pct)
    qty      = notional / entry_price
    if margin is None:
        margin = required_margin(symbol, r_dist_pct, exchange)
    leverage = min(math.ceil(notional / margin), max_leverage(symbol, exchange))
    return Sizing(notional=notional, qty=qty, margin=margin, leverage=max(1, leverage))


def compute_sl_price(symbol: str, direction: str, invalidation_level: float) -> float:
    """SL 价：默认 = invalidation；BTC 外扩 0.1% buffer 躲 wick（§6.2）。
    远离持仓方向：做空 SL 在上方 ×(1+buf)，做多 SL 在下方 ×(1-buf)。"""
    if symbol != "BTC/USDT":
        return invalidation_level
    buf = cfg.BTC_SL_BUFFER
    if direction == "short":
        return invalidation_level * (1 + buf)
    return invalidation_level * (1 - buf)


# ── level 合理性校验（§21 C 类）──────────────────────────────────────────────

def validate_levels(pb: dict) -> tuple[bool, str]:
    """入场前校验 playbook 的 level 是否自洽。不通过 → 不进场。"""
    direction = pb.get("direction")
    if direction not in ("long", "short"):
        return False, f"bad direction: {direction}"

    for key in ("primary_touch", "activates_if", "invalidation"):
        d = pb.get(key) or {}
        if d.get("level") in (None, 0):
            return False, f"{key} level missing/zero"

    r = pb.get("r_dist_pct")
    if not r or r <= 0:
        return False, f"r_dist_pct invalid: {r}"

    tp1 = pb.get("tp1_level")
    if tp1 in (None, 0):
        return False, "tp1_level missing/zero"

    act = pb["activates_if"]["level"]
    inv = pb["invalidation"]["level"]
    # 方向自洽：做多 TP1 在 act 上方、SL(inv) 在下方；做空相反
    if direction == "long":
        if not (tp1 > act):
            return False, f"long: tp1({tp1}) not above act({act})"
        if not (inv < act):
            return False, f"long: inv({inv}) not below act({act})"
    else:  # short
        if not (tp1 < act):
            return False, f"short: tp1({tp1}) not below act({act})"
        if not (inv > act):
            return False, f"short: inv({inv}) not above act({act})"

    tp2 = pb.get("tp2_level")
    if tp2 not in (None, 0):
        if direction == "long" and not (tp2 > tp1):
            return False, f"long: tp2({tp2}) not above tp1({tp1})"
        if direction == "short" and not (tp2 < tp1):
            return False, f"short: tp2({tp2}) not below tp1({tp1})"

    return True, ""
