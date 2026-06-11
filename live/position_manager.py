"""
入场 + 挂三单 + S3 出场联动（§6）+ 故障处理（§10.2/§21，task #16）。只用 Broker 接口。

安全原则：
- **绝不裸仓**：开仓后 SL 必须挂上，挂不上 → 立即市价平退出；平不掉 → 标裸仓交人工。
- **任何时刻有 SL**：TP1 后移 BE 时**先挂 BE、成功后再撤旧 SL**（不会出现"撤了旧、新没挂上"的裸奔）。
- 撤单失败重试 + 告警（防孤儿挂单）；TP 挂失败降级不致命（SL 仍在）。
"""
from __future__ import annotations

import time
from typing import Optional

from live.broker.base import Broker, PosSide, OrderState, Fill, open_side
from live.playbook_fsm import PBStatus, compute_sizing, compute_sl_price
from live import notify


class SLPlacementError(Exception):
    """SL 挂单失败，但持仓已市价平退出（安全，不裸仓）。"""


class NakedPositionError(Exception):
    """SL 挂单失败且平仓也失败 —— 裸仓，需人工接管。"""


def pos_side_of(direction: str) -> PosSide:
    return PosSide.SHORT if direction == "short" else PosSide.LONG


def open_position(broker: Broker, symbol: str, pb: dict, entry_estimate: float,
                  account: str, margin: float, client_id_base: str) -> dict:
    """开仓 + 挂三单。SL 必须挂上，否则平仓退出（不裸仓）。返回 exec dict。"""
    direction = pb["direction"]
    ps = pos_side_of(direction)

    sizing = compute_sizing(symbol, pb["r_dist_pct"], entry_estimate)
    qty = broker.round_qty(symbol, sizing.qty)

    broker.set_leverage(symbol, ps, sizing.leverage)
    try:
        fill = broker.market_open(symbol, ps, qty, f"{client_id_base}_E")
    except Exception as e:
        # 市价单可能超时但已成交 → 查持仓/同 clientOrderId 接管（幂等，防孤儿仓 §18）
        time.sleep(1.0)
        pos = broker.get_position(symbol, ps)
        if pos is not None and pos.qty > 0:
            od = broker.get_order(symbol, client_id=f"{client_id_base}_E")
            fill = Fill(od.order_id if od else "RECOVERED", f"{client_id_base}_E",
                        symbol, ps, open_side(ps), pos.entry_price, pos.qty)
            notify.feishu_alert(f"market_open timeout but position found, recovered: {symbol} {account}")
        else:
            raise RuntimeError(f"market_open failed, no position: {e}") from e
    entry = fill.price
    qty = fill.qty                                   # 以实际成交为准（含部分成交）
    if qty <= 0:
        raise RuntimeError("market_open filled 0 qty")

    sl_price = broker.round_price(symbol, compute_sl_price(symbol, direction, pb["invalidation"]["level"]))
    # ── 不允许裸仓：SL 挂不上 → 立即平退出 ──
    try:
        sl_oid = broker.place_stop_market(symbol, ps, qty, sl_price, f"{client_id_base}_S")
    except Exception as e:
        try:
            broker.market_close(symbol, ps, qty, f"{client_id_base}_SLFAIL{int(time.time()) % 100000}")
        except Exception as ce:
            raise NakedPositionError(f"SL failed AND close failed: {e} / {ce}") from e
        raise SLPlacementError(f"SL failed, position closed: {e}") from e

    half = broker.round_qty(symbol, qty / 2)
    rest = broker.round_qty(symbol, qty - half)
    tp1 = broker.round_price(symbol, pb["tp1_level"])
    tp1_oid = _try_tp(broker, symbol, ps, half, tp1, f"{client_id_base}_T1")

    tp2 = pb.get("tp2_level")
    tp2_oid = None
    if tp2:
        tp2 = broker.round_price(symbol, tp2)
        tp2_oid = _try_tp(broker, symbol, ps, rest, tp2, f"{client_id_base}_T2")

    actual_r = qty * abs(entry - sl_price)
    return {
        "account": account, "exchange": broker.exchange, "pos_side": ps.value,
        "entry_order_id": fill.order_id, "entry_price": entry,
        "qty": qty, "qty_remaining": qty, "half_qty": half,
        "margin": margin, "leverage": sizing.leverage,
        "sl_order_id": sl_oid, "tp1_order_id": tp1_oid, "tp2_order_id": tp2_oid,
        "sl_price": sl_price, "tp1": tp1, "tp2": tp2,
        "actual_r_usdt": round(actual_r, 4),
        "client_id_base": client_id_base,
        "tp1_filled_at": None,
        "tp_degraded": (tp1_oid is None) or (bool(tp2) and tp2_oid is None),
    }


def _try_tp(broker: Broker, symbol: str, ps: PosSide, qty: float, price: float, cid: str) -> Optional[str]:
    """TP 挂单；失败不致命（SL 仍在保护），降级为 None + 告警。"""
    try:
        return broker.place_reduce_limit(symbol, ps, qty, price, cid)
    except Exception as e:
        notify.feishu_alert(f"TP place failed (degraded, SL still on): {symbol} {cid} — {e}")
        notify.trade_log("WARN_TP_PLACE_FAILED", symbol=symbol, cid=cid, error=str(e))
        return None


def manage_open_position(broker: Broker, symbol: str, pb: dict) -> Optional[str]:
    """推进 ACTIVATED / TP1_HIT。SL/BE 触发用持仓 ground truth；TP 用普通单查询。"""
    ex = pb["exec"]
    ps = PosSide(ex["pos_side"])
    base = ex["client_id_base"]
    status = pb["status"]
    pos = broker.get_position(symbol, ps)

    if status == PBStatus.ACTIVATED.value:
        tp1 = broker.get_order(symbol, ex.get("tp1_order_id")) if ex.get("tp1_order_id") else None
        if tp1 and tp1.state == OrderState.UNKNOWN:
            return None                                                # 查单异常，本轮不臆测（§19）
        if tp1 and tp1.state == OrderState.FILLED:                     # 半仓止盈 → 移 SL 到 BE
            rest = ex["qty"] - ex["half_qty"]
            try:                                                        # 先挂 BE
                be_oid = broker.place_stop_market(symbol, ps, rest, ex["entry_price"], f"{base}_SBE")
            except Exception as e:                                      # BE 挂不上 → 保留原 SL（仍有止损）
                notify.feishu_alert(f"BE SL place failed, keeping original SL: {symbol} {base} — {e}")
                ex["qty_remaining"] = rest
                ex["tp1_filled_at"] = True
                ex["be_failed"] = True
                pb["status"] = PBStatus.TP1_HIT.value
                pb["result"] = "tp1_be_degraded"
                return pb["status"]
            _cancel(broker, symbol, ex.get("sl_order_id"))              # BE 成功后才撤旧 SL
            ex["sl_order_id"] = be_oid
            ex["sl_price"] = ex["entry_price"]
            ex["qty_remaining"] = rest
            ex["tp1_filled_at"] = True
            pb["status"] = PBStatus.TP1_HIT.value
            return pb["status"]
        if pos is None:                                                # 全仓没了且 TP1 未成交 = SL 触发
            _cancel(broker, symbol, ex.get("tp1_order_id"))
            _cancel(broker, symbol, ex.get("tp2_order_id"))
            pb["status"] = PBStatus.DONE_SL.value
            pb["result"] = "sl"
            return pb["status"]
        return None

    if status == PBStatus.TP1_HIT.value:
        tp2 = broker.get_order(symbol, ex.get("tp2_order_id")) if ex.get("tp2_order_id") else None
        if tp2 and tp2.state == OrderState.UNKNOWN:
            return None                                                # 查单异常，本轮不臆测（§19）
        if tp2 and tp2.state == OrderState.FILLED:                     # TP2 全达成
            _cancel(broker, symbol, ex.get("sl_order_id"))
            pb["status"] = PBStatus.DONE_TP2.value
            pb["result"] = "tp2"
            return pb["status"]
        if pos is None:                                                # 剩余半仓没了且 TP2 未成交 = BE/SL 触发
            _cancel(broker, symbol, ex.get("tp2_order_id"))
            pb["status"] = PBStatus.DONE_BE.value
            pb["result"] = "be"
            return pb["status"]
        return None

    return None


def _cancel(broker: Broker, symbol: str, oid: Optional[str], retries: int = 2) -> None:
    """撤单，失败重试；仍失败 → 告警（孤儿挂单风险），不抛。"""
    if not oid:
        return
    last = None
    for _ in range(retries + 1):
        try:
            broker.cancel_order(symbol, order_id=oid)
            return
        except Exception as e:
            last = e
            time.sleep(0.2)
    notify.feishu_alert(f"cancel FAILED (orphan order risk): {symbol} {oid} — {last}")
    notify.trade_log("ERROR_CANCEL_FAILED", symbol=symbol, order_id=oid, error=str(last))
