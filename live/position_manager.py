"""
入场 + 挂三单 + S3 出场联动（§6）。只用 Broker 接口，不依赖具体交易所。

入场（open_position）：set_leverage → 市价开仓 → 一次挂 TP1/TP2/SL 三单 → 返回 exec dict。
联动（manage_open_position，每 tick 查订单状态）：
  ACTIVATED: SL 成交→撤 TP→DONE_SL ; TP1 成交→撤原 SL、挂 BE SL→TP1_HIT
  TP1_HIT  : TP2 成交→撤 BE→DONE_TP2 ; BE 成交→撤 TP2→DONE_BE
原则：任一单成交后先撤其余单，再做下一步（§6.5）。
"""
from __future__ import annotations

from typing import Optional

from live.broker.base import Broker, PosSide, OrderState
from live.playbook_fsm import PBStatus, compute_sizing, compute_sl_price


def _pos_side(direction: str) -> PosSide:
    return PosSide.SHORT if direction == "short" else PosSide.LONG


def open_position(broker: Broker, symbol: str, pb: dict, entry_estimate: float,
                  account: str, margin: float, client_id_base: str) -> dict:
    """开仓 + 挂三单，返回 exec dict（写入 pb["exec"]，由 caller 负责）。"""
    direction = pb["direction"]
    ps = _pos_side(direction)

    sizing = compute_sizing(symbol, pb["r_dist_pct"], entry_estimate)
    qty = broker.round_qty(symbol, sizing.qty)

    broker.set_leverage(symbol, ps, sizing.leverage)
    fill = broker.market_open(symbol, ps, qty, f"{client_id_base}_E")
    entry = fill.price
    qty = fill.qty                                   # 以实际成交为准

    sl_price = broker.round_price(symbol, compute_sl_price(symbol, direction, pb["invalidation"]["level"]))
    tp1 = broker.round_price(symbol, pb["tp1_level"])
    half = broker.round_qty(symbol, qty / 2)
    rest = broker.round_qty(symbol, qty - half)

    sl_oid = broker.place_stop_market(symbol, ps, qty, sl_price, f"{client_id_base}_S")
    tp1_oid = broker.place_reduce_limit(symbol, ps, half, tp1, f"{client_id_base}_T1")

    tp2 = pb.get("tp2_level")
    tp2_oid = None
    if tp2:
        tp2 = broker.round_price(symbol, tp2)
        tp2_oid = broker.place_reduce_limit(symbol, ps, rest, tp2, f"{client_id_base}_T2")

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
    }


def manage_open_position(broker: Broker, symbol: str, pb: dict) -> Optional[str]:
    """推进 ACTIVATED / TP1_HIT。返回新 status（变化时），否则 None。

    SL/BE 触发用**持仓**判断（ground truth，跨两家通用，且绕开交易所 algo 历史查询差异）：
    持仓没了而 TP 未成交 = 被止损/保本平掉。TP 用普通限价单查询（可靠）。
    """
    ex = pb["exec"]
    ps = PosSide(ex["pos_side"])
    base = ex["client_id_base"]
    status = pb["status"]
    pos = broker.get_position(symbol, ps)

    if status == PBStatus.ACTIVATED.value:
        tp1 = broker.get_order(symbol, ex.get("tp1_order_id"))
        if tp1 and tp1.state == OrderState.FILLED:                     # 半仓止盈 → 移 SL 到 BE
            _cancel(broker, symbol, ex.get("sl_order_id"))
            rest = ex["qty"] - ex["half_qty"]
            be_oid = broker.place_stop_market(symbol, ps, rest, ex["entry_price"], f"{base}_SBE")
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
        if tp2 and tp2.state == OrderState.FILLED:                     # TP2 全达成
            _cancel(broker, symbol, ex.get("sl_order_id"))
            pb["status"] = PBStatus.DONE_TP2.value
            pb["result"] = "tp2"
            return pb["status"]
        if pos is None:                                                # 剩余半仓没了且 TP2 未成交 = BE 触发
            _cancel(broker, symbol, ex.get("tp2_order_id"))
            pb["status"] = PBStatus.DONE_BE.value
            pb["result"] = "be"
            return pb["status"]
        return None

    return None


def _cancel(broker: Broker, symbol: str, oid: Optional[str]) -> None:
    if oid:
        try:
            broker.cancel_order(symbol, order_id=oid)
        except Exception:
            pass
