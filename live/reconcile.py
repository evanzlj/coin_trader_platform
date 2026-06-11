"""
对账与崩溃恢复（§11.3, §21 #2/#5）。

原则（§21.1）：**交易所实际持仓 = 唯一真相**；对不上一律以交易所为准，
无法明确判定的 → 挂起该 pb（manual_override）+ 告警，宁可暂停不瞎动。

- startup_reconcile：重启时（§21 #2）WAITING 旧包丢弃；ACTIVATED/TP1_HIT 对账恢复。
- periodic_reconcile：运行时每 15min（§21 #5）核对 open pb，抓强平 / 漂移 / 漏检。
- reconcile_position：核对单个 open pb 与交易所实际。
"""
from __future__ import annotations

import logging

from live.broker.base import Broker, PosSide, OrderState
from live.playbook_fsm import PBStatus, TERMINAL

logger = logging.getLogger(__name__)

WAITING = {
    PBStatus.WAITING_FOR_PRIMARY_TOUCH.value,
    PBStatus.WAITING_FOR_ACTIVATION.value,
}
OPEN = {PBStatus.ACTIVATED.value, PBStatus.TP1_HIT.value}
TERMINAL_V = {s.value for s in TERMINAL}


def reconcile_position(broker: Broker, symbol: str, pb: dict) -> str:
    """核对一个 ACTIVATED/TP1_HIT 的 pb 与交易所实际状态。
    返回 'ok'（一致继续）| 'resolved'（已被平，定终态）| 'manual'（对不上，挂起人工）。
    原地更新 pb（status / exec.manual_override）。"""
    ex = pb.get("exec") or {}
    ps = PosSide(ex["pos_side"])
    pos = broker.get_position(symbol, ps)
    status = pb["status"]

    def filled(oid) -> bool:
        if not oid:
            return False
        o = broker.get_order(symbol, oid)
        return o is not None and o.state == OrderState.FILLED

    # 有持仓 → 仓位还在，继续（挂单缺失检查留待补挂逻辑；先认 ok）
    if pos is not None:
        return "ok"

    # 无持仓：宕机期间已被平，按订单成交定真实终态
    if status == PBStatus.ACTIVATED.value:
        if filled(ex.get("sl_order_id")):
            pb["status"] = PBStatus.DONE_SL.value
            pb["result"] = "sl_reconciled"
            return "resolved"
        if filled(ex.get("tp2_order_id")):
            pb["status"] = PBStatus.DONE_TP2.value
            pb["result"] = "tp2_reconciled"
            return "resolved"
        # TP1 成交但持仓已空 → BE 段在宕机期发生，无法确定终态 → 人工
        if filled(ex.get("tp1_order_id")):
            return _flag_manual(pb, "manual_tp1_ambiguous", symbol)

    elif status == PBStatus.TP1_HIT.value:
        if filled(ex.get("sl_order_id")):          # 此时 sl_order_id 已是 BE 单
            pb["status"] = PBStatus.DONE_BE.value
            pb["result"] = "be_reconciled"
            return "resolved"
        if filled(ex.get("tp2_order_id")):
            pb["status"] = PBStatus.DONE_TP2.value
            pb["result"] = "tp2_reconciled"
            return "resolved"

    # 持仓没了但订单状态也对不上（强平 / 人工平 / 未知）→ 人工
    return _flag_manual(pb, "manual_no_position", symbol)


def _flag_manual(pb: dict, result: str, symbol: str) -> str:
    pb.setdefault("exec", {})["manual_override"] = True
    pb["result"] = result
    logger.error("RECONCILE manual needed: %s %s — %s",
                 symbol, pb.get("hypothesis"), result)
    from live import notify
    notify.feishu_alert(f"RECONCILE manual: {symbol} {pb.get('hypothesis')} — {result}")
    return "manual"


def startup_reconcile(engine) -> None:
    """启动对账（§21 #2）：WAITING 旧包丢弃；ACTIVATED/TP1_HIT 对账恢复。"""
    for pkg_dir, state in engine.load_states():
        symbol = state["symbol"]
        changed = False
        for pb in state["playbooks"]:
            st = pb["status"]
            if st in WAITING:
                pb["status"] = PBStatus.DONE_CANCELLED.value
                pb["result"] = "restart_discard"
                changed = True
            elif st in OPEN:
                broker = engine.brokers.get((pb.get("exec") or {}).get("account"))
                if broker is None:
                    _flag_manual(pb, "manual_no_broker", symbol)
                else:
                    reconcile_position(broker, symbol, pb)
                changed = True
        if changed:
            engine.save_state(pkg_dir, state)
        if all(pb["status"] in TERMINAL_V for pb in state["playbooks"]):
            engine.archive(pkg_dir)
    logger.info("startup reconcile done")


def periodic_reconcile(engine) -> None:
    """定期对账（§21 #5）：核对所有 open pb，抓强平 / 漂移 / 漏检。"""
    for pkg_dir, state in engine.load_states():
        symbol = state["symbol"]
        changed = False
        for pb in state["playbooks"]:
            if pb["status"] in OPEN and not (pb.get("exec") or {}).get("manual_override"):
                broker = engine.brokers.get(pb["exec"]["account"])
                if broker is None:
                    continue
                if reconcile_position(broker, symbol, pb) != "ok":
                    changed = True
        if changed:
            engine.save_state(pkg_dir, state)
        if all(pb["status"] in TERMINAL_V for pb in state["playbooks"]):
            engine.archive(pkg_dir)
