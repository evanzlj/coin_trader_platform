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

import pandas as pd

from live.broker.base import Broker, PosSide, OrderState
from live.playbook_fsm import PBStatus, TERMINAL
from live import notify
from live import exec_config as cfg

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
    if ex.get("recovering"):
        # 裸仓恢复中：exec 信息不完整（无 qty/sl_price），交给 tick 的 _try_recover 用持仓量平，不走普通对账（§21）
        return "ok"
    ps = PosSide(ex["pos_side"])
    try:
        pos = broker.get_position(symbol, ps)
    except Exception as e:
        logger.warning("reconcile %s get_position error, hold state: %s", symbol, e)
        return "ok"                                     # 查询异常 → 保持当前状态，不臆测（UNKNOWN 不判死）
    status = pb["status"]

    def filled(oid) -> bool:
        if not oid:
            return False
        o = broker.get_order(symbol, oid)
        return o is not None and o.state == OrderState.FILLED

    # 有持仓 → 检查 SL 还在否，缺则补挂；补不上则平退出（防无保护裸持 §19/§21）
    if pos is not None:
        if _ensure_sl(broker, symbol, pb):
            return "resolved"                          # 已平退出（status 变了）
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
        # TP1 成交但持仓已空 → BE 段在宕机期发生，无敞口 → 安全终态（§21）
        if filled(ex.get("tp1_order_id")):
            pb["status"] = PBStatus.DONE_UNKNOWN.value
            pb["result"] = "tp1_then_unknown_exit"
            notify.feishu_alert(f"reconcile: TP1 filled then closed, unknown exit ({symbol})")
            return "resolved"

    elif status == PBStatus.TP1_HIT.value:
        if filled(ex.get("sl_order_id")):          # 此时 sl_order_id 已是 BE 单
            pb["status"] = PBStatus.DONE_BE.value
            pb["result"] = "be_reconciled"
            return "resolved"
        if filled(ex.get("tp2_order_id")):
            pb["status"] = PBStatus.DONE_TP2.value
            pb["result"] = "tp2_reconciled"
            return "resolved"

    # 持仓没了但订单状态也对不上 → 无敞口，安全终态（API-only 账户无人工出口，不挂人工 §21）
    pb["status"] = PBStatus.DONE_UNKNOWN.value
    pb["result"] = "no_position_unknown_exit"
    notify.feishu_alert(f"reconcile: no position, unknown exit ({symbol} {pb.get('hypothesis')})")
    return "resolved"


def _ensure_sl(broker: Broker, symbol: str, pb: dict) -> bool:
    """持仓还在但 SL 没了 → 重挂 SL；补不上则市价平退出（不留无保护裸持 §19/§21）。
    返回 True = 已平退出（status 改为 DONE_UNKNOWN）。"""
    ex = pb["exec"]
    ps = PosSide(ex["pos_side"])
    sl = broker.get_order(symbol, ex.get("sl_order_id"))
    if sl is not None and sl.state == OrderState.UNKNOWN:
        return False                                    # 查单异常，本轮不动
    if sl is None or sl.state in (OrderState.CANCELED, OrderState.EXPIRED, OrderState.REJECTED):
        qty = ex.get("qty_remaining") or ex["qty"]
        try:
            new_sl = broker.place_stop_market(symbol, ps, qty, ex["sl_price"],
                                              f"{ex['client_id_base']}_SR")
            ex["sl_order_id"] = new_sl
            notify.feishu_alert(f"reconcile: SL missing → replaced ({symbol} {ex.get('account')})")
        except Exception as e:
            # SL 补不上 → 不留无保护仓，市价平退出；平也失败 → recovering（tick 重试）
            notify.feishu_alert(f"reconcile: SL replace FAILED, closing ({symbol}): {e}")
            try:
                broker.market_close(symbol, ps, qty, f"{ex['client_id_base']}_SLFC")
                pb["status"] = PBStatus.DONE_UNKNOWN.value
                pb["result"] = "sl_unplaceable_closed"
                return True
            except Exception as ce:
                ex["recovering"] = True
                notify.feishu_alert(f"reconcile: close also FAILED, recovering ({symbol}): {ce}")
    return False


def _flag_manual(pb: dict, result: str, symbol: str) -> str:
    pb.setdefault("exec", {})["manual_override"] = True
    pb["result"] = result
    logger.error("RECONCILE manual needed: %s %s — %s",
                 symbol, pb.get("hypothesis"), result)
    from live import notify
    notify.feishu_alert(f"RECONCILE manual: {symbol} {pb.get('hypothesis')} — {result}")
    return "manual"


def _recover_opening(engine, symbol: str, pb: dict) -> None:
    """OPENING（开仓中崩溃）恢复（§18 P0-2）：查 {base}_E 订单/实际持仓 → 接管 / 平退出 / 作废 / 保持。
    任何查询 API 异常（get_position 抛 / get_order UNKNOWN）→ 保持 OPENING，绝不武断判死。"""
    ex = pb.get("exec") or {}
    broker = engine.brokers.get(ex.get("account"))
    if broker is None:
        _flag_manual(pb, "manual_no_broker", symbol)
        return
    ps = PosSide(ex["pos_side"])
    try:
        pos = broker.get_position(symbol, ps)
    except Exception as e:
        logger.warning("recover_opening %s get_position error, hold OPENING: %s", symbol, e)
        return                                          # 持仓查询 API 异常 → 保持 OPENING（不臆测）
    od = broker.get_order(symbol, client_id=f"{ex['client_id_base']}_E")
    od_unknown = od is not None and od.state == OrderState.UNKNOWN

    if pos is not None and pos.qty > 0:
        # 有**实际持仓** → 接管补 SL/TP（用持仓量，不靠 od.filled，避免接管已平掉的仓）。
        # entry_price 也可能未同步（UNKNOWN）：用 entry 单 avg_price 补；补不出 → 保持 OPENING，绝不 adopt entry=0（污染 BE §22）。
        entry = pos.entry_price
        if entry <= 0:
            if od is not None and od.state == OrderState.FILLED and od.avg_price > 0:
                entry = od.avg_price
            else:
                logger.warning("recover_opening %s pos visible but entry_price<=0, hold OPENING", symbol)
                return                                  # entry_price 未同步 → 保持 OPENING，下 tick 再查
        from live.position_manager import adopt_position, SLPlacementError, NakedPositionError
        try:
            pb["exec"] = adopt_position(broker, symbol, ex, pos.qty, entry)
            pb["status"] = PBStatus.ACTIVATED.value
            notify.feishu_alert(f"OPENING recovered → adopted ({symbol} {ex.get('account')} qty={pos.qty})")
        except SLPlacementError as e:
            pb["status"] = PBStatus.DONE_UNKNOWN.value
            pb["result"] = "adopt_sl_failed_closed"
            notify.feishu_alert(f"OPENING adopt SL failed, position closed ({symbol}): {e}")
        except NakedPositionError as e:
            ex["recovering"] = True
            pb["exec"] = ex
            pb["status"] = PBStatus.ACTIVATED.value
            pb["result"] = f"adopt_naked_recovering:{e}"
            notify.feishu_alert(f"OPENING adopt naked, recovering ({symbol}): {e}")
        return

    # 无持仓
    if od_unknown:
        return                                          # 查单 API 异常 → 保持 OPENING（不臆测）
    if od is not None and od.state == OrderState.FILLED:
        # entry 成交但当前查不到仓 → 可能是仓位最终一致性窗口（订单先于仓位可见）。
        # grace 内保持 OPENING 等下 tick 再查；只有超过 grace 仍无仓才认已平退出（§22 P0：不靠一次查询判终态）。
        opening_at = ex.get("opening_at")
        within_grace = True                             # 缺 opening_at → 保守保持 OPENING（不误判孤儿）
        if opening_at:
            try:
                age = (pd.Timestamp.now("UTC") - pd.Timestamp(opening_at)).total_seconds()
                within_grace = age < cfg.OPENING_GRACE_SECONDS
            except Exception:
                within_grace = True
        if within_grace:
            return                                      # 保持 OPENING（仓位可能还没同步），下 tick 再查
        pb["status"] = PBStatus.DONE_UNKNOWN.value
        pb["result"] = "opening_filled_then_flat"
        notify.feishu_alert(f"OPENING filled then flat after grace ({symbol} {ex.get('account')})")
        return
    if od is None or od.state in (OrderState.CANCELED, OrderState.REJECTED, OrderState.EXPIRED):
        # entry 单不存在 / 明确失败（撤/拒/过期）+ 无仓 → 没开成，作废（明确失败，不永久占 slot §22）
        pb["status"] = PBStatus.DONE_CANCELLED.value
        pb["result"] = "opening_aborted"
        notify.feishu_alert(f"OPENING aborted, entry dead/absent + no position ({symbol} {ex.get('account')})")
        return
    # od 存在但未成交（NEW）+ 无仓 → 保持 OPENING（下 tick 再看）


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
            elif st == PBStatus.OPENING.value:
                _recover_opening(engine, symbol, pb)    # 开仓中崩溃 → 查交易所接管/作废（§18）
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
