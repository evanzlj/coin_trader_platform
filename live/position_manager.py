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

from live.broker.base import Broker, PosSide, OrderState, Fill, open_side, safe_get_order
from live.playbook_fsm import PBStatus, compute_sizing, compute_sl_price
from live import notify


class SLPlacementError(Exception):
    """SL 挂单失败，但持仓已市价平退出（安全，不裸仓）。"""


class NakedPositionError(Exception):
    """SL 挂单失败且平仓也失败 —— 裸仓，需人工接管。"""


class AdoptUnknownError(Exception):
    """接管时保护单查询 UNKNOWN，无法确认是否已挂 → 保持 OPENING（不重复挂、不平退出，§22）。"""


PROTECTIVE_SUFFIXES = ("_E", "_S", "_SR", "_T1", "_T2", "_SBE")
"""本系统所有 deterministic client id 后缀（§22 七律7 Every Order Must Be Owned）。
   entry(_E) / SL(_S) / reconcile 补挂 SL(_SR) / TP1(_T1) / TP2(_T2) / BE(_SBE)。
   所有 place 的 client id 都必须是 {base}+这些后缀之一；drain 按全集撤，确保无遗漏。"""


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
            od = safe_get_order(broker, symbol, client_id=f"{client_id_base}_E")
            fill = Fill(od.order_id if od else "RECOVERED", f"{client_id_base}_E",
                        symbol, ps, open_side(ps), pos.entry_price, pos.qty)
            notify.feishu_alert(f"market_open timeout but position found, recovered: {symbol} {account}")
        else:
            raise RuntimeError(f"market_open failed, no position: {e}") from e
    entry = fill.price
    qty = fill.qty                                   # 以实际成交为准（含部分成交）
    if qty <= 0:
        raise RuntimeError("market_open filled 0 qty")
    if entry <= 0:
        # 成交价未知（UNKNOWN：查询失败返回 price=0）→ 查持仓补足；补不出 → 抛（保持 OPENING，不接受 price=0 §22）
        try:
            pos = broker.get_position(symbol, ps)
        except Exception:
            pos = None
        if pos is not None and pos.entry_price > 0:
            entry = pos.entry_price
            qty = pos.qty or qty
        else:
            raise RuntimeError("market_open entry price unknown (price<=0), hold for recovery")

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


def adopt_position(broker: Broker, symbol: str, intent: dict, qty: float, entry: float) -> dict:
    """接管 OPENING 崩溃后已存在的仓：补 SL/TP（确定性 client id，先查再挂），返回完整 exec（§18）。"""
    direction = intent["direction"]
    ps = pos_side_of(direction)
    base = intent["client_id_base"]
    sl_price = broker.round_price(symbol, compute_sl_price(symbol, direction, intent["invalidation"]["level"]))
    half = broker.round_qty(symbol, qty / 2)
    rest = broker.round_qty(symbol, qty - half)

    sl_oid = _ensure_protective(broker, symbol, ps, qty, sl_price, f"{base}_S", "stop")
    if sl_oid is None:
        # SL 是硬要求：补不上 → 立即市价平退出；平不掉 → 裸仓 recovering（绝不留无保护仓 §18/§16）
        try:
            broker.market_close(symbol, ps, qty, f"{base}_ADPSLFC")
        except Exception as ce:
            raise NakedPositionError(f"adopt SL failed AND close failed: {ce}")
        raise SLPlacementError("adopt SL failed, position closed")
    tp1_oid = tp2_oid = None
    tp1 = intent.get("tp1_level")
    if tp1:
        tp1 = broker.round_price(symbol, tp1)
        tp1_oid = _ensure_protective(broker, symbol, ps, half, tp1, f"{base}_T1", "limit")
    tp2 = intent.get("tp2_level")
    if tp2:
        tp2 = broker.round_price(symbol, tp2)
        tp2_oid = _ensure_protective(broker, symbol, ps, rest, tp2, f"{base}_T2", "limit")

    actual_r = qty * abs(entry - sl_price)
    return {
        "account": intent["account"], "exchange": broker.exchange, "pos_side": ps.value,
        "entry_order_id": f"{base}_E", "entry_price": entry,
        "qty": qty, "qty_remaining": qty, "half_qty": half,
        "margin": intent.get("margin"), "leverage": intent.get("leverage"),
        "sl_order_id": sl_oid, "tp1_order_id": tp1_oid, "tp2_order_id": tp2_oid,
        "sl_price": sl_price, "tp1": tp1, "tp2": tp2,
        "actual_r_usdt": round(actual_r, 4),
        "client_id_base": base, "tp1_filled_at": None,
        "tp_degraded": (tp1_oid is None and bool(intent.get("tp1_level"))) or (bool(tp2) and tp2_oid is None),
        "adopted": True,
    }


def _ensure_protective(broker: Broker, symbol: str, ps: PosSide, qty: float, price: float,
                       cid: str, kind: str) -> Optional[str]:
    """先按 client id 查保护单是否已活跃，存在则复用；否则挂（确定性 id，重启幂等 §18）。"""
    existing = safe_get_order(broker, symbol, client_id=cid)   # 不抛，UNKNOWN 归一
    if existing is not None and existing.state == OrderState.UNKNOWN:
        raise AdoptUnknownError(cid)                   # 查询不确定是否已挂 → 保持 OPENING（不重复挂、不平退出 §22）
    if existing is not None and existing.state in (OrderState.NEW, OrderState.PARTIALLY_FILLED):
        return existing.order_id                       # 已挂着 → 复用
    try:
        if kind == "stop":
            return broker.place_stop_market(symbol, ps, qty, price, cid)
        return broker.place_reduce_limit(symbol, ps, qty, price, cid)
    except Exception as e:
        notify.feishu_alert(f"adopt: place {cid} failed: {e}")
        return None


def _reached_tp(ref_price: float, target: Optional[float], ps: PosSide) -> bool:
    """降级判断：ref_price 是否到达 TP（short 在下方，long 在上方）。"""
    if target is None:
        return False
    return ref_price <= target if ps == PosSide.SHORT else ref_price >= target


def manage_open_position(broker: Broker, symbol: str, pb: dict,
                         ref_price: Optional[float] = None) -> Optional[str]:
    """推进 ACTIVATED / TP1_HIT。SL/BE 触发用持仓 ground truth；TP 用普通单查询。
    TP 挂单缺失（降级）时用 ref_price 到价市价平（§20）。"""
    ex = pb["exec"]
    ps = PosSide(ex["pos_side"])
    base = ex["client_id_base"]
    status = pb["status"]
    pos = broker.get_position(symbol, ps)

    if status == PBStatus.ACTIVATED.value:
        tp1_done = False
        tp1_dead = ex.get("tp1_order_id") is None                     # 无挂单 → 需降级
        if ex.get("tp1_order_id"):
            tp1 = safe_get_order(broker, symbol, ex["tp1_order_id"])
            if tp1 and tp1.state == OrderState.UNKNOWN:
                if pos is not None:
                    return None                                        # 有仓 + 查询 UNKNOWN → 保持，等查清（§19）
                # 无仓（get_position 可信）→ 无敞口优先于查询不确定 → 诚实终态（drain 后归档 §22.5）
                return terminalize(broker, symbol, pb, PBStatus.DONE_UNKNOWN.value, "exit_unknown")
            if tp1 and tp1.state == OrderState.FILLED:
                tp1_done = True
            elif tp1 is None or tp1.state in (OrderState.CANCELED, OrderState.EXPIRED, OrderState.REJECTED):
                tp1_dead = True                                        # 死单（撤/过期/拒）→ 降级（§20）
        if not tp1_done and tp1_dead and ref_price is not None and _reached_tp(ref_price, ex.get("tp1"), ps):
            try:                                                       # TP1 缺失/失效（降级）→ 到价市价平半仓
                broker.market_close(symbol, ps, ex["half_qty"], f"{base}_T1M")
                tp1_done = True
            except Exception as e:
                notify.feishu_alert(f"TP1 degraded market-close failed: {symbol} — {e}")
        if tp1_done:                                                   # 半仓止盈
            if pos is None:                                            # 全仓已无 → 不在空仓上挂 BE（孤儿单），drain 后终态（§22.5 P0）
                return terminalize(broker, symbol, pb, PBStatus.DONE_UNKNOWN.value, "tp1_then_flat")
            rest = ex["qty"] - ex["half_qty"]                          # 有半仓 → 移 SL 到 BE
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
            return terminalize(broker, symbol, pb, PBStatus.DONE_SL.value, "sl")
        return None

    if status == PBStatus.TP1_HIT.value:
        tp2_done = False
        tp2_dead = ex.get("tp2_order_id") is None
        if ex.get("tp2_order_id"):
            tp2 = safe_get_order(broker, symbol, ex["tp2_order_id"])
            if tp2 and tp2.state == OrderState.UNKNOWN:
                if pos is not None:
                    return None                                        # 有仓 + 查询 UNKNOWN → 保持（§19）
                # 无仓 → 无敞口优先 → 诚实终态（drain 后归档 §22.5）
                return terminalize(broker, symbol, pb, PBStatus.DONE_UNKNOWN.value, "exit_unknown")
            if tp2 and tp2.state == OrderState.FILLED:
                tp2_done = True
            elif tp2 is None or tp2.state in (OrderState.CANCELED, OrderState.EXPIRED, OrderState.REJECTED):
                tp2_dead = True                                        # 死单（撤/过期/拒）→ 降级（§20）
        if not tp2_done and tp2_dead and ref_price is not None and _reached_tp(ref_price, ex.get("tp2"), ps):
            try:                                                       # TP2 缺失/失效（降级）→ 到价市价平剩余
                broker.market_close(symbol, ps, ex.get("qty_remaining") or ex["qty"], f"{base}_T2M")
                tp2_done = True
            except Exception as e:
                notify.feishu_alert(f"TP2 degraded market-close failed: {symbol} — {e}")
        if tp2_done:                                                   # TP2 全达成
            return terminalize(broker, symbol, pb, PBStatus.DONE_TP2.value, "tp2")
        if pos is None:                                                # 剩余半仓没了且 TP2 未成交 = BE/SL 触发
            return terminalize(broker, symbol, pb, PBStatus.DONE_BE.value, "be")
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


def _drain_orders(broker: Broker, symbol: str, ex: dict) -> bool:
    """撤本 playbook 所有活跃订单。除 exec 里的 order_id，还按 deterministic {base}_S/_T1/_T2/_SBE 查
    （崩溃恢复时 exec 可能只剩 client_id_base，§22.5/§22 七律7）。全撤掉/已不活跃 → True；撤不掉/UNKNOWN → False。"""
    clean = True
    seen: set = set()
    candidates: list = []
    for key in ("sl_order_id", "tp1_order_id", "tp2_order_id"):
        if ex.get(key):
            candidates.append(("order_id", ex[key]))
    base = ex.get("client_id_base")
    if base:
        for sfx in PROTECTIVE_SUFFIXES:                 # 本系统全部 deterministic client id（含 _E entry / _SR 补挂 SL）
            candidates.append(("client_id", f"{base}{sfx}"))
    for by, ident in candidates:
        o = safe_get_order(broker, symbol, **{by: ident})
        if o is None:
            continue                                    # 不存在 = 已不活跃
        oid = o.order_id
        if oid in seen:
            continue                                    # 同一单经 order_id/client_id 查到两次
        seen.add(oid)
        if o.state == OrderState.UNKNOWN:
            clean = False                               # 查不清 → 不能确认已撤
            continue
        if o.state in (OrderState.NEW, OrderState.PARTIALLY_FILLED):
            try:
                broker.cancel_order(symbol, order_id=oid)
            except Exception:
                clean = False                           # 撤不掉
    return clean


def terminalize(broker: Broker, symbol: str, pb: dict, target: str, result: str) -> str:
    """进入终态前必须 drain 本 playbook 所有订单（§22.5 Terminal Means Drained：终态=无仓+无活跃挂单）。
    撤干净 → 置 target 终态；有撤不掉/UNKNOWN → 标 draining，保持非终态，由 tick 每轮重试 drain。"""
    ex = pb["exec"]
    if _drain_orders(broker, symbol, ex):
        for k in ("draining", "drain_target", "drain_result"):
            ex.pop(k, None)
        pb["status"] = target
        pb["result"] = result
        return target
    ex["draining"] = True                               # 没撤干净 → 保持非终态，tick 重试
    ex["drain_target"] = target
    ex["drain_result"] = result
    notify.feishu_alert(f"terminalize draining (orders not drained): {symbol} {ex.get('account')} → {target}")
    return pb["status"]
