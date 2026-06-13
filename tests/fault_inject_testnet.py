"""第四段：testnet 机械故障注入（Codex 矩阵）。btc-ml 真实 testnet(Binance) + demo(OKX) 跑，不连 mock。

    EXECUTOR_ENV=testnet python3 tests/fault_inject_testnet.py [scenario|all] [binance|okx|both]

每个 scenario_*(label, broker, accts)：真实开仓/挂单到某阶段 → 注入故障 → 恢复 → 核对交易所实际 → 清理。
main 结尾做**全局收尾审计**：每账户无持仓 + 无 fi* 前缀活跃订单，否则整轮 FAIL（_cleanup 吞异常不算数）。
绝不打印 key；只用 load_keys 内部构造 broker。
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live import exec_config as cfg
from live.keys_loader import load_keys
from live.broker.base import PosSide, OrderState, OrderStatus
from live.executor import ExecutorEngine, build_brokers
from live.slot_pool import build_accounts
from live import reconcile

SYM = "BTC/USDT"
PS = PosSide.SHORT


def _qty(broker):
    """每家最小可用 BTC 数量（OKX BTC ctVal 0.01 → 0.01；binance → 0.002）。"""
    return 0.01 if broker.exchange == "okx" else 0.002


def _open_real(broker, base, qty=None):
    """真实开仓 + 挂三单（用 fill.price 算 level，交易所无关）。返回 exec dict。SL/TP 远离现价避免误触发。"""
    qty = qty if qty is not None else _qty(broker)
    half = round(qty / 2, 8)
    broker.set_leverage(SYM, PS, 10)
    fill = broker.market_open(SYM, PS, qty, f"{base}_E")
    entry = fill.price
    sl_price = round(entry * 1.003, 1)
    tp1, tp2 = round(entry * 0.99, 1), round(entry * 0.985, 1)
    sl_oid = broker.place_stop_market(SYM, PS, qty, sl_price, f"{base}_S")
    tp1_oid = broker.place_reduce_limit(SYM, PS, half, tp1, f"{base}_T1")
    tp2_oid = broker.place_reduce_limit(SYM, PS, half, tp2, f"{base}_T2")
    return {
        "account": broker.label, "exchange": broker.exchange, "pos_side": PS.value,
        "entry_order_id": fill.order_id, "entry_price": entry,
        "qty": qty, "qty_remaining": qty, "half_qty": half, "margin": 40, "leverage": 10,
        "sl_order_id": sl_oid, "tp1_order_id": tp1_oid, "tp2_order_id": tp2_oid,
        "sl_price": sl_price, "tp1": tp1, "tp2": tp2,
        "client_id_base": base, "tp1_filled_at": None,
    }


def _cleanup(broker, ex):
    for oid in (ex.get("sl_order_id"), ex.get("tp1_order_id"), ex.get("tp2_order_id")):
        if oid:
            try:
                broker.cancel_order(SYM, order_id=oid)
            except Exception:
                pass
    try:
        pos = broker.get_position(SYM, PS)
    except Exception:
        pos = None
    if pos:
        try:
            broker.market_close(SYM, PS, pos.qty, f"{ex['client_id_base']}_CLN{int(time.time()) % 100000}")
        except Exception:
            pass
    time.sleep(2)


# ── 故障注入器（包装真实 broker）─────────────────────────────────────────────
class _TimeoutOnceBroker:
    """market_open 真实下单后抛一次（模拟 SDK 超时但已成交），之后透传。"""
    def __init__(self, inner):
        self._inner, self._fired = inner, False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def market_open(self, *a, **k):
        fill = self._inner.market_open(*a, **k)
        if not self._fired:
            self._fired = True
            raise RuntimeError("injected SDK timeout (order actually filled)")
        return fill


class _UnknownGetOrderBroker:
    """get_order 全部返回 UNKNOWN（模拟查单 API 抖）。"""
    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_order(self, symbol, order_id=None, client_id=None):
        return OrderStatus(order_id or "", client_id or "", OrderState.UNKNOWN, 0.0, 0.0)


class _DelayedPositionBroker:
    """get_position 前 n 次返回 None（模拟仓位最终一致性延迟）。"""
    def __init__(self, inner, n=1):
        self._inner, self._left = inner, n

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_position(self, symbol, pos_side):
        if self._left > 0:
            self._left -= 1
            return None
        return self._inner.get_position(symbol, pos_side)


class _AllThrowBroker:
    """get_position / get_order 全抛（模拟断网）。"""
    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_position(self, *a, **k):
        raise RuntimeError("injected network down (get_position)")

    def get_order(self, *a, **k):
        raise RuntimeError("injected network down (get_order)")


class _TimeoutOnceCancel:
    """cancel_order 真撤后抛一次（模拟撤单超时但实际已撤）→ drain 重试见 gone → drained。"""
    def __init__(self, inner):
        self._inner, self._fired = inner, False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def cancel_order(self, symbol, order_id=None, client_id=None):
        self._inner.cancel_order(symbol, order_id=order_id, client_id=client_id)   # 真撤成功
        if not self._fired:
            self._fired = True
            raise RuntimeError("injected cancel timeout (order already cancelled)")


# ── 场景 ─────────────────────────────────────────────────────────────────────
def s_crash_activated_sl_removed(label, broker, accts):
    """① crash@ACTIVATED + SL 被撤 → reconcile 补挂 SL。"""
    base = f"fi1{broker.exchange[:1]}{int(time.time()) % 100000}"
    ex = _open_real(broker, base)
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    old = ex["sl_order_id"]
    print(f"  setup: opened sl={old}")
    broker.cancel_order(SYM, order_id=old)
    print("  inject: SL cancelled")
    reconcile.reconcile_position(broker, SYM, pb)
    new = pb["exec"].get("sl_order_id")
    st = broker.get_order(SYM, new) if new else None
    ok = bool(new) and new != old and st and st.state == OrderState.NEW
    print(f"  recover: new sl={new} state={st.state.value if st else None}  {'✓' if ok else '✗'}")
    _cleanup(broker, pb["exec"])
    return ok


def s_crash_opening_adopts(label, broker, accts):
    """② crash@OPENING（成交未挂保护单）→ _recover_opening adopt 补 SL/TP。"""
    base = f"fi2{broker.exchange[:1]}{int(time.time()) % 100000}"
    qty = _qty(broker)
    broker.set_leverage(SYM, PS, 10)
    fill = broker.market_open(SYM, PS, qty, f"{base}_E")
    ex = {"account": label, "exchange": broker.exchange, "pos_side": PS.value, "client_id_base": base,
          "direction": "short", "margin": 40, "opening_at": pd.Timestamp.now("UTC").isoformat(),
          "invalidation": {"level": round(fill.price * 1.003, 1), "dir": "above"},
          "tp1_level": round(fill.price * 0.99, 1), "tp2_level": round(fill.price * 0.985, 1)}
    pb = {"hypothesis": "X", "direction": "short", "status": "OPENING", "exec": ex}
    print(f"  setup: entry filled @ {fill.price}, no protective orders")
    reconcile._recover_opening(ExecutorEngine(None, {label: broker}, accts), SYM, pb)
    new = pb["exec"].get("sl_order_id")
    st = broker.get_order(SYM, new) if new else None
    ok = pb["status"] == "ACTIVATED" and pb["exec"].get("adopted") and st and st.state == OrderState.NEW
    print(f"  recover: status={pb['status']} adopted={pb['exec'].get('adopted')} sl={st.state.value if st else None}  {'✓' if ok else '✗'}")
    _cleanup(broker, pb["exec"])
    return ok


def s_terminal_drains_orders(label, broker, accts):
    """③ 仓位被平 + 保护单仍挂 → reconcile terminalize drain → 交易所无活跃单。"""
    base = f"fi3{broker.exchange[:1]}{int(time.time()) % 100000}"
    ex = _open_real(broker, base)
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    sl_id, t1, t2 = ex["sl_order_id"], ex["tp1_order_id"], ex["tp2_order_id"]
    broker.market_close(SYM, PS, ex["qty"], f"{base}_FLAT")
    time.sleep(2)
    print(f"  inject: closed (pos={broker.get_position(SYM, PS) or 'FLAT'}), orders resting")
    reconcile.reconcile_position(broker, SYM, pb)

    def gone(oid):
        o = broker.get_order(SYM, oid)
        return o is None or o.state in (OrderState.CANCELED, OrderState.EXPIRED, OrderState.FILLED)
    ok = pb["status"].startswith("DONE") and gone(sl_id) and gone(t1) and gone(t2)
    print(f"  recover: status={pb['status']} draining={pb['exec'].get('draining')}  {'✓ all drained' if ok else '✗'}")
    _cleanup(broker, ex)
    return ok


def s_drain_unknown_then_retry(label, broker, accts):
    """⑤ drain 时查单 UNKNOWN → draining 保持非终态；恢复后 _try_drain → DONE。"""
    base = f"fi5{broker.exchange[:1]}{int(time.time()) % 100000}"
    ex = _open_real(broker, base)
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    broker.market_close(SYM, PS, ex["qty"], f"{base}_FLAT"); time.sleep(2)
    reconcile.reconcile_position(_UnknownGetOrderBroker(broker), SYM, pb)
    draining_ok = pb["status"] != "DONE_UNKNOWN" and pb["exec"].get("draining")
    print(f"  inject: get_order UNKNOWN → status={pb['status']} draining={pb['exec'].get('draining')}")
    eng = ExecutorEngine(None, {label: broker}, accts)
    eng._try_drain(broker, SYM, pb)
    ok = bool(draining_ok) and pb["status"].startswith("DONE")
    print(f"  recover: _try_drain → status={pb['status']}  {'✓' if ok else '✗'}")
    _cleanup(broker, ex)
    return ok


def s_position_lag_grace(label, broker, accts):
    """⑥ OPENING + entry 成交但仓位延迟可见 → grace 保持 OPENING；仓位出现后 adopt。"""
    base = f"fi6{broker.exchange[:1]}{int(time.time()) % 100000}"
    qty = _qty(broker)
    broker.set_leverage(SYM, PS, 10)
    fill = broker.market_open(SYM, PS, qty, f"{base}_E")
    ex = {"account": label, "exchange": broker.exchange, "pos_side": PS.value, "client_id_base": base,
          "direction": "short", "margin": 40, "opening_at": pd.Timestamp.now("UTC").isoformat(),
          "invalidation": {"level": round(fill.price * 1.003, 1), "dir": "above"},
          "tp1_level": round(fill.price * 0.99, 1), "tp2_level": round(fill.price * 0.985, 1)}
    pb = {"hypothesis": "X", "direction": "short", "status": "OPENING", "exec": ex}
    reconcile._recover_opening(ExecutorEngine(None, {label: _DelayedPositionBroker(broker)}, accts), SYM, pb)
    held = pb["status"] == "OPENING"
    print(f"  inject: pos lag → held OPENING={held}")
    reconcile._recover_opening(ExecutorEngine(None, {label: broker}, accts), SYM, pb)
    ok = held and pb["status"] == "ACTIVATED" and pb["exec"].get("adopted")
    print(f"  recover: status={pb['status']} adopted={pb['exec'].get('adopted')}  {'✓' if ok else '✗'}")
    _cleanup(broker, pb["exec"])
    return ok


def s_crash_tp1hit_be_removed(label, broker, accts):
    """⑦ crash@TP1_HIT（半仓+BE）+ BE 被撤 → reconcile 补挂 BE。"""
    base = f"fi7{broker.exchange[:1]}{int(time.time()) % 100000}"
    qty = _qty(broker); half = round(qty / 2, 8)
    broker.set_leverage(SYM, PS, 10)
    fill = broker.market_open(SYM, PS, half, f"{base}_E")          # 剩半仓
    be_price = round(fill.price * 1.001, 1)
    be = broker.place_stop_market(SYM, PS, half, be_price, f"{base}_SBE")
    tp2 = broker.place_reduce_limit(SYM, PS, half, round(fill.price * 0.985, 1), f"{base}_T2")
    ex = {"account": label, "exchange": broker.exchange, "pos_side": PS.value, "client_id_base": base,
          "direction": "short", "qty": qty, "qty_remaining": half, "half_qty": half,
          "sl_order_id": be, "tp2_order_id": tp2, "tp1_order_id": None,
          "sl_price": be_price, "entry_price": fill.price,
          "tp1": round(fill.price * 0.99, 1), "tp2": round(fill.price * 0.985, 1)}
    pb = {"hypothesis": "X", "direction": "short", "status": "TP1_HIT", "exec": ex}
    old = ex["sl_order_id"]
    print(f"  setup: TP1_HIT BE={old}")
    broker.cancel_order(SYM, order_id=old)
    reconcile.reconcile_position(broker, SYM, pb)
    new = pb["exec"].get("sl_order_id")
    st = broker.get_order(SYM, new) if new else None
    ok = bool(new) and new != old and st and st.state == OrderState.NEW
    print(f"  recover: new BE={new} state={st.state.value if st else None}  {'✓' if ok else '✗'}")
    _cleanup(broker, ex)
    return ok


def s_market_open_timeout_recovers(label, broker, accts):
    """④ market_open 超时但成交 → open_position 查持仓接管，不重发不翻倍（§18）。"""
    from live.position_manager import open_position
    base = f"fi4{broker.exchange[:1]}{int(time.time()) % 100000}"
    # 先拿一个成交价基准（开一笔再平，得到现价）——用 _qty 一手
    probe = broker.market_open(SYM, PS, _qty(broker), f"{base}p_E")
    P = probe.price
    broker.market_close(SYM, PS, _qty(broker), f"{base}p_C"); time.sleep(2)
    pb = {"direction": "short", "r_dist_pct": 0.5,
          "invalidation": {"level": round(P * 1.003, 1), "dir": "above"},
          "tp1_level": round(P * 0.99, 1), "tp2_level": round(P * 0.985, 1)}
    ex = open_position(_TimeoutOnceBroker(broker), SYM, pb, P, label, 40, base)
    pos = broker.get_position(SYM, PS)
    qty = pos.qty if pos else 0
    ok = bool(pos) and qty < ex["qty"] * 1.5 and ex["entry_price"] > 0
    print(f"  recover: pos={qty} sized={ex['qty']} entry={ex['entry_price']}  {'✓ no double' if ok else '✗'}")
    _cleanup(broker, ex)
    return ok


def s_network_down_no_crash(label, broker, accts):
    """⑧ 断网（get_* 全抛）→ reconcile/recover 不崩、保持态、不臆测终态。纯状态。"""
    base = f"fi8{broker.exchange[:1]}{int(time.time()) % 100000}"
    fb = _AllThrowBroker(broker)
    eng = ExecutorEngine(None, {label: fb}, accts)
    pb_a = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED",
            "exec": {"account": label, "pos_side": "SHORT", "client_id_base": base,
                     "sl_order_id": "x", "tp1_order_id": "y", "tp2_order_id": "z", "qty": 0.002, "sl_price": 63000.0}}
    r = reconcile.reconcile_position(fb, SYM, pb_a)
    pb_o = {"hypothesis": "X", "direction": "short", "status": "OPENING",
            "exec": {"account": label, "pos_side": "SHORT", "client_id_base": base, "direction": "short",
                     "opening_at": pd.Timestamp.now("UTC").isoformat(), "invalidation": {"level": 63000.0, "dir": "above"}}}
    reconcile._recover_opening(eng, SYM, pb_o)
    ok = r == "unknown" and pb_a["status"] == "ACTIVATED" and pb_o["status"] == "OPENING"
    print(f"  inject: net down → reconcile={r} ACTIVATED held={pb_a['status']=='ACTIVATED'} OPENING held={pb_o['status']=='OPENING'}  {'✓' if ok else '✗'}")
    return ok


def s_cancel_timeout_drains(label, broker, accts):
    """⑨ #4 撤单超时半成功：drain 时 cancel 抛但实际已撤 → draining；_try_drain 重试见 gone → drained。
    binance-only：OKX 平仓后自动清关联 algo/limit 单，drain 时无 resting order 可触发 cancel 超时
    （OKX cancel 契约由 terminal_drains_orders[okx] + _check_cancel sCode-gone 覆盖）。"""
    base = f"fi9{broker.exchange[:1]}{int(time.time()) % 100000}"
    ex = _open_real(broker, base)
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    sl, t1, t2 = ex["sl_order_id"], ex["tp1_order_id"], ex["tp2_order_id"]
    broker.market_close(SYM, PS, ex["qty"], f"{base}_FLAT"); time.sleep(2)
    # drain 时撤单超时一次（实际撤成功）→ drain 视为没撤干净 → draining（非终态）
    reconcile.reconcile_position(_TimeoutOnceCancel(broker), SYM, pb)
    draining = pb["exec"].get("draining")
    eng = ExecutorEngine(None, {label: broker}, accts)
    if draining:
        eng._try_drain(broker, SYM, pb)        # 重试：order 已撤 → cancel gone → drained

    def gone(oid):
        o = broker.get_order(SYM, oid)
        return o is None or o.state in (OrderState.CANCELED, OrderState.EXPIRED, OrderState.FILLED)
    ok = bool(draining) and pb["status"].startswith("DONE") and gone(sl) and gone(t1) and gone(t2)
    print(f"  inject: cancel timeout → draining={draining} → status={pb['status']}  {'✓ drained' if ok else '✗'}")
    _cleanup(broker, ex)
    return ok


# 场景 → 适用交易所
SCENARIOS = {
    "crash_activated_sl_removed": (s_crash_activated_sl_removed, ("binance", "okx")),
    "crash_opening_adopts": (s_crash_opening_adopts, ("binance", "okx")),
    "terminal_drains_orders": (s_terminal_drains_orders, ("binance", "okx")),
    "drain_unknown_then_retry": (s_drain_unknown_then_retry, ("binance", "okx")),
    "position_lag_grace": (s_position_lag_grace, ("binance", "okx")),
    "crash_tp1hit_be_removed": (s_crash_tp1hit_be_removed, ("binance", "okx")),
    "market_open_timeout_recovers": (s_market_open_timeout_recovers, ("binance",)),
    "network_down_no_crash": (s_network_down_no_crash, ("binance", "okx")),
    "cancel_timeout_drains": (s_cancel_timeout_drains, ("binance",)),
}


def _active_fi_orders(b):
    """查 fi* 前缀的活跃订单（普通 + algo）。查询异常**向上抛**，由审计判 UNKNOWN→DIRTY
    （§22 哲学：查不清绝不吞成空 = 干净）。"""
    out = []
    if b.exchange == "binance":
        for o in b.client.get_orders(symbol="BTCUSDT"):
            if o.get("status") in ("NEW", "PARTIALLY_FILLED") and str(o.get("clientOrderId", "")).startswith("fi"):
                out.append(o["clientOrderId"])
        algos = b.client.sign_request("GET", "/fapi/v1/openAlgoOrders", {"symbol": "BTCUSDT"})
        for a in (algos if isinstance(algos, list) else algos.get("orders", [])):
            if str(a.get("clientAlgoId", "")).startswith("fi"):
                out.append(a["clientAlgoId"])
    else:
        for o in b._data(b.trade.get_order_list(instType="SWAP", instId="BTC-USDT-SWAP")):
            if str(o.get("clOrdId", "")).startswith("fi"):
                out.append(o["clOrdId"])
        for a in b._data(b.trade.order_algos_list(ordType="conditional", instId="BTC-USDT-SWAP")):
            if str(a.get("algoClOrdId", "")).startswith("fi"):
                out.append(a["algoClOrdId"])
    return out


def _final_audit(test_brokers):
    """全局收尾审计（§22：查不清 ≠ 干净）：每账户无持仓 + 无 fi* 活跃单；
    任一查询异常 → UNKNOWN → 审计 DIRTY（绝不把查询失败当 CLEAN）。_cleanup 吞异常不算数。"""
    print("=== FINAL AUDIT (no position + no fi* active orders) ===")
    clean = True
    for label, b in test_brokers:
        unknown = False
        for ps in (PosSide.SHORT, PosSide.LONG):
            try:
                pos = b.get_position(SYM, ps)
            except Exception as e:
                print(f"  ✗ {label} {ps.value} POSITION QUERY FAILED → UNKNOWN ({str(e)[:50]})")
                clean = False; unknown = True; continue
            if pos:
                print(f"  ✗ {label} {ps.value} RESIDUAL POSITION qty={pos.qty}")
                clean = False
        try:
            left = _active_fi_orders(b)
        except Exception as e:
            print(f"  ✗ {label} ORDER QUERY FAILED → UNKNOWN ({str(e)[:50]})")
            clean = False; unknown = True; left = None
        if left:
            print(f"  ✗ {label} RESIDUAL fi* ORDERS: {left}")
            clean = False
        elif not unknown:
            print(f"  ✓ {label} clean")
    print(f"=== AUDIT {'CLEAN ✓' if clean else 'DIRTY ✗ (round FAILED)'} ===")
    return clean


def _test_brokers(brokers, which):
    out = []
    for label, b in brokers.items():
        if which == "both" or b.exchange == which:
            out.append((label, b))
    # 每家只取一个账户（testnet 各 1）
    seen = set(); uniq = []
    for label, b in out:
        if b.exchange not in seen:
            seen.add(b.exchange); uniq.append((label, b))
    return uniq


def main():
    keys = load_keys("testnet")
    accts = build_accounts(keys)
    brokers = build_brokers(keys)
    name_arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    exch_arg = sys.argv[2] if len(sys.argv) > 2 else "both"
    tb = _test_brokers(brokers, exch_arg)
    names = list(SCENARIOS) if name_arg == "all" else [name_arg]
    results = {}
    for name in names:
        fn, applies = SCENARIOS[name]
        for label, broker in tb:
            if broker.exchange not in applies:
                continue
            key = f"{name}[{broker.exchange}]"
            print(f"=== {key} ===")
            try:
                results[key] = fn(label, broker, accts)
            except Exception:
                import traceback
                traceback.print_exc()
                results[key] = False
    audit_clean = _final_audit(tb)
    print("=== summary ===")
    all_pass = all(results.values()) if results else False
    for key, ok in results.items():
        print(f"  {key}: {'PASS' if ok else 'FAIL'}")
    print(f"  FINAL AUDIT: {'CLEAN' if audit_clean else 'DIRTY'}")
    # 门禁（可交给 watchdog/CI/部署）：任一场景 FAIL 或 audit DIRTY → 非零退出
    if not (all_pass and audit_clean):
        print("RESULT: FAILED ✗")
        sys.exit(1)
    print("RESULT: ALL PASS ✓")


if __name__ == "__main__":
    main()
