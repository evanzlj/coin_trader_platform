"""第四段：testnet 机械故障注入（Codex 矩阵）。在 btc-ml 真实 testnet/demo 跑，不连 mock。

    EXECUTOR_ENV=testnet python3 tests/fault_inject_testnet.py [scenario|all]

每个 scenario_* 函数：真实开仓/挂单到某阶段 → 注入故障 → 重启/对账恢复 → 核对交易所实际状态 → 清理。
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
from live.broker.base import PosSide, OrderState
from live.executor import ExecutorEngine, build_brokers
from live.slot_pool import build_accounts
from live import reconcile


def _binance_broker(brokers):
    for label, b in brokers.items():
        if getattr(b, "exchange", "") == "binance":
            return label, b
    raise RuntimeError("no binance broker in keys")


def _open_real(broker, base, sym="BTC/USDT", ps=PosSide.SHORT):
    """真实开仓 + 挂三单，返回 (exec dict, levels)。SL/TP 远离现价，避免误触发。"""
    P = float(broker.client.ticker_price(symbol="BTCUSDT")["price"])
    broker.set_leverage(sym, ps, 10)
    fill = broker.market_open(sym, ps, 0.002, f"{base}_E")
    entry = fill.price
    sl_price = round(entry * 1.003, 1)          # short SL 上方
    tp1 = round(entry * 0.99, 1)                # short TP 下方
    tp2 = round(entry * 0.985, 1)
    sl_oid = broker.place_stop_market(sym, ps, 0.002, sl_price, f"{base}_S")
    tp1_oid = broker.place_reduce_limit(sym, ps, 0.001, tp1, f"{base}_T1")
    tp2_oid = broker.place_reduce_limit(sym, ps, 0.001, tp2, f"{base}_T2")
    ex = {
        "account": None, "exchange": broker.exchange, "pos_side": ps.value,
        "entry_order_id": fill.order_id, "entry_price": entry,
        "qty": 0.002, "qty_remaining": 0.002, "half_qty": 0.001,
        "margin": 40, "leverage": 10,
        "sl_order_id": sl_oid, "tp1_order_id": tp1_oid, "tp2_order_id": tp2_oid,
        "sl_price": sl_price, "tp1": tp1, "tp2": tp2,
        "client_id_base": base, "tp1_filled_at": None,
    }
    return ex


def _cleanup(broker, ex, sym="BTC/USDT", ps=PosSide.SHORT):
    for oid in (ex.get("sl_order_id"), ex.get("tp1_order_id"), ex.get("tp2_order_id")):
        if oid:
            try:
                broker.cancel_order(sym, order_id=oid)
            except Exception:
                pass
    pos = None
    try:
        pos = broker.get_position(sym, ps)
    except Exception:
        pass
    if pos:
        try:
            broker.market_close(sym, ps, pos.qty, f"{ex['client_id_base']}_CLN{int(time.time()) % 100000}")
        except Exception:
            pass
    time.sleep(2)


def scenario_crash_activated_sl_removed(brokers, accts):
    """① 进程崩溃在 ACTIVATED + 崩溃期 SL 被撤 → 重启 reconcile 应补挂 SL（§19/§22）。"""
    label, broker = _binance_broker(brokers)
    sym, ps = "BTC/USDT", PosSide.SHORT
    base = f"fi1{int(time.time()) % 1000000}"
    ex = _open_real(broker, base)
    ex["account"] = label
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    old_sl_id = ex["sl_order_id"]                        # 保存字符串（reconcile 会改 ex["sl_order_id"]）
    print(f"  setup: opened, sl={old_sl_id}")

    # —— 注入：崩溃期 SL 被撤 ——
    broker.cancel_order(sym, order_id=old_sl_id)
    print("  inject: SL cancelled (simulating crash-window loss)")

    # —— 重启对账 ——
    eng = ExecutorEngine(None, brokers, accts)
    reconcile.reconcile_position(broker, sym, pb)        # startup 对单个 pb 的核对
    new_sl = pb["exec"].get("sl_order_id")
    print(f"  recover: new sl={new_sl} (changed={new_sl != old_sl_id})")
    st = broker.get_order(sym, new_sl) if new_sl else None
    ok = new_sl and new_sl != old_sl_id and st and st.state == OrderState.NEW
    print(f"  RESULT: {'✓ SL re-armed' if ok else '✗ FAILED'}  (new SL state={st.state.value if st else None})")

    _cleanup(broker, pb["exec"])
    print(f"  cleanup: {broker.get_position(sym, ps) or 'FLAT'}")
    return ok


def scenario_crash_opening_adopts(brokers, accts):
    """② 进程崩溃在 OPENING（market_open 成交但 SL 未挂、exec 未写回）→ 重启 _recover_opening 接管补 SL/TP（§18）。"""
    label, broker = _binance_broker(brokers)
    sym, ps = "BTC/USDT", PosSide.SHORT
    base = f"fi2{int(time.time()) % 1000000}"
    P = float(broker.client.ticker_price(symbol="BTCUSDT")["price"])
    broker.set_leverage(sym, ps, 10)
    fill = broker.market_open(sym, ps, 0.002, f"{base}_E")    # 只开仓，不挂 SL/TP（崩溃在 arming 前）
    inval = round(fill.price * 1.003, 1)
    print(f"  setup: market_open filled @ {fill.price}, no SL/TP yet (crash before arming)")
    ex = {"account": label, "exchange": broker.exchange, "pos_side": ps.value,
          "client_id_base": base, "direction": "short", "margin": 40,
          "opening_at": pd.Timestamp.now("UTC").isoformat(),
          "invalidation": {"level": inval, "dir": "above"},
          "tp1_level": round(fill.price * 0.99, 1), "tp2_level": round(fill.price * 0.985, 1)}
    pb = {"hypothesis": "X", "direction": "short", "status": "OPENING", "exec": ex}

    eng = ExecutorEngine(None, brokers, accts)
    reconcile._recover_opening(eng, sym, pb)                  # 崩溃恢复：查 {base}_E + 持仓 → adopt
    new_sl = pb["exec"].get("sl_order_id")
    print(f"  recover: status={pb['status']} sl={new_sl} tp1={pb['exec'].get('tp1_order_id')} adopted={pb['exec'].get('adopted')}")
    sl_st = broker.get_order(sym, new_sl) if new_sl else None
    ok = (pb["status"] == "ACTIVATED" and pb["exec"].get("adopted")
          and sl_st and sl_st.state == OrderState.NEW)
    print(f"  RESULT: {'✓ adopted + SL armed on exchange' if ok else '✗ FAILED'}")
    _cleanup(broker, pb["exec"])
    print(f"  cleanup: {broker.get_position(sym, ps) or 'FLAT'}")
    return ok


def scenario_terminal_drains_orders(brokers, accts):
    """③ 仓位被平（模拟 SL 触发/手动）但 TP/SL 仍挂 → reconcile 终态前 terminalize drain，交易所确认无活跃单（§22.5）。"""
    label, broker = _binance_broker(brokers)
    sym, ps = "BTC/USDT", PosSide.SHORT
    base = f"fi3{int(time.time()) % 1000000}"
    ex = _open_real(broker, base); ex["account"] = label
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    sl_id, tp1_id, tp2_id = ex["sl_order_id"], ex["tp1_order_id"], ex["tp2_order_id"]
    print(f"  setup: opened, sl/tp1/tp2 = {sl_id}/{tp1_id}/{tp2_id}")

    # —— 注入：平仓（模拟 SL 触发/手动平），保护单仍挂 ——
    broker.market_close(sym, ps, 0.002, f"{base}_FLAT")
    time.sleep(2)
    print(f"  inject: position closed (pos={broker.get_position(sym, ps) or 'FLAT'}), orders still resting")

    # —— 对账：无仓终态 → terminalize drain ——
    eng = ExecutorEngine(None, brokers, accts)
    reconcile.reconcile_position(broker, sym, pb)
    print(f"  recover: status={pb['status']} draining={pb['exec'].get('draining')}")

    def _gone(oid):
        o = broker.get_order(sym, oid)
        return o is None or o.state in (OrderState.CANCELED, OrderState.EXPIRED, OrderState.FILLED)
    ok = pb["status"].startswith("DONE") and _gone(sl_id) and _gone(tp1_id) and _gone(tp2_id)
    print(f"  RESULT: {'✓ all orders drained on exchange' if ok else '✗ FAILED'}")
    _cleanup(broker, ex)
    return ok


class _TimeoutOnceBroker:
    """包装真实 broker：market_open 真实下单后抛一次（模拟 SDK 超时但订单已成交），之后透传。"""
    def __init__(self, inner):
        self._inner = inner
        self._fired = False

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def market_open(self, *a, **k):
        fill = self._inner.market_open(*a, **k)              # 真实下单（已成交）
        if not self._fired:
            self._fired = True
            raise RuntimeError("injected SDK timeout (order actually filled)")
        return fill


class _UnknownGetOrderBroker:
    """包装：get_order 前 n 次返回 UNKNOWN（模拟查单 API 抖），之后透传。"""
    def __init__(self, inner, n=99):
        self._inner = inner
        self._left = n

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_order(self, symbol, order_id=None, client_id=None):
        if self._left > 0:
            self._left -= 1
            from live.broker.base import OrderStatus, OrderState as _OS
            return OrderStatus(order_id or "", client_id or "", _OS.UNKNOWN, 0.0, 0.0)
        return self._inner.get_order(symbol, order_id=order_id, client_id=client_id)


def scenario_drain_unknown_then_retry(brokers, accts):
    """⑤ 终态 drain 时查单 UNKNOWN → draining 保持非终态；恢复后 _try_drain 重试 → DONE（§22.5）。"""
    label, broker = _binance_broker(brokers)
    sym, ps = "BTC/USDT", PosSide.SHORT
    base = f"fi5{int(time.time()) % 1000000}"
    ex = _open_real(broker, base); ex["account"] = label
    pb = {"hypothesis": "X", "direction": "short", "status": "ACTIVATED", "exec": ex}
    broker.market_close(sym, ps, 0.002, f"{base}_FLAT"); time.sleep(2)
    print(f"  setup: opened + closed (FLAT), orders resting")

    # —— 注入：drain 时 get_order 全 UNKNOWN（查不清）→ 不能归档 ——
    fb = _UnknownGetOrderBroker(broker)
    reconcile.reconcile_position(fb, sym, pb)
    print(f"  inject: get_order UNKNOWN → status={pb['status']} draining={pb['exec'].get('draining')}")
    draining_ok = pb["status"] != "DONE_UNKNOWN" and pb["exec"].get("draining")

    # —— 恢复：get_order 正常 → _try_drain 撤干净 → DONE ——
    eng = ExecutorEngine(None, {label: broker}, accts)
    drained = eng._try_drain(broker, sym, pb)
    print(f"  recover: _try_drain → status={pb['status']} (drained={drained})")
    ok = bool(draining_ok) and pb["status"].startswith("DONE")
    print(f"  RESULT: {'✓ held draining then drained' if ok else '✗ FAILED'}")
    _cleanup(broker, ex)
    return ok


class _DelayedPositionBroker:
    """包装：get_position 前 n 次返回 None（模拟仓位最终一致性延迟），之后透传。"""
    def __init__(self, inner, n=1):
        self._inner = inner
        self._left = n

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_position(self, symbol, pos_side):
        if self._left > 0:
            self._left -= 1
            return None
        return self._inner.get_position(symbol, pos_side)


def scenario_position_lag_grace(brokers, accts):
    """⑥ OPENING + entry FILLED 但仓位延迟可见 → grace 内保持 OPENING；仓位出现后 adopt（§22.5 grace 最终一致性）。"""
    label, broker = _binance_broker(brokers)
    sym, ps = "BTC/USDT", PosSide.SHORT
    base = f"fi6{int(time.time()) % 1000000}"
    broker.set_leverage(sym, ps, 10)
    fill = broker.market_open(sym, ps, 0.002, f"{base}_E")    # entry 真实成交
    inval = round(fill.price * 1.003, 1)
    ex = {"account": label, "exchange": "binance", "pos_side": ps.value, "client_id_base": base,
          "direction": "short", "margin": 40, "opening_at": pd.Timestamp.now("UTC").isoformat(),
          "invalidation": {"level": inval, "dir": "above"},
          "tp1_level": round(fill.price * 0.99, 1), "tp2_level": round(fill.price * 0.985, 1)}
    pb = {"hypothesis": "X", "direction": "short", "status": "OPENING", "exec": ex}
    print(f"  setup: entry filled @ {fill.price}, position will lag")

    # —— 注入：get_position 延迟返回 None + grace 内 → 保持 OPENING ——
    db = _DelayedPositionBroker(broker, n=1)
    reconcile._recover_opening(ExecutorEngine(None, {label: db}, accts), sym, pb)
    held = pb["status"] == "OPENING"
    print(f"  inject: pos lag → status={pb['status']} (held OPENING={held})")

    # —— 恢复：仓位可见 → adopt ——
    reconcile._recover_opening(ExecutorEngine(None, {label: broker}, accts), sym, pb)
    print(f"  recover: pos visible → status={pb['status']} adopted={pb['exec'].get('adopted')}")
    ok = held and pb["status"] == "ACTIVATED" and pb["exec"].get("adopted")
    print(f"  RESULT: {'✓ grace held then adopted' if ok else '✗ FAILED'}")
    _cleanup(broker, pb["exec"])
    print(f"  cleanup: {broker.get_position(sym, ps) or 'FLAT'}")
    return ok


def scenario_market_open_timeout_recovers(brokers, accts):
    """④ market_open 超时但实际成交 → open_position 查持仓接管，**不重发** → 不翻倍开仓（§18 真实验证）。
    重要发现：binance MARKET 单 client_id 不防重复（成交即完成 → 同 id 重发 = 新仓），所以幂等
    必须靠 executor「超时不重发、查持仓接管」，不能靠交易所 client_id 唯一性。"""
    from live.position_manager import open_position
    label, broker = _binance_broker(brokers)
    sym, ps = "BTC/USDT", PosSide.SHORT
    base = f"fi4{int(time.time()) % 1000000}"
    P = float(broker.client.ticker_price(symbol="BTCUSDT")["price"])
    pb = {"direction": "short", "r_dist_pct": 0.5,
          "invalidation": {"level": round(P * 1.003, 1), "dir": "above"},
          "tp1_level": round(P * 0.99, 1), "tp2_level": round(P * 0.985, 1)}
    wrapped = _TimeoutOnceBroker(broker)
    ex = open_position(wrapped, sym, pb, P, label, 40, base)  # market_open 抛 → 查持仓接管
    pos = broker.get_position(sym, ps)
    qty = pos.qty if pos else 0
    ok = bool(pos) and qty < ex["qty"] * 1.5 and ex["entry_price"] > 0   # 没翻倍 + entry 接管出来
    print(f"  RESULT: {'✓ recovered, no double-open' if ok else '✗ FAILED'} pos={qty} sized={ex['qty']} entry={ex['entry_price']}")
    _cleanup(broker, ex)
    print(f"  cleanup: {broker.get_position(sym, ps) or 'FLAT'}")
    return ok


SCENARIOS = {
    "crash_activated_sl_removed": scenario_crash_activated_sl_removed,
    "crash_opening_adopts": scenario_crash_opening_adopts,
    "terminal_drains_orders": scenario_terminal_drains_orders,
    "market_open_timeout_recovers": scenario_market_open_timeout_recovers,
    "drain_unknown_then_retry": scenario_drain_unknown_then_retry,
    "position_lag_grace": scenario_position_lag_grace,
}


def main():
    keys = load_keys("testnet")
    accts = build_accounts(keys)
    brokers = build_brokers(keys)
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    names = list(SCENARIOS) if which == "all" else [which]
    results = {}
    for name in names:
        print(f"=== scenario: {name} ===")
        try:
            results[name] = SCENARIOS[name](brokers, accts)
        except Exception as e:
            import traceback
            traceback.print_exc()
            results[name] = False
    print("=== summary ===")
    for name, ok in results.items():
        print(f"  {name}: {'PASS' if ok else 'FAIL'}")


if __name__ == "__main__":
    main()
