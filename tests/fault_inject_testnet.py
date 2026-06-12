"""第四段：testnet 机械故障注入（Codex 矩阵）。在 btc-ml 真实 testnet/demo 跑，不连 mock。

    EXECUTOR_ENV=testnet python3 tests/fault_inject_testnet.py [scenario|all]

每个 scenario_* 函数：真实开仓/挂单到某阶段 → 注入故障 → 重启/对账恢复 → 核对交易所实际状态 → 清理。
绝不打印 key；只用 load_keys 内部构造 broker。
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

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


SCENARIOS = {
    "crash_activated_sl_removed": scenario_crash_activated_sl_removed,
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
