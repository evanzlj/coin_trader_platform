"""#2 进程级故障注入 drill（Codex 要求的真杀进程演练）。btc-ml 真实 testnet 跑。

    EXECUTOR_ENV=testnet python3 tests/process_drill_testnet.py

真启 `python3 -m live.executor` 子进程（隔离的 signal_active/lock/DB via env）→ 它自己
激活开仓写 ACTIVATED state.json → kill -9（崩溃）→ 崩溃期撤 SL → 重启 executor.py →
startup_reconcile 从 state.json 恢复 + 补挂 SL → 核对交易所 + state.json 收敛 → 清理 + 审计。
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from live import exec_config as cfg
from live.keys_loader import load_keys
from live.executor import build_brokers
from live.broker.base import PosSide, OrderState

SYM, PS = "BTC/USDT", PosSide.SHORT
PKG = "btcusdt_A_drill"


def _drill_env(tmp: Path) -> dict:
    env = dict(os.environ)
    env.update({
        "EXECUTOR_ENV": "testnet",
        "SIGNAL_ACTIVE": str(tmp / "active"), "SIGNAL_DONE": str(tmp / "done"),
        "HEARTBEAT_FILE": str(tmp / "hb.txt"), "LOCK_FILE": str(tmp / "executor.lock"),
        "CURSOR_FILE": str(tmp / "cursor.txt"), "TRADE_LOG": str(tmp / "trade_log.jsonl"),
        "OHLCV_DB": str(tmp / "o.db"),
    })
    return env


def _build_db(db: Path, P: float):
    now = pd.Timestamp.now("UTC"); lc = now.floor("15min")
    rows = []
    for dm, o, h, l, c in [(-60, 1.002, 1.003, 1.0015, 1.002), (-45, 1.001, 1.002, 0.9995, 1.000),
                           (-30, 1.000, 1.001, 0.998, 0.999), (-15, 0.999, 1.000, 0.996, 0.9965)]:
        ot = lc + pd.Timedelta(minutes=dm)
        rows.append(("BTC/USDT", "15m", ot.isoformat(), (ot + pd.Timedelta(minutes=15)).isoformat(),
                     P * o, P * h, P * l, P * c, 100))
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE ohlcv_bars(symbol TEXT,timeframe TEXT,open_time TIMESTAMP,close_time TIMESTAMP,"
                "open REAL,high REAL,low REAL,close REAL,volume REAL)")
    con.executemany("INSERT INTO ohlcv_bars(symbol,timeframe,open_time,close_time,open,high,low,close,volume)"
                    " VALUES(?,?,?,?,?,?,?,?,?)", rows)
    con.commit(); con.close()
    return lc


def _write_signal(active: Path, P: float):
    prim, inval, act = round(P, 1), round(P * 1.003, 1), round(P * 0.997, 1)
    tp1, tp2 = round(P * 0.99, 1), round(P * 0.985, 1)
    r_dist = abs(act - inval) / act * 100
    now = pd.Timestamp.now("UTC"); lc = now.floor("15min")
    bar_time = (lc + pd.Timedelta(minutes=-60)).isoformat()
    state = {"signal_dir": PKG, "symbol": "BTC/USDT", "overall_status": "WATCHING", "bar_time": bar_time,
             "playbooks": [{"hypothesis": "DOWNSIDE", "direction": "short",
                            "status": "WAITING_FOR_PRIMARY_TOUCH",
                            "primary_touch": {"level": prim, "side": "low"},
                            "activates_if": {"level": act, "dir": "below"},
                            "cancels_if": {"level": inval, "dir": "above"},
                            "invalidation": {"level": inval, "dir": "above"},
                            "tp1_level": tp1, "tp2_level": tp2, "r_dist_pct": round(r_dist, 3)}]}
    pkg = active / PKG
    pkg.mkdir(parents=True)
    (pkg / "state.json").write_text(json.dumps(state))
    (pkg / ".ready").touch()
    return pkg


def _read_state(pkg_active: Path, pkg_done: Path):
    for p in (pkg_active / PKG / "state.json", pkg_done / PKG / "state.json"):
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None


def _wait(cond_fn, timeout=70, poll=2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = cond_fn()
        if v:
            return v
        time.sleep(poll)
    return None


def main():
    keys = load_keys("testnet")
    brokers = build_brokers(keys)
    binance = next(b for b in brokers.values() if b.exchange == "binance")
    okx = next((b for b in brokers.values() if b.exchange == "okx"), None)
    P = float(binance.client.ticker_price(symbol="BTCUSDT")["price"])

    tmp = Path(tempfile.mkdtemp())
    (tmp / "active").mkdir()
    env = _drill_env(tmp)
    _build_db(tmp / "o.db", P)
    _write_signal(tmp / "active", P)
    print(f"drill setup: P={P}, isolated env at {tmp}")

    # === 启 executor.py 子进程，等它激活开仓 ===
    print("launching executor.py subprocess #1 ...")
    log1 = open(tmp / "exec1.log", "w")
    p1 = subprocess.Popen([sys.executable, "-m", "live.executor"], env=env, cwd=str(ROOT),
                          stdout=log1, stderr=subprocess.STDOUT)

    def _opened():
        st = _read_state(tmp / "active", tmp / "done")
        if st and st["playbooks"][0]["status"] == "ACTIVATED":
            return st["playbooks"][0]["exec"]
        return None
    ex = _wait(_opened, timeout=70)
    if not ex:
        p1.kill(); p1.wait()
        print("DRILL FAIL: executor did not open in time. exec1.log tail:")
        print((tmp / "exec1.log").read_text()[-2500:])
        return
    acct, exch, sl_oid = ex["account"], ex["exchange"], ex.get("sl_order_id")
    broker = next(b for b in brokers.values() if b.exchange == exch)
    print(f"  opened: ACTIVATED on {acct}({exch}) sl={sl_oid}")

    # === kill -9 (真崩溃) ===
    p1.kill(); p1.wait()
    print("  KILL -9 (crash)")
    broker.cancel_order(SYM, order_id=sl_oid)          # 崩溃期 SL 被撤
    print("  inject: SL cancelled during crash window")

    # === 重启 executor.py → startup_reconcile 恢复 ===
    print("relaunching executor.py subprocess #2 ...")
    log2 = open(tmp / "exec2.log", "w")
    p2 = subprocess.Popen([sys.executable, "-m", "live.executor"], env=env, cwd=str(ROOT),
                          stdout=log2, stderr=subprocess.STDOUT)

    def _rearmed():
        st = _read_state(tmp / "active", tmp / "done")
        if not st:
            return None
        new = st["playbooks"][0].get("exec", {}).get("sl_order_id")
        return new if (new and new != sl_oid) else None
    new_sl = _wait(_rearmed, timeout=70)
    p2.kill(); p2.wait()
    if not new_sl:
        print("  WARN: SL not rearmed in time. exec2.log tail:")
        print((tmp / "exec2.log").read_text()[-2500:])

    st = broker.get_order(SYM, new_sl) if new_sl else None
    ok = bool(new_sl) and new_sl != sl_oid and st and st.state == OrderState.NEW
    print(f"  recover: state.json new sl={new_sl}, exchange state={st.state.value if st else None}  {'✓' if ok else '✗'}")

    # === 清理 + 审计 ===
    final = _read_state(tmp / "active", tmp / "done")
    fex = final["playbooks"][0].get("exec", {}) if final else {}
    for oid in (fex.get("sl_order_id"), fex.get("tp1_order_id"), fex.get("tp2_order_id")):
        if oid:
            try:
                broker.cancel_order(SYM, order_id=oid)
            except Exception:
                pass
    pos = broker.get_position(SYM, PS)
    if pos:
        broker.market_close(SYM, PS, pos.qty, f"{fex.get('client_id_base', 'drill')}_CLN")
    time.sleep(2)
    flat = broker.get_position(SYM, PS) or "FLAT"
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"  cleanup: {flat}")
    print(f"DRILL (process kill -9 + restart recover): {'PASS ✓' if ok and flat == 'FLAT' else 'FAIL ✗'}")


if __name__ == "__main__":
    main()
