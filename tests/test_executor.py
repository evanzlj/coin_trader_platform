"""
Executor 回归测试（task #14）。标准库 unittest，无需 pytest。

运行：  python3 -m unittest tests.test_executor -v
覆盖：状态机全分支 / 仓位 / slot 约束 / 出场三路径 / 对账恢复 / 端到端。
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import exec_config as cfg
from live.playbook_fsm import (
    Bar, WaitEvent, step_waiting, compute_sizing, compute_sl_price, validate_levels,
)
from live.slot_pool import Occupancy, build_accounts, allocate
from live.position_manager import (
    open_position, manage_open_position, _cancel, SLPlacementError, NakedPositionError,
)
from live.reconcile import reconcile_position, startup_reconcile
from live.broker.mock import MockBroker
from live.broker.base import SymbolSpec, OrderStatus, OrderState, Position, PosSide
from live.data_reader import OhlcvReader
from live.executor import ExecutorEngine

SPEC = SymbolSpec("BTC/USDT", 0.001, 0.1, 0.001, 5.0)


def short_pb():
    return {
        "hypothesis": "DOWNSIDE", "direction": "short",
        "status": "WAITING_FOR_PRIMARY_TOUCH",
        "primary_touch": {"level": 73000, "side": "low"},
        "activates_if": {"level": 72800, "dir": "below"},
        "cancels_if": {"level": 74000, "dir": "above"},
        "invalidation": {"level": 73000, "dir": "above"},
        "tp1_level": 72400, "tp2_level": 72000, "r_dist_pct": 0.5,
    }


def bar(t, o, h, l, c):
    return Bar(pd.Timestamp(t, tz="UTC"), o, h, l, c)


class TestFSM(unittest.TestCase):
    def test_primary_then_activate(self):
        pb = short_pb()
        self.assertEqual(step_waiting(pb, bar("2026-06-11T00:00", 73500, 73600, 73100, 73400)), WaitEvent.NONE)
        self.assertEqual(step_waiting(pb, bar("2026-06-11T00:15", 73200, 73300, 72950, 73100)), WaitEvent.PRIMARY_TOUCH)
        self.assertEqual(step_waiting(pb, bar("2026-06-11T00:30", 73000, 73100, 72850, 72900)), WaitEvent.NONE)
        self.assertEqual(step_waiting(pb, bar("2026-06-11T00:45", 72850, 72900, 72650, 72700)), WaitEvent.ACTIVATE)
        self.assertEqual(pb["b2act"], 2)

    def test_b2act_skip(self):
        pb = short_pb()
        step_waiting(pb, bar("2026-06-11T00:15", 73200, 73300, 72950, 73100))     # primary
        ev = step_waiting(pb, bar("2026-06-11T00:30", 73000, 73100, 72700, 72700))  # b2act=1
        self.assertEqual(ev, WaitEvent.SKIP_B2ACT)
        self.assertEqual(pb["status"], "DONE_CANCELLED")
        self.assertEqual(pb["result"], "skipped_b2act")

    def test_cancel(self):
        pb = short_pb()
        step_waiting(pb, bar("2026-06-11T00:15", 73200, 73300, 72950, 73100))     # primary
        ev = step_waiting(pb, bar("2026-06-11T00:30", 73500, 74200, 73400, 74100))  # close>74000
        self.assertEqual(ev, WaitEvent.CANCEL)
        self.assertEqual(pb["result"], "cancelled")

    def test_sizing(self):
        s = compute_sizing("BTC/USDT", 0.5, 72700)
        self.assertAlmostEqual(s.notional, 2000.0)
        self.assertEqual(s.leverage, 50)
        self.assertEqual(s.margin, 40)

    def test_sl_buffer(self):
        self.assertAlmostEqual(compute_sl_price("BTC/USDT", "short", 73000), 73073, places=0)
        self.assertAlmostEqual(compute_sl_price("BTC/USDT", "long", 73000), 72927, places=0)
        self.assertEqual(compute_sl_price("ETH/USDT", "short", 3500), 3500)

    def test_validate(self):
        self.assertTrue(validate_levels(short_pb())[0])
        bad = short_pb(); bad["direction"] = "long"     # long 时 tp1<act 不自洽
        self.assertFalse(validate_levels(bad)[0])


class TestSlot(unittest.TestCase):
    def setUp(self):
        keys = {"binance": [{"label": f"binance_{i}", "api_key": "x", "secret": "x"} for i in range(5)],
                "okx": [{"label": f"okx_{i}", "api_key": "x", "secret": "x", "passphrase": "x"} for i in range(5)]}
        self.accts = build_accounts(keys)

    def test_c2_same_symbol_excluded(self):
        occ = Occupancy({"binance_0": [("BTC/USDT", 40)]})
        landed = {allocate(self.accts, "BTC/USDT", 40, occ) for _ in range(50)}
        self.assertNotIn("binance_0", landed)

    def test_c2_other_symbol_allowed(self):
        occ = Occupancy({"binance_0": [("BTC/USDT", 40)]})
        landed = {allocate(self.accts, "ETH/USDT", 40, occ) for _ in range(50)}
        self.assertIn("binance_0", landed)

    def test_c1_margin(self):
        occ = Occupancy({"okx_0": [("BNB/USDT", 70)]})
        one = [a for a in self.accts if a.label == "okx_0"]
        self.assertIsNone(allocate(one, "BNB/USDT", 70, occ))   # 同 symbol + 超额
        self.assertEqual(allocate(one, "ETH/USDT", 40, occ), "okx_0")  # 70+40<=120

    def test_max_per_account(self):
        occ = Occupancy({"binance_1": [("BTC/USDT", 40), ("ETH/USDT", 40)]})
        landed = {allocate(self.accts, "SOL/USDT", 40, occ) for _ in range(50)}
        self.assertNotIn("binance_1", landed)

    def test_c4_exclude_okx(self):
        occ = Occupancy({})
        landed = {allocate(self.accts, "BTC/USDT", 40, occ, c4_ok=lambda a, s: a.exchange != "okx") for _ in range(50)}
        self.assertTrue(all(g.startswith("binance") for g in landed))

    def test_full_pool(self):
        full = {a.label: [("BTC/USDT", 40), ("ETH/USDT", 40)] for a in self.accts}
        self.assertIsNone(allocate(self.accts, "SOL/USDT", 40, Occupancy(full)))


class TestExitPaths(unittest.TestCase):
    def _open(self):
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = {"direction": "short", "r_dist_pct": 0.5,
              "invalidation": {"level": 73000, "dir": "above"},
              "tp1_level": 72400, "tp2_level": 72000}
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        return b, pb, ex

    def test_open(self):
        b, pb, ex = self._open()
        self.assertEqual(list(b.leverage_set.values())[0], 50)
        self.assertAlmostEqual(ex["sl_price"], 73073, places=0)
        self.assertAlmostEqual(ex["actual_r_usdt"], 10.07, places=1)

    def test_sl_path(self):
        b, pb, ex = self._open()
        b.fill_order(ex["sl_order_id"])
        self.assertEqual(manage_open_position(b, "BTC/USDT", pb), "DONE_SL")
        self.assertEqual(b.orders[ex["tp1_order_id"]].state, OrderState.CANCELED)

    def test_tp1_tp2(self):
        b, pb, ex = self._open()
        b.fill_order(ex["tp1_order_id"])
        self.assertEqual(manage_open_position(b, "BTC/USDT", pb), "TP1_HIT")
        self.assertEqual(pb["exec"]["sl_price"], 72700)        # SL 移 BE
        b.fill_order(ex["tp2_order_id"])
        self.assertEqual(manage_open_position(b, "BTC/USDT", pb), "DONE_TP2")

    def test_tp1_be(self):
        b, pb, ex = self._open()
        b.fill_order(ex["tp1_order_id"]); manage_open_position(b, "BTC/USDT", pb)
        b.fill_order(pb["exec"]["sl_order_id"])                # BE 单成交
        self.assertEqual(manage_open_position(b, "BTC/USDT", pb), "DONE_BE")


class TestReconcile(unittest.TestCase):
    def _active(self):
        return {"hypothesis": "X", "direction": "short", "status": "ACTIVATED",
                "exec": {"account": "a", "pos_side": "SHORT", "sl_order_id": "o2",
                         "tp1_order_id": "o3", "tp2_order_id": "o4",
                         "entry_price": 72700, "qty": 0.027, "half_qty": 0.013,
                         "sl_price": 73073, "client_id_base": "base"}}

    def test_position_present_ok(self):
        b = MockBroker(spec=SPEC)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        b.orders["o2"] = OrderStatus("o2", "x", OrderState.NEW, 0, 0)   # SL 还在
        self.assertEqual(reconcile_position(b, "BTC/USDT", self._active()), "ok")

    def test_reconcile_replaces_missing_sl(self):
        # 持仓还在但 SL 被撤 → 对账补挂新 SL（§19 P0-3）
        b = MockBroker(spec=SPEC)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        b.orders["o2"] = OrderStatus("o2", "x", OrderState.CANCELED, 0, 0)
        pb = self._active()
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "ok")
        self.assertNotEqual(pb["exec"]["sl_order_id"], "o2")           # 换了新 SL
        self.assertTrue(any(c[0] == "place_stop_market" for c in b.calls))

    def test_sl_reconciled(self):
        b = MockBroker(spec=SPEC)
        b.orders["o2"] = OrderStatus("o2", "x", OrderState.FILLED, 0.027, 73073)
        pb = self._active()
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "resolved")
        self.assertEqual(pb["status"], "DONE_SL")

    def test_no_position_unknown_exit(self):
        # 无持仓 + 订单状态对不上 → 无敞口安全终态（不挂人工，§21）
        b = MockBroker(spec=SPEC)
        b.orders["o2"] = OrderStatus("o2", "x", OrderState.NEW, 0, 0)
        pb = self._active()
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "resolved")
        self.assertEqual(pb["status"], "DONE_UNKNOWN")
        self.assertFalse(pb["exec"].get("manual_override"))

    def test_sl_unplaceable_closes(self):
        # 持仓在 + SL 没了 + 补挂失败 → 市价平退出（DONE_UNKNOWN，不裸持不挂人工 §21）
        b = MockBroker(spec=SPEC, fail_on={"place_stop_market"})
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        b.orders["o2"] = OrderStatus("o2", "x", OrderState.CANCELED, 0, 0)
        pb = self._active()
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "resolved")
        self.assertEqual(pb["status"], "DONE_UNKNOWN")
        self.assertTrue(any(c[0] == "market_close" for c in b.calls))

    def test_recover_flat_done_unknown(self):
        # 裸仓恢复：持仓已无 → DONE_UNKNOWN（自动收口，无人工出口 §21）
        b = MockBroker(spec=SPEC)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = {"exec": {"account": "a", "pos_side": "SHORT", "client_id_base": "base"}, "status": "ACTIVATED"}
        self.assertTrue(eng._try_recover(b, "BTC/USDT", pb))
        self.assertEqual(pb["status"], "DONE_UNKNOWN")

    def test_recover_retries_close(self):
        # 裸仓恢复：仍有持仓 → 重试市价平
        b = MockBroker(spec=SPEC)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = {"exec": {"account": "a", "pos_side": "SHORT", "client_id_base": "base"}, "status": "ACTIVATED"}
        self.assertFalse(eng._try_recover(b, "BTC/USDT", pb))
        self.assertTrue(any(c[0] == "market_close" for c in b.calls))

    def test_startup_discards_waiting_keeps_active(self):
        b = MockBroker("binance_0", "binance", spec=SPEC)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        active = self._active(); active["exec"]["account"] = "binance_0"
        state = {"symbol": "BTC/USDT", "playbooks": [
            {"hypothesis": "W", "status": "WAITING_FOR_ACTIVATION"}, active]}

        class FakeEngine:
            def __init__(s): s.brokers = {"binance_0": b}; s.archived = []
            def load_states(s): return [("p", state)]
            def save_state(s, p, st): pass
            def archive(s, p): s.archived.append(p)

        startup_reconcile(FakeEngine())
        self.assertEqual(state["playbooks"][0]["status"], "DONE_CANCELLED")
        self.assertEqual(state["playbooks"][0]["result"], "restart_discard")
        self.assertEqual(state["playbooks"][1]["status"], "ACTIVATED")


class TestEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._orig = (cfg.SIGNAL_ACTIVE, cfg.SIGNAL_DONE, cfg.CURSOR_FILE, cfg.HEARTBEAT_FILE)
        cfg.SIGNAL_ACTIVE = self.tmp / "active"
        cfg.SIGNAL_DONE = self.tmp / "done"
        cfg.CURSOR_FILE = self.tmp / "state" / "cursor.json"
        cfg.HEARTBEAT_FILE = self.tmp / "hb" / "hb.txt"
        cfg.SIGNAL_ACTIVE.mkdir(parents=True)

        self.dbp = self.tmp / "o.db"
        c = sqlite3.connect(self.dbp)
        c.execute("CREATE TABLE ohlcv_bars (symbol TEXT,timeframe TEXT,open_time TIMESTAMP,close_time TIMESTAMP,open REAL,high REAL,low REAL,close REAL,volume REAL)")
        bars = [("2026-06-11T00:00:00+00:00", "2026-06-11T00:15:00+00:00", 73500, 73600, 73100, 73400),
                ("2026-06-11T00:15:00+00:00", "2026-06-11T00:30:00+00:00", 73200, 73300, 72950, 73100),
                ("2026-06-11T00:30:00+00:00", "2026-06-11T00:45:00+00:00", 73000, 73100, 72850, 72900),
                ("2026-06-11T00:45:00+00:00", "2026-06-11T01:00:00+00:00", 72850, 72900, 72650, 72700)]
        for ot, ct, o, h, l, cl in bars:
            c.execute("INSERT INTO ohlcv_bars(symbol,timeframe,open_time,close_time,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?,?)",
                      ("BTC/USDT", "15m", ot, ct, o, h, l, cl, 100))
        c.commit(); c.close()

        pkg = cfg.SIGNAL_ACTIVE / "btcusdt_A_test"
        pkg.mkdir()
        state = {"signal_dir": "btcusdt_A_test", "symbol": "BTC/USDT", "overall_status": "WATCHING",
                 "bar_time": "2026-06-11T00:00:00+00:00",
                 "playbooks": [{**short_pb(), "hypothesis": "DOWNSIDE"}]}
        (pkg / "state.json").write_text(json.dumps(state), encoding="utf-8")
        (pkg / ".ready").touch()
        self.pkg = pkg

    def tearDown(self):
        (cfg.SIGNAL_ACTIVE, cfg.SIGNAL_DONE, cfg.CURSOR_FILE, cfg.HEARTBEAT_FILE) = self._orig
        shutil.rmtree(self.tmp)

    def test_full_lifecycle(self):
        keys = {"binance": [{"label": f"binance_{i}", "api_key": "x", "secret": "x"} for i in range(5)],
                "okx": [{"label": f"okx_{i}", "api_key": "x", "secret": "x", "passphrase": "x"} for i in range(5)]}
        accts = build_accounts(keys)
        brokers = {a.label: MockBroker(a.label, a.exchange, spec=SPEC, fill_price=72700) for a in accts}
        eng = ExecutorEngine(OhlcvReader(self.dbp), brokers, accts)
        now = pd.Timestamp("2026-06-11T01:00:00+00:00")

        eng.tick(now=now)
        pb = json.loads((self.pkg / "state.json").read_text())["playbooks"][0]
        self.assertEqual(pb["status"], "ACTIVATED")
        acc = pb["exec"]["account"]; ex = pb["exec"]

        brokers[acc].fill_order(ex["tp1_order_id"]); eng.tick(now=now)
        pb = json.loads((self.pkg / "state.json").read_text())["playbooks"][0]
        self.assertEqual(pb["status"], "TP1_HIT")

        brokers[acc].fill_order(ex["tp2_order_id"]); eng.tick(now=now)
        self.assertFalse(self.pkg.exists())                       # 已归档
        done = json.loads((cfg.SIGNAL_DONE / "btcusdt_A_test" / "state.json").read_text())
        self.assertEqual(done["playbooks"][0]["status"], "DONE_TP2")

    def test_stale_signal_discarded_no_order(self):
        # 信号 T0(00:00) 距 now(10:00) 600min > 240 → 丢弃，绝不下单（防错时开仓 §17）
        keys = {"binance": [{"label": f"binance_{i}", "api_key": "x", "secret": "x"} for i in range(5)],
                "okx": [{"label": f"okx_{i}", "api_key": "x", "secret": "x", "passphrase": "x"} for i in range(5)]}
        accts = build_accounts(keys)
        brokers = {a.label: MockBroker(a.label, a.exchange, spec=SPEC, fill_price=72700) for a in accts}
        eng = ExecutorEngine(OhlcvReader(self.dbp), brokers, accts)
        eng.tick(now=pd.Timestamp("2026-06-11T10:00:00+00:00"))
        done = json.loads((cfg.SIGNAL_DONE / "btcusdt_A_test" / "state.json").read_text())
        self.assertEqual(done["playbooks"][0]["status"], "DONE_CANCELLED")
        self.assertEqual(done["playbooks"][0]["result"], "stale_discard")
        self.assertFalse(any(c[0] == "market_open" for b in brokers.values() for c in b.calls))


class TestFaultInjection(unittest.TestCase):
    """故障注入（task #16）：SL 挂不上 / 撤单失败 / BE 挂失败 —— 不裸仓、不弃管。"""

    def setUp(self):
        self._tl = cfg.TRADE_LOG
        self.tmp = Path(tempfile.mkdtemp())
        cfg.TRADE_LOG = self.tmp / "tl.jsonl"

    def tearDown(self):
        cfg.TRADE_LOG = self._tl
        shutil.rmtree(self.tmp)

    def _pb(self):
        return {"direction": "short", "r_dist_pct": 0.5, "invalidation": {"level": 73000, "dir": "above"},
                "tp1_level": 72400, "tp2_level": 72000}

    def test_sl_fail_closes_position(self):
        # SL 挂不上 → 市价平退出，不裸仓
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"place_stop_market"})
        with self.assertRaises(SLPlacementError):
            open_position(b, "BTC/USDT", self._pb(), 72700, "a", 40, "base")
        self.assertIsNone(b.get_position("BTC/USDT", PosSide.SHORT))

    def test_sl_and_close_fail_naked(self):
        # SL 挂不上 且 平仓也失败 → NakedPositionError（裸仓还在，交人工）
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"place_stop_market", "market_close"})
        with self.assertRaises(NakedPositionError):
            open_position(b, "BTC/USDT", self._pb(), 72700, "a", 40, "base")
        self.assertIsNotNone(b.get_position("BTC/USDT", PosSide.SHORT))

    def test_tp_fail_degraded_sl_still_on(self):
        # TP 挂不上 → 不致命，SL 仍在
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"place_reduce_limit"})
        ex = open_position(b, "BTC/USDT", self._pb(), 72700, "a", 40, "base")
        self.assertIsNone(ex["tp1_order_id"])
        self.assertTrue(ex["tp_degraded"])
        self.assertIsNotNone(ex["sl_order_id"])

    def test_cancel_fail_no_raise(self):
        b = MockBroker(spec=SPEC, fail_on={"cancel_order"})
        _cancel(b, "BTC/USDT", "someoid")   # 重试后告警，不抛

    def test_market_open_timeout_recovered(self):
        # 市价单超时但已成交 → 查持仓接管，不抛、不孤儿，SL 挂上
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"market_open_timeout"})
        ex = open_position(b, "BTC/USDT", self._pb(), 72700, "a", 40, "base")
        self.assertEqual(ex["entry_price"], 72700)
        self.assertIsNotNone(b.get_position("BTC/USDT", PosSide.SHORT))
        self.assertIsNotNone(ex["sl_order_id"])

    def test_market_open_fail_no_position_raises(self):
        # 市价单失败且无持仓 → 抛（entry_failed），无裸仓
        b = MockBroker(spec=SPEC, fail_on={"market_open"})
        with self.assertRaises(Exception):
            open_position(b, "BTC/USDT", self._pb(), 72700, "a", 40, "base")
        self.assertIsNone(b.get_position("BTC/USDT", PosSide.SHORT))

    def test_get_order_unknown_no_premature_done(self):
        # TP 查单 UNKNOWN（API 异常）+ 持仓没了 → 不臆测 DONE_SL，本轮跳过（§19 P0-6）
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        b.fail_on = {"get_order"}        # 查单返回 UNKNOWN
        b.positions.clear()              # 持仓查不到
        r = manage_open_position(b, "BTC/USDT", pb)
        self.assertIsNone(r)
        self.assertEqual(pb["status"], "ACTIVATED")   # 没误判 DONE_SL

    def test_tp1_degraded_market_close(self):
        # TP1 挂单缺失 + 价格到 tp1 → 市价平半仓 + 移 BE（§20 降级闭环）
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        ex["tp1_order_id"] = None                 # 模拟 TP1 挂失败
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        r = manage_open_position(b, "BTC/USDT", pb, ref_price=72300)   # short: ref<=tp1(72400)
        self.assertEqual(r, "TP1_HIT")
        self.assertTrue(any(c[0] == "market_close" for c in b.calls))

    def test_tp1_degraded_not_reached_holds(self):
        # TP1 缺失但价格未到 → 不平，保持 ACTIVATED
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        ex["tp1_order_id"] = None
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        r = manage_open_position(b, "BTC/USDT", pb, ref_price=72600)   # 未到 tp1(72400)
        self.assertIsNone(r)
        self.assertEqual(pb["status"], "ACTIVATED")

    def test_be_fail_keeps_original_sl(self):
        # TP1 成交后挂 BE 失败 → 保留原 SL（仍有止损），不裸奔
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        orig_sl = ex["sl_order_id"]
        b.fill_order(ex["tp1_order_id"])
        b.fail_on = {"place_stop_market"}     # BE 挂会失败
        r = manage_open_position(b, "BTC/USDT", pb)
        self.assertEqual(r, "TP1_HIT")
        self.assertEqual(pb["result"], "tp1_be_degraded")
        self.assertEqual(pb["exec"]["sl_order_id"], orig_sl)
        self.assertTrue(pb["exec"].get("be_failed"))


if __name__ == "__main__":
    unittest.main()
