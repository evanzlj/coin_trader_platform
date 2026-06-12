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
from live.slot_pool import Occupancy, build_accounts, allocate, build_occupancy
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
        # binance_0 已持 BTC，不同 symbol(ETH) 不被 C2 排除 → 单账户必分到（确定，不依赖随机覆盖）
        occ = Occupancy({"binance_0": [("BTC/USDT", 40)]})
        one = [a for a in self.accts if a.label == "binance_0"]
        self.assertEqual(allocate(one, "ETH/USDT", 40, occ), "binance_0")

    def test_c1_margin(self):
        occ = Occupancy({"okx_0": [("BNB/USDT", 70)]})
        one = [a for a in self.accts if a.label == "okx_0"]
        self.assertIsNone(allocate(one, "BNB/USDT", 70, occ))   # 同 symbol + 超额
        self.assertEqual(allocate(one, "ETH/USDT", 40, occ), "okx_0")  # 70+40<=120

    def test_max_per_account(self):
        occ = Occupancy({"binance_1": [("BTC/USDT", 40), ("ETH/USDT", 40)]})
        landed = {allocate(self.accts, "SOL/USDT", 40, occ) for _ in range(50)}
        self.assertNotIn("binance_1", landed)

    def test_opening_occupies_slot(self):
        # OPENING 也算占用：占满某账户 2 slot → allocate 不再分到它（§18）
        states = [
            {"symbol": "BTC/USDT", "playbooks": [{"status": "OPENING",
             "exec": {"account": "binance_1", "pos_side": "SHORT", "margin": 40}}]},
            {"symbol": "ETH/USDT", "playbooks": [{"status": "OPENING",
             "exec": {"account": "binance_1", "pos_side": "SHORT", "margin": 40}}]},
        ]
        occ = build_occupancy(states)
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

    def test_reconcile_position_api_error_holds(self):
        # get_position 抛(API error) → 保持状态，不判死（UNKNOWN 不臆测，§21 全局不变量）
        b = MockBroker(spec=SPEC, fail_on={"get_position"})
        pb = self._active()
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "ok")
        self.assertEqual(pb["status"], "ACTIVATED")

    def test_safe_get_order_normalizes_raise(self):
        # adapter get_order 抛（如 OKX _inst_meta 抖）→ safe_get_order 归一 UNKNOWN，不外抛（§22 不变量3）
        from live.broker.base import safe_get_order
        b = MockBroker(spec=SPEC)
        def boom(*a, **k):
            raise RuntimeError("order api down")
        b.get_order = boom
        o = safe_get_order(b, "BTC/USDT", "x")
        self.assertEqual(o.state, OrderState.UNKNOWN)

    def test_reconcile_get_order_raise_no_crash(self):
        # broker.get_order 抛 → reconcile_position 经 safe wrapper 不崩（防 startup/periodic 被打崩 §22 P0）
        b = MockBroker(spec=SPEC)
        def boom(*a, **k):
            raise RuntimeError("order api down")
        b.get_order = boom
        pb = self._active()                          # 无持仓 → filled() 经 safe_get_order(UNKNOWN)，不抛
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "resolved")

    def test_recover_opening_adopt_sl_unknown_holds(self):
        # adopt 补 SL 时保护单查询 UNKNOWN → 保持 OPENING（不重复挂、不平退出 §22 P1）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"get_order"})
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "OPENING")
        self.assertFalse(any(c[0] == "market_close" for c in b.calls))   # 没平退出

    def test_recover_opening_unexpected_error_holds(self):
        # _recover_opening 内任何未预期异常（如 get_symbol_spec 抖，adopt round 用到）→ 保持 OPENING，不打崩（§22）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fill_price=72700)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        def boom(*a, **k):
            raise RuntimeError("spec api down")
        b.get_symbol_spec = boom
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)        # 不抛
        self.assertEqual(pb["status"], "OPENING")

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

    def test_recovering_reconcile_no_crash(self):
        # recovering pb（exec 无 qty/sl_price）走 reconcile 不崩，交给 tick 处理（§21 边界）
        b = MockBroker(spec=SPEC)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        pb = {"hypothesis": "X", "status": "ACTIVATED",
              "exec": {"account": "a", "pos_side": "SHORT", "recovering": True, "client_id_base": "base"}}
        self.assertEqual(reconcile_position(b, "BTC/USDT", pb), "ok")   # 不 KeyError

    def test_opening_recover_adopts(self):
        # OPENING + 有仓 → 接管补 SL/TP，转 ACTIVATED（§18 崩溃恢复）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fill_price=72700)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = {"hypothesis": "X", "status": "OPENING",
              "exec": {"account": "a", "exchange": "mock", "pos_side": "SHORT", "client_id_base": "base",
                       "direction": "short", "margin": 40, "invalidation": {"level": 73000, "dir": "above"},
                       "tp1_level": 72400, "tp2_level": 72000}}
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "ACTIVATED")
        self.assertIsNotNone(pb["exec"]["sl_order_id"])
        self.assertTrue(pb["exec"].get("adopted"))

    def test_opening_recover_aborts(self):
        # OPENING + 无仓 + 订单不存在 → 作废
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = {"hypothesis": "X", "status": "OPENING",
              "exec": {"account": "a", "pos_side": "SHORT", "client_id_base": "base", "direction": "short",
                       "opening_at": "2020-01-01T00:00:00+00:00",          # grace 外
                       "invalidation": {"level": 73000, "dir": "above"}}}
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "DONE_CANCELLED")
        self.assertEqual(pb["result"], "opening_aborted")

    def test_recover_opening_order_none_within_grace_holds(self):
        # 订单与仓位都暂时不可见 + grace 内 → 保持 OPENING（统一 grace 门槛，§22 P0）
        from live.reconcile import _recover_opening
        import pandas as pd
        b = MockBroker(spec=SPEC)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        pb["exec"]["opening_at"] = pd.Timestamp.now("UTC").isoformat()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "OPENING")

    def test_recover_opening_decision_table(self):
        """穷举 _recover_opening 的 (od 状态 × 有无仓 × grace内外) 决策组合，
        把状态空间显式化为一张会失败的表（§22：不靠枚举 if，靠 grace 门槛+证据）。"""
        import pandas as pd
        from live.reconcile import _recover_opening
        now, old = pd.Timestamp.now("UTC").isoformat(), "2020-01-01T00:00:00+00:00"
        HASPOS = (0.027, 72700)                          # 有仓 entry>0
        # (od_state, 有无仓, opening_at, od_filled_qty, 期望 status)
        cases = [
            # 有持仓 entry>0 → adopt（不看 grace/od/fill）
            (OrderState.FILLED, HASPOS, now, 0.027, "ACTIVATED"),
            (OrderState.FILLED, HASPOS, old, 0.027, "ACTIVATED"),
            (None,              HASPOS, now, 0.0,   "ACTIVATED"),
            # 无持仓 + 死单 + **零成交** → 立即作废
            (OrderState.CANCELED, None, now, 0.0, "DONE_CANCELLED"),
            (OrderState.REJECTED, None, now, 0.0, "DONE_CANCELLED"),
            (OrderState.EXPIRED,  None, now, 0.0, "DONE_CANCELLED"),
            # 无持仓 + 死单 + **有成交(filled>0)** → 有成交证据，grace 内保持（关键新增维度）
            (OrderState.CANCELED, None, now, 0.01, "OPENING"),
            (OrderState.EXPIRED,  None, now, 0.01, "OPENING"),
            # 死单 + 有成交 + 超 grace → 已平 DONE_UNKNOWN（不是 aborted）
            (OrderState.CANCELED, None, old, 0.01, "DONE_UNKNOWN"),
            # 无持仓 + grace 内 → 一律保持（任何 od）
            (OrderState.FILLED, None, now, 0.027, "OPENING"),
            (None,              None, now, 0.0,   "OPENING"),
            (OrderState.NEW,    None, now, 0.0,   "OPENING"),
            # 无持仓 + grace 外 → 按成交证据终态化
            (OrderState.FILLED, None, old, 0.027, "DONE_UNKNOWN"),   # 有成交→已平
            (None,              None, old, 0.0,   "DONE_CANCELLED"), # 无单→没开成
            (OrderState.NEW,    None, old, 0.0,   "DONE_CANCELLED"), # 未落实
        ]
        for od_state, pos, oat, fq, expected in cases:
            b = MockBroker(spec=SPEC, fill_price=72700)
            if pos:
                b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, pos[0], pos[1])
            if od_state is not None:
                b.orders["base_E"] = OrderStatus("base_E", "base_E", od_state, fq, 72700)
            eng = ExecutorEngine(None, {"a": b}, [])
            pb = self._opening_pb()
            pb["exec"]["opening_at"] = oat
            _recover_opening(eng, "BTC/USDT", pb)
            self.assertEqual(pb["status"], expected,
                             f"od={od_state} haspos={bool(pos)} fq={fq} grace={'in' if oat == now else 'out'}")

    def test_opening_recover_unknown_holds(self):
        # OPENING + 查单 UNKNOWN（API 异常）→ 保持 OPENING，不臆测（§18）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fail_on={"get_order"})
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = {"hypothesis": "X", "status": "OPENING",
              "exec": {"account": "a", "pos_side": "SHORT", "client_id_base": "base", "direction": "short",
                       "invalidation": {"level": 73000, "dir": "above"}}}
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "OPENING")

    def _opening_pb(self):
        return {"hypothesis": "X", "status": "OPENING",
                "exec": {"account": "a", "exchange": "mock", "pos_side": "SHORT", "client_id_base": "base",
                         "direction": "short", "margin": 40, "invalidation": {"level": 73000, "dir": "above"},
                         "tp1_level": 72400, "tp2_level": 72000}}

    def test_opening_adopt_sl_fail_closes(self):
        # OPENING 接管时补 SL 失败 → 市价平退出（不留裸仓，§18 P0）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"place_stop_market"})
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "DONE_UNKNOWN")
        self.assertEqual(pb["result"], "adopt_sl_failed_closed")
        self.assertTrue(any(c[0] == "market_close" for c in b.calls))

    def test_opening_adopt_sl_and_close_fail_recovering(self):
        # OPENING 接管补 SL 失败 + 平不掉 → recovering（tick 重试，§18/§21）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fill_price=72700, fail_on={"place_stop_market", "market_close"})
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "ACTIVATED")
        self.assertTrue(pb["exec"].get("recovering"))

    def test_recover_opening_position_api_error_holds(self):
        # get_position 抛(API error) → 保持 OPENING，不判死（§18 P0-2 UNKNOWN 语义）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fail_on={"get_position"})
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "OPENING")

    def test_recover_opening_filled_then_flat(self):
        # 无仓 + entry 订单 FILLED（开过已平）→ DONE_UNKNOWN（无敞口安全终态）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC)
        b.orders["base_E"] = OrderStatus("base_E", "base_E", OrderState.FILLED, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        pb["exec"]["opening_at"] = "2020-01-01T00:00:00+00:00"   # grace 外 → 认已平
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "DONE_UNKNOWN")
        self.assertEqual(pb["result"], "opening_filled_then_flat")

    def test_recover_opening_filled_no_pos_grace_then_adopt(self):
        # 仓位最终一致性窗口：FILLED+暂无仓(grace内)→ 保持 OPENING；下次仓位出现 → adopt（§22 P0）
        from live.reconcile import _recover_opening
        import pandas as pd
        b = MockBroker(spec=SPEC, fill_price=72700)
        b.orders["base_E"] = OrderStatus("base_E", "base_E", OrderState.FILLED, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        pb["exec"]["opening_at"] = pd.Timestamp.now("UTC").isoformat()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "OPENING")            # grace 内保持，不判死
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 72700)
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "ACTIVATED")          # 仓位出现 → 接管补 SL
        self.assertTrue(pb["exec"].get("adopted"))

    def test_recover_opening_pos_zero_entry_holds(self):
        # 仓位可见但 entry_price 未同步 + entry 单也无均价 → 保持 OPENING，不 adopt entry=0（§22 P1-high）
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 0.0)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "OPENING")

    def test_recover_opening_pos_zero_entry_backfilled(self):
        # entry_price 未同步但 entry 单 FILLED 有均价 → 用均价补 → adopt
        from live.reconcile import _recover_opening
        b = MockBroker(spec=SPEC, fill_price=72700)
        b.positions[("BTC/USDT", PosSide.SHORT)] = Position("BTC/USDT", PosSide.SHORT, 0.027, 0.0)
        b.orders["base_E"] = OrderStatus("base_E", "base_E", OrderState.FILLED, 0.027, 72700)
        eng = ExecutorEngine(None, {"a": b}, [])
        pb = self._opening_pb()
        _recover_opening(eng, "BTC/USDT", pb)
        self.assertEqual(pb["status"], "ACTIVATED")
        self.assertEqual(pb["exec"]["entry_price"], 72700)

    def test_recover_opening_entry_dead_aborts(self):
        # 无仓 + entry 单 CANCELED/REJECTED/EXPIRED → 作废（明确失败，不永久占 slot §22 P1）
        from live.reconcile import _recover_opening
        for st in (OrderState.CANCELED, OrderState.REJECTED, OrderState.EXPIRED):
            b = MockBroker(spec=SPEC)
            b.orders["base_E"] = OrderStatus("base_E", "base_E", st, 0, 0)
            eng = ExecutorEngine(None, {"a": b}, [])
            pb = self._opening_pb()
            _recover_opening(eng, "BTC/USDT", pb)
            self.assertEqual(pb["status"], "DONE_CANCELLED", str(st))

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

    def test_open_position_zero_price_raises(self):
        # market_open 成交价未知(price<=0)且持仓 entry 也未知 → 抛（保持 OPENING，不接受 price=0 §22 P1）
        b = MockBroker(spec=SPEC, fill_price=0.0)
        with self.assertRaises(Exception):
            open_position(b, "BTC/USDT", self._pb(), 72700, "a", 40, "base")

    def test_get_order_unknown_with_position_holds(self):
        # TP 查单 UNKNOWN + 有持仓 → 保持 ACTIVATED（有敞口，等查清，不臆测 §19）
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        b.fail_on = {"get_order"}
        r = manage_open_position(b, "BTC/USDT", pb)
        self.assertIsNone(r)
        self.assertEqual(pb["status"], "ACTIVATED")

    def test_get_order_unknown_no_position_done_unknown(self):
        # TP 查单 UNKNOWN + 无持仓 → 无敞口优先，DONE_UNKNOWN（不臆测 DONE_SL，也不傻等 reconcile §22）
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        b.fail_on = {"get_order"}
        b.positions.clear()
        r = manage_open_position(b, "BTC/USDT", pb)
        self.assertEqual(r, "DONE_UNKNOWN")
        self.assertEqual(pb["result"], "exit_unknown")

    def test_manage_activated_decision_table(self):
        """manage ACTIVATED 的 (tp1 状态 × 有无仓 × ref 到价) 决策穷举（§22 热点封闭）。"""
        def setup(tp1_state, has_pos, fail_order=False):
            b = MockBroker(spec=SPEC, fill_price=72700)
            pb = self._pb()
            ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
            pb["exec"] = ex; pb["status"] = "ACTIVATED"
            oid = ex["tp1_order_id"]
            if tp1_state is None:
                ex["tp1_order_id"] = None
            elif tp1_state == "FILLED":
                b.orders[oid].state = OrderState.FILLED
            elif tp1_state == "dead":
                b.orders[oid].state = OrderState.CANCELED
            # "NEW" 保持 open_position 挂的 NEW
            if not has_pos:
                b.positions.clear()
            if fail_order:
                b.fail_on = {"get_order"}
            return b, pb
        # (tp1_state, has_pos, fail_order, ref_price, 期望)
        cases = [
            ("FILLED", True,  False, None,  "TP1_HIT"),        # 半仓止盈 → 移 BE
            (None,     True,  False, 72300, "TP1_HIT"),        # 降级：无挂单 + 到价 → 市价平
            (None,     True,  False, 72600, None),             # 降级：未到价 → 保持
            ("dead",   True,  False, 72300, "TP1_HIT"),        # 死单 + 到价 → 降级平
            ("dead",   True,  False, 72600, None),             # 死单 + 未到 → 保持
            ("NEW",    True,  False, None,  None),             # 挂着 + 有仓 → 保持
            ("NEW",    False, False, None,  "DONE_SL"),        # 无仓 + TP1 未成交 → SL 触发
            ("NEW",    True,  True,  None,  None),             # UNKNOWN + 有仓 → 保持
            ("NEW",    False, True,  None,  "DONE_UNKNOWN"),   # UNKNOWN + 无仓 → 无敞口终态
        ]
        for tp1_state, has_pos, fail_order, ref, expected in cases:
            b, pb = setup(tp1_state, has_pos, fail_order)
            r = manage_open_position(b, "BTC/USDT", pb, ref_price=ref)
            self.assertEqual(r, expected,
                             f"tp1={tp1_state} pos={has_pos} unknown={fail_order} ref={ref}")

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

    def test_tp1_canceled_degraded_close(self):
        # TP1 订单被取消（非 None）+ 价格到 tp1 → 仍走降级市价平（§20 死单边界）
        b = MockBroker(spec=SPEC, fill_price=72700)
        pb = self._pb()
        ex = open_position(b, "BTC/USDT", pb, 72700, "a", 40, "base")
        b.cancel_order("BTC/USDT", order_id=ex["tp1_order_id"])        # TP1 → CANCELED
        pb["exec"] = ex; pb["status"] = "ACTIVATED"
        r = manage_open_position(b, "BTC/USDT", pb, ref_price=72300)
        self.assertEqual(r, "TP1_HIT")
        self.assertTrue(any(c[0] == "market_close" for c in b.calls))

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
