"""
vlm_finalizer (re-arch Phase 2, hardened) tests — mock filesystem, no network.

Covers full decision table (§3.4 / A–F):
  A. Success path: staging → .ready + state.json → atomic rename to signal_active
  B. Incoming trust boundary: extra files (signal.json/state.json/evil.txt) ignored
  C. Strict VLM schema validation (no exceptions from _filter_playbooks)
  D. Pending lifecycle: archived after moved/rejected, with VLM_PENDING_DONE
  E. Sandbox: all write paths protected
  F. Executor load_states compatibility (state.json + .ready)
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import vlm_finalizer as vf


def _recent_iso(minutes_ago: int = 10) -> str:
    return (pd.Timestamp.now("UTC") - pd.Timedelta(minutes=minutes_ago)).isoformat()


def _valid_pb() -> list[dict]:
    """A playbook that passes BTC filter (r_dist=1.1%>=0.5%, tp1=0.55%<1.5%)."""
    return [{
        "hypothesis": "DOWNSIDE", "conditional_trade_plan": {
            "activation_rule": {
                "direction_if_activated": "short",
                "primary_touch": {"level": 73000, "side": "low"},
                "activates_if_close_crosses": {"level": 72800, "dir": "below"},
                "cancels_if_close_crosses_first": {"level": 74000, "dir": "above"},
                "invalidation_after_activation": {"level": 72000, "dir": "above"},
                "objectives": [{"level": 72400}],
            }
        }
    }]


def _create_incoming(tmp: Path, name="btcusdt_a_20260601_0000", playbooks=None,
                     bar_time=None, extra_files=None) -> Path:
    """Create a minimal vlm_done_incoming pkg. Returns pkg dir."""
    d = tmp / "vlm_done_incoming" / name
    d.mkdir(parents=True)
    if bar_time is None:
        bar_time = _recent_iso(10)
    (d / "vlm_response.json").write_text(json.dumps({
        "watch_summary": "analysis text", "prompt_version": "v1",
        "playbooks": playbooks if playbooks is not None else _valid_pb(),
    }), encoding="utf-8")
    # Extra files that should NOT enter signal_active (B)
    if extra_files:
        for ef_name, ef_content in extra_files.items():
            (d / ef_name).write_text(ef_content, encoding="utf-8")
    return d


def _create_pending(tmp: Path, name="btcusdt_a_20260601_0000",
                    bar_time=None, symbol="BTC/USDT", with_ready=True) -> Path:
    """Create a local vlm_pending pkg. Returns pkg dir."""
    d = tmp / "vlm_pending" / name
    d.mkdir(parents=True)
    if bar_time is None:
        bar_time = _recent_iso(10)
    (d / "signal.json").write_text(json.dumps({
        "symbol": symbol, "grade": "A", "bar_time": bar_time,
        "close": 70000, "structure_side": "near_resistance",
        "structure_space": 2.0, "position_in_structure": 0.95,
    }), encoding="utf-8")
    if with_ready:
        (d / ".ready").touch()
    return d


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._olds = {k: getattr(vf, k) for k in
                      ("VLM_DONE_INCOMING", "VLM_PENDING", "VLM_REJECTED",
                       "SIGNAL_ACTIVE", "SIGNAL_DONE", "SIGNAL_MAX_AGE_MIN",
                       "VLM_PENDING_DONE", "_STAGING_DIR",
                       "STATUS_FILE", "HEARTBEAT_FILE")}
        vf.VLM_DONE_INCOMING = self.tmp / "vlm_done_incoming"
        vf.VLM_PENDING = self.tmp / "vlm_pending"
        vf.VLM_REJECTED = self.tmp / "vlm_rejected"
        vf.SIGNAL_ACTIVE = self.tmp / "signal_active"
        vf.SIGNAL_DONE = self.tmp / "signal_done"
        vf.VLM_PENDING_DONE = self.tmp / "vlm_pending_done"
        vf._STAGING_DIR = self.tmp / ".f_active_incoming"
        vf.SIGNAL_MAX_AGE_MIN = 240
        vf.STATUS_FILE = self.tmp / "finalizer_status.json"
        vf.HEARTBEAT_FILE = self.tmp / "hb.txt"
        vf.VLM_DONE_INCOMING.mkdir(parents=True)

    def tearDown(self):
        for k, v in self._olds.items():
            setattr(vf, k, v)


# ── A: 成功路径含 .ready + state.json ────────────────────────────────────────

class TestSuccessPath(_Base):
    def test_moved_has_ready_and_state(self):
        _create_pending(self.tmp)
        _create_incoming(self.tmp)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "moved")

        dest = vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000"
        self.assertTrue(dest.exists())
        self.assertTrue((dest / ".ready").exists(), "executor requires .ready")
        self.assertTrue((dest / "state.json").exists())

        state = json.loads((dest / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["overall_status"], "WATCHING")
        self.assertEqual(state["symbol"], "BTC/USDT")
        self.assertEqual(state["grade"], "A")

    def test_state_fields_from_local_pending(self):
        """symbol/grade/bar_time/structure 来自本地 signal.json，非 incoming（B）。"""
        # ETH requires r_dist >= 1.5%; narrow inv=72300 gives r=500/72800=0.687% < 1.5%
        # Use inv=71300 for r=1500/72800=2.06% >= 1.5%, and TP1=72400=0.55% (outside dead zone 1-2%)
        eth_pb = [{
            "hypothesis": "DOWNSIDE", "conditional_trade_plan": {
                "activation_rule": {
                    "direction_if_activated": "short",
                    "primary_touch": {"level": 73000, "side": "low"},
                    "activates_if_close_crosses": {"level": 72800, "dir": "below"},
                    "cancels_if_close_crosses_first": {"level": 74000, "dir": "above"},
                    "invalidation_after_activation": {"level": 71300, "dir": "above"},
                    "objectives": [{"level": 72400}],
                }
            }
        }]
        _create_pending(self.tmp, symbol="ETH/USDT", bar_time=_recent_iso(10))
        # incoming includes signal.json/state.json (should be ignored — B)
        extra = {"signal.json": '{"symbol":"SOL/USDT","grade":"A+"}',
                 "state.json": '{"overall_status":"OPENING"}'}
        _create_incoming(self.tmp, playbooks=eth_pb, extra_files=extra)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "moved")
        state = json.loads((vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000" / "state.json")
                           .read_text(encoding="utf-8"))
        self.assertEqual(state["symbol"], "ETH/USDT")
        self.assertEqual(state["grade"], "A")

    def test_incoming_extra_files_not_in_signal_active(self):
        """B: evil.txt, fake signal.json from Windows must NOT leak into executor."""
        _create_pending(self.tmp)
        extra = {"evil.txt": "pwned", "state.json": '{"overall_status":"OPENED"}'}
        _create_incoming(self.tmp, extra_files=extra)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "moved")

        dest = vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000"
        self.assertFalse((dest / "evil.txt").exists(), "extra files must not enter signal_active")
        self.assertFalse((dest / "fake_signal.json").exists())

    def test_clean_staging_before_build(self):
        """P3: stale staging residue (evil.txt) must not leak into signal_active."""
        _create_pending(self.tmp)
        _create_incoming(self.tmp)
        # Pre-seed staging with unwanted file
        stale = vf._STAGING_DIR / "btcusdt_a_20260601_0000" / "evil.txt"
        stale.parent.mkdir(parents=True)
        stale.write_text("stale", encoding="utf-8")
        self.assertTrue(stale.exists(), "precondition: stale file exists in staging")

        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "moved")
        dest = vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000"
        self.assertFalse((dest / "evil.txt").exists(),
                         "stale evil.txt must NOT be in signal_active")
        self.assertTrue((dest / ".ready").exists())

    def test_executor_load_states_compatible(self):
        """F: executor.Engine.load_states 能读到该包。"""
        _create_pending(self.tmp)
        _create_incoming(self.tmp)
        vf.process_one("btcusdt_a_20260601_0000")
        # Mimic executor.load_states: scan signal_active for dirs with .ready + state.json
        pkg = vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000"
        self.assertTrue(pkg.exists() and (pkg / ".ready").exists())
        state = json.loads((pkg / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["overall_status"], "WATCHING")
        self.assertEqual(len(state["playbooks"]), 1)
        self.assertEqual(state["playbooks"][0]["status"], "WAITING_FOR_PRIMARY_TOUCH")


# ── B/C: 信任边界 + 严格 schema ─────────────────────────────────────────────

class TestSchemaValidation(_Base):
    def make(self, overrides: dict) -> None:
        _create_pending(self.tmp)
        d = self.tmp / "vlm_done_incoming" / "btcusdt_a_20260601_0000"
        d.mkdir(parents=True)
        base = {"watch_summary": "x", "playbooks": _valid_pb()}
        base.update(overrides)
        (d / "vlm_response.json").write_text(json.dumps(base), encoding="utf-8")

    def test_top_level_list(self):
        """C: [] 顶层非 dict → bad_schema 不抛异常。"""
        _create_pending(self.tmp)
        d = self.tmp / "vlm_done_incoming" / "btcusdt_a_20260601_0000"
        d.mkdir(parents=True)
        (d / "vlm_response.json").write_text("[]", encoding="utf-8")
        r = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(r, "bad_schema")
        self.assertFalse((vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000").exists())

    def test_missing_watch_summary(self):
        self.make({"watch_summary": ""})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_error_flag(self):
        self.make({"error": "chatgpt timeout"})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_playbooks_string(self):
        self.make({"playbooks": "oops"})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_playbooks_list_of_int(self):
        self.make({"playbooks": [1, 2, 3]})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_playbook_missing_conditional_trade_plan(self):
        self.make({"playbooks": [{"hypothesis": "TEST"}]})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_playbook_activation_rule_illegal_type(self):
        # 非 dict 非 null（如 string）→ 仍 bad_schema
        self.make({"playbooks": [{"hypothesis": "T",
                                  "conditional_trade_plan": {"activation_rule": "oops"}}]})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_playbook_activation_rule_null_allowed(self):
        # null activation_rule（no_trade/CHOP_WAIT 剧本）→ 不再 bad_schema；
        # 单个 null pb 被 _filter_playbooks 跳过 → 无有效 pb → filtered（对齐回测）。
        self.make({"playbooks": [{"hypothesis": "CHOP_WAIT",
                                  "conditional_trade_plan": {"activation_rule": None}}]})
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "filtered")

    def test_watch_summary_dict_accepted(self):
        # prompt 模板已把 watch_summary 定义为结构化对象（dict）；带 one_sentence_read
        # 的 dict 必须被接受，不能误判 bad_schema（实盘 ethusdt_A_20260617_1800 回归）。
        self.make({"watch_summary": {"symbol": "BTC/USDT",
                                     "one_sentence_read": "structural watch near resistance"}})
        # schema 通过后走正常过滤路径，不应是 bad_schema
        self.assertNotEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_schema")

    def test_bad_schema_no_exception_in_main_loop(self):
        """C: malformed response must NOT throw; it archives cleanly."""
        _create_pending(self.tmp)
        d = self.tmp / "vlm_done_incoming" / "btcusdt_a_20260601_0000"
        d.mkdir(parents=True)
        (d / "vlm_response.json").write_text("not json", encoding="utf-8")
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "bad_json")
        # incoming archived, not stuck
        self.assertFalse(d.exists())


# ── D: pending 生命周期 ──────────────────────────────────────────────────────

class TestPendingLifecycle(_Base):
    def test_orphan_missing_ready(self):
        """pending 缺 .ready → orphan。"""
        _create_pending(self.tmp, with_ready=False)
        _create_incoming(self.tmp)
        r = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(r, "orphan")
        self.assertFalse((vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000").exists())

    def test_moved_archives_pending_to_done(self):
        """D: moved 后原始 pending 移出 vlm_pending → vlm_pending_done。"""
        _create_pending(self.tmp)
        _create_incoming(self.tmp)
        vf.process_one("btcusdt_a_20260601_0000")
        self.assertFalse((vf.VLM_PENDING / "btcusdt_a_20260601_0000").exists(),
                         "pending must leave active queue after moved")
        done_dir = vf.VLM_PENDING_DONE / "btcusdt_a_20260601_0000"
        self.assertTrue(done_dir.exists() or
                        any("__dup_" in p.name for p in vf.VLM_PENDING_DONE.iterdir()),
                        "pending should be archived to vlm_pending_done")

    def test_filtered_archives_pending(self):
        """D: filtered 后 pending 也被移出。"""
        # Use SOL with default BTC playbook: r_dist=1.1% < SOL min 1.5% → filtered (schema-valid)
        _create_pending(self.tmp, symbol="SOL/USDT")
        _create_incoming(self.tmp)  # default playbook (r_dist=1.1% passes BTC but not SOL)
        r = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(r, "filtered")
        self.assertFalse((vf.VLM_PENDING / "btcusdt_a_20260601_0000").exists())

    def test_stale_archives_pending(self):
        vf.SIGNAL_MAX_AGE_MIN = 10
        _create_pending(self.tmp, bar_time="2026-01-01T00:00:00+00:00")
        _create_incoming(self.tmp)
        r = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(r, "stale")
        self.assertFalse((vf.VLM_PENDING / "btcusdt_a_20260601_0000").exists())

    def test_bad_schema_archives_pending(self):
        _create_pending(self.tmp)
        _create_incoming(self.tmp, playbooks=[])
        r = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(r, "bad_schema")
        self.assertFalse((vf.VLM_PENDING / "btcusdt_a_20260601_0000").exists())


# ── E: sandbox 保护所有写路径 ────────────────────────────────────────────────

class TestSandbox(_Base):
    def test_rejects_production_signal_active(self):
        tmp = Path(tempfile.mkdtemp())
        vf.SIGNAL_ACTIVE = tmp / "signal_active"
        vf.ROOT = tmp
        vf.PRODUCER_SANDBOX = True
        with self.assertRaises(SystemExit):
            vf._check_sandbox()
        self.assertFalse((tmp / "signal_active").exists(), "sandbox must NOT create dir before rejecting")

    def test_covered_paths(self):
        """E: sandbox guards all write paths. Test each one individually."""
        for name in ("SIGNAL_ACTIVE", "VLM_DONE_INCOMING", "VLM_REJECTED",
                     "VLM_PENDING", "VLM_PENDING_DONE"):
            tmp = Path(tempfile.mkdtemp())
            setattr(vf, name, tmp / name.lower())
            vf.ROOT = tmp
            vf.PRODUCER_SANDBOX = True
            with self.assertRaises(SystemExit, msg=f"{name} should trigger sandbox"):
                vf._check_sandbox()
            self.assertFalse(getattr(vf, name).exists(),
                             f"{name}: sandbox must not create dir before rejecting")

    def test_allows_custom_paths(self):
        tmp = Path(tempfile.mkdtemp())
        vf.SIGNAL_ACTIVE = tmp / "custom_signal_active"
        vf.VLM_DONE_INCOMING = tmp / "custom_incoming"
        vf.VLM_REJECTED = tmp / "custom_rejected"
        vf.VLM_PENDING_DONE = tmp / "custom_pending_done"
        vf.ROOT = tmp
        vf.PRODUCER_SANDBOX = True
        try:
            vf._check_sandbox()
        except SystemExit:
            self.fail("custom paths should not trigger sandbox")


# ── 原有逻辑回归 ──────────────────────────────────────────────────────────────

class TestRegression(_Base):
    def test_duplicate_active(self):
        _create_pending(self.tmp)
        _create_incoming(self.tmp)
        (vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000").mkdir(parents=True)
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "duplicate_active")

    def test_duplicate_done(self):
        _create_pending(self.tmp)
        _create_incoming(self.tmp)
        (vf.SIGNAL_DONE / "btcusdt_a_20260601_0000").mkdir(parents=True)
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "duplicate_done")

    def test_stale_bar_time(self):
        vf.SIGNAL_MAX_AGE_MIN = 10
        _create_pending(self.tmp, bar_time="2026-01-01T00:00:00+00:00")
        _create_incoming(self.tmp)
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "stale")

    def test_filtered_no_valid_playbooks(self):
        # SOL with BTC-passing playbook: r_dist=1.1% < SOL 1.5% → filtered
        _create_pending(self.tmp, symbol="SOL/USDT")
        _create_incoming(self.tmp)
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "filtered")

    def test_btc_low_r_dist_filtered(self):
        _create_pending(self.tmp, symbol="BTC/USDT")
        _create_incoming(self.tmp, playbooks=[{
            "hypothesis": "DOWNSIDE", "conditional_trade_plan": {
                "activation_rule": {
                    "direction_if_activated": "short",
                    "primary_touch": {"level": 73000, "side": "low"},
                    "activates_if_close_crosses": {"level": 72800, "dir": "below"},
                    "invalidation_after_activation": {"level": 72950, "dir": "above"},
                    "objectives": [{"level": 72500}],
                }
            }
        }])
        self.assertEqual(vf.process_one("btcusdt_a_20260601_0000"), "filtered")

    def test_sandbox_history(self):
        tmp = Path(tempfile.mkdtemp())
        vf.SIGNAL_ACTIVE = tmp / "custom_sig_act"
        vf.ROOT = tmp
        vf.PRODUCER_SANDBOX = True
        try:
            vf._check_sandbox()
        except SystemExit:
            self.fail("custom signal_active should pass sandbox; only prod root rejected")


if __name__ == "__main__":
    unittest.main()
