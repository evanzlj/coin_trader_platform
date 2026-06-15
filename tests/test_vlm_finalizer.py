"""
vlm_finalizer (re-arch Phase 2) tests — mock filesystem, no network.

Covers: decision table (§3.4 / G5):
  - orphan (no local vlm_pending)
  - duplicate (signal_active exists, signal_done exists)
  - bad schema (missing watch_summary, error set, no playbooks)
  - filtered (no valid playbooks after post-VLM filter)
  - stale (bar_time > TTL)
  - moved (full pass)
  - sandbox guard (G2)
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


def _local_pending(tmp, name="btcusdt_a_20260601_0000", bar_time=None,
                   symbol="BTC/USDT", grade="A") -> Path:
    if bar_time is None:
        bar_time = _recent_iso(10)
    d = tmp / "vlm_pending" / name
    d.mkdir(parents=True)
    (d / "signal.json").write_text(json.dumps({
        "symbol": symbol, "grade": grade,
        "bar_time": bar_time,
        "close": 70000, "structure_side": "near_resistance",
        "structure_space": 2.0, "position_in_structure": 0.95,
    }), encoding="utf-8")
    return d


def _pkg(tmp, name="btcusdt_a_20260601_0000", bar_time=None,
         symbol="BTC/USDT", grade="A", playbooks=None) -> Path:
    """Create a minimal vlm_done_incoming pkg with vlm_response.json, return pkg dir."""
    d = tmp / "vlm_done_incoming" / name
    d.mkdir(parents=True)
    pbs = playbooks if playbooks is not None else [{
        "hypothesis": "DOWNSIDE",
        "conditional_trade_plan": {
            "activation_rule": {
                "direction_if_activated": "short",
                "primary_touch": {"level": 73000, "side": "low"},
                "activates_if_close_crosses": {"level": 72800, "dir": "below"},
                "cancels_if_close_crosses_first": {"level": 74000, "dir": "above"},
                # r_dist=1.1% >= BTC 0.5% AND tp1=0.55% < 1.5% → passes BTC filter
                "invalidation_after_activation": {"level": 72000, "dir": "above"},
                "objectives": [{"level": 72400}],
            }
        }
    }]
    (d / "vlm_response.json").write_text(json.dumps({
        "watch_summary": "summary", "prompt_version": "v1",
        "playbooks": pbs,
    }), encoding="utf-8")
    return d


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._olds = {k: getattr(vf, k) for k in
                      ("VLM_DONE_INCOMING", "VLM_PENDING", "VLM_REJECTED",
                       "SIGNAL_ACTIVE", "SIGNAL_DONE", "SIGNAL_MAX_AGE_MIN",
                       "STATUS_FILE", "HEARTBEAT_FILE")}
        vf.VLM_DONE_INCOMING = self.tmp / "vlm_done_incoming"
        vf.VLM_PENDING = self.tmp / "vlm_pending"
        vf.VLM_REJECTED = self.tmp / "vlm_rejected"
        vf.SIGNAL_ACTIVE = self.tmp / "signal_active"
        vf.SIGNAL_DONE = self.tmp / "signal_done"
        vf.SIGNAL_MAX_AGE_MIN = 240
        vf.STATUS_FILE = self.tmp / "finalizer_status.json"
        vf.HEARTBEAT_FILE = self.tmp / "hb.txt"
        vf.VLM_DONE_INCOMING.mkdir(parents=True)

    def tearDown(self):
        for k, v in self._olds.items():
            setattr(vf, k, v)


class TestDecisionTable(_Base):
    def test_moved(self):
        _local_pending(self.tmp)
        _pkg(self.tmp)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "moved")
        dest = vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000"
        self.assertTrue(dest.exists())
        self.assertTrue((dest / "state.json").exists())
        state = json.loads((dest / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["overall_status"], "WATCHING")
        self.assertEqual(state["symbol"], "BTC/USDT")

    def test_orphan_no_local_pending(self):
        pkg_dir = _pkg(self.tmp)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "orphan")
        # incoming pkg must be archived (not left in done_incoming)
        self.assertFalse(pkg_dir.exists())
        archives = list((vf.VLM_REJECTED).iterdir())
        self.assertGreaterEqual(len(archives), 1)
        self.assertIn("orphan", (archives[0] / "reject_reason.txt").read_text(encoding="utf-8"))

    def test_duplicate_active(self):
        _local_pending(self.tmp)
        _pkg(self.tmp)
        (vf.SIGNAL_ACTIVE / "btcusdt_a_20260601_0000").mkdir(parents=True)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "duplicate_active")

    def test_duplicate_done(self):
        _local_pending(self.tmp)
        _pkg(self.tmp)
        (vf.SIGNAL_DONE / "btcusdt_a_20260601_0000").mkdir(parents=True)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "duplicate_done")

    def test_bad_schema_missing_watch_summary(self):
        _local_pending(self.tmp)
        pkg_dir = _pkg(self.tmp)
        resp = json.loads((pkg_dir / "vlm_response.json").read_text(encoding="utf-8"))
        del resp["watch_summary"]
        (pkg_dir / "vlm_response.json").write_text(json.dumps(resp), encoding="utf-8")
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "bad_schema")

    def test_bad_schema_error_flag(self):
        _local_pending(self.tmp)
        pkg_dir = _pkg(self.tmp)
        resp = json.loads((pkg_dir / "vlm_response.json").read_text(encoding="utf-8"))
        resp["error"] = "chatgpt timeout"
        (pkg_dir / "vlm_response.json").write_text(json.dumps(resp), encoding="utf-8")
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "bad_schema")

    def test_bad_schema_no_playbooks(self):
        _local_pending(self.tmp)
        pkg_dir = _pkg(self.tmp)
        (pkg_dir / "vlm_response.json").write_text(json.dumps({
            "watch_summary": "x", "playbooks": [],
        }), encoding="utf-8")
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "bad_schema")

    def test_filtered_no_valid_playbooks(self):
        _local_pending(self.tmp)
        # playbook with no activation_rule → filtered out → no valid playbooks
        pkg_dir = _pkg(self.tmp, playbooks=[{"hypothesis": "TEST", "conditional_trade_plan": {}}])
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "filtered")

    def test_stale_bar_time(self):
        vf.SIGNAL_MAX_AGE_MIN = 10  # 10 min TTL
        _local_pending(self.tmp, bar_time="2026-01-01T00:00:00+00:00")  # very old
        _pkg(self.tmp)
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "stale")

    def test_bad_local_signal_json(self):
        _local_pending(self.tmp)
        _pkg(self.tmp)
        (vf.VLM_PENDING / "btcusdt_a_20260601_0000" / "signal.json").write_text(
            "not json", encoding="utf-8")
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "bad_local_signal")

    def test_bad_response_json(self):
        _local_pending(self.tmp)
        pkg_dir = _pkg(self.tmp)
        (pkg_dir / "vlm_response.json").write_text("not json at all", encoding="utf-8")
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "bad_json")


class TestSandbox(_Base):
    def test_sandbox_rejects_production_signal_active(self):
        tmp = Path(tempfile.mkdtemp())
        vf.SIGNAL_ACTIVE = tmp / "signal_active"
        vf.ROOT = tmp
        vf.PRODUCER_SANDBOX = True
        with self.assertRaises(SystemExit):
            vf._check_sandbox()

    def test_sandbox_allows_custom_signal_active(self):
        tmp = Path(tempfile.mkdtemp())
        vf.SIGNAL_ACTIVE = tmp / "custom_signal_active"
        vf.ROOT = tmp
        vf.PRODUCER_SANDBOX = True
        try:
            vf._check_sandbox()
        except SystemExit:
            self.fail("custom path should not trigger sandbox rejection")


class TestFilterLogic(_Base):
    def test_btc_low_r_dist_filtered(self):
        """BTC r_dist < 0.5% must be filtered out."""
        _local_pending(self.tmp, symbol="BTC/USDT")
        # Narrow r_dist (inv=72950 → r=150/72800=0.206% < 0.5%) → should filter
        _pkg(self.tmp, playbooks=[{
            "hypothesis": "DOWNSIDE",
            "conditional_trade_plan": {
                "activation_rule": {
                    "direction_if_activated": "short",
                    "primary_touch": {"level": 73000, "side": "low"},
                    "activates_if_close_crosses": {"level": 72800, "dir": "below"},
                    "invalidation_after_activation": {"level": 72950, "dir": "above"},
                    "objectives": [{"level": 72400}],
                }
            }
        }])
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "filtered", "r_dist=0.206% must be < 0.5% BTC threshold")

    def test_btc_with_adequate_r_dist_passes(self):
        """Make a playbook with r_dist >= 0.5% and TP1 < 1.5%."""
        _local_pending(self.tmp, symbol="BTC/USDT")
        _pkg(self.tmp, playbooks=[{
            "hypothesis": "DOWNSIDE",
            "conditional_trade_plan": {
                "activation_rule": {
                    "direction_if_activated": "short",
                    "primary_touch": {"level": 73000, "side": "low"},
                    "activates_if_close_crosses": {"level": 72800, "dir": "below"},  # r=200/72800=0.275%
                    "cancels_if_close_crosses_first": {"level": 74000, "dir": "above"},
                    "invalidation_after_activation": {"level": 72300, "dir": "above"},  # r=500/72800=0.687%
                    "objectives": [{"level": 72400}],  # TP1=400/72800=0.549%
                }
            }
        }])
        result = vf.process_one("btcusdt_a_20260601_0000")
        self.assertEqual(result, "moved")

    def test_sol_low_r_dist_filtered(self):
        _local_pending(self.tmp, symbol="SOL/USDT")
        _pkg(self.tmp)
        result = vf.process_one("btcusdt_a_20260601_0000")
        # SOL requires r_dist >= 1.5%, default is 0.027%
        self.assertEqual(result, "filtered")


if __name__ == "__main__":
    unittest.main()
