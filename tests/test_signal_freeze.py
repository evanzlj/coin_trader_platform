"""
Regression tests for the frozen live A/A+ signal universe.

These tests intentionally fail when production signal generation changes.  A
real detector change should mint a new signal universe id and update the
manifest, not silently drift under the old research label.
"""
from __future__ import annotations

from dataclasses import asdict
import inspect
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import monitor
from signal_generator.frozen_universe import (
    FROZEN_POST_VLM_PLAYBOOK_FILTERS,
    FROZEN_SIGNAL_UNIVERSE_ID,
    FROZEN_SYMBOL_PARAMS,
    FROZEN_SYMBOLS,
    SignalUniverseDriftError,
    assert_frozen_runtime,
    diff_frozen_runtime,
)
from signal_generator.generator import SignalGenerator


class TestFrozenSignalUniverse(unittest.TestCase):
    def test_manifest_matches_runtime_params_symbols_and_source(self):
        self.assertEqual(diff_frozen_runtime(symbols=monitor.SYMBOLS), [])
        assert_frozen_runtime(symbols=monitor.SYMBOLS)

    def test_assertion_reports_symbol_drift(self):
        with self.assertRaises(SignalUniverseDriftError):
            assert_frozen_runtime(symbols=("BTC/USDT",))

    def test_signal_generator_defaults_match_freeze(self):
        gen = SignalGenerator(feed=None, symbols=list(FROZEN_SYMBOLS))
        actual = {sym: asdict(gen._sym_params[sym]) for sym in FROZEN_SYMBOLS}
        self.assertEqual(actual, FROZEN_SYMBOL_PARAMS)

    def test_post_vlm_filters_are_not_signal_structure_space(self):
        self.assertIn("activation_rule", FROZEN_POST_VLM_PLAYBOOK_FILTERS["inputs"])
        self.assertNotIn("SignalEvent.structure_space", FROZEN_POST_VLM_PLAYBOOK_FILTERS["metrics"]["r_dist_pct"])
        self.assertEqual(
            FROZEN_POST_VLM_PLAYBOOK_FILTERS["rules"]["BNB/USDT"],
            {"r_dist_min": 0.3, "r_dist_max": 1.0},
        )

    def test_live_grade_policy_is_still_frozen(self):
        source = inspect.getsource(monitor.main)
        self.assertIn(f"assert_frozen_runtime(symbols=SYMBOLS)", source)
        self.assertEqual(monitor.FROZEN_SIGNAL_UNIVERSE_ID, FROZEN_SIGNAL_UNIVERSE_ID)
        self.assertIn('if sig.grade == "A+":', source)
        self.assertIn("A+ signal held (not live)", source)
        self.assertIn("age > 30", source)


if __name__ == "__main__":
    unittest.main()
