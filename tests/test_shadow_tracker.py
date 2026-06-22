"""
shadow_tracker tests — 影子追踪器（只读观察，复用 replay scorer）。

覆盖：激活→TP 的相位推进会推流水；终态后不再重复推；无 activation_rule 的剧本跳过。
mock 文件系统 + mock flow_event，不连 broker / 不连交易所。
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import shadow_tracker as st
from live import notify


def _write_signal(active_dir: Path, pkg: str, symbol="BTC/USDT", side="near_resistance"):
    d = active_dir / pkg
    d.mkdir(parents=True)
    t0 = "2026-06-01T00:00:00+00:00"
    (d / "state.json").write_text(json.dumps({
        "symbol": symbol, "grade": "A", "bar_time": t0, "structure_side": side,
        "overall_status": "WATCHING", "playbooks": [],
    }), encoding="utf-8")
    (d / "vlm_response.json").write_text(json.dumps({
        "watch_summary": "x",
        "playbooks": [{
            "hypothesis": "DOWNSIDE", "conditional_trade_plan": {"activation_rule": {
                "direction_if_activated": "short",
                "primary_touch": {"level": 100.0, "side": "high"},
                "activates_if_close_crosses": {"level": 98.0, "dir": "below"},
                "cancels_if_close_crosses_first": {"level": 102.0, "dir": "above"},
                "invalidation_after_activation": {"level": 101.0, "dir": "above"},
                "objectives": [{"level": 95.0}, {"level": 92.0}],
            }}},
            {"hypothesis": "CHOP_WAIT", "conditional_trade_plan": {"activation_rule": None}},
        ],
    }), encoding="utf-8")
    return d, pd.Timestamp(t0)


def _write_bars(data_dir: Path, slug: str, bars: list[tuple], t0: pd.Timestamp):
    """bars: list of (minutes_after_t0, high, low, close)."""
    ohlcv = data_dir / "ohlcv"
    ohlcv.mkdir(parents=True, exist_ok=True)
    rows = []
    for mins, hi, lo, cl in bars:
        ot = t0 + pd.Timedelta(minutes=mins)
        rows.append({"symbol": "BTC/USDT", "timeframe": "15m", "open_time": ot.isoformat(),
                     "close_time": (ot + pd.Timedelta(minutes=15)).isoformat(),
                     "open": cl, "high": hi, "low": lo, "close": cl, "volume": 1.0})
    pd.DataFrame(rows).to_csv(ohlcv / f"{slug}_15m.csv", index=False)


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._olds = {k: getattr(st, k) for k in ("SIGNAL_ACTIVE", "DATA_DIR", "STATE_FILE", "LEDGER_FILE", "HEARTBEAT")}
        st.SIGNAL_ACTIVE = self.tmp / "signal_active"
        st.DATA_DIR = self.tmp / "data"
        st.STATE_FILE = self.tmp / "shadow_state.json"
        st.LEDGER_FILE = self.tmp / "shadow_ledger.jsonl"
        st.HEARTBEAT = self.tmp / "hb.txt"
        st.SIGNAL_ACTIVE.mkdir(parents=True)
        self.pushed = []
        self._orig_flow = notify.flow_event
        notify.flow_event = lambda m: self.pushed.append(m)

    def tearDown(self):
        notify.flow_event = self._orig_flow
        for k, v in self._olds.items():
            setattr(st, k, v)


class TestShadowTracker(_Base):
    def test_activation_to_tp2_pushes_and_terminal(self):
        d, t0 = _write_signal(st.SIGNAL_ACTIVE, "btcusdt_A_test")
        # 触发 primary(high>=100) → close<98 激活 → 触 TP1(95) → 触 TP2(92)
        _write_bars(st.DATA_DIR, "btcusdt", [
            (15, 100.5, 99.0, 99.5),
            (30,  99.0, 97.0, 97.5),
            (45,  96.0, 94.5, 95.0),
            (60,  93.0, 91.5, 92.0),
        ], t0)
        n = st.scan_once()
        self.assertGreaterEqual(n, 1, "应至少推一条相位流水")
        # 终态应是 tp2
        state = json.loads(st.STATE_FILE.read_text(encoding="utf-8"))
        self.assertEqual(state["btcusdt_A_test::DOWNSIDE"], "tp2")
        self.assertTrue(any("TP1+TP2" in m or "TP2" in m for m in self.pushed),
                        f"pushed={self.pushed}")

    def test_chop_wait_skipped(self):
        _write_signal(st.SIGNAL_ACTIVE, "btcusdt_A_test")
        _write_bars(st.DATA_DIR, "btcusdt", [(15, 99.0, 98.0, 98.5)], t0=pd.Timestamp("2026-06-01T00:00:00+00:00"))
        st.scan_once()
        state = json.loads(st.STATE_FILE.read_text(encoding="utf-8"))
        # CHOP_WAIT(activation_rule=None) 不应进 state
        self.assertNotIn("btcusdt_A_test::CHOP_WAIT", state)

    def test_ledger_and_report(self):
        _write_signal(st.SIGNAL_ACTIVE, "btcusdt_A_test")
        t0 = pd.Timestamp("2026-06-01T00:00:00+00:00")
        _write_bars(st.DATA_DIR, "btcusdt", [
            (15, 100.5, 99.0, 99.5), (30, 99.0, 97.0, 97.5),
            (45, 96.0, 94.5, 95.0), (60, 93.0, 91.5, 92.0),
        ], t0)
        st.scan_once()
        # 台账应有一条终态记录
        self.assertTrue(st.LEDGER_FILE.exists())
        lines = [l for l in st.LEDGER_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
        self.assertEqual(len(lines), 1)
        rec = json.loads(lines[0])
        self.assertEqual(rec["phase"], "tp2")
        self.assertEqual(rec["struct_r"], 2.0)
        # report 能汇总
        rpt = st.report()
        self.assertIn("影子绩效", rpt)
        self.assertIn("TP2=1", rpt)

    def test_terminal_not_repushed(self):
        _write_signal(st.SIGNAL_ACTIVE, "btcusdt_A_test")
        t0 = pd.Timestamp("2026-06-01T00:00:00+00:00")
        _write_bars(st.DATA_DIR, "btcusdt", [
            (15, 100.5, 99.0, 99.5), (30, 99.0, 97.0, 97.5),
            (45, 96.0, 94.5, 95.0), (60, 93.0, 91.5, 92.0),
        ], t0)
        st.scan_once()
        self.pushed.clear()
        st.scan_once()                       # 第二轮：已终态，不应再推
        self.assertEqual(self.pushed, [], "终态后不应重复推送")


if __name__ == "__main__":
    unittest.main()
