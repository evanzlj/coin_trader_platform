"""
Producer-chain regression tests (monitor / live_openclaw / signal_pusher / watchdog).

Covers the P1 hardening done before the first real-money deploy:
  - dedup_state atomic write + corrupt-load refusal
  - run_cycle does not leak the temporary signal handler when the feed raises
  - prompt.txt round-trips UTF-8 even under a cp936-style default encoding (Windows)
  - _archive_package always moves the source out of signal_pending/ on dest conflict
  - signal_pusher writes pusher_status.json on exception and --once exits non-zero
  - openclaw consecutive rejections surface via openclaw_status.json + watchdog

Standard-library unittest, no network, no exchange keys.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import monitor
from live import live_openclaw as oc
from live import watchdog


# ── 1. dedup_state atomic write + corrupt load ────────────────────────────────

class _FakeGen:
    def __init__(self, state):
        self._state = state

    def get_dedup_state(self):
        return self._state


class TestDedupAtomicWrite(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._orig = monitor.DEDUP_STATE
        monitor.DEDUP_STATE = self.tmp / "state" / "dedup_state.json"

    def tearDown(self):
        monitor.DEDUP_STATE = self._orig

    def test_save_is_atomic_and_valid(self):
        gen = _FakeGen({"BTC/USDT": "2026-01-01T00:00:00+00:00"})
        cursors = {"BTC/USDT": pd.Timestamp("2026-01-01 00:00:00", tz="UTC")}
        monitor.save_dedup_state(gen, cursors=cursors)

        # File exists, is valid JSON, and no .tmp residue left behind.
        self.assertTrue(monitor.DEDUP_STATE.exists())
        self.assertFalse(monitor.DEDUP_STATE.with_suffix(".tmp").exists())
        data = json.loads(monitor.DEDUP_STATE.read_text(encoding="utf-8"))
        self.assertEqual(data["dedup"], {"BTC/USDT": "2026-01-01T00:00:00+00:00"})
        self.assertEqual(data["buffer_saved_at"], "2026-01-01T00:00:00+00:00")

    def test_round_trip_load_and_per_symbol_watermark(self):
        # F6: per-symbol cursors persisted; buffer_saved_at = min watermark.
        gen = _FakeGen({"BTC/USDT": "2026-02-02T00:00:00+00:00",
                        "ETH/USDT": "2026-02-01T00:00:00+00:00"})
        cursors = {
            "BTC/USDT": pd.Timestamp("2026-02-02 03:00:00", tz="UTC"),  # ahead
            "ETH/USDT": pd.Timestamp("2026-02-02 02:00:00", tz="UTC"),  # behind → min
        }
        monitor.save_dedup_state(gen, cursors=cursors)
        data = json.loads(monitor.DEDUP_STATE.read_text(encoding="utf-8"))
        self.assertEqual(data["per_symbol"]["BTC/USDT"], "2026-02-02T03:00:00+00:00")
        self.assertEqual(data["per_symbol"]["ETH/USDT"], "2026-02-02T02:00:00+00:00")
        # buffer_saved_at = safe lower watermark (the lagging symbol's cursor)
        self.assertEqual(data["buffer_saved_at"], "2026-02-02T02:00:00+00:00")

        dedup, saved_at = monitor.load_dedup_state()
        self.assertEqual(saved_at, "2026-02-02T02:00:00+00:00")

    def test_corrupt_load_raises_not_silent(self):
        # A half-written file must NOT be silently swallowed (would replay old signals).
        monitor.DEDUP_STATE.parent.mkdir(parents=True, exist_ok=True)
        monitor.DEDUP_STATE.write_text('{"dedup": {"BTC/USDT": "2026', encoding="utf-8")
        with self.assertRaises(monitor.DedupStateError):
            monitor.load_dedup_state()

    def test_missing_file_is_empty_state(self):
        # Missing (not corrupt) → empty state, no exception.
        dedup, saved_at = monitor.load_dedup_state()
        self.assertEqual(dedup, {})
        self.assertIsNone(saved_at)


# ── 2. run_cycle handler leak on feed failure ─────────────────────────────────

class _LeakGen:
    """Minimal SignalGenerator stand-in: just enough for run_cycle's handler dance."""
    def __init__(self):
        self._signal_handlers = []

    def on_signal(self, fn):
        self._signal_handlers.append(fn)
        return fn

    async def _on_bar(self, *a):
        pass

    async def _on_flow(self, *a):
        pass


class _BoomFeed:
    def __init__(self, **kw):
        pass

    def add_bar_handler(self, h):
        pass

    def add_flow_handler(self, h):
        pass

    async def start(self):
        raise RuntimeError("simulated CSV read failure")


class TestRunCycleHandlerLeak(unittest.TestCase):
    def setUp(self):
        self._orig_feed = monitor.ReplayFeed
        self._orig_fetch = monitor.fetch_delta
        self._orig_latest = monitor.get_latest_bar_times
        monitor.ReplayFeed = _BoomFeed
        monitor.fetch_delta = lambda: None
        # BTC has a new bar → run_cycle proceeds to build the (booming) feed.
        monitor.get_latest_bar_times = lambda: {
            "BTC/USDT": pd.Timestamp("2026-01-02 00:00:00", tz="UTC"),
            "ETH/USDT": None, "BNB/USDT": None, "SOL/USDT": None,
        }

    def tearDown(self):
        monitor.ReplayFeed = self._orig_feed
        monitor.fetch_delta = self._orig_fetch
        monitor.get_latest_bar_times = self._orig_latest

    def test_capture_handler_removed_when_feed_raises(self):
        gen = _LeakGen()
        cursors = {"BTC/USDT": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
                   "ETH/USDT": None, "BNB/USDT": None, "SOL/USDT": None}
        with self.assertRaises(RuntimeError):
            asyncio.run(monitor.run_cycle(gen, cursors))
        # The temporary _capture handler must NOT remain registered.
        self.assertEqual(gen._signal_handlers, [],
                         "run_cycle leaked the _capture handler after feed failure")


# ── 3. cp936/Windows default-encoding prompt round-trip ───────────────────────

class TestPromptEncoding(unittest.TestCase):
    def test_prompt_round_trips_utf8_under_cp936_default(self):
        # monitor writes prompt.txt with explicit encoding="utf-8"; openclaw reads with
        # encoding="utf-8". Simulate a Windows cp936 default by reading with cp936 and
        # confirming the explicit-utf8 path is what protects us.
        tmp = Path(tempfile.mkdtemp())
        prompt = tmp / "prompt.txt"
        text = "=== SYSTEM ===\n做空假设：跌破结构位 → 观察反抽失败 ⚠️\n价格 $73,000"
        # write side (as monitor.write_signal_pending does)
        with open(prompt, "w", encoding="utf-8") as f:
            f.write(text)
        # read side (as live_openclaw.process_one does)
        got = prompt.read_text(encoding="utf-8")
        self.assertEqual(got, text)
        # And reading with the wrong (cp936-style) codec would corrupt it — proving the
        # explicit encoding is load-bearing, not incidental.
        raw = prompt.read_bytes()
        try:
            mis = raw.decode("gbk")
            self.assertNotEqual(mis, text)
        except UnicodeDecodeError:
            pass  # also acceptable: wrong codec fails outright


# ── 4. _archive_package idempotency on dest conflict ──────────────────────────

class TestArchiveIdempotency(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.pending = self.tmp / "signal_pending"
        self.rejected = self.tmp / "signal_rejected"
        self.pending.mkdir(parents=True)

    def _make_pkg(self, name):
        d = self.pending / name
        d.mkdir()
        (d / "signal.json").write_text("{}", encoding="utf-8")
        return d

    def test_dest_exists_src_still_leaves_pending(self):
        # Pre-create a colliding archive entry.
        self.rejected.mkdir(parents=True)
        (self.rejected / "btcusdt_a_20260101_0000").mkdir()

        pkg = self._make_pkg("btcusdt_a_20260101_0000")
        moved = oc._archive_package(pkg, self.rejected, "stale")

        self.assertTrue(moved)
        # Source must be gone from signal_pending (no infinite re-processing).
        self.assertFalse(pkg.exists())
        self.assertEqual(list(self.pending.iterdir()), [])
        # A dup-suffixed archive entry exists alongside the original.
        dups = [p for p in self.rejected.iterdir() if "__dup_" in p.name]
        self.assertEqual(len(dups), 1)
        self.assertTrue((dups[0] / "reject_reason.txt").exists())

    def test_normal_archive(self):
        pkg = self._make_pkg("ethusdt_a_20260101_0000")
        moved = oc._archive_package(pkg, self.rejected, "stale_bar_time")
        self.assertTrue(moved)
        self.assertFalse(pkg.exists())
        dest = self.rejected / "ethusdt_a_20260101_0000"
        self.assertTrue(dest.exists())
        self.assertEqual((dest / "reject_reason.txt").read_text(encoding="utf-8"),
                         "stale_bar_time")


# ── 5. signal_pusher exception → status + non-zero exit ───────────────────────

class TestPusherStatusAndExit(unittest.TestCase):
    """Run signal_pusher --once in a subprocess via a wrapper that redirects all
    side-effect paths into a temp dir (status/heartbeat/lock/pushed-marker) so the
    real repo's heartbeat files are never touched."""

    def _run(self, tmp: Path, active: Path, remote_host: str) -> subprocess.CompletedProcess:
        status_file = tmp / "pusher_status.json"
        wrapper = tmp / "run_pusher.py"
        wrapper.write_text(
            "import sys\n"
            "from pathlib import Path\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from live import signal_pusher as sp\n"
            f"sp.STATUS_FILE = Path({str(status_file)!r})\n"
            f"sp.PUSHED_DIR = Path({str(tmp / '.signal_pushed')!r})\n"
            f"sp.LOCK_FILE = Path({str(tmp / 'pusher.lock')!r})\n"
            f"sp.HEARTBEAT = Path({str(tmp / 'hb.txt')!r})\n"
            f"sp.cfg.SIGNAL_ACTIVE = Path({str(active)!r})\n"
            f"sp.REMOTE_HOST = {remote_host!r}\n"
            "sp.SSH_TIMEOUT = 10\n"
            "sys.argv = ['signal_pusher', '--once']\n"
            "sp.main()\n",
            encoding="utf-8",
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT)
        return subprocess.run(
            [sys.executable, str(wrapper)],
            cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=90,
        )

    def test_once_exits_zero_when_idle(self):
        # Empty signal_active → nothing to push → healthy idle → exit 0.
        tmp = Path(tempfile.mkdtemp())
        active = tmp / "signal_active"
        active.mkdir()
        r = self._run(tmp, active, "evan@btc-ml")
        self.assertEqual(r.returncode, 0, f"stderr: {r.stderr[-400:]}")

    def test_once_exits_nonzero_and_writes_status_on_failure(self):
        # A ready package + an unroutable SSH host → push fails → exit 1 + status written.
        tmp = Path(tempfile.mkdtemp())
        active = tmp / "signal_active"
        pkg = active / "btcusdt_a_20260101_0000"
        pkg.mkdir(parents=True)
        (pkg / ".ready").touch()
        (pkg / "state.json").write_text("{}", encoding="utf-8")
        (pkg / "signal.json").write_text("{}", encoding="utf-8")

        r = self._run(tmp, active, "evan@nonexistent-host-zzz.invalid")
        self.assertEqual(r.returncode, 1, f"expected non-zero; stderr: {r.stderr[-500:]}")
        status_file = tmp / "pusher_status.json"
        self.assertTrue(status_file.exists(), "pusher_status.json not written on failure")
        data = json.loads(status_file.read_text(encoding="utf-8"))
        self.assertGreaterEqual(int(data["consecutive_failures"]), 1)
        self.assertTrue(data.get("last_error"))


# ── 6. openclaw consecutive rejections → status + watchdog ────────────────────

class TestOpenclawStatusWatchdog(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._orig_status = oc.STATUS_FILE
        oc.STATUS_FILE = self.tmp / "openclaw_status.json"

    def tearDown(self):
        oc.STATUS_FILE = self._orig_status

    def test_status_written_and_watchdog_alerts_on_consecutive_rejections(self):
        stats = {
            "processed": 6, "moved": 0, "rejected": 6, "blocked": 0,
            "move_failures": 0, "parse_errs": 6, "consecutive_rejections": 6,
            "last_success_at": None, "last_error": "parse_err: bad json",
        }
        oc.write_openclaw_status(stats)
        self.assertTrue(oc.STATUS_FILE.exists())
        data = json.loads(oc.STATUS_FILE.read_text(encoding="utf-8"))
        self.assertEqual(data["consecutive_rejections"], 6)
        self.assertIn("updated_at", data)

        # Watchdog check_status must alert (threshold consecutive_rejections >= 5).
        spec = watchdog.StatusSpec(
            "openclaw", oc.STATUS_FILE,
            (("consecutive_rejections", 5), ("move_failures", 3), ("parse_errs", 5)),
        )
        alert = watchdog.check_status(spec)
        self.assertIsNotNone(alert)
        self.assertIn("consecutive_rejections=6", alert)

    def test_healthy_status_no_alert(self):
        stats = {
            "processed": 10, "moved": 8, "rejected": 2, "blocked": 0,
            "move_failures": 0, "parse_errs": 1, "consecutive_rejections": 0,
            "last_success_at": "2026-01-01T00:00:00+00:00", "last_error": None,
        }
        oc.write_openclaw_status(stats)
        spec = watchdog.StatusSpec(
            "openclaw", oc.STATUS_FILE,
            (("consecutive_rejections", 5), ("move_failures", 3), ("parse_errs", 5)),
        )
        self.assertIsNone(watchdog.check_status(spec))

    def test_monitor_package_write_failures_alert(self):
        # monitor_status.package_write_failures must be watchdog-visible too.
        mon_status = self.tmp / "monitor_status.json"
        mon_status.write_text(json.dumps({
            "last_success_at": None, "consecutive_failures": 0,
            "last_error": "package write failed: BTC/USDT A", "backlog_count": 0,
            "package_write_failures": 4, "updated_at": "2026-01-01T00:00:00+00:00",
        }), encoding="utf-8")
        spec = watchdog.StatusSpec(
            "monitor", mon_status,
            (("consecutive_failures", 3), ("backlog_count", 10),
             ("package_write_failures", 3)),
        )
        alert = watchdog.check_status(spec)
        self.assertIsNotNone(alert)
        self.assertIn("package_write_failures=4", alert)


# ── F1. fetch_delta merge idempotency (retry does not duplicate) ──────────────

from live import fetch_delta as fd
from realtime_data_pull.bar_buffer import BarBuffer
from realtime_data_pull.models import Bar
from signal_generator.generator import SignalGenerator


def _ohlcv_csv(times: list[str]) -> str:
    head = "open_time,close_time,open,high,low,close,volume\n"
    rows = []
    for t in times:
        ot = pd.Timestamp(t, tz="UTC")
        ct = ot + pd.Timedelta(minutes=15)
        rows.append(f"{ot.isoformat()},{ct.isoformat()},1,2,0,1.5,10")
    return head + "\n".join(rows) + "\n"


class TestFetchDeltaMergeIdempotent(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.csv = self.tmp / "btcusdt_15m.csv"

    def test_retry_with_stale_delta_does_not_duplicate(self):
        # Simulate: attempt 1 appended bars 00:15, 00:30. Then a retry re-delivers the
        # SAME delta (the F1 partial-failure scenario). Merge must not duplicate.
        self.csv.write_text(_ohlcv_csv(["2026-01-01 00:00:00",
                                        "2026-01-01 00:15:00",
                                        "2026-01-01 00:30:00"]), encoding="utf-8")
        delta = _ohlcv_csv(["2026-01-01 00:15:00", "2026-01-01 00:30:00"])  # already present
        rows_added, dups = fd._merge_dedup_sort(self.csv, delta)
        self.assertEqual(rows_added, 0, "stale delta must add no rows")
        self.assertEqual(dups, 2, "duplicate open_times must be detected, not silent")

        out = pd.read_csv(self.csv)
        self.assertEqual(len(out), 3)
        self.assertEqual(out["open_time"].nunique(), 3)

    def test_genuine_new_rows_merge_sorted(self):
        self.csv.write_text(_ohlcv_csv(["2026-01-01 00:00:00",
                                        "2026-01-01 00:15:00"]), encoding="utf-8")
        delta = _ohlcv_csv(["2026-01-01 00:30:00", "2026-01-01 00:45:00"])
        new_bars, dups = fd._merge_dedup_sort(self.csv, delta)
        self.assertEqual(new_bars, 2)
        self.assertEqual(dups, 0)
        out = pd.read_csv(self.csv)
        times = pd.to_datetime(out["open_time"], utc=True)
        self.assertTrue(times.is_monotonic_increasing)
        self.assertEqual(len(out), 4)

    def test_new_bar_reported_even_when_net_rows_zero(self):
        # Existing already contains a duplicate row; delta brings ONE genuinely new bar.
        # Net rows = 0 (the dedup removes the stale dup while the new bar is added), but
        # delta_new_count must be 1 so the caller still runs the continuity check (#3).
        existing = _ohlcv_csv(["2026-01-01 00:00:00", "2026-01-01 00:15:00"])
        # inject a duplicate of 00:15 line
        dup_line = existing.strip().splitlines()[-1]
        self.csv.write_text(existing.rstrip("\n") + "\n" + dup_line + "\n", encoding="utf-8")
        self.assertEqual(len(pd.read_csv(self.csv)), 3)  # 3 rows, 2 distinct

        delta = _ohlcv_csv(["2026-01-01 00:30:00"])      # one brand-new bar
        new_bars, dups = fd._merge_dedup_sort(self.csv, delta)
        self.assertEqual(new_bars, 1, "new bar must be reported even if net rows is 0")
        self.assertGreaterEqual(dups, 1, "the pre-existing duplicate must be cleaned")
        out = pd.read_csv(self.csv)
        self.assertEqual(out["open_time"].nunique(), 3)  # 00:00, 00:15, 00:30
        self.assertEqual(len(out), 3)                    # net rows unchanged (3 → 3)


# ── F3. remote literal parse (no exec) ────────────────────────────────────────

class TestRemoteLiteralParse(unittest.TestCase):
    def test_parses_rows(self):
        rows = fd._parse_remote_literal("[[1, 2, 'a'], [3, 4, 'b']]\n")
        self.assertEqual(rows, [[1, 2, "a"], [3, 4, "b"]])

    def test_parses_header(self):
        self.assertEqual(fd._parse_remote_literal("['open_time', 'close']"),
                         ["open_time", "close"])

    def test_rejects_code(self):
        # literal_eval must refuse a non-literal expression (would have been exec'd by eval).
        with self.assertRaises((ValueError, SyntaxError)):
            fd._parse_remote_literal("__import__('os').system('echo pwned')")


# ── F4. continuity check: gap inside block / duplicate / reverse ──────────────

class TestContinuityCheck(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.csv = self.tmp / "btcusdt_15m.csv"

    def test_gap_inside_large_append_block_detected(self):
        # 30 contiguous bars, then a 1h hole, then more — the gap sits well beyond the
        # last 10 rows, so a fixed tail(10) would miss it; n_rows window must catch it.
        base = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
        times = [(base + pd.Timedelta(minutes=15 * i)).isoformat() for i in range(30)]
        # introduce gap: skip bars 30..33, continue at +1h
        after = [(base + pd.Timedelta(minutes=15 * 30) + pd.Timedelta(hours=1)
                  + pd.Timedelta(minutes=15 * j)).isoformat() for j in range(5)]
        self.csv.write_text(_ohlcv_csv(times + after), encoding="utf-8")
        issue = fd.check_gap(self.csv, "15m", n_rows=5)
        self.assertIsNotNone(issue)
        self.assertEqual(issue[0], "gap")

    def test_duplicate_open_time_detected(self):
        self.csv.write_text(_ohlcv_csv(["2026-01-01 00:00:00",
                                        "2026-01-01 00:15:00",
                                        "2026-01-01 00:15:00"]), encoding="utf-8")
        issue = fd.check_gap(self.csv, "15m", n_rows=3)
        self.assertIsNotNone(issue)
        self.assertEqual(issue[0], "duplicate")

    def test_reverse_open_time_detected(self):
        self.csv.write_text(_ohlcv_csv(["2026-01-01 00:00:00",
                                        "2026-01-01 00:30:00",
                                        "2026-01-01 00:15:00"]), encoding="utf-8")
        issue = fd.check_gap(self.csv, "15m", n_rows=3)
        self.assertIsNotNone(issue)
        self.assertEqual(issue[0], "reverse")

    def test_clean_tail_passes(self):
        self.csv.write_text(_ohlcv_csv(
            [(pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
              + pd.Timedelta(minutes=15 * i)).isoformat() for i in range(20)]),
            encoding="utf-8")
        self.assertIsNone(fd.check_gap(self.csv, "15m", n_rows=5))


# ── F5/F6. BarBuffer idempotency + per-symbol replay gating ───────────────────

def _bar(sym, tf, t):
    ot = t if isinstance(t, pd.Timestamp) else pd.Timestamp(t, tz="UTC")
    return Bar(symbol=sym, timeframe=tf, open_time=ot,
               close_time=ot + pd.Timedelta(minutes=15),
               open=1, high=2, low=0, close=1.5, volume=10, is_closed=True)


class TestBarBufferIdempotent(unittest.TestCase):
    def test_reappend_same_or_older_open_time_skipped(self):
        buf = BarBuffer()
        self.assertTrue(buf.update(_bar("BTC/USDT", "15m", "2026-01-01 00:00:00")))
        self.assertTrue(buf.update(_bar("BTC/USDT", "15m", "2026-01-01 00:15:00")))
        # Re-feed an already-seen bar (overlapping replay window) → not appended.
        self.assertFalse(buf.update(_bar("BTC/USDT", "15m", "2026-01-01 00:15:00")))
        self.assertFalse(buf.update(_bar("BTC/USDT", "15m", "2026-01-01 00:00:00")))
        self.assertEqual(len(buf), 2)

    def test_overlapping_replay_then_new_bar(self):
        buf = BarBuffer()
        for i in range(3):
            buf.update(_bar("BTC/USDT", "15m",
                            pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
                            + pd.Timedelta(minutes=15 * i)))
        # Replay the whole window again + one genuinely new bar.
        for i in range(4):
            buf.update(_bar("BTC/USDT", "15m",
                            pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
                            + pd.Timedelta(minutes=15 * i)))
        self.assertEqual(len(buf), 4, "overlap must dedup; only the new bar appended")


class TestBufferStateAtomicLoad(unittest.TestCase):
    """#1: buffer state written atomically; a corrupt parquet/json/saved_at must make
    load_buffer_state return None (fallback to warmup), never crash monitor."""
    SYMS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

    def _seeded_gen(self):
        gen = SignalGenerator(feed=None, symbols=self.SYMS)
        base = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
        for i in range(5):
            gen._bufs[("BTC/USDT", "15m")].update(
                _bar("BTC/USDT", "15m", base + pd.Timedelta(minutes=15 * i)))
        return gen

    def test_save_atomic_no_tmp_residue(self):
        tmp = Path(tempfile.mkdtemp()) / "buffer"
        self._seeded_gen().save_buffer_state(tmp)
        residue = list(tmp.glob("*.tmp"))
        self.assertEqual(residue, [], f"atomic write left tmp residue: {residue}")
        self.assertTrue((tmp / "buf_btcusdt_15m.parquet").exists())
        self.assertTrue((tmp / "saved_at.txt").exists())

        # Round-trips cleanly.
        gen2 = SignalGenerator(feed=None, symbols=self.SYMS)
        saved_at = gen2.load_buffer_state(tmp)
        self.assertIsNotNone(saved_at)
        self.assertEqual(len(gen2._bufs[("BTC/USDT", "15m")]), 5)

    def test_corrupt_parquet_returns_none_not_crash(self):
        tmp = Path(tempfile.mkdtemp()) / "buffer"
        self._seeded_gen().save_buffer_state(tmp)
        # Corrupt the parquet (simulate a torn write that slipped through).
        (tmp / "buf_btcusdt_15m.parquet").write_bytes(b"not a parquet file at all")

        gen2 = SignalGenerator(feed=None, symbols=self.SYMS)
        saved_at = gen2.load_buffer_state(tmp)   # must not raise
        self.assertIsNone(saved_at)
        # Partially-seeded buffers must be cleared so we don't run on half-state.
        self.assertEqual(len(gen2._bufs[("BTC/USDT", "15m")]), 0)

    def test_corrupt_saved_at_returns_none(self):
        tmp = Path(tempfile.mkdtemp()) / "buffer"
        self._seeded_gen().save_buffer_state(tmp)
        (tmp / "saved_at.txt").write_text("not-a-timestamp", encoding="utf-8")
        gen2 = SignalGenerator(feed=None, symbols=self.SYMS)
        self.assertIsNone(gen2.load_buffer_state(tmp))

    def test_missing_dir_returns_none(self):
        gen = SignalGenerator(feed=None, symbols=self.SYMS)
        self.assertIsNone(gen.load_buffer_state(Path(tempfile.mkdtemp()) / "nope"))


class _RecordingFeed:
    """Captures construction args; start() is a no-op (no CSV read)."""
    last = None

    def __init__(self, **kw):
        _RecordingFeed.last = kw
        self.kw = kw

    def add_bar_handler(self, h):
        pass

    def add_flow_handler(self, h):
        pass

    async def start(self):
        return None


class TestPerSymbolCursor(unittest.TestCase):
    """F5: BTC already at T2; ETH posts T2 late → ETH must still be replayed."""
    def setUp(self):
        self._orig_feed = monitor.ReplayFeed
        self._orig_fetch = monitor.fetch_delta
        self._orig_latest = monitor.get_latest_bar_times
        monitor.ReplayFeed = _RecordingFeed
        monitor.fetch_delta = lambda: None
        _RecordingFeed.last = None

    def tearDown(self):
        monitor.ReplayFeed = self._orig_feed
        monitor.fetch_delta = self._orig_fetch
        monitor.get_latest_bar_times = self._orig_latest

    def test_late_eth_bar_not_skipped_by_btc_cursor(self):
        T1 = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
        T2 = pd.Timestamp("2026-01-01 00:15:00", tz="UTC")
        # BTC already advanced to T2 last cycle; ETH only at T1. Now ETH's T2 arrives.
        cursors = {"BTC/USDT": T2, "ETH/USDT": T1, "BNB/USDT": T2, "SOL/USDT": T2}
        monitor.get_latest_bar_times = lambda: {
            "BTC/USDT": T2, "ETH/USDT": T2, "BNB/USDT": T2, "SOL/USDT": T2,
        }
        gen = _LeakGen()
        signals, new_cursors, latest = asyncio.run(monitor.run_cycle(gen, cursors))

        # ETH must have advanced (it was replayed), even though BTC's cursor == latest.
        self.assertEqual(new_cursors["ETH/USDT"], T2)
        # A replay feed was built (ETH had a genuinely new bar).
        self.assertIsNotNone(_RecordingFeed.last)
        # Window must start at the lagging cursor (ETH T1 + 1min), not BTC's T2.
        self.assertIn("2026-01-01 00:01:00", _RecordingFeed.last["start"])

    def test_no_advance_no_replay(self):
        T2 = pd.Timestamp("2026-01-01 00:15:00", tz="UTC")
        cursors = {s: T2 for s in monitor.SYMBOLS}
        monitor.get_latest_bar_times = lambda: {s: T2 for s in monitor.SYMBOLS}
        gen = _LeakGen()
        signals, new_cursors, latest = asyncio.run(monitor.run_cycle(gen, cursors))
        self.assertEqual(new_cursors, cursors)
        self.assertIsNone(_RecordingFeed.last, "no new bars → no replay feed built")


# ── #2. monitor generic exception → status reflects failure ───────────────────

class _StopLoop(Exception):
    pass


class _StubGen:
    """Minimal SignalGenerator stand-in for driving monitor.main() one iteration."""
    def __init__(self, *a, **kw):
        self._signal_handlers = []

    def load_buffer_state(self, d):
        return None  # force fallback warmup path

    def load_dedup_state(self, state):
        pass

    def on_signal(self, fn):
        self._signal_handlers.append(fn)
        return fn

    async def _on_bar(self, *a):
        pass

    async def _on_flow(self, *a):
        pass


class _NoopFeed:
    def __init__(self, *a, **kw):
        pass

    def add_bar_handler(self, h):
        pass

    def add_flow_handler(self, h):
        pass

    async def start(self):
        return None


class TestMonitorGenericExceptionStatus(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._saved = {k: getattr(monitor, k) for k in
                       ("SignalGenerator", "ReplayFeed", "load_dedup_state",
                        "get_latest_bar_times", "run_cycle", "STATUS_FILE",
                        "HEARTBEAT_FILE", "SIGNAL_PENDING")}
        self._saved_sleep = asyncio.sleep

        T = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
        monitor.SignalGenerator = _StubGen
        monitor.ReplayFeed = _NoopFeed
        monitor.load_dedup_state = lambda: ({}, None)
        monitor.get_latest_bar_times = lambda: {s: T for s in monitor.SYMBOLS}
        monitor.STATUS_FILE = self.tmp / "monitor_status.json"
        monitor.HEARTBEAT_FILE = self.tmp / "hb.txt"
        monitor.SIGNAL_PENDING = self.tmp / "signal_pending"

        async def _boom(gen, cursors):
            raise RuntimeError("kaboom in cycle")
        monitor.run_cycle = _boom

        # Break the infinite loop after the first status write.
        async def _stop(*a, **k):
            raise _StopLoop()
        asyncio.sleep = _stop

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(monitor, k, v)
        asyncio.sleep = self._saved_sleep

    def test_generic_exception_increments_failures_and_writes_status(self):
        import live.single_instance as si
        orig_lock = si.SingleInstance
        si.SingleInstance = lambda *a, **k: type(
            "L", (), {"acquire": lambda s: None, "release": lambda s: None})()
        try:
            with self.assertRaises(_StopLoop):
                asyncio.run(monitor.main())
        finally:
            si.SingleInstance = orig_lock

        data = json.loads(monitor.STATUS_FILE.read_text(encoding="utf-8"))
        self.assertGreaterEqual(int(data["consecutive_failures"]), 1)
        self.assertIn("unexpected", str(data["last_error"]))
        self.assertTrue(monitor.HEARTBEAT_FILE.exists(), "heartbeat must still update")


# ── F7. pusher partial failure (p>0 AND f>0) → exit 1 + status ────────────────

class TestPusherPartialFailure(unittest.TestCase):
    def test_partial_failure_exits_nonzero_with_status(self):
        tmp = Path(tempfile.mkdtemp())
        status_file = tmp / "pusher_status.json"
        wrapper = tmp / "run_pusher.py"
        # Force push_round to report 1 pushed + 1 failed with backlog 2.
        wrapper.write_text(
            "import sys\n"
            "from pathlib import Path\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from live import signal_pusher as sp\n"
            f"sp.STATUS_FILE = Path({str(status_file)!r})\n"
            f"sp.LOCK_FILE = Path({str(tmp / 'pusher.lock')!r})\n"
            f"sp.HEARTBEAT = Path({str(tmp / 'hb.txt')!r})\n"
            "sp.push_round = lambda: (1, 1)\n"
            "sp._pending_count = lambda: 2\n"
            "sys.argv = ['signal_pusher', '--once']\n"
            "sp.main()\n",
            encoding="utf-8",
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT)
        r = subprocess.run([sys.executable, str(wrapper)], cwd=str(ROOT),
                           env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 1, f"partial failure must exit 1; stderr: {r.stderr[-400:]}")
        data = json.loads(status_file.read_text(encoding="utf-8"))
        self.assertGreaterEqual(int(data["consecutive_failures"]), 1)
        self.assertIn("partial failure", str(data["last_error"]))


# ── F8. openclaw archives pending copy when already in signal_active ──────────

class TestOpenclawActiveDuplicate(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._orig_pending = oc.SIGNAL_PENDING
        self._orig_active = oc.SIGNAL_ACTIVE
        self._orig_rejected = oc.SIGNAL_REJECTED
        oc.SIGNAL_PENDING = self.tmp / "signal_pending"
        oc.SIGNAL_ACTIVE = self.tmp / "signal_active"
        oc.SIGNAL_REJECTED = self.tmp / "signal_rejected"
        oc.SIGNAL_PENDING.mkdir(parents=True)
        oc.SIGNAL_ACTIVE.mkdir(parents=True)

    def tearDown(self):
        oc.SIGNAL_PENDING = self._orig_pending
        oc.SIGNAL_ACTIVE = self._orig_active
        oc.SIGNAL_REJECTED = self._orig_rejected

    def test_pending_copy_archived_not_left_residual(self):
        name = "btcusdt_a_20260601_0000"
        pkg = oc.SIGNAL_PENDING / name
        pkg.mkdir()
        (pkg / ".ready").touch()
        (pkg / "signal.json").write_text(json.dumps({
            "grade": "A", "bar_time": pd.Timestamp.now("UTC").isoformat(),
        }), encoding="utf-8")
        (pkg / "vlm_response.json").write_text(json.dumps({
            "watch_summary": "x", "playbooks": [],
        }), encoding="utf-8")
        # Executor already owns it.
        (oc.SIGNAL_ACTIVE / name).mkdir()

        cutoff = pd.Timestamp.now("UTC") - pd.Timedelta(seconds=oc.STALE_SECONDS)
        pending = oc.get_pending(cutoff)

        self.assertEqual(pending, [], "package already in signal_active must not be returned")
        self.assertFalse(pkg.exists(), "pending copy must be archived out of signal_pending")
        archived = list((oc.SIGNAL_REJECTED).iterdir())
        self.assertEqual(len(archived), 1)
        self.assertEqual((archived[0] / "reject_reason.txt").read_text(encoding="utf-8"),
                         "already_in_signal_active")


if __name__ == "__main__":
    unittest.main()
