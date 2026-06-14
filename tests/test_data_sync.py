"""
data_sync (re-arch Phase 1) tests — mock sqlite, no btc-ml, no network.

Covers: CLOSED-bar-only materialization (no in-progress bar), bootstrap full pull,
incremental open_time>tail, readiness gate (min of 15m ohlcv/flow), 4h as-of context,
fail-closed (missing DB / dup / reverse), audit.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import exec_config as cfg
from live import data_sync as ds


def _mkdb(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE ohlcv_bars(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, timeframe TEXT,
        open_time TIMESTAMP, close_time TIMESTAMP,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        source TEXT, is_fallback INTEGER)""")
    conn.execute("""CREATE TABLE taker_flow_bars(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, timeframe TEXT,
        open_time TIMESTAMP, close_time TIMESTAMP,
        taker_buy_base_volume REAL, taker_sell_base_volume REAL,
        taker_buy_quote_volume REAL, taker_sell_quote_volume REAL,
        trade_count INTEGER, imbalance REAL, source TEXT, payload_json TEXT)""")
    return conn


def _iso(ts):
    return pd.Timestamp(ts, tz="UTC").isoformat()


def _seed_15m(conn, sym, base, n, dataset="ohlcv"):
    """Insert n contiguous 15m bars starting at `base` (open_time)."""
    for i in range(n):
        ot = pd.Timestamp(base, tz="UTC") + pd.Timedelta(minutes=15 * i)
        ct = ot + pd.Timedelta(minutes=15)
        if dataset == "ohlcv":
            conn.execute("INSERT INTO ohlcv_bars(symbol,timeframe,open_time,close_time,open,high,low,close,volume,source,is_fallback)"
                         " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                         (sym, "15m", ot.isoformat(), ct.isoformat(), 1, 2, 0, 1.5, 10, "k", 0))
        else:
            conn.execute("INSERT INTO taker_flow_bars(symbol,timeframe,open_time,close_time,taker_buy_base_volume,"
                         "taker_sell_base_volume,taker_buy_quote_volume,taker_sell_quote_volume,trade_count,imbalance,source,payload_json)"
                         " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                         (sym, "15m", ot.isoformat(), ct.isoformat(), 5, 5, 50, 50, 100, 0.0, "k", "{}"))


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.dbp = self.tmp / "test.db"
        self.conn = _mkdb(self.dbp)
        self._orig_db = cfg.OHLCV_DB
        self._orig_data = ds.DATA_DIR
        self._orig_syms = ds.SYMBOLS
        self._orig_otfs = ds.OHLCV_TFS
        cfg.OHLCV_DB = self.dbp
        ds.DATA_DIR = self.tmp / "data"
        ds.SYMBOLS = ["BTC/USDT"]    # single symbol for focused tests
        ds.OHLCV_TFS = ["15m"]       # most tests only seed 15m; 4h tested explicitly

    def tearDown(self):
        self.conn.close()
        cfg.OHLCV_DB = self._orig_db
        ds.DATA_DIR = self._orig_data
        ds.SYMBOLS = self._orig_syms
        ds.OHLCV_TFS = self._orig_otfs


class TestClosedOnly(_Base):
    def test_in_progress_bar_excluded(self):
        # 4 closed 15m + the current in-progress bar (close_time > now)
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T07:00:00+00:00", 5, "ohlcv")
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T07:00:00+00:00", 5, "taker_flow")
        self.conn.commit()
        # now = 07:48 → bars open 07:00..08:00; 07:45 bar closes 08:00 (>now) = in-progress
        now = pd.Timestamp("2026-06-14T07:48:00+00:00")
        ds.bootstrap(now=now)
        df = pd.read_csv(ds._csv_path("ohlcv", "BTC/USDT", "15m"))
        last = pd.Timestamp(df["open_time"].iloc[-1])
        # closed: 07:00,07:15,07:30 (close<=07:48); in-progress 07:45(→08:00) & 08:00(→08:15) excluded
        self.assertEqual(last, pd.Timestamp("2026-06-14T07:30:00+00:00"))
        self.assertEqual(len(df), 3)


class TestBootstrapIncremental(_Base):
    def test_bootstrap_then_incremental(self):
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 10, "ohlcv")
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 10, "taker_flow")
        self.conn.commit()
        now1 = pd.Timestamp("2026-06-14T03:00:00+00:00")   # all 10 closed
        ds.bootstrap(now=now1)
        df = pd.read_csv(ds._csv_path("ohlcv", "BTC/USDT", "15m"))
        self.assertEqual(len(df), 10)

        # add 3 more bars, incremental should add exactly those
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T02:30:00+00:00", 5, "ohlcv")  # overlaps tail + 3 new
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T02:30:00+00:00", 5, "taker_flow")
        self.conn.commit()
        now2 = pd.Timestamp("2026-06-14T05:00:00+00:00")
        added = ds.data_sync(now=now2)
        df2 = pd.read_csv(ds._csv_path("ohlcv", "BTC/USDT", "15m"))
        self.assertEqual(df2["open_time"].nunique(), len(df2))   # no dups after merge
        self.assertTrue(pd.to_datetime(df2["open_time"], utc=True).is_monotonic_increasing)


class TestReadinessGate(_Base):
    def test_horizon_is_min_of_ohlcv_and_flow(self):
        # ohlcv up to 02:00, flow lags to 01:30
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 9, "ohlcv")   # ...02:00
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 7, "taker_flow")  # ...01:30
        self.conn.commit()
        now = pd.Timestamp("2026-06-14T03:00:00+00:00")
        h = ds.ready_horizon("BTC/USDT", now=now)
        self.assertEqual(h, pd.Timestamp("2026-06-14T01:30:00+00:00"))   # min = flow tail

    def test_horizon_none_when_flow_missing(self):
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 5, "ohlcv")
        self.conn.commit()  # no flow rows
        now = pd.Timestamp("2026-06-14T03:00:00+00:00")
        self.assertIsNone(ds.ready_horizon("BTC/USDT", now=now))

    def test_4h_context_asof_t0(self):
        # No 4h at all → False
        T0 = pd.Timestamp("2026-06-14T02:00:00+00:00")
        self.assertFalse(ds.has_4h_context("BTC/USDT", T0))

        # A 4h bar closed by T0
        ot = pd.Timestamp("2026-06-13T20:00:00+00:00")
        self.conn.execute("INSERT INTO ohlcv_bars(symbol,timeframe,open_time,close_time,open,high,low,close,volume,source,is_fallback)"
                          " VALUES('BTC/USDT','4h',?,?,1,2,0,1.5,10,'k',0)",
                          (ot.isoformat(), (ot + pd.Timedelta(hours=4)).isoformat()))
        self.conn.commit()
        self.assertTrue(ds.has_4h_context("BTC/USDT", T0))

        # A different T0 earlier than the 4h close → must NOT say yes (4h close=00:00 > T0=23:00)
        T0_early = pd.Timestamp("2026-06-13T22:00:00+00:00")
        self.assertFalse(ds.has_4h_context("BTC/USDT", T0_early))


class TestFailClosed(_Base):
    def test_missing_db_raises(self):
        cfg.OHLCV_DB = self.tmp / "nope.db"
        with self.assertRaises(ds.SyncError):
            ds.data_sync(now=pd.Timestamp("2026-06-14T03:00:00+00:00"))

    def test_db_duplicate_raises_before_merge(self):
        """P1-1: dup in DB must cause SyncError during _merge_csv (not get silently deduped)."""
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 5, "ohlcv")
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 5, "taker_flow")
        # inject a duplicate open_time into the DB
        self.conn.execute("INSERT INTO ohlcv_bars(symbol,timeframe,open_time,close_time,open,high,low,close,volume,source,is_fallback)"
                          " VALUES('BTC/USDT','15m','2026-06-14T00:15:00+00:00','2026-06-14T00:30:00+00:00',1,2,0,1.5,10,'k',0)")
        self.conn.commit()
        now = pd.Timestamp("2026-06-14T03:00:00+00:00")
        # _validate_open_times runs BEFORE _merge_csv writes anything — this must fail
        with self.assertRaises(ds.SyncError):
            ds.bootstrap(now=now)

    def test_incremental_without_bootstrap_raises(self):
        """P1-2: data_sync() without existing CSV must raise SyncError, not silently full-pull."""
        # NO bootstrap step — just call data_sync directly on empty DATA_DIR
        now = pd.Timestamp("2026-06-14T03:00:00+00:00")
        with self.assertRaises(ds.SyncError):
            ds.data_sync(now=now)

    def test_raw_dup_csv_audit(self):
        """audit_series still catches a raw dup file (belt-and-suspenders)."""
        p = self.tmp / "dup.csv"
        p.write_text("open_time\n2026-06-14T00:00:00+00:00\n2026-06-14T00:15:00+00:00\n2026-06-14T00:15:00+00:00\n",
                     encoding="utf-8")
        self.assertEqual(ds.audit_series(p, "15m")[0], "duplicate")

    def test_audit_detects_reverse(self):
        p = self.tmp / "rev.csv"
        p.write_text("open_time\n2026-06-14T00:00:00+00:00\n2026-06-14T00:30:00+00:00\n2026-06-14T00:15:00+00:00\n",
                     encoding="utf-8")
        issue = ds.audit_series(p, "15m")
        self.assertEqual(issue[0], "reverse")


class TestCsvFormat(_Base):
    def test_columns_match_legacy(self):
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 3, "ohlcv")
        _seed_15m(self.conn, "BTC/USDT", "2026-06-14T00:00:00+00:00", 3, "taker_flow")
        self.conn.commit()
        ds.bootstrap(now=pd.Timestamp("2026-06-14T03:00:00+00:00"))
        oh = pd.read_csv(ds._csv_path("ohlcv", "BTC/USDT", "15m"))
        fl = pd.read_csv(ds._csv_path("taker_flow", "BTC/USDT", "15m"))
        self.assertEqual(list(oh.columns), ds.OHLCV_COLS)
        self.assertEqual(list(fl.columns), ds.FLOW_COLS)


if __name__ == "__main__":
    unittest.main()
