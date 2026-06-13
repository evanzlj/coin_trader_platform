#!/usr/bin/env python3
"""
Live signal monitor — Process A.

Polls for new 15m bar closes, runs signal detection, generates charts,
and writes signal packages to signal_pending/ for openclaw to pick up.

Startup:
  1. Load dedup state from live/state/dedup_state.json (produced by warmup_replay.py)
  2. Enter polling loop

Loop (triggered by new bar, polled every 30s):
  1. fetch_delta  — pull new bars from btc-ml
  2. reload CSVs into SignalGenerator via a lightweight re-scan
  3. detect signals on the new bar
  4. for each signal: apply filters, generate charts, write signal_pending/
  5. save dedup state + update heartbeat

Usage:
    python3 live/monitor.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from realtime_data_pull.feed import ReplayFeed
from signal_generator.generator import SignalGenerator
from signal_generator.events import SignalEvent
from draw_kline import render
from prompt_generator.builder import build_prompt
from live.fetch_delta import fetch_delta, FetchError, GapError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR         = ROOT / "history_data_manager" / "data"
DEDUP_STATE      = ROOT / "live" / "state" / "dedup_state.json"
BUFFER_STATE_DIR = ROOT / "live" / "state" / "buffer"
HEARTBEAT_FILE   = ROOT / "live" / "heartbeat" / "monitor_last_run.txt"
SIGNAL_PENDING   = ROOT / "signal_pending"
SIGNAL_REJECTED  = ROOT / "signal_rejected"
CHARTS_DIR       = ROOT / "live" / "charts"
LOCK_FILE        = ROOT / "live" / "monitor.lock"
STATUS_FILE      = ROOT / "live" / "heartbeat" / "monitor_status.json"

SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

POLL_INTERVAL   = 30   # seconds between new-bar checks
SIGNAL_TTL_BARS = 2    # discard signal_pending entries older than this many bars (30 min)

# ── Per-symbol live filters ───────────────────────────────────────────────────
# Matches research findings; applied after VLM response but pre-scored here
# to avoid wasting a VLM call on signals that will be discarded anyway.

def _passes_filter(sig: SignalEvent) -> tuple[bool, str]:
    """
    Return (passes, reason).
    Checks r_dist (structure_space as proxy) and b2act >= 2 enforced by scorer.
    TP1 zone exclusions are checked post-VLM in executor; not pre-filterable here.
    """
    sym = sig.symbol

    if sym == "BTC/USDT":
        if sig.structure_space < 0.5:
            return False, f"r_dist {sig.structure_space:.2f}% < 0.5%"

    elif sym == "ETH/USDT":
        if sig.structure_space < 1.5:
            return False, f"r_dist {sig.structure_space:.2f}% < 1.5%"

    elif sym == "BNB/USDT":
        if not (0.3 <= sig.structure_space <= 1.0):
            return False, f"r_dist {sig.structure_space:.2f}% outside 0.3-1.0%"

    elif sym == "SOL/USDT":
        if sig.structure_space < 1.5:
            return False, f"r_dist {sig.structure_space:.2f}% < 1.5%"

    return True, ""


# ── Dedup state I/O ───────────────────────────────────────────────────────────

def load_dedup_state() -> tuple[dict, str | None]:
    """Returns (dedup_dict, buffer_saved_at_str | None)."""
    if not DEDUP_STATE.exists():
        logger.warning("dedup_state.json not found — run warmup_replay.py first")
        return {}, None
    with open(DEDUP_STATE) as f:
        data = json.load(f)
    return data.get("dedup", {}), data.get("buffer_saved_at")


def save_dedup_state(gen: SignalGenerator, buffer_saved_at: str | None = None) -> None:
    state = gen.get_dedup_state()
    DEDUP_STATE.parent.mkdir(parents=True, exist_ok=True)
    payload: dict = {"saved_at": pd.Timestamp.utcnow().isoformat(), "dedup": state}
    if buffer_saved_at is not None:
        payload["buffer_saved_at"] = buffer_saved_at
    with open(DEDUP_STATE, "w") as f:
        json.dump(payload, f, indent=2)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def update_heartbeat() -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_FILE.write_text(pd.Timestamp.utcnow().isoformat())


def _write_status(consecutive_failures: int, last_success_at: "str | None",
                  last_error: "str | None", backlog_count: int) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "last_success_at":      last_success_at,
        "consecutive_failures": consecutive_failures,
        "last_error":           last_error,
        "backlog_count":        backlog_count,
        "updated_at":           pd.Timestamp.utcnow().isoformat(),
    }, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_FILE)


# ── Data loaders for prompt builder ──────────────────────────────────────────

def _load_df(path: Path, t0: pd.Timestamp,
             lookback_hours: int) -> pd.DataFrame:
    """Load a CSV and return rows up to and including t0 within lookback window."""
    df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
    for col in ("open_time", "close_time"):
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        else:
            df[col] = df[col].dt.tz_convert("UTC")
    cutoff = t0 - pd.Timedelta(hours=lookback_hours)
    return df[(df["open_time"] >= cutoff) & (df["open_time"] <= t0)].reset_index(drop=True)


def _load_frames(sig: SignalEvent) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load df15m, df4h, df_flow for one signal."""
    slug = sig.symbol.replace("/", "").lower()
    t0   = sig.bar_time
    df15m  = _load_df(DATA_DIR / "ohlcv"      / f"{slug}_15m.csv", t0, lookback_hours=14*24)
    df4h   = _load_df(DATA_DIR / "ohlcv"      / f"{slug}_4h.csv",  t0, lookback_hours=90*24)
    df_flow = _load_df(DATA_DIR / "taker_flow" / f"{slug}_15m.csv", t0, lookback_hours=14*24)
    return df15m, df4h, df_flow


# ── Signal package writer ─────────────────────────────────────────────────────

def write_signal_pending(sig: SignalEvent) -> "Path | None":
    """
    Write signal package to signal_pending/{sym}_{grade}_{ts}/.
    Returns the package directory path, or None if prompt/charts failed
    (incomplete packages are moved to signal_rejected/ instead).
    """
    sym_slug  = sig.symbol.replace("/", "").lower()
    grade_str = sig.grade.replace("+", "plus")
    ts_str    = sig.bar_time.strftime("%Y%m%d_%H%M")
    pkg_name  = f"{sym_slug}_{grade_str}_{ts_str}"
    pkg_dir   = SIGNAL_PENDING / pkg_name

    pkg_dir.mkdir(parents=True, exist_ok=True)

    # signal.json — always written (useful for diagnostics in rejected/ too)
    signal_data = {
        "symbol":                 sig.symbol,
        "grade":                  sig.grade,
        "bar_time":               sig.bar_time.isoformat(),
        "close":                  sig.close,
        "structure_side":         sig.structure_side,
        "structure_space":        sig.structure_space,
        "position_in_structure":  sig.position_in_structure,
        "vol_ratio":              sig.vol_ratio,
        "weekly_trend":           sig.weekly_trend,
        "h4_support":             sig.h4_support,
        "h4_resistance":          sig.h4_resistance,
    }
    (pkg_dir / "signal.json").write_text(json.dumps(signal_data, indent=2))

    prompt_ok = False
    charts_ok = False

    # prompt
    try:
        df15m, df4h, df_flow = _load_frames(sig)
        bundle = build_prompt(sig, df15m, df4h, df_flow)
        with open(pkg_dir / "prompt.txt", "w") as f:
            f.write("=== SYSTEM ===\n")
            f.write(bundle.system_text)
            f.write("\n\n=== USER TEXT ===\n")
            f.write(bundle.user_text)
        prompt_ok = True
    except Exception as e:
        logger.warning("prompt generation failed for %s: %s", pkg_name, e)

    # charts
    try:
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        path_4h, path_15m = render(sig, data_dir=DATA_DIR, out_dir=CHARTS_DIR)
        shutil.copy(path_4h,  pkg_dir / path_4h.name)
        shutil.copy(path_15m, pkg_dir / path_15m.name)
        charts_ok = True
    except Exception as e:
        logger.warning("chart rendering failed for %s: %s", pkg_name, e)

    if not prompt_ok or not charts_ok:
        # Package is incomplete — move to signal_rejected/ rather than touching .ready
        SIGNAL_REJECTED.mkdir(parents=True, exist_ok=True)
        dest = SIGNAL_REJECTED / pkg_name
        try:
            if not dest.exists():
                pkg_dir.rename(dest)
        except Exception as mv_err:
            logger.warning("could not move rejected package %s: %s", pkg_name, mv_err)
        logger.warning("signal rejected (prompt_ok=%s charts_ok=%s): %s",
                       prompt_ok, charts_ok, pkg_name)
        return None

    # .ready marker — only written when package is complete
    (pkg_dir / ".ready").touch()

    logger.info("signal_pending written: %s", pkg_name)
    return pkg_dir


# ── New-bar detector ──────────────────────────────────────────────────────────

def get_latest_bar_time() -> pd.Timestamp | None:
    """Read the last open_time from the BTC 15m CSV (proxy for all symbols)."""
    csv_path = DATA_DIR / "ohlcv" / "btcusdt_15m.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if df.empty:
            return None
        return pd.Timestamp(df["open_time"].iloc[-1], tz="UTC")
    except Exception:
        return None


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_cycle(gen: SignalGenerator, last_bar_time: pd.Timestamp | None,
                    ) -> tuple[list[SignalEvent], pd.Timestamp | None]:
    """
    Run one monitor cycle:
      1. fetch delta
      2. replay new bars through gen
      3. return detected signals + new last_bar_time
    """
    # Step 1: fetch delta — FetchError/GapError propagate to main loop for counter tracking
    fetch_delta()

    new_bar_time = get_latest_bar_time()
    if new_bar_time is None or new_bar_time == last_bar_time:
        return [], last_bar_time

    # Step 2: replay only the new bars through generator
    # We re-use the existing gen (which already has dedup + buffer state)
    # by feeding it just the new bars via a narrow ReplayFeed window.
    detected: list[SignalEvent] = []

    @gen.on_signal
    async def _capture(evt: SignalEvent) -> None:
        detected.append(evt)

    start_str = (last_bar_time + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S") \
        if last_bar_time else None
    end_str   = new_bar_time.strftime("%Y-%m-%d %H:%M:%S")

    mini_feed = ReplayFeed(
        data_dir = DATA_DIR,
        symbols  = SYMBOLS,
        start    = start_str,
        end      = end_str,
    )
    mini_feed.add_bar_handler(gen._on_bar)
    mini_feed.add_flow_handler(gen._on_flow)
    await mini_feed.start()

    # Remove the temporary handler
    gen._signal_handlers.remove(_capture)

    return detected, new_bar_time


async def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("monitor already running: %s", e)
        sys.exit(1)

    SIGNAL_PENDING.mkdir(parents=True, exist_ok=True)

    dedup, buffer_saved_at_str = load_dedup_state()
    if not dedup:
        logger.warning("starting with empty dedup state — signals may repeat")

    # ── Try to restore persisted buffer state (fast path) ────────────────────
    # warmup_replay.py writes buffer state after a full 2020-present replay.
    # On restart we just load those BarBuffers and replay only the gap
    # (buffer_saved_at → now), which is minutes to hours, not years.

    gen = SignalGenerator(feed=None, symbols=SYMBOLS)
    saved_at = gen.load_buffer_state(BUFFER_STATE_DIR)

    if saved_at is not None:
        gen.load_dedup_state(dedup)
        incr_start = (saved_at + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("buffer state loaded — incremental warmup from %s ...", incr_start)

        incr_feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS, start=incr_start)
        incr_feed.add_bar_handler(gen._on_bar)
        incr_feed.add_flow_handler(gen._on_flow)

        suppressed: list = []
        @gen.on_signal
        async def _dummy_incr(evt): suppressed.append(evt)
        await incr_feed.start()
        gen._signal_handlers.remove(_dummy_incr)
        logger.info("incremental warmup complete (suppressed %d signals)", len(suppressed))

    else:
        # ── Fallback: 56-week internal warmup (pre-warmup_replay baseline) ──
        warmup_start = (pd.Timestamp.utcnow() - pd.Timedelta(weeks=56)).strftime("%Y-%m-%d")
        logger.warning(
            "no persisted buffer state found — falling back to 56-week warmup from %s",
            warmup_start,
        )
        init_feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS, start=warmup_start)
        # Re-create gen with this feed since we need it registered at construction
        gen = SignalGenerator(feed=init_feed, symbols=SYMBOLS)
        gen.load_dedup_state(dedup)

        dummy_signals: list = []
        @gen.on_signal
        async def _dummy(evt): dummy_signals.append(evt)
        await init_feed.start()
        gen._signal_handlers.remove(_dummy)
        logger.info("buffer warmup complete (suppressed %d historical signals)", len(dummy_signals))

    last_bar_time = get_latest_bar_time()
    logger.info("monitor started — last bar: %s", last_bar_time)

    consecutive_failures = 0
    last_success_at: "str | None" = None
    last_error: "str | None" = None

    while True:
        try:
            signals, new_last = await run_cycle(gen, last_bar_time)
            consecutive_failures = 0
            last_error = None

            if new_last and new_last != last_bar_time:
                last_bar_time = new_last
                last_success_at = pd.Timestamp.utcnow().isoformat()

                now = pd.Timestamp.utcnow()
                for sig in signals:
                    # Grade filter: A+ not yet live — remove when A+ execution is ready
                    if sig.grade == "A+":
                        logger.info("A+ signal held (not live): %s %s", sig.symbol, sig.bar_time)
                        continue
                    # Recency filter: discard signals older than 2 bars (30 min)
                    age = (now - sig.bar_time).total_seconds() / 60
                    if age > 30:
                        logger.info("stale signal skipped (%d min old): %s %s",
                                    int(age), sig.symbol, sig.grade)
                        continue
                    passes, reason = _passes_filter(sig)
                    if not passes:
                        logger.info("filtered out %s %s: %s", sig.symbol, sig.grade, reason)
                        continue
                    write_signal_pending(sig)

                save_dedup_state(gen, buffer_saved_at=buffer_saved_at_str)

            update_heartbeat()

        except (FetchError, GapError) as e:
            consecutive_failures += 1
            last_error = str(e)[:200]
            logger.error("fetch/gap error (consecutive %d): %s", consecutive_failures, e)
            update_heartbeat()   # process is alive; status file carries the failure info

        except Exception as e:
            logger.exception("unexpected error in monitor cycle: %s", e)

        try:
            backlog = sum(
                1 for d in SIGNAL_PENDING.iterdir()
                if d.is_dir() and (d / ".ready").exists()
            ) if SIGNAL_PENDING.exists() else 0
        except Exception:
            backlog = -1
        _write_status(consecutive_failures, last_success_at, last_error, backlog)

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
