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

DATA_DIR        = ROOT / "history_data_manager" / "data"
DEDUP_STATE     = ROOT / "live" / "state" / "dedup_state.json"
HEARTBEAT_FILE  = ROOT / "live" / "heartbeat" / "monitor_last_run.txt"
SIGNAL_PENDING  = ROOT / "signal_pending"
CHARTS_DIR      = ROOT / "live" / "charts"

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

def load_dedup_state() -> dict:
    if not DEDUP_STATE.exists():
        logger.warning("dedup_state.json not found — run warmup_replay.py first")
        return {}
    with open(DEDUP_STATE) as f:
        data = json.load(f)
    return data.get("dedup", {})


def save_dedup_state(gen: SignalGenerator) -> None:
    state = gen.get_dedup_state()
    DEDUP_STATE.parent.mkdir(parents=True, exist_ok=True)
    with open(DEDUP_STATE, "w") as f:
        json.dump(
            {"saved_at": pd.Timestamp.utcnow().isoformat(), "dedup": state},
            f, indent=2,
        )


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def update_heartbeat() -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_FILE.write_text(pd.Timestamp.utcnow().isoformat())


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

def write_signal_pending(sig: SignalEvent) -> Path:
    """
    Write signal package to signal_pending/{sym}_{grade}_{ts}/.
    Returns the package directory path.
    """
    sym_slug  = sig.symbol.replace("/", "").lower()
    grade_str = sig.grade.replace("+", "plus")
    ts_str    = sig.bar_time.strftime("%Y%m%d_%H%M")
    pkg_name  = f"{sym_slug}_{grade_str}_{ts_str}"
    pkg_dir   = SIGNAL_PENDING / pkg_name

    pkg_dir.mkdir(parents=True, exist_ok=True)

    # signal.json
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

    # prompt — same format as generate_replay_materials.py
    try:
        df15m, df4h, df_flow = _load_frames(sig)
        bundle = build_prompt(sig, df15m, df4h, df_flow)
        with open(pkg_dir / "prompt.txt", "w") as f:
            f.write("=== SYSTEM ===\n")
            f.write(bundle.system_text)
            f.write("\n\n=== USER TEXT ===\n")
            f.write(bundle.user_text)
    except Exception as e:
        logger.warning("prompt generation failed for %s: %s", pkg_name, e)
        (pkg_dir / "prompt.txt").write_text(f"[prompt generation error: {e}]")

    # charts
    try:
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        path_4h, path_15m = render(sig, data_dir=DATA_DIR, out_dir=CHARTS_DIR)
        shutil.copy(path_4h,  pkg_dir / path_4h.name)
        shutil.copy(path_15m, pkg_dir / path_15m.name)
    except Exception as e:
        logger.warning("chart rendering failed for %s: %s", pkg_name, e)

    # .ready marker — openclaw polls for this
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
    # Step 1: fetch delta
    try:
        fetch_delta()
    except FetchError as e:
        logger.error("fetch failed, skipping cycle: %s", e)
        return [], last_bar_time
    except GapError as e:
        logger.error("gap error, skipping cycle: %s", e)
        return [], last_bar_time

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
    SIGNAL_PENDING.mkdir(parents=True, exist_ok=True)

    # Load dedup state
    dedup = load_dedup_state()
    if not dedup:
        logger.warning("starting with empty dedup state — signals may repeat")

    # Build generator with full history for buffer warmup
    logger.info("loading history for buffer warmup...")
    init_feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS)
    gen = SignalGenerator(feed=init_feed, symbols=SYMBOLS)
    gen.load_dedup_state(dedup)

    # Suppress signal events during initial warmup
    dummy_signals: list = []
    @gen.on_signal
    async def _dummy(evt): dummy_signals.append(evt)
    await init_feed.start()
    gen._signal_handlers.remove(_dummy)
    logger.info("buffer warmup complete (suppressed %d historical signals)", len(dummy_signals))

    last_bar_time = get_latest_bar_time()
    logger.info("monitor started — last bar: %s", last_bar_time)

    while True:
        try:
            signals, new_last = await run_cycle(gen, last_bar_time)

            if new_last and new_last != last_bar_time:
                last_bar_time = new_last

                for sig in signals:
                    passes, reason = _passes_filter(sig)
                    if not passes:
                        logger.info("filtered out %s %s: %s", sig.symbol, sig.grade, reason)
                        continue
                    write_signal_pending(sig)

                save_dedup_state(gen)

            update_heartbeat()

        except Exception as e:
            logger.exception("unexpected error in monitor cycle: %s", e)

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
