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

POLL_INTERVAL        = 30    # seconds between new-bar checks
SIGNAL_TTL_BARS      = 2     # discard signal_pending entries older than this many bars (30 min)
BUFFER_SAVE_SECONDS  = 600   # F6: persist buffer state at most this often during operation

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

class DedupStateError(RuntimeError):
    """dedup_state.json exists but is corrupt/unreadable — refuse to start blind."""


def load_dedup_state() -> tuple[dict, str | None]:
    """Returns (dedup_dict, buffer_saved_at_str | None).

    A missing file → empty state (first run / pre-warmup). A *corrupt* file
    (half-written / invalid JSON) raises DedupStateError instead of being
    silently swallowed: starting with unknown dedup state would replay old
    signals. Caller (main) turns this into an explicit status + non-zero exit.
    """
    if not DEDUP_STATE.exists():
        logger.warning("dedup_state.json not found — run warmup_replay.py first")
        return {}, None
    try:
        data = json.loads(DEDUP_STATE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, ValueError) as e:
        raise DedupStateError(
            f"dedup_state.json corrupt/unreadable ({e}); refusing to start with "
            f"unknown dedup state — fix the file or rerun warmup_replay.py"
        ) from e
    return data.get("dedup", {}), data.get("buffer_saved_at")


def _cursor_iso(cursors: "dict[str, pd.Timestamp | None] | None") -> dict:
    if not cursors:
        return {}
    return {s: (t.isoformat() if t is not None else None) for s, t in cursors.items()}


def save_dedup_state(gen: SignalGenerator,
                     cursors: "dict[str, pd.Timestamp | None] | None" = None) -> None:
    """Atomically persist dedup state + per-symbol cursors.

    buffer_saved_at is the *minimum* cursor across symbols (a safe lower watermark:
    every symbol has been processed at least up to it), not a single BTC time (F6).
    per_symbol carries each symbol's own cursor for observability / debugging.
    """
    state = gen.get_dedup_state()
    DEDUP_STATE.parent.mkdir(parents=True, exist_ok=True)
    payload: dict = {"saved_at": pd.Timestamp.utcnow().isoformat(), "dedup": state}
    per_symbol = _cursor_iso(cursors)
    if per_symbol:
        payload["per_symbol"] = per_symbol
        watermarks = [t for t in (cursors or {}).values() if t is not None]
        if watermarks:
            payload["buffer_saved_at"] = min(watermarks).isoformat()
    # Atomic write: tmp + os.replace so a crash mid-write can't leave a half
    # JSON that the next startup's load_dedup_state would choke on.
    tmp = DEDUP_STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, DEDUP_STATE)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def update_heartbeat() -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_FILE.write_text(pd.Timestamp.utcnow().isoformat(), encoding="utf-8")


def _write_status(consecutive_failures: int, last_success_at: "str | None",
                  last_error: "str | None", backlog_count: int,
                  package_write_failures: int = 0,
                  per_symbol: "dict | None" = None) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "last_success_at":        last_success_at,
        "consecutive_failures":   consecutive_failures,
        "last_error":             last_error,
        "backlog_count":          backlog_count,
        "package_write_failures": package_write_failures,
        "per_symbol":             per_symbol or {},
        "updated_at":             pd.Timestamp.utcnow().isoformat(),
    }, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_FILE)


def _build_per_symbol_status(cursors: "dict[str, pd.Timestamp | None]",
                             latest: "dict[str, pd.Timestamp | None]") -> dict:
    """Per-symbol {cursor, latest_bar, staleness_min} so a single-symbol data outage
    is visible to ops / watchdog even when the others keep flowing (F5)."""
    now = pd.Timestamp.utcnow()
    out: dict = {}
    for sym in SYMBOLS:
        lt = latest.get(sym)
        # latest is an open_time; the bar closes 15m later.
        staleness = None
        if lt is not None:
            staleness = round((now - (lt + pd.Timedelta(minutes=15))).total_seconds() / 60, 1)
        cur = cursors.get(sym)
        out[sym] = {
            "cursor":        cur.isoformat() if cur is not None else None,
            "latest_bar":    lt.isoformat() if lt is not None else None,
            "staleness_min": staleness,
        }
    return out


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
    (pkg_dir / "signal.json").write_text(
        json.dumps(signal_data, indent=2, ensure_ascii=False), encoding="utf-8")

    prompt_ok = False
    charts_ok = False

    # prompt
    try:
        df15m, df4h, df_flow = _load_frames(sig)
        bundle = build_prompt(sig, df15m, df4h, df_flow)
        with open(pkg_dir / "prompt.txt", "w", encoding="utf-8") as f:
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

def _latest_for_symbol(symbol: str) -> pd.Timestamp | None:
    """Last open_time in this symbol's 15m CSV, or None."""
    slug = symbol.replace("/", "").lower()
    csv_path = DATA_DIR / "ohlcv" / f"{slug}_15m.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if df.empty:
            return None
        return pd.Timestamp(df["open_time"].iloc[-1], tz="UTC")
    except Exception:
        return None


def get_latest_bar_times() -> dict[str, pd.Timestamp | None]:
    """Per-symbol latest 15m open_time. Each symbol is tracked independently so a
    late-arriving bar on ETH/BNB/SOL is not skipped just because BTC's cursor already
    advanced (F5)."""
    return {sym: _latest_for_symbol(sym) for sym in SYMBOLS}


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_cycle(gen: SignalGenerator,
                    cursors: "dict[str, pd.Timestamp | None]",
                    ) -> tuple[list[SignalEvent], dict[str, pd.Timestamp | None],
                               dict[str, pd.Timestamp | None]]:
    """
    Run one monitor cycle:
      1. fetch delta
      2. replay new bars (per-symbol cursors) through gen
      3. return (detected signals, new cursors, latest-bar map)

    Per-symbol cursors (F5): the replay window starts at the *earliest* lagging
    symbol's cursor (min) and ends at the latest bar seen. All symbols are replayed
    over that window; symbols already ahead simply re-feed bars they've buffered, and
    BarBuffer.update is idempotent on open_time so those are skipped (no duplicate
    bars, no re-fired signals). A symbol that fell behind (late T2) is caught up.
    """
    # Step 1: fetch delta — FetchError/GapError propagate to main loop for counter tracking
    fetch_delta()

    latest = get_latest_bar_times()

    # Which symbols have a genuinely newer bar than their own cursor?
    advancing = {
        s: latest[s] for s in SYMBOLS
        if latest.get(s) is not None
        and (cursors.get(s) is None or latest[s] > cursors[s])
    }
    if not advancing:
        return [], cursors, latest

    # Window: from the earliest cursor among advancing symbols (None → from beginning),
    # to the latest bar across them.
    adv_cursors = [cursors.get(s) for s in advancing]
    start_ts = None if any(c is None for c in adv_cursors) else min(adv_cursors)
    end_ts = max(advancing.values())

    detected: list[SignalEvent] = []

    @gen.on_signal
    async def _capture(evt: SignalEvent) -> None:
        detected.append(evt)

    start_str = (start_ts + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S") \
        if start_ts is not None else None
    end_str   = end_ts.strftime("%Y-%m-%d %H:%M:%S")

    mini_feed = ReplayFeed(
        data_dir = DATA_DIR,
        symbols  = SYMBOLS,
        start    = start_str,
        end      = end_str,
    )
    mini_feed.add_bar_handler(gen._on_bar)
    mini_feed.add_flow_handler(gen._on_flow)
    try:
        await mini_feed.start()
    finally:
        # Always unregister the temporary handler — if mini_feed.start() raises
        # (CSV read error, etc.) a leaked _capture would fire on every future
        # signal and pin a stale `detected` list. ValueError = already gone.
        try:
            gen._signal_handlers.remove(_capture)
        except ValueError:
            pass

    new_cursors = dict(cursors)
    for s, t in advancing.items():
        new_cursors[s] = t
    return detected, new_cursors, latest


async def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("monitor already running: %s", e)
        sys.exit(1)

    SIGNAL_PENDING.mkdir(parents=True, exist_ok=True)

    try:
        dedup, buffer_saved_at_str = load_dedup_state()
    except DedupStateError as e:
        logger.error("FATAL: %s", e)
        # Surface the corruption in status (not just a raw traceback) so the
        # watchdog/operator sees *why* monitor refused to start, then exit non-zero.
        try:
            _write_status(consecutive_failures=1, last_success_at=None,
                          last_error=str(e)[:200], backlog_count=-1,
                          package_write_failures=0)
        except Exception:
            pass
        sys.exit(1)
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
        # Incremental warmup start = the *earliest* 15m buffer tail across symbols (a
        # safe lower watermark). Replaying from there re-feeds bars some symbols already
        # buffered, but BarBuffer.update is idempotent on open_time so those are skipped
        # — no holes for a lagging symbol, no duplicates for an ahead one (F6).
        tails = [gen._bufs[(s, "15m")].last for s in SYMBOLS]
        tail_times = [b.open_time for b in tails if b is not None]
        incr_from = min(tail_times) if tail_times else saved_at
        incr_start = (incr_from + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("buffer state loaded — incremental warmup from %s ...", incr_start)

        incr_feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS, start=incr_start)
        incr_feed.add_bar_handler(gen._on_bar)
        incr_feed.add_flow_handler(gen._on_flow)

        suppressed: list = []
        @gen.on_signal
        async def _dummy_incr(evt): suppressed.append(evt)
        try:
            await incr_feed.start()
        finally:
            try:
                gen._signal_handlers.remove(_dummy_incr)
            except ValueError:
                pass
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
        try:
            await init_feed.start()
        finally:
            try:
                gen._signal_handlers.remove(_dummy)
            except ValueError:
                pass
        logger.info("buffer warmup complete (suppressed %d historical signals)", len(dummy_signals))

    cursors: dict[str, pd.Timestamp | None] = get_latest_bar_times()
    latest = dict(cursors)
    logger.info("monitor started — per-symbol cursors: %s",
                {s: (t.isoformat() if t is not None else None) for s, t in cursors.items()})

    consecutive_failures = 0
    last_success_at: "str | None" = None
    last_error: "str | None" = None
    package_write_failures = 0   # cumulative: signals that detected but failed to write a complete package
    last_buffer_save = pd.Timestamp.utcnow()

    while True:
        try:
            signals, new_cursors, latest = await run_cycle(gen, cursors)
            consecutive_failures = 0
            last_error = None

            if new_cursors != cursors:
                cursors = new_cursors
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
                    # None = prompt/charts failed → package moved to signal_rejected/.
                    # This is a functional failure (a real signal never reached openclaw),
                    # so surface it in status rather than dropping silently.
                    if write_signal_pending(sig) is None:
                        package_write_failures += 1
                        last_error = f"package write failed: {sig.symbol} {sig.grade} {sig.bar_time}"

                save_dedup_state(gen, cursors=cursors)

                # F6: periodically persist buffer state so a restart replays only minutes
                # of bars, not days. Throttled to BUFFER_SAVE_SECONDS to bound parquet I/O.
                if (pd.Timestamp.utcnow() - last_buffer_save).total_seconds() >= BUFFER_SAVE_SECONDS:
                    try:
                        gen.save_buffer_state(BUFFER_STATE_DIR)
                        last_buffer_save = pd.Timestamp.utcnow()
                    except Exception as save_err:
                        logger.warning("buffer state save failed: %s", save_err)

            update_heartbeat()

        except (FetchError, GapError) as e:
            consecutive_failures += 1
            last_error = str(e)[:200]
            logger.error("fetch/gap error (consecutive %d): %s", consecutive_failures, e)
            update_heartbeat()   # process is alive; status file carries the failure info

        except Exception as e:
            # Unexpected failure must also surface in functional health, not just logs:
            # otherwise a persistently-throwing cycle keeps a stale "last success" status
            # while the watchdog sees nothing wrong.
            consecutive_failures += 1
            last_error = f"unexpected: {str(e)[:180]}"
            logger.exception("unexpected error in monitor cycle (consecutive %d): %s",
                             consecutive_failures, e)
            update_heartbeat()   # process is alive; status carries the failure

        try:
            backlog = sum(
                1 for d in SIGNAL_PENDING.iterdir()
                if d.is_dir() and (d / ".ready").exists()
            ) if SIGNAL_PENDING.exists() else 0
        except Exception:
            backlog = -1
        _write_status(consecutive_failures, last_success_at, last_error, backlog,
                      package_write_failures=package_write_failures,
                      per_symbol=_build_per_symbol_status(cursors, latest))

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
