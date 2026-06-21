#!/usr/bin/env python3
"""
Live signal monitor — re-arch Phase 1b (producer on btc-ml).

Polls for new 15m bar closes, runs signal detection, generates charts,
and writes VLM signal packages to vlm_pending/ for the vlm_finalizer/signal_sync.

Startup:
  1. Load dedup state from live/state/dedup_state.json (produced by warmup_replay.py)
  2. Enter polling loop

Loop (triggered by new bar, polled every 30s):
  1. data_sync  — pull new closed bars from local ai_crypto_analyst.db
  2. gate each cursor to ready_horizon (min of ohlcv_15m, flow_15m)
  3. detect signals on new bar (per-symbol, per-readiness)
  4. for each signal: check 4h context, apply filters, generate charts, write vlm_pending/
  5. save dedup state + periodic buffer state save + update heartbeat

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
from signal_generator.frozen_universe import (
    FROZEN_SIGNAL_UNIVERSE_ID,
    SignalUniverseDriftError,
    assert_frozen_runtime,
)
from draw_kline import render
from prompt_generator.builder import build_prompt
from live.data_sync import data_sync as _data_sync, SyncError as _SyncError
# NOTE: also expose has_4h_context and ready_horizon for cursor gating (G1)
from live.data_sync import ready_horizon, has_4h_context

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
SIGNAL_PENDING   = ROOT / "signal_pending"      # legacy (re-arch: write to VLM_PENDING)
SIGNAL_REJECTED  = ROOT / "signal_rejected"
VLM_PENDING      = Path(os.environ.get("VLM_PENDING", str(ROOT / "vlm_pending")))
VLM_REJECTED     = Path(os.environ.get("VLM_REJECTED", str(ROOT / "vlm_rejected")))
CHARTS_DIR       = ROOT / "live" / "charts"
LOCK_FILE        = ROOT / "live" / "monitor.lock"
STATUS_FILE      = ROOT / "live" / "heartbeat" / "monitor_status.json"
PRODUCER_SANDBOX = os.environ.get("PRODUCER_SANDBOX") == "1"

SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

POLL_INTERVAL        = 30    # seconds between new-bar checks
SIGNAL_TTL_BARS      = 2     # discard signal_pending entries older than this many bars (30 min)
BUFFER_SAVE_SECONDS  = 600   # F6: persist buffer state at most this often during operation
SIGNAL_MAX_AGE_MIN   = 240   # mirror of vlm_finalizer.SIGNAL_MAX_AGE_MIN — reaper threshold

# ── 所有 A 信号直接过 ChatGPT，不过滤结构宽度 ────────────────────────────────
# 之前有一版用 structure_space 做前置过滤，但 structure_space != playbook r_dist。
# 宽结构里可以有窄 r_dist 的 playbook，前置过滤会错误拦掉有效信号。
# 过滤只在 finalizer 里按实际 playbook activation/invalidation 计算 r_dist 执行。


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
    payload: dict = {"saved_at": pd.Timestamp.now("UTC").isoformat(), "dedup": state}
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
    HEARTBEAT_FILE.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")


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
        "updated_at":             pd.Timestamp.now("UTC").isoformat(),
    }, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_FILE)


def _build_per_symbol_status(cursors: "dict[str, pd.Timestamp | None]",
                             latest: "dict[str, pd.Timestamp | None]") -> dict:
    """Per-symbol {cursor, latest_bar, staleness_min} so a single-symbol data outage
    is visible to ops / watchdog even when the others keep flowing (F5)."""
    now = pd.Timestamp.now("UTC")
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


# ── VLM package writer (re-arch: output vlm_pending/) ─────────────────────────

def write_vlm_pending(sig: SignalEvent) -> "Path | None":
    """
    Write VLM signal package to vlm_pending/{sym}_{grade}_{ts}/.
    Returns the package directory path, or None if prompt/charts failed
    (incomplete packages are moved to vlm_rejected/ instead).
    """
    sym_slug  = sig.symbol.replace("/", "").lower()
    grade_str = sig.grade.replace("+", "plus")
    ts_str    = sig.bar_time.strftime("%Y%m%d_%H%M")
    pkg_name  = f"{sym_slug}_{grade_str}_{ts_str}"
    pkg_dir   = VLM_PENDING / pkg_name

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
        # Package is incomplete — move to vlm_rejected/ rather than touching .ready.
        # On name conflict, append a microsecond suffix so the source ALWAYS leaves
        # vlm_pending/ (no half-package residue).
        VLM_REJECTED.mkdir(parents=True, exist_ok=True)
        dest = VLM_REJECTED / pkg_name
        if dest.exists():
            dest = VLM_REJECTED / f"{pkg_name}__dup_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S_%f')}"
        try:
            pkg_dir.rename(dest)
        except Exception as mv_err:
            logger.warning("could not move rejected package %s: %s", pkg_name, mv_err)
        logger.warning("package rejected (prompt_ok=%s charts_ok=%s): %s",
                       prompt_ok, charts_ok, pkg_name)
        return None

    # .ready marker — only written when package is complete
    (pkg_dir / ".ready").touch()

    logger.info("vlm_pending written: %s", pkg_name)
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


def reap_stale_pending() -> int:
    """Archive vlm_pending packages whose bar_time exceeds the finalizer's max age.

    Such a package can never produce a valid signal_active (the finalizer rejects
    bar_time > SIGNAL_MAX_AGE_MIN as stale). But if it stays in vlm_pending,
    signal_sync re-pulls it every cycle and the worker re-rejects it forever — a
    stale_bar_time death loop. Reaping at the source (btc-ml, the producer that
    owns vlm_pending) is the only place that stops the re-supply. Returns count.
    """
    if not VLM_PENDING.exists():
        return 0
    now = pd.Timestamp.now("UTC")
    reaped = 0
    for d in list(VLM_PENDING.iterdir()):
        if not d.is_dir():
            continue
        sig_file = d / "signal.json"
        if not sig_file.exists():
            continue
        try:
            sig = json.loads(sig_file.read_text(encoding="utf-8"))
            bt = pd.Timestamp(sig["bar_time"])
            if bt.tzinfo is None:
                bt = bt.tz_localize("UTC")
        except Exception:
            continue   # unreadable signal.json — leave it for the finalizer to judge
        age_min = (now - bt).total_seconds() / 60
        if age_min <= SIGNAL_MAX_AGE_MIN:
            continue
        VLM_REJECTED.mkdir(parents=True, exist_ok=True)
        try:
            (d / "reject_reason.txt").write_text(
                f"reaped_stale age={age_min:.0f}min > {SIGNAL_MAX_AGE_MIN}min", encoding="utf-8")
        except Exception:
            pass
        dest = VLM_REJECTED / d.name
        if dest.exists():
            dest = VLM_REJECTED / f"{d.name}__dup_{now.strftime('%Y%m%d_%H%M%S_%f')}"
        try:
            d.rename(dest)
            logger.info("reaped stale vlm_pending: %s (age %.0fmin > %dmin)",
                        d.name, age_min, SIGNAL_MAX_AGE_MIN)
            reaped += 1
        except Exception as e:
            logger.warning("reap failed %s: %s", d.name, e)
    return reaped


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run_cycle(gen: SignalGenerator,
                    cursors: "dict[str, pd.Timestamp | None]",
                    ) -> tuple[list[SignalEvent], dict[str, pd.Timestamp | None],
                               dict[str, pd.Timestamp | None]]:
    """
    Run one monitor cycle (re-arch Phase 1b):
      1. data_sync  — pull new closed bars from local DB (no more SSH)
      2. gate each cursor to ready_horizon (min of ohlcv_15m, flow_15m)
      3. replay new bars (per-symbol, per-readiness) through gen
      4. return (detected signals, new cursors, latest-bar map)

    ready_horizon ensures a symbol only advances when BOTH ohlcv_15m and
    taker_flow_15m have closed to that bar (G1). 4h context is checked at
    package-production time in main() via has_4h_context.
    """
    # Step 1: pull delta from local DB — SyncError propagates to main for counter tracking
    _data_sync()

    latest = get_latest_bar_times()

    # Step 2: gate advancing cursor to ready_horizon (G1)
    advancing = {}
    for s in SYMBOLS:
        l = latest.get(s)
        if l is None:
            continue
        h = ready_horizon(s)
        candidate = min(l, h) if h else None
        if candidate is not None and (cursors.get(s) is None or candidate > cursors[s]):
            advancing[s] = candidate

    if not advancing:
        return [], cursors, latest

    detected: list[SignalEvent] = []

    @gen.on_signal
    async def _capture(evt: SignalEvent) -> None:
        detected.append(evt)

    # Step 3: replay EACH symbol independently, capped to its own ready_horizon.
    # A single global end_ts=max(advancing.values()) would push OHLCV beyond a
    # lagging symbol's ready_horizon, pre-filling its buffer before the flow/4h
    # context is ready — suppressing that bar's signal when flow finally catches up
    # (Codex P1, monitor.py:377 hole).
    try:
        for s, horizon in advancing.items():
            cur = cursors.get(s)
            if cur is None:
                start_str = None
            else:
                start_str = (cur + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
            end_str = horizon.strftime("%Y-%m-%d %H:%M:%S")

            mini_feed = ReplayFeed(
                data_dir=DATA_DIR,
                symbols=[s],
                start=start_str,
                end=end_str,
            )
            mini_feed.add_bar_handler(gen._on_bar)
            mini_feed.add_flow_handler(gen._on_flow)
            await mini_feed.start()
    finally:
        # Always unregister the temporary handler — if a per-symbol feed.start() raises
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

    # ── Sandbox / 生产路径保护（G2）── resolve BEFORE creating any directory ──
    if PRODUCER_SANDBOX:
        prod_vlm = (ROOT / "vlm_pending").resolve()
        prod_rej = (ROOT / "vlm_rejected").resolve()
        if VLM_PENDING.resolve() == prod_vlm or VLM_REJECTED.resolve() == prod_rej:
            logger.error("PRODUCER_SANDBOX=1 but path resolves to production directory: "
                         "VLM_PENDING=%s VLM_REJECTED=%s. Override via env or unset "
                         "PRODUCER_SANDBOX.", VLM_PENDING.resolve(), VLM_REJECTED.resolve())
            sys.exit(1)

    try:
        assert_frozen_runtime(symbols=SYMBOLS)
        logger.info("signal universe freeze verified: %s", FROZEN_SIGNAL_UNIVERSE_ID)
    except SignalUniverseDriftError as e:
        logger.error("FATAL: %s", e)
        try:
            _write_status(consecutive_failures=1, last_success_at=None,
                          last_error=str(e)[:200], backlog_count=-1,
                          package_write_failures=0)
        except Exception:
            pass
        sys.exit(1)

    VLM_PENDING.mkdir(parents=True, exist_ok=True)

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
        warmup_start = (pd.Timestamp.now("UTC") - pd.Timedelta(weeks=56)).strftime("%Y-%m-%d")
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
    last_buffer_save = pd.Timestamp.now("UTC")

    while True:
        try:
            signals, new_cursors, latest = await run_cycle(gen, cursors)
            consecutive_failures = 0
            last_error = None

            if new_cursors != cursors:
                cursors = new_cursors
                last_success_at = pd.Timestamp.now("UTC").isoformat()

                now = pd.Timestamp.now("UTC")
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
                    # 4h context gate (G1): refuse to produce a package if the 4h bar
                    # current at T0 hasn't been collected yet (e.g. first 4h of a new
                    # symbol hasn't closed, or flow data is missing for that era).
                    if not has_4h_context(sig.symbol, sig.bar_time):
                        logger.info("skipping %s %s @ %s: 4h context not ready",
                                    sig.symbol, sig.grade, sig.bar_time)
                        continue
                    # None = prompt/charts failed → package moved to vlm_rejected/.
                    # This is a functional failure (a real signal never reached openclaw),
                    # so surface it in status rather than dropping silently.
                    if write_vlm_pending(sig) is None:
                        package_write_failures += 1
                        last_error = f"package write failed: {sig.symbol} {sig.grade} {sig.bar_time}"

                save_dedup_state(gen, cursors=cursors)

                # F6: periodically persist buffer state so a restart replays only minutes
                # of bars, not days. Throttled to BUFFER_SAVE_SECONDS to bound parquet I/O.
                if (pd.Timestamp.now("UTC") - last_buffer_save).total_seconds() >= BUFFER_SAVE_SECONDS:
                    try:
                        gen.save_buffer_state(BUFFER_STATE_DIR)
                        last_buffer_save = pd.Timestamp.now("UTC")
                    except Exception as save_err:
                        logger.warning("buffer state save failed: %s", save_err)

            update_heartbeat()

        except _SyncError as e:
            consecutive_failures += 1
            last_error = str(e)[:200]
            logger.error("data_sync error (consecutive %d): %s", consecutive_failures, e)
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

        # Source-side cleanup: archive vlm_pending packages too old for the
        # finalizer to ever accept. Stops the signal_sync re-pull death loop.
        # Must never take down the loop — guard it.
        try:
            reap_stale_pending()
        except Exception:
            logger.exception("reap_stale_pending failed (non-fatal; loop continues)")

        try:
            backlog = sum(
                1 for d in VLM_PENDING.iterdir()
                if d.is_dir() and (d / ".ready").exists()
            ) if VLM_PENDING.exists() else 0
        except Exception:
            backlog = -1
        # Status reporting must NEVER take down the main loop: a bad timestamp / disk
        # error in _write_status or _build_per_symbol_status is non-fatal — log and go on.
        try:
            _write_status(consecutive_failures, last_success_at, last_error, backlog,
                          package_write_failures=package_write_failures,
                          per_symbol=_build_per_symbol_status(cursors, latest))
        except Exception:
            logger.exception("status write failed (non-fatal; loop continues)")

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
