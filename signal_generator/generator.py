"""
SignalGenerator — wires a DataFeed to the A/A+ signal logic and state machine.

Usage (replay):
    from pathlib import Path
    from realtime_data_pull import ReplayFeed
    from signal_generator import SignalGenerator

    feed = ReplayFeed(
        data_dir = Path("../history_data_manager/data"),
        symbols  = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"],
        start    = "2025-01-01",
        end      = "2026-06-05",
    )
    gen = SignalGenerator(feed=feed, symbols=["BTC/USDT", ...])

    @gen.on_signal
    async def on_signal(evt):
        print(evt)

    @gen.on_state
    async def on_state(evt):
        print(evt)

    await feed.start()   # drives everything

Usage (real-time):
    from realtime_data_pull import RealtimeFeed
    feed = RealtimeFeed(symbols=["BTCUSDT", ...])
    gen  = SignalGenerator(feed=feed, symbols=["BTC/USDT", ...])
    ...
    await feed.start()
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Awaitable, Callable, Optional

import pandas as pd

from .params import SignalParams, SYMBOL_PARAMS
from .events import SignalEvent, StateEvent
from .signal_logic import evaluate
from .state_machine import SymbolStateMachine
from . import indicators as ind

from realtime_data_pull.models import Bar, FlowBar
from realtime_data_pull.bar_buffer import BarBuffer

logger = logging.getLogger(__name__)

SignalHandler = Callable[[SignalEvent], Awaitable[None]]
StateHandler  = Callable[[StateEvent],  Awaitable[None]]


class SignalGenerator:
    """
    Subscribes to a DataFeed, maintains multi-TF bar buffers per symbol,
    and evaluates A/A+ signals on every closed 15m bar.

    params may be a single SignalParams (applied to all symbols) or a
    dict mapping symbol → SignalParams for per-symbol configuration.
    """

    def __init__(
        self,
        feed,                          # RealtimeFeed | ReplayFeed | None
        symbols: list[str],            # "BTC/USDT" display names
        params: "Optional[SignalParams | dict[str, SignalParams]]" = None,
    ) -> None:
        self.symbols = symbols

        # Normalise to per-symbol dict
        # Default: use SYMBOL_PARAMS (BTC/BNB→standard, ETH/SOL→conservative)
        if params is None:
            self._sym_params: dict[str, SignalParams] = {
                s: SYMBOL_PARAMS.get(s, SignalParams.standard()) for s in symbols
            }
        elif isinstance(params, dict):
            self._sym_params = {
                s: params.get(s, SYMBOL_PARAMS.get(s, SignalParams.standard())) for s in symbols
            }
        else:
            self._sym_params = {s: params for s in symbols}

        # Keep a single .params for backwards-compat (first symbol's params)
        self.params = next(iter(self._sym_params.values()))

        self._signal_handlers: list[SignalHandler] = []
        self._state_handlers:  list[StateHandler]  = []

        # Bar buffers per (symbol, timeframe)
        self._bufs: dict[tuple[str, str], BarBuffer] = {
            (sym, tf): BarBuffer()
            for sym in symbols
            for tf in ("15m", "4h", "1w")
        }

        # State machine per symbol (each uses its own params)
        self._sm: dict[str, SymbolStateMachine] = {
            sym: SymbolStateMachine(sym, self._sym_params[sym])
            for sym in symbols
        }

        # Dedup: tracks bar open_time when last signal fired per symbol (timestamp-based,
        # survives restarts; None = never fired)
        self._last_signal_time: dict[str, Optional[pd.Timestamp]] = {
            sym: None for sym in symbols
        }

        # Register with feed (None when loading from persisted buffer state)
        if feed is not None:
            feed.add_bar_handler(self._on_bar)
            feed.add_flow_handler(self._on_flow)

    # ── Handler registration ─────────────────────────────────────────────────

    def on_signal(self, fn: SignalHandler) -> SignalHandler:
        """Decorator / direct call to register a signal handler."""
        self._signal_handlers.append(fn)
        return fn

    def on_state(self, fn: StateHandler) -> StateHandler:
        """Decorator / direct call to register a state-change handler."""
        self._state_handlers.append(fn)
        return fn

    # ── Internal: bar ingestion ───────────────────────────────────────────────

    async def _on_bar(self, bar: Bar) -> None:
        if bar.symbol not in self.symbols:
            return
        key = (bar.symbol, bar.timeframe)
        if key not in self._bufs:
            return

        newly_closed = self._bufs[key].update(bar)

        if bar.timeframe == "15m" and newly_closed:
            await self._process_15m(bar.symbol, bar)

    async def _on_flow(self, flow: FlowBar) -> None:
        # Flow data stored for context; not used in core A/A+ logic
        pass

    # ── Dedup state persistence ───────────────────────────────────────────────

    def get_dedup_state(self) -> dict:
        """Serialize dedup state to a JSON-safe dict (timestamps as ISO strings)."""
        return {
            sym: t.isoformat() if t is not None else None
            for sym, t in self._last_signal_time.items()
        }

    def load_dedup_state(self, state: dict) -> None:
        """Load dedup state from a previously saved dict."""
        for sym, val in state.items():
            if sym in self._last_signal_time:
                self._last_signal_time[sym] = (
                    pd.Timestamp(val, tz="UTC") if val is not None else None
                )

    # ── Buffer state persistence ──────────────────────────────────────────────

    def save_buffer_state(self, state_dir: Path) -> None:
        """
        Serialize all BarBuffers to parquet files in state_dir.
        One file per (symbol, timeframe): buf_{slug}_{tf}.parquet
        Also saves state machine bar counts and a saved_at timestamp.

        Every file is written atomically (tmp + os.replace): monitor calls this
        periodically while running, and a crash mid-write must never leave a
        half-written parquet/json that the next startup's load_buffer_state chokes on.
        saved_at.txt is written *last* so it only appears once all buffers are durable.
        """
        state_dir.mkdir(parents=True, exist_ok=True)

        def _atomic(path: Path, write_fn) -> None:
            tmp = path.with_suffix(path.suffix + ".tmp")
            write_fn(tmp)
            os.replace(tmp, path)

        for (sym, tf), buf in self._bufs.items():
            bars = list(buf._bars)
            if not bars:
                continue
            slug = sym.replace("/", "").lower()
            rows = [{
                "symbol":    b.symbol,
                "timeframe": b.timeframe,
                "open_time": b.open_time,
                "close_time": b.close_time,
                "open":      b.open,
                "high":      b.high,
                "low":       b.low,
                "close":     b.close,
                "volume":    b.volume,
                "is_closed": b.is_closed,
            } for b in bars]
            df = pd.DataFrame(rows)
            df["open_time"]  = pd.to_datetime(df["open_time"],  utc=True)
            df["close_time"] = pd.to_datetime(df["close_time"], utc=True)
            _atomic(state_dir / f"buf_{slug}_{tf}.parquet",
                    lambda p, _df=df: _df.to_parquet(p, index=False))

        sm_counts = {sym: self._sm[sym]._bar_count for sym in self.symbols}
        _atomic(state_dir / "sm_bar_counts.json",
                lambda p: p.write_text(json.dumps(sm_counts), encoding="utf-8"))

        _atomic(state_dir / "saved_at.txt",
                lambda p: p.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8"))
        logger.info("buffer state saved → %s (%d symbol×tf buffers)", state_dir,
                    sum(1 for buf in self._bufs.values() if buf._bars))

    def load_buffer_state(self, state_dir: Path) -> "Optional[pd.Timestamp]":
        """
        Load BarBuffers from parquet files in state_dir via BarBuffer.seed().
        Returns the saved_at timestamp if successful, else None.

        Corruption-tolerant: a bad parquet / json / saved_at (e.g. a crash that
        slipped past the atomic write, or a truncated file) must NOT take monitor
        down — it logs a warning and returns None so the caller falls back to a full
        warmup instead of starting with partial/garbage buffers.
        """
        if not state_dir.exists():
            return None

        try:
            loaded = 0
            for (sym, tf), buf in self._bufs.items():
                slug = sym.replace("/", "").lower()
                path = state_dir / f"buf_{slug}_{tf}.parquet"
                if not path.exists():
                    continue
                df = pd.read_parquet(path)
                bars = [
                    Bar(
                        symbol    = row.symbol,
                        timeframe = row.timeframe,
                        open_time = pd.Timestamp(row.open_time).tz_convert("UTC"),
                        close_time= pd.Timestamp(row.close_time).tz_convert("UTC"),
                        open      = float(row.open),
                        high      = float(row.high),
                        low       = float(row.low),
                        close     = float(row.close),
                        volume    = float(row.volume),
                        is_closed = bool(row.is_closed),
                    )
                    for row in df.itertuples(index=False)
                ]
                buf.seed(bars)
                loaded += 1

            sm_path = state_dir / "sm_bar_counts.json"
            if sm_path.exists():
                counts = json.loads(sm_path.read_text(encoding="utf-8"))
                for sym, count in counts.items():
                    if sym in self._sm:
                        self._sm[sym]._bar_count = int(count)

            sat_path = state_dir / "saved_at.txt"
            if loaded == 0 or not sat_path.exists():
                return None

            saved_at = pd.Timestamp(sat_path.read_text(encoding="utf-8").strip(), tz="UTC")
        except Exception as e:
            # Reset any partially-seeded buffers so we don't run on a half-loaded state.
            for buf in self._bufs.values():
                buf._bars.clear()
            logger.warning("buffer state load failed (%s) — falling back to full warmup", e)
            return None

        logger.info("buffer state loaded from %s (saved_at=%s)", state_dir, saved_at)
        return saved_at

    # ── Core: evaluate signal on closed 15m bar ───────────────────────────────

    async def _process_15m(self, symbol: str, bar: Bar) -> None:
        sm    = self._sm[symbol]
        p     = self._sym_params[symbol]
        buf15 = self._bufs[(symbol, "15m")]
        buf4h = self._bufs[(symbol, "4h")]
        buf1w = self._bufs[(symbol, "1w")]

        sm.advance()

        atr_val = ind.atr(buf15.highs(), buf15.lows(), buf15.closes(), p.atr_len)

        # ── Check pending state ──────────────────────────────────────────────
        if sm.has_pending:
            state_evt = sm.check(
                bar_time = bar.open_time,
                close    = bar.close,
                params   = p,
            )
            if state_evt is not None:
                logger.debug("%s", state_evt)
                for h in self._state_handlers:
                    await h(state_evt)

        # ── Evaluate new signal ───────────────────────────────────────────────
        last_t = self._last_signal_time[symbol]
        dedup_window = pd.Timedelta(minutes=15 * p.window_dedup)
        in_dedup_window = (
            last_t is not None and (bar.open_time - last_t) < dedup_window
        )

        if not in_dedup_window:
            sig = evaluate(
                open_       = bar.open,
                high        = bar.high,
                low         = bar.low,
                close       = bar.close,
                bar_time    = bar.open_time,
                closes_15m  = buf15.closes(),
                highs_15m   = buf15.highs(),
                lows_15m    = buf15.lows(),
                opens_15m   = buf15.opens(),
                volumes_15m = buf15.volumes(),
                highs_4h    = buf4h.highs(),
                lows_4h     = buf4h.lows(),
                closes_1w   = buf1w.closes(),
                symbol      = symbol,
                params      = p,
            )
            if sig is not None:
                self._last_signal_time[symbol] = bar.open_time
                sm.on_signal(sig, bar_high=bar.high, bar_low=bar.low, atr=atr_val or 0.0)
                logger.info("%s", sig)
                for h in self._signal_handlers:
                    await h(sig)
