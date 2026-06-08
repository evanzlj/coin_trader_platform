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
import logging
from typing import Awaitable, Callable, Optional

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
        feed,                          # RealtimeFeed | ReplayFeed
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

        # Dedup: tracks bar_count when last signal fired per symbol
        self._last_signal_bar: dict[str, int] = {sym: -9999 for sym in symbols}

        # Register with feed
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
        bars_since_last = sm._bar_count - self._last_signal_bar[symbol]
        in_dedup_window = bars_since_last < p.window_dedup

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
                self._last_signal_bar[symbol] = sm._bar_count
                sm.on_signal(sig, bar_high=bar.high, bar_low=bar.low, atr=atr_val or 0.0)
                logger.info("%s", sig)
                for h in self._signal_handlers:
                    await h(sig)
