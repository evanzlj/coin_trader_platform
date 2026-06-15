"""
DataFeed interface + two implementations:

  RealtimeFeed  — Binance Futures WebSocket with REST warmup
  ReplayFeed    — reads history_data_manager/data/ CSVs, emits in time order
"""

from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Awaitable, Callable, Optional

import pandas as pd

from .config import SYMBOL_DISPLAY, SYMBOL_SLUG, KLINE_TIMEFRAMES
from .models import Bar, FlowBar
from .bar_buffer import BarBuffer
from .binance_ws import BinanceFuturesWS

logger = logging.getLogger(__name__)

BarCallback  = Callable[[Bar],     Awaitable[None]]
FlowCallback = Callable[[FlowBar], Awaitable[None]]


# ── RealtimeFeed ─────────────────────────────────────────────────────────────

class RealtimeFeed:
    """
    Wraps BinanceFuturesWS. On start():
      1. Fetches warmup bars via REST and emits them as synthetic closed-bar events.
      2. Opens WebSocket and streams live updates.
    """

    def __init__(
        self,
        symbols: list[str],       # ["BTCUSDT", ...]
        timeframes: list[str] = KLINE_TIMEFRAMES,
        warmup: bool = True,
    ):
        self._bar_handlers:  list[BarCallback]  = []
        self._flow_handlers: list[FlowCallback] = []

        self._ws = BinanceFuturesWS(
            symbols    = symbols,
            timeframes = timeframes,
            on_bar     = self._emit_bar,
            on_flow    = self._emit_flow,
            warmup     = warmup,
        )
        self._warmup = warmup

    def add_bar_handler(self, h: BarCallback)   -> None: self._bar_handlers.append(h)
    def add_flow_handler(self, h: FlowCallback) -> None: self._flow_handlers.append(h)

    async def start(self) -> None:
        if self._warmup:
            warmup_bars = await self._ws.fetch_warmup()
            for bar in warmup_bars:
                await self._emit_bar(bar)
            logger.info("warmup complete (%d bars emitted)", len(warmup_bars))
        await self._ws.start()

    async def stop(self) -> None:
        await self._ws.stop()

    async def _emit_bar(self, bar: Bar) -> None:
        for h in self._bar_handlers:
            await h(bar)

    async def _emit_flow(self, flow: FlowBar) -> None:
        for h in self._flow_handlers:
            await h(flow)


# ── ReplayFeed ───────────────────────────────────────────────────────────────

class ReplayFeed:
    """
    Reads historical CSVs from history_data_manager/data/ and emits events in
    chronological order.  Speed=0 means as fast as possible (useful for backtesting).

    OHLCV:      data/ohlcv/{slug}_{tf}.csv          for tf in [15m, 4h]
    Weekly:     resampled from 4h data
    Taker flow: data/taker_flow/{slug}_15m.csv
    """

    def __init__(
        self,
        data_dir: Path,
        symbols: list[str],          # "BTC/USDT" display names
        start: Optional[str] = None, # ISO date, e.g. "2025-01-01"
        end:   Optional[str] = None, # ISO date, e.g. "2026-06-05"
        speed: float = 0.0,          # seconds between events (0 = instant)
    ):
        self.data_dir = Path(data_dir)
        self.symbols  = symbols
        self._start   = pd.Timestamp(start, tz="UTC") if start else None
        self._end     = pd.Timestamp(end,   tz="UTC") if end   else None
        self.speed    = speed

        self._bar_handlers:  list[BarCallback]  = []
        self._flow_handlers: list[FlowCallback] = []

    def add_bar_handler(self, h: BarCallback)   -> None: self._bar_handlers.append(h)
    def add_flow_handler(self, h: FlowCallback) -> None: self._flow_handlers.append(h)

    async def start(self) -> None:
        events: list[tuple[pd.Timestamp, object]] = []

        for symbol in self.symbols:
            slug = SYMBOL_SLUG.get(symbol)
            if slug is None:
                logger.warning("unknown symbol: %s", symbol)
                continue

            # ── OHLCV 15m (start-filtered: signals only fire on 15m) ─────────
            path_15m = self.data_dir / "ohlcv" / f"{slug}_15m.csv"
            if not path_15m.exists():
                logger.warning("missing: %s", path_15m)
            else:
                df = self._load_ohlcv(path_15m, symbol, "15m", apply_start=True)
                for _, row in df.iterrows():
                    events.append((row["close_time"], self._row_to_bar(row, symbol, "15m")))

            # ── OHLCV 4h (start-filtered during incremental replay, full during warmup)
            # During live monitor cycles the buffer already has all historical 4h bars,
            # so re-loading 6+ years every 15min wastes ~8s per symbol. Use a 30-day
            # lookback window when start is set (more than enough for 4h structure
            # calculations that need ≤ 20 bars = 80h). None → batch/warmup = full load.
            path_4h = self.data_dir / "ohlcv" / f"{slug}_4h.csv"
            if not path_4h.exists():
                logger.warning("missing: %s", path_4h)
            else:
                _4h_start = self._start - pd.Timedelta(days=30) \
                    if self._start is not None else None
                df4h = self._load_ohlcv(path_4h, symbol, "4h", apply_start=True,
                                        start_override=_4h_start)
                for _, row in df4h.iterrows():
                    events.append((row["close_time"], self._row_to_bar(row, symbol, "4h")))

                # ── Weekly: resample from filtered 4h (same window)
                weekly = self._resample_weekly(df4h, symbol)
                for _, row in weekly.iterrows():
                    events.append((row["close_time"], self._row_to_bar(row, symbol, "1w")))

            # ── Taker flow 15m (start-filtered) ─────────────────────────────
            path_flow = self.data_dir / "taker_flow" / f"{slug}_15m.csv"
            if path_flow.exists():
                df_flow = self._load_flow(path_flow, symbol)
                for _, row in df_flow.iterrows():
                    events.append((row["close_time"], self._row_to_flow(row, symbol)))

        # Sort all events chronologically by bar close time
        events.sort(key=lambda x: x[0])
        logger.info("replay: %d events for %s", len(events), self.symbols)

        for _ts, obj in events:
            if isinstance(obj, Bar):
                for h in self._bar_handlers:
                    await h(obj)
            elif isinstance(obj, FlowBar):
                for h in self._flow_handlers:
                    await h(obj)
            if self.speed > 0:
                await asyncio.sleep(self.speed)

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _load_ohlcv(self, path: Path, symbol: str, tf: str,
                    apply_start: bool = True,
                    start_override: "Optional[pd.Timestamp]" = None) -> pd.DataFrame:
        df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
        for col in ("open_time", "close_time"):
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")
            else:
                df[col] = df[col].dt.tz_convert("UTC")
        _start = start_override if start_override is not None else self._start
        if apply_start and _start is not None:
            df = df[df["open_time"] >= _start]
        if self._end is not None:
            df = df[df["open_time"] <= self._end]
        return df.reset_index(drop=True)

    def _load_flow(self, path: Path, symbol: str) -> pd.DataFrame:
        df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
        for col in ("open_time", "close_time"):
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")
            else:
                df[col] = df[col].dt.tz_convert("UTC")
        if self._start is not None:
            df = df[df["open_time"] >= self._start]
        if self._end is not None:
            df = df[df["open_time"] <= self._end]
        return df.reset_index(drop=True)

    def _resample_weekly(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Resample 4H bars to weekly (Binance week = Mon 00:00 UTC)."""
        df = df.set_index("open_time").sort_index()
        weekly = df.resample("W-MON", closed="left", label="left").agg(
            open   = ("open",   "first"),
            high   = ("high",   "max"),
            low    = ("low",    "min"),
            close  = ("close",  "last"),
            volume = ("volume", "sum"),
        ).dropna()
        weekly = weekly.reset_index().rename(columns={"open_time": "open_time"})
        weekly["close_time"] = weekly["open_time"] + pd.Timedelta(weeks=1)
        # Drop the current incomplete week (last row if end >= now)
        if len(weekly) > 0:
            weekly = weekly.iloc[:-1]
        return weekly

    @staticmethod
    def _row_to_bar(row: pd.Series, symbol: str, tf: str) -> Bar:
        return Bar(
            symbol     = symbol,
            timeframe  = tf,
            open_time  = row["open_time"],
            close_time = row["close_time"],
            open       = float(row["open"]),
            high       = float(row["high"]),
            low        = float(row["low"]),
            close      = float(row["close"]),
            volume     = float(row["volume"]),
            is_closed  = True,
        )

    @staticmethod
    def _row_to_flow(row: pd.Series, symbol: str) -> FlowBar:
        buy_q  = float(row.get("taker_buy_quote_volume",  0))
        sell_q = float(row.get("taker_sell_quote_volume", 0))
        total  = buy_q + sell_q
        return FlowBar(
            symbol                = symbol,
            timeframe             = "15m",
            open_time             = row["open_time"],
            close_time            = row["close_time"],
            taker_buy_base_volume = float(row.get("taker_buy_base_volume",  0)),
            taker_sell_base_volume= float(row.get("taker_sell_base_volume", 0)),
            taker_buy_quote_volume= buy_q,
            taker_sell_quote_volume=sell_q,
            trade_count           = int(row.get("trade_count", 0)),
            imbalance             = (buy_q - sell_q) / total if total > 0 else 0.0,
        )
