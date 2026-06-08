"""
Binance Futures WebSocket client.

Subscribes to:
  {symbol}@kline_{tf}   for tf in ["15m", "4h", "1w"]
  {symbol}@aggTrade

kline events → Bar callbacks (live + closed)
aggTrade events → accumulated into 15m FlowBar, flushed on bar boundary → FlowBar callbacks
"""

from __future__ import annotations
import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional

import aiohttp
import pandas as pd

try:
    import websockets
    _HAS_WEBSOCKETS = True
except ImportError:
    _HAS_WEBSOCKETS = False

from .config import BINANCE_FUTURES_WS, BINANCE_FUTURES_REST, SYMBOL_DISPLAY, WARMUP_BARS
from .models import Bar, FlowBar, FlowAccum

logger = logging.getLogger(__name__)

BarCallback  = Callable[[Bar],     Awaitable[None]]
FlowCallback = Callable[[FlowBar], Awaitable[None]]


class BinanceFuturesWS:
    """Async WebSocket client for Binance Futures kline + aggTrade streams."""

    def __init__(
        self,
        symbols: list[str],           # ["BTCUSDT", ...]
        timeframes: list[str],         # ["15m", "4h", "1w"]
        on_bar:  BarCallback,
        on_flow: FlowCallback,
        warmup: bool = True,
    ):
        self.symbols    = symbols
        self.timeframes = timeframes
        self.on_bar     = on_bar
        self.on_flow    = on_flow
        self.warmup     = warmup
        self._running   = False
        self._flow_accum: dict[tuple[str, pd.Timestamp], FlowAccum] = {}

    # ── Stream URL ───────────────────────────────────────────────────────────

    def _stream_url(self) -> str:
        parts = []
        for sym in self.symbols:
            s = sym.lower()
            for tf in self.timeframes:
                parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@aggTrade")
        return f"{BINANCE_FUTURES_WS}?streams={'/'.join(parts)}"

    # ── Warmup (REST) ────────────────────────────────────────────────────────

    async def fetch_warmup(self) -> list[Bar]:
        """Fetch historical closed bars via REST for buffer seeding."""
        bars: list[Bar] = []
        async with aiohttp.ClientSession() as session:
            for sym in self.symbols:
                for tf in self.timeframes:
                    limit = WARMUP_BARS.get(tf, 100)
                    url   = f"{BINANCE_FUTURES_REST}/fapi/v1/klines"
                    params = {"symbol": sym, "interval": tf, "limit": limit + 1}
                    try:
                        async with session.get(url, params=params) as resp:
                            data = await resp.json()
                        for k in data[:-1]:  # exclude current unclosed bar
                            bars.append(self._parse_rest_kline(sym, tf, k))
                    except Exception as e:
                        logger.warning("warmup failed %s %s: %s", sym, tf, e)
        logger.info("warmup fetched %d bars", len(bars))
        return bars

    def _parse_rest_kline(self, sym: str, tf: str, k: list) -> Bar:
        return Bar(
            symbol    = SYMBOL_DISPLAY[sym],
            timeframe = tf,
            open_time = pd.Timestamp(k[0], unit="ms", tz="UTC"),
            close_time= pd.Timestamp(k[6], unit="ms", tz="UTC"),
            open      = float(k[1]),
            high      = float(k[2]),
            low       = float(k[3]),
            close     = float(k[4]),
            volume    = float(k[5]),
            is_closed = True,
        )

    # ── Main loop ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if not _HAS_WEBSOCKETS:
            raise ImportError("pip install websockets")
        self._running = True
        url = self._stream_url()
        logger.info("connecting: %s", url[:120])

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("WebSocket connected")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            await self._dispatch(json.loads(raw))
                        except Exception as e:
                            logger.error("dispatch error: %s", e)
            except Exception as e:
                if self._running:
                    logger.warning("WebSocket error: %s — reconnecting in 5s", e)
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False

    # ── Message dispatch ─────────────────────────────────────────────────────

    async def _dispatch(self, msg: dict) -> None:
        stream  = msg.get("stream", "")
        payload = msg.get("data", msg)
        if "@kline_" in stream:
            await self._handle_kline(payload)
        elif "@aggTrade" in stream:
            await self._handle_agg_trade(payload)

    # ── Kline handler ────────────────────────────────────────────────────────

    async def _handle_kline(self, payload: dict) -> None:
        k   = payload["k"]
        sym = SYMBOL_DISPLAY.get(k["s"])
        if sym is None:
            return
        bar = Bar(
            symbol    = sym,
            timeframe = k["i"],
            open_time = pd.Timestamp(k["t"], unit="ms", tz="UTC"),
            close_time= pd.Timestamp(k["T"], unit="ms", tz="UTC"),
            open      = float(k["o"]),
            high      = float(k["h"]),
            low       = float(k["l"]),
            close     = float(k["c"]),
            volume    = float(k["v"]),
            is_closed = k["x"],
        )
        await self.on_bar(bar)

    # ── aggTrade handler → 15m FlowBar ───────────────────────────────────────

    async def _handle_agg_trade(self, payload: dict) -> None:
        sym = SYMBOL_DISPLAY.get(payload["s"])
        if sym is None:
            return

        price    = float(payload["p"])
        qty      = float(payload["q"])
        notional = price * qty
        ts       = pd.Timestamp(payload["T"], unit="ms", tz="UTC")
        is_buyer_maker: bool = payload["m"]

        bar_open  = ts.floor("15min")
        bar_close = bar_open + pd.Timedelta("15min")
        key       = (sym, bar_open)

        # Flush completed bars for this symbol
        stale = [k for k in self._flow_accum if k[0] == sym and k[1] < bar_open]
        for sk in stale:
            acc = self._flow_accum.pop(sk)
            await self.on_flow(acc.to_flow_bar())

        if key not in self._flow_accum:
            self._flow_accum[key] = FlowAccum(symbol=sym, open_time=bar_open, close_time=bar_close)

        self._flow_accum[key].add(qty, notional, is_buyer_maker)
