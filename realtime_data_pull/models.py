from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class Bar:
    symbol: str        # "BTC/USDT"
    timeframe: str     # "15m" | "4h" | "1w"
    open_time: pd.Timestamp
    close_time: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool = True


@dataclass
class FlowBar:
    symbol: str
    timeframe: str     # always "15m"
    open_time: pd.Timestamp
    close_time: pd.Timestamp
    taker_buy_base_volume: float
    taker_sell_base_volume: float
    taker_buy_quote_volume: float
    taker_sell_quote_volume: float
    trade_count: int
    imbalance: float   # (buy_q - sell_q) / (buy_q + sell_q), range [-1, 1]


@dataclass
class FlowAccum:
    """Accumulator for building a 15m FlowBar from aggTrade events."""
    symbol: str
    open_time: pd.Timestamp
    close_time: pd.Timestamp
    taker_buy_base: float = 0.0
    taker_sell_base: float = 0.0
    taker_buy_quote: float = 0.0
    taker_sell_quote: float = 0.0
    trade_count: int = 0

    def add(self, qty: float, notional: float, is_buyer_maker: bool) -> None:
        if is_buyer_maker:  # aggressor = taker seller
            self.taker_sell_base  += qty
            self.taker_sell_quote += notional
        else:               # aggressor = taker buyer
            self.taker_buy_base  += qty
            self.taker_buy_quote += notional
        self.trade_count += 1

    def to_flow_bar(self) -> FlowBar:
        total = self.taker_buy_quote + self.taker_sell_quote
        imbalance = (self.taker_buy_quote - self.taker_sell_quote) / total if total > 0 else 0.0
        return FlowBar(
            symbol=self.symbol,
            timeframe="15m",
            open_time=self.open_time,
            close_time=self.close_time,
            taker_buy_base_volume=self.taker_buy_base,
            taker_sell_base_volume=self.taker_sell_base,
            taker_buy_quote_volume=self.taker_buy_quote,
            taker_sell_quote_volume=self.taker_sell_quote,
            trade_count=self.trade_count,
            imbalance=imbalance,
        )
