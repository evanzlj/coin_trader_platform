"""
Per-symbol state machine: Pending → Confirmed / Expired.
Direction-free: confirmation = price moves away from structure by ATR buffer.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import pandas as pd

from .events import SignalEvent, StateEvent
from .params import SignalParams


@dataclass
class _Pending:
    signal: SignalEvent
    bar_index: int
    signal_high: float
    signal_low: float
    atr: float


class SymbolStateMachine:
    def __init__(self, symbol: str, params: SignalParams) -> None:
        self.symbol = symbol
        self.params = params
        self._pending: Optional[_Pending] = None
        self._bar_count: int = 0

    @property
    def has_pending(self) -> bool:
        return self._pending is not None

    def on_signal(self, sig: SignalEvent, bar_high: float, bar_low: float, atr: float) -> None:
        self._pending = _Pending(
            signal=sig,
            bar_index=self._bar_count,
            signal_high=bar_high,
            signal_low=bar_low,
            atr=atr,
        )

    def check(self, bar_time: pd.Timestamp, close: float, params: SignalParams) -> Optional[StateEvent]:
        if self._pending is None:
            return None
        p    = self._pending
        bars = self._bar_count - p.bar_index
        if bars == 0:
            return None

        buf = p.atr * params.confirm_buffer_atr
        confirmed = (close >= p.signal_high + buf or close <= p.signal_low - buf)

        if confirmed:
            self._pending = None
            return StateEvent(symbol=self.symbol, state="confirmed",
                              original=p.signal, bar_time=bar_time)

        if bars > params.confirm_window_bars:
            self._pending = None
            return StateEvent(symbol=self.symbol, state="expired",
                              original=p.signal, bar_time=bar_time)

        return None

    def advance(self) -> None:
        self._bar_count += 1
