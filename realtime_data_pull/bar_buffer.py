from __future__ import annotations
from collections import deque
from typing import Optional
from .models import Bar


class BarBuffer:
    """Rolling buffer of closed bars for a single (symbol, timeframe)."""

    def __init__(self, maxlen: int = 600):
        self._bars: deque[Bar] = deque(maxlen=maxlen)

    def update(self, bar: Bar) -> bool:
        """Add a closed bar. Returns True only if a genuinely new bar was appended.

        Idempotent on open_time: a bar whose open_time is <= the current last bar is
        a duplicate (== last) or a late/out-of-order arrival (< last) and must NOT be
        re-appended. This lets callers safely re-feed overlapping replay windows
        (per-symbol cursors, incremental warmup) without duplicating bars or
        re-triggering signals — `_process_15m` only runs when this returns True.

        For ascending, unique input (the normal feed path) behaviour is unchanged.
        """
        if not bar.is_closed:
            return False
        if self._bars and bar.open_time <= self._bars[-1].open_time:
            if bar.open_time == self._bars[-1].open_time:
                # Same bar re-observed → refresh in place; not a new close.
                self._bars[-1] = bar
            # else: older bar arriving late → ignore (don't rewrite history).
            return False
        self._bars.append(bar)
        return True

    def seed(self, bars: list[Bar]) -> None:
        """Pre-fill with historical bars (must be sorted ascending)."""
        for b in bars:
            if b.is_closed:
                self._bars.append(b)

    # ── Field accessors ──────────────────────────────────────────────────────

    def _field(self, name: str, n: Optional[int] = None) -> list[float]:
        src = list(self._bars)
        vals = [getattr(b, name) for b in src]
        return vals[-n:] if n is not None else vals

    def closes(self, n: Optional[int] = None)  -> list[float]: return self._field("close",  n)
    def opens(self, n: Optional[int] = None)   -> list[float]: return self._field("open",   n)
    def highs(self, n: Optional[int] = None)   -> list[float]: return self._field("high",   n)
    def lows(self, n: Optional[int] = None)    -> list[float]: return self._field("low",    n)
    def volumes(self, n: Optional[int] = None) -> list[float]: return self._field("volume", n)

    @property
    def last(self) -> Optional[Bar]:
        return self._bars[-1] if self._bars else None

    def __len__(self) -> int:
        return len(self._bars)
