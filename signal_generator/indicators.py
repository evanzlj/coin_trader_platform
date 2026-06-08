"""
Stateless indicator functions operating on plain Python lists (most-recent last).
All return None when there is insufficient data.
"""
from __future__ import annotations
from typing import Optional


def sma(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def rma(values: list[float], period: int) -> Optional[float]:
    """Wilder's smoothed MA (Pine ta.rma) — used inside ATR."""
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    for v in values[period:]:
        result = (result * (period - 1) + v) / period
    return result


def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> Optional[float]:
    """ATR using Wilder's smoothing (matches Pine ta.atr)."""
    if len(closes) < 2:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i - 1]),
            abs(lows[i]   - closes[i - 1]),
        )
        trs.append(tr)
    return rma(trs, period)


def highest(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return max(values[-period:])


def lowest(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return min(values[-period:])


def crossover(series: list[float], ref: list[float]) -> bool:
    """series[-1] crossed above ref[-1] (was below ref[-2] last bar)."""
    if len(series) < 2 or len(ref) < 2:
        return False
    return series[-2] < ref[-2] and series[-1] >= ref[-1]


def crossunder(series: list[float], ref: list[float]) -> bool:
    """series[-1] crossed below ref[-1]."""
    if len(series) < 2 or len(ref) < 2:
        return False
    return series[-2] > ref[-2] and series[-1] <= ref[-1]


def pivot_structure(
    highs: list[float],
    lows:  list[float],
    left:  int = 3,
    right: int = 3,
    max_lookback: Optional[int] = None,
) -> tuple[Optional[float], Optional[float]]:
    """
    Return (pivot_resistance, pivot_support) using swing high/low detection.

    Bar i is a confirmed swing high if:
      highs[i] > highs[i-j] for j=1..left  AND  highs[i] > highs[i+j] for j=1..right
    Confirmation happens at bar i+right (same logic as Pine ta.pivothigh).

    max_lookback: if set, only search within the most recent max_lookback bars.

    Returns the most recent confirmed pivot high and pivot low.
    Returns None for either side if no pivot has been confirmed yet.
    """
    n = len(highs)
    min_bars = left + right + 1
    if n < min_bars:
        return None, None

    # Search backwards from the last confirmed bar (index n-1-right)
    last_confirmable = n - 1 - right
    # earliest bar index to search (inclusive)
    earliest = (n - max_lookback) if max_lookback is not None else 0
    earliest = max(earliest, left)

    ph: Optional[float] = None
    pl: Optional[float] = None

    for i in range(last_confirmable, earliest - 1, -1):
        if ph is None:
            if all(highs[i] > highs[i - j] for j in range(1, left + 1)) and \
               all(highs[i] > highs[i + j] for j in range(1, right + 1)):
                ph = highs[i]

        if pl is None:
            if all(lows[i] < lows[i - j] for j in range(1, left + 1)) and \
               all(lows[i] < lows[i + j] for j in range(1, right + 1)):
                pl = lows[i]

        if ph is not None and pl is not None:
            break

    return ph, pl


def rolling_sma_series(values: list[float], period: int) -> list[Optional[float]]:
    """Return SMA at every index (None where data insufficient) — used for crossover detection."""
    result: list[Optional[float]] = []
    for i in range(len(values)):
        if i + 1 < period:
            result.append(None)
        else:
            result.append(sum(values[i + 1 - period: i + 1]) / period)
    return result
