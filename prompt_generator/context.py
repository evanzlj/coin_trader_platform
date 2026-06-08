"""
Compute daily regime context from 4H OHLCV data.

Daily bars are resampled from 4H (6 bars = 1 day) using only completed
daily candles available at T0.  The regime describes the macro backdrop
at signal time — it is context, not a trade direction.
"""
from __future__ import annotations
import numpy as np
import pandas as pd


def _ema(series: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1)
    out = np.empty_like(series, dtype=float)
    out[0] = series[0]
    for i in range(1, len(series)):
        out[i] = alpha * series[i] + (1 - alpha) * out[i - 1]
    return out


def compute_daily_regime(df4h: pd.DataFrame, t0: pd.Timestamp) -> dict:
    """
    Resample 4H data to daily bars using data strictly before t0's day
    (completed daily candles only), then compute:

        ema20_slope     "up" | "down"
        price_vs_ema20  "above" | "below"
        return_5d_pct   float  (5-day % return rounded to 4dp)
        structure       "higher_highs" | "lower_lows" | "ranging"
    """
    if df4h.empty:
        return _fallback()

    df = df4h.copy().sort_values("open_time").reset_index(drop=True)

    # keep only bars that closed before T0 (completed at signal time)
    df = df[df["close_time"] < t0]
    if df.empty:
        return _fallback()

    # resample to daily
    df = df.set_index("open_time")
    daily = df["close"].resample("1D").last().dropna()

    if len(daily) < 6:
        return _fallback()

    closes = daily.values.astype(float)
    ema20 = _ema(closes, 20)

    last_close = closes[-1]
    last_ema   = ema20[-1]

    # slope: direction of last EMA20 step
    ema_slope = "up" if ema20[-1] >= ema20[-2] else "down"
    price_vs  = "above" if last_close >= last_ema else "below"

    # 5-day return
    if len(closes) >= 6:
        ret5 = round((closes[-1] / closes[-6] - 1) * 100, 4)
    else:
        ret5 = 0.0

    # structure: compare last 5 daily highs/lows
    highs = df["high"].resample("1D").max().dropna().values.astype(float)
    lows  = df["low"].resample("1D").min().dropna().values.astype(float)

    structure = _classify_structure(highs, lows)

    return {
        "ema20_slope":    ema_slope,
        "price_vs_ema20": price_vs,
        "return_5d_pct":  ret5,
        "structure":      structure,
    }


def _classify_structure(highs: np.ndarray, lows: np.ndarray) -> str:
    """Compare last 5 swing highs and lows to classify market structure."""
    n = 5
    if len(highs) < n or len(lows) < n:
        return "ranging"

    h = highs[-n:]
    l = lows[-n:]

    hh = all(h[i] > h[i - 1] for i in range(1, n))
    hl = all(l[i] > l[i - 1] for i in range(1, n))
    lh = all(h[i] < h[i - 1] for i in range(1, n))
    ll = all(l[i] < l[i - 1] for i in range(1, n))

    if hh and hl:
        return "higher_highs"
    if lh and ll:
        return "lower_lows"
    if hh and ll:
        return "expanding"
    if lh and hl:
        return "contracting"
    return "ranging"


def _fallback() -> dict:
    return {
        "ema20_slope":    "unknown",
        "price_vs_ema20": "unknown",
        "return_5d_pct":  0.0,
        "structure":      "ranging",
    }
