"""Indicator calculations for draw_kline charts (no lookahead)."""
from __future__ import annotations
import numpy as np
import pandas as pd


def rolling_ma(arr: np.ndarray, n: int) -> np.ndarray:
    out = np.full(len(arr), np.nan)
    cs = np.cumsum(arr, dtype=float)
    out[n - 1:] = (cs[n - 1:] - np.concatenate([[0], cs[:-n]])[: len(cs) - n + 1]) / n
    return out


def compute_qrc(
    highs:     np.ndarray,
    lows:      np.ndarray,
    closes:    np.ndarray,
    n:         int   = 192,
    upper_pct: float = 90.0,
    lower_pct: float = 10.0,
    use_wicks: bool  = True,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    QRC192 frozen at T0 — matches Pine Script MTF v3.4.

    Pine freezes the QRC when a signal fires: fit a linear regression on
    hlc3 over the last n bars, compute percentile offsets from high/low
    residuals, then draw three perfectly parallel straight lines from the
    window start to T0.

    Returns (upper, mid, lower) arrays of length len(closes).
    Only the last n bars are populated; earlier values are NaN.
    The three arrays are literally a straight line — perfectly parallel.
    """
    length = len(closes)
    upper  = np.full(length, np.nan)
    mid    = np.full(length, np.nan)
    lower  = np.full(length, np.nan)

    if length < n:
        return upper, mid, lower

    hlc3 = (highs + lows + closes) / 3.0

    # Window: last n bars (T0 is the last bar)
    h_win    = highs[-n:]
    l_win    = lows[-n:]
    hlc3_win = hlc3[-n:]

    # x: 0 = oldest bar in window, n-1 = T0
    x  = np.arange(n, dtype=float)
    xm = x.mean()
    x2 = float(((x - xm) ** 2).sum())
    ym = float(hlc3_win.mean())

    slope     = float(((x - xm) * (hlc3_win - ym)).sum()) / x2
    intercept = ym - slope * xm

    # Regression line values across the window (this IS a straight line)
    basis = slope * x + intercept

    # Residuals from high (upper) and low (lower)
    upper_residuals = h_win - basis if use_wicks else hlc3_win - basis
    lower_residuals = l_win - basis if use_wicks else hlc3_win - basis

    upper_offset = float(np.percentile(upper_residuals, upper_pct))
    lower_offset = float(np.percentile(lower_residuals, lower_pct))

    # Fill last n bars — three perfectly parallel straight lines
    start = length - n
    mid[start:]   = basis
    upper[start:] = basis + upper_offset
    lower[start:] = basis + lower_offset

    return upper, mid, lower


def compute_4h_structure_steps(
    df4h: pd.DataFrame,
    df15m: pd.DataFrame,
    left: int = 3,
    right: int = 3,
) -> tuple[np.ndarray, np.ndarray]:
    """
    4H swing high/low structure — matches Pine Script ta.pivothigh/pivotlow
    with pivotLen4H=3 (left=3, right=3).

    A swing high at bar i requires high[i] > all bars in [i-left, i-1] AND
    [i+1, i+right]. Confirmed at bar i+right (no lookahead before that).
    last4HResistance / last4HSupport hold the most recent confirmed pivot and
    stay flat until the next one → produces long flat steps like Pine visual.

    Returns (resist, supp) arrays aligned to df15m.
    """
    df4h  = df4h.sort_values("close_time").reset_index(drop=True)
    highs = df4h["high"].values
    lows  = df4h["low"].values
    n4h   = len(df4h)

    # Collect pivot confirmations: (confirmed_at_bar_index, level)
    resist_events: list[tuple[int, float]] = []
    supp_events:   list[tuple[int, float]] = []

    for i in range(left, n4h - right):
        if all(highs[i] > highs[i - j] for j in range(1, left + 1)) and \
           all(highs[i] > highs[i + j] for j in range(1, right + 1)):
            resist_events.append((i + right, float(highs[i])))

        if all(lows[i] < lows[i - j] for j in range(1, left + 1)) and \
           all(lows[i] < lows[i + j] for j in range(1, right + 1)):
            supp_events.append((i + right, float(lows[i])))

    # Build per-4H-bar arrays: hold last confirmed level (flat between pivots)
    resist_4h = np.full(n4h, np.nan)
    supp_4h   = np.full(n4h, np.nan)

    r_ptr, s_ptr = 0, 0
    cur_r = cur_s = np.nan
    for i in range(n4h):
        while r_ptr < len(resist_events) and resist_events[r_ptr][0] <= i:
            cur_r = resist_events[r_ptr][1]
            r_ptr += 1
        while s_ptr < len(supp_events) and supp_events[s_ptr][0] <= i:
            cur_s = supp_events[s_ptr][1]
            s_ptr += 1
        resist_4h[i] = cur_r
        supp_4h[i]   = cur_s

    # Map to 15m close_times via searchsorted
    h4_ct = df4h["close_time"].values.astype("datetime64[ns]")
    ct15  = df15m["close_time"].values.astype("datetime64[ns]")
    idxs  = np.searchsorted(h4_ct, ct15, side="right") - 1

    resist = np.where(idxs >= 0, resist_4h[np.clip(idxs, 0, n4h - 1)], np.nan)
    supp   = np.where(idxs >= 0, supp_4h[np.clip(idxs, 0, n4h - 1)],   np.nan)
    return resist, supp
