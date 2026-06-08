"""
A / A+ signal logic — direction-free radar.

Structure space (direction-free RR proxy):
    space = distance_to_far_side / (distance_to_near_side + atr_buffer)

    near support → far side = h4_resistance
    near resistance → far side = h4_support

    Interpretation: how many "near-side distances" fit between price and the
    opposite structure wall.  min_space=2.0 means the far wall is at least 2×
    as far as the near wall — the structure has meaningful room.

A+  extra conditions on top of A:
    - position in structure is extreme (≤10% or ≥90%)
    - strong energy: wick/body ratio > threshold, OR MA crossover, OR vol spike
"""
from __future__ import annotations
from typing import Optional
import pandas as pd

from .params import SignalParams
from .events import SignalEvent
from . import indicators as ind


def evaluate(
    open_: float,
    high: float,
    low: float,
    close: float,
    bar_time: pd.Timestamp,
    closes_15m:  list[float],
    highs_15m:   list[float],
    lows_15m:    list[float],
    opens_15m:   list[float],
    volumes_15m: list[float],
    highs_4h:    list[float],
    lows_4h:     list[float],
    closes_1w:   list[float],
    symbol: str,
    params: SignalParams,
) -> Optional[SignalEvent]:
    p = params

    # ── 15m indicators ──────────────────────────────────────────────────────
    if len(closes_15m) < max(p.ma_len, p.atr_len + 1, p.vol_len):
        return None

    ma15_series = ind.rolling_sma_series(closes_15m, p.ma_len)
    ma15_now  = ma15_series[-1]
    ma15_prev = ma15_series[-2] if len(ma15_series) >= 2 else None
    if ma15_now is None or ma15_prev is None:
        return None

    atr_val = ind.atr(highs_15m, lows_15m, closes_15m, p.atr_len)
    if atr_val is None:
        return None

    avg_vol = ind.sma(volumes_15m, p.vol_len)
    if avg_vol is None or avg_vol <= 0:
        return None
    vol_ratio = volumes_15m[-1] / avg_vol
    vol_ok    = vol_ratio > p.vol_mult

    # ── 4H structure (pivot-based, left=3 right=3, matches visual layer) ────────
    # Use full buffer — Pine's var last4HResistance/last4HSupport persist from
    # chart inception, so there is no lookback cap on the visual path.
    h4_resist, h4_supp = ind.pivot_structure(highs_4h, lows_4h, left=3, right=3)
    if h4_resist is None or h4_supp is None:
        return None

    # ── Near structure ────────────────────────────────────────────────────────
    near_support    = abs(close - h4_supp)   / close * 100 <= p.zone_pct
    near_resistance = abs(close - h4_resist) / close * 100 <= p.zone_pct and not near_support
    if not near_support and not near_resistance:
        return None

    # ── Structure space (direction-free, replaces RR filter) ──────────────────
    # near_side_dist: how far price is from the level it's touching
    # far_side_dist:  how far to the opposite structure wall
    # atr_buf added to near_side_dist mirrors the SL buffer in the original RR
    atr_buf = atr_val * p.sl_atr_mult
    if near_support:
        near_side_dist = abs(close - h4_supp)
        far_side_dist  = h4_resist - close
    else:
        near_side_dist = abs(close - h4_resist)
        far_side_dist  = close - h4_supp

    denom = near_side_dist + atr_buf
    structure_space = far_side_dist / denom if denom > 0 else 0.0
    if structure_space < p.min_space:
        return None

    # ── Position in structure ─────────────────────────────────────────────────
    struct_width = h4_resist - h4_supp
    pos_in_struct: Optional[float] = (
        (close - h4_supp) / struct_width if struct_width > 0 else None
    )

    # ── Weekly context (informational) ────────────────────────────────────────
    weekly_trend = "neutral"
    if len(closes_1w) >= p.weekly_slow_len:
        w_close = closes_1w[-1]
        w_ma20  = ind.sma(closes_1w, p.weekly_fast_len)
        w_ma50  = ind.sma(closes_1w, p.weekly_slow_len)
        if w_ma20 is not None and w_ma50 is not None:
            if w_close > w_ma20 and w_ma20 > w_ma50:
                weekly_trend = "bull"
            elif w_close < w_ma20 and w_ma20 < w_ma50:
                weekly_trend = "bear"

    # ── MA reclaim + reversal ─────────────────────────────────────────────────
    close_prev = closes_15m[-2] if len(closes_15m) >= 2 else None

    reclaim_cross = (
        close_prev is not None and (
            (close_prev < ma15_prev and close >= ma15_now) or  # crossover
            (close_prev > ma15_prev and close <= ma15_now)     # crossunder
        )
    )
    reclaim = (
        reclaim_cross
        or (close > ma15_now and close > open_)
        or (close < ma15_now and close < open_)
    )
    reversal = (
        close_prev is not None and
        abs(close - close_prev) / close_prev * 100 >= p.reversal_pct
    )

    if not (reclaim and reversal and vol_ok):
        return None

    # ── A+ upgrade conditions ─────────────────────────────────────────────────
    body       = abs(close - open_)
    safe_body  = body if body > 1e-10 else 1e-10
    upper_wick = high  - max(open_, close)
    lower_wick = min(open_, close) - low

    extreme = (
        pos_in_struct is not None and (
            pos_in_struct <= p.extreme_pos_long or
            pos_in_struct >= p.extreme_pos_short
        )
    )
    strong_energy = (
        lower_wick / safe_body > p.wick_body_threshold or
        upper_wick / safe_body > p.wick_body_threshold or
        reclaim_cross or
        vol_ratio > p.vol_ratio_threshold
    )

    grade = "A+" if (extreme and strong_energy) else "A"

    return SignalEvent(
        symbol                = symbol,
        grade                 = grade,
        bar_time              = bar_time,
        close                 = close,
        structure_side        = "near_support" if near_support else "near_resistance",
        structure_space       = round(structure_space, 2),
        position_in_structure = round(pos_in_struct, 3) if pos_in_struct is not None else 0.0,
        vol_ratio             = round(vol_ratio, 2),
        weekly_trend          = weekly_trend,
        h4_support            = h4_supp,
        h4_resistance         = h4_resist,
    )
