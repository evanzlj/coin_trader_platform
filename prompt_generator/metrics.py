"""
Compute per-signal numerical metrics from bar and flow data.

All metrics are computed from data that was available before T0 closed
(i.e., the signal bar itself is included as the last data point).
"""
from __future__ import annotations
import numpy as np
import pandas as pd


def wick_body_ratio(df15m: pd.DataFrame, t0: pd.Timestamp, structure_side: str) -> float:
    """
    Relevant wick / body of the T0 bar.

    near_support  → lower wick (shadow below body) / body
    near_resistance → upper wick (shadow above body) / body

    If body is zero (doji), returns 0.0.
    """
    row = df15m[df15m["close_time"] == t0]
    if row.empty and not df15m.empty:
        row = df15m.iloc[[-1]]
    if row.empty:
        return 0.0

    o = float(row["open"].iloc[0])
    h = float(row["high"].iloc[0])
    l = float(row["low"].iloc[0])
    c = float(row["close"].iloc[0])

    body = abs(c - o)
    if body < 1e-10:
        return 0.0

    if structure_side == "near_support":
        wick = min(o, c) - l   # lower shadow
    else:
        wick = h - max(o, c)   # upper shadow

    return max(wick, 0.0) / body


def flow_metrics(
    df_flow: pd.DataFrame,
    t0: pd.Timestamp,
    n_bars: int = 8,
) -> tuple[float, float]:
    """
    Compute avg_imbalance and cum_flow for the last n_bars ending at T0.

    Returns:
        avg_imbalance  — mean of `imbalance` column over the window
        cum_flow       — sum of (taker_buy_quote_volume - taker_sell_quote_volume)
                         over the window (in USDT quote currency)
    """
    if df_flow.empty:
        return 0.0, 0.0

    df = df_flow.sort_values("open_time").reset_index(drop=True)

    # find bars whose close_time <= t0
    mask = df["close_time"] <= t0 if "close_time" in df.columns else df["open_time"] <= t0
    window = df[mask].tail(n_bars)

    if window.empty:
        return 0.0, 0.0

    avg_imb = float(window["imbalance"].fillna(0.0).mean())
    net = (
        window["taker_buy_quote_volume"].fillna(0.0)
        - window["taker_sell_quote_volume"].fillna(0.0)
    )
    cum_flow = float(net.sum())

    return avg_imb, cum_flow
