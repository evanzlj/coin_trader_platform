"""
Post-T0 flow direction classifier.

Computes cumulative taker-flow imbalance over the first N 15m bars after T0,
then classifies into buying / neutral / selling using p33/p67 thresholds
derived from the full set of signal windows (no data snooping on individual signals).

If local CSV is missing post-T0 rows (signal too recent), returns (nan, 0).
Caller should fetch from btc-ml and re-run if coverage is insufficient.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from functools import lru_cache

FLOW_WINDOW_BARS = 8  # 2h of 15m bars
MIN_BARS = 4          # require at least this many bars, else mark unknown


@lru_cache(maxsize=8)
def _load_flow(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["open_time"])
    if df["open_time"].dt.tz is None:
        df["open_time"] = df["open_time"].dt.tz_localize("UTC")
    else:
        df["open_time"] = df["open_time"].dt.tz_convert("UTC")
    return df.sort_values("open_time").reset_index(drop=True)


def get_post_t0_cum_imbalance(
    symbol: str,
    bar_time: pd.Timestamp,
    data_dir: Path,
    window_bars: int = FLOW_WINDOW_BARS,
) -> tuple[float, int]:
    """
    Returns (cumulative_imbalance, n_bars_available).

    cumulative_imbalance = sum of per-bar imbalance over first window_bars bars after T0.
    Returns (nan, 0) if not enough local data — fetch from btc-ml to fill.
    """
    slug = symbol.replace("/", "").lower()
    csv_path = str(data_dir / "taker_flow" / f"{slug}_15m.csv")
    df = _load_flow(csv_path)

    post = df[df["open_time"] > bar_time].head(window_bars)
    n = len(post)
    if n < MIN_BARS:
        return float("nan"), n
    return float(post["imbalance"].sum()), n


def compute_thresholds(
    cum_values: list[float],
    low_pct: float = 33.0,
    high_pct: float = 67.0,
) -> tuple[float, float]:
    """
    Compute p33/p67 of cumulative imbalances across all signals.
    Values below p33 → selling, above p67 → buying, middle → neutral.
    """
    arr = np.array([v for v in cum_values if not np.isnan(v)])
    if len(arr) == 0:
        return -0.5, 0.5
    return float(np.percentile(arr, low_pct)), float(np.percentile(arr, high_pct))


def classify_flow(
    cum_imbalance: float,
    low_thresh: float,
    high_thresh: float,
) -> str:
    if np.isnan(cum_imbalance):
        return "unknown"
    if cum_imbalance >= high_thresh:
        return "buying"
    if cum_imbalance <= low_thresh:
        return "selling"
    return "neutral"
