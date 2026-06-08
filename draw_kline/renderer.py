"""
Main entry point for draw_kline.

Usage:
    from draw_kline import render
    path_4h, path_15m = render(signal_event, data_dir=Path("..."), out_dir=Path("/tmp/charts"))
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

from .common import SLUG_MAP, load_ohlcv, load_flow
from .chart_4h  import draw_4h_context
from .chart_15m import draw_15m_detail

# Extra 4H bars loaded before the 15m window to warm up the structure lookback
_STRUCT_LOOKBACK = 20
_STRUCT_WARMUP_H = _STRUCT_LOOKBACK * 4 + 8   # 88h buffer


def render(
    signal,
    data_dir: Path,
    out_dir:  Path,
    lookback_4h_days:  int = 90,
    lookback_15m_days: int = 14,
) -> tuple[Path, Path]:
    """
    Draw 4H context chart + 15m detail chart for one SignalEvent.
    Returns (path_4h, path_15m).
    """
    slug = SLUG_MAP.get(signal.symbol)
    if slug is None:
        raise ValueError(f"Unknown symbol: {signal.symbol}")

    t0    = signal.bar_time
    grade = signal.grade.replace("+", "plus")
    ts    = t0.strftime("%Y%m%d_%H%M")
    stem  = slug.replace("usdt", "")

    out_dir.mkdir(parents=True, exist_ok=True)
    path_4h  = out_dir / f"{stem}_{grade}_{ts}_4h.png"
    path_15m = out_dir / f"{stem}_{grade}_{ts}_15m.png"

    # ── Time windows ─────────────────────────────────────────────────────────
    t_3m  = t0 - pd.Timedelta(days=lookback_4h_days)
    t_14d = t0 - pd.Timedelta(days=lookback_15m_days)

    # ── Load ─────────────────────────────────────────────────────────────────
    # df4h covers 90 days — used for both the 4H overview chart AND as the
    # structure pivot source for the 15m chart (matches Pine's var-persistent
    # last4HResistance/last4HSupport which reference the most recent pivot
    # regardless of age).
    df4h  = load_ohlcv(data_dir, slug, "4h",  t_3m,  t0)
    df15m = load_ohlcv(data_dir, slug, "15m", t_14d, t0)
    df_flow = load_flow(data_dir, slug,        t_14d, t0)

    # ── Draw ─────────────────────────────────────────────────────────────────
    draw_4h_context(df4h, signal, path_4h)
    draw_15m_detail(df15m, df4h, df_flow, signal, path_15m)

    return path_4h, path_15m
