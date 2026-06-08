"""4H context chart: 3 months before T0, volume subplot, anonymized x-axis."""
from __future__ import annotations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

from .common import BG, PANEL_BG, BULL, BEAR, TEXT, GRID, T0_COL


def _style(ax):
    ax.set_facecolor(PANEL_BG)
    ax.tick_params(colors=TEXT, labelsize=8)
    ax.grid(axis="y", color=GRID, lw=0.35, alpha=0.7)
    for sp in ax.spines.values():
        sp.set_color(GRID)


def _candles(ax, o, h, l, c, xs):
    bull = c >= o
    bh = np.abs(c - o)
    bb = np.minimum(o, c)
    bh = np.where(bh == 0, (h - l) * 0.05, bh)
    ax.vlines(xs[bull],  l[bull],  h[bull],  colors=BULL, lw=0.7)
    ax.vlines(xs[~bull], l[~bull], h[~bull], colors=BEAR, lw=0.7)
    ax.bar(xs[bull],  bh[bull],  bottom=bb[bull],  color=BULL, width=0.75, linewidth=0)
    ax.bar(xs[~bull], bh[~bull], bottom=bb[~bull], color=BEAR, width=0.75, linewidth=0)


def draw_4h_context(df4h: pd.DataFrame, signal, out_path: Path) -> None:
    """
    4H context chart.
    - 3 months of closed 4H bars ending at T0
    - X-axis: relative days to T0 (no dates)
    - Subplot: 4H volume, bull/bear coloured
    """
    if df4h.empty:
        return

    t0 = signal.bar_time
    df = df4h.copy().sort_values("close_time").reset_index(drop=True)
    n  = len(df)
    xs = np.arange(n)

    rel_days = ((df["close_time"] - t0).dt.total_seconds() / 86400.0).values
    o = df["open"].values
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    v = df["volume"].values

    fig = plt.figure(figsize=(20, 7), facecolor=BG)
    gs  = gridspec.GridSpec(
        2, 1, height_ratios=[4, 1], hspace=0.06,
        top=0.92, bottom=0.09, left=0.055, right=0.97,
    )
    ax_main = fig.add_subplot(gs[0])
    ax_vol  = fig.add_subplot(gs[1], sharex=ax_main)

    _style(ax_main)
    _style(ax_vol)

    _candles(ax_main, o, h, l, c, xs)

    # T0 vertical dashed
    for ax in (ax_main, ax_vol):
        ax.axvline(n - 1, color=T0_COL, lw=0.9, ls="--", alpha=0.5)

    # T0 price horizontal
    ax_main.axhline(signal.close, color=T0_COL, lw=0.7, ls="--", alpha=0.4)
    ax_main.text(
        0.005, signal.close,
        f"T0 price {signal.close:.2f}",
        transform=ax_main.get_yaxis_transform(),
        color=TEXT, fontsize=7.5, va="bottom", ha="left",
    )

    # Volume
    bull = c >= o
    ax_vol.bar(xs[bull],  v[bull],  color=BULL, width=0.75, linewidth=0, alpha=0.85)
    ax_vol.bar(xs[~bull], v[~bull], color=BEAR, width=0.75, linewidth=0, alpha=0.85)

    # X ticks: evenly spaced relative days
    tick_idx = np.linspace(0, n - 1, 9, dtype=int)
    ax_vol.set_xticks(tick_idx)
    ax_vol.set_xticklabels([f"{rel_days[i]:.0f}" for i in tick_idx], color=TEXT, fontsize=8)
    ax_main.tick_params(labelbottom=False)

    ax_main.set_ylabel("Price",     color=TEXT, fontsize=9)
    ax_vol.set_ylabel("4H volume",  color=TEXT, fontsize=9)
    ax_vol.set_xlabel("Relative days to T0", color=TEXT, fontsize=9)

    fig.suptitle(
        f"{signal.symbol}  {signal.grade}  4H context  |  "
        f"3 months before T0  |  closed 4H bars only",
        color=TEXT, fontsize=11,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(str(out_path), dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  4H → {out_path.name}")
