"""
15m detail chart: 14 days before T0.
Main:  candles + stepped 4H structure + QRC192 (upper/mid/lower) + MA20
Sub1:  flow imbalance bars  (±0.2 ref, T0 bar highlighted)
Sub2:  visible cumulative flow  (white line + fill)
All time info hidden — relative hours only.
"""
from __future__ import annotations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd

from .common import (
    BG, PANEL_BG, BULL, BEAR, TEXT, GRID, T0_COL,
    MA_COL, RES_COL, SUP_COL, QRC_U, QRC_M, QRC_L,
)
from .indicators import rolling_ma, compute_qrc, compute_4h_structure_steps


def _style(ax):
    ax.set_facecolor(PANEL_BG)
    ax.tick_params(colors=TEXT, labelsize=7.5)
    ax.grid(axis="y", color=GRID, lw=0.35, alpha=0.6)
    for sp in ax.spines.values():
        sp.set_color(GRID)


def _candles(ax, o, h, l, c, xs):
    bull = c >= o
    bh = np.abs(c - o)
    bb = np.minimum(o, c)
    bh = np.where(bh == 0, (h - l) * 0.05, bh)
    ax.vlines(xs[bull],  l[bull],  h[bull],  colors=BULL, lw=0.5)
    ax.vlines(xs[~bull], l[~bull], h[~bull], colors=BEAR, lw=0.5)
    ax.bar(xs[bull],  bh[bull],  bottom=bb[bull],  color=BULL, width=0.75, linewidth=0)
    ax.bar(xs[~bull], bh[~bull], bottom=bb[~bull], color=BEAR, width=0.75, linewidth=0)


def draw_15m_detail(
    df15m:          pd.DataFrame,
    df4h_warmup:    pd.DataFrame,   # 4H bars with extra warmup before the 14d window
    df_flow:        pd.DataFrame,
    signal,
    out_path:       Path,
    qrc_n:          int   = 192,
    ma_n:           int   = 20,
    pivot_left:  int = 3,
    pivot_right: int = 3,
) -> None:
    if df15m.empty:
        return

    t0 = signal.bar_time
    df = df15m.copy().sort_values("close_time").reset_index(drop=True)
    n  = len(df)
    xs = np.arange(n)

    rel_h = ((df["close_time"] - t0).dt.total_seconds() / 3600.0).values

    o = df["open"].values
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values

    # ── Indicators ────────────────────────────────────────────────────────────
    ma20 = rolling_ma(c, ma_n)
    qrc_upper, qrc_mid, qrc_lower = compute_qrc(h, l, c, n=qrc_n)
    struct_resist, struct_supp = compute_4h_structure_steps(
        df4h_warmup, df, left=pivot_left, right=pivot_right
    )

    # ── Flow data ─────────────────────────────────────────────────────────────
    imbalance = np.zeros(n)
    netflow   = np.zeros(n)
    if not df_flow.empty:
        merged = df[["open_time"]].merge(
            df_flow[["open_time", "imbalance",
                      "taker_buy_quote_volume", "taker_sell_quote_volume"]],
            on="open_time", how="left",
        )
        imbalance = merged["imbalance"].fillna(0.0).values
        netflow   = (
            merged["taker_buy_quote_volume"].fillna(0.0).values
            - merged["taker_sell_quote_volume"].fillna(0.0).values
        )

    vis_cum_flow = np.cumsum(netflow)

    # ── Layout ────────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(22, 11), facecolor=BG)
    gs  = gridspec.GridSpec(
        3, 1, height_ratios=[5, 1.3, 1.3], hspace=0.07,
        top=0.93, bottom=0.07, left=0.055, right=0.93,
    )
    ax_main = fig.add_subplot(gs[0])
    ax_imb  = fig.add_subplot(gs[1], sharex=ax_main)
    ax_vcf  = fig.add_subplot(gs[2], sharex=ax_main)

    for ax in (ax_main, ax_imb, ax_vcf):
        _style(ax)

    # ── Candles ───────────────────────────────────────────────────────────────
    _candles(ax_main, o, h, l, c, xs)

    # ── 4H structure steps ────────────────────────────────────────────────────
    v = ~np.isnan(struct_resist)
    if v.any():
        ax_main.step(xs[v], struct_resist[v], where="post",
                     color=RES_COL, lw=1.3, alpha=0.9, label="4H Resistance Visual")
        ax_main.step(xs[v], struct_supp[v],   where="post",
                     color=SUP_COL, lw=1.3, alpha=0.9, label="4H Support Visual")

    # ── QRC192 ────────────────────────────────────────────────────────────────
    vq = ~np.isnan(qrc_mid)
    if vq.any():
        ax_main.plot(xs[vq], qrc_upper[vq], color=QRC_U, lw=1.0,
                     label="QRC192 upper")
        ax_main.plot(xs[vq], qrc_mid[vq],   color=QRC_M, lw=1.0, ls="--",
                     label="QRC192 mid")
        ax_main.plot(xs[vq], qrc_lower[vq], color=QRC_L, lw=1.0,
                     label="QRC192 lower")

    # ── MA20 ──────────────────────────────────────────────────────────────────
    vm = ~np.isnan(ma20)
    if vm.any():
        ax_main.plot(xs[vm], ma20[vm], color=MA_COL, lw=1.0, label="MA20")

    # ── T0 vertical dashed ────────────────────────────────────────────────────
    for ax in (ax_main, ax_imb, ax_vcf):
        ax.axvline(n - 1, color=T0_COL, lw=0.9, ls="--", alpha=0.45)

    # ── Right-axis labels at T0 ───────────────────────────────────────────────
    def _rlabel(y, text, color, fontsize=7.5):
        ax_main.annotate(
            text, xy=(1.002, y),
            xycoords=ax_main.get_yaxis_transform(),
            color=color, fontsize=fontsize, va="center", ha="left",
            clip_on=False,
        )

    if not np.isnan(qrc_upper[-1]):
        _rlabel(qrc_upper[-1], "QRC U",   QRC_U)
        _rlabel(qrc_mid[-1],   "QRC MID", QRC_M)
        _rlabel(qrc_lower[-1], "QRC L",   QRC_L)

    # Signal marker label — right of the T0 dashed line, outside plot area
    ax_main.annotate(
        f"{signal.grade} Signal ↑ T0",
        xy=(1.002, h[-1]),
        xycoords=ax_main.get_yaxis_transform(),
        color=T0_COL, fontsize=8, va="bottom", ha="left",
        clip_on=False,
    )

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_handles = [
        Line2D([0], [0], color=RES_COL, lw=1.3,        label="4H Resistance Visual"),
        Line2D([0], [0], color=SUP_COL, lw=1.3,        label="4H Support Visual"),
        Line2D([0], [0], color=QRC_U,   lw=1.0,        label="QRC192 upper"),
        Line2D([0], [0], color=QRC_M,   lw=1.0, ls="--", label="QRC192 mid"),
        Line2D([0], [0], color=QRC_L,   lw=1.0,        label="QRC192 lower"),
        Line2D([0], [0], color=MA_COL,  lw=1.0,        label="MA20"),
    ]
    ax_main.legend(
        handles=legend_handles, loc="upper left",
        facecolor="#111827", edgecolor=GRID,
        labelcolor=TEXT, fontsize=7, ncol=3,
    )

    # ── Imbalance subplot ─────────────────────────────────────────────────────
    pos = imbalance >= 0
    ax_imb.bar(xs[pos],  imbalance[pos],  color=BULL, width=0.75, linewidth=0, alpha=0.9)
    ax_imb.bar(xs[~pos], imbalance[~pos], color=BEAR, width=0.75, linewidth=0, alpha=0.9)
    # T0 bar override
    ax_imb.bar(n - 1, imbalance[n - 1], color="#3b82f6", width=0.75, linewidth=0)
    # ±0.2 reference
    ax_imb.axhline( 0.2, color=BULL, lw=0.6, ls="--", alpha=0.45)
    ax_imb.axhline(-0.2, color=BEAR, lw=0.6, ls="--", alpha=0.45)
    ax_imb.axhline(0,    color=GRID, lw=0.4)
    avg8 = imbalance[max(0, n - 8) : n].mean()
    ax_imb.text(
        0.01, 0.93, f"avg imbalance last 8 bars: {avg8:.4f}",
        transform=ax_imb.transAxes, color=TEXT, fontsize=7.5, va="top",
    )
    ax_imb.set_ylabel("Imbalance", color=TEXT, fontsize=8)

    # ── Visible Cumulative Flow subplot ───────────────────────────────────────
    ax_vcf.plot(xs, vis_cum_flow, color=T0_COL, lw=1.0)
    ax_vcf.fill_between(xs, 0, vis_cum_flow,
                        where=vis_cum_flow >= 0, color=BULL, alpha=0.35)
    ax_vcf.fill_between(xs, 0, vis_cum_flow,
                        where=vis_cum_flow <  0, color=BEAR, alpha=0.35)
    ax_vcf.axhline(0, color=GRID, lw=0.4)
    base_sym = signal.symbol.split("/")[0]
    ax_vcf.set_ylabel(f"Visible Cum Flow {base_sym}", color=TEXT, fontsize=8)

    # ── X ticks: relative hours ────────────────────────────────────────────────
    tick_idx = np.linspace(0, n - 1, 9, dtype=int)
    tick_lbs = [f"T{rel_h[i]:+.0f}h" for i in tick_idx]
    ax_vcf.set_xticks(tick_idx)
    ax_vcf.set_xticklabels(tick_lbs, color=TEXT, fontsize=8)
    ax_main.tick_params(labelbottom=False)
    ax_imb.tick_params(labelbottom=False)
    ax_vcf.set_xlabel("Relative time to signal (T0)", color=TEXT, fontsize=9)

    # ── Title ─────────────────────────────────────────────────────────────────
    pre_h = abs(int(rel_h[0]))
    fig.suptitle(
        f"{signal.symbol}  {signal.grade}  Signal-Time Review  |  "
        f"{signal.structure_side}  |  "
        f"pre{pre_h}h + 4H structure + QRC{qrc_n}  |  "
        f"anonymized time axis  |  no-lookahead cutoff",
        color=TEXT, fontsize=10,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(str(out_path), dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  15m → {out_path.name}")
