#!/usr/bin/env python3
"""
Last-N-signals chart per symbol — 2x5 grid, one panel per signal.
Each panel: 60 bars before + 35 bars after the signal bar.

Usage:
    python3 plot_signals.py                  # all 4 symbols, last 10 each
    python3 plot_signals.py --symbol BTC     # single symbol
    python3 plot_signals.py -n 20            # last 20 per symbol
"""
import asyncio, sys, argparse
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

sys.path.insert(0, str(Path(__file__).parent))
from realtime_data_pull import ReplayFeed
from signal_generator import SignalGenerator, SignalEvent

DATA_DIR       = Path(__file__).parent / "history_data_manager" / "data"
ALL_SYMBOLS    = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
SLUG_MAP       = {"BTC/USDT": "btcusdt", "ETH/USDT": "ethusdt",
                  "BNB/USDT": "bnbusdt", "SOL/USDT": "solusdt"}
CONTEXT_BEFORE = 60
CONTEXT_AFTER  = 35
GRID_COLS      = 5
GRID_ROWS      = 2   # 2×5 = 10 panels


# ── Replay ────────────────────────────────────────────────────────────────────

async def collect_all(symbols: list[str]) -> dict[str, list[SignalEvent]]:
    feed = ReplayFeed(data_dir=DATA_DIR, symbols=symbols,
                      start="2025-01-01", end="2026-06-05", speed=0.0)
    gen  = SignalGenerator(feed=feed, symbols=symbols)  # uses SYMBOL_PARAMS by default
    buf: dict[str, list[SignalEvent]] = {s: [] for s in symbols}

    @gen.on_signal
    async def _h(evt: SignalEvent):
        buf[evt.symbol].append(evt)

    await feed.start()
    return buf


# ── Data loader ───────────────────────────────────────────────────────────────

def load_15m(slug: str, t_from: pd.Timestamp, t_to: pd.Timestamp) -> pd.DataFrame:
    path = DATA_DIR / "ohlcv" / f"{slug}_15m.csv"
    df   = pd.read_csv(path, parse_dates=["open_time", "close_time"])
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    mask = (df["open_time"] >= t_from) & (df["open_time"] <= t_to)
    return df[mask].set_index("open_time").sort_index()


# ── Candles ───────────────────────────────────────────────────────────────────

def draw_candles(ax, df: pd.DataFrame):
    xs = np.arange(len(df))
    o, h, l, c = df["open"].values, df["high"].values, df["low"].values, df["close"].values
    bull = c >= o
    ax.vlines(xs[bull],  l[bull],  h[bull],  colors="#26a69a", lw=0.55)
    ax.vlines(xs[~bull], l[~bull], h[~bull], colors="#ef5350", lw=0.55)
    bh = np.abs(c - o)
    bb = np.minimum(o, c)
    bh = np.where(bh == 0, (h - l) * 0.05, bh)
    ax.bar(xs[bull],  bh[bull],  bottom=bb[bull],  color="#26a69a", width=0.75)
    ax.bar(xs[~bull], bh[~bull], bottom=bb[~bull], color="#ef5350", width=0.75)


# ── Draw one symbol ───────────────────────────────────────────────────────────

def draw_symbol(symbol: str, signals: list[SignalEvent], out_path: str, n: int):
    signals = signals[-n:]
    slug    = SLUG_MAP[symbol]

    t_min = signals[0].bar_time  - pd.Timedelta(minutes=15 * (CONTEXT_BEFORE + 10))
    t_max = signals[-1].bar_time + pd.Timedelta(minutes=15 * (CONTEXT_AFTER  + 10))
    df_all = load_15m(slug, t_min, t_max)

    cols = GRID_COLS
    rows = (len(signals) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols,
                             figsize=(cols * 6.5, rows * 3.8),
                             facecolor="#0e1117")
    axes_flat = axes.flatten() if rows > 1 else list(axes) if cols > 1 else [axes]

    for pi, sig in enumerate(signals):
        ax = axes_flat[pi]
        ax.set_facecolor("#131722")

        t0 = sig.bar_time - pd.Timedelta(minutes=15 * CONTEXT_BEFORE)
        t1 = sig.bar_time + pd.Timedelta(minutes=15 * CONTEXT_AFTER)
        df = df_all[(df_all.index >= t0) & (df_all.index <= t1)]
        if df.empty:
            ax.axis("off")
            continue

        draw_candles(ax, df)

        idx_map = {ts: i for i, ts in enumerate(df.index)}
        xi = idx_map.get(sig.bar_time)
        n_bars = len(df)

        if xi is not None:
            c_grade = "#ff00ff" if sig.grade == "A+" else "#3b82f6"
            ax.scatter(xi, df["low"].iloc[xi]  * 0.9993, marker="^", color=c_grade, s=60, zorder=6)
            ax.scatter(xi, df["high"].iloc[xi] * 1.0007, marker="v", color=c_grade, s=60, zorder=6)

            # h4 structure lines
            ax.axhline(sig.h4_support,    color="#f59e0b", lw=0.6, ls="--", alpha=0.6)
            ax.axhline(sig.h4_resistance, color="#f59e0b", lw=0.6, ls="--", alpha=0.6)

        tick_xs  = list(range(0, n_bars, 8))
        tick_lbs = [df.index[i].strftime("%d %H:%M") for i in tick_xs]
        ax.set_xticks(tick_xs)
        ax.set_xticklabels(tick_lbs, rotation=40, ha="right", fontsize=4, color="#6b7280")
        ax.tick_params(axis="y", colors="#6b7280", labelsize=4.5)
        ax.grid(axis="y", color="#1e2535", lw=0.35)
        for sp in ax.spines.values():
            sp.set_color("#2d3748")
        ax.set_xlim(-1, n_bars + n_bars * 0.08)

        c_grade = "#ff00ff" if sig.grade == "A+" else "#3b82f6"
        ax.set_title(
            f"#{pi+1}  [{sig.grade}]  "
            f"{sig.bar_time.strftime('%m-%d %H:%M')}  "
            f"pos={sig.position_in_structure:.2f}  space={sig.structure_space:.1f}  "
            f"vol={sig.vol_ratio:.1f}x  {sig.weekly_trend}",
            color=c_grade, fontsize=5.5, pad=2,
        )

    for j in range(len(signals), len(axes_flat)):
        axes_flat[j].axis("off")

    fig.suptitle(
        f"{symbol}  |  Last {len(signals)} signals  "
        f"({signals[0].bar_time.strftime('%Y-%m-%d')} → "
        f"{signals[-1].bar_time.strftime('%Y-%m-%d')})",
        color="white", fontsize=11, y=1.002,
    )
    legend_els = [
        mpatches.Patch(color="#ff00ff", label="A+"),
        mpatches.Patch(color="#3b82f6", label="A"),
        mpatches.Patch(color="#f59e0b", label="4H structure"),
    ]
    fig.legend(handles=legend_els, loc="lower center", ncol=6,
               facecolor="#1f2937", edgecolor="#374151",
               labelcolor="white", fontsize=7,
               bbox_to_anchor=(0.5, -0.012))

    plt.tight_layout(rect=[0, 0.025, 1, 1])
    plt.savefig(out_path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()
    print(f"  saved → {out_path}")


# ── Entry ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=None, help="BTC / ETH / BNB / SOL")
    parser.add_argument("-n", type=int, default=10, help="signals per symbol")
    args = parser.parse_args()

    if args.symbol:
        sym_map = {"BTC": "BTC/USDT", "ETH": "ETH/USDT",
                   "BNB": "BNB/USDT", "SOL": "SOL/USDT"}
        symbols = [sym_map[args.symbol.upper()]]
    else:
        symbols = ALL_SYMBOLS

    print(f"collecting signals for {symbols} ...")
    buf = await collect_all(symbols)

    for sym in symbols:
        sigs = buf[sym]
        slug = SLUG_MAP[sym].replace("usdt", "")
        print(f"\n{sym}  total={len(sigs)}, showing last {args.n}")
        for i, s in enumerate(sigs[-args.n:], 1):
            print(f"  #{i:2d}  [{s.grade}]  {s.bar_time.strftime('%Y-%m-%d %H:%M')}  "
                  f"pos={s.position_in_structure:.2f}  space={s.structure_space:.1f}  "
                  f"vol={s.vol_ratio:.1f}x  {s.weekly_trend}")
        out = f"/tmp/{slug}_signals.png"
        draw_symbol(sym, sigs, out, args.n)


if __name__ == "__main__":
    asyncio.run(main())
