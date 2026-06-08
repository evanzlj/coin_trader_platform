#!/usr/bin/env python3
"""
跑四个品种完整回放，统计信号分布差异，输出参数调优建议。
"""
import asyncio, sys
from pathlib import Path
from collections import defaultdict

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from realtime_data_pull import ReplayFeed
from signal_generator import SignalGenerator, SignalEvent

DATA_DIR = Path(__file__).parent / "history_data_manager" / "data"
SYMBOLS  = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]


async def collect() -> list[SignalEvent]:
    feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS,
                      start="2026-01-01", end="2026-05-30", speed=0.0)
    gen  = SignalGenerator(feed=feed, symbols=SYMBOLS)  # uses SYMBOL_PARAMS by default
    sigs: list[SignalEvent] = []

    @gen.on_signal
    async def _h(evt): sigs.append(evt)

    await feed.start()
    return sigs


def analyze(sigs: list[SignalEvent]):
    df = pd.DataFrame([{
        "symbol":    s.symbol,
        "grade":     s.grade,
        "bar_time":  s.bar_time,
        "close":     s.close,
        "pos":       s.position_in_structure,
        "space":     s.structure_space,
        "vol_ratio": s.vol_ratio,
        "weekly":    s.weekly_trend,
        "h4_sup":    s.h4_support,
        "h4_res":    s.h4_resistance,
    } for s in sigs])

    df["month"]          = df["bar_time"].dt.to_period("M")
    df["struct_width_pct"] = (df["h4_res"] - df["h4_sup"]) / df["close"] * 100

    # ── 总体概览 ──────────────────────────────────────────────────────────────
    print("=" * 70)
    print(f"总信号数: {len(df)}  期间: {df['bar_time'].min().date()} → {df['bar_time'].max().date()}")
    print("=" * 70)

    total_15m_bars = 365 * 24 * 4 + 157 * 24 * 4   # 2025 + 2026 Jan-Jun5 ≈
    days = (df["bar_time"].max() - df["bar_time"].min()).days or 1

    for sym in SYMBOLS:
        s = df[df["symbol"] == sym]
        a_plus = s[s["grade"] == "A+"]
        a_only = s[s["grade"] == "A"]
        freq_days = len(s) / days

        print(f"\n{'─'*60}")
        print(f"  {sym}   总计={len(s)}  A+={len(a_plus)}  A={len(a_only)}  "
              f"频率={freq_days:.2f}/天")

        print(f"\n  【position_in_structure】")
        print(f"    mean={s['pos'].mean():.3f}  std={s['pos'].std():.3f}  "
              f"min={s['pos'].min():.3f}  max={s['pos'].max():.3f}")
        print(f"    A+: mean={a_plus['pos'].mean():.3f}  std={a_plus['pos'].std():.3f}" if len(a_plus) else "    A+: n/a")
        # pos 分布区间
        bins = [-9, 0.05, 0.15, 0.50, 0.85, 0.95, 9]
        labels = ["<5%(极端低)", "5-15%(低)", "15-50%(中)", "50-85%(中)", "85-95%(高)", ">95%(极端高)"]
        cuts = pd.cut(s["pos"], bins=bins, labels=labels)
        dist = cuts.value_counts().sort_index()
        for lbl, cnt in dist.items():
            bar = "█" * int(cnt / max(dist) * 20)
            print(f"      {lbl:<18} {cnt:4d}  {bar}")

        print(f"\n  【structure_space】")
        print(f"    mean={s['space'].mean():.2f}  std={s['space'].std():.2f}  "
              f"p25={s['space'].quantile(0.25):.2f}  p75={s['space'].quantile(0.75):.2f}  "
              f"max={s['space'].max():.2f}")

        print(f"\n  【vol_ratio】")
        print(f"    mean={s['vol_ratio'].mean():.2f}  std={s['vol_ratio'].std():.2f}  "
              f"p75={s['vol_ratio'].quantile(0.75):.2f}  p95={s['vol_ratio'].quantile(0.95):.2f}")

        print(f"\n  【4H 结构宽度 (% of price)】")
        print(f"    mean={s['struct_width_pct'].mean():.2f}%  "
              f"std={s['struct_width_pct'].std():.2f}%  "
              f"p25={s['struct_width_pct'].quantile(0.25):.2f}%  "
              f"p75={s['struct_width_pct'].quantile(0.75):.2f}%")

        print(f"\n  【月度分布  A+ / A】")
        monthly_aplus = a_plus.groupby("month").size()
        monthly_aonly = a_only.groupby("month").size()
        monthly_total = s.groupby("month").size()
        for m in monthly_total.index:
            n_ap = monthly_aplus.get(m, 0)
            n_a  = monthly_aonly.get(m, 0)
            tot  = monthly_total[m]
            bar  = "█" * int(tot / monthly_total.max() * 20)
            print(f"      {m}  总={tot:3d}  A+={n_ap:3d}  A={n_a:3d}  {bar}")

        print(f"\n  【weekly_trend @ 信号时】")
        wt = s["weekly"].value_counts()
        for k, v in wt.items():
            print(f"      {k:<10} {v:4d} ({v/len(s)*100:.0f}%)")

    # ── A+ 比例 ───────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("A+ 占比 & 参数调优建议")
    print("=" * 70)
    for sym in SYMBOLS:
        s     = df[df["symbol"] == sym]
        aplus = s[s["grade"] == "A+"]
        ratio = len(aplus) / len(s) * 100 if len(s) else 0

        pos_std        = s["pos"].std()
        space_mean     = s["space"].mean()
        vol_p95        = s["vol_ratio"].quantile(0.95)
        width_mean_pct = s["struct_width_pct"].mean()

        print(f"\n  {sym}  A+占比={ratio:.0f}%  pos_std={pos_std:.3f}  "
              f"space_mean={space_mean:.1f}  vol_p95={vol_p95:.2f}  "
              f"4H宽度={width_mean_pct:.2f}%")

        suggestions = []
        if ratio < 10:
            suggestions.append("A+占比过低 → 考虑放宽 extreme_pos 阈值 或 降低 wick_body_threshold")
        if ratio > 35:
            suggestions.append("A+占比偏高 → 考虑收紧 extreme_pos 或 提高 vol_ratio_threshold")
        if width_mean_pct < 3:
            suggestions.append(f"4H结构较窄({width_mean_pct:.1f}%) → zone_pct 可适当收窄")
        if width_mean_pct > 8:
            suggestions.append(f"4H结构宽({width_mean_pct:.1f}%) → zone_pct 可适当放宽, min_space 可提高")
        if pos_std > 0.30:
            suggestions.append("pos分布极散 → 结构位不稳定，4H lookback 可缩短")
        if vol_p95 > 5:
            suggestions.append(f"vol_ratio p95={vol_p95:.1f} 偏高 → vol_ratio_threshold 考虑上调")

        for sg in suggestions:
            print(f"    ⚑  {sg}")
        if not suggestions:
            print("    ✓  参数基本合适")


if __name__ == "__main__":
    sigs = asyncio.run(collect())
    analyze(sigs)
