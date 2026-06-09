"""
Generate A+ replay materials for 2020-01-01 → 2026-06-09 (full cycle).

Only processes A+ signals. Skips packages that already exist.
Run after fetch_history.py has pulled the full 2020-2026 dataset.

Output: replay_materials/{sym}_Aplus_{YYYYMMDD_HHMM}/
    signal.json, prompt.txt, *_4h.png, *_15m.png
"""

import asyncio
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from realtime_data_pull import ReplayFeed
from signal_generator import SignalGenerator
from draw_kline import render
from draw_kline.common import SLUG_MAP, load_ohlcv, load_flow
from prompt_generator import build_prompt

DATA_DIR = ROOT / "history_data_manager" / "data"
OUT_DIR  = ROOT / "replay_materials"
SYMBOLS  = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
START    = "2020-01-01"
END      = "2026-06-09"

signals = []


async def collect():
    feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS, start=START, end=END)
    gen  = SignalGenerator(feed=feed, symbols=SYMBOLS)

    @gen.on_signal
    async def h(evt):
        if evt.grade == "A+":
            signals.append(evt)

    await feed.start()


def _pkg_name(sig) -> str:
    slug  = SLUG_MAP[sig.symbol].replace("usdt", "")
    ts    = sig.bar_time.strftime("%Y%m%d_%H%M")
    return f"{slug}_Aplus_{ts}"


def generate_one(sig, idx: int, total: int) -> bool:
    name = _pkg_name(sig)
    out  = OUT_DIR / name

    if out.exists():
        print(f"[{idx+1:4d}/{total}] {name} — skip (exists)")
        return False

    out.mkdir(parents=True, exist_ok=True)

    t0    = sig.bar_time
    t_14d = t0 - pd.Timedelta(days=14)
    t_3m  = t0 - pd.Timedelta(days=90)
    slug  = SLUG_MAP[sig.symbol]

    try:
        df15m   = load_ohlcv(DATA_DIR, slug, "15m", t_14d, t0)
        df4h    = load_ohlcv(DATA_DIR, slug, "4h",  t_3m,  t0)
        df_flow = load_flow( DATA_DIR, slug,         t_14d, t0)
    except Exception as e:
        print(f"[{idx+1:4d}/{total}] {name} — ERROR loading data: {e}")
        out.rmdir()
        return False

    # charts
    try:
        render(sig, DATA_DIR, out)
    except Exception as e:
        print(f"[{idx+1:4d}/{total}] {name} — ERROR chart: {e}")

    # prompt
    try:
        bundle = build_prompt(sig, df15m, df4h, df_flow)
        with open(out / "prompt.txt", "w") as f:
            f.write("=== SYSTEM ===\n")
            f.write(bundle.system_text)
            f.write("\n\n=== USER TEXT ===\n")
            f.write(bundle.user_text)
    except Exception as e:
        print(f"[{idx+1:4d}/{total}] {name} — ERROR prompt: {e}")

    # signal metadata
    with open(out / "signal.json", "w") as f:
        json.dump({
            "symbol":                sig.symbol,
            "grade":                 sig.grade,
            "bar_time":              str(sig.bar_time),
            "close":                 sig.close,
            "structure_side":        sig.structure_side,
            "structure_space":       sig.structure_space,
            "position_in_structure": sig.position_in_structure,
            "vol_ratio":             sig.vol_ratio,
            "weekly_trend":          sig.weekly_trend,
            "h4_support":            sig.h4_support,
            "h4_resistance":         sig.h4_resistance,
        }, f, indent=2)

    print(f"[{idx+1:4d}/{total}] {name} — ok")
    return True


def main():
    print(f"Collecting A+ signals {START} → {END} ...")
    asyncio.run(collect())

    total     = len(signals)
    existing  = sum(1 for s in signals if (OUT_DIR / _pkg_name(s)).exists())
    new_count = total - existing

    print(f"Total A+ signals: {total}")
    print(f"  Already exists: {existing}")
    print(f"  To generate:    {new_count}\n")

    OUT_DIR.mkdir(exist_ok=True)
    generated = 0
    for i, sig in enumerate(signals):
        try:
            if generate_one(sig, i, total):
                generated += 1
        except Exception as e:
            print(f"  FATAL ERROR [{i+1}/{total}]: {e}")

    print(f"\nDone. {generated} new packages generated → {OUT_DIR}")


if __name__ == "__main__":
    main()
