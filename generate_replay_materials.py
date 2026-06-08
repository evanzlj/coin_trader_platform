"""
Batch generate replay materials for all signals: 2026-01-01 to 2026-05-30.

For each signal:
  replay_materials/{SLUG}_{GRADE}_{TIMESTAMP}/
      4h.png
      15m.png
      prompt.txt
      signal.json
"""
import asyncio, json, sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from realtime_data_pull import ReplayFeed
from signal_generator import SignalGenerator
from draw_kline import render
from draw_kline.common import SLUG_MAP, load_ohlcv, load_flow
from prompt_generator import build_prompt

DATA_DIR = Path(__file__).parent / "history_data_manager/data"
OUT_DIR  = Path(__file__).parent / "replay_materials"
SYMBOLS  = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
START    = "2026-01-01"
END      = "2026-05-30"

signals = []


async def collect():
    feed = ReplayFeed(data_dir=DATA_DIR, symbols=SYMBOLS, start=START, end=END)
    gen  = SignalGenerator(feed=feed, symbols=SYMBOLS)

    @gen.on_signal
    async def h(evt):
        signals.append(evt)

    await feed.start()


def generate_one(sig, idx: int, total: int) -> Path:
    slug  = SLUG_MAP[sig.symbol]
    grade = sig.grade.replace("+", "plus")
    ts    = sig.bar_time.strftime("%Y%m%d_%H%M")
    stem  = slug.replace("usdt", "")
    name  = f"{stem}_{grade}_{ts}"

    out = OUT_DIR / name
    out.mkdir(parents=True, exist_ok=True)

    t0    = sig.bar_time
    t_14d = t0 - pd.Timedelta(days=14)
    t_3m  = t0 - pd.Timedelta(days=90)

    df15m  = load_ohlcv(DATA_DIR, slug, "15m", t_14d, t0)
    df4h   = load_ohlcv(DATA_DIR, slug, "4h",  t_3m,  t0)
    df_flow = load_flow(DATA_DIR, slug,         t_14d, t0)

    # charts
    path_4h, path_15m = render(sig, DATA_DIR, out)

    # prompt
    bundle = build_prompt(sig, df15m, df4h, df_flow)
    prompt_path = out / "prompt.txt"
    with open(prompt_path, "w") as f:
        f.write("=== SYSTEM ===\n")
        f.write(bundle.system_text)
        f.write("\n\n=== USER TEXT ===\n")
        f.write(bundle.user_text)

    # signal metadata
    sig_path = out / "signal.json"
    with open(sig_path, "w") as f:
        json.dump({
            "symbol":         sig.symbol,
            "grade":          sig.grade,
            "bar_time":       str(sig.bar_time),
            "close":          sig.close,
            "structure_side": sig.structure_side,
            "structure_space": sig.structure_space,
            "position_in_structure": sig.position_in_structure,
            "vol_ratio":      sig.vol_ratio,
            "weekly_trend":   sig.weekly_trend,
            "h4_support":     sig.h4_support,
            "h4_resistance":  sig.h4_resistance,
        }, f, indent=2)

    print(f"[{idx+1:3d}/{total}] {name}")
    return out


def main():
    print(f"Collecting signals {START} → {END} ...")
    asyncio.run(collect())
    print(f"Total signals: {len(signals)}\n")

    OUT_DIR.mkdir(exist_ok=True)

    for i, sig in enumerate(signals):
        try:
            generate_one(sig, i, len(signals))
        except Exception as e:
            print(f"  ERROR {sig}: {e}")

    print(f"\nDone. Materials saved to: {OUT_DIR}")


if __name__ == "__main__":
    main()
