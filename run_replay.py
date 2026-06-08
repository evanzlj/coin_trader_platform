#!/usr/bin/env python3
"""
历史回放验证：读取 2025-01-01 → 2026-06-05 的本地数据，跑 A/A+ 信号生成器。

用法：
    python3 run_replay.py
    python3 run_replay.py --symbol BTC/USDT --start 2025-06-01 --end 2025-12-31
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Make both packages importable from this directory
sys.path.insert(0, str(Path(__file__).parent))

from realtime_data_pull import ReplayFeed
from signal_generator import SignalGenerator, SignalParams

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)

DATA_DIR = Path(__file__).parent / "history_data_manager" / "data"
ALL_SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]


async def main(symbols, start, end):
    feed = ReplayFeed(
        data_dir = DATA_DIR,
        symbols  = symbols,
        start    = start,
        end      = end,
        speed    = 0.0,  # as fast as possible
    )

    gen = SignalGenerator(feed=feed, symbols=symbols)

    signals_seen: list = []

    @gen.on_signal
    async def on_signal(evt):
        signals_seen.append(evt)
        print(f"  SIGNAL  {evt}")

    @gen.on_state
    async def on_state(evt):
        print(f"  STATE   {evt}")

    print(f"\nReplay: {symbols}  {start} → {end}\n")
    await feed.start()

    # Summary
    a_plus = [s for s in signals_seen if s.grade == "A+"]
    a_only = [s for s in signals_seen if s.grade == "A"]
    print(f"\n{'='*60}")
    print(f"Total signals : {len(signals_seen)}")
    print(f"  A+          : {len(a_plus)}")
    print(f"  A           : {len(a_only)}")
    if signals_seen:
        from collections import Counter
        by_sym  = Counter(s.symbol for s in signals_seen)
        by_side = Counter(s.side   for s in signals_seen)
        print(f"By symbol : {dict(by_sym)}")
        print(f"By side   : {dict(by_side)}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", nargs="*", default=ALL_SYMBOLS)
    parser.add_argument("--start",  default="2025-01-01")
    parser.add_argument("--end",    default="2026-06-05")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.symbol, args.start, args.end))
