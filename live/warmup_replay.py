#!/usr/bin/env python3
"""
Warmup replay — run once before starting live monitor.

Replays historical bars from START to END (default: 2025-01-01 to today)
through SignalGenerator to build:
  - dedup state (serialized to live/state/dedup_state.json)
  - 4H structure buffer
  - weekly MA50

Usage:
    python3 live/warmup_replay.py
    python3 live/warmup_replay.py --start 2025-01-01 --end 2026-06-09
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from realtime_data_pull.feed import ReplayFeed
from signal_generator.generator import SignalGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR   = ROOT / "history_data_manager" / "data"
STATE_PATH = ROOT / "live" / "state" / "dedup_state.json"
SYMBOLS    = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

DEFAULT_START = "2025-01-01"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=DEFAULT_START,
                        help="Replay start date (default: 2025-01-01)")
    parser.add_argument("--end",   default=None,
                        help="Replay end date (default: today UTC)")
    return parser.parse_args()


async def run(start: str, end: str) -> None:
    logger.info("warmup replay: %s → %s", start, end)

    feed = ReplayFeed(
        data_dir = DATA_DIR,
        symbols  = SYMBOLS,
        start    = start,
        end      = end,
    )
    gen = SignalGenerator(feed=feed, symbols=SYMBOLS)

    signal_count = 0

    @gen.on_signal
    async def _count(evt):
        nonlocal signal_count
        signal_count += 1

    await feed.start()

    logger.info("warmup complete — %d signals detected during replay", signal_count)

    state = gen.get_dedup_state()
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(
            {"saved_at": pd.Timestamp.utcnow().isoformat(), "dedup": state},
            f, indent=2,
        )
    logger.info("dedup state saved → %s", STATE_PATH)
    for sym, t in state.items():
        logger.info("  %s: last_signal = %s", sym, t or "never")


def main() -> None:
    args = parse_args()
    end  = args.end or pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    asyncio.run(run(args.start, end))


if __name__ == "__main__":
    main()
