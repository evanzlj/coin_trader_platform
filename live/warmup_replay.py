#!/usr/bin/env python3
"""
Warmup replay — run once before starting live monitor.

Replays historical bars from START to END (default: 2020-01-01 to today)
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
import os
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

DATA_DIR        = ROOT / "history_data_manager" / "data"
STATE_PATH      = ROOT / "live" / "state" / "dedup_state.json"
BUFFER_STATE_DIR = ROOT / "live" / "state" / "buffer"
SYMBOLS         = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

DEFAULT_START = "2020-01-01"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=DEFAULT_START,
                        help="Replay start date (default: 2020-01-01)")
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

    now = pd.Timestamp.utcnow()

    # Save buffer state (BarBuffers + state machine bar counts)
    gen.save_buffer_state(BUFFER_STATE_DIR)

    # Save dedup state + buffer_saved_at (end of replay = freshest bar processed).
    # Atomic write (tmp + os.replace): monitor refuses to start on a corrupt dedup
    # file, so the producer of that file must never leave a half-written one.
    state = gen.get_dedup_state()
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at":        now.isoformat(),
        "buffer_saved_at": end,
        "dedup":           state,
    }
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, STATE_PATH)
    logger.info("dedup state saved → %s", STATE_PATH)
    logger.info("buffer_saved_at = %s", end)
    for sym, t in state.items():
        logger.info("  %s: last_signal = %s", sym, t or "never")


def main() -> None:
    args = parse_args()
    end  = args.end or pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    asyncio.run(run(args.start, end))


if __name__ == "__main__":
    main()
