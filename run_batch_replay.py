"""
Batch replay runner.

Scans replay_materials/ for dirs that have vlm_response.json but no
replay_result.json, runs the scorer, and saves replay_result.json in place.

Usage:
    python3 run_batch_replay.py
    python3 run_batch_replay.py --force           # re-score all, overwrite existing
    python3 run_batch_replay.py --materials-dir replay_materials
    python3 run_batch_replay.py --data-dir history_data_manager/data
"""
import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from draw_kline.common import SLUG_MAP, load_ohlcv
from replay.session import ReplaySession


def _process(sig_dir: Path, data_dir: Path) -> None:
    sig_path = sig_dir / "signal.json"
    vlm_path = sig_dir / "vlm_response.json"

    with open(sig_path) as f:
        sig_d = json.load(f)
    with open(vlm_path) as f:
        vlm = json.load(f)

    signal = SimpleNamespace(
        symbol         = sig_d["symbol"],
        grade          = sig_d["grade"],
        bar_time       = pd.Timestamp(sig_d["bar_time"]),
        close          = sig_d["close"],
        structure_side = sig_d["structure_side"],
    )

    slug  = SLUG_MAP[signal.symbol]
    t0    = signal.bar_time
    t_end = pd.Timestamp("2099-01-01", tz="UTC")

    df_full = load_ohlcv(data_dir, slug, "15m", t0, t_end)
    df_post = df_full[df_full["open_time"] > t0].copy()
    df_post = df_post.sort_values("open_time").reset_index(drop=True)

    session = ReplaySession.from_signal_and_response(signal, vlm)
    session.score(df_post)
    session.save(sig_dir / "replay_result.json")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Re-score even if replay_result.json already exists")
    parser.add_argument("--materials-dir", default="replay_materials")
    parser.add_argument("--data-dir", default="history_data_manager/data")
    args = parser.parse_args()

    materials_dir = Path(args.materials_dir)
    data_dir      = Path(args.data_dir)

    dirs = sorted(d for d in materials_dir.iterdir() if d.is_dir())
    total     = len(dirs)
    processed = skipped = errors = 0

    for i, d in enumerate(dirs):
        prefix = f"[{i+1:4d}/{total}]"

        if not (d / "vlm_response.json").exists():
            print(f"{prefix} SKIP  (no vlm_response.json)  {d.name}")
            skipped += 1
            continue

        if not (d / "signal.json").exists():
            print(f"{prefix} SKIP  (no signal.json)         {d.name}")
            skipped += 1
            continue

        if (d / "replay_result.json").exists() and not args.force:
            skipped += 1
            continue

        try:
            _process(d, data_dir)
            processed += 1
            print(f"{prefix} OK    {d.name}")
        except Exception as e:
            errors += 1
            print(f"{prefix} ERROR {d.name}: {e}")

    print(f"\nDone — processed={processed}  skipped={skipped}  errors={errors}")


if __name__ == "__main__":
    main()
