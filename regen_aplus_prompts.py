#!/usr/bin/env python3
"""
Regenerate prompt.txt for all existing A+ replay materials using the new prompt template,
then add .ready marker so openclaw can pick them up.

Does NOT re-render charts — reuses existing pngs.
Output: replay_materials/{name}/ with updated prompt.txt + .ready
"""

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from draw_kline.common import load_ohlcv, load_flow, SLUG_MAP
from prompt_generator import build_prompt

DATA_DIR     = ROOT / "history_data_manager" / "data"
REPLAY_DIR   = ROOT / "replay_materials"

SLUG_MAP_REV = {v: k for k, v in SLUG_MAP.items()}  # e.g. "btcusdt" → "BTC/USDT"


def regen_one(pkg_dir: Path) -> bool:
    sig_path = pkg_dir / "signal.json"
    if not sig_path.exists():
        return False

    sig_data = json.loads(sig_path.read_text())

    # Reconstruct minimal signal-like object
    class _Sig:
        pass

    sig = _Sig()
    sig.symbol          = sig_data["symbol"]
    sig.grade           = sig_data["grade"]
    sig.bar_time        = pd.Timestamp(sig_data["bar_time"], tz="UTC")
    sig.close           = sig_data["close"]
    sig.structure_side  = sig_data["structure_side"]
    sig.structure_space = sig_data.get("structure_space", 0.0)
    sig.vol_ratio       = sig_data.get("vol_ratio", 0.0)
    sig.weekly_trend    = sig_data.get("weekly_trend", "neutral")
    sig.h4_support      = sig_data.get("h4_support", 0.0)
    sig.h4_resistance   = sig_data.get("h4_resistance", 0.0)

    slug = SLUG_MAP.get(sig.symbol)
    if slug is None:
        print(f"  [skip] unknown symbol: {sig.symbol}")
        return False

    t0    = sig.bar_time
    t_14d = t0 - pd.Timedelta(days=14)
    t_3m  = t0 - pd.Timedelta(days=90)

    try:
        df15m   = load_ohlcv(DATA_DIR, slug, "15m", t_14d, t0)
        df4h    = load_ohlcv(DATA_DIR, slug, "4h",  t_3m,  t0)
        df_flow = load_flow( DATA_DIR, slug,         t_14d, t0)
    except Exception as e:
        print(f"  [error] data load failed for {pkg_dir.name}: {e}")
        return False

    bundle = build_prompt(sig, df15m, df4h, df_flow)
    with open(pkg_dir / "prompt.txt", "w") as f:
        f.write("=== SYSTEM ===\n")
        f.write(bundle.system_text)
        f.write("\n\n=== USER TEXT ===\n")
        f.write(bundle.user_text)

    # .ready marker for openclaw
    (pkg_dir / ".ready").touch()
    return True


def main():
    aplus_dirs = sorted(
        d for d in REPLAY_DIR.iterdir()
        if d.is_dir() and "Aplus" in d.name
    )
    total = len(aplus_dirs)
    print(f"Found {total} A+ directories — regenerating prompts...\n")

    ok = 0
    for i, pkg_dir in enumerate(aplus_dirs):
        success = regen_one(pkg_dir)
        status  = "ok" if success else "skip"
        print(f"[{i+1:3d}/{total}] {pkg_dir.name} — {status}")
        if success:
            ok += 1

    print(f"\nDone. {ok}/{total} prompts regenerated, .ready markers added.")


if __name__ == "__main__":
    main()
