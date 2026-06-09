"""
A+ replay analysis.

Reads aplus_scores.csv and produces grouped win-rate / trigger-rate tables.

Key groupings:
  1. impulse_phase × flow_direction
  2. structure_side × flow_direction
  3. hypothesis × flow_direction
  4. flow_confirmed vs not (high/medium plausibility only)
  5. trigger rate by impulse_phase

Usage:
    python3 scoring/aplus_analysis.py
    python3 scoring/aplus_analysis.py --min-n 5   # filter groups with fewer than N rows
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

SCORES_PATH = ROOT / "scoring" / "aplus_scores.csv"
OUT_DIR     = ROOT / "scoring"

SCOREABLE = {"win", "loss", "open"}
TRIGGERED = {"triggered"}


def _win_rate(grp: pd.core.groupby.GroupBy, min_n: int) -> pd.DataFrame:
    r = grp["outcome"].agg(
        triggered_n =lambda x: x.isin(SCOREABLE).sum(),
        win         =lambda x: (x == "win").sum(),
        loss        =lambda x: (x == "loss").sum(),
        open        =lambda x: (x == "open").sum(),
    ).reset_index()
    r["win_rate"] = (r["win"] / r["triggered_n"].replace(0, pd.NA) * 100).round(1)
    return r[r["triggered_n"] >= min_n].sort_values("win_rate", ascending=False)


def _trigger_rate(grp: pd.core.groupby.GroupBy, min_n: int) -> pd.DataFrame:
    r = grp["activation_status"].agg(
        total        ="count",
        triggered_n  =lambda x: (x == "triggered").sum(),
        not_triggered=lambda x: (x == "not_triggered").sum(),
        cancelled    =lambda x: (x == "cancelled").sum(),
    ).reset_index()
    r["trigger_pct"] = (r["triggered_n"] / r["total"].replace(0, pd.NA) * 100).round(1)
    return r[r["total"] >= min_n].sort_values("trigger_pct", ascending=False)


def _section(title: str, df: pd.DataFrame) -> None:
    print(f"\n{'=' * 60}")
    print(title)
    print(df.to_string(index=False))


def run(min_n: int = 3) -> None:
    if not SCORES_PATH.exists():
        print(f"[error] {SCORES_PATH} not found — run aplus_scorer.py first")
        sys.exit(1)

    df = pd.read_csv(SCORES_PATH)
    print(f"Loaded {len(df)} playbook rows, {df['pkg'].nunique()} packages")
    print(f"\nFlow direction dist:\n{df['flow_direction'].value_counts().to_string()}")
    print(f"\nImpulse phase dist:\n{df['impulse_phase'].value_counts().to_string()}")
    print(f"\nOutcome dist:\n{df['outcome'].value_counts().to_string()}")

    # Exclude no_rule / invalid_rule / no_data from rate calculations
    scorable_df = df[~df["activation_status"].isin(
        ["no_rule", "invalid_rule", "no_data"]
    )].copy()
    triggered_df = scorable_df[scorable_df["activation_status"] == "triggered"].copy()

    # 1. impulse_phase × flow_direction — win rate (triggered only)
    _section(
        "Win rate by impulse_phase × flow_direction  (triggered playbooks only)",
        _win_rate(triggered_df.groupby(["impulse_phase", "flow_direction"]), min_n),
    )

    # 2. structure_side × flow_direction
    _section(
        "Win rate by structure_side × flow_direction",
        _win_rate(triggered_df.groupby(["structure_side", "flow_direction"]), min_n),
    )

    # 3. hypothesis × flow_direction
    _section(
        "Win rate by hypothesis × flow_direction",
        _win_rate(triggered_df.groupby(["hypothesis", "flow_direction"]), min_n),
    )

    # 4. flow_confirmed effect (high/medium plausibility only)
    hm = triggered_df[triggered_df["plausibility"].isin(["high", "medium"])].copy()
    _section(
        "Win rate: flow_confirmed vs not  (high/medium plausibility playbooks)",
        _win_rate(hm.groupby("flow_confirmed"), min_n),
    )

    # 5. Trigger rate by impulse_phase
    _section(
        "Trigger rate by impulse_phase  (all scorable playbooks)",
        _trigger_rate(scorable_df.groupby("impulse_phase"), min_n),
    )

    # 6. Trigger rate by impulse_phase × flow_direction
    _section(
        "Trigger rate by impulse_phase × flow_direction",
        _trigger_rate(scorable_df.groupby(["impulse_phase", "flow_direction"]), min_n),
    )

    # 7. CVD relationship effect
    if "cvd_price_rel" in df.columns:
        _section(
            "Win rate by cvd_price_relationship",
            _win_rate(triggered_df.groupby("cvd_price_rel"), min_n),
        )

    # Save CSVs
    groups = [
        ("by_phase_flow",    triggered_df.groupby(["impulse_phase", "flow_direction"])),
        ("by_side_flow",     triggered_df.groupby(["structure_side", "flow_direction"])),
        ("by_hyp_flow",      triggered_df.groupby(["hypothesis", "flow_direction"])),
        ("flow_confirmed",   hm.groupby("flow_confirmed")),
        ("trigger_by_phase", scorable_df.groupby("impulse_phase")),
    ]
    for name, grp in groups:
        out = _win_rate(grp, min_n=1) if "trigger" not in name else _trigger_rate(grp, min_n=1)
        out.to_csv(OUT_DIR / f"aplus_analysis_{name}.csv", index=False)
    print(f"\nCSVs saved to {OUT_DIR}/aplus_analysis_*.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-n", type=int, default=3,
                        help="Minimum triggered_n to include a group in output")
    args = parser.parse_args()
    run(min_n=args.min_n)
