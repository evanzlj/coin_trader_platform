"""
run_replay: given a signal + VLM response JSON, score all playbooks
against actual post-T0 price data and save the session.

Usage:
    from replay import run_replay
    session = run_replay(signal, vlm_response_dict, data_dir, out_dir)

Or from CLI:
    python3 -m replay.runner --session /tmp/replay_sample_v2/prompt.txt \\
        --response response.json --data-dir history_data_manager/data
"""
from __future__ import annotations
import json
import argparse
from pathlib import Path

import pandas as pd

from draw_kline.common import SLUG_MAP, load_ohlcv
from .session import ReplaySession

def run_replay(
    signal,
    vlm_response: dict,
    data_dir: Path,
    out_dir:  Path,
) -> ReplaySession:
    """
    Score a single signal's VLM response against post-T0 price data.

    Loads all available post-T0 data (no time cap). Scoring runs until the
    playbook resolves (TP2 hit or invalidation) or data is exhausted
    (result = activated_tp1_unresolved or not_triggered).

    Args:
        signal        — SignalEvent
        vlm_response  — parsed VLM JSON (the full playbook map)
        data_dir      — path to history_data_manager/data
        out_dir       — where to save the session JSON

    Returns:
        ReplaySession with scores populated and saved to out_dir
    """
    slug = SLUG_MAP.get(signal.symbol)
    if slug is None:
        raise ValueError(f"Unknown symbol: {signal.symbol}")

    t0    = signal.bar_time
    t_end = pd.Timestamp("2099-01-01", tz="UTC")   # far future → load all available

    df_full = load_ohlcv(data_dir, slug, "15m", t0, t_end)
    # strictly post-T0 bars (open_time > T0)
    df_post = df_full[df_full["open_time"] > t0].copy()
    df_post = df_post.sort_values("open_time").reset_index(drop=True)

    session = ReplaySession.from_signal_and_response(signal, vlm_response)
    session.score(df_post)

    ts   = t0.strftime("%Y%m%d_%H%M")
    stem = slug.replace("usdt", "")
    grade = signal.grade.replace("+", "plus")
    out_dir.mkdir(parents=True, exist_ok=True)
    save_path = out_dir / f"{stem}_{grade}_{ts}_replay.json"
    session.save(save_path)

    return session


def _print_session(session: ReplaySession) -> None:
    sig = session
    print(f"\nSignal:  {sig.signal_grade} {sig.signal_symbol} {sig.signal_side}"
          f" @ {sig.signal_close}  t={sig.signal_bar_time}")

    l1 = session.vlm_response.get("replay_scoring_notes", {}).get("l1_playbook", "")
    print(f"L1:      {l1}")

    if session.l1_score:
        s = session.l1_score
        print(f"L1 score: {s.result}")
        if s.primary_touched_at:
            print(f"  primary_touched_at  {s.primary_touched_at}  (bar {s.bars_to_primary})")
        if s.activated_at:
            print(f"  activated_at        {s.activated_at}  (+{s.bars_to_activation} bars)")
        if s.tp1_at:
            print(f"  tp1_hit             {s.tp1_at}  (+{s.bars_to_tp1} bars)  level={s.tp1_level}")
        if s.tp2_at:
            print(f"  tp2_hit             {s.tp2_at}  (+{s.bars_to_tp2} bars)  level={s.tp2_level}")
        if s.invalidated_at:
            print(f"  invalidated         {s.invalidated_at}  (+{s.bars_to_invalidation} bars)")
    else:
        print("L1 score: (no l1 playbook found in scores)")

    print("\nAll playbooks:")
    for hyp, sc in session.playbook_scores.items():
        marker = " ← L1" if hyp == l1 else ""
        print(f"  {hyp:45s}  {sc.result}{marker}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    parser = argparse.ArgumentParser()
    parser.add_argument("--signal-json",  required=True, help="JSON file with signal fields")
    parser.add_argument("--response",     required=True, help="VLM response JSON file")
    parser.add_argument("--data-dir",     default="history_data_manager/data")
    parser.add_argument("--out-dir",      default="/tmp/replay_out")
    args = parser.parse_args()

    with open(args.signal_json) as f:
        sig_d = json.load(f)
    with open(args.response) as f:
        vlm = json.load(f)

    # reconstruct a lightweight signal object
    from types import SimpleNamespace
    signal = SimpleNamespace(
        symbol         = sig_d["symbol"],
        grade          = sig_d["grade"],
        bar_time       = pd.Timestamp(sig_d["bar_time"]),
        close          = sig_d["close"],
        structure_side = sig_d["structure_side"],
    )

    session = run_replay(
        signal, vlm,
        data_dir = Path(args.data_dir),
        out_dir  = Path(args.out_dir),
    )
    _print_session(session)
    print(f"\nSaved: {Path(args.out_dir)}")
