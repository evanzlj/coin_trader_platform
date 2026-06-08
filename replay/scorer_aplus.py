"""
A+ specific scorer — thin wrapper over score_playbook.

Derives activation_window_bars from impulse_phase (VLM does not set it).
A-only signals bypass this module entirely and call score_playbook directly.

Window policy (15m bars from T0):
  CLIMAX_TRAP      → 2  (must trigger fast or the impulse has already resolved)
  EARLY_ACCEPTANCE → 4  (retest/hold window)
  LATE_EXTENSION   → 6  (trend-follow confirmation window)
  AMBIGUOUS        → None (no window, same as base scorer)
"""
from __future__ import annotations
from typing import Optional
import pandas as pd

from .scorer import PlaybookScore, score_playbook

WINDOW_BY_PHASE: dict[str, Optional[int]] = {
    "CLIMAX_TRAP":      2,
    "EARLY_ACCEPTANCE": 4,
    "LATE_EXTENSION":   6,
    "AMBIGUOUS":        None,
}


def score_playbook_aplus(
    activation_rule: dict,
    hypothesis:      str,
    df_post:         pd.DataFrame,
    structure_side:  str = "near_support",
    impulse_phase:   Optional[str] = None,
    prompt_version:  Optional[str] = None,
) -> PlaybookScore:
    """
    Score one A+ playbook.

    impulse_phase: value from a_plus_impulse_assessment.impulse_phase.
    Maps to activation_window_bars via WINDOW_BY_PHASE.
    Unknown or missing impulse_phase → no window (safe fallback).
    """
    activation_window_bars = WINDOW_BY_PHASE.get(impulse_phase) if impulse_phase else None

    return score_playbook(
        activation_rule=activation_rule,
        hypothesis=hypothesis,
        df_post=df_post,
        structure_side=structure_side,
        prompt_version=prompt_version,
        activation_window_bars=activation_window_bars,
    )
