"""ReplaySession: one signal + VLM response + all playbook scores."""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from .scorer import PlaybookScore, score_playbook
from .scorer_aplus import score_playbook_aplus


@dataclass
class ReplaySession:
    signal_symbol:    str
    signal_grade:     str
    signal_bar_time:  pd.Timestamp
    signal_close:     float
    signal_side:      str           # "near_support" | "near_resistance"

    vlm_response:     dict          # parsed JSON from VLM

    # populated by score()
    playbook_scores:  dict[str, PlaybookScore] = field(default_factory=dict)
    l1_score:         Optional[PlaybookScore]  = None
    winning_score:    Optional[PlaybookScore]  = None

    @classmethod
    def from_signal_and_response(cls, signal, vlm_response: dict) -> "ReplaySession":
        return cls(
            signal_symbol   = signal.symbol,
            signal_grade    = signal.grade,
            signal_bar_time = signal.bar_time,
            signal_close    = signal.close,
            signal_side     = signal.structure_side,
            vlm_response    = vlm_response,
        )

    def score(self, df_post: pd.DataFrame) -> None:
        """
        Score all playbooks in vlm_response against post-T0 bars.
        df_post: 15m bars with open_time > signal_bar_time, sorted ascending.

        Multi-playbook winner selection:
          - Score every playbook independently (each runs its own state machine).
          - Winner = playbook with the earliest activated_at timestamp.
          - If no playbook activated, fall back to L1 (backward-compatible).
        """
        playbooks = self.vlm_response.get("playbooks", [])
        l1_hypothesis = (
            self.vlm_response
            .get("replay_scoring_notes", {})
            .get("l1_playbook", "")
        )
        prompt_version = self.vlm_response.get("prompt_version")

        is_aplus = self.signal_grade == "A+"
        impulse_phase = (
            self.vlm_response
            .get("a_plus_impulse_assessment", {})
            .get("impulse_phase")
        ) if is_aplus else None

        for pb in playbooks:
            hyp  = pb.get("hypothesis", "")
            plan = pb.get("conditional_trade_plan", {})
            rule = plan.get("activation_rule")

            if is_aplus:
                s = score_playbook_aplus(
                    rule, hyp, df_post,
                    structure_side=self.signal_side,
                    impulse_phase=impulse_phase,
                    prompt_version=prompt_version,
                )
            else:
                s = score_playbook(
                    rule, hyp, df_post,
                    structure_side=self.signal_side,
                    prompt_version=prompt_version,
                )

            self.playbook_scores[hyp] = s
            if hyp == l1_hypothesis:
                self.l1_score = s

        # Pick winning playbook: earliest activated_at among all scores
        activated = [
            s for s in self.playbook_scores.values()
            if s.activated_at is not None
        ]
        if activated:
            self.winning_score = min(activated, key=lambda s: s.activated_at)
        else:
            # No playbook activated — keep L1 result for backward compatibility
            self.winning_score = self.l1_score

    def to_dict(self) -> dict:
        def _score_dict(s: Optional[PlaybookScore]) -> Optional[dict]:
            if s is None:
                return None
            return {
                "hypothesis":           s.hypothesis,
                "result":               s.result,
                "primary_touched_at":   str(s.primary_touched_at) if s.primary_touched_at else None,
                "activated_at":         str(s.activated_at) if s.activated_at else None,
                "tp1_at":               str(s.tp1_at) if s.tp1_at else None,
                "tp2_at":               str(s.tp2_at) if s.tp2_at else None,
                "invalidated_at":       str(s.invalidated_at) if s.invalidated_at else None,
                "bars_to_primary":      s.bars_to_primary,
                "bars_to_activation":   s.bars_to_activation,
                "bars_to_tp1":          s.bars_to_tp1,
                "bars_to_tp2":          s.bars_to_tp2,
                "bars_to_invalidation": s.bars_to_invalidation,
                "tp1_level":            s.tp1_level,
                "tp2_level":            s.tp2_level,
                "activation_price":     s.activation_price,
                "invalidation_level":   s.invalidation_level,
                "r_distance":           s.r_distance,
                "mfe_pct":              s.mfe_pct,
                "mae_pct":              s.mae_pct,
                "mfe_r":                s.mfe_r,
                "mae_r":                s.mae_r,
                "prompt_version":       s.prompt_version,
            }

        return {
            "signal": {
                "symbol":    self.signal_symbol,
                "grade":     self.signal_grade,
                "bar_time":  str(self.signal_bar_time),
                "close":     self.signal_close,
                "side":      self.signal_side,
            },
            "l1_hypothesis":   (
                self.vlm_response
                .get("replay_scoring_notes", {})
                .get("l1_playbook", "")
            ),
            "winning_score":   _score_dict(self.winning_score),
            "l1_score":        _score_dict(self.l1_score),
            "playbook_scores": {h: _score_dict(s) for h, s in self.playbook_scores.items()},
            "vlm_response":    self.vlm_response,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "ReplaySession":
        with open(path) as f:
            d = json.load(f)
        sig = d["signal"]
        sess = cls(
            signal_symbol   = sig["symbol"],
            signal_grade    = sig["grade"],
            signal_bar_time = pd.Timestamp(sig["bar_time"]),
            signal_close    = sig["close"],
            signal_side     = sig["side"],
            vlm_response    = d["vlm_response"],
        )
        return sess
