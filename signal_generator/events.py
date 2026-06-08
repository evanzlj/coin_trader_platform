from __future__ import annotations
from dataclasses import dataclass
import pandas as pd


@dataclass
class SignalEvent:
    """
    Emitted when A or A+ fires on a closed 15m bar.

    A+  — extreme position in structure + strong rejection energy.
          High-confidence structural reaction zone.
    A   — structural touch that didn't qualify as A+.
          Still structure-derived, less intense.

    Both grades are directional-agnostic radars: they signal that the
    structure is reacting, not which way price will move.
    """
    symbol: str
    grade: str             # "A" or "A+"
    bar_time: pd.Timestamp
    close: float

    # How much room exists to the far side of the structure relative to
    # how close price is to the near side.  Higher = more space = structure
    # quality is better.  Used as a pre-filter (min_space), not a trade target.
    structure_space: float

    # Which structure level price is touching
    structure_side: str            # "near_support" | "near_resistance"

    # Context
    position_in_structure: float  # 0–1  (0 = at support, 1 = at resistance)
    vol_ratio: float
    weekly_trend: str             # "bull" | "bear" | "neutral"
    h4_support: float
    h4_resistance: float

    def __str__(self) -> str:
        return (
            f"[{self.grade}] {self.symbol} {self.structure_side} @ {self.close:.2f} "
            f"space={self.structure_space:.1f}  pos={self.position_in_structure:.2f} "
            f"weekly={self.weekly_trend}  t={self.bar_time}"
        )


@dataclass
class StateEvent:
    """Emitted when a pending signal transitions to confirmed or expired."""
    symbol: str
    state: str             # "confirmed" | "expired"
    original: SignalEvent
    bar_time: pd.Timestamp

    def __str__(self) -> str:
        return f"[{self.state.upper()}] {self.symbol} {self.original.grade} t={self.bar_time}"
