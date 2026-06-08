"""
Signal parameters — mirrors Pine Script MTF v3.4 input defaults.

Two presets:
    SignalParams.standard()      — BTC, BNB
    SignalParams.conservative()  — ETH, SOL
"""
from dataclasses import dataclass


@dataclass
class SignalParams:
    # ── 15m indicators ──────────────────────────────────────────────────────
    ma_len: int            = 20
    atr_len: int           = 14
    vol_len: int           = 20
    vol_mult: float        = 1.0    # minimum vol ratio for any signal
    vol_ratio_threshold: float = 1.5  # vol spike threshold for A+ strong-energy

    # ── 4H structure (pivot left=3 right=3, matches visual layer) ───────────────
    zone_pct: float = 1.5  # % distance to structure to qualify as "near"

    # ── Position in structure for A+ ────────────────────────────────────────
    extreme_pos_long: float  = 0.10   # bottom N% → A+ long zone
    extreme_pos_short: float = 0.90   # top (1-N)% → A+ short zone

    # ── Wick/body threshold for A+ ───────────────────────────────────────────
    wick_body_threshold: float = 1.0

    # ── Entry conditions ─────────────────────────────────────────────────────
    reversal_pct: float = 0.3
    swing_lookback: int = 6
    sl_atr_mult: float  = 0.3

    # ── Structure space filter ───────────────────────────────────────────────
    min_space: float = 2.0

    # ── Weekly filter ────────────────────────────────────────────────────────
    weekly_fast_len: int          = 20
    weekly_slow_len: int          = 50
    strict_long_with_weekly: bool  = False
    strict_short_with_weekly: bool = False

    # ── Dedup window (15m bars; 32 = 8 hours) ────────────────────────────────
    window_dedup: int = 32

    # ── Pending state machine ────────────────────────────────────────────────
    confirm_window_bars: int  = 48
    confirm_buffer_atr: float = 0.1

    # ── Presets ──────────────────────────────────────────────────────────────

    @classmethod
    def standard(cls) -> "SignalParams":
        """BTC, BNB — wider structure tolerance, lower energy bar."""
        return cls(
            extreme_pos_long      = 0.10,
            extreme_pos_short     = 0.90,
            wick_body_threshold   = 1.0,
            zone_pct              = 1.5,
            vol_ratio_threshold   = 1.5,
            vol_mult              = 1.0,
            reversal_pct          = 0.3,
        )

    @classmethod
    def conservative(cls) -> "SignalParams":
        """ETH, SOL — tighter structure zone, stronger energy bar required."""
        return cls(
            extreme_pos_long      = 0.05,
            extreme_pos_short     = 0.95,
            wick_body_threshold   = 1.5,
            zone_pct              = 1.0,
            vol_ratio_threshold   = 2.0,
            vol_mult              = 1.2,
            reversal_pct          = 0.4,
        )


# Per-symbol parameter assignment (fixed, not tunable at runtime)
#   BTC / BNB → standard:     wider zone, lower energy bar
#   ETH / SOL → conservative: tighter zone, stronger energy required
SYMBOL_PARAMS: dict[str, "SignalParams"] = {
    "BTC/USDT": SignalParams.standard(),
    "ETH/USDT": SignalParams.conservative(),
    "BNB/USDT": SignalParams.standard(),
    "SOL/USDT": SignalParams.conservative(),
}
