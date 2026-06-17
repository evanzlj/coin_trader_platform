"""
Frozen live A/A+ signal universe manifest.

This module is the guardrail for the production signal generator.  If params or
core detector source changes, live startup and tests should fail until a new
signal universe id is explicitly minted.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any
import hashlib
import json

from .params import SYMBOL_PARAMS


FROZEN_SIGNAL_UNIVERSE_ID = "live-aa-v1-20260615"
FROZEN_AT_UTC = "2026-06-15T00:00:00Z"

FROZEN_SYMBOLS = ("BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT")

FROZEN_SYMBOL_PROFILES = {
    "BTC/USDT": "standard",
    "ETH/USDT": "conservative",
    "BNB/USDT": "standard",
    "SOL/USDT": "conservative",
}

FROZEN_SYMBOL_PARAMS: dict[str, dict[str, Any]] = {
    "BTC/USDT": {
        "ma_len": 20,
        "atr_len": 14,
        "vol_len": 20,
        "vol_mult": 1.0,
        "vol_ratio_threshold": 1.5,
        "zone_pct": 1.5,
        "extreme_pos_long": 0.10,
        "extreme_pos_short": 0.90,
        "wick_body_threshold": 1.0,
        "reversal_pct": 0.3,
        "swing_lookback": 6,
        "sl_atr_mult": 0.3,
        "min_space": 2.0,
        "weekly_fast_len": 20,
        "weekly_slow_len": 50,
        "strict_long_with_weekly": False,
        "strict_short_with_weekly": False,
        "window_dedup": 32,
        "confirm_window_bars": 48,
        "confirm_buffer_atr": 0.1,
    },
    "ETH/USDT": {
        "ma_len": 20,
        "atr_len": 14,
        "vol_len": 20,
        "vol_mult": 1.2,
        "vol_ratio_threshold": 2.0,
        "zone_pct": 1.0,
        "extreme_pos_long": 0.05,
        "extreme_pos_short": 0.95,
        "wick_body_threshold": 1.5,
        "reversal_pct": 0.4,
        "swing_lookback": 6,
        "sl_atr_mult": 0.3,
        "min_space": 2.0,
        "weekly_fast_len": 20,
        "weekly_slow_len": 50,
        "strict_long_with_weekly": False,
        "strict_short_with_weekly": False,
        "window_dedup": 32,
        "confirm_window_bars": 48,
        "confirm_buffer_atr": 0.1,
    },
    "BNB/USDT": {
        "ma_len": 20,
        "atr_len": 14,
        "vol_len": 20,
        "vol_mult": 1.0,
        "vol_ratio_threshold": 1.5,
        "zone_pct": 1.5,
        "extreme_pos_long": 0.10,
        "extreme_pos_short": 0.90,
        "wick_body_threshold": 1.0,
        "reversal_pct": 0.3,
        "swing_lookback": 6,
        "sl_atr_mult": 0.3,
        "min_space": 2.0,
        "weekly_fast_len": 20,
        "weekly_slow_len": 50,
        "strict_long_with_weekly": False,
        "strict_short_with_weekly": False,
        "window_dedup": 32,
        "confirm_window_bars": 48,
        "confirm_buffer_atr": 0.1,
    },
    "SOL/USDT": {
        "ma_len": 20,
        "atr_len": 14,
        "vol_len": 20,
        "vol_mult": 1.2,
        "vol_ratio_threshold": 2.0,
        "zone_pct": 1.0,
        "extreme_pos_long": 0.05,
        "extreme_pos_short": 0.95,
        "wick_body_threshold": 1.5,
        "reversal_pct": 0.4,
        "swing_lookback": 6,
        "sl_atr_mult": 0.3,
        "min_space": 2.0,
        "weekly_fast_len": 20,
        "weekly_slow_len": 50,
        "strict_long_with_weekly": False,
        "strict_short_with_weekly": False,
        "window_dedup": 32,
        "confirm_window_bars": 48,
        "confirm_buffer_atr": 0.1,
    },
}

FROZEN_SOURCE_SHA256 = {
    "signal_generator/params.py": "3c0569c9c67dedbae7231629f72ec05eacbb31eb9a2c723613acf3350c8dd857",
    "signal_generator/signal_logic.py": "d2020d39f25c5f22788ea6c7162fdc118f6123b764dc44a4ed42edc7f0cdc322",
    "signal_generator/indicators.py": "e1348c8cdffe888c6f8551b5cecdf14865f174353fa431e551104bfd005aab25",
    "signal_generator/generator.py": "9ae1fd32b238b8f74b71c335b3b432e980b98d354d24e1676d8a81deace9f49d",
}

FROZEN_LOGIC = {
    "trusted_source": "coin_trader_platform live SignalGenerator",
    "entrypoint": "live/monitor.py -> SignalGenerator -> signal_logic.evaluate",
    "bar_trigger": "closed 15m bar",
    "structure_timeframe": "4h",
    "structure_method": "pivot_structure(left=3,right=3,max_lookback=None)",
    "weekly_context": "informational only; not a gate",
    "flow_data": "not used in core A/A+ detector",
    "dedup_scope": "per symbol, any A/A+ grade, 32 closed 15m bars",
}

FROZEN_POST_VLM_PLAYBOOK_FILTERS = {
    "source": "live/vlm_finalizer.py and live/live_openclaw.py",
    "inputs": "VLM playbook activation_rule, not SignalEvent.structure_space",
    "metrics": {
        "r_dist_pct": "abs(activation_price - invalidation_level) / activation_price * 100",
        "tp1_dist_pct": "abs(tp1_level - activation_price) / activation_price * 100",
    },
    "rules": {
        "BTC/USDT": {"r_dist_min": 0.5, "tp1_max_exclusive": 1.5},
        "ETH/USDT": {"r_dist_min": 1.5, "tp1_dead_zone_inclusive": [1.0, 2.0]},
        "BNB/USDT": {"r_dist_min": 0.3, "r_dist_max": 1.0},
        "SOL/USDT": {"r_dist_min": 1.5},
    },
    "executor_gate": "bars_to_activation >= 2 is enforced after activation, not by SignalEvent",
}


class SignalUniverseDriftError(RuntimeError):
    """Raised when runtime signal generation no longer matches the freeze."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def current_symbol_params() -> dict[str, dict[str, Any]]:
    return {sym: asdict(SYMBOL_PARAMS[sym]) for sym in FROZEN_SYMBOLS}


def current_source_sha256() -> dict[str, str]:
    root = _repo_root()
    return {rel: _sha256(root / rel) for rel in FROZEN_SOURCE_SHA256}


def diff_frozen_runtime(symbols: tuple[str, ...] | list[str] | None = None) -> list[str]:
    diffs: list[str] = []

    actual_symbols = tuple(symbols) if symbols is not None else FROZEN_SYMBOLS
    if actual_symbols != FROZEN_SYMBOLS:
        diffs.append(f"symbols changed: frozen={FROZEN_SYMBOLS!r} actual={actual_symbols!r}")

    actual_params = current_symbol_params()
    if actual_params != FROZEN_SYMBOL_PARAMS:
        diffs.append("SYMBOL_PARAMS changed")

    actual_hashes = current_source_sha256()
    for rel, frozen_hash in FROZEN_SOURCE_SHA256.items():
        actual_hash = actual_hashes.get(rel)
        if actual_hash != frozen_hash:
            diffs.append(f"{rel} sha256 changed: frozen={frozen_hash} actual={actual_hash}")

    return diffs


def assert_frozen_runtime(symbols: tuple[str, ...] | list[str] | None = None) -> None:
    diffs = diff_frozen_runtime(symbols=symbols)
    if diffs:
        details = "; ".join(diffs)
        raise SignalUniverseDriftError(
            f"{FROZEN_SIGNAL_UNIVERSE_ID} drift detected: {details}. "
            "Mint a new signal universe id before running live."
        )


def manifest() -> dict[str, Any]:
    return {
        "signal_universe_id": FROZEN_SIGNAL_UNIVERSE_ID,
        "frozen_at_utc": FROZEN_AT_UTC,
        "symbols": list(FROZEN_SYMBOLS),
        "symbol_profiles": FROZEN_SYMBOL_PROFILES,
        "symbol_params": FROZEN_SYMBOL_PARAMS,
        "source_sha256": FROZEN_SOURCE_SHA256,
        "logic": FROZEN_LOGIC,
        "post_vlm_playbook_filters": FROZEN_POST_VLM_PLAYBOOK_FILTERS,
    }


if __name__ == "__main__":
    assert_frozen_runtime()
    print(json.dumps(manifest(), indent=2, sort_keys=True))
