"""
build_prompt: assemble a VLM prompt bundle for one SignalEvent.

Usage:
    from prompt_generator import build_prompt
    bundle = build_prompt(signal, df15m, df4h, df_flow)
    # bundle.system_text  → system prompt string
    # bundle.user_text    → user text (signal details + context + chart description)
    # attach bundle images (path_4h, path_15m) separately when calling the VLM API
"""
from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .templates import get_system, get_user_chart_section
from .metrics   import wick_body_ratio, flow_metrics
from .context   import compute_daily_regime


@dataclass
class PromptBundle:
    system_text: str
    user_text:   str
    signal_type: str   # "A+" or "A-only"
    symbol:      str


def build_prompt(
    signal,
    df15m:   pd.DataFrame,
    df4h:    pd.DataFrame,
    df_flow: pd.DataFrame,
) -> PromptBundle:
    """
    Build a VLM prompt bundle from a SignalEvent + data frames.

    Args:
        signal   — SignalEvent with .grade, .symbol, .bar_time, .structure_side, .vol_ratio
        df15m    — 15m OHLCV bars up to and including T0
        df4h     — 4H OHLCV bars (ideally 3 months; used for daily regime context)
        df_flow  — 15m taker flow bars aligned with df15m
    """
    t0           = signal.bar_time
    grade        = signal.grade              # "A" or "A+"
    signal_type  = "A+" if grade == "A+" else "A-only"
    symbol       = signal.symbol
    struct_side  = signal.structure_side

    # ── Computed metrics ─────────────────────────────────────────────────────
    wb_ratio = wick_body_ratio(df15m, t0, struct_side)
    avg_imb, cum_flow = flow_metrics(df_flow, t0, n_bars=8)
    daily_regime = compute_daily_regime(df4h, t0)

    # ── Signal details JSON ───────────────────────────────────────────────────
    signal_details = {
        "symbol":                          symbol,
        "signal_time":                     "T0_hidden_absolute_time",
        "signal_type":                     signal_type,
        "signal_structure_context":        struct_side,
        "structural_price_levels_in_text": False,
        "volume_ratio_at_signal":          signal.vol_ratio,
        "wick_body_ratio":                 wb_ratio,
        "avg_imbalance_last_8_bars_to_signal":    avg_imb,
        "cum_flow_last_8_bars_to_signal_usdt":     cum_flow,
    }

    # ── Context block ─────────────────────────────────────────────────────────
    context_block = {"daily_regime": daily_regime}

    # ── Assemble user text ────────────────────────────────────────────────────
    user_text = (
        "Signal details:\n"
        + json.dumps(signal_details, indent=2)
        + "\n\nContext block:\n"
        "The following regime facts are computed from completed daily candles"
        " available at T0 only. They are context, not trade direction.\n"
        + json.dumps(context_block, indent=2)
        + "\n\n"
        + get_user_chart_section(symbol, signal_type)
    )

    return PromptBundle(
        system_text=get_system(grade),
        user_text=user_text,
        signal_type=signal_type,
        symbol=symbol,
    )
