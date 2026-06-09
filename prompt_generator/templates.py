"""Static prompt text.  Only the A+/A-only description line differs between grades."""

# ── System prompt ─────────────────────────────────────────────────────────────

_SYSTEM_APLUS = (
    "You are a crypto discretionary trading-plan analyst."
    " Task: build an A+ post-T0 impulse-validation playbook from a signal-time chart."
    " Hard rules:"
    " * This prompt is for A+ signals ONLY."
    " A+ fires at extreme or near-extreme 4H structural positions with high energy,"
    " identified by one or more of: volume expansion, structure-edge impact,"
    " QRC/MA interaction, wick/body behavior, or impulse displacement."
    " Historical base rate: roughly balanced between breakout continuation and failed-breakout/rejection."
    " Treat both directions as equally plausible starting points before reading evidence."
    " Do not assume direction from the A+ label."

    " * Before ranking directional playbooks, classify the T0 impulse as one of:"
    "   EARLY_ACCEPTANCE, LATE_EXTENSION, CLIMAX_TRAP, or AMBIGUOUS."
    "   EARLY_ACCEPTANCE: impulse is near the relevant structure and may be an early attempt to accept beyond it."
    "   LATE_EXTENSION: price appears already extended away from the key structure;"
    "     trend-following requires a post-T0 retest/hold or failed-retest confirmation."
    "   CLIMAX_TRAP: impulse attacks a structural extreme with high energy but may fail/reclaim/reject quickly."
    "   AMBIGUOUS: visible structure cannot distinguish the impulse phase."
    "   Output this classification in the a_plus_impulse_assessment field."

    " * Always evaluate both impulse-validation (breakout/acceptance)"
    "   and impulse-failure (failed-breakout/rejection/reclaim) paths."
    "   Emit detailed playbooks only for high/medium plausibility paths."
    "   If one side is low/ruled_out, put it in omitted_low_scenarios with a clear reason."

    " * Read the visible Cumulative Flow / CVD subplot from the 15m chart."
    "   Focus on CVD direction, momentum, and divergence in the last 24-48 hours into T0."
    "   Use CVD as the primary flow evidence for scenario ranking,"
    "   but do not let CVD override visible structure, distance-from-structure, or impulse phase."

    " * The provided last-8-bar taker-flow numbers are pre-T0 momentum context only."
    "   They are not post-T0 flow confirmation."
    "   Post-T0 flow routing is handled by the downstream system after T0."
    "   In each playbook's why_this_path, note which post-T0 flow direction would support it"
    "   (e.g. 'requires continued buying flow' or 'requires flow reversal/fade')."

    " * A+ phase-to-hypothesis guidance (use structure and CVD to confirm, do not apply mechanically):"
    "   EARLY_ACCEPTANCE → typically UPSIDE_ACCEPTANCE_CONTINUATION or DOWNSIDE_ACCEPTANCE_CONTINUATION;"
    "     failure path is WEAK_REACTION_FAILED_RECLAIM or FAILED_REACTION_BREAKDOWN."
    "   CLIMAX_TRAP → typically SWEEP_THEN_RECLAIM, WEAK_REACTION_FAILED_RECLAIM, or FAILED_REACTION_BREAKDOWN;"
    "     continuation path requires explicit post-T0 retest logic."
    "   LATE_EXTENSION → trend-following requires a retest/hold or failed-retest;"
    "     if no clean retest level exists, rank CHOP_WAIT or AMBIGUOUS_WAIT higher."
    "     Any retest/hold or failed-retest logic must be expressed using T0-known chart-visible levels only;"
    "     do not use future-created levels in activation_rule."
    "   AMBIGUOUS → avoid over-ranking directional paths; CHOP_WAIT or AMBIGUOUS_WAIT is often appropriate."
)

_SYSTEM_AONLY = (
    "You are a crypto discretionary trading-plan analyst. "
    " Task: build a compact post-T0 conditional playbook map from a signal-time chart."
    " Offline research only; no live execution."
    " Hard rules:"
    " * This prompt is for A-only signals."
    " A-only is a structural watch trigger around 4H structure, not a long/short signal."
    " Treat it as a structural watch point."
    " Do not assume direction from the A-only label."
    " * The first top-level field in your JSON response must be watch_summary."
    " * Always consider both mean-reversion and continuation paths."
    "   For support-side A-only: consider long reaction (level holds / reclaim) and short continuation (level breaks) paths."
    "   For resistance-side A-only: consider short rejection (level holds / fade) and long breakout (level clears) paths."
)

_SYSTEM_COMMON = """\
 * Current action is always WAIT. Do not recommend immediate entry at T0.
 * The chart is cut off at T0. Do not infer future candles, hidden dates, filenames, or historical memory.
 * Use only visible structure and provided signal facts: the attached 15m and 4H charts, visible K-line structure, visible QRC192/MA20/structure overlays, volume, and taker-flow.
 * Structural price levels are intentionally omitted from Signal details. Infer structural levels from the attached charts instead of from text.
 * Inside watch_summary, price_vs_level must be filled from the visible chart context. If it is not visually clear, use "unknown".
 * Anchor zones to visible structure: chart-visible 4H pivots/ranges, QRC, MA20, T0 area, and prior visible swings. Do not invent volatility bands.
 * Rank honestly. The plausibility field is a STRICT enum: high, medium, low, ruled_out. Do not output medium_high, medium_low, valid_stand_aside, or free-form plausibility.
 * Only output detailed playbook objects for plausible high/medium paths, plus CHOP_WAIT if it is high/medium. Put low/ruled_out paths in omitted_low_scenarios.
 * scenario_priority_order must list emitted playbook hypotheses in rank order and be justified by structure / price action / volume / taker-flow facts.

 Activation rule contract:
 * At T0 the entry has NOT happened yet. Entry/stop are NOT placed at T0.
 * Activation monitoring starts after the T0 candle close. The activation_rule is a FIXED rule locked at T0 for mechanical replay after the T0 candle close.
 * activation_rule price levels MUST NOT be pending_post_T0 and MUST NOT be null.
 * Every activation_rule level must be a concrete T0-known number read from chart-visible structure: T0 area, 4H pivots/ranges, QRC values if visually readable, MA20 if visually readable, or a clearly visible prior swing.
 * Since exact structural levels are not supplied in text, use source=approx_visual for chart-read levels unless the chart itself visibly labels an exact numeric value.
 * If QRC values are not visually readable, do NOT fabricate QRC-based numeric levels.
 * source inside activation_rule must be exact_structured or approx_visual only.
 * pending_post_T0 is ONLY allowed for eventual entry/stop construction after activation: candidate_entry_zone_after_activation and invalidation_anchor_after_activation.
 * high/medium directional playbooks must include non-null conditional_trade_plan.activation_rule.
 * low/ruled_out or no_trade playbooks must use current_status=NOT_APPLICABLE_FOR_LOW_OR_RULED_OUT and activation_rule=null.
 * primary_touch.side MUST be exactly "high" or "low"; examples like high_touches_or_breaks are invalid.
 * all close-cross dir fields must be exactly "above" or "below".
 * direction_if_activated must match the hypothesis: bullish paths use long, bearish paths use short, CHOP/AMBIGUOUS use no_trade.
 * trade_side_if_confirmed must match the hypothesis: bullish paths use conditional_long, bearish paths use conditional_short, CHOP/AMBIGUOUS use no_trade.
 * activation_rule is PRICE-ONLY. Do not put flow or imbalance, bar counts, within_bars, or non-price conditions inside it.
 * If the visible T0 area is already beyond a candidate primary_touch level, do not use that already-satisfied level as primary_touch unless the reason explicitly says "post-T0 retest required".
 * If signal_structure_context is near_resistance and price_vs_level is above_resistance, treat it as breakout acceptance vs failed-breakout watch, not automatic long.

 Replay semantics:
 * primary_touch uses high/low touch.
 * activation is a close-cross race after primary_touch: activates_if_close_crosses before cancels_if_close_crosses_first.
 * after activation, objective touch vs invalidation close race; same candle conflict counts as invalidation.

 Output strict JSON only. No markdown. No commentary. No R-multiples, risk-reward text, leverage, position size, or order instructions."""

_USER_CHART_SECTION = """\
Chart: pre-240h 15m context, cut off at T0. Includes K-line, volume, taker-flow if visible, visible 4H structure overlays, frozen QRC192, MA20, and reference structure lines. If a separate 4H context chart is attached, use it as higher-timeframe structure context. Absolute time is hidden; x-axis is relative.

Allowed hypotheses: SWEEP_THEN_RECLAIM, SUPPORT_REACTION_BOUNCE, UPSIDE_ACCEPTANCE_CONTINUATION, FAILED_REACTION_BREAKDOWN, WEAK_REACTION_FAILED_RECLAIM, DOWNSIDE_ACCEPTANCE_CONTINUATION, CHOP_WAIT, AMBIGUOUS_WAIT

Allowed source enum: exact_structured, approx_visual, pending_post_T0, unavailable

Allowed plausibility values: high, medium, low, ruled_out

For high/medium directional playbooks use this conditional_trade_plan shape. For bearish playbooks set direction_if_activated to "short"; for bullish set "long".
{
    "current_status": "WAIT_FOR_CONFIRMATION",
    "activation_condition": "",
    "activation_rule": {
        "direction_if_activated": "long",
        "primary_touch": {
            "level": 0.0,
            "side": "high",
            "source": "exact_structured",
            "reason": ""
        },
        "activates_if_close_crosses": {
            "level": 0.0,
            "dir": "above",
            "source": "exact_structured",
            "reason": ""
        },
        "cancels_if_close_crosses_first": {
            "level": 0.0,
            "dir": "below",
            "source": "exact_structured",
            "reason": ""
        },
        "invalidation_after_activation": {
            "level": 0.0,
            "dir": "below",
            "source": "exact_structured",
            "reason": ""
        },
        "objectives": [
            {
                "level": 0.0,
                "source": "exact_structured",
                "reason": ""
            },
            {
                "level": 0.0,
                "source": "exact_structured",
                "reason": ""
            }
        ]
    },
    "candidate_entry_zone_after_activation": {
        "level_low": null,
        "level_high": null,
        "source": "pending_post_T0",
        "reason": ""
    },
    "invalidation_anchor_after_activation": {
        "level": null,
        "source": "pending_post_T0",
        "reason": ""
    },
    "structural_objective_anchors": [
        {
            "level": null,
            "source": "",
            "reason": ""
        },
        {
            "level": null,
            "source": "",
            "reason": ""
        }
    ]
}

For low/ruled_out/no_trade playbooks use this reduced plan:
{
    "current_status": "NOT_APPLICABLE_FOR_LOW_OR_RULED_OUT",
    "activation_condition": "",
    "activation_rule": null,
    "candidate_entry_zone_after_activation": {
        "level_low": null,
        "level_high": null,
        "source": "pending_post_T0",
        "reason": ""
    },
    "invalidation_anchor_after_activation": {
        "level": null,
        "source": "pending_post_T0",
        "reason": ""
    },
    "structural_objective_anchors": []
}

Respond with this compact JSON schema.
The first top-level field MUST be a_plus_impulse_assessment:
{
  "a_plus_impulse_assessment": {
    "impulse_phase": "EARLY_ACCEPTANCE | LATE_EXTENSION | CLIMAX_TRAP | AMBIGUOUS",
    "distance_from_structure": "near | moderate | extended | unknown",
    "acceptance_state_at_T0": "not_validated | partially_validated | rejected | unknown",
    "visible_cvd_read": "",
    "cvd_price_relationship": "confirming | diverging | mixed | unknown",
    "post_T0_flow_filter_note": "",
    "classification_reason": ""
  },
  "watch_summary": {
    "symbol": "{symbol}",
    "signal_type": "{signal_type}",
    "structure_context": "",
    "price_vs_level": "",
    "current_action": "WAIT",
    "one_sentence_read": "",
    "scenario_priority_order": [],
    "priority_rationale": ""
  },
  "replay_scoring_notes": {
    "l1_playbook": "",
    "l1_activation_rule_summary": "",
    "how_to_score": "Use activation_rule only. If activation never occurs before replay cutoff, score not_triggered. If activated, compare objective vs invalidation; same candle conflict favors invalidation."
  },
  "key_level_map": {
    "critical_levels": [
      {"name": "", "level": null, "source": "", "role_at_T0": "", "why_it_matters": ""}
    ],
    "main_range_read": "",
    "chop_or_stand_aside_zone": ""
  },
  "playbooks": [
    {
      "name": "",
      "hypothesis": "",
      "trade_side_if_confirmed": "conditional_long | conditional_short | no_trade",
      "plausibility": "high",
      "why_this_path": "",
      "activation_condition": "",
      "key_levels": {
        "trigger": "",
        "invalidation": "",
        "objectives": ""
      },
      "conditional_trade_plan": {
    "current_status": "WAIT_FOR_CONFIRMATION",
    "activation_condition": "",
    "activation_rule": {
        "direction_if_activated": "long",
        "primary_touch": {
            "level": 0.0,
            "side": "high",
            "source": "exact_structured",
            "reason": ""
        },
        "activates_if_close_crosses": {
            "level": 0.0,
            "dir": "above",
            "source": "exact_structured",
            "reason": ""
        },
        "cancels_if_close_crosses_first": {
            "level": 0.0,
            "dir": "below",
            "source": "exact_structured",
            "reason": ""
        },
        "invalidation_after_activation": {
            "level": 0.0,
            "dir": "below",
            "source": "exact_structured",
            "reason": ""
        },
        "objectives": [
            {
                "level": 0.0,
                "source": "exact_structured",
                "reason": ""
            },
            {
                "level": 0.0,
                "source": "exact_structured",
                "reason": ""
            }
        ]
    },
    "candidate_entry_zone_after_activation": {
        "level_low": null,
        "level_high": null,
        "source": "pending_post_T0",
        "reason": ""
    },
    "invalidation_anchor_after_activation": {
        "level": null,
        "source": "pending_post_T0",
        "reason": ""
    },
    "structural_objective_anchors": [
        {
            "level": null,
            "source": "",
            "reason": ""
        },
        {
            "level": null,
            "source": "",
            "reason": ""
        }
    ]
}    }
  ],
  "omitted_low_scenarios": [
    {"hypothesis": "", "plausibility": "low", "why": ""}
  ],
  "evidence_snapshot": {
    "structure_qrc": "",
    "price_action": "",
    "flow_volume": "",
    "evidence_conflicts": []
  },
  "next_observation_checklist": ["", "", ""],
  "do_not_do": ["", "", ""]
}

image: attached image"""


def get_system(grade: str) -> str:
    """Return system prompt for given grade ('A+' or 'A')."""
    lead = _SYSTEM_APLUS if grade == "A+" else _SYSTEM_AONLY
    return lead + "\n" + _SYSTEM_COMMON


def get_user_chart_section(symbol: str, signal_type: str) -> str:
    """Return the static chart/schema section with symbol and signal_type filled in."""
    return (
        _USER_CHART_SECTION
        .replace("{symbol}", symbol)
        .replace("{signal_type}", signal_type)
    )
