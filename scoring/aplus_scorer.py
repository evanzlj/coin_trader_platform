"""
A+ replay scorer with post-T0 flow enrichment.

For each replay_materials package that has a vlm_response.json:
  - Scores every playbook's activation_rule against 15m OHLCV bars
  - Classifies post-T0 flow direction (buying / neutral / selling)
  - Outputs one row per playbook

Outcome semantics:
  - not_triggered : primary_touch never hit within available data
  - cancelled     : touch hit but cancel side won the close-cross race
  - win           : activated → objective (TP1) hit before invalidation close
  - loss          : activated → invalidation close before objective
  - open          : activated but neither objective nor invalidation hit yet
  - no_rule       : playbook has activation_rule=null (low/ruled_out/no_trade)
  - invalid_rule  : VLM wrote zero or missing levels — unusable

Output: scoring/aplus_scores.csv
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from scoring.flow_classifier import (
    FLOW_WINDOW_BARS,
    classify_flow,
    compute_thresholds,
    get_post_t0_cum_imbalance,
)

logger = logging.getLogger(__name__)

DATA_DIR   = ROOT / "history_data_manager" / "data"
REPLAY_DIR = ROOT / "replay_materials"
OUT_PATH   = ROOT / "scoring" / "aplus_scores.csv"


# ── Expected flow per hypothesis ──────────────────────────────────────────────

_EXPECTED_FLOW: dict[str, dict[str, str]] = {
    # hypothesis → {near_support: expected, near_resistance: expected}
    "UPSIDE_ACCEPTANCE_CONTINUATION":   {"near_support": "buying",  "near_resistance": "buying"},
    "DOWNSIDE_ACCEPTANCE_CONTINUATION": {"near_support": "selling", "near_resistance": "selling"},
    "SWEEP_THEN_RECLAIM":               {"near_support": "buying",  "near_resistance": "selling"},
    "SUPPORT_REACTION_BOUNCE":          {"near_support": "buying",  "near_resistance": "selling"},
    "WEAK_REACTION_FAILED_RECLAIM":     {"near_support": "buying",  "near_resistance": "selling"},
    "FAILED_REACTION_BREAKDOWN":        {"near_support": "selling", "near_resistance": "buying"},
    "CHOP_WAIT":                        {"near_support": "no_trade","near_resistance": "no_trade"},
    "AMBIGUOUS_WAIT":                   {"near_support": "no_trade","near_resistance": "no_trade"},
}

def get_expected_flow(hypothesis: str, structure_side: str) -> str:
    return _EXPECTED_FLOW.get(hypothesis, {}).get(structure_side, "any")


# ── OHLCV loader ──────────────────────────────────────────────────────────────

def _load_post_t0_ohlcv(symbol: str, bar_time: pd.Timestamp) -> pd.DataFrame:
    slug = symbol.replace("/", "").lower()
    path = DATA_DIR / "ohlcv" / f"{slug}_15m.csv"
    df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
    for col in ("open_time", "close_time"):
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        else:
            df[col] = df[col].dt.tz_convert("UTC")
    return df[df["open_time"] > bar_time].reset_index(drop=True)


# ── Activation rule scorer ────────────────────────────────────────────────────

def _crosses(close: float, direction: str, level: float) -> bool:
    if direction == "above":
        return close > level
    if direction == "below":
        return close < level
    return False


def score_activation_rule(activation_rule: dict, df_post: pd.DataFrame) -> dict:
    """
    Mechanically score one activation_rule against post-T0 15m bars.

    Returns dict with keys:
        activation_status, outcome, activation_bar_offset, outcome_bar_offset
    """
    base = {"activation_bar_offset": None, "outcome_bar_offset": None}

    if df_post.empty:
        return {"activation_status": "no_data", "outcome": "no_data", **base}

    pt   = activation_rule.get("primary_touch", {})
    act  = activation_rule.get("activates_if_close_crosses", {})
    can  = activation_rule.get("cancels_if_close_crosses_first", {})
    inv  = activation_rule.get("invalidation_after_activation", {})
    objs = activation_rule.get("objectives", [])
    direction = activation_rule.get("direction_if_activated", "long")

    pt_level = float(pt.get("level") or 0)
    pt_side  = pt.get("side", "low")
    act_lv   = float(act.get("level") or 0)
    act_dir  = act.get("dir", "above")
    can_lv   = float(can.get("level") or 0)
    can_dir  = can.get("dir", "below")
    inv_lv   = float(inv.get("level") or 0)
    inv_dir  = inv.get("dir", "below")
    obj_lv   = float(objs[0].get("level") or 0) if objs else 0

    if pt_level == 0 or obj_lv == 0:
        return {"activation_status": "invalid_rule", "outcome": "invalid_rule", **base}

    bars = df_post.reset_index(drop=True)

    # Phase 1 — find primary touch
    touch_i = None
    for i, row in bars.iterrows():
        if pt_side == "high" and row["high"] >= pt_level:
            touch_i = i; break
        if pt_side == "low"  and row["low"]  <= pt_level:
            touch_i = i; break

    if touch_i is None:
        return {"activation_status": "not_triggered", "outcome": "not_triggered", **base}

    # Phase 2 — close-cross race after touch
    activated = False
    act_bar_i = None
    for i, row in bars.loc[touch_i:].iterrows():
        c_act = _crosses(row["close"], act_dir, act_lv)
        c_can = _crosses(row["close"], can_dir, can_lv)
        if c_act and c_can:
            return {"activation_status": "cancelled", "outcome": "not_triggered",
                    "activation_bar_offset": int(i), "outcome_bar_offset": None}
        if c_act:
            activated = True; act_bar_i = int(i); break
        if c_can:
            return {"activation_status": "cancelled", "outcome": "not_triggered",
                    "activation_bar_offset": int(i), "outcome_bar_offset": None}

    if not activated:
        return {"activation_status": "not_triggered", "outcome": "not_triggered", **base}

    # Phase 3 — objective touch vs invalidation close race
    for i, row in bars.loc[act_bar_i:].iterrows():
        obj_hit = (
            (direction == "long"  and row["high"] >= obj_lv) or
            (direction == "short" and row["low"]  <= obj_lv)
        )
        inv_hit = inv_lv > 0 and _crosses(row["close"], inv_dir, inv_lv)

        if obj_hit and inv_hit:
            # Same candle conflict → invalidation wins
            return {"activation_status": "triggered", "outcome": "loss",
                    "activation_bar_offset": act_bar_i, "outcome_bar_offset": int(i)}
        if obj_hit:
            return {"activation_status": "triggered", "outcome": "win",
                    "activation_bar_offset": act_bar_i, "outcome_bar_offset": int(i)}
        if inv_hit:
            return {"activation_status": "triggered", "outcome": "loss",
                    "activation_bar_offset": act_bar_i, "outcome_bar_offset": int(i)}

    return {"activation_status": "triggered", "outcome": "open",
            "activation_bar_offset": act_bar_i, "outcome_bar_offset": None}


def score_playbook(playbook: dict, df_post: pd.DataFrame) -> dict:
    plan   = playbook.get("conditional_trade_plan", {})
    status = plan.get("current_status", "")
    ar     = plan.get("activation_rule")

    if ar is None or status == "NOT_APPLICABLE_FOR_LOW_OR_RULED_OUT":
        return {"activation_status": "no_rule", "outcome": "no_rule",
                "activation_bar_offset": None, "outcome_bar_offset": None}

    return score_activation_rule(ar, df_post)


# ── Main ──────────────────────────────────────────────────────────────────────

def score_all(replay_dir: Path = REPLAY_DIR) -> pd.DataFrame:
    pkg_dirs = sorted(
        d for d in replay_dir.iterdir()
        if d.is_dir() and "Aplus" in d.name
    )

    # Pass 1: collect cum_imbalance values for threshold calibration
    logger.info("Pass 1: computing flow values for %d packages", len(pkg_dirs))
    meta = []
    for pkg_dir in pkg_dirs:
        sig_path = pkg_dir / "signal.json"
        vlm_path = pkg_dir / "vlm_response.json"
        if not sig_path.exists() or not vlm_path.exists():
            continue
        sig = json.loads(sig_path.read_text())
        bar_time = pd.Timestamp(sig["bar_time"], tz="UTC")
        cum_imb, n_bars = get_post_t0_cum_imbalance(sig["symbol"], bar_time, DATA_DIR)
        meta.append((pkg_dir, sig, cum_imb, n_bars))

    if not meta:
        logger.warning("No packages with vlm_response.json found — run openclaw first")
        return pd.DataFrame()

    low_thresh, high_thresh = compute_thresholds([m[2] for m in meta])
    logger.info(
        "Flow thresholds (p33/p67): low=%.4f, high=%.4f  (n=%d signals)",
        low_thresh, high_thresh, len(meta),
    )

    # Pass 2: score each package
    logger.info("Pass 2: scoring playbooks...")
    rows = []
    skipped_no_vlm = sum(
        1 for d in pkg_dirs
        if not (d / "vlm_response.json").exists()
    )

    for pkg_dir, sig, cum_imb, n_bars in meta:
        try:
            vlm = json.loads((pkg_dir / "vlm_response.json").read_text())
        except Exception as e:
            logger.warning("vlm parse error %s: %s", pkg_dir.name, e)
            continue

        bar_time     = pd.Timestamp(sig["bar_time"], tz="UTC")
        symbol       = sig["symbol"]
        structure_side = sig.get("structure_side", "unknown")

        imp = vlm.get("a_plus_impulse_assessment", {})
        impulse_phase = imp.get("impulse_phase", "unknown")
        cvd_read = imp.get("visible_cvd_read", "")
        cvd_rel  = imp.get("cvd_price_relationship", "")

        flow_dir = classify_flow(cum_imb, low_thresh, high_thresh)

        try:
            df_post = _load_post_t0_ohlcv(symbol, bar_time)
        except Exception as e:
            logger.warning("ohlcv load error %s: %s", pkg_dir.name, e)
            continue

        playbooks = vlm.get("playbooks", [])
        for pb in playbooks:
            hypothesis   = pb.get("hypothesis", "unknown")
            plausibility = pb.get("plausibility", "unknown")
            exp_flow     = get_expected_flow(hypothesis, structure_side)
            flow_confirmed = (
                exp_flow in (flow_dir, "any")
            ) if exp_flow != "no_trade" else (flow_dir == "neutral")

            score = score_playbook(pb, df_post)

            rows.append({
                "pkg":               pkg_dir.name,
                "symbol":            symbol,
                "bar_time":          bar_time.isoformat(),
                "structure_side":    structure_side,
                "impulse_phase":     impulse_phase,
                "cvd_read":          cvd_read[:80] if cvd_read else "",
                "cvd_price_rel":     cvd_rel,
                "cum_flow_8bar":     round(cum_imb, 4) if not np.isnan(cum_imb) else None,
                "n_flow_bars":       n_bars,
                "flow_direction":    flow_dir,
                "hypothesis":        hypothesis,
                "plausibility":      plausibility,
                "expected_flow":     exp_flow,
                "flow_confirmed":    flow_confirmed,
                **score,
            })

    df = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    logger.info(
        "Scored %d playbook rows from %d packages (%d skipped — no vlm_response yet)",
        len(df), len(meta), skipped_no_vlm,
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    score_all()
