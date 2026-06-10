"""
Mechanical replay scorer for a single playbook's activation_rule.

Scoring logic (from prompt contract):
  1. WAITING_FOR_PRIMARY_TOUCH
       side="low"  → bar.low  <= level
       side="high" → bar.high >= level
  2. WAITING_FOR_ACTIVATION  (race after primary touch)
       activates_if_close_crosses: dir="above" → close > level
                                   dir="below" → close < level
       cancels_if_close_crosses_first: same logic, checked on same bar
       → whichever triggers first wins; same bar → cancels wins
  3. ACTIVATED  (race to outcome)
       objective: any objective level touched (bar.low <= level or bar.high >= level)
       invalidation_after_activation: dir="above" → bar.high >= level (touch-based)
                                      dir="below" → bar.low  <= level (touch-based)
       → same bar → invalidation wins
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


RESULT_NOT_TRIGGERED              = "not_triggered"
RESULT_ACTIVATION_CANCELLED       = "activation_cancelled"
RESULT_INVALIDATED                = "activated_invalidated"          # before TP1
RESULT_TP1_HIT                    = "activated_tp1_hit"              # TP1 hit, then invalidated or no TP2
RESULT_TP1_TP2_HIT                = "activated_tp1_tp2_hit"          # both hit
RESULT_TP1_UNRESOLVED             = "activated_tp1_unresolved"       # TP1 hit, TP2 defined but data ran out


@dataclass
class PlaybookScore:
    hypothesis:          str
    result:              str       # one of the RESULT_* constants above
    primary_touched_at:  Optional[pd.Timestamp] = None
    activated_at:        Optional[pd.Timestamp] = None
    tp1_at:              Optional[pd.Timestamp] = None
    tp2_at:              Optional[pd.Timestamp] = None
    invalidated_at:      Optional[pd.Timestamp] = None
    bars_to_primary:     Optional[int] = None
    bars_to_activation:  Optional[int] = None
    bars_to_tp1:         Optional[int] = None
    bars_to_tp2:         Optional[int] = None
    bars_to_invalidation: Optional[int] = None
    tp1_level:           Optional[float] = None
    tp2_level:           Optional[float] = None
    # — new fields —
    activation_price:    Optional[float] = None
    invalidation_level:  Optional[float] = None
    r_distance:          Optional[float] = None   # abs(activation_price - invalidation_level)
    mfe_pct:             Optional[float] = None   # max favorable excursion from activation, %
    mae_pct:             Optional[float] = None   # max adverse excursion from activation, %
    mfe_r:               Optional[float] = None   # mfe as R multiple
    mae_r:               Optional[float] = None   # mae as R multiple
    prompt_version:      Optional[str]   = None


def _close_crosses(close: float, level: float, direction: str) -> bool:
    if direction == "above":
        return close > level
    if direction == "below":
        return close < level
    return False


def _touch(bar_high: float, bar_low: float, level: float, side: str) -> bool:
    if side == "low":
        return bar_low <= level
    if side == "high":
        return bar_high >= level
    return False


def _obj_touch(bar_high: float, bar_low: float, level: float) -> bool:
    return bar_low <= level <= bar_high


def _mfe_mae(
    act_price: float,
    running_max_high: float,
    running_min_low: float,
    r_dist: Optional[float],
    structure_side: str,
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if act_price <= 0:
        return None, None, None, None
    if structure_side == "near_support":   # implied long
        mfe_dist = max(0.0, running_max_high - act_price)
        mae_dist = max(0.0, act_price - running_min_low)
    else:                                  # near_resistance, implied short
        mfe_dist = max(0.0, act_price - running_min_low)
        mae_dist = max(0.0, running_max_high - act_price)
    mfe_pct = mfe_dist / act_price * 100
    mae_pct = mae_dist / act_price * 100
    mfe_r   = mfe_dist / r_dist if r_dist and r_dist > 0 else None
    mae_r   = mae_dist / r_dist if r_dist and r_dist > 0 else None
    return mfe_pct, mae_pct, mfe_r, mae_r


def score_playbook(
    activation_rule:        dict,
    hypothesis:             str,
    df_post:                pd.DataFrame,
    structure_side:         str = "near_support",
    prompt_version:         Optional[str] = None,
    activation_window_bars: Optional[int] = None,
) -> PlaybookScore:
    """
    Replay activation_rule against post-T0 15m bars.

    activation_window_bars: if set, activation must occur within this many bars
    from T0 (index 0 of df_post). Exceeding the window → activation_cancelled.
    Used by A+ scorer; None for A-only (no cap).

    df_post must have columns: open_time, high, low, close (sorted ascending).
    """
    if activation_rule is None:
        return PlaybookScore(hypothesis=hypothesis, result=RESULT_NOT_TRIGGERED,
                             prompt_version=prompt_version)

    pt        = activation_rule.get("primary_touch", {})
    act_cross = activation_rule.get("activates_if_close_crosses", {})
    can_cross = activation_rule.get("cancels_if_close_crosses_first", {})
    inv       = activation_rule.get("invalidation_after_activation", {})
    objectives = activation_rule.get("objectives", [])

    pt_level  = pt.get("level")
    pt_side   = pt.get("side")
    act_level = act_cross.get("level")
    act_dir   = act_cross.get("dir")
    can_level = can_cross.get("level")
    can_dir   = can_cross.get("dir")
    inv_level = inv.get("level")
    inv_dir   = inv.get("dir")

    tp_levels = [obj.get("level") for obj in objectives if obj.get("level") is not None]
    tp1_level = tp_levels[0] if len(tp_levels) > 0 else None
    tp2_level = tp_levels[1] if len(tp_levels) > 1 else None

    if pt_level is None or pt_side is None:
        return PlaybookScore(hypothesis=hypothesis, result=RESULT_NOT_TRIGGERED,
                             prompt_version=prompt_version)

    df = df_post.sort_values("open_time").reset_index(drop=True)

    state = "WAITING_FOR_PRIMARY_TOUCH"
    primary_bar_idx   = None
    activated_bar_idx = None
    tp1_bar_idx       = None
    primary_ts = activated_ts = tp1_ts = None

    # MAE/MFE tracking (initialised at activation)
    activation_price  = None
    running_max_high  = None
    running_min_low   = None
    r_dist            = None

    for i, row in df.iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        cl = float(row["close"])
        ts = row["open_time"]

        # update running extremes once activated
        if activation_price is not None:
            running_max_high = max(running_max_high, hi)
            running_min_low  = min(running_min_low,  lo)

        if state == "WAITING_FOR_PRIMARY_TOUCH":
            if activation_window_bars is not None and i >= activation_window_bars:
                return PlaybookScore(hypothesis=hypothesis, result=RESULT_ACTIVATION_CANCELLED,
                                     prompt_version=prompt_version)
            if _touch(hi, lo, pt_level, pt_side):
                state = "WAITING_FOR_ACTIVATION"
                primary_bar_idx = i
                primary_ts = ts

        elif state == "WAITING_FOR_ACTIVATION":
            if activation_window_bars is not None and i >= activation_window_bars:
                return PlaybookScore(
                    hypothesis=hypothesis,
                    result=RESULT_ACTIVATION_CANCELLED,
                    primary_touched_at=primary_ts,
                    bars_to_primary=int(primary_bar_idx),
                    prompt_version=prompt_version,
                )
            activated = act_level is not None and _close_crosses(cl, act_level, act_dir)
            cancelled = can_level is not None and _close_crosses(cl, can_level, can_dir)

            if cancelled:
                return PlaybookScore(
                    hypothesis=hypothesis,
                    result=RESULT_ACTIVATION_CANCELLED,
                    primary_touched_at=primary_ts,
                    bars_to_primary=int(primary_bar_idx),
                    prompt_version=prompt_version,
                )
            if activated:
                state = "WAITING_FOR_TP1"
                activated_bar_idx = i
                activated_ts      = ts
                activation_price  = cl
                running_max_high  = hi
                running_min_low   = lo
                r_dist = abs(activation_price - inv_level) if inv_level is not None else None

        elif state == "WAITING_FOR_TP1":
            inv_side    = "low" if inv_dir == "below" else "high"
            invalidated = inv_level is not None and _touch(hi, lo, inv_level, inv_side)
            tp1_hit     = tp1_level is not None and _obj_touch(hi, lo, tp1_level)

            if invalidated:
                mfe_pct, mae_pct, mfe_r, mae_r = _mfe_mae(
                    activation_price, running_max_high, running_min_low, r_dist, structure_side)
                return PlaybookScore(
                    hypothesis=hypothesis,
                    result=RESULT_INVALIDATED,
                    primary_touched_at=primary_ts,
                    activated_at=activated_ts,
                    invalidated_at=ts,
                    bars_to_primary=int(primary_bar_idx),
                    bars_to_activation=int(activated_bar_idx - primary_bar_idx),
                    bars_to_invalidation=int(i - activated_bar_idx),
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    activation_price=activation_price,
                    invalidation_level=inv_level,
                    r_distance=r_dist,
                    mfe_pct=mfe_pct, mae_pct=mae_pct,
                    mfe_r=mfe_r,     mae_r=mae_r,
                    prompt_version=prompt_version,
                )
            if tp1_hit:
                if tp2_level is None:
                    mfe_pct, mae_pct, mfe_r, mae_r = _mfe_mae(
                        activation_price, running_max_high, running_min_low, r_dist, structure_side)
                    return PlaybookScore(
                        hypothesis=hypothesis,
                        result=RESULT_TP1_HIT,
                        primary_touched_at=primary_ts,
                        activated_at=activated_ts,
                        tp1_at=ts,
                        bars_to_primary=int(primary_bar_idx),
                        bars_to_activation=int(activated_bar_idx - primary_bar_idx),
                        bars_to_tp1=int(i - activated_bar_idx),
                        tp1_level=tp1_level,
                        tp2_level=None,
                        activation_price=activation_price,
                        invalidation_level=inv_level,
                        r_distance=r_dist,
                        mfe_pct=mfe_pct, mae_pct=mae_pct,
                        mfe_r=mfe_r,     mae_r=mae_r,
                        prompt_version=prompt_version,
                    )
                state = "WAITING_FOR_TP2"
                tp1_bar_idx = i
                tp1_ts = ts

        elif state == "WAITING_FOR_TP2":
            inv_side    = "low" if inv_dir == "below" else "high"
            invalidated = inv_level is not None and _touch(hi, lo, inv_level, inv_side)
            tp2_hit     = tp2_level is not None and _obj_touch(hi, lo, tp2_level)

            if invalidated:
                mfe_pct, mae_pct, mfe_r, mae_r = _mfe_mae(
                    activation_price, running_max_high, running_min_low, r_dist, structure_side)
                return PlaybookScore(
                    hypothesis=hypothesis,
                    result=RESULT_TP1_HIT,
                    primary_touched_at=primary_ts,
                    activated_at=activated_ts,
                    tp1_at=tp1_ts,
                    invalidated_at=ts,
                    bars_to_primary=int(primary_bar_idx),
                    bars_to_activation=int(activated_bar_idx - primary_bar_idx),
                    bars_to_tp1=int(tp1_bar_idx - activated_bar_idx),
                    bars_to_invalidation=int(i - tp1_bar_idx),
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    activation_price=activation_price,
                    invalidation_level=inv_level,
                    r_distance=r_dist,
                    mfe_pct=mfe_pct, mae_pct=mae_pct,
                    mfe_r=mfe_r,     mae_r=mae_r,
                    prompt_version=prompt_version,
                )
            if tp2_hit:
                mfe_pct, mae_pct, mfe_r, mae_r = _mfe_mae(
                    activation_price, running_max_high, running_min_low, r_dist, structure_side)
                return PlaybookScore(
                    hypothesis=hypothesis,
                    result=RESULT_TP1_TP2_HIT,
                    primary_touched_at=primary_ts,
                    activated_at=activated_ts,
                    tp1_at=tp1_ts,
                    tp2_at=ts,
                    bars_to_primary=int(primary_bar_idx),
                    bars_to_activation=int(activated_bar_idx - primary_bar_idx),
                    bars_to_tp1=int(tp1_bar_idx - activated_bar_idx),
                    bars_to_tp2=int(i - tp1_bar_idx),
                    tp1_level=tp1_level,
                    tp2_level=tp2_level,
                    activation_price=activation_price,
                    invalidation_level=inv_level,
                    r_distance=r_dist,
                    mfe_pct=mfe_pct, mae_pct=mae_pct,
                    mfe_r=mfe_r,     mae_r=mae_r,
                    prompt_version=prompt_version,
                )

    # data exhausted
    if activation_price is not None:
        mfe_pct, mae_pct, mfe_r, mae_r = _mfe_mae(
            activation_price, running_max_high, running_min_low, r_dist, structure_side)
    else:
        mfe_pct = mae_pct = mfe_r = mae_r = None

    if tp1_bar_idx is not None:
        # TP1 hit but TP2 not resolved — unresolved if TP2 was defined
        result = RESULT_TP1_UNRESOLVED if tp2_level is not None else RESULT_TP1_HIT
        return PlaybookScore(
            hypothesis=hypothesis,
            result=result,
            primary_touched_at=primary_ts,
            activated_at=activated_ts,
            tp1_at=tp1_ts,
            bars_to_primary=int(primary_bar_idx),
            bars_to_activation=int(activated_bar_idx - primary_bar_idx),
            bars_to_tp1=int(tp1_bar_idx - activated_bar_idx),
            tp1_level=tp1_level,
            tp2_level=tp2_level,
            activation_price=activation_price,
            invalidation_level=inv_level,
            r_distance=r_dist,
            mfe_pct=mfe_pct, mae_pct=mae_pct,
            mfe_r=mfe_r,     mae_r=mae_r,
            prompt_version=prompt_version,
        )

    return PlaybookScore(
        hypothesis=hypothesis,
        result=RESULT_NOT_TRIGGERED,
        primary_touched_at=primary_ts,
        bars_to_primary=int(primary_bar_idx) if primary_bar_idx is not None else None,
        prompt_version=prompt_version,
    )
