"""
BTC wick-sweep analysis.

对过滤后的 BTC A-only 止损单，判断是否属于"wick 扫止损但收盘收回"，
并用 close-based SL 重跑，看反事实结果。
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT    = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from draw_kline.common import SLUG_MAP, load_ohlcv
from replay.scorer import (
    _touch, _obj_touch, _close_crosses,
    RESULT_INVALIDATED, RESULT_TP1_HIT, RESULT_TP1_TP2_HIT,
    RESULT_TP1_UNRESOLVED, RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED,
)

MAT_DIR  = ROOT / "replay_materials"
DATA_DIR = ROOT / "history_data_manager" / "data"
SLUG     = SLUG_MAP["BTC/USDT"]


# ── same filter as aonly_filter_analysis.py ──────────────────────────────────

def passes_filter_btc(score: dict) -> bool:
    result = score.get("result", "")
    if result in (RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED):
        return False
    ap    = score.get("activation_price")
    il    = score.get("invalidation_level")
    t1    = score.get("tp1_level")
    b2act = score.get("bars_to_activation")
    if ap is None or il is None:
        return False
    if b2act is None or b2act < 2:
        return False
    r_dist_pct = abs(ap - il) / ap * 100
    tp1_pct    = abs(t1 - ap) / ap * 100 if t1 is not None else None
    if r_dist_pct < 0.5:
        return False
    if tp1_pct is not None and tp1_pct >= 1.5:
        return False
    return True


# ── close-based SL re-scorer ──────────────────────────────────────────────────

def rescore_close_based(score: dict, df_post: pd.DataFrame) -> dict:
    """
    Re-score from activation bar onward using close-based SL.
    Returns {"result", "tp1_at", "tp2_at", "invalidated_at"}.
    """
    inv_level = score.get("invalidation_level")
    inv_dir   = score.get("inv_dir")   # may be None in stored JSON
    tp1_level = score.get("tp1_level")
    tp2_level = score.get("tp2_level")
    act_ts    = score.get("activated_at")

    if inv_level is None or act_ts is None:
        return {"result": "unknown"}

    # infer inv_dir from activation_price vs invalidation_level
    ap = score.get("activation_price", 0)
    if inv_dir is None:
        inv_dir = "below" if inv_level < ap else "above"

    # slice post-activation bars
    act_ts_pd = pd.Timestamp(act_ts)
    df = df_post[df_post["open_time"] > act_ts_pd].copy().reset_index(drop=True)

    state = "WAITING_FOR_TP1"
    for _, row in df.iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        cl = float(row["close"])
        ts = row["open_time"]

        if state == "WAITING_FOR_TP1":
            invalidated = _close_crosses(cl, inv_level, inv_dir)
            tp1_hit     = tp1_level is not None and _obj_touch(hi, lo, tp1_level)
            if invalidated:
                return {"result": RESULT_INVALIDATED, "invalidated_at": str(ts)}
            if tp1_hit:
                if tp2_level is None:
                    return {"result": RESULT_TP1_HIT, "tp1_at": str(ts)}
                state = "WAITING_FOR_TP2"
                tp1_ts = ts

        elif state == "WAITING_FOR_TP2":
            invalidated = _close_crosses(cl, inv_level, inv_dir)
            tp2_hit     = tp2_level is not None and _obj_touch(hi, lo, tp2_level)
            if invalidated:
                return {"result": RESULT_TP1_HIT, "tp1_at": str(tp1_ts),
                        "invalidated_at": str(ts)}
            if tp2_hit:
                return {"result": RESULT_TP1_TP2_HIT, "tp1_at": str(tp1_ts),
                        "tp2_at": str(ts)}

    if state == "WAITING_FOR_TP2":
        return {"result": RESULT_TP1_UNRESOLVED}
    return {"result": RESULT_NOT_TRIGGERED}


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    wick_cases = []   # touch-invalidated but close held
    clean_cases = []  # touch-invalidated and close also broke

    for d in sorted(MAT_DIR.iterdir()):
        rp = d / "replay_result.json"
        if not rp.exists():
            continue
        data = json.loads(rp.read_text())
        sig  = data.get("signal", {})
        if sig.get("symbol") != "BTC/USDT" or sig.get("grade") != "A":
            continue

        bar_time = pd.Timestamp(sig["bar_time"])
        t_end    = pd.Timestamp("2099-01-01", tz="UTC")
        try:
            df_full = load_ohlcv(DATA_DIR, SLUG, "15m", bar_time, t_end)
        except Exception:
            continue
        df_post = df_full[df_full["open_time"] > bar_time].reset_index(drop=True)

        pb_scores = data.get("playbook_scores", {})
        if not pb_scores:
            ws = data.get("winning_score") or data.get("l1_score")
            if ws:
                pb_scores = {ws.get("hypothesis", "l1"): ws}

        for hyp, score in pb_scores.items():
            if score is None:
                continue
            if score.get("result") != RESULT_INVALIDATED:
                continue
            if not passes_filter_btc(score):
                continue

            inv_level  = score.get("invalidation_level")
            inv_ts_str = score.get("invalidated_at")
            ap         = score.get("activation_price", 0)
            if inv_level is None or inv_ts_str is None:
                continue

            inv_dir = "below" if inv_level < ap else "above"
            inv_ts  = pd.Timestamp(inv_ts_str)

            # find the invalidation bar
            inv_bar = df_post[df_post["open_time"] == inv_ts]
            if inv_bar.empty:
                # try within 1 bar
                inv_bar = df_post[
                    (df_post["open_time"] >= inv_ts - pd.Timedelta("15min")) &
                    (df_post["open_time"] <= inv_ts + pd.Timedelta("15min"))
                ].head(1)
            if inv_bar.empty:
                continue

            row = inv_bar.iloc[0]
            cl  = float(row["close"])
            close_broke = _close_crosses(cl, inv_level, inv_dir)

            case = {
                "pkg":        d.name,
                "hyp":        hyp,
                "inv_ts":     inv_ts_str,
                "inv_level":  inv_level,
                "inv_dir":    inv_dir,
                "bar_open":   str(row["open_time"]),
                "bar_high":   float(row["high"]),
                "bar_low":    float(row["low"]),
                "bar_close":  cl,
                "close_broke": close_broke,
                "score":      score,
                "df_post":    df_post,
            }

            if close_broke:
                clean_cases.append(case)
            else:
                wick_cases.append(case)

    print(f"\n{'='*65}")
    print("BTC wick-sweep 分析")
    print(f"{'='*65}")
    print(f"过滤后 BTC 止损单总数: {len(wick_cases) + len(clean_cases)}")
    print(f"  wick 扫止损（收盘未破）: {len(wick_cases)}")
    print(f"  收盘真正破位:            {len(clean_cases)}")

    if not wick_cases:
        print("\n无 wick 扫单。")
        return

    # wick size stats
    wick_pcts = []
    for c in wick_cases:
        ap = c["score"].get("activation_price", 0)
        if not ap:
            continue
        if c["inv_dir"] == "below":
            # low went below inv_level, close above
            wick_depth = (c["inv_level"] - c["bar_low"]) / ap * 100
        else:
            wick_depth = (c["bar_high"] - c["inv_level"]) / ap * 100
        wick_pcts.append(wick_depth)

    wick_pcts.sort()
    n = len(wick_pcts)
    p = lambda q: wick_pcts[int(q * n / 100)] if n > 0 else 0
    print(f"\n── Wick 超越 SL 的深度（以 activation_price 为基）")
    print(f"  p25={p(25):.3f}%  median={p(50):.3f}%  p75={p(75):.3f}%  "
          f"p90={p(90):.3f}%  max={max(wick_pcts):.3f}%")

    # re-score wick cases with close-based SL
    outcomes = {"TP2": 0, "TP1": 0, "still_SL": 0, "unresolved": 0}
    sl_overshoot_r = []   # how far past SL the close was, in R units

    for c in wick_cases:
        cf = rescore_close_based(c["score"], c["df_post"])
        r  = cf.get("result", "")
        if r == RESULT_TP1_TP2_HIT:
            outcomes["TP2"] += 1
        elif r == RESULT_TP1_HIT:
            outcomes["TP1"] += 1
        elif r == RESULT_TP1_UNRESOLVED:
            outcomes["unresolved"] += 1
        else:
            outcomes["still_SL"] += 1
            # measure how far close was beyond SL
            inv_ts_str = cf.get("invalidated_at")
            inv_level  = c["score"].get("invalidation_level")
            ap         = c["score"].get("activation_price", 0)
            r_dist     = c["score"].get("r_distance")
            if inv_ts_str and inv_level and ap and r_dist and r_dist > 0:
                inv_ts  = pd.Timestamp(inv_ts_str)
                sl_bar  = c["df_post"][c["df_post"]["open_time"] == inv_ts]
                if sl_bar.empty:
                    sl_bar = c["df_post"][
                        (c["df_post"]["open_time"] >= inv_ts - pd.Timedelta("15min")) &
                        (c["df_post"]["open_time"] <= inv_ts + pd.Timedelta("15min"))
                    ].head(1)
                if not sl_bar.empty:
                    close_price = float(sl_bar.iloc[0]["close"])
                    overshoot   = abs(close_price - inv_level) / r_dist
                    sl_overshoot_r.append(overshoot)

    total_wick = len(wick_cases)
    print(f"\n── 如果用 close-based SL，这 {total_wick} 笔的反事实结果")
    print(f"  TP2 命中:   {outcomes['TP2']:3d}  ({outcomes['TP2']/total_wick*100:.1f}%)")
    print(f"  TP1 命中:   {outcomes['TP1']:3d}  ({outcomes['TP1']/total_wick*100:.1f}%)")
    print(f"  仍然止损:   {outcomes['still_SL']:3d}  ({outcomes['still_SL']/total_wick*100:.1f}%)")
    print(f"  unresolved: {outcomes['unresolved']:3d}  ({outcomes['unresolved']/total_wick*100:.1f}%)")

    if sl_overshoot_r:
        sl_overshoot_r.sort()
        n = len(sl_overshoot_r)
        p = lambda q: sl_overshoot_r[int(q * n / 100)]
        # actual loss = -(1 + overshoot_r) since SL is -1R baseline + extra slippage
        losses = [-(1 + v) for v in sl_overshoot_r]
        print(f"\n── 仍然止损的 {n} 笔：close 超出 SL 位的深度（R 单位）")
        print(f"  p25={p(25):.3f}R  median={p(50):.3f}R  p75={p(75):.3f}R  "
              f"p90={p(90):.3f}R  max={max(sl_overshoot_r):.3f}R")
        print(f"  （即实际亏损：-1R 基础上额外多亏了这些）")
        print(f"  实际亏损分布: p25={losses[0]:.3f}R  median={losses[n//2]:.3f}R  "
              f"max={min(losses):.3f}R")


if __name__ == "__main__":
    main()
