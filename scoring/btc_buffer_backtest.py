"""
BTC +0.1% SL buffer 回测。

对所有过滤后的 BTC A-only activated playbook，
将 invalidation_level 向外推 activation_price * 0.1%，
用新 SL 重跑 post-activation 评分，按月汇报 S3 绩效。
"""
import json
import sys
from pathlib import Path
from collections import defaultdict

import pandas as pd

ROOT    = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from draw_kline.common import SLUG_MAP, load_ohlcv
from replay.scorer import (
    _obj_touch, _touch,
    RESULT_INVALIDATED, RESULT_TP1_HIT, RESULT_TP1_TP2_HIT,
    RESULT_TP1_UNRESOLVED, RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED,
)

MAT_DIR  = ROOT / "replay_materials"
DATA_DIR = ROOT / "history_data_manager" / "data"
SLUG     = SLUG_MAP["BTC/USDT"]

BUFFER_PCT        = 0.1   # %
FEE_PER_LEG       = 0.06
SLIPPAGE_PER_LEG  = 0.05
ROUND_TRIP_PCT    = 2 * (FEE_PER_LEG + SLIPPAGE_PER_LEG)
FUNDING_PER_8H    = 0.01
BARS_PER_8H       = 32

ACTIVATED = {RESULT_INVALIDATED, RESULT_TP1_HIT, RESULT_TP1_TP2_HIT, RESULT_TP1_UNRESOLVED}


def passes_filter_btc(score):
    if score.get("result") not in ACTIVATED:
        return False
    ap    = score.get("activation_price")
    il    = score.get("invalidation_level")
    t1    = score.get("tp1_level")
    b2act = score.get("bars_to_activation")
    if ap is None or il is None or b2act is None or b2act < 2:
        return False
    r_dist_pct = abs(ap - il) / ap * 100
    tp1_pct    = abs(t1 - ap) / ap * 100 if t1 is not None else None
    return r_dist_pct >= 0.5 and (tp1_pct is None or tp1_pct < 1.5)


def rescore_with_buffer(score: dict, df_post: pd.DataFrame) -> dict:
    """Re-score post-activation with buffered SL, return result dict."""
    ap        = score["activation_price"]
    il        = score["invalidation_level"]
    t1        = score.get("tp1_level")
    t2        = score.get("tp2_level")
    act_ts    = pd.Timestamp(score["activated_at"])

    inv_dir   = "below" if il < ap else "above"
    buffer    = ap * BUFFER_PCT / 100
    new_il    = il - buffer if inv_dir == "below" else il + buffer
    inv_side  = "low" if inv_dir == "below" else "high"

    df = df_post[df_post["open_time"] > act_ts].reset_index(drop=True)

    state  = "TP1"
    tp1_ts = tp2_ts = inv_ts = None

    for _, row in df.iterrows():
        hi = float(row["high"])
        lo = float(row["low"])
        cl = float(row["close"])
        ts = row["open_time"]

        if state == "TP1":
            if _touch(hi, lo, new_il, inv_side):
                inv_ts = ts
                return {"result": RESULT_INVALIDATED, "inv_ts": inv_ts,
                        "new_il": new_il, "new_r_dist": abs(ap - new_il)}
            if t1 is not None and _obj_touch(hi, lo, t1):
                tp1_ts = ts
                if t2 is None:
                    return {"result": RESULT_TP1_HIT, "tp1_ts": tp1_ts,
                            "new_il": new_il, "new_r_dist": abs(ap - new_il)}
                state = "TP2"

        elif state == "TP2":
            if _touch(hi, lo, new_il, inv_side):
                inv_ts = ts
                return {"result": RESULT_TP1_HIT, "tp1_ts": tp1_ts, "inv_ts": inv_ts,
                        "new_il": new_il, "new_r_dist": abs(ap - new_il)}
            if t2 is not None and _obj_touch(hi, lo, t2):
                tp2_ts = ts
                return {"result": RESULT_TP1_TP2_HIT, "tp1_ts": tp1_ts, "tp2_ts": tp2_ts,
                        "new_il": new_il, "new_r_dist": abs(ap - new_il)}

    if state == "TP2":
        return {"result": RESULT_TP1_UNRESOLVED, "tp1_ts": tp1_ts,
                "new_il": new_il, "new_r_dist": abs(ap - new_il)}
    return {"result": RESULT_NOT_TRIGGERED,
            "new_il": new_il, "new_r_dist": abs(ap - new_il)}


def cost_r(new_r_dist: float, ap: float, result: str,
           b_tp1, b_tp2, b_inv) -> float:
    r_dist_pct = new_r_dist / ap * 100
    if r_dist_pct == 0:
        return 0.0
    b_tp1 = b_tp1 or 0; b_tp2 = b_tp2 or 0; b_inv = b_inv or 0
    if result == RESULT_INVALIDATED:
        hold = b_inv
    elif result == RESULT_TP1_HIT:
        hold = b_tp1 + b_inv
    elif result == RESULT_TP1_TP2_HIT:
        hold = b_tp1 + b_tp2
    else:
        hold = b_tp1
    return ROUND_TRIP_PCT / r_dist_pct + (hold / BARS_PER_8H) * FUNDING_PER_8H / r_dist_pct


def compute_r_s3(orig_score: dict, new_result: dict) -> float:
    result    = new_result["result"]
    new_r_dist = new_result.get("new_r_dist")
    ap        = orig_score["activation_price"]
    t1        = orig_score.get("tp1_level")
    t2        = orig_score.get("tp2_level")

    if result in (RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED):
        return 0.0
    if new_r_dist is None or new_r_dist == 0:
        return 0.0

    tp1_r = abs(t1 - ap) / new_r_dist if t1 is not None else None
    tp2_r = abs(t2 - ap) / new_r_dist if t2 is not None else None

    b_tp1 = orig_score.get("bars_to_tp1")
    b_tp2 = orig_score.get("bars_to_tp2")
    b_inv = orig_score.get("bars_to_invalidation")

    if result == RESULT_INVALIDATED:
        raw = -1.0
    elif result == RESULT_TP1_UNRESOLVED:
        return 0.0
    elif result == RESULT_TP1_TP2_HIT:
        raw = 0.5 * (tp1_r or 0) + 0.5 * (tp2_r or 0)
    elif result == RESULT_TP1_HIT:
        raw = 0.5 * (tp1_r or 0)
    else:
        raw = -1.0

    return raw - cost_r(new_r_dist, ap, result, b_tp1, b_tp2, b_inv)


def _pct(a, b): return f"{a/b*100:.1f}%" if b > 0 else "—"
def _fmt(v):    return f"{v:+.3f}" if v is not None else "—"


def main():
    by_month_orig = defaultdict(list)   # original touch-based S3 R
    by_month_buf  = defaultdict(list)   # buffered S3 R

    for d in sorted(MAT_DIR.iterdir()):
        rp = d / "replay_result.json"
        if not rp.exists():
            continue
        data = json.loads(rp.read_text())
        sig  = data.get("signal", {})
        if sig.get("symbol") != "BTC/USDT" or sig.get("grade") != "A":
            continue

        bar_time = pd.Timestamp(sig["bar_time"])
        month    = str(bar_time)[:7]
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

        for _, score in pb_scores.items():
            if score is None or not passes_filter_btc(score):
                continue

            # original touch-based R (from stored r_s3_adj is not here,
            # recompute inline)
            orig_result = score.get("result", "")
            ap  = score["activation_price"]
            il  = score["invalidation_level"]
            t1  = score.get("tp1_level")
            t2  = score.get("tp2_level")
            r_d = abs(ap - il)
            r_d_pct = r_d / ap * 100
            tp1_r = abs(t1 - ap) / r_d if t1 and r_d else None
            tp2_r = abs(t2 - ap) / r_d if t2 and r_d else None
            b_tp1 = score.get("bars_to_tp1")
            b_tp2 = score.get("bars_to_tp2")
            b_inv = score.get("bars_to_invalidation")

            if orig_result == RESULT_INVALIDATED:
                orig_raw = -1.0
            elif orig_result == RESULT_TP1_UNRESOLVED:
                orig_raw = None
            elif orig_result == RESULT_TP1_TP2_HIT:
                orig_raw = 0.5*(tp1_r or 0) + 0.5*(tp2_r or 0)
            elif orig_result == RESULT_TP1_HIT:
                orig_raw = 0.5*(tp1_r or 0)
            else:
                orig_raw = 0.0

            if orig_raw is not None:
                cr = cost_r(r_d, ap, orig_result, b_tp1, b_tp2, b_inv)
                by_month_orig[month].append(orig_raw - cr)

            # buffered re-score
            new_res = rescore_with_buffer(score, df_post)
            r_buf   = compute_r_s3(score, new_res)
            if new_res["result"] != RESULT_TP1_UNRESOLVED:
                by_month_buf[month].append(r_buf)

    # print comparison table
    months = sorted(set(list(by_month_orig.keys()) + list(by_month_buf.keys())))

    print(f"\n{'='*72}")
    print(f"BTC A-only 过滤后：原始 touch SL vs +{BUFFER_PCT}% buffer SL（S3 adj）")
    print(f"{'='*72}")

    hdr = (f"  {'月份':<10}  {'笔(orig)':>8}  {'期望(orig)':>10}  {'累计(orig)':>10}  "
           f"{'笔(buf)':>7}  {'期望(buf)':>9}  {'累计(buf)':>9}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    cum_o = cum_b = 0.0
    for m in months:
        ro = by_month_orig.get(m, [])
        rb = by_month_buf.get(m, [])
        eo = sum(ro)/len(ro) if ro else None
        eb = sum(rb)/len(rb) if rb else None
        if eo is not None: cum_o += sum(ro)
        if eb is not None: cum_b += sum(rb)
        print(f"  {m:<10}  {len(ro):>8}  {_fmt(eo):>10}  {cum_o:>+10.2f}  "
              f"{len(rb):>7}  {_fmt(eb):>9}  {cum_b:>+9.2f}")

    print("  " + "-" * (len(hdr) - 2))
    all_o = [r for rs in by_month_orig.values() for r in rs]
    all_b = [r for rs in by_month_buf.values()  for r in rs]
    eo = sum(all_o)/len(all_o) if all_o else None
    eb = sum(all_b)/len(all_b) if all_b else None
    print(f"  {'合计':<10}  {len(all_o):>8}  {_fmt(eo):>10}  {sum(all_o):>+10.2f}  "
          f"{len(all_b):>7}  {_fmt(eb):>9}  {sum(all_b):>+9.2f}")


if __name__ == "__main__":
    main()
