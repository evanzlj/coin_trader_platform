"""
计算 BTC SL 加 0.1% buffer 后，每笔止损单的额外亏损（R 单位）。

原框架：1R = activation_price → invalidation_level 的距离
加 buffer 后：SL 推到 invalidation_level ± 0.1%
额外亏损（原 R 单位）= buffer_pct / r_dist_pct
"""
import json
from pathlib import Path

ROOT    = Path(__file__).parent.parent
MAT_DIR = ROOT / "replay_materials"

BUFFER_PCT = 0.1  # %

NOT_TRIGGERED = "not_triggered"
ACT_CANCELLED = "activation_cancelled"
INVALIDATED   = "activated_invalidated"
ACTIVATED     = {"activated_invalidated", "activated_tp1_hit",
                 "activated_tp1_tp2_hit", "activated_tp1_unresolved"}


def passes_filter_btc(score):
    result = score.get("result", "")
    if result in (NOT_TRIGGERED, ACT_CANCELLED):
        return False
    ap    = score.get("activation_price")
    il    = score.get("invalidation_level")
    t1    = score.get("tp1_level")
    b2act = score.get("bars_to_activation")
    if ap is None or il is None or b2act is None or b2act < 2:
        return False
    r_dist_pct = abs(ap - il) / ap * 100
    tp1_pct    = abs(t1 - ap) / ap * 100 if t1 is not None else None
    if r_dist_pct < 0.5:
        return False
    if tp1_pct is not None and tp1_pct >= 1.5:
        return False
    return True


def main():
    stop_r_dists = []   # r_dist_pct for all stop-out trades
    all_r_dists  = []   # r_dist_pct for all filtered activated trades

    for d in sorted(MAT_DIR.iterdir()):
        rp = d / "replay_result.json"
        if not rp.exists():
            continue
        data = json.loads(rp.read_text())
        sig  = data.get("signal", {})
        if sig.get("symbol") != "BTC/USDT" or sig.get("grade") != "A":
            continue

        pb_scores = data.get("playbook_scores", {})
        if not pb_scores:
            ws = data.get("winning_score") or data.get("l1_score")
            if ws:
                pb_scores = {ws.get("hypothesis", "l1"): ws}

        for _, score in pb_scores.items():
            if score is None or score.get("result") not in ACTIVATED:
                continue
            if not passes_filter_btc(score):
                continue
            ap     = score["activation_price"]
            il     = score["invalidation_level"]
            r_dist = abs(ap - il) / ap * 100
            all_r_dists.append(r_dist)
            if score["result"] == INVALIDATED:
                stop_r_dists.append(r_dist)

    stop_r_dists.sort()
    all_r_dists.sort()
    n_stop = len(stop_r_dists)
    n_all  = len(all_r_dists)

    extra_r = [BUFFER_PCT / r for r in stop_r_dists]
    extra_r.sort()

    def p(lst, q):
        return lst[int(q * len(lst) / 100)] if lst else 0

    print(f"\n── BTC 过滤后全量激活单 r_dist_pct 分布（n={n_all}）")
    print(f"  p25={p(all_r_dists,25):.3f}%  median={p(all_r_dists,50):.3f}%  "
          f"p75={p(all_r_dists,75):.3f}%  min={min(all_r_dists):.3f}%  max={max(all_r_dists):.3f}%")

    print(f"\n── 止损单 r_dist_pct 分布（n={n_stop}）")
    print(f"  p25={p(stop_r_dists,25):.3f}%  median={p(stop_r_dists,50):.3f}%  "
          f"p75={p(stop_r_dists,75):.3f}%  min={min(stop_r_dists):.3f}%  max={max(stop_r_dists):.3f}%")

    print(f"\n── 加 {BUFFER_PCT}% buffer 后，每笔止损单额外亏损（原 R 单位）= {BUFFER_PCT}% / r_dist_pct")
    print(f"  p25={p(extra_r,25):.3f}R  median={p(extra_r,50):.3f}R  "
          f"p75={p(extra_r,75):.3f}R  p90={p(extra_r,90):.3f}R  max={max(extra_r):.3f}R")

    total_extra = sum(extra_r)
    avg_extra   = total_extra / n_stop if n_stop else 0
    print(f"  平均额外亏损: {avg_extra:.3f}R / 笔")
    print(f"  {n_stop} 笔止损单总额外亏损: {total_extra:.2f}R")
    print(f"  （全量 {n_all} 笔折算: {total_extra/n_all:.3f}R / 笔）")


if __name__ == "__main__":
    main()
