"""
A-only filtered performance analysis (touch-based SL, all-playbook mode).

全剧本模式：每个信号下所有激活的 playbook 各自作为一笔独立交易。
过滤规则来自 aonly_research_summary_20260608.md：
  BTC: r_dist >= 0.5%  + tp1_pct < 1.5%  + b2act >= 2
  ETH: r_dist >= 1.5%  + tp1_pct NOT in [1%, 2%)  + b2act >= 2
  BNB: 0.3% <= r_dist <= 1.0%  + b2act >= 2
  SOL: r_dist >= 1.5%  + b2act >= 2

数据源：replay_materials/*/replay_result.json（A-only）
"""
import json
import sys
from pathlib import Path
from collections import defaultdict

ROOT      = Path(__file__).parent.parent
MAT_DIR   = ROOT / "replay_materials"
COST_FILE = ROOT / "replay_report" / "per_signal.csv"  # for cost params reference

# Cost params (同 replay_report.py 默认值)
FEE_PER_LEG       = 0.06    # %
SLIPPAGE_PER_LEG  = 0.05    # %
ROUND_TRIP_PCT    = 2 * (FEE_PER_LEG + SLIPPAGE_PER_LEG)   # 0.22%
FUNDING_PER_8H    = 0.01    # %
BARS_PER_8H       = 32

NOT_TRIGGERED      = "not_triggered"
ACT_CANCELLED      = "activation_cancelled"
INVALIDATED        = "activated_invalidated"
TP1_HIT            = "activated_tp1_hit"
TP1_TP2_HIT        = "activated_tp1_tp2_hit"
TP1_UNRESOLVED     = "activated_tp1_unresolved"
ACTIVATED_RESULTS  = {INVALIDATED, TP1_HIT, TP1_TP2_HIT, TP1_UNRESOLVED}
TP1_RESULTS        = {TP1_HIT, TP1_TP2_HIT, TP1_UNRESOLVED}


def to_f(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (TypeError, ValueError):
        return None


def cost_r(score: dict) -> float:
    ap     = to_f(score.get("activation_price"))
    r_dist = to_f(score.get("r_distance"))
    if not ap or not r_dist or r_dist == 0 or ap == 0:
        return 0.0
    r_dist_pct = r_dist / ap * 100
    if r_dist_pct == 0:
        return 0.0
    result = score.get("result", "")
    b_tp1  = to_f(score.get("bars_to_tp1")) or 0
    b_tp2  = to_f(score.get("bars_to_tp2")) or 0
    b_inv  = to_f(score.get("bars_to_invalidation")) or 0
    if result == INVALIDATED:
        hold = b_inv
    elif result == TP1_HIT:
        hold = b_tp1 + b_inv
    elif result == TP1_TP2_HIT:
        hold = b_tp1 + b_tp2
    elif result == TP1_UNRESOLVED:
        hold = b_tp1
    else:
        hold = 0
    fee_slip_r = ROUND_TRIP_PCT / r_dist_pct
    funding_r  = (hold / BARS_PER_8H) * FUNDING_PER_8H / r_dist_pct
    return fee_slip_r + funding_r


def compute_r_s3(score: dict) -> float:
    """S3: half at TP1, move BE, half at TP2. Returns adjusted R (after cost)."""
    result = score.get("result", NOT_TRIGGERED)
    ap     = to_f(score.get("activation_price"))
    il     = to_f(score.get("invalidation_level"))
    t1     = to_f(score.get("tp1_level"))
    t2     = to_f(score.get("tp2_level"))
    inv_at = score.get("invalidated_at")

    if result in (NOT_TRIGGERED, ACT_CANCELLED):
        return 0.0
    if ap is None or il is None:
        return 0.0
    r_dist = abs(ap - il)
    if r_dist == 0:
        return 0.0

    tp1_r = abs(t1 - ap) / r_dist if t1 is not None else None
    tp2_r = abs(t2 - ap) / r_dist if t2 is not None else None

    has_tp1 = result in TP1_RESULTS
    has_tp2 = result == TP1_TP2_HIT

    if result == INVALIDATED:
        raw = -1.0
    elif result == TP1_UNRESOLVED:
        return 0.0   # exclude from denominator -> skip via caller
    elif has_tp2:
        raw = 0.5 * (tp1_r or 0) + 0.5 * (tp2_r or 0)
    elif has_tp1:
        raw = 0.5 * (tp1_r or 0) + 0.0   # second half exits at BE
    else:
        raw = -1.0

    return raw - cost_r(score)


def passes_filter(sym: str, score: dict) -> bool:
    """Filter one activated playbook score."""
    result = score.get("result", "")
    if result in (NOT_TRIGGERED, ACT_CANCELLED):
        return False

    ap    = to_f(score.get("activation_price"))
    il    = to_f(score.get("invalidation_level"))
    t1    = to_f(score.get("tp1_level"))
    b2act = to_f(score.get("bars_to_activation"))

    if ap is None or il is None:
        return False
    if b2act is None or b2act < 2:
        return False

    r_dist_pct = abs(ap - il) / ap * 100
    tp1_pct    = abs(t1 - ap) / ap * 100 if t1 is not None else None

    if sym == "BTC/USDT":
        if r_dist_pct < 0.5:
            return False
        if tp1_pct is not None and tp1_pct >= 1.5:
            return False

    elif sym == "ETH/USDT":
        if r_dist_pct < 1.5:
            return False
        if tp1_pct is not None and 1.0 <= tp1_pct < 2.0:
            return False

    elif sym == "BNB/USDT":
        if r_dist_pct < 0.3 or r_dist_pct > 1.0:
            return False

    elif sym == "SOL/USDT":
        if r_dist_pct < 1.5:
            return False

    return True


def load_all_trades():
    """
    Load all activated playbook scores from A-only replay_result.json files.
    Returns list of dicts: {symbol, month, result, score_dict}
    """
    trades = []
    for d in sorted(MAT_DIR.iterdir()):
        rp = d / "replay_result.json"
        if not rp.exists():
            continue
        try:
            data = json.loads(rp.read_text())
        except Exception:
            continue

        sig = data.get("signal", {})
        if sig.get("grade") != "A":
            continue

        sym      = sig.get("symbol", "")
        bar_time = sig.get("bar_time", "")
        month    = bar_time[:7] if len(bar_time) >= 7 else "unknown"

        pb_scores = data.get("playbook_scores", {})
        if not pb_scores:
            # legacy: fall back to winning_score
            ws = data.get("winning_score") or data.get("l1_score")
            if ws:
                pb_scores = {ws.get("hypothesis", "l1"): ws}

        for hyp, score in pb_scores.items():
            if score is None:
                continue
            if score.get("result") not in ACTIVATED_RESULTS:
                continue
            trades.append({
                "symbol": sym,
                "month":  month,
                "hyp":    hyp,
                "score":  score,
                "pkg":    d.name,
            })

    return trades


def _pct(a, b):
    return f"{a/b*100:.1f}%" if b > 0 else "—"


def _fmt(v, d=3):
    return f"{v:+.{d}f}" if v is not None else "—"


def stats(trades):
    n     = len(trades)
    inv   = sum(1 for t in trades if t["score"]["result"] == INVALIDATED)
    tp2   = sum(1 for t in trades if t["score"]["result"] == TP1_TP2_HIT)
    rs    = []
    for t in trades:
        r = compute_r_s3(t["score"])
        if t["score"]["result"] != TP1_UNRESOLVED:
            rs.append(r)
    exp    = sum(rs) / len(rs) if rs else None
    total  = sum(rs) if rs else 0.0
    return {"n": n, "sl_pct": _pct(inv, n), "tp2_pct": _pct(tp2, n),
            "exp": exp, "total": total}


def print_table(sym, by_month):
    months  = sorted(by_month.keys())
    all_t   = []
    cum     = 0.0
    hdr = f"  {'月份':<10}  {'笔数':>4}  {'止损%':>7}  {'TP2%':>6}  {'期望R':>8}  {'累计R':>8}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for m in months:
        t = by_month[m]
        all_t.extend(t)
        s = stats(t)
        if s["exp"] is not None:
            cum += s["total"]
        print(f"  {m:<10}  {s['n']:>4}  {s['sl_pct']:>7}  {s['tp2_pct']:>6}  "
              f"{_fmt(s['exp']):>8}  {cum:>+8.2f}")
    s = stats(all_t)
    print("  " + "-" * (len(hdr) - 2))
    print(f"  {'合计':<10}  {s['n']:>4}  {s['sl_pct']:>7}  {s['tp2_pct']:>6}  "
          f"{_fmt(s['exp']):>8}  {s['total']:>+8.2f}")
    return all_t


def main():
    all_trades = load_all_trades()
    print(f"\n{'='*65}")
    print("A-only 过滤分析 — touch-based SL（全剧本模式）")
    print(f"{'='*65}")
    print(f"全量激活 playbook 数: {len(all_trades)}")

    symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
    grand_all = []

    for sym in symbols:
        sym_trades = [t for t in all_trades if t["symbol"] == sym]
        filtered   = [t for t in sym_trades if passes_filter(sym, t["score"])]
        by_month   = defaultdict(list)
        for t in filtered:
            by_month[t["month"]].append(t)

        print(f"\n### {sym}  (激活:{len(sym_trades)} → 过滤后:{len(filtered)})")
        filt_all = print_table(sym, by_month)
        grand_all.extend(filt_all)

    s = stats(grand_all)
    print(f"\n{'='*65}")
    print(f"四币合计  n={s['n']}  止损={s['sl_pct']}  TP2={s['tp2_pct']}  "
          f"期望R={_fmt(s['exp'])}  累计R={s['total']:+.2f}")

    print(f"\n── 旧结论（close-based SL）对比")
    old = [("BTC/USDT", 84, 0.236, 19.8),
           ("ETH/USDT", 43, 0.328, 14.1),
           ("BNB/USDT", 39, 0.406, 15.8),
           ("SOL/USDT", 43, 0.151,  6.5)]
    print(f"  {'币种':<12}  {'旧笔数':>6}  {'旧期望R':>8}  {'旧累计R':>8}")
    print("  " + "-" * 45)
    for sym, n, e, t in old:
        print(f"  {sym:<12}  {n:>6}  {e:>+8.3f}  {t:>+8.1f}R")
    print(f"  {'合计':<12}  {209:>6}  {'':>8}  {56.2:>+8.1f}R")


if __name__ == "__main__":
    main()
