"""
Replay performance report.

Reads all replay_result.json files under replay_materials/ and outputs:
  - Core metrics: Expectancy, Profit Factor, Edge Ratio
  - Funnel conversion rates
  - 5 exit strategy R comparison
  - Grouped breakdown (symbol / grade / side / month / prompt_version)
  - MAE/MFE distribution
  - Monthly trend (ASCII)
  - CSV exports to replay_report/

Usage:
    python3 replay_report.py
    python3 replay_report.py --materials-dir replay_materials
    python3 replay_report.py --out-dir replay_report
"""
from __future__ import annotations
import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

from replay.scorer import (
    RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED,
    RESULT_INVALIDATED, RESULT_TP1_HIT, RESULT_TP1_TP2_HIT,
    RESULT_TP1_UNRESOLVED,
)

_ACTIVATED_RESULTS = {RESULT_INVALIDATED, RESULT_TP1_HIT, RESULT_TP1_TP2_HIT, RESULT_TP1_UNRESOLVED}
_TP1_RESULTS       = {RESULT_TP1_HIT, RESULT_TP1_TP2_HIT, RESULT_TP1_UNRESOLVED}


# ── Cost model ───────────────────────────────────────────────────────────────

@dataclass
class CostParams:
    """
    Conservative cost assumptions for Deepcoin perpetual futures (taker).

    fee_per_leg:              taker fee per leg (%)
    slippage_per_leg:         estimated slippage per leg (%)
    funding_rate_per_8h:      average funding rate per 8h period (%)
    bars_per_funding_period:  15m bars in one 8h funding period (32)
    """
    fee_per_leg:              float = 0.06   # Deepcoin standard taker
    slippage_per_leg:         float = 0.05   # conservative estimate
    funding_rate_per_8h:      float = 0.01   # neutral-market average
    bars_per_funding_period:  int   = 32     # 8h / 15m

    @property
    def round_trip_pct(self) -> float:
        return 2 * (self.fee_per_leg + self.slippage_per_leg)


DEFAULT_COSTS = CostParams()


def _hold_bars(rec: dict) -> int:
    """Bars held from activation to exit (used for funding rate accrual)."""
    result = rec.get("result", "")
    b_tp1 = rec.get("bars_to_tp1") or 0
    b_tp2 = rec.get("bars_to_tp2") or 0
    b_inv = rec.get("bars_to_invalidation") or 0
    if result == RESULT_INVALIDATED:
        return b_inv
    if result == RESULT_TP1_HIT:
        return b_tp1 + b_inv
    if result == RESULT_TP1_TP2_HIT:
        return b_tp1 + b_tp2
    if result == RESULT_TP1_UNRESOLVED:
        return b_tp1
    return 0


def _cost_r(rec: dict, cost: CostParams) -> float:
    """
    Total cost in R units for one activated trade.
    cost_R = round_trip% / r_distance%  +  funding_periods × funding_rate% / r_distance%
    """
    ap     = rec.get("activation_price")
    r_dist = rec.get("r_distance")
    if not ap or not r_dist or r_dist == 0 or ap == 0:
        return 0.0
    r_dist_pct = r_dist / ap * 100
    if r_dist_pct == 0:
        return 0.0
    fee_slip_r = cost.round_trip_pct / r_dist_pct
    hold        = _hold_bars(rec)
    funding_r   = (hold / cost.bars_per_funding_period) * cost.funding_rate_per_8h / r_dist_pct
    return fee_slip_r + funding_r


# ── R calculation ────────────────────────────────────────────────────────────

def _compute_r(rec: dict, strategy: int,
               cost: Optional[CostParams] = None) -> Optional[float]:
    """
    Compute R for one record under the given exit strategy.
    Returns None to exclude the record from R statistics (unresolved).

    Strategies:
      1  — exit all at TP1
      2  — exit all at TP2
      3  — half at TP1, move stop to BE, half at TP2
      4  — half at TP1, keep original stop, half at TP2
      5  — exit all at TP1, activated signals only (not_triggered/cancelled → None)
    """
    result = rec["result"]
    ap     = rec.get("activation_price")
    il     = rec.get("invalidation_level")
    t1     = rec.get("tp1_level")
    t2     = rec.get("tp2_level")
    inv_at = rec.get("invalidated_at")

    # not activated
    if result in (RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED):
        if strategy == 5:
            return None   # excluded from denominator
        return 0.0        # strategies 1-4: 0R idle

    # compute R distances (require activation and invalidation level)
    if ap is None or il is None:
        return None
    r_dist = abs(ap - il)
    if r_dist == 0:
        return None

    tp1_r = abs(t1 - ap) / r_dist if t1 is not None else None
    tp2_r = abs(t2 - ap) / r_dist if t2 is not None else None

    has_tp2_defined = t2 is not None
    has_tp1         = result in _TP1_RESULTS
    has_tp2         = result == RESULT_TP1_TP2_HIT
    tp1_then_inv    = result == RESULT_TP1_HIT and inv_at is not None
    unresolved      = result == RESULT_TP1_UNRESOLVED

    if result == RESULT_INVALIDATED:
        raw = -1.0
        return raw - (cost and _cost_r(rec, cost) or 0.0)

    if strategy in (1, 5):
        raw = (tp1_r or 0.0) if has_tp1 else -1.0
        return raw - (cost and _cost_r(rec, cost) or 0.0)

    if strategy == 2:
        if unresolved:
            return None
        if has_tp2:
            raw = tp2_r or 0.0
        else:
            raw = -1.0
        return raw - (cost and _cost_r(rec, cost) or 0.0)

    if strategy == 3:   # half TP1 + move to BE + half TP2
        if unresolved:
            return None
        half1 = 0.5 * (tp1_r or 0.0)
        if has_tp2:
            raw = half1 + 0.5 * (tp2_r or 0.0)
        elif has_tp1:
            raw = half1 + 0.0   # remaining half exits at BE
        else:
            raw = -1.0
        return raw - (cost and _cost_r(rec, cost) or 0.0)

    if strategy == 4:   # half TP1 + keep original stop + half TP2
        if unresolved:
            return None
        half1 = 0.5 * (tp1_r or 0.0)
        if has_tp2:
            raw = half1 + 0.5 * (tp2_r or 0.0)
        elif has_tp1:
            raw = half1 - 0.5
        else:
            raw = -1.0
        return raw - (cost and _cost_r(rec, cost) or 0.0)

    return None


# ── Aggregation helpers ──────────────────────────────────────────────────────

def _percentile(vals: list[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _max_consec_loss(rs: list[float]) -> int:
    best = cur = 0
    for r in rs:
        if r < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _strategy_stats(records: list[dict], strategy: int,
                    cost: Optional[CostParams] = None) -> dict:
    rs_raw = [_compute_r(r, strategy, cost) for r in records]
    rs     = [r for r in rs_raw if r is not None]
    if not rs:
        return {"n": 0, "total_r": None, "expectancy": None,
                "profit_factor": None, "win_rate": None,
                "avg_win_r": None, "avg_loss_r": None, "max_consec_loss": None}

    wins   = [r for r in rs if r > 0]
    losses = [r for r in rs if r < 0]
    n = len(rs)
    win_rate  = len(wins) / n
    loss_rate = 1.0 - win_rate
    avg_win   = sum(wins)  / len(wins)  if wins   else 0.0
    avg_loss  = abs(sum(losses) / len(losses)) if losses else 0.0
    expectancy    = win_rate * avg_win - loss_rate * avg_loss
    total_loss_r  = abs(sum(losses))
    profit_factor = sum(wins) / total_loss_r if total_loss_r > 0 else None

    return {
        "n":               n,
        "total_r":         round(sum(rs), 3),
        "expectancy":      round(expectancy, 4),
        "profit_factor":   round(profit_factor, 3) if profit_factor is not None else None,
        "win_rate":        round(win_rate, 3),
        "avg_win_r":       round(avg_win, 3),
        "avg_loss_r":      round(avg_loss, 3),
        "max_consec_loss": _max_consec_loss(rs),
    }


def _funnel(records: list[dict]) -> dict:
    n_total    = len(records)
    n_touched  = sum(1 for r in records if r.get("primary_touched_at"))
    n_activated = sum(1 for r in records if r["result"] in _ACTIVATED_RESULTS)
    n_cancelled = sum(1 for r in records if r["result"] == RESULT_ACTIVATION_CANCELLED)
    n_tp1       = sum(1 for r in records if r["result"] in _TP1_RESULTS)
    n_tp2       = sum(1 for r in records if r["result"] == RESULT_TP1_TP2_HIT)
    n_inv_pre   = sum(1 for r in records if r["result"] == RESULT_INVALIDATED)
    n_inv_post  = sum(1 for r in records
                      if r["result"] == RESULT_TP1_HIT and r.get("invalidated_at"))
    n_unresolved = sum(1 for r in records if r["result"] == RESULT_TP1_UNRESOLVED)

    def pct(a, b):
        return f"{a/b*100:.1f}%" if b > 0 else "—"

    return {
        "n_total":      n_total,
        "n_touched":    n_touched,    "pct_touched":    pct(n_touched, n_total),
        "n_activated":  n_activated,  "pct_activated":  pct(n_activated, n_touched),
        "n_cancelled":  n_cancelled,  "pct_cancelled":  pct(n_cancelled, n_touched),
        "n_tp1":        n_tp1,        "pct_tp1":        pct(n_tp1, n_activated),
        "n_tp2":        n_tp2,        "pct_tp2":        pct(n_tp2, n_tp1),
        "n_inv_pre":    n_inv_pre,    "pct_inv_pre":    pct(n_inv_pre, n_activated),
        "n_inv_post":   n_inv_post,   "pct_inv_post":   pct(n_inv_post, n_tp1),
        "n_unresolved": n_unresolved, "pct_unresolved": pct(n_unresolved, n_total),
    }


def _mfe_mae_dist(records: list[dict]) -> dict:
    activated = [r for r in records if r["result"] in _ACTIVATED_RESULTS]
    mfe_rs = [r["mfe_r"] for r in activated if r.get("mfe_r") is not None]
    mae_rs = [r["mae_r"] for r in activated if r.get("mae_r") is not None]
    avg_mfe = sum(mfe_rs) / len(mfe_rs) if mfe_rs else None
    avg_mae = sum(mae_rs) / len(mae_rs) if mae_rs else None
    edge    = round(avg_mfe / avg_mae, 3) if avg_mfe and avg_mae and avg_mae > 0 else None
    return {
        "mfe_r_p25": _percentile(mfe_rs, 25), "mfe_r_p50": _percentile(mfe_rs, 50),
        "mfe_r_p75": _percentile(mfe_rs, 75), "mfe_r_p90": _percentile(mfe_rs, 90),
        "mae_r_p25": _percentile(mae_rs, 25), "mae_r_p50": _percentile(mae_rs, 50),
        "mae_r_p75": _percentile(mae_rs, 75), "mae_r_p90": _percentile(mae_rs, 90),
        "avg_mfe_r": round(avg_mfe, 3) if avg_mfe is not None else None,
        "avg_mae_r": round(avg_mae, 3) if avg_mae is not None else None,
        "edge_ratio": edge,
    }


def _avg_bars(records: list[dict], field: str) -> Optional[float]:
    vals = [r[field] for r in records if r.get(field) is not None]
    return round(sum(vals) / len(vals), 1) if vals else None


# ── Load records ─────────────────────────────────────────────────────────────

def _load_records(materials_dir: Path) -> list[dict]:
    records = []
    for d in sorted(materials_dir.iterdir()):
        rp = d / "replay_result.json"
        if not rp.exists():
            continue
        try:
            with open(rp) as f:
                data = json.load(f)
        except Exception:
            continue

        sig   = data.get("signal", {})
        # Prefer winning_score (multi-playbook); fall back to l1_score for old files
        score = data.get("winning_score") or data.get("l1_score")
        if score is None:
            continue

        bar_time = sig.get("bar_time", "")
        month    = bar_time[:7] if len(bar_time) >= 7 else "unknown"

        rec = {
            "signal_id":       d.name,
            "symbol":          sig.get("symbol", ""),
            "grade":           sig.get("grade", ""),
            "month":           month,
            "side":            sig.get("side", ""),
            "prompt_version":  score.get("prompt_version", "unknown"),
            "l1_playbook":     data.get("l1_hypothesis", ""),
            "winning_playbook": score.get("hypothesis", ""),
            # score fields
            "result":           score.get("result", RESULT_NOT_TRIGGERED),
            "primary_touched_at": score.get("primary_touched_at"),
            "activated_at":     score.get("activated_at"),
            "tp1_at":           score.get("tp1_at"),
            "tp2_at":           score.get("tp2_at"),
            "invalidated_at":   score.get("invalidated_at"),
            "bars_to_primary":  score.get("bars_to_primary"),
            "bars_to_activation": score.get("bars_to_activation"),
            "bars_to_tp1":      score.get("bars_to_tp1"),
            "bars_to_tp2":      score.get("bars_to_tp2"),
            "bars_to_invalidation": score.get("bars_to_invalidation"),
            "tp1_level":        score.get("tp1_level"),
            "tp2_level":        score.get("tp2_level"),
            "activation_price": score.get("activation_price"),
            "invalidation_level": score.get("invalidation_level"),
            "r_distance":       score.get("r_distance"),
            "mfe_pct":          score.get("mfe_pct"),
            "mae_pct":          score.get("mae_pct"),
            "mfe_r":            score.get("mfe_r"),
            "mae_r":            score.get("mae_r"),
        }
        records.append(rec)
    return records


# ── Printing helpers ─────────────────────────────────────────────────────────

def _fmt(v, digits=3):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def _print_strategy_table(records: list[dict],
                          cost: Optional[CostParams] = None) -> None:
    headers = ["Strategy", "n", "Total R", "Expectancy", "Profit F.", "Win%", "MaxConsecLoss"]
    names   = ["1. TP1全出", "2. TP2全出", "3. 半+BE+半", "4. 半+原止损+半", "5. TP1(激活)"]
    rows = []
    for i, name in enumerate(names, 1):
        s = _strategy_stats(records, i, cost)
        rows.append([
            name,
            str(s["n"]),
            _fmt(s["total_r"]),
            _fmt(s["expectancy"], 4),
            _fmt(s["profit_factor"]),
            _fmt(s["win_rate"] * 100 if s["win_rate"] is not None else None, 1) + "%" if s["win_rate"] is not None else "—",
            str(s["max_consec_loss"]) if s["max_consec_loss"] is not None else "—",
        ])
    col_w = [max(len(h), max(len(r[j]) for r in rows)) for j, h in enumerate(headers)]
    fmt   = "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in col_w))
    for r in rows:
        print(fmt.format(*r))


def _print_funnel(f: dict) -> None:
    print(f"  总信号          {f['n_total']}")
    print(f"  primary 触线    {f['n_touched']}  ({f['pct_touched']})")
    print(f"    激活           {f['n_activated']}  ({f['pct_activated']} of touched)")
    print(f"    取消           {f['n_cancelled']}  ({f['pct_cancelled']} of touched)")
    print(f"      TP1 命中     {f['n_tp1']}  ({f['pct_tp1']} of activated)")
    print(f"        TP2 命中   {f['n_tp2']}  ({f['pct_tp2']} of TP1 hit)")
    print(f"        TP1后止损  {f['n_inv_post']}  ({f['pct_inv_post']} of TP1 hit)")
    print(f"      TP1前止损    {f['n_inv_pre']}  ({f['pct_inv_pre']} of activated)")
    print(f"  unresolved       {f['n_unresolved']}  ({f['pct_unresolved']})")


def _print_mfe_mae(d: dict) -> None:
    def r(v): return _fmt(v, 2)
    print(f"  MFE_R   p25={r(d['mfe_r_p25'])}  median={r(d['mfe_r_p50'])}  p75={r(d['mfe_r_p75'])}  p90={r(d['mfe_r_p90'])}  avg={r(d['avg_mfe_r'])}")
    print(f"  MAE_R   p25={r(d['mae_r_p25'])}  median={r(d['mae_r_p50'])}  p75={r(d['mae_r_p75'])}  p90={r(d['mae_r_p90'])}  avg={r(d['avg_mae_r'])}")
    print(f"  Edge Ratio (avg_MFE / avg_MAE) = {r(d['edge_ratio'])}")


def _print_group_table(records: list[dict], groups: list[tuple[str, list[dict]]],
                       cost: Optional[CostParams] = None) -> None:
    cols   = ["Group", "n", "激活率", "TP1率", "TP2率", "止损率", "Expect(S1)", "ProfitF", "EdgeR", "avgBarsTP1"]
    rows   = []
    for label, grp in groups:
        f  = _funnel(grp)
        s1 = _strategy_stats(grp, 1, cost)
        mm = _mfe_mae_dist(grp)
        rows.append([
            label,
            str(f["n_total"]),
            f["pct_activated"],
            f["pct_tp1"],
            f["pct_tp2"],
            f["pct_inv_pre"],
            _fmt(s1["expectancy"], 4),
            _fmt(s1["profit_factor"]),
            _fmt(mm["edge_ratio"]),
            str(_avg_bars(grp, "bars_to_tp1")),
        ])
    col_w = [max(len(h), max(len(r[j]) for r in rows)) for j, h in enumerate(cols)]
    fmt   = "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format(*cols))
    print("  ".join("-" * w for w in col_w))
    for r in rows:
        print(fmt.format(*r))


def _print_monthly_trend(records: list[dict]) -> None:
    months = sorted(set(r["month"] for r in records))
    for month in months:
        grp = [r for r in records if r["month"] == month]
        f   = _funnel(grp)
        act_pct  = int(float(f["pct_activated"].rstrip("%")) if "%" in f["pct_activated"] else 0)
        tp1_pct  = int(float(f["pct_tp1"].rstrip("%"))       if "%" in f["pct_tp1"]       else 0)
        bar_act  = "█" * (act_pct // 5)
        bar_tp1  = "▒" * (tp1_pct // 5)
        print(f"  {month}  激活={f['pct_activated']:>6}  {bar_act}")
        print(f"           TP1={f['pct_tp1']:>6}  {bar_tp1}")


# ── CSV export ───────────────────────────────────────────────────────────────

def _write_per_signal_csv(records: list[dict], path: Path,
                          cost: Optional[CostParams] = None) -> None:
    if not records:
        return
    extra_cols = ["r_s1", "r_s2", "r_s3", "r_s4", "r_s5",
                  "r_s1_adj", "r_s2_adj", "r_s3_adj", "r_s4_adj", "r_s5_adj",
                  "cost_r"]
    fieldnames = list(records[0].keys()) + extra_cols
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rec in records:
            row = dict(rec)
            for i in range(1, 6):
                row[f"r_s{i}"]     = _compute_r(rec, i)
                row[f"r_s{i}_adj"] = _compute_r(rec, i, cost)
            row["cost_r"] = _cost_r(rec, cost) if cost else 0.0
            w.writerow(row)


def _write_summary_csv(groups: list[tuple[str, list[dict]]], path: Path,
                       cost: Optional[CostParams] = None) -> None:
    rows = []
    for label, grp in groups:
        f  = _funnel(grp)
        mm = _mfe_mae_dist(grp)
        row = {"group": label, **f,
               "avg_mfe_r": mm["avg_mfe_r"], "avg_mae_r": mm["avg_mae_r"],
               "edge_ratio": mm["edge_ratio"]}
        for i in range(1, 6):
            s_raw = _strategy_stats(grp, i)
            s_adj = _strategy_stats(grp, i, cost)
            row[f"s{i}_expectancy_raw"] = s_raw["expectancy"]
            row[f"s{i}_expectancy_adj"] = s_adj["expectancy"]
            row[f"s{i}_total_r_raw"]    = s_raw["total_r"]
            row[f"s{i}_total_r_adj"]    = s_adj["total_r"]
        rows.append(row)
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--materials-dir",   default="replay_materials")
    parser.add_argument("--out-dir",         default="replay_report")
    parser.add_argument("--fee",             type=float, default=0.06,
                        help="Taker fee per leg %% (default 0.06)")
    parser.add_argument("--slippage",        type=float, default=0.05,
                        help="Slippage per leg %% (default 0.05)")
    parser.add_argument("--funding",         type=float, default=0.01,
                        help="Funding rate per 8h %% (default 0.01)")
    parser.add_argument("--no-cost",         action="store_true",
                        help="Show raw R without cost deduction")
    args = parser.parse_args()

    materials_dir = Path(args.materials_dir)
    out_dir       = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)

    cost = None if args.no_cost else CostParams(
        fee_per_leg=args.fee,
        slippage_per_leg=args.slippage,
        funding_rate_per_8h=args.funding,
    )

    records = _load_records(materials_dir)
    if not records:
        print("No replay_result.json found.")
        return

    print(f"\n{'='*70}")
    print(f"REPLAY PERFORMANCE REPORT  —  {len(records)} signals scored")
    print(f"{'='*70}\n")

    if cost:
        print(f"── 成本假设（Deepcoin 永续，保守值）")
        print(f"  手续费（单腿）  {cost.fee_per_leg}%   taker 标准档")
        print(f"  滑点（单腿）    {cost.slippage_per_leg}%   保守估计")
        print(f"  来回总计        {cost.round_trip_pct}%")
        print(f"  资金费率        {cost.funding_rate_per_8h}%/8h   中性市场均值")
        print(f"  （所有 R 值已扣除以上成本，越紧止损成本越高）\n")
    else:
        print("  [--no-cost 模式，显示原始 R，未扣除费用]\n")

    # ── Core metrics ─────────────────────────────────────────────────────────
    print("── 核心指标（五种止盈方式）")
    print()
    _print_strategy_table(records, cost)

    print()
    mm_all = _mfe_mae_dist(records)
    print(f"  Edge Ratio (全量): {_fmt(mm_all['edge_ratio'])}")

    # ── Funnel ───────────────────────────────────────────────────────────────
    print(f"\n── 漏斗转化率")
    _print_funnel(_funnel(records))

    # ── MAE/MFE ──────────────────────────────────────────────────────────────
    print(f"\n── MAE/MFE 分布（激活信号）")
    _print_mfe_mae(mm_all)

    # ── Group breakdown ───────────────────────────────────────────────────────
    symbols = sorted(set(r["symbol"] for r in records))
    months  = sorted(set(r["month"]  for r in records))
    prompt_versions = sorted(set(r["prompt_version"] for r in records))

    groups: list[tuple[str, list[dict]]] = [("总体", records)]
    for sym in symbols:
        groups.append((sym, [r for r in records if r["symbol"] == sym]))
    for g in ("A+", "A"):
        groups.append((g, [r for r in records if r["grade"] == g]))
    for side in ("near_support", "near_resistance"):
        groups.append((side, [r for r in records if r["side"] == side]))
    for m in months:
        groups.append((m, [r for r in records if r["month"] == m]))
    for pv in prompt_versions:
        groups.append((f"prompt:{pv}", [r for r in records if r["prompt_version"] == pv]))

    print(f"\n── 分组对比")
    _print_group_table(records, groups, cost)

    # ── Monthly trend ─────────────────────────────────────────────────────────
    print(f"\n── 月度趋势")
    _print_monthly_trend(records)

    # ── CSV export ────────────────────────────────────────────────────────────
    _write_per_signal_csv(records, out_dir / "per_signal.csv", cost)
    _write_summary_csv(groups, out_dir / "summary.csv", cost)
    print(f"\n── CSV 已保存 → {out_dir}/per_signal.csv  {out_dir}/summary.csv")


if __name__ == "__main__":
    main()
