"""
Generate a self-contained HTML replay performance report.

Usage:
    python3 replay_report_html.py
    python3 replay_report_html.py --out replay_report/report.html
    python3 replay_report_html.py --no-cost
    open replay_report/report.html
"""
from __future__ import annotations
import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))

from replay_report import (
    CostParams, DEFAULT_COSTS,
    _load_records, _funnel, _strategy_stats, _mfe_mae_dist,
    _avg_bars, _compute_r, _cost_r,
    RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED,
    RESULT_INVALIDATED, RESULT_TP1_HIT, RESULT_TP1_TP2_HIT, RESULT_TP1_UNRESOLVED,
    _ACTIVATED_RESULTS, _TP1_RESULTS,
)


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _color_r(v: Optional[float]) -> str:
    if v is None:
        return "#888"
    if v > 0.1:
        return "#4ade80"
    if v < -0.05:
        return "#f87171"
    return "#facc15"


def _fmt(v, digits=3, suffix=""):
    if v is None:
        return "<span style='color:#555'>—</span>"
    if isinstance(v, float):
        return f"{v:.{digits}f}{suffix}"
    return str(v) + suffix


def _pct_bar(pct_str: str, color: str = "#60a5fa", width: int = 120) -> str:
    try:
        val = float(pct_str.rstrip("%"))
    except Exception:
        return pct_str
    bar_w = int(val / 100 * width)
    return (
        f"<div style='display:flex;align-items:center;gap:6px'>"
        f"<div style='width:{width}px;background:#1e293b;border-radius:3px;height:10px'>"
        f"<div style='width:{bar_w}px;background:{color};height:10px;border-radius:3px'></div></div>"
        f"<span>{pct_str}</span></div>"
    )


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0f172a; color: #e2e8f0; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 13px; padding: 24px; }
h1 { color: #f8fafc; font-size: 20px; margin-bottom: 4px; }
h2 { color: #94a3b8; font-size: 13px; font-weight: normal; margin-bottom: 24px; }
h3 { color: #7dd3fc; font-size: 14px; margin: 32px 0 12px; border-left: 3px solid #3b82f6; padding-left: 10px; }
table { border-collapse: collapse; width: 100%; margin-bottom: 8px; }
th { background: #1e293b; color: #94a3b8; padding: 8px 12px; text-align: left; font-weight: normal; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }
td { padding: 7px 12px; border-bottom: 1px solid #1e293b; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #1e293b44; }
.card { background: #1e293b; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; margin-bottom: 24px; }
.metric { background: #1e293b; border-radius: 8px; padding: 14px 16px; }
.metric-label { color: #64748b; font-size: 11px; margin-bottom: 4px; }
.metric-value { font-size: 22px; font-weight: bold; }
.pos { color: #4ade80; } .neg { color: #f87171; } .neu { color: #facc15; }
.tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; }
.funnel { display: flex; flex-direction: column; gap: 6px; }
.funnel-row { display: flex; align-items: center; gap: 8px; }
.funnel-indent { margin-left: 20px; }
.funnel-indent2 { margin-left: 40px; }
"""


def _metric_card(label: str, value: str, cls: str = "") -> str:
    return f"""
    <div class='metric'>
      <div class='metric-label'>{label}</div>
      <div class='metric-value {cls}'>{value}</div>
    </div>"""


def _strategy_table(records: list[dict], cost: Optional[CostParams]) -> str:
    names = ["1. TP1 全出", "2. TP2 全出", "3. 半+BE+半", "4. 半+原止损+半", "5. TP1（仅激活）"]
    rows = ""
    for i, name in enumerate(names, 1):
        s = _strategy_stats(records, i, cost)
        exp = s["expectancy"]
        pf  = s["profit_factor"]
        exp_cls = "pos" if (exp or 0) > 0 else "neg"
        pf_cls  = "pos" if (pf  or 0) > 1 else "neg"
        wr = f"{s['win_rate']*100:.1f}%" if s["win_rate"] is not None else "—"
        rows += f"""<tr>
          <td>{name}</td>
          <td>{s['n']}</td>
          <td style='color:{_color_r(s["total_r"])}'>{_fmt(s['total_r'])}</td>
          <td class='{exp_cls}'>{_fmt(exp, 4)}</td>
          <td class='{pf_cls}'>{_fmt(pf)}</td>
          <td>{wr}</td>
          <td>{s['max_consec_loss'] if s['max_consec_loss'] is not None else '—'}</td>
        </tr>"""
    return f"""
    <table>
      <tr><th>方式</th><th>n</th><th>Total R</th><th>Expectancy</th><th>Profit F.</th><th>Win%</th><th>最大连亏</th></tr>
      {rows}
    </table>"""


def _funnel_html(f: dict) -> str:
    def row(label: str, n: int, pct: str, indent: int = 0, color: str = "#60a5fa") -> str:
        ind = "&nbsp;" * (indent * 4)
        bar_w = int(float(pct.rstrip("%")) / 100 * 150) if "%" in pct else 0
        return f"""<div style='display:flex;align-items:center;gap:8px;padding:3px 0'>
          <span style='color:#475569;width:180px;flex-shrink:0'>{ind}{label}</span>
          <span style='width:40px;text-align:right'>{n}</span>
          <div style='width:150px;background:#0f172a;border-radius:3px;height:8px'>
            <div style='width:{bar_w}px;background:{color};height:8px;border-radius:3px'></div>
          </div>
          <span style='color:#94a3b8'>{pct}</span>
        </div>"""

    return f"""
    <div class='card'>
      {row('总信号', f['n_total'], '100%', color='#3b82f6')}
      {row('primary 触线', f['n_touched'], f['pct_touched'], color='#60a5fa')}
      {row('→ 激活', f['n_activated'], f['pct_activated'], 1, '#4ade80')}
      {row('→ 取消', f['n_cancelled'], f['pct_cancelled'], 1, '#f87171')}
      {row('→ TP1 命中', f['n_tp1'], f['pct_tp1'], 2, '#4ade80')}
      {row('→ TP2 命中', f['n_tp2'], f['pct_tp2'], 3, '#4ade80')}
      {row('→ TP1后止损', f['n_inv_post'], f['pct_inv_post'], 3, '#f87171')}
      {row('→ TP1前止损', f['n_inv_pre'], f['pct_inv_pre'], 2, '#f87171')}
      {row('unresolved', f['n_unresolved'], f['pct_unresolved'], 1, '#facc15')}
    </div>"""


def _group_table(records: list[dict], groups: list[tuple[str, list[dict]]],
                 cost: Optional[CostParams]) -> str:
    rows = ""
    for label, grp in groups:
        if not grp:
            continue
        f  = _funnel(grp)
        s1 = _strategy_stats(grp, 1, cost)
        mm = _mfe_mae_dist(grp)
        exp = s1["expectancy"]
        pf  = s1["profit_factor"]
        exp_cls = "pos" if (exp or 0) > 0 else "neg"
        pf_cls  = "pos" if (pf  or 0) > 1 else "neg"
        er = mm["edge_ratio"]
        er_cls = "pos" if (er or 0) > 1 else "neg"
        rows += f"""<tr>
          <td style='color:#e2e8f0;font-weight:500'>{label}</td>
          <td>{f['n_total']}</td>
          <td>{_pct_bar(f['pct_activated'], '#60a5fa', 80)}</td>
          <td>{_pct_bar(f['pct_tp1'], '#4ade80', 80)}</td>
          <td>{_pct_bar(f['pct_tp2'], '#4ade80', 80)}</td>
          <td>{_pct_bar(f['pct_inv_pre'], '#f87171', 80)}</td>
          <td class='{exp_cls}'>{_fmt(exp, 4)}</td>
          <td class='{pf_cls}'>{_fmt(pf)}</td>
          <td class='{er_cls}'>{_fmt(er)}</td>
          <td style='color:#94a3b8'>{_avg_bars(grp, 'bars_to_tp1') or '—'}</td>
        </tr>"""
    return f"""
    <table>
      <tr>
        <th>分组</th><th>n</th><th>激活率</th><th>TP1率</th><th>TP2率</th>
        <th>止损率</th><th>Expect(S1)</th><th>ProfitF</th><th>EdgeR</th><th>avgBarsTP1</th>
      </tr>
      {rows}
    </table>"""


def _mfe_mae_html(mm: dict) -> str:
    def bar_row(label: str, vals: list, color: str) -> str:
        cells = ""
        for v in vals:
            w = int(min((v or 0) / 4 * 100, 100)) if v else 0
            cells += f"""<td>
              <div style='display:flex;align-items:center;gap:6px'>
                <div style='width:80px;background:#0f172a;border-radius:3px;height:8px'>
                  <div style='width:{w}px;background:{color};height:8px;border-radius:3px'></div>
                </div>
                <span>{_fmt(v, 2)}</span>
              </div></td>"""
        return f"<tr><td style='color:#94a3b8'>{label}</td>{cells}</tr>"

    mfe_vals = [mm['mfe_r_p25'], mm['mfe_r_p50'], mm['mfe_r_p75'], mm['mfe_r_p90'], mm['avg_mfe_r']]
    mae_vals = [mm['mae_r_p25'], mm['mae_r_p50'], mm['mae_r_p75'], mm['mae_r_p90'], mm['avg_mae_r']]
    er = mm['edge_ratio']
    er_cls = "pos" if (er or 0) > 1 else "neg"

    return f"""
    <table>
      <tr><th></th><th>p25</th><th>p50</th><th>p75</th><th>p90</th><th>avg</th></tr>
      {bar_row('MFE_R', mfe_vals, '#4ade80')}
      {bar_row('MAE_R', mae_vals, '#f87171')}
    </table>
    <p style='margin-top:10px'>Edge Ratio (avg_MFE / avg_MAE) = <span class='{er_cls}'>{_fmt(er)}</span></p>"""


def _monthly_html(records: list[dict], cost: Optional[CostParams]) -> str:
    months = sorted(set(r["month"] for r in records))
    rows = ""
    for m in months:
        grp = [r for r in records if r["month"] == m]
        f   = _funnel(grp)
        s1  = _strategy_stats(grp, 1, cost)
        act = float(f["pct_activated"].rstrip("%")) if "%" in f["pct_activated"] else 0
        tp1 = float(f["pct_tp1"].rstrip("%"))       if "%" in f["pct_tp1"]       else 0
        exp = s1["expectancy"]
        exp_cls = "pos" if (exp or 0) > 0 else "neg"
        rows += f"""<tr>
          <td>{m}</td>
          <td>{f['n_total']}</td>
          <td>{_pct_bar(f['pct_activated'], '#60a5fa', 100)}</td>
          <td>{_pct_bar(f['pct_tp1'], '#4ade80', 100)}</td>
          <td class='{exp_cls}'>{_fmt(exp, 4)}</td>
        </tr>"""
    return f"""
    <table>
      <tr><th>月份</th><th>n</th><th>激活率</th><th>TP1率</th><th>Expect(S1)</th></tr>
      {rows}
    </table>"""


def build_html(records: list[dict], cost: Optional[CostParams]) -> str:
    n = len(records)
    f_all = _funnel(records)
    mm_all = _mfe_mae_dist(records)
    s1 = _strategy_stats(records, 1, cost)

    symbols = sorted(set(r["symbol"] for r in records))
    months  = sorted(set(r["month"]  for r in records))
    pvs     = sorted(set(r["prompt_version"] for r in records))

    groups: list[tuple[str, list[dict]]] = [("总体", records)]
    for sym in symbols:
        groups.append((sym, [r for r in records if r["symbol"] == sym]))
    for g in ("A+", "A"):
        grp = [r for r in records if r["grade"] == g]
        if grp: groups.append((g, grp))
    for side in ("near_support", "near_resistance"):
        grp = [r for r in records if r["side"] == side]
        if grp: groups.append((side, grp))
    for m in months:
        groups.append((m, [r for r in records if r["month"] == m]))
    for pv in pvs:
        groups.append((f"prompt:{pv}", [r for r in records if r["prompt_version"] == pv]))

    cost_html = ""
    if cost:
        cost_html = f"""
        <div class='card' style='margin-bottom:24px;color:#94a3b8;font-size:12px'>
          <b style='color:#7dd3fc'>成本假设（Deepcoin 永续，保守值）</b><br><br>
          手续费（单腿）&nbsp;&nbsp;{cost.fee_per_leg}%&nbsp;&nbsp;taker 标准档 &nbsp;|&nbsp;
          滑点（单腿）&nbsp;&nbsp;{cost.slippage_per_leg}%&nbsp;&nbsp;保守估计 &nbsp;|&nbsp;
          来回总计&nbsp;&nbsp;<b style='color:#e2e8f0'>{cost.round_trip_pct}%</b> &nbsp;|&nbsp;
          资金费率&nbsp;&nbsp;{cost.funding_rate_per_8h}%/8h
        </div>"""

    exp = s1["expectancy"]
    pf  = s1["profit_factor"]
    er  = mm_all["edge_ratio"]

    return f"""<!DOCTYPE html>
<html lang='zh'>
<head><meta charset='utf-8'><title>Replay Report</title>
<style>{CSS}</style></head>
<body>
<h1>Replay Performance Report</h1>
<h2>{n} signals scored &nbsp;·&nbsp; {', '.join(symbols)} &nbsp;·&nbsp; {months[0]} → {months[-1]}</h2>

{cost_html}

<div class='grid'>
  {_metric_card('Expectancy (S1)', _fmt(exp, 4), 'pos' if (exp or 0) > 0 else 'neg')}
  {_metric_card('Profit Factor (S1)', _fmt(pf), 'pos' if (pf or 0) > 1 else 'neg')}
  {_metric_card('Edge Ratio', _fmt(er), 'pos' if (er or 0) > 1 else 'neg')}
  {_metric_card('激活率', f_all['pct_activated'], '')}
  {_metric_card('TP1 命中率', f_all['pct_tp1'], '')}
  {_metric_card('TP2 命中率', f_all['pct_tp2'], '')}
</div>

<h3>五种止盈方式对比</h3>
{_strategy_table(records, cost)}

<h3>漏斗转化率</h3>
{_funnel_html(f_all)}

<h3>MAE / MFE 分布（激活信号）</h3>
<div class='card'>{_mfe_mae_html(mm_all)}</div>

<h3>分组对比</h3>
{_group_table(records, groups, cost)}

<h3>月度趋势</h3>
{_monthly_html(records, cost)}

</body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--materials-dir", default="replay_materials")
    parser.add_argument("--out",           default="replay_report/report.html")
    parser.add_argument("--fee",           type=float, default=0.06)
    parser.add_argument("--slippage",      type=float, default=0.05)
    parser.add_argument("--funding",       type=float, default=0.01)
    parser.add_argument("--no-cost",       action="store_true")
    args = parser.parse_args()

    cost = None if args.no_cost else CostParams(
        fee_per_leg=args.fee,
        slippage_per_leg=args.slippage,
        funding_rate_per_8h=args.funding,
    )

    records = _load_records(Path(args.materials_dir))
    if not records:
        print("No replay_result.json found.")
        return

    out = Path(args.out)
    out.parent.mkdir(exist_ok=True)
    out.write_text(build_html(records, cost), encoding="utf-8")
    print(f"Saved → {out}")


if __name__ == "__main__":
    main()
