"""交易所真实账单汇总（ground truth,非估算)。按需跑,不需实时。

为什么要它:dashboard 的净R是**估算**(固定费率+maker/taker假设);此工具直接从交易所
拉真实账单(已实现盈亏/手续费/资金费),尤其币安虚拟子账户网页登不进,只能靠 API 看账。

用法:
  python3 -m live.bill_report                # 全部自动账户(okx_1/2/3 + 5币安),近7天
  python3 -m live.bill_report --days 3
  python3 -m live.bill_report --json         # 机读

口径:realizedPnl(已实现盈亏) + fees(手续费,负) + funding(资金费) = net(净)。
OKX account bills 默认只回近7天(更久需 archive 接口,上线才2天够用)。
"""
from __future__ import annotations
import argparse, json, sys
from collections import defaultdict

from live import exec_config as cfg
from live.keys_loader import load_keys


def _okx_bills(broker, days: int) -> dict:
    """OKX：account bills（type2=交易 有pnl+fee;type8=资金费)。"""
    realized = fees = funding = 0.0
    n = 0
    resp = broker.account.get_account_bills(instType="SWAP")
    for r in broker._data(resp):
        t = r.get("type")
        if t == "2":                                  # 交易
            realized += float(r.get("pnl") or 0)
            fees += float(r.get("fee") or 0)
            n += 1
        elif t == "8":                                # 资金费
            funding += float(r.get("balChg") or r.get("fee") or 0)
    return {"realized": realized, "fees": fees, "funding": funding, "fills": n}


def _binance_income(broker, days: int) -> dict:
    """Binance：income history 按 incomeType 归类。"""
    realized = fees = funding = 0.0
    n = 0
    rows = broker.client.get_income_history(limit=1000)
    for r in rows:
        it = r.get("incomeType"); v = float(r.get("income") or 0)
        if it == "REALIZED_PNL":
            realized += v; n += 1
        elif it == "COMMISSION":
            fees += v
        elif it == "FUNDING_FEE":
            funding += v
    return {"realized": realized, "fees": fees, "funding": funding, "fills": n}


def build_report(days: int) -> list[dict]:
    keys = load_keys("live")
    out = []
    # OKX
    from live.broker.okx import OKXBroker
    for a in keys.get("okx", []):
        try:
            b = OKXBroker(a["label"], a["api_key"], a["secret"], a["passphrase"], flag=cfg.okx_simulated_flag())
            r = _okx_bills(b, days); r["bal"] = b.get_available_balance()
            r["acct"] = a["label"]; r["exch"] = "okx"; out.append(r)
        except Exception as e:
            out.append({"acct": a["label"], "exch": "okx", "err": str(e)[:80]})
    # Binance
    from live.broker.binance import BinanceBroker
    for a in keys.get("binance", []):
        try:
            b = BinanceBroker(a["label"], a["api_key"], a["secret"], cfg.binance_base_url())
            r = _binance_income(b, days); r["bal"] = b.get_available_balance()
            r["acct"] = a["label"].split("@")[0]; r["exch"] = "binance"; out.append(r)
        except Exception as e:
            out.append({"acct": a["label"].split("@")[0], "exch": "binance", "err": str(e)[:80]})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rep = build_report(args.days)
    if args.json:
        print(json.dumps(rep, ensure_ascii=False, indent=2)); return

    print(f"=== 交易所真实账单(近{args.days}天,ground truth)===")
    print(f"{'账户':22}{'已实现':>10}{'手续费':>10}{'资金费':>9}{'净':>10}{'笔数':>5}{'余额':>10}")
    print("-" * 78)
    tot = defaultdict(float)
    for r in rep:
        if r.get("err"):
            print(f"{r['exch']+'/'+r['acct']:22} 失败: {r['err']}"); continue
        net = r["realized"] + r["fees"] + r["funding"]
        for k in ("realized", "fees", "funding"): tot[k] += r[k]
        tot["net"] += net; tot["bal"] += r["bal"] or 0
        print(f"{r['exch']+'/'+r['acct']:22}{r['realized']:>+10.4f}{r['fees']:>+10.4f}{r['funding']:>+9.4f}"
              f"{net:>+10.4f}{r['fills']:>5}{r['bal']:>10.2f}")
    print("-" * 78)
    print(f"{'合计':22}{tot['realized']:>+10.4f}{tot['fees']:>+10.4f}{tot['funding']:>+9.4f}"
          f"{tot['net']:>+10.4f}{'':>5}{tot['bal']:>10.2f}")
    print("\n注:realizedPnl 是价差已实现盈亏(不含fee);净 = 已实现+手续费+资金费。OKX 只回近7天。")


if __name__ == "__main__":
    main()
