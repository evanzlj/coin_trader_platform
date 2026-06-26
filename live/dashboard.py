"""只读交易 dashboard（纯 stdlib，无第三方依赖）。

数据全部来自 btc-ml 本地只读文件，不碰执行器、不刷交易所 API：
  - signal_active/*/state.json   等待激活 / 进行中的剧本
  - signal_done/*/state.json     已了结的剧本（算战报）
  - live/trade_log.jsonl         成交事件流
  - live/heartbeat/*_last_run.txt 各服务健康
  - OHLCV sqlite                 各品种最新价（算距激活/浮盈）

绑 127.0.0.1，经 SSH 隧道访问：
  ssh -L 8080:localhost:8080 btc-ml   然后本地浏览器开 http://localhost:8080

跑：python3 -m live.dashboard   （或 systemd user service coin-dashboard）
"""
from __future__ import annotations
import json, glob, os, sqlite3, html, subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from datetime import datetime, timezone

from live import exec_config as cfg

ROOT          = cfg.ROOT
SIGNAL_ACTIVE = ROOT / "signal_active"
SIGNAL_DONE   = ROOT / "signal_done"
TRADE_LOG     = ROOT / "live" / "trade_log.jsonl"
HEARTBEAT_DIR = ROOT / "live" / "heartbeat"
PORT          = int(os.environ.get("DASHBOARD_PORT", "8080"))

SLUG   = {"BTC/USDT":"BTC/USDT","ETH/USDT":"ETH/USDT","BNB/USDT":"BNB/USDT","SOL/USDT":"SOL/USDT"}
WAIT   = {"WAITING_FOR_PRIMARY_TOUCH","WAITING_FOR_ACTIVATION"}
OPEN   = {"ACTIVATED","TP1_HIT"}
RESULT_LABEL = {"tp2":"TP1+TP2","be":"TP1→保本","sl":"止损","cancelled":"取消","stale_discard":"过期丢弃"}


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def latest_prices():
    out = {}
    try:
        conn = sqlite3.connect(f"file:{cfg.OHLCV_DB}?mode=ro", uri=True, timeout=3)
        for sym in SLUG:
            r = conn.execute(
                "SELECT close FROM ohlcv_bars WHERE symbol=? AND timeframe='15m' "
                "ORDER BY open_time DESC LIMIT 1", (sym,)).fetchone()
            if r:
                out[sym] = float(r[0])
        conn.close()
    except Exception as e:
        out["_err"] = str(e)
    return out


def _iter_states(d: Path):
    for f in sorted(glob.glob(str(d / "*" / "state.json"))):
        try:
            yield os.path.basename(os.path.dirname(f)), json.load(open(f))
        except Exception:
            continue


def _pct(a, b):
    return None if not b else round((a - b) / b * 100, 2)


def _trade_r(pb):
    """从 exec + result 算这笔的 R（actual_r_usdt = $1R）。"""
    e = pb.get("exec") or {}
    res = pb.get("result")
    ap, sl1R = e.get("entry_price"), e.get("actual_r_usdt")
    tp1, tp2 = e.get("tp1"), e.get("tp2")
    half = e.get("half_qty"); qty = e.get("qty")
    if not (ap and sl1R):
        return None
    if res == "sl":
        return -1.0
    if res in ("tp2", "be") and half is not None and tp1:
        tp1_usd = half * abs(ap - tp1)
        if res == "be":
            return round(tp1_usd / sl1R, 3)
        rest = (qty - half) if qty else half
        tp2_usd = rest * abs(ap - (tp2 or tp1))
        return round((tp1_usd + tp2_usd) / sl1R, 3)
    return None


def build_state():
    prices = latest_prices()
    waiting, openpos, closed = [], [], []

    for pkg, d in _iter_states(SIGNAL_ACTIVE):
        sym = d.get("symbol"); px = prices.get(sym)
        for pb in d.get("playbooks", []):
            st = pb.get("status"); hyp = pb.get("hypothesis"); dr = pb.get("direction")
            if st in WAIT:
                gate_name = "primary触线" if st == "WAITING_FOR_PRIMARY_TOUCH" else "激活收破"
                gate_lvl  = (pb.get("primary_touch") or {}).get("level") if st=="WAITING_FOR_PRIMARY_TOUCH" else (pb.get("activates_if") or {}).get("level")
                waiting.append({
                    "sym": sym, "hyp": hyp, "dir": dr, "status": st,
                    "sig_time": (d.get("bar_time") or "")[:16].replace("T"," "),
                    "gate_name": gate_name, "gate_lvl": gate_lvl,
                    "cancels": (pb.get("cancels_if") or {}).get("level"),
                    "tp1": pb.get("tp1_level"), "tp2": pb.get("tp2_level"),
                    "price": px, "dist_pct": _pct(px, gate_lvl) if (px and gate_lvl) else None,
                })
            elif st in OPEN:
                e = pb.get("exec") or {}
                ap = e.get("entry_price"); rem = e.get("qty_remaining")
                upnl = None
                if px and ap and rem:
                    sign = 1 if dr == "short" else -1
                    upnl = round(sign * (ap - px) * rem, 2)
                openpos.append({
                    "sym": sym, "hyp": hyp, "dir": dr, "status": st,
                    "entry": ap, "sl": e.get("sl_price"), "tp1": e.get("tp1"), "tp2": e.get("tp2"),
                    "qty_rem": rem, "price": px, "upnl_usd": upnl,
                    "r1_usd": e.get("actual_r_usdt"),
                })

    for src in (SIGNAL_ACTIVE, SIGNAL_DONE):
        for pkg, d in _iter_states(src):
            for pb in d.get("playbooks", []):
                res = pb.get("result")
                if res in RESULT_LABEL and (pb.get("exec") or res in ("cancelled","stale_discard")):
                    if res in ("cancelled","stale_discard"):
                        continue  # 没真成交，不计战报
                    closed.append({
                        "sym": d.get("symbol"), "hyp": pb.get("hypothesis"), "dir": pb.get("direction"),
                        "result": res, "label": RESULT_LABEL.get(res, res),
                        "entry": (pb.get("exec") or {}).get("entry_price"),
                        "r": _trade_r(pb), "pkg": pkg,
                    })

    # 战报
    rs = [c["r"] for c in closed if c["r"] is not None]
    wins = sum(1 for r in rs if r > 0)
    scorecard = {
        "n": len(closed), "wins": wins, "losses": sum(1 for r in rs if r <= 0),
        "winrate": round(wins/len(rs)*100, 1) if rs else 0,
        "total_r": round(sum(rs), 2),
        "by_result": {k: sum(1 for c in closed if c["result"]==k) for k in ("tp2","be","sl")},
    }

    # 健康：有心跳文件的看心跳新鲜度；watchdog 不写心跳 → 查 systemd
    health = {}
    for name in ("executor","monitor","finalizer","shadow_tracker"):
        f = HEARTBEAT_DIR / f"{name}_last_run.txt"
        try:
            ts = datetime.fromisoformat(f.read_text().strip())
            age = (datetime.now(timezone.utc) - ts).total_seconds()/60
            health[name] = {"age_min": round(age,1), "ok": age < 10}
        except Exception:
            health[name] = {"age_min": None, "ok": False}
    try:
        r = subprocess.run(["systemctl","--user","is-active","coin-watchdog"],
                           capture_output=True, text=True, timeout=3)
        health["watchdog"] = {"age_min": None, "ok": r.stdout.strip() == "active"}
    except Exception:
        health["watchdog"] = {"age_min": None, "ok": False}

    # 最近事件
    events = []
    try:
        lines = TRADE_LOG.read_text().strip().splitlines()[-25:]
        for ln in reversed(lines):
            try: events.append(json.loads(ln))
            except: pass
    except Exception:
        pass

    return {
        "now": _now_iso(), "prices": prices,
        "waiting": sorted(waiting, key=lambda x:(x["sym"], abs(x["dist_pct"]) if x["dist_pct"] is not None else 9e9)),
        "open": openpos, "closed": list(reversed(closed))[:30],
        "scorecard": scorecard, "health": health, "events": events,
    }


PAGE = """<!doctype html><html><head><meta charset=utf-8>
<title>OKX demo 交易面板</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<style>
 body{background:#0d1117;color:#c9d1d9;font:13px/1.5 -apple-system,Menlo,monospace;margin:0;padding:16px}
 h2{color:#58a6ff;border-bottom:1px solid #21262d;padding-bottom:4px;margin:22px 0 8px;font-size:15px}
 table{border-collapse:collapse;width:100%;margin-bottom:8px}
 th,td{text-align:right;padding:4px 8px;border-bottom:1px solid #161b22;white-space:nowrap}
 th{color:#8b949e;font-weight:600;text-align:right}
 td.l,th.l{text-align:left}
 .pos{color:#3fb950}.neg{color:#f85149}.dim{color:#6e7681}
 .short{color:#f85149}.long{color:#3fb950}
 .pill{padding:1px 6px;border-radius:4px;background:#21262d;font-size:11px}
 .ok{color:#3fb950}.bad{color:#f85149}
 #top{display:flex;gap:18px;flex-wrap:wrap;align-items:baseline}
 .big{font-size:20px;font-weight:700}
 .muted{color:#6e7681;font-size:11px}
</style></head><body>
<div id=top>
 <span class=big>OKX demo 交易面板</span>
 <span id=score class=muted></span>
 <span id=health class=muted></span>
 <span id=upd class=muted></span>
</div>
<div id=body></div>
<script>
const f=(v,d=2)=>v==null?'—':(+v).toFixed(d);
const dirc=d=>`<span class=${d}>${d=='short'?'空':'多'}</span>`;
const sgn=v=>v==null?'<span class=dim>—</span>':`<span class=${v>=0?'pos':'neg'}>${v>=0?'+':''}${f(v)}</span>`;
async function tick(){
 let s; try{s=await (await fetch('/api/state')).json()}catch(e){return}
 const S=s.scorecard;
 document.getElementById('score').innerHTML=
   `战报: <b>${S.total_r>=0?'+':''}${S.total_r}R</b> · ${S.wins}胜${S.losses}负 (${S.winrate}%) · TP2:${S.by_result.tp2} 保本:${S.by_result.be} 止损:${S.by_result.sl}`;
 document.getElementById('health').innerHTML='服务: '+Object.entries(s.health).map(([k,v])=>
   `<span class=${v.ok?'ok':'bad'}>${k}${v.ok?'':'✗'}</span>`).join(' ');
 document.getElementById('upd').textContent='· '+s.now+' (5s刷新)';
 let h='';
 // 进行中
 h+='<h2>进行中持仓 ('+s.open.length+')</h2>';
 if(s.open.length){h+='<table><tr><th class=l>品种</th><th class=l>剧本</th><th>方向</th><th>状态</th><th>入场</th><th>现价</th><th>SL</th><th>TP1</th><th>TP2</th><th>剩余</th><th>浮盈$</th></tr>';
  for(const o of s.open)h+=`<tr><td class=l>${o.sym}</td><td class=l>${o.hyp}</td><td>${dirc(o.dir)}</td><td>${o.status}</td><td>${f(o.entry)}</td><td>${f(o.price)}</td><td>${f(o.sl)}</td><td>${f(o.tp1)}</td><td>${f(o.tp2)}</td><td>${f(o.qty_rem,4)}</td><td>${sgn(o.upnl_usd)}</td></tr>`;
  h+='</table>';}else h+='<div class=dim>空仓</div>';
 // 等待激活
 h+='<h2>等待激活的剧本 ('+s.waiting.length+')</h2>';
 if(s.waiting.length){h+='<table><tr><th class=l>品种</th><th class=l>剧本</th><th>方向</th><th class=l>信号时间</th><th class=l>下一关</th><th>触发位</th><th>现价</th><th>距离%</th><th>取消位</th><th>TP1</th><th>TP2</th></tr>';
  for(const w of s.waiting)h+=`<tr><td class=l>${w.sym}</td><td class=l>${w.hyp}</td><td>${dirc(w.dir)}</td><td class=l>${w.sig_time}</td><td class=l><span class=pill>${w.gate_name}</span></td><td>${f(w.gate_lvl)}</td><td>${f(w.price)}</td><td>${sgn(w.dist_pct)}</td><td class=dim>${f(w.cancels)}</td><td>${f(w.tp1)}</td><td>${f(w.tp2)}</td></tr>`;
  h+='</table>';}else h+='<div class=dim>无</div>';
 // 历史
 h+='<h2>已了结 ('+s.closed.length+')</h2>';
 if(s.closed.length){h+='<table><tr><th class=l>品种</th><th class=l>剧本</th><th>方向</th><th>入场</th><th>结局</th><th>R</th></tr>';
  for(const c of s.closed)h+=`<tr><td class=l>${c.sym}</td><td class=l>${c.hyp}</td><td>${dirc(c.dir)}</td><td>${f(c.entry)}</td><td>${c.label}</td><td>${sgn(c.r)}</td></tr>`;
  h+='</table>';}else h+='<div class=dim>无</div>';
 // 事件流
 h+='<h2>最近事件</h2><table>';
 for(const e of s.events)h+=`<tr><td class=l dim>${(e.ts||'').slice(5,19).replace('T',' ')}</td><td class=l>${e.event}</td><td class=l>${e.symbol||''}</td><td class=l dim>${e.hypothesis||''}</td><td>${e.entry?f(e.entry):''}</td></tr>`;
 h+='</table>';
 document.getElementById('body').innerHTML=h;
}
tick();setInterval(tick,5000);
</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path.startswith("/api/state"):
            body = json.dumps(build_state()).encode()
            self.send_response(200); self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        elif self.path == "/" or self.path.startswith("/index"):
            body = PAGE.encode()
            self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8")
            self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), H)
    print(f"dashboard on http://127.0.0.1:{PORT}  (SSH 隧道访问)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
