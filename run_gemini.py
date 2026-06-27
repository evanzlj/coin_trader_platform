#!/usr/bin/env python3
"""
Gemini web automation — replay_materials → Gemini(网页, 经 CDP) → gemini_runs/<sig>.json

跟 run_v7.py(ChatGPT)同一套路:Playwright 连 CDP → 驱动已登录的 Gemini 网页,
上传 prompt + 4h/15m 两张图,抽 JSON,存到 gemini_runs/(不碰 GPT 的 vlm_response.json)。

前置:
  1. 起 openclaw 浏览器(CDP 18800),在里面登录 gemini.google.com
  2. python3 run_gemini.py --probe        # 先探 DOM,确认/修正下面的 SELECTORS
  3. python3 run_gemini.py --filter eth_A --limit 1   # 试 1 个
  4. python3 run_gemini.py --limit 15     # 批量试点

⚠️ 下面 SELECTORS 是最佳猜测,务必先 --probe 核对再批量。
"""
import argparse, asyncio, json, os, random, re, sys, time
from pathlib import Path
from playwright.async_api import async_playwright

ROOT = Path(__file__).parent
MATERIALS_DIR = ROOT / "replay_materials"
CDP_URL = "http://127.0.0.1:18800"
GEMINI_URL = "https://gemini.google.com/app"
TIMEOUT = 360
COOLDOWN_MIN, COOLDOWN_MAX = 40, 70   # 信号间随机间隔,防 Gemini 限流

# 模型:菜单里的型号名片段。GEMINI_MODEL 可设 "3.5 Flash" / "Pro" / "Flash-Lite"
MODEL_NAME = os.environ.get("GEMINI_MODEL", "3.5 Flash")
MODEL_SLUG = ("pro" if "pro" in MODEL_NAME.lower()
              else "flash-lite" if "lite" in MODEL_NAME.lower() else "flash")
MODEL_KEY  = "Pro" if MODEL_SLUG == "pro" else "Flash"      # 选完 pill 上应出现的词
OUT_DIR = ROOT / ("gemini_runs" if MODEL_SLUG == "pro" else f"gemini_runs_{MODEL_SLUG}")
PROMPT_VERSION = "gemini-" + MODEL_SLUG
TABS = int(os.environ.get("GEMINI_TABS", "2"))   # 并发 tab 数;1=最不招 Google cookie 轮换

# ── Gemini DOM 选择器(已 --probe + 端到端实测确认 2026-06-27)─────────────
SEL = {
    "input":     'div.ql-editor',                                   # Quill 编辑器,用 keyboard.insert_text 填(Trusted Types 禁 innerHTML)
    "tools":     'button[aria-label*="上传"], button[aria-label*="工具"]',  # "上传和工具" 按钮
    "upload":    "上传文件",                                          # 菜单项文字 → 触发原生文件框(expect_file_chooser 拦)
    "send":      'button[aria-label*="发送"], button[aria-label*="Send"]',
    "response":  '.model-response-text, message-content',
}

# ── JSON 工具(沿用 run_v7)──────────────────────────────────────────
def strip_fences(text: str) -> str:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if m: return m.group(1).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text); text = re.sub(r"\s*```$", "", text)
    return text.strip()

def extract_json(raw: str):
    text = strip_fences(raw)
    if "{" in text and "}" in text:
        text = text[text.find("{"):text.rfind("}")+1]
    for cand in (text, re.sub(r",(\s*[}\]])", r"\1", text)):
        try:
            d = json.loads(cand)
            if isinstance(d, dict): return d
        except json.JSONDecodeError: pass
    return None

def validate(d: dict):
    return [k for k in ("watch_summary", "playbooks") if k not in d]

# ── 选 Pro 模型(goto /app 会默认回 Flash,每次开新对话后必须重选)──────────
_MSW_FIND = """()=>{const e=[...document.querySelectorAll('button,[role=button]')].find(x=>/flash|pro/i.test(x.innerText||'')&&(x.innerText||'').length<25);return e?e.innerText.trim():'?';}"""
async def select_model(page) -> bool:
    """开新对话后选 MODEL_NAME(goto /app 默认回 Flash;每次重选)。"""
    try:
        await page.evaluate("""()=>{const e=[...document.querySelectorAll('button,[role=button]')].find(x=>/flash|pro/i.test(x.innerText||'')&&(x.innerText||'').length<25);if(e)e.setAttribute('data-msw','1');}""")
        await page.locator('[data-msw="1"]').first.click()
        await asyncio.sleep(1.5)
        await page.evaluate("""(name)=>{const it=[...document.querySelectorAll('[role=menuitem],[role=menuitemradio],[role=option]')];const m=it.find(e=>(e.innerText||'').includes(name));if(m)m.click();}""", MODEL_NAME)
        await asyncio.sleep(1.5)
        label = await page.evaluate(_MSW_FIND)
        return MODEL_KEY in label
    except Exception:
        return False

# ── 探测模式:打印真实 DOM 候选,帮我们改对 SELECTORS ─────────────────
async def probe(page):
    await page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=40000)
    await asyncio.sleep(5)
    print("URL:", page.url, "| title:", await page.title())
    js = """() => {
      const out = {contenteditable:[], fileInputs:0, buttons:[], maybeResponse:[]};
      document.querySelectorAll('[contenteditable="true"]').forEach(e=>out.contenteditable.push(e.className||e.tagName));
      out.fileInputs = document.querySelectorAll('input[type=file]').length;
      document.querySelectorAll('button[aria-label]').forEach(b=>{const a=b.getAttribute('aria-label'); if(a) out.buttons.push(a);});
      ['message-content','.model-response-text','.markdown','model-response','response-container'].forEach(s=>{
        const n=document.querySelectorAll(s).length; if(n) out.maybeResponse.push(s+':'+n);});
      return out;
    }"""
    info = await page.evaluate(js)
    print("contenteditable 类名:", info["contenteditable"][:8])
    print("file inputs:", info["fileInputs"])
    print("button aria-labels:", [b for b in info["buttons"] if any(k in b.lower() for k in ('send','发送','stop','停止','attach','上传','image','file'))][:15])
    print("候选响应容器:", info["maybeResponse"])
    await page.screenshot(path=str(ROOT / "screenshots" / f"gemini_probe_{int(time.time())}.png"))
    print("截图已存 screenshots/")

# ── 等响应稳定 ────────────────────────────────────────────────────
async def wait_for_response(page, prev_count):
    start = time.time(); last = ""; stable = 0
    while time.time() - start < TIMEOUT:
        await asyncio.sleep(4)
        resp = page.locator(SEL["response"])
        cnt = await resp.count()
        if cnt <= prev_count: continue
        try: cur = (await resp.nth(cnt-1).inner_text()).strip()
        except Exception: continue
        if cur == last and len(cur) > 500: stable += 1
        elif cur != last:
            stable = 0
            if len(cur) > 0: print(f"    {len(cur)} chars")
        last = cur
        if stable >= 4: return cur, None
    return (last if len(last) > 500 else None), "timeout"

# ── 处理一个信号 ──────────────────────────────────────────────────
async def process_one(page, d: Path, idx, total):
    outf = OUT_DIR / f"{d.name}.json"
    if outf.exists(): return {"dir": d.name, "status": "cached"}
    pf = d / "prompt.txt"
    img4 = sorted(d.glob("*_4h.png")); img15 = sorted(d.glob("*_15m.png"))
    if not pf.exists() or not img4 or not img15:
        return {"dir": d.name, "status": "error", "error": "missing materials"}
    prompt = pf.read_text(encoding="utf-8")
    try:
        prev = await page.locator(SEL["response"]).count()
        # 上传:点"上传和工具" → 拦原生文件框 → 点"上传文件" → 投 2 图
        await page.locator(SEL["tools"]).first.click()
        await asyncio.sleep(1)
        async with page.expect_file_chooser() as fc:
            await page.get_by_text(SEL["upload"], exact=True).first.click()
        await (await fc.value).set_files([str(img4[0]), str(img15[0])])
        await asyncio.sleep(8)   # 等图上传完成
        # 填 prompt:focus + insert_text(Trusted Types 禁 innerHTML)
        await page.locator(SEL["input"]).first.click()
        await asyncio.sleep(0.5)
        await page.keyboard.insert_text(prompt)
        await asyncio.sleep(2)
        sb = page.locator(SEL["send"]).first
        await sb.wait_for(state="visible", timeout=8000)
        await sb.evaluate("el=>el.click()")
        print(f"  [{idx}/{total}] {d.name}")
        text, err = await wait_for_response(page, prev)
        if err or not text or len(text) < 200:
            return {"dir": d.name, "status": "error", "error": err or "too_short"}
        parsed = extract_json(text)
        if parsed is None:
            (OUT_DIR / f"{d.name}_raw.txt").write_text(text, encoding="utf-8")
            return {"dir": d.name, "status": "parse_err"}
        miss = validate(parsed)
        if miss: return {"dir": d.name, "status": "validation_err", "error": f"missing:{miss}"}
        parsed["prompt_version"] = PROMPT_VERSION
        outf.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"dir": d.name, "status": "ok", "pbs": len(parsed.get("playbooks", []))}
    except Exception as e:
        return {"dir": d.name, "status": "error", "error": str(e)[:160]}

# ── 双 tab 并发:T1=btc+eth, T2=bnb+sol。每 tab 串行 + 40-70s 间隔 + 限流退避 ──
GROUPS = [["btc", "eth"], ["bnb", "sol"]]

def coin_dirs(coins, limit):
    out = []
    for c in coins:
        ds = sorted(d for d in MATERIALS_DIR.iterdir()
                    if d.is_dir() and d.name.startswith(c+"_A_")
                    and not (OUT_DIR/f"{d.name}.json").exists())
        out += ds[:limit] if limit else ds
    return out

async def rate_limited(page):
    try:
        return await page.evaluate("""()=>{const t=(document.body.innerText||'');return /请求过于频繁|稍后再试|too many requests|rate limit|try again later|额度|用量上限|exceeded/i.test(t);}""")
    except Exception:
        return False

async def ensure_gemini(page, tries=5) -> bool:
    """goto gemini;若被重定向到 Google 认证/cookie 轮换页,重试回 gemini。"""
    for _ in range(tries):
        try:
            await page.goto(GEMINI_URL, wait_until="domcontentloaded", timeout=40000)
            await asyncio.sleep(4)
        except Exception:
            await asyncio.sleep(5); continue
        u = page.url
        if "gemini.google.com" in u and "RotateCookies" not in u and "accounts.google" not in u:
            return True
        await asyncio.sleep(5)   # cookie 轮换中,等它跳回再重试
    return "gemini.google.com" in page.url

async def worker(page, dirs, tag):
    results = []; i = 0; back = 0
    while i < len(dirs):
        d = dirs[i]
        if not await ensure_gemini(page):
            print(f"  [{tag}] 卡 Google 认证页,退避90s"); await asyncio.sleep(90); continue
        if await rate_limited(page):
            w = 120 + back*120; back = min(back+1, 4)
            print(f"  [{tag}] 限流,退避 {w}s"); await asyncio.sleep(w); continue
        if not await select_model(page):
            print(f"  [{tag}] {d.name} 非{MODEL_KEY}({MODEL_NAME}),退避60s"); await asyncio.sleep(60); continue
        r = await process_one(page, d, i+1, len(dirs))
        print(f"  [{tag}] {d.name} → {r['status']} {r.get('error','') if r['status']!='ok' else '('+str(r.get('pbs'))+'pbs)'}")
        if r["status"] in ("ok", "cached"):
            results.append(r); i += 1; back = 0
            await asyncio.sleep(random.uniform(COOLDOWN_MIN, COOLDOWN_MAX))
        else:
            back += 1
            if back > 3:                       # 同一信号退避3次仍不行 → 放弃,继续下一个
                results.append(r); i += 1; back = 0
            else:
                await asyncio.sleep(90*back)
    return results

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="每个币最多跑几个(0=全部)")
    args = ap.parse_args()
    OUT_DIR.mkdir(exist_ok=True); (ROOT/"screenshots").mkdir(exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        ctx = browser.contexts[0]
        if args.probe:
            await probe(ctx.pages[0]); await browser.close(); return 0
        while len(ctx.pages) < TABS:
            await ctx.new_page(); await asyncio.sleep(1)
        pages = ctx.pages[:TABS]
        if TABS == 1:
            allg = coin_dirs(["btc", "eth", "bnb", "sol"], args.limit)
            print(f"模型={MODEL_NAME} | 单tab: {len(allg)} 个 → {OUT_DIR}")
            res = [await worker(pages[0], allg, "T1")]
        else:
            groups = [coin_dirs(g, args.limit) for g in GROUPS]
            print(f"模型={MODEL_NAME} | T1(btc+eth):{len(groups[0])} T2(bnb+sol):{len(groups[1])} → {OUT_DIR}")
            res = await asyncio.gather(worker(pages[0], groups[0], "T1"),
                                       worker(pages[1], groups[1], "T2"))
        allr = [r for sub in res for r in sub]
        ok = sum(1 for r in allr if r["status"] == "ok")
        print(f"\n完成: ok={ok} / {len(allr)}")
        await browser.close()
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
