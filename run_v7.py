#!/usr/bin/env python3
"""
ChatGPT browser automation v7 — replay_materials → ChatGPT → vlm_response.json

Scans replay_materials/ for signal dirs without vlm_response.json.
For each: uploads prompt + 4h/15m charts to ChatGPT web, extracts JSON,
validates, injects prompt_version, and saves vlm_response.json in place.

Usage:
    python3 run_v7.py
    python3 run_v7.py --delay-min 40 --delay-max 60
    python3 run_v7.py --filter btc                 # only process btc_* dirs
    python3 run_v7.py --filter sol_Aplus           # only sol A+ signals
"""

import argparse, asyncio, json, random, re, sys, time
from pathlib import Path
from playwright.async_api import async_playwright

STOP_FLAG = Path(__file__).parent / "STOP_FLAG"
if STOP_FLAG.exists():
    print("🛑 STOP_FLAG exists — exiting immediately")
    sys.exit(0)

MATERIALS_DIR = Path(__file__).parent / "replay_materials"
CDP_URL = "http://127.0.0.1:18800"
TIMEOUT = 360          # per-signal timeout in seconds
MAX_RETRIES = 1        # retries per signal after recoverable errors
PROMPT_VERSION = "v1.0"
COOLDOWN_MIN = 90      # seconds between signals
COOLDOWN_MAX = 120

# ── JSON utilities ──────────────────────────────────────────────────

def strip_fences(text: str) -> str:
    text = text.strip()
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def extract_json(raw: str) -> dict | None:
    """Try to extract a valid JSON dict from raw ChatGPT response."""
    text = strip_fences(raw)
    for candidate in [text, re.sub(r",(\s*[}\]])", r"\1", text)]:
        try:
            d = json.loads(candidate)
            if isinstance(d, dict):
                return d
        except json.JSONDecodeError:
            pass
    return None


def validate_response(parsed: dict) -> list[str]:
    """Return list of missing required top-level keys."""
    required = ["watch_summary", "playbooks"]
    return [k for k in required if k not in parsed]

# ── ChatGPT page helpers ────────────────────────────────────────────

async def is_responding(page) -> bool:
    """Check if ChatGPT is busy (thinking/generating)."""
    try:
        th = page.locator('text="已思考"').first
        if await th.count() > 0:
            return True
    except Exception:
        pass
    try:
        gen = page.locator('[aria-label="停止生成"], button:has(svg[class*="stop"])').first
        if await gen.count() > 0:
            is_vis = await gen.is_visible()
            if is_vis:
                return True
    except Exception:
        pass
    return False


async def fill_prompt(page, text: str) -> bool:
    """Fill the ChatGPT prompt box with text via evaluate (more reliable than type)."""
    div = page.locator('div[contenteditable="true"]').first
    await div.wait_for(state="visible", timeout=5000)
    await div.click(force=True, timeout=10000)
    await asyncio.sleep(0.5)
    ok = await page.evaluate(
        """(t) => {
            const d = document.querySelector('div[contenteditable="true"]');
            if (!d) return 'no_div';
            d.focus(); d.textContent = t;
            d.dispatchEvent(new Event('input', {bubbles: true}));
            return 'ok';
        }""",
        text,
    )
    await asyncio.sleep(0.5)
    await page.keyboard.press("Space")
    await asyncio.sleep(0.2)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.5)
    return ok == "ok"


async def send_message(page) -> bool:
    """Click send button, fallback to Enter. Returns True if send likely succeeded."""
    # Wait for send button to be visible and enabled
    try:
        sb = page.locator('button[data-testid="send-button"]').first
        await sb.wait_for(state="visible", timeout=5000)
        # Get button state before clicking
        disabled = await sb.get_attribute("disabled")
        if disabled is not None:
            print(f"    ⚠️ send button disabled — trying Enter fallback")
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)
            return True
        # Try evaluate-click first (bypasses visibility checks)
        await sb.evaluate("el => el.click()")
        await asyncio.sleep(3)
        return True
    except Exception as e:
        print(f"    ⚠️ send evaluate failed: {e}, trying native click...")
        try:
            sb = page.locator('button[data-testid="send-button"]').first
            await sb.click(timeout=5000)
            await asyncio.sleep(3)
            return True
        except Exception as e2:
            print(f"    ⚠️ native click failed: {e2}, trying Enter...")
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(3)
                return True
            except Exception as e3:
                print(f"    ❌ send FAILED: {e3}")
                await screenshot(page, "send_failed")
                return False


async def screenshot(page, name: str) -> str:
    ss_dir = Path(__file__).parent / "screenshots"
    ss_dir.mkdir(parents=True, exist_ok=True)
    path = ss_dir / f"{name}_{int(time.time())}.png"
    await page.screenshot(path=str(path))
    return str(path)


async def handle_error(page, error_type: str) -> bool:
    """Handle recoverable errors. Returns True if retryable, False if blocked."""
    if error_type in ("thinking_failed", "stuck_analyzing"):
        ss = await screenshot(page, error_type)
        print(f"    📸 {ss}")
        print(f"    ⏳ {error_type} → waiting 30s then retry once")
        await asyncio.sleep(30)
        return True

    if error_type == "server_error":
        rb = page.locator('button:has-text("重试")').first
        if await rb.count() > 0:
            try:
                await rb.click(timeout=5000)
            except Exception:
                await rb.evaluate("el => el.click()")
            print("    ↻ clicked retry — waiting for response")
            await asyncio.sleep(5)
            return True
        return False

    if error_type == "timeout":
        ss = await screenshot(page, "timeout")
        print(f"    📸 {ss}")
        print(f"    ⏳ timeout → waiting 10s then retry")
        await asyncio.sleep(10)
        return True

    if error_type in ("rate_limit", "blocked"):
        print(f"    🛑 BLOCKED: {error_type} — PAUSING, awaiting user")
        return False

    print(f"    ❓ unknown error: {error_type} — report to user")
    return False


async def wait_for_response(page, prev_count: int):
    """Wait for a new assistant message to appear and stabilise.

    Returns (text, error_type_or_None).
    """
    start = time.time()
    last_text = ""
    stable = 0
    stuck_start = 0
    rate_limit_hits = 0

    while time.time() - start < TIMEOUT:
        await asyncio.sleep(4)

        # Rate-limit modal
        try:
            rl = page.locator('text="请求过于频繁"').first
            if await rl.count() > 0:
                rate_limit_hits += 1
                print(f"    ⚠️ rate limit modal #{rate_limit_hits}")
                dismiss = page.locator('button:has-text("明白了")').first
                if await dismiss.count() > 0:
                    try:
                        await dismiss.click(timeout=5000)
                    except Exception:
                        await dismiss.evaluate("el => el.click()")
                    await asyncio.sleep(3)
                if rate_limit_hits >= 10:
                    return None, "rate_limit"
                await asyncio.sleep(30)
                continue
        except Exception:
            pass

        # Server error
        try:
            err = page.locator('text="Something went wrong"').first
            if await err.count() > 0:
                rb = page.locator('button:has-text("重试")').first
                if await rb.count() > 0:
                    try:
                        await rb.click(timeout=5000)
                    except Exception:
                        await rb.evaluate("el => el.click()")
                    await asyncio.sleep(5)
                    continue
                return None, "server_error"
        except Exception:
            pass

        # Thinking stopped without output
        try:
            stopped = page.locator('text="已停止思考"').first
            if await stopped.count() > 0 and (time.time() - start) > 30:
                return None, "thinking_failed"
        except Exception:
            pass

        msgs = page.locator('[data-message-author-role="assistant"]')
        cnt = await msgs.count()

        if cnt <= prev_count:
            continue

        text = await msgs.nth(cnt - 1).inner_text()
        cur = text.strip()
        cur_len = len(cur)

        # Stuck "正在分析"
        is_analyzing = "正在分析" in cur and cur_len < 50
        if is_analyzing:
            if stuck_start == 0:
                stuck_start = time.time()
            elif time.time() - stuck_start > 60:
                return None, "stuck_analyzing"
        else:
            stuck_start = 0

        if cur == last_text and cur_len > 500:
            stable += 1
        elif cur != last_text:
            stable = 0
            if cur_len > 0 and not is_analyzing:
                print(f"    {cur_len} chars")

        last_text = cur

        if stable >= 5:
            return text, None

    return last_text if len(last_text) > 500 else None, "timeout"

# ── Per-signal processing ───────────────────────────────────────────

async def process_one(page, sig_dir: Path, idx: int, total: int,
                      retry_budget: int) -> dict:
    """Process one signal directory. Returns status dict."""
    dir_name = sig_dir.name

    # Already done?
    vlm_path = sig_dir / "vlm_response.json"
    if vlm_path.exists():
        try:
            existing = json.loads(vlm_path.read_text())
            if existing.get("watch_summary") and not existing.get("error"):
                return {"dir": dir_name, "status": "cached"}
        except (json.JSONDecodeError, OSError):
            pass

    # Locate input files
    prompt_file = sig_dir / "prompt.txt"
    if not prompt_file.exists():
        return {"dir": dir_name, "status": "error", "error": "no_prompt.txt"}

    imgs_4h  = sorted(sig_dir.glob("*_4h.png"))
    imgs_15m = sorted(sig_dir.glob("*_15m.png"))
    img_4h   = imgs_4h[0] if imgs_4h else None
    img_15m  = imgs_15m[0] if imgs_15m else None

    if not img_4h or not img_15m:
        missing = []
        if not img_4h: missing.append("4h.png")
        if not img_15m: missing.append("15m.png")
        return {"dir": dir_name, "status": "error", "error": f"missing_images:{missing}"}

    prompt = prompt_file.read_text(encoding="utf-8")

    for attempt in range(retry_budget + 1):
        try:
            if attempt > 0:
                print(f"    ↻ retry {attempt}/{retry_budget}")
                await asyncio.sleep(3)

            prev_count = await page.locator(
                '[data-message-author-role="assistant"]'
            ).count()

            # Upload images: 4h first, then 15m (SPEC order)
            fi = page.locator('input[type="file"]').first
            await fi.set_input_files([str(img_4h), str(img_15m)])
            # Wait for ChatGPT to finish processing uploaded images
            # (send button becomes enabled once upload completes)
            sb = page.locator('button[data-testid="send-button"]').first
            for _ in range(12):  # up to 24 seconds
                await asyncio.sleep(2)
                disabled = await sb.get_attribute("disabled")
                if disabled is None:
                    break

            # Fill prompt
            if not await fill_prompt(page, prompt):
                return {"dir": dir_name, "status": "error", "error": "fill_failed"}

            # Wait for send button to become enabled after filling
            await asyncio.sleep(2)
            for _ in range(6):  # up to 12 more seconds
                disabled = await sb.get_attribute("disabled")
                if disabled is None:
                    break
                await asyncio.sleep(2)

            if not await send_message(page):
                if attempt < retry_budget:
                    print(f"    ↻ send failed — retrying")
                    await asyncio.sleep(5)
                    continue
                return {"dir": dir_name, "status": "error", "error": "send_failed"}

            print(f"  [{idx}/{total}] {dir_name}")
            text, err = await wait_for_response(page, prev_count)

            if err:
                if attempt < retry_budget:
                    recoverable = await handle_error(page, err)
                    if recoverable:
                        continue
                    return {"dir": dir_name, "status": "blocked", "error": err}
                return {"dir": dir_name, "status": "error", "error": err}

            if not text or len(text) < 200:
                if attempt < retry_budget:
                    continue
                return {"dir": dir_name, "status": "error", "error": "too_short"}

            # Parse JSON
            parsed = extract_json(text)
            if parsed is None:
                if attempt < retry_budget:
                    continue
                # Save raw text so we can debug later
                raw_path = sig_dir / "_raw_response.txt"
                raw_path.write_text(text, encoding="utf-8")
                return {"dir": dir_name, "status": "parse_err"}

            # Validate required fields
            missing = validate_response(parsed)
            if missing:
                if attempt < retry_budget:
                    continue
                raw_path = sig_dir / "_raw_response.txt"
                raw_path.write_text(text, encoding="utf-8")
                vlm_path.write_text(
                    json.dumps(
                        {"parsed": parsed, "error": f"missing:{missing}"},
                        ensure_ascii=False, indent=2,
                    ),
                    encoding="utf-8",
                )
                return {"dir": dir_name, "status": "validation_err",
                        "error": f"missing:{missing}"}

            # ── Success: inject prompt_version and save ──
            parsed["prompt_version"] = PROMPT_VERSION
            vlm_path.write_text(
                json.dumps(parsed, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return {
                "dir": dir_name,
                "status": "ok",
                "chars": len(text),
                "pbs": len(parsed.get("playbooks", [])),
            }

        except Exception as e:
            if attempt >= retry_budget:
                return {"dir": dir_name, "status": "error",
                        "error": str(e)[:200]}

    return {"dir": dir_name, "status": "error", "error": "max_retries"}

# ── Main ─────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="v7: replay_materials → ChatGPT → vlm_response.json")
    parser.add_argument("--filter", default="",
                        help="Only process dirs whose name starts with this (e.g. 'btc', 'sol_Aplus')")
    parser.add_argument("--delay-min", type=float, default=COOLDOWN_MIN)
    parser.add_argument("--delay-max", type=float, default=COOLDOWN_MAX)
    parser.add_argument("--tab-index", type=int, default=0,
                        help="Browser tab index (0 = first tab)")
    parser.add_argument("--progress-file", default="_v7_progress.json",
                        help="Progress JSON filename (under replay_materials/)")
    args = parser.parse_args()

    # Gather signal directories
    all_dirs = sorted(
        d for d in MATERIALS_DIR.iterdir()
        if d.is_dir() and d.name.startswith(args.filter)
    )
    if not all_dirs:
        print(f"No signal dirs found in {MATERIALS_DIR} with filter '{args.filter}'")
        return 1

    # Separate cached vs todo
    done = 0
    todo = []
    for d in all_dirs:
        rp = d / "vlm_response.json"
        if rp.exists():
            try:
                ex = json.loads(rp.read_text())
                if ex.get("watch_summary") and not ex.get("error"):
                    done += 1
                    continue
            except (json.JSONDecodeError, OSError):
                pass
        todo.append(d)

    total = len(all_dirs)
    print(f"\n{'='*55}")
    print(f"  replay_materials/  |  Filter: '{args.filter or 'all'}'")
    print(f"  Total: {total}  Done: {done}  Todo: {len(todo)}")
    print(f"  Cooldown: {args.delay_min}-{args.delay_max}s  |  Per-coin chat, fresh on error  |  STOP: _STOP")
    print(f"{'='*55}")

    if not todo:
        print("  >> All done!")
        return 0

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        pages = browser.contexts[0].pages
        if args.tab_index >= len(pages):
            print(f"  ❌ tab index {args.tab_index} out of range ({len(pages)} pages)")
            return 1

        page = pages[args.tab_index]
        print(f"  Using tab[{args.tab_index}]: {await page.title()}")

        # Navigate to a fresh ChatGPT page
        try:
            await asyncio.wait_for(
                page.goto("https://chatgpt.com/", wait_until="domcontentloaded",
                          timeout=30000),
                timeout=40,
            )
            await asyncio.sleep(4)
        except Exception as e:
            print(f"  ⚠️ initial nav failed: {e}, continuing with current page")

        results = []
        counter = 0
        blocked = False
        current_symbol = None
        needs_fresh_chat = True  # first signal always needs a chat

        STOP_FILE = MATERIALS_DIR / "_STOP"
        for i, sig_dir in enumerate(todo):
            # Check STOP flag
            if STOP_FILE.exists():
                print(f"  🛑 STOP flag detected — exiting gracefully")
                break

            sym = sig_dir.name.split("_")[0]

            # Only new chat: first signal, symbol change, or after error
            if needs_fresh_chat or current_symbol is None or sym != current_symbol:
                reason = "first" if current_symbol is None else ("symbol change" if sym != current_symbol else "recovery")
                print(f"  🆕 New chat ({reason})")
                try:
                    await asyncio.wait_for(
                        page.goto("https://chatgpt.com/",
                                  wait_until="domcontentloaded", timeout=30000),
                        timeout=40,
                    )
                    await asyncio.sleep(4)
                except Exception as e:
                    print(f"  ⚠️ new-chat nav failed: {e}")
                needs_fresh_chat = False
            current_symbol = sym

            idx = done + i + 1
            result = await process_one(page, sig_dir, idx, total, MAX_RETRIES)
            results.append(result)

            s = result["status"]
            tail = ""
            if s == "ok":
                tail = f" ✓ ({result['chars']}ch, {result['pbs']}pbs)"
            elif s == "cached":
                tail = " ⏭ cached"
            elif s == "blocked":
                tail = f" 🛑 {result.get('error', '')}"
            else:
                tail = f" ✗ {result.get('error', '')[:60]}"
            print(f"    → {s}{tail}")

            # After error → fresh chat next time to reset state
            if s not in ("ok", "cached"):
                needs_fresh_chat = True

            # Persist progress every signal
            progress = {
                "done_before": done,
                "processed": len(results),
                "ok": sum(1 for r in results if r["status"] == "ok"),
                "err": sum(1 for r in results if r["status"] not in ("ok", "cached")),
                "blocked": sum(1 for r in results if r["status"] == "blocked"),
                "cached": sum(1 for r in results if r["status"] == "cached"),
                "last": result,
            }
            (MATERIALS_DIR / args.progress_file).write_text(
                json.dumps(progress, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            if s == "blocked":
                blocked = True
                print(f"  🛑 BLOCKED — awaiting user instruction")
                break

            counter += 1
            if counter % 10 == 0:
                ok_n = sum(1 for r in results if r["status"] == "ok")
                err_n = sum(1 for r in results if r["status"] not in ("ok", "cached"))
                print(f"  📊 progress: {done + len(results)}/{total}  ok={ok_n}  err={err_n}")

            # ── Between-signal cooldown ──
            if i < len(todo) - 1 and not blocked:
                delay = random.uniform(args.delay_min, args.delay_max)
                print(f"  ⏸ {delay:.0f}s cooldown...")
                await asyncio.sleep(delay)

        # Final report
        ok_n   = sum(1 for r in results if r["status"] == "ok")
        err_n  = sum(1 for r in results if r["status"] not in ("ok", "cached"))
        blocked_n = sum(1 for r in results if r["status"] == "blocked")
        print(f"\n  >> DONE  |  ok={ok_n}  cached={done}  err={err_n}  blocked={blocked_n}")
        await browser.close()

    return 0 if not blocked else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
