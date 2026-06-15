#!/usr/bin/env python3
"""
vlm_worker（re-arch Phase 3）— Windows 纯 ChatGPT 终端。

职责（严格限于浏览器交互，绝不参与交易决策）：
  - 读 local_vlm_pending/{pkg}/ → prompt.txt + 图
  - 调 ChatGPT 页面→ 抽 JSON → 写 local_vlm_done/{pkg}/vlm_response.json + .ready
  - 绝不写 state.json，绝不调用 _filter_playbooks / _build_state

铁律：
  - 只读 `signal.json` 用于获取 bar_time 做 stale 检查（不出现在 response 中）。
    不允许读取/写入/modify signal.json 中的交易字段用于任何决策。
  - 成功/拒绝后 pending 包移出活跃队列。
  - 失败包移 local_vlm_rejected/，不无限重试。

用法：
    python3 -m live.vlm_worker --cdp-url http://127.0.0.1:18800

环境变量：
    LOCAL_VLM_PENDING    本地 pending（默认 ROOT/local_vlm_pending）
    LOCAL_VLM_DONE       已完成（默认 ROOT/local_vlm_done）
    LOCAL_VLM_REJECTED   拒绝归档（默认 ROOT/local_vlm_rejected）
    LOCAL_VLM_DONE_PENDING 已完成 pending 归档（默认 ROOT/local_vlm_done_pending/archive）
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("vlm_worker")

# ── Paths ─────────────────────────────────────────────────────────────────────

LOCAL_VLM_PENDING       = Path(__file__).parent.parent / "local_vlm_pending"
LOCAL_VLM_DONE          = Path(__file__).parent.parent / "local_vlm_done"
LOCAL_VLM_REJECTED      = Path(__file__).parent.parent / "local_vlm_rejected"
LOCAL_VLM_DONE_PENDING  = Path(__file__).parent.parent / "local_vlm_done_pending" / "archive"

STATUS_FILE    = Path(__file__).parent.parent / "live" / "heartbeat" / "vlm_worker_status.json"
HEARTBEAT_FILE = Path(__file__).parent.parent / "live" / "heartbeat" / "vlm_worker_last_run.txt"
LOCK_FILE      = Path(__file__).parent.parent / "live" / "vlm_worker.lock"

CDP_URL     = "http://127.0.0.1:18800"
TIMEOUT     = 360
MAX_RETRIES = 1
POLL_SECONDS = 30
STALE_SECONDS = 7200
COOLDOWN_MIN = 90
COOLDOWN_MAX = 120


# ── JSON 解析（复用 openclaw 逻辑）────────────────────────────────────────────

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
    text = strip_fences(raw)
    for candidate in [text, re.sub(r",(\s*[}\]])", r"\1", text)]:
        try:
            d = json.loads(candidate)
            if isinstance(d, dict):
                return d
        except json.JSONDecodeError:
            pass
    return None


# ── ChatGPT 页面交互（从 openclaw 复用的核心逻辑）───────────────────────────

async def _fill_prompt(page, text: str) -> bool:
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
        }""", text)
    await asyncio.sleep(0.5)
    await page.keyboard.press("Space")
    await asyncio.sleep(0.2)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.5)
    return ok == "ok"


async def _send_message(page) -> bool:
    try:
        sb = page.locator('button[data-testid="send-button"]').first
        await sb.wait_for(state="visible", timeout=5000)
        disabled = await sb.get_attribute("disabled")
        if disabled is not None:
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)
            return True
        await sb.evaluate("el => el.click()")
        await asyncio.sleep(3)
        return True
    except Exception:
        try:
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)
            return True
        except Exception:
            return False


async def _screenshot(page, name: str) -> str:
    ss_dir = ROOT / "live" / "screenshots"
    ss_dir.mkdir(parents=True, exist_ok=True)
    path = ss_dir / f"{name}_{int(time.time())}.png"
    await page.screenshot(path=str(path))
    return str(path)


async def _wait_for_response(page, prev_count: int) -> tuple[Optional[str], Optional[str]]:
    start = time.time()
    last_text = ""
    stable = 0
    stuck_start = 0
    rate_limit_hits = 0

    while time.time() - start < TIMEOUT:
        await asyncio.sleep(4)
        try:
            rl = page.locator('text="请求过于频繁"').first
            if await rl.count() > 0:
                rate_limit_hits += 1
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
        try:
            if await page.locator('text="已停止思考"').first.count() > 0 and (time.time() - start) > 30:
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
                logger.debug("response growing: %d chars", cur_len)
        last_text = cur
        if stable >= 5:
            return text, None
    return last_text if len(last_text) > 500 else None, "timeout"


# ── 归档 ──────────────────────────────────────────────────────────────────────

def _archive(src: Path, dst_dir: Path, reason: str) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    try:
        (src / "reject_reason.txt").write_text(reason, encoding="utf-8")
    except Exception:
        pass
    dest = dst_dir / src.name
    if dest.exists():
        dest = dst_dir / f"{src.name}__dup_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S_%f')}"
    try:
        src.rename(dest)
        logger.info("archived %s → %s/ (%s)", src.name, dst_dir.name, reason)
    except Exception as e:
        logger.warning("archive failed %s: %s", src.name, e)


# ── 处理单包 ──────────────────────────────────────────────────────────────────

async def process_one(page, pkg_dir: Path) -> dict:
    dir_name = pkg_dir.name
    done_path = LOCAL_VLM_DONE / dir_name
    rec_path = done_path / "vlm_response.json"

    # Already done?
    if rec_path.exists():
        try:
            existing = json.loads(rec_path.read_text(encoding="utf-8"))
            if existing.get("watch_summary") and not existing.get("error"):
                return {"dir": dir_name, "status": "cached"}
        except Exception:
            pass

    prompt_file = pkg_dir / "prompt.txt"
    if not prompt_file.exists():
        return {"dir": dir_name, "status": "error", "error": "no_prompt.txt"}

    imgs_4h = sorted(pkg_dir.glob("*_4h.png"))
    imgs_15m = sorted(pkg_dir.glob("*_15m.png"))
    img_4h = imgs_4h[0] if imgs_4h else None
    img_15m = imgs_15m[0] if imgs_15m else None
    if not img_4h or not img_15m:
        return {"dir": dir_name, "status": "error", "error": "missing_images"}

    prompt = prompt_file.read_text(encoding="utf-8")

    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                logger.info("retry %d/%d for %s", attempt, MAX_RETRIES, dir_name)
                await asyncio.sleep(3)

            prev_count = await page.locator('[data-message-author-role="assistant"]').count()
            fi = page.locator('input[type="file"]').first
            await fi.set_input_files([str(img_4h), str(img_15m)])
            sb = page.locator('button[data-testid="send-button"]').first
            for _ in range(12):
                await asyncio.sleep(2)
                if await sb.get_attribute("disabled") is None:
                    break
            if not await _fill_prompt(page, prompt):
                return {"dir": dir_name, "status": "error", "error": "fill_failed"}
            await asyncio.sleep(2)
            for _ in range(6):
                if await sb.get_attribute("disabled") is None:
                    break
                await asyncio.sleep(2)
            if not await _send_message(page):
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(5)
                    continue
                return {"dir": dir_name, "status": "error", "error": "send_failed"}
            logger.info("sent to ChatGPT: %s", dir_name)
            text, err = await _wait_for_response(page, prev_count)
            if err:
                if attempt < MAX_RETRIES:
                    continue
                return {"dir": dir_name, "status": "error", "error": err}
            if not text or len(text) < 200:
                if attempt < MAX_RETRIES:
                    continue
                return {"dir": dir_name, "status": "error", "error": "too_short"}
            parsed = extract_json(text)
            if parsed is None:
                if attempt < MAX_RETRIES:
                    continue
                (pkg_dir / "_raw_response.txt").write_text(text, encoding="utf-8")
                return {"dir": dir_name, "status": "parse_err"}
            # Only write vlm_response.json (G5: NO state.json, NO signal.json)
            LOCAL_VLM_DONE.mkdir(parents=True, exist_ok=True)
            done_path.mkdir(parents=True, exist_ok=True)
            rec_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
            (done_path / ".ready").touch()
            # Archive the processed pending package (移出活跃队列)
            _archive(pkg_dir, LOCAL_VLM_DONE_PENDING, "processed")
            logger.info("vlm_response written: %s (%d chars)", dir_name, len(text))
            return {"dir": dir_name, "status": "ok", "chars": len(text),
                    "pbs": len(parsed.get("playbooks", []))}
        except Exception as e:
            if attempt >= MAX_RETRIES:
                return {"dir": dir_name, "status": "error", "error": str(e)[:200]}
    return {"dir": dir_name, "status": "error", "error": "max_retries"}


# ── Heartbeat / Status ────────────────────────────────────────────────────────

def _update_heartbeat() -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        HEARTBEAT_FILE.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")
    except Exception as e:
        logger.warning("heartbeat failed: %s", e)


def _write_status(stats: dict) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = dict(stats, updated_at=pd.Timestamp.now("UTC").isoformat())
        tmp = STATUS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, STATUS_FILE)
    except Exception as e:
        logger.warning("status write failed: %s", e)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("vlm_worker already running: %s", e)
        sys.exit(1)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-url", default=CDP_URL)
    parser.add_argument("--tab-index", type=int, default=0)
    args = parser.parse_args()

    LOCAL_VLM_PENDING.mkdir(parents=True, exist_ok=True)
    logger.info("vlm_worker started — watching %s", LOCAL_VLM_PENDING)

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(args.cdp_url)
        pages = browser.contexts[0].pages
        if args.tab_index >= len(pages):
            logger.error("tab index %d out of range (%d pages)", args.tab_index, len(pages))
            return
        page = pages[args.tab_index]
        try:
            await asyncio.wait_for(page.goto("https://chatgpt.com/",
                                             wait_until="domcontentloaded", timeout=30000), timeout=40)
            await asyncio.sleep(4)
        except Exception as e:
            logger.warning("nav failed: %s", e)

        stats = {"processed": 0, "ok": 0, "parse_err": 0, "error": 0,
                 "last_success_at": None, "last_error": None}
        stale_cutoff = pd.Timestamp.now("UTC") - pd.Timedelta(seconds=STALE_SECONDS)
        needs_fresh_chat = True
        current_symbol = None

        while True:
            pending = sorted([
                d for d in LOCAL_VLM_PENDING.iterdir()
                if d.is_dir() and (d / ".ready").exists()
                and not (d / "vlm_response.json").exists()
            ])
            if not pending:
                _update_heartbeat()
                _write_status(stats)
                await asyncio.sleep(POLL_SECONDS)
                continue

            for pkg_dir in pending:
                sym = pkg_dir.name.split("usdt")[0] + "usdt"
                if needs_fresh_chat or current_symbol != sym:
                    try:
                        await asyncio.wait_for(
                            page.goto("https://chatgpt.com/",
                                      wait_until="domcontentloaded", timeout=30000), timeout=40)
                        await asyncio.sleep(4)
                    except Exception as e:
                        logger.warning("nav failed: %s", e)
                    needs_fresh_chat = False
                current_symbol = sym

                result = await process_one(page, pkg_dir)
                stats["processed"] += 1
                s = result["status"]
                if s == "ok":
                    stats["ok"] += 1
                    stats["last_success_at"] = pd.Timestamp.now("UTC").isoformat()
                elif s in ("parse_err", "error", "cached"):
                    stats[s] = stats.get(s, 0) + 1
                    if s != "cached":
                        _archive(pkg_dir, LOCAL_VLM_REJECTED, f"{s}:{result.get('error','')[:100]}")
                        stats["last_error"] = f"{s}: {result.get('error','')[:80]}"
                        needs_fresh_chat = True
                _update_heartbeat()
                _write_status(stats)
                await asyncio.sleep(min(COOLDOWN_MIN, COOLDOWN_MAX))

            _update_heartbeat()
            _write_status(stats)


async def run() -> None:
    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="playwright")
    await main()


if __name__ == "__main__":
    asyncio.run(run())
