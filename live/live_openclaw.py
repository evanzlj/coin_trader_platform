#!/usr/bin/env python3
"""
Live openclaw — signal_pending/ → ChatGPT → vlm_response.json → signal_active/

Polls signal_pending/ for packages with .ready but no vlm_response.json.
For each: checks signal freshness, uploads prompt + charts to ChatGPT,
extracts JSON, applies post-VLM playbook filter, writes state.json,
and moves the package to signal_active/ for the executor to pick up.

Usage:
    python3 live/live_openclaw.py
    python3 live/live_openclaw.py --cdp-url http://127.0.0.1:18800
    python3 live/live_openclaw.py --delay-min 40 --delay-max 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import re
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

SIGNAL_PENDING  = ROOT / "signal_pending"
SIGNAL_ACTIVE   = ROOT / "signal_active"
SIGNAL_REJECTED = ROOT / "signal_rejected"
SIGNAL_STALE    = ROOT / "signal_stale"
HEARTBEAT_FILE  = ROOT / "live" / "heartbeat" / "openclaw_last_run.txt"
STATUS_FILE     = ROOT / "live" / "heartbeat" / "openclaw_status.json"
LOCK_FILE       = ROOT / "live" / "openclaw.lock"
CDP_URL         = "http://127.0.0.1:18800"
TIMEOUT        = 360      # per-signal wait timeout (seconds)
MAX_RETRIES    = 1
PROMPT_VERSION = "v1.0"
COOLDOWN_MIN   = 90       # seconds between signals
COOLDOWN_MAX   = 120
POLL_INTERVAL  = 30       # seconds between pending-queue checks
STALE_SECONDS  = 7200     # 2h — skip signal if bar_time is older than this


# ── JSON utilities ────────────────────────────────────────────────────────────

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


def validate_response(parsed: dict) -> list[str]:
    required = ["watch_summary", "playbooks"]
    return [k for k in required if k not in parsed]


# ── ChatGPT page helpers ──────────────────────────────────────────────────────

async def fill_prompt(page, text: str) -> bool:
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
    try:
        sb = page.locator('button[data-testid="send-button"]').first
        await sb.wait_for(state="visible", timeout=5000)
        disabled = await sb.get_attribute("disabled")
        if disabled is not None:
            logger.warning("send button disabled — trying Enter fallback")
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)
            return True
        await sb.evaluate("el => el.click()")
        await asyncio.sleep(3)
        return True
    except Exception as e:
        logger.warning("send evaluate failed: %s, trying native click", e)
        try:
            sb = page.locator('button[data-testid="send-button"]').first
            await sb.click(timeout=5000)
            await asyncio.sleep(3)
            return True
        except Exception:
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(3)
                return True
            except Exception as e3:
                logger.error("send FAILED: %s", e3)
                await screenshot(page, "send_failed")
                return False


async def screenshot(page, name: str) -> str:
    ss_dir = ROOT / "live" / "screenshots"
    ss_dir.mkdir(parents=True, exist_ok=True)
    path = ss_dir / f"{name}_{int(time.time())}.png"
    await page.screenshot(path=str(path))
    return str(path)


async def handle_error(page, error_type: str) -> bool:
    if error_type in ("thinking_failed", "stuck_analyzing"):
        ss = await screenshot(page, error_type)
        logger.warning("%s → screenshot: %s, waiting 30s then retry", error_type, ss)
        await asyncio.sleep(30)
        return True

    if error_type == "server_error":
        rb = page.locator('button:has-text("重试")').first
        if await rb.count() > 0:
            try:
                await rb.click(timeout=5000)
            except Exception:
                await rb.evaluate("el => el.click()")
            logger.info("clicked retry — waiting for response")
            await asyncio.sleep(5)
            return True
        return False

    if error_type == "timeout":
        ss = await screenshot(page, "timeout")
        logger.warning("timeout → screenshot: %s, waiting 10s then retry", ss)
        await asyncio.sleep(10)
        return True

    if error_type in ("rate_limit", "blocked"):
        logger.error("BLOCKED: %s — pausing, awaiting user", error_type)
        return False

    logger.warning("unknown error: %s", error_type)
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
                logger.warning("rate limit modal #%d", rate_limit_hits)
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


# ── Per-signal processing ─────────────────────────────────────────────────────

async def process_one(page, pkg_dir: Path, retry_budget: int) -> dict:
    dir_name = pkg_dir.name
    vlm_path = pkg_dir / "vlm_response.json"

    # Already done?
    if vlm_path.exists():
        try:
            existing = json.loads(vlm_path.read_text(encoding="utf-8"))
            if existing.get("watch_summary") and not existing.get("error"):
                return {"dir": dir_name, "status": "cached"}
        except (json.JSONDecodeError, OSError):
            pass

    prompt_file = pkg_dir / "prompt.txt"
    if not prompt_file.exists():
        return {"dir": dir_name, "status": "error", "error": "no_prompt.txt"}

    imgs_4h  = sorted(pkg_dir.glob("*_4h.png"))
    imgs_15m = sorted(pkg_dir.glob("*_15m.png"))
    img_4h   = imgs_4h[0] if imgs_4h else None
    img_15m  = imgs_15m[0] if imgs_15m else None

    if not img_4h or not img_15m:
        missing = []
        if not img_4h:  missing.append("4h.png")
        if not img_15m: missing.append("15m.png")
        return {"dir": dir_name, "status": "error", "error": f"missing_images:{missing}"}

    prompt = prompt_file.read_text(encoding="utf-8")

    for attempt in range(retry_budget + 1):
        try:
            if attempt > 0:
                logger.info("retry %d/%d for %s", attempt, retry_budget, dir_name)
                await asyncio.sleep(3)

            prev_count = await page.locator(
                '[data-message-author-role="assistant"]'
            ).count()

            fi = page.locator('input[type="file"]').first
            await fi.set_input_files([str(img_4h), str(img_15m)])

            sb = page.locator('button[data-testid="send-button"]').first
            for _ in range(12):  # up to 24 seconds for upload
                await asyncio.sleep(2)
                disabled = await sb.get_attribute("disabled")
                if disabled is None:
                    break

            if not await fill_prompt(page, prompt):
                return {"dir": dir_name, "status": "error", "error": "fill_failed"}

            await asyncio.sleep(2)
            for _ in range(6):
                disabled = await sb.get_attribute("disabled")
                if disabled is None:
                    break
                await asyncio.sleep(2)

            if not await send_message(page):
                if attempt < retry_budget:
                    await asyncio.sleep(5)
                    continue
                return {"dir": dir_name, "status": "error", "error": "send_failed"}

            logger.info("sent to ChatGPT: %s", dir_name)
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

            parsed = extract_json(text)
            if parsed is None:
                if attempt < retry_budget:
                    continue
                (pkg_dir / "_raw_response.txt").write_text(text, encoding="utf-8")
                return {"dir": dir_name, "status": "parse_err"}

            missing_keys = validate_response(parsed)
            if missing_keys:
                if attempt < retry_budget:
                    continue
                (pkg_dir / "_raw_response.txt").write_text(text, encoding="utf-8")
                vlm_path.write_text(
                    json.dumps(
                        {"parsed": parsed, "error": f"missing:{missing_keys}"},
                        ensure_ascii=False, indent=2,
                    ),
                    encoding="utf-8",
                )
                return {"dir": dir_name, "status": "validation_err",
                        "error": f"missing:{missing_keys}"}

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
                return {"dir": dir_name, "status": "error", "error": str(e)[:200]}

    return {"dir": dir_name, "status": "error", "error": "max_retries"}


# ── Package archival helper ───────────────────────────────────────────────────

def _archive_package(pkg_dir: Path, dest_dir: Path, reason: str) -> bool:
    """Write reject_reason.txt then rename pkg_dir → dest_dir/. Returns True if the
    source left signal_pending/.

    Idempotency (#22 P1): if the destination already exists we must STILL move the
    source out of signal_pending — otherwise the same package is re-scanned and
    re-rejected every poll forever. On conflict we append a microsecond timestamp
    suffix (the reject_reason is preserved inside the dir either way).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        (pkg_dir / "reject_reason.txt").write_text(reason, encoding="utf-8")
    except Exception:
        pass
    dest = dest_dir / pkg_dir.name
    if dest.exists():
        suffix = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        dest = dest_dir / f"{pkg_dir.name}__dup_{suffix}"
    try:
        pkg_dir.rename(dest)
        logger.info("archived → %s/ (%s): %s", dest_dir.name, reason, dest.name)
        return True
    except Exception as e:
        logger.warning("archive failed for %s: %s", pkg_dir.name, e)
        return False


# ── Post-VLM filter + signal_active/ move ────────────────────────────────────

def _r_dist_pct(activation_rule: dict) -> float | None:
    """Return r_dist (%) = abs(activation_price - invalidation_level) / activation_price."""
    act = activation_rule.get("activates_if_close_crosses", {})
    inv = activation_rule.get("invalidation_after_activation", {})
    act_level = act.get("level")
    inv_level = inv.get("level")
    if act_level is None or inv_level is None or act_level == 0:
        return None
    return abs(act_level - inv_level) / act_level * 100


def _tp1_dist_pct(activation_rule: dict) -> float | None:
    """Return TP1 distance (%) from activation price."""
    act = activation_rule.get("activates_if_close_crosses", {})
    act_level = act.get("level")
    objectives = activation_rule.get("objectives", [])
    tp1_levels = [o.get("level") for o in objectives if o.get("level") is not None]
    if not tp1_levels or act_level is None or act_level == 0:
        return None
    return abs(tp1_levels[0] - act_level) / act_level * 100


def _passes_filters(symbol: str, r_dist: float | None, tp1_pct: float | None) -> tuple[bool, str]:
    """
    Apply per-symbol post-VLM filters. Returns (passes, reason).

    Rules (from aonly_research_summary_20260608.md):
      BTC: r_dist >= 0.5%  +  TP1 < 1.5%
      ETH: r_dist >= 1.5%  +  排除 TP1 1–2%
      BNB: 0.3% <= r_dist <= 1.0%
      SOL: r_dist >= 1.5%

    b2act >= 2 is enforced by executor at activation time.
    """
    if r_dist is None or tp1_pct is None:
        return True, ""   # can't compute → don't filter

    if symbol == "BTC/USDT":
        if r_dist < 0.5:
            return False, f"r_dist {r_dist:.3f}% < 0.5%"
        if tp1_pct >= 1.5:
            return False, f"tp1 {tp1_pct:.3f}% >= 1.5%"

    elif symbol == "ETH/USDT":
        if r_dist < 1.5:
            return False, f"r_dist {r_dist:.3f}% < 1.5%"
        if 1.0 <= tp1_pct <= 2.0:
            return False, f"tp1 {tp1_pct:.3f}% in dead zone 1–2%"

    elif symbol == "BNB/USDT":
        if not (0.3 <= r_dist <= 1.0):
            return False, f"r_dist {r_dist:.3f}% outside 0.3–1.0%"

    elif symbol == "SOL/USDT":
        if r_dist < 1.5:
            return False, f"r_dist {r_dist:.3f}% < 1.5%"

    return True, ""


def _filter_playbooks(signal: dict, vlm: dict) -> list[dict]:
    """
    Return playbooks that pass post-VLM filters.

    Excludes:
    - activation_rule is None (can never activate)
    - fails per-symbol r_dist or TP1 zone filter

    b2act >= 2 is enforced by executor at activation time.
    No plausibility or trade_side filter (replay scores all playbooks).
    """
    symbol = signal.get("symbol", "")
    valid = []
    for pb in vlm.get("playbooks", []):
        plan = pb.get("conditional_trade_plan", {})
        ar = plan.get("activation_rule")
        if ar is None:
            continue
        r_dist  = _r_dist_pct(ar)
        tp1_pct = _tp1_dist_pct(ar)
        passes, reason = _passes_filters(symbol, r_dist, tp1_pct)
        if not passes:
            logger.info("filter: %s %s excluded — %s",
                        symbol, pb.get("hypothesis", "?"), reason)
            continue
        valid.append(pb)
    return valid


def _build_state(signal: dict, valid_playbooks: list[dict], pkg_name: str) -> dict:
    """Build initial state.json for the executor."""
    pb_states = []
    for pb in valid_playbooks:
        plan = pb.get("conditional_trade_plan", {})
        ar = plan.get("activation_rule", {}) or {}
        objectives = ar.get("objectives", [])
        tp_levels = [o.get("level") for o in objectives if o.get("level") is not None]
        act = ar.get("activates_if_close_crosses", {})
        inv = ar.get("invalidation_after_activation", {})

        tp1_pct = _tp1_dist_pct(ar)
        act_level = act.get("level")
        inv_level = inv.get("level")
        r_dist_pct = (
            abs(act_level - inv_level) / act_level * 100
            if act_level and inv_level and act_level != 0
            else None
        )

        pb_states.append({
            "hypothesis":          pb.get("hypothesis"),
            "direction":           ar.get("direction_if_activated"),
            "status":              "WAITING_FOR_PRIMARY_TOUCH",
            "primary_touch":       ar.get("primary_touch"),
            "activates_if":        ar.get("activates_if_close_crosses"),
            "cancels_if":          ar.get("cancels_if_close_crosses_first"),
            "invalidation":        ar.get("invalidation_after_activation"),
            "tp1_level":           tp_levels[0] if len(tp_levels) > 0 else None,
            "tp2_level":           tp_levels[1] if len(tp_levels) > 1 else None,
            "tp1_dist_pct":        round(tp1_pct, 4) if tp1_pct is not None else None,
            "r_dist_pct":          round(r_dist_pct, 4) if r_dist_pct is not None else None,
            "bars_since_t0":       0,
            "primary_touched_at":  None,
            "activated_at":        None,
            "activation_price":    None,
            "tp1_hit_at":          None,
            "done_at":             None,
            "result":              None,
            "pnl_r":               None,
        })

    return {
        "signal_dir":     pkg_name,
        "symbol":         signal.get("symbol"),
        "grade":          signal.get("grade"),
        "bar_time":       signal.get("bar_time"),
        "structure_side": signal.get("structure_side"),
        "created_at":     pd.Timestamp.utcnow().isoformat(),
        "overall_status": "WATCHING",
        "playbooks":      pb_states,
    }


def move_to_active(pkg_dir: Path, signal: dict, vlm: dict) -> str:
    """
    Apply post-VLM filter, build state.json, move pkg_dir → signal_active/.
    Returns one of: "moved" (handed to executor), "rejected" (no valid playbooks),
    "exists" (executor already had it; pending copy archived).
    """
    valid_pbs = _filter_playbooks(signal, vlm)
    if not valid_pbs:
        logger.info("all playbooks filtered for %s → signal_rejected/", pkg_dir.name)
        _archive_package(pkg_dir, SIGNAL_REJECTED, "filtered_no_valid_playbooks")
        return "rejected"

    state = _build_state(signal, valid_pbs, pkg_dir.name)
    (pkg_dir / "state.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    SIGNAL_ACTIVE.mkdir(parents=True, exist_ok=True)
    dest = SIGNAL_ACTIVE / pkg_dir.name
    if dest.exists():
        # Executor already owns this package — don't clobber its authoritative copy,
        # but DO get the redundant pending copy out of signal_pending/ (idempotency).
        logger.warning("signal_active/%s already exists — archiving pending copy", pkg_dir.name)
        _archive_package(pkg_dir, SIGNAL_REJECTED, "already_in_signal_active")
        return "exists"

    pkg_dir.rename(dest)
    logger.info("moved to signal_active/: %s (%d playbooks)", pkg_dir.name, len(valid_pbs))
    return "moved"


# ── Pending queue helpers ─────────────────────────────────────────────────────

def get_pending(stale_cutoff: pd.Timestamp) -> list[Path]:
    """Return package dirs that have .ready but no valid vlm_response.json,
    sorted by directory name (= bar_time order).
    Stale packages (bar_time < stale_cutoff) are logged and excluded.
    """
    if not SIGNAL_PENDING.exists():
        return []

    pending = []
    for d in sorted(SIGNAL_PENDING.iterdir()):
        if not d.is_dir():
            continue
        if not (d / ".ready").exists():
            continue

        # vlm done?  Only skip if package already landed in signal_active/.
        # If still in signal_pending/, vlm was written but move_to_active crashed —
        # return it so the cached branch can complete the move.
        vlm = d / "vlm_response.json"
        if vlm.exists():
            try:
                ex = json.loads(vlm.read_text(encoding="utf-8"))
                if ex.get("watch_summary") and not ex.get("error"):
                    if (SIGNAL_ACTIVE / d.name).exists():
                        # Executor already owns it. Don't just `continue` (that leaves a
                        # residual pending dir re-scanned every poll) — archive the
                        # redundant pending copy out of signal_pending/ (F8).
                        _archive_package(d, SIGNAL_REJECTED, "already_in_signal_active")
                        continue
                    # otherwise: fall through → process_one returns "cached" → move_to_active
            except (json.JSONDecodeError, OSError):
                pass

        # Stale check + A+ guard — archive instead of skip to stop repeated logging
        sig_file = d / "signal.json"
        if sig_file.exists():
            try:
                sig = json.loads(sig_file.read_text(encoding="utf-8"))
                if sig.get("grade") == "A+":
                    _archive_package(d, SIGNAL_STALE, "a_plus_not_live")
                    continue
                bar_time = pd.Timestamp(sig["bar_time"])
                if bar_time.tzinfo is None:
                    bar_time = bar_time.tz_localize("UTC")
                if bar_time < stale_cutoff:
                    _archive_package(d, SIGNAL_STALE, f"stale_bar_time={bar_time.date()}")
                    continue
            except Exception as e:
                logger.warning("could not read bar_time from %s: %s", d.name, e)

        pending.append(d)

    return pending


def extract_symbol(dir_name: str) -> str:
    """btcusdt_A_20260608_2100 → btc"""
    return dir_name.split("usdt")[0]


def update_heartbeat() -> None:
    """Write current timestamp to heartbeat file for watchdog."""
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_FILE.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")


def write_openclaw_status(stats: dict) -> None:
    """Atomically write functional-health status for the watchdog (#13).

    Tracks cumulative processed/moved/rejected/blocked/move_failures plus
    consecutive_rejections (resets to 0 on any successful move). The watchdog
    --role china alerts when consecutive_rejections / move_failures / parse_errs
    breach thresholds — a producer that parses/moves nothing is functionally
    dead even though its heartbeat is fresh.
    """
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(stats)
    payload["updated_at"] = pd.Timestamp.now("UTC").isoformat()
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_FILE)


def resolve_cdp_url(http_url: str) -> str:
    """Resolve HTTP CDP endpoint to WebSocket URL.
    Playwright 1.60+ may reject HTTP-style CDP on some Chrome versions;
    fall back to extracting webSocketDebuggerUrl from /json/version/.
    """
    try:
        resp = urllib.request.urlopen(f"{http_url.rstrip('/')}/json/version/", timeout=5)
        data = json.loads(resp.read().decode())
        ws_url = data.get("webSocketDebuggerUrl")
        if ws_url:
            logger.info("resolved CDP WS: %s", ws_url[:60] + "...")
            return ws_url
    except Exception as e:
        logger.warning("could not resolve WS from %s: %s", http_url, e)
    return http_url  # fallback, let Playwright try original


# ── Main loop ─────────────────────────────────────────────────────────────────

async def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("openclaw already running: %s", e)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="live_openclaw: signal_pending → ChatGPT → vlm_response.json")
    parser.add_argument("--cdp-url", default=CDP_URL)
    parser.add_argument("--delay-min", type=float, default=COOLDOWN_MIN)
    parser.add_argument("--delay-max", type=float, default=COOLDOWN_MAX)
    parser.add_argument("--tab-index", type=int, default=0)
    args = parser.parse_args()

    SIGNAL_PENDING.mkdir(parents=True, exist_ok=True)
    logger.info("live_openclaw started — watching %s", SIGNAL_PENDING)

    async with async_playwright() as p:
        cdp_target = resolve_cdp_url(args.cdp_url)
        browser = await p.chromium.connect_over_cdp(cdp_target)
        pages = browser.contexts[0].pages
        if args.tab_index >= len(pages):
            logger.error("tab index %d out of range (%d pages)", args.tab_index, len(pages))
            return

        page = pages[args.tab_index]
        logger.info("using tab[%d]: %s", args.tab_index, await page.title())

        try:
            await asyncio.wait_for(
                page.goto("https://chatgpt.com/", wait_until="domcontentloaded", timeout=30000),
                timeout=40,
            )
            await asyncio.sleep(4)
        except Exception as e:
            logger.warning("initial nav failed: %s, continuing with current page", e)

        current_symbol = None
        needs_fresh_chat = True

        # Cumulative functional-health counters (process lifetime). consecutive_rejections
        # resets to 0 on any successful move → it's the "producer is alive AND productive"
        # signal the watchdog keys off.
        stats = {
            "processed":              0,
            "moved":                  0,
            "rejected":               0,
            "blocked":                0,
            "move_failures":          0,
            "parse_errs":             0,
            "consecutive_rejections": 0,
            "last_success_at":        None,
            "last_error":             None,
        }

        while True:
            stale_cutoff = pd.Timestamp.now("UTC") - pd.Timedelta(seconds=STALE_SECONDS)
            pending = get_pending(stale_cutoff)

            if not pending:
                update_heartbeat()
                write_openclaw_status(stats)
                logger.debug("no pending packages — sleeping %ds", POLL_INTERVAL)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            logger.info("%d package(s) pending", len(pending))

            for pkg_dir in pending:
                # Heartbeat per package: a batch of 3 × 360s timeout could otherwise
                # approach the watchdog's 20-min stale threshold (#13).
                update_heartbeat()
                stats["processed"] += 1
                sym = extract_symbol(pkg_dir.name)

                if needs_fresh_chat or current_symbol is None or sym != current_symbol:
                    reason = ("first" if current_symbol is None
                              else "symbol_change" if sym != current_symbol
                              else "recovery")
                    logger.info("new chat (%s)", reason)
                    try:
                        await asyncio.wait_for(
                            page.goto("https://chatgpt.com/",
                                      wait_until="domcontentloaded", timeout=30000),
                            timeout=40,
                        )
                        await asyncio.sleep(4)
                    except Exception as e:
                        logger.warning("new-chat nav failed: %s", e)
                    needs_fresh_chat = False
                current_symbol = sym

                result = await process_one(page, pkg_dir, MAX_RETRIES)
                s = result["status"]

                if s in ("ok", "cached"):
                    if s == "ok":
                        logger.info("ok: %s  (%d chars, %d playbooks)",
                                    result["dir"], result["chars"], result["pbs"])
                    else:
                        logger.info("cached: %s", result["dir"])
                    try:
                        signal = json.loads((pkg_dir / "signal.json").read_text(encoding="utf-8"))
                        vlm    = json.loads((pkg_dir / "vlm_response.json").read_text(encoding="utf-8"))
                        outcome = move_to_active(pkg_dir, signal, vlm)   # #22 P1：有 playbooks → executor 接管
                        if outcome in ("moved", "exists"):
                            stats["moved"] += 1
                            stats["consecutive_rejections"] = 0
                            stats["last_success_at"] = pd.Timestamp.now("UTC").isoformat()
                        else:  # "rejected": all playbooks filtered
                            stats["rejected"] += 1
                            stats["consecutive_rejections"] += 1
                    except Exception as e:
                        logger.warning("move_to_active failed for %s: %s", pkg_dir.name, e)
                        stats["move_failures"] += 1
                        stats["consecutive_rejections"] += 1
                        stats["last_error"] = f"move_to_active: {str(e)[:120]}"
                elif s == "blocked":
                    logger.error("BLOCKED: %s — %s", result["dir"], result.get("error", ""))
                    logger.error("pausing — please resolve and restart")
                    stats["blocked"] += 1
                    stats["last_error"] = f"blocked: {result.get('error', '')[:120]}"
                    write_openclaw_status(stats)
                    await browser.close()
                    return
                else:
                    logger.warning("%s: %s — %s", s, result["dir"], result.get("error", "")[:80])
                    _archive_package(
                        pkg_dir, SIGNAL_REJECTED,
                        f"{s}:{result.get('error', '')[:100]}",
                    )
                    stats["rejected"] += 1
                    stats["consecutive_rejections"] += 1
                    if s == "parse_err":
                        stats["parse_errs"] += 1
                    stats["last_error"] = f"{s}: {result.get('error', '')[:120]}"

                if s not in ("ok", "cached"):
                    needs_fresh_chat = True

                update_heartbeat()
                write_openclaw_status(stats)

                # Cooldown between signals (skip after last in batch)
                remaining = get_pending(pd.Timestamp.now("UTC") - pd.Timedelta(seconds=STALE_SECONDS))
                if remaining:
                    delay = random.uniform(args.delay_min, args.delay_max)
                    logger.info("cooldown %.0fs ...", delay)
                    await asyncio.sleep(delay)

            update_heartbeat()
            write_openclaw_status(stats)

            # Brief pause before next poll
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
