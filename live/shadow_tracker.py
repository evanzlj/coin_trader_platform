#!/usr/bin/env python3
"""
shadow_tracker — 影子追踪器（只读观察，零下单）。

对 signal_active 里每个已通过的信号，用 replay scorer 在真实 15m K 线上「前向」回放，
观察每个剧本的走向（未触发 → 激活 → TP1/TP2 或 SL），状态变化时推业务流水。

铁律：
  - 只读 signal_active + 历史 K 线 CSV。绝不连 broker、绝不下任何单。
  - 口径直接复用 replay.scorer.score_playbook —— 和回测 / +33.6R 完全一致。
  - 每个 (信号包, 剧本) 的「相位」持久化；只在相位变化时推流水，不刷屏。

用法：
    python3 -m live.shadow_tracker            # 常驻轮询
    python3 -m live.shadow_tracker --once     # 单次（测试用）

环境变量：
    SIGNAL_ACTIVE   executor 信号目录（默认 ROOT/signal_active）
    SHADOW_DATA_DIR 15m CSV 根目录（默认 ROOT/history_data_manager/data）
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from replay.scorer import (
    score_playbook,
    RESULT_NOT_TRIGGERED, RESULT_ACTIVATION_CANCELLED, RESULT_INVALIDATED,
    RESULT_TP1_HIT, RESULT_TP1_TP2_HIT, RESULT_TP1_UNRESOLVED,
)
from realtime_data_pull.config import SYMBOL_SLUG

logger = logging.getLogger("shadow_tracker")

SIGNAL_ACTIVE = Path(os.environ.get("SIGNAL_ACTIVE", str(ROOT / "signal_active")))
DATA_DIR      = Path(os.environ.get("SHADOW_DATA_DIR", str(ROOT / "history_data_manager" / "data")))
STATE_FILE    = ROOT / "live" / "state" / "shadow_tracker_state.json"
HEARTBEAT     = ROOT / "live" / "heartbeat" / "shadow_tracker_last_run.txt"
LOCK_FILE     = ROOT / "live" / "shadow_tracker.lock"
POLL_SECONDS  = 300


# ── 相位：把 scorer 的 result 映射成可观察的阶段 ──────────────────────────────

def _phase(score) -> str:
    if score.result == RESULT_ACTIVATION_CANCELLED:
        return "cancelled"
    if score.activated_at is None:
        return "waiting"          # 还没激活（not_triggered）
    if score.result == RESULT_INVALIDATED:
        return "stopped"          # 激活后被 SL
    if score.result == RESULT_TP1_TP2_HIT:
        return "tp2"
    if score.result in (RESULT_TP1_HIT, RESULT_TP1_UNRESOLVED):
        return "tp1"
    return "activated"            # 已激活，尚无终态


_TERMINAL = {"tp2", "stopped", "cancelled"}

# 相位 → 流水文案
_EMOJI = {
    "activated": "🎯 激活",
    "tp1":       "🟢 TP1 命中",
    "tp2":       "🟢🟢 TP1+TP2 命中",
    "stopped":   "🔴 SL 止损",
    "cancelled": "⚪ 激活前取消",
}


# ── K 线加载 ───────────────────────────────────────────────────────────────────

def _load_post_bars(symbol: str, t0: pd.Timestamp) -> Optional[pd.DataFrame]:
    slug = SYMBOL_SLUG.get(symbol)
    if slug is None:
        return None
    path = DATA_DIR / "ohlcv" / f"{slug}_15m.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=["open_time"])
    if df["open_time"].dt.tz is None:
        df["open_time"] = df["open_time"].dt.tz_localize("UTC")
    else:
        df["open_time"] = df["open_time"].dt.tz_convert("UTC")
    df = df[df["open_time"] > t0]                       # 仅 T0 之后
    return df[["open_time", "high", "low", "close"]].sort_values("open_time").reset_index(drop=True)


# ── 状态持久化 ─────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATE_FILE)


def _flow(message: str) -> None:
    """业务流水推送，永不抛。"""
    try:
        from live import notify
        notify.flow_event(message)
    except Exception as e:
        logger.warning("flow_event failed (non-fatal): %s", e)


# ── 单轮扫描 ───────────────────────────────────────────────────────────────────

def scan_once() -> int:
    """扫一遍 signal_active，推进每个剧本的相位，变化则推流水。返回推送条数。"""
    state = _load_state()
    pushed = 0
    if not SIGNAL_ACTIVE.exists():
        return 0
    for pkg in sorted(p for p in SIGNAL_ACTIVE.iterdir() if p.is_dir()):
        st_file = pkg / "state.json"
        vr_file = pkg / "vlm_response.json"
        if not st_file.exists() or not vr_file.exists():
            continue
        try:
            st = json.loads(st_file.read_text(encoding="utf-8"))
            vr = json.loads(vr_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        symbol = st.get("symbol")
        bar_time = st.get("bar_time")
        side = st.get("structure_side", "near_support")
        if not symbol or not bar_time:
            continue
        try:
            t0 = pd.Timestamp(bar_time)
            if t0.tzinfo is None:
                t0 = t0.tz_localize("UTC")
        except Exception:
            continue
        df_post = _load_post_bars(symbol, t0)
        if df_post is None or df_post.empty:
            continue

        for pb in vr.get("playbooks", []):
            ar = (pb.get("conditional_trade_plan") or {}).get("activation_rule")
            if not ar:
                continue                       # CHOP_WAIT / no_trade
            hyp = pb.get("hypothesis", "?")
            key = f"{pkg.name}::{hyp}"
            if state.get(key) in _TERMINAL:
                continue                       # 已终态，不再追踪

            try:
                score = score_playbook(ar, hyp, df_post, structure_side=side)
            except Exception as e:
                logger.warning("score_playbook failed %s: %s", key, e)
                continue
            new_phase = _phase(score)
            old_phase = state.get(key, "waiting")
            if new_phase != old_phase and new_phase != "waiting":
                label = _EMOJI.get(new_phase, new_phase)
                _flow(f"{label} {symbol} {hyp}")
                pushed += 1
            state[key] = new_phase

    _save_state(state)
    return pushed


def _heartbeat() -> None:
    HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    try:
        HEARTBEAT.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")
    except Exception:
        pass


def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("shadow_tracker already running: %s", e)
        sys.exit(1)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    logger.info("shadow_tracker started — watching %s (read-only, no broker)", SIGNAL_ACTIVE)
    while True:
        try:
            n = scan_once()
            if n:
                logger.info("pushed %d phase transition(s)", n)
        except Exception:
            logger.exception("shadow_tracker scan error (non-fatal)")
        _heartbeat()
        if args.once:
            return
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
