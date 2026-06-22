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
LEDGER_FILE   = ROOT / "live" / "state" / "shadow_ledger.jsonl"   # 每个终态一条,供绩效汇总
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
    if score.result == RESULT_TP1_HIT:
        return "tp1_final"        # TP1 命中后止损/无 TP2（终态）
    if score.result == RESULT_TP1_UNRESOLVED:
        return "tp1_running"      # TP1 命中，TP2 未决（仍在跑）
    return "activated"            # 已激活，尚无终态


# 终态相位（写台账 + 停止追踪）
_TERMINAL = {"tp2", "stopped", "cancelled", "tp1_final"}

# 相位 → 流水文案
_EMOJI = {
    "activated":   "🎯 激活",
    "tp1_running": "🟢 TP1 命中(TP2 未决)",
    "tp1_final":   "🟢 TP1 命中(终)",
    "tp2":         "🟢🟢 TP1+TP2 命中",
    "stopped":     "🔴 SL 止损",
    "cancelled":   "⚪ 激活前取消",
}

# 终态 → 结构 R（粗口径：SL=-1, TP1=+1, TP2=+2;非交易=0。
# 注意:这不是精确的 S3+成本 R——入场用收盘代理,仅供看走向/胜率,不当真实盈亏）
_STRUCT_R = {"stopped": -1.0, "tp1_final": 1.0, "tp2": 2.0, "cancelled": 0.0}


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


def _append_ledger(rec: dict) -> None:
    LEDGER_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(LEDGER_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("ledger append failed: %s", e)


def report() -> str:
    """读台账,汇总影子绩效。返回可打印文本。"""
    if not LEDGER_FILE.exists():
        return "影子台账为空（还没有信号走到终态）。"
    recs = []
    for line in LEDGER_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                recs.append(json.loads(line))
            except Exception:
                pass
    if not recs:
        return "影子台账为空。"

    def _summ(rows: list) -> str:
        n = len(rows)
        tp2 = sum(1 for r in rows if r["phase"] == "tp2")
        tp1 = sum(1 for r in rows if r["phase"] == "tp1_final")
        sl  = sum(1 for r in rows if r["phase"] == "stopped")
        can = sum(1 for r in rows if r["phase"] == "cancelled")
        activated = n - can                       # cancelled = 激活前取消
        wins = tp1 + tp2
        total_r = sum(r.get("struct_r", 0.0) for r in rows)
        wr = (wins / activated * 100) if activated else 0.0
        exp = (total_r / activated) if activated else 0.0
        return (f"n={n} 激活={activated} (取消{can}) | TP2={tp2} TP1={tp1} SL={sl} | "
                f"胜率={wr:.0f}% | 结构R合计={total_r:+.1f} 期望={exp:+.2f}R/笔")

    lines = ["=== 影子绩效（结构 R 粗口径，非真实盈亏）===",
             "总体: " + _summ(recs), "", "按品种:"]
    syms = sorted({r["symbol"] for r in recs})
    for s in syms:
        lines.append(f"  {s:10s}: " + _summ([r for r in recs if r["symbol"] == s]))
    lines.append("")
    lines.append("最近 10 条:")
    for r in recs[-10:]:
        lines.append(f"  {r.get('bar_time','?')[:16]} {r['symbol']} {r['hypothesis']} "
                     f"→ {r['phase']} ({r.get('struct_r',0):+.0f}R)")
    return "\n".join(lines)


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
                # 进入终态 → 写台账(供绩效汇总),每个 key 只写一次
                if new_phase in _TERMINAL:
                    _append_ledger({
                        "ts": pd.Timestamp.now("UTC").isoformat(),
                        "pkg": pkg.name, "symbol": symbol, "hypothesis": hyp,
                        "phase": new_phase, "result": score.result,
                        "struct_r": _STRUCT_R.get(new_phase, 0.0),
                        "r_distance": score.r_distance,
                        "mfe_r": score.mfe_r, "mae_r": score.mae_r,
                        "bars_to_activation": score.bars_to_activation,
                        "bars_to_tp1": score.bars_to_tp1,
                        "bars_to_invalidation": score.bars_to_invalidation,
                        "bar_time": bar_time,
                    })
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

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--report", action="store_true", help="打印影子绩效汇总后退出")
    args = ap.parse_args()

    # --report 只读，不抢锁——常驻进程跑着时也能随时看绩效
    if args.report:
        print(report())
        return

    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("shadow_tracker already running: %s", e)
        sys.exit(1)

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
