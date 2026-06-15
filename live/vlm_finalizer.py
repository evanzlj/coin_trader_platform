#!/usr/bin/env python3
"""
vlm_finalizer（re-arch Phase 2, §REARCH §10 G5）— 接收 VLM response，过滤 + 建 state → signal_active。

btc-ml 侧运行。读取：
  - vlm_done_incoming/{pkg}/vlm_response.json  （由 Windows signal_sync 推回）
  - vlm_pending/{pkg}/signal.json              （btc-ml 本地原始包，唯一可信任源）
写入：
  - signal_active/{pkg}/                        （全量通过后，executor 接管）
  - vlm_rejected/{pkg}/                         （拒绝：orphan/dup/bad-schema/stale/no-playbooks）

铁律（G5）：
  - finalizer 只信 btc-ml 本地原始 vlm_pending/{pkg}。
    Windows 回传只接受 vlm_response.json 一个文件，signal.json/state.json 参与决策一概拒绝。
  - 幂等/拒绝决策表（§3.4）：每个分支都归档 + 不静默。

用法：
    python3 -m live.vlm_finalizer              # 常驻轮询
    python3 -m live.vlm_finalizer --once       # 单次（测试用）

环境变量：
    VLM_PENDING         本地 pending（默认 ROOT/vlm_pending）
    VLM_DONE_INCOMING   Windows 推回的 response（默认 ROOT/vlm_done_incoming）
    SIGNAL_ACTIVE       executor 目录（默认 ROOT/signal_active）
    VLM_REJECTED        拒绝归档（默认 ROOT/vlm_rejected）
    PRODUCER_SANDBOX=1  sandbox 模式（Phase 1/2 测试用，拒写生产 signal_active）
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

from live import exec_config as cfg

logger = logging.getLogger("vlm_finalizer")

# ── Paths ─────────────────────────────────────────────────────────────────────

VLM_PENDING        = Path(os.environ.get("VLM_PENDING",        str(ROOT / "vlm_pending")))
VLM_DONE_INCOMING  = Path(os.environ.get("VLM_DONE_INCOMING",  str(ROOT / "vlm_done_incoming")))
VLM_REJECTED       = Path(os.environ.get("VLM_REJECTED",       str(ROOT / "vlm_rejected")))
SIGNAL_ACTIVE      = Path(os.environ.get("SIGNAL_ACTIVE",      str(ROOT / "signal_active")))
SIGNAL_DONE        = Path(os.environ.get("SIGNAL_DONE",        str(ROOT / "signal_done")))

STATUS_FILE        = ROOT / "live" / "heartbeat" / "finalizer_status.json"
HEARTBEAT_FILE     = ROOT / "live" / "heartbeat" / "finalizer_last_run.txt"
LOCK_FILE          = ROOT / "live" / "vlm_finalizer.lock"

PRODUCER_SANDBOX   = os.environ.get("PRODUCER_SANDBOX") == "1"
SIGNAL_MAX_AGE_MIN = 240    # bar_time 超过此分钟数即 stale（来自 exec_config 概念）
POLL_SECONDS       = 30

# ── Sandbox (G2) ──────────────────────────────────────────────────────────────

def _check_sandbox() -> None:
    """Phase 1/2 时 finalizer 必须拒写生产 signal_active，防止误投 executor（G2）。"""
    if PRODUCER_SANDBOX:
        prod_act = (ROOT / "signal_active").resolve()
        if SIGNAL_ACTIVE.resolve() == prod_act:
            logger.error(
                "PRODUCER_SANDBOX=1 but SIGNAL_ACTIVE resolves to production directory: %s. "
                "Override SIGNAL_ACTIVE or unset PRODUCER_SANDBOX.", SIGNAL_ACTIVE.resolve())
            sys.exit(1)


# ── Helpers：归档 / 心跳 / status ────────────────────────────────────────────

def _archive_package(src: Path, dst_dir: Path, reason: str) -> None:
    """原子归档，dest 冲突时加微秒后缀（复用 openclaw 同名函数逻辑）。"""
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


def _update_heartbeat() -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        HEARTBEAT_FILE.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")
    except Exception as e:
        logger.warning("heartbeat write failed (non-fatal): %s", e)


def _write_status(stats: dict) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = dict(stats)
        payload["updated_at"] = pd.Timestamp.now("UTC").isoformat()
        tmp = STATUS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, STATUS_FILE)
    except Exception as e:
        logger.warning("finalizer status write failed (non-fatal): %s", e)


# ── Helpers：playbook filter（复用 live_openclaw 同逻辑，搬至 btc-ml）─────────

def _r_dist_pct(activation_rule: dict) -> Optional[float]:
    act = activation_rule.get("activates_if_close_crosses", {})
    inv = activation_rule.get("invalidation_after_activation", {})
    act_level = act.get("level")
    inv_level = inv.get("level")
    if act_level is None or inv_level is None or act_level == 0:
        return None
    return abs(act_level - inv_level) / act_level * 100


def _tp1_dist_pct(activation_rule: dict) -> Optional[float]:
    act = activation_rule.get("activates_if_close_crosses", {})
    act_level = act.get("level")
    objectives = activation_rule.get("objectives", [])
    tp1_levels = [o.get("level") for o in objectives if o.get("level") is not None]
    if not tp1_levels or act_level is None or act_level == 0:
        return None
    return abs(tp1_levels[0] - act_level) / act_level * 100


def _passes_filters(symbol: str, r_dist: Optional[float], tp1_pct: Optional[float]) -> tuple[bool, str]:
    if r_dist is None or tp1_pct is None:
        return True, ""
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
            logger.info("filter: %s %s excluded — %s", symbol, pb.get("hypothesis", "?"), reason)
            continue
        valid.append(pb)
    return valid


def _build_state(signal: dict, valid_playbooks: list[dict], pkg_name: str) -> dict:
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
            if act_level and inv_level and act_level != 0 else None
        )
        pb_states.append({
            "hypothesis":            pb.get("hypothesis"),
            "direction":             ar.get("direction_if_activated"),
            "status":                "WAITING_FOR_PRIMARY_TOUCH",
            "primary_touch":         ar.get("primary_touch"),
            "activates_if":          ar.get("activates_if_close_crosses"),
            "cancels_if":            ar.get("cancels_if_close_crosses_first"),
            "invalidation":          ar.get("invalidation_after_activation"),
            "tp1_level":             tp_levels[0] if len(tp_levels) > 0 else None,
            "tp2_level":             tp_levels[1] if len(tp_levels) > 1 else None,
            "tp1_dist_pct":          round(tp1_pct, 4) if tp1_pct is not None else None,
            "r_dist_pct":            round(r_dist_pct, 4) if r_dist_pct is not None else None,
            "bars_since_t0":         0,
            "primary_touched_at":    None,
            "activated_at":          None,
            "activation_price":      None,
            "tp1_hit_at":            None,
            "done_at":               None,
            "result":                None,
            "pnl_r":                 None,
        })
    return {
        "signal_dir":     pkg_name,
        "symbol":         signal.get("symbol"),
        "grade":          signal.get("grade"),
        "bar_time":       signal.get("bar_time"),
        "structure_side": signal.get("structure_side"),
        "created_at":     pd.Timestamp.now("UTC").isoformat(),
        "overall_status": "WATCHING",
        "playbooks":      pb_states,
    }


# ── Core：处理单包 ────────────────────────────────────────────────────────────

def process_one(pkg_name: str) -> str:
    """处理 Windows 返回的一个 vlm_response 包。返回状态字串供 stats 统计。

    决策表（§3.4 / G5）：
      local vlm_pending 无 → orphan
      signal_active 已有 → duplicate
      signal_done 已有   → duplicate
      vlm_response schema 不合格 → bad_schema
      过滤后无有效 playbook → filtered
      bar_time 超 TTL     → stale
      全部通过             → moved
    """
    done_pkg = VLM_DONE_INCOMING / pkg_name
    done_resp = done_pkg / "vlm_response.json"
    pending_pkg = VLM_PENDING / pkg_name
    pending_sig = pending_pkg / "signal.json"

    # ── 只提取 vlm_response.json，不信任其它文件（G5）──────────────────────
    if not done_resp.exists() or not done_pkg.exists():
        return "incomplete"     # 不是完整包，跳过

    # 1. local vlm_pending 不存在 → orphan
    if not pending_pkg.exists() or not pending_sig.exists():
        _archive_package(done_pkg, VLM_REJECTED, "orphan_no_local_pending")
        return "orphan"

    # 2. signal_active 或 signal_done 已有 → duplicate
    if (SIGNAL_ACTIVE / pkg_name).exists():
        _archive_package(done_pkg, VLM_REJECTED, "duplicate_already_in_signal_active")
        return "duplicate_active"
    if (SIGNAL_DONE / pkg_name).exists():
        _archive_package(done_pkg, VLM_REJECTED, "duplicate_already_in_signal_done")
        return "duplicate_done"

    # 3. 读 vlm_response.json → schema 校验
    try:
        vlm = json.loads(done_resp.read_text(encoding="utf-8"))
    except Exception as e:
        _archive_package(done_pkg, VLM_REJECTED, f"bad_response_json:{str(e)[:80]}")
        return "bad_json"

    if not vlm.get("watch_summary") or vlm.get("error") or not vlm.get("playbooks"):
        _archive_package(done_pkg, VLM_REJECTED,
                         f"bad_schema:watch_summary={bool(vlm.get('watch_summary'))} "
                         f"playbooks={len(vlm.get('playbooks', []) or [])} error={bool(vlm.get('error'))}")
        return "bad_schema"

    # 4. 读本地 signal.json（唯一信任源，G5）
    try:
        signal = json.loads(pending_sig.read_text(encoding="utf-8"))
    except Exception as e:
        _archive_package(done_pkg, VLM_REJECTED, f"local_signal_json_bad:{str(e)[:80]}")
        return "bad_local_signal"

    # 5. bar_time TTL 检查
    bar_time_str = signal.get("bar_time")
    if bar_time_str:
        try:
            age = (pd.Timestamp.now("UTC") - pd.Timestamp(bar_time_str)).total_seconds() / 60
            if age > SIGNAL_MAX_AGE_MIN:
                _archive_package(done_pkg, VLM_REJECTED,
                                 f"stale_bar_time age={age:.0f}min > {SIGNAL_MAX_AGE_MIN}min")
                return "stale"
        except Exception:
            pass

    # 6. post-VLM 过滤（G5）：playbook 只来自 vlm_response，
    #    但 symbol / structure 等决策数据取自本地 signal.json。
    valid_pbs = _filter_playbooks(signal, vlm)
    if not valid_pbs:
        _archive_package(done_pkg, VLM_REJECTED, "filtered_no_valid_playbooks")
        return "filtered"

    # 7. 全量通过：建 state.json → signal_active/
    state = _build_state(signal, valid_pbs, pkg_name)
    done_state = done_pkg / "state.json"
    done_state.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    SIGNAL_ACTIVE.mkdir(parents=True, exist_ok=True)
    dest = SIGNAL_ACTIVE / pkg_name
    try:
        done_pkg.rename(dest)
        logger.info("moved to signal_active/: %s (%d playbooks)", pkg_name, len(valid_pbs))
        return "moved"
    except Exception as e:
        logger.warning("move to signal_active failed for %s: %s", pkg_name, e)
        return "move_failed"


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")

    _check_sandbox()

    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("vlm_finalizer already running: %s", e)
        sys.exit(1)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true", help="单次处理（测试用）")
    args = ap.parse_args()

    VLM_DONE_INCOMING.mkdir(parents=True, exist_ok=True)

    logger.info("vlm_finalizer started: %s → %s", VLM_DONE_INCOMING, SIGNAL_ACTIVE)
    logger.info("sandbox=%s, signal_active=%s, stages: orphan/dup/stale/schema/filter -> signal_active",
                "ON" if PRODUCER_SANDBOX else "OFF", SIGNAL_ACTIVE)

    stats = {
        "processed": 0, "moved": 0, "orphan": 0, "duplicate": 0,
        "stale": 0, "bad_schema": 0, "filtered": 0, "move_failed": 0,
        "last_success_at": None, "last_error": None,
    }

    while True:
        try:
            pkgs = sorted([
                d.name for d in VLM_DONE_INCOMING.iterdir()
                if d.is_dir() and (d / "vlm_response.json").exists()
            ])
            for pkg_name in pkgs:
                result = process_one(pkg_name)
                stats["processed"] += 1
                if result == "moved":
                    stats["moved"] += 1
                    stats["last_success_at"] = pd.Timestamp.now("UTC").isoformat()
                elif result == "orphan":
                    stats["orphan"] += 1
                    stats["last_error"] = f"orphan: {pkg_name}"
                elif result.startswith("duplicate"):
                    stats["duplicate"] += 1
                elif result == "stale":
                    stats["stale"] += 1
                elif result in ("bad_json", "bad_schema", "bad_local_signal"):
                    stats["bad_schema"] += 1
                    stats["last_error"] = f"{result}: {pkg_name}"
                elif result == "filtered":
                    stats["filtered"] += 1
                elif result == "move_failed":
                    stats["move_failed"] += 1
                    stats["last_error"] = f"move_failed: {pkg_name}"
                # incomplete → silently skip (not yet fully transferred)

            _update_heartbeat()
            _write_status(stats)

        except Exception as e:
            logger.exception("finalizer cycle error")
            stats["last_error"] = str(e)[:200]
            _update_heartbeat()
            _write_status(stats)

        if args.once:
            break
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
