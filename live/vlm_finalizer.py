#!/usr/bin/env python3
"""
vlm_finalizer（re-arch Phase 2, §REARCH §10 G5）— 接收 VLM response，过滤 + 建 state → signal_active。

btc-ml 侧运行。读取：
  - vlm_done_incoming/{pkg}/vlm_response.json  （由 Windows signal_sync 推回）
  - vlm_pending/{pkg}/signal.json              （btc-ml 本地原始包，唯一可信任源）
写入：
  - signal_active/{pkg}/                        （全量通过后，finalizer 本地构建，含 .ready + state.json）
  - vlm_rejected/{pkg}/                         （拒绝：orphan/dup/bad-schema/stale/no-playbooks）
  - vlm_pending_done/{pkg}/                     （成功/拒绝后原始 pending 移出）

铁律（G5）：
  - finalizer 只信 btc-ml 本地原始 vlm_pending/{pkg}。
    Windows 回传只接受 vlm_response.json 一个文件，signal.json/state.json 一概忽略。
  - signal_active 只包含 finalizer 本地生成的 state.json + .ready。
    不把 Windows 回传目录整体带入执行路径。
  - 幂等/拒绝决策表（§3.4）：每个分支都归档 + 不静默。

用法：
    python3 -m live.vlm_finalizer              # 常驻轮询
    python3 -m live.vlm_finalizer --once       # 单次（测试用）

环境变量：
    VLM_PENDING         本地 pending（默认 ROOT/vlm_pending）
    VLM_DONE_INCOMING   Windows 推回的 response（默认 ROOT/vlm_done_incoming）
    SIGNAL_ACTIVE       executor 目录（默认 ROOT/signal_active）
    VLM_REJECTED        拒绝归档（默认 ROOT/vlm_rejected）
    VLM_PENDING_DONE    已处理 pending 归档（默认 ROOT/vlm_pending_done）
    PRODUCER_SANDBOX=1  sandbox 模式，拒写所有生产路径
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import exec_config as cfg

logger = logging.getLogger("vlm_finalizer")

# ── Paths ─────────────────────────────────────────────────────────────────────

VLM_PENDING        = Path(os.environ.get("VLM_PENDING",        str(ROOT / "vlm_pending")))
VLM_DONE_INCOMING  = Path(os.environ.get("VLM_DONE_INCOMING",  str(ROOT / "vlm_done_incoming")))
VLM_REJECTED       = Path(os.environ.get("VLM_REJECTED",       str(ROOT / "vlm_rejected")))
VLM_PENDING_DONE   = Path(os.environ.get("VLM_PENDING_DONE",   str(ROOT / "vlm_pending_done")))
SIGNAL_ACTIVE      = Path(os.environ.get("SIGNAL_ACTIVE",      str(ROOT / "signal_active")))
SIGNAL_DONE        = Path(os.environ.get("SIGNAL_DONE",        str(ROOT / "signal_done")))

# Staging: locally-built package before atomic move into SIGNAL_ACTIVE.
# Keep it outside signal_active to avoid executor seeing half-written packages.
_STAGING_DIR       = SIGNAL_ACTIVE.parent / ".f_active_incoming"

STATUS_FILE        = ROOT / "live" / "heartbeat" / "finalizer_status.json"
HEARTBEAT_FILE     = ROOT / "live" / "heartbeat" / "finalizer_last_run.txt"
LOCK_FILE          = ROOT / "live" / "vlm_finalizer.lock"

PRODUCER_SANDBOX   = os.environ.get("PRODUCER_SANDBOX") == "1"
SIGNAL_MAX_AGE_MIN = 240    # bar_time 超过此分钟数即 stale
POLL_SECONDS       = 30


# ── Sandbox (G2/E) ───────────────────────────────────────────────────────────

# Known production defaults: path name → directory name under ROOT.
# Used by _check_sandbox to compare against (not path.name — that would match any
# custom dir with the same leaf name).
def _check_sandbox() -> None:
    """PRODUCER_SANDBOX=1 时拒绝所有写路径解析到生产默认目录。检查必须在 mkdir 前。"""
    if not PRODUCER_SANDBOX:
        return
    # Compare env-configurable paths against their ROOT-based defaults.
    checks = [
        ("SIGNAL_ACTIVE", SIGNAL_ACTIVE, ROOT / "signal_active"),
        ("VLM_DONE_INCOMING", VLM_DONE_INCOMING, ROOT / "vlm_done_incoming"),
        ("VLM_REJECTED", VLM_REJECTED, ROOT / "vlm_rejected"),
        ("VLM_PENDING", VLM_PENDING, ROOT / "vlm_pending"),
        ("VLM_PENDING_DONE", VLM_PENDING_DONE, ROOT / "vlm_pending_done"),
    ]
    failed = []
    for name, actual, default in checks:
        if actual.resolve() == default.resolve():
            failed.append(f"{name}={actual} == production {default}")
    if failed:
        logger.error("PRODUCER_SANDBOX=1 but paths resolve to production: %s. "
                     "Override via env or unset PRODUCER_SANDBOX.", "; ".join(failed))
        sys.exit(1)


# ── Helpers：归档 / 心跳 / status ────────────────────────────────────────────

def _archive_package(src: Path, dst_dir: Path, reason: str) -> bool:
    """原子归档，dest 冲突时加 __dup_<ts> 后缀。源必离开。"""
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
        return True
    except Exception as e:
        logger.warning("archive failed %s: %s", src.name, e)
        return False


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


def _archive_both(incoming: Path, pending: "Optional[Path]",
                  dst_dir: Path, reason: str) -> None:
    """归档 incoming，如果 pending 存在也一并移出活跃队列（D）。"""
    _archive_package(incoming, dst_dir, reason)
    if pending is not None and pending.exists():
        _archive_package(pending, dst_dir, f"{reason}__pending")


# ── Schema validation (C) ─────────────────────────────────────────────────────

def _validate_vlm_response(raw: Any) -> Optional[str]:
    """Return error string, or None if valid."""
    if not isinstance(raw, dict):
        return "top_level_not_dict"
    ws = raw.get("watch_summary")
    if not (isinstance(ws, str) and ws.strip()) and not (isinstance(ws, dict) and ws.get("one_sentence_read")):
        return "watch_summary_missing_or_empty"
    if raw.get("error"):
        return f"error_flag_present: {str(raw['error'])[:80]}"
    pbs = raw.get("playbooks")
    if not isinstance(pbs, list) or len(pbs) == 0:
        return "playbooks_not_nonempty_list"
    for i, pb in enumerate(pbs):
        if not isinstance(pb, dict):
            return f"playbook[{i}]_not_dict"
        plan = pb.get("conditional_trade_plan")
        if not isinstance(plan, dict):
            return f"playbook[{i}]_conditional_trade_plan_not_dict"
        ar = plan.get("activation_rule")
        if not isinstance(ar, dict):
            return f"playbook[{i}]_activation_rule_not_dict"
    return None


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
    # 口径对齐回测 scoring/aonly_filter_analysis.py:passes_filter：
    #   - r_dist 算不出（act/inv level 缺）→ 拒绝（回测剔除，不再「None 即放行」）
    #   - tp1_pct 缺只跳过 tp1 子句，r_dist 阈值照常判（不再整体放行）
    #   - ETH 死区右开 [1.0, 2.0)，与回测一致（原 live 闭区间多排除了 2.0 这一点）
    #   注：b2act >= 2 不在此层——finalizer 无 bar 流、算不出 b2act，由执行期
    #   状态机 step_waiting 兜底（playbook_fsm），与回测 filter 逻辑等价。
    if r_dist is None:
        return False, "r_dist unavailable (act/inv level missing)"
    if symbol == "BTC/USDT":
        if r_dist < 0.5:
            return False, f"r_dist {r_dist:.3f}% < 0.5%"
        if tp1_pct is not None and tp1_pct >= 1.5:
            return False, f"tp1 {tp1_pct:.3f}% >= 1.5%"
    elif symbol == "ETH/USDT":
        if r_dist < 1.5:
            return False, f"r_dist {r_dist:.3f}% < 1.5%"
        if tp1_pct is not None and 1.0 <= tp1_pct < 2.0:
            return False, f"tp1 {tp1_pct:.3f}% in dead zone [1,2)%"
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
        r_val = _r_dist_pct(ar)
        t_val = _tp1_dist_pct(ar)
        p, reason = _passes_filters(symbol, r_val, t_val)
        if not p:
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

    决策表（§3.4 / G5 / A–E）：
      local vlm_pending 不存在或缺 .ready → orphan/bad_pending
      signal_active 已有                  → duplicate
      signal_done 已有                    → duplicate
      vlm_response schema 不合格           → bad_schema（严格校验，不抛异常）
      过滤后无有效 playbook               → filtered
      bar_time 超 TTL                     → stale
      全部通过 → 本地构建 state.json + .ready → staging → atomic rename → signal_active
    """
    done_pkg = VLM_DONE_INCOMING / pkg_name
    done_resp = done_pkg / "vlm_response.json"
    pending_pkg = VLM_PENDING / pkg_name
    pending_sig = pending_pkg / "signal.json"
    pending_ready = pending_pkg / ".ready"

    if not done_resp.exists() or not done_pkg.exists():
        return "incomplete"

    # ── 1. local vlm_pending 必须存在且含 .ready / signal.json ──────────────
    if not pending_pkg.exists() or not pending_ready.exists():
        _archive_package(done_pkg, VLM_REJECTED, "orphan_no_local_ready")
        return "orphan"
    if not pending_sig.exists():
        _archive_package(done_pkg, VLM_REJECTED, "bad_pending_missing_signal_json")
        return "bad_pending"

    # ── 2. signal_active / signal_done 已有 → duplicate（D）─────────────────
    if (SIGNAL_ACTIVE / pkg_name).exists():
        _archive_both(done_pkg, pending_pkg, VLM_REJECTED, "duplicate_active")
        return "duplicate_active"
    if (SIGNAL_DONE / pkg_name).exists():
        _archive_both(done_pkg, pending_pkg, VLM_REJECTED, "duplicate_done")
        return "duplicate_done"

    # ── 3. 读 vlm_response.json（B: 只读 vlm_response，忽略其它文件）───────
    try:
        raw = json.loads(done_resp.read_text(encoding="utf-8"))
    except Exception as e:
        _archive_both(done_pkg, pending_pkg, VLM_REJECTED, f"bad_response_json:{str(e)[:80]}")
        return "bad_json"

    schema_err = _validate_vlm_response(raw)
    if schema_err is not None:
        _archive_both(done_pkg, pending_pkg, VLM_REJECTED, f"bad_schema:{schema_err}")
        return "bad_schema"

    vlm: dict = raw  # validated

    # ── 4. 读本地 signal.json（G5: 唯一信任源）─────────────────────────────
    try:
        signal = json.loads(pending_sig.read_text(encoding="utf-8"))
    except Exception as e:
        _archive_both(done_pkg, pending_pkg, VLM_REJECTED, f"bad_local_signal:{str(e)[:80]}")
        return "bad_local_signal"

    # ── 5. bar_time TTL 检查 ────────────────────────────────────────────────
    bar_time_str = signal.get("bar_time")
    if bar_time_str:
        try:
            age = (pd.Timestamp.now("UTC") - pd.Timestamp(bar_time_str)).total_seconds() / 60
            if age > SIGNAL_MAX_AGE_MIN:
                _archive_both(done_pkg, pending_pkg, VLM_REJECTED,
                              f"stale_bar_time age={age:.0f}min > {SIGNAL_MAX_AGE_MIN}min")
                return "stale"
        except Exception:
            pass

    # ── 6. post-VLM 过滤 ────────────────────────────────────────────────────
    valid_pbs = _filter_playbooks(signal, vlm)
    if not valid_pbs:
        _archive_both(done_pkg, pending_pkg, VLM_REJECTED, "filtered_no_valid_playbooks")
        return "filtered"

    # ── 7. 全量通过：btc-ml 本地构建信号包（A）─────────────────────────────
    #     7a. 在 staging 目录构建（安全隔离，executor 看不到半写包）
    state = _build_state(signal, valid_pbs, pkg_name)
    staging_root = _STAGING_DIR / pkg_name
    # P3: clean any stale staging residue from a prior crash so no leftover files
    # (evil.txt from a bad fetch, half-written json) leak into signal_active.
    if staging_root.exists():
        import shutil
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)

    staging_state = staging_root / "state.json"
    staging_state.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    (staging_root / ".ready").touch()

    # Write a copy of vlm_response.json for audit (optional, not used by executor)
    (staging_root / "vlm_response.json").write_text(
        json.dumps(vlm, ensure_ascii=False, indent=2), encoding="utf-8")

    #     7b. 原子 rename → signal_active/{pkg}
    SIGNAL_ACTIVE.mkdir(parents=True, exist_ok=True)
    dest = SIGNAL_ACTIVE / pkg_name
    try:
        staging_root.rename(dest)
    except Exception as e:
        logger.warning("move to signal_active failed for %s: %s", pkg_name, e)
        _archive_package(staging_root, VLM_REJECTED, f"move_failed:{str(e)[:80]}")
        return "move_failed"

    #     7c. 归档 incoming + pending（D: 移出活跃队列）
    _archive_package(done_pkg, VLM_PENDING_DONE, "moved_incoming")
    _archive_package(pending_pkg, VLM_PENDING_DONE, "moved_pending")

    logger.info("signal_active/%s .ready + state.json — %d playbooks", pkg_name, len(valid_pbs))
    return "moved"


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
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("vlm_finalizer started: %s → %s", VLM_DONE_INCOMING, SIGNAL_ACTIVE)
    logger.info("sandbox=%s, signal_active=%s, staging=%s",
                "ON" if PRODUCER_SANDBOX else "OFF", SIGNAL_ACTIVE, _STAGING_DIR)
    logger.info("paths: pending=%s done_incoming=%s pending_done=%s rejected=%s",
                VLM_PENDING, VLM_DONE_INCOMING, VLM_PENDING_DONE, VLM_REJECTED)

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
                elif result in ("orphan", "bad_pending"):
                    stats["orphan"] += 1
                    stats["last_error"] = f"{result}: {pkg_name}"
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
