#!/usr/bin/env python3
"""Watchdog（#13）— 各机守本机心跳，executor 卡死 → 让 systemd 重启 + 飞书。

职责分离（btc-ml）：
  - systemd（user service `coin-executor`）管「进程在」：Restart=always + StartLimitBurst=3。
  - watchdog 管「心跳活」：executor 5min 无心跳=卡死（进程活着但循环僵死，systemd 测不到）
    → `systemctl --user kill -s KILL coin-executor` → systemd 自动拉起（计入 StartLimit）。
    15min 内超 3 次 → systemd 停拉起 → watchdog 持续 stale → 持续飞书（等人工，§22 line 903）。

运行：
  btc-ml:  python3 -m live.watchdog --role btcml --loop    # 守 executor（卡死 systemctl kill）
  中国:    python3 live/watchdog.py   --role china --loop   # 守 monitor/openclaw/signal_pusher（告警）
  单次：   去掉 --loop（健康=exit0 / 有告警=exit1），可挂 cron / Task Scheduler。
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("watchdog")

HEARTBEAT_DIR = ROOT / "live" / "heartbeat"
ALERT_LOG     = HEARTBEAT_DIR / "alerts.log"
LOOP_SECONDS  = 300


@dataclass(frozen=True)
class ProcSpec:
    name: str
    heartbeat: Path
    stale_min: float
    unit: str | None        # systemd --user unit（btcml executor）；None = 仅告警，不自动重启


@dataclass(frozen=True)
class StatusSpec:
    """Check functional-health status JSON (monitor/openclaw/pusher write these every cycle).

    `thresholds` maps a status field → alert-if-value-is >= threshold. This is generic
    so each producer reports its own health metrics (fetch failures, write failures,
    consecutive rejections, parse errors, backlog) without a bespoke check per process.
    """
    name: str
    status_file: Path
    thresholds: tuple[tuple[str, int], ...]
    max_stale_min: float | None = None   # alert if updated_at older than this (status writer stalled)


def _hb(name: str) -> Path:
    return HEARTBEAT_DIR / f"{name}_last_run.txt"


# 各机守本机进程（re-arch Phase 3: Windows = signal_sync + vlm_worker）。
# executor 走 systemd 自动重启，其余告警（Windows 侧人工/Task Scheduler）。
SPECS: dict[str, list[ProcSpec]] = {
    "btcml": [
        ProcSpec("executor",      _hb("executor"),      5.0, "coin-executor"),
        ProcSpec("monitor",       _hb("monitor"),       20.0, None),
        ProcSpec("vlm_finalizer", _hb("vlm_finalizer"), 20.0, None),
    ],
    "china": [
        ProcSpec("signal_sync", _hb("signal_sync"), 10.0, None),
        ProcSpec("vlm_worker",  _hb("vlm_worker"),  20.0, None),
    ],
}

# Functional-health status specs (re-arch Phase 3). Watchdog alerts on stale
# status or breached thresholds: SSH failures, backlog, parse errors, rejects.
STATUS_SPECS: dict[str, list[StatusSpec]] = {
    "btcml": [
        StatusSpec("monitor",       HEARTBEAT_DIR / "monitor_status.json",
                   (("consecutive_failures", 3), ("backlog_count", 10),
                    ("package_write_failures", 3)), max_stale_min=5.0),
        StatusSpec("vlm_finalizer", HEARTBEAT_DIR / "finalizer_status.json",
                   (("bad_schema", 5), ("filtered", 10), ("move_failed", 3)),
                   max_stale_min=5.0),
    ],
    "china": [
        StatusSpec("signal_sync", HEARTBEAT_DIR / "signal_sync_status.json",
                   (("consecutive_failures", 3), ("backlog_count", 5)),
                   max_stale_min=5.0),
        StatusSpec("vlm_worker", HEARTBEAT_DIR / "vlm_worker_status.json",
                   (("consecutive_failures", 3), ("parse_err", 5),
                    ("stale", 5)), max_stale_min=25.0),
    ],
}

# Fields whose alert line should append last_error for context.
_ERR_CONTEXT_FIELDS = {"consecutive_failures", "consecutive_rejections",
                       "move_failures", "package_write_failures"}


def check_status(spec: StatusSpec) -> str | None:
    """Read functional-health status JSON. Returns alert string or None."""
    if not spec.status_file.exists():
        return None  # process hasn't written a full cycle yet — don't alarm
    try:
        data = json.loads(spec.status_file.read_text(encoding="utf-8"))
    except Exception as e:
        return f"{spec.name}: status file unreadable ({e})"
    last_err = str(data.get("last_error") or "")
    alerts = []

    # Stale status = the writer stopped updating (e.g. status write failing while the
    # heartbeat still ticks). Without this, the watchdog would keep trusting an old
    # "healthy" snapshot. updated_at missing/unparseable also counts as stale.
    if spec.max_stale_min is not None:
        updated = data.get("updated_at")
        # to_datetime(..., errors="coerce") → NaT for None / bad strings (pd.Timestamp(None)
        # silently returns NaT and would slip past, hence the explicit isna check).
        ts = pd.to_datetime(updated, utc=True, errors="coerce")
        if pd.isna(ts):
            alerts.append(f"status updated_at missing/unparseable ({updated!r})")
        else:
            age_min = (pd.Timestamp.now("UTC") - ts).total_seconds() / 60
            if age_min > spec.max_stale_min:
                alerts.append(f"status stale {age_min:.1f}min (threshold {spec.max_stale_min})")

    for field, threshold in spec.thresholds:
        try:
            val = int(data.get(field, 0) or 0)
        except (TypeError, ValueError):
            continue
        if val >= threshold:
            if field in _ERR_CONTEXT_FIELDS and last_err:
                alerts.append(f"{field}={val}: {last_err[:80]}")
            else:
                alerts.append(f"{field}={val}")
    return f"{spec.name}: {'; '.join(alerts)}" if alerts else None


def check_heartbeat(spec: ProcSpec) -> str | None:
    """stale/缺失 → 告警字符串；健康 → None。"""
    if not spec.heartbeat.exists():
        return f"{spec.name}: heartbeat file not found ({spec.heartbeat})"
    try:
        last = pd.Timestamp(spec.heartbeat.read_text().strip(), tz="UTC")
    except Exception as e:
        return f"{spec.name}: cannot parse heartbeat ({e})"
    age = (pd.Timestamp.now("UTC") - last).total_seconds() / 60
    if age > spec.stale_min:
        return f"{spec.name}: last heartbeat {age:.1f} min ago (threshold {spec.stale_min} min) — 卡死/死亡"
    return None


def send_alert(message: str) -> None:
    """log + alerts.log + 飞书 + （Windows）系统通知。"""
    logger.error("ALERT: %s", message)
    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.now("UTC").isoformat()   # hoisted: nested " in an f-string is a SyntaxError on Python 3.11
    with open(ALERT_LOG, "a", encoding="utf-8") as f:
        f.write(f"{ts} ALERT: {message}\n")
    try:
        from live import notify
        notify.feishu_alert(f"[watchdog] {message}")
    except Exception as e:
        logger.warning("feishu alert failed: %s", e)
    try:
        from plyer import notification
        notification.notify(title="coin_trader ALERT", message=message, timeout=10)
    except Exception:
        pass


def restart_via_systemd(spec: ProcSpec) -> None:
    """卡死 → systemctl --user kill -s KILL，让 systemd Restart 拉起（计入 StartLimitBurst=3）。"""
    unit = f"{spec.unit}.service"
    try:
        r = subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", unit],
                           capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            logger.warning("%s 卡死 → systemctl --user kill %s（systemd 将自动拉起）", spec.name, unit)
        else:
            send_alert(f"{spec.name}: systemctl kill {unit} 失败 rc={r.returncode}: {r.stderr.strip()[:120]}")
    except Exception as e:
        send_alert(f"{spec.name}: systemctl kill {unit} 异常: {e}")


def check_data_source_health() -> str | None:
    """Check per-symbol staleness from monitor_status.json. Alerts if any symbol's
    latest 15m bar is behind by more than STALE_DATA_MINUTES — the DB collection
    may have stalled even while the monitor process is alive."""
    mon_status = HEARTBEAT_DIR / "monitor_status.json"
    if not mon_status.exists():
        return None
    try:
        data = json.loads(mon_status.read_text(encoding="utf-8"))
    except Exception as e:
        return f"data_source: monitor_status unreadable ({e})"
    per = data.get("per_symbol", {})
    alerts = []
    for sym, info in per.items():
        sm = info.get("staleness_min")
        if sm is not None and isinstance(sm, (int, float)) and sm > cfg.STALE_DATA_MINUTES:
            cursor = info.get("cursor", "?")
            alerts.append(f"{sym} stale {sm:.0f}min (cursor={cursor}, threshold={cfg.STALE_DATA_MINUTES}min)")
    return f"data_source: {'; '.join(alerts)}" if alerts else None


def run_once(role: str) -> bool:
    all_ok = True
    for spec in SPECS[role]:
        alert = check_heartbeat(spec)
        if alert:
            send_alert(alert)
            if spec.unit:                       # 有 systemd unit → 卡死强制重启（systemd StartLimit 限次）
                restart_via_systemd(spec)
            all_ok = False
        else:
            logger.info("%s heartbeat: OK", spec.name)
    for spec in STATUS_SPECS.get(role, []):
        alert = check_status(spec)
        if alert:
            send_alert(alert)
            all_ok = False
        else:
            logger.info("%s status: OK", spec.name)
    if role == "btcml":
        alert = check_data_source_health()
        if alert:
            send_alert(alert)
            all_ok = False
        else:
            logger.info("data_source health: OK")
    return all_ok


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--role", choices=list(SPECS), required=True, help="本机角色（守哪些进程）")
    ap.add_argument("--loop", action="store_true", help=f"常驻每 {LOOP_SECONDS//60} 分钟检查（默认单次）")
    args = ap.parse_args()
    if args.loop:
        logger.info("watchdog loop [%s] started (interval %ds)", args.role, LOOP_SECONDS)
        while True:
            try:
                run_once(args.role)
            except Exception:
                logger.exception("watchdog round error")
            time.sleep(LOOP_SECONDS)
    else:
        sys.exit(0 if run_once(args.role) else 1)


if __name__ == "__main__":
    main()
