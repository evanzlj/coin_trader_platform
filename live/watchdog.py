#!/usr/bin/env python3
"""
Watchdog — checks heartbeat files for monitor.py and executor.py.

Run independently (e.g. via Windows Task Scheduler every 5 minutes):
    python3 live/watchdog.py

If any process heartbeat is older than STALE_MINUTES, writes an alert
to live/heartbeat/alerts.log (and optionally sends a system notification).
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HEARTBEAT_DIR  = ROOT / "live" / "heartbeat"
ALERT_LOG      = HEARTBEAT_DIR / "alerts.log"
STALE_MINUTES  = 20

PROCESSES = {
    "monitor":  HEARTBEAT_DIR / "monitor_last_run.txt",
    "openclaw": HEARTBEAT_DIR / "openclaw_last_run.txt",
    "executor": HEARTBEAT_DIR / "executor_last_run.txt",
}


def check_heartbeat(name: str, path: Path) -> str | None:
    """
    Returns an alert string if the heartbeat is stale or missing, else None.
    """
    if not path.exists():
        return f"{name}: heartbeat file not found ({path})"

    try:
        raw = path.read_text().strip()
        last_run = pd.Timestamp(raw, tz="UTC")
    except Exception as e:
        return f"{name}: cannot parse heartbeat ({e})"

    age_minutes = (pd.Timestamp.utcnow() - last_run).total_seconds() / 60
    if age_minutes > STALE_MINUTES:
        return (
            f"{name}: last heartbeat {age_minutes:.1f} min ago "
            f"(threshold {STALE_MINUTES} min) — process may be dead"
        )
    return None


def send_alert(message: str) -> None:
    """Log alert + write to alerts.log + optionally system notification."""
    logger.error("ALERT: %s", message)

    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp.utcnow().isoformat()
    with open(ALERT_LOG, "a") as f:
        f.write(f"{ts} ALERT: {message}\n")

    # Windows system tray notification (requires win10toast or plyer)
    try:
        from plyer import notification
        notification.notify(
            title="coin_trader_platform ALERT",
            message=message,
            timeout=10,
        )
    except ImportError:
        pass  # plyer not installed — log-only is fine


def run_once() -> bool:
    """Check all processes. Returns True if all healthy."""
    all_ok = True
    for name, path in PROCESSES.items():
        alert = check_heartbeat(name, path)
        if alert:
            send_alert(alert)
            all_ok = False
        else:
            logger.info("%s: OK", name)
    return all_ok


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--loop", action="store_true",
        help="Run in a loop every 5 minutes (default: run once and exit)",
    )
    args = parser.parse_args()

    if args.loop:
        logger.info("watchdog loop started (interval: 5 min, stale threshold: %d min)",
                    STALE_MINUTES)
        while True:
            run_once()
            time.sleep(300)
    else:
        ok = run_once()
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
