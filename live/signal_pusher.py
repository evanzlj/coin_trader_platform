"""signal_pusher（#11 跨机 state.json 同步，中国 → btc-ml）。

中国侧常驻轮询本地 `signal_active/`，把 openclaw 产出的新信号包单向推到 btc-ml
（新加坡有公网 IP，中国 push 最简单；只传几 KB json，图不传）。executor 在 btc-ml
本地维护 state.json（唯一 writer），pusher 只投递**初始包**、绝不覆盖 executor 已接管的。

完整性（取代同机原子 rename，§20.2）：
  Python tarfile 打包 `.ready` + 必要 json（state.json/signal.json/vlm_response.json，
  跨平台、Windows 无需 rsync）→ ssh stdin → btc-ml 解到 `.signal_incoming/{pkg}`
  → btc-ml 先校验 `.ready` + `state.json` 存在且 JSON 可读（BAD_PACKAGE=不落 pushed）
  → `mv -T`（no-clobber）到 `signal_active/{pkg}`。
executor `load_states` 只认带 .ready 的完整包，mv 完成前看不到 → 绝不读半包。

去重 / 幂等：
  - 本地 `.signal_pushed/{pkg}` 标记，推成功后落，常驻/重启不重推。
  - btc-ml 已有 signal_active/{pkg} 或 signal_done/{pkg}（executor 接管/归档过）→ 远端 SKIP_EXISTS，
    不覆盖；本地照样落 .pushed（视为已处理）。
  - 断网/ssh 失败 → 不落 .pushed，下轮自然重传（中国侧积压）。

运行：
    python3 -m live.signal_pusher              # 常驻轮询
    python3 -m live.signal_pusher --once       # 推一轮即退出（测试用）
    SIGNAL_ACTIVE=/tmp/fake python3 -m live.signal_pusher --once   # Mac 模拟中国侧
"""
from __future__ import annotations

import argparse
import datetime
import io
import json
import logging
import os
import re
import subprocess
import tarfile
import time
from pathlib import Path

from live import exec_config as cfg

logger = logging.getLogger("signal_pusher")

REMOTE_HOST  = "evan@btc-ml"
REMOTE_ROOT  = "/home/evan/repo/coin_trader_platform"
PUSHED_DIR   = cfg.ROOT / ".signal_pushed"          # 本地去重标记
HEARTBEAT    = cfg.ROOT / "live" / "heartbeat" / "signal_pusher_last_run.txt"   # watchdog 监控（#13）
STATUS_FILE  = cfg.ROOT / "live" / "heartbeat" / "pusher_status.json"
LOCK_FILE    = cfg.ROOT / "live" / "pusher.lock"
POLL_SECONDS = 10
SSH_TIMEOUT  = 60

_PKG_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")          # 包名白名单（防 shell 注入）


def _ready_pkgs() -> list[Path]:
    """本地 signal_active 内带 .ready 的包目录。"""
    base = cfg.SIGNAL_ACTIVE
    if not base.exists():
        return []
    return [d for d in sorted(base.iterdir())
            if d.is_dir() and (d / ".ready").exists() and _PKG_RE.match(d.name)]


def _already_pushed(pkg_name: str) -> bool:
    return (PUSHED_DIR / pkg_name).exists()


def _mark_pushed(pkg_name: str) -> None:
    PUSHED_DIR.mkdir(parents=True, exist_ok=True)
    (PUSHED_DIR / pkg_name).touch()


_PACK_FILES = {".ready", "state.json", "signal.json", "vlm_response.json"}  # 只传必要 json，不传 PNG（缩小带宽/超时窗口）


def _tar_bytes(pkg_dir: Path) -> bytes:
    """只打包必要的元文件（.ready + 几个 json），不传无关大文件（PNG 等）；arcname 带包名前缀。"""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for fn in _PACK_FILES:
            fp = pkg_dir / fn
            if fp.exists():
                tar.add(fp, arcname=f"{pkg_dir.name}/{fn}")
    return buf.getvalue()


def _remote_script(pkg_name: str) -> str:
    """btc-ml 侧：解到 .signal_incoming → 校验包完整性 → mv -T no-clobber 到 signal_active。
    远端失败输出 BAD_PACKAGE（不落 pushed，下轮重试），竞态/已存在输出 SKIP_EXISTS。"""
    return (
        f"set -e; cd {REMOTE_ROOT}; "
        f"mkdir -p .signal_incoming signal_active; "
        f"rm -rf .signal_incoming/{pkg_name}; "
        f"tar -xzf - -C .signal_incoming; "
        f"# 远端校验：.ready + state.json 存在且 JSON 可读（防源端半坏包，§11 P1）\n"
        f"p=.signal_incoming/{pkg_name}; "
        f"if [ ! -f \"$p/.ready\" ] || [ ! -f \"$p/state.json\" ]; then "
        f"  rm -rf \"$p\"; echo BAD_PACKAGE; exit 0; fi; "
        f"python3 -c \"import json; json.loads(open('$p/state.json').read())\" 2>/dev/null || "
        f"  {{ rm -rf \"$p\"; echo BAD_PACKAGE; exit 0; }}; "
        f"if [ -e signal_active/{pkg_name} ] || [ -e signal_done/{pkg_name} ]; then "
        f"  rm -rf \"$p\"; echo SKIP_EXISTS; "
        f"else mv -T \"$p\" signal_active/{pkg_name}; echo MOVED; fi"
    )


def push_one(pkg_dir: Path) -> bool:
    """推一个包到 btc-ml。返回是否已处理（MOVED/SKIP_EXISTS 都算，可落 .pushed）。"""
    pkg = pkg_dir.name
    data = _tar_bytes(pkg_dir)
    try:
        r = subprocess.run(
            ["ssh", REMOTE_HOST, _remote_script(pkg)],
            input=data, capture_output=True, timeout=SSH_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        logger.warning("push %s timeout → will retry next round", pkg)
        return False
    out = r.stdout.decode(errors="replace").strip()
    err = r.stderr.decode(errors="replace").strip()
    if r.returncode != 0:
        logger.warning("push %s ssh failed rc=%s: %s", pkg, r.returncode, err[:200])
        return False
    if out.endswith("MOVED"):
        logger.info("pushed → btc-ml signal_active/%s", pkg)
        return True
    if out.endswith("SKIP_EXISTS"):
        logger.info("skip %s — already on btc-ml (executor took over)", pkg)
        return True
    if out.endswith("BAD_PACKAGE"):
        logger.warning("push %s → BAD_PACKAGE (remote rejected: missing/broken .ready/state.json), "
                       "will retry next round", pkg)
        return False
    logger.warning("push %s unexpected remote output: %r / %r", pkg, out, err[:200])
    return False


def _heartbeat() -> None:
    HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT.write_text(datetime.datetime.now(datetime.timezone.utc).isoformat(),
                         encoding="utf-8")


def _pending_count() -> int:
    """Count of local signal_active packages not yet pushed."""
    return sum(1 for p in _ready_pkgs() if not _already_pushed(p.name))


def _write_pusher_status(consecutive_failures: int, last_success_at: "str | None",
                          last_error: "str | None", backlog_count: int) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "last_success_at":      last_success_at,
        "consecutive_failures": consecutive_failures,
        "last_error":           last_error,
        "backlog_count":        backlog_count,
        "updated_at":           datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_FILE)


def push_round() -> tuple[int, int]:
    """扫一轮，推所有未推送的 ready 包。返回 (pushed, failed)。"""
    pushed = failed = 0
    for pkg_dir in _ready_pkgs():
        if _already_pushed(pkg_dir.name):
            continue
        if push_one(pkg_dir):
            _mark_pushed(pkg_dir.name)
            pushed += 1
        else:
            failed += 1
    return pushed, failed


def main() -> None:
    import sys
    from live.single_instance import SingleInstance, AlreadyRunning
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("signal_pusher already running: %s", e)
        sys.exit(1)

    ap = argparse.ArgumentParser(description="signal_pusher: 中国 signal_active → btc-ml（#11）")
    ap.add_argument("--once", action="store_true", help="推一轮即退出（测试用）")
    args = ap.parse_args()
    logger.info("signal_pusher start: local=%s → %s:%s/signal_active",
                cfg.SIGNAL_ACTIVE, REMOTE_HOST, REMOTE_ROOT)

    consecutive_failures = 0
    last_success_at: "str | None" = None
    last_error: "str | None" = None

    while True:
        round_failed = False
        try:
            backlog = _pending_count()
            p, f = push_round()
            if p or f:
                logger.info("round done: pushed=%d failed=%d", p, f)

            if f > 0:
                # Any failure this round is unhealthy — even if some packages went
                # through (partial failure). A partial push means a real signal is
                # still stuck on the China side (F7).
                consecutive_failures += 1
                round_failed = True
                if p > 0:
                    last_success_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    last_error = f"partial failure: pushed={p} failed={f} backlog={backlog}"
                else:
                    last_error = f"pushed=0 failed={f} backlog={backlog}"
            elif p > 0:
                # All attempted pushes succeeded.
                consecutive_failures = 0
                last_error = None
                last_success_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            else:
                # Nothing to push (backlog == 0) — healthy idle.
                consecutive_failures = 0
                last_error = None

            _heartbeat()
            _write_pusher_status(consecutive_failures, last_success_at, last_error, backlog)

        except Exception as e:
            # Unexpected failure (e.g. ssh subprocess blew up): still record functional
            # health so the watchdog sees it instead of a stale "last success" snapshot.
            logger.exception("push_round error")
            consecutive_failures += 1
            last_error = f"exception: {str(e)[:160]}"
            round_failed = True
            try:
                backlog = _pending_count()
            except Exception:
                backlog = -1
            try:
                _heartbeat()
                _write_pusher_status(consecutive_failures, last_success_at, last_error, backlog)
            except Exception:
                logger.exception("failed to write pusher status after exception")

        if args.once:
            # --once is used by tests / cron; a failed or errored round must exit
            # non-zero so the caller can tell push didn't go through.
            sys.exit(1 if round_failed else 0)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
