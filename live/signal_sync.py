#!/usr/bin/env python3
"""
signal_sync（re-arch Phase 3）— Windows 双向同步：拉 btc-ml vlm_pending → 本地，推 vlm_response → btc-ml。

职责（严格限于传输层，不参与任何交易决策）：
  拉：btc-ml vlm_pending/{pkg}/ → Windows local_vlm_pending/{pkg}/
      只拉 .ready / signal.json / prompt.txt / chart png，不拉/不写 state.json。
  推：Windows local_vlm_done/{pkg}/vlm_response.json → btc-ml vlm_done_incoming/{pkg}/
      远端 staging → atomict mv 落地，finalizer 绝看不到半包。

铁律：
  - 绝不写 btc-ml signal_active。
  - 绝不生成或修改 state.json。
  - 远端已存在（signal_active/signal_done/vlm_done_incoming 已有 pkg）→ skip。
  - 本地/远端都幂等；已拉/已推标记 + 远端存在检查。

用法：
    python3 -m live.signal_sync            # 常驻轮询
    python3 -m live.signal_sync --once     # 单次（测试用）

环境变量：
    LOCAL_VLM_PENDING      本地 pending 目录
    LOCAL_VLM_DONE         已完成（待推）目录
    REMOTE_HOST            远端 btc-ml（默认 evan@btc-ml）
    SIGNAL_ACTIVE          btc-ml signal_active（做远端已存在检查）
    SSH_OPTS               额外 ssh 参数
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
import sys
import tarfile
import time
from pathlib import Path
from typing import Optional

from live import exec_config as cfg

logger = logging.getLogger("signal_sync")

# ── Paths (Windows 本地) ─────────────────────────────────────────────────────

LOCAL_VLM_PENDING = Path(os.environ.get("LOCAL_VLM_PENDING", str(cfg.ROOT / "local_vlm_pending")))
LOCAL_VLM_DONE    = Path(os.environ.get("LOCAL_VLM_DONE",    str(cfg.ROOT / "local_vlm_done")))
SYNCED_PULL_DIR   = cfg.ROOT / ".synced_pulled"
SYNCED_PUSH_DIR   = cfg.ROOT / ".synced_pushed"

STATUS_FILE       = cfg.ROOT / "live" / "heartbeat" / "signal_sync_status.json"
HEARTBEAT_FILE    = cfg.ROOT / "live" / "heartbeat" / "signal_sync_last_run.txt"
LOCK_FILE         = cfg.ROOT / "live" / "sync.lock"

# ── Remote 配置 ───────────────────────────────────────────────────────────────

REMOTE_HOST    = os.environ.get("REMOTE_HOST", "evan@btc-ml")
REMOTE_ROOT    = "/home/evan/repo/coin_trader_platform"
SSH_TIMEOUT    = 90
POLL_SECONDS   = 30

# 只拉这些远端文件（不拉 state.json — 它由 btc-ml finalizer 生成）
_PULL_FILE_PATTERNS = {".ready", "signal.json", "prompt.txt", "*_4h.png", "*_15m.png"}

# 远端目录
_RP = lambda p: f"{REMOTE_ROOT}/{p}"
_VLM_PENDING       = _RP("vlm_pending")
_VLM_DONE_INCOMING = _RP("vlm_done_incoming")
_SIGNAL_ACTIVE     = _RP("signal_active")
_SIGNAL_DONE       = _RP("signal_done")

_PKG_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ssh(cmd: list[str], timeout: int = SSH_TIMEOUT, text: bool = True,
         input_data: Optional[bytes] = None) -> subprocess.CompletedProcess:
    """Run ssh with standard hardening opts."""
    full_cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
                "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3",
                REMOTE_HOST] + cmd
    try:
        r = subprocess.run(
            full_cmd, capture_output=True, text=text, timeout=timeout,
            input=input_data.decode() if isinstance(input_data, bytes) and text else input_data,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"ssh timed out after {timeout}s") from e
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "unknown error").strip()[:200])
    return r


# Lease (seconds). If Windows claims a package then crashes, lease expires and the
# package becomes available for re-claim (P1-1). >> chatbot TIMEOUT 360s.
_CLAIM_LEASE = 1800   # 30 min


def _remote_list_new() -> list[str]:
    """List remote vlm_pending packages claimable, claim atomically.

    Atomic claim via `mkdir .claimed` (no-clobber — if the dir exists, mkdir
    fails and the package is skipped or falls to lease check). Single-Windows
    worker is the default but the door is atomic (G6)."""
    script = (
        f"cd {_VLM_PENDING} 2>/dev/null || exit 0; "
        f"NOW=$(date +%s); LEASE={_CLAIM_LEASE}; "
        f"for d in */; do "
        f"  pkg=${{d%/}}; "
        f"  if [ ! -f \"$pkg/.ready\" ]; then continue; fi; "
        f"  # Atomically claim via mkdir (fails fast if .claimed dir exists)\n"
        f"  if [ -d \"$pkg/.claimed\" ]; then "
        f"    # Lease expired? rm -rf (non-empty dir) then mkdir (single-worker safe)\n"
        f"    if [ -f \"$pkg/.claimed/lease_ts\" ]; then "
        f"      TS=$(cat \"$pkg/.claimed/lease_ts\" 2>/dev/null); "
        f"      if [ -n \"$TS\" ] && [ \"$(( NOW - TS ))\" -lt \"$LEASE\" ]; then continue; fi; "
        f"    else continue; fi; "
        f"    rm -rf \"$pkg/.claimed\"; "
        f"  fi; "
        f"  mkdir \"$pkg/.claimed\" 2>/dev/null || continue; "
        f"  echo \"$NOW\" > \"$pkg/.claimed/lease_ts\" && "
        f"  echo \"$pkg\"; "
        f"done"
    )
    r = _ssh([script], text=True)
    names = [n.strip() for n in r.stdout.strip().splitlines() if n.strip()]
    return sorted(names)


def _remote_package_exists(name: str) -> bool:
    """Check if remote already has this pkg in done/active/done_incoming (skip)."""
    checks = f"{_VLM_DONE_INCOMING}/{name} {_SIGNAL_ACTIVE}/{name} {_SIGNAL_DONE}/{name}"
    r = _ssh([f"ls -d {checks} 2>/dev/null | head -1"], text=True)
    return bool(r.stdout.strip())


def _tar_pull(name: str) -> Optional[Path]:
    """Tar a single remote vlm_pending/{name} locally. Returns local path or None on bad package."""
    dest = LOCAL_VLM_PENDING / name
    if dest.exists():
        logger.debug("local %s already exists — skipping pull", name)
        return dest

    remote_tar = (
        f"cd {_VLM_PENDING} && "
        f"if [ -d \"{name}\" ] && [ -f \"{name}/.ready\" ] && [ -f \"{name}/signal.json\" ]; then "
        f"  tar czf - -C {name} .ready signal.json prompt.txt *_4h.png *_15m.png 2>/dev/null; "
        f"  echo '\nTAR_OK'; "
        f"fi"
    )
    try:
        r = _ssh([remote_tar], text=False, timeout=SSH_TIMEOUT)
    except RuntimeError:
        return None

    out = r.stdout
    # Find TAR_OK marker in the output (tar data comes before echo)
    tar_data = out[:out.rfind(b"\nTAR_OK\n")] if b"\nTAR_OK\n" in out else out
    if not tar_data:
        return None

    LOCAL_VLM_PENDING.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r:gz") as tar:
            members = tar.getmembers()
            if not members:
                return None
            dest.mkdir(parents=True, exist_ok=True)
            # Only extract permitted files
            for m in members:
                if "/" in m.name:
                    # Strip any path prefix — tar stores relative to package dir
                    m.name = Path(m.name).name
                tar.extract(m, path=dest)
        if not (dest / ".ready").exists() or not (dest / "signal.json").exists():
            import shutil
            shutil.rmtree(dest, ignore_errors=True)
            return None
        logger.info("pulled %s → local_vlm_pending/ (%d files)", name, len(members))
        return dest
    except Exception as e:
        logger.warning("tar extract failed for %s: %s", name, e)
        import shutil
        shutil.rmtree(dest, ignore_errors=True)
        return None


_REQUIRED_PULL_FILES = {".ready", "signal.json", "prompt.txt"}
_PNG_PATTERNS = ("*_4h.png", "*_15m.png")


def _local_pull_complete(name: str) -> bool:
    """Verify local package has ALL required files (P1: marker !== truth).
    Even if .synced_pulled/{name} exists, a corrupt/missing local package
    must NOT block re-pull. Required: .ready + signal.json + prompt.txt + both PNGs."""
    pkg = LOCAL_VLM_PENDING / name
    if not pkg.exists():
        return False
    for req in _REQUIRED_PULL_FILES:
        if not (pkg / req).exists():
            return False
    for pat in _PNG_PATTERNS:
        if not list(pkg.glob(pat)):
            return False
    return True


def _maybe_clear_pulled(name: str) -> None:
    """If .synced_pulled says pulled but local is incomplete, clear both so
    the package can be re-pulled (P1: self-healing after crash/partial failure)."""
    if not (SYNCED_PULL_DIR / name).exists():
        return
    if not _local_pull_complete(name):
        logger.warning("local %s incomplete despite pulled marker — clearing for re-pull", name)
        import shutil
        shutil.rmtree(LOCAL_VLM_PENDING / name, ignore_errors=True)
        (SYNCED_PULL_DIR / name).unlink(missing_ok=True)


def _mark_pulled(name: str) -> None:
    SYNCED_PULL_DIR.mkdir(parents=True, exist_ok=True)
    (SYNCED_PULL_DIR / name).touch()


def _already_pulled(name: str) -> bool:
    return (SYNCED_PULL_DIR / name).exists()


def pull_round() -> tuple[int, int]:
    """Pull new packages from remote. Returns (pulled, failed)."""
    pulled = failed = 0

    # Before pulling new, heal any incomplete local packages (P1)
    for existing in (SYNCED_PULL_DIR.iterdir() if SYNCED_PULL_DIR.exists() else []):
        _maybe_clear_pulled(existing.name)

    for name in _remote_list_new():
        if _already_pulled(name):
            continue
        if _remote_package_exists(name):
            logger.info("remote already has %s in done/active — marking pulled", name)
            _mark_pulled(name)
            continue
        local = _tar_pull(name)
        if local is not None and _local_pull_complete(name):
            _mark_pulled(name)
            pulled += 1
        else:
            if local is not None:
                import shutil
                shutil.rmtree(local, ignore_errors=True)
            failed += 1
    return pulled, failed


# ── 推送方向 ─────────────────────────────────────────────────────────────────

def _local_done_pkgs() -> list[Path]:
    """Local packages with vlm_response.json + .ready (ready to push)."""
    if not LOCAL_VLM_DONE.exists():
        return []
    return sorted([
        d for d in LOCAL_VLM_DONE.iterdir()
        if d.is_dir() and (d / ".ready").exists() and (d / "vlm_response.json").exists()
        and _PKG_RE.match(d.name)
    ])


def _already_pushed(name: str) -> bool:
    return (SYNCED_PUSH_DIR / name).exists()


def _mark_pushed(name: str) -> None:
    SYNCED_PUSH_DIR.mkdir(parents=True, exist_ok=True)
    (SYNCED_PUSH_DIR / name).touch()


def _remote_push_staging(pkg_dir: Path) -> str:
    """Push vlm_response.json to remote staging → atomic mv. Returns 'MOVED'/'SKIP'/'FAIL'."""
    name = pkg_dir.name

    # Check remote existence first
    if _remote_package_exists(name):
        return "SKIP"

    resp = pkg_dir / "vlm_response.json"
    if not resp.exists():
        return "FAIL"

    # Build tar with only vlm_response.json (G5: never send state.json/signal.json)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(resp, arcname=f"{name}/vlm_response.json")
        # Also send .ready so remote can verify completeness
        ready = pkg_dir / ".ready"
        if ready.exists():
            tar.add(ready, arcname=f"{name}/.ready")
    data = buf.getvalue()

    # Remote: extract to staging → atomic mv → finalizer never sees half-package
    remote_script = (
        f"mkdir -p {_VLM_DONE_INCOMING} && "
        f"cd {_VLM_DONE_INCOMING} && "
        f"mkdir -p .tmp_{name} && "
        f"tar xzf - -C .tmp_{name} && "
        f"if [ -f \".tmp_{name}/{name}/vlm_response.json\" ]; then "
        f"  mv .tmp_{name}/{name} {name} && rm -rf .tmp_{name} && echo MOVED; "
        f"else "
        f"  rm -rf .tmp_{name} && echo BAD_PACKAGE; "
        f"fi"
    )
    try:
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", REMOTE_HOST, remote_script],
            input=data, capture_output=True, timeout=SSH_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return "FAIL"
    out = r.stdout.decode(errors="replace").strip()
    err = r.stderr.decode(errors="replace").strip()
    if r.returncode != 0:
        logger.warning("push %s ssh failed rc=%s: %s", name, r.returncode, err[:120])
        return "FAIL"
    if out.endswith("MOVED"):
        return "MOVED"
    if out.endswith("BAD_PACKAGE"):
        logger.warning("push %s remote rejected as bad", name)
        return "FAIL"
    # Could be SKIP_EXISTS if vlm_done_incoming/{name} already existed
    logger.warning("push %s unexpected: %s", name, out[:120])
    return "FAIL"


def push_round() -> tuple[int, int]:
    """Push done responses to remote. Returns (pushed, failed)."""
    pushed = failed = 0
    for pkg_dir in _local_done_pkgs():
        if _already_pushed(pkg_dir.name):
            continue
        result = _remote_push_staging(pkg_dir)
        if result == "MOVED":
            _mark_pushed(pkg_dir.name)
            pushed += 1
            logger.info("pushed → vlm_done_incoming/%s", pkg_dir.name)
        elif result == "SKIP":
            _mark_pushed(pkg_dir.name)
            logger.info("skip %s — already on remote", pkg_dir.name)
        else:
            failed += 1
    return pushed, failed


# ── Heartbeat / Status ────────────────────────────────────────────────────────

def _heartbeat() -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        HEARTBEAT_FILE.write_text(datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                  encoding="utf-8")
    except Exception as e:
        logger.warning("heartbeat write failed (non-fatal): %s", e)


def _pending_count() -> int:
    count = 0
    try:
        script = f"cd {_VLM_PENDING} 2>/dev/null && ls -d */ 2>/dev/null | wc -l"
        r = _ssh([script], text=True)
        count = int(r.stdout.strip())
    except Exception:
        pass
    return count


def _backlog_count() -> int:
    return sum(1 for d in LOCAL_VLM_DONE.iterdir() if d.is_dir() and (d / ".ready").exists()
               and not _already_pushed(d.name)) if LOCAL_VLM_DONE.exists() else 0


def _write_status(pushed: int, failed: int, backlog: int, last_error: Optional[str] = None) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        tmp = STATUS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "pushed": pushed, "failed": failed, "backlog_count": backlog,
            "last_error": last_error,
            "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }, indent=2), encoding="utf-8")
        os.replace(tmp, STATUS_FILE)
    except Exception as e:
        logger.warning("status write failed (non-fatal): %s", e)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    from live.single_instance import SingleInstance, AlreadyRunning

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    try:
        _lock = SingleInstance(LOCK_FILE)
        _lock.acquire()
    except AlreadyRunning as e:
        logger.error("signal_sync already running: %s", e)
        sys.exit(1)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true", help="单次（测试用）")
    args = ap.parse_args()

    logger.info("signal_sync started: pull %s → %s | push %s → %s",
                _VLM_PENDING, LOCAL_VLM_PENDING, LOCAL_VLM_DONE, _VLM_DONE_INCOMING)

    while True:
        round_failed = False
        try:
            p, f = pull_round()
            if p or f:
                logger.info("pull round: pulled=%d failed=%d", p, f)

            backlog = _backlog_count()
            pd, fd = push_round()
            if pd or fd:
                logger.info("push round: pushed=%d failed=%d backlog=%d", pd, fd, backlog)

            if f > 0 or fd > 0:
                round_failed = True

            _heartbeat()
            _write_status(p + pd, f + fd, backlog)

        except Exception as e:
            logger.exception("signal_sync cycle error")
            round_failed = True
            try:
                _heartbeat()
                _write_status(0, 0, _backlog_count(), last_error=str(e)[:200])
            except Exception:
                pass

        if args.once:
            sys.exit(1 if round_failed else 0)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
