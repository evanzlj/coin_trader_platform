#!/usr/bin/env python3
"""
vlm_local_worker — btc-ml 本地 VLM worker(无头 codex,取代 Mini 浏览器 + signal_sync)。

流程:
  watch vlm_pending/{pkg}/ (有 .ready、未处理、未过期)
  → vlm_reader(默认 codex,ChatGPT 订阅,无浏览器) 读 prompt+2图 出 playbook JSON
  → 写 vlm_done_incoming/{pkg}/vlm_response.json(finalizer 配对 vlm_pending/{pkg}/signal.json 处理)

跟 Mini 浏览器版的区别:同进程同机,无浏览器(无 stuck_analyzing/cookie/1155)、无跨境 signal_sync。
不写 state.json、不碰交易决策——只产 vlm_response.json,和原 Mini vlm_worker 同契约。

跑(需 nvm node 20 在 PATH 里,codex 才可用):
  python3 -m live.vlm_local_worker
环境变量:VLM_BACKEND(codex|gemini|claude,默认 codex)
"""
from __future__ import annotations
import json, os, sys, time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
from vlm_reader import vlm_read  # noqa: E402

VLM_PENDING       = Path(os.environ.get("VLM_PENDING",       str(ROOT / "vlm_pending")))
VLM_DONE_INCOMING = Path(os.environ.get("VLM_DONE_INCOMING", str(ROOT / "vlm_done_incoming")))
HEARTBEAT = ROOT / "live" / "heartbeat" / "vlm_local_worker_last_run.txt"
STATUS    = ROOT / "live" / "heartbeat" / "vlm_local_worker_status.json"
LOCK      = ROOT / "live" / "vlm_local_worker.lock"

BACKEND   = os.environ.get("VLM_BACKEND", "codex")
POLL_SECONDS = int(os.environ.get("VLM_POLL_SECONDS", "30"))
# 本地 stale 闸:< finalizer 的 240min,留 buffer,不浪费 codex 调用在会过期的包上
STALE_MIN = int(os.environ.get("VLM_STALE_MIN", "210"))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("vlm_local_worker")


def _hb():
    HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
    try: HEARTBEAT.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")
    except Exception: pass

def _status(stats):
    try:
        tmp = STATUS.with_suffix(".tmp")
        tmp.write_text(json.dumps(dict(stats, updated_at=pd.Timestamp.now("UTC").isoformat()),
                                  ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, STATUS)
    except Exception: pass


def fresh_pending():
    out = []
    cutoff = pd.Timestamp.now("UTC") - pd.Timedelta(minutes=STALE_MIN)
    if not VLM_PENDING.exists(): return out
    for d in sorted(VLM_PENDING.iterdir()):
        if not d.is_dir() or not (d / ".ready").exists():
            continue
        if (VLM_DONE_INCOMING / d.name / "vlm_response.json").exists():
            continue                       # 已产出,等 finalizer 收
        sig = d / "signal.json"
        if sig.exists():
            try:
                bt = pd.Timestamp(json.loads(sig.read_text()).get("bar_time", "2000-01-01"), tz="UTC")
                if bt < cutoff:
                    continue               # 会过期,跳过不浪费 codex
            except Exception:
                pass
        out.append(d)
    return out


def process_one(d: Path) -> str:
    prompt_f = d / "prompt.txt"
    img4 = sorted(d.glob("*_4h.png")); img15 = sorted(d.glob("*_15m.png"))
    if not prompt_f.exists() or not img4 or not img15:
        return "missing_materials"
    res = vlm_read(prompt_f.read_text(encoding="utf-8"), img4[0], img15[0], backend=BACKEND)
    if res is None:
        return "vlm_failed"
    out = VLM_DONE_INCOMING / d.name
    out.mkdir(parents=True, exist_ok=True)
    tmp = out / "vlm_response.json.tmp"
    tmp.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, out / "vlm_response.json")
    return "ok"


def main():
    from live.single_instance import SingleInstance, AlreadyRunning
    try:
        SingleInstance(LOCK).acquire()
    except AlreadyRunning as e:
        logger.error("already running: %s", e); sys.exit(1)
    VLM_DONE_INCOMING.mkdir(parents=True, exist_ok=True)
    logger.info("vlm_local_worker started — backend=%s, watching %s", BACKEND, VLM_PENDING)
    stats = {"processed": 0, "ok": 0, "error": 0, "consecutive_failures": 0,
             "last_success_at": None, "last_error": None, "backend": BACKEND}
    while True:
        pend = fresh_pending()
        if not pend:
            _hb(); _status(stats); time.sleep(POLL_SECONDS); continue
        logger.info("scan: %d pending: %s", len(pend), [d.name for d in pend])
        for d in pend:
            logger.info("processing %s (codex) ...", d.name)
            try:
                st = process_one(d)
            except Exception as e:
                st = f"exc:{str(e)[:120]}"
            stats["processed"] += 1
            if st == "ok":
                stats["ok"] += 1; stats["consecutive_failures"] = 0
                stats["last_success_at"] = pd.Timestamp.now("UTC").isoformat()
                logger.info("✓ %s → vlm_done_incoming", d.name)
            else:
                stats["error"] += 1; stats["consecutive_failures"] += 1
                stats["last_error"] = st
                logger.warning("✗ %s: %s", d.name, st)
            _hb(); _status(stats)
        _hb(); _status(stats)


if __name__ == "__main__":
    main()
