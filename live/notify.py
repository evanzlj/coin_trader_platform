"""
飞书告警 + trade_log（§10.3, §13）。

- feishu_alert：推 FEISHU_WEBHOOK；失败只记日志，**绝不影响主流程**（告警挂了不能拖垮交易）。
- trade_log：append 一行到 trade_log.jsonl（每个交易事件，含 actual_r_usdt 等）。
"""
from __future__ import annotations

import json
import logging

import pandas as pd

from live import exec_config as cfg

try:
    import requests
except ImportError:                       # requests 未装时降级为只记日志
    requests = None

logger = logging.getLogger(__name__)


def feishu_alert(message: str) -> None:
    logger.error("ALERT: %s", message)
    url = cfg.FEISHU_WEBHOOK
    if not url or requests is None:
        return
    try:
        requests.post(
            url,
            json={"msg_type": "text", "content": {"text": f"[executor] {message}"}},
            timeout=5,
        )
    except Exception as e:                 # noqa: BLE001 — 告警失败不可影响主流程
        logger.warning("feishu post failed: %s", e)


def flow_event(message: str) -> None:
    """业务流水推送（信号出现 / finalizer 结论 / executor 里程碑）。
    走 FLOW_WEBHOOK（默认回退到 FEISHU_WEBHOOK）。失败只记日志，绝不影响主流程。
    message 自带 emoji 前缀（📡/✅/🚫…），不加 [executor] 前缀以便和报警区分。"""
    logger.info("FLOW: %s", message)
    url = cfg.FLOW_WEBHOOK
    if not url or requests is None:
        return
    try:
        requests.post(
            url,
            json={"msg_type": "text", "content": {"text": message}},
            timeout=5,
        )
    except Exception as e:                 # noqa: BLE001 — 流水推送失败不可影响主流程
        logger.warning("flow post failed: %s", e)


def trade_log(event: str, **fields) -> None:
    rec = {"ts": pd.Timestamp.now("UTC").isoformat(), "event": event, **fields}
    try:
        cfg.TRADE_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(cfg.TRADE_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:                 # noqa: BLE001
        logger.warning("trade_log write failed: %s", e)
