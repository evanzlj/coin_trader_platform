"""飞书连通性检查 —— **全套测试里唯一会真发消息的用例**，默认 skip。

其余所有测试一律静默（tests/__init__.py 设 NOTIFY_DISABLED=1），否则单测走遍
executor 的异常分支会把几十条假告警轰进群。

想验证 webhook 还通时手动开：

    NOTIFY_LIVE_CHECK=1 python3 -m unittest tests.test_notify_live -v

发一条 [connectivity check] 到报警群，仅此一条。
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import tests  # noqa: F401,E402 — 直跑本文件（绕开包导入）时也要过生产隔离，见 tests/__init__.py

import pandas as pd

from live import exec_config as cfg


@unittest.skipUnless(os.environ.get("NOTIFY_LIVE_CHECK") == "1",
                     "需 NOTIFY_LIVE_CHECK=1 显式开启（会真发飞书）")
class TestFeishuConnectivity(unittest.TestCase):
    def test_send_one_message(self):
        self.assertTrue(cfg.FEISHU_WEBHOOK,
                        "FEISHU_WEBHOOK 未配置（env 或 live/feishu_webhook.txt）")

        # 绕开 _muted()：本用例的目的就是打真实网络，直接走 requests
        import requests

        ts = pd.Timestamp.now("UTC").isoformat(timespec="seconds")
        resp = requests.post(
            cfg.FEISHU_WEBHOOK,
            json={"msg_type": "text",
                  "content": {"text": f"[connectivity check] coin_trader 测试自检 {ts}"}},
            timeout=10,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json().get("code"), 0, resp.text)


if __name__ == "__main__":
    unittest.main()
