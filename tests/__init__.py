"""测试包。导入即生效的生产隔离 —— 必须在任何 live.* 之前跑到。

`python3 -m unittest tests.x` 和 pytest（tests 是包，收集时会先导入本文件）都会经过这里。
两件事，都用 setdefault，需要时可从外部 env 显式覆盖：

1. NOTIFY_DISABLED —— 单测走遍 executor 的异常分支，每条都调 feishu_alert，
   不拦就是几十条真告警轰进飞书群。
2. TRADE_LOG —— notify.trade_log() 默认 append 到 live/trade_log.jsonl，
   测试的 mock 成交会混进生产流水，污染 dashboard 战报。
"""
import os
import tempfile
from pathlib import Path

os.environ.setdefault("NOTIFY_DISABLED", "1")
os.environ.setdefault(
    "TRADE_LOG", str(Path(tempfile.gettempdir()) / "coin_trader_test_trade_log.jsonl")
)
