"""
Executor 配置（§12.2 + §21 运行参数）。

参数集中在此。代码里不出现 API key（key 走 keys_loader）。
部署：executor 跑在 btc-ml（新加坡），ENV 切换 testnet / live。

环境变量覆盖：
  EXECUTOR_ENV    testnet | live
  OHLCV_DB        btc-ml 本地采集 DB 路径
  FEISHU_WEBHOOK  告警 webhook
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).parent.parent

# ── 环境 ────────────────────────────────────────────────────────────────────
ENV = os.environ.get("EXECUTOR_ENV", "testnet")   # "testnet" | "live"

# ── 品种与仓位（§7）──────────────────────────────────────────────────────────
SYMBOLS        = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
ONE_R_USDT     = 10
SYMBOL_MARGIN  = {"BTC/USDT": 40, "ETH/USDT": 40, "BNB/USDT": 70, "SOL/USDT": 40}
SYMBOL_MAX_LEV = {"BTC/USDT": 50, "ETH/USDT": 20, "BNB/USDT": 50, "SOL/USDT": 20}

# ── 止损 / 出场（§6）─────────────────────────────────────────────────────────
SL_MODE         = "touch"      # 真实 stop 单，盘中触发
TRIGGER_PX_TYPE = "last"       # Binance CONTRACT_PRICE / OKX last，与信号 level 同口径
BTC_SL_BUFFER   = 0.001        # 0.1%，仅 BTC，SL 外扩躲 wick
TP1_PROTECT     = "be"         # TP1 后剩余半仓 SL = 入场价（保本）

# ── 持仓模式（§8.4）─────────────────────────────────────────────────────────
POS_MODE    = "hedge"          # long/short；平仓靠 posSide + 反向 side，不用 reduceOnly
MARGIN_MODE = "isolated"       # 逐仓

# ── 账户池（§9）─────────────────────────────────────────────────────────────
ACCOUNTS            = 10        # 5 Binance + 5 OKX
CAPITAL_PER_ACCOUNT = 120       # 每账户本金 USDT
MAX_PER_ACCOUNT     = 2         # 每账户最多并行 2 笔
OKX_R_DEV_THRESHOLD = 0.05      # |取整偏差|超此 → 该所账户对该品种不满足约束 C4（§8.3）

# ── 循环 / 运行参数（§4, §21）────────────────────────────────────────────────
POLL_SECONDS       = 15         # 主循环节拍；按真实环境实测调（§18 P1-9）
STALE_DATA_MINUTES = 20         # 最新 bar 超此未更新 → 停开新仓（§21 #3）
RECONCILE_MINUTES  = 15         # 定期对账间隔（§21 #5）

# ── 路径 ────────────────────────────────────────────────────────────────────
SIGNAL_ACTIVE  = ROOT / "signal_active"
SIGNAL_DONE    = ROOT / "signal_done"
HEARTBEAT_FILE = ROOT / "live" / "heartbeat" / "executor_last_run.txt"
TRADE_LOG      = ROOT / "live" / "trade_log.jsonl"
LOCK_FILE      = ROOT / "live" / "executor.lock"
CURSOR_FILE    = ROOT / "live" / "state" / "executor_cursor.txt"

# btc-ml 本地采集 DB（§20.3）—— executor 直读取行情
OHLCV_DB = Path(os.environ.get(
    "OHLCV_DB",
    "/home/evan/repo/ai_crypto_analyst/data/ai_crypto_analyst.db",
))

# ── 告警（§10.3, §13）───────────────────────────────────────────────────────
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "")   # 从 env 注入，不硬编码

# ── 交易所 base url / 模拟盘开关（ENV 切换，§19）──────────────────────────────
BINANCE_BASE = {
    "testnet": "https://testnet.binancefuture.com",
    "live":    "https://fapi.binance.com",
}
# OKX：base url 不变，靠 header x-simulated-trading 区分（demo=1）
OKX_SIMULATED = {"testnet": "1", "live": "0"}


def binance_base_url() -> str:
    return BINANCE_BASE[ENV]


def okx_simulated_flag() -> str:
    return OKX_SIMULATED[ENV]
