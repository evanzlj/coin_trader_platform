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

# 只在这些交易所建账户/Broker（即使 keys 里有更多）。默认两所；
# EXECUTOR_EXCHANGES=okx → 纯 OKX（如 Binance testnet 行情失真时只跑 OKX demo）。
EXCHANGES = {s.strip().lower() for s in
             os.environ.get("EXECUTOR_EXCHANGES", "binance,okx").split(",") if s.strip()}

# ── 品种与仓位（§7）──────────────────────────────────────────────────────────
SYMBOLS        = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
# ONE_R_USDT：锁定的名义风险（1R=$X）。仓位 notional = ONE_R_USDT*100/r_dist_pct。
# 可用 env 覆盖 → 实盘最小仓验证阶段设成几毛钱（1R=$0.30），验证管路后再抬回 10。
ONE_R_USDT     = float(os.environ.get("ONE_R_USDT", "10"))
SYMBOL_MARGIN  = {"BTC/USDT": 40, "ETH/USDT": 40, "BNB/USDT": 70, "SOL/USDT": 40}
# SYMBOL_MARGIN override（只影响杠杆与 slot C1 记账，不影响仓位大小——仓位由 ONE_R_USDT 决定）。
# 最小仓验证阶段设小，让低余额账户也能进 slot。两种 env（标量优先，systemd 无引号更省事）：
#   SYMBOL_MARGIN_ALL=5                          所有品种统一
#   SYMBOL_MARGIN_JSON={"BTC/USDT":5,...}        逐品种
_sma = os.environ.get("SYMBOL_MARGIN_ALL")
if _sma:
    SYMBOL_MARGIN = {s: float(_sma) for s in SYMBOL_MARGIN}
_smj = os.environ.get("SYMBOL_MARGIN_JSON")
if _smj:
    import json as _json
    SYMBOL_MARGIN = {**SYMBOL_MARGIN, **{k: float(v) for k, v in _json.loads(_smj).items()}}
SYMBOL_MAX_LEV = {"BTC/USDT": 50, "ETH/USDT": 20, "BNB/USDT": 50, "SOL/USDT": 20}
# 交易所级杠杆上限，与 SYMBOL_MAX_LEV 取更小者。币安自 2025-08-12 起对普通用户子账户
# 强制 ≤5x（超了报 -4421，账户设置改不掉）；未列出的所（含 mock）不额外设限。
# 上限低于 SYMBOL_MAX_LEV 时，逐仓保证金由 notional/上限 反推（见 playbook_fsm.required_margin），
# 不是按 SYMBOL_MARGIN 记账 —— 否则 slot C1 会严重低估实际占用。
EXCHANGE_MAX_LEV = {"binance": 5, "okx": 100}
_emj = os.environ.get("EXCHANGE_MAX_LEV_JSON")
if _emj:
    import json as _json2
    EXCHANGE_MAX_LEV = {**EXCHANGE_MAX_LEV, **{k: int(v) for k, v in _json2.loads(_emj).items()}}

# ── 手续费（用于 dashboard 净R 估算，非精确对账）──────────────────────────────
# 入场 / 保本 / 止损走市价=taker；TP1/TP2 限价 reduceOnly=maker（与 OKX 实测逐笔吻合）。
TAKER_FEE = 0.0005             # 0.05%
MAKER_FEE = 0.0002             # 0.02%

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
SIGNAL_MAX_AGE_MINUTES = 240    # 信号 T0 距今超此 → 丢弃不激活（防同步积压旧信号错时开仓，§17）
OPENING_GRACE_SECONDS  = 120    # OPENING 内 entry FILLED+暂时无仓 → grace 内保持 OPENING（仓位最终一致性窗口，§22 P0）
STALE_DATA_MINUTES = 20         # 最新 bar 超此未更新 → 停开新仓（§21 #3）
RECONCILE_MINUTES  = 15         # 定期对账间隔（§21 #5）
STARTUP_RECONCILE_RETRIES       = 4    # 启动对账撞交易所抖动（get_position 抛 unknown）→ 短重试次数（§22 P1：缩短无 SL 窗口）
STARTUP_RECONCILE_RETRY_SECONDS = 1.5  # 启动对账短重试间隔（秒）

# ── 路径 ────────────────────────────────────────────────────────────────────
def _p(env_key: str, default: Path) -> Path:
    """路径支持 env 覆盖（默认生产路径；进程级故障注入 drill 用隔离目录，不污染生产）。"""
    v = os.environ.get(env_key)
    return Path(v) if v else default

SIGNAL_ACTIVE  = _p("SIGNAL_ACTIVE",  ROOT / "signal_active")
SIGNAL_DONE    = _p("SIGNAL_DONE",    ROOT / "signal_done")
HEARTBEAT_FILE = _p("HEARTBEAT_FILE", ROOT / "live" / "heartbeat" / "executor_last_run.txt")
TRADE_LOG      = _p("TRADE_LOG",      ROOT / "live" / "trade_log.jsonl")
LOCK_FILE      = _p("LOCK_FILE",      ROOT / "live" / "executor.lock")
CURSOR_FILE    = _p("CURSOR_FILE",    ROOT / "live" / "state" / "executor_cursor.txt")

# btc-ml 本地采集 DB（§20.3）—— executor 直读取行情
OHLCV_DB = Path(os.environ.get(
    "OHLCV_DB",
    "/home/evan/repo/ai_crypto_analyst/data/ai_crypto_analyst.db",
))

# ── 告警 / 业务流水（§10.3, §13）─────────────────────────────────────────────
# webhook 来源优先级：env > 文件 live/{name}.txt（gitignored）。
# 文件方式让 launchd / nohup 不必把 secret 写进 plist，且各机本地保管、不入库。
def _load_webhook(env_name: str, filename: str) -> str:
    env = os.environ.get(env_name, "").strip()
    if env:
        return env
    f = Path(__file__).parent / filename
    try:
        if f.exists():
            return f.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return ""

# 报警（something 坏了）
FEISHU_WEBHOOK = _load_webhook("FEISHU_WEBHOOK", "feishu_webhook.txt")
# 业务流水（信号出现 / finalizer 结论 / 后续 executor 里程碑）。独立 webhook，
# 想分群就配 COIN_FLOW_WEBHOOK；不配则回退到报警 webhook，混在同群里。
FLOW_WEBHOOK = _load_webhook("COIN_FLOW_WEBHOOK", "flow_webhook.txt") or FEISHU_WEBHOOK

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
