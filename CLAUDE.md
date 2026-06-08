# coin_trader_platform — 项目导航

## 目录结构

```
coin_trader_platform/
├── history_data_manager/    # 历史数据拉取（从 evan@btc-ml）
│   ├── fetch_history.py     # SSH + rsync 拉 OHLCV + taker_flow
│   └── data/
│       ├── ohlcv/           # {slug}_{tf}.csv  (15m / 4h)
│       └── taker_flow/      # {slug}_15m.csv
├── realtime_data_pull/      # K 线 + flow 数据层
│   ├── config.py            # SYMBOLS / WARMUP_BARS / WS URL
│   ├── models.py            # Bar / FlowBar / FlowAccum
│   ├── bar_buffer.py        # BarBuffer(maxlen=600) 滚动双端队列
│   ├── binance_ws.py        # Binance Futures WebSocket + REST warmup
│   └── feed.py              # RealtimeFeed / ReplayFeed
├── signal_generator/        # A / A+ 信号生成
│   ├── params.py            # SignalParams + 两套预设 + SYMBOL_PARAMS
│   ├── events.py            # SignalEvent / StateEvent
│   ├── indicators.py        # sma / rma / atr / highest / lowest / crossover
│   ├── signal_logic.py      # evaluate() → SignalEvent | None
│   ├── state_machine.py     # Pending → Confirmed / Expired
│   └── generator.py         # SignalGenerator（订阅 feed，维护 buffer）
├── draw_kline/              # 信号 K 线图绘制（给 VLM 用）
│   ├── common.py            # 颜色 / SLUG_MAP / 数据加载工具
│   ├── indicators.py        # rolling_ma / compute_qrc / compute_4h_structure_steps
│   ├── chart_4h.py          # 4H 上下文图（3个月 + volume 副图）
│   ├── chart_15m.py         # 15m 详情图（主图 + imbalance + VCF 副图）
│   └── renderer.py          # render(signal, data_dir, out_dir) → (path_4h, path_15m)
├── analyze_params.py        # 全量回放，输出四品种信号分布统计
├── plot_signals.py          # 生成最近 N 个信号的 K 线面板图
└── pine/
    └── tradingview.pine     # 原始 Pine Script MTF v3.4（参考用）
```

---

## 数据

**来源**：`evan@btc-ml:~/repo/ai_crypto_analyst` SQLite → rsync 到本地

**品种**：BTC/USDT · ETH/USDT · BNB/USDT · SOL/USDT

**文件**：
```
data/ohlcv/btcusdt_15m.csv   data/ohlcv/btcusdt_4h.csv
data/ohlcv/ethusdt_15m.csv   ...
data/taker_flow/btcusdt_15m.csv   # 含 imbalance + taker_buy/sell_quote_volume
```

**ReplayFeed 关键修复**：
- 15m 数据按 `_start` 截断（信号只在 15m 触发）
- 4h / 1w 数据**不截断**，从完整历史加载做 warmup——否则 weekly MA50 在 2026 年初会全部 fallback 到 neutral

---

## 信号生成

### 信号定义

- **A+**：极端结构位置（pos ≤ 5%/95% for conservative，≤ 10%/90% for standard）+ 强能量（wick/body 高阈值 or vol spike or MA cross）
- **A**：结构触线但未升级到 A+
- **输出字段**：`symbol / grade / bar_time / close / structure_side / structure_space / position_in_structure / vol_ratio / weekly_trend / h4_support / h4_resistance`
- **无方向**：没有 long/short；`structure_side = "near_support" | "near_resistance"`

### 参数（固化，不需要外部传入）

`signal_generator/params.py` 中的 `SYMBOL_PARAMS`：

| 参数 | standard (BTC/BNB) | conservative (ETH/SOL) |
|------|-------------------|----------------------|
| extreme_pos | 10% / 90% | 5% / 95% |
| wick_body_threshold | 1.0 | 1.5 |
| zone_pct | 1.5% | 1.0% |
| vol_ratio_threshold | 1.5 | 2.0 |
| vol_mult | 1.0 | 1.2 |
| reversal_pct | 0.3% | 0.4% |
| window_dedup | 32 bar (8h) | 32 bar (8h) |
| alert_struct_lookback | 20 (4H bar) | 20 (4H bar) |
| min_space | 2.0 | 2.0 |

**alert 层结构位**（signal_logic.py 用）：`highest(highs_4h, 20)` / `lowest(lows_4h, 20)`（Non-repaint，和 Pine Alert 版对齐）

### 信号频率（2026-01 → 2026-05，145天）

| 品种 | 参数 | 总信号 | 频率/天 | A+占比 |
|------|------|--------|---------|--------|
| BTC | standard | 172 | 1.17 | 34% |
| ETH | conservative | 120 | 0.82 | 33% |
| BNB | standard | 175 | 1.19 | 29% |
| SOL | conservative | 120 | 0.82 | 38% |

### 使用方式

```python
from realtime_data_pull import ReplayFeed, RealtimeFeed
from signal_generator import SignalGenerator

# Replay
feed = ReplayFeed(data_dir=Path("history_data_manager/data"),
                  symbols=["BTC/USDT", ...], start="2026-01-01", end="2026-06-05")
gen  = SignalGenerator(feed=feed, symbols=["BTC/USDT", ...])
# params 不传 → 自动按 SYMBOL_PARAMS 分配

@gen.on_signal
async def handle(evt): ...

await feed.start()
```

---

## K 线图绘制（draw_kline）

### 用途

信号触发后生成两张图，交给 VLM 做剧本判断。所有时间信息隐藏，X 轴只显示相对时间。

### 两张图

**图1：4H 上下文（`chart_4h.py`）**
- 信号前 3 个月的 4H K 线
- 副图：4H volume（bull/bear 着色）
- X 轴：Relative days to T0（-90 到 0）
- T0 竖虚线 + T0 价格横虚线

**图2：15m 详情（`chart_15m.py`）**
- 信号前 14 天的 15m K 线
- 主图叠加：
  - **4H 结构位**（红/绿阶梯线）：pivot high/low，`left=3, right=3`，和 Pine visual 层对齐，台阶少、持平时间长
  - **QRC192**（magenta/slate/lime 三条平行直线）：T0 时刻冻结，`hlc3` 线性回归 + high/low 残差 90/10 分位，和 Pine 实现完全一致，三线严格平行
  - **MA20**（amber）
- 副图1：Flow Imbalance（teal/red bar，T0 bar 蓝色高亮，±0.2 参考线）
- 副图2：Visible Cumulative Flow（白线 + teal/red fill）
- X 轴：T-336h 到 T+0h

### 关键实现说明

**QRC192**：Pine 里是信号时刻冻结的直线，不是 rolling 计算。用 `hlc3` 做 OLS，上下偏移 = `percentile(high/low 残差, 90/10)`，是常数偏移 → 三线平行。

**4H 结构位（视觉层）**：`ta.pivothigh(high, 3, 3)` 等价实现。bar i 是 swing high 当且仅当左右各 3 根都比它低，确认发生在 i+3，`last4HResistance` 持平到下一个 pivot。

**4H 结构位（报警层）**：`highest(highs_4h, 20)` rolling，用于 signal_logic.py 的 near_support/near_resistance 判断，和视觉层是两套不同实现（和 Pine 一致）。

### 调用

```python
from draw_kline import render

path_4h, path_15m = render(
    signal,                         # SignalEvent
    data_dir=Path("history_data_manager/data"),
    out_dir=Path("/tmp/charts"),
)
# 输出文件名：{sym}_{grade}_{YYYYMMDD_HHMM}_{4h|15m}.png
```

---

## 辅助脚本

```bash
# 统计四品种信号分布
python3 analyze_params.py

# 生成最近 N 个信号面板图（默认 10 个/品种）
python3 plot_signals.py
python3 plot_signals.py --symbol BTC -n 20

# 快速验证回放
python3 run_replay.py --symbol BTC/USDT --start 2026-05-01 --end 2026-05-10
```
