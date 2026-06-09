# A-only 实盘交易流程
**整理日期**: 2026-06-09  
**交易所**: DeepCoin（超级分仓）  
**策略**: A-only 全剧本 S3 出场

---

## 一、启动前一次性准备

### 1. 拉取历史数据
从 btc-ml 同步四币历史数据到本地 Windows PC：
```
history_data_manager/data/ohlcv/   → btcusdt_15m.csv / btcusdt_4h.csv / ...
history_data_manager/data/taker_flow/ → btcusdt_15m.csv / ...
```
时间范围：
- **4h OHLCV**：2024-01-01 至今（Weekly MA50 需要 50 周数据）
- **15m OHLCV + taker_flow**：2025-01-01 至今（dedup 连续性热身）

`fetch_history.py` 的 START 已设为 2023-01-01，涵盖以上两个要求。

### 2. 初始化 ReplayFeed 热身（关键）
从 2026-01-01 开始 replay，让 signal generator 的 dedup 状态自然累积到当前时间。  
目的：确保 8h dedup 窗口与历史连续，避免冷启动误触发。

```bash
python3 run_replay.py --start 2026-01-01 --end <today> --warmup-only
```

热身跑完后可以查看最近几个信号状态，确认 dedup 窗口正常后再接 live。

---

## 二、运行时两个独立进程

### 进程 A：信号监控 + VLM 调用
### 进程 B：实时交易执行监控

---

## 三、进程 A：信号监控

### 触发时机
每根 **15m K 线收线后立即触发**（xx:00 / xx:15 / xx:30 / xx:45），不是每分钟轮询。

### 流程

```
[15m K线收线]
       ↓
拉取 btc-ml 最新数据
  - 四币最近已收线 15m K线
  - 四币最近已收线 flow 数据（15m）
  - 四币最近已收线 4h K线
       ↓
信号生成算法（signal_generator）
  - 四币逐一评估 A-only 条件
  - dedup 窗口过滤（8h/32 bars）
       ↓
    有信号？
  No → 结束，等下一根 K线
  Yes ↓
生成 prompt + 15m图 + 4h图
写入 signal_pending/ 目录：
  signal_pending/{sym}_{grade}_{YYYYMMDD_HHMM}/
    ├── signal.json
    ├── prompt.txt
    ├── {sym}_{grade}_{YYYYMMDD_HHMM}_15m.png
    └── {sym}_{grade}_{YYYYMMDD_HHMM}_4h.png
       ↓
openclaw 监听到新文件
  → 浏览器打开 ChatGPT bot
  → 发送 prompt.txt + 两张图
  → 等待响应（通常 1-3 分钟）
  → 抓取返回 JSON
  → 写入 vlm_response.json
       ↓
移动到 signal_active/ 目录：
  signal_active/{sym}_{grade}_{YYYYMMDD_HHMM}/
    ├── signal.json
    ├── vlm_response.json
    └── state.json  ← 初始状态 WAITING
```

### 过滤规则（A-only，写入 signal.json 时预过滤）

| 币种 | r_dist 要求 | TP1 要求 | b2act |
|------|------------|---------|-------|
| BTC  | >= 0.5%    | < 1.5%  | >= 2  |
| ETH  | >= 1.5%    | 排除 1-2% | >= 2  |
| BNB  | 0.3-1.0%   | —       | >= 2  |
| SOL  | >= 1.5%    | —       | >= 2  |

> **注**：r_dist 和 TP1 在 VLM 响应回来后、写入 signal_active 前校验。不符合过滤条件的直接丢弃。

---

## 四、进程 B：实时交易执行监控

### 触发时机
每根 **15m K 线收线后立即触发**（与进程 A 同步但独立运行）。

### 流程

```
[15m K线收线]
       ↓
拉取 btc-ml 最新 15m K线 + flow 数据
       ↓
扫描 signal_active/ 下所有 state=WAITING 的信号
       ↓
对每个信号下的每个 playbook 跑 scorer 逻辑：
  ┌─────────────────────────────────────┐
  │ State: WAITING_FOR_PRIMARY_TOUCH    │
  │   high >= primary_touch.level       │
  │   OR low <= primary_touch.level     │
  │   → 进入 WAITING_FOR_ACTIVATION     │
  └─────────────────────────────────────┘
       ↓
  ┌─────────────────────────────────────┐
  │ State: WAITING_FOR_ACTIVATION       │
  │   close 穿越 activates_if_close_crosses │
  │   → ACTIVATED → 入场                │
  │   OR close 穿越 cancels_if          │
  │   → CANCELLED → 该 playbook 结束   │
  │   b2act < 2 → 跳过（运营约束）     │
  └─────────────────────────────────────┘
       ↓
    激活？
  No → 继续监控
  Yes ↓
【入场】
  DeepCoin 开新分仓
  市价/限价入场
  同时挂单：
    止损单  @ invalidation_level（全仓）
    TP1单   @ objectives[0]（半仓，平半）
    TP2单   @ objectives[1]（另半仓）
  state → ACTIVATED
       ↓
    监控 TP1
  TP1 触达 ↓
  修改止损单价格 → activation_price（保本价 BE）
  state → TP1_HIT
       ↓
    监控 TP2 / BE止损
  TP2 触达 → 全部平仓，state → DONE
  BE止损触达 → 平剩余半仓，state → DONE
```

### 全剧本处理（同一信号多个 playbook）

每个激活的 playbook = 1 个独立分仓，互不干扰：
```
同一信号下：
  playbook_1 激活 → 分仓 #1
  playbook_2 激活 → 分仓 #2
  playbook_3 未激活 → 无操作
```

DeepCoin 超级分仓支持同一币种多个独立仓位，各自有独立止损/止盈。

---

## 五、资金配置

| 项目 | 数值 |
|------|------|
| 总资金 | 1000u |
| 每笔风险 | 10u（1R = 10u）|
| 每笔 margin | 40u |
| 峰值并发 | ~7 笔 = 280u |
| 日常占用 | 120-200u |

杠杆倍数 = 40u / r_dist_pct × 1u 面值（按各币 r_dist 动态计算）

---

## 六、信号生命周期状态机

```
WAITING
  → WAITING_FOR_PRIMARY_TOUCH（primary touch 未触）
  → WAITING_FOR_ACTIVATION（primary touch 已触）
  → CANCELLED（activation cancelled）
  → ACTIVATED（已入场）
  → TP1_HIT（TP1 已触，SL 移至 BE）
  → DONE（TP2 触 / BE止损 / SL止损）
```

每个 playbook 独立维护自己的状态，写在 state.json 里。

---

## 七、目录结构

```
coin_trader_platform/
├── signal_pending/      ← 进程 A 写入，openclaw 监听
├── signal_active/       ← openclaw 填完 vlm_response 后移入，进程 B 读取
├── signal_done/         ← 结束的信号归档
└── live/
    ├── monitor.py       ← 进程 A：信号监控
    ├── executor.py      ← 进程 B：交易执行
    └── deepcoin_api.py  ← DeepCoin API 封装
```

---

## 八、openclaw 监听逻辑

openclaw 监听 `signal_pending/` 目录，检测到新目录出现后：
1. 读取 `prompt.txt` + 两张图
2. 打开 ChatGPT bot，发送
3. 等待并抓取响应
4. 解析 JSON，写入 `vlm_response.json`
5. 将整个目录移动到 `signal_active/`

> 可以用文件锁或 `.ready` 标记文件表示 signal_pending 写入完成，避免 openclaw 读取不完整的文件。
