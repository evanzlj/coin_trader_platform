# Live 部署上下文 — 给新 Session 读

> 写于 2026-06-09，由上一个 Claude session 整理。新 session 读完这份文档就能直接接手。

---

## 项目位置

```
/Users/evanzhang/Documents/repo/coin_strategy/coin_trader_platform/
```

CLAUDE.md 在项目根目录和 coin_strategy/ 根目录，进去先读。

---

## 当前系统架构（已完成的部分）

### 信号生成链路（已跑通）

```
live/warmup_replay.py   — 一次性跑 2020→今 ReplayFeed，持久化 buffer state + dedup state
live/monitor.py         — 常驻进程，每 30s fetch_delta + mini ReplayFeed 检测新信号
live/fetch_delta.py     — SSH 到 evan@btc-ml，rsync 增量 OHLCV + taker_flow
signal_pending/         — 信号包落地目录（monitor.py 写入）
```

每个信号包的内容：
```
signal_pending/{sym}_{grade}_{ts}/
  signal.json       — 信号元数据
  prompt.txt        — VLM system+user prompt（=== SYSTEM === / === USER TEXT === 分隔）
  *_4h.png          — 4H K线图
  *_15m.png         — 15m K线图
  .ready            — 写完后 touch，供下游 poll
```

包名格式：`btcusdt_A_20260608_2100`、`ethusdt_A_20260608_2100`（`{slug}_{grade}_{YYYYMMDD_HHMM}`）

### buffer state 持久化（本 session 新做的）

```
live/state/dedup_state.json    — dedup 时间戳（+ buffer_saved_at 字段）
live/state/buffer/             — BarBuffer parquet 文件（warmup_replay 跑完后生成）
```

`warmup_replay.py` 跑完后会写 `buffer/` 目录。monitor 重启时：
- 有 buffer/ → 增量热身（saved_at → now），秒级完成
- 无 buffer/ → 56周 fallback（monitor 首次部署前还没跑 warmup_replay 时）

**注意**：当前 Windows 机器上还没有跑过新版 warmup_replay.py（dedup_state.json 里没有 buffer_saved_at 字段），Windows 部署时要先跑一遍。

---

## 这周的目标：A-only 上线（交易暂缓）

### 优先级

1. **Windows 上跑通 monitor.py** — 信号检测 → signal_pending/
2. **Windows 上跑通 live openclaw** — watch signal_pending/，调 ChatGPT，写 vlm_response.json
3. 交易执行层：**后置，本周不做**

### A-only 过滤规则（post-VLM，执行时用）

| 币种 | 规则 |
|------|------|
| BTC  | r_dist ≥ 0.5% + TP1 zone < 1.5% + b2act ≥ 2 |
| ETH  | r_dist ≥ 1.5% + 排除 TP1 zone 1–2% + b2act ≥ 2 |
| BNB  | 0.3% ≤ r_dist ≤ 1.0% + b2act ≥ 2 |
| SOL  | r_dist ≥ 1.5% + b2act ≥ 2 |

r_dist = `signal.json` 里的 `structure_space`，b2act = vlm_response 里激活状态的 playbook 数量。

---

## 下一步要建的：live_openclaw.py

现有的 `run_v7.py` 是批量处理 `replay_materials/` 的历史回放版本，**不要动它**。

live openclaw 要新建 `live/live_openclaw.py`，主要区别：

| | run_v7.py（历史回放） | live_openclaw.py（要建的） |
|---|---|---|
| 扫描目录 | `replay_materials/` | `signal_pending/` |
| 触发 | 启动时全量扫 | 轮询 `.ready` 文件 + 无 `vlm_response.json` |
| 包名前缀 | `btc_Aplus_...` | `btcusdt_A_...` |
| 品种判断 | `name.split("_")[0]` → `btc` | `name.split("usdt")[0]` → `btc` |
| 幂等跳过 | 已有 vlm_response.json → skip | 同 |
| 其他 | — | 完全复用 run_v7.py 的逻辑 |

VLM 调用方式：ChatGPT 浏览器自动化（Playwright + CDP），和 run_v7.py 完全一样。

---

## Windows 部署启动顺序

```bash
# 1. 先拉最新数据（在 coin_trader_platform/ 目录下）
python3 history_data_manager/fetch_history.py

# 2. 跑一次 warmup replay（建立 buffer state，只跑一次）
python3 live/warmup_replay.py

# 3. 启动 monitor（常驻）
python3 live/monitor.py

# 4. 启动 live openclaw（常驻，Chrome 需提前开好 CDP）
python3 live/live_openclaw.py
```

---

## 关键文件速查

```
live/monitor.py              — 信号检测主进程
live/warmup_replay.py        — 一次性热身（跑完再启 monitor）
live/fetch_delta.py          — 增量数据拉取
live/watchdog.py             — 进程守护（确认是否需要在 Windows 上调整）
signal_generator/generator.py — SignalGenerator，含 save/load_buffer_state
realtime_data_pull/feed.py   — ReplayFeed / RealtimeFeed
prompt_generator/            — A/A+ prompt 构建
draw_kline/                  — K 线图生成
run_v7.py                    — ChatGPT 浏览器自动化（历史回放用，参考实现）
OPENCLAW_SPEC.md             — openclaw 详细规范
```

---

## A+ 未来接入 Checklist

A+ 接入前提：2020-2026 全周期回放评分完成，执行规则确定。

### 不需要动的（已提前设计好）

- `warmup_replay.py` — 已从 2020-01-01 开始，dedup 覆盖所有 A+ 历史信号，A+ 接入时**不需要重跑**
- `live/monitor.py` 信号检测逻辑 — 不变，A+ 信号一直在被检测，只是被 grade filter 拦住
- `signal_pending/` 接口格式 — A+ 包和 A 包格式完全一样，下游不需要改

### 需要改的

**1. `live/monitor.py` — 删除 grade filter（3 行）**
```python
# 删掉这三行：
if sig.grade == "A+":
    logger.info("A+ signal held (not live): %s %s", sig.symbol, sig.bar_time)
    continue
```

**2. `live/live_openclaw.py` — 加 signal 时效检查**

signal_pending/ 里会有积压的旧 A+ 包（grade filter 期间积累的），它们对应的信号机会早已过期。live_openclaw 处理每个包时，读 signal.json 的 bar_time，超过一定时间（建议 2h）直接跳过，不发 VLM：
```python
signal = json.loads((pkg_dir / "signal.json").read_text())
bar_time = pd.Timestamp(signal["bar_time"])
if (pd.Timestamp.utcnow() - bar_time).total_seconds() > 7200:
    logger.info("stale package skipped: %s", pkg_dir.name)
    continue
```
这个检查对 A 和 A+ 都适用，可以在接入 A+ 的同时一并加入。

**3. `live/live_openclaw.py` — A+ vlm_response 结构不同**

A+ 的 vlm_response.json 第一个字段是 `a_plus_impulse_assessment`（impulse phase + flow hypothesis），A-only 没有这个字段。如果 live_openclaw 后续要做 post-VLM 过滤，需要按 grade 分支处理。

**4. post-VLM 过滤规则（A+ 专用，执行规则待定）**

A-only 的过滤是 r_dist + TP1 zone + b2act。A+ 的过滤规则目前还没有从 2020-2026 全周期回放中确定，接入前需要先跑完：
```
generate_aplus_historical.py（2020→2026 素材包）
→ openclaw 批量 VLM（run_v7.py on replay_materials/）
→ scoring/aplus_scorer.py
→ 分析结论 → 确定 A+ 执行规则
```

**5. 账户池并发上限**

A-only 和 A+ 共用 10 个子账户（5 Binance + 5 OKX，各 100u），接入 A+ 后并发峰值需重新评估，避免两套信号同时触发撑爆账户池。

---

## 注意事项

- **A+ 信号在 monitor.py 里已被过滤**（grade filter 在 polling loop 最前面），signal_pending/ 里只有 A 信号。A+ 接入时删掉那三行即可。不在 live_openclaw 里过滤的原因：signal_pending/ 里积压的旧 A+ 包对应的信号机会早已过期，接入时处理它们没意义
- 品种参数：BTC/BNB 用 standard，ETH/SOL 用 conservative（已在 signal_generator/params.py 固化）
- dedup window = 8h（32×15m bar），monitor 重启后 dedup 从持久化状态恢复，不会重复发信号
- monitor.py 里的 `_passes_filter()` 已有 r_dist 前置过滤，b2act+TP1 过滤在 VLM 之后做
