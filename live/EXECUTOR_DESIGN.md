# Executor 设计方案（Process C）v2

> 写于 2026-06-10，根据你的逐条批注重写。取代 `history_data_manager/live_trading_flow.md`
> 里 **DeepCoin 超级分仓** 那套（已废弃）。现行：**10 个独立账户（5 Binance + 5 OKX）
> + 各交易所官方 SDK + hedge（long/short）持仓模式 + 逐仓**。
>
> 本版已落实的决策（见 §16 决策台账）：
> - 并发上限 **20 笔**（10 账户 × 每账户最多 2 笔）
> - 止损统一用 **touch-based 真实 stop 单**；**BTC 的 SL 在 VLM invalidation 上外扩 0.1% buffer**
> - notional 按**原 r_dist** 算（BTC 真实最大亏损约 1.2R，接受）
> - TP1 后剩余半仓保护线 = **BE（入场价）**
> - executor 主循环 **15s 轮询**（订单状态查询 + 新 bar 开仓判断同一循环）
> - 不上数据库；CSV 改**原子写**（前置修复）；slot **随机分配**
> - OKX 数量按 instruments 接口的 **ctVal / lotSz / minSz 实时计算**，不默认取整到整张

---

## 0. TL;DR

```
signal_active/{pkg}/state.json   ← openclaw 原子 rename 进来，executor 读
        │  executor 每 15s 轮询：
        │    (a) 查活跃持仓的交易所订单状态 → 推进 ACTIVATED/TP1_HIT
        │    (b) 有新 15m 收线 bar → 对 WAITING 的 playbook 判 primary/activation → 开仓
        ▼
  全部 playbook 进终态 → 归档 signal_done/
```

| 项目 | 值 |
|------|-----|
| 账户数 | 10（Binance 5 + OKX 5） |
| 每账户本金 | 120 USDT |
| 持仓模式 | hedge（long/short）+ 逐仓 isolated |
| 1R | 10 USDT |
| margin（BTC/ETH/SOL） | 40 USDT |
| margin（BNB） | 70 USDT |
| 每账户最多并行 | 2 笔（受 margin + 同 symbol 约束，见 §9） |
| 并发上限 | 20 笔 |
| 止损 | touch-based 真实 stop 单；BTC 外扩 0.1% buffer |
| TP1 后保护 | BE（入场价） |
| 主循环 | 15s 轮询 |

---

## 1. 链路位置

```
[A] monitor.py        信号检测 → signal_pending/
[B] live_openclaw.py  VLM + post-VLM 过滤 → signal_active/（写 state.json）
[C] executor.py       状态机 + 真实下单 → signal_done/   ← 本文档
```

三进程只通过文件系统解耦。executor 只认 `signal_active/*/state.json`，它是唯一真实来源。

---

## 2. 输入契约：state.json

openclaw 的 `_build_state()`（`live_openclaw.py:487`）已生成，executor 直接消费：

```jsonc
{
  "signal_dir":     "btcusdt_A_20260608_2100",
  "symbol":         "BTC/USDT",
  "grade":          "A",
  "bar_time":       "2026-06-08T21:00:00+00:00",   // T0
  "structure_side": "near_resistance",
  "overall_status": "WATCHING",
  "playbooks": [{
    "hypothesis":   "DOWNSIDE_ACCEPTANCE_CONTINUATION",
    "direction":    "short",                        // direction_if_activated
    "status":       "WAITING_FOR_PRIMARY_TOUCH",
    "primary_touch":{"level": 73000, "side": "low"},
    "activates_if": {"level": 72800, "dir": "below"},
    "cancels_if":   {"level": 74000, "dir": "above"},
    "invalidation": {"level": 73000, "dir": "above"},
    "tp1_level":    72400,
    "tp2_level":    72000,
    "r_dist_pct":   0.3008,
    ...
  }]
}
```

每个 playbook 的字段**完全以 VLM response 为准**。不同 playbook 的激活/取消条件由 VLM 给出，
**不假设它们之间有任何固定关系**（不假设互为 cancel）。executor 对每个 playbook 独立按其
自己的 activation_rule 跑状态机。

executor 运行时会在每个 playbook 上追加 `exec` 子对象（§9.4）。

### 2.1 一个信号几个 playbook

VLM 通常给 3-4 个。`activation_rule=null` 的（如 CHOP_WAIT）已被 openclaw 过滤掉，不进
state.json。剩下的每个**独立监控、独立激活、独立分仓**（研究的全剧本模式，验证优于先触发先激活）。
**slot 在「激活进场」时刻才占用，监控阶段不占。**

---

## 3. 数据与文件竞态（前置修复）

### 3.1 state.json：无竞态 ✓

- openclaw 在 `signal_pending/{pkg}/` 内写完 state.json，再 `rename` 整个目录到
  `signal_active/`（`live_openclaw.py:562`）。同盘 rename 是原子操作 → executor 扫到目录时
  state.json 必然完整。
- executor 更新 state.json 用「写 `state.json.tmp` → `os.replace()`」原子替换。executor 是
  `signal_active/` 内 state.json 的**唯一 writer**，无需文件锁。
- ⚠️ **双机部署后（§20）**：openclaw 在中国写 state.json、executor 在 btc-ml 读 —— 跨机器后同机
  原子 rename 失效，改为「传完落 `.ready` 标记」保证 executor 不读半包（见 §20.2）。

### 3.2 CSV：有竞态，需修复 ⚠️

`fetch_delta.py` 当前：
- 增量追加用 `open(csv, "a")` 逐行写（`:216`）——非原子；
- gap fill 用 `to_csv()` 整体覆盖（`:309`）——非原子（先 truncate）。

executor / monitor 用 `pd.read_csv` 并发读时，可能读到半行或被 truncate 的文件。

**修复方案（不上数据库）**：把 fetch_delta 的写入改为「读入内存 → 追加/合并 → 写 `.tmp` →
`os.replace()`」原子替换。

- 为什么不上数据库：SQLite 多进程写有锁竞争；Windows 文件锁跨平台是坑；要重写
  fetch_delta / monitor / executor / ReplayFeed 全部数据层，对「4 币 / 15m / 单机 / 低频」
  规模得不偿失。竞态点本质只是「写不原子」，os.replace 即可根治（Windows 上 os.replace 也是
  原子覆盖）。
- 数据量小（4 币 × {15m,4h} + flow，每文件几万行），每 15~30s 全量重写 to_csv 仅数十 ms，可接受。
- executor 读侧再加一道防御：read_csv 失败或最后一行字段数不符 → sleep 0.2s 重读一次。

> ⚠️ 这是 executor 上线前的**前置修复项**，改的是 `fetch_delta.py`，本轮不动，等方案确认后单独做。

---

## 4. Executor 主循环（单循环 15s）

订单状态查询走交易所 REST，不依赖本地 CSV；开仓判断依赖新 15m 收线 bar。两件事放同一个
15s 循环里：

```python
last_processed_bar = load_cursor()

while True:
    # (a) 管理已开仓：查订单状态，推进 ACTIVATED / TP1_HIT —— 每轮都做（≤15s 及时移 SL→BE）
    for pkg in list_signal_active():
        state = load_state(pkg)
        for pb in state["playbooks"]:
            if pb["status"] in ("ACTIVATED", "TP1_HIT"):
                manage_open_position(state, pb)      # §6.5
        save_if_changed(pkg, state)

    # (b) 开仓判断：仅当出现新的 15m 收线 bar
    latest = get_latest_closed_bar_time()            # btcusdt_15m.csv 末行（带半行防御）
    if latest and latest > last_processed_bar:
        bars = load_new_bars_per_symbol(last_processed_bar, latest)  # 可能多根，逐根喂
        for pkg in list_signal_active():
            state = load_state(pkg)
            for pb in state["playbooks"]:
                if pb["status"] in ("WAITING_FOR_PRIMARY_TOUCH", "WAITING_FOR_ACTIVATION"):
                    for bar in bars[state["symbol"]]:
                        step_waiting(state, pb, bar)  # §4 状态机
            save_if_changed(pkg, state)
        last_processed_bar = latest
        save_cursor(latest)

    # (c) 归档 + 心跳
    for pkg in list_signal_active():
        if all_terminal(pkg): archive_to_done(pkg)
    update_heartbeat()
    sleep(POLL_SECONDS)        # 干完一轮活再 sleep（非定时器），天然不堆积请求
```

> **防请求堆积（中国 Windows + 美国代理下尤其重要）**：主循环是「干完一轮、再 sleep」的结构，
> **不是固定间隔定时器** —— 上一轮没跑完绝不叠加发起新一轮（单实例 + 重入保护）。每轮用**批量接口**
> （每账户一次查全部 open orders / positions，而非每单一次请求）压低请求数；每个请求设超时 + 退避重试。
> `POLL_SECONDS` 是默认值，须按真实往返延迟实测单轮耗时来定（§18 P1-9）。

- `for bar in bars[...]`：重启/补数据时一次可能补多根，**必须逐根顺序喂**，否则错过中间 bar
  的 primary touch / activation（与 scorer 逐 bar 迭代一致）。
- 开仓延迟 ≤15s，SL→BE 延迟 ≤15s，比「每分钟」更积极。

---

## 5. 状态机（与 scorer.py 对齐）

### 5.1 状态

```
WAITING_FOR_PRIMARY_TOUCH   等 primary_touch 触及
WAITING_FOR_ACTIVATION      primary 已触，等 activation / cancel
ACTIVATED                   已入场满仓，三单已挂（TP1/TP2/SL）
TP1_HIT                     TP1 成交半仓，SL 已移到 BE
── 终态 ──
DONE_CANCELLED   activation 前被 cancel，或 b2act<2 跳过（未入场）
DONE_SL          入场后触 SL（约 -1R；BTC 含 buffer 约 -1.2R）
DONE_BE          TP1 后触 BE（剩余半仓保本出）
DONE_TP2         TP1 + TP2 全达成
```

### 5.2 WAITING_FOR_PRIMARY_TOUCH（15m 收线判断）

| scorer | executor |
|--------|----------|
| `side="low" → bar.low<=level`；`side="high" → bar.high>=level` | 同。触及 → 记 `primary_open_time`，转 WAITING_FOR_ACTIVATION |

A-only 无激活窗口上限，primary touch 可等任意久。

### 5.3 WAITING_FOR_ACTIVATION（15m 收线判断）

| scorer | executor |
|--------|----------|
| `cancelled = close 穿 cancels_if`（先判）<br>`activated = close 穿 activates_if` | 同序：先 cancel 后 activate |
| cancelled | → DONE_CANCELLED |
| activated | 算 b2act（§5.5），≥2 → 开仓（§6）；<2 → DONE_CANCELLED(`skipped_b2act`) |

`close 穿越`：above → `close>level`；below → `close<level`（严格不等号，与 scorer 一致）。

入场用**激活那根 15m 收线后的市价单**。研究成本模型已含滑点，入场价≈收线价，不另作处理。

### 5.4 ACTIVATED / TP1_HIT

入场后 SL/TP1/TP2 都是**交易所挂单**，盘中自动触发。这两个状态的推进靠 §6.5 的订单状态查询
（每 15s），不靠 K 线判断。详见 §6。

### 5.5 b2act（运营硬约束）

scorer 定义（`scorer.py:223`）：

```
b2act = activated_bar_idx - primary_bar_idx          # 约束：>= 2
```

即激活那根 bar 与 primary touch 那根 bar 的序号差 ≥ 2。executor 用 open_time 计算最稳（不依赖
内存计数器，重启也准）：

```
b2act = (activated_open_time - primary_open_time) / 15min
```

`b2act < 2` → 不进场，DONE_CANCELLED(`skipped_b2act`)。照研究口径执行，不引申。

---

## 6. 下单逻辑（核心）

### 6.1 入场 + 一次性挂三单

activation 收盘确认后，**开仓并一次性把 TP1 / TP2 / SL 三张 reduce-only 单挂到交易所**，把执行
尽量交给交易所，executor 只需轮询状态做联动（§6.5）。

```
1. set_leverage(symbol, posSide, lev)            # §7；hedge 模式
2. market_open(symbol, posSide, qty)             # 市价入场，记 entry_price / filled qty
3. place TP1 limit  reduce-only  qty/2 @ tp1_level
4. place TP2 limit  reduce-only  qty/2 @ tp2_level
5. place SL  stop-market reduce-only  qty @ sl_price   # §6.2
6. 占 slot，写 state.exec
```

**两家都支持对同一仓位同时挂多个离场单**（已查证）：
- Binance USDⓈ-M：2× `LIMIT` 做 TP1/TP2 + 1× `STOP_MARKET` 做 SL。
  ⚠️ **条件单已于 2025-12-09 迁移到 Algo Service**，adapter 必须用新的 Algo Order 接口，
  不是旧的 `/fapi/v1/order` 带 stopPrice。hedge 模式按 §8.4 用 positionSide+side，不传 reduceOnly。
- OKX Swap：2× limit 做 TP + 1× algo `conditional`(slTriggerPx, slOrdPx=-1 市价) 做 SL；
  long/short 模式带 `posSide`。OKX `attachAlgoOrds` 每组通常只带一对 TP/SL，**两档 TP 建议下独立
  algo orders**（需测试网实测确认，见 §18）。

> **关键结论**：我们**不依赖交易所的自动 OCO/bracket**。标准 OCO 只能表达「一个 TP + 一个 SL
> 两两互撤」，无法表达我们的「分批 TP1/TP2 + TP1 后移动 SL 到 BE」。所以三单之间的联动
> （任一成交后撤其余、TP1 后改 SL）全部由 executor 每 15s 轮询主动管（§6.5）。这反而让两家
> 实现更统一、更可控。

### 6.2 止损：touch-based 真实 stop 单

统一用挂在交易所的 **STOP_MARKET / conditional**，盘中价格触及即成交（不等收线）。

```
sl_price:
  ETH/BNB/SOL: = invalidation_level
  BTC:         = invalidation_level 外扩 0.1% buffer（远离持仓方向 0.1%）
       short(near_resistance) → sl = invalidation_level × (1 + 0.001)
       long (near_support)    → sl = invalidation_level × (1 - 0.001)
```

BTC buffer 为躲 wick 插针。**notional 仍按原 r_dist 算**（§7），所以 BTC 触 SL 的真实亏损 >1R：
median ≈ 1.11R（≈11.1u），最窄 r_dist 时 ≈ 1.19R（≈11.9u），封顶 1.2R。BTC 止损单约 27 笔/5月，
多亏部分微乎其微，已接受。

> **研究依据（已用 touch-based scorer 重跑验证）**：BTC 止损单 67% 是「wick 扫损但收盘收回」，
> 故需缓解。对比过三套：纯 touch（+0.029R）、touch+0.1% buffer（+0.068R）、收盘确认+1.5R 兜底
> （+0.071R）。后两者效果几乎一样，但「收盘确认+兜底」四币验证发现**只对 BTC 有利，对 ETH/BNB/SOL
> 反而有害**（它们 touch 本就健康，收盘确认让真破位单跑更远多亏，期望腰斩甚至归零）。故最终选
> **统一 touch-based + 仅 BTC 加 0.1% buffer**：一个参数解决，不必分币种写两套逻辑，也不需要常驻
> 进程逐 bar 收盘检查。touch-based 口径下四币 5 个月：BTC +5.7R / ETH +13.7R / BNB +12.5R /
> SOL +1.7R。

**触发价类型（已查证 + 决策）**：Binance `workingType` / OKX `slTriggerPxType` 统一用
**last price（Binance `CONTRACT_PRICE`，OKX `last`）**，不用 mark price。理由：我们的
invalidation / TP level 全部基于币安 K 线**成交价**算出，触发口径必须与之一致；mark price 是
标记价，与 level 口径错配。代价是 last price 更易被插针扫 —— 这正是 BTC 加 0.1% buffer 的原因，
逻辑自洽。TP1/TP2 限价单本身按成交价撮合，天然是 last 口径。

### 6.3 止盈：真实 reduce-only 限价单

TP1/TP2 限价单（maker），盘中触及成交，与 scorer 的 high/low 触及口径一致。TP1=qty/2 @ tp1，
TP2=qty/2 @ tp2。

### 6.4 BE（保本损）

= **入场价 entry_price**，不计手续费/资金费（你的定义：10000 进、10000 出即可）。TP1 成交后
SL 移到这里。

### 6.5 manage_open_position（每 15s，靠订单状态查询联动）

```
ACTIVATED:
  查 SL / TP1 / TP2 三单状态
  ├─ SL filled（全仓平）      → 撤 TP1+TP2 → DONE_SL，释放 slot
  ├─ TP1 filled（半仓平）     → 撤原 SL；挂新 SL(stop-market, 剩余半仓 @ BE=entry_price)
  │                            → TP1_HIT（TP2 保持挂着）
  └─ TP2 先 filled（罕见跳空）→ 撤 SL+TP1（若 TP1 也成交则全平）→ 视实际持仓判 DONE_TP2

TP1_HIT:
  查 TP2 / BE-SL 状态
  ├─ TP2 filled → 撤 BE-SL → DONE_TP2，释放 slot
  └─ BE-SL filled → 撤 TP2 → DONE_BE，释放 slot
```

原则：**任一单成交后，先撤其余单再做下一步**，避免残留单在无仓时挂着或反向。撤单后复查持仓
确认数量归零。

> TP1 成交到移 SL 最多延迟 15s。这 15s 内剩余半仓仍受原始 SL（在 invalidation，比 BE 远）保护，
> 风险可控。

---

## 7. 仓位与杠杆

```
r_dist_pct = state.r_dist_pct                       # abs(act-inv)/act×100，不含 BTC buffer
notional   = 1000 / r_dist_pct                      # USDT，1R=10u（统一按原 r_dist）
qty_coin   = notional / entry_price                 # 实际成交价折算
margin     = SYMBOL_MARGIN[symbol]                  # 40 / 70
leverage   = min(ceil(notional / margin), SYMBOL_MAX_LEV[symbol])
```

| 币种 | margin | max_lev | r_dist 下限 | 最坏 notional | 需要 margin | 实际 lev |
|------|--------|---------|------------|--------------|------------|----------|
| BTC  | 40 | 50 | 0.5% | 2000 | 40 ✓ | ≤50 |
| ETH  | 40 | 20 | 1.5% | 667  | 34 ✓ | ≤17 |
| BNB  | 70 | 50 | 0.3% | 3333 | 67 ✓ | ≤48 |
| SOL  | 40 | 20 | 1.5% | 667  | 34 ✓ | ≤17 |

默认 margin 在各币最坏 r_dist 下都够。每笔按实际 r_dist 动态 `set_leverage`。

- entry 市价滑点 + OKX 张数取整会让实际 R 偏离 10u，日志记录 `actual_r_usdt`。
- BTC 因 SL 含 0.1% buffer，触 SL 实际亏损 ≈ 1.2R（已决策接受）。

---

## 8. 交易所差异（Binance vs OKX）

用官方 SDK，但**必须有一层 broker adapter 抹平差异**，executor 只调统一接口。

### 8.1 统一接口

```python
class Broker(Protocol):
    def set_leverage(symbol, pos_side, leverage) -> None
    def get_available_balance() -> float
    def get_position(symbol, pos_side) -> Position | None       # 对账
    def market_open(symbol, pos_side, qty, client_id) -> Fill
    def market_close(symbol, pos_side, qty, client_id) -> Fill
    def place_reduce_limit(symbol, pos_side, qty, price, client_id) -> OrderId
    def place_stop_market(symbol, pos_side, qty, stop_price, client_id) -> OrderId
    def cancel_order(symbol, order_id) -> None
    def get_order(symbol, order_id) -> OrderStatus
    def round_qty(symbol, qty) -> float        # 精度，§8.3
    def round_price(symbol, price) -> float
```

### 8.2 关键差异

| 维度 | Binance USDⓈ-M | OKX Swap |
|------|----------------|----------|
| SDK | binance-futures-connector | python-okx |
| symbol | BTCUSDT | BTC-USDT-SWAP |
| **数量单位** | 币数量 quantity | **张数 sz**（sz×ctVal=币量）⚠️ |
| 持仓模式 | hedge：positionSide=LONG/SHORT | long/short：posSide=long/short |
| 市价 | MARKET, quantity | ordType=market, sz |
| 限价 | LIMIT GTC, price, quantity | ordType=limit, px, sz |
| 止损 | STOP_MARKET, stopPrice, reduceOnly | algo conditional, slTriggerPx, slOrdPx=-1 |
| 杠杆 | /fapi/v1/leverage | /api/v5/account/set-leverage(mgnMode=isolated) |
| 逐仓 | marginType=ISOLATED | 下单 tdMode=isolated |
| 精度 | LOT_SIZE.stepSize / PRICE_FILTER.tickSize | lotSz / tickSz |
| 最小量 | MIN_NOTIONAL(~5u) | minSz / ctVal |
| 客户端单号 | newClientOrderId | clOrdId |

### 8.3 OKX 数量：动态读规格，不默认取整到整张

启动时拉 OKX `GET /api/v5/public/instruments` 读每个 instId 的 `ctVal / lotSz / minSz`，
按真实精度算 sz：

```
sz_raw   = qty_coin / ctVal
sz       = floor(sz_raw / lotSz) * lotSz        # 按真实 lotSz 取整，不是整数张
actual_qty = sz * ctVal
deviation  = (actual_qty - qty_coin) / qty_coin
```

绝大多数合约 lotSz 足够细，deviation 很小。**若某品种在某交易所实测 |deviation| 超阈值（默认 ±5%），
则该 (品种, 交易所) 组合不满足分配约束 C4**（§9.2）——分配时把该交易所的账户从候选**排除**，在剩余
满足约束的账户中**随机选**（不是加权偏好，仍是纯随机，只是候选集变小）。Binance 用 stepSize 取整，
同理。每笔记录 `actual_r_usdt`。

### 8.4 持仓模式 + 平仓语义（已查证）

你的 Binance 子账户和 OKX 都用 **long/short（hedge / 双向）模式**。

⚠️ **hedge 模式下不能用 `reduceOnly`**（Binance 双向模式会拒单）。开/平仓完全靠
`positionSide` + `side` 表达：

| 动作 | positionSide | side |
|------|-------------|------|
| 开多 | LONG  | BUY  |
| 平多 | LONG  | SELL |
| 开空 | SHORT | SELL |
| 平空 | SHORT | BUY  |

TP1/TP2/SL 这三张「减仓单」也按此规则：与持仓**同 positionSide、相反 side，不传 reduceOnly**。
OKX long/short 模式同理（平仓 posSide 不变、side 相反）。

> 本文档其余处写的「reduce-only / 减仓单」均指**此语义**（同 posSide + 反向 side 的离场单），
> 不是字面的 `reduceOnly` 参数。

- 同账户同 symbol 的 long 与 short 本是两个独立 position，但 §9 约束 C2 已禁止同账户放同 symbol。

---

## 9. 账户池（Slot）

### 9.1 模型

```
10 账户 × 每账户最多 2 笔 = 20 逻辑 slot 上限
每账户 120u，hedge + 逐仓
```

### 9.2 分配约束（新笔激活进场时，全满足才可入）

```
C1  账户剩余可用 margin ≥ 本笔 margin        # 已占 margin + 本笔 ≤ 120
C2  账户内无相同 symbol 的活跃持仓           # 避免 hedge 合并 + Binance symbol 级杠杆冲突
C3  交易所与品种兼容                          # 目前无硬限制（BNB margin 70u 已兼容 OKX 50x）
C4  该(symbol,交易所)数量取整偏差 ≤ 阈值     # OKX |deviation|>阈值 → 排除该所账户（§8.3）
```

- C1 自动排除「2 笔 BNB 同账户」（70+70>120）；允许「BNB 70 + 其他 40 = 110 ≤120」。
- C2：Binance 同 symbol 的 long/short 共用 symbol 级杠杆，两笔 r_dist 不同会杠杆打架，故同账户
  干脆不放同 symbol（任何方向）。每账户 2 笔必须不同 symbol。
- **满足约束的账户中纯随机选**（不做加权/软路由）。§8.3 的取整偏差超阈值不是「偏好」而是约束 C4：
  把超阈值的交易所账户从候选**排除**，再在剩余满足约束的账户里随机 —— 候选集变小，但仍是纯随机。
- 无可用账户 → 放弃进场，`result="skipped_no_slot"`，记日志。研究峰值并发才 7，20 上限充裕。

### 9.3 Slot 状态恢复（崩溃重启）

不维护独立 slot 表。重启时从两处重建：
1. 扫 `signal_active/*/state.json` 中 `status∈{ACTIVATED,TP1_HIT}` 且带 `exec` 的 playbook →
   它们的 (account, symbol, margin) 即当前占用。
2. 与交易所实际持仓对账（§11.3）。

### 9.4 state.exec 字段

```jsonc
"exec": {
  "account":        "binance_2",
  "exchange":       "binance",
  "pos_side":       "SHORT",
  "entry_order_id": "...", "entry_price": 72780.5,
  "qty": 0.0274, "qty_remaining": 0.0274,
  "margin": 40, "leverage": 50,
  "tp1_order_id": "...", "tp2_order_id": "...", "sl_order_id": "...",
  "sl_price": 73073.0,              // BTC 含 buffer
  "actual_r_usdt": 12.1,
  "entry_at": "...", "tp1_filled_at": null,
  "client_id_base": "btcusdt2100_DOWNSIDE"
}
```

---

## 10. 错误处理

> 完整异常清单（8 类 × 方案 + 已拍板运行参数）见 **§21**；本节是下单层失败处理的细节。

### 10.1 幂等（防重复下单）

每 playbook 唯一 `client_id_base`，所有单的客户端单号派生：`{base}_E / _T1 / _T2 / _S / _C{n}`。
重复提交同单号被交易所拒（duplicate）→ 天然幂等。崩溃后查单号即知「到底下没下」，不会重复开仓。

### 10.2 失败处理

| 场景 | 处理 |
|------|------|
| 入场市价单失败 | 同单号重试 1 次；仍失败 → 不进场，`result="entry_failed"`，**不占 slot**，告警。绝不在无实仓时记 ACTIVATED |
| 入场部分成交 | 以实际 filled qty 重算 TP/SL 数量 |
| TP/SL 挂单失败 | 重试；TP 挂不上 → 降级为 executor 轮询 + 市价平（记录降级）；**SL 挂不上 → 立即市价平仓退出 + 飞书告警**（不允许无止损裸仓），通知人工可介入重挂（见 §10.3） |
| 撤单失败 | 重试 + 查单状态（可能撤时刚成交） |
| 查单超时 | 本轮跳过该 pb，下轮重试，不臆测 |
| 数据滞后/无新 bar | 最新 bar **超 20 分钟**没更新 → **停开新仓 + 告警**；已持仓不受影响（SL/TP 在所端） |
| API 限频 429 | 退避重试；adapter 内置最小请求间隔 |
| 时间不同步 -1021 | Binance 重新对齐服务器时间 |
| 进程崩溃 | 重启走 §9.3 + §11.3 对账 |

无滑点保护（市价滑点不可预知，靠未来平均值衡量）。

### 10.3 飞书告警 + 人工接管

预留**飞书 webhook 口子**（`FEISHU_WEBHOOK`，§12.2）。以下推飞书：SL 挂不上已强平、入场失败、
对账不一致、无 slot、数据滞后。

**人工接管机制**（避免 executor 与人工重复操作）：
- 人收到告警后可手动介入（重挂 SL / 手动平仓 / 接管该笔）。
- executor 每轮扫描时，若发现某 (账户, symbol) 上有**非自己下的订单/持仓变化**（client_id 不在它
  记录里），或 state.exec 标了 `manual_override=true` → **暂停对该 pb 的自动操作**，仅记录 + 心跳，
  等人工处理完显式清除标记再恢复。
- 人工接管入口：在该 pb 的 state.json 置 `manual_override=true`（人工或小工具写入），executor 读到即让位。
- 原则同 §11.3 对账不一致：**宁可让位、不抢操作**。

---

## 11. 持久化与崩溃恢复

- **原子写**：state.json / cursor 一律 tmp + os.replace。
- **无独立内存真相**：任意时刻杀进程，从 `signal_active/*/state.json` + 交易所持仓即可重建。
- **重启对账（reconcile，安全核心）**：对每个 `ACTIVATED/TP1_HIT` 的 pb，用 client_id 查交易所
  实际 position 与三单状态：
  - state=ACTIVATED 但无持仓 → 宕机期间可能已被 SL/TP 平 → 查成交历史定真实终态补记；
  - 有持仓但缺挂单 → 补挂；
  - state=TP1_HIT 但仍满仓 → 回退；
  - 对不上 → 告警，**该 pb 暂停自动操作，人工介入**（宁可保守不瞎动）。
- **重启边界（关键）**：WAITING 的旧信号包**全丢**；ACTIVATED / TP1_HIT 的持仓**一律对账恢复接管，
  绝不当旧包丢掉**（丢已激活持仓 = 孤儿仓 = 最严重事故）。
- **定期对账**：除启动对账外，运行时**每 15 分钟**全量查持仓 + 挂单 vs state，抓强平 / 漂移 / 漏检。
- **单实例锁**：启动用 pid 文件 / 端口锁，检测到已有实例则拒启，防两实例重复下单。

---

## 12. 配置

### 12.1 API key 文件（gitignored，你填）

按环境分**两个文件**，executor 按 `ENV` 加载对应文件，**绝不混用**。
**注意两个文件账号数量不同**（见 §19 测试网账号策略）：

`live/keys_live.json`（实盘 = 10 账户）：
```jsonc
{
  "binance": [{"label":"binance_0","api_key":"","secret":""}, ... ×5],
  "okx":     [{"label":"okx_0","api_key":"","secret":"","passphrase":""}, ... ×5]
}
```

`live/keys_testnet.json`（测试网 = 每所 1 个即够）：
```jsonc
{
  "binance": [{"label":"binance_tn", "api_key":"", "secret":""}],
  "okx":     [{"label":"okx_demo",   "api_key":"", "secret":"", "passphrase":""}]
}
```

- Binance testnet key 来自 `testnet.binancefuture.com`（独立注册的测试账号），只在 testnet
  base url 工作。
- OKX testnet 用 **Demo Trading** 创建的 demo key（在已有子账户内开通，无需新注册），调用时带 header
  `x-simulated-trading: 1`。
- **测试网各 1 个就够**：验 adapter 接口 + 状态机全分支；10 账户的 slot 调度逻辑用 mock 单元测试
  覆盖，不依赖真实多账户。10 个真实账户在「小额实盘」阶段才全上（§19）。
- executor 启动 `json.load`，代码中不出现 key。`.gitignore` 加 `live/keys_*.json`。

### 12.2 live/exec_config.py（进 git）

```python
ENV             = "testnet"    # "testnet" | "live"；切数据源 key + base url/header（§19）
SYMBOL_MARGIN   = {"BTC/USDT": 40, "ETH/USDT": 40, "BNB/USDT": 70, "SOL/USDT": 40}
SYMBOL_MAX_LEV  = {"BTC/USDT": 50, "ETH/USDT": 20, "BNB/USDT": 50, "SOL/USDT": 20}
ONE_R_USDT      = 10
ACCOUNTS        = 10
MAX_PER_ACCOUNT = 2
BTC_SL_BUFFER   = 0.001        # 0.1%，仅 BTC，SL 外扩
TP1_PROTECT     = "be"         # TP1 后剩余半仓 SL = 入场价
SL_MODE         = "touch"      # 真实 stop 单，盘中触发
TRIGGER_PX_TYPE = "last"       # 触发价口径：last(=Binance CONTRACT_PRICE/OKX last)，与信号 level 一致
POS_MODE        = "hedge"      # long/short；平仓靠 posSide+反向 side，不用 reduceOnly
MARGIN_MODE     = "isolated"
POLL_SECONDS    = 15           # 默认值，按真实环境（中国Win+美国代理往返延迟）实测单轮耗时调整（§18 P1-9）
OKX_R_DEV_THRESHOLD = 0.05     # |取整偏差|超此 → 该所账户对该品种不满足约束 C4（§8.3/§9.2）
FEISHU_WEBHOOK  = ""           # 告警/人工接管通知口子（§10.3）；从 env/keys 注入，不硬编码
```

---

## 13. 日志与监控

- `live/trade_log.jsonl`：append-only，每事件一行（ACTIVATED / TP1_FILLED / SL_HIT /
  TP2_FILLED / BE_EXIT / SKIPPED_* / ERROR_*），含 account/exchange/symbol/pos_side/价格/qty/
  actual_r_usdt/时间。
- `live/heartbeat/executor_last_run.txt`：每轮更新，watchdog 监控。
- 醒目告警 + **飞书推送**（`FEISHU_WEBHOOK`，§10.3）：入场失败、SL 挂不上已强平、对账不一致、
  无 slot、数据滞后、检测到人工接管。

---

## 14. 部署启动顺序（双机，详见 §20）

```bash
# —— 中国 Windows（有 UI：monitor + openclaw）——
python3 history_data_manager/fetch_history.py      # 一次性：拉数据
python3 live/warmup_replay.py                      # 一次性：buffer/dedup
python3 live/monitor.py                            # 常驻 A
python3 live/live_openclaw.py                      # 常驻 B（Chrome CDP 先开）

# —— btc-ml 新加坡（executor）——
#   先填好 btc-ml 本地的 keys_live.json / keys_testnet.json
python3 live/executor.py                           # 常驻 C（读本地 DB + 收 state.json）

# —— watchdog：各机守本机心跳 ——
python3 live/watchdog.py                           # 中国守 monitor/openclaw；btc-ml 守 executor
```

---

## 15. 与研究（scorer）的差异清单

**止损口径已统一（重要）**：研究 scorer 已从 close-confirm 改为 **touch-based**
（`replay/scorer.py`，invalidation 用 bar.high/low 触及判定），与实盘真实 stop 单口径一致。
原 `aonly_research_summary`（close-based，+56.2R）已被 touch-based 重跑取代：touch 口径下四币
5 个月 BTC +5.7R / ETH +13.7R / BNB +12.5R / SOL +1.7R，过滤规则仍成立，BTC 绩效已含 0.1% buffer。
**所以止损口径不再是实盘与研究的差异。** 重跑分析脚本：`scoring/aonly_filter_analysis.py`、
`btc_buffer_backtest.py`、`btc_wick_analysis.py`、`btc_buffer_cost.py`。

剩余差异：

> TP1 后保护用 **BE（入场价）**——研究 S3 本来就是 BE，实盘一致，**非差异**（不再扯 replay_report）。

| # | 差异 | scorer/研究 | 实盘 | 影响/态度 |
|---|------|------------|------|----------|
| 1 | 入场价 | 激活 K 线 close | 收线后市价（含滑点） | 研究已含滑点假设 |
| 2 | BTC 真实亏损 | 1R（含 buffer 回测） | >1R（median 1.11R，封顶 1.2R） | 已接受 |
| 3 | OKX 数量 | 连续币量 | 按 ctVal/lotSz/minSz 实算 | 偏差很小，超阈值则排除该所账户（C4） |
| 4 | 成本 | 0.22% + funding 0.01%/8h | 真实 fee + 滑点 + funding | 可能略高于假设 |

研究是 in-sample 5 个月小样本，实盘 = OOS。上线后用 `trade_log.jsonl` 持续比对止损率 / TP2 率 / 月度 R。

---

## 16. 决策台账（已确认 ✓）

1. ✓ 并发 20 笔（10 账户 × 2，受 §9 约束）
2. ✓ 止损 = touch-based 真实 stop 单（已用 touch scorer 四币重跑验证；「收盘确认+1.5R 兜底」因对 ETH/BNB/SOL 有害而否决）
3. ✓ BTC SL = invalidation 外扩 **0.1%** buffer（BTC 67% 止损是 wick 误扫，buffer 是验证过的对策）
4. ✓ notional 按**原 r_dist** 算（BTC 真实亏损 ≈1.2R，接受）
5. ✓ TP1 后保护线 = **BE（入场价）**
6. ✓ 主循环 **15s** 轮询（订单查询 + 开仓判断同循环）
7. ✓ 持仓模式 hedge（long/short）+ 逐仓
8. ✓ slot **随机分配**；OKX 按真实规格算量，超 ±5% 偏差才考虑路由
9. ✓ 不上数据库；CSV 改原子写（前置修复 fetch_delta）；state.json 无竞态
10. ✓ 无滑点保护

11. ✓ hedge 平仓靠 posSide+反向 side，**不用 reduceOnly**（已查证，§8.4）
12. ✓ 触发价口径 = **last price**（Binance CONTRACT_PRICE / OKX last），与信号 level 一致（§6.2）
13. ✓ Binance 条件单走 2025-12 迁移后的 **Algo Service** 新接口（已查证，§6.1）
14. ✓ 跨所价格基准：**接受偏差，以币安为准**，不锁交易所、不做价差补偿（§18 P2-1）
15. ✓ 测试网必做：三段 gate（测试网 → 小额实盘 → 满配），config 用 ENV 切换（§19）

### 待编码时核对

见 §18 完整调研清单。

---

## 17. 不做的事

- 不做 paper trading（直接小额实盘）。
- 不设信号过期（A-only 无激活窗口，监控到全 playbook 终态，与研究一致）。
- 不碰 A+（grade filter 拦截，执行规则未定）。
- 不加研究外的过滤；不自创入场/离场条件。
- 不做动态加减仓 / 移动止盈，严格 S3（TP1 半出移 BE、TP2 全出）。

---

## 18. 实现前待调研清单（提前暴露，别现场抓瞎）

标 ✓ 的已查证有结论；标 ⚠️ 的必须在**写 adapter 前用测试网/真实接口验证**，不能凭假设编码。

### P0 — 影响正确性与资金安全，必须先验证

| # | 项 | 现状 | 风险若不查 |
|---|----|------|-----------|
| P0-1 | hedge 平仓语义 | ✓ posSide+反向 side，不用 reduceOnly（§8.4） | 所有平仓/挂单被拒 |
| P0-2 | 触发价口径 last vs mark | ✓ 定 last（§6.2） | 止损口径与 level 错配 |
| P0-3 | **SL 是否一定先于强平触发** | ⚠️ 需按各币真实 MMR/档位复核 | 先爆仓后止损，亏损远超 1R |
| P0-4 | **最小下单量 / 最小名义** | ⚠️ 各币 minQty/minNotional(B ~5u)/OKX minSz；尤其 TP 分批后 qty/2 是否仍达标 | 分批单被拒，半仓平不掉 |
| P0-5 | **杠杆档位名义上限**（leverage bracket / tier） | ⚠️ 确认目标杠杆(50x/20x)对应的 maxNotional 覆盖我们 notional | set_leverage 被拒或仓位开不出 |

> **P0-3 粗算（待真实参数复核）**：逐仓强平距离 ≈ 1/leverage − MMR。
> 而 leverage = notional/margin，r_dist 越小 notional 越大杠杆越高 —— 但 r_dist 小时 SL 也越近，
> 系统自洽，SL 始终在强平内侧。例：BTC r_dist=0.5%→50x→强平≈1.6%，SL≈0.6%(含buffer) ✓；
> ETH/SOL ≤20x→强平≈4.5%，SL≥1.5% ✓；BNB 最坏 r_dist=0.3%→48x→强平≈1.4%，SL=0.3% ✓。
> 理论都安全，但 MMR/档位用各交易所真实值复核，留足滑点+funding 余量。

### P1 — 影响实现细节，写 adapter 前查文档/测试网

| # | 项 | 备注 |
|---|----|------|
| P1-1 | Binance 条件单 Algo Service 新接口 | ✓ 已知（§6.1），用新 endpoint |
| P1-2 | OKX attachAlgoOrds 能否带两档 TP | ⚠️ 大概率要下独立 algo，测试网确认 |
| P1-3 | OKX 市价单 sz 单位 + ctVal/lotSz/minSz | ⚠️ 启动拉 instruments 实算（§8.3） |
| P1-4 | set_leverage 时机/层级 | ⚠️ hedge 下是 symbol 级还是 posSide 级；有持仓时能否改（§9 C2 已规避同 symbol） |
| P1-5 | clientOrderId 格式 | ⚠️ Binance `^[\.A-Z\:/a-z0-9_-]{1,36}$` / OKX 字母数字 1-32；client_id_base 要同时兼容 |
| P1-6 | API 限频权重 | ⚠️ 限频按账户(UID)算 → 10 个 key 各自宽裕；但 Binance **查询走 IP 权重**，5 个币安账户同一出口 IP 共享（粗估 ~400 vs 上限 ~2400/min，余量足，具体值按文档核对）。对策：批量接口 + 最小间隔 + 429 退避。**若用公共代理，IP 权重可能被代理上他人占用 → 建议独享 IP** |
| P1-7 | 查历史成交接口 + 保留时长 | ⚠️ 重启对账（§11.3）依赖，确认能查到宕机期间的成交 |
| P1-8 | 子账户 API 权限/IP 白名单 | ⚠️ 每个子账户 key 有合约权限、是否需绑 IP |
| P1-9 | **跨境代理轮询耗时** | ⚠️ 中国 Win + 美国代理往返延迟下，实测单轮（批量查 N 账户）耗时，据此定 `POLL_SECONDS`；循环「干完再 sleep」+ 重入保护防堆积（§4） |

### P2 — 跨系统一致性，需明确口径

| # | 项 | 备注 |
|---|----|------|
| P2-1 | **跨交易所价格基准** | ✓ 已决策：**接受偏差，以币安为准**。信号 level 基于币安 K 线算，OKX 账户照常执行，不锁交易所、不为价差做特殊处理。日常偏差 <0.05%，极端行情会放大但接受 |
| P2-2 | executor 读 CSV 的 open_time 口径 | 确认与 scorer/monitor 一致（收线对齐、tz=UTC），否则 primary/activation 判断错位 |
| P2-3 | 资金费率结算对逐仓可用 margin 的影响 | 持仓跨结算点扣 funding，确认不会把逐仓 margin 吃到触发追保 |
| P2-4 | fetch_delta 原子写改造 | ✓ 已知前置修复（§3.2） |

> **P2-1 已决策**：接受跨所价差，以币安为准。信号 level 用币安 K 线算，OKX 账户照常参与 slot
> 随机分配，不锁交易所、不做价差补偿。日常偏差 <0.05%（对 r_dist≥0.3% 的单影响小），极端行情会
> 放大但接受。上线后在 trade_log 里观察 OKX 单的实际触发偏离即可。

---

## 19. 测试网与上线验证流程

> 交易无小事：测试网这一层把「会被拒单 / 参数算错 / 状态机走歪」的风险消化在不花钱的环境里。
> **测试网过不了的，绝不进实盘。**

### 19.1 两家测试网机制（已查证）

| | Binance Futures Testnet | OKX Demo Trading |
|---|------------------------|------------------|
| 形态 | **独立站点** testnet.binancefuture.com，单独注册的测试账号 | 同账户内「模拟交易」模式 |
| 切换 | **换 base url**（fapi.binance.com → testnet.binancefuture.com）+ 换 key | **base url 不变**，加 header `x-simulated-trading: 1` + 用 demo key |
| key | 测试站点单独生成 | Demo Trading 内单独创建 |
| 行情 | 自有一套行情（与真实币安不同步） | 用真实行情数据 |
| 资金 | 测试 USDT | 10 万美元模拟资金，可重置 |

→ adapter 把 `ENV`（§12.2）作为开关，两家各自实现切换：Binance 切 base url+key；OKX 切
header+demo key。

**测试网账号数量：每所 1 个就够，不必凑 10 个。** 理由：
- adapter 接口（下单/查单/撤单/挂三单/set_leverage/查仓）+ 状态机全分支 → 单账户即可验证；
  hedge 模式下单账户开多个不同 symbol 仓位，也能覆盖「同账户多 slot」「同 symbol 冲突」约束。
- 10 账户的 slot **调度逻辑**（选空闲账户、margin/同 symbol 约束）是纯 Python，用 **mock broker
  单元测试**覆盖，不依赖真实多账户。
- 真正需要凑齐 10 个的，只有**真实并发**——留到 §19.3 的「② 小额实盘」阶段，用真实 5+5 子账户。

→ `keys_testnet.json` 各 1 个（§12.1），`keys_live.json` 才是 5+5。

### 19.2 验证手段矩阵（关键：Binance testnet 行情独立）

⚠️ **Binance testnet 有自己独立的一套行情，与真实币安不同步。** 我们的信号 level
（activation/invalidation/TP）基于真实币安 K 线算出，所以**不能在 Binance testnet 上用真实
信号 level 走端到端**——level 和 testnet 现价对不上，会瞬间成交或永不触发。

因此「完整流程」拆成三种手段，各管一块：

| 验证目标 | 手段 | 说明 |
|---------|------|------|
| 状态机全分支（primary→activation→b2act→ACTIVATED→TP1→BE/TP2/SL、cancel、同bar） | **mock broker 单元测试** | 喂构造 bar 序列 + state.json，精确控制每分支；不依赖交易所价格，最可靠，是状态机验证主力 |
| slot 池调度（选账户 / margin / 同 symbol 约束） | **mock 单测** | 纯 Python 逻辑 |
| 重启对账（§11.3） | mock 单测 + testnet | |
| adapter 机械正确性（市价开仓 / 挂三单 / 撤 / 查 / set_leverage / hedge posSide / 精度 / clientId） | **Binance testnet 真实跑**，用 **testnet 现价**构造订单参数（非信号 level） | 验「和交易所对话」对不对，与价格真不真实无关 |
| 信号 level → 真实价格触发 → 成交（端到端） | **OKX demo（真实行情）** + 小额实盘 | OKX demo 用真实行情，level 对得上 → 能跑端到端；Binance 端到端只能等小额实盘 |
| 真实滑点 / 成交质量 / 强平 MMR / 限频 / 跨所价差 | **小额实盘**（§19.3 ②） | testnet 盘口模拟、深度浅，不可信 |

> **OKX demo 用真实行情是个优势**：免费就能验「信号 level → 真实价格 → 触发成交」的端到端链路；
> Binance 这条只能靠小额实盘补。状态机逻辑本身则由 **mock 单测**保证（能遍历所有分支），不依赖
> 任何 testnet 的真实价格。

### 19.3 上线验证（gate，三层）

资金分层（用户口径）：**1200u 整体就是「小额验证」**，可承受全损；真正的「大额」是 1200u
验证有效后才注资的几万~几十万 u，距今还早。

```
① 测试网 / mock 阶段（ENV=testnet）—— 验「系统↔交易所」交互 + API 行为 + 代码 bug
   - mock 单测：状态机全分支 + slot 调度 + 重启对账（不依赖交易所价格）
   - Binance testnet：adapter 机械正确性（用 testnet 现价构造订单，非信号 level）
   - OKX demo（真实行情）：adapter + 信号端到端（真实 level 触发成交）
   - 故意杀进程，验证重启对账
   - 全部 P0 + 可测 P1 项打勾 → 才进下一阶段
   ⚠️ 这一关与金额无关，是免费抖代码 bug 的地方（qty 算错/posSide 反/SL 挂反等灾难性动作），不可省
        ↓
② 1200u 实盘（= 小额验证，ENV=live，10 账户直接全开，每账户 120u）
   - 代码 bug 已在 ① 抖净，1200u 整体可承受全损，故 10 账户直接全开，不再切金额/账户数
   - 验真实环境：成交质量 / 滑点 / funding / 强平余量 / 限频 / 跨所价差
   - 跑一段时间，对比 trade_log.jsonl 与研究预期（止损率 / TP2率 / 实际R）
        ↓
③ 大额注资（几万~几十万 u）—— 仅在 ② 验证研究真实有效后，距今还早
   - 按 ② 的实测放大仓位/账户结构，1R/margin 等参数按比例重定
```

### 19.4 keys 与配置

- `live/keys_testnet.json` / `live/keys_live.json` 两份（§12.1）。
- `ENV` 切换即切 key 文件 + base url/header，代码一份，环境两套。
- 误用保护：OKX 用 demo key 打实盘 base url（或反之）会直接报错，天然防串环境。

---

## 20. 部署架构（实测确定 2026-06-10）

### 20.1 进程分布（双机）

按「需求各回各家」拆到两台机器：

| 进程 | 机器 | 为什么 |
|------|------|--------|
| monitor + openclaw | **中国 Windows**（有 UI） | openclaw 靠 Chrome + ChatGPT 登录态，绑死能开浏览器、你能盯着的机器；monitor 与它同机，signal_pending 本地交接 |
| **executor** | **btc-ml（新加坡 Vultr）** | 数据源本机、独立 IP、低延迟直连交易所；executor 不需要 UI |

btc-ml 实测：新加坡 Vultr，独立 IP `45.76.176.244`，UTC 时区，2 核 3.8G、load≈0.08（极闲），
到 Binance ~160-200ms / OKX ~140-160ms（完整 HTTPS，连接复用后更低），采集 DB 实时写入。
低频交易绰绰有余，远胜中国 + 美国代理。**不需要再买服务器。**

### 20.2 数据流与跨机器同步

```
中国 Windows:
  monitor（SSH 拉 btc-ml 数据，现状不变）→ signal_pending → openclaw（ChatGPT）→ state.json
                                                                       │
                                          （state.json 单向推到 btc-ml，几 KB）
                                                                       ▼
btc-ml（新加坡）:
  executor ← 本地采集 DB（行情）+ 收到的 state.json → 直连币安/OKX 下单（低延迟）
```

- **跨机器只有一段**：中国 openclaw 产出的 `state.json` 单向推到 btc-ml（新加坡有公网 IP，中国
  push 最简单；只传几 KB json，图不传——executor 用不到图）。
- **完整性（取代同机原子 rename）**：跨机器无原子 rename。改为「先传到临时名，传完再落 `.ready`
  标记」，executor 只认带 ready 的包、绝不读半个；断网时中国侧积压、恢复后重传。
- state.json 之后由 btc-ml executor **本地**维护（唯一 writer），signal_done 也在 btc-ml 本地。

### 20.3 executor 在 btc-ml 读行情

- btc-ml 上 **sqlite3 CLI 没装，但 python3 sqlite3 模块可用**（fetch_delta 已在用）。executor 走
  python sqlite3 **直读本地采集 DB**（`/home/evan/repo/ai_crypto_analyst/data/ai_crypto_analyst.db`，
  表 `ohlcv_bars`），或本地导一份 CSV。本地读，比中国 SSH 拉更实时。
- ⚠️ 读取口径必须与 monitor/scorer 一致（P2-2：open_time、收线对齐、tz=UTC），否则 primary/activation
  判断会与中国侧 monitor 产出的 level 错位。

### 20.4 keys 与安全

- 交易 `keys_live.json` / `keys_testnet.json` 放 **btc-ml 本地**（executor 在那跑），gitignored。
- btc-ml 已有的采集 key 是只读行情；下单是另一套有交易权限的 key，互不影响。

### 20.5 单 repo 双机（不拆项目）

一个 `coin_trader_platform` repo，两台机器都 clone 同一个，各跑各的进程、各装各的依赖：

| 机器 | 跑的进程 | 装的依赖 |
|------|---------|---------|
| 中国 Windows | monitor / openclaw / warmup_replay / fetch_delta / watchdog | playwright、绘图、pandas… → `requirements-windows.txt` |
| btc-ml 新加坡 | executor（读旁边 `ai_crypto_analyst` 的 DB 取行情） | 交易所官方 SDK、pandas… → `requirements-executor.txt` |

**为什么不拆两个 repo**：`replay/scorer.py`（状态机）、state.json schema、数据口径这三样，executor
与 monitor/研究**必须逐字一致**——同一 repo 里是同一份文件，天然不漂；拆开必然漂移（已被 Win/Mac
分叉坑过一次）。

- 代码在 repo ≠ 要装它的依赖：btc-ml 不 import playwright/draw_kline，就不装它们。
- 共享核心改一处，两机 `git pull` 同步，单线不分叉。
- 运行时产物（state/keys/charts/logs）已 gitignore，各机本地各管各的。
- `requirements-*.txt` 分组文件在开工写 executor 时一并建。

---

## 21. 异常处理全清单（交易系统核心）

### 21.1 贯穿原则

**交易所的实际持仓 / 成交 = 唯一真相**；executor 的 state 只是它的视图。凡两者对不上，
**一律以交易所为准 + 宁可暂停不瞎动**（暂停该笔、告警、等人工，绝不在不确定状态下继续下单）。

### 21.2 已拍板的运行参数

| # | 决策 |
|---|------|
| 1 | **信号永不主动弃**（运行中：每个 playbook 总会触发 activation 或 cancel，不存在永挂） |
| 2 | **重启边界**：WAITING 旧信号包**全丢**；ACTIVATED / TP1_HIT 持仓**一律对账后恢复接管，绝不丢**（丢已激活持仓 = 孤儿仓 = 最严重事故） |
| 3 | **数据陈旧 20 分钟** → 停开新仓 + 告警；**已持仓不受影响**（SL/TP 在交易所端，查单走 API 不依赖本地数据） |
| 4 | **executor 卡死 = 5 分钟无心跳** → 告警 + 强制重启，**最多 3 次**；仍卡死 → 停止自动重启 + 持续告警 + 等人工 |
| 5 | **定期对账**：除启动对账外，运行时**每 15 分钟**全量查持仓 + 挂单 vs state |
| — | 心跳：executor 15s / 卡死 5min；openclaw·monitor 30s / 卡死 20min |

### 21.3 八类异常 × 方案

**A. 行情数据**（executor 读 btc-ml 采集 DB）
| Case | 方案 |
|------|------|
| 采集挂了 DB 停更 → 用过期行情 | 最新 bar >20min 未更新 → 停开新仓 + 告警（#3） |
| DB 读失败（写中 / 锁 / 损坏） | 重试；连续失败暂停该 tick + 告警 |
| 数据 gap（缺某根 15m） | 检测连续性，缺口 → 该 symbol 暂停判断 + 告警 |
| 采集端事后回补 / 修正历史 bar | 已激活不回溯；未激活用最新值（需确认采集端是否改已收线 bar） |

**B. 跨机 state.json 同步**（中国 → btc-ml）
| Case | 方案 |
|------|------|
| 推送中断 | 中国侧积压、恢复重传；executor 没新包就不动 |
| 传一半 / 不完整 | 先传临时名 → 原子改名 → 落 `.ready`；executor 只认 .ready 且校验 json 完整 |
| 重复推送 | signal_dir 唯一名幂等，已存在跳过 |
| 积压恢复后涌入 | **重启后旧 WAITING 包全丢（#2）**；运行中按 bar_time 顺序 |

**C. 信号与状态机**
| Case | 方案 |
|------|------|
| state.json 字段缺失 / 格式错 | 入口校验 schema，非法包不进场 + 告警 |
| level 非法（null/0/负 / 方向矛盾） | 入场前合理性校验（方向、TP 在盈利侧、SL 在亏损侧、r_dist>0），不过则丢弃 + 告警 |
| 同 bar 多事件 | cancel 先于 activation；持仓后以交易所实际成交为准 |

**D. 下单与交易所 API**（细节见 §10.2）
| Case | 方案 |
|------|------|
| 🔴 入场单超时不知成没成 | **幂等 clientOrderId**：先查该 ID 状态再决定，重试用同 ID，防重复开仓 |
| 入场失败 / 部分成交 | 同 ID 重试 1 次；不占 slot 不记 ACTIVATED；部分成交按实际 qty 重算 TP/SL |
| SL 挂不上 | 立即市价平退出 + 飞书 + 人工接管（§10.3） |
| 撤单失败 | 重试 + 查状态（可能撤时刚成交） |
| set_leverage 失败 | 开仓前设，失败则不进场 + 告警 |
| 限频 429 | 退避 + 批量接口（btc-ml 独立 IP 已大幅缓解） |
| 交易所维护 / 宕机 | 已挂 TP/SL 在所端仍有效；executor 安全等待不开新仓，恢复后对账 |
| API key 失效 / IP 变动 | 启动自检 + 运行时鉴权失败告警 |
| TP/SL 成交漏检 | 每 tick + 每 15min 全量查持仓 / 挂单对账，不只查记录单号 |

**E. 持仓与资金**
| Case | 方案 |
|------|------|
| 🔴 逐仓强平（SL 未触发先被强平） | 对账发现「该有仓却没仓」→ 查强平记录 → 记终态 + 告警（SL 理论在强平内侧，P0-3） |
| funding 吃 margin 触发追保 | P2-3 待核算；120u 只用 40-70u 留余量 |
| 持仓与 state 漂移 | 启动对账 + 每 15min 定期对账 |
| 账户被动变化（划转 / 他程序动） | 对账异常 → 暂停该账户 + 告警 |

**F. 进程与系统**
| Case | 方案 |
|------|------|
| 🔴 重复启动两实例 → 重复下单 | **单实例锁**（pid 文件 / 端口锁），启动检测已有实例则拒启 |
| 崩溃 | 重启从 state.json + 交易所对账重建（§11.3） |
| 卡死（心跳停没崩） | watchdog 5min 无心跳 → 告警 + 强制重启 ≤3 次 → 停止 + 人工（#4） |
| btc-ml 宕机 / 重启 | 挂的 TP/SL 在所端有效；重启对账，宕机期被平的靠对账补记 |
| 磁盘满写盘失败 | 写失败告警；监控磁盘 |
| 时钟跳变 | 用 bar open_time 而非本地钟；b2act 用 bar 序号差 |

**G. 人工介入**
| Case | 方案 |
|------|------|
| 人工平仓没标记 | 对账发现「记录有仓实际无仓」→ 暂停该笔 + 告警 |
| 人工挂单冲突 | 检测非自己的单 → 让位（§10.3） |
| manual_override 忘清 | 该笔挂起；告警提醒 |

**H. 市场极端**
| Case | 方案 |
|------|------|
| 闪崩 / 插针 | touch SL 在所端触发；BTC 0.1% buffer 缓解 |
| 🔴 跳空（跨过 TP 和 SL） | 以交易所实际成交为准（SL 可能更差价成交 / 同根）；查实际成交定终态 |
| 交易对下架 / 暂停 | 开仓前 + 持仓中检测 symbol 状态；暂停则告警 + 人工 |
| 交易所离谱价格 | 触发价用 last；单根靠 buffer 兜底 |

### 21.4 四个「杀手级」重点机制

| 机制 | 防什么 | 状态 |
|------|--------|------|
| 幂等 clientOrderId | 入场超时重试导致重复开仓 | §10.1 |
| 单实例锁 | 两实例同时跑重复下单 | **新增，写 executor 时实现** |
| 启动 + 每 15min 对账 | 强平 / 漂移 / 孤儿仓 | §11.3 + #5 |
| SL 挂不上 → 平退 + 人工 | 无止损裸仓 | §10.3 |
