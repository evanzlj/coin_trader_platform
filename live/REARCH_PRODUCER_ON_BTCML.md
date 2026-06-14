# 重构方案：Producer 迁回 btc-ml，Windows 退化为 VLM Worker

状态：**提案，待评审**。本文只定方向与步骤，不含最终代码。

---

## 0. 原则

**谁离实时数据最近，谁就负责数据完整性、信号生成、状态推进。**

实时行情已经在 btc-ml 的 `ai_crypto_analyst.db`。让 Windows 再通过 SSH 拉一份 CSV，等于在实时链路里人为造了一层"最终一致性"。这两天 producer 补的大量东西（fetch_delta 幂等/去重/gap/SSH 超时/进行中 bar/残值/多进程）几乎都在弥补这层中转。把 producer 搬回数据同机，整类脆弱性从根上消失。

**所有权铁律：只有 btc-ml 能生成最终 `state.json` 并投给 executor。** Windows 只产出 `vlm_response.json`，不参与 playbook 过滤 / state 构建 / slot / executor 输入。

---

## 1. 目标拓扑与数据流

```
btc-ml (新加坡，有公网 IP)
  ai_crypto_analyst.db (实时行情, 已含 close_time<=now 纪律)
    └─ monitor / signal_generator        ← 本地读 DB，不再 SSH
        └─ draw_kline (charts, Agg)
            └─ vlm_pending/{pkg}/         ← prompt.txt + 2 PNG + signal.json + .ready
                                  ▲ Windows 拉
                                  │ Windows 推 vlm_response 回
                                  ▼
  vlm_done_incoming/{pkg}/vlm_response.json
    └─ vlm_finalizer                      ← 校验 + post-VLM 过滤 + 建 state.json
        └─ signal_active/{pkg}/           ← executor 唯一输入
            └─ executor                   ← 零改

Windows (中国，NAT 后)
  signal_sync (Windows 发起的双向同步)
    ├─ 拉 btc-ml:vlm_pending → 本地 vlm_pending
    └─ 推 本地 vlm_done → btc-ml:vlm_done_incoming
  vlm_worker (= 变薄的 openclaw)
    └─ 读本地 vlm_pending → ChatGPT → vlm_response.json → 本地 vlm_done
```

**跨境约束**：btc-ml 有公网 IP，Windows 在 NAT 后 → btc-ml 够不着 Windows。**所有跨境传输都由 Windows 发起**（拉 + 推），复用现有 `signal_pusher` 的 ssh+tar 机制。

**跨境负载**：从"每 30s 复制行情"降为"每天几次发包"。链路抽风时只是某信号晚标注，不再是持续数据完整性问题。

---

## 2. 组件矩阵

| 组件 | 处置 | 说明 |
|------|------|------|
| `SignalGenerator` / `signal_logic` / `params` / `indicators` / `state_machine` | **零改** | 信号规则不动 |
| `realtime_data_pull/ReplayFeed` | **零改** | 仍读 CSV（CSV 改由本地 DB 物化）|
| `draw_kline/*` | **微改** | 加 `matplotlib.use("Agg")`；数据源仍 CSV |
| `executor` / `data_reader` / `position_manager` / `reconcile` / `broker/*` | **零改** | |
| 包格式 `.ready`/`signal.json`/`vlm_response.json`/`state.json` | **零改** | |
| `live/fetch_delta.py` | **替换** → `live/data_sync.py`（本地 sqlite 读 + close 过滤 → CSV，无 SSH）|
| `live/signal_pusher.py` | **替换** → `live/signal_sync.py`（Windows 双向：拉 pending + 推 response）|
| `live/live_openclaw.py` | **拆分** → Windows `vlm_worker`（仅 ChatGPT）+ btc-ml `vlm_finalizer`（过滤+建 state）|
| `live/monitor.py` | **改数据源 + 移机** | `data_sync()` 替 `fetch_delta()`；跑在 btc-ml；输出 `vlm_pending/` |
| `live/warmup_replay.py` | **移机** | 在 btc-ml 从 DB 历史 warmup（数据弃旧重建，不迁移）|
| `live/watchdog.py` | **角色调整** | monitor/vlm_finalizer 进 btcml；vlm_worker/signal_sync 进 china |

复用约 80%。信号与执行核心一行不改。

---

## 3. 新件/改件详细设计

### 3.1 `live/data_sync.py`（btc-ml，替代 fetch_delta）
- 本地 `sqlite3` 读 `ai_crypto_analyst.db`，**只取 `close_time<=now` 的已收线 bar**（与 `data_reader` 同纪律）。
- 增量：`open_time > local_tail` → 写本地 CSV（复用 `_merge_dedup_sort` 的 dedup+sort+原子写）。
- 无 SSH、无跨境、无 gap 填补（本地 DB 即源真值；gap=真实数据缺口，直接 `GapError`）。
- monitor 把 `fetch_delta()` 调用替换成 `data_sync()`，下游全不变。
- **[P1] 三路 data readiness gate（保留）**：本地化后跨境问题没了，但 DB 内 `ohlcv_15m / taker_flow_15m / ohlcv_4h` 写入仍可能有先后。monitor **只在三路都已收线到同一根 bar 时才推进 cursor / 产 `vlm_pending`**。即：对每个 symbol，取 `min(latest_closed(ohlcv_15m), latest_closed(taker_flow_15m), latest_closed_4h_aligned)` 作为可推进水位；任一路滞后则该 symbol 本轮不推进（其余 symbol 不受影响，沿用 §F5 per-symbol cursor）。**绝不在 flow/4h 未 ready 时产包。**
- **为何保留 CSV 物化**：`ReplayFeed` 和 `draw_kline` 都以 CSV 为输入接口；本地 DB→CSV 是廉价、安全的物化视图，避免改动信号/画图代码（最高风险区）。DBFeed 直读 DB 记为后续清理项，非本次范围。

### 3.2 `live/signal_sync.py`（Windows，替代 signal_pusher，双向）
- **拉**：`ssh btc-ml "tar czf - -C vlm_pending <新包>"` → 解到本地 `vlm_pending/`。去重标记 `.synced_pulled/{pkg}`。
- **推**：本地 `vlm_done/{pkg}/vlm_response.json` → `ssh btc-ml "解到 vlm_done_incoming/{pkg}"`（no-clobber）。去重标记 `.synced_pushed/{pkg}`。
- 复用 signal_pusher 已硬化的：幂等标记、远端校验、SSH_TIMEOUT、partial-failure 退出码、status/heartbeat。
- 单向链路只走小 json（推）+ 中等 PNG（拉，几百 KB/包）。

### 3.3 `vlm_worker`（Windows，= 变薄的 openclaw）
- 轮询本地 `vlm_pending/`（signal_sync 拉来的）。
- 每包：上传 prompt+2图 → ChatGPT → 抽 JSON → 写 `vlm_done/{pkg}/vlm_response.json`。
- **不做** playbook 过滤、不建 state.json、不碰 signal_active。
- 复用 openclaw 现有的页面交互/重试/解析/openclaw_status。

### 3.4 `live/vlm_finalizer.py`（btc-ml）
- 轮询 `vlm_done_incoming/`（Windows 推回的）。
- 每包：`validate_response` → `_filter_playbooks`（per-symbol post-VLM 过滤）→ `_build_state` → 移到 `signal_active/`。
- 即把现 `live_openclaw.py` 的 `move_to_active`/`_filter_playbooks`/`_build_state`/`_passes_filters` 原样搬到 btc-ml。
- 复用归档幂等（dup 后缀）、status/heartbeat。

**[P1] 信任边界（写死）**：finalizer 决策只信 **btc-ml 本地原始包** `vlm_pending/{pkg}` 里的 `signal.json` / prompt / charts 元信息。Windows 回传**只接受 `vlm_response.json` 一个文件**；回传包里若带 `signal.json`/`state.json`/任何其它文件，一律忽略丢弃，**绝不参与决策**。`_build_state` 的 symbol/grade/bar_time/structure 等全部取自本地 `vlm_pending/{pkg}/signal.json`，VLM 只贡献 `playbooks` 内容。

**[P1] 幂等 / 拒绝决策表（写死，任何"不进 executor"的分支都归档 + 不静默）**：

| 条件 | 处置 |
|------|------|
| 本地 `vlm_pending/{pkg}` 不存在 | response orphan → 归档 `vlm_rejected/` + 告警（Windows 回了个 btc-ml 没产过的包）|
| `signal_active/{pkg}` 已存在 | 不覆盖（executor 已接管）→ 归档 duplicate response |
| `signal_done/{pkg}` 已存在 | 不复活旧信号 → 归档 |
| `vlm_response.json` schema 不合格（缺 `watch_summary`/`playbooks` 或 `error`）| rejected → 不进 executor，归档 |
| `_filter_playbooks` 后无有效 playbook | filtered → 归档 `vlm_rejected/`（reason=`no_valid_playbooks`）|
| 本地 `signal.json.bar_time` 超过 TTL（`SIGNAL_MAX_AGE`）| stale → 不进 executor，归档 `vlm_stale/` |
| 全部通过 | 建 `state.json` → 原子 move 到 `signal_active/{pkg}` |

### 3.5 状态 / warmup（btc-ml）
- buffer/dedup 状态全在 btc-ml，从 DB 历史 `warmup_replay` 一次重建。**旧 Windows 状态弃用，不迁移。**

### 3.6 charts（btc-ml）
- `draw_kline` 顶部 `import matplotlib; matplotlib.use("Agg")`（headless）。
- btc-ml 装 `matplotlib` + `pyarrow`（见 §6）。

### 3.7 [P1] `vlm_pending` 生命周期 / 租约（写死）
pending 包是跨境队列，必须有明确状态机，否则会出现重复 ChatGPT、重复 response、pending 永久积压。

**状态（btc-ml 侧，pkg 同时只处于一态）**：
- `pending`：monitor 刚产出、带 `.ready`，未被拉取。
- `claimed`：Windows `signal_sync` 拉取时，在 btc-ml 上打 `claimed_at` 标记（如 `vlm_pending/{pkg}/.claimed`，含时间戳）。
- `done`：finalizer 已据其 response 产出 `signal_active/{pkg}`（或归档），原始包移入 `vlm_pending_done/`。

**规则**：
1. **单 worker 假设**：默认只有一个 Windows worker。`signal_sync` 单实例锁（复用 `single_instance`）。多 worker **暂不支持**（要支持需把 claim 做成原子租约，记为后续）。
2. **claim 续约 / 回收**：`claimed` 超过 `CLAIM_LEASE`（如 20min，覆盖 ChatGPT TIMEOUT 360s × 重试 + 跨境往返）仍无 response → 视为 worker 拉了但挂了 → **回收**：清 `.claimed`，退回 `pending`，可被重新拉取。
3. **TTL / stale**：pending 的 `signal.json.bar_time` 超 `SIGNAL_MAX_AGE` → monitor/扫描器直接归档 `vlm_stale/`，不再投递（避免积压老信号过 ChatGPT）。
4. **重复 response**：finalizer 已处理过某 pkg（done）后又收到同名 response → 按 §3.4 决策表归档 duplicate，不二次入场。
5. **拉取幂等**：`signal_sync` 用 `.synced_pulled/{pkg}` 本地标记，重启不重拉；同理 `.synced_pushed/{pkg}`。
6. **背压**：pending 积压数 > 阈值（如 20）→ status 报 backlog，watchdog 告警（Windows/链路可能挂）。

---

## 4. 故障与降级

| 场景 | 行为 |
|------|------|
| **Windows 挂** | monitor 在 btc-ml 继续产 `vlm_pending`（排队）；executor/持仓/SL/TP/reconcile 照常。仅"新信号无法过 ChatGPT"。**安全降级。** |
| **中↔新链路抽风** | 包排队、Windows 重试；不影响 btc-ml 本机数据与执行。 |
| **btc-ml 挂** | 全停（但它本就是 executor 宿主，systemd+watchdog 已覆盖）。 |
| **vlm_finalizer 挂** | response 排队 `vlm_done_incoming/`；watchdog 告警；恢复后续跑。 |

对比现状：Windows 一旦出问题会同时影响"行情数据完整性 + 信号 + 推送"，新架构把它收敛成"只影响新信号标注"。

---

## 5. 迁移顺序（每步独立、有验证关、可回滚）

> 旧 Windows monitor/fetch_delta/signal_pusher 在 Phase 5 前一直保留可跑，随时回退。

- **Phase 1（btc-ml，隔离）**：`data_sync.py` + monitor 移机 → 本地出 `vlm_pending`。
  - **沙箱**：Phase 1/2 **绝不写生产 `signal_active`**。用 `SIGNAL_ACTIVE=/tmp/ct_signal_active_test`（及 `VLM_PENDING` 等同样指向 sandbox），或确保 executor 明确 stopped/testnet，否则手工 response 可能被真实 executor 接走。
  - 验收（强）：
    1. **信号一致性**：同一历史窗口下，新 btc-ml monitor 产出的 signal（symbol/grade/bar_time/structure）与旧 Windows 链路**逐条一致**。
    2. **readiness gate 生效**：构造 flow/4h 滞后 15m 的场景，确认 monitor **不产**该 symbol 的 `vlm_pending`，直到三路 ready。
    3. 不接 Windows、不影响现状。
- **Phase 2（btc-ml）**：`vlm_finalizer.py`（从 openclaw 抽逻辑）。
  - **沙箱**：同 Phase 1，输出指向 sandbox `signal_active`，executor stopped/testnet。
  - 验收：手工放含 `vlm_response.json` 的包到 sandbox `vlm_done_incoming/`，逐条走 §3.4 决策表（orphan / dup / schema 坏 / stale / 无有效 playbook / 正常）各验一次。
- **Phase 3（Windows）**：`signal_sync.py`（拉/推）+ `vlm_worker`（变薄 openclaw）。
  - 关：拉一个真包 → ChatGPT → 推回；验证 btc-ml 收到 response。
- **Phase 4**：端到端贯通 + watchdog 角色 + systemd units。
  - 关：DB 一根新信号 → 走完 btc-ml→Windows→btc-ml→executor 接管。
- **Phase 5**：下线 Windows 旧 monitor / fetch_delta / signal_pusher。

---

## 6. 依赖与部署

- btc-ml 新增依赖：`matplotlib`、`pyarrow`（+ 已有 pandas）。新建 `requirements-producer.txt` 或并入 executor 那份。
- btc-ml 已是同一 repo（git pull 即得全部 producer 代码）。
- 新 systemd user units：`coin-monitor`、`coin-vlm-finalizer`（btc-ml）；Windows 侧 `signal_sync` + `vlm_worker`（Task Scheduler / 手动）。
- 资源：4G/2核 跑 每15m 查4币 + signal_generator + 2张图 + 小包，够。Chrome/ChatGPT 留 Windows。

### 6.1 watchdog 角色（具体）
| 角色 | 监控对象 | 心跳 stale | status 阈值（示例）|
|------|----------|-----------|-------------------|
| **btcml** | `executor` | 5min（systemd 重启）| 已有 |
| | `monitor` | 20min | `consecutive_failures≥3` / `package_write_failures≥3` / `backlog≥10` / status stale 5min |
| | `vlm_finalizer` | 20min | `consecutive_rejections≥5` / `orphan/dup/stale 计数` / status stale 5min |
| | `data_source` health | — | DB 三路最新已收线 bar 落后 now 超阈值（如 >40min）→ 采集端可能挂 |
| **china** | `signal_sync` | 10min | `consecutive_failures≥3`（拉/推失败）/ `backlog≥5` / status stale 5min |
| | `vlm_worker` | 20min | `consecutive_rejections≥5` / `parse_errs≥5` / status stale 25min |

各进程沿用现有 `status.json`（atomic 写）+ heartbeat 双轨；watchdog 复用 §P2-2 的 `max_stale_min`。

---

## 7. 默认决策（有异议即拦）

1. 数据源：**本地 DB→CSV 物化**（复用 ReplayFeed+draw_kline），不做 DBFeed 大改。
2. 过滤 + 建 state.json：**btc-ml 的 vlm_finalizer**。
3. 跨境同步：**独立 `signal_sync.py`**，由 Windows 发起拉+推；不塞进 vlm_worker。
4. 目录语义：`vlm_pending`（btc-ml 产）/ `vlm_done_incoming`（Windows 推回）/ `signal_active`（finalizer 产，executor 读）。
5. 旧数据：**全弃**，btc-ml 从 DB 重新 warmup。

---

## 8. 明确不改 / 非目标

- 信号规则、参数、playbook 过滤口径：**不动**（只搬位置）。
- executor 交易逻辑、对账、broker：**不动**。
- 本次不做 DBFeed 直读、不做 openclaw→ChatGPT-API 替换（记为后续：若 openclaw 也能上 btc-ml 或换 API，则中国 Windows 可彻底退役，全栈单机零跨境）。

---

## 9. 回滚

- 每个 Phase 产物独立；Phase 5 前 Windows 旧链路保持可启动。
- 出问题：停新 units、重启旧 Windows monitor + signal_pusher 即回到现状。
- btc-ml 的 `data_sync`/`vlm_finalizer`/新 `vlm_pending` 目录与旧链路互不写同一文件，并存安全。
