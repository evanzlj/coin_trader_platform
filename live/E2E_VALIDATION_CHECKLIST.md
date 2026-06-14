> ⚠️ **LEGACY / 已废弃方向** —— 本清单针对"Windows 跑 producer"的旧架构。
> 项目已 pivot 到 **producer 迁回 btc-ml、Windows 只做 VLM worker**（见 `REARCH_PRODUCER_ON_BTCML.md`）。
> 旧 Windows monitor/fetch_delta/signal_pusher 仅作 rollback 资产，**不要再按本清单执行**。保留供历史参考。

# Producer/Data 端到端验证清单（Windows 中国侧 ↔ btc-ml 新加坡侧）[LEGACY]

> 适用提交：`7f979df` 起（#22 P1 producer/data 链路硬化）。
> 拓扑：**[Win]** monitor · openclaw · signal_pusher（中国）→ **[btc-ml]** executor · watchdog（新加坡）。
> 信号流：monitor → `signal_pending/` → openclaw → `signal_active/` → pusher → `btc-ml:signal_active/` → executor。
>
> 重点回归（本次新逻辑，优先确认）：**F5** per-symbol cursor · **F6** buffer 重启快 · **F7** pusher 失败退出码 · **F8** active 重复清理。
>
> 安全约束：脚本可 `load_keys()` 内部使用，**绝不打印 API key 值**。

---

## 阶段 0 — Pre-flight

- [x] **[Win][btc-ml]** 两侧 `git pull`，确认到目标提交：`git log -1 --oneline`
- [x] **[Win]** 依赖在位：`python3 -c "import pandas, playwright, pyarrow"`（buffer parquet 需 pyarrow/fastparquet）
- [x] **[Win][btc-ml]** `python3 -m compileall -q live` 无报错
- [x] **[btc-ml]** `EXECUTOR_ENV=testnet`（先别上 live）；keys 文件在位
- [x] **[Win]** 到 btc-ml SSH 免密通：`ssh evan@btc-ml echo ok`
- [x] **[Win][btc-ml]** 两侧系统时钟**准确（NTP 同步）**即可——**时区无所谓**。Windows 保持 UTC+8（A股用）完全 OK：producer/executor 全程用 tz-aware UTC（`pd.Timestamp.utcnow()` / `now("UTC")` / `datetime.now(timezone.utc)`），不读系统本地时区。只需两机绝对时间偏差 < 数秒：
  ```bash
  # 两侧各跑，比对输出（应几乎相同，与本地时区无关）
  python3 -c "import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat())"
  # Windows 确认时间服务在同步：  w32tm /query /status
  # btc-ml 确认：              timedatectl | grep -i sync
  ```

---

## 阶段 1 — 数据层 fetch_delta（F1/F3/F4）

- [x] **[Win]** 单跑 `python3 live/fetch_delta.py`
  - 通过：日志 `merged N new bar(s)`，无 traceback
- [x] **[Win]** F1 幂等：连跑两次，第二次应 `merged 0 new bar(s)`（retry 不重复追加）
- [x] **[Win]** F4 由**单元测试**验证（不是 live pipeline）：`python3 -m unittest tests.test_producer.TestContinuityCheck -v` → 4 个 ok
  - 注意：continuity check 跑在 `_merge_dedup_sort` **之后**，merge 已去重+排序，故 live 路径里 **`duplicate`/`reverse` 分支不可达**（纯兜底，防 merge 将来出 bug / 源端病态）；**只有 `gap` 分支 live 可达**（源库真缺 bar 时，如 SOL 那种）。
  - 想看真文件 I/O 触发：`python3 -c "import pandas as pd,tempfile; from pathlib import Path; from live import fetch_delta as fd; df=pd.read_csv('history_data_manager/data/ohlcv/btcusdt_15m.csv').tail(10).reset_index(drop=True); t=Path(tempfile.mkdtemp())/'x.csv'; pd.concat([df,df.tail(1)],ignore_index=True).to_csv(t,index=False); print('dup',fd.check_gap(t,'15m',n_rows=3)); d=df.copy(); d.loc[9,'open_time'],d.loc[8,'open_time']=d.loc[8,'open_time'],d.loc[9,'open_time']; d.to_csv(t,index=False); print('rev',fd.check_gap(t,'15m',n_rows=3))"`
- [x] **[Win]** 全量 CSV audit，确认 15m/4h `dup=0 reverse=0 gap=0`（PowerShell 单行版）：
  ```powershell
  python3 -c "import pandas as pd; from pathlib import Path; IV={'15m':15,'1h':60,'4h':240}; rows=[(f.parent.name+'/'+f.name, pd.to_datetime(pd.read_csv(f,usecols=['open_time'])['open_time'],utc=True).diff().dropna(), pd.Timedelta(minutes=IV.get(f.stem.rsplit('_',1)[-1],15))) for f in sorted(Path('history_data_manager/data').glob('**/*.csv'))]; [print(f'{n:<30} dup={int((d==pd.Timedelta(0)).sum())} rev={int((d<pd.Timedelta(0)).sum())} gap={int((d>iv*1.5).sum())}') for n,d,iv in rows]"
  ```
  通过：16 行全 `dup=0 rev=0 gap=0`（dup/rev 任意非 0 = 硬故障，立即停）。

---

## 阶段 2 — monitor（F5 per-symbol cursor / F6 运行期持久化 / status）

- [x] **[Win]** 启动 `python3 live/monitor.py`
  - 通过：`buffer state loaded — incremental warmup from ...` 或冷启 `falling back to ... warmup`
  - 启动日志打印 `per-symbol cursors: {...}`（F5，四币种各自时间）
- [x] **[Win]** status JSON 内容齐全：`live/heartbeat/monitor_status.json` 含
  `per_symbol`（每币种 cursor/latest_bar/staleness_min）、`package_write_failures`、`consecutive_failures`、`backlog_count`
- [x] **[Win]** 心跳推进：`live/heartbeat/monitor_last_run.txt` 每 ~30s 更新
- [ ] **[Win]** F6 持久化：跑 >10min（`BUFFER_SAVE_SECONDS=600`）后 `live/state/buffer/` 的 parquet mtime 被刷新，且**无 `.tmp` 残留**
  - 然后重启 → 增量 warmup 区间是"分钟级"而非从原始 warmup 点回放数天
- [ ] **[Win]** F6 容错：把某个 `buf_*.parquet` 写坏 → monitor **不崩**，日志 `buffer state load failed ... falling back to full warmup`
- [ ] **[Win]** 信号包写出：`signal_pending/<sym>_<grade>_<ts>/` 含 `.ready` + `signal.json`(utf-8) + `prompt.txt` + 两张 png
- [ ] **[Win]** F5 场景（可选，需构造）：BTC 已推进、ETH 后补同一根 → 下一轮 ETH 仍被 replay（日志见 ETH cursor 推进）
- [ ] **[Win]** dedup 原子写：`live/state/dedup_state.json` 无 `.tmp` 残留；手动写坏该文件 → monitor 拒启 + status 记 `last_error`（不静默崩）

---

## 阶段 3 — openclaw（F8 active 重复清理 / openclaw_status）

- [ ] **[Win]** Chrome 带 CDP 起好，`python3 live/live_openclaw.py --cdp-url http://127.0.0.1:18800`
  - 通过：`using tab[..]` → 处理 pending 包 → 写 `vlm_response.json` → move 到 `signal_active/`
- [ ] **[Win]** `live/heartbeat/openclaw_status.json` 有内容：`processed/moved/rejected/blocked/move_failures/parse_errs/consecutive_rejections`
- [ ] **[Win]** 心跳节奏：每个包处理**前后**都刷 heartbeat，批量 3 包不逼近 20min 阈值
- [ ] **[Win]** F8 幂等：手动在 `signal_active/` 预放同名目录，再让 openclaw 处理对应 pending 包 → pending 副本归档到 `signal_rejected/`（reason `already_in_signal_active`），**不残留、不反复处理**
- [ ] **[Win]** filtered/parse_err 包进 `signal_rejected/`；dest 冲突时带 `__dup_<ts>` 后缀，源不留 pending

---

## 阶段 4 — signal_pusher（F7 partial failure）

- [ ] **[Win]** `python3 -m live.signal_pusher --once`
  - 空闲（无包）：**exit 0**
  - 有包且推成功：btc-ml `signal_active/<pkg>/` 出现（只传 json，不传 png）
- [ ] **[Win]** F7 失败语义：断网或改错 `REMOTE_HOST` 跑 `--once` → **exit 1**，`pusher_status.json` 的 `last_error` 形如 `partial failure: pushed=X failed=Y backlog=Z` 或 `pushed=0 failed=...`
- [ ] **[Win]** 去重：对已推过的包再跑 → 远端 `SKIP_EXISTS`，本地 `.signal_pushed/` 不重推
- [ ] **[btc-ml]** 远端校验：坏包（缺 `.ready`/`state.json`）→ `BAD_PACKAGE` 不落地

---

## 阶段 5 — executor（testnet，验证 producer 输入未被破坏）

- [ ] **[btc-ml]** `EXECUTOR_ENV=testnet python3 -m live.executor`（或 systemd）
  - 通过：`load_states` 只认带 `.ready` 的完整包；新包进入 WAITING 状态机
- [ ] **[btc-ml]** 心跳 `executor_last_run.txt` 每 ~15s 推进
- [ ] **[btc-ml]** 对账：重启 → `startup reconcile done`；get_position 异常走 `unknown`（hold + 快速重试），不裸开仓
- [ ] **[btc-ml]** `trade_log.jsonl` 有事件行；飞书能收测试告警

> ⚠️ 真正下单链路（入场/挂三单/出场/drain）仍用既有 testnet 联调 + `tests/fault_inject_testnet.py` 覆盖。本清单聚焦"producer 改动是否破坏喂给 executor 的输入"。

---

## 阶段 6 — watchdog（两侧）

- [ ] **[Win]** `python3 live/watchdog.py --role china`（单次）
  - 通过：monitor/openclaw/signal_pusher 心跳 OK；读三个 status JSON 无误报
  - 造高 `consecutive_rejections`/`package_write_failures` 的 status → 触发飞书告警（通用阈值 StatusSpec）
- [ ] **[btc-ml]** `python3 -m live.watchdog --role btcml --loop`
  - 通过：executor 心跳活；模拟卡死（停心跳）→ `systemctl --user kill` 拉起

---

## 阶段 7 — 全链路贯通（端到端一根信号）

- [ ] 产生一个 A 信号，走完：**[Win]** monitor→pending → openclaw→active → pusher→btc-ml → **[btc-ml]** executor 接管
  - 通过：同一 pkg 名在 Win `signal_active/` 与 btc-ml `signal_active/` 都出现；executor `state.json` 接管；Win `.signal_pushed/` 标记落地

---

## 阶段 8 — 上小钱前最后一步

- [ ] **[btc-ml]** testnet 全链路稳定 ≥24h 无异常告警
- [ ] **[btc-ml]** `deploy/coin-executor.service` 改 `EXECUTOR_ENV=live`，`systemctl --user enable --now coin-executor coin-watchdog`
- [ ] 首单用最小名义金额，人盯第一笔入场→挂三单→出场闭环

---

## 附：源数据库完整性（一次性，已于 2026-06-14 做过）

btc-ml `ai_crypto_analyst.db` 全量 dup/reverse/gap audit：`ohlcv_bars` 与 `taker_flow_bars` 均 `dup=0 reverse=0 gap=0`。
已回填 15 根缺失 flow bar（SOL×1、ADA×1、LINK×1、DOT×12，均 2026-05-08~10 采集端抖动）。
**P2 backlog**：`ai_crypto_analyst` 采集端在该时段有系统性丢档，待查重试/补采逻辑。
