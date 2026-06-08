# Replay System Design

## 目标

对每个信号，将 VLM 生成的剧本在真实价格数据上机械回放，计算绩效指标，用于评估和迭代 prompt/素材质量。

---

## 文件约定

```
replay_materials/
  {sym}_{grade}_{ts}/
    signal.json          ← 信号元数据（generate_replay_materials.py 生成）
    4h.png               ← 4H 上下文图
    15m.png              ← 15m 详情图
    prompt.txt           ← VLM 输入 prompt
    vlm_response.json    ← OpenClaw 写入（见 OPENCLAW_SPEC.md）
    replay_result.json   ← run_batch_replay.py 写入
```

---

## 模块职责

| 文件 | 职责 |
|------|------|
| `replay/scorer.py` | 单剧本机械回放，输出 PlaybookScore |
| `replay/session.py` | 单信号全剧本评分 + 序列化 |
| `replay/runner.py` | 单信号端到端（加载数据 → 评分 → 保存） |
| `run_batch_replay.py` | 批量处理所有 replay_materials 子目录 |
| `replay_report.py` | 读所有 replay_result.json，输出绩效报告 |

---

## 回放窗口

从 T0 加载到数据文件末尾（无时间上限）。

结果状态：
- 正常出局：TP2 命中 / invalidation 触发
- `activated_tp1_unresolved`：TP1 命中，TP2 有定义但数据用完前未触发，不参与 R 统计
- `not_triggered`：数据用完前 primary 从未触线

---

## PlaybookScore 字段

```
hypothesis, result
primary_touched_at, activated_at, tp1_at, tp2_at, invalidated_at
bars_to_primary, bars_to_activation, bars_to_tp1, bars_to_tp2, bars_to_invalidation
tp1_level, tp2_level
activation_price        # 激活 bar 的 close
invalidation_level      # VLM 设置的原始止损价
r_distance              # abs(activation_price - invalidation_level)
mfe_pct, mae_pct        # 激活后最大顺/逆向浮动 %
mfe_r,   mae_r          # 同上，以 R 倍数表示
prompt_version          # 透传自 vlm_response.json
```

MAE/MFE 方向：near_support → 多方；near_resistance → 空方

---

## 结果状态说明

| result | 含义 |
|--------|------|
| `not_triggered` | primary 从未触线 |
| `activation_cancelled` | primary 触线但激活被取消 |
| `activated_invalidated` | 激活后 TP1 前止损 |
| `activated_tp1_hit` | TP1 命中（之后止损出局或无 TP2） |
| `activated_tp1_tp2_hit` | TP1 + TP2 均命中 |
| `activated_tp1_unresolved` | TP1 命中，TP2 未决（数据用完） |

---

## 五种止盈方式 R 计算

`tp1_r = abs(tp1_level - activation_price) / r_distance`
`tp2_r = abs(tp2_level - activation_price) / r_distance`

| 方式 | TP1+TP2命中 | TP1命中后止损 | TP1前止损 | 未触线/取消 | unresolved |
|------|------|------|------|------|------|
| 1. TP1全出 | +tp1_r | +tp1_r | -1 | 0 | +tp1_r |
| 2. TP2全出 | +tp2_r | -1 | -1 | 0 | 排除 |
| 3. 半TP1+移BE+半TP2 | +0.5×tp1_r+0.5×tp2_r | +0.5×tp1_r+0 | -1 | 0 | +0.5×tp1_r+0 |
| 4. 半TP1+原止损+半TP2 | +0.5×tp1_r+0.5×tp2_r | +0.5×tp1_r−0.5 | -1 | 0 | +0.5×tp1_r−0.5 |
| 5. TP1全出（仅激活信号） | +tp1_r | +tp1_r | -1 | 排除 | +tp1_r |

方式 3 的 BE 止损为近似值：TP1 后如果跌回原始止损，则原始止损先于 BE 触发不可能，因此近似为剩余半仓 0R 出局。

---

## 报告结构（replay_report.py）

### 首屏核心指标（每种止盈方式各一行）

```
Expectancy    = win_rate × avg_win_R − loss_rate × avg_loss_R
Profit Factor = 总盈利R / 总亏损R
Edge Ratio    = avg_MFE_R / avg_MAE_R
```

### 漏斗转化率

```
总信号 → primary触线率 → 激活率 → TP1命中率 → TP2命中率
                                              ↘ TP1后止损率
                       ↘ 取消率    ↘ TP1前止损率
                                              + unresolved数量和占比
```

### 分组对比表

行维度：总体 / BTC / ETH / BNB / SOL / A+ / A-only / near_support / near_resistance / 按月 / 按 prompt_version

列：信号数 / 激活率 / TP1率 / TP2率 / 止损率 / Expectancy(方式1) / Profit Factor / Edge Ratio / avg_bars_to_tp1

### MAE/MFE 分布（仅激活信号）

```
MFE_R: p25 / median / p75 / p90
MAE_R: p25 / median / p75 / p90
Edge Ratio = avg_MFE_R / avg_MAE_R
```

### 月度趋势

各月激活率 + TP1率 ASCII 折线对比

### 输出文件

- stdout 文本报告
- `replay_report/summary.csv`（分组明细）
- `replay_report/per_signal.csv`（每条信号原始字段 + 五种方式R值）
