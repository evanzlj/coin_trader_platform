# Replay Materials Spec — 浏览器自动化方案

本文档描述通过 Playwright + CDP 操控浏览器端 ChatGPT，批量处理回放素材的完整流程。

---

## 运行环境

| 依赖 | 说明 |
|------|------|
| **Chrome 浏览器** | 需提前启动，已登录 ChatGPT 账号 |
| **CDP 端口** | `http://127.0.0.1:18800`（Chrome 启动参数 `--remote-debugging-port=18800`） |
| **Playwright** | `pip install playwright` |
| **执行脚本** | `run_v7.py` |

Chrome 启动示例（macOS）：

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=18800 \
  --user-data-dir=/tmp/chrome-debug-profile
```

---

## 素材目录

```
coin_trader_platform/replay_materials/
  {sym}_{grade}_{ts}/            # 例：btc_Aplus_20260115_0800
    signal.json                  # 信号元数据
    prompt.txt                   # system/user prompt（=== SYSTEM === / === USER TEXT === 分隔）
    {sym}_{grade}_{ts}_4h.png    # 4 小时 K 线图
    {sym}_{grade}_{ts}_15m.png   # 15 分钟 K 线图
    vlm_response.json            ← run_v7.py 写入
```

每个子目录对应一个信号。`run_v7.py` 遍历所有子目录，跳过已有有效 `vlm_response.json` 的目录（幂等）。

图片文件名通过 glob 匹配：`*_4h.png` 和 `*_15m.png`，不硬编码文件名。

---

## 发送流程（Playwright 浏览器自动化）

**一个币一个聊天窗口**，模拟真实人工做单习惯。同品种内的信号共享上下文（连续发送），切换品种时 `goto` 首页开新对话。

### 单信号处理步骤

1. **定位输入文件**：`prompt.txt` + glob 匹配的 4h/15m 图片
2. **检查幂等**：若 `vlm_response.json` 已存在且含 `watch_summary` 且无 `error` 字段 → 跳过
3. **上传图片**：通过 `input[type="file"]` 上传，先 4h 后 15m
4. **填充 prompt**：通过 `evaluate()` 直接写入 `div[contenteditable="true"]`，触发 `input` 事件
5. **发送**：点击 `button[data-testid="send-button"]`，fallback 按 Enter
6. **等待回复**：轮询 `[data-message-author-role="assistant"]`，检测消息数量增长 + 内容稳定（5 次连续不变）
7. **提取 JSON**：从回复中解析 JSON（支持 ` ```json ``` ` 包裹或裸 JSON）
8. **校验**：检查必备字段 `watch_summary`、`playbooks` 存在
9. **注入 `prompt_version` + 写入** `vlm_response.json`

### 限流与冷却

| 策略 | 参数 |
|------|------|
| 信号间随机冷却 | 30–50 秒（可调 `--delay-min` / `--delay-max`） |
| 品种切换新对话 | 检测目录名前缀变化（`bnb`→`btc`→`eth`→`sol`）时 `goto` 首页开新聊天 |
| 速率限制检测 | 检测「请求过于频繁」弹窗 → 点「明白了」→ 等待 30s |

---

## 错误处理

| 错误类型 | 检测方式 | 处理 |
|----------|----------|------|
| **thinking_failed** | 出现「已停止思考」且已等 30s 以上 | 截图 → 等 30s → 重试一次 |
| **stuck_analyzing** | 消息停留在「正在分析」超过 60s | 同上 |
| **server_error** | 出现「Something went wrong」 | 点击「重试」按钮 → 等 5s → 继续等回复 |
| **timeout** | 等待回复超过 360s | 截图 → 等 10s → 重试一次 |
| **rate_limit** | 出现「请求过于频繁」弹窗 ≥10 次 | 标记 BLOCKED → 脚本暂停 |
| **parse_err** | 回复无法解析为合法 JSON | 原始回复写入 `_raw_response.txt`，**不写 `vlm_response.json`** |
| **validation_err** | JSON 缺少 `watch_summary` 或 `playbooks` | 保存带 error 标记的 `vlm_response.json`，原始回复写入 `_raw_response.txt` |
| **fill_failed** | `evaluate()` 写入 prompt 失败 | 直接标记 error，不重试 |

**重试上限**：每个信号最多重试 1 次（`MAX_RETRIES=1`），不可恢复错误（`fill_failed`、`blocked`）不重试。

---

## 保存返回结果

### vlm_response.json

每个信号目录下写入 `vlm_response.json`。由 `run_v7.py` 负责：

1. 校验 ChatGPT 返回的 JSON 可被 `json.loads()` 解析
2. 校验必备字段 `watch_summary`、`playbooks` 存在
3. **注入 `prompt_version` 字段**（例如 `"v1.0"`）
4. 写入 `vlm_response.json`

### 运行时产物

| 文件 | 说明 |
|------|------|
| `replay_materials/_v7_progress.json` | 实时进度（ok/err/blocked/cached 计数 + 最后一条结果） |
| `screenshots/*.png` | 错误截图（thinking_failed / stuck / timeout） |
| `{signal_dir}/_raw_response.txt` | 解析失败时的原始 ChatGPT 回复 |

---

## vlm_response.json 结构

模型输出格式由 `prompt.txt` 中的 system prompt 驱动。当前 v1.0 的输出结构如下：

```json
{
  "prompt_version": "v1.0",

  "watch_summary": {
    "symbol": "SOL/USDT",
    "signal_type": "A+",
    "structure_context": "near_support",
    "price_vs_level": "at_support",
    "current_action": "WAIT",
    "one_sentence_read": "...",
    "scenario_priority_order": ["SUPPORT_REACTION_BOUNCE", "FAILED_REACTION_BREAKDOWN"],
    "priority_rationale": "..."
  },

  "replay_scoring_notes": {
    "l1_playbook": "SUPPORT_REACTION_BOUNCE",
    "l1_activation_rule_summary": "...",
    "how_to_score": "..."
  },

  "key_level_map": {
    "critical_levels": [
      {"name": "4H Support", "level": 79.89, "source": "approx_visual", "role_at_T0": "support", "why_it_matters": "..."}
    ],
    "main_range_read": "...",
    "chop_or_stand_aside_zone": "..."
  },

  "playbooks": [
    {
      "name": "剧本 #1",
      "hypothesis": "SUPPORT_REACTION_BOUNCE",
      "trade_side_if_confirmed": "conditional_long",
      "plausibility": "high",
      "why_this_path": "...",
      "activation_condition": "...",
      "key_levels": {"trigger": "...", "invalidation": "...", "objectives": "..."},
      "conditional_trade_plan": {
        "current_status": "WAIT_FOR_CONFIRMATION",
        "activation_condition": "...",
        "activation_rule": {
          "direction_if_activated": "long",
          "primary_touch": {"level": 79.89, "side": "low", "source": "approx_visual", "reason": "..."},
          "activates_if_close_crosses": {"level": 81.50, "dir": "above", "source": "approx_visual", "reason": "..."},
          "cancels_if_close_crosses_first": {"level": 78.00, "dir": "below", "source": "approx_visual", "reason": "..."},
          "invalidation_after_activation": {"level": 78.00, "dir": "below", "source": "approx_visual", "reason": "..."},
          "objectives": [
            {"level": 86.59, "source": "approx_visual", "reason": "..."},
            {"level": 92.00, "source": "approx_visual", "reason": "..."}
          ]
        },
        "candidate_entry_zone_after_activation": {"level_low": null, "level_high": null, "source": "pending_post_T0", "reason": ""},
        "invalidation_anchor_after_activation": {"level": null, "source": "pending_post_T0", "reason": ""},
        "structural_objective_anchors": []
      }
    }
  ]
}
```

### 字段说明

| 字段 | 说明 |
|------|------|
| `prompt_version` | **run_v7.py 注入**，标识 prompt 版本 |
| `watch_summary` | 信号快照（结构上下文、价格 vs 关键位、场景优先级） |
| `replay_scoring_notes.l1_playbook` | 主剧本 hypothesis 名，评分器以此为准 |
| `playbooks[].hypothesis` | 剧本唯一标识，`l1_playbook` 引用时必须完全一致 |
| `playbooks[].plausibility` | STRICT enum: `high` / `medium` / `low` / `ruled_out` |
| `activation_rule.primary_touch.side` | `"low"` = bar.low 触线；`"high"` = bar.high 触线 |
| `activates_if_close_crosses.dir` | `"above"` = close 上穿；`"below"` = close 下穿 |
| `invalidation_after_activation` | 激活后的止损触发（close 穿越方向） |
| `objectives` | 按顺序，第一个 TP1，第二个 TP2 |

---

## 处理要求汇总

- ✅ 一个币一个聊天窗口：同品种信号共享上下文，切换品种时开新对话
- ✅ 已有有效 `vlm_response.json` 的目录跳过（幂等）
- ✅ 返回 JSON 必须可被 `json.loads()` 解析；解析失败写 `_raw_response.txt`，不写 `vlm_response.json`
- ✅ 缺少必备字段（`watch_summary`、`playbooks`）标记 validation_err
- ✅ `prompt_version` 由 `run_v7.py` 在保存前注入，不依赖 ChatGPT 返回
- ✅ 每个信号最多重试 1 次（可恢复错误）
- ✅ 信号间随机冷却 30–50s，切换品种时自动 `goto` 新对话
- ✅ 被限流（rate_limit）或不可恢复错误时暂停，等待人工介入
