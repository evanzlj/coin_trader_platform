#!/bin/bash
# launch_replay.sh — 启动回放任务（方案 A：预开 tab + 间隔启动）
set -e

CDP="http://127.0.0.1:18800"
PROJECT="/Users/evanzhang/Documents/repo/coin_strategy/coin_trader_platform"
INTERVAL=60  # 每个进程启动间隔秒数

# ── 1. 确认 CDP 在线 ──
if ! curl -s "$CDP/json/version" > /dev/null 2>&1; then
    echo "❌ Chrome CDP 未运行，请先启动 Chrome with --remote-debugging-port=18800"
    exit 1
fi
echo "✅ CDP 在线"

# ── 2. 清理旧 STOP flag ──
rm -f "$PROJECT/replay_materials/_STOP"
echo "✅ STOP flag 已清理"

# ── 3. 指定要跑的币 ──
COINS=("$@")
if [ ${#COINS[@]} -eq 0 ]; then
    COINS=("btc" "eth" "sol_A" "bnb")
fi
echo "📋 计划: ${COINS[*]}"
echo ""

# ── 4. 预开所有 tab ──
echo "🔄 预开 ${#COINS[@]} 个 ChatGPT tab..."
for i in $(seq 1 ${#COINS[@]}); do
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$CDP/json/new?https://chatgpt.com/")
    echo "  tab $i: HTTP $HTTP"
    sleep 3
done
sleep 5

# ── 5. 确认 tab 索引 ──
echo ""
echo "📋 Tab 清单:"
TAB_JSON=$(curl -s "$CDP/json")
echo "$TAB_JSON" | python3 -c "
import json, sys
for i,p in enumerate(json.load(sys.stdin)):
    if p.get('type')=='page' and 'chrome' not in p.get('url',''):
        print(f'  [{i}] {p[\"title\"][:60]}')
"

# ── 6. 依次启动进程（间隔 60s） ──
echo ""
idx=0
for coin in "${COINS[@]}"; do
    echo "🚀 启动 $coin (tab $idx) ..."
    python3 -u "$PROJECT/run_v7.py" \
        --filter "$coin" \
        --tab-index "$idx" \
        --delay-min 90 \
        --delay-max 120 \
        --progress-file "_v7_${coin}.json" &
    
    ((idx++))
    
    if [ "$coin" != "${COINS[${#COINS[@]}-1]}" ]; then
        echo "   ⏸ 等待 ${INTERVAL}s 后启动下一个..."
        sleep $INTERVAL
    fi
done

echo ""
echo "🏁 全部启动完成！"
echo "   大盘: http://localhost:8765"
echo "   停止: touch $PROJECT/replay_materials/_STOP"
wait
