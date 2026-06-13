# 部署（btc-ml / 新加坡，systemd user service）

executor + watchdog 用 **systemd user service** 托管（无需 sudo）。职责分离：

- **systemd** 管「进程在」：`coin-executor` 崩溃/退出自动 `Restart=always`，15min 内最多 3 次（`StartLimitBurst=3`），超限停拉起。
- **watchdog** 管「心跳活」：`coin-executor` 5min 无心跳=卡死（进程活着但循环僵死，systemd 测不到）→ `systemctl --user kill -s KILL coin-executor` → systemd 自动拉起（计入 StartLimit）。超 3 次 → systemd 停 → watchdog 持续飞书等人工。

## 一次性前提

```bash
loginctl enable-linger          # user service 登出后也常驻（已开则跳过，不需 sudo）
which python3                   # 确认 ExecStart 路径（默认 /usr/bin/python3，不同则改 .service）
```

## secret（不进 repo）

```bash
mkdir -p ~/.config/coin_trader
cat > ~/.config/coin_trader/executor.env <<'EOF'
FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxxx
EOF
chmod 600 ~/.config/coin_trader/executor.env
```

交易 key 走 `keys_testnet.json` / `keys_live.json`（repo 本地、gitignored），不放这里。

## 安装

```bash
cd ~/repo/coin_trader_platform
mkdir -p ~/.config/systemd/user
cp deploy/coin-executor.service ~/.config/systemd/user/
cp deploy/coin-watchdog.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now coin-executor
systemctl --user enable --now coin-watchdog
```

## 验证 / 运维

```bash
systemctl --user status coin-executor coin-watchdog
journalctl --user -u coin-executor -f          # 实时日志
systemctl --user restart coin-executor          # 手动重启
tail -f live/heartbeat/alerts.log               # watchdog 告警

# 卡死重启演练：暂停 executor 进程模拟僵死，≤5min 后 watchdog 应 kill→systemd 拉起
kill -STOP $(pgrep -f "live.executor")          # 进程活着但僵死（心跳停更新）
# 等 watchdog 一轮 → journalctl 看 "systemctl --user kill" + executor 重启
```

实盘前把 `coin-executor.service` 的 `EXECUTOR_ENV=testnet` 改成 `live`。

中国 Windows 侧（monitor / openclaw / signal_pusher / watchdog --role china）用 Task Scheduler，不在此。
