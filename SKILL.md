---
name: aster-monitor
description: 实时监控Aster DEX (MAINNET) 合约标的价格，Supertrend/Vegas Tunnel信号推送，飞书Webhook提醒
---

# Aster Monitor

实时监控Aster DEX (MAINNET) 合约标的价格，使用Hybrid模式连接WebSocket，监控Supertrend和Vegas Tunnel状态变化，通过飞书WebHook推送TradingView Alert格式提醒。

## 功能特性

- **Hybrid模式**: WebSocket优先，断线自动降级REST
- **Supertrend信号**: ST(9,2.5) + ST(14,1.7)，状态11/10/01/00
- **Vegas Tunnel信号**: EMA9/144/169，状态11/00
- **突破确认**: ST变11/00时启动15m监控，确认/假突破推送
- **TradingView Alert格式**: 标准化消息格式
- **错误分级**: Error推Webhook，Warning只Log
- **7天日志滚动**: webhook_history.log自动清理
- **守护进程**: 心跳监控+自动重启

## 使用方法

### 配置与启动

```bash
# 创建配置 (Agent命令)
python notification_service.py -w "飞书Webhook" -s "BTCUSDT,ETHUSDT"

# 添加标的
python notification_service.py -a "DOGEUSDT"

# 移除标的
python notification_service.py -r "XAUUSDT"

# 查看标的
python notification_service.py -l

# 启动服务
python notification_service.py
```

### Linux服务

```bash
./scripts/start.sh
./scripts/stop.sh
# cron: * * * * * /path/to/scripts/watch.sh
```

## 信号说明

| 信号 | 格式 | 含义 |
|------|------|------|
| ST_11 | `ST: BTCUSDT 11 LONG` | 做多突破 |
| ST_00 | `ST: BTCUSDT 00 SHORT` | 做空突破 |
| VT_11 | `VT: BTCUSDT 11 BULLISH` | 多头趋势 |
| VT_00 | `VT: BTCUSDT 00 BEARISH` | 空头趋势 |
| CONFIRMED | `BREAKOUT: BTCUSDT LONG CONFIRMED` | 突破确认 |
| FALSE | `BREAKOUT: BTCUSDT LONG FALSE (REVERSE)` | 假突破 |

## 命令行参数

| 参数 | 说明 |
|------|------|
| `-w, --webhook` | 飞书WebHook URL |
| `-s, --symbols` | 监控标的 (逗号分隔) |
| `-a, --add-symbol` | 添加标的 |
| `-r, --remove-symbol` | 移除标的 |
| `-l, --list-symbols` | 查看当前标的 |
| `-c, --config` | 配置文件路径 (默认config.toml) |

## 文件结构

```
aster-monitor/
├── SKILL.md
├── README.md
├── notification_service.py
├── config.toml.example
├── requirements.txt
└── scripts/
    ├── start.sh
    ├── stop.sh
    └── watch.sh
```

## 依赖

- asterdex (自动检查安装)
- talib, aiohttp, toml, numpy

## 注意

- 仅使用MAINNET公开数据，无需私钥
- 1小时内同状态不重复推送
- 确保cron任务有执行权限