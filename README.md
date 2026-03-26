# Aster Monitor

实时监控Aster DEX (MAINNET) 合约标的价格，使用Hybrid模式连接WebSocket，监控Supertrend和Vegas Tunnel状态变化，通过飞书WebHook推送TradingView Alert格式提醒。

## 功能特性

- **Hybrid模式**: WebSocket优先，断线自动降级REST
- **Supertrend信号**: ST(9,2.5) + ST(14,1.7)，状态11/10/01/00
- **Vegas Tunnel信号**: EMA9/144/169，状态11/00
- **突破确认**: ST变11/00时启动15m监控，确认/假突破推送
- **TradingView Alert格式**: 标准化消息格式，兼容各种Webhook
- **错误分级**: Error推Webhook，Warning只Log
- **7天日志滚动**: webhook_history.log自动清理
- **守护进程**: 心跳监控+自动重启

## 快速开始

### 1. 安装依赖

```bash
# 检查asterdex是否已安装
python -c "import asterdex" 2>/dev/null || \
(git clone https://github.com/HYwooo/asterdex-python-sdk.git && \
cd asterdex-python-sdk && pip install -e .)

# 安装其他依赖
pip install -r requirements.txt

# 安装TA-Lib (Ubuntu)
sudo apt-get install ta-lib
pip install ta-lib
```

### 2. 配置服务

```bash
# 通过命令行配置 (Agent命令)
python notification_service.py -w "飞书Webhook URL" -s "BTCUSDT,ETHUSDT"

# 或手动复制配置
cp config.toml.example config.toml
# 编辑config.toml填入你的Webhook URL
```

### 3. 启动服务

```bash
# 前台运行
python notification_service.py

# 后台运行
./scripts/start.sh
```

### 4. 设置守护进程 (Linux)

```bash
# 添加cron任务
crontab -e
# 添加行:
* * * * * /path/to/scripts/watch.sh
```

## 信号格式 (TradingView Alert)

| 信号 | 格式 | 含义 |
|------|------|------|
| ST | `ST: BTCUSDT 11 LONG` | Supertrend做多突破 |
| ST | `ST: BTCUSDT 00 SHORT` | Supertrend做空突破 |
| VT | `VT: BTCUSDT 11 BULLISH` | Vegas Tunnel多头 |
| VT | `VT: BTCUSDT 00 BEARISH` | Vegas Tunnel空头 |
| BREAKOUT | `BREAKOUT: BTCUSDT LONG CONFIRMED` | 突破确认 |
| BREAKOUT | `BREAKOUT: BTCUSDT LONG FALSE (REVERSE)` | 假突破-反向 |
| BREAKOUT | `BREAKOUT: BTCUSDT LONG FALSE (NO_CONTINUATION)` | 假突破-无延续 |
| SYSTEM | `SYSTEM: Aster DEX connected to MAINNET` | 系统消息 |

## 命令行参数

| 参数 | 说明 |
|------|------|
| `-w, --webhook` | 飞书WebHook URL |
| `-s, --symbols` | 监控标的 (逗号分隔) |
| `-a, --add-symbol` | 添加标的 |
| `-r, --remove-symbol` | 移除标的 |
| `-l, --list-symbols` | 查看当前标的 |
| `-c, --config` | 配置文件路径 |

## 配置文件 (config.toml)

```toml
[webhook]
url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"

[symbols]
monitor_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "XAUUSDT"]

[supertrend]
period1 = 9
multiplier1 = 2.5
period2 = 14
multiplier2 = 1.7

[vegas]
ema_signal = 9
ema_upper = 144
ema_lower = 169

[service]
heartbeat_file = "notification_heartbeat"
heartbeat_timeout = 120
```

## 文件结构

```
aster-monitor/
├── SKILL.md              # Skill元数据
├── README.md             # 本文件
├── notification_service.py
├── config.toml.example
├── requirements.txt
├── webhook_history.log   # 运行时生成
├── notification_heartbeat # 运行时生成
├── notification_service.pid # 运行时生成
└── scripts/
    ├── start.sh          # 启动脚本
    ├── stop.sh           # 停止脚本
    └── watch.sh          # 守护进程脚本
```

## 错误处理

- **Error级别**: 连接失败、严重异常等 → 推送到Webhook
- **Warning级别**: 临时失败、可恢复错误 → 只写入日志

## 日志

- `webhook_history.log`: 推送记录，7天自动清理
- `notification_service.log`: 服务运行日志

## License

MIT