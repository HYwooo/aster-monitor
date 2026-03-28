# Aster Monitor

实时监控Aster DEX (Mainnet) 合约标的价格，使用WebSocket连接监控Supertrend和Vegas Tunnel状态变化，通过飞书WebHook推送提醒。

## 功能特性

- **WebSocket实时行情**: 批量订阅多币种ticker，秒级延迟
- **Supertrend信号**: ST(9,2.5) + ST(14,1.7)，状态11/10/01/00
- **Vegas Tunnel信号**: EMA9/144/169，状态11/00
- **突破确认**: ST变11/00时启动15m监控，确认/假突破推送
- **飞书推送**: 支持Card和TradingView两种消息格式
- **每2分钟状态报告**: 自动打印所有标的Mark Price、ST、EMA值
- **热重载**: 修改config.toml自动重载配置，支持回滚
- **初始化过滤**: 启动时已有状态不重复推送，只在状态切换时推送
- **错误分级**: Error推Webhook，Warning只Log
- **7天日志滚动**: webhook_history.log自动清理
- **守护进程**: 心跳监控+自动重连

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
# 通过命令行配置
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

## 飞书消息格式

### Card格式 (默认)

飞书Interactive Card格式，标题蓝色(错误时红色)，内容使用Markdown。

### TradingView格式

设置 `format = "tradingview"` 后使用传统文本格式。

## 信号格式

| 信号 | 格式 | 含义 |
|------|------|------|
| ST | `ST: BTCUSDT 11 LONG` | Supertrend做多突破 |
| ST | `ST: BTCUSDT 00 SHORT` | Supertrend做空突破 |
| VT | `VT: BTCUSDT 11 BULLISH` | Vegas Tunnel多头 |
| VT | `VT: BTCUSDT 00 BEARISH` | Vegas Tunnel空头 |
| BREAKOUT | `BREAKOUT: BTCUSDT LONG CONFIRMED` | 突破确认 |
| BREAKOUT | `BREAKOUT: BTCUSDT LONG FALSE (REVERSE)` | 假突破-反向 |
| BREAKOUT | `BREAKOUT: BTCUSDT LONG FALSE (NO_CONTINUATION)` | 假突破-无延续 |
| SYSTEM | `Aster Monitor connected to Mainnet` | 系统消息 |
| CONFIG | `Hot reload successful` | 配置热重载 |

## 状态报告 (每2分钟)

每120秒打印所有监控标的状态：

```
[STATUS] 2026-03-28 22:44:00
[STATUS] BTCUSDT: Price=66787.7, ST=00, EMA_S=66465.8, EMA_U=68747.2, EMA_L=68951.6, VT=00
[STATUS] ETHUSDT: Price=2024.6, ST=11, EMA_S=2006.16, EMA_U=2078.44, EMA_L=2085.63, VT=00
```

数值格式：整数部分非零时按6位有效数字，整数为零时保留6位小数。

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
format = "card"   # card (默认) 或 tradingview

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

### 热重载

修改config.toml后，服务会自动检测并重载配置：
- 验证配置合法性，非法配置拒绝重载并回滚
- 成功重载后推送飞书通知
- 重载失败时自动恢复之前配置

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
- 控制台输出: 服务运行日志

## License

MIT
