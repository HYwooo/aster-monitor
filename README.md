# Aster Monitor

实时监控Aster DEX (Mainnet) 合约标的价格，使用WebSocket连接监控ATR Channel状态变化，通过飞书WebHook推送提醒。

## 功能特性

- **WebSocket实时行情**: 批量订阅多币种ticker，秒级延迟
- **REST Polling Fallback**: WebSocket数据不可用时自动切换REST polling（1秒间隔）
- **指数退避重连**: WebSocket断开后1s→2s→4s→...→60s指数退避重连
- **ATR Channel**: 1H DEMA ATR通道突破推送 + 15m HMA ATR追踪止损
- **PairTrading支持**: 支持 "BTCUSDT/ETHUSDT"、"XAUUSDT/XAGUSDT" 等交易对，K线h=max(o,c),l=min(o,c)
- **飞书推送**: 支持Card和TradingView两种消息格式
- **每2分钟状态报告**: 可通过命令控制打印/暂停
- **每日报告**: 定时推送过去24小时警报次数
- **热重载**: 修改config.toml自动重载配置，支持回滚
- **初始化过滤**: 启动时已有状态不重复推送，只在状态切换时推送
- **代理支持**: 可配置HTTP/HTTPS代理
- **错误分级**: Error推Webhook，Warning只Log
- **日志轮转**: webhook_history.log超过max_log_lines自动轮转
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

## 命令行控制

运行时可输入以下命令：

| 命令 | 说明 |
|------|------|
| `stop print` | 暂停每2分钟的状态打印 |
| `resume print` | 恢复状态打印 |
| `status` | 查看当前状态和警报计数 |

## 飞书消息格式

### Card格式 (默认)

飞书Interactive Card格式，标题蓝色(错误时红色)，内容使用Markdown。

### TradingView格式

设置 `format = "tradingview"` 后使用传统文本格式。

## 信号格式

| 信号 | 格式 | 含义 |
|------|------|------|
| ATR_Ch | `[BTCUSDT] LONG` | 1H ATR Channel 多头突破 |
| ATR_Ch | `[BTCUSDT] SHORT` | 1H ATR Channel 空头突破 |
| ATR_Ch | `[BTCUSDT] LONG TRAILING STOP` | 15m 追踪止损触发(多头) |
| ATR_Ch | `[BTCUSDT] SHORT TRAILING STOP` | 15m 追踪止损触发(空头) |
| SYSTEM | `Aster Monitor connected to Mainnet` | 系统消息 |
| CONFIG | `Hot reload successful` | 配置热重载 |
| REPORT | `Alert count in last 24h: 5` | Daily report |

## 状态报告 (每2分钟)

每120秒打印所有监控标的状态：

```
[STATUS] 2026-03-31 19:04:03
[STATUS] BTCUSDT: Price=66288.5, ATR_Ch=SHORT, Channel=66091.9~67179.346
[STATUS] ETHUSDT: Price=2026.3, ATR_Ch=SHORT, Channel=2017.1~2053.5111
[STATUS] BTCUSDT/ETHUSDT: Price=32.71406, ATR_Ch=SHORT, Channel=32.730607~32.808087
[STATUS] XAUUSDT/XAGUSDT: Price=62.563961, ATR_Ch=SHORT, Channel=62.451388~63.012791
```

数值格式：整数部分非零时按8位有效数字，整数为零时保留8位小数。

## 配置文件 (config.toml)

```toml
[webhook]
url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
format = "card"   # card (默认) 或 tradingview

[symbols]
monitor_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "BTCUSDT/ETHUSDT", "XAUUSDT/XAGUSDT"]

[atr_1h]
ma_type = "DEMA"      # DEMA, RMA, EMA, SMA, WMA
period = 14
mult = 1.618

[atr_15m]
ma_type = "HMA"       # DEMA, RMA, EMA, SMA, WMA, HMA
period = 14
mult = 1.3

[service]
heartbeat_file = "notification_heartbeat"
heartbeat_timeout = 120

[proxy]
enable = false
url = "http://127.0.0.1:7890"

[report]
enable = false
times = ["08:00", "20:00"]

[settings]
timezone = "Z"       # Z=UTC+0, "+08:00"=UTC+8, "-05:00"=UTC-5
max_log_lines = 1000
```

### 代理配置

```toml
[proxy]
enable = true
url = "http://127.0.0.1:7890"
```

### 每日报告配置

```toml
[report]
enable = true
times = ["08:00", "20:00"]
```

### 时间与日志配置

```toml
[settings]
timezone = "Z"          # Z=UTC+0, "+08:00"=UTC+8, "-05:00"=UTC-5
max_log_lines = 1000   # 超过此行数自动轮转日志
```

### 热重载

修改config.toml后，服务会自动检测并重载配置：
- 验证配置合法性，非法配置拒绝重载并回滚
- 成功重载后推送飞书通知
- 重载失败时自动恢复之前配置

## 命令行参数

| 参数 | 说明 |
|------|------|
| `-w, --webhook` | 飞书WebHook URL |
| `-s, --symbols` | 监控标的 (逗号分隔) |
| `-a, --add-symbol` | 添加标的 |
| `-r, --remove-symbol` | 移除标的 |
| `-l, --list-symbols` | 查看当前标的 |
| `-c, --config` | 配置文件路径 |

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

- `webhook_history.log`: 推送记录，超过 `max_log_lines` 行后自动轮转
- 控制台输出: 服务运行日志

## License

MIT
