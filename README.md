# Aster Monitor

实时监控 Aster DEX (Mainnet) 合约标的价格，通过 WebSocket 连接接收行情数据，使用 ATR Channel（单一标的）或 Clustering SuperTrend（配对交易）检测趋势变化，通过飞书 WebHook 推送信号通知。

---

## 一、架构总览

### 1.1 标的分类

| 类型 | 配置 | 指标 | 追踪止损 |
|------|------|------|----------|
| **SINGLE** | `single_list` | ATR Channel（DEMA EMA ATR 通道突破） | 15m HMA ATR 通道 |
| **PAIR** | `pair_list` | Clustering SuperTrend（K-Means 聚类自适应 ATR） | Clustering TS 追踪止损线 |

当前配置（config.toml）：
```
single_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "MONUSDT", "ZECUSDT"]
pair_list   = ["BTCUSDT/ETHUSDT", "ETHUSDT/SOLUSDT", "BTCUSDT/SOLUSDT"]
```

### 1.2 核心数据流

```
WebSocket ──► Ticker 回调 ──► 更新 mark_prices
                    │
                    └──► _update_pair_price (仅 PAIR)
                              ├──► 计算配对价格 = component1 / component2
                              └──► 触发 check_signals_clustering

          ──► Kline 1h 回调 ──► update_klines
                              ├──► SINGLE: fetch_klines → recalculate_states
                              │                              └──► 计算 ATR Channel → benchmark
                              └──► PAIR: fetch_pair_klines → recalculate_states_clustering
                                                └──► K-Means Clustering SuperTrend → benchmark

          ──► Kline 15m 回调 ──► update_15m_atr
                                        └──► 计算 15m ATR → 更新 trailing_stop 通道

REST _poll_prices (兜底, 每秒):
          ──► SINGLE: REST fetch ticker → check_signals
          ──► PAIR:   REST fetch components → check_signals_clustering
          ──► 若 SINGLE benchmark 未就绪 → 强制 update_klines（避免 WS 漏发导致死锁）
          ──► 若 trailing_stop active → update_15m_atr_from_poll（每15秒）
```

---

## 二、数据缓存结构

### 2.1 kline_cache（内存，WebSocket + REST 双重填充）

```
kline_cache = {
    "BTCUSDT":         [[t, o, h, l, c, v], ...],  # 500 条 1h
    "ETHUSDT":         [[t, o, h, l, c, v], ...],
    "BTCUSDT/ETHUSDT": [[t, ratio_o, ratio_h, ratio_l, ratio_c, v], ...],  # merge 后
}
```

PAIR 的 component klines 优先从 `kline_cache` 读取（如 BTCUSDT 已存在则不再重复 fetch），避免重复 REST 调用。

### 2.2 benchmark（指标计算结果）

```
benchmark["BTCUSDT"] = {
    "st1", "st2",              # Supertrend 双周期
    "ema_s", "ema_u", "ema_l",  # Vegas Tunnel（仅供参考，非信号触发条件）
    "atr1h_upper", "atr1h_lower", "atr1h_ch",  # ATR Channel
    "atr1h_state": (upper, lower, ch),           # ATR Channel 迭代状态
    "kline_time": int,                            # 最新 K 线时间戳
}
```

PAIR 额外包含 Clustering 相关字段：
```
benchmark["BTCUSDT/ETHUSDT"] = {
    ...  # ATR Channel + Supertrend + Vegas（与 SINGLE 相同计算逻辑）
    "ts",                   # Clustering SuperTrend 追踪止损线
    "perf_ama",             # EMA 平滑后的 AMA
    "target_factor",        # K-Means 聚类得出的 ATR 倍数
}
```

### 2.3 trailing_stop（追踪止损状态）

**SINGLE（ATR Channel 模式）**：
```
trailing_stop["BTCUSDT"] = {
    "direction": "LONG" | "SHORT",
    "entry_price": 66000.0,
    "entry_time": 1775120400.0,
    "atr_mult": 1.382,          # 追踪系数
    "atr15m_upper": 66500.0,   # 15m ATR 上轨（由 update_15m_atr 实时更新）
    "atr15m_lower": 65500.0,   # 15m ATR 下轨
    "atr15m_state": (66500, 65500, 1),  # ATR Channel 迭代状态
    "active": True,
    "use_clustering_ts": False,
}
```

**PAIR（Clustering SuperTrend 模式）**：
```
trailing_stop["BTCUSDT/ETHUSDT"] = {
    "direction": "LONG" | "SHORT",
    "entry_price": 32.5,
    "entry_time": 1775120400.0,
    "atr_mult": 1.382,
    "atr15m_upper": 0,
    "atr15m_lower": 0,
    "atr15m_state": (nan, nan, 0),
    "active": True,
    "use_clustering_ts": True,  # 标记使用 Clustering TS
    "clustering_ts": 32.1,       # 实时更新的 Clustering 追踪止损线
}
```

---

## 三、SINGLE — ATR Channel 信号逻辑

适用标的：`single_list` 中的所有币种（如 BTCUSDT、ETHUSDT 等）

### 3.1 ATR Channel 计算（recalculate_states）

使用 500 条 1h K 线，配置参数：
```python
atr1h = DEMA_ATR(high, low, close, period=14)  # 双指数移动平均 ATR
通道倍数 = 1.618
```

计算过程（逐根 K 线迭代）：
```python
prev_state = benchmark[symbol].get("atr1h_state", (nan, nan, 0))
for i in range(len(close)):
    upper, lower, ch = run_atr_channel(close[i], atr1h[i], mult=1.618, prev_state)
    prev_state = (upper, lower, ch)
```

最终结果存入 `benchmark[symbol]`：
- `atr1h_upper` = 最后一次迭代的通道上轨
- `atr1h_lower` = 最后一次迭代的通道下轨
- `atr1h_ch` = 最后一次迭代的状态（-1 空头 / 0 震荡 / 1 多头）

### 3.2 信号检测（check_signals_impl）

**触发条件**：
- `_initialized = True`（初始化完成后才推送信号）
- `mark_price_times[symbol]` 显示价格数据新鲜（< 5 分钟）
- `benchmark[symbol]` 已就绪（有至少 200 条 K 线）

**信号判断**：
```
┌─────────────────────────────────────────────────────────────┐
│  当前价格 >= atr1h_upper  且 上次 atr1h_ch != 1               │
│       → 推送 [symbol] LONG                                   │
│       → 建立追踪止损（direction=LONG, active=True）            │
├─────────────────────────────────────────────────────────────┤
│  当前价格 <= atr1h_lower  且 上次 atr1h_ch != -1             │
│       → 推送 [symbol] SHORT                                  │
│       → 建立追踪止损（direction=SHORT, active=True）           │
└─────────────────────────────────────────────────────────────┘
```

**防重复推送**：每次推送后记录 `last_alert_time[f"ATR_Ch_{symbol}"]`，同一方向需间隔 **3600 秒（1 小时）** 才能再次推送。追踪止损触发后 `last_alert_time` 重置为 0，下次信号立即可推送。

### 3.3 追踪止损（check_trailing_stop + update_15m_atr）

追踪止损在信号推送**后**建立，使用 **15m HMA ATR 通道**，随价格移动不断调整：

**update_15m_atr**（每收到 15m 闭合 K 线时调用）：
```python
# 从 15m K 线 OHLC 计算 ATR
atr15m = HMA_ATR(h, l, c, period=14)

# 用 ATR 和通道倍数计算上下轨
prev_state = trailing_stop[symbol]["atr15m_state"]
upper, lower, ch = run_atr_channel(c, atr15m, atr_mult=1.382, prev_state)

# 更新 trailing_stop
trailing_stop[symbol]["atr15m_upper"] = upper
trailing_stop[symbol]["atr15m_lower"] = lower
trailing_stop[symbol]["atr15m_state"] = (upper, lower, ch)
```

**check_trailing_stop**（每次价格更新时调用）：
```
direction == "LONG" 且 当前价 < atr15m_lower  → 触发止损（推送 TRAILING STOP，停用）
direction == "SHORT" 且 当前价 > atr15m_upper → 触发止损
```

止损线是**动态追踪**的：随着 15m ATR 通道向上移动（下移），止损线也跟着向上移（下移），自动锁定更多利润，而非固定不变。

---

## 四、PAIR — Clustering SuperTrend 信号逻辑

适用标的：`pair_list` 中的所有交易对（如 BTCUSDT/ETHUSDT）

### 4.1 配对价格计算

PAIR 不直接获取价格，而是由两个 component 实时计算比率：

```
BTCUSDT/ETHUSDT = BTCUSDT_mark_price / ETHUSDT_mark_price

配对 K 线合并（fetch_pair_klines，使用 kline_cache 复用）：
    BTC1h = kline_cache["BTCUSDT"]（或 REST fetch）
    ETH1h = kline_cache["ETHUSDT"]（或 REST fetch）
    按时间戳对齐后：
        ratio_open  = open_BTC / open_ETH
        ratio_high  = max(ratio_open, ratio_close)
        ratio_low   = min(ratio_open, ratio_close)
        ratio_close = close_BTC / close_ETH
```

合并后的 ratio K 线存入 `kline_cache["BTCUSDT/ETHUSDT"]`，供指标计算使用。

### 4.2 Clustering SuperTrend 原理（PineScript V6 移植版）

参考：LuxAlgo SuperTrend AI (Clustering) — https://www.tradingview.com/scripts/oxT6MfOv/

**核心思想**：不依赖单一固定 ATR 倍数，而是用 K-Means 聚类从多个候选 ATR 倍数中自适应选择最优值。

**当前配置参数**：
```python
min_mult = 1.0, max_mult = 5.0, step = 0.5  # 候选倍数：1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0
perf_alpha = 10.0   # perf EMA 平滑系数
from_cluster = "Best"  # 选 perf 均值最大的 cluster
history_klines = 500  # 用于 K-Means 的历史 K 线数量
```

**Step 1 — 批量 SuperTrend 生成**

对 500 根历史 K 线，用 9 个候选 ATR 倍数同步计算 SuperTrend：
```python
factors = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]

for each kline_i:
    for each factor_j:
        atr_val = ATR(kline_i)
        up = hl2 + atr_val * factor_j
        dn = hl2 - atr_val * factor_j
        # 更新 SuperTrend 方向：多头=1，空头=-1
        trends_j[i] = (close > up) ? 1 : (close < dn) ? -1 : trends_j[i-1]
        outputs_j[i] = (trends_j[i] == 1) ? lower_j : upper_j

    # 计算趋势强度 perf = EMA(price_diff * sign, alpha)
    perf_i = EMA((close_i - close_{i-1}) * sign, alpha=10)
    perfs_j[i] = perf_i  # 所有 factor 共用同一个 perf（同根 K 线）
```

**Step 2 — K-Means 聚类（K=3）**

```python
# 初始化质心为 perf 的 Q25 / Q50 / Q75
centroids = [percentile(perfs, 25), percentile(perfs, 50), percentile(perfs, 75)]

for iter in range(max_iter):
    # 分配每个 factor 到最近质心
    clusters = [[], [], []]
    for each factor_j:
        distances = [abs(perf_j - c) for c in centroids]
        cluster_idx = argmin(distances)
        clusters[cluster_idx].append(factor_j)

    # 重新计算质心
    new_centroids = [mean(cluster) for cluster in clusters]
    if new_centroids == centroids: break
    centroids = new_centroids
```

**Step 3 — 目标倍数与追踪止损**

```python
# 选 "Best" cluster = perf 均值最大的 cluster（趋势最强）
best_cluster_idx = argmax([mean(perfs_cluster_0), mean(perfs_cluster_1), mean(perfs_cluster_2)])
target_factors = clusters[best_cluster_idx]
target_factor = mean(target_factors)  # 例：3.0

# 用 target_factor 计算最终 SuperTrend
atr_last = ATR[-1]
hl2 = (high[-1] + low[-1]) / 2
up = hl2 + atr_last * target_factor
dn = hl2 - atr_last * target_factor

trend = (close > up) ? 1 : (close < dn) ? -1 : previous_trend
ts = (trend == 1) ? lower : upper  # 追踪止损线
perf_ama = EMA(ts, perf_idx)        # EMA 平滑后的 AMA
```

### 4.3 信号检测（check_signals_clustering_impl）

**触发条件**：
- `_initialized = True`
- `mark_price_times[symbol]` 显示价格新鲜（< 5 分钟）
- `benchmark[symbol]` 已就绪

**信号判断**：
```
┌──────────────────────────────────────────────────────────────┐
│  Clustering State.trend: -1（空头）→ +1（多头）                  │
│       → 推送 [symbol] LONG                                    │
│       → 建立追踪止损（direction=LONG, use_clustering_ts=True）  │
├──────────────────────────────────────────────────────────────┤
│  Clustering State.trend: +1（多头）→ -1（空头）                  │
│       → 推送 [symbol] SHORT                                   │
│       → 建立追踪止损（direction=SHORT, use_clustering_ts=True） │
└──────────────────────────────────────────────────────────────┘
```

注意：PAIR 的 **ATR Channel 仍然计算**（用于 `_print_status` 和飞书推送显示），但信号触发仅依赖 **Clustering trend 切换**。

**防重复推送**：每次推送后记录 `last_alert_time[f"ClusterST_{symbol}"]`，同一方向需间隔 **3600 秒**。追踪止损触发后 `last_alert_time` 重置为 0，下次信号立即可推送。

**Clustering TS 实时同步**：
每次 `check_signals_clustering_impl` 被调用时，将 `benchmark["ts"]`（最新 Clustering 追踪止损线）同步到 `trailing_stop["clustering_ts"]`，确保追踪止损线随行情实时移动。

### 4.4 Clustering 追踪止损（check_trailing_stop）

**check_trailing_stop**（每次 PAIR 价格更新时调用）：
```
direction == "LONG" 且 当前价 < clustering_ts  → 触发止损
direction == "SHORT" 且 当前价 > clustering_ts  → 触发止损
```

与 SINGLE 的 15m ATR 通道不同，PAIR 的追踪止损直接使用 **Clustering SuperTrend 追踪止损线（ts）**，这是 K-Means 自适应计算的最优止损位置。

---

## 五、WebSocket 与 REST Polling 策略

### 5.1 WS 优先原则

| 数据类型 | 更新来源 | 说明 |
|---------|---------|------|
| Ticker（价格） | WebSocket | 实时推送，`mark_prices` 实时更新 |
| 1h Kline（指标计算） | WebSocket | 闭合时（`x=True`）触发 `update_klines` → 指标重算 |
| 15m Kline（追踪止损） | WebSocket | 闭合时触发 `update_15m_atr` → 通道更新 |

### 5.2 REST Polling 兜底

`_poll_prices` 每秒运行，**WS 不可用或数据不新鲜（> 5s）时**触发 REST：

```
for SINGLE symbol:
    if current_time - mark_price_times[symbol] > 5:
        REST fetch ticker → 更新 mark_prices
        # 关键修复：如果 benchmark 未就绪，无条件调用 update_klines
        # （避免 WS 1h kline 漏发导致 benchmark 永远死锁）
        → check_signals
        → check_trailing_stop

for PAIR symbol:
    REST fetch component1 & component2
    计算配对价格
    → check_signals_clustering
    → check_trailing_stop

每 15 秒（若 trailing_stop active）：
    update_15m_atr_from_poll（REST fetch 20 条 15m K 线 → 计算 ATR → 更新通道）
```

### 5.3 初始化流程（initialize）

```
1. _initialized = False（防止初始化期间误推信号）
2. 对所有 symbols（ SINGLE + PAIR）调用 update_klines（REST fetch 500 条 1h K 线）
3. SINGLE → recalculate_states（计算 ATR Channel）
   PAIR   → recalculate_states_clustering（K-Means Clustering SuperTrend）
4. _initialized = True
5. 将 benchmark 未就绪的 symbols 加入 _pending_status（由 _poll_prices 后续处理）
6. 所有 symbols 就绪后 → _send_startup_summary（推送启动摘要）
```

---

## 六、配置说明（config.toml）

```toml
[webhook]
url = "https://open.feishu.cn/open-apis/bot/v2/hook/..."
format = "card"         # card 或 tradingview

[symbols]
# monitor_list 已废弃，请使用 single_list + pair_list
single_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "MONUSDT", "ZECUSDT"]  # ATR Channel
pair_list   = ["BTCUSDT/ETHUSDT", "ETHUSDT/SOLUSDT", "BTCUSDT/SOLUSDT"]              # Clustering SuperTrend

[atr_1h]                 # 1h ATR Channel 参数（用于 SINGLE 和 PAIR 的通道显示）
ma_type = "DEMA"         # 双指数移动平均
period  = 14             # ATR 周期
mult   = 1.618          # 通道倍数

[atr_15m]               # 15m 追踪止损 ATR 参数（仅 SINGLE 使用）
ma_type = "HMA"          # 赫尔移动平均
period  = 14
mult   = 1.382          # 追踪系数（ATR Channel 倍数）

[clustering_st]          # Clustering SuperTrend 参数（仅 PAIR 使用）
min_mult   = 1.0         # ATR 倍数范围下界
max_mult   = 5.0         # ATR 倍数范围上界
step       = 0.5         # 步长（共 9 个候选：1.0~5.0）
perf_alpha = 10.0        # perf EMA 平滑系数
from_cluster = "Best"   # 选哪个 cluster: "Best" | "Average" | "Worst"
max_iter = 1000          # K-Means 最大迭代次数
history_klines = 500    # 聚类用 K 线历史条数

[service]
heartbeat_file = "heartbeat"
heartbeat_timeout = 120

[proxy]
enable = false           # true 时使用 url 代理
url = "http://127.0.0.1:7890"

[report]
enable = true
times  = ["08:00", "20:00"]  # 每日报告时间

[settings]
timezone = "+08:00"      # Z=UTC+0, "+08:00"=UTC+8
max_log_lines = 1000     # 超过此行数自动轮转 webhook_history.log
```

---

## 七、飞书推送格式

### 7.1 信号类型汇总

| 消息类型 | 触发条件 | 示例 |
|---------|---------|------|
| `ATR_Ch` LONG | SINGLE 多头突破 atr1h_upper | `[BTCUSDT] LONG` |
| `ATR_Ch` SHORT | SINGLE 空头突破 atr1h_lower | `[BTCUSDT] SHORT` |
| `ATR_Ch` TRAILING STOP | SINGLE 15m ATR 止损触发 | `[BTCUSDT] TRAILING STOP` |
| `ClusterST` LONG | PAIR Clustering trend 空→多 | `[BTCUSDT/ETHUSDT] LONG` |
| `ClusterST` SHORT | PAIR Clustering trend 多→空 | `[BTCUSDT/ETHUSDT] SHORT` |
| `ClusterST` TRAILING STOP | PAIR Clustering TS 触发 | `[BTCUSDT/ETHUSDT] TRAILING STOP` |
| `SYSTEM` | 连接/重连/错误 | `Aster Monitor connected to Mainnet` |
| `CONFIG` | 热重载成功/失败 | `Hot reload successful` |
| `REPORT` | 每日报告 | `Alert count in last 24h: 5` |

### 7.2 推送内容字段

**ATR_Ch LONG / SHORT**:
```
symbol:     BTCUSDT
direction: LONG
price:     66,500.0
atr_upper: 66,300.0    ← 1h ATR 通道上轨
atr_lower: 65,800.0    ← 1h ATR 通道下轨
```

**TRAILING STOP**:
```
symbol:     BTCUSDT
direction:  LONG
price:      65,800.0      ← 当前价格（触发止损）
stop_line:  65,500.0      ← 触发的止损线
entry_price: 66,000.0    ← 入场价格
reason:     trailing_stop
```

**ClusterST LONG / SHORT**:
```
symbol:       BTCUSDT/ETHUSDT
direction:    LONG
price:        32.54
ts:           32.12           ← Clustering 追踪止损线
perf_ama:     32.20
target_factor: 3.0            ← K-Means 选中的 ATR 倍数
```

---

## 八、命令行控制

运行时可输入命令（stdin）：

| 命令 | 说明 |
|------|------|
| `stop print` | 暂停每 2 分钟的状态打印 |
| `resume print` | 恢复状态打印 |
| `status` | 打印当前警报计数和打印状态 |

---

## 九、已知问题修复记录

1. **`is_pair_trading` 参数命名冲突 bug**（v2）：`update_klines` 的参数 `is_pair_trading` 本应是函数，但直接 `if is_pair_trading:` 判断的是函数对象本身（永远 truthy），导致所有 symbols 都走 PAIR 分支，`fetch_pair_klines` split 对单标崩溃被静默吞掉。修复：参数改名为 `is_pair_trading_fn`，调用时 `is_pair_trading_fn(symbol)` 取布尔值。

2. **SINGLE benchmark 初始化死锁**（v2）：WS ticker 持续推送价格，`_poll_prices` 因 `last_update < 5s` 跳过 REST fetch 和 `update_klines`，SINGLE benchmark 永远为空。修复：将对 benchmark 的检查移出价格新鲜度判断，无 benchmark 的 SINGLE 强制通过 REST 刷新 klines。

3. **Clustering 追踪止损失效**（v3）：PAIR 建仓时设置了 `use_clustering_ts=True` 和 `clustering_ts`，但 `check_trailing_stop` 只检查 `atr15m_upper/lower`（全为 0）， Clustering TS 从未被使用。修复：新增 `use_clustering_ts=True` 分支，直接用 `clustering_ts` 作为止损线判断。

4. **Clustering TS 不同步**（v3）：`clustering_ts` 在建仓时固定，但 `benchmark["ts"]` 随每根新 K 线更新，`trailing_stop["clustering_ts"]` 未同步。修复：在 `check_signals_clustering_impl` 每次调用时同步 `trailing_stop["clustering_ts"] = benchmark["ts"]`。

5. **追踪止损触发后未重置 alert timer**（v3）：`check_trailing_stop` 触发后只设置 `active=False`，但 `last_alert_time` 未重置，导致下次同向信号要等 3600 秒才能推送。修复：触发时将 `last_alert_time[symbol] = 0`，下次信号立即可推送。

---

## 十、文件结构

```
aster-monitor/
├── main.py                     # 入口：python main.py
├── notification_service.py     # 兼容层（re-exports from submodules）
├── config.toml                # 运行时配置
├── config.example.toml
├── README.md                  # 本文件
├── requirements.txt
├── config/
│   ├── __init__.py
│   └── manager.py             # load_config, create_config, update_symbols
├── indicators/
│   ├── __init__.py
│   ├── calculations.py        # ATR, Supertrend, Vegas Tunnel, ATR Channel
│   └── clustering.py         # Clustering SuperTrend (K-Means)
├── notifications/
│   ├── __init__.py
│   ├── formatters.py          # format_number
│   └── webhook.py             # build_feishu_card, send_webhook
├── rest_api/
│   ├── __init__.py
│   └── client.py             # fetch_klines, fetch_ticker_price
├── websocket/
│   ├── __init__.py
│   └── subscriptions.py      # create_single_symbol_callbacks, create_pair_trading_callbacks
├── signals/
│   ├── __init__.py
│   ├── detection.py          # check_signals, check_signals_clustering, check_trailing_stop
│   └── breakout.py           # 突破监控（SINGLE 专用）
├── service/
│   ├── __init__.py
│   └── notification_service.py  # 主服务类 NotificationService
└── candlesticks/            # 微服务骨架（待集成 ICandlestickService）
    ├── __init__.py
    ├── models.py
    ├── persistence.py
    ├── cache.py
    ├── rest_client.py
    ├── ws_client.py
    ├── pair_merger.py
    ├── service.py
    └── interface.py
```
