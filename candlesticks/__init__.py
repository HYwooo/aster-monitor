"""
CandleSticks 微服务模块。

提供统一的 K 线缓存、微服务，支持 REST/WebSocket 双通道获取，
数据通过 SQLite(L2) + Memory(L1) 两级缓存。

设计原则：
- 微服务通用设计：不耦合具体交易所，数据源 URL 可配置
- PairTrading 支持：自动合并配对 K 线
- 两级缓存：L1(内存) + L2(SQLite)，SQLite 作为 LRU 缓存而非持久存储
- 所有数据变更通过回调通知消费者

使用示例：
```python
from candlesticks import CandleSticksService

cs = CandleSticksService(
    rest_url="https://fapi.asterdex.com",
    ws_url="wss://fstream.asterdex.com/stream",
    proxy="http://127.0.0.1:7890",
)

# 注册配对
cs.register_pair("BTCUSDT/ETHUSDT", "BTCUSDT", "ETHUSDT")

# 注册 K 线回调
async def on_kline(symbol, interval, kline):
    print(f"{symbol} {interval}: close={kline.close}")
cs.subscribe_klines(["BTCUSDT", "BTCUSDT/ETHUSDT"], ["1h", "15m"], on_kline)

# 注册价格回调
async def on_price(symbol, price):
    print(f"{symbol}: {price}")
cs.subscribe_prices(["BTCUSDT", "BTCUSDT/ETHUSDT"], on_price)

# 启动
await cs.fetch_and_cache(["BTCUSDT", "BTCUSDT/ETHUSDT"], ["1h", "15m"])
await cs.start()
```
"""

from .interface import ICandlestickService
from .models import Kline, PairState, Ticker
from .service import CandleSticksService

__all__ = [
    "ICandlestickService",
    "Kline",
    "PairState",
    "Ticker",
    "CandleSticksService",
]
