"""
CandleSticks 微服务接口定义。

定义了其他模块与 CandleSticks 交互的抽象接口。
实现与接口分离：CandlesticksService 实现 ICandlesticksService。
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Callable

from .models import Kline, Ticker


class ICandlestickService(ABC):
    """
    CandleSticks 微服务抽象接口。

    其他模块通过此接口与 CandleSticks 交互：
    - 同步获取：get_klines / get_latest / get_price
    - 异步订阅：subscribe_klines / subscribe_prices（数据 push 到 callback）
    - 生命周期：fetch_and_cache（启动预热）/ start / stop
    """

    @abstractmethod
    async def get_klines(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 500,
    ) -> list[Kline]:
        """
        获取历史 K 线（从缓存，优先 L1 > L2）。

        Args:
            symbol: 交易对名称，如 "BTCUSDT" 或 "BTCUSDT/ETHUSDT"
            interval: K 线周期，如 "1h", "15m"
            limit: 最大返回条数

        Returns:
            Kline 列表，最老在前
        """

    @abstractmethod
    async def get_latest(self, symbol: str, interval: str = "1h") -> Kline | None:
        """
        获取最新一根 K 线。

        Returns:
            最新 Kline，缓存为空则 None
        """

    @abstractmethod
    async def get_price(self, symbol: str) -> float:
        """
        获取当前价格。

        Returns:
            最新价格，没有数据返回 0.0
        """

    @abstractmethod
    async def subscribe_klines(
        self,
        symbols: list[str],
        intervals: list[str],
        callback: Callable[[str, str, Kline], None],
    ) -> list[asyncio.Task]:
        """
        订阅实时 K 线更新。

        当任一 symbol 任一 interval 有新 K 线（is_closed=True）时，
        回调 callback(symbol, interval, kline)。

        Args:
            symbols: 交易对列表
            intervals: K 线周期列表
            callback: 回调函数，signature: (symbol, interval, Kline)

        Returns:
            asyncio.Task 列表（供 stop 时 cancel）
        """

    @abstractmethod
    async def subscribe_prices(
        self,
        symbols: list[str],
        callback: Callable[[str, float], None],
    ) -> list[asyncio.Task]:
        """
        订阅实时价格更新。

        当任一 symbol 的价格更新时，回调 callback(symbol, price)。

        Args:
            symbols: 交易对列表
            callback: 回调函数，signature: (symbol, price)

        Returns:
            asyncio.Task 列表
        """

    @abstractmethod
    async def fetch_and_cache(
        self,
        symbols: list[str],
        intervals: list[str],
    ):
        """
        启动时从 REST API 预热缓存。

        1. 批量 fetch 所有 symbol + interval 的 K 线
        2. 写入 L1 (cache) 和 L2 (SQLite)
        3. 批量 fetch 所有 symbol 的当前价格

        Args:
            symbols: 交易对列表（不含 components，只含配对名称）
            intervals: K 线周期列表
        """

    @abstractmethod
    async def start(self):
        """
        启动服务：
        1. 连接 WebSocket
        2. 启动 K 线订阅
        3. 启动价格订阅
        4. 启动 REST polling fallback 循环
        """

    @abstractmethod
    async def stop(self):
        """
        停止服务：
        1. 断开 WebSocket
        2. 取消所有订阅任务
        3. 关闭数据库连接
        """

    @abstractmethod
    async def get_all_prices(self) -> dict[str, float]:
        """获取所有已缓存的价格。"""
