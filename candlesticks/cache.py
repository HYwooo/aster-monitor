"""
CandleSticks L1 内存缓存。

设计：
- kline_cache: {symbol: {interval: [Kline, ...]}}  — 有序列表，最老在前
- price_cache: {symbol: Ticker}                    — 最新价格
- PairTrading 状态下，symbol = "BTCUSDT/ETHUSDT" 的 klines 存储的是合并后的 ratio K 线

淘汰策略：
- 内存中最多保留 max_klines_per_symbol_interval 条（默认 2000）
- 超出时丢弃最老的 K 线
- 与 L2 (SQLite) 配合，SQLite 存 1000 条，内存可以略多保证性能
"""

import threading
import time
from typing import Optional

from .models import Kline, Ticker

DEFAULT_MAX_IN_MEMORY = 2000


class CandleCache:
    """
    L1 内存缓存。

    线程安全：读写操作通过 threading.Lock 保护。
    """

    def __init__(self, max_in_memory: int = DEFAULT_MAX_IN_MEMORY):
        self.max_in_memory = max_in_memory
        # kline_cache: {symbol: {interval: list[Kline]}}  — 有序，最老在前
        self.kline_cache: dict[str, dict[str, list[Kline]]] = {}
        # price_cache: {symbol: Ticker}
        self.price_cache: dict[str, Ticker] = {}
        # price_update_times: {symbol: unix_timestamp}  — 用于判断价格是否过期
        self.price_update_times: dict[str, float] = {}
        # kline_staleness: {symbol: {interval: open_time of last kline}} — 判断 WS 是否还有推送
        self._lock = threading.Lock()

    # ==================== K Line Cache ====================

    def append_kline(self, kline: Kline) -> bool:
        """
        添加一根 K 线到缓存。

        返回:
            True 表示这是新 K 线（open_time > 已有的最新），False 表示是更新
        """
        sym = kline.symbol
        intv = kline.interval
        with self._lock:
            if sym not in self.kline_cache:
                self.kline_cache[sym] = {}
            if intv not in self.kline_cache[sym]:
                self.kline_cache[sym][intv] = []

            arr = self.kline_cache[sym][intv]
            is_new = True
            if arr:
                last = arr[-1]
                if kline.open_time == last.open_time:
                    # 同 open_time，更新最新 K 线
                    arr[-1] = kline
                    is_new = False
                elif kline.open_time < last.open_time:
                    # 比最新还老，忽略（乱序）
                    is_new = False
                else:
                    # 新 K 线，append
                    arr.append(kline)
                    is_new = True
            else:
                arr.append(kline)

            # 淘汰：超过 max_in_memory 条时丢弃最老的
            if len(arr) > self.max_in_memory:
                excess = len(arr) - self.max_in_memory
                self.kline_cache[sym][intv] = arr[excess:]

            return is_new

    def get_klines(self, symbol: str, interval: str, limit: int = 500) -> list[Kline]:
        """
        获取 K 线列表（最老在前，方便指标计算）。

        Returns:
            list[Kline]，最老在前
        """
        with self._lock:
            arr = self.kline_cache.get(symbol, {}).get(interval, [])
            return list(arr[-limit:]) if limit < len(arr) else list(arr)

    def get_latest_kline(self, symbol: str, interval: str) -> Optional[Kline]:
        """获取最新一根 K 线（最新时间戳）。"""
        with self._lock:
            arr = self.kline_cache.get(symbol, {}).get(interval, [])
            return arr[-1] if arr else None

    def get_latest_open_time(self, symbol: str, interval: str) -> Optional[int]:
        """获取最新 K 线的 open_time。"""
        k = self.get_latest_kline(symbol, interval)
        return k.open_time if k else None

    def set_klines(self, symbol: str, interval: str, klines: list[Kline]):
        """
        批量设置 K 线（用于从 DB 或 REST 加载时）。
        直接替换现有列表，不做 merge。
        """
        with self._lock:
            if symbol not in self.kline_cache:
                self.kline_cache[symbol] = {}
            # 保证最老在前
            self.kline_cache[symbol][interval] = list(
                sorted(klines, key=lambda k: k.open_time)
            )

    def has_data(self, symbol: str, interval: str) -> bool:
        """判断是否有 K 线数据。"""
        with self._lock:
            arr = self.kline_cache.get(symbol, {}).get(interval, [])
            return len(arr) > 0

    # ==================== Price Cache ====================

    def update_price(self, ticker: Ticker):
        """更新最新价格。"""
        with self._lock:
            self.price_cache[ticker.symbol] = ticker
            self.price_update_times[ticker.symbol] = time.time()

    def get_price(self, symbol: str) -> Optional[float]:
        """获取最新价格。"""
        with self._lock:
            t = self.price_cache.get(symbol)
            return t.price if t else None

    def get_price_staleness(self, symbol: str) -> float:
        """
        返回价格最后更新时间距离现在的秒数。
        如果没有数据，返回 float('inf')。
        """
        with self._lock:
            t = self.price_update_times.get(symbol)
            if t is None:
                return float("inf")
            return time.time() - t

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        """批量获取价格。"""
        with self._lock:
            return {
                sym: t.price for sym, t in self.price_cache.items() if sym in symbols
            }

    # ==================== Stats ====================

    def stats(self) -> dict:
        """返回缓存统计信息。"""
        with self._lock:
            total_klines = sum(
                len(arr)
                for intervals in self.kline_cache.values()
                for arr in intervals.values()
            )
            return {
                "symbols": list(self.kline_cache.keys()),
                "total_klines": total_klines,
                "prices": len(self.price_cache),
            }
