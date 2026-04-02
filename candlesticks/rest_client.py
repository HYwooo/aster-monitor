"""
CandleSticks REST API 客户端。

通过 HTTP 请求获取交易所 K 线数据。
这是通用设计，数据源 URL 可配置。
"""

import aiohttp

from .models import Kline


class RestClient:
    """
    通用 REST API 客户端。

    数据源配置：
    - base_url: REST API 根地址
    - proxy: HTTP 代理 URL（如 "http://127.0.0.1:7890"），可选
    """

    def __init__(
        self,
        base_url: str = "https://fapi.asterdex.com",
        proxy: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.proxy = proxy

    async def fetch_klines(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 500,
    ) -> list[Kline]:
        """
        获取 K 线历史数据。

        Args:
            symbol: 交易对名称，如 "BTCUSDT"
            interval: K 线周期，如 "1h", "15m", "5m"
            limit: 最大返回条数（交易所限制一般 ≤ 1500）

        Returns:
            Kline 列表，最老在前（按 open_time ASC 排序）
        """
        url = f"{self.base_url}/fapi/v3/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(limit, 1500),
        }
        kwargs = {}
        if self.proxy:
            kwargs["proxy"] = self.proxy

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=30, **kwargs
                ) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()
                    if not data:
                        return []
                    return [Kline.from_rest(symbol, interval, k) for k in data]
        except Exception:
            return []

    async def fetch_ticker_price(self, symbol: str) -> float:
        """
        获取当前最新价格。

        Args:
            symbol: 交易对名称，如 "BTCUSDT"

        Returns:
            最新价格，失败返回 0.0
        """
        url = f"{self.base_url}/fapi/v1/ticker/price"
        params = {"symbol": symbol.upper()}
        kwargs = {}
        if self.proxy:
            kwargs["proxy"] = self.proxy

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params, timeout=10, **kwargs
                ) as resp:
                    if resp.status != 200:
                        return 0.0
                    data = await resp.json()
                    return float(data.get("price", 0.0))
        except Exception:
            return 0.0

    async def fetch_prices(self, symbols: list[str]) -> dict[str, float]:
        """
        批量获取价格。

        Returns:
            {symbol: price} 字典
        """
        result = {}
        for sym in symbols:
            price = await self.fetch_ticker_price(sym)
            if price > 0:
                result[sym] = price
        return result
