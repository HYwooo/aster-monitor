"""
REST API 模块 - 通过 HTTP 请求获取交易所行情数据。

Aster DEX 合约行情 API：
- fetch_klines: 获取 K 线历史数据
- fetch_ticker_price: 获取当前价格
"""

import aiohttp


async def fetch_klines(
    symbol: str, limit: int = 500, interval: str = "1h", proxy: str = None
) -> list:
    """
    获取指定交易对的 K 线历史数据。

    API: GET https://fapi.asterdex.com/fapi/v3/klines

    参数:
        symbol: 交易对名称，如 "BTCUSDT"
        limit: K 线数量（默认 500）
        interval: K 线周期（默认 "1h"，支持 1m/5m/15m/1h/4h/1d 等）
        proxy: HTTP 代理 URL（如 "http://127.0.0.1:7890"），可选

    返回:
        K 线数组列表，每条 K 线格式：
        [open_time, open, high, low, close, volume, ...]
        失败或无数据时返回空列表 []
    """
    try:
        url = f"https://fapi.asterdex.com/fapi/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        kwargs = {}
        if proxy:
            kwargs["proxy"] = proxy
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30, **kwargs) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if data else []
                else:
                    return []
    except Exception:
        return []


async def fetch_ticker_price(symbol: str, proxy: str = None) -> float:
    """
    获取指定交易对的当前最新价格。

    API: GET https://fapi.asterdex.com/fapi/v1/ticker/price

    参数:
        symbol: 交易对名称，如 "BTCUSDT"
        proxy: HTTP 代理 URL，可选

    返回:
        当前价格（float），失败或无数据时返回 0.0
    """
    try:
        url = f"https://fapi.asterdex.com/fapi/v1/ticker/price?symbol={symbol}"
        kwargs = {}
        if proxy:
            kwargs["proxy"] = proxy
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10, **kwargs) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("price", 0))
                else:
                    return 0
    except Exception:
        return 0
