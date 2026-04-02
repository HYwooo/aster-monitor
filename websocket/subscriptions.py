"""
WebSocket 订阅模块 - 提供回调工厂函数，供 NotificationService 注册 WebSocket 事件处理。

架构说明：
- 回调函数由 create_*_callbacks 工厂生成
- 回调注册到 asterdex SDK 的 client.on_ticker / client.on_kline
- SDK 在收到消息时会调用这些回调
- 回调内部将 async 逻辑通过 asyncio.create_task 调度到事件循环
  （因为 SDK 的回调是同步的，不能直接 await async 函数）
"""

import asyncio
import time


def create_single_symbol_callbacks(
    symbol: str,
    mark_prices: dict,
    mark_price_times: dict,
    check_trailing_stop_fn,
    check_signals_fn,
    update_klines_fn,
    update_15m_atr_fn,
    warn_fn,
):
    """
    为单一交易对创建 WebSocket 回调函数集。

    参数:
        symbol: 交易对名称，如 "BTCUSDT"
        mark_prices: 价格缓存字典 {symbol: price}
        mark_price_times: 价格更新时间戳字典 {symbol: unix_time}
        check_trailing_stop_fn: async 函数，触发追踪止损检查
        check_signals_fn: async 函数，触发信号检查
        update_klines_fn: async 函数，触发 K 线更新
        update_15m_atr_fn: async 函数，触发 15m ATR 更新
        warn_fn: 警告日志函数 fn(msg: str, ctx: str)

    返回:
        (on_ticker, on_kline_1h, on_kline_15m) 三个回调函数
    """

    def on_ticker(data):
        """
        处理 WebSocket ticker 推送。
        更新 mark_prices 和 mark_price_times，然后调度信号检查。
        """
        try:
            price = float(data.get("c", data.get("lastPrice", 0)))
            if price > 0:
                mark_prices[symbol] = price
                mark_price_times[symbol] = time.time()
                # 调度 async 函数：追踪止损检查和信号检查
                asyncio.create_task(check_trailing_stop_fn(symbol, price))
                asyncio.create_task(check_signals_fn(symbol))
        except Exception as e:
            warn_fn(f"ticker callback error: {e}", f"symbol={symbol}")

    def on_kline_1h(kline):
        """
        处理 WebSocket 1h K 线推送。
        仅在 K 线完成 (x=True) 时触发 K 线更新。
        """
        try:
            if kline.get("x", False):
                asyncio.create_task(update_klines_fn(symbol))
        except Exception as e:
            warn_fn(f"kline callback error: {e}", f"symbol={symbol}")

    def on_kline_15m(kline):
        """
        处理 WebSocket 15m K 线推送。
        仅在 K 线完成 (x=True) 时触发 15m ATR 更新。
        """
        try:
            if kline.get("x", False):
                asyncio.create_task(update_15m_atr_fn(symbol, kline))
        except Exception as e:
            warn_fn(f"15m kline callback error: {e}", f"symbol={symbol}")

    return on_ticker, on_kline_1h, on_kline_15m


def create_pair_trading_callbacks(
    symbol: str,
    symbol1: str,
    symbol2: str,
    mark_prices: dict,
    mark_price_times: dict,
    pair_components: dict,
    breakout_comp_prices: dict,
    update_pair_price_fn,
    update_klines_fn,
    update_15m_atr_fn,
    warn_fn,
):
    """
    为配对交易创建 WebSocket 回调函数集。

    参数:
        symbol: 配对交易对名称，如 "BTCUSDT/ETHUSDT"
        symbol1: 配对第一个交易对，如 "BTCUSDT"
        symbol2: 配对第二个交易对，如 "ETHUSDT"
        mark_prices: 价格缓存字典
        mark_price_times: 价格更新时间戳字典
        pair_components: 配对组件字典 {symbol: [symbol1, symbol2]}
        breakout_comp_prices: 配对交易组件价格缓存（用于计算配对价格）
        update_pair_price_fn: async 函数，计算并更新配对价格
        update_klines_fn: async 函数，触发 K 线更新
        update_15m_atr_fn: async 函数，触发 15m ATR 更新
        warn_fn: 警告日志函数

    返回:
        (on_ticker, on_kline_1h, on_15m_kline_p1, on_15m_kline_p2) 四个回调函数
    """
    # 记录配对组件关系
    pair_components[symbol] = [symbol1, symbol2]

    def on_ticker(data):
        """
        处理 WebSocket ticker 推送（两个组件的交易对共用此回调）。
        更新 symbol1 或 symbol2 的价格，然后调度配对价格计算。
        """
        try:
            ticker_sym = data.get("s", "").upper()
            price = float(data.get("c", data.get("lastPrice", 0)))
            if price <= 0:
                return
            mark_prices[ticker_sym] = price
            mark_price_times[ticker_sym] = time.time()
            # 调度 async 函数：计算配对价格
            asyncio.create_task(update_pair_price_fn(symbol, symbol1, symbol2))
        except Exception as e:
            warn_fn(f"pair ticker error: {e}", f"pair={symbol}")

    def on_kline_1h(kline):
        """
        处理 WebSocket 1h K 线推送（仅 symbol1 的 1h K 线触发更新）。
        配对交易对共用 symbol1 的 K 线。
        """
        try:
            if kline.get("x", False):
                asyncio.create_task(update_klines_fn(symbol))
        except Exception as e:
            warn_fn(f"pair kline error: {e}", f"pair={symbol}")

    def on_15m_kline_p1(kline):
        """
        处理 WebSocket 15m K 线推送（symbol1 的 15m K 线）。
        触发 15m ATR 更新。
        """
        try:
            if kline.get("x", False):
                asyncio.create_task(update_15m_atr_fn(symbol, kline))
        except Exception as e:
            warn_fn(f"pair 15m kline error: {e}", f"pair={symbol}")

    def on_15m_kline_p2(kline):
        """
        处理 WebSocket 15m K 线推送（symbol2 的 15m K 线）。
        触发 15m ATR 更新。
        """
        try:
            if kline.get("x", False):
                asyncio.create_task(update_15m_atr_fn(symbol, kline))
        except Exception as e:
            warn_fn(f"pair 15m kline2 error: {e}", f"pair={symbol}")

    return on_ticker, on_kline_1h, on_15m_kline_p1, on_15m_kline_p2


def subscribe_single_symbol(client, symbol, callbacks):
    """
    将单一交易对的回调注册到 WebSocket client。

    参数:
        client: asterdex WebSocketClient 实例
        symbol: 交易对名称
        callbacks: create_single_symbol_callbacks 返回的回调元组
    """
    on_ticker, on_kline_1h, on_kline_15m = callbacks
    client.on_ticker(symbol)(on_ticker)
    client.on_kline(symbol, "1h")(on_kline_1h)
    client.on_kline(symbol, "15m")(on_kline_15m)


def subscribe_pair_trading(client, symbol, symbol1, symbol2, callbacks):
    """
    将配对交易对的回调注册到 WebSocket client。

    参数:
        client: asterdex WebSocketClient 实例
        symbol: 配对交易对名称
        symbol1: 配对第一个交易对
        symbol2: 配对第二个交易对
        callbacks: create_pair_trading_callbacks 返回的回调元组
    """
    on_ticker, on_kline_1h, on_15m_kline_p1, on_15m_kline_p2 = callbacks
    client.on_ticker(symbol1)(on_ticker)
    client.on_ticker(symbol2)(on_ticker)
    client.on_kline(symbol1, "1h")(on_kline_1h)
    client.on_kline(symbol1, "15m")(on_15m_kline_p1)
    client.on_kline(symbol2, "15m")(on_15m_kline_p2)


async def subscribe_all_tickers(client, symbols, is_pair_trading_fn):
    """
    批量订阅所有交易对的 ticker 流。

    参数:
        client: asterdex WebSocketClient 实例
        symbols: 所有交易对名称列表
        is_pair_trading_fn: 判断是否为配对交易对的函数 fn(symbol) -> bool
    """
    params = []
    for sym in symbols:
        if is_pair_trading_fn(sym):
            parts = sym.split("/")
            params.append(f"{parts[0].lower()}@ticker")
            params.append(f"{parts[1].lower()}@ticker")
        else:
            params.append(f"{sym.lower()}@ticker")
    await client.subscribe_batch(params)
