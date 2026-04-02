"""
CandleSticks WebSocket 客户端。

订阅交易所实时 K线和价格推送。
指数退避重连机制，支持多个 symbol 的批量订阅。
"""

import asyncio
import json
import time
from typing import Callable, Optional

import aiohttp

from asterdex.logging_config import get_logger
from .models import Kline, Ticker

logger = get_logger(__name__)


class WsKlineCallback:
    """K 线 WebSocket 回调处理器。"""

    def __init__(
        self,
        symbol: str,
        interval: str,
        on_kline: Callable[[Kline], None],
        on_error: Optional[Callable[[str], None]] = None,
    ):
        self.symbol = symbol
        self.interval = interval
        self.on_kline = on_kline
        self.on_error = on_error


class WsPriceCallback:
    """价格 WebSocket 回调处理器。"""

    def __init__(
        self,
        symbol: str,
        on_ticker: Callable[[Ticker], None],
        on_error: Optional[Callable[[str], None]] = None,
    ):
        self.symbol = symbol
        self.on_ticker = on_ticker
        self.on_error = on_error


class WebSocketClient:
    """
    WebSocket 客户端，支持 K线和Ticker订阅。

    功能：
    - 订阅/取消订阅 K线流（多 symbol 多 interval）
    - 订阅/取消订阅 Ticker 流（多 symbol）
    - 自动重连（指数退避：1s→2s→4s→...→60s）
    - 心跳保活

    数据流：
    on_ws_message() 解析数据
      → kline_stream 回调（外部传入的 on_kline_fn）
      → ticker_stream 回调（外部传入的 on_ticker_fn）
    """

    def __init__(
        self,
        ws_url: str = "wss://fstream.asterdex.com/stream",
        proxy: Optional[str] = None,
    ):
        self.ws_url = ws_url
        self.proxy = proxy
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._listen_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._reconnect_attempts = 0
        self._reconnect_base = 1
        self._reconnect_max = 60

        # 订阅映射: {(symbol, interval): on_kline_fn}
        self._kline_cbs: dict[tuple[str, str], Callable[[Kline], None]] = {}
        # Ticker 回调: {symbol: on_ticker_fn}
        self._price_cbs: dict[str, Callable[[Ticker], None]] = {}

        self._lock = asyncio.Lock()

    # ==================== Public API ====================

    def on_kline(self, symbol: str, interval: str) -> Callable:
        """
        装饰器：注册 K 线回调。

        Usage:
            @client.on_kline("BTCUSDT", "1h")
            def handle(kline: Kline):
                print(kline.close)

        Returns:
            装饰器函数
        """

        def decorator(fn: Callable[[Kline], None]):
            self._kline_cbs[(symbol.upper(), interval)] = fn
            return fn

        return decorator

    def on_ticker(self, symbol: str) -> Callable:
        """
        装饰器：注册 Ticker 回调。

        Usage:
            @client.on_ticker("BTCUSDT")
            def handle(ticker: Ticker):
                print(ticker.price)
        """

        def decorator(fn: Callable[[Ticker], None]):
            self._price_cbs[symbol.upper()] = fn
            return fn

        return decorator

    async def subscribe_klines(
        self,
        symbols_intervals: list[tuple[str, str]],
    ):
        """
        批量订阅 K 线流。

        Args:
            symbols_intervals: [(symbol, interval), ...]，如 [("BTCUSDT", "1h"), ("ETHUSDT", "15m")]
        """
        params = []
        for sym, intv in symbols_intervals:
            sym = sym.upper()
            self._kline_cbs[(sym, intv)] = self._kline_cbs.get(
                (sym, intv), lambda k: None
            )
            params.append(f"{sym.lower()}@kline_{intv}")

        if params:
            await self._subscribe(params)

    async def subscribe_tickers(self, symbols: list[str]):
        """
        批量订阅 Ticker 流。

        Args:
            symbols: ["BTCUSDT", "ETHUSDT", ...]
        """
        params = [s.upper().lower() + "@ticker" for s in symbols]
        if params:
            await self._subscribe(params)

    async def unsubscribe_klines(self, symbols_intervals: list[tuple[str, str]]):
        """取消订阅 K 线流。"""
        params = []
        for sym, intv in symbols_intervals:
            self._kline_cbs.pop((sym.upper(), intv), None)
            params.append(f"{sym.lower()}@kline_{intv}")
        if params:
            await self._unsubscribe(params)

    async def unsubscribe_tickers(self, symbols: list[str]):
        """取消订阅 Ticker 流。"""
        for s in symbols:
            self._price_cbs.pop(s.upper(), None)

    # ==================== Connection Management ====================

    async def connect(self):
        """建立 WebSocket 连接。"""
        self._running = True
        self._session = aiohttp.ClientSession()
        headers = {"X-LUX-INTERVAL": "1000"}
        kwargs = {}
        if self.proxy:
            kwargs["proxy"] = self.proxy
        self._ws = await self._session.ws_connect(
            self.ws_url,
            headers=headers,
            timeout=aiohttp.ClientWSTimeout(heartbeat=30),
            **kwargs,
        )
        self._reconnect_attempts = 0
        logger.info(f"[WS] Connected to {self.ws_url}")
        # 重连后重新订阅
        await self._resubscribe_all()

    async def disconnect(self):
        """断开 WebSocket 连接。"""
        self._running = False
        if self._listen_task:
            self._listen_task.cancel()
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        self._ws = None
        self._session = None
        logger.info("[WS] Disconnected")

    async def _resubscribe_all(self):
        """重新订阅所有流（在连接/重连后）。"""
        if self._kline_cbs:
            items = list(self._kline_cbs.keys())
            params = [f"{sym.lower()}@kline_{intv}" for sym, intv in items]
            await self._subscribe(params)
        if self._price_cbs:
            params = [s.lower() + "@ticker" for s in self._price_cbs.keys()]
            await self._subscribe(params)

    # ==================== Message Handling ====================

    async def _run(self):
        """
        主循环：
        1. 监听 WebSocket 消息
        2. 解析并路由到对应回调
        3. 断线时自动重连
        """
        while self._running:
            if self._ws is None:
                await self._do_reconnect()
                if not self._running:
                    break
                continue

            try:
                msg = await self._ws.receive(timeout=30)
                if msg.type == aiohttp.WSMsgType.PONG:
                    continue
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.warning(f"[WS] Error: {self._ws.exception()}")
                    await self._handle_disconnect()
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("[WS] Connection closed")
                    await self._handle_disconnect()
                else:
                    pass
            except asyncio.TimeoutError:
                # 心跳超时，发送 ping
                if self._ws and not self._ws.closed:
                    try:
                        await self._ws.ping()
                    except Exception:
                        await self._handle_disconnect()
            except Exception as e:
                logger.warning(f"[WS] Receive error: {e}")
                await self._handle_disconnect()

    async def _handle_disconnect(self):
        """处理断线：停止监听，等待重连。"""
        self._running = False
        if self._session:
            await self._session.close()
        self._ws = None
        self._session = None
        await self._do_reconnect()

    async def _do_reconnect(self):
        """指数退避重连。"""
        if not self._running:
            return
        self._reconnect_attempts += 1
        delay = min(
            self._reconnect_base * (2 ** (self._reconnect_attempts - 1)),
            self._reconnect_max,
        )
        logger.info(
            f"[WS] Reconnecting in {delay}s (attempt {self._reconnect_attempts})..."
        )
        await asyncio.sleep(delay)
        try:
            await self.connect()
            self._listen_task = asyncio.create_task(self._run())
        except Exception as e:
            logger.warning(f"[WS] Reconnect failed: {e}")
            await self._do_reconnect()

    def _handle_message(self, raw: str):
        """
        解析 WebSocket 消息，路由到对应回调。

        消息格式：
        {"stream": "btcusdt@kline_1h", "data": {...}}
        """
        try:
            msg = json.loads(raw)
            stream = msg.get("stream", "")
            data = msg.get("data", {})

            if "@kline_" in stream:
                # K 线消息
                sym, intv = self._parse_kline_stream(stream)
                if not sym or not intv:
                    return
                kline = Kline.from_ws(sym, intv, data, is_closed=data.get("x", False))
                cb = self._kline_cbs.get((sym, intv))
                if cb:
                    cb(kline)

            elif "@ticker" in stream:
                # Ticker 消息
                sym = stream.replace("@ticker", "").upper()
                ticker = Ticker.from_ws(sym, data)
                cb = self._price_cbs.get(sym)
                if cb:
                    cb(ticker)

        except json.JSONDecodeError:
            pass

    def _parse_kline_stream(self, stream: str) -> tuple[str, str]:
        """从 stream 名解析 symbol 和 interval。"""
        # 格式：btcusdt@kline_1h
        try:
            parts = stream.split("@kline_")
            if len(parts) != 2:
                return "", ""
            sym = parts[0].upper()
            intv = parts[1]
            return sym, intv
        except Exception:
            return "", ""

    # ==================== Subscribe/Unsubscribe ====================

    async def _subscribe(self, params: list[str]):
        """发送订阅请求。"""
        if not self._ws or self._ws.closed:
            return
        msg = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000),
        }
        await self._ws.send_json(msg)
        logger.info(f"[WS] Subscribed: {params}")

    async def _unsubscribe(self, params: list[str]):
        """发送取消订阅请求。"""
        if not self._ws or self._ws.closed:
            return
        msg = {
            "method": "UNSUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000),
        }
        await self._ws.send_json(msg)
        logger.info(f"[WS] Unsubscribed: {params}")

    # ==================== Batch API ====================

    async def subscribe_batch(self, params: list[str]):
        """
        批量订阅任意流（通用接口）。

        Args:
            params: ["btcusdt@ticker", "ethusdt@kline_1h", ...]
        """
        if not params:
            return
        await self._subscribe(params)

    async def start_listening(self):
        """启动监听循环（在 connect 之后调用）。"""
        self._listen_task = asyncio.create_task(self._run())

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.closed
