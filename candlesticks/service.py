"""
CandleSticks 微服务主实现。

整合 cache(L1) + persistence(L2) + rest_client + ws_client + pair_merger，
实现 ICandlestickService 接口。

数据流：
  REST fetch → cache.append_kline → db.upsert_kline
  WS kline  → cache.append_kline → 新K线? → callback → db.upsert_kline
  WS ticker → cache.update_price → callback
  REST polling(5s staleness) → cache + db → callback
"""

import asyncio
import time
from typing import Callable, Optional

from asterdex.logging_config import get_logger

from .cache import CandleCache
from .interface import ICandlestickService
from .models import Kline, Ticker
from .pair_merger import PairMerger
from .persistence import PersistenceDB
from .rest_client import RestClient
from .ws_client import WebSocketClient

logger = get_logger(__name__)

# WebSocket 订阅数据超过此秒数认为过期，触发 REST fallback
STALENESS_THRESHOLD = 5


class CandleSticksService(ICandlestickService):
    """
    CandleSticks 微服务主类。

    集成以下组件：
    - CandleCache (L1): 内存缓存，最新 K 线
    - PersistenceDB (L2): SQLite 持久化，每 symbol+interval ≤ 1000 条
    - RestClient: REST API（历史 + 启动预热 + fallback）
    - WebSocketClient: 实时 K 线 + 价格订阅
    - PairMerger: 配对交易对的 ratio 合并

    使用方式：
    1. 实例化（传入配置）
    2. fetch_and_cache(symbols, intervals) — 预热
    3. subscribe_klines(...) / subscribe_prices(...) — 注册回调
    4. start() — 连接 WS，开始监听
    5. stop() — 断开连接
    """

    def __init__(
        self,
        ws_url: str = "wss://fstream.asterdex.com/stream",
        rest_url: str = "https://fapi.asterdex.com",
        proxy: Optional[str] = None,
        db_path: str = "candlesticks_cache.db",
        staleness_threshold: float = STALENESS_THRESHOLD,
        poll_interval: float = 5.0,
        pair_separator: str = "/",
    ):
        self.ws_url = ws_url
        self.rest_url = rest_url
        self.proxy = proxy
        self.staleness_threshold = staleness_threshold
        self.poll_interval = poll_interval
        self.pair_separator = pair_separator

        # L1 缓存
        self._cache = CandleCache()
        # L2 持久化
        self._db = PersistenceDB(db_path=db_path)
        # REST 客户端
        self._rest = RestClient(base_url=rest_url, proxy=proxy)
        # WS 客户端
        self._ws = WebSocketClient(ws_url=ws_url, proxy=proxy)
        # 配对合并器
        self._merger = PairMerger()

        # 外部回调：[(symbol, interval, callback), ...]
        self._kline_cbs: list[tuple[str, str, Callable]] = []
        # 外部价格回调：[(symbol, callback), ...]
        self._price_cbs: list[tuple[str, Callable]] = []

        # 内部轮询任务
        self._poll_task: Optional[asyncio.Task] = None
        self._running = False

        # PairTrading 组件 symbol → 配对名称 的反向映射
        self._component_to_pair: dict[str, str] = {}

    # ==================== ICandlestickService 实现 ====================

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 500,
    ) -> list[Kline]:
        """
        获取历史 K 线，优先从 L1 缓存返回，L1 没有则查 L2。

        Returns:
            Kline 列表，最老在前
        """
        # 先查 L1
        klines = self._cache.get_klines(symbol, interval, limit)
        if klines:
            return klines
        # L1 没有，查 L2
        klines = self._db.get_klines(symbol, interval, limit)
        if klines:
            # 回填 L1
            self._cache.set_klines(symbol, interval, klines)
        return klines

    async def get_latest(self, symbol: str, interval: str = "1h") -> Kline | None:
        """获取最新一根 K 线。"""
        k = self._cache.get_latest_kline(symbol, interval)
        if k:
            return k
        klines = self._db.get_klines(symbol, interval, limit=1)
        if klines:
            self._cache.set_klines(symbol, interval, klines)
            return klines[-1]
        return None

    async def get_price(self, symbol: str) -> float:
        """获取当前价格。"""
        return self._cache.get_price(symbol) or 0.0

    async def get_all_prices(self) -> dict[str, float]:
        """获取所有已缓存的价格。"""
        return self._cache.get_prices(list(self._cache.price_cache.keys()))

    async def subscribe_klines(
        self,
        symbols: list[str],
        intervals: list[str],
        callback: Callable[[str, str, Kline], None],
    ) -> list[asyncio.Task]:
        """
        注册 K 线回调。

        回调在 WS 推送新 K 线（is_closed=True）时触发。
        返回的 Task 供 stop 时 cancel。
        """
        for sym in symbols:
            for intv in intervals:
                self._kline_cbs.append((sym, intv, callback))
        return []

    async def subscribe_prices(
        self,
        symbols: list[str],
        callback: Callable[[str, float], None],
    ) -> list[asyncio.Task]:
        """
        注册价格回调。

        回调在 WS ticker 推送时触发。
        返回的 Task 供 stop 时 cancel。
        """
        for sym in symbols:
            self._price_cbs.append((sym, callback))
        return []

    async def fetch_and_cache(
        self,
        symbols: list[str],
        intervals: list[str],
    ):
        """
        启动时从 REST API 预热缓存。

        对每个 symbol + interval 组合：
        1. 从 L2 (SQLite) 加载已有数据到 L1
        2. REST fetch 最新数据
        3. L1 + L2 双重写入
        """
        logger.info(
            f"[CandleSticks] fetch_and_cache: {len(symbols)} symbols, {intervals}"
        )

        for sym in symbols:
            # 从 L2 加载到 L1
            for intv in intervals:
                db_klines = self._db.get_klines(sym, intv, limit=1000)
                if db_klines:
                    self._cache.set_klines(sym, intv, db_klines)
                    logger.info(
                        f"[CandleSticks] DB → Cache: {sym} {intv} ({len(db_klines)} klines)"
                    )

            # REST fetch 更新数据（补充 DB 没有的）
            for intv in intervals:
                rest_klines = await self._rest.fetch_klines(sym, intv, limit=500)
                if rest_klines:
                    for k in rest_klines:
                        is_new = self._cache.append_kline(k)
                    # 持久化到 DB
                    self._db.upsert_klines(rest_klines)
                    logger.info(
                        f"[CandleSticks] REST → Cache+DB: {sym} {intv} ({len(rest_klines)} klines)"
                    )

            # 批量获取价格
            all_syms = list(set([sym] + self._merger.all_components()))
            prices = await self._rest.fetch_prices(all_syms)
            for sym_p, price in prices.items():
                ticker = Ticker(symbol=sym_p, price=price, update_time=time.time())
                self._cache.update_price(ticker)
                self._db.upsert_price(ticker)
            logger.info(f"[CandleSticks] Prices loaded: {list(prices.keys())}")

    async def start(self):
        """
        启动服务：
        1. 注册 WS 回调
        2. 连接 WS
        3. 启动 REST polling fallback 循环
        """
        if self._running:
            return
        self._running = True

        # 注册 WS 回调
        self._ws.on_ticker("*")(self._on_ws_ticker)

        # 注册 K 线回调（每个 symbol + interval）
        for sym, intv, _ in self._kline_cbs:
            self._ws.on_kline(sym, intv)(self._make_kline_cb(sym, intv))

        # 注册 PairTrading 组件的 ticker 回调
        for comp in self._merger.all_components():
            self._ws.on_ticker(comp)(self._on_pair_component_ticker(comp))

        # 连接 WS 并启动监听
        await self._ws.connect()
        await self._ws.start_listening()

        # 启动 REST polling fallback 循环
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("[CandleSticks] Service started")

    async def stop(self):
        """停止服务，断开 WS，取消轮询。"""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            self._poll_task = None
        await self._ws.disconnect()
        self._db.close()
        logger.info("[CandleSticks] Service stopped")

    # ==================== PairTrading ====================

    def register_pair(self, pair_symbol: str, component1: str, component2: str):
        """
        注册配对交易对。

        后续 ticker/kline 推送时，会自动计算 ratio。

        Args:
            pair_symbol: 配对名称，如 "BTCUSDT/ETHUSDT"
            component1: 组件1，如 "BTCUSDT"
            component2: 组件2，如 "ETHUSDT"
        """
        self._merger.register_pair(pair_symbol, component1, component2)
        self._component_to_pair[component1] = pair_symbol
        self._component_to_pair[component2] = pair_symbol

        # 注册配对的 ticker 回调
        self._ws.on_ticker(component1)(self._on_pair_component_ticker(component1))
        self._ws.on_ticker(component2)(self._on_pair_component_ticker(component2))

        # 注册配对的 K 线回调（两个组件都用）
        for intv in ["1h", "15m"]:
            self._ws.on_kline(component1, intv)(
                self._make_pair_kline_cb(pair_symbol, component1, intv)
            )
            self._ws.on_kline(component2, intv)(
                self._make_pair_kline_cb(pair_symbol, component2, intv)
            )
        logger.info(
            f"[CandleSticks] Registered pair: {pair_symbol} = {component1}/{component2}"
        )

    # ==================== Internal Callbacks ====================

    def _make_kline_cb(self, symbol: str, interval: str):
        """为 symbol+interval 创建 WS K 线回调。"""

        async def cb(kline: Kline):
            if not kline.is_closed:
                return
            is_new = self._cache.append_kline(kline)
            if is_new:
                self._db.upsert_kline(kline)
                self._dispatch_kline(symbol, interval, kline)

        return cb

    def _make_pair_kline_cb(self, pair_symbol: str, comp: str, interval: str):
        """为配对创建 WS K 线回调。"""

        async def cb(kline: Kline):
            if not kline.is_closed:
                return
            ratio_kline = self._merger.on_component_kline(comp, kline)
            if ratio_kline:
                is_new = self._cache.append_kline(ratio_kline)
                if is_new:
                    self._db.upsert_kline(ratio_kline)
                    self._dispatch_kline(pair_symbol, interval, ratio_kline)

        return cb

    def _on_ws_ticker(self, ticker: Ticker):
        """处理 WS ticker 推送。"""
        self._cache.update_price(ticker)
        self._db.upsert_price(ticker)
        self._dispatch_price(ticker.symbol, ticker.price)

    def _on_pair_component_ticker(self, comp: str):
        """处理配对组件的 ticker 推送。"""

        async def cb(ticker: Ticker):
            ratio = self._merger.update_price(comp, ticker.price)
            self._cache.update_price(ticker)
            self._db.upsert_price(ticker)
            self._dispatch_price(ticker.symbol, ticker.price)
            if ratio is not None:
                pair_symbol = self._component_to_pair.get(comp, "")
                if pair_symbol:
                    self._dispatch_price(pair_symbol, ratio)

        return cb

    def _dispatch_kline(self, symbol: str, interval: str, kline: Kline):
        """将 K 线推送给所有注册回调。"""
        for sym, intv, cb in self._kline_cbs:
            if sym == symbol and intv == interval:
                try:
                    asyncio.create_task(self._safe_cb(cb, symbol, interval, kline))
                except Exception:
                    pass

    def _dispatch_price(self, symbol: str, price: float):
        """将价格推送给所有注册回调。"""
        for sym, cb in self._price_cbs:
            if sym == symbol:
                try:
                    asyncio.create_task(self._safe_cb(cb, symbol, price))
                except Exception:
                    pass

    async def _safe_cb(self, cb, *args):
        """安全调用回调，捕获异常。"""
        try:
            if asyncio.iscoroutinefunction(cb):
                await cb(*args)
            else:
                cb(*args)
        except Exception as e:
            logger.warning(f"[CandleSticks] Callback error: {e}")

    # ==================== REST Polling Fallback ====================

    async def _poll_loop(self):
        """
        REST polling fallback 循环。

        每 poll_interval 秒：
        - 检查 L1 中所有 symbol+interval 的 K 线是否过期
        - 过期（超过 staleness_threshold 无推送）的触发 REST fetch
        - 新 K 线推送给回调
        """
        last_poll = 0.0
        while self._running:
            try:
                await asyncio.sleep(1)
                now = time.time()
                if now - last_poll < self.poll_interval:
                    continue
                last_poll = now

                for sym, intv, _ in self._kline_cbs:
                    latest = self._cache.get_latest_kline(sym, intv)
                    if latest is None:
                        continue
                    age = now - (latest.open_time / 1000)
                    if age > self.staleness_threshold:
                        klines = await self._rest.fetch_klines(sym, intv, limit=500)
                        if klines:
                            for k in klines:
                                is_new = self._cache.append_kline(k)
                                if is_new:
                                    self._db.upsert_kline(k)
                                    self._dispatch_kline(sym, intv, k)

                for sym, _ in self._price_cbs:
                    stale = self._cache.get_price_staleness(sym)
                    if stale > self.staleness_threshold:
                        price = await self._rest.fetch_ticker_price(sym)
                        if price > 0:
                            ticker = Ticker(symbol=sym, price=price, update_time=now)
                            self._cache.update_price(ticker)
                            self._db.upsert_price(ticker)
                            self._dispatch_price(sym, price)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[CandleSticks] Poll error: {e}")
