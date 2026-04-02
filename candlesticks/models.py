"""
CandleSticks 数据模型。

提供 K 线、K线缓存、价格 等数据结构的定义。
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Kline:
    """
    单根 K 线数据结构。

    属性对应交易所 REST API 返回格式：
    [open_time, open, high, low, close, volume, close_time, ...]
    WebSocket 格式：
    {t: open_time, o: open, h: high, l: low, c: close, v: volume}
    """

    symbol: str  # 交易对名称，如 "BTCUSDT"
    interval: str  # K 线周期，如 "1h", "15m"
    open_time: int  # Unix ms 时间戳
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: Optional[int] = 0  # Unix ms，K 线关闭时间
    is_closed: bool = True  # 是否为已结束的 K 线

    @classmethod
    def from_rest(cls, symbol: str, interval: str, data: list) -> "Kline":
        """
        从 REST API 返回的 list 创建一个 Kline。

        Args:
            symbol: 交易对名称
            interval: K 线周期
            data: REST 返回的 list，格式：[open_time, open, high, low, close, volume, close_time, ...]
        """
        return cls(
            symbol=symbol,
            interval=interval,
            open_time=int(data[0]),
            open=float(data[1]),
            high=float(data[2]),
            low=float(data[3]),
            close=float(data[4]),
            volume=float(data[5]),
            close_time=int(data[6]) if len(data) > 6 else 0,
            is_closed=True,
        )

    @classmethod
    def from_ws(
        cls, symbol: str, interval: str, data: dict, is_closed: bool = True
    ) -> "Kline":
        """
        从 WebSocket 推送的 dict 创建一个 Kline。

        Args:
            symbol: 交易对名称
            interval: K 线周期
            data: WebSocket kline dict，{t, o, h, l, c, v}
            is_closed: 是否为已结束的 K 线（WebSocket 的 x 字段）
        """
        return cls(
            symbol=symbol,
            interval=interval,
            open_time=int(data.get("t", 0)),
            open=float(data.get("o", 0)),
            high=float(data.get("h", 0)),
            low=float(data.get("l", 0)),
            close=float(data.get("c", 0)),
            volume=float(data.get("v", 0)),
            close_time=int(data.get("T", 0)),
            is_closed=is_closed,
        )

    def to_list(self) -> list:
        """转换为 REST API 格式的 list。"""
        return [
            self.open_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.close_time or 0,
        ]

    def to_dict(self) -> dict:
        """转换为 dict。"""
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "close_time": self.close_time,
            "is_closed": self.is_closed,
        }


@dataclass
class Ticker:
    """
    最新价格数据结构。
    """

    symbol: str
    price: float
    update_time: float  # Unix 时间戳

    @classmethod
    def from_ws(cls, symbol: str, data: dict) -> "Ticker":
        """
        从 WebSocket ticker 数据创建 Ticker。

        WebSocket ticker 格式：{s: "BTCUSDT", c: "66000.0"} 或 {s: "BTCUSDT", lastPrice: "66000.0"}
        """
        price_str = data.get("c") or data.get("lastPrice") or "0"
        return cls(
            symbol=symbol,
            price=float(price_str),
            update_time=data.get("E", 0) / 1000.0 if data.get("E") else 0.0,
        )

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "update_time": self.update_time,
        }


@dataclass
class PairState:
    """
    配对交易的状态追踪。

    用于计算配对价格（如 BTCUSDT/ETHUSDT = BTC_price / ETH_price）。
    需要等待两个 component 的 ticker 都到达后，才能计算出 ratio。
    """

    symbol: str  # 配对名称，如 "BTCUSDT/ETHUSDT"
    component1: str  # 第一个组件，如 "BTCUSDT"
    component2: str  # 第二个组件，如 "ETHUSDT"
    price1: float = 0.0  # 组件1 最新价格
    price2: float = 0.0  # 组件2 最新价格
    last_kline1: Optional[Kline] = None  # 组件1 上一根 1h K 线（用于计算 ratio open）
    last_kline2: Optional[Kline] = None  # 组件2 上一根 1h K 线
    last_ratio_kline: Optional[Kline] = None  # 上一根合并后的 ratio K 线
    ratio: float = 0.0  # 当前 ratio = price1 / price2

    def update_price(self, comp_symbol: str, price: float):
        """更新某个组件的价格。"""
        if comp_symbol == self.component1:
            self.price1 = price
        elif comp_symbol == self.component2:
            self.price2 = price

        if self.price1 > 0 and self.price2 > 0:
            self.ratio = self.price1 / self.price2

    def is_ready(self) -> bool:
        """两个组件价格都到位了吗？"""
        return self.price1 > 0 and self.price2 > 0

    def make_ratio_kline(self, new_kline: Kline, comp: int) -> Kline:
        """
        用新到达的 K 线和另一个组件的最新 K 线，计算配对 ratio K 线。

        Args:
            new_kline: 新到达的 K 线（组件1 或 组件2 的 1h K 线）
            comp: 新 K 线属于哪个组件，1 或 2

        Returns:
            配对 ratio K 线：
            - open = open1 / open2  （ratio at open）
            - high = max(open, ratio)  （按 Pinescript: max(ratio_o, ratio_c)）
            - low = min(open, ratio)
            - close = ratio  （最新 ratio）
            - volume = new_kline.volume
        """
        if comp == 1:
            other = self.last_kline2
        else:
            other = self.last_kline1

        if other is None:
            # 没有历史 K 线，用当前 ratio 填充
            return Kline(
                symbol=self.symbol,
                interval=new_kline.interval,
                open_time=new_kline.open_time,
                open=self.ratio,
                high=self.ratio,
                low=self.ratio,
                close=self.ratio,
                volume=new_kline.volume,
                close_time=new_kline.close_time,
                is_closed=new_kline.is_closed,
            )

        # 计算 ratio
        open_ratio = new_kline.open / other.open if other.open > 0 else self.ratio
        close_ratio = self.ratio
        high_ratio = max(open_ratio, close_ratio)
        low_ratio = min(open_ratio, close_ratio)

        # 更新 last_kline
        if comp == 1:
            self.last_kline1 = new_kline
        else:
            self.last_kline2 = new_kline

        k = Kline(
            symbol=self.symbol,
            interval=new_kline.interval,
            open_time=new_kline.open_time,
            open=open_ratio,
            high=high_ratio,
            low=low_ratio,
            close=close_ratio,
            volume=new_kline.volume,
            close_time=new_kline.close_time,
            is_closed=new_kline.is_closed,
        )
        self.last_ratio_kline = k
        return k
