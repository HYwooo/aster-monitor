"""
配对 K 线合并器。

功能：
- 追踪配对交易对（如 BTCUSDT/ETHUSDT）的两个组件价格
- 当两个组件的 ticker 都到位后，计算出 ratio 价格
- 当任一组件的新 K 线到达时，计算配对 ratio K 线

K 线合并规则（Pinescript 逻辑）：
- open_ratio = open1 / open2
- close_ratio = ratio（当前价格）
- high_ratio = max(open_ratio, close_ratio)
- low_ratio = min(open_ratio, close_ratio)
- volume = max(volume1, volume2)
"""

from typing import Optional

from .models import Kline, PairState


class PairMerger:
    """
    配对合并器。

    管理多个配对交易对的状态，当两个组件价格或 K 线到达时，
    计算出配对的 ratio 价格或 ratio K 线。
    """

    def __init__(self):
        # symbol_pair_name → PairState
        # 例如 "BTCUSDT/ETHUSDT" → PairState(...)
        self._pairs: dict[str, PairState] = {}

    def register_pair(self, pair_symbol: str, component1: str, component2: str):
        """
        注册一个配对交易对。

        Args:
            pair_symbol: 配对名称，如 "BTCUSDT/ETHUSDT"
            component1: 第一个组件，如 "BTCUSDT"
            component2: 第二个组件，如 "ETHUSDT"
        """
        self._pairs[pair_symbol] = PairState(
            symbol=pair_symbol,
            component1=component1,
            component2=component2,
        )

    def update_price(self, comp_symbol: str, price: float) -> Optional[float]:
        """
        更新组件价格。

        当两个组件价格都到位后，返回 ratio = price1 / price2。
        否则返回 None。

        Returns:
            ratio 价格，如果任一组件价格未到位则 None
        """
        for pair in self._pairs.values():
            if comp_symbol in (pair.component1, pair.component2):
                pair.update_price(comp_symbol, price)
        return self.get_ratio(comp_symbol)

    def get_ratio(self, component_symbol: str) -> Optional[float]:
        """
        获取某个组件所属配对的当前 ratio。
        如果该组件的配对未就绪，返回 None。
        """
        for pair in self._pairs.values():
            if component_symbol in (pair.component1, pair.component2):
                if pair.is_ready():
                    return pair.ratio
        return None

    def on_component_kline(
        self,
        comp_symbol: str,
        kline: Kline,
    ) -> Optional[Kline]:
        """
        当组件 K 线到达时，计算配对 ratio K 线。

        Args:
            comp_symbol: K 线所属的组件名称（如 "BTCUSDT"）
            kline: 组件的 K 线

        Returns:
            配对的 ratio K 线，如果无法计算则 None
        """
        for pair in self._pairs.values():
            if comp_symbol in (pair.component1, pair.component2):
                return pair.make_ratio_kline(
                    kline, 1 if comp_symbol == pair.component1 else 2
                )
        return None

    def is_ready(self, pair_symbol: str) -> bool:
        """判断配对是否就绪（两个组件价格都有）。"""
        pair = self._pairs.get(pair_symbol)
        return pair.is_ready() if pair else False

    def get_pair(self, pair_symbol: str) -> Optional[PairState]:
        """获取配对状态。"""
        return self._pairs.get(pair_symbol)

    def all_components(self) -> list[str]:
        """返回所有组件名称（去重）。"""
        comps = set()
        for pair in self._pairs.values():
            comps.add(pair.component1)
            comps.add(pair.component2)
        return list(comps)

    def all_pairs(self) -> list[str]:
        """返回所有配对名称。"""
        return list(self._pairs.keys())
