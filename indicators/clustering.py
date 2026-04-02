"""
Clustering SuperTrend 指标（Pinescript V6 移植版）。

论文：SuperTrend AI (Clustering) [LuxAlgo]
https://www.tradingview.com/scripts/oxT6MfOv/

核心思想：
1. 批量 SuperTrend：用不同 factor（min_mult~max_mult，step=0.5）
   计算 N 个 ST 实例，每个记录其趋势方向、性能（perf）
2. K-Means 聚类（K=3）：
   - 以 perf 的 Q25/Q50/Q75 初始化 3 个质心
   - 迭代分配点到最近质心，重新计算质心，直到收敛
3. 最终使用：
   - target_factor = 选中 cluster 内所有 factor 的均值
   - perf_idx = cluster_avg_perf / EMA(|price_diff|)
   - ts = 普通 SuperTrend(target_factor)
   - perf_ama = EMA(ts, perf_idx)  — 平滑后的追踪止损线

适用场景：PairTrading 交易对（如 BTCUSDT/ETHUSDT）
"""

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np
import talib


@dataclass
class ClusteringState:
    """
    Clustering SuperTrend 的状态（用于跨调用持久化）。
    """

    # 聚类状态
    centroids: tuple[float, float, float] = (math.nan, math.nan, math.nan)
    cluster_factors: tuple[list[float], list[float], list[float]] = ([], [], [])
    cluster_perfs: tuple[list[float], list[float], list[float]] = ([], [], [])

    # 当前 ST 状态
    target_factor: float = math.nan
    ts: float = math.nan
    ts_prev: float = math.nan
    perf_ama: float = math.nan
    trend: int = 0  # 0=空头, 1=多头
    upper: float = math.nan
    lower: float = math.nan
    prev_upper: float = math.nan
    prev_lower: float = math.nan


def _kmeans_clustering(
    perfs: np.ndarray,
    factors: np.ndarray,
    max_iter: int = 1000,
) -> tuple[
    tuple[float, float, float],  # centroids
    tuple[list[float], list[float], list[float]],  # cluster_factors
    tuple[list[float], list[float], list[float]],  # cluster_perfs
]:
    """
    K-Means 聚类（K=3）。

    初始化：以 perf 的百分位数(Q25/Q50/Q75)作为 3 个质心。
    迭代：分配点到最近质心 → 重新计算质心 → 直到质心不再变化。

    Args:
        perfs: 性能数组（每个 factor 对应一个 perf）
        factors: factor 数组（与 perfs 一一对应）
        max_iter: 最大迭代次数

    Returns:
        centroids: (c0, c1, c2) 三个质心值
        cluster_factors: ([f0...], [f1...], [f2...]) 每cluster的factors
        cluster_perfs: ([p0...], [p1...], [p2...]) 每cluster的perfs
    """
    if len(perfs) < 3:
        return (math.nan, math.nan, math.nan), ([], [], [])

    perfs = np.array(perfs, dtype=float)
    factors = np.array(factors, dtype=float)

    # 初始化质心为 Q25, Q50, Q75
    def percentile(arr, q):
        """计算百分位数。"""
        sorted_arr = np.sort(arr)
        idx = (len(sorted_arr) - 1) * q / 100
        lo = int(math.floor(idx))
        hi = int(math.ceil(idx))
        if lo == hi:
            return sorted_arr[lo]
        frac = idx - lo
        return sorted_arr[lo] * (1 - frac) + sorted_arr[hi] * frac

    c0 = percentile(perfs, 25)
    c1 = percentile(perfs, 50)
    c2 = percentile(perfs, 75)
    centroids = (c0, c1, c2)

    cluster_factors = ([], [], [])
    cluster_perfs = ([], [], [])

    for _ in range(max_iter):
        # 清空 clusters
        cf = ([], [], [])
        cp = ([], [], [])

        # 分配每个点到最近质心
        for i, p in enumerate(perfs):
            d0 = abs(p - centroids[0])
            d1 = abs(p - centroids[1])
            d2 = abs(p - centroids[2])
            if d0 <= d1 and d0 <= d2:
                cf[0].append(factors[i])
                cp[0].append(p)
            elif d1 <= d0 and d1 <= d2:
                cf[1].append(factors[i])
                cp[1].append(p)
            else:
                cf[2].append(factors[i])
                cp[2].append(p)

        # 计算新质心
        nc0 = sum(cf[0]) / len(cf[0]) if cf[0] else centroids[0]
        nc1 = sum(cf[1]) / len(cf[1]) if cf[1] else centroids[1]
        nc2 = sum(cf[2]) / len(cf[2]) if cf[2] else centroids[2]

        # 质心未变化，退出
        if (
            math.isclose(nc0, centroids[0])
            and math.isclose(nc1, centroids[1])
            and math.isclose(nc2, centroids[2])
        ):
            break

        centroids = (nc0, nc1, nc2)
        cluster_factors = (cf[0], cf[1], cf[2])
        cluster_perfs = (cp[0], cp[1], cp[2])

    return (
        centroids,
        (cluster_factors[0], cluster_factors[1], cluster_factors[2]),
        (
            cluster_perfs[0],
            cluster_perfs[1],
            cluster_perfs[2],
        ),
    )


def clustering_supertrend(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    atr: np.ndarray,
    prev_state: Optional[ClusteringState],
    min_mult: float = 1.0,
    max_mult: float = 5.0,
    step: float = 0.5,
    perf_alpha: float = 10.0,
    from_cluster: str = "Best",
    max_iter: int = 1000,
    max_data: int = 10000,
) -> tuple[float, float, ClusteringState]:
    """
    Clustering SuperTrend 主函数。

    对 close/high/low/atr 数组的**最后 max_data 个 bars** 进行聚类分析，
    选出最优 cluster，计算 target_factor，然后用 target_factor 计算 TS 和 AMA。

    Args:
        close: 收盘价数组
        high: 最高价数组
        low: 最低价数组
        atr: ATR 数组（已计算好的）
        prev_state: 上一个状态 ClusteringState（用于跨调用持久化）
        min_mult: ATR 倍数范围下界
        max_mult: ATR 倍数范围上界
        step: ATR 倍数步长
        perf_alpha: perf EMA 的平滑系数
        from_cluster: 选哪个 cluster，"Best" | "Average" | "Worst"
        max_iter: K-Means 最大迭代次数
        max_data: 参与聚类的最大 bar 数（从最新往回算）

    Returns:
        (ts, perf_ama, new_state)
        ts: 追踪止损线
        perf_ama: 平滑后的 AMA
        new_state: 更新后的 ClusteringState（供下次调用传入）
    """
    n = len(close)
    if n < 3:
        default_state = ClusteringState()
        if prev_state:
            return prev_state.ts, prev_state.perf_ama, prev_state
        return math.nan, math.nan, default_state

    # === 1. 生成 factors 数组 ===
    factors = []
    f = min_mult
    while f <= max_mult + 1e-9:
        factors.append(f)
        f += step
    factors = np.array(factors, dtype=float)

    # === 2. 批量计算 SuperTrend perf ===
    # 只对最后 max_data 个 bar 进行聚类
    start_idx = max(0, n - max_data)
    n_data = n - start_idx

    perfs_out = np.zeros(len(factors))
    outputs_out = np.zeros(len(factors))
    uppers_out = np.full(len(factors), math.nan)
    lowers_out = np.full(len(factors), math.nan)
    trends_out = np.zeros(len(factors), dtype=int)

    prev_perf = 0.0

    for i in range(start_idx, n):
        close_i = close[i]
        prev_close = close[i - 1] if i > 0 else close_i
        hl2 = (high[i] + low[i]) / 2

        for k, factor in enumerate(factors):
            atr_val = atr[i] if i < len(atr) else atr[-1]
            if not math.isfinite(atr_val) or atr_val <= 0:
                continue

            up = hl2 + atr_val * factor
            dn = hl2 - atr_val * factor

            # 更新 trend
            if close_i > uppers_out[k]:
                trends_out[k] = 1
            elif close_i < lowers_out[k]:
                trends_out[k] = -1
            # 否则保持 trend

            # 更新 upper/lower
            prev_up = (
                uppers_out[k - 1]
                if i > start_idx and math.isfinite(uppers_out[k - 1])
                else up
            )
            prev_low = (
                lowers_out[k - 1]
                if i > start_idx and math.isfinite(lowers_out[k - 1])
                else dn
            )

            uppers_out[k] = min(up, prev_up) if math.isfinite(prev_up) else up
            lowers_out[k] = max(dn, prev_low) if math.isfinite(prev_low) else dn

            # 输出 = trend==1 ? lower : upper
            outputs_out[k] = lowers_out[k] if trends_out[k] == 1 else uppers_out[k]

        # 计算 diff = sign(close - prev_close)
        diff = 0.0
        if close_i != prev_close:
            diff = 1.0 if close_i > prev_close else -1.0

        # perf EMA 平滑
        alpha = 2.0 / (perf_alpha + 1.0)
        price_diff = close_i - prev_close
        perf = alpha * (price_diff * diff) + (1 - alpha) * prev_perf
        prev_perf = perf

        for k in range(len(factors)):
            perfs_out[k] = perf  # 每个 factor 的 perf 相同（同一根 bar）

    # === 3. K-Means 聚类 ===
    centroids, cluster_factors, cluster_perfs = _kmeans_clustering(
        perfs_out, factors, max_iter
    )

    state = prev_state or ClusteringState()
    state.centroids = centroids
    state.cluster_factors = cluster_factors
    state.cluster_perfs = cluster_perfs

    # === 4. 选出 target cluster ===
    # from_cluster: Best=2(最大质心), Average=1, Worst=0(最小质心)
    cluster_idx = {"Best": 2, "Average": 1, "Worst": 0}.get(from_cluster, 2)
    target_factors = cluster_factors[cluster_idx]
    target_perfs = cluster_perfs[cluster_idx]

    if target_factors:
        state.target_factor = sum(target_factors) / len(target_factors)
    else:
        state.target_factor = (min_mult + max_mult) / 2

    if not math.isfinite(state.target_factor):
        state.target_factor = (min_mult + max_mult) / 2

    # === 5. 用 target_factor 计算最终 ST ===
    factor = state.target_factor
    atr_last = atr[-1] if len(atr) > 0 else 0.0
    if not math.isfinite(atr_last) or atr_last <= 0:
        return state.ts, state.perf_ama, state

    hl2 = (high[-1] + low[-1]) / 2
    up = hl2 + atr_last * factor
    dn = hl2 - atr_last * factor

    state.prev_upper = state.upper
    state.prev_lower = state.lower
    state.upper = up if math.isfinite(up) else hl2
    state.lower = dn if math.isfinite(dn) else hl2

    if math.isfinite(state.prev_upper):
        state.upper = min(state.upper, state.prev_upper)
    if math.isfinite(state.prev_lower):
        state.lower = max(state.lower, state.prev_lower)

    # Trend
    if close[-1] > state.upper:
        state.trend = 1
    elif close[-1] < state.lower:
        state.trend = -1

    # TS = 多头? lower : upper
    new_ts = state.lower if state.trend == 1 else state.upper
    state.ts_prev = state.ts
    state.ts = new_ts

    # === 6. perf_idx 和 perf_ama ===
    cluster_avg_perf = sum(target_perfs) / len(target_perfs) if target_perfs else 0.0
    cluster_avg_perf = max(cluster_avg_perf, 0.0)

    price_diffs = np.abs(np.diff(close[start_idx:]))
    if len(price_diffs) > 0:
        den = (
            talib.EMA(price_diffs, timeperiod=int(perf_alpha))[-1]
            if len(price_diffs) >= int(perf_alpha)
            else np.mean(price_diffs)
        )
        den = float(den) if math.isfinite(den) and den > 0 else 1.0
    else:
        den = 1.0

    perf_idx = cluster_avg_perf / den

    # perf_ama EMA
    alpha_ama = 2.0 / (perf_alpha + 1.0)
    if math.isnan(state.perf_ama):
        state.perf_ama = state.ts
    elif math.isfinite(state.ts):
        state.perf_ama = state.perf_ama + perf_idx * (state.ts - state.perf_ama)

    return state.ts, state.perf_ama, state


def clustering_supertrend_single(
    close: float,
    high: float,
    low: float,
    prev_state: Optional[ClusteringState],
    target_factor: float,
    atr: float,
) -> tuple[float, float, int, float, float, ClusteringState]:
    """
    单步更新 Clustering SuperTrend（用于实时推送场景）。

    不做聚类（聚类在上电时已完成），只更新 ST 追踪止损。

    Args:
        close: 当前收盘价
        high: 当前最高价
        low: 当前最低价
        prev_state: 上一个状态
        target_factor: 聚类得出的 target_factor（由 clustering_supertrend 批量计算后传入）
        atr: 当前 ATR 值

    Returns:
        (ts, perf_ama, trend, upper, lower, new_state)
    """
    if atr <= 0 or not math.isfinite(atr):
        return (
            prev_state.ts,
            prev_state.perf_ama,
            prev_state.trend,
            prev_state.upper,
            prev_state.lower,
            prev_state,
        )

    state = prev_state or ClusteringState()
    state.target_factor = target_factor

    hl2 = (high + low) / 2
    up = hl2 + atr * target_factor
    dn = hl2 - atr * target_factor

    state.prev_upper = state.upper
    state.prev_lower = state.lower
    state.upper = up
    state.lower = dn

    if math.isfinite(state.prev_upper):
        state.upper = min(state.upper, state.prev_upper)
    if math.isfinite(state.prev_lower):
        state.lower = max(state.lower, state.prev_lower)

    # Trend
    if close > state.upper:
        state.trend = 1
    elif close < state.lower:
        state.trend = -1

    new_ts = state.lower if state.trend == 1 else state.upper
    state.ts_prev = state.ts
    state.ts = new_ts

    # perf_idx（简化：用 target_factor / atr 作为近似）
    perf_idx = target_factor / (atr * 10) if atr > 0 else 0.0
    alpha_ama = 2.0 / (11.0 + 1.0)  # perf_alpha=10
    if math.isnan(state.perf_ama):
        state.perf_ama = state.ts
    else:
        state.perf_ama = state.perf_ama + perf_idx * (state.ts - state.perf_ama)

    return state.ts, state.perf_ama, state.trend, state.upper, state.lower, state
