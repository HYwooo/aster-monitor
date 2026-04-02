"""
信号检测模块 - 核心交易信号判断逻辑。

职责：
- fetch_pair_klines: 获取并合并配对交易的两条 K 线数据
- update_klines: 获取最新 K 线并触发状态重算
- recalculate_states: 重算所有技术指标（Supertrend, Vegas, ATR Channel）
- check_signals: ATR Channel 突破信号检测入口
- check_signals_impl: ATR Channel 突破信号检测实现
- check_trailing_stop: 追踪止损触发检查
"""

import math
import time

import numpy as np

from indicators import (
    calculate_supertrend,
    calculate_vegas_tunnel,
    calculate_atr,
    run_atr_channel,
    ClusteringState,
    clustering_supertrend,
    clustering_supertrend_single,
)
from notifications import format_number
from rest_api import fetch_klines


async def fetch_pair_klines(
    symbol: str,
    limit: int = 500,
    interval: str = "1h",
    proxy: str = None,
    kline_cache: dict = None,
    fetch_klines_fn=None,
) -> list:
    """
    获取配对交易对的合并 K 线数据。

    配对交易对（如 BTCUSDT/ETHUSDT）的价格 = BTC价格 / ETH价格。
    将两个交易对的 K 线按时间戳对齐后，合并为一条配对 K 线：
    - open = ratio_open = open1 / open2
    - high = max(ratio_open, ratio_close)
    - low = min(ratio_open, ratio_close)
    - close = ratio_close = close1 / close2

    参数:
        symbol: 配对交易对，如 "BTCUSDT/ETHUSDT"
        limit: K 线数量
        interval: K 线周期
        proxy: HTTP 代理
        kline_cache: 可选，K 线缓存字典 {symbol: klines}。如有提供，优先从 cache 读取
                     component klines，避免重复 REST fetch。
        fetch_klines_fn: 可选，REST fetch 函数。如为 None 则使用内置 fetch_klines。

    返回:
        合并后的 K 线数组列表，每条格式：
        [timestamp, ratio_open, ratio_high, ratio_low, ratio_close, volume]
    """
    parts = symbol.split("/")
    _fetch = fetch_klines_fn or fetch_klines

    # 优先从 kline_cache 读取 component klines，避免重复 fetch
    sym1, sym2 = parts[0], parts[1]
    if kline_cache is not None and sym1 in kline_cache and sym2 in kline_cache:
        klines1 = kline_cache[sym1]
        klines2 = kline_cache[sym2]
    else:
        klines1 = await _fetch(sym1, limit, interval, proxy)
        klines2 = await _fetch(sym2, limit, interval, proxy)
        # 有 kline_cache 时，将 fetch 结果写入 cache（供后续复用）
        if kline_cache is not None:
            if klines1:
                kline_cache[sym1] = klines1
            if klines2:
                kline_cache[sym2] = klines2

    if not klines1 or not klines2:
        return []

    # 以 symbol2 的 K 线为基准，建立时间戳索引
    k2_by_time = {int(k[0]): k for k in klines2}
    merged = []
    for k1 in klines1:
        t = int(k1[0])
        if t not in k2_by_time:
            continue
        k2 = k2_by_time[t]
        o1, c1 = float(k1[1]), float(k1[4])
        o2, c2 = float(k2[1]), float(k2[4])
        if o2 == 0 or c2 == 0:
            continue
        ratio_o = o1 / o2
        ratio_c = c1 / c2
        merged.append(
            [
                t,
                ratio_o,
                max(ratio_o, ratio_c),
                min(ratio_o, ratio_c),
                ratio_c,
                float(k1[5]),
            ]
        )
    return sorted(merged, key=lambda x: x[0])


async def update_klines(
    symbol: str,
    kline_cache: dict,
    last_kline_time: dict,
    is_pair_trading_fn,
    proxy: str = None,
    recalculate_states_fn=None,
    fetch_pair_klines_fn=None,
    fetch_klines_fn=None,
):
    """
    获取指定交易对的最新 K 线数据，并触发指标重算。

    参数:
        symbol: 交易对名称
        kline_cache: K 线缓存字典 {symbol: klines}
        last_kline_time: 上次 K 线时间戳字典 {symbol: unix_ms}
        is_pair_trading_fn: 判断是否为配对交易对的函数 fn(symbol) -> bool
        proxy: HTTP 代理
        recalculate_states_fn: async 回调，触发指标重算
        fetch_pair_klines_fn: async 回调，获取配对 K 线（可选）
        fetch_klines_fn: async 回调，单一 K 线 fetch 函数（可选）。用于配对时透传给 fetch_pair_klines 以支持 cache 复用。
    """
    try:
        is_pair = is_pair_trading_fn(symbol)
        _fetch = fetch_klines_fn or fetch_klines
        if is_pair:
            klines = await (fetch_pair_klines_fn or fetch_klines)(
                symbol, kline_cache=kline_cache, fetch_klines_fn=_fetch
            )
        else:
            klines = await _fetch(symbol, proxy=proxy)
            if klines and symbol not in kline_cache:
                kline_cache[symbol] = klines
        if klines:
            if is_pair:
                kline_cache[symbol] = klines
            last_time = last_kline_time.get(symbol, 0)
            new_time = int(klines[-1][0])
            # 仅在新 K 线到达时触发重算
            if new_time > last_time:
                last_kline_time[symbol] = new_time
                if recalculate_states_fn:
                    await recalculate_states_fn(symbol)
    except Exception:
        pass


async def recalculate_states(
    symbol: str,
    kline_cache: dict,
    benchmark: dict,
    is_pair_trading: bool,
    st_period1: int,
    st_multiplier1: float,
    st_period2: int,
    st_multiplier2: float,
    vt_ema_signal: int,
    vt_ema_upper: int,
    vt_ema_lower: int,
    atr1h_period: int,
    atr1h_ma_type: str,
    atr1h_mult: float,
    atr15m_period: int,
    atr15m_ma_type: str,
    atr15m_mult: float,
):
    """
    重算指定交易对的所有技术指标，并将结果存入 benchmark 字典。

    计算内容：
    1. Supertrend (双周期: period1+multiplier1, period2+multiplier2)
    2. Vegas Tunnel (3 条 EMA)
    3. ATR Channel (1h ATR，上轨/下轨/状态)

    参数:
        symbol: 交易对名称
        kline_cache: K 线缓存
        benchmark: 指标缓存字典，重算结果写入这里
        is_pair_trading: 是否为配对交易对
        st_period1/multiplier1: Supertrend 第一周期参数
        st_period2/multiplier2: Supertrend 第二周期参数
        vt_ema_signal/upper/lower: Vegas Tunnel EMA 参数
        atr1h_period/ma_type/mult: 1h ATR Channel 参数
        atr15m_period/ma_type/mult: 15m ATR Channel 参数（用于追踪止损）
    """
    klines = kline_cache.get(symbol, [])
    if len(klines) < 200:
        return
    try:
        close = np.array([float(k[4]) for k in klines], dtype=float)
        # 配对交易对的 H/L 取开/收的 max/min
        if is_pair_trading:
            open_arr = np.array([float(k[1]) for k in klines], dtype=float)
            high = np.maximum(open_arr, close)
            low = np.minimum(open_arr, close)
        else:
            high = np.array([float(k[2]) for k in klines], dtype=float)
            low = np.array([float(k[3]) for k in klines], dtype=float)

        # 计算 Supertrend（双周期）
        st1 = calculate_supertrend(high, low, close, st_period1, st_multiplier1)
        st2 = calculate_supertrend(high, low, close, st_period2, st_multiplier2)

        # 计算 Vegas Tunnel
        ema_s, ema_u, ema_l = calculate_vegas_tunnel(
            close, vt_ema_signal, vt_ema_upper, vt_ema_lower
        )

        # 计算 1h ATR Channel
        atr1h = calculate_atr(high, low, close, atr1h_period, atr1h_ma_type)
        prev_atr_state = benchmark.get(symbol, {}).get(
            "atr1h_state", (float("nan"), float("nan"), 0)
        )
        # 从缓存的 ATR Channel 状态继续迭代到最新 K 线
        for i in range(len(close)):
            upper, lower, ch = run_atr_channel(
                close[i], atr1h[i], atr1h_mult, prev_atr_state
            )
            prev_atr_state = (upper, lower, ch)
        atr1h_upper, atr1h_lower, atr1h_ch = prev_atr_state

        # 取最后一个有效值
        st1_val, st2_val = float(st1[-1]), float(st2[-1])
        ema_s_val, ema_u_val, ema_l_val = (
            float(ema_s[-1]),
            float(ema_u[-1]),
            float(ema_l[-1]),
        )
        if not all(
            math.isfinite(v)
            for v in [st1_val, st2_val, ema_s_val, ema_u_val, ema_l_val]
        ):
            return

        benchmark[symbol] = {
            "st1": st1_val,
            "st2": st2_val,
            "ema_s": ema_s_val,
            "ema_u": ema_u_val,
            "ema_l": ema_l_val,
            "kline_time": int(klines[-1][0]),
            "atr1h_upper": float(atr1h_upper) if not math.isnan(atr1h_upper) else 0,
            "atr1h_lower": float(atr1h_lower) if not math.isnan(atr1h_lower) else 0,
            "atr1h_ch": atr1h_ch,
            "atr1h_state": prev_atr_state,
        }
    except Exception:
        pass


async def check_signals(
    symbol: str,
    mark_prices: dict,
    mark_price_times: dict,
    benchmark: dict,
    trailing_stop: dict,
    last_atr_state: dict,
    last_alert_time: dict,
    _initialized: bool,
    last_st_state: dict,
    atr1h_ma_type: str,
    atr1h_period: int,
    atr1h_mult: float,
    atr15m_ma_type: str,
    atr15m_period: int,
    atr15m_mult: float,
    send_webhook_fn,
    increment_alert_count_fn,
):
    """
    ATR Channel 信号检测入口函数（包装器）。
    捕获异常防止信号检查崩溃影响其他逻辑。
    """
    if symbol not in benchmark:
        return
    try:
        await check_signals_impl(
            symbol,
            mark_prices,
            mark_price_times,
            benchmark,
            trailing_stop,
            last_atr_state,
            last_alert_time,
            _initialized,
            last_st_state,
            atr1h_ma_type,
            atr1h_period,
            atr1h_mult,
            atr15m_ma_type,
            atr15m_period,
            atr15m_mult,
            send_webhook_fn,
            increment_alert_count_fn,
        )
    except Exception:
        pass


async def check_signals_impl(
    symbol: str,
    mark_prices: dict,
    mark_price_times: dict,
    benchmark: dict,
    trailing_stop: dict,
    last_atr_state: dict,
    last_alert_time: dict,
    _initialized: bool,
    last_st_state: dict,
    atr1h_ma_type: str,
    atr1h_period: int,
    atr1h_mult: float,
    atr15m_ma_type: str,
    atr15m_period: int,
    atr15m_mult: float,
    send_webhook_fn,
    increment_alert_count_fn,
):
    """
    ATR Channel 信号检测实现。

    逻辑：
    - 初始化阶段（_initialized=False）只记录 last_st_state，不推送信号
    - 非初始化阶段：
        - 价格 >= atr1h_upper 且上次状态不是多头 -> 推送 LONG，建立追踪止损
        - 价格 <= atr1h_lower 且上次状态不是空头 -> 推送 SHORT，建立追踪止损
    - 两次同向信号间隔至少 1 小时（3600 秒）
    """
    current_price = mark_prices.get(symbol)
    if not current_price:
        return
    # 价格数据超过 5 分钟不新鲜则跳过
    last_update = mark_price_times.get(symbol, 0)
    if time.time() - last_update > 300:
        return
    bm = benchmark.get(symbol)
    if not bm:
        return

    # Supertrend 状态（用于观察，非信号触发条件）
    st1_val = bm["st1"]
    st2_val = bm["st2"]
    st_state = ("1" if current_price > st1_val else "0") + (
        "1" if current_price > st2_val else "0"
    )

    # 初始化阶段：只记录状态，不推送
    if not _initialized:
        last_st_state[symbol] = st_state
        return

    now = time.time()
    atr1h_upper = bm.get("atr1h_upper", 0)
    atr1h_lower = bm.get("atr1h_lower", 0)
    prev_atr_state = last_atr_state.get(symbol, {"ch": 0, "sent": None})

    # === 多头突破 ===
    if current_price >= atr1h_upper and prev_atr_state["ch"] != 1:
        last_alert = last_alert_time.get(f"ATR_Ch_{symbol}", 0)
        if now - last_alert > 3600:
            last_alert_time[f"ATR_Ch_{symbol}"] = now
            last_atr_state[symbol] = {"ch": 1, "sent": "LONG"}
            await send_webhook_fn(
                "ATR_Ch",
                f"[{symbol}] LONG",
                {
                    "symbol": symbol,
                    "direction": "LONG",
                    "price": format_number(current_price),
                    "atr_upper": format_number(atr1h_upper),
                    "atr_lower": format_number(atr1h_lower),
                },
            )
            increment_alert_count_fn()
            # 建立追踪止损
            trailing_stop[symbol] = {
                "direction": "LONG",
                "entry_price": current_price,
                "entry_time": now,
                "atr_mult": atr15m_mult,
                "atr15m_upper": 0,
                "atr15m_lower": 0,
                "atr15m_state": (float("nan"), float("nan"), 0),
                "active": True,
            }

    # === 空头突破 ===
    elif current_price <= atr1h_lower and prev_atr_state["ch"] != -1:
        last_alert = last_alert_time.get(f"ATR_Ch_{symbol}", 0)
        if now - last_alert > 3600:
            last_alert_time[f"ATR_Ch_{symbol}"] = now
            last_atr_state[symbol] = {"ch": -1, "sent": "SHORT"}
            await send_webhook_fn(
                "ATR_Ch",
                f"[{symbol}] SHORT",
                {
                    "symbol": symbol,
                    "direction": "SHORT",
                    "price": format_number(current_price),
                    "atr_upper": format_number(atr1h_upper),
                    "atr_lower": format_number(atr1h_lower),
                },
            )
            increment_alert_count_fn()
            # 建立追踪止损
            trailing_stop[symbol] = {
                "direction": "SHORT",
                "entry_price": current_price,
                "entry_time": now,
                "atr_mult": atr15m_mult,
                "atr15m_upper": 0,
                "atr15m_lower": 0,
                "atr15m_state": (float("nan"), float("nan"), 0),
                "active": True,
            }


async def check_trailing_stop(
    symbol: str,
    current_price: float,
    trailing_stop: dict,
    send_webhook_fn,
    increment_alert_count_fn,
    last_alert_time: dict = None,
):
    """
    检查追踪止损是否触发。

    逻辑：
    - ATR Channel 模式（SINGLE）：
        - LONG 方向：当前价 < atr15m_lower（15m 下轨）-> 触发止损
        - SHORT 方向：当前价 > atr15m_upper（15m 上轨）-> 触发止损
    - Clustering SuperTrend 模式（PAIR）：
        - LONG 方向：当前价 < clustering_ts（追踪止损线）-> 触发止损
        - SHORT 方向：当前价 > clustering_ts -> 触发止损

    参数:
        symbol: 交易对名称
        current_price: 当前价格
        trailing_stop: 追踪止损状态字典
        send_webhook_fn: async 回调，发送 Webhook
        increment_alert_count_fn: 递增警报计数
    """
    if not current_price or current_price <= 0:
        return
    if symbol not in trailing_stop:
        return
    ts_entry = trailing_stop.get(symbol)
    if not ts_entry.get("active"):
        return
    try:
        direction = ts_entry.get("direction", "")

        # Clustering SuperTrend 模式（PAIR Trading）
        if ts_entry.get("use_clustering_ts"):
            clustering_ts_val = ts_entry.get("clustering_ts", 0)
            if clustering_ts_val > 0:
                if direction == "LONG" and current_price < clustering_ts_val:
                    await send_webhook_fn(
                        "ATR_Ch",
                        f"[{symbol}] TRAILING STOP",
                        {
                            "symbol": symbol,
                            "direction": "LONG",
                            "price": format_number(current_price),
                            "stop_line": format_number(clustering_ts_val),
                            "entry_price": format_number(
                                ts_entry.get("entry_price", 0)
                            ),
                            "reason": "trailing_stop",
                        },
                    )
                    increment_alert_count_fn()
                    ts_entry["active"] = False
                    if last_alert_time is not None:
                        last_alert_time[symbol] = 0
                elif direction == "SHORT" and current_price > clustering_ts_val:
                    await send_webhook_fn(
                        "ATR_Ch",
                        f"[{symbol}] TRAILING STOP",
                        {
                            "symbol": symbol,
                            "direction": "SHORT",
                            "price": format_number(current_price),
                            "stop_line": format_number(clustering_ts_val),
                            "entry_price": format_number(
                                ts_entry.get("entry_price", 0)
                            ),
                            "reason": "trailing_stop",
                        },
                    )
                    increment_alert_count_fn()
                    ts_entry["active"] = False
                    if last_alert_time is not None:
                        last_alert_time[symbol] = 0
            return

        # ATR Channel 模式（SINGLE）
        upper = ts_entry.get("atr15m_upper", 0)
        lower = ts_entry.get("atr15m_lower", 0)

        if direction == "LONG" and lower > 0 and current_price < lower:
            await send_webhook_fn(
                "ATR_Ch",
                f"[{symbol}] TRAILING STOP",
                {
                    "symbol": symbol,
                    "direction": "LONG",
                    "price": format_number(current_price),
                    "stop_line": format_number(lower),
                    "entry_price": format_number(ts_entry.get("entry_price", 0)),
                    "reason": "trailing_stop",
                },
            )
            increment_alert_count_fn()
            ts_entry["active"] = False
            if last_alert_time is not None:
                last_alert_time[symbol] = 0

        elif direction == "SHORT" and upper > 0 and current_price > upper:
            await send_webhook_fn(
                "ATR_Ch",
                f"[{symbol}] TRAILING STOP",
                {
                    "symbol": symbol,
                    "direction": "SHORT",
                    "price": format_number(current_price),
                    "stop_line": format_number(upper),
                    "entry_price": format_number(ts_entry.get("entry_price", 0)),
                    "reason": "trailing_stop",
                },
            )
            increment_alert_count_fn()
            ts_entry["active"] = False
            if last_alert_time is not None:
                last_alert_time[symbol] = 0
    except Exception:
        pass


async def recalculate_states_clustering(
    symbol: str,
    kline_cache: dict,
    benchmark: dict,
    clustering_states: dict,
    is_pair_trading: bool,
    st_period1: int,
    st_multiplier1: float,
    st_period2: int,
    st_multiplier2: float,
    vt_ema_signal: int,
    vt_ema_upper: int,
    vt_ema_lower: int,
    atr1h_period: int,
    atr1h_ma_type: str,
    atr1h_mult: float,
    atr15m_period: int,
    atr15m_ma_type: str,
    atr15m_mult: float,
    clustering_min_mult: float,
    clustering_max_mult: float,
    clustering_step: float,
    clustering_perf_alpha: float,
    clustering_from_cluster: str,
    clustering_max_iter: int,
    clustering_max_data: int,
):
    """
    重算 PairTrading 交易对的 Clustering SuperTrend 指标（批量模式）。

    适用于 PairTrading 符号（如 BTCUSDT/ETHUSDT），使用 Clustering SuperTrend
    代替传统的 ATR Channel。

    参数:
        symbol: 交易对名称
        kline_cache: K 线缓存
        benchmark: 指标缓存字典
        clustering_states: ClusteringState 缓存字典 {symbol: ClusteringState}
        is_pair_trading: 是否为配对交易对
        st_period1/multiplier1: Supertrend 第一周期参数
        st_period2/multiplier2: Supertrend 第二周期参数
        vt_ema_signal/upper/lower: Vegas Tunnel EMA 参数
        atr1h_period/ma_type/mult: 1h ATR Channel 参数
        atr15m_period/ma_type/mult: 15m ATR Channel 参数
        clustering_*: Clustering SuperTrend 参数
    """
    klines = kline_cache.get(symbol, [])
    if len(klines) < 200:
        return
    try:
        close = np.array([float(k[4]) for k in klines], dtype=float)
        if is_pair_trading:
            open_arr = np.array([float(k[1]) for k in klines], dtype=float)
            high = np.maximum(open_arr, close)
            low = np.minimum(open_arr, close)
        else:
            high = np.array([float(k[2]) for k in klines], dtype=float)
            low = np.array([float(k[3]) for k in klines], dtype=float)

        st1 = calculate_supertrend(high, low, close, st_period1, st_multiplier1)
        st2 = calculate_supertrend(high, low, close, st_period2, st_multiplier2)

        ema_s, ema_u, ema_l = calculate_vegas_tunnel(
            close, vt_ema_signal, vt_ema_upper, vt_ema_lower
        )

        atr1h = calculate_atr(high, low, close, atr1h_period, atr1h_ma_type)
        prev_atr_state = benchmark.get(symbol, {}).get(
            "atr1h_state", (float("nan"), float("nan"), 0)
        )
        for i in range(len(close)):
            upper, lower, ch = run_atr_channel(
                close[i], atr1h[i], atr1h_mult, prev_atr_state
            )
            prev_atr_state = (upper, lower, ch)
        atr1h_upper, atr1h_lower, atr1h_ch = prev_atr_state

        st1_val, st2_val = float(st1[-1]), float(st2[-1])
        ema_s_val = float(ema_s[-1])
        if not all(math.isfinite(v) for v in [st1_val, st2_val, ema_s_val]):
            return

        prev_state = clustering_states.get(symbol)
        ts, perf_ama, new_state = clustering_supertrend(
            close,
            high,
            low,
            atr1h,
            prev_state,
            min_mult=clustering_min_mult,
            max_mult=clustering_max_mult,
            step=clustering_step,
            perf_alpha=clustering_perf_alpha,
            from_cluster=clustering_from_cluster,
            max_iter=clustering_max_iter,
            max_data=clustering_max_data,
        )
        clustering_states[symbol] = new_state

        benchmark[symbol] = {
            "st1": st1_val,
            "st2": st2_val,
            "ema_s": ema_s_val,
            "ema_u": float(ema_u[-1]),
            "ema_l": float(ema_l[-1]),
            "kline_time": int(klines[-1][0]),
            "atr1h_upper": float(atr1h_upper) if not math.isnan(atr1h_upper) else 0,
            "atr1h_lower": float(atr1h_lower) if not math.isnan(atr1h_lower) else 0,
            "atr1h_ch": atr1h_ch,
            "atr1h_state": prev_atr_state,
            "ts": float(ts) if math.isfinite(ts) else 0,
            "perf_ama": float(perf_ama) if math.isfinite(perf_ama) else 0,
            "target_factor": new_state.target_factor,
        }
    except Exception:
        pass


async def check_signals_clustering(
    symbol: str,
    mark_prices: dict,
    mark_price_times: dict,
    benchmark: dict,
    trailing_stop: dict,
    last_clustering_state: dict,
    last_alert_time: dict,
    _initialized: bool,
    last_st_state: dict,
    clustering_states: dict,
    atr1h_ma_type: str,
    atr1h_period: int,
    atr1h_mult: float,
    atr15m_ma_type: str,
    atr15m_period: int,
    atr15m_mult: float,
    clustering_min_mult: float,
    clustering_max_mult: float,
    clustering_step: float,
    clustering_perf_alpha: float,
    clustering_from_cluster: str,
    clustering_max_iter: int,
    send_webhook_fn,
    increment_alert_count_fn,
):
    """
    Clustering SuperTrend 信号检测入口函数（包装器）。
    捕获异常防止信号检查崩溃影响其他逻辑。
    """
    if symbol not in benchmark:
        return
    try:
        await check_signals_clustering_impl(
            symbol,
            mark_prices,
            mark_price_times,
            benchmark,
            trailing_stop,
            last_clustering_state,
            last_alert_time,
            _initialized,
            last_st_state,
            clustering_states,
            atr1h_ma_type,
            atr1h_period,
            atr1h_mult,
            atr15m_ma_type,
            atr15m_period,
            atr15m_mult,
            clustering_min_mult,
            clustering_max_mult,
            clustering_step,
            clustering_perf_alpha,
            clustering_from_cluster,
            clustering_max_iter,
            send_webhook_fn,
            increment_alert_count_fn,
        )
    except Exception:
        pass


async def check_signals_clustering_impl(
    symbol: str,
    mark_prices: dict,
    mark_price_times: dict,
    benchmark: dict,
    trailing_stop: dict,
    last_clustering_state: dict,
    last_alert_time: dict,
    _initialized: bool,
    last_st_state: dict,
    clustering_states: dict,
    atr1h_ma_type: str,
    atr1h_period: int,
    atr1h_mult: float,
    atr15m_ma_type: str,
    atr15m_period: int,
    atr15m_mult: float,
    clustering_min_mult: float,
    clustering_max_mult: float,
    clustering_step: float,
    clustering_perf_alpha: float,
    clustering_from_cluster: str,
    clustering_max_iter: int,
    send_webhook_fn,
    increment_alert_count_fn,
):
    """
    Clustering SuperTrend 信号检测实现（用于 PairTrading）。

    逻辑：
    - 初始化阶段（_initialized=False）只记录 last_st_state，不推送信号
    - 非初始化阶段：
        - trend 从 -1 变为 1（空转多）-> 推送 LONG，建立追踪止损（用 ts 作为止损线）
        - trend 从 1 变为 -1（多转空）-> 推送 SHORT，建立追踪止损
    - 两次同向信号间隔至少 1 小时（3600 秒）
    - 追踪止损使用 ts（Clustering SuperTrend 追踪止损线）
    """
    current_price = mark_prices.get(symbol)
    if not current_price:
        return
    last_update = mark_price_times.get(symbol, 0)
    if time.time() - last_update > 300:
        return
    bm = benchmark.get(symbol)
    if not bm:
        return

    prev_trend = last_clustering_state.get(symbol, {}).get("trend", 0)
    target_factor = bm.get(
        "target_factor", (clustering_min_mult + clustering_max_mult) / 2
    )
    ts = bm.get("ts", 0)
    perf_ama = bm.get("perf_ama", 0)

    if not _initialized:
        last_st_state[symbol] = f"clust_{prev_trend}"
        return

    now = time.time()
    current_trend = prev_trend

    cluster_state = clustering_states.get(symbol)
    if cluster_state and math.isfinite(cluster_state.trend):
        current_trend = int(cluster_state.trend)

    if current_trend != prev_trend and prev_trend != 0:
        last_alert = last_alert_time.get(f"ClusterST_{symbol}", 0)
        if now - last_alert > 3600:
            last_alert_time[f"ClusterST_{symbol}"] = now
            direction = "LONG" if current_trend == 1 else "SHORT"
            last_clustering_state[symbol] = {"trend": current_trend, "sent": direction}
            await send_webhook_fn(
                "ClusterST",
                f"[{symbol}] {direction}",
                {
                    "symbol": symbol,
                    "direction": direction,
                    "price": format_number(current_price),
                    "ts": format_number(ts),
                    "perf_ama": format_number(perf_ama),
                    "target_factor": format_number(target_factor),
                },
            )
            increment_alert_count_fn()
            trailing_stop[symbol] = {
                "direction": direction,
                "entry_price": current_price,
                "entry_time": now,
                "atr_mult": atr15m_mult,
                "atr15m_upper": 0,
                "atr15m_lower": 0,
                "atr15m_state": (float("nan"), float("nan"), 0),
                "active": True,
                "use_clustering_ts": True,
                "clustering_ts": ts,
            }
    else:
        last_clustering_state[symbol] = {
            "trend": current_trend,
            "sent": last_clustering_state.get(symbol, {}).get("sent"),
        }

    # 同步 clustering_ts 到 trailing_stop（实时更新追踪止损线）
    if symbol in trailing_stop and trailing_stop[symbol].get("use_clustering_ts"):
        trailing_stop[symbol]["clustering_ts"] = ts
