"""
技术指标计算模块 - 纯计算函数，不涉及任何 I/O 或网络操作。

包含：
- calculate_supertrend: Supertrend 指标（双周期）
- calculate_vegas_tunnel: Vegas Tunnel 指标（3 EMA）
- calculate_dema: 双指数移动平均线
- calculate_hma: Hull 移动平均线
- calculate_tr: True Range（真实波幅）
- calculate_atr: ATR（平均真实波幅），支持多种 MA 类型
- run_atr_channel: ATR Channel 状态机（核心追踪止损逻辑）
"""

import math

import numpy as np
import talib


def calculate_supertrend(high, low, close, period, multiplier):
    """
    计算 Supertrend 指标。

    参数:
        high: 最高价数组 (np.array)
        low: 最低价数组 (np.array)
        close: 收盘价数组 (np.array)
        period: ATR 计算周期
        multiplier: ATR 倍数

    返回:
        supertrend 数组，nan 表示无效值
    """
    # 真实波幅 = max(H-L, |H-PC|, |L-PC|)，PC = 前一收盘价
    close_shifted = np.roll(close, 1)
    close_shifted[0] = close[0]
    tr1 = high - low
    tr2 = np.abs(high - close_shifted)
    tr3 = np.abs(low - close_shifted)
    tr = np.maximum(tr1, np.maximum(tr2, tr3))

    # ATR = SMA(TR[1:period+1])，之后使用 EMA 式平滑
    atr = np.zeros_like(close)
    if len(tr) >= period:
        sma = np.mean(tr[1 : period + 1])
        for i in range(period, len(tr)):
            if np.isnan(atr[i - 1]):
                atr[i] = sma
            else:
                atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    # 中价 = (H+L)/2，上轨 = 中价 + multiplier*ATR，下轨 = 中价 - multiplier*ATR
    hl_avg = (high + low) / 2
    upper_band = hl_avg + (multiplier * atr)
    lower_band = hl_avg - (multiplier * atr)

    supertrend = np.full_like(close, np.nan)
    direction = np.ones_like(close)  # 1=多头，-1=空头
    first_valid = period
    supertrend[first_valid] = lower_band[first_valid]
    direction[first_valid] = 1

    # 遍历后续 K 线：收盘价上穿上轨变为多头，下穿下轨变为空头
    for i in range(first_valid + 1, len(close)):
        if close[i] > upper_band[i - 1]:
            direction[i] = 1
        elif close[i] < lower_band[i - 1]:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1]
        supertrend[i] = lower_band[i] if direction[i] == 1 else upper_band[i]
    return supertrend


def calculate_vegas_tunnel(close, vt_ema_signal=9, vt_ema_upper=144, vt_ema_lower=169):
    """
    计算 Vegas Tunnel 指标（3 条 EMA 线）。

    参数:
        close: 收盘价数组
        vt_ema_signal: 信号线周期（默认 9）
        vt_ema_upper: 上轨 EMA 周期（默认 144）
        vt_ema_lower: 下轨 EMA 周期（默认 169）

    返回:
        (ema_signal, ema_upper, ema_lower) 三条 EMA 数组
    """
    ema_signal = talib.EMA(close, timeperiod=vt_ema_signal)
    ema_upper = talib.EMA(close, timeperiod=vt_ema_upper)
    ema_lower = talib.EMA(close, timeperiod=vt_ema_lower)
    return ema_signal, ema_upper, ema_lower


def calculate_dema(data, period):
    """
    双指数移动平均线 (DEMA)。
    DEMA = 2*EMA(data,period) - EMA(EMA(data,period),period)
    """
    ema1 = talib.EMA(data, timeperiod=period)
    ema2 = talib.EMA(ema1, timeperiod=period)
    return 2 * ema1 - ema2


def calculate_hma(data, period):
    """
    Hull 移动平均线 (HMA)。
    HMA = WMA(2*WMA(data,period/2) - WMA(data,period), sqrt(period))
    """
    return talib.WMA(
        2 * talib.WMA(data, period // 2) - talib.WMA(data, period),
        int(math.sqrt(period)),
    )


def calculate_tr(high, low, close):
    """
    真实波幅 (True Range)。
    TR = max(H-L, |H-PC|, |L-PC|)，PC = 前一收盘价
    """
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    return np.maximum(tr1, np.maximum(tr2, tr3))


def calculate_atr(high, low, close, period, ma_type="DEMA"):
    """
    平均真实波幅 (ATR)，支持多种移动平均类型。

    参数:
        high, low, close: 价格数组
        period: ATR 周期
        ma_type: MA 类型，DEMA | HMA | EMA | SMA | WMA（默认 DEMA）

    返回:
        ATR 数组
    """
    tr = calculate_tr(high, low, close)
    if ma_type == "DEMA":
        atr = calculate_dema(tr, period)
    elif ma_type == "HMA":
        atr = calculate_hma(tr, period)
    elif ma_type == "EMA":
        atr = talib.EMA(tr, period)
    elif ma_type == "SMA":
        atr = talib.SMA(tr, period)
    elif ma_type == "WMA":
        atr = talib.WMA(tr, period)
    else:
        atr = talib.EMA(tr, period)
    return atr


def run_atr_channel(close, atr, mult, prev_state):
    """
    ATR Channel 状态机 - 根据当前价格和 ATR 更新通道上下轨。

    逻辑：
    - 初始：上下轨以当前价格为中心，宽度 = atr*mult
    - 价格上涨突破上轨：上轨更新为当前价格，下轨取 max(旧下轨, 新下轨)（只抬升不下降）
    - 价格下跌突破下轨：下轨更新为当前价格，上轨取 min(旧上轨, 新上轨)（只下降不抬升）

    参数:
        close: 当前收盘价（标量）
        atr: 当前 ATR 值（标量）
        mult: ATR 倍数（用于计算通道宽度）
        prev_state: 上一个状态 (prev_upper, prev_lower, prev_ch)
                    prev_ch: 0=中性, 1=多头通道, -1=空头通道

    返回:
        (upper_band, lower_band, ch_state) 新状态
    """
    upper_band, lower_band, ch_state = prev_state
    if math.isnan(atr) or atr <= 0:
        return upper_band, lower_band, ch_state

    # 初始状态：以当前价格为中心建立通道
    if math.isnan(upper_band) or math.isnan(lower_band):
        width = atr * mult
        upper_band = close + width / 2
        lower_band = close - width / 2
        ch_state = 0
    # 价格上涨突破上轨 -> 多头通道（追踪止损上移）
    elif close > upper_band:
        width = atr * mult
        new_upper = close
        new_lower = close - width
        # 下轨只抬升不下降：保留更高的旧下轨
        lower_band = max(lower_band, new_lower)
        upper_band = new_upper
        ch_state = 1
    # 价格下跌突破下轨 -> 空头通道（追踪止损下移）
    elif close < lower_band:
        width = atr * mult
        new_lower = close
        new_upper = close + width
        # 上轨只下降不抬升：保留更低的旧上轨
        upper_band = min(upper_band, new_upper)
        lower_band = new_lower
        ch_state = -1
    return upper_band, lower_band, ch_state
