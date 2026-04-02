"""
突破监控模块 - 监控价格突破后的二次确认信号。

职责：
- start_breakout_monitor: 开始监控突破后的 15m K 线
- _on_15m_kline: 处理 15m K 线数据（内部用）
- check_breakout: 检测突破是否确认或失败
"""

import time

from notifications import format_number
from rest_api import fetch_klines


async def start_breakout_monitor(
    symbol: str,
    direction: str,
    price: float,
    trigger_time: float,
    breakout_monitor: dict,
    _is_pair_trading: bool,
    _breakout_comp_prices: dict,
    client,
    update_15m_atr_fn,
    fetch_pair_klines_fn=None,
    proxy: str = None,
):
    """
    开始监控指定交易对的突破情况。

    监控逻辑（以 direction="11" 即 LONG 为例）：
    - 获取最近 20 条 15m K 线作为基准
    - 等待下一条 15m K 线完成：
        - 如果新收盘价 > 前 20 条最高价 -> 确认突破，推送 LONG CONFIRMED
        - 如果新收盘价 < 前 20 条最低价 -> 假突破，推送 LONG FALSE (REVERSE)
        - 如果 20 条 K 线内无突破 -> 失效，推送 LONG FALSE (NO_CONTINUATION)
    - 空头（direction="00"）逻辑对称

    参数:
        symbol: 交易对名称
        direction: 突破方向，"11"=LONG，"00"=SHORT
        price: 触发价格
        trigger_time: 触发时间戳
        breakout_monitor: 突破监控状态字典（会被写入）
        _is_pair_trading: 判断是否为配对交易对的函数
        _breakout_comp_prices: 配对交易组件价格缓存
        client: asterdex WebSocketClient 实例（用于注册回调）
        update_15m_atr_fn: async 回调，触发 15m ATR 更新
        fetch_pair_klines_fn: async 回调，获取配对 K 线（可选）
        proxy: HTTP 代理
    """
    if symbol in breakout_monitor:
        return

    # 获取最近 20 条 15m K 线作为基准
    if _is_pair_trading(symbol):
        history = await (fetch_pair_klines_fn or fetch_klines)(
            symbol,
            limit=20,
            interval="15m",
            proxy=proxy,
        )
    else:
        history = await fetch_klines(
            symbol,
            limit=20,
            interval="15m",
            proxy=proxy,
        )
    if not history:
        return

    breakout_monitor[symbol] = {
        "direction": direction,
        "trigger_price": price,
        "trigger_time": trigger_time,
        "kline_15m_count": 0,  # 自基准 K 线之后完成的数量
        "klines_15m": history,
    }

    # 配对交易：初始化组件价格缓存
    if _is_pair_trading(symbol):
        parts = symbol.split("/")
        _breakout_comp_prices[parts[0]] = 0
        _breakout_comp_prices[parts[1]] = 0

        async def on_15m_kline_p1(kline):
            await _on_15m_kline(
                symbol,
                kline,
                parts[0],
                parts[1],
                breakout_monitor,
                _is_pair_trading,
                _breakout_comp_prices,
            )

        async def on_15m_kline_p2(kline):
            await _on_15m_kline(
                symbol,
                kline,
                parts[0],
                parts[1],
                breakout_monitor,
                _is_pair_trading,
                _breakout_comp_prices,
            )

        client.on_kline(parts[0], "15m")(on_15m_kline_p1)
        client.on_kline(parts[1], "15m")(on_15m_kline_p2)
    else:

        async def on_15m_kline(kline):
            await _on_15m_kline(
                symbol,
                kline,
                None,
                None,
                breakout_monitor,
                _is_pair_trading,
                _breakout_comp_prices,
            )

        client.on_kline(symbol, "15m")(on_15m_kline)


async def _on_15m_kline(
    symbol,
    kline,
    comp1,
    comp2,
    breakout_monitor: dict,
    _is_pair_trading_fn,
    _breakout_comp_prices: dict,
):
    """
    处理 15m K 线数据，追加到监控 K 线列表。

    配对交易：需要等待两个组件的 K 线都到达后，计算配对价格再追加。
    """
    if symbol not in breakout_monitor:
        return
    try:
        monitor = breakout_monitor[symbol]
        t = int(kline.get("t", 0))

        if _is_pair_trading_fn(symbol):
            if comp1 is None or comp2 is None:
                return
            c = float(kline.get("c", 0))
            if c == 0:
                return
            # 配对价格需要两个组件的价格
            other_comp = comp2 if comp1 == symbol.split("/")[0] else comp1
            other_price = _breakout_comp_prices.get(other_comp, 0)
            if other_price == 0:
                _breakout_comp_prices[comp1] = c
                return
            ratio_close = c / other_price
            _breakout_comp_prices[comp1] = 0
            _breakout_comp_prices[comp2] = 0
            monitor["klines_15m"].append([t, 0, ratio_close])
        else:
            kline_close = float(kline.get("c", 0))
            monitor["klines_15m"].append([t, 0, kline_close])

        # 保持最近 20 条
        if len(monitor["klines_15m"]) > 20:
            monitor["klines_15m"] = monitor["klines_15m"][-20:]
        monitor["kline_15m_count"] += 1
    except Exception:
        pass


async def check_breakout(
    symbol: str,
    breakout_monitor: dict,
    send_webhook_fn,
    increment_alert_count_fn,
    stop_breakout_monitor_fn=None,
):
    """
    检测突破是否确认或失败。

    LONG (direction="11")：
    - 确认：新收盘价 > 前 20 条最高价
    - 假突破（反转）：新收盘价 < 前 20 条最低价
    - 失效（无延续）：20 条内未确认也未反转

    SHORT (direction="00")：逻辑对称

    参数:
        symbol: 交易对名称
        breakout_monitor: 突破监控状态字典
        send_webhook_fn: async 回调，发送 Webhook
        increment_alert_count_fn: 递增警报计数
        stop_breakout_monitor_fn: async 回调，停止监控（可选）
    """
    monitor = breakout_monitor.get(symbol)
    if not monitor:
        return
    direction = monitor["direction"]
    trigger_price = monitor["trigger_price"]
    klines = monitor["klines_15m"]
    count = monitor["kline_15m_count"]
    if len(klines) < 2:
        return

    current_close = klines[-1][2]
    prev_closes = [k[2] for k in klines[:-1]]
    max_prev = max(prev_closes) if prev_closes else 0
    min_prev = min(prev_closes) if prev_closes else float("inf")

    # ===================== LONG (direction="11") =====================
    if direction == "11":
        if current_close > max_prev:
            await send_webhook_fn(
                "BREAKOUT",
                f"{symbol} LONG CONFIRMED",
                {
                    "symbol": symbol,
                    "direction": "LONG",
                    "confirmed": True,
                    "price": format_number(current_close),
                    "trigger": format_number(trigger_price),
                },
            )
            increment_alert_count_fn()
            if stop_breakout_monitor_fn:
                await stop_breakout_monitor_fn(symbol)

        elif current_close < min_prev:
            await send_webhook_fn(
                "BREAKOUT",
                f"{symbol} LONG FALSE (REVERSE)",
                {
                    "symbol": symbol,
                    "direction": "LONG",
                    "confirmed": False,
                    "reason": "reverse",
                    "price": format_number(current_close),
                },
            )
            increment_alert_count_fn()
            if stop_breakout_monitor_fn:
                await stop_breakout_monitor_fn(symbol)

        elif count >= 20:
            await send_webhook_fn(
                "BREAKOUT",
                f"{symbol} LONG FALSE (NO_CONTINUATION)",
                {
                    "symbol": symbol,
                    "direction": "LONG",
                    "confirmed": False,
                    "reason": "no_continuation",
                    "price": format_number(current_close),
                },
            )
            increment_alert_count_fn()
            if stop_breakout_monitor_fn:
                await stop_breakout_monitor_fn(symbol)

    # ===================== SHORT (direction="00") =====================
    elif direction == "00":
        if current_close < min_prev:
            await send_webhook_fn(
                "BREAKOUT",
                f"{symbol} SHORT CONFIRMED",
                {
                    "symbol": symbol,
                    "direction": "SHORT",
                    "confirmed": True,
                    "price": format_number(current_close),
                    "trigger": format_number(trigger_price),
                },
            )
            increment_alert_count_fn()
            if stop_breakout_monitor_fn:
                await stop_breakout_monitor_fn(symbol)

        elif current_close > max_prev:
            await send_webhook_fn(
                "BREAKOUT",
                f"{symbol} SHORT FALSE (REVERSE)",
                {
                    "symbol": symbol,
                    "direction": "SHORT",
                    "confirmed": False,
                    "reason": "reverse",
                    "price": format_number(current_close),
                },
            )
            increment_alert_count_fn()
            if stop_breakout_monitor_fn:
                await stop_breakout_monitor_fn(symbol)

        elif count >= 20:
            await send_webhook_fn(
                "BREAKOUT",
                f"{symbol} SHORT FALSE (NO_CONTINUATION)",
                {
                    "symbol": symbol,
                    "direction": "SHORT",
                    "confirmed": False,
                    "reason": "no_continuation",
                    "price": format_number(current_close),
                },
            )
            increment_alert_count_fn()
            if stop_breakout_monitor_fn:
                await stop_breakout_monitor_fn(symbol)
