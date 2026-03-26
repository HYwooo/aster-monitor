import asyncio
import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Optional

import aiohttp
import toml
import numpy as np
import talib

from asterdex import HybridClient, Network
from asterdex.logging_config import get_logger

logger = get_logger(__name__)

WEBHOOK_LOG_FILE = "webhook_history.log"
LOG_RETENTION_DAYS = 7


def cleanup_old_logs():
    try:
        if not os.path.exists(WEBHOOK_LOG_FILE):
            return
        cutoff_time = time.time() - (LOG_RETENTION_DAYS * 24 * 3600)
        temp_file = WEBHOOK_LOG_FILE + ".tmp"
        with open(WEBHOOK_LOG_FILE, "r", encoding="utf-8") as f_in:
            with open(temp_file, "w", encoding="utf-8") as f_out:
                for line in f_in:
                    try:
                        log_time_str = line.split("]")[0].strip("[")
                        log_time = datetime.strptime(log_time_str, "%Y-%m-%d %H:%M:%S")
                        if log_time.timestamp() > cutoff_time:
                            f_out.write(line)
                    except:
                        f_out.write(line)
        os.replace(temp_file, WEBHOOK_LOG_FILE)
        logger.info(f"Log cleanup completed, kept last {LOG_RETENTION_DAYS} days")
    except Exception as e:
        logger.warning(f"Log cleanup failed: {e}")


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return toml.load(config_path)


def save_config(config_path: str, config: dict):
    with open(config_path, "w", encoding="utf-8") as f:
        toml.dump(config, f)
    print(f"Config saved: {config_path}")


def update_symbols(config_path: str, action: str, symbols: list):
    config = load_config(config_path)
    current = set(config["symbols"]["monitor_list"])
    if action == "add":
        current.update(symbols)
        print(f"Added: {symbols}")
    elif action == "remove":
        current -= set(symbols)
        print(f"Removed: {symbols}")
    config["symbols"]["monitor_list"] = sorted(list(current))
    save_config(config_path, config)
    print(f"Current symbols: {config['symbols']['monitor_list']}")


def create_config(config_path: str, webhook_url: str, symbols: list = None) -> dict:
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "XAUUSDT"]
    config = {
        "webhook": {"url": webhook_url},
        "symbols": {"monitor_list": symbols},
        "supertrend": {
            "period1": 9,
            "multiplier1": 2.5,
            "period2": 14,
            "multiplier2": 1.7,
        },
        "vegas": {"ema_signal": 9, "ema_upper": 144, "ema_lower": 169},
        "service": {
            "heartbeat_file": "notification_heartbeat",
            "heartbeat_timeout": 120,
        },
    }
    with open(config_path, "w", encoding="utf-8") as f:
        toml.dump(config, f)
    print(f"Config created: {config_path}")
    return config


def log_warning(msg: str):
    logger.warning(msg)


def log_error(msg: str):
    logger.error(msg)


class NotificationService:
    def __init__(self, config_path: str = "config.toml"):
        self.config = self._load_config(config_path)
        self.symbols = self.config["symbols"]["monitor_list"]
        self.webhook_url = self.config["webhook"]["url"]
        self.st_period1 = self.config["supertrend"]["period1"]
        self.st_multiplier1 = self.config["supertrend"]["multiplier1"]
        self.st_period2 = self.config["supertrend"]["period2"]
        self.st_multiplier2 = self.config["supertrend"]["multiplier2"]
        self.vt_ema_signal = self.config["vegas"]["ema_signal"]
        self.vt_ema_upper = self.config["vegas"]["ema_upper"]
        self.vt_ema_lower = self.config["vegas"]["ema_lower"]
        self.heartbeat_file = self.config["service"]["heartbeat_file"]
        self.client: Optional[HybridClient] = None
        self.mark_prices: dict[str, float] = {}
        self.kline_cache: dict[str, list] = {}
        self.benchmark: dict[str, dict] = {}
        self.last_st_state: dict[str, str] = {}
        self.last_vt_state: dict[str, str] = {}
        self.last_alert_time: dict[str, float] = {}
        self.last_kline_time: dict[str, int] = {}
        self.breakout_monitor: dict[str, dict] = {}
        self.connected = False
        self.running = False

    def _load_config(self, config_path: str) -> dict:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return toml.load(config_path)

    async def send_webhook(self, alert_type: str, message: str, extra: dict = None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # TradingView Alert Format
        full_content = f"{alert_type}: {message}"
        if extra:
            full_content += f" | {json.dumps(extra)}"

        logger.info(f"[WEBHOOK] {full_content}")

        try:
            with open(WEBHOOK_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {full_content}\n")
        except Exception as e:
            logger.warning(f"Write webhook log failed: {e}")

        msg = {"msg_type": "text", "content": {"text": full_content}}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=msg, timeout=10) as resp:
                    if resp.status == 200:
                        logger.info(f"Webhook sent: {alert_type}")
                    else:
                        log_error(f"Webhook failed: {resp.status}")
        except Exception as e:
            log_error(f"Webhook error: {e}")

    async def send_error(self, error: Exception, context: str = ""):
        error_msg = f"ERROR {context}: {type(error).__name__} - {str(error)}"
        log_error(f"Error{context}: {error}")
        await self.send_webhook("SYSTEM", error_msg)

    def warn(self, msg: str, context: str = ""):
        log_warning(f"Warning{f' ({context})' if context else ''}: {msg}")

    def write_heartbeat(self):
        try:
            with open(self.heartbeat_file, "w") as f:
                f.write(str(time.time()))
        except Exception as e:
            log_warning(f"Heartbeat write failed: {e}")

    async def connect(self):
        try:
            self.client = HybridClient(network=Network.MAINNET)
            self.client._ws_client._callbacks.setdefault("__error__", []).append(
                self._on_ws_error
            )
            await self.client.connect()
            self.connected = True
            await self.send_webhook("SYSTEM", "Aster DEX connected to MAINNET")
            for symbol in self.symbols:
                self.subscribe_symbol(symbol)
        except Exception as e:
            await self.send_error(e, "Connect")
            raise

    async def _on_ws_error(self, error):
        log_error(f"WebSocket error: {error}")
        if self.connected:
            self.connected = False
            await self.send_webhook("SYSTEM", f"WebSocket disconnected: {error}")

    def subscribe_symbol(self, symbol: str):
        @self.client.on_ticker(symbol)
        async def on_ticker(data):
            try:
                price = float(data.get("lastPrice", 0))
                self.mark_prices[symbol] = price
                await self.check_signals(symbol)
            except Exception as e:
                self.warn(f"ticker callback error: {e}", f"symbol={symbol}")

        @self.client.on_kline(symbol, "1h")
        async def on_kline(kline):
            try:
                if kline.get("x", False):
                    await self.update_klines(symbol)
            except Exception as e:
                self.warn(f"kline callback error: {e}", f"symbol={symbol}")

        logger.info(f"Subscribed to {symbol}")

    async def fetch_klines(
        self, symbol: str, limit: int = 500, interval: str = "1h"
    ) -> list:
        try:
            url = f"https://fapi.asterdex.com/fapi/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data if data else []
                    else:
                        self.warn(f"fetch_klines HTTP {resp.status} for {symbol}")
                        return []
        except Exception as e:
            self.warn(f"fetch_klines error for {symbol}: {e}")
            return []

    def calculate_supertrend(self, high, low, close, period, multiplier):
        close_shifted = np.roll(close, 1)
        close_shifted[0] = close[0]
        tr1 = high - low
        tr2 = np.abs(high - close_shifted)
        tr3 = np.abs(low - close_shifted)
        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        atr = np.zeros_like(close)
        if len(tr) >= period:
            sma = np.mean(tr[1 : period + 1])
            for i in range(period, len(tr)):
                if np.isnan(atr[i - 1]):
                    atr[i] = sma
                else:
                    atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
        hl_avg = (high + low) / 2
        upper_band = hl_avg + (multiplier * atr)
        lower_band = hl_avg - (multiplier * atr)
        supertrend = np.full_like(close, np.nan)
        direction = np.ones_like(close)
        first_valid = period
        supertrend[first_valid] = lower_band[first_valid]
        direction[first_valid] = 1
        for i in range(first_valid + 1, len(close)):
            if close[i] > upper_band[i - 1]:
                direction[i] = 1
            elif close[i] < lower_band[i - 1]:
                direction[i] = -1
            else:
                direction[i] = direction[i - 1]
            supertrend[i] = lower_band[i] if direction[i] == 1 else upper_band[i]
        return supertrend

    def calculate_vegas_tunnel(self, close):
        ema_signal = talib.EMA(close, timeperiod=self.vt_ema_signal)
        ema_upper = talib.EMA(close, timeperiod=self.vt_ema_upper)
        ema_lower = talib.EMA(close, timeperiod=self.vt_ema_lower)
        return ema_signal, ema_upper, ema_lower

    async def start_breakout_monitor(self, symbol, direction, price, trigger_time):
        if symbol in self.breakout_monitor:
            return
        history = await self.fetch_klines(symbol, limit=20, interval="15m")
        if not history:
            self.warn(f"Failed to fetch 15m history for {symbol}", "breakout")
            return
        self.breakout_monitor[symbol] = {
            "direction": direction,
            "trigger_price": price,
            "trigger_time": trigger_time,
            "kline_15m_count": 0,
            "klines_15m": history,
        }

        @self.client.on_kline(symbol, "15m")
        async def on_15m_kline(kline):
            await self.on_15m_kline(symbol, kline)

        logger.info(f"Breakout monitor started for {symbol}, direction={direction}")

    async def stop_breakout_monitor(self, symbol):
        if symbol in self.breakout_monitor:
            del self.breakout_monitor[symbol]
            logger.info(f"Breakout monitor stopped for {symbol}")

    async def on_15m_kline(self, symbol, kline):
        if symbol not in self.breakout_monitor:
            return
        try:
            monitor = self.breakout_monitor[symbol]
            kline_close = float(kline.get("c", 0))
            monitor["klines_15m"].append([int(kline.get("t", 0)), 0, kline_close])
            if len(monitor["klines_15m"]) > 20:
                monitor["klines_15m"] = monitor["klines_15m"][-20:]
            monitor["kline_15m_count"] += 1
            await self.check_breakout(symbol)
        except Exception as e:
            self.warn(f"on_15m_kline error for {symbol}: {e}", "breakout")
            await self.stop_breakout_monitor(symbol)

    async def check_breakout(self, symbol):
        monitor = self.breakout_monitor.get(symbol)
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

        if direction == "11":
            if current_close > max_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} LONG CONFIRMED | price={current_close:.4f} trigger={trigger_price:.4f}",
                    {"symbol": symbol, "direction": "long", "confirmed": True},
                )
                await self.stop_breakout_monitor(symbol)
            elif current_close < min_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} LONG FALSE (REVERSE) | price={current_close:.4f}",
                    {
                        "symbol": symbol,
                        "direction": "long",
                        "confirmed": False,
                        "reason": "reverse",
                    },
                )
                await self.stop_breakout_monitor(symbol)
            elif count >= 20:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} LONG FALSE (NO_CONTINUATION) | price={current_close:.4f}",
                    {
                        "symbol": symbol,
                        "direction": "long",
                        "confirmed": False,
                        "reason": "no_continuation",
                    },
                )
                await self.stop_breakout_monitor(symbol)

        elif direction == "00":
            if current_close < min_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} SHORT CONFIRMED | price={current_close:.4f} trigger={trigger_price:.4f}",
                    {"symbol": symbol, "direction": "short", "confirmed": True},
                )
                await self.stop_breakout_monitor(symbol)
            elif current_close > max_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} SHORT FALSE (REVERSE) | price={current_close:.4f}",
                    {
                        "symbol": symbol,
                        "direction": "short",
                        "confirmed": False,
                        "reason": "reverse",
                    },
                )
                await self.stop_breakout_monitor(symbol)
            elif count >= 20:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} SHORT FALSE (NO_CONTINUATION) | price={current_close:.4f}",
                    {
                        "symbol": symbol,
                        "direction": "short",
                        "confirmed": False,
                        "reason": "no_continuation",
                    },
                )
                await self.stop_breakout_monitor(symbol)

    async def update_klines(self, symbol: str):
        try:
            klines = await self.fetch_klines(symbol)
            if klines:
                self.kline_cache[symbol] = klines
                last_time = self.last_kline_time.get(symbol, 0)
                new_time = int(klines[-1][0])
                if new_time > last_time:
                    self.last_kline_time[symbol] = new_time
                    await self.recalculate_states(symbol)
        except Exception as e:
            self.warn(f"update_klines error for {symbol}: {e}")

    async def recalculate_states(self, symbol: str):
        klines = self.kline_cache.get(symbol, [])
        if len(klines) < 200:
            return
        try:
            close = np.array([float(k[4]) for k in klines], dtype=float)
            high = np.array([float(k[2]) for k in klines], dtype=float)
            low = np.array([float(k[3]) for k in klines], dtype=float)
            st1 = self.calculate_supertrend(
                high, low, close, self.st_period1, self.st_multiplier1
            )
            st2 = self.calculate_supertrend(
                high, low, close, self.st_period2, self.st_multiplier2
            )
            ema_s, ema_u, ema_l = self.calculate_vegas_tunnel(close)
            if np.isnan(st1[-1]) or np.isnan(st2[-1]):
                return
            self.benchmark[symbol] = {
                "st1": st1[-1],
                "st2": st2[-1],
                "ema_s": ema_s[-1],
                "ema_u": ema_u[-1],
                "ema_l": ema_l[-1],
                "kline_time": int(klines[-1][0]),
            }
        except Exception as e:
            self.warn(f"recalculate_states error for {symbol}: {e}")

    async def check_signals(self, symbol: str):
        if symbol not in self.benchmark:
            return
        try:
            await self.check_signals_impl(symbol)
        except Exception as e:
            self.warn(f"check_signals error for {symbol}: {e}")

    async def check_signals_impl(self, symbol: str):
        current_price = self.mark_prices.get(symbol)
        if not current_price:
            return
        bm = self.benchmark.get(symbol)
        if not bm:
            return
        st1_val = bm["st1"]
        st2_val = bm["st2"]
        ema_s_val = bm["ema_s"]
        ema_u_val = bm["ema_u"]
        ema_l_val = bm["ema_l"]

        st_state = ("1" if current_price > st1_val else "0") + (
            "1" if current_price > st2_val else "0"
        )
        vt_state = ("1" if ema_s_val > ema_u_val else "0") + (
            "1" if ema_s_val > ema_l_val else "0"
        )

        now = time.time()

        if st_state != self.last_st_state.get(symbol):
            old_state = self.last_st_state.get(symbol, "??")
            self.last_st_state[symbol] = st_state
            if st_state in ["11", "00"]:
                last_alert = self.last_alert_time.get(f"ST_{symbol}", 0)
                if now - last_alert > 3600:
                    self.last_alert_time[f"ST_{symbol}"] = now
                    direction = "LONG" if st_state == "11" else "SHORT"
                    await self.send_webhook(
                        "ST",
                        f"{symbol} {st_state} {direction} | price={current_price:.4f} st1={st1_val:.4f} st2={st2_val:.4f}",
                        {"symbol": symbol, "state": st_state, "direction": direction},
                    )
                    await self.start_breakout_monitor(
                        symbol, st_state, current_price, bm["kline_time"]
                    )

        if vt_state != self.last_vt_state.get(symbol):
            old_state = self.last_vt_state.get(symbol, "??")
            self.last_vt_state[symbol] = vt_state
            if vt_state in ["11", "00"]:
                last_alert = self.last_alert_time.get(f"VT_{symbol}", 0)
                if now - last_alert > 3600:
                    self.last_alert_time[f"VT_{symbol}"] = now
                    direction = "BULLISH" if vt_state == "11" else "BEARISH"
                    await self.send_webhook(
                        "VT",
                        f"{symbol} {vt_state} {direction} | ema9={ema_s_val:.4f} ema144={ema_u_val:.4f} ema169={ema_l_val:.4f}",
                        {"symbol": symbol, "state": vt_state, "direction": direction},
                    )

    async def initialize(self):
        logger.info(f"Initializing klines for {len(self.symbols)} symbols...")
        for symbol in self.symbols:
            await self.update_klines(symbol)
        await asyncio.sleep(2)
        for symbol in self.symbols:
            if symbol in self.benchmark:
                bm = self.benchmark[symbol]
                current_price = self.mark_prices.get(symbol)
                if not current_price and symbol in self.kline_cache:
                    klines = self.kline_cache[symbol]
                    current_price = float(klines[-1][4]) if klines else 0
                st_state = ""
                if current_price and current_price > bm["st1"]:
                    st_state += "1"
                else:
                    st_state += "0"
                if current_price and current_price > bm["st2"]:
                    st_state += "1"
                else:
                    st_state += "0"
                vt_state = ("1" if bm["ema_s"] > bm["ema_u"] else "0") + (
                    "1" if bm["ema_s"] > bm["ema_l"] else "0"
                )
                self.last_st_state[symbol] = st_state
                self.last_vt_state[symbol] = vt_state
                logger.info(f"{symbol}: ST={st_state}, VT={vt_state}")

    async def heartbeat_loop(self):
        while self.running:
            self.write_heartbeat()
            await asyncio.sleep(30)

    async def run(self):
        self.running = True
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        await self.connect()
        await self.initialize()
        last_kline_update = 0
        while self.running:
            try:
                await asyncio.sleep(60)
                current_time = time.time()
                if current_time - last_kline_update > 3600:
                    for symbol in self.symbols:
                        await self.update_klines(symbol)
                    last_kline_update = current_time
                if self.client and not self.client.is_connected:
                    log_warning("Connection lost, reconnecting...")
                    await self.send_webhook(
                        "SYSTEM", "Connection lost, reconnecting..."
                    )
                    await self.client.connect()
                    self.connected = True
                    await self.send_webhook("SYSTEM", "Reconnected to Aster DEX")
                    for symbol in self.symbols:
                        self.subscribe_symbol(symbol)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.send_error(e, "Main Loop")
                await asyncio.sleep(5)
        heartbeat_task.cancel()

    async def stop(self):
        self.running = False
        self.breakout_monitor.clear()
        if self.client:
            await self.client.disconnect()
        if os.path.exists(self.heartbeat_file):
            os.remove(self.heartbeat_file)
        logger.info("Service stopped")


async def main():
    parser = argparse.ArgumentParser(description="Aster Monitor")
    parser.add_argument(
        "--config", "-c", default="config.toml", help="Config file path"
    )
    parser.add_argument("--webhook", "-w", help="Feishu WebHook URL")
    parser.add_argument("--symbols", "-s", help="Comma-separated symbol list")
    parser.add_argument("--add-symbol", "-a", help="Add symbol(s)")
    parser.add_argument("--remove-symbol", "-r", help="Remove symbol(s)")
    parser.add_argument(
        "--list-symbols", "-l", action="store_true", help="List symbols"
    )
    args = parser.parse_args()

    if args.webhook:
        symbols = args.symbols.split(",") if args.symbols else None
        create_config(args.config, args.webhook, symbols)
    elif args.add_symbol:
        update_symbols(
            args.config, "add", [s.strip().upper() for s in args.add_symbol.split(",")]
        )
        return
    elif args.remove_symbol:
        update_symbols(
            args.config,
            "remove",
            [s.strip().upper() for s in args.remove_symbol.split(",")],
        )
        return
    elif args.list_symbols:
        config = load_config(args.config)
        print(f"Current symbols: {config['symbols']['monitor_list']}")
        return

    cleanup_old_logs()
    service = NotificationService(args.config)
    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt, stopping...")
    except Exception as e:
        log_error(f"Main error: {e}")
        await service.send_error(e, f"Main\n{traceback.format_exc()}")
    finally:
        await service.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()
