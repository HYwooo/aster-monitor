import asyncio
import argparse
import fcntl
import json
import math
import os
import sys
import threading
import time
import traceback
from datetime import datetime
from typing import Optional

import aiohttp
import toml
import numpy as np
import talib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from asterdex import WebSocketClient, Network
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
        "webhook": {"url": webhook_url, "format": "card"},
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
        "proxy": {
            "enable": False,
            "url": "",
        },
        "report": {
            "enable": False,
            "times": ["08:00", "20:00"],
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


def format_number(value: float) -> str:
    if not math.isfinite(value):
        return str(value)
    if value == 0:
        return "0.00000000"
    abs_val = abs(value)
    if abs_val < 1:
        return f"{value:.8f}"
    int_digits = len(str(int(abs_val)))
    decimal_places = max(0, 8 - int_digits)
    formatted = f"{value:.{decimal_places}f}"
    return formatted.rstrip("0").rstrip(".")


class NotificationService:
    def __init__(self, config_path: str = "config.toml"):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.symbols = self.config["symbols"]["monitor_list"]
        self.webhook_url = self.config["webhook"]["url"]
        self.webhook_format = self.config["webhook"].get("format", "card")
        self.st_period1 = self.config["supertrend"]["period1"]
        self.st_multiplier1 = self.config["supertrend"]["multiplier1"]
        self.st_period2 = self.config["supertrend"]["period2"]
        self.st_multiplier2 = self.config["supertrend"]["multiplier2"]
        self.vt_ema_signal = self.config["vegas"]["ema_signal"]
        self.vt_ema_upper = self.config["vegas"]["ema_upper"]
        self.vt_ema_lower = self.config["vegas"]["ema_lower"]
        self.heartbeat_file = self.config["service"]["heartbeat_file"]
        self.proxy_enable = self.config.get("proxy", {}).get("enable", False)
        self.proxy_url = self.config.get("proxy", {}).get("url", "")
        self.report_enable = self.config.get("report", {}).get("enable", False)
        self.report_times = self.config.get("report", {}).get(
            "times", ["08:00", "20:00"]
        )
        self.timezone = self.config.get("settings", {}).get("timezone", "Z")
        self.max_log_lines = self.config.get("settings", {}).get("max_log_lines", 1000)
        self.client: Optional[WebSocketClient] = None
        self.mark_prices: dict[str, float] = {}
        self.mark_price_times: dict[str, float] = {}
        self.kline_cache: dict[str, list] = {}
        self.benchmark: dict[str, dict] = {}
        self.last_st_state: dict[str, str] = {}
        self.last_vt_state: dict[str, str] = {}
        self.last_alert_time: dict[str, float] = {}
        self.last_kline_time: dict[str, int] = {}
        self.breakout_monitor: dict[str, dict] = {}
        self.connected = False
        self.running = False
        self.observer: Optional[Observer] = None
        self._initialized = False
        self._status_print_enabled = True
        self._alert_count = 0
        self._last_report_time = 0
        self._lock = threading.Lock()

    def _get_timestamp(self) -> str:
        tz = self.timezone
        if tz == "Z":
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        elif tz.startswith("+") or tz.startswith("-"):
            return datetime.now().strftime(f"%Y-%m-%dT%H:%M:%S{tz}")
        else:
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def _rotate_webhook_log_if_needed(self):
        try:
            if not os.path.exists(WEBHOOK_LOG_FILE):
                return
            with open(WEBHOOK_LOG_FILE, "r+", encoding="utf-8") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                lines = f.readlines()
                if len(lines) > self.max_log_lines:
                    f.seek(0)
                    f.truncate()
                    f.writelines(lines[-self.max_log_lines :])
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            logger.warning(f"Log rotation failed: {e}")

    def _load_config(self, config_path: str) -> dict:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return toml.load(config_path)

    def _validate_config(self, config: dict) -> bool:
        try:
            if "symbols" not in config or "monitor_list" not in config["symbols"]:
                return False
            if not isinstance(config["symbols"]["monitor_list"], list):
                return False
            if "webhook" not in config or "url" not in config["webhook"]:
                return False
            if "supertrend" not in config:
                return False
            st_keys = ["period1", "multiplier1", "period2", "multiplier2"]
            if not all(k in config["supertrend"] for k in st_keys):
                return False
            if not all(
                isinstance(config["supertrend"][k], (int, float)) for k in st_keys
            ):
                return False
            if "vegas" not in config:
                return False
            vt_keys = ["ema_signal", "ema_upper", "ema_lower"]
            if not all(k in config["vegas"] for k in vt_keys):
                return False
            if not all(isinstance(config["vegas"][k], int) for k in vt_keys):
                return False
            if "service" not in config or "heartbeat_file" not in config["service"]:
                return False
            heartbeat_dir = os.path.dirname(config["service"]["heartbeat_file"])
            if heartbeat_dir and not os.path.exists(heartbeat_dir):
                return False
            return True
        except Exception:
            return False

    def reload_config(self):
        old_state = {
            "config": self.config,
            "symbols": self.symbols,
            "webhook_url": self.webhook_url,
            "webhook_format": self.webhook_format,
            "st_period1": self.st_period1,
            "st_multiplier1": self.st_multiplier1,
            "st_period2": self.st_period2,
            "st_multiplier2": self.st_multiplier2,
            "vt_ema_signal": self.vt_ema_signal,
            "vt_ema_upper": self.vt_ema_upper,
            "vt_ema_lower": self.vt_ema_lower,
            "heartbeat_file": self.heartbeat_file,
            "proxy_enable": self.proxy_enable,
            "proxy_url": self.proxy_url,
            "report_enable": self.report_enable,
            "report_times": self.report_times,
            "timezone": self.timezone,
            "max_log_lines": self.max_log_lines,
        }

        def restore_state():
            self.config = old_state["config"]
            self.symbols = old_state["symbols"]
            self.webhook_url = old_state["webhook_url"]
            self.webhook_format = old_state["webhook_format"]
            self.st_period1 = old_state["st_period1"]
            self.st_multiplier1 = old_state["st_multiplier1"]
            self.st_period2 = old_state["st_period2"]
            self.st_multiplier2 = old_state["st_multiplier2"]
            self.vt_ema_signal = old_state["vt_ema_signal"]
            self.vt_ema_upper = old_state["vt_ema_upper"]
            self.vt_ema_lower = old_state["vt_ema_lower"]
            self.heartbeat_file = old_state["heartbeat_file"]
            self.proxy_enable = old_state["proxy_enable"]
            self.proxy_url = old_state["proxy_url"]
            self.report_enable = old_state["report_enable"]
            self.report_times = old_state["report_times"]
            self.timezone = old_state["timezone"]
            self.max_log_lines = old_state["max_log_lines"]

        try:
            new_config = self._load_config(self.config_path)
            if not self._validate_config(new_config):
                logger.warning("[CONFIG] Hot reload skipped: invalid config format")
                asyncio.create_task(
                    self.send_webhook(
                        "CONFIG ERROR", "Hot reload skipped: invalid config format"
                    )
                )
                return

            self.config = new_config
            self.symbols = self.config["symbols"]["monitor_list"]
            self.webhook_url = self.config["webhook"]["url"]
            self.webhook_format = self.config["webhook"].get("format", "card")
            self.st_period1 = self.config["supertrend"]["period1"]
            self.st_multiplier1 = self.config["supertrend"]["multiplier1"]
            self.st_period2 = self.config["supertrend"]["period2"]
            self.st_multiplier2 = self.config["supertrend"]["multiplier2"]
            self.vt_ema_signal = self.config["vegas"]["ema_signal"]
            self.vt_ema_upper = self.config["vegas"]["ema_upper"]
            self.vt_ema_lower = self.config["vegas"]["ema_lower"]
            self.heartbeat_file = self.config["service"]["heartbeat_file"]
            self.proxy_enable = self.config.get("proxy", {}).get("enable", False)
            self.proxy_url = self.config.get("proxy", {}).get("url", "")
            self.report_enable = self.config.get("report", {}).get("enable", False)
            self.report_times = self.config.get("report", {}).get(
                "times", ["08:00", "20:00"]
            )
            self.timezone = self.config.get("settings", {}).get("timezone", "Z")
            self.max_log_lines = self.config.get("settings", {}).get(
                "max_log_lines", 1000
            )

            new_symbols = set(self.symbols)
            old_symbols = set(old_state["symbols"])
            if new_symbols != old_symbols:
                logger.info(
                    f"Config reload: symbols changed from {old_symbols} to {new_symbols}"
                )
            if self.webhook_url != old_state["webhook_url"]:
                logger.info(f"Config reload: webhook URL changed")

            logger.info(f"[CONFIG] Hot reload successful: {len(self.symbols)} symbols")
            asyncio.create_task(
                self.send_webhook(
                    "CONFIG", f"Hot reload successful: {len(self.symbols)} symbols"
                )
            )
        except Exception as e:
            restore_state()
            error_msg = f"Hot reload failed, restored previous config: {e}"
            logger.warning(f"[CONFIG] {error_msg}")
            asyncio.create_task(self.send_webhook("CONFIG ERROR", error_msg))

    class ConfigFileHandler(FileSystemEventHandler):
        def __init__(self, service):
            self.service = service
            self.last_reload = 0

        def on_modified(self, event):
            if event.src_path.endswith("config.toml"):
                now = time.time()
                if now - self.last_reload > 5:
                    self.last_reload = now
                    logger.info(f"[CONFIG] Detected config change, reloading...")
                    self.service.reload_config()

    async def config_watch_loop(self):
        config_dir = os.path.dirname(os.path.abspath(self.config_path)) or "."
        event_handler = self.ConfigFileHandler(self)
        self.observer = Observer()
        self.observer.schedule(event_handler, config_dir, recursive=False)
        self.observer.start()
        logger.info(f"[CONFIG] Watching for config changes: {self.config_path}")
        try:
            while self.running:
                await asyncio.sleep(1)
        finally:
            self.observer.stop()
            self.observer.join()

    def _build_feishu_card(
        self, alert_type: str, message: str, extra: dict, timestamp: str
    ) -> dict:
        extra = extra or {}
        direction = extra.get("direction", "").lower()
        symbol = extra.get("symbol", "")
        state = extra.get("state", "")
        reason = extra.get("reason", "")

        if alert_type in ("ST", "VT"):
            if direction == "long" or direction == "bullish":
                color = "green"
                emoji = "📈"
                direction_label = "LONG" if alert_type == "ST" else "BULLISH"
            else:
                color = "red"
                emoji = "📉"
                direction_label = "SHORT" if alert_type == "ST" else "BEARISH"

            if alert_type == "ST":
                title = f"{emoji} SuperTrend {symbol}"
                price = extra.get("price", "")
                st1 = extra.get("st1", "")
                st2 = extra.get("st2", "")
                elements = [
                    {"tag": "markdown", "content": f"**Direction:** {direction_label}"},
                    {"tag": "markdown", "content": f"**Price:** {price}"},
                    {"tag": "markdown", "content": f"**ST1:** {st1} | **ST2:** {st2}"},
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
                ]
            else:
                title = f"{emoji} Vegas Tunnel {symbol}"
                ema9 = extra.get("ema9", "")
                ema144 = extra.get("ema144", "")
                ema169 = extra.get("ema169", "")
                elements = [
                    {"tag": "markdown", "content": f"**Direction:** {direction_label}"},
                    {"tag": "markdown", "content": f"**EMA9:** {ema9}"},
                    {
                        "tag": "markdown",
                        "content": f"**EMA144:** {ema144} | **EMA169:** {ema169}",
                    },
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
                ]

        elif alert_type == "BREAKOUT":
            confirmed = extra.get("confirmed", False)
            if confirmed:
                color = "orange"
                emoji = "🚀"
                status = "CONFIRMED"
            elif reason == "reverse":
                color = "yellow"
                emoji = "🔄"
                status = "REVERSE"
            else:
                color = "yellow"
                emoji = "⏳"
                status = "NO_CONTINUATION"

            direction_label = direction.upper() if direction else ""
            price = extra.get("price", "")
            trigger = extra.get("trigger", "")
            elements = [
                {"tag": "markdown", "content": f"**Direction:** {direction_label}"},
                {"tag": "markdown", "content": f"**Price:** {price}"},
                {"tag": "markdown", "content": f"**Trigger:** {trigger}"},
                {"tag": "markdown", "content": f"**Status:** {status}"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]
            title = f"{emoji} Breakout {symbol} {status}"

        elif alert_type == "SYSTEM":
            color = "blue"
            title = f"🔔 System"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]

        elif alert_type == "ERROR":
            color = "red"
            title = f"⚠️ Error"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]

        elif alert_type == "CONFIG":
            color = "purple"
            title = f"⚙️ Config"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]

        elif alert_type == "CONFIG ERROR":
            color = "red"
            title = f"⚙️ Config Error"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]

        elif alert_type == "REPORT":
            color = "purple"
            title = f"📊 Daily Report"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]

        else:
            color = "blue"
            title = f"Aster Monitor - {alert_type}"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** `{timestamp}`"},
            ]

        return {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": color,
            },
            "elements": elements,
        }

    async def send_webhook(self, alert_type: str, message: str, extra: dict = None):
        timestamp = self._get_timestamp()
        full_content = f"[{timestamp}] {alert_type}: {message}"

        try:
            self._rotate_webhook_log_if_needed()
            with open(WEBHOOK_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {full_content}\n")
        except Exception as e:
            logger.warning(f"Write webhook log failed: {e}")

        if self.webhook_format == "card":
            card = self._build_feishu_card(alert_type, message, extra, timestamp)
            msg = {"msg_type": "interactive", "card": card}
        else:
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
            self.client = WebSocketClient(
                network=Network.MAINNET,
                proxy=self.proxy_url if self.proxy_enable else None,
            )
            self.client.on_error(self._on_ws_error)
            await self.client.connect()
            self.connected = True
            await self.send_webhook("SYSTEM", "Aster Monitor connected to Mainnet")
            for symbol in self.symbols:
                await self.subscribe_symbol(symbol)
            await self.subscribe_all_tickers()
        except Exception as e:
            await self.send_error(e, "Connect")
            raise

    async def _on_ws_error(self, error):
        log_error(f"WebSocket error: {error}")
        if self.connected:
            self.connected = False
            await self.send_webhook("SYSTEM", f"WebSocket disconnected: {error}")

    async def subscribe_symbol(self, symbol: str):
        @self.client.on_ticker(symbol)
        async def on_ticker(data):
            try:
                price = float(data.get("c", data.get("lastPrice", 0)))
                if price > 0:
                    self.mark_prices[symbol] = price
                    self.mark_price_times[symbol] = time.time()
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

    async def subscribe_all_tickers(self):
        params = [f"{symbol.lower()}@ticker" for symbol in self.symbols]
        await self.client.subscribe_batch(params)
        logger.info(f"Batch subscribed to {len(params)} tickers")

    async def fetch_klines(
        self, symbol: str, limit: int = 500, interval: str = "1h", proxy: str = None
    ) -> list:
        try:
            url = f"https://fapi.asterdex.com/fapi/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
            kwargs = {}
            if proxy:
                kwargs["proxy"] = proxy
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30, **kwargs) as resp:
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
        history = await self.fetch_klines(
            symbol,
            limit=20,
            interval="15m",
            proxy=self.proxy_url if self.proxy_enable else None,
        )
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
                    f"{symbol} LONG CONFIRMED",
                    {
                        "symbol": symbol,
                        "direction": "long",
                        "confirmed": True,
                        "price": f"{current_close:.4f}",
                        "trigger": f"{trigger_price:.4f}",
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)
            elif current_close < min_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} LONG FALSE (REVERSE)",
                    {
                        "symbol": symbol,
                        "direction": "long",
                        "confirmed": False,
                        "reason": "reverse",
                        "price": f"{current_close:.4f}",
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)
            elif count >= 20:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} LONG FALSE (NO_CONTINUATION)",
                    {
                        "symbol": symbol,
                        "direction": "long",
                        "confirmed": False,
                        "reason": "no_continuation",
                        "price": f"{current_close:.4f}",
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)

        elif direction == "00":
            if current_close < min_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} SHORT CONFIRMED",
                    {
                        "symbol": symbol,
                        "direction": "short",
                        "confirmed": True,
                        "price": f"{current_close:.4f}",
                        "trigger": f"{trigger_price:.4f}",
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)
            elif current_close > max_prev:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} SHORT FALSE (REVERSE)",
                    {
                        "symbol": symbol,
                        "direction": "short",
                        "confirmed": False,
                        "reason": "reverse",
                        "price": f"{current_close:.4f}",
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)
            elif count >= 20:
                await self.send_webhook(
                    "BREAKOUT",
                    f"{symbol} SHORT FALSE (NO_CONTINUATION)",
                    {
                        "symbol": symbol,
                        "direction": "short",
                        "confirmed": False,
                        "reason": "no_continuation",
                        "price": f"{current_close:.4f}",
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)

    async def update_klines(self, symbol: str):
        try:
            klines = await self.fetch_klines(
                symbol, proxy=self.proxy_url if self.proxy_enable else None
            )
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
                logger.warning(f"recalculate_states: invalid float value for {symbol}")
                return
            self.benchmark[symbol] = {
                "st1": st1_val,
                "st2": st2_val,
                "ema_s": ema_s_val,
                "ema_u": ema_u_val,
                "ema_l": ema_l_val,
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
        last_update = self.mark_price_times.get(symbol, 0)
        if time.time() - last_update > 300:
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

        if not self._initialized:
            self.last_st_state[symbol] = st_state
            self.last_vt_state[symbol] = vt_state
            return

        now = time.time()

        if st_state != self.last_st_state.get(symbol):
            old_state = self.last_st_state.get(symbol, "??")
            self.last_st_state[symbol] = st_state
            if st_state in ["11", "00"] and old_state not in ["11", "00"]:
                last_alert = self.last_alert_time.get(f"ST_{symbol}", 0)
                if now - last_alert > 3600:
                    self.last_alert_time[f"ST_{symbol}"] = now
                    direction = "LONG" if st_state == "11" else "SHORT"
                    await self.send_webhook(
                        "ST",
                        f"{symbol} {st_state} {direction}",
                        {
                            "symbol": symbol,
                            "state": st_state,
                            "direction": direction,
                            "price": f"{current_price:.4f}",
                            "st1": f"{st1_val:.4f}",
                            "st2": f"{st2_val:.4f}",
                        },
                    )
                    self._increment_alert_count()
                    await self.start_breakout_monitor(
                        symbol, st_state, current_price, bm["kline_time"]
                    )

        if vt_state != self.last_vt_state.get(symbol):
            old_state = self.last_vt_state.get(symbol, "??")
            self.last_vt_state[symbol] = vt_state
            if vt_state in ["11", "00"] and old_state not in ["11", "00"]:
                last_alert = self.last_alert_time.get(f"VT_{symbol}", 0)
                if now - last_alert > 3600:
                    self.last_alert_time[f"VT_{symbol}"] = now
                    direction = "BULLISH" if vt_state == "11" else "BEARISH"
                    await self.send_webhook(
                        "VT",
                        f"{symbol} {vt_state} {direction}",
                        {
                            "symbol": symbol,
                            "state": vt_state,
                            "direction": direction,
                            "ema9": f"{ema_s_val:.4f}",
                            "ema144": f"{ema_u_val:.4f}",
                            "ema169": f"{ema_l_val:.4f}",
                        },
                    )
                    self._increment_alert_count()

    async def initialize(self):
        logger.info(f"Initializing klines for {len(self.symbols)} symbols...")
        self._initialized = False
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
        self._initialized = True
        logger.info("Initialization complete")

    async def heartbeat_loop(self):
        while self.running:
            self.write_heartbeat()
            await asyncio.sleep(30)

    async def status_loop(self):
        while self.running:
            try:
                await self._check_and_send_daily_report()
                if self._status_print_enabled:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logger.info(f"[STATUS] {timestamp}")
                    symbols_copy = list(self.symbols)
                    for symbol in symbols_copy:
                        try:
                            current_price = self.mark_prices.get(symbol)
                            bm = self.benchmark.get(symbol)
                            if bm:
                                ema_s = bm["ema_s"]
                                ema_u = bm["ema_u"]
                                ema_l = bm["ema_l"]
                                st1_val = bm["st1"]
                                st2_val = bm["st2"]
                                st_state = (
                                    "1"
                                    if current_price and current_price > st1_val
                                    else "0"
                                ) + (
                                    "1"
                                    if current_price and current_price > st2_val
                                    else "0"
                                )
                                vt_state = ("1" if ema_s > ema_u else "0") + (
                                    "1" if ema_s > ema_l else "0"
                                )
                                logger.info(
                                    f"[STATUS] {symbol}: Price={format_number(current_price) if current_price is not None else 'N/A'}, ST={st_state}, EMA_S={format_number(ema_s)}, EMA_U={format_number(ema_u)}, EMA_L={format_number(ema_l)}, VT={vt_state}"
                                )
                            else:
                                logger.warning(
                                    f"[STATUS] {symbol}: benchmark not ready (klines insufficient), price={current_price}"
                                )
                        except Exception as e:
                            logger.warning(f"[STATUS] {symbol}: error: {e}")
            except Exception as e:
                logger.warning(f"[STATUS] loop error: {e}")
            await asyncio.sleep(120)

    async def _check_and_send_daily_report(self):
        if not self.report_enable:
            return
        now = datetime.now()
        current_time = now.timestamp()
        current_minute = now.strftime("%H:%M")
        if current_time - self._last_report_time >= 3600:
            for report_time in self.report_times:
                report_hour, report_min = report_time.split(":")
                current_hour, current_min = current_minute.split(":")
                if (
                    current_hour == report_hour
                    and abs(int(current_min) - int(report_min)) <= 1
                ):
                    with self._lock:
                        alert_count = self._alert_count
                        self._alert_count = 0
                    if alert_count > 0:
                        await self.send_webhook(
                            "REPORT", f"Alert count in last 24h: {alert_count}"
                        )
                        self._last_report_time = current_time
                        logger.info(
                            f"[REPORT] Sent daily report, alerts: {alert_count}"
                        )
                    break

    def _increment_alert_count(self):
        with self._lock:
            self._alert_count += 1

    def _handle_command(self, cmd: str):
        with self._lock:
            cmd = cmd.strip().lower()
            if cmd == "stop print":
                self._status_print_enabled = False
                logger.info("[CMD] Status print stopped")
            elif cmd == "resume print":
                self._status_print_enabled = True
                logger.info("[CMD] Status print resumed")
            elif cmd == "status":
                logger.info(
                    f"[CMD] Status print enabled: {self._status_print_enabled}, Alert count: {self._alert_count}"
                )
            else:
                logger.info(
                    f"[CMD] Unknown command: {cmd}. Available: stop print, resume print, status"
                )

    def _input_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while self.running:
                try:
                    line = input()
                    if line.strip():
                        loop.call_soon_threadsafe(lambda: self._handle_command(line))
                except EOFError:
                    break
                except Exception as e:
                    logger.warning(f"[CMD] Input error: {e}")
        finally:
            loop.close()

    async def run(self):
        self.running = True

        input_thread = threading.Thread(target=self._input_loop, daemon=True)
        input_thread.start()
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        config_task = asyncio.create_task(self.config_watch_loop())
        await self.connect()
        await self.initialize()
        status_task = asyncio.create_task(self.status_loop())
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
                    await self.send_webhook(
                        "SYSTEM", "Aster Monitor reconnected to Mainnet"
                    )
                    for symbol in self.symbols:
                        await self.subscribe_symbol(symbol)
                    await self.subscribe_all_tickers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.send_error(e, "Main Loop")
                await asyncio.sleep(5)
        heartbeat_task.cancel()
        status_task.cancel()
        config_task.cancel()

    async def stop(self):
        self.running = False
        self.breakout_monitor.clear()
        if self.observer:
            self.observer.stop()
            self.observer.join()
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
