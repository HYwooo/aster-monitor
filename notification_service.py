import asyncio
import argparse
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
        self.st_period1 = self.config.get("supertrend", {}).get("period1", 9)
        self.st_multiplier1 = self.config.get("supertrend", {}).get("multiplier1", 2.5)
        self.st_period2 = self.config.get("supertrend", {}).get("period2", 14)
        self.st_multiplier2 = self.config.get("supertrend", {}).get("multiplier2", 1.7)
        self.vt_ema_signal = self.config.get("vegas", {}).get("ema_signal", 9)
        self.vt_ema_upper = self.config.get("vegas", {}).get("ema_upper", 144)
        self.vt_ema_lower = self.config.get("vegas", {}).get("ema_lower", 169)
        self.atr1h_ma_type = self.config.get("atr_1h", {}).get("ma_type", "DEMA")
        self.atr1h_period = self.config.get("atr_1h", {}).get("period", 14)
        self.atr1h_mult = self.config.get("atr_1h", {}).get("mult", 1.618)
        self.atr15m_ma_type = self.config.get("atr_15m", {}).get("ma_type", "HMA")
        self.atr15m_period = self.config.get("atr_15m", {}).get("period", 14)
        self.atr15m_mult = self.config.get("atr_15m", {}).get("mult", 1.3)
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
        self._pair_components: dict[str, list] = {}
        self._breakout_comp_prices: dict[str, float] = {}
        self.kline_cache: dict[str, list] = {}
        self.benchmark: dict[str, dict] = {}
        self.last_st_state: dict[str, str] = {}
        self.last_atr_state: dict[str, dict] = {}
        self.last_alert_time: dict[str, float] = {}
        self.last_kline_time: dict[str, int] = {}
        self.breakout_monitor: dict[str, dict] = {}
        self.trailing_stop: dict[str, dict] = {}
        self.last_atr1h_ch: dict[str, int] = {}
        self.connected = False
        self.running = False
        self.observer: Optional[Observer] = None
        self._initialized = False
        self._status_print_enabled = True
        self._alert_count = 0
        self._last_report_time = 0
        self._lock = threading.Lock()
        self._pending_status: set = set()

    def _is_pair_trading(self, symbol: str) -> bool:
        return "/" in symbol

    def _rotate_webhook_log_if_needed(self):
        try:
            if not os.path.exists(WEBHOOK_LOG_FILE):
                return
            with open(WEBHOOK_LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > self.max_log_lines:
                with open(WEBHOOK_LOG_FILE, "w", encoding="utf-8") as f:
                    f.writelines(lines[-self.max_log_lines :])
        except Exception as e:
            logger.warning(f"Log rotation failed: {e}")

    async def _fetch_pair_klines(
        self, symbol: str, limit: int = 500, interval: str = "1h", proxy: str = None
    ) -> list:
        parts = symbol.split("/")
        klines1 = await self.fetch_klines(parts[0], limit, interval, proxy)
        klines2 = await self.fetch_klines(parts[1], limit, interval, proxy)
        if not klines1 or not klines2:
            return []
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

    def _get_timestamp(self) -> str:
        tz = self.timezone
        if tz == "Z":
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        elif tz.startswith("+") or tz.startswith("-"):
            return datetime.now().strftime(f"%Y-%m-%dT%H:%M:%S{tz}")
        else:
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")

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
            if "supertrend" in config:
                st_keys = ["period1", "multiplier1", "period2", "multiplier2"]
                if not all(k in config["supertrend"] for k in st_keys):
                    return False
                if not all(
                    isinstance(config["supertrend"][k], (int, float)) for k in st_keys
                ):
                    return False
            if "vegas" in config:
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
            "atr1h_ma_type": self.atr1h_ma_type,
            "atr1h_period": self.atr1h_period,
            "atr1h_mult": self.atr1h_mult,
            "atr15m_ma_type": self.atr15m_ma_type,
            "atr15m_period": self.atr15m_period,
            "atr15m_mult": self.atr15m_mult,
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
            self.atr1h_ma_type = old_state["atr1h_ma_type"]
            self.atr1h_period = old_state["atr1h_period"]
            self.atr1h_mult = old_state["atr1h_mult"]
            self.atr15m_ma_type = old_state["atr15m_ma_type"]
            self.atr15m_period = old_state["atr15m_period"]
            self.atr15m_mult = old_state["atr15m_mult"]
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
            self.st_period1 = self.config.get("supertrend", {}).get("period1", 9)
            self.st_multiplier1 = self.config.get("supertrend", {}).get(
                "multiplier1", 2.5
            )
            self.st_period2 = self.config.get("supertrend", {}).get("period2", 14)
            self.st_multiplier2 = self.config.get("supertrend", {}).get(
                "multiplier2", 1.7
            )
            self.vt_ema_signal = self.config.get("vegas", {}).get("ema_signal", 9)
            self.vt_ema_upper = self.config.get("vegas", {}).get("ema_upper", 144)
            self.vt_ema_lower = self.config.get("vegas", {}).get("ema_lower", 169)
            self.atr1h_ma_type = self.config.get("atr_1h", {}).get("ma_type", "DEMA")
            self.atr1h_period = self.config.get("atr_1h", {}).get("period", 14)
            self.atr1h_mult = self.config.get("atr_1h", {}).get("mult", 1.618)
            self.atr15m_ma_type = self.config.get("atr_15m", {}).get("ma_type", "HMA")
            self.atr15m_period = self.config.get("atr_15m", {}).get("period", 14)
            self.atr15m_mult = self.config.get("atr_15m", {}).get("mult", 1.3)
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

        if alert_type == "ATR_Ch":
            is_trailing = reason == "trailing_stop"
            if is_trailing:
                color = "orange"
                emoji = "🛑"
            elif direction == "long":
                color = "green"
                emoji = "📈"
            elif direction == "short":
                color = "red"
                emoji = "📉"
            else:
                color = "blue"
                emoji = "📊"

            price = extra.get("price", "")
            atr_upper = extra.get("atr_upper", "")
            atr_lower = extra.get("atr_lower", "")
            stop_line = extra.get("stop_line", "")
            entry_price = extra.get("entry_price", "")
            if is_trailing:
                elements = [
                    {
                        "tag": "markdown",
                        "content": f"**Direction:** {direction.upper()} TRAILING STOP",
                    },
                    {"tag": "markdown", "content": f"**Price:** {price}"},
                    {"tag": "markdown", "content": f"**Stop Line:** {stop_line}"},
                    {"tag": "markdown", "content": f"**Entry:** {entry_price}"},
                ]
            else:
                elements = [
                    {
                        "tag": "markdown",
                        "content": f"**Direction:** {direction.upper()}",
                    },
                    {"tag": "markdown", "content": f"**Price:** {price}"},
                ]
            if stop_line:
                elements.append(
                    {"tag": "markdown", "content": f"**Stop Line:** {stop_line}"}
                )
            if atr_upper and atr_lower and not is_trailing:
                elements.append(
                    {
                        "tag": "markdown",
                        "content": f"**ATR Channel:** {atr_lower} ~ {atr_upper}",
                    }
                )
            elements.extend(
                [
                    {"tag": "hr"},
                    {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
                ]
            )
            title = f"{emoji} {symbol}"

        elif alert_type == "SYSTEM":
            color = "blue"
            title = f"🔔 System"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
            ]

        elif alert_type == "ERROR":
            color = "red"
            title = f"⚠️ Error"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
            ]

        elif alert_type == "CONFIG":
            color = "purple"
            title = f"⚙️ Config"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
            ]

        elif alert_type == "CONFIG ERROR":
            color = "red"
            title = f"⚙️ Config Error"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
            ]

        elif alert_type == "REPORT":
            color = "purple"
            title = f"📊 Daily Report"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
            ]

        else:
            color = "blue"
            title = f"Aster Monitor - {alert_type}"
            elements = [
                {"tag": "markdown", "content": f"**{message}**"},
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
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
        full_content = f"[{timestamp}] [{alert_type}] {message}"

        try:
            self._rotate_webhook_log_if_needed()
            with open(WEBHOOK_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"{full_content}\n")
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
                        extra = extra or {}
                        direction = extra.get("direction", "").upper()
                        price = extra.get("price", "")
                        atr_upper = extra.get("atr_upper", "")
                        atr_lower = extra.get("atr_lower", "")
                        stop_line = extra.get("stop_line", "")
                        entry_price = extra.get("entry_price", "")
                        reason = extra.get("reason", "")

                        if reason == "trailing_stop":
                            log_msg = f"[WEBHOOK] {message} | Price={price} | Stop={stop_line} | Entry={entry_price}"
                        else:
                            log_msg = f"[WEBHOOK] {message} | Price={price} | Channel={atr_lower}~{atr_upper}"
                        logger.info(log_msg)
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
        if self._is_pair_trading(symbol):
            await self._subscribe_pair_trading(symbol)
        else:
            await self._subscribe_single_symbol(symbol)

    async def _subscribe_single_symbol(self, symbol: str):
        @self.client.on_ticker(symbol)
        async def on_ticker(data):
            try:
                price = float(data.get("c", data.get("lastPrice", 0)))
                if price > 0:
                    self.mark_prices[symbol] = price
                    self.mark_price_times[symbol] = time.time()
                await self.check_trailing_stop(symbol, price)
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

        @self.client.on_kline(symbol, "15m")
        async def on_15m_kline(kline):
            try:
                if kline.get("x", False):
                    await self.update_15m_atr(symbol, kline)
            except Exception as e:
                self.warn(f"15m kline callback error: {e}", f"symbol={symbol}")

        logger.info(f"Subscribed to {symbol}")

    async def _subscribe_pair_trading(self, symbol: str):
        parts = symbol.split("/")
        symbol1, symbol2 = parts[0], parts[1]
        self._pair_components[symbol] = [symbol1, symbol2]

        async def on_ticker(data):
            try:
                ticker_sym = data.get("s", "").upper()
                price = float(data.get("c", data.get("lastPrice", 0)))
                if price <= 0:
                    return
                self.mark_prices[ticker_sym] = price
                self.mark_price_times[ticker_sym] = time.time()
                await self._update_pair_price(symbol, symbol1, symbol2)
            except Exception as e:
                self.warn(f"pair ticker error: {e}", f"pair={symbol}")

        self.client.on_ticker(symbol1)(on_ticker)
        self.client.on_ticker(symbol2)(on_ticker)

        @self.client.on_kline(symbol1, "1h")
        async def on_kline(kline):
            try:
                if kline.get("x", False):
                    await self.update_klines(symbol)
            except Exception as e:
                self.warn(f"pair kline error: {e}", f"pair={symbol}")

        @self.client.on_kline(symbol1, "15m")
        async def on_15m_kline(kline):
            try:
                if kline.get("x", False):
                    await self.update_15m_atr(symbol, kline)
            except Exception as e:
                self.warn(f"pair 15m kline error: {e}", f"pair={symbol}")

        @self.client.on_kline(symbol2, "15m")
        async def on_15m_kline2(kline):
            try:
                if kline.get("x", False):
                    await self.update_15m_atr(symbol, kline)
            except Exception as e:
                self.warn(f"pair 15m kline2 error: {e}", f"pair={symbol}")

        logger.info(f"Subscribed to PairTrading {symbol} ({symbol1}/{symbol2})")

    async def subscribe_all_tickers(self):
        params = []
        for sym in self.symbols:
            if self._is_pair_trading(sym):
                parts = sym.split("/")
                params.append(f"{parts[0].lower()}@ticker")
                params.append(f"{parts[1].lower()}@ticker")
            else:
                params.append(f"{sym.lower()}@ticker")
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

    async def fetch_ticker_price(self, symbol: str, proxy: str = None) -> float:
        try:
            url = f"https://fapi.asterdex.com/fapi/v1/ticker/price?symbol={symbol}"
            kwargs = {}
            if proxy:
                kwargs["proxy"] = proxy
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10, **kwargs) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get("price", 0))
                    else:
                        return 0
        except Exception:
            return 0

    async def _update_pair_price(self, symbol: str, symbol1: str, symbol2: str):
        p1 = self.mark_prices.get(symbol1, 0)
        p2 = self.mark_prices.get(symbol2, 0)
        if p1 <= 0 or p2 <= 0:
            p1_fallback = await self.fetch_ticker_price(
                symbol1, self.proxy_url if self.proxy_enable else None
            )
            p2_fallback = await self.fetch_ticker_price(
                symbol2, self.proxy_url if self.proxy_enable else None
            )
            if p1_fallback > 0:
                self.mark_prices[symbol1] = p1_fallback
                self.mark_price_times[symbol1] = time.time()
                p1 = p1_fallback
            if p2_fallback > 0:
                self.mark_prices[symbol2] = p2_fallback
                self.mark_price_times[symbol2] = time.time()
                p2 = p2_fallback
        if p1 > 0 and p2 > 0:
            pair_price = p1 / p2
            self.mark_prices[symbol] = pair_price
            self.mark_price_times[symbol] = time.time()
            await self.check_trailing_stop(symbol, pair_price)
            await self.check_signals(symbol)

    async def _poll_prices(self):
        proxy = self.proxy_url if self.proxy_enable else None
        last_15m_update = 0
        while self.running:
            try:
                current_time = time.time()
                for sym in self.symbols:
                    last_update = self.mark_price_times.get(sym, 0)
                    if current_time - last_update > 5:
                        price = await self.fetch_ticker_price(sym, proxy)
                        if price > 0:
                            self.mark_prices[sym] = price
                            self.mark_price_times[sym] = current_time
                            if sym in self._pending_status:
                                self._pending_status.discard(sym)
                                self._print_status(sym)
                                if not self._pending_status:
                                    await self._send_startup_summary()
                            await self.check_trailing_stop(sym, price)
                            await self.check_signals(sym)
                for pair_symbol, components in self._pair_components.items():
                    symbol1, symbol2 = components[0], components[1]
                    p1 = await self.fetch_ticker_price(symbol1, proxy)
                    p2 = await self.fetch_ticker_price(symbol2, proxy)
                    if p1 > 0:
                        self.mark_prices[symbol1] = p1
                        self.mark_price_times[symbol1] = current_time
                    if p2 > 0:
                        self.mark_prices[symbol2] = p2
                        self.mark_price_times[symbol2] = current_time
                    if p1 > 0 and p2 > 0:
                        pair_price = p1 / p2
                        self.mark_prices[pair_symbol] = pair_price
                        self.mark_price_times[pair_symbol] = current_time
                        if pair_symbol in self._pending_status:
                            self._pending_status.discard(pair_symbol)
                            self._print_status(pair_symbol)
                            if not self._pending_status:
                                await self._send_startup_summary()
                        await self.check_trailing_stop(pair_symbol, pair_price)
                        await self.check_signals(pair_symbol)
                if current_time - last_15m_update > 15:
                    last_15m_update = current_time
                    for sym in self.symbols:
                        if sym in self.trailing_stop and self.trailing_stop[sym].get(
                            "active"
                        ):
                            price = self.mark_prices.get(sym)
                            if price:
                                await self._update_15m_atr_from_poll(sym, price)
                    for pair_sym in self._pair_components:
                        if pair_sym in self.trailing_stop and self.trailing_stop[
                            pair_sym
                        ].get("active"):
                            price = self.mark_prices.get(pair_sym)
                            if price:
                                await self._update_15m_atr_from_poll(pair_sym, price)
            except Exception as e:
                logger.warning(f"Price poll error: {e}")
            await asyncio.sleep(1)

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

    def calculate_dema(self, data, period):
        ema1 = talib.EMA(data, timeperiod=period)
        ema2 = talib.EMA(ema1, timeperiod=period)
        return 2 * ema1 - ema2

    def calculate_hma(self, data, period):
        return talib.WMA(
            2 * talib.WMA(data, period // 2) - talib.WMA(data, period),
            int(math.sqrt(period)),
        )

    def calculate_tr(self, high, low, close):
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]
        tr1 = high - low
        tr2 = np.abs(high - prev_close)
        tr3 = np.abs(low - prev_close)
        return np.maximum(tr1, np.maximum(tr2, tr3))

    def calculate_atr(self, high, low, close, period, ma_type="DEMA"):
        tr = self.calculate_tr(high, low, close)
        if ma_type == "DEMA":
            atr = self.calculate_dema(tr, period)
        elif ma_type == "HMA":
            atr = self.calculate_hma(tr, period)
        elif ma_type == "EMA":
            atr = talib.EMA(tr, period)
        elif ma_type == "SMA":
            atr = talib.SMA(tr, period)
        elif ma_type == "WMA":
            atr = talib.WMA(tr, period)
        else:
            atr = talib.EMA(tr, period)
        return atr

    def run_atr_channel(self, close, atr, mult, prev_state):
        upper_band, lower_band, ch_state = prev_state
        if math.isnan(atr) or atr <= 0:
            return upper_band, lower_band, ch_state
        if math.isnan(upper_band) or math.isnan(lower_band):
            width = atr * mult
            upper_band = close + width / 2
            lower_band = close - width / 2
            ch_state = 0
        elif close > upper_band:
            width = atr * mult
            new_upper = close
            new_lower = close - width
            lower_band = max(lower_band, new_lower)
            upper_band = new_upper
            ch_state = 1
        elif close < lower_band:
            width = atr * mult
            new_lower = close
            new_upper = close + width
            upper_band = min(upper_band, new_upper)
            lower_band = new_lower
            ch_state = -1
        return upper_band, lower_band, ch_state

    async def update_15m_atr(self, symbol, kline):
        try:
            if symbol not in self.trailing_stop or not self.trailing_stop[symbol].get(
                "active"
            ):
                return
            o, h, l, c = (
                float(kline.get("o", 0)),
                float(kline.get("h", 0)),
                float(kline.get("l", 0)),
                float(kline.get("c", 0)),
            )
            if c == 0:
                return
            if self._is_pair_trading(symbol):
                parts = symbol.split("/")
                comp1, comp2 = parts[0], parts[1]
                c1 = self.mark_prices.get(comp1, 0)
                c2 = self.mark_prices.get(comp2, 0)
                if c1 == 0 or c2 == 0:
                    return
                c = c1 / c2
                h_ratio = max(o, c) if o > 0 else c
                l_ratio = min(o, c) if o > 0 else c
                atr = self.calculate_atr(
                    np.array([h_ratio]),
                    np.array([l_ratio]),
                    np.array([c]),
                    self.atr15m_period,
                    self.atr15m_ma_type,
                )[0]
            else:
                atr = self.calculate_atr(
                    np.array([h]),
                    np.array([l]),
                    np.array([c]),
                    self.atr15m_period,
                    self.atr15m_ma_type,
                )[0]
            if math.isnan(atr) or atr <= 0:
                return
            ts = self.trailing_stop[symbol]
            prev_state = ts.get("atr15m_state", (float("nan"), float("nan"), 0))
            upper, lower, ch = self.run_atr_channel(c, atr, ts["atr_mult"], prev_state)
            ts["atr15m_upper"] = upper
            ts["atr15m_lower"] = lower
            ts["atr15m_state"] = (upper, lower, ch)
        except Exception as e:
            self.warn(f"update_15m_atr error for {symbol}: {e}")

    async def _update_15m_atr_from_poll(self, symbol, current_price):
        try:
            if symbol not in self.trailing_stop or not self.trailing_stop[symbol].get(
                "active"
            ):
                return
            if current_price <= 0:
                return
            klines = await self.fetch_klines(
                symbol,
                limit=20,
                interval="15m",
                proxy=self.proxy_url if self.proxy_enable else None,
            )
            if not klines:
                return
            if self._is_pair_trading(symbol):
                parts = symbol.split("/")
                klines1 = await self.fetch_klines(
                    parts[0],
                    limit=20,
                    interval="15m",
                    proxy=self.proxy_url if self.proxy_enable else None,
                )
                klines2 = await self.fetch_klines(
                    parts[1],
                    limit=20,
                    interval="15m",
                    proxy=self.proxy_url if self.proxy_enable else None,
                )
                if not klines1 or not klines2:
                    return
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
                klines = merged[-20:] if len(merged) >= 20 else merged
            if len(klines) < 2:
                return
            ts = self.trailing_stop[symbol]
            close_arr = np.array([float(k[4]) for k in klines], dtype=float)
            high_arr = np.array([float(k[2]) for k in klines], dtype=float)
            low_arr = np.array([float(k[3]) for k in klines], dtype=float)
            atr_arr = self.calculate_atr(
                high_arr, low_arr, close_arr, self.atr15m_period, self.atr15m_ma_type
            )
            atr = float(atr_arr[-1])
            if math.isnan(atr) or atr <= 0:
                return
            prev_state = ts.get("atr15m_state", (float("nan"), float("nan"), 0))
            upper, lower, ch = self.run_atr_channel(
                current_price, atr, ts["atr_mult"], prev_state
            )
            ts["atr15m_upper"] = upper
            ts["atr15m_lower"] = lower
            ts["atr15m_state"] = (upper, lower, ch)
        except Exception as e:
            self.warn(f"_update_15m_atr_from_poll error for {symbol}: {e}")

    async def check_trailing_stop(self, symbol, current_price):
        if not current_price or current_price <= 0:
            return
        if symbol not in self.trailing_stop:
            return
        ts = self.trailing_stop[symbol]
        if not ts.get("active"):
            return
        try:
            upper = ts.get("atr15m_upper", 0)
            lower = ts.get("atr15m_lower", 0)
            direction = ts.get("direction", "")
            if direction == "LONG" and lower > 0 and current_price < lower:
                await self.send_webhook(
                    "ATR_Ch",
                    f"[{symbol}] TRAILING STOP",
                    {
                        "symbol": symbol,
                        "direction": "LONG",
                        "price": format_number(current_price),
                        "stop_line": format_number(lower),
                        "entry_price": format_number(ts.get("entry_price", 0)),
                        "reason": "trailing_stop",
                    },
                )
                self._increment_alert_count()
                ts["active"] = False
                logger.info(f"Trailing stop hit for {symbol} at {lower}")
            elif direction == "SHORT" and upper > 0 and current_price > upper:
                await self.send_webhook(
                    "ATR_Ch",
                    f"[{symbol}] TRAILING STOP",
                    {
                        "symbol": symbol,
                        "direction": "SHORT",
                        "price": format_number(current_price),
                        "stop_line": format_number(upper),
                        "entry_price": format_number(ts.get("entry_price", 0)),
                        "reason": "trailing_stop",
                    },
                )
                self._increment_alert_count()
                ts["active"] = False
                logger.info(f"Trailing stop hit for {symbol} at {upper}")
        except Exception as e:
            self.warn(f"check_trailing_stop error for {symbol}: {e}")

    async def start_breakout_monitor(self, symbol, direction, price, trigger_time):
        if symbol in self.breakout_monitor:
            return
        if self._is_pair_trading(symbol):
            history = await self._fetch_pair_klines(
                symbol,
                limit=20,
                interval="15m",
                proxy=self.proxy_url if self.proxy_enable else None,
            )
        else:
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

        if self._is_pair_trading(symbol):
            parts = symbol.split("/")
            self._breakout_comp_prices[parts[0]] = 0
            self._breakout_comp_prices[parts[1]] = 0

            @self.client.on_kline(parts[0], "15m")
            async def on_15m_kline_p1(kline):
                await self.on_15m_kline(symbol, kline, parts[0], parts[1])

            @self.client.on_kline(parts[1], "15m")
            async def on_15m_kline_p2(kline):
                await self.on_15m_kline(symbol, kline, parts[0], parts[1])
        else:

            @self.client.on_kline(symbol, "15m")
            async def on_15m_kline(kline):
                await self.on_15m_kline(symbol, kline)

        logger.info(f"Breakout monitor started for {symbol}, direction={direction}")

    async def stop_breakout_monitor(self, symbol):
        if symbol in self.breakout_monitor:
            del self.breakout_monitor[symbol]
            logger.info(f"Breakout monitor stopped for {symbol}")

    async def on_15m_kline(self, symbol, kline, comp1=None, comp2=None):
        if symbol not in self.breakout_monitor:
            return
        try:
            monitor = self.breakout_monitor[symbol]
            t = int(kline.get("t", 0))
            if self._is_pair_trading(symbol):
                if comp1 is None or comp2 is None:
                    return
                c = float(kline.get("c", 0))
                if c == 0:
                    return
                other_comp = comp2 if comp1 == symbol.split("/")[0] else comp1
                other_price = self._breakout_comp_prices.get(other_comp, 0)
                if other_price == 0:
                    self._breakout_comp_prices[comp1] = c
                    return
                ratio_close = c / other_price
                self._breakout_comp_prices[comp1] = 0
                self._breakout_comp_prices[comp2] = 0
                monitor["klines_15m"].append([t, 0, ratio_close])
            else:
                kline_close = float(kline.get("c", 0))
                monitor["klines_15m"].append([t, 0, kline_close])
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
                        "direction": "LONG",
                        "confirmed": True,
                        "price": format_number(current_close),
                        "trigger": format_number(trigger_price),
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
                        "direction": "LONG",
                        "confirmed": False,
                        "reason": "reverse",
                        "price": format_number(current_close),
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
                        "direction": "LONG",
                        "confirmed": False,
                        "reason": "no_continuation",
                        "price": format_number(current_close),
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
                        "direction": "SHORT",
                        "confirmed": True,
                        "price": format_number(current_close),
                        "trigger": format_number(trigger_price),
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
                        "direction": "SHORT",
                        "confirmed": False,
                        "reason": "reverse",
                        "price": format_number(current_close),
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
                        "direction": "SHORT",
                        "confirmed": False,
                        "reason": "no_continuation",
                        "price": format_number(current_close),
                    },
                )
                self._increment_alert_count()
                await self.stop_breakout_monitor(symbol)

    async def update_klines(self, symbol: str):
        try:
            if self._is_pair_trading(symbol):
                klines = await self._fetch_pair_klines(
                    symbol, proxy=self.proxy_url if self.proxy_enable else None
                )
            else:
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
            if self._is_pair_trading(symbol):
                open_arr = np.array([float(k[1]) for k in klines], dtype=float)
                high = np.maximum(open_arr, close)
                low = np.minimum(open_arr, close)
            else:
                high = np.array([float(k[2]) for k in klines], dtype=float)
                low = np.array([float(k[3]) for k in klines], dtype=float)
            st1 = self.calculate_supertrend(
                high, low, close, self.st_period1, self.st_multiplier1
            )
            st2 = self.calculate_supertrend(
                high, low, close, self.st_period2, self.st_multiplier2
            )
            ema_s, ema_u, ema_l = self.calculate_vegas_tunnel(close)
            atr1h = self.calculate_atr(
                high, low, close, self.atr1h_period, self.atr1h_ma_type
            )
            prev_atr_state = self.benchmark.get(symbol, {}).get(
                "atr1h_state", (float("nan"), float("nan"), 0)
            )
            for i in range(len(close)):
                upper, lower, ch = self.run_atr_channel(
                    close[i], atr1h[i], self.atr1h_mult, prev_atr_state
                )
                prev_atr_state = (upper, lower, ch)
            atr1h_upper, atr1h_lower, atr1h_ch = prev_atr_state
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
                "atr1h_upper": float(atr1h_upper) if not math.isnan(atr1h_upper) else 0,
                "atr1h_lower": float(atr1h_lower) if not math.isnan(atr1h_lower) else 0,
                "atr1h_ch": atr1h_ch,
                "atr1h_state": prev_atr_state,
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

        if not self._initialized:
            self.last_st_state[symbol] = st_state
            return

        now = time.time()

        atr1h_upper = bm.get("atr1h_upper", 0)
        atr1h_lower = bm.get("atr1h_lower", 0)
        prev_atr_state = self.last_atr_state.get(symbol, {"ch": 0, "sent": None})

        if current_price >= atr1h_upper and prev_atr_state["ch"] != 1:
            last_alert = self.last_alert_time.get(f"ATR_Ch_{symbol}", 0)
            if now - last_alert > 3600:
                self.last_alert_time[f"ATR_Ch_{symbol}"] = now
                self.last_atr_state[symbol] = {"ch": 1, "sent": "LONG"}
                await self.send_webhook(
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
                self._increment_alert_count()
                self.trailing_stop[symbol] = {
                    "direction": "LONG",
                    "entry_price": current_price,
                    "entry_time": now,
                    "atr_mult": self.atr15m_mult,
                    "atr15m_upper": 0,
                    "atr15m_lower": 0,
                    "atr15m_state": (float("nan"), float("nan"), 0),
                    "active": True,
                }
        elif current_price <= atr1h_lower and prev_atr_state["ch"] != -1:
            last_alert = self.last_alert_time.get(f"ATR_Ch_{symbol}", 0)
            if now - last_alert > 3600:
                self.last_alert_time[f"ATR_Ch_{symbol}"] = now
                self.last_atr_state[symbol] = {"ch": -1, "sent": "SHORT"}
                await self.send_webhook(
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
                self._increment_alert_count()
                self.trailing_stop[symbol] = {
                    "direction": "SHORT",
                    "entry_price": current_price,
                    "entry_time": now,
                    "atr_mult": self.atr15m_mult,
                    "atr15m_upper": 0,
                    "atr15m_lower": 0,
                    "atr15m_state": (float("nan"), float("nan"), 0),
                    "active": True,
                }

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
                atr_ch = bm.get("atr1h_ch", 0)
                direction = (
                    "LONG" if atr_ch == 1 else ("SHORT" if atr_ch == -1 else "N/A")
                )
                self.last_st_state[symbol] = direction
                logger.info(f"{symbol}: ATR_Ch={direction}")
        self._initialized = True
        logger.info("Initialization complete")
        for sym in self.symbols:
            if sym not in self.mark_prices or self.mark_prices.get(sym, 0) <= 0:
                self._pending_status.add(sym)
        for pair_sym in self._pair_components:
            if (
                pair_sym not in self.mark_prices
                or self.mark_prices.get(pair_sym, 0) <= 0
            ):
                self._pending_status.add(pair_sym)
        if not self._pending_status:
            await self._send_startup_summary()

    async def _send_startup_summary(self):
        lines = []
        for sym in self.symbols:
            current_price = self.mark_prices.get(sym)
            bm = self.benchmark.get(sym)
            if bm and current_price:
                atr_upper = bm.get("atr1h_upper", 0)
                atr_lower = bm.get("atr1h_lower", 0)
                atr_ch = bm.get("atr1h_ch", 0)
                direction = (
                    "LONG" if atr_ch == 1 else ("SHORT" if atr_ch == -1 else "N/A")
                )
                lines.append(
                    f"{sym}: {direction} | Price={format_number(current_price)} | Channel={format_number(atr_lower)}~{format_number(atr_upper)}"
                )
        if lines:
            await self.send_webhook("SYSTEM", "\n".join(lines))

    def _print_status(self, symbol: str):
        current_price = self.mark_prices.get(symbol)
        bm = self.benchmark.get(symbol)
        if bm and current_price:
            atr_upper = bm.get("atr1h_upper", 0)
            atr_lower = bm.get("atr1h_lower", 0)
            atr_ch = bm.get("atr1h_ch", 0)
            direction = "LONG" if atr_ch == 1 else ("SHORT" if atr_ch == -1 else "N/A")
            logger.info(
                f"[STATUS] {symbol}: Price={format_number(current_price)}, ATR_Ch={direction}, Channel={format_number(atr_lower)}~{format_number(atr_upper)}"
            )

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
                            if bm and current_price:
                                atr_upper = bm.get("atr1h_upper", 0)
                                atr_lower = bm.get("atr1h_lower", 0)
                                atr_ch = bm.get("atr1h_ch", 0)
                                direction = (
                                    "LONG"
                                    if atr_ch == 1
                                    else ("SHORT" if atr_ch == -1 else "N/A")
                                )
                                logger.info(
                                    f"[STATUS] {symbol}: Price={format_number(current_price)}, ATR_Ch={direction}, Channel={format_number(atr_lower)}~{format_number(atr_upper)}"
                                )
                            elif not current_price:
                                logger.warning(f"[STATUS] {symbol}: price=N/A")
                            else:
                                logger.warning(
                                    f"[STATUS] {symbol}: benchmark not ready"
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

    def _input_loop(self, main_loop):
        while self.running:
            try:
                line = input()
                if line.strip():
                    main_loop.call_soon_threadsafe(self._handle_command, line)
            except EOFError:
                break
            except Exception as e:
                logger.warning(f"[CMD] Input error: {e}")

    async def run(self):
        self.running = True
        main_loop = asyncio.get_running_loop()

        input_thread = threading.Thread(
            target=self._input_loop, args=(main_loop,), daemon=True
        )
        input_thread.start()
        heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        config_task = asyncio.create_task(self.config_watch_loop())
        await self.connect()
        await self.initialize()
        status_task = asyncio.create_task(self.status_loop())
        poll_task = asyncio.create_task(self._poll_prices())
        last_kline_update = 0
        ws_reconnect_attempts = 0
        ws_reconnect_base = 1
        ws_reconnect_max = 60
        while self.running:
            try:
                await asyncio.sleep(1)
                current_time = time.time()
                if current_time - last_kline_update > 3600:
                    for symbol in self.symbols:
                        await self.update_klines(symbol)
                    last_kline_update = current_time
                if self.client and not self.client.is_connected:
                    ws_reconnect_attempts += 1
                    delay = min(
                        ws_reconnect_base * (2 ** (ws_reconnect_attempts - 1)),
                        ws_reconnect_max,
                    )
                    logger.warning(
                        f"Connection lost, reconnecting in {delay}s (attempt {ws_reconnect_attempts})..."
                    )
                    await self.send_webhook(
                        "SYSTEM", f"Connection lost, reconnecting in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                    try:
                        await self.client.connect()
                        self.connected = True
                        ws_reconnect_attempts = 0
                        await self.send_webhook(
                            "SYSTEM", "Aster Monitor reconnected to Mainnet"
                        )
                        for symbol in self.symbols:
                            await self.subscribe_symbol(symbol)
                        await self.subscribe_all_tickers()
                    except Exception as e:
                        logger.warning(f"Reconnect failed: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.send_error(e, "Main Loop")
                await asyncio.sleep(5)
        heartbeat_task.cancel()
        status_task.cancel()
        config_task.cancel()
        poll_task.cancel()

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
