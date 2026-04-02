import asyncio
import math
import os
import sys
import threading
import time
import traceback
from datetime import datetime
from typing import Optional

import numpy as np
import toml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from asterdex import WebSocketClient, Network
from asterdex.logging_config import get_logger

from config import load_config as _load_config
from indicators import (
    calculate_supertrend,
    calculate_vegas_tunnel,
    calculate_atr,
    run_atr_channel,
)
from notifications import format_number, log_warning, log_error
from notifications.webhook import build_feishu_card, _rotate_webhook_log_if_needed
from rest_api import fetch_klines, fetch_ticker_price
from signals.detection import (
    fetch_pair_klines as _fetch_pair_klines,
    update_klines as _update_klines,
    recalculate_states as _recalculate_states,
    check_signals as _check_signals,
    check_trailing_stop as _check_trailing_stop,
    recalculate_states_clustering as _recalculate_states_clustering,
    check_signals_clustering as _check_signals_clustering,
)
from signals.breakout import (
    start_breakout_monitor as _start_breakout_monitor,
    check_breakout as _check_breakout,
)
from websocket.subscriptions import (
    create_single_symbol_callbacks,
    create_pair_trading_callbacks,
    subscribe_single_symbol,
    subscribe_pair_trading,
    subscribe_all_tickers,
)

logger = get_logger(__name__)

WEBHOOK_LOG_FILE = "webhook_history.log"
LOG_RETENTION_DAYS = 7


class NotificationService:
    """
    Aster Monitor 主服务类 - 协调所有模块，管理 WebSocket 连接和交易信号检测。

    状态字典说明：
    - mark_prices: {symbol: price} 当前价格
    - mark_price_times: {symbol: unix_time} 价格更新时间
    - kline_cache: {symbol: klines} K 线缓存
    - benchmark: {symbol: {st1, st2, ema_*, atr1h_*, ...}} 技术指标结果
    - trailing_stop: {symbol: {direction, entry_price, atr15m_*, active}} 追踪止损状态
    - breakout_monitor: {symbol: {direction, trigger_price, klines_15m, ...}} 突破监控状态
    """

    def __init__(self, config_path: str = "config.toml"):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        sym_config = self.config.get("symbols", {})
        self.single_list: list = sym_config.get("single_list", [])
        self.pair_list: list = sym_config.get("pair_list", [])
        self.symbols: list = self.single_list + self.pair_list
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
        cs_config = self.config.get("clustering_st", {})
        self.clustering_min_mult = cs_config.get("min_mult", 1.0)
        self.clustering_max_mult = cs_config.get("max_mult", 5.0)
        self.clustering_step = cs_config.get("step", 0.5)
        self.clustering_perf_alpha = cs_config.get("perf_alpha", 10.0)
        self.clustering_from_cluster = cs_config.get("from_cluster", "Best")
        self.clustering_max_iter = cs_config.get("max_iter", 1000)
        self.clustering_history_klines = cs_config.get("history_klines", 500)
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
        self.clustering_states: dict[str, "ClusteringState"] = {}
        self.last_clustering_state: dict[str, dict] = {}
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
        """判断是否为配对交易对（有 / 字符）。"""
        return "/" in symbol

    def _rotate_webhook_log_if_needed(self):
        """转发到 notifications.webhook._rotate_webhook_log_if_needed。"""
        _rotate_webhook_log_if_needed(self.max_log_lines)

    async def _fetch_pair_klines(
        self,
        symbol: str,
        limit: int = 500,
        interval: str = "1h",
        proxy: str = None,
        kline_cache: dict = None,
        fetch_klines_fn=None,
    ) -> list:
        """转发到 signals.detection.fetch_pair_klines（支持 kline_cache 复用）。"""
        return await _fetch_pair_klines(
            symbol,
            limit,
            interval,
            proxy,
            kline_cache=kline_cache,
            fetch_klines_fn=fetch_klines_fn,
        )

    def _get_timestamp(self) -> str:
        """返回格式化的时间戳字符串（根据配置的时区）。"""
        tz = self.timezone
        if tz == "Z":
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        elif tz.startswith("+") or tz.startswith("-"):
            return datetime.now().strftime(f"%Y-%m-%dT%H:%M:%S{tz}")
        else:
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def _load_config(self, config_path: str) -> dict:
        """从 TOML 文件加载配置。"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return toml.load(config_path)

    def _validate_config(self, config: dict) -> bool:
        """
        验证配置字典的合法性。
        检查 symbols、webhook、atr_1h、atr_15m、clustering_st、service 字段。
        """
        try:
            if "symbols" not in config:
                return False
            sym = config["symbols"]
            single_list = sym.get("single_list", [])
            pair_list = sym.get("pair_list", [])
            if not isinstance(single_list, list) or not isinstance(pair_list, list):
                return False
            if not single_list and not pair_list:
                return False
            if "webhook" not in config or "url" not in config["webhook"]:
                return False
            if "atr_1h" in config:
                atrh_keys = ["ma_type", "period", "mult"]
                if not all(k in config["atr_1h"] for k in atrh_keys):
                    return False
            if "atr_15m" in config:
                atr15_keys = ["ma_type", "period", "mult"]
                if not all(k in config["atr_15m"] for k in atr15_keys):
                    return False
            if "clustering_st" in config:
                cs_keys = ["min_mult", "max_mult", "step", "perf_alpha", "from_cluster"]
                if not all(k in config["clustering_st"] for k in cs_keys):
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
            "single_list": self.single_list,
            "pair_list": self.pair_list,
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
            "clustering_min_mult": self.clustering_min_mult,
            "clustering_max_mult": self.clustering_max_mult,
            "clustering_step": self.clustering_step,
            "clustering_perf_alpha": self.clustering_perf_alpha,
            "clustering_from_cluster": self.clustering_from_cluster,
            "clustering_max_iter": self.clustering_max_iter,
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
            self.single_list = old_state["single_list"]
            self.pair_list = old_state["pair_list"]
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
            self.clustering_min_mult = old_state["clustering_min_mult"]
            self.clustering_max_mult = old_state["clustering_max_mult"]
            self.clustering_step = old_state["clustering_step"]
            self.clustering_perf_alpha = old_state["clustering_perf_alpha"]
            self.clustering_from_cluster = old_state["clustering_from_cluster"]
            self.clustering_max_iter = old_state["clustering_max_iter"]
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
            sym = self.config.get("symbols", {})
            self.single_list = sym.get("single_list", [])
            self.pair_list = sym.get("pair_list", [])
            self.symbols = self.single_list + self.pair_list
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
            cs = self.config.get("clustering_st", {})
            self.clustering_min_mult = cs.get("min_mult", 1.0)
            self.clustering_max_mult = cs.get("max_mult", 5.0)
            self.clustering_step = cs.get("step", 0.5)
            self.clustering_perf_alpha = cs.get("perf_alpha", 10.0)
            self.clustering_from_cluster = cs.get("from_cluster", "Best")
            self.clustering_max_iter = cs.get("max_iter", 1000)
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
                    "CONFIG",
                    f"Hot reload successful: single={len(self.single_list)}, pair={len(self.pair_list)}",
                )
            )
        except Exception as e:
            restore_state()
            error_msg = f"Hot reload failed, restored previous config: {e}"
            logger.warning(f"[CONFIG] {error_msg}")
            asyncio.create_task(self.send_webhook("CONFIG ERROR", error_msg))

    class ConfigFileHandler(FileSystemEventHandler):
        """
        配置文件监控处理器（继承 watchdog.FileSystemEventHandler）。
        当检测到 config.toml 被修改时，触发热重载。
        """

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
        """
        配置文件监控循环。
        使用 watchdog.Observer 监控 config.toml 的修改事件。
        收到修改事件后调用 reload_config() 进行热重载。
        """
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
        """转发到 notifications.webhook.build_feishu_card。"""
        return build_feishu_card(alert_type, message, extra, timestamp)

    async def send_webhook(self, alert_type: str, message: str, extra: dict = None):
        """
        发送飞书 Webhook 消息。
        1. 追加日志到 webhook_history.log
        2. 构建消息体（card 或 text）
        3. 发送 HTTP POST 请求
        """
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

        import aiohttp

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
        """发送错误通知到飞书，并将错误日志写入。"""
        error_msg = f"ERROR {context}: {type(error).__name__} - {str(error)}"
        log_error(f"Error{context}: {error}")
        await self.send_webhook("SYSTEM", error_msg)

    def warn(self, msg: str, context: str = ""):
        """发送警告日志（不推送飞书）。"""
        log_warning(f"Warning{f' ({context})' if context else ''}: {msg}")

    def write_heartbeat(self):
        """
        写入心跳文件，用于守护进程检测服务是否存活。
        文件内容为当前 Unix 时间戳。
        """

    async def connect(self):
        """
        连接 WebSocket 并订阅所有交易对。
        1. 创建 WebSocketClient 并注册错误回调
        2. 连接并标记 connected=True
        3. 推送连接成功消息
        4. 逐个订阅 symbol（根据配对或单一类型）
        5. 批量订阅所有 ticker 流
        """
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
        """WebSocket 错误回调，标记断开并推送通知。"""
        log_error(f"WebSocket error: {error}")
        if self.connected:
            self.connected = False
            await self.send_webhook("SYSTEM", f"WebSocket disconnected: {error}")

    async def subscribe_symbol(self, symbol: str):
        """根据 symbol 类型选择配对或单一订阅。"""
        if self._is_pair_trading(symbol):
            await self._subscribe_pair_trading(symbol)
        else:
            await self._subscribe_single_symbol(symbol)

    async def _subscribe_single_symbol(self, symbol: str):
        """
        订阅单一交易对的 WebSocket 流。
        注册 ticker、1h kline、15m kline 回调。
        """
        callbacks = create_single_symbol_callbacks(
            symbol,
            self.mark_prices,
            self.mark_price_times,
            self._ct_check_trailing_stop,
            self._ct_check_signals,
            self._ct_update_klines,
            self._ct_update_15m_atr,
            lambda msg, ctx="": self.warn(msg, ctx),
        )
        on_ticker, on_kline_1h, on_kline_15m = callbacks
        self.client.on_ticker(symbol)(on_ticker)
        self.client.on_kline(symbol, "1h")(on_kline_1h)
        self.client.on_kline(symbol, "15m")(on_kline_15m)
        logger.info(f"Subscribed to {symbol}")

    async def _subscribe_pair_trading(self, symbol: str):
        """
        订阅配对交易对的 WebSocket 流。
        symbol1 和 symbol2 共用 ticker 回调，1h kline 用 symbol1，15m kline 用 symbol1 和 symbol2。
        使用 Clustering SuperTrend 进行信号检测。
        """
        parts = symbol.split("/")
        symbol1, symbol2 = parts[0], parts[1]
        callbacks = create_pair_trading_callbacks(
            symbol,
            symbol1,
            symbol2,
            self.mark_prices,
            self.mark_price_times,
            self._pair_components,
            self._breakout_comp_prices,
            self._ct_update_pair_price,
            self._ct_recalculate_states_clustering,
            self._ct_update_15m_atr,
            lambda msg, ctx="": self.warn(msg, ctx),
        )
        on_ticker, on_kline_1h, on_15m_kline_p1, on_15m_kline_p2 = callbacks
        self.client.on_ticker(symbol1)(on_ticker)
        self.client.on_ticker(symbol2)(on_ticker)
        self.client.on_kline(symbol1, "1h")(on_kline_1h)
        self.client.on_kline(symbol1, "15m")(on_15m_kline_p1)
        self.client.on_kline(symbol2, "15m")(on_15m_kline_p2)
        logger.info(f"Subscribed to PairTrading {symbol} ({symbol1}/{symbol2})")

    async def subscribe_all_tickers(self):
        """批量订阅所有交易对的 ticker 流（用于 REST polling 补充）。"""
        await subscribe_all_tickers(
            self.client, self.symbols, lambda s: self._is_pair_trading(s)
        )
        logger.info(f"Batch subscribed to {len(self.symbols)} symbols")

    async def fetch_klines(
        self, symbol: str, limit: int = 500, interval: str = "1h", proxy: str = None
    ) -> list:
        """转发到 rest_api.fetch_klines。"""
        return await fetch_klines(symbol, limit, interval, proxy)

    async def fetch_ticker_price(self, symbol: str, proxy: str = None) -> float:
        """转发到 rest_api.fetch_ticker_price。"""
        return await fetch_ticker_price(symbol, proxy)

    async def _update_pair_price(self, symbol: str, symbol1: str, symbol2: str):
        """
        计算并更新配对交易对的价格。
        如果 component 价格为空，尝试通过 REST fallback 获取。
        价格 = symbol1_price / symbol2_price。
        更新后触发追踪止损检查和信号检查。
        """
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
            await self._ct_check_trailing_stop(symbol, pair_price)
            await self._ct_check_signals_clustering(symbol)

    async def _poll_prices(self):
        """
        REST 价格轮询循环（WebSocket 数据的兜底补充）。

        逻辑：
        - 每秒轮询所有 symbol 的最新价格（WebSocket 数据超过 5 秒不新鲜则使用 REST）
        - 配对交易：同时轮询两个 component，计算配对价格
        - 每 15 秒：如果有活跃的追踪止损，更新其 15m ATR
        - 新价格到达后触发追踪止损检查和信号检查
        - 所有价格就绪后推送启动摘要
        """
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
                            await self._ct_check_trailing_stop(sym, price)
                            if self._is_pair_trading(sym):
                                await self._ct_check_signals_clustering(sym)
                            else:
                                await self._ct_check_signals(sym)
                    # SINGLE symbol: klines 通过 REST 定期刷新（即使价格新鲜，klines 也可能未初始化）
                    if not self._is_pair_trading(sym) and sym not in self.benchmark:
                        await self._ct_update_klines(sym)
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
                        await self._ct_check_trailing_stop(pair_symbol, pair_price)
                        await self._ct_check_signals_clustering(pair_symbol)
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

    async def _ct_check_trailing_stop(self, symbol, price):
        """连接器：将 _check_trailing_stop 桥接到实例方法。"""
        await _check_trailing_stop(
            symbol,
            price,
            self.trailing_stop,
            self.send_webhook,
            self._increment_alert_count,
            self.last_alert_time,
        )

    async def _ct_check_signals(self, symbol):
        """连接器：将 _check_signals 桥接到实例方法。"""
        await _check_signals(
            symbol,
            self.mark_prices,
            self.mark_price_times,
            self.benchmark,
            self.trailing_stop,
            self.last_atr_state,
            self.last_alert_time,
            self._initialized,
            self.last_st_state,
            self.atr1h_ma_type,
            self.atr1h_period,
            self.atr1h_mult,
            self.atr15m_ma_type,
            self.atr15m_period,
            self.atr15m_mult,
            self.send_webhook,
            self._increment_alert_count,
        )

    async def _ct_update_klines(self, symbol):
        """连接器：将 _update_klines 桥接到实例方法。"""
        await _update_klines(
            symbol,
            self.kline_cache,
            self.last_kline_time,
            lambda s: self._is_pair_trading(s),
            self.proxy_url if self.proxy_enable else None,
            lambda s: self._recalculate_states(s),
            self._fetch_pair_klines,
            self.fetch_klines,
        )

    async def _ct_update_15m_atr(self, symbol, kline):
        """连接器：桥接到 update_15m_atr 实例方法。"""
        await self.update_15m_atr(symbol, kline)

    async def _ct_update_pair_price(self, symbol, symbol1, symbol2):
        """连接器：桥接到 _update_pair_price 实例方法。"""
        await self._update_pair_price(symbol, symbol1, symbol2)

    async def _ct_check_signals_clustering(self, symbol):
        """连接器：将 _check_signals_clustering 桥接到实例方法。"""
        await _check_signals_clustering(
            symbol,
            self.mark_prices,
            self.mark_price_times,
            self.benchmark,
            self.trailing_stop,
            self.last_clustering_state,
            self.last_alert_time,
            self._initialized,
            self.last_st_state,
            self.clustering_states,
            self.atr1h_ma_type,
            self.atr1h_period,
            self.atr1h_mult,
            self.atr15m_ma_type,
            self.atr15m_period,
            self.atr15m_mult,
            self.clustering_min_mult,
            self.clustering_max_mult,
            self.clustering_step,
            self.clustering_perf_alpha,
            self.clustering_from_cluster,
            self.clustering_max_iter,
            self.send_webhook,
            self._increment_alert_count,
        )

    async def _ct_recalculate_states_clustering(self, symbol: str):
        """连接器：将 _recalculate_states_clustering 桥接到实例方法。"""
        await _recalculate_states_clustering(
            symbol,
            self.kline_cache,
            self.benchmark,
            self.clustering_states,
            self._is_pair_trading(symbol),
            self.st_period1,
            self.st_multiplier1,
            self.st_period2,
            self.st_multiplier2,
            self.vt_ema_signal,
            self.vt_ema_upper,
            self.vt_ema_lower,
            self.atr1h_period,
            self.atr1h_ma_type,
            self.atr1h_mult,
            self.atr15m_period,
            self.atr15m_ma_type,
            self.atr15m_mult,
            self.clustering_min_mult,
            self.clustering_max_mult,
            self.clustering_step,
            self.clustering_perf_alpha,
            self.clustering_from_cluster,
            self.clustering_max_iter,
            self.clustering_history_klines,
        )

    def calculate_supertrend(self, high, low, close, period, multiplier):
        """转发到 indicators.calculate_supertrend。"""
        return calculate_supertrend(high, low, close, period, multiplier)

    def calculate_vegas_tunnel(self, close):
        """转发到 indicators.calculate_vegas_tunnel（传入实例的 vegas 参数）。"""
        return calculate_vegas_tunnel(
            close, self.vt_ema_signal, self.vt_ema_upper, self.vt_ema_lower
        )

    def calculate_dema(self, data, period):
        """转发到 indicators.calculate_dema。"""
        from indicators import calculate_dema as _calculate_dema

        return _calculate_dema(data, period)

    def calculate_hma(self, data, period):
        """转发到 indicators.calculate_hma。"""
        from indicators import calculate_hma as _calculate_hma

        return _calculate_hma(data, period)

    def calculate_tr(self, high, low, close):
        """转发到 indicators.calculate_tr。"""
        from indicators import calculate_tr as _calculate_tr

        return _calculate_tr(high, low, close)

    def calculate_atr(self, high, low, close, period, ma_type="DEMA"):
        """转发到 indicators.calculate_atr。"""
        return calculate_atr(high, low, close, period, ma_type)

    def run_atr_channel(self, close, atr, mult, prev_state):
        """转发到 indicators.run_atr_channel。"""
        return run_atr_channel(close, atr, mult, prev_state)

    async def update_15m_atr(self, symbol, kline):
        """
        通过 WebSocket 15m K 线更新追踪止损的 ATR 通道。

        条件：symbol 必须在 trailing_stop 中且 active=True。
        根据 K 线 OHLC 计算 15m ATR，并通过 run_atr_channel 更新上下轨。
        """
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
        """
        通过 REST polling 更新追踪止损的 15m ATR 通道（WebSocket 不可用时的兜底）。

        获取最近 20 条 15m K 线，计算 ATR，然后用 run_atr_channel 更新上下轨。
        """
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

    async def _recalculate_states(self, symbol: str):
        """转发到 signals.detection.recalculate_states（桥接实例参数）。"""
        await _recalculate_states(
            symbol,
            self.kline_cache,
            self.benchmark,
            lambda s: self._is_pair_trading(s),
            self.st_period1,
            self.st_multiplier1,
            self.st_period2,
            self.st_multiplier2,
            self.vt_ema_signal,
            self.vt_ema_upper,
            self.vt_ema_lower,
            self.atr1h_period,
            self.atr1h_ma_type,
            self.atr1h_mult,
            self.atr15m_period,
            self.atr15m_ma_type,
            self.atr15m_mult,
        )

    async def start_breakout_monitor(self, symbol, direction, price, trigger_time):
        """转发到 signals.breakout.start_breakout_monitor（桥接实例参数）。"""
        await _start_breakout_monitor(
            symbol,
            direction,
            price,
            trigger_time,
            self.breakout_monitor,
            lambda s: self._is_pair_trading(s),
            self._breakout_comp_prices,
            self.client,
            lambda s, k: self.update_15m_atr(s, k),
            fetch_pair_klines_fn=self._fetch_pair_klines,
            proxy=self.proxy_url if self.proxy_enable else None,
        )
        logger.info(f"Breakout monitor started for {symbol}, direction={direction}")

    async def stop_breakout_monitor(self, symbol):
        """停止突破监控，删除对应状态。"""
        if symbol in self.breakout_monitor:
            del self.breakout_monitor[symbol]
            logger.info(f"Breakout monitor stopped for {symbol}")

    async def on_15m_kline(self, symbol, kline, comp1=None, comp2=None):
        """
        处理 15m K 线追加到突破监控列表，并触发突破检查。
        内部调用 signals.breakout._on_15m_kline 和 check_breakout。
        """
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
            await _check_breakout(
                symbol,
                self.breakout_monitor,
                self.send_webhook,
                self._increment_alert_count,
                stop_breakout_monitor_fn=self.stop_breakout_monitor,
            )
        except Exception as e:
            self.warn(f"on_15m_kline error for {symbol}: {e}", "breakout")
            await self.stop_breakout_monitor(symbol)

    async def update_klines(self, symbol: str):
        """转发到 signals.detection.update_klines（桥接实例方法）。"""
        await _update_klines(
            symbol,
            self.kline_cache,
            self.last_kline_time,
            lambda s: self._is_pair_trading(s),
            self.proxy_url if self.proxy_enable else None,
            lambda s: self._recalculate_states(s),
            self._fetch_pair_klines,
            self.fetch_klines,
        )

    async def recalculate_states(self, symbol: str):
        """公开的指标重算入口（转发到 _recalculate_states）。"""
        await self._recalculate_states(symbol)

    async def check_signals(self, symbol: str):
        """公开的信号检查入口（转发到 _ct_check_signals）。"""
        await self._ct_check_signals(symbol)

    async def check_trailing_stop(self, symbol, current_price):
        """公开的追踪止损检查入口（转发到 _ct_check_trailing_stop）。"""
        await self._ct_check_trailing_stop(symbol, current_price)

    async def initialize(self):
        """
        初始化服务：获取所有 symbol 的历史 K 线并计算基准指标。

        流程：
        1. 设置 _initialized=False，防止初始化期间误推信号
        2. 获取所有 symbol 的 500 条 1h K 线
        3. 重算 Supertrend、Vegas Tunnel、ATR Channel
        4. 设置 _initialized=True
        5. 等待所有价格就绪后推送启动摘要
        """
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
            # SINGLE symbol 还需要 benchmark 就绪
            elif not self._is_pair_trading(sym) and sym not in self.benchmark:
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
        """
        发送启动摘要 WebHook。
        SINGLE 显示 ATR_Ch 状态，PAIR 显示 ClusterST 状态。
        """
        lines = []
        for sym in self.symbols:
            current_price = self.mark_prices.get(sym)
            bm = self.benchmark.get(sym)
            if bm and current_price:
                if self._is_pair_trading(sym):
                    ts = bm.get("ts", 0)
                    direction = self.last_clustering_state.get(sym, {}).get(
                        "sent", "N/A"
                    )
                    lines.append(
                        f"{sym}: ClusterST={direction} | Price={format_number(current_price)} | TS={format_number(ts)}"
                    )
                else:
                    atr_upper = bm.get("atr1h_upper", 0)
                    atr_lower = bm.get("atr1h_lower", 0)
                    atr_ch = bm.get("atr1h_ch", 0)
                    direction = (
                        "LONG" if atr_ch == 1 else ("SHORT" if atr_ch == -1 else "N/A")
                    )
                    lines.append(
                        f"{sym}: ATR_Ch={direction} | Price={format_number(current_price)} | Channel={format_number(atr_lower)}~{format_number(atr_upper)}"
                    )
        if lines:
            await self.send_webhook("SYSTEM", "\n".join(lines))

    def _print_status(self, symbol: str):
        """打印单个 symbol 的状态到日志。SINGLE 显示 ATR_Ch，PAIR 显示 ClusterST。"""
        current_price = self.mark_prices.get(symbol)
        bm = self.benchmark.get(symbol)
        if bm and current_price:
            if self._is_pair_trading(symbol):
                ts = bm.get("ts", 0)
                direction = self.last_clustering_state.get(symbol, {}).get(
                    "sent", "N/A"
                )
                logger.info(
                    f"[STATUS] {symbol}: Price={format_number(current_price)}, ClusterST={direction}, TS={format_number(ts)}"
                )
            else:
                atr_upper = bm.get("atr1h_upper", 0)
                atr_lower = bm.get("atr1h_lower", 0)
                atr_ch = bm.get("atr1h_ch", 0)
                direction = (
                    "LONG" if atr_ch == 1 else ("SHORT" if atr_ch == -1 else "N/A")
                )
                logger.info(
                    f"[STATUS] {symbol}: Price={format_number(current_price)}, ATR_Ch={direction}, Channel={format_number(atr_lower)}~{format_number(atr_upper)}"
                )

    async def heartbeat_loop(self):
        """
        心跳循环：每 30 秒写入心跳文件。
        用于守护进程检测服务是否存活。
        """
        while self.running:
            self.write_heartbeat()
            await asyncio.sleep(30)

    async def status_loop(self):
        """
        状态报告循环（每 120 秒执行一次）。
        1. 检查是否需要发送每日报告
        2. 打印所有 symbol 的当前状态到日志
        """
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
                                if self._is_pair_trading(symbol):
                                    ts = bm.get("ts", 0)
                                    direction = self.last_clustering_state.get(
                                        symbol, {}
                                    ).get("sent", "N/A")
                                    logger.info(
                                        f"[STATUS] {symbol}: Price={format_number(current_price)}, ClusterST={direction}, TS={format_number(ts)}"
                                    )
                                else:
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
        """
        检查是否到达每日报告时间，到达时推送警报计数。

        触发条件：当前时间在 report_times 列表中某个时间的 ±1 分钟内，
        且距离上次报告已超过 1 小时。
        """
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
        """线程安全地递增警报计数。"""
        with self._lock:
            self._alert_count += 1

    def _handle_command(self, cmd: str):
        """
        处理运行时命令。
        - stop print：暂停状态打印
        - resume print：恢复状态打印
        - status：打印当前状态
        """
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
        """
        标准输入监听循环（后台线程）。
        读取一行命令后，通过 call_soon_threadsafe 调度到事件循环执行。
        """
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
        """
        主运行循环。

        启动流程：
        1. 标记 running=True
        2. 启动 stdin 命令输入线程
        3. 启动心跳循环（每 30s）
        4. 启动配置监控循环（热重载）
        5. 连接 WebSocket 并订阅所有交易对
        6. 初始化 K 线数据
        7. 启动状态报告循环（每 120s）
        8. 启动 REST 价格轮询循环

        主循环：监控 WebSocket 连接状态，断开时指数退避重连（1s→2s→4s→...→60s）
        """
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
        """
        优雅停止服务。

        清理步骤：
        1. 标记 running=False，停止所有循环
        2. 清空突破监控状态
        3. 停止配置监控 Observer
        4. 断开 WebSocket 连接
        5. 删除心跳文件
        """
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
