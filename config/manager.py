"""
配置管理模块 - 处理配置文件的加载、保存、创建和热重载。

主要功能：
- load_config / save_config：读写 TOML 配置文件
- create_config：创建默认配置文件
- update_symbols：添加/移除监控交易对
- cleanup_old_logs：清理过期的 webhook 历史日志
"""

import os
import time
from datetime import datetime

import toml

# Webhook 日志文件路径（与服务中的一致）
WEBHOOK_LOG_FILE = "webhook_history.log"
# 日志保留天数
LOG_RETENTION_DAYS = 7


def cleanup_old_logs():
    """
    清理 WEBHOOK_LOG_FILE 中超过 LOG_RETENTION_DAYS 的旧日志。
    按每行开头的 [timestamp] 解析日期，删除超过截止时间的行。
    """
    try:
        if not os.path.exists(WEBHOOK_LOG_FILE):
            return
        cutoff_time = time.time() - (LOG_RETENTION_DAYS * 24 * 3600)
        temp_file = WEBHOOK_LOG_FILE + ".tmp"
        with open(WEBHOOK_LOG_FILE, "r", encoding="utf-8") as f_in:
            with open(temp_file, "w", encoding="utf-8") as f_out:
                for line in f_in:
                    try:
                        # 日志格式：[2026-04-01 16:56:20] [...]
                        log_time_str = line.split("]")[0].strip("[")
                        log_time = datetime.strptime(log_time_str, "%Y-%m-%d %H:%M:%S")
                        if log_time.timestamp() > cutoff_time:
                            f_out.write(line)
                    except Exception:
                        # 解析失败的行保留（如空行、格式异常）
                        f_out.write(line)
        os.replace(temp_file, WEBHOOK_LOG_FILE)
    except Exception as e:
        print(f"Log cleanup failed: {e}")


def load_config(config_path: str) -> dict:
    """
    从 TOML 文件加载配置字典。

    参数:
        config_path: 配置文件路径

    返回:
        配置字典

    异常:
        FileNotFoundError: 配置文件不存在
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return toml.load(config_path)


def save_config(config_path: str, config: dict):
    """
    将配置字典保存到 TOML 文件。

    参数:
        config_path: 配置文件路径
        config: 配置字典
    """
    with open(config_path, "w", encoding="utf-8") as f:
        toml.dump(config, f)
    print(f"Config saved: {config_path}")


def update_symbols(config_path: str, action: str, symbols: list, target: str = None):
    """
    添加或移除监控交易对。

    参数:
        config_path: 配置文件路径
        action: "add" 或 "remove"
        symbols: 交易对名称列表
        target: "single_list" 或 "pair_list" 或 None (自动判断)
               若为 None，自动判断：含 "/" 的归 pair_list，否则归 single_list
    """
    config = load_config(config_path)
    sym_config = config.setdefault("symbols", {})

    if target is None:
        single = [s for s in symbols if "/" not in s]
        pair = [s for s in symbols if "/" in s]
    else:
        single = symbols if target == "single_list" else []
        pair = symbols if target == "pair_list" else []

    if single:
        current_single = set(sym_config.get("single_list", []))
        if action == "add":
            current_single.update(single)
            print(f"Added (single): {single}")
        elif action == "remove":
            current_single -= set(single)
            print(f"Removed (single): {single}")
        sym_config["single_list"] = sorted(list(current_single))

    if pair:
        current_pair = set(sym_config.get("pair_list", []))
        if action == "add":
            current_pair.update(pair)
            print(f"Added (pair): {pair}")
        elif action == "remove":
            current_pair -= set(pair)
            print(f"Removed (pair): {pair}")
        sym_config["pair_list"] = sorted(list(current_pair))

    save_config(config_path, config)
    print(f"Current single_list: {sym_config.get('single_list', [])}")
    print(f"Current pair_list: {sym_config.get('pair_list', [])}")


def create_config(
    config_path: str, webhook_url: str, single_list: list = None, pair_list: list = None
) -> dict:
    """
    创建默认配置文件。

    参数:
        config_path: 配置文件路径
        webhook_url: 飞书 Webhook URL
        single_list: 初始 ATR Channel 监控交易对列表，默认为 ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "XAUUSDT"]
        pair_list: 初始 Clustering SuperTrend 监控交易对列表，默认为 []

    返回:
        创建的配置字典
    """
    if single_list is None:
        single_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT", "XAUUSDT"]
    if pair_list is None:
        pair_list = []
    config = {
        "webhook": {"url": webhook_url, "format": "card"},
        "symbols": {
            "single_list": single_list,
            "pair_list": pair_list,
        },
        "atr_1h": {
            "ma_type": "DEMA",
            "period": 14,
            "mult": 1.618,
        },
        "atr_15m": {
            "ma_type": "HMA",
            "period": 14,
            "mult": 1.3,
        },
        "clustering_st": {
            "min_mult": 1.0,
            "max_mult": 5.0,
            "step": 0.5,
            "perf_alpha": 10.0,
            "from_cluster": "Best",
            "max_iter": 1000,
            "history_klines": 500,
        },
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
        "settings": {
            "timezone": "Z",
            "max_log_lines": 1000,
        },
    }
    with open(config_path, "w", encoding="utf-8") as f:
        toml.dump(config, f)
    print(f"Config created: {config_path}")
    return config
