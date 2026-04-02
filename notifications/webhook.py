"""
飞书 Webhook 通知模块 - 负责构建消息体和发送 Webhook 请求。

主要功能：
- log_warning / log_error: 日志记录（透传到 asterdex logger）
- _rotate_webhook_log_if_needed: Webhook 日志文件自动轮转
- build_feishu_card: 构建飞书 Interactive Card 格式消息
- send_webhook: 发送 Webhook 请求（支持 card 和 text 格式）
"""

import os

from asterdex.logging_config import get_logger
from .formatters import format_number

logger = get_logger(__name__)

# Webhook 历史日志文件路径
WEBHOOK_LOG_FILE = "webhook_history.log"


def log_warning(msg: str):
    """透传警告日志到 asterdex logger."""
    logger.warning(msg)


def log_error(msg: str):
    """透传错误日志到 asterdex logger."""
    logger.error(msg)


def _rotate_webhook_log_if_needed(max_log_lines: int = 1000):
    """
    如果 webhook_history.log 超过 max_log_lines 行，
    则截断为最新的 max_log_lines 行。

    参数:
        max_log_lines: 最大保留行数（默认 1000）
    """
    try:
        if not os.path.exists(WEBHOOK_LOG_FILE):
            return
        with open(WEBHOOK_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > max_log_lines:
            with open(WEBHOOK_LOG_FILE, "w", encoding="utf-8") as f:
                f.writelines(lines[-max_log_lines:])
    except Exception as e:
        logger.warning(f"Log rotation failed: {e}")


def build_feishu_card(
    alert_type: str, message: str, extra: dict, timestamp: str
) -> dict:
    """
    根据警报类型构建飞书 Interactive Card 消息体。

    支持的 alert_type:
    - ATR_Ch: ATR Channel 信号（多、空头、追踪止损）
    - SYSTEM: 系统消息（连接/断开）
    - ERROR: 错误消息
    - CONFIG: 配置热重载成功
    - CONFIG ERROR: 配置热重载失败
    - REPORT: 每日报告
    - BREAKOUT: 突破确认/失败信号

    参数:
        alert_type: 警报类型字符串
        message: 消息内容
        extra: 扩展数据字典，包含 symbol, direction, price 等字段
        timestamp: 触发时间字符串

    返回:
        飞书 Card 格式的 dict，可直接作为 HTTP POST 的 json body
    """
    extra = extra or {}
    direction = extra.get("direction", "").lower()
    symbol = extra.get("symbol", "")
    state = extra.get("state", "")
    reason = extra.get("reason", "")

    # ===================== ATR_Ch =====================
    if alert_type == "ATR_Ch":
        is_trailing = reason == "trailing_stop"
        # 颜色和 emoji 根据方向和类型区分
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

        # 追踪止损：显示方向、现价、止损线、入场价
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
        # 普通 ATR_Ch 信号：显示方向、现价、通道上下轨
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

    # ===================== SYSTEM =====================
    elif alert_type == "SYSTEM":
        color = "blue"
        title = f"🔔 System"
        elements = [
            {"tag": "markdown", "content": f"**{message}**"},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
        ]

    # ===================== ERROR =====================
    elif alert_type == "ERROR":
        color = "red"
        title = f"⚠️ Error"
        elements = [
            {"tag": "markdown", "content": f"**{message}**"},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
        ]

    # ===================== CONFIG =====================
    elif alert_type == "CONFIG":
        color = "purple"
        title = f"⚙️ Config"
        elements = [
            {"tag": "markdown", "content": f"**{message}**"},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
        ]

    # ===================== CONFIG ERROR =====================
    elif alert_type == "CONFIG ERROR":
        color = "red"
        title = f"⚙️ Config Error"
        elements = [
            {"tag": "markdown", "content": f"**{message}**"},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
        ]

    # ===================== REPORT =====================
    elif alert_type == "REPORT":
        color = "purple"
        title = f"📊 Daily Report"
        elements = [
            {"tag": "markdown", "content": f"**{message}**"},
            {"tag": "hr"},
            {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
        ]

    # ===================== BREAKOUT =====================
    elif alert_type == "BREAKOUT":
        color = "orange"
        emoji = "💥"
        title = f"{emoji} {symbol}"
        confirmed = extra.get("confirmed", False)
        direction_disp = extra.get("direction", "")
        confirmed_text = "CONFIRMED" if confirmed else "FALSE"
        elements = [
            {
                "tag": "markdown",
                "content": f"**Breakout:** {direction_disp} {confirmed_text}",
            },
            {"tag": "markdown", "content": f"**Price:** {extra.get('price', '')}"},
        ]
        if confirmed:
            elements.append(
                {
                    "tag": "markdown",
                    "content": f"**Trigger:** {extra.get('trigger', '')}",
                }
            )
        else:
            elements.append(
                {"tag": "markdown", "content": f"**Reason:** {extra.get('reason', '')}"}
            )
        elements.extend(
            [
                {"tag": "hr"},
                {"tag": "markdown", "content": f"**Trigger Time:** {timestamp}"},
            ]
        )

    # ===================== 默认 =====================
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


async def send_webhook(
    webhook_url: str,
    webhook_format: str,
    alert_type: str,
    message: str,
    extra: dict = None,
    max_log_lines: int = 1000,
    get_timestamp_fn=None,
):
    """
    发送飞书 Webhook 消息。

    流程：
    1. 追加日志到 webhook_history.log（带自动轮转）
    2. 根据 format 构建消息体（card 或 text）
    3. 发送 HTTP POST 请求到 webhook_url
    4. 成功时记录 info 日志，失败时记录 error 日志

    参数:
        webhook_url: 飞书 Webhook URL
        webhook_format: "card" 或 "text"
        alert_type: 警报类型
        message: 消息内容
        extra: 扩展数据字典
        max_log_lines: 日志轮转阈值
        get_timestamp_fn: 获取时间戳的函数（可选，默认返回空）
    """
    timestamp = get_timestamp_fn() if get_timestamp_fn else ""
    full_content = f"[{timestamp}] [{alert_type}] {message}"

    # Step 1: 写入日志文件
    try:
        _rotate_webhook_log_if_needed(max_log_lines)
        with open(WEBHOOK_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{full_content}\n")
    except Exception as e:
        logger.warning(f"Write webhook log failed: {e}")

    # Step 2: 构建消息体
    if webhook_format == "card":
        card = build_feishu_card(alert_type, message, extra, timestamp)
        msg = {"msg_type": "interactive", "card": card}
    else:
        msg = {"msg_type": "text", "content": {"text": full_content}}

    # Step 3: 发送 HTTP 请求
    import aiohttp

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=msg, timeout=10) as resp:
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
