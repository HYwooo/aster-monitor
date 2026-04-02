"""
通知格式化模块 - 纯格式化工具函数，不涉及网络或文件 I/O。

主要功能：
- format_number: 将浮点数格式化为合适的小数位数
"""

import math


def format_number(value: float) -> str:
    """
    将浮点数格式化为可读的数字字符串。

    规则：
    - 非有限值（inf/nan）返回原字符串
    - 零返回 "0.00000000"
    - 绝对值 < 1：保留 8 位小数
    - 绝对值 >= 1：整数位数 + 小数位数 = 8 位（末尾 0 省略）

    示例：
        0.00001234  -> "0.00001234"
        1.5         -> "1.5"
        123456.789  -> "123456.79"
        0           -> "0.00000000"
    """
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
