"""
CandleSticks SQLite 持久化层（L2 缓存）。

策略：
- 每 symbol + interval 最多存 MAX_KLINES 条（默认 1000）
- 新 K 线写入后，自动淘汰超出部分的旧数据
- prices 表存最新价格（symbol → price + update_time）
"""

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from .models import Kline, Ticker

DEFAULT_DB_PATH = "candlesticks_cache.db"
MAX_KLINES_PER_SYM_INT = 1000


class PersistenceDB:
    """
    SQLite 持久化封装。

    线程安全：所有操作通过 threading.Lock 保护。
    """

    def __init__(
        self, db_path: str = DEFAULT_DB_PATH, max_klines: int = MAX_KLINES_PER_SYM_INT
    ):
        self.db_path = db_path
        self.max_klines = max_klines
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """初始化数据库表。"""
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS klines (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    open REAL, high REAL, low REAL, close REAL, volume REAL,
                    PRIMARY KEY (symbol, interval, open_time)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_klines_lookup
                ON klines(symbol, interval, open_time DESC)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                    symbol TEXT PRIMARY KEY,
                    price REAL,
                    update_time INTEGER
                )
            """)

    @contextmanager
    def _conn(self):
        """获取数据库连接的上下文管理器。"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    # ==================== K Lines ====================

    def upsert_kline(self, kline: Kline):
        """
        插入或更新一根 K 线，并淘汰超出限制的旧数据。

        淘汰策略：
        - 先 INSERT OR REPLACE
        - 再删除超出 max_klines 的旧行（按 open_time ASC，保留最新的）
        """
        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO klines
                    (symbol, interval, open_time, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        kline.symbol,
                        kline.interval,
                        kline.open_time,
                        kline.open,
                        kline.high,
                        kline.low,
                        kline.close,
                        kline.volume,
                    ),
                )
                # 淘汰旧数据：只保留最新的 max_klines 条
                conn.execute(
                    f"""
                    DELETE FROM klines
                    WHERE (symbol, interval) = (?, ?)
                    AND open_time NOT IN (
                        SELECT open_time FROM klines
                        WHERE symbol = ? AND interval = ?
                        ORDER BY open_time DESC
                        LIMIT ?
                    )
                """,
                    (
                        kline.symbol,
                        kline.interval,
                        kline.symbol,
                        kline.interval,
                        self.max_klines,
                    ),
                )

    def upsert_klines(self, klines: list[Kline]):
        """批量插入 K 线。"""
        if not klines:
            return
        with self._lock:
            with self._conn() as conn:
                for k in klines:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO klines
                        (symbol, interval, open_time, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            k.symbol,
                            k.interval,
                            k.open_time,
                            k.open,
                            k.high,
                            k.low,
                            k.close,
                            k.volume,
                        ),
                    )

    def get_klines(self, symbol: str, interval: str, limit: int = 500) -> list[Kline]:
        """
        获取 K 线（从最新到最老）。

        Args:
            symbol: 交易对名称
            interval: K 线周期
            limit: 最大返回条数

        Returns:
            Kline 列表，最新在前
        """
        with self._lock:
            with self._conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT open_time, open, high, low, close, volume
                    FROM klines
                    WHERE symbol = ? AND interval = ?
                    ORDER BY open_time DESC
                    LIMIT ?
                """,
                    (symbol, interval, limit),
                )
                rows = cursor.fetchall()

        return [
            Kline(
                symbol=symbol,
                interval=interval,
                open_time=int(r[0]),
                open=float(r[1]),
                high=float(r[2]),
                low=float(r[3]),
                close=float(r[4]),
                volume=float(r[5]),
            )
            for r in reversed(rows)  # 最老在前（方便指标计算）
        ]

    def get_latest_kline(self, symbol: str, interval: str) -> Optional[Kline]:
        """获取最新一根 K 线。"""
        klines = self.get_klines(symbol, interval, limit=1)
        return klines[-1] if klines else None

    def get_oldest_open_time(self, symbol: str, interval: str) -> Optional[int]:
        """获取最老一根 K 线的 open_time。"""
        with self._lock:
            with self._conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT open_time FROM klines
                    WHERE symbol = ? AND interval = ?
                    ORDER BY open_time ASC
                    LIMIT 1
                """,
                    (symbol, interval),
                )
                row = cursor.fetchone()
        return int(row[0]) if row else None

    # ==================== Prices ====================

    def upsert_price(self, ticker: Ticker):
        """更新最新价格。"""
        with self._lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO prices (symbol, price, update_time)
                    VALUES (?, ?, ?)
                """,
                    (ticker.symbol, ticker.price, int(ticker.update_time * 1000)),
                )

    def get_price(self, symbol: str) -> Optional[float]:
        """获取最新价格。"""
        with self._lock:
            with self._conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT price FROM prices WHERE symbol = ?
                """,
                    (symbol,),
                )
                row = cursor.fetchone()
        return float(row[0]) if row else None

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        """批量获取价格。"""
        if not symbols:
            return {}
        with self._lock:
            with self._conn() as conn:
                placeholders = ",".join("?" * len(symbols))
                cursor = conn.execute(
                    f"""
                    SELECT symbol, price FROM prices WHERE symbol IN ({placeholders})
                """,
                    symbols,
                )
                rows = cursor.fetchall()
        return {r[0]: float(r[1]) for r in rows}

    # ==================== Housekeeping ====================

    def count_klines(self, symbol: str, interval: str) -> int:
        """返回当前缓存的 K 线数量。"""
        with self._lock:
            with self._conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM klines WHERE symbol = ? AND interval = ?
                """,
                    (symbol, interval),
                )
                row = cursor.fetchone()
        return row[0] if row else 0

    def close(self):
        """关闭连接（当前无持久连接，保留接口）。"""
        pass
