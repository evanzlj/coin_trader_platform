"""
行情读取层（§20.3）—— executor 在 btc-ml 本地直读采集 DB（表 ohlcv_bars）。

要点：
- **只读连接 + busy_timeout**：与采集进程并发安全（采集在写，executor 只读）。
- **只返回已收线 bar**（`close_time <= now`）：严格对齐 scorer 的收线判断口径（P2-2），
  即便采集端写了进行中的当前 bar 也会被排除。
- open_time / close_time 存为 ISO8601 带 `+00:00`（UTC），字典序 == 时间序，可直接字符串比较。

表结构（btc-ml 实测）：
  ohlcv_bars(id, symbol, timeframe, open_time, close_time, open, high, low, close,
             volume, source, is_fallback, ingest_time)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from live import exec_config as cfg

COLS = ["open_time", "open", "high", "low", "close", "volume"]


class OhlcvReader:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path or cfg.OHLCV_DB)

    # ── 内部 ──────────────────────────────────────────────────────────────────

    def _query(self, sql: str, params: tuple) -> list[tuple]:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5.0)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    @staticmethod
    def _now(now: Optional[pd.Timestamp]) -> pd.Timestamp:
        return now if now is not None else pd.Timestamp.now("UTC")

    @staticmethod
    def _iso(ts: pd.Timestamp) -> str:
        # 去微秒，与 DB 的 '...:00+00:00' 同格式，保证字符串比较干净
        return ts.tz_convert("UTC").replace(microsecond=0).isoformat()

    @staticmethod
    def _to_df(rows: list[tuple]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=COLS)
        if not df.empty:
            df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
            for c in ("open", "high", "low", "close", "volume"):
                df[c] = df[c].astype(float)
        return df

    # ── 新 bar 检测 ───────────────────────────────────────────────────────────

    def latest_closed_bar_time(self, symbol: str, tf: str = "15m",
                               now: Optional[pd.Timestamp] = None) -> Optional[pd.Timestamp]:
        """最新一根**已收线** bar 的 open_time（tz-aware UTC）；无数据返回 None。"""
        now = self._now(now)
        rows = self._query(
            "SELECT MAX(open_time) FROM ohlcv_bars "
            "WHERE symbol=? AND timeframe=? AND close_time<=?",
            (symbol, tf, self._iso(now)),
        )
        v = rows[0][0] if rows else None
        return pd.Timestamp(v) if v else None

    # ── 加载 bar ──────────────────────────────────────────────────────────────

    def load_closed_since(self, symbol: str, since_open_time: Optional[pd.Timestamp],
                          tf: str = "15m", now: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """open_time > since 且已收线的 bar，升序。since=None → 全部已收线。
        列：open_time/open/high/low/close/volume。"""
        now = self._now(now)
        if since_open_time is not None:
            rows = self._query(
                "SELECT open_time,open,high,low,close,volume FROM ohlcv_bars "
                "WHERE symbol=? AND timeframe=? AND open_time>? AND close_time<=? "
                "ORDER BY open_time",
                (symbol, tf, self._iso(since_open_time), self._iso(now)),
            )
        else:
            rows = self._query(
                "SELECT open_time,open,high,low,close,volume FROM ohlcv_bars "
                "WHERE symbol=? AND timeframe=? AND close_time<=? ORDER BY open_time",
                (symbol, tf, self._iso(now)),
            )
        return self._to_df(rows)

    def load_recent(self, symbol: str, n: int, tf: str = "15m",
                    now: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """最近 n 根已收线 bar，升序。"""
        now = self._now(now)
        rows = self._query(
            "SELECT open_time,open,high,low,close,volume FROM ohlcv_bars "
            "WHERE symbol=? AND timeframe=? AND close_time<=? ORDER BY open_time DESC LIMIT ?",
            (symbol, tf, self._iso(now), n),
        )
        df = self._to_df(rows)
        return df.iloc[::-1].reset_index(drop=True) if not df.empty else df

    # ── 数据新鲜度（§21 #3）──────────────────────────────────────────────────

    def staleness_minutes(self, symbol: str, tf: str = "15m",
                          now: Optional[pd.Timestamp] = None) -> Optional[float]:
        """最新已收线 bar 的 close_time 距 now 的分钟数；None = 无数据。"""
        now = self._now(now)
        rows = self._query(
            "SELECT MAX(close_time) FROM ohlcv_bars "
            "WHERE symbol=? AND timeframe=? AND close_time<=?",
            (symbol, tf, self._iso(now)),
        )
        v = rows[0][0] if rows else None
        if not v:
            return None
        return (now - pd.Timestamp(v)).total_seconds() / 60

    def is_stale(self, symbol: str, tf: str = "15m",
                 now: Optional[pd.Timestamp] = None,
                 max_minutes: Optional[int] = None) -> bool:
        """最新已收线 bar 收线后超过阈值（默认 STALE_DATA_MINUTES）→ stale，停开新仓。"""
        max_minutes = max_minutes if max_minutes is not None else cfg.STALE_DATA_MINUTES
        age = self.staleness_minutes(symbol, tf, now)
        return age is None or age > max_minutes
