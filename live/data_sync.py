#!/usr/bin/env python3
"""
data_sync — 本地 DB → CSV 物化（btc-ml producer，替代 fetch_delta 的跨境 SSH 拉取）。

re-arch Phase 1（见 live/REARCH_PRODUCER_ON_BTCML.md）：monitor 搬到 btc-ml 后，
行情不再跨境拉取，而是直接读本机 ai_crypto_analyst.db，物化成下游（ReplayFeed /
draw_kline）消费的 CSV。CSV 列与原 fetch_delta 输出完全一致，下游零改。

铁律：
  - 只物化 **已收线** bar（close_time <= now，沿用 data_reader 纪律）→ 进行中 15m bar
    永不进入信号/画图链路（修掉旧 fetch_delta 的 in-progress 残值问题，G1/根因）。
  - 缺 DB / dup / reverse / gap → 非零退出 + 抛 SyncError（G4 fail-closed）。
  - readiness gate（G1）：per-symbol ready_horizon = min(ohlcv_15m_tail, flow_15m_tail)；
    monitor 只能 replay 到该水位。4h 是 as-of context（不要求同根）。

用法：
    python3 -m live.data_sync --bootstrap   # 全量 2020→今 物化 + 全量 audit（首跑/新机）
    python3 -m live.data_sync               # 增量（monitor 每轮调 data_sync()）
环境变量：
    OHLCV_DB   源 DB 路径（默认 btc-ml 本地；测试指 temp sqlite）
    DATA_DIR   CSV 输出根（默认 history_data_manager/data；sandbox 可改）
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import exec_config as cfg

logger = logging.getLogger("data_sync")

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = Path(os.environ.get("DATA_DIR", str(ROOT / "history_data_manager" / "data")))

SYMBOLS   = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
OHLCV_TFS = ["15m", "4h"]
FLOW_TFS  = ["15m"]

INTERVAL_MIN = {"15m": 15, "4h": 240}

# CSV column sets — identical to the old fetch_delta output so ReplayFeed/draw_kline are unchanged.
OHLCV_COLS = ["symbol", "timeframe", "open_time", "close_time",
              "open", "high", "low", "close", "volume", "source", "is_fallback"]
FLOW_COLS  = ["symbol", "timeframe", "open_time", "close_time",
              "taker_buy_base_volume", "taker_sell_base_volume",
              "taker_buy_quote_volume", "taker_sell_quote_volume",
              "trade_count", "imbalance", "source"]

BOOTSTRAP_START = "2020-01-01T00:00:00+00:00"


class SyncError(Exception):
    """Data sync failed in a way that must stop the pipeline (fail-closed)."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _slug(symbol: str) -> str:
    return symbol.replace("/", "").lower()


def _table_cols(dataset: str) -> tuple[str, list[str]]:
    return ("ohlcv_bars", OHLCV_COLS) if dataset == "ohlcv" else ("taker_flow_bars", FLOW_COLS)


def _tfs(dataset: str) -> list[str]:
    return OHLCV_TFS if dataset == "ohlcv" else FLOW_TFS


def _now_iso(now: Optional[pd.Timestamp] = None) -> str:
    """now → 'YYYY-MM-DDTHH:MM:SS+00:00'（无微秒，与 DB close_time 同格式，字典序==时间序）。"""
    ts = now if now is not None else pd.Timestamp.now("UTC")
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").replace(microsecond=0).isoformat()


def _connect() -> sqlite3.Connection:
    db = cfg.OHLCV_DB
    if not Path(db).exists():
        raise SyncError(f"source DB not found: {db}")
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10.0)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _csv_path(dataset: str, symbol: str, tf: str) -> Path:
    return DATA_DIR / dataset / f"{_slug(symbol)}_{tf}.csv"


def _local_tail(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    df = pd.read_csv(path, usecols=["open_time"])
    if df.empty:
        return None
    return str(df["open_time"].iloc[-1])


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# ── Query (CLOSED bars only) ──────────────────────────────────────────────────

def _query_closed(conn, dataset: str, symbol: str, tf: str,
                  since: Optional[str], now_iso: str) -> pd.DataFrame:
    """Closed bars (close_time <= now). since=None → full history; else open_time > since."""
    table, cols = _table_cols(dataset)
    sql = (f"SELECT {', '.join(cols)} FROM {table} "
           f"WHERE symbol=? AND timeframe=? AND close_time<=?")
    params: list = [symbol, tf, now_iso]
    if since is not None:
        sql += " AND open_time>?"
        params.append(since)
    sql += " ORDER BY open_time"
    rows = conn.execute(sql, params).fetchall()
    return pd.DataFrame(rows, columns=cols)


def _validate_open_times(series_name: str, t: pd.Series, tf: str) -> None:
    """Check a raw open_time series (no pre-dedup) for dup/reverse/gap. Raises SyncError."""
    if len(t) < 2:
        return
    interval = pd.Timedelta(minutes=INTERVAL_MIN.get(tf, 15))
    d = t.diff().dropna()
    dup = d[d == pd.Timedelta(0)]
    if not dup.empty:
        i = dup.index[0]
        raise SyncError(f"{series_name}: duplicate open_time ({t.iloc[i-1].isoformat()} == {t.iloc[i].isoformat()}) — "
                        "data integrity, refusing to merge")
    rev = d[d < pd.Timedelta(0)]
    if not rev.empty:
        i = rev.index[0]
        raise SyncError(f"{series_name}: reverse open_time ({t.iloc[i-1].isoformat()} > {t.iloc[i].isoformat()}) — "
                        "data integrity, refusing to merge")
    bad = d[d > interval * 1.5]
    if not bad.empty:
        i = bad.index[0]
        logger.warning("gap in %s (%s → %s) — proceeding",
                       series_name, t.iloc[i-1].isoformat(), t.iloc[i].isoformat())


def _merge_csv(path: Path, new_df: pd.DataFrame) -> int:
    """Union existing + new → dedup on open_time (keep last) → sort → atomic write.

    Returns count of genuinely-new open_times added.
    **Must NOT silently fix data corruption**: validate BOTH the existing CSV and the
    DB query result BEFORE merge (P1-1). A dup/reverse in either source causes SyncError
    before any write happens.
    """
    if new_df.empty:
        return 0
    new_ots = pd.to_datetime(new_df["open_time"], utc=True)
    _validate_open_times(f"DB new ({path.name})", new_ots, path.stem.rsplit("_", 1)[-1])

    if path.exists():
        existing = pd.read_csv(path)
        existing_ots = pd.to_datetime(existing["open_time"], utc=True)
        _validate_open_times(f"existing CSV ({path.name})", existing_ots, path.stem.rsplit("_", 1)[-1])
        existing_keys = set(existing_ots)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        existing_keys = set()
        combined = new_df

    added = len(set(new_ots) - existing_keys)
    key = pd.to_datetime(combined["open_time"], utc=True)
    combined = (combined.assign(_k=key).sort_values("_k")
                .drop_duplicates("_k", keep="last").drop(columns="_k").reset_index(drop=True))
    _atomic_write(path, combined.to_csv(index=False))
    return added


# ── Continuity audit (G4 fail-closed) — full-file, post-merge ─────────────────

def audit_series(path: Path, tf: str) -> Optional[tuple[str, str, str]]:
    """Full-series continuity check on a final CSV. Returns (kind, a, b) or None.
    kind ∈ {duplicate, reverse, gap, check_failed}. Fail-closed on its own error."""
    interval = pd.Timedelta(minutes=INTERVAL_MIN.get(tf, 15))
    try:
        t = pd.to_datetime(pd.read_csv(path, usecols=["open_time"])["open_time"], utc=True).reset_index(drop=True)
        if len(t) < 2:
            return None
        d = t.diff().dropna()
        dup = d[d == pd.Timedelta(0)]
        if not dup.empty:
            i = dup.index[0]
            return "duplicate", t.iloc[i - 1].isoformat(), t.iloc[i].isoformat()
        rev = d[d < pd.Timedelta(0)]
        if not rev.empty:
            i = rev.index[0]
            return "reverse", t.iloc[i - 1].isoformat(), t.iloc[i].isoformat()
        bad = d[d > interval * 1.5]
        if not bad.empty:
            i = bad.index[0]
            return "gap", t.iloc[i - 1].isoformat(), t.iloc[i].isoformat()
        return None
    except Exception as e:
        return "check_failed", str(e)[:160], ""


# ── readiness gate (G1) ───────────────────────────────────────────────────────

def _latest_closed_open_time(conn, table: str, symbol: str, tf: str, now_iso: str) -> Optional[pd.Timestamp]:
    row = conn.execute(
        f"SELECT MAX(open_time) FROM {table} WHERE symbol=? AND timeframe=? AND close_time<=?",
        (symbol, tf, now_iso),
    ).fetchone()
    v = row[0] if row else None
    return pd.Timestamp(v) if v else None


def ready_horizon(symbol: str, now: Optional[pd.Timestamp] = None) -> Optional[pd.Timestamp]:
    """per-symbol 可推进水位 = min(latest_closed ohlcv_15m, latest_closed taker_flow_15m)。
    任一路缺 → None（该 symbol 本轮不推进）。monitor 只能 replay 到此 open_time（G1）。"""
    now_iso = _now_iso(now)
    with _connect() as conn:
        o = _latest_closed_open_time(conn, "ohlcv_bars", symbol, "15m", now_iso)
        f = _latest_closed_open_time(conn, "taker_flow_bars", symbol, "15m", now_iso)
    if o is None or f is None:
        return None
    return min(o, f)


def has_4h_context(symbol: str, t0: pd.Timestamp) -> bool:
    """4h as-of T0 context（G1）：存在至少一根 close_time<=T0.close_time 的已收线 4h bar。
    不要求与 T0 同根；缺失 → fail-closed。用候选 T0 参数（不是当前 now）来防止历史重叠 replay
    被后来的 4h bar 误判为有上下文（Codex P1-3）。"""
    t0_iso = t0.tz_convert("UTC").replace(microsecond=0).isoformat()
    with _connect() as conn:
        row = conn.execute(
            "SELECT MAX(close_time) FROM ohlcv_bars WHERE symbol=? AND timeframe='4h' AND close_time<=?",
            (symbol, t0_iso),
        ).fetchone()
    return bool(row and row[0])


# ── Sync entry points ─────────────────────────────────────────────────────────

def _sync(since_is_none: bool, now: Optional[pd.Timestamp] = None) -> dict[Path, int]:
    """Materialize closed bars for all series. bootstrap → since=None; incremental → since=tail.

    P1-2: incremental 路径必须先确认所有 expected CSV 已存在且非空，缺失 → SyncError
    （不允许静默全量物化绕过 bootstrap→audit→warmup 门禁）。bootstrap 自动创建基线。"""
    now_iso = _now_iso(now)
    out: dict[Path, int] = {}

    # Fail fast on missing DB (order matters: check DB before CSV so a missing-db
    # test or scenario gets SyncError about the DB, not about CSV).
    db = cfg.OHLCV_DB
    if not Path(db).exists():
        raise SyncError(f"source DB not found: {db}")

    # Incremental: all expected CSVs must already exist and be non-empty.
    if not since_is_none:
        for dataset in ("ohlcv", "taker_flow"):
            for sym in SYMBOLS:
                for tf in _tfs(dataset):
                    path = _csv_path(dataset, sym, tf)
                    if not path.exists() or path.stat().st_size == 0:
                        raise SyncError(
                            f"expected CSV missing/empty: {path} — run --bootstrap first"
                        )

    with _connect() as conn:
        for dataset in ("ohlcv", "taker_flow"):
            for sym in SYMBOLS:
                for tf in _tfs(dataset):
                    path = _csv_path(dataset, sym, tf)
                    since = None if since_is_none else _local_tail(path)
                    df = _query_closed(conn, dataset, sym, tf, since, now_iso)
                    out[path] = _merge_csv(path, df)
    return out


def data_sync(now: Optional[pd.Timestamp] = None) -> dict[Path, int]:
    """增量物化（monitor 每轮调用）。返回 {csv_path: rows_added}。"""
    appended = _sync(since_is_none=False, now=now)
    for path, n in appended.items():
        if n <= 0:
            continue
        tf = path.stem.rsplit("_", 1)[-1]
        issue = audit_series(path, tf)
        if issue is not None:
            kind, a, b = issue
            raise SyncError(f"{kind} in {path.name} ({a} → {b}) — data integrity, not proceeding")
    return appended


def bootstrap(now: Optional[pd.Timestamp] = None) -> dict[Path, int]:
    """全量物化 BOOTSTRAP_START→今（首跑/新机）+ 全量 audit。任一序列 dup/reverse/gap/check_failed → SyncError。"""
    now_iso = _now_iso(now)
    out: dict[Path, int] = {}
    with _connect() as conn:
        for dataset in ("ohlcv", "taker_flow"):
            for sym in SYMBOLS:
                for tf in _tfs(dataset):
                    path = _csv_path(dataset, sym, tf)
                    table, cols = _table_cols(dataset)
                    sql = (f"SELECT {', '.join(cols)} FROM {table} "
                           f"WHERE symbol=? AND timeframe=? AND open_time>=? AND close_time<=? "
                           f"ORDER BY open_time")
                    rows = conn.execute(sql, (sym, tf, BOOTSTRAP_START, now_iso)).fetchall()
                    df = pd.DataFrame(rows, columns=cols)
                    out[path] = _merge_csv(path, df)
    for path, _ in out.items():
        if not path.exists():
            raise SyncError(f"bootstrap produced no CSV: {path}")
        tf = path.stem.rsplit("_", 1)[-1]
        issue = audit_series(path, tf)
        if issue is not None:
            kind, a, b = issue
            raise SyncError(f"bootstrap audit failed: {kind} in {path.name} ({a} → {b})")
    return out


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bootstrap", action="store_true", help="全量物化 2020→今 + 全量 audit")
    args = ap.parse_args()
    try:
        result = bootstrap() if args.bootstrap else data_sync()
    except SyncError as e:
        logger.error("data_sync FAILED: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("data_sync UNEXPECTED ERROR: %s", e)
        sys.exit(2)
    total = sum(v for v in result.values())
    logger.info("data_sync %s complete: %d new row(s) across %d series",
                "bootstrap" if args.bootstrap else "incremental", total, len(result))


if __name__ == "__main__":
    main()
