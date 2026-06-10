#!/usr/bin/env python3
"""
Incremental data fetch — pull only new bars from btc-ml since local CSV tail.

For each (dataset, symbol, timeframe), reads the last open_time from the local
CSV and queries btc-ml for rows strictly newer than that timestamp.
New rows are appended to the local CSV, then gap continuity is validated.

Gap handling:
  - Gap detected → attempt one re-fetch of the gap range
  - Re-fetch succeeds → continue normally
  - Re-fetch fails  → log warning + return GapError (caller decides to skip cycle)

Retry: up to 3 attempts on SSH failure (15s / 30s / 60s backoff).

Usage (standalone):
    python3 live/fetch_delta.py
    python3 live/fetch_delta.py --datasets ohlcv
"""

from __future__ import annotations

import csv
import io
import logging
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

REMOTE_HOST = "evan@btc-ml"
REMOTE_DB   = "/home/evan/repo/ai_crypto_analyst/data/ai_crypto_analyst.db"
REMOTE_TMP  = "/tmp/history_delta"

SYMBOLS    = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
OHLCV_TFS  = ["15m", "4h"]   # 1h not needed for live
FLOW_TFS   = ["15m"]

LOCAL_DATA = ROOT / "history_data_manager" / "data"

# Expected bar intervals for gap detection
INTERVAL_MINUTES = {"15m": 15, "4h": 240, "1h": 60}

RETRY_DELAYS = [15, 30, 60]  # seconds between retries


# ── Exceptions ────────────────────────────────────────────────────────────────

class FetchError(Exception):
    """SSH or remote export failed after all retries."""

class GapError(Exception):
    """Unfillable gap detected in local CSV after delta append."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _slug(symbol: str) -> str:
    return symbol.replace("/", "").lower()


def _ssh_run(cmd: list[str], check: bool = True, text: bool = True) -> subprocess.CompletedProcess:
    kwargs = {"capture_output": True, "encoding": "utf-8", "errors": "replace"} if text else {"capture_output": True}
    result = subprocess.run(cmd, **kwargs)
    if check and result.returncode != 0:
        err = result.stderr.decode("utf-8", "replace") if isinstance(result.stderr, bytes) else (result.stderr or "unknown error")
        raise RuntimeError(err.strip())
    return result


def _ssh_python(script: str) -> subprocess.CompletedProcess:
    remote_cmd = f"python3 << 'PYEOF'\n{script}\nPYEOF"
    result = subprocess.run(
        ["ssh", REMOTE_HOST, remote_cmd],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or "unknown error").strip())
    return result


def get_last_open_time(csv_path: Path) -> Optional[str]:
    """Return the last open_time value in a CSV as ISO string, or None."""
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if df.empty:
            return None
        return str(df["open_time"].iloc[-1])
    except Exception:
        return None


# ── Remote export ─────────────────────────────────────────────────────────────

def _build_since_map(dataset: str) -> dict[tuple[str, str], Optional[str]]:
    """
    For each (symbol, timeframe), read last open_time from local CSV.
    Returns {(symbol, tf): iso_str_or_None}.
    """
    tfs = OHLCV_TFS if dataset == "ohlcv" else FLOW_TFS
    result = {}
    for sym in SYMBOLS:
        for tf in tfs:
            path = LOCAL_DATA / dataset / f"{_slug(sym)}_{tf}.csv"
            result[(sym, tf)] = get_last_open_time(path)
    return result


def export_delta_remote(dataset: str, since_map: dict[tuple[str, str], Optional[str]]) -> None:
    """
    Run SQLite export on btc-ml for rows strictly newer than per-series since times.
    Files written to REMOTE_TMP/{dataset}/ — one CSV per (symbol, tf).
    """
    if dataset == "ohlcv":
        table = "ohlcv_bars"
        cols  = ("symbol, timeframe, open_time, close_time, "
                 "open, high, low, close, volume, source, is_fallback")
    else:
        table = "taker_flow_bars"
        cols  = ("symbol, timeframe, open_time, close_time, "
                 "taker_buy_base_volume, taker_sell_base_volume, "
                 "taker_buy_quote_volume, taker_sell_quote_volume, "
                 "trade_count, imbalance, source")

    since_repr = repr({f"{s}|{tf}": v for (s, tf), v in since_map.items()})

    script = textwrap.dedent(f"""
        import sqlite3, csv, pathlib

        DB    = "{REMOTE_DB}"
        OUT   = pathlib.Path("{REMOTE_TMP}/{dataset}")
        OUT.mkdir(parents=True, exist_ok=True)

        SINCE_MAP = {since_repr}

        db = sqlite3.connect(DB)
        db.row_factory = sqlite3.Row

        for key, since in SINCE_MAP.items():
            symbol, tf = key.split("|", 1)
            slug = symbol.replace("/", "").lower()
            out_file = OUT / f"{{slug}}_{{tf}}.csv"

            if since is None:
                where = "WHERE symbol=? AND timeframe=? ORDER BY open_time"
                params = (symbol, tf)
            else:
                where = "WHERE symbol=? AND timeframe=? AND open_time>? ORDER BY open_time"
                params = (symbol, tf, since)

            cur = db.execute(
                f"SELECT {cols} FROM {table} {{where}}",
                params,
            )
            rows = cur.fetchall()
            if not rows:
                print(f"  [skip] {{symbol}} {{tf}} — no new rows", flush=True)
                continue
            with open(out_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([d[0] for d in cur.description])
                writer.writerows(rows)
            print(f"  {{symbol}} {{tf}} → {{len(rows)}} new rows", flush=True)

        db.close()
        print("done", flush=True)
    """)

    result = _ssh_python(script)
    logger.debug("remote export output:\n%s", result.stdout.strip())


# ── Transfer + append ─────────────────────────────────────────────────────────

def transfer_and_append(dataset: str) -> dict[Path, int]:
    """
    ssh+tar the remote delta dir, append new rows to local CSVs.
    Returns {csv_path: rows_appended}.
    """
    import tarfile

    remote_dir = f"{REMOTE_TMP}/{dataset}"
    result = _ssh_run(["ssh", REMOTE_HOST, f"tar -czf - -C {remote_dir} . 2>/dev/null || true"], text=False)

    appended: dict[Path, int] = {}

    with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            fname    = Path(member.name).name
            local_csv = LOCAL_DATA / dataset / fname
            if not local_csv.exists():
                logger.warning("local CSV not found, skipping append: %s", local_csv)
                continue

            raw = tar.extractfile(member).read().decode()
            lines = raw.splitlines()
            if len(lines) <= 1:
                continue  # header only, no data

            # Skip header row, append data rows
            data_rows = lines[1:]
            with open(local_csv, "a", newline="") as f:
                for row in data_rows:
                    f.write(row + "\n")

            appended[local_csv] = len(data_rows)
            logger.info("  appended %d rows → %s", len(data_rows), local_csv.name)

    return appended


# ── Gap detection ─────────────────────────────────────────────────────────────

def check_gap(csv_path: Path, tf: str) -> Optional[tuple[str, str]]:
    """
    Check the last ~10 rows of csv_path for continuity.
    Returns (gap_start, gap_end) as ISO strings if a gap is found, else None.
    """
    interval = pd.Timedelta(minutes=INTERVAL_MINUTES.get(tf, 15))
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if len(df) < 2:
            return None
        # Check last 10 rows for efficiency
        tail = pd.to_datetime(df["open_time"].tail(10), utc=True)
        diffs = tail.diff().dropna()
        bad = diffs[diffs > interval * 1.5]
        if bad.empty:
            return None
        # First gap found
        idx = bad.index[0]
        gap_start = tail.iloc[tail.index.get_loc(idx) - 1].isoformat()
        gap_end   = tail.loc[idx].isoformat()
        return gap_start, gap_end
    except Exception as e:
        logger.warning("gap check failed for %s: %s", csv_path.name, e)
        return None


def fill_gap_remote(dataset: str, symbol: str, tf: str,
                    gap_start: str, gap_end: str) -> None:
    """Fetch rows strictly between gap_start and gap_end and append to local CSV."""
    if dataset == "ohlcv":
        table = "ohlcv_bars"
        cols  = ("symbol, timeframe, open_time, close_time, "
                 "open, high, low, close, volume, source, is_fallback")
    else:
        table = "taker_flow_bars"
        cols  = ("symbol, timeframe, open_time, close_time, "
                 "taker_buy_base_volume, taker_sell_base_volume, "
                 "taker_buy_quote_volume, taker_sell_quote_volume, "
                 "trade_count, imbalance, source")

    script = textwrap.dedent(f"""
        import sqlite3, csv, pathlib

        DB = "{REMOTE_DB}"
        db = sqlite3.connect(DB)
        db.row_factory = sqlite3.Row
        cur = db.execute(
            "SELECT {cols} FROM {table} "
            "WHERE symbol=? AND timeframe=? AND open_time>? AND open_time<? "
            "ORDER BY open_time",
            ("{symbol}", "{tf}", "{gap_start}", "{gap_end}"),
        )
        rows = cur.fetchall()
        print(repr([list(r) for r in rows]))
        db.close()
    """)

    result = _ssh_python(script)
    raw_rows = eval(result.stdout.strip())  # list of lists
    if not raw_rows:
        return

    local_csv = LOCAL_DATA / dataset / f"{_slug(symbol)}_{tf}.csv"
    # Read existing to know insertion point
    df = pd.read_csv(local_csv, parse_dates=["open_time"])
    gap_s = pd.Timestamp(gap_start, tz="UTC")
    gap_e = pd.Timestamp(gap_end,   tz="UTC")

    # Build gap dataframe
    header_cur = _ssh_python(
        f"import sqlite3; db=sqlite3.connect('{REMOTE_DB}'); "
        f"cur=db.execute('SELECT {cols} FROM {table} LIMIT 0'); "
        f"print([d[0] for d in cur.description])"
    )
    header = eval(header_cur.stdout.strip())
    gap_df = pd.DataFrame(raw_rows, columns=header)
    gap_df["open_time"] = pd.to_datetime(gap_df["open_time"], utc=True)

    # Merge and sort
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    merged = pd.concat([df, gap_df]).drop_duplicates("open_time").sort_values("open_time")
    merged.to_csv(local_csv, index=False)
    logger.info("gap filled: %d rows inserted into %s", len(gap_df), local_csv.name)


# ── Main fetch_delta entry point ──────────────────────────────────────────────

def fetch_delta(datasets: Optional[list[str]] = None) -> None:
    """
    Pull delta data from btc-ml for all symbols/timeframes.
    Raises FetchError on persistent SSH failure.
    Raises GapError if a gap cannot be filled.
    """
    if datasets is None:
        datasets = ["ohlcv", "taker_flow"]

    for dataset in datasets:
        tfs = OHLCV_TFS if dataset == "ohlcv" else FLOW_TFS
        since_map = _build_since_map(dataset)

        # Log what we're fetching
        for (sym, tf), since in since_map.items():
            logger.debug("  %s %s %s: since=%s", dataset, sym, tf, since or "beginning")

        # Retry loop
        last_exc: Optional[Exception] = None
        for attempt, delay in enumerate([0] + RETRY_DELAYS):
            if delay:
                logger.warning("fetch attempt %d failed, retrying in %ds: %s",
                               attempt, delay, last_exc)
                time.sleep(delay)
            try:
                export_delta_remote(dataset, since_map)
                appended = transfer_and_append(dataset)
                break
            except Exception as e:
                last_exc = e
        else:
            raise FetchError(f"fetch_delta failed after retries: {last_exc}")

        # Gap check for each appended file
        for csv_path, n_rows in appended.items():
            if n_rows == 0:
                continue
            # Determine timeframe from filename (e.g. btcusdt_15m.csv → 15m)
            tf = csv_path.stem.rsplit("_", 1)[-1]
            gap = check_gap(csv_path, tf)
            if gap is None:
                continue

            gap_start, gap_end = gap
            logger.warning("gap detected in %s: %s → %s — attempting fill",
                           csv_path.name, gap_start, gap_end)

            # Identify symbol from filename
            slug_tf = csv_path.stem           # e.g. "btcusdt_15m"
            slug    = slug_tf.rsplit("_", 1)[0]  # e.g. "btcusdt"
            sym = next((s for s in SYMBOLS if _slug(s) == slug), None)
            if sym is None:
                raise GapError(f"gap in {csv_path.name}: unknown symbol slug '{slug}'")

            try:
                fill_gap_remote(dataset, sym, tf, gap_start, gap_end)
                # Re-check after fill
                if check_gap(csv_path, tf) is not None:
                    raise GapError(
                        f"gap in {csv_path.name} ({gap_start} → {gap_end}) "
                        "persists after fill attempt — btc-ml may have missing data"
                    )
            except GapError:
                raise
            except Exception as e:
                raise GapError(
                    f"gap fill failed for {csv_path.name}: {e}"
                ) from e

    logger.info("fetch_delta complete")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets", nargs="+",
        choices=["ohlcv", "taker_flow"],
        default=["ohlcv", "taker_flow"],
    )
    args = parser.parse_args()

    try:
        fetch_delta(args.datasets)
    except FetchError as e:
        logger.error("FETCH FAILED: %s", e)
        sys.exit(1)
    except GapError as e:
        logger.error("GAP ERROR: %s", e)
        sys.exit(2)


if __name__ == "__main__":
    main()
