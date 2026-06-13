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

import ast
import csv
import io
import logging
import os
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


def _atomic_write(path: Path, data: str) -> None:
    """写临时文件 → os.replace 原子替换（#22 P1：防崩溃半写 CSV）。"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)

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


def _parse_remote_literal(stdout: str):
    """Parse a remote python `print(repr(...))` payload safely (F3).

    Uses ast.literal_eval — only Python literals (lists/tuples/str/num/None) are
    accepted; arbitrary remote stdout can never be *executed* as code.
    """
    return ast.literal_eval(stdout.strip())


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
        import sqlite3, csv, io as _io, os, pathlib, shutil

        DB    = "{REMOTE_DB}"
        OUT   = pathlib.Path("{REMOTE_TMP}/{dataset}")

        # Clear old delta CSVs so stale files are never re-transferred
        if OUT.exists():
            shutil.rmtree(OUT)
        OUT.mkdir(parents=True, exist_ok=True)

        def _atomic_write(path, data):
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(data, encoding="utf-8")
            os.replace(tmp, path)

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
            buf = _io.StringIO()
            import csv as _csv
            w = _csv.writer(buf)
            w.writerow([d[0] for d in cur.description])
            w.writerows(rows)
            _atomic_write(out_file, buf.getvalue())
            print(f"  {{symbol}} {{tf}} → {{len(rows)}} new rows", flush=True)

        db.close()
        print("done", flush=True)
    """)

    result = _ssh_python(script)
    logger.debug("remote export output:\n%s", result.stdout.strip())


# ── Transfer + append ─────────────────────────────────────────────────────────

def _merge_dedup_sort(local_csv: Path, delta_text: str) -> tuple[int, int]:
    """Merge delta CSV text into local_csv: union → dedup on open_time → sort → atomic write.

    Returns (delta_new_count, duplicates_dropped):
      - delta_new_count = number of distinct delta open_times NOT already in the local
        CSV. This is the count of genuinely-new bars — *not* the net row delta. The two
        differ when the merge also cleans pre-existing duplicate rows out of the local
        file: net rows could be 0 while new bars were in fact added. Using the net would
        make the caller skip the continuity check on a file that just grew (F3 follow-up).
      - duplicates_dropped = rows removed by dedup (existing dups + delta repeats).

    Idempotent: replaying the same delta after a partial-success retry adds nothing.
    """
    delta = pd.read_csv(io.StringIO(delta_text))
    if delta.empty:
        return 0, 0

    delta_keys = pd.to_datetime(delta["open_time"], utc=True)
    if local_csv.exists():
        existing = pd.read_csv(local_csv)
        existing_keys = set(pd.to_datetime(existing["open_time"], utc=True))
        combined = pd.concat([existing, delta], ignore_index=True)
    else:
        existing_keys = set()
        combined = delta

    # Genuinely-new bars = distinct delta open_times absent from the existing file.
    distinct_delta = delta_keys.drop_duplicates()
    delta_new_count = int((~distinct_delta.isin(existing_keys)).sum())

    total_rows = len(combined)
    # Dedup keeping the last (freshest) copy, then sort chronologically.
    key = pd.to_datetime(combined["open_time"], utc=True)
    combined = combined.assign(_k=key).sort_values("_k").drop_duplicates(
        "_k", keep="last").drop(columns="_k").reset_index(drop=True)

    duplicates = total_rows - len(combined)
    _atomic_write(local_csv, combined.to_csv(index=False))
    return delta_new_count, duplicates


def transfer_and_append(dataset: str) -> dict[Path, int]:
    """
    ssh+tar the remote delta dir, merge new rows into local CSVs (dedup + sort, atomic).
    Returns {csv_path: rows_added}.
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

            delta_text = tar.extractfile(member).read().decode("utf-8")
            lines = delta_text.splitlines()
            if len(lines) <= 1:
                continue  # header only, no data

            new_bars, duplicates = _merge_dedup_sort(local_csv, delta_text)
            appended[local_csv] = new_bars
            if duplicates:
                # Not silent: a retry re-delivering already-stored rows lands here.
                logger.warning("  %s: dropped %d duplicate open_time row(s) on merge",
                               local_csv.name, duplicates)
            logger.info("  merged %d new bar(s) → %s", new_bars, local_csv.name)

    return appended


# ── Gap detection ─────────────────────────────────────────────────────────────

def check_gap(csv_path: Path, tf: str, n_rows: int = 0) -> Optional[tuple[str, str, str]]:
    """
    Check the tail of csv_path for continuity over at least the freshly-appended range.

    Window = last max(10, n_rows + 10) rows so a gap *inside* a large append block (e.g.
    the first fetch after an outage) is not missed by a fixed tail(10).

    Returns (kind, a, b) where kind is one of:
      - "gap"        : a→b spans more than 1.5× the bar interval (fillable)
      - "duplicate"  : a==b open_time appears twice (data integrity — not fillable)
      - "reverse"    : open_time goes backwards a→b (data integrity — not fillable)
    Returns None if the tail is clean. Never silently passes a corrupt tail.
    """
    interval = pd.Timedelta(minutes=INTERVAL_MINUTES.get(tf, 15))
    try:
        df = pd.read_csv(csv_path, usecols=["open_time"])
        if len(df) < 2:
            return None
        window = max(10, int(n_rows) + 10)
        tail = pd.to_datetime(df["open_time"].tail(window), utc=True).reset_index(drop=True)
        diffs = tail.diff().dropna()

        # Duplicate open_time (diff == 0)
        dup = diffs[diffs == pd.Timedelta(0)]
        if not dup.empty:
            i = dup.index[0]
            return "duplicate", tail.iloc[i - 1].isoformat(), tail.iloc[i].isoformat()

        # Out-of-order / reverse (diff < 0)
        rev = diffs[diffs < pd.Timedelta(0)]
        if not rev.empty:
            i = rev.index[0]
            return "reverse", tail.iloc[i - 1].isoformat(), tail.iloc[i].isoformat()

        # Gap (diff > 1.5× interval)
        bad = diffs[diffs > interval * 1.5]
        if not bad.empty:
            i = bad.index[0]
            return "gap", tail.iloc[i - 1].isoformat(), tail.iloc[i].isoformat()

        return None
    except Exception as e:
        logger.warning("continuity check failed for %s: %s", csv_path.name, e)
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
    raw_rows = _parse_remote_literal(result.stdout)  # list of lists — literal_eval, never exec remote stdout
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
    header = _parse_remote_literal(header_cur.stdout)
    gap_df = pd.DataFrame(raw_rows, columns=header)
    gap_df["open_time"] = pd.to_datetime(gap_df["open_time"], utc=True)

    # Merge and sort
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    merged = pd.concat([df, gap_df]).drop_duplicates("open_time").sort_values("open_time")
    _atomic_write(local_csv, merged.to_csv(index=False))                   # #22 P1 原子写
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

        # Retry loop. since_map is recomputed *inside* each attempt from the current CSV
        # tails (F1): if a previous attempt partially appended before failing, the next
        # attempt's `since` already reflects the new tail, so it never re-fetches rows
        # that already landed. (transfer_and_append also dedups as a second line of
        # defence.)
        last_exc: Optional[Exception] = None
        appended: dict[Path, int] = {}
        for attempt, delay in enumerate([0] + RETRY_DELAYS):
            if delay:
                logger.warning("fetch attempt %d failed, retrying in %ds: %s",
                               attempt, delay, last_exc)
                time.sleep(delay)
            try:
                since_map = _build_since_map(dataset)
                for (sym, tf), since in since_map.items():
                    logger.debug("  %s %s %s: since=%s", dataset, sym, tf, since or "beginning")
                export_delta_remote(dataset, since_map)
                appended = transfer_and_append(dataset)
                break
            except Exception as e:
                last_exc = e
        else:
            raise FetchError(f"fetch_delta failed after retries: {last_exc}")

        # Continuity check for each appended file (covers the appended range, F4)
        for csv_path, n_rows in appended.items():
            if n_rows == 0:
                continue
            # Determine timeframe from filename (e.g. btcusdt_15m.csv → 15m)
            tf = csv_path.stem.rsplit("_", 1)[-1]
            issue = check_gap(csv_path, tf, n_rows=n_rows)
            if issue is None:
                continue

            kind, a, b = issue

            # Duplicate / reverse = data-integrity corruption — filling won't help.
            if kind in ("duplicate", "reverse"):
                raise GapError(
                    f"{kind} open_time in {csv_path.name} ({a} → {b}) — "
                    "data integrity violation, not fetching"
                )

            logger.warning("gap detected in %s: %s → %s — attempting fill",
                           csv_path.name, a, b)

            # Identify symbol from filename
            slug_tf = csv_path.stem           # e.g. "btcusdt_15m"
            slug    = slug_tf.rsplit("_", 1)[0]  # e.g. "btcusdt"
            sym = next((s for s in SYMBOLS if _slug(s) == slug), None)
            if sym is None:
                raise GapError(f"gap in {csv_path.name}: unknown symbol slug '{slug}'")

            try:
                fill_gap_remote(dataset, sym, tf, a, b)
                # Re-check after fill (full tail incl. the filled range)
                if check_gap(csv_path, tf, n_rows=n_rows) is not None:
                    raise GapError(
                        f"gap in {csv_path.name} ({a} → {b}) "
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
