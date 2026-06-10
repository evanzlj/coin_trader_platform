#!/usr/bin/env python3
"""
Pull BTC/ETH/BNB/SOL OHLCV + taker_flow data from evan@btc-ml
and save to ./data/{ohlcv,taker_flow}/<symbol>_<tf>.csv

Date range: 2020-01-01 → 2026-06-09 (inclusive)
"""

import argparse
import subprocess
import sys
import textwrap
from pathlib import Path

REMOTE_HOST = "evan@btc-ml"
REMOTE_DB = "/home/evan/repo/ai_crypto_analyst/data/ai_crypto_analyst.db"
REMOTE_TMP = "/tmp/history_export"

SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
TIMEFRAMES = ["15m", "1h", "4h"]

START = "2020-01-01T00:00:00+00:00"
END = "2026-06-09T00:00:00+00:00"  # exclusive upper bound → captures up to Jun 8 23:xx

LOCAL_DATA = Path(__file__).parent / "data"


def _symbol_slug(symbol: str) -> str:
    return symbol.replace("/", "").lower()


def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, check=check, text=True,
                          capture_output=True, encoding="utf-8", errors="replace")


def ssh_run(script: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a Python heredoc on the remote host."""
    remote_cmd = f"python3 << 'PYEOF'\n{script}\nPYEOF"
    result = subprocess.run(
        ["ssh", REMOTE_HOST, remote_cmd],
        text=True, capture_output=True, encoding="utf-8", errors="replace",
    )
    if check and result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Remote script failed (exit {result.returncode})")
    return result


def export_remote(dataset: str) -> None:
    """Run remote SQLite export for ohlcv or taker_flow."""
    symbols_repr = repr(SYMBOLS)
    tfs_repr = repr(TIMEFRAMES)

    if dataset == "ohlcv":
        table = "ohlcv_bars"
        cols = "symbol, timeframe, open_time, close_time, open, high, low, close, volume, source, is_fallback"
    else:
        table = "taker_flow_bars"
        cols = ("symbol, timeframe, open_time, close_time, "
                "taker_buy_base_volume, taker_sell_base_volume, "
                "taker_buy_quote_volume, taker_sell_quote_volume, "
                "trade_count, imbalance, source")

    script = textwrap.dedent(f"""
        import sqlite3, csv, pathlib, sys

        DB = "{REMOTE_DB}"
        OUT = pathlib.Path("{REMOTE_TMP}/{dataset}")
        OUT.mkdir(parents=True, exist_ok=True)

        SYMBOLS = {symbols_repr}
        TIMEFRAMES = {tfs_repr}
        START = "{START}"
        END = "{END}"

        db = sqlite3.connect(DB)
        db.row_factory = sqlite3.Row

        for symbol in SYMBOLS:
            for tf in TIMEFRAMES:
                slug = symbol.replace("/", "").lower()
                out_file = OUT / f"{{slug}}_{{tf}}.csv"
                cur = db.cursor()
                cur.execute(
                    "SELECT {cols} FROM {table} "
                    "WHERE symbol=? AND timeframe=? AND open_time>=? AND open_time<? "
                    "ORDER BY open_time",
                    (symbol, tf, START, END)
                )
                rows = cur.fetchall()
                if not rows:
                    print(f"  [skip] {{symbol}} {{tf}} — no rows", flush=True)
                    continue
                with open(out_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([d[0] for d in cur.description])
                    writer.writerows(rows)
                print(f"  {{symbol}} {{tf}} → {{len(rows)}} rows → {{out_file.name}}", flush=True)

        db.close()
        print("done", flush=True)
    """)

    print(f"[remote] exporting {dataset} ...")
    result = ssh_run(script)
    print(result.stdout.strip())


def rsync_from_remote(dataset: str) -> None:
    """Transfer remote directory via ssh+tar (no rsync required — works on Windows)."""
    import io
    import tarfile

    local_dir = LOCAL_DATA / dataset
    local_dir.mkdir(parents=True, exist_ok=True)

    remote_dir = f"{REMOTE_TMP}/{dataset}"
    print(f"[transfer] {REMOTE_HOST}:{remote_dir}/ → {local_dir}/")

    result = subprocess.run(
        ["ssh", REMOTE_HOST, f"tar -czf - -C {remote_dir} ."],
        capture_output=True,
        encoding=None,  # binary transfer, don't decode
    )
    if result.returncode != 0:
        print(result.stderr.decode("utf-8", errors="replace"), file=sys.stderr)
        raise RuntimeError("ssh+tar transfer failed")

    with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            fname = Path(member.name).name
            f = tar.extractfile(member)
            (local_dir / fname).write_bytes(f.read())
            print(f"  ↓ {fname}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets", nargs="+",
        choices=["ohlcv", "taker_flow"],
        default=["ohlcv", "taker_flow"],
        help="Which datasets to fetch (default: both)",
    )
    parser.add_argument(
        "--skip-export", action="store_true",
        help="Skip remote export step and only rsync (if remote files already exist)",
    )
    args = parser.parse_args()

    print(f"Target: {SYMBOLS}")
    print(f"Period: {START} → {END}")
    print(f"Datasets: {args.datasets}")
    print()

    for dataset in args.datasets:
        if not args.skip_export:
            export_remote(dataset)
        rsync_from_remote(dataset)
        print()

    print("All done. Data saved to:", LOCAL_DATA)


if __name__ == "__main__":
    main()
