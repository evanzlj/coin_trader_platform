"""Shared constants and data loading helpers for draw_kline."""
from __future__ import annotations
from pathlib import Path
import pandas as pd

# ── Symbol → slug ─────────────────────────────────────────────────────────────
SLUG_MAP = {
    "BTC/USDT": "btcusdt",
    "ETH/USDT": "ethusdt",
    "BNB/USDT": "bnbusdt",
    "SOL/USDT": "solusdt",
}

# ── Dark theme palette ─────────────────────────────────────────────────────────
BG       = "#0b0e17"
PANEL_BG = "#0d1120"
BULL     = "#26a69a"
BEAR     = "#ef5350"
TEXT     = "#c0c8d8"
GRID     = "#192030"
T0_COL   = "#e0e8f0"
MA_COL   = "#f59e0b"    # MA20: amber
RES_COL  = "#f87171"    # 4H resistance: red
SUP_COL  = "#34d399"    # 4H support: green
QRC_U    = "#d946ef"    # QRC upper: magenta
QRC_M    = "#94a3b8"    # QRC mid: slate
QRC_L    = "#4ade80"    # QRC lower: lime


# ── Data helpers ──────────────────────────────────────────────────────────────

def _tz(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("open_time", "close_time"):
        if col not in df.columns:
            continue
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        else:
            df[col] = df[col].dt.tz_convert("UTC")
    return df


def load_ohlcv(
    data_dir: Path, slug: str, tf: str,
    t_from: pd.Timestamp, t_to: pd.Timestamp,
) -> pd.DataFrame:
    path = data_dir / "ohlcv" / f"{slug}_{tf}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
    df = _tz(df)
    df = df[(df["open_time"] >= t_from) & (df["close_time"] <= t_to)]
    return df.sort_values("open_time").reset_index(drop=True)


def load_flow(
    data_dir: Path, slug: str,
    t_from: pd.Timestamp, t_to: pd.Timestamp,
) -> pd.DataFrame:
    path = data_dir / "taker_flow" / f"{slug}_15m.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
    df = _tz(df)
    df = df[(df["open_time"] >= t_from) & (df["close_time"] <= t_to)]
    return df.sort_values("open_time").reset_index(drop=True)
