#!/usr/bin/env python3
"""
Binance klines-only collector for SPOT, USD-M futures (UM), and COIN-M futures (CM).

What this does:
  1) Downloads historical SPOT 1m kline CSVs from Binance Vision in bulk.
  2) Downloads historical USD-M FUTURES 1m kline CSVs from Binance Vision in bulk.
  3) Downloads historical COIN-M FUTURES 1m kline CSVs from Binance Vision in bulk.
  4) Pulls last 72 hours of SPOT + UM + CM 1m OHLCV bars with REST.
  5) Runs live SPOT + UM + CM multiplex kline streams, max 200 symbols per stream.
  6) Reconnects live streams automatically; on reconnect it REST backfills missing bars.
  7) RUN_MODE="all" starts three separate processes at the same time: live, REST72, and bulk.
  8) Writes daily .bin files, one symbol/market/day per file, max 1440 unique 1m bars per UTC day.

Storage rule:
  - REST, bulk, and reconnect gap-fill writes dedupe + sort the affected daily .bin file.
  - LIVE stream writes append-only to today's UTC daily .bin file.
  - REST/live data is never written to CSV. Only Binance Vision bulk downloads are kept as CSVs.

Dependencies:
  pip install requests pandas numpy websockets certifi portalocker
"""

from __future__ import annotations

import asyncio
import calendar
import contextlib
import json
import logging
import multiprocessing as mp
import os
import random
import re
import signal
import socket
import ssl
import sys
import tempfile
import time
import traceback
import zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import local
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse

import certifi
import numpy as np
import pandas as pd
import requests
import websockets

try:
    import portalocker
except Exception:  # pragma: no cover
    portalocker = None

# =============================================================================
# CONFIG
# =============================================================================

# -----------------------------------------------------------------------------
# DO NOT USE ARGS. Change these variables here at the top of the file instead.
# -----------------------------------------------------------------------------

BASE_DIR = Path("\get spot and futures klines live\data_dump")
BULK_CSV_DIR = BASE_DIR / "bulk_csv"
DAILY_BIN_DIR = BASE_DIR / "daily_bin"
LOG_DIR = BASE_DIR / "logs"
SYMBOLS_OUTPUT_DIR = BASE_DIR / "symbols_output"
LOG_PATH = LOG_DIR / "klines_collector.log"

# Choose one: "bulk", "rest72", "live", "all", "validate", "symbols"
RUN_MODE = "all"

# Historical bulk lists. These can include old/dead/delisted symbols.
# CM historical symbols may include dated contracts like ADAUSD_200925. Keep those for bulk.
SYMBOLS_SPOT_BULK = ["AAVEUSDT", "BTCUSDT", "ETHUSDT", "LTCUSDT", "SOLUSDT", "XRPUSDT"]
SYMBOLS_FUTURES_UM_BULK = [    'AAVEUSDT']
SYMBOLS_FUTURES_CM_BULK = ["AAVEUSD_PERP", "BTCUSD_PERP"]

# Live + REST 72h lists. These should only be live/trading symbols.
# Do not put expired CM dated contracts here.
SYMBOLS_SPOT_LIVE_REST = ["AAVEUSDT", "BTCUSDT", "ETHUSDT", "LTCUSDT", "SOLUSDT", "XRPUSDT"]
SYMBOLS_FUTURES_UM_LIVE_REST = [    'AAVEUSDT', 'ADAUSDT', 'ALGOUSDT', 'XLMUSDT', 'XRPUSDT']
SYMBOLS_FUTURES_CM_LIVE_REST = ["AAVEUSD_PERP", "BTCUSD_PERP"]

# Auto symbol discovery.
# If a market is True:
#   - historical bulk symbols come from Binance Vision directories, including old/dead symbols
#   - live/rest symbols come from Binance exchangeInfo TRADING symbols
AUTO_FIND_SYMBOLS_SPOT = False
AUTO_FIND_SYMBOLS_UM = False
AUTO_FIND_SYMBOLS_CM = False

# Optional: after exchangeInfo, check that each live/rest symbol returns a recent closed 1m kline.
# This is slower, but avoids stale/broken stream/REST symbols.
AUTO_VALIDATE_LIVE_SYMBOLS_WITH_KLINE = False
AUTO_VALIDATE_MAX_WORKERS = 25
AUTO_VALIDATE_MAX_AGE_MINUTES = 180

INTERVAL = "1m"
INTERVAL_MS = 60_000
REST_LOOKBACK_HOURS = 72
REST_SYMBOL_THREADS = 10              # requested: up to 10 threads, one symbol per worker pool
BULK_DOWNLOAD_THREADS = 40              # bulk CSV downloads use up to 40 threads
BULK_PLAN_THREADS = 10
BULK_DEBUG_LOG_EVERY_CSV_CHECK = False   # True = log every bulk CSV exists/missing decision
BULK_DEBUG_LOG_REMOTE_SUMMARY = False    # True = log remote monthly/daily archive counts per symbol

# RUN_MODE="all" process supervision.
# live starts first, then REST72 and bulk are started in their own processes immediately after.
RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S = 1.0
RUN_ALL_MONITOR_SLEEP_S = 5.0
RUN_ALL_RESTART_LIVE_IF_PROCESS_EXITS = True
MAX_SYMBOLS_PER_STREAM = 200          # requested: up to 200 symbols per multiplex connection
STREAM_RECONNECT_MAX_BACKOFF_S = 120
STREAM_RECV_TIMEOUT_S = 600           # no kline messages for this long => reconnect; ping frames are handled below
WS_OPEN_TIMEOUT_S = 30
WS_CLOSE_TIMEOUT_S = 10
WS_MAX_QUEUE = 8192

# Binance sends websocket PING frames and requires PONG frames.
# The websockets library automatically replies to server PING frames with matching PONG frames.
# Keep client-side automatic pings disabled to avoid extra control messages; liveness is checked by STREAM_RECV_TIMEOUT_S.
WS_CLIENT_PING_INTERVAL_S: Optional[float] = None
WS_CLIENT_PING_TIMEOUT_S: Optional[float] = None

# Optional empty unsolicited PONG keepalive. Futures docs allow this; spot docs say the required PONG
# is the response to the server PING, which websockets handles automatically.
WS_UNSOLICITED_PONG_INTERVAL_S = 300

# Recoverable websocket/network errors such as Windows WinError 121/1236,
# TCP resets, and "no close frame received" are expected on long-running streams.
# Keep tracebacks off by default so logs stay readable; gap-fill still runs after reconnect.
LOG_RECOVERABLE_NETWORK_TRACEBACKS = False
NETWORK_FAILURE_RETRY_MIN_S = 1.0
NETWORK_FAILURE_RETRY_MAX_S = 15.0
NETWORK_FAILURE_BACKOFF_MAX_S = 30.0

# On Windows, the selector event loop is often more stable for long-running
# websocket clients than the default proactor loop.
WINDOWS_USE_SELECTOR_EVENT_LOOP = True

HTTP_TIMEOUT_S = 30
HTTP_RETRIES = 8

# Bulk downloads are larger and data.binance.vision can pause mid-transfer,
# especially with 40 parallel downloads. Keep 40 threads, but give each
# individual download a longer read timeout and more retries.
BULK_DOWNLOAD_CONNECT_TIMEOUT_S = 10
BULK_DOWNLOAD_READ_TIMEOUT_S = 180
BULK_DOWNLOAD_RETRIES = 12
BULK_DOWNLOAD_CHUNK_SIZE = 2 * 1024 * 1024
BULK_DOWNLOAD_BACKOFF_MAX_S = 180.0

# Used only when RUN_MODE = "bulk".
BULK_START_DATE = date(2025, 6, 1)
BULK_END_DATE = date(2099, 12, 31)
BULK_IMPORT_TO_BINS = True
BULK_DELETE_ZIPS_AFTER_EXTRACT = True

# Bulk import skip rule. Before reading/importing a bulk CSV, quickly check the target
# daily .bin files. A complete 1m UTC day is exactly 1440 rows. If every day that
# a CSV would write already has exactly 1440 rows, that CSV is skipped. If any day
# is missing, partial, corrupt, or has duplicate extra rows, only those incomplete
# days are imported from the CSV.
BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS = True
BULK_EXPECTED_ROWS_PER_DAY = 1440
BULK_BIN_COMPLETENESS_WORKERS = 1

# Used only when RUN_MODE = "validate". None means validate today's UTC file.
VALIDATE_DATE: Optional[date] = None

# Binance REST endpoints.
SPOT_REST_KLINES = "https://api.binance.com/api/v3/klines"
UM_REST_KLINES = "https://fapi.binance.com/fapi/v1/klines"
CM_REST_KLINES = "https://dapi.binance.com/dapi/v1/klines"
SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
UM_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
CM_EXCHANGE_INFO = "https://dapi.binance.com/dapi/v1/exchangeInfo"

# Binance websocket endpoints.
# Spot supports both :443 and :9443. Prefer :443 first because it is less likely to be blocked by routers/firewalls.
# Binance also provides data-stream.binance.vision for market-data-only Spot streams.
# USD-M klines are market streams and must use the routed /market path after Binance's websocket migration.
SPOT_WS_BASE = "wss://stream.binance.com:443/stream?streams="
SPOT_WS_BASE_FALLBACKS = [
    "wss://stream.binance.com:443/stream?streams=",
    "wss://stream.binance.com:9443/stream?streams=",
    "wss://data-stream.binance.vision/stream?streams=",
]
UM_WS_BASE = "wss://fstream.binance.com/market/stream?streams="
UM_WS_BASE_FALLBACKS = [
    "wss://fstream.binance.com/market/stream?streams=",
]
CM_WS_BASE = "wss://dstream.binance.com/stream?streams="
CM_WS_BASE_FALLBACKS = [
    "wss://dstream.binance.com/stream?streams=",
]

# DNS failures such as [Errno 11001] getaddrinfo failed are name-resolution failures,
# not Binance API rejections. Retry those quickly and do not let them grow to a 120s backoff.
DNS_FAILURE_RETRY_MIN_S = 5.0
DNS_FAILURE_RETRY_MAX_S = 30.0
DNS_FORCE_IPV4 = True

# Binance Vision S3-compatible listing endpoint.
S3_LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

Market = Literal["spot", "um", "cm"]
WriteMode = Literal["dedupe_sort", "append_live"]


class StreamReconnect(Exception):
    """Expected websocket reconnect trigger, not a fatal error."""


# Force CA bundle from certifi. This fixes many Windows SSL issues.
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
ssl_context = ssl.create_default_context(cafile=certifi.where())

if sys.platform == "win32" and WINDOWS_USE_SELECTOR_EVENT_LOOP:
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# =============================================================================
# LOGGING
# =============================================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)
SYMBOLS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.raiseExceptions = False

logger = logging.getLogger("binance_klines_only")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()

_formatter = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03dZ [%(levelname)s] [%(processName)s/%(threadName)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
_formatter.converter = time.gmtime

_file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_formatter)
logger.addHandler(_console_handler)


def log_exception(prefix: str, exc: BaseException) -> None:
    logger.error("%s: %s", prefix, exc)
    logger.debug("%s traceback:\n%s", prefix, traceback.format_exc())


# =============================================================================
# KLINE SCHEMA
# =============================================================================

FINAL_COLUMNS = [
    "symbol",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
    "taker_base_vol",
    "taker_quote_vol",
    "maker_base_vol",
    "maker_quote_vol",
]

REST_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_base_vol",
    "taker_quote_vol",
    "ignore",
]

KLINE_COLUMN_ALIASES = {
    "quote_asset_volume": "quote_volume",
    "quoteVolume": "quote_volume",
    "num_trades": "trades",
    "numTrades": "trades",
    "taker_buy_base": "taker_base_vol",
    "takerBuyBase": "taker_base_vol",
    "taker_buy_quote": "taker_quote_vol",
    "takerBuyQuote": "taker_quote_vol",
}

# Timestamps are stored as epoch milliseconds.
KLINE_DTYPE = np.dtype(
    [
        ("symbol", "S24"),
        ("open_time", "i8"),
        ("close_time", "i8"),
        ("open", "f8"),
        ("high", "f8"),
        ("low", "f8"),
        ("close", "f8"),
        ("volume", "f8"),
        ("quote_volume", "f8"),
        ("trades", "i8"),
        ("taker_base_vol", "f8"),
        ("taker_quote_vol", "f8"),
        ("maker_base_vol", "f8"),
        ("maker_quote_vol", "f8"),
    ]
)


# =============================================================================
# TIME HELPERS
# =============================================================================


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def floor_minute_ms(ms: int) -> int:
    return ms - (ms % INTERVAL_MS)


def last_closed_minute_open_ms(now_ms: Optional[int] = None) -> int:
    """Open time of the most recently closed 1m candle."""
    if now_ms is None:
        now_ms = utc_now_ms()
    return floor_minute_ms(now_ms) - INTERVAL_MS


def ms_to_utc_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def day_start_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_day(ms: int) -> date:
    return ms_to_utc_dt(ms).date()


def date_range_days(start_date: date, end_date: date) -> Iterable[date]:
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def add_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


# =============================================================================
# MARKET HELPERS
# =============================================================================


def market_label(market: Market) -> str:
    return {"spot": "SPOT", "um": "USD-M", "cm": "COIN-M"}[market]


def market_rest_url(market: Market) -> str:
    if market == "spot":
        return SPOT_REST_KLINES
    if market == "um":
        return UM_REST_KLINES
    if market == "cm":
        return CM_REST_KLINES
    raise ValueError(f"unknown market {market!r}")


def market_exchange_info_url(market: Market) -> str:
    if market == "spot":
        return SPOT_EXCHANGE_INFO
    if market == "um":
        return UM_EXCHANGE_INFO
    if market == "cm":
        return CM_EXCHANGE_INFO
    raise ValueError(f"unknown market {market!r}")


def market_ws_bases(market: Market) -> List[str]:
    if market == "spot":
        return list(SPOT_WS_BASE_FALLBACKS)
    if market == "um":
        return list(UM_WS_BASE_FALLBACKS)
    if market == "cm":
        return list(CM_WS_BASE_FALLBACKS)
    raise ValueError(f"unknown market {market!r}")


def market_ws_base(market: Market) -> str:
    return market_ws_bases(market)[0]


def rest_limit(market: Market) -> int:
    return 1000 if market == "spot" else 1500


def vision_market_path(market: Market) -> str:
    if market == "spot":
        return "spot"
    if market == "um":
        return "futures/um"
    if market == "cm":
        return "futures/cm"
    raise ValueError(f"unknown market {market!r}")


def clean_symbol_list(symbols: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


# =============================================================================
# FILE LOCKING + DAILY BIN PATHS
# =============================================================================


def safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@contextlib.contextmanager
def file_lock(path: Path, timeout_s: float = 30.0):
    """Cross-process lock. Falls back to a no-op lock if portalocker is missing."""
    safe_mkdir(path.parent)
    lock_path = Path(str(path) + ".lock")
    if portalocker is None:
        logger.warning("portalocker not installed; using no-op file lock for %s", path)
        yield
        return

    start = time.time()
    lock = None
    while True:
        try:
            lock = portalocker.Lock(
                str(lock_path),
                mode="a+b",
                flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
                fail_when_locked=True,
            )
            lock.acquire()
            break
        except portalocker.exceptions.LockException:
            if time.time() - start > timeout_s:
                raise TimeoutError(f"Timed out waiting for lock: {lock_path}")
            time.sleep(0.05)

    try:
        yield
    finally:
        try:
            lock.release()
        except Exception:
            pass


def daily_bin_path(market: Market, symbol: str, day: date) -> Path:
    sym = symbol.upper()
    return DAILY_BIN_DIR / market / sym / f"{sym}_{market}_{INTERVAL}_{day:%Y-%m-%d}.bin"


def bulk_csv_path(market: Market, symbol: str, timeperiod: str, d: date) -> Path:
    sym = symbol.upper()
    if timeperiod == "monthly":
        suffix = f"{d:%Y-%m}"
    else:
        suffix = f"{d:%Y-%m-%d}"
    return BULK_CSV_DIR / market / timeperiod / "klines" / sym / INTERVAL / f"{sym}-{INTERVAL}-{suffix}.csv"


def read_bin_file(path: Path) -> np.ndarray:
    if not path.exists() or path.stat().st_size == 0:
        return np.empty(0, dtype=KLINE_DTYPE)
    rows = path.stat().st_size // KLINE_DTYPE.itemsize
    if rows <= 0:
        return np.empty(0, dtype=KLINE_DTYPE)
    arr = np.memmap(path, dtype=KLINE_DTYPE, mode="r", shape=(rows,))
    out = np.array(arr)
    del arr
    return out


def atomic_write_records(path: Path, records: np.ndarray) -> None:
    safe_mkdir(path.parent)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        with open(tmp_path, "wb") as f:
            if records is not None and len(records) > 0:
                records.tofile(f)
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def append_records(path: Path, records: np.ndarray) -> None:
    if records is None or len(records) == 0:
        return
    safe_mkdir(path.parent)
    with open(path, "ab") as f:
        records.tofile(f)


# =============================================================================
# DATA NORMALIZATION
# =============================================================================


def to_epoch_ms_series(series: pd.Series) -> pd.Series:
    """Convert mixed timestamp values to nullable epoch milliseconds."""
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series(pd.NA, index=series.index, dtype="Int64")

    num_mask = numeric.notna()
    if num_mask.any():
        vals = numeric[num_mask].astype("float64")
        converted = pd.Series(np.nan, index=vals.index, dtype="float64")
        converted.loc[vals > 1e14] = np.floor(vals[vals > 1e14] / 1000.0)  # microseconds -> ms
        converted.loc[(vals > 1e11) & (vals <= 1e14)] = vals[(vals > 1e11) & (vals <= 1e14)]  # ms
        converted.loc[(vals > 1e9) & (vals <= 1e11)] = vals[(vals > 1e9) & (vals <= 1e11)] * 1000.0  # s -> ms
        valid = converted.notna()
        out.loc[converted[valid].index] = converted[valid].round().astype("int64")

    str_mask = out.isna()
    if str_mask.any():
        parsed = pd.to_datetime(series[str_mask], utc=True, errors="coerce")
        parsed_valid = parsed.notna()
        if parsed_valid.any():
            out.loc[parsed[parsed_valid].index] = (parsed[parsed_valid].astype("int64") // 1_000_000).astype("int64")

    return out


def normalize_kline_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Normalize REST/Binance Vision/live rows into FINAL_COLUMNS with ms timestamps."""
    if df is None or df.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    df = df.copy()
    for raw, canonical in KLINE_COLUMN_ALIASES.items():
        if raw in df.columns and canonical not in df.columns:
            df.rename(columns={raw: canonical}, inplace=True)
        elif raw in df.columns and canonical in df.columns:
            df.drop(columns=[raw], inplace=True)

    df["symbol"] = symbol.upper()

    for col in ("open_time", "close_time"):
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = to_epoch_ms_series(df[col])

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades",
        "taker_base_vol",
        "taker_quote_vol",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["open_time"].notna()].copy()
    df = df[df["close_time"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    df["open_time"] = df["open_time"].astype("int64")
    df["close_time"] = df["close_time"].astype("int64")

    # Only 1m bars aligned to minute boundaries.
    df = df[(df["open_time"] % INTERVAL_MS) == 0].copy()

    if "maker_base_vol" not in df.columns:
        df["maker_base_vol"] = df["volume"] - df["taker_base_vol"]
    else:
        df["maker_base_vol"] = pd.to_numeric(df["maker_base_vol"], errors="coerce")
        mask = df["maker_base_vol"].isna()
        df.loc[mask, "maker_base_vol"] = df.loc[mask, "volume"] - df.loc[mask, "taker_base_vol"]

    if "maker_quote_vol" not in df.columns:
        df["maker_quote_vol"] = df["quote_volume"] - df["taker_quote_vol"]
    else:
        df["maker_quote_vol"] = pd.to_numeric(df["maker_quote_vol"], errors="coerce")
        mask = df["maker_quote_vol"].isna()
        df.loc[mask, "maker_quote_vol"] = df.loc[mask, "quote_volume"] - df.loc[mask, "taker_quote_vol"]

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    df = df[FINAL_COLUMNS]
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    if df.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    df["trades"] = pd.to_numeric(df["trades"], errors="coerce").fillna(0).astype("int64")
    return df.reset_index(drop=True)


def rest_klines_to_df(symbol: str, rows: Sequence[Sequence[Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=FINAL_COLUMNS)
    df = pd.DataFrame(rows, columns=REST_KLINE_COLUMNS)
    return normalize_kline_df(df, symbol)


def ws_kline_to_df(symbol: str, k: Dict[str, Any]) -> pd.DataFrame:
    row = {
        "symbol": symbol.upper(),
        "open_time": k.get("t"),
        "close_time": k.get("T"),
        "open": k.get("o"),
        "high": k.get("h"),
        "low": k.get("l"),
        "close": k.get("c"),
        "volume": k.get("v"),
        "quote_volume": k.get("q"),
        "trades": k.get("n"),
        "taker_base_vol": k.get("V"),
        "taker_quote_vol": k.get("Q"),
    }
    return normalize_kline_df(pd.DataFrame([row]), symbol)


def bulk_csv_to_df(csv_path: Path, symbol: str) -> pd.DataFrame:
    # Headerless read handles both true headerless CSV and header CSV: a header row becomes NaN and is dropped.
    df = pd.read_csv(csv_path, header=None, names=REST_KLINE_COLUMNS, low_memory=False)
    return normalize_kline_df(df, symbol)


def df_to_records(df: pd.DataFrame) -> np.ndarray:
    if df is None or df.empty:
        return np.empty(0, dtype=KLINE_DTYPE)

    df = df.copy()
    records = np.empty(len(df), dtype=KLINE_DTYPE)
    records["symbol"] = df["symbol"].astype(str).str.upper().str.encode("ascii", "ignore")
    records["open_time"] = df["open_time"].astype("int64").to_numpy()
    records["close_time"] = df["close_time"].astype("int64").to_numpy()
    records["open"] = df["open"].astype("float64").to_numpy()
    records["high"] = df["high"].astype("float64").to_numpy()
    records["low"] = df["low"].astype("float64").to_numpy()
    records["close"] = df["close"].astype("float64").to_numpy()
    records["volume"] = df["volume"].astype("float64").to_numpy()
    records["quote_volume"] = df["quote_volume"].astype("float64").to_numpy()
    records["trades"] = df["trades"].astype("int64").to_numpy()
    records["taker_base_vol"] = df["taker_base_vol"].astype("float64").to_numpy()
    records["taker_quote_vol"] = df["taker_quote_vol"].astype("float64").to_numpy()
    records["maker_base_vol"] = df["maker_base_vol"].astype("float64").to_numpy()
    records["maker_quote_vol"] = df["maker_quote_vol"].astype("float64").to_numpy()
    return records


def records_to_df(records: np.ndarray) -> pd.DataFrame:
    if records is None or len(records) == 0:
        return pd.DataFrame(columns=FINAL_COLUMNS)
    return pd.DataFrame(
        {
            "symbol": np.char.decode(records["symbol"], errors="ignore"),
            "open_time": records["open_time"],
            "close_time": records["close_time"],
            "open": records["open"],
            "high": records["high"],
            "low": records["low"],
            "close": records["close"],
            "volume": records["volume"],
            "quote_volume": records["quote_volume"],
            "trades": records["trades"],
            "taker_base_vol": records["taker_base_vol"],
            "taker_quote_vol": records["taker_quote_vol"],
            "maker_base_vol": records["maker_base_vol"],
            "maker_quote_vol": records["maker_quote_vol"],
        }
    )


# =============================================================================
# DAILY BIN WRITER
# =============================================================================


def clean_one_day_df(df: pd.DataFrame, day: date) -> pd.DataFrame:
    start = day_start_ms(day)
    end = start + 24 * 60 * 60 * 1000
    out = df[(df["open_time"] >= start) & (df["open_time"] < end)].copy()
    out = out.sort_values("open_time")
    out = out.drop_duplicates(subset=["open_time"], keep="last")
    # At 1m resolution there can only be 1440 unique open times in a UTC day.
    if len(out) > 1440:
        logger.warning("%s had %d rows after dedupe; trimming to last 1440", day, len(out))
        out = out.tail(1440)
    return out.reset_index(drop=True)


def write_klines_daily(
    market: Market,
    symbol: str,
    df: pd.DataFrame,
    *,
    mode: WriteMode,
    source: str,
) -> int:
    """
    Write kline rows to per-day bin files.

    mode='dedupe_sort': REST/bulk/gap-fill. Read existing day file, merge, dedupe by open_time, sort, rewrite.
    mode='append_live': live stream. Append rows to the relevant UTC day file without dedupe/sort.
    """
    if df is None or df.empty:
        logger.debug("[%s %s %s] no rows to write", market, symbol, source)
        return 0

    symbol = symbol.upper()
    df = normalize_kline_df(df, symbol)
    if df.empty:
        logger.debug("[%s %s %s] no valid normalized rows", market, symbol, source)
        return 0

    written = 0
    df["_day"] = df["open_time"].apply(lambda x: ms_to_day(int(x)))

    for day, day_df in df.groupby("_day", sort=True):
        day = day if isinstance(day, date) else pd.Timestamp(day).date()
        path = daily_bin_path(market, symbol, day)
        new_records = df_to_records(day_df.drop(columns=["_day"]))

        with file_lock(path):
            if mode == "append_live":
                append_records(path, new_records)
                written += len(new_records)
                logger.debug("[%s %s live] appended %d rows to %s", market, symbol, len(new_records), path)
                continue

            existing = read_bin_file(path)
            combined_records = np.concatenate([existing, new_records]) if len(existing) else new_records
            combined_df = records_to_df(combined_records)
            before = len(combined_df)
            clean_df = clean_one_day_df(combined_df, day)
            after = len(clean_df)
            atomic_write_records(path, df_to_records(clean_df))
            written += len(new_records)
            logger.debug(
                "[%s %s %s] day=%s existing=%d incoming=%d before=%d after=%d deduped=%d path=%s",
                market,
                symbol,
                source,
                day,
                len(existing),
                len(new_records),
                before,
                after,
                before - after,
                path,
            )

    return written


def read_latest_open_time_ms(market: Market, symbol: str, lookback_days: int = 7) -> Optional[int]:
    today = datetime.now(timezone.utc).date()
    latest: Optional[int] = None
    for offset in range(lookback_days):
        d = today - timedelta(days=offset)
        path = daily_bin_path(market, symbol, d)
        if not path.exists():
            continue
        try:
            with file_lock(path):
                arr = read_bin_file(path)
            if len(arr) == 0:
                continue
            mx = int(np.max(arr["open_time"]))
            if latest is None or mx > latest:
                latest = mx
        except Exception as exc:
            log_exception(f"read_latest_open_time_ms {market} {symbol} {path}", exc)
    return latest


# =============================================================================
# HTTP HELPERS
# =============================================================================

_thread_state = local()


def get_session() -> requests.Session:
    sess = getattr(_thread_state, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent": "klines-only-collector/2.0"})
        _thread_state.session = sess
    return sess


def safe_get_json(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: int = HTTP_TIMEOUT_S,
    retries: int = HTTP_RETRIES,
    label: str = "request",
) -> Any:
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            logger.debug("[%s] GET %s params=%s attempt=%d/%d", label, url, params, attempt, retries)
            resp = get_session().get(url, params=params, timeout=timeout_s)
            if resp.status_code == 200:
                return resp.json()

            body = resp.text[:500]
            logger.warning("[%s] HTTP %s body=%s", label, resp.status_code, body)

            if resp.status_code in (418, 429):
                retry_after = resp.headers.get("Retry-After")
                sleep_s = max(float(retry_after), delay) if retry_after else delay + random.random()
                logger.warning("[%s] rate limited; sleeping %.2fs", label, sleep_s)
                time.sleep(sleep_s)
                delay = min(delay * 2, 120.0)
                continue

            if resp.status_code in (500, 502, 503, 504):
                time.sleep(delay + random.random())
                delay = min(delay * 2, 60.0)
                continue

            resp.raise_for_status()

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            logger.warning("[%s] network error attempt %d/%d: %s", label, attempt, retries, exc)
            time.sleep(delay + random.random())
            delay = min(delay * 2, 60.0)
        except Exception as exc:
            log_exception(f"{label} unexpected error attempt {attempt}/{retries}", exc)
            if attempt >= retries:
                raise
            time.sleep(delay + random.random())
            delay = min(delay * 2, 60.0)

    raise RuntimeError(f"{label} failed after {retries} retries")


def safe_get_text(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout_s: int = HTTP_TIMEOUT_S,
    retries: int = HTTP_RETRIES,
    label: str = "request_text",
) -> str:
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            logger.debug("[%s] GET text %s params=%s attempt=%d/%d", label, url, params, attempt, retries)
            resp = get_session().get(url, params=params, timeout=timeout_s)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (418, 429, 500, 502, 503, 504):
                logger.warning("[%s] HTTP %s; retrying", label, resp.status_code)
                time.sleep(delay + random.random())
                delay = min(delay * 2, 60.0)
                continue
            resp.raise_for_status()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            logger.warning("[%s] network error: %s", label, exc)
            time.sleep(delay + random.random())
            delay = min(delay * 2, 60.0)
        except Exception as exc:
            log_exception(f"{label} unexpected error", exc)
            if attempt >= retries:
                raise
            time.sleep(delay + random.random())
            delay = min(delay * 2, 60.0)
    raise RuntimeError(f"{label} failed after {retries} retries")


def safe_download(
    url: str,
    out_path: Path,
    *,
    label: str,
    retries: int = BULK_DOWNLOAD_RETRIES,
    connect_timeout_s: int = BULK_DOWNLOAD_CONNECT_TIMEOUT_S,
    read_timeout_s: int = BULK_DOWNLOAD_READ_TIMEOUT_S,
    chunk_size: int = BULK_DOWNLOAD_CHUNK_SIZE,
) -> bool:
    """
    Download one Binance Vision archive robustly.

    Important:
      - Requests uses timeout=(connect_timeout, read_timeout).
      - A read timeout means no bytes arrived for read_timeout_s seconds. It does
        not mean the whole download exceeded read_timeout_s.
      - Read timeouts are expected sometimes with many parallel downloads, so this
        function retries and logs them as retryable transient errors.
    """
    safe_mkdir(out_path.parent)
    if out_path.exists() and out_path.stat().st_size > 0:
        logger.debug("[%s] exists: %s", label, out_path)
        return True

    delay = 1.0
    part = out_path.with_suffix(out_path.suffix + ".part")
    timeout = (connect_timeout_s, read_timeout_s)

    for attempt in range(1, retries + 1):
        bytes_written = 0
        try:
            logger.debug(
                "[%s] download %s attempt=%d/%d timeout=(connect=%ss, read=%ss)",
                label,
                url,
                attempt,
                retries,
                connect_timeout_s,
                read_timeout_s,
            )
            with get_session().get(url, stream=True, timeout=timeout) as resp:
                if resp.status_code == 404:
                    logger.info("[%s] remote not found: %s", label, url)
                    return False

                if resp.status_code in (418, 429):
                    retry_after = resp.headers.get("Retry-After")
                    sleep_s = max(float(retry_after), delay) if retry_after else delay + random.random()
                    logger.warning(
                        "[%s] rate limited HTTP %d attempt=%d/%d; sleeping %.2fs",
                        label,
                        resp.status_code,
                        attempt,
                        retries,
                        sleep_s,
                    )
                    time.sleep(sleep_s)
                    delay = min(delay * 2, BULK_DOWNLOAD_BACKOFF_MAX_S)
                    continue

                if resp.status_code in (500, 502, 503, 504):
                    sleep_s = delay + random.random()
                    logger.warning(
                        "[%s] server HTTP %d attempt=%d/%d; sleeping %.2fs",
                        label,
                        resp.status_code,
                        attempt,
                        retries,
                        sleep_s,
                    )
                    time.sleep(sleep_s)
                    delay = min(delay * 2, BULK_DOWNLOAD_BACKOFF_MAX_S)
                    continue

                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)

            os.replace(part, out_path)
            logger.debug("[%s] downloaded %s bytes=%d", label, out_path, bytes_written)
            return True

        except requests.exceptions.ReadTimeout as exc:
            sleep_s = delay + random.random()
            logger.warning(
                "[%s] read timeout attempt=%d/%d after no data for %ss bytes_before_timeout=%d; retrying in %.2fs: %s",
                label,
                attempt,
                retries,
                read_timeout_s,
                bytes_written,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
            delay = min(delay * 2, BULK_DOWNLOAD_BACKOFF_MAX_S)

        except requests.exceptions.ConnectTimeout as exc:
            sleep_s = delay + random.random()
            logger.warning(
                "[%s] connect timeout attempt=%d/%d connect_timeout=%ss; retrying in %.2fs: %s",
                label,
                attempt,
                retries,
                connect_timeout_s,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
            delay = min(delay * 2, BULK_DOWNLOAD_BACKOFF_MAX_S)

        except requests.exceptions.ConnectionError as exc:
            sleep_s = delay + random.random()
            logger.warning(
                "[%s] connection error attempt=%d/%d bytes_before_error=%d; retrying in %.2fs: %s",
                label,
                attempt,
                retries,
                bytes_written,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
            delay = min(delay * 2, BULK_DOWNLOAD_BACKOFF_MAX_S)

        except Exception as exc:
            sleep_s = delay + random.random()
            log_exception(f"{label} download unexpected error attempt {attempt}/{retries}", exc)
            time.sleep(sleep_s)
            delay = min(delay * 2, BULK_DOWNLOAD_BACKOFF_MAX_S)

        finally:
            try:
                if part.exists():
                    part.unlink()
            except Exception:
                pass

    logger.error("[%s] failed download after %d retries: %s", label, retries, url)
    return False

# =============================================================================
# BINANCE VISION SYMBOL DISCOVERY + BULK CSV DOWNLOAD + IMPORT
# =============================================================================


@dataclass(frozen=True)
class BulkArchiveJob:
    market: Market
    symbol: str
    timeperiod: Literal["monthly", "daily"]
    d: date
    url: str
    csv_path: Path
    remote_key: str
    local_csv_exists: bool

    @property
    def archive_label(self) -> str:
        if self.timeperiod == "monthly":
            return f"{self.symbol}-{INTERVAL}-{self.d:%Y-%m}"
        return f"{self.symbol}-{INTERVAL}-{self.d:%Y-%m-%d}"

    @property
    def needs_download(self) -> bool:
        return not self.local_csv_exists


@dataclass(frozen=True)
class SymbolSets:
    spot_bulk: List[str]
    um_bulk: List[str]
    cm_bulk: List[str]
    spot_live_rest: List[str]
    um_live_rest: List[str]
    cm_live_rest: List[str]


def s3_list_keys_and_prefixes(prefix: str, *, max_pages: int = 500) -> Tuple[List[str], List[str]]:
    keys: List[str] = []
    prefixes: List[str] = []
    marker: Optional[str] = None

    for page in range(max_pages):
        params = {"delimiter": "/", "prefix": prefix}
        if marker:
            params["marker"] = marker
        xml_text = safe_get_text(S3_LIST_URL, params=params, label=f"S3 {prefix} page={page}")
        root = ET.fromstring(xml_text)

        page_keys: List[str] = []
        for c in root.findall(".//{*}Contents"):
            k = c.findtext("{*}Key")
            if k:
                page_keys.append(k)
        keys.extend(page_keys)

        for cp in root.findall(".//{*}CommonPrefixes"):
            p = cp.findtext("{*}Prefix")
            if p:
                prefixes.append(p)

        is_truncated = (root.findtext(".//{*}IsTruncated") or "").strip().lower() == "true"
        if not is_truncated:
            break
        marker = root.findtext(".//{*}NextMarker") or (page_keys[-1] if page_keys else None)
        if not marker:
            break

    return keys, prefixes


def s3_list_keys(prefix: str, *, max_pages: int = 500) -> List[str]:
    keys, _prefixes = s3_list_keys_and_prefixes(prefix, max_pages=max_pages)
    return keys


def vision_symbol_root_prefix(market: Market, timeperiod: str) -> str:
    return f"data/{vision_market_path(market)}/{timeperiod}/klines/"


def vision_prefix(market: Market, symbol: str, timeperiod: str) -> str:
    sym = symbol.upper()
    return f"data/{vision_market_path(market)}/{timeperiod}/klines/{sym}/{INTERVAL}/"


def vision_base_url(market: Market, timeperiod: str) -> str:
    return f"https://data.binance.vision/data/{vision_market_path(market)}/{timeperiod}/klines"


def discover_vision_symbols(market: Market) -> List[str]:
    """Find symbols from Binance Vision directory names. Keeps old/dead/delisted historical symbols."""
    symbols: set[str] = set()
    for timeperiod in ("daily", "monthly"):
        root_prefix = vision_symbol_root_prefix(market, timeperiod)
        _keys, prefixes = s3_list_keys_and_prefixes(root_prefix)
        for p in prefixes:
            rest = p[len(root_prefix):] if p.startswith(root_prefix) else p
            sym = rest.split("/", 1)[0].strip().upper()
            if sym:
                symbols.add(sym)
    out = sorted(symbols)
    logger.info("[SYMBOLS %s bulk] discovered %d Binance Vision symbols", market, len(out))
    return out


def list_available_archives(market: Market, symbol: str) -> Tuple[set[date], set[date]]:
    """Return remote Binance Vision monthly and daily kline archive dates for one symbol."""
    sym = symbol.upper()
    re_month = re.compile(rf"^{re.escape(sym)}-{re.escape(INTERVAL)}-(\d{{4}})-(\d{{2}})\.zip$", re.I)
    re_day = re.compile(rf"^{re.escape(sym)}-{re.escape(INTERVAL)}-(\d{{4}})-(\d{{2}})-(\d{{2}})\.zip$", re.I)

    monthly_prefix = vision_prefix(market, sym, "monthly")
    daily_prefix = vision_prefix(market, sym, "daily")

    logger.debug("[BULK REMOTE %s %s] listing monthly prefix=%s", market, sym, monthly_prefix)
    monthly_keys = s3_list_keys(monthly_prefix)
    logger.debug("[BULK REMOTE %s %s] listing daily prefix=%s", market, sym, daily_prefix)
    daily_keys = s3_list_keys(daily_prefix)

    months: set[date] = set()
    days: set[date] = set()

    for key in monthly_keys:
        base = os.path.basename(key)
        if base.lower().endswith(".checksum"):
            continue
        m = re_month.match(base)
        if m:
            months.add(date(int(m.group(1)), int(m.group(2)), 1))

    for key in daily_keys:
        base = os.path.basename(key)
        if base.lower().endswith(".checksum"):
            continue
        m = re_day.match(base)
        if m:
            try:
                days.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            except ValueError:
                pass

    if BULK_DEBUG_LOG_REMOTE_SUMMARY:
        first_month = min(months).isoformat() if months else "none"
        last_month = max(months).isoformat() if months else "none"
        first_day = min(days).isoformat() if days else "none"
        last_day = max(days).isoformat() if days else "none"
        logger.info(
            "[BULK REMOTE %s %s] monthly_keys=%d valid_months=%d range=%s..%s | daily_keys=%d valid_days=%d range=%s..%s",
            market,
            sym,
            len(monthly_keys),
            len(months),
            first_month,
            last_month,
            len(daily_keys),
            len(days),
            first_day,
            last_day,
        )
    else:
        logger.debug("[BULK REMOTE %s %s] months=%d days=%d", market, sym, len(months), len(days))

    return months, days


def plan_bulk_jobs_for_symbol(
    market: Market,
    symbol: str,
    start_date: date,
    end_date: date,
) -> List[BulkArchiveJob]:
    """
    Decide exactly which Binance Vision CSV archives are needed for a symbol.

    Planning rule:
      1) Prefer monthly archives where the remote month exists.
      2) For months not covered by monthly archives, use daily archives where remote days exist.
      3) Check the local bulk CSV path. Existing CSVs are NOT downloaded again but can still be imported to bins.
    """
    sym = symbol.upper()
    months_avail, days_avail = list_available_archives(market, sym)
    jobs: List[BulkArchiveJob] = []
    if not months_avail and not days_avail:
        logger.warning("[BULK PLAN %s %s] no remote archives", market, sym)
        return jobs

    wanted_months: List[date] = []
    m = month_start(start_date)
    while m <= month_start(end_date):
        wanted_months.append(m)
        m = add_month(m)

    months_to_use = sorted(set(wanted_months).intersection(months_avail))
    months_covered = {(m.year, m.month) for m in months_to_use}

    days_in_range = list(date_range_days(start_date, end_date))
    remote_daily_in_range = [d for d in days_in_range if d in days_avail]
    daily_to_use = [d for d in remote_daily_in_range if (d.year, d.month) not in months_covered]

    logger.info(
        "[BULK PLAN %s %s] range=%s..%s wanted_months=%d remote_months=%d monthly_used=%d "
        "remote_days_in_range=%d daily_used=%d monthly_covered_months=%d",
        market,
        sym,
        start_date,
        end_date,
        len(wanted_months),
        len(months_avail),
        len(months_to_use),
        len(remote_daily_in_range),
        len(daily_to_use),
        len(months_covered),
    )

    def add_job(timeperiod: Literal["monthly", "daily"], archive_date: date) -> None:
        if timeperiod == "monthly":
            zip_name = f"{sym}-{INTERVAL}-{archive_date:%Y-%m}.zip"
        else:
            zip_name = f"{sym}-{INTERVAL}-{archive_date:%Y-%m-%d}.zip"

        csv_path = bulk_csv_path(market, sym, timeperiod, archive_date)
        local_exists = csv_path.exists() and csv_path.stat().st_size > 0
        remote_key = f"data/{vision_market_path(market)}/{timeperiod}/klines/{sym}/{INTERVAL}/{zip_name}"
        url = f"{vision_base_url(market, timeperiod)}/{sym}/{INTERVAL}/{quote(zip_name)}"

        job = BulkArchiveJob(
            market=market,
            symbol=sym,
            timeperiod=timeperiod,
            d=archive_date,
            url=url,
            csv_path=csv_path,
            remote_key=remote_key,
            local_csv_exists=local_exists,
        )
        jobs.append(job)

        if BULK_DEBUG_LOG_EVERY_CSV_CHECK:
            logger.info(
                "[BULK CHECK %s %s] %s %s remote_key=%s local_csv=%s action=%s",
                market,
                sym,
                timeperiod,
                archive_date.strftime("%Y-%m") if timeperiod == "monthly" else archive_date.isoformat(),
                remote_key,
                csv_path,
                "SKIP_DOWNLOAD_IMPORT_EXISTING_CSV" if local_exists else "DOWNLOAD_WITH_40_THREAD_POOL",
            )
        else:
            logger.debug(
                "[BULK CHECK %s %s] %s local_exists=%s csv=%s",
                market,
                sym,
                job.archive_label,
                local_exists,
                csv_path,
            )

    for mm in months_to_use:
        add_job("monthly", mm)

    for d in daily_to_use:
        add_job("daily", d)

    need_download = sum(1 for j in jobs if j.needs_download)
    have_local = len(jobs) - need_download
    logger.info(
        "[BULK PLAN %s %s] planned_total=%d need_download=%d already_have_csv=%d",
        market,
        sym,
        len(jobs),
        need_download,
        have_local,
    )
    return jobs


def extract_first_csv(zip_path: Path, csv_path: Path) -> bool:
    safe_mkdir(csv_path.parent)
    with zipfile.ZipFile(zip_path, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            return False
        with z.open(names[0]) as src, open(csv_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
    return csv_path.exists() and csv_path.stat().st_size > 0


def download_and_extract_archive(job: BulkArchiveJob, delete_zip: bool = True) -> Optional[Path]:
    """Download one missing Binance Vision zip and extract its CSV."""
    if job.csv_path.exists() and job.csv_path.stat().st_size > 0:
        logger.info(
            "[BULK DOWNLOAD %s %s] local CSV appeared before download; skipping url=%s csv=%s",
            job.market,
            job.symbol,
            job.url,
            job.csv_path,
        )
        return job.csv_path

    safe_mkdir(job.csv_path.parent)
    zip_path = job.csv_path.with_suffix(".zip")
    logger.info(
        "[BULK DOWNLOAD START %s %s] %s url=%s zip=%s csv=%s",
        job.market,
        job.symbol,
        job.archive_label,
        job.url,
        zip_path,
        job.csv_path,
    )
    ok = safe_download(job.url, zip_path, label=f"BULK {job.market} {job.symbol} {job.timeperiod} {job.d}")
    if not ok:
        logger.warning("[BULK DOWNLOAD MISS %s %s] remote missing or failed url=%s", job.market, job.symbol, job.url)
        return None
    try:
        if extract_first_csv(zip_path, job.csv_path):
            size_kb = job.csv_path.stat().st_size / 1024.0
            logger.info(
                "[BULK DOWNLOAD DONE %s %s] extracted_csv=%s size_kb=%.2f",
                job.market,
                job.symbol,
                job.csv_path,
                size_kb,
            )
            return job.csv_path
        logger.warning("[BULK %s %s] zip had no CSV: %s", job.market, job.symbol, zip_path)
        return None
    except zipfile.BadZipFile:
        logger.warning("[BULK %s %s] bad zip: %s", job.market, job.symbol, zip_path)
        return None
    finally:
        if delete_zip:
            try:
                if zip_path.exists():
                    zip_path.unlink()
                    logger.debug("[BULK CLEANUP %s %s] deleted zip=%s", job.market, job.symbol, zip_path)
            except Exception:
                pass




def bulk_job_expected_days(job: BulkArchiveJob, start_date: date, end_date: date) -> List[date]:
    """Return the UTC day files that this bulk archive can write inside the requested range."""
    if job.timeperiod == "daily":
        if start_date <= job.d <= end_date:
            return [job.d]
        return []

    archive_start = job.d
    archive_end = month_end(job.d)
    start = max(start_date, archive_start)
    end = min(end_date, archive_end)
    if start > end:
        return []
    return list(date_range_days(start, end))


def _daily_bin_quick_status_worker(args: Tuple[str, str, str, str, str, int, int]) -> Tuple[str, str, str, int, bool, str, str]:
    """
    Worker used by ProcessPoolExecutor.

    Fast completeness check:
      - no CSV is read
      - no .bin content is loaded
      - only os.stat(file).st_size is used
      - complete means exactly expected_rows records in the day file
    """
    market, symbol, day_iso, daily_bin_root, interval, dtype_itemsize, expected_rows = args
    sym = symbol.upper()
    path = Path(daily_bin_root) / market / sym / f"{sym}_{market}_{interval}_{day_iso}.bin"

    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return market, sym, day_iso, 0, False, str(path), "missing"
    except Exception as exc:
        return market, sym, day_iso, -1, False, str(path), f"stat_error={exc}"

    if size <= 0:
        return market, sym, day_iso, 0, False, str(path), "empty"
    if size % dtype_itemsize != 0:
        return market, sym, day_iso, -1, False, str(path), f"bad_size={size}_not_multiple_of_{dtype_itemsize}"

    rows = size // dtype_itemsize
    if rows == expected_rows:
        return market, sym, day_iso, int(rows), True, str(path), "complete_1440_rows"
    if rows < expected_rows:
        return market, sym, day_iso, int(rows), False, str(path), f"partial_{rows}_rows"
    return market, sym, day_iso, int(rows), False, str(path), f"too_many_{rows}_rows_needs_dedupe"


def check_bulk_daily_bins_complete_parallel(
    jobs: Sequence[BulkArchiveJob],
    start_date: date,
    end_date: date,
) -> Dict[Tuple[str, str, date], Dict[str, Any]]:
    """
    Check all daily .bin files touched by the completed bulk CSV jobs.

    This is intentionally very fast: it uses file size only. For KLINE_DTYPE, a
    correct complete 1m UTC day is exactly 1440 records, so:

        rows = file_size // KLINE_DTYPE.itemsize

    If rows == 1440, the day is considered already imported and the CSV rows for
    that day are skipped. If rows is missing, partial, corrupt, or >1440, that day
    is re-imported/deduped from the bulk CSV.
    """
    unique: Dict[Tuple[str, str, date], Tuple[str, str, str, str, str, int, int]] = {}
    for job in jobs:
        for d in bulk_job_expected_days(job, start_date, end_date):
            key = (job.market, job.symbol.upper(), d)
            if key not in unique:
                unique[key] = (
                    job.market,
                    job.symbol.upper(),
                    d.isoformat(),
                    str(DAILY_BIN_DIR),
                    INTERVAL,
                    KLINE_DTYPE.itemsize,
                    BULK_EXPECTED_ROWS_PER_DAY,
                )

    if not unique:
        return {}

    workers = max(1, min(BULK_BIN_COMPLETENESS_WORKERS, len(unique)))
    logger.info(
        "[BULK BIN PRECHECK] checking %d daily .bin files with multiprocessing workers=%d expected_rows_per_day=%d",
        len(unique),
        workers,
        BULK_EXPECTED_ROWS_PER_DAY,
    )

    values = list(unique.values())
    if workers == 1:
        results = [_daily_bin_quick_status_worker(v) for v in values]
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(_daily_bin_quick_status_worker, values, chunksize=64))

    status: Dict[Tuple[str, str, date], Dict[str, Any]] = {}
    complete = 0
    incomplete = 0
    reasons: Dict[str, int] = {}

    for market, symbol, day_iso, rows, is_complete, path, reason in results:
        d = date.fromisoformat(day_iso)
        status[(market, symbol, d)] = {
            "rows": rows,
            "complete": is_complete,
            "path": path,
            "reason": reason,
        }
        if is_complete:
            complete += 1
        else:
            incomplete += 1
            reasons[reason] = reasons.get(reason, 0) + 1

    logger.info(
        "[BULK BIN PRECHECK] complete=%d incomplete_or_needs_repair=%d reason_counts=%s",
        complete,
        incomplete,
        json.dumps(reasons, sort_keys=True),
    )
    return status


def incomplete_days_for_bulk_job(
    job: BulkArchiveJob,
    status: Dict[Tuple[str, str, date], Dict[str, Any]],
    start_date: date,
    end_date: date,
) -> Tuple[List[date], List[date]]:
    """Return (complete_days, incomplete_days) for one bulk CSV job."""
    complete_days: List[date] = []
    incomplete_days: List[date] = []
    for d in bulk_job_expected_days(job, start_date, end_date):
        st = status.get((job.market, job.symbol.upper(), d))
        if st and st.get("complete") is True:
            complete_days.append(d)
        else:
            incomplete_days.append(d)
    return complete_days, incomplete_days

def import_bulk_csv_to_daily_bins(
    job: BulkArchiveJob,
    start_date: date,
    end_date: date,
    only_days: Optional[Sequence[date]] = None,
) -> int:
    """
    Import a bulk CSV into per-day .bin files.

    If only_days is provided, rows for already-complete daily .bin files are not
    touched at all. This is the important skip behaviour: existing 1440-row days
    stay untouched, and only missing/partial/corrupt/duplicate days are repaired.
    """
    if not job.csv_path.exists() or job.csv_path.stat().st_size == 0:
        logger.warning("[BULK IMPORT %s %s] missing/empty csv=%s", job.market, job.symbol, job.csv_path)
        return 0

    only_days_set = set(only_days or [])
    df = bulk_csv_to_df(job.csv_path, job.symbol)
    original_rows = len(df)

    start_ms = day_start_ms(start_date)
    end_ms = day_start_ms(end_date + timedelta(days=1)) - 1
    df = df[(df["open_time"] >= start_ms) & (df["open_time"] <= end_ms)].copy()

    if only_days_set:
        df["_bulk_import_day"] = df["open_time"].apply(lambda x: ms_to_day(int(x)))
        before_day_filter = len(df)
        df = df[df["_bulk_import_day"].isin(only_days_set)].copy()
        df.drop(columns=["_bulk_import_day"], inplace=True, errors="ignore")
        logger.info(
            "[BULK IMPORT FILTER %s %s] csv=%s only_incomplete_days=%s rows_before_day_filter=%d rows_after_day_filter=%d",
            job.market,
            job.symbol,
            job.csv_path.name,
            ",".join(d.isoformat() for d in sorted(only_days_set)),
            before_day_filter,
            len(df),
        )

    day_count = df["open_time"].apply(lambda x: ms_to_day(int(x))).nunique() if not df.empty else 0
    logger.info(
        "[BULK IMPORT START %s %s] csv=%s original_rows=%d rows_in_requested_range_or_needed_days=%d days_touched=%d output=DAILY_BIN_ONLY",
        job.market,
        job.symbol,
        job.csv_path,
        original_rows,
        len(df),
        day_count,
    )

    if df.empty:
        logger.info(
            "[BULK IMPORT DONE %s %s] csv=%s rows_imported=0 reason=no_rows_after_complete-bin_filter",
            job.market,
            job.symbol,
            job.csv_path.name,
        )
        return 0

    written = write_klines_daily(job.market, job.symbol, df, mode="dedupe_sort", source="bulk_csv")
    logger.info(
        "[BULK IMPORT DONE %s %s] csv=%s rows_imported=%d row_write_attempts=%d bin_root=%s",
        job.market,
        job.symbol,
        job.csv_path.name,
        len(df),
        written,
        DAILY_BIN_DIR / job.market / job.symbol,
    )
    return written


def download_bulk_historical_klines(
    spot_symbols: Sequence[str],
    um_symbols: Sequence[str],
    cm_symbols: Sequence[str],
    start_date: date,
    end_date: date,
    *,
    import_to_bins: bool = True,
    delete_zip: bool = True,
) -> None:
    plan_items: List[Tuple[Market, str]] = []
    plan_items.extend(("spot", s.upper()) for s in clean_symbol_list(spot_symbols))
    plan_items.extend(("um", s.upper()) for s in clean_symbol_list(um_symbols))
    plan_items.extend(("cm", s.upper()) for s in clean_symbol_list(cm_symbols))
    if not plan_items:
        logger.info("[BULK] no symbols")
        return

    logger.info("[BULK] planning %d symbols %s -> %s", len(plan_items), start_date, end_date)
    logger.info(
        "[BULK] data policy: bulk CSVs are kept under %s; imported OHLCV output is daily .bin under %s; REST/live do not write CSV",
        BULK_CSV_DIR,
        DAILY_BIN_DIR,
    )
    logger.info(
        "[BULK] planner_threads=%d download_threads=%d import_to_bins=%s delete_zip_after_extract=%s",
        BULK_PLAN_THREADS,
        BULK_DOWNLOAD_THREADS,
        import_to_bins,
        delete_zip,
    )

    all_jobs: List[BulkArchiveJob] = []
    with ThreadPoolExecutor(max_workers=min(BULK_PLAN_THREADS, len(plan_items)), thread_name_prefix="bulk-plan") as ex:
        futs = {ex.submit(plan_bulk_jobs_for_symbol, m, s, start_date, end_date): (m, s) for m, s in plan_items}
        for fut in as_completed(futs):
            m, s = futs[fut]
            try:
                jobs = fut.result()
                all_jobs.extend(jobs)
                logger.info("[BULK PLAN DONE %s %s] jobs=%d", m, s, len(jobs))
            except Exception as exc:
                log_exception(f"bulk plan failed {m} {s}", exc)

    if not all_jobs:
        logger.info("[BULK] no remote archives found for requested symbols/range")
        return

    existing_csv_jobs = [job for job in all_jobs if not job.needs_download]
    download_jobs = [job for job in all_jobs if job.needs_download]

    by_market: Dict[str, Dict[str, int]] = {}
    for job in all_jobs:
        stats = by_market.setdefault(job.market, {"total": 0, "download": 0, "existing": 0})
        stats["total"] += 1
        if job.needs_download:
            stats["download"] += 1
        else:
            stats["existing"] += 1

    logger.info(
        "[BULK] planned_total_csvs=%d need_download=%d already_have_csv=%d",
        len(all_jobs),
        len(download_jobs),
        len(existing_csv_jobs),
    )
    for market, stats in sorted(by_market.items()):
        logger.info(
            "[BULK] market=%s total_csvs=%d need_download=%d already_have_csv=%d",
            market,
            stats["total"],
            stats["download"],
            stats["existing"],
        )

    completed: List[BulkArchiveJob] = list(existing_csv_jobs)

    if download_jobs:
        workers = min(BULK_DOWNLOAD_THREADS, len(download_jobs))
        logger.info("[BULK DOWNLOAD] starting %d missing CSV downloads with workers=%d", len(download_jobs), workers)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bulk-dl") as ex:
            futs = {ex.submit(download_and_extract_archive, job, delete_zip): job for job in download_jobs}
            for idx, fut in enumerate(as_completed(futs), 1):
                job = futs[fut]
                try:
                    csv_path = fut.result()
                    if csv_path is not None:
                        completed.append(job)
                    logger.info(
                        "[BULK DOWNLOAD PROGRESS] %d/%d market=%s symbol=%s archive=%s result=%s",
                        idx,
                        len(download_jobs),
                        job.market,
                        job.symbol,
                        job.archive_label,
                        "downloaded" if csv_path else "missing_or_failed",
                    )
                except Exception as exc:
                    log_exception(f"bulk download failed {job.market} {job.symbol} {job.d}", exc)
    else:
        logger.info("[BULK DOWNLOAD] nothing to download; all planned bulk CSVs already exist locally")

    if import_to_bins and completed:
        imported = 0

        if BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS:
            status = check_bulk_daily_bins_complete_parallel(completed, start_date, end_date)
            import_plan: List[Tuple[BulkArchiveJob, List[date]]] = []
            skipped_complete = 0
            skipped_complete_days = 0
            incomplete_days_total = 0

            for job in completed:
                complete_days, incomplete_days = incomplete_days_for_bulk_job(job, status, start_date, end_date)
                if not incomplete_days:
                    skipped_complete += 1
                    skipped_complete_days += len(complete_days)
                    logger.debug(
                        "[BULK IMPORT SKIP %s %s] archive=%s all_days_complete_1440 days=%d csv=%s",
                        job.market,
                        job.symbol,
                        job.archive_label,
                        len(complete_days),
                        job.csv_path,
                    )
                else:
                    incomplete_days_total += len(incomplete_days)
                    import_plan.append((job, incomplete_days))
                    logger.info(
                        "[BULK IMPORT NEEDED %s %s] archive=%s incomplete_days=%d complete_days=%d needed_days=%s",
                        job.market,
                        job.symbol,
                        job.archive_label,
                        len(incomplete_days),
                        len(complete_days),
                        ",".join(d.isoformat() for d in incomplete_days[:10]) + ("..." if len(incomplete_days) > 10 else ""),
                    )

            logger.info(
                "[BULK IMPORT PRECHECK] csvs_total=%d csvs_skipped_all_bins_complete=%d skipped_complete_days=%d csvs_to_import=%d incomplete_days_to_repair=%d",
                len(completed),
                skipped_complete,
                skipped_complete_days,
                len(import_plan),
                incomplete_days_total,
            )
        else:
            import_plan = [(job, bulk_job_expected_days(job, start_date, end_date)) for job in completed]
            logger.warning(
                "[BULK IMPORT PRECHECK] disabled; importing all completed CSVs. Set BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS=True to skip complete bins."
            )

        if not import_plan:
            logger.info("[BULK IMPORT] nothing to import; every target daily .bin already has exactly 1440 rows")
            return

        logger.info(
            "[BULK IMPORT] importing %d/%d CSVs; only incomplete daily bins will be touched",
            len(import_plan),
            len(completed),
        )
        for idx, (job, needed_days) in enumerate(import_plan, 1):
            try:
                imported += import_bulk_csv_to_daily_bins(job, start_date, end_date, only_days=needed_days)
                logger.info(
                    "[BULK IMPORT PROGRESS] %d/%d market=%s symbol=%s archive=%s needed_days=%d",
                    idx,
                    len(import_plan),
                    job.market,
                    job.symbol,
                    job.archive_label,
                    len(needed_days),
                )
            except Exception as exc:
                log_exception(f"bulk import failed {job.market} {job.symbol} {job.csv_path}", exc)
        logger.info("[BULK IMPORT] done csvs_imported=%d row_write_attempts=%d", len(import_plan), imported)
    elif not import_to_bins:
        logger.warning("[BULK IMPORT] disabled by BULK_IMPORT_TO_BINS=False; bulk CSVs were not imported to .bin files")


# =============================================================================
# LIVE/REST SYMBOL DISCOVERY
# =============================================================================


def fetch_exchangeinfo_live_symbols(market: Market) -> List[str]:
    payload = safe_get_json(market_exchange_info_url(market), label=f"exchangeInfo {market}")
    rows = payload.get("symbols", []) if isinstance(payload, dict) else []
    out: List[str] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        sym = str(item.get("symbol", "")).upper().strip()
        if not sym:
            continue

        status = str(item.get("status", item.get("contractStatus", ""))).upper()
        contract_status = str(item.get("contractStatus", "")).upper()
        trading = status == "TRADING" or contract_status == "TRADING"
        if not trading:
            continue

        if market == "spot" and item.get("isSpotTradingAllowed") is False:
            continue
        out.append(sym)

    symbols = sorted(set(out))
    logger.info("[SYMBOLS %s live/rest] exchangeInfo trading symbols=%d", market, len(symbols))
    return symbols


def latest_closed_kline_is_recent(market: Market, symbol: str) -> Tuple[str, bool, str]:
    try:
        now_ms = utc_now_ms()
        end_ms = last_closed_minute_open_ms(now_ms)
        start_ms = end_ms - AUTO_VALIDATE_MAX_AGE_MINUTES * INTERVAL_MS
        params = {
            "symbol": symbol.upper(),
            "interval": INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms + INTERVAL_MS - 1,
            "limit": 1,
        }
        data = safe_get_json(market_rest_url(market), params=params, label=f"live-check {market} {symbol}")
        if not data:
            return symbol.upper(), False, "no_rows"
        open_ms = int(data[-1][0])
        age_min = (end_ms - open_ms) // INTERVAL_MS
        if age_min <= AUTO_VALIDATE_MAX_AGE_MINUTES:
            return symbol.upper(), True, f"age_min={age_min}"
        return symbol.upper(), False, f"stale_age_min={age_min}"
    except Exception as exc:
        return symbol.upper(), False, f"error={exc}"


def filter_symbols_with_recent_klines(market: Market, symbols: Sequence[str]) -> List[str]:
    symbols = clean_symbol_list(symbols)
    if not symbols:
        return []
    logger.info("[SYMBOLS %s live-check] checking %d symbols", market, len(symbols))
    live: List[str] = []
    failures: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(
        max_workers=min(AUTO_VALIDATE_MAX_WORKERS, len(symbols)),
        thread_name_prefix=f"live-check-{market}",
    ) as ex:
        futs = [ex.submit(latest_closed_kline_is_recent, market, sym) for sym in symbols]
        for fut in as_completed(futs):
            sym, ok, detail = fut.result()
            if ok:
                live.append(sym)
            else:
                failures.append((sym, detail))

    live = sorted(set(live))
    failures_path = SYMBOLS_OUTPUT_DIR / f"{market}_live_check_failures.tsv"
    with open(failures_path, "w", encoding="utf-8") as f:
        f.write("symbol\treason\n")
        for sym, reason in sorted(failures):
            f.write(f"{sym}\t{reason}\n")
    logger.info("[SYMBOLS %s live-check] live=%d failed=%d failures=%s", market, len(live), len(failures), failures_path)
    return live


def write_symbol_list(name: str, symbols: Sequence[str]) -> None:
    safe_mkdir(SYMBOLS_OUTPUT_DIR)
    path = SYMBOLS_OUTPUT_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        for sym in clean_symbol_list(symbols):
            f.write(sym + "\n")
    logger.info("[SYMBOLS] saved %d -> %s", len(clean_symbol_list(symbols)), path)


def resolve_market_symbols(market: Market, auto_find: bool, bulk_predefined: Sequence[str], live_predefined: Sequence[str]) -> Tuple[List[str], List[str]]:
    if not auto_find:
        bulk_symbols = clean_symbol_list(bulk_predefined)
        live_symbols = clean_symbol_list(live_predefined)
        logger.info(
            "[SYMBOLS %s] using predefined bulk=%d live/rest=%d",
            market,
            len(bulk_symbols),
            len(live_symbols),
        )
        return bulk_symbols, live_symbols

    bulk_symbols = discover_vision_symbols(market)
    live_symbols = fetch_exchangeinfo_live_symbols(market)
    if AUTO_VALIDATE_LIVE_SYMBOLS_WITH_KLINE:
        live_symbols = filter_symbols_with_recent_klines(market, live_symbols)
    return bulk_symbols, live_symbols


def resolve_symbol_sets() -> SymbolSets:
    spot_bulk, spot_live = resolve_market_symbols(
        "spot",
        AUTO_FIND_SYMBOLS_SPOT,
        SYMBOLS_SPOT_BULK,
        SYMBOLS_SPOT_LIVE_REST,
    )
    um_bulk, um_live = resolve_market_symbols(
        "um",
        AUTO_FIND_SYMBOLS_UM,
        SYMBOLS_FUTURES_UM_BULK,
        SYMBOLS_FUTURES_UM_LIVE_REST,
    )
    cm_bulk, cm_live = resolve_market_symbols(
        "cm",
        AUTO_FIND_SYMBOLS_CM,
        SYMBOLS_FUTURES_CM_BULK,
        SYMBOLS_FUTURES_CM_LIVE_REST,
    )

    write_symbol_list("spot_bulk_symbols.txt", spot_bulk)
    write_symbol_list("um_bulk_symbols.txt", um_bulk)
    write_symbol_list("cm_bulk_symbols.txt", cm_bulk)
    write_symbol_list("spot_live_rest_symbols.txt", spot_live)
    write_symbol_list("um_live_rest_symbols.txt", um_live)
    write_symbol_list("cm_live_rest_symbols.txt", cm_live)
    write_symbol_list("combined_futures_bulk_symbols.txt", clean_symbol_list([*um_bulk, *cm_bulk]))
    write_symbol_list("combined_futures_live_rest_symbols.txt", clean_symbol_list([*um_live, *cm_live]))

    return SymbolSets(
        spot_bulk=spot_bulk,
        um_bulk=um_bulk,
        cm_bulk=cm_bulk,
        spot_live_rest=spot_live,
        um_live_rest=um_live,
        cm_live_rest=cm_live,
    )


# =============================================================================
# REST LAST 72 HOURS
# =============================================================================


def fetch_rest_klines_range(
    market: Market,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> pd.DataFrame:
    """Fetch closed 1m klines in [start_ms, end_ms] inclusive by open time."""
    symbol = symbol.upper()
    url = market_rest_url(market)
    limit = rest_limit(market)
    rows: List[List[Any]] = []
    cursor = floor_minute_ms(start_ms)
    end_ms = floor_minute_ms(end_ms)

    while cursor <= end_ms:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": cursor,
            "endTime": end_ms + (INTERVAL_MS - 1),
            "limit": limit,
        }
        data = safe_get_json(url, params=params, label=f"REST {market} {symbol}")
        if not data:
            break

        rows.extend(data)
        last_open = int(data[-1][0])
        next_cursor = last_open + INTERVAL_MS
        logger.debug(
            "[REST %s %s] got=%d total=%d cursor=%s last=%s",
            market,
            symbol,
            len(data),
            len(rows),
            ms_to_utc_dt(cursor).isoformat(),
            ms_to_utc_dt(last_open).isoformat(),
        )
        if len(data) < limit or next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(0.05)

    df = rest_klines_to_df(symbol, rows)
    df = df[(df["open_time"] >= start_ms) & (df["open_time"] <= end_ms)].copy()
    df = df.sort_values("open_time").drop_duplicates("open_time", keep="last")
    return df.reset_index(drop=True)


def update_rest_lookback_for_symbol(market: Market, symbol: str, hours: int = REST_LOOKBACK_HOURS) -> Tuple[str, int]:
    end_ms = last_closed_minute_open_ms()
    start_ms = end_ms - hours * 60 * 60 * 1000 + INTERVAL_MS
    df = fetch_rest_klines_range(market, symbol, start_ms, end_ms)
    written = write_klines_daily(market, symbol, df, mode="dedupe_sort", source=f"rest_{hours}h")
    logger.info("[REST %s %s] fetched=%d wrote=%d", market, symbol, len(df), written)
    return symbol.upper(), len(df)


def update_last_72h_rest_all_symbols(
    spot_symbols: Sequence[str],
    um_symbols: Sequence[str],
    cm_symbols: Sequence[str],
    *,
    max_workers: int = REST_SYMBOL_THREADS,
) -> None:
    jobs: List[Tuple[Market, str]] = []
    jobs.extend(("spot", s.upper()) for s in clean_symbol_list(spot_symbols))
    jobs.extend(("um", s.upper()) for s in clean_symbol_list(um_symbols))
    jobs.extend(("cm", s.upper()) for s in clean_symbol_list(cm_symbols))
    if not jobs:
        logger.info("[REST72] no symbols")
        return

    logger.info("[REST72] starting %d jobs with max_workers=%d", len(jobs), max_workers)
    try:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(jobs)), thread_name_prefix="rest72") as ex:
            futs = {ex.submit(update_rest_lookback_for_symbol, m, s): (m, s) for m, s in jobs}
            for fut in as_completed(futs):
                market, symbol = futs[fut]
                try:
                    _, rows = fut.result()
                    logger.info("[REST72] done %s %s rows=%d", market, symbol, rows)
                except Exception as exc:
                    log_exception(f"REST72 failed {market} {symbol}", exc)
    except KeyboardInterrupt:
        logger.warning("[REST72] KeyboardInterrupt received; stopping cleanly")
        raise


# =============================================================================
# LIVE MULTIPLEX STREAMS + RECONNECT GAP FILL
# =============================================================================


def chunk_symbols(symbols: Sequence[str], size: int = MAX_SYMBOLS_PER_STREAM) -> List[List[str]]:
    clean = clean_symbol_list(symbols)
    return [clean[i : i + size] for i in range(0, len(clean), size)]


def ws_url(market: Market, symbols: Sequence[str], ws_base: Optional[str] = None) -> str:
    streams = "/".join(f"{s.lower()}@kline_{INTERVAL}" for s in symbols)
    return (ws_base or market_ws_base(market)) + streams


def ws_base_host_port(ws_base: str) -> Tuple[str, int]:
    parsed = urlparse(ws_base)
    host = parsed.hostname or ""
    port = parsed.port or 443
    return host, port


def dns_probe_ws_base(ws_base: str) -> str:
    """Resolve a websocket host for logging. Uses IPv4 when DNS_FORCE_IPV4=True."""
    host, port = ws_base_host_port(ws_base)
    if not host:
        return f"invalid_ws_base={ws_base!r}"
    family = socket.AF_INET if DNS_FORCE_IPV4 else 0
    infos = socket.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM)
    addrs = sorted({str(info[4][0]) for info in infos})
    return f"host={host} port={port} addresses={addrs[:8]} count={len(addrs)}"


def is_dns_resolution_error(exc: BaseException) -> bool:
    """True for Windows/Python DNS lookup failures like [Errno 11001] getaddrinfo failed."""
    seen = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        msg = str(cur).lower()
        winerror = getattr(cur, "winerror", None)
        errno = getattr(cur, "errno", None)
        if isinstance(cur, socket.gaierror):
            return True
        if winerror == 11001 or errno in (socket.EAI_NONAME, getattr(socket, "EAI_AGAIN", -999999)):
            return True
        if "getaddrinfo failed" in msg or "name or service not known" in msg or "nodename nor servname" in msg:
            return True
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return False


def is_recoverable_websocket_network_error(exc: BaseException) -> bool:
    """
    True for common recoverable websocket/network drops.

    These are not data errors. They mean the TCP/websocket connection died and
    the stream should reconnect, then REST gap-fill from the latest saved candle.
    """
    seen = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        msg = str(cur).lower()
        winerror = getattr(cur, "winerror", None)
        errno = getattr(cur, "errno", None)

        try:
            if isinstance(cur, websockets.exceptions.ConnectionClosed):
                return True
        except Exception:
            pass

        # Windows socket/network errors commonly seen on long-running websocket streams.
        if winerror in (121, 1236, 10053, 10054, 10060, 10061, 10065, 11001):
            return True

        # Cross-platform socket/network timeout/reset markers.
        if errno in (
            getattr(socket, "ETIMEDOUT", 110),
            getattr(socket, "ECONNRESET", 104),
            getattr(socket, "ECONNABORTED", 103),
            getattr(socket, "EHOSTUNREACH", 113),
            getattr(socket, "ENETUNREACH", 101),
        ):
            return True

        markers = (
            "no close frame received or sent",
            "semaphore timeout period has expired",
            "network connection was aborted",
            "connection was aborted",
            "forcibly closed by the remote host",
            "connection reset",
            "connection timed out",
            "timed out",
            "ssl handshake",
            "did not receive a valid http response",
        )
        if any(m in msg for m in markers):
            return True

        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return False


def format_exception_chain(exc: BaseException) -> str:
    parts: List[str] = []
    seen = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        parts.append(f"{type(cur).__name__}: {cur}")
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return " <- ".join(parts)


def gap_fill_symbol(
    market: Market,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> Tuple[str, int, Optional[int]]:
    if start_ms > end_ms:
        return symbol.upper(), 0, None
    df = fetch_rest_klines_range(market, symbol, start_ms, end_ms)
    written = write_klines_daily(market, symbol, df, mode="dedupe_sort", source="stream_gap_fill")
    latest = int(df["open_time"].max()) if not df.empty else None
    logger.info(
        "[GAP %s %s] %s -> %s fetched=%d wrote=%d",
        market,
        symbol,
        ms_to_utc_dt(start_ms).isoformat(),
        ms_to_utc_dt(end_ms).isoformat(),
        len(df),
        written,
    )
    return symbol.upper(), len(df), latest


def gap_fill_after_reconnect(
    market: Market,
    symbols: Sequence[str],
    gap_since_ms: int,
    last_seen_open_ms: Dict[str, int],
    *,
    max_workers: int = REST_SYMBOL_THREADS,
) -> None:
    """
    Backfill after a websocket reconnect.

    IMPORTANT FIX:
      For every symbol, always check the latest candle already saved on disk
      and backfill from latest_saved_open_time + 1 minute. This avoids the
      bad case where the wall-clock disconnect time is after the latest closed
      candle, which made older versions incorrectly log "no closed candles".

    Priority per symbol:
      1) latest candle saved in that symbol's daily .bin files,
      2) latest closed candle seen in this live process memory,
      3) fallback to the websocket gap_since time if nothing exists yet.

    REST/gap-fill writes use dedupe+sort, so starting from the last saved
    candle + 1 minute is safe and will not corrupt today's file.
    """
    symbols = clean_symbol_list(symbols)
    if not symbols:
        logger.info("[GAP %s] no symbols to check", market)
        return

    now_ms = utc_now_ms()
    end_ms = last_closed_minute_open_ms(now_ms)
    fallback_start_ms = floor_minute_ms(gap_since_ms)

    logger.info(
        "[GAP %s] reconnect backfill check symbols=%d fallback_gap_since=%s latest_closed=%s now=%s",
        market,
        len(symbols),
        ms_to_utc_dt(fallback_start_ms).isoformat(),
        ms_to_utc_dt(end_ms).isoformat(),
        ms_to_utc_dt(now_ms).isoformat(),
    )

    jobs: List[Tuple[str, int, int]] = []

    for sym in symbols:
        mem_latest = last_seen_open_ms.get(sym)

        try:
            disk_latest = read_latest_open_time_ms(market, sym)
        except Exception as exc:
            disk_latest = None
            log_exception(f"gap read_latest failed {market} {sym}", exc)

        latest_candidates = [x for x in (disk_latest, mem_latest) if x is not None]

        if latest_candidates:
            latest_known = int(max(latest_candidates))
            start_ms = latest_known + INTERVAL_MS
            source = (
                f"latest_saved_or_seen={ms_to_utc_dt(latest_known).isoformat()} "
                f"disk={ms_to_utc_dt(int(disk_latest)).isoformat() if disk_latest is not None else 'none'} "
                f"mem={ms_to_utc_dt(int(mem_latest)).isoformat() if mem_latest is not None else 'none'}"
            )
        else:
            latest_known = None
            start_ms = fallback_start_ms
            source = "no_saved_or_seen_candle_using_gap_since"

        start_ms = floor_minute_ms(start_ms)

        if start_ms <= end_ms:
            jobs.append((sym, start_ms, end_ms))
            logger.info(
                "[GAP CHECK %s %s] QUEUE source=%s start=%s end=%s minutes=%d",
                market,
                sym,
                source,
                ms_to_utc_dt(start_ms).isoformat(),
                ms_to_utc_dt(end_ms).isoformat(),
                int((end_ms - start_ms) // INTERVAL_MS) + 1,
            )
        else:
            logger.info(
                "[GAP CHECK %s %s] SKIP source=%s start=%s end=%s reason=already_up_to_date_or_no_closed_candle_yet",
                market,
                sym,
                source,
                ms_to_utc_dt(start_ms).isoformat(),
                ms_to_utc_dt(end_ms).isoformat(),
            )

    if not jobs:
        logger.info("[GAP %s] no symbol gaps after checking latest saved candle for every symbol", market)
        return

    logger.info("[GAP %s] REST backfilling %d symbols with max_workers=%d", market, len(jobs), max_workers)
    with ThreadPoolExecutor(max_workers=min(max_workers, len(jobs)), thread_name_prefix=f"gap-{market}") as ex:
        futs = {ex.submit(gap_fill_symbol, market, sym, st, en): sym for sym, st, en in jobs}
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                symbol, rows, latest = fut.result()
                if latest is not None:
                    last_seen_open_ms[symbol] = max(last_seen_open_ms.get(symbol, 0), latest)
                logger.info("[GAP %s %s] complete rows=%d latest=%s", market, symbol, rows, ms_to_utc_dt(latest).isoformat() if latest is not None else "none")
            except Exception as exc:
                log_exception(f"gap fill failed {market} {sym}", exc)


async def websocket_unsolicited_pong_loop(ws: Any, label: str, stop_event: asyncio.Event) -> None:
    """
    Optional keepalive: send an empty PONG frame periodically.
    Binance futures docs allow unsolicited PONG frames; required PONG replies to
    server PING frames are still handled automatically by websockets.
    """
    if not WS_UNSOLICITED_PONG_INTERVAL_S or WS_UNSOLICITED_PONG_INTERVAL_S <= 0:
        return
    try:
        while not stop_event.is_set():
            await asyncio.sleep(float(WS_UNSOLICITED_PONG_INTERVAL_S))
            if stop_event.is_set():
                return
            try:
                await ws.pong(b"")
                logger.debug("[%s] sent empty unsolicited PONG keepalive", label)
            except Exception as exc:
                logger.debug("[%s] unsolicited PONG failed: %s", label, exc)
                return
    except asyncio.CancelledError:
        raise


async def live_multiplex_loop(
    market: Market,
    symbols: Sequence[str],
    stop_event: asyncio.Event,
) -> None:
    symbols = clean_symbol_list(symbols)
    if not symbols:
        return

    streams = [f"{s.lower()}@kline_{INTERVAL}" for s in symbols]
    label = f"LIVE {market} {symbols[0]}..{symbols[-1]} n={len(symbols)}"
    ws_bases = market_ws_bases(market)
    ws_base_index = 0

    last_seen_open_ms: Dict[str, int] = {}
    backoff = 1.0
    first_connect = True
    pending_gap_since_ms: Optional[int] = None

    logger.info(
        "[%s] stream setup bases=%s streams=%d first_streams=%s dns_force_ipv4=%s",
        label,
        ws_bases,
        len(streams),
        streams[:10],
        DNS_FORCE_IPV4,
    )

    while not stop_event.is_set():
        connect_attempt_ms = utc_now_ms()
        connection_open_ms: Optional[int] = None
        last_ws_message_ms: Optional[int] = None
        pong_task: Optional[asyncio.Task] = None

        try:
            current_ws_base = ws_bases[ws_base_index % len(ws_bases)]
            url = ws_url(market, symbols, current_ws_base)
            logger.info("[%s] connecting url_base=%s", label, current_ws_base)

            # Important:
            # - Binance sends server PING frames; websockets automatically sends matching PONG frames.
            # - USD-M klines now require the /market routed endpoint.
            # - Client-side pings are disabled by default to avoid extra control messages.
            # - family=AF_INET avoids some Windows/IPv6 resolver/socket edge cases.
            connect_kwargs: Dict[str, Any] = dict(
                ssl=ssl_context,
                ping_interval=WS_CLIENT_PING_INTERVAL_S,
                ping_timeout=WS_CLIENT_PING_TIMEOUT_S,
                open_timeout=WS_OPEN_TIMEOUT_S,
                close_timeout=WS_CLOSE_TIMEOUT_S,
                max_queue=WS_MAX_QUEUE,
                compression=None,
            )
            if DNS_FORCE_IPV4:
                connect_kwargs["family"] = socket.AF_INET

            async with websockets.connect(url, **connect_kwargs) as ws:
                connection_open_ms = utc_now_ms()
                last_ws_message_ms = connection_open_ms
                logger.info("[%s] connected at=%s", label, ms_to_utc_dt(connection_open_ms).isoformat())

                pong_task = asyncio.create_task(websocket_unsolicited_pong_loop(ws, label, stop_event))

                if not first_connect:
                    reconnect_gap_ms = pending_gap_since_ms or connection_open_ms
                    logger.info(
                        "[%s] reconnected; running REST gap-fill from latest saved candle per symbol fallback_gap_since=%s",
                        label,
                        ms_to_utc_dt(reconnect_gap_ms).isoformat(),
                    )
                    await asyncio.to_thread(
                        gap_fill_after_reconnect,
                        market,
                        symbols,
                        reconnect_gap_ms,
                        last_seen_open_ms,
                    )
                    pending_gap_since_ms = None

                first_connect = False
                backoff = 1.0

                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=STREAM_RECV_TIMEOUT_S)
                    except asyncio.TimeoutError as exc:
                        gap_from = last_ws_message_ms or connection_open_ms or connect_attempt_ms
                        pending_gap_since_ms = pending_gap_since_ms or gap_from
                        raise StreamReconnect(
                            f"recv timeout {STREAM_RECV_TIMEOUT_S}s; "
                            f"last_ws_message={ms_to_utc_dt(gap_from).isoformat()}"
                        ) from exc

                    last_ws_message_ms = utc_now_ms()

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.debug("[%s] non-json websocket payload: %r", label, raw[:200])
                        continue

                    stream_name = str(msg.get("stream", ""))
                    data = msg.get("data", msg)
                    if not isinstance(data, dict):
                        logger.debug("[%s] non-dict websocket data stream=%s msg=%s", label, stream_name, msg)
                        continue

                    if data.get("e") == "serverShutdown" or stream_name == "!serverShutdown":
                        pending_gap_since_ms = last_ws_message_ms or utc_now_ms()
                        raise StreamReconnect("serverShutdown event")

                    k = data.get("k")
                    if not k:
                        # Combined stream wrappers and other control-ish payloads can land here.
                        logger.debug("[%s] websocket payload without kline stream=%s data_keys=%s", label, stream_name, list(data.keys()))
                        continue

                    if not k.get("x", False):
                        # Only write final/closed 1m bars.
                        continue

                    symbol = str(k.get("s") or data.get("s") or "").upper()
                    if not symbol:
                        logger.debug("[%s] kline payload without symbol: %s", label, k)
                        continue

                    if symbol not in symbols:
                        logger.warning("[%s] received unexpected symbol=%s stream=%s", label, symbol, stream_name)

                    df = ws_kline_to_df(symbol, k)
                    if df.empty:
                        logger.debug("[%s] empty normalized websocket kline: %s", label, k)
                        continue

                    write_klines_daily(market, symbol, df, mode="append_live", source="live")
                    open_ms = int(df["open_time"].iloc[-1])
                    last_seen_open_ms[symbol] = max(last_seen_open_ms.get(symbol, 0), open_ms)
                    logger.debug("[%s] appended %s %s", label, symbol, ms_to_utc_dt(open_ms).isoformat())

        except asyncio.CancelledError:
            raise
        except StreamReconnect as exc:
            if pending_gap_since_ms is None:
                pending_gap_since_ms = last_ws_message_ms or connection_open_ms or connect_attempt_ms
            sleep_s = backoff + random.random()
            logger.warning(
                "[%s] reconnect trigger: %s; gap_from=%s; reconnect_sleep=%.2fs; gap-fill will run after reconnect",
                label,
                exc,
                ms_to_utc_dt(pending_gap_since_ms).isoformat(),
                sleep_s,
            )
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2, STREAM_RECONNECT_MAX_BACKOFF_S)
        except Exception as exc:
            if pending_gap_since_ms is None:
                pending_gap_since_ms = last_ws_message_ms or connection_open_ms or connect_attempt_ms

            if is_dns_resolution_error(exc):
                current_ws_base = ws_bases[ws_base_index % len(ws_bases)]
                try:
                    dns_status = await asyncio.to_thread(dns_probe_ws_base, current_ws_base)
                except Exception as dns_exc:
                    dns_status = f"dns_probe_failed={format_exception_chain(dns_exc)}"

                if len(ws_bases) > 1:
                    old_base = current_ws_base
                    ws_base_index = (ws_base_index + 1) % len(ws_bases)
                    next_base = ws_bases[ws_base_index % len(ws_bases)]
                    fallback_msg = f" rotating_ws_base {old_base} -> {next_base}"
                else:
                    fallback_msg = " no_alternate_ws_base"

                sleep_s = random.uniform(DNS_FAILURE_RETRY_MIN_S, DNS_FAILURE_RETRY_MAX_S)
                logger.warning(
                    "[%s] DNS resolution failed; gap_from=%s; retry_sleep=%.2fs;%s; dns_status=%s; error_chain=%s",
                    label,
                    ms_to_utc_dt(pending_gap_since_ms).isoformat(),
                    sleep_s,
                    fallback_msg,
                    dns_status,
                    format_exception_chain(exc),
                )
                # DNS errors are usually transient/local resolver failures. Do not let them climb to 120s.
                backoff = 1.0
                await asyncio.sleep(sleep_s)
                continue

            if is_recoverable_websocket_network_error(exc):
                sleep_upper = min(max(backoff + random.random(), NETWORK_FAILURE_RETRY_MIN_S), NETWORK_FAILURE_RETRY_MAX_S)
                sleep_s = random.uniform(NETWORK_FAILURE_RETRY_MIN_S, sleep_upper)
                logger.warning(
                    "[%s] recoverable websocket/network drop; gap_from=%s; reconnect_sleep=%.2fs; error_chain=%s",
                    label,
                    ms_to_utc_dt(pending_gap_since_ms).isoformat(),
                    sleep_s,
                    format_exception_chain(exc),
                )
                if LOG_RECOVERABLE_NETWORK_TRACEBACKS:
                    logger.debug("[%s] recoverable websocket/network traceback:\n%s", label, traceback.format_exc())
                await asyncio.sleep(sleep_s)
                backoff = min(backoff * 2, NETWORK_FAILURE_BACKOFF_MAX_S)
                continue

            sleep_s = backoff + random.random()
            log_exception(f"{label} disconnected; gap_from={ms_to_utc_dt(pending_gap_since_ms).isoformat()}; reconnect_sleep={sleep_s:.2f}s", exc)
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2, STREAM_RECONNECT_MAX_BACKOFF_S)
        finally:
            if pong_task is not None:
                pong_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await pong_task


async def run_live_streams_async(
    spot_symbols: Sequence[str],
    um_symbols: Sequence[str],
    cm_symbols: Sequence[str],
) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_stop() -> None:
        logger.info("[LIVE] stop signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _signal_stop())

    spot_chunks = chunk_symbols(spot_symbols)
    um_chunks = chunk_symbols(um_symbols)
    cm_chunks = chunk_symbols(cm_symbols)

    logger.info(
        "[LIVE] chunk summary spot_symbols=%d spot_connections=%d | um_symbols=%d um_connections=%d | cm_symbols=%d cm_connections=%d | max_per_stream=%d",
        len(clean_symbol_list(spot_symbols)),
        len(spot_chunks),
        len(clean_symbol_list(um_symbols)),
        len(um_chunks),
        len(clean_symbol_list(cm_symbols)),
        len(cm_chunks),
        MAX_SYMBOLS_PER_STREAM,
    )
    logger.info("[LIVE] websocket bases spot=%s um=%s cm=%s", SPOT_WS_BASE_FALLBACKS, UM_WS_BASE_FALLBACKS, CM_WS_BASE_FALLBACKS)

    tasks: List[asyncio.Task] = []
    for idx, chunk in enumerate(spot_chunks, 1):
        logger.info("[LIVE] starting spot stream chunk=%d/%d symbols=%s", idx, len(spot_chunks), chunk[:20])
        tasks.append(asyncio.create_task(live_multiplex_loop("spot", chunk, stop_event)))
    for idx, chunk in enumerate(um_chunks, 1):
        logger.info("[LIVE] starting um stream chunk=%d/%d symbols=%s", idx, len(um_chunks), chunk[:20])
        tasks.append(asyncio.create_task(live_multiplex_loop("um", chunk, stop_event)))
    for idx, chunk in enumerate(cm_chunks, 1):
        logger.info("[LIVE] starting cm stream chunk=%d/%d symbols=%s", idx, len(cm_chunks), chunk[:20])
        tasks.append(asyncio.create_task(live_multiplex_loop("cm", chunk, stop_event)))

    if not tasks:
        logger.info("[LIVE] no stream tasks")
        return

    logger.info("[LIVE] started %d multiplex connections", len(tasks))
    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("[LIVE] stopped")


def start_live_streams(spot_symbols: Sequence[str], um_symbols: Sequence[str], cm_symbols: Sequence[str]) -> None:
    asyncio.run(run_live_streams_async(spot_symbols, um_symbols, cm_symbols))



# =============================================================================
# RUN_MODE="all": THREE PROCESS STARTER
# =============================================================================


def _process_entry_live(spot_symbols: Sequence[str], um_symbols: Sequence[str], cm_symbols: Sequence[str]) -> None:
    logger.info(
        "[PROCESS LIVE] START pid=%s spot=%d um=%d cm=%d",
        os.getpid(),
        len(clean_symbol_list(spot_symbols)),
        len(clean_symbol_list(um_symbols)),
        len(clean_symbol_list(cm_symbols)),
    )
    try:
        start_live_streams(spot_symbols, um_symbols, cm_symbols)
    except KeyboardInterrupt:
        logger.warning("[PROCESS LIVE] KeyboardInterrupt; exiting")
    except Exception as exc:
        log_exception("[PROCESS LIVE] crashed", exc)
        raise
    finally:
        logger.info("[PROCESS LIVE] EXIT pid=%s", os.getpid())


def _process_entry_rest72(spot_symbols: Sequence[str], um_symbols: Sequence[str], cm_symbols: Sequence[str]) -> None:
    logger.info(
        "[PROCESS REST72] START pid=%s spot=%d um=%d cm=%d max_symbol_threads=%d",
        os.getpid(),
        len(clean_symbol_list(spot_symbols)),
        len(clean_symbol_list(um_symbols)),
        len(clean_symbol_list(cm_symbols)),
        REST_SYMBOL_THREADS,
    )
    try:
        update_last_72h_rest_all_symbols(spot_symbols, um_symbols, cm_symbols)
    except KeyboardInterrupt:
        logger.warning("[PROCESS REST72] KeyboardInterrupt; exiting")
    except Exception as exc:
        log_exception("[PROCESS REST72] crashed", exc)
        raise
    finally:
        logger.info("[PROCESS REST72] EXIT pid=%s", os.getpid())


def _process_entry_bulk(spot_symbols: Sequence[str], um_symbols: Sequence[str], cm_symbols: Sequence[str]) -> None:
    logger.info(
        "[PROCESS BULK] START pid=%s spot=%d um=%d cm=%d start=%s end=%s download_threads=%d import_to_bins=%s",
        os.getpid(),
        len(clean_symbol_list(spot_symbols)),
        len(clean_symbol_list(um_symbols)),
        len(clean_symbol_list(cm_symbols)),
        BULK_START_DATE,
        BULK_END_DATE,
        BULK_DOWNLOAD_THREADS,
        BULK_IMPORT_TO_BINS,
    )
    try:
        download_bulk_historical_klines(
            spot_symbols,
            um_symbols,
            cm_symbols,
            BULK_START_DATE,
            BULK_END_DATE,
            import_to_bins=BULK_IMPORT_TO_BINS,
            delete_zip=BULK_DELETE_ZIPS_AFTER_EXTRACT,
        )
    except KeyboardInterrupt:
        logger.warning("[PROCESS BULK] KeyboardInterrupt; exiting")
    except Exception as exc:
        log_exception("[PROCESS BULK] crashed", exc)
        raise
    finally:
        logger.info("[PROCESS BULK] EXIT pid=%s", os.getpid())


def _make_live_process(symbols: SymbolSets) -> mp.Process:
    return mp.Process(
        target=_process_entry_live,
        args=(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest),
        name="PROC-LIVE",
        daemon=False,
    )


def _make_rest_process(symbols: SymbolSets) -> mp.Process:
    return mp.Process(
        target=_process_entry_rest72,
        args=(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest),
        name="PROC-REST72",
        daemon=False,
    )


def _make_bulk_process(symbols: SymbolSets) -> mp.Process:
    return mp.Process(
        target=_process_entry_bulk,
        args=(symbols.spot_bulk, symbols.um_bulk, symbols.cm_bulk),
        name="PROC-BULK",
        daemon=False,
    )


def run_all_three_processes(symbols: SymbolSets) -> None:
    """Start live, REST72, and bulk concurrently in three separate processes."""
    logger.info(
        "[ALL] starting THREE processes: live first, then REST72 and BULK %.1fs later",
        RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S,
    )

    live_proc = _make_live_process(symbols)
    rest_proc = _make_rest_process(symbols)
    bulk_proc = _make_bulk_process(symbols)

    live_proc.start()
    logger.info("[ALL] started live process pid=%s", live_proc.pid)

    time.sleep(max(0.0, float(RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S)))

    rest_proc.start()
    logger.info("[ALL] started REST72 process pid=%s", rest_proc.pid)

    bulk_proc.start()
    logger.info("[ALL] started BULK process pid=%s", bulk_proc.pid)

    one_shot_processes: Dict[str, mp.Process] = {
        "REST72": rest_proc,
        "BULK": bulk_proc,
    }
    reported_done: set[str] = set()

    try:
        while True:
            if not live_proc.is_alive():
                logger.error("[ALL] LIVE process exited exitcode=%s", live_proc.exitcode)
                if RUN_ALL_RESTART_LIVE_IF_PROCESS_EXITS:
                    live_proc.join(timeout=2)
                    live_proc = _make_live_process(symbols)
                    live_proc.start()
                    logger.warning("[ALL] restarted LIVE process pid=%s", live_proc.pid)
                else:
                    logger.error("[ALL] live process restart disabled; stopping monitor")
                    break

            for name, proc in one_shot_processes.items():
                if name not in reported_done and not proc.is_alive():
                    reported_done.add(name)
                    level = logger.info if proc.exitcode == 0 else logger.error
                    level("[ALL] %s process finished exitcode=%s", name, proc.exitcode)

            time.sleep(max(1.0, float(RUN_ALL_MONITOR_SLEEP_S)))

    except KeyboardInterrupt:
        logger.warning("[ALL] KeyboardInterrupt; terminating child processes")
    finally:
        for proc in [live_proc, rest_proc, bulk_proc]:
            if proc.is_alive():
                logger.warning("[ALL] terminating %s pid=%s", proc.name, proc.pid)
                proc.terminate()
        for proc in [live_proc, rest_proc, bulk_proc]:
            proc.join(timeout=30)
            logger.info("[ALL] joined %s exitcode=%s", proc.name, proc.exitcode)


# =============================================================================
# READING / DEBUGGING HELPERS
# =============================================================================


def read_daily_bin(market: Market, symbol: str, d: date) -> pd.DataFrame:
    path = daily_bin_path(market, symbol, d)
    with file_lock(path):
        arr = read_bin_file(path)
    df = records_to_df(arr)
    if df.empty:
        return df
    df["open_dt"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_dt"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df.sort_values("open_time").reset_index(drop=True)


def validate_daily_bin(market: Market, symbol: str, d: date) -> Dict[str, Any]:
    df = read_daily_bin(market, symbol, d)
    if df.empty:
        return {"market": market, "symbol": symbol.upper(), "date": str(d), "rows": 0}
    dupes = int(df["open_time"].duplicated().sum())
    sorted_ok = bool(df["open_time"].is_monotonic_increasing)
    expected_start = day_start_ms(d)
    expected_end = expected_start + 24 * 60 * 60 * 1000
    out_of_day = int(((df["open_time"] < expected_start) | (df["open_time"] >= expected_end)).sum())
    return {
        "market": market,
        "symbol": symbol.upper(),
        "date": str(d),
        "rows": int(len(df)),
        "duplicates": dupes,
        "sorted": sorted_ok,
        "out_of_day": out_of_day,
        "first": df["open_dt"].iloc[0].isoformat(),
        "last": df["open_dt"].iloc[-1].isoformat(),
        "path": str(daily_bin_path(market, symbol, d)),
    }


# =============================================================================
# MAIN - NO COMMAND LINE ARGS
# =============================================================================


def main() -> None:
    safe_mkdir(BASE_DIR)
    safe_mkdir(BULK_CSV_DIR)
    safe_mkdir(DAILY_BIN_DIR)
    safe_mkdir(SYMBOLS_OUTPUT_DIR)

    symbols = resolve_symbol_sets()
    mode = str(RUN_MODE).strip().lower()

    logger.info("BASE_DIR=%s", BASE_DIR)
    logger.info("RUN_MODE=%s", mode)
    logger.info(
        "symbol_counts bulk spot=%d um=%d cm=%d | live/rest spot=%d um=%d cm=%d",
        len(symbols.spot_bulk),
        len(symbols.um_bulk),
        len(symbols.cm_bulk),
        len(symbols.spot_live_rest),
        len(symbols.um_live_rest),
        len(symbols.cm_live_rest),
    )

    if mode == "symbols":
        logger.info("[SYMBOLS] startup symbol discovery/list saving complete")
        return

    if mode == "bulk":
        download_bulk_historical_klines(
            symbols.spot_bulk,
            symbols.um_bulk,
            symbols.cm_bulk,
            BULK_START_DATE,
            BULK_END_DATE,
            import_to_bins=BULK_IMPORT_TO_BINS,
            delete_zip=BULK_DELETE_ZIPS_AFTER_EXTRACT,
        )
        return

    if mode == "rest72":
        update_last_72h_rest_all_symbols(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest)
        return

    if mode == "live":
        start_live_streams(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest)
        return

    if mode == "all":
        run_all_three_processes(symbols)
        return

    if mode == "validate":
        d = VALIDATE_DATE or datetime.now(timezone.utc).date()
        for market, syms in (
            ("spot", symbols.spot_live_rest),
            ("um", symbols.um_live_rest),
            ("cm", symbols.cm_live_rest),
        ):
            for symbol in syms:
                logger.info("[VALIDATE] %s", json.dumps(validate_daily_bin(market, symbol, d), indent=2))
        return

    raise ValueError(f"Invalid RUN_MODE={RUN_MODE!r}. Use: bulk, rest72, live, all, validate, symbols")


if __name__ == "__main__":
    mp.freeze_support()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received; exiting cleanly")
