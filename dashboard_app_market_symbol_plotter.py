from __future__ import annotations

"""
Standalone FastAPI dashboard for the collector's daily .bin OHLCV format.

No command-line args are used. Edit the CONFIG VARIABLES section, then run:

    python dashboard_app_market_symbol_plotter.py

This app reads files saved like:

    daily_bin/<market>/<symbol>/<symbol>_<market>_1m_YYYY-MM-DD.bin

Example:

    daily_bin/spot/BTCUSDT/BTCUSDT_spot_1m_2026-05-21.bin
    daily_bin/um/BTCUSDT/BTCUSDT_um_1m_2026-05-21.bin
    daily_bin/cm/BTCUSD_PERP/BTCUSD_PERP_cm_1m_2026-05-21.bin

The .bin dtype must match the collector format:

    symbol S24
    open_time int64 epoch milliseconds
    close_time int64 epoch milliseconds
    open/high/low/close/volume/quote_volume float64
    trades int64
    taker_base_vol/taker_quote_vol/maker_base_vol/maker_quote_vol float64

Install dependencies:

    pip install fastapi uvicorn numpy

Then open:

    http://127.0.0.1:8050
"""

import os
import re
import threading
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

# =============================================================================
# CONFIG VARIABLES - EDIT THESE ONLY
# =============================================================================

BASE_DIR = Path(r"E:\online wannabequant website\get spot and futures klines live\data_dump")
DAILY_BIN_DIR = BASE_DIR / "daily_bin"

HOST = "127.0.0.1"
PORT = 8050
AUTO_OPEN_BROWSER = True

INTERVAL = "1m"
INTERVAL_MS = 60_000

# Markets shown in the first dropdown.
MARKETS = ("spot", "um", "cm")
DEFAULT_MARKET = "spot"

# Initial amount of bars to plot.
DEFAULT_BARS = 500
MAX_BARS = 250_000

# If True, hide symbols that have no readable .bin files.
ONLY_SYMBOLS_WITH_BIN_FILES = True

# =============================================================================
# END CONFIG VARIABLES
# =============================================================================


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


@dataclass(frozen=True, slots=True)
class FileEdge:
    path: Path
    rows: int
    first_open_time_ms: int | None
    last_open_time_ms: int | None
    first_close_time_ms: int | None
    last_close_time_ms: int | None


def utc_iso_from_ms(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_market(market: str) -> str:
    market = str(market or "").lower().strip()
    if market not in MARKETS:
        raise ValueError(f"market must be one of {MARKETS}; got {market!r}")
    return market


def normalize_symbol(symbol: str) -> str:
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        raise ValueError("symbol is required")
    # Keep Binance symbols like BTCUSD_PERP, but reject path separators.
    if "/" in symbol or "\\" in symbol or ".." in symbol:
        raise ValueError(f"invalid symbol {symbol!r}")
    return symbol


def market_dir(market: str) -> Path:
    return DAILY_BIN_DIR / normalize_market(market)


def symbol_dir(market: str, symbol: str) -> Path:
    return market_dir(market) / normalize_symbol(symbol)


def daily_file_pattern(market: str, symbol: str) -> str:
    symbol = normalize_symbol(symbol)
    market = normalize_market(market)
    return f"{symbol}_{market}_{INTERVAL}_*.bin"


def parse_day_from_filename(path: Path, market: str, symbol: str) -> str:
    symbol = re.escape(normalize_symbol(symbol))
    market = re.escape(normalize_market(market))
    pattern = re.compile(rf"^{symbol}_{market}_{re.escape(INTERVAL)}_(\d{{4}}-\d{{2}}-\d{{2}})\.bin$", re.I)
    match = pattern.match(path.name)
    return match.group(1) if match else ""


def daily_files_for_symbol(market: str, symbol: str) -> list[Path]:
    root = symbol_dir(market, symbol)
    if not root.exists():
        return []
    files = sorted(root.glob(daily_file_pattern(market, symbol)), key=lambda p: parse_day_from_filename(p, market, symbol) or p.name)
    return [p for p in files if p.is_file()]


def validate_binary_size(path: Path) -> int:
    size = path.stat().st_size
    if size <= 0:
        return 0
    if size % KLINE_DTYPE.itemsize != 0:
        raise ValueError(f"bad file size: {path} size={size} is not divisible by itemsize={KLINE_DTYPE.itemsize}")
    return size // KLINE_DTYPE.itemsize


def read_records(path: Path) -> np.ndarray:
    rows = validate_binary_size(path)
    if rows <= 0:
        return np.empty(0, dtype=KLINE_DTYPE)
    mm = np.memmap(path, dtype=KLINE_DTYPE, mode="r", shape=(rows,))
    try:
        return np.array(mm, dtype=KLINE_DTYPE, copy=True)
    finally:
        del mm


def read_file_edge(path: Path) -> FileEdge:
    rows = validate_binary_size(path)
    if rows <= 0:
        return FileEdge(path, 0, None, None, None, None)
    mm = np.memmap(path, dtype=KLINE_DTYPE, mode="r", shape=(rows,))
    try:
        return FileEdge(
            path=path,
            rows=int(rows),
            first_open_time_ms=int(mm[0]["open_time"]),
            last_open_time_ms=int(mm[rows - 1]["open_time"]),
            first_close_time_ms=int(mm[0]["close_time"]),
            last_close_time_ms=int(mm[rows - 1]["close_time"]),
        )
    finally:
        del mm


def discover_symbols_for_market(market: str) -> list[str]:
    root = market_dir(market)
    if not root.exists():
        return []

    symbols: list[str] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        sym = child.name.upper().strip()
        if not sym:
            continue
        if ONLY_SYMBOLS_WITH_BIN_FILES:
            if not list(child.glob(f"{sym}_{market}_{INTERVAL}_*.bin")):
                continue
        symbols.append(sym)

    return sorted(set(symbols))


def market_counts() -> dict[str, int]:
    return {market: len(discover_symbols_for_market(market)) for market in MARKETS}


def available_range(market: str, symbol: str) -> dict[str, Any]:
    market = normalize_market(market)
    symbol = normalize_symbol(symbol)
    files = daily_files_for_symbol(market, symbol)
    edges: list[FileEdge] = []
    corrupt: list[dict[str, str]] = []

    for path in files:
        try:
            edge = read_file_edge(path)
        except Exception as exc:
            corrupt.append({"path": str(path), "error": str(exc)})
            continue
        if edge.rows > 0:
            edges.append(edge)

    if not edges:
        return {
            "market": market,
            "symbol": symbol,
            "interval": INTERVAL,
            "has_data": False,
            "file_count": len(files),
            "readable_file_count": 0,
            "row_count": 0,
            "first_open_time_ms": None,
            "last_open_time_ms": None,
            "first_close_time_ms": None,
            "last_close_time_ms": None,
            "first_close_utc": None,
            "last_close_utc": None,
            "corrupt_files": corrupt,
        }

    first_open = min(e.first_open_time_ms for e in edges if e.first_open_time_ms is not None)
    last_open = max(e.last_open_time_ms for e in edges if e.last_open_time_ms is not None)
    first_close = min(e.first_close_time_ms for e in edges if e.first_close_time_ms is not None)
    last_close = max(e.last_close_time_ms for e in edges if e.last_close_time_ms is not None)

    return {
        "market": market,
        "symbol": symbol,
        "interval": INTERVAL,
        "has_data": True,
        "file_count": len(files),
        "readable_file_count": len(edges),
        "row_count": int(sum(e.rows for e in edges)),
        "first_open_time_ms": int(first_open),
        "last_open_time_ms": int(last_open),
        "first_close_time_ms": int(first_close),
        "last_close_time_ms": int(last_close),
        "first_close_utc": utc_iso_from_ms(int(first_close)),
        "last_close_utc": utc_iso_from_ms(int(last_close)),
        "corrupt_files": corrupt,
    }


def dedupe_sort_rows(rows: np.ndarray) -> np.ndarray:
    if len(rows) == 0:
        return rows
    order = np.argsort(rows["open_time"], kind="stable")
    rows = rows[order]

    # Keep the last row for each duplicate open_time.
    reversed_open = rows["open_time"][::-1]
    _unique_values, reversed_first_indexes = np.unique(reversed_open, return_index=True)
    keep_indexes = len(rows) - 1 - reversed_first_indexes
    keep_indexes.sort()
    return np.array(rows[keep_indexes], dtype=KLINE_DTYPE, copy=True)


def read_latest_rows(market: str, symbol: str, bars: int) -> tuple[np.ndarray, list[dict[str, str]]]:
    market = normalize_market(market)
    symbol = normalize_symbol(symbol)
    bars = max(1, min(int(bars), MAX_BARS))
    files = daily_files_for_symbol(market, symbol)

    arrays: list[np.ndarray] = []
    skipped: list[dict[str, str]] = []
    loaded_rows = 0

    for path in reversed(files):
        try:
            arr = read_records(path)
        except Exception as exc:
            skipped.append({"path": str(path), "error": str(exc)})
            continue

        if len(arr) == 0:
            continue

        arrays.append(arr)
        loaded_rows += int(len(arr))

        # Load a little extra so duplicates/gaps are handled after sorting.
        if loaded_rows >= bars + 2_000:
            break

    if not arrays:
        return np.empty(0, dtype=KLINE_DTYPE), skipped

    rows = np.concatenate(arrays) if len(arrays) > 1 else arrays[0]
    rows = dedupe_sort_rows(rows)
    if len(rows) > bars:
        rows = rows[-bars:]
    return np.array(rows, dtype=KLINE_DTYPE, copy=True), skipped


def gap_info_from_open_times(open_times_ms: np.ndarray) -> tuple[list[dict[str, Any]], int]:
    if len(open_times_ms) <= 1:
        return [], 0
    open_times_ms = np.asarray(open_times_ms, dtype=np.int64)
    diffs = open_times_ms[1:] - open_times_ms[:-1]
    mask = diffs > INTERVAL_MS
    gaps: list[dict[str, Any]] = []
    missing_total = 0

    for previous_open, next_open, diff in zip(open_times_ms[:-1][mask], open_times_ms[1:][mask], diffs[mask]):
        missing_count = int((diff // INTERVAL_MS) - 1)
        if missing_count <= 0:
            continue
        missing_total += missing_count
        if len(gaps) < 2000:
            start = int(previous_open + INTERVAL_MS)
            end = int(next_open - INTERVAL_MS)
            gaps.append(
                {
                    "start_open_time_ms": start,
                    "end_open_time_ms": end,
                    "start_utc": utc_iso_from_ms(start),
                    "end_utc": utc_iso_from_ms(end),
                    "missing_count": missing_count,
                    "previous_open_time_ms": int(previous_open),
                    "next_open_time_ms": int(next_open),
                }
            )
    return gaps, missing_total


def rows_to_json(rows: np.ndarray) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        open_time_ms = int(row["open_time"])
        close_time_ms = int(row["close_time"])
        out.append(
            {
                "symbol": row["symbol"].decode("ascii", errors="ignore").strip("\x00") or "",
                "open_time_ms": open_time_ms,
                "close_time_ms": close_time_ms,
                "open_time_utc": utc_iso_from_ms(open_time_ms),
                "close_time_utc": utc_iso_from_ms(close_time_ms),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "quote_volume": float(row["quote_volume"]),
                "trades": int(row["trades"]),
                "taker_base_vol": float(row["taker_base_vol"]),
                "taker_quote_vol": float(row["taker_quote_vol"]),
                "maker_base_vol": float(row["maker_base_vol"]),
                "maker_quote_vol": float(row["maker_quote_vol"]),
            }
        )
    return out


def ohlcv_payload(market: str, symbol: str, bars: int) -> dict[str, Any]:
    rows, skipped = read_latest_rows(market, symbol, bars)
    gaps, missing_total = gap_info_from_open_times(rows["open_time"] if len(rows) else np.empty(0, dtype=np.int64))
    latest_close_ms = int(rows["close_time"][-1]) if len(rows) else None
    first_close_ms = int(rows["close_time"][0]) if len(rows) else None
    return {
        "market": normalize_market(market),
        "symbol": normalize_symbol(symbol),
        "interval": INTERVAL,
        "requested_bars": int(bars),
        "row_count": int(len(rows)),
        "first_close_time_ms": first_close_ms,
        "latest_close_time_ms": latest_close_ms,
        "first_close_utc": utc_iso_from_ms(first_close_ms),
        "latest_close_utc": utc_iso_from_ms(latest_close_ms),
        "missing_rows_plotted": int(missing_total),
        "gap_ranges": gaps,
        "skipped_files": skipped,
        "rows": rows_to_json(rows),
    }


def symbol_summary_rows(market: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in discover_symbols_for_market(market):
        info = available_range(market, symbol)
        rows.append(
            {
                "symbol": symbol,
                "files": info["readable_file_count"],
                "rows": info["row_count"],
                "first_close_utc": info["first_close_utc"] or "-",
                "last_close_utc": info["last_close_utc"] or "-",
                "corrupt_files": len(info.get("corrupt_files") or []),
            }
        )
    rows.sort(key=lambda r: (str(r["symbol"])))
    return rows


INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Binance Daily .bin OHLCV Viewer</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root { color-scheme: dark; }
    body { margin: 0; font-family: Segoe UI, Arial, sans-serif; background: #0b1020; color: #e5e7eb; }
    header { padding: 18px 22px; background: #111827; border-bottom: 1px solid #263244; }
    h1 { margin: 0 auto; max-width: 1600px; font-size: 22px; }
    .caption { color: #9ca3af; font-size: 13px; margin: 5px auto 0; max-width: 1600px; }
    .wrap { padding: 18px 22px; display: grid; gap: 16px; max-width: 1600px; margin: 0 auto; }
    .panel { background: #111827; border: 1px solid #263244; border-radius: 14px; padding: 14px; box-shadow: 0 10px 22px rgba(0,0,0,0.25); }
    .controls { display: grid; grid-template-columns: 140px minmax(220px, 1fr) 140px 150px 160px 110px; gap: 12px; align-items: end; }
    label { display: grid; gap: 6px; font-size: 12px; color: #b9c0cc; }
    select, input, button { border: 1px solid #374151; background: #0b1020; color: #f9fafb; border-radius: 10px; padding: 9px 10px; font-size: 14px; }
    button { cursor: pointer; background: #1f2937; }
    button:hover { background: #273449; }
    .status { display: flex; flex-wrap: wrap; gap: 12px; font-size: 13px; color: #cbd5e1; margin-top: 12px; }
    .pill { padding: 6px 9px; border-radius: 999px; background: #172033; border: 1px solid #263244; }
    .ok { color: #86efac; }
    .bad { color: #fca5a5; }
    #chart { width: 100%; height: 72vh; min-height: 520px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; table-layout: fixed; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #243044; overflow-wrap: anywhere; vertical-align: top; }
    th { color: #93c5fd; position: sticky; top: 0; background: #111827; z-index: 1; }
    .scroll { height: 340px; overflow: auto; resize: vertical; border: 1px solid rgba(38, 50, 68, 0.55); border-radius: 10px; }
    @media (max-width: 1100px) { .controls { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
  <header>
    <h1>Binance Daily .bin OHLCV Viewer</h1>
    <div class="caption">Reads collector files directly from daily_bin/&lt;market&gt;/&lt;symbol&gt;/. Select spot, um, or cm, then select a symbol.</div>
  </header>

  <main class="wrap">
    <section class="panel">
      <div class="controls">
        <label>Market
          <select id="market"></select>
        </label>
        <label>Symbol
          <select id="symbol"></select>
        </label>
        <label>Rows
          <input id="bars" type="number" min="1" max="250000" value="500" />
        </label>
        <label>Plot type
          <select id="plotType">
            <option value="candles">Candles + volume</option>
            <option value="close">Close line</option>
          </select>
        </label>
        <label>Refresh seconds
          <input id="refresh" type="number" min="1" max="60" value="3" />
        </label>
        <button id="plotBtn">Plot</button>
      </div>
      <div class="status">
        <span class="pill" id="rootStatus">Loading...</span>
        <span class="pill" id="rangeStatus">Range: -</span>
        <span class="pill" id="latestStatus">Latest: -</span>
        <span class="pill" id="gapStatus">Gaps plotted: -</span>
      </div>
    </section>

    <section class="panel"><div id="chart"></div></section>

    <section class="panel">
      <h3>Symbols in selected market</h3>
      <div class="caption">This table is built by scanning your local market folder.</div>
      <div class="scroll"><table id="summaryTable"></table></div>
    </section>
  </main>

<script>
const $ = (id) => document.getElementById(id);
let pollHandle = null;
let lastSeenCloseMs = null;

function qs(params){ return new URLSearchParams(params).toString(); }
function fmtInt(n){ return Number(n || 0).toLocaleString(); }
function fmtMs(ms){ return ms ? new Date(Number(ms)).toISOString().replace('.000Z','Z') : '-'; }

async function fetchJSON(url){
  const response = await fetch(url, {cache: 'no-store'});
  if (!response.ok) throw new Error(await response.text());
  return await response.json();
}

function renderTable(id, rows){
  const el = $(id);
  if (!rows || !rows.length){ el.innerHTML = '<tr><td>No rows</td></tr>'; return; }
  const cols = Object.keys(rows[0]);
  const head = '<thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead>';
  const body = '<tbody>' + rows.map(r => '<tr>' + cols.map(c => `<td>${r[c] ?? ''}</td>`).join('') + '</tr>').join('') + '</tbody>';
  el.innerHTML = head + body;
}

async function loadMarkets(){
  const data = await fetchJSON('/api/markets');
  const sel = $('market');
  sel.innerHTML = '';
  data.markets.forEach(m => {
    const option = document.createElement('option');
    option.value = m;
    option.textContent = `${m} (${data.counts[m] || 0})`;
    sel.appendChild(option);
  });
  if (data.default_market && data.markets.includes(data.default_market)) sel.value = data.default_market;
  $('rootStatus').innerHTML = `Data root: ${data.daily_bin_dir}`;
}

async function loadSymbols(){
  const market = $('market').value;
  const data = await fetchJSON('/api/symbols?' + qs({market}));
  const sel = $('symbol');
  sel.innerHTML = '';
  data.symbols.forEach(sym => {
    const option = document.createElement('option');
    option.value = sym;
    option.textContent = sym;
    sel.appendChild(option);
  });
  if (!data.symbols.length){
    $('rangeStatus').innerHTML = `<span class="bad">No symbols found for ${market}</span>`;
    Plotly.purge('chart');
    renderTable('summaryTable', []);
    return;
  }
  await loadSummary();
  await loadRangeAndPlot();
}

async function loadSummary(){
  const market = $('market').value;
  const data = await fetchJSON('/api/summary?' + qs({market}));
  renderTable('summaryTable', data.rows || []);
}

async function loadRangeAndPlot(){
  const market = $('market').value;
  const symbol = $('symbol').value;
  if (!market || !symbol) return;
  const range = await fetchJSON('/api/range?' + qs({market, symbol}));
  if (!range.has_data){
    $('rangeStatus').innerHTML = `<span class="bad">No data for ${market} ${symbol}</span>`;
    Plotly.purge('chart');
    return;
  }
  $('rangeStatus').textContent = `Range: ${range.first_close_utc} → ${range.last_close_utc} | files: ${fmtInt(range.readable_file_count)} | rows: ${fmtInt(range.row_count)}`;
  await plotData();
}

async function plotData(){
  const market = $('market').value;
  const symbol = $('symbol').value;
  const bars = $('bars').value || 500;
  const plotType = $('plotType').value;
  if (!market || !symbol) return;

  const data = await fetchJSON('/api/ohlcv?' + qs({market, symbol, bars}));
  const rows = data.rows || [];
  if (!rows.length){
    $('latestStatus').innerHTML = `<span class="bad">No rows loaded</span>`;
    Plotly.purge('chart');
    return;
  }

  const x = rows.map(r => new Date(r.open_time_ms));
  const close = rows.map(r => r.close);
  const volume = rows.map(r => r.volume);
  let traces = [];
  let layout = {
    paper_bgcolor: '#111827',
    plot_bgcolor: '#0b1020',
    font: {color: '#e5e7eb'},
    margin: {l: 60, r: 28, t: 35, b: 45},
    xaxis: {title: 'UTC open time', gridcolor: '#233047', rangeslider: {visible: false}},
    hovermode: 'x unified',
    legend: {orientation: 'h'},
  };

  if (plotType === 'candles'){
    traces.push({
      x,
      open: rows.map(r => r.open),
      high: rows.map(r => r.high),
      low: rows.map(r => r.low),
      close,
      type: 'candlestick',
      name: `${market} ${symbol}`,
      yaxis: 'y'
    });
    traces.push({
      x,
      y: volume,
      type: 'bar',
      name: 'volume',
      yaxis: 'y2',
      opacity: 0.35
    });
    layout.yaxis = {title: 'Price', gridcolor: '#233047', domain: [0.28, 1.0]};
    layout.yaxis2 = {title: 'Volume', gridcolor: '#233047', domain: [0.0, 0.20]};
  } else {
    const lineX = [];
    const lineY = [];
    for (let i = 0; i < rows.length; i++){
      if (i > 0 && (rows[i].open_time_ms - rows[i-1].open_time_ms) > 60000){
        lineX.push(null); lineY.push(null);
      }
      lineX.push(new Date(rows[i].open_time_ms));
      lineY.push(rows[i].close);
    }
    traces.push({x: lineX, y: lineY, type: 'scattergl', mode: 'lines', name: `${market} ${symbol} close`, line: {width: 1.7}});
    layout.yaxis = {title: 'Close', gridcolor: '#233047'};
  }

  const shapes = (data.gap_ranges || []).slice(0, 2000).map(g => ({
    type: 'rect', xref: 'x', yref: 'paper',
    x0: new Date(g.start_open_time_ms - 30000),
    x1: new Date(g.end_open_time_ms + 30000),
    y0: 0, y1: 1,
    fillcolor: 'rgba(239, 68, 68, 0.16)',
    line: {width: 0},
    layer: 'below'
  }));
  layout.shapes = shapes;

  Plotly.react('chart', traces, layout, {responsive: true, displaylogo: false});
  lastSeenCloseMs = data.latest_close_time_ms || lastSeenCloseMs;
  $('latestStatus').textContent = `Latest plotted: ${data.latest_close_utc} | rows plotted: ${fmtInt(data.row_count)}`;
  $('gapStatus').textContent = `Gaps plotted: ${fmtInt(data.missing_rows_plotted)}`;
}

async function pollLatest(){
  const market = $('market').value;
  const symbol = $('symbol').value;
  if (!market || !symbol) return;
  try {
    const range = await fetchJSON('/api/range?' + qs({market, symbol}));
    if (!range.has_data || !range.last_close_time_ms) return;
    if (!lastSeenCloseMs) lastSeenCloseMs = range.last_close_time_ms;
    if (range.last_close_time_ms > lastSeenCloseMs){
      lastSeenCloseMs = range.last_close_time_ms;
      await plotData();
    }
  } catch (err){
    console.warn('poll failed', err);
  }
}

function resetPolling(){
  if (pollHandle) clearInterval(pollHandle);
  pollHandle = setInterval(pollLatest, Math.max(1, Number($('refresh').value || 3)) * 1000);
}

$('market').addEventListener('change', async () => { lastSeenCloseMs = null; await loadSymbols(); });
$('symbol').addEventListener('change', async () => { lastSeenCloseMs = null; await loadRangeAndPlot(); });
$('plotBtn').addEventListener('click', plotData);
$('bars').addEventListener('change', plotData);
$('plotType').addEventListener('change', plotData);
$('refresh').addEventListener('change', resetPolling);

(async function init(){
  await loadMarkets();
  await loadSymbols();
  resetPolling();
})();
</script>
</body>
</html>
"""


def create_app() -> FastAPI:
    app = FastAPI(title="Binance Daily .bin OHLCV Viewer", version="1.0")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

    @app.get("/api/markets")
    def api_markets() -> dict[str, Any]:
        return {
            "markets": list(MARKETS),
            "default_market": DEFAULT_MARKET if DEFAULT_MARKET in MARKETS else MARKETS[0],
            "counts": market_counts(),
            "base_dir": str(BASE_DIR),
            "daily_bin_dir": str(DAILY_BIN_DIR),
            "dtype_itemsize": KLINE_DTYPE.itemsize,
        }

    @app.get("/api/symbols")
    def api_symbols(market: str = Query(DEFAULT_MARKET)) -> dict[str, Any]:
        market = normalize_market(market)
        symbols = discover_symbols_for_market(market)
        return {"market": market, "symbols": symbols, "count": len(symbols)}

    @app.get("/api/summary")
    def api_summary(market: str = Query(DEFAULT_MARKET)) -> dict[str, Any]:
        market = normalize_market(market)
        rows = symbol_summary_rows(market)
        return {"market": market, "rows": rows, "count": len(rows)}

    @app.get("/api/range")
    def api_range(market: str = Query(DEFAULT_MARKET), symbol: str = Query(...)) -> dict[str, Any]:
        return available_range(market, symbol)

    @app.get("/api/ohlcv")
    def api_ohlcv(
        market: str = Query(DEFAULT_MARKET),
        symbol: str = Query(...),
        bars: int = Query(DEFAULT_BARS, ge=1, le=MAX_BARS),
    ) -> dict[str, Any]:
        return ohlcv_payload(market, symbol, bars)

    return app


app = create_app()


def open_browser_later() -> None:
    if not AUTO_OPEN_BROWSER:
        return

    def _open() -> None:
        time.sleep(1.0)
        webbrowser.open(f"http://{HOST}:{PORT}")

    threading.Thread(target=_open, daemon=True).start()


def main() -> None:
    import uvicorn

    print(f"Starting web app at http://{HOST}:{PORT}")
    print(f"Reading daily .bin data from: {DAILY_BIN_DIR}")
    print("Edit BASE_DIR / DAILY_BIN_DIR at the top of this file if the path is wrong.")
    open_browser_later()
    uvicorn.run(create_app(), host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
