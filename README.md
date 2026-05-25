# binance_OHLCV_data_collecter_live_historical



This Python program uses a multiplex stream to get OHLCV data live from spot, USD margin, or coin margin tradable symbols. 
Each symbol has its own .bin file where OHLCV data is stored with a 1-minute timeframe.
The program gets the last 3 days' worth of 1-minute OHLCV data and stores it into .bin files.
This program downloads zipped CSVs from Binance and stores the CSV in storage instead of redownloading it.
Historical CSV data is added to the symbol .bin files.
All data from Live, REST call 3 days, and the bulk CSV download is stored in a .bin file per symbol with fast appends for Live or writes for REST API and bulk download.
If the stream disconnects, then when it reconnects, the REST API is called to retrieve any missing bars/rows of data from the 1m time frame.
You can set the program to discover all symbols on the exchange or use the variable input lists.


Installation

Install collector dependencies:

pip install requests pandas numpy websockets certifi portalocker

Install dashboard dependencies:

pip install fastapi uvicorn numpy

If you want everything at once:

pip install requests pandas numpy websockets certifi portalocker fastapi uvicorn
Project files

Recommended file layout:

project/
  binance_klines_cm_um_spot_NETWORK_ERRORS_HANDLED.py
  dashboard_app_market_symbol_plotter.py
  README.md

The data folder is controlled by BASE_DIR inside both scripts.

Example output layout:

data_dump/
  bulk_csv/
  daily_bin/
    spot/
      BTCUSDT/
        BTCUSDT_spot_1m_2026-05-25.bin
    um/
      BTCUSDT/
        BTCUSDT_um_1m_2026-05-25.bin
    cm/
      BTCUSD_PERP/
        BTCUSD_PERP_cm_1m_2026-05-25.bin
  logs/
  symbols_output/
Binary data format

The collector and dashboard must use the same .bin dtype.

Each row stores:

Field	Type	Meaning
symbol	S24	Symbol name as bytes
open_time	int64	Candle open time in epoch milliseconds
close_time	int64	Candle close time in epoch milliseconds
open	float64	Open price
high	float64	High price
low	float64	Low price
close	float64	Close price
volume	float64	Base asset volume
quote_volume	float64	Quote asset volume
trades	int64	Number of trades
taker_base_vol	float64	Taker buy base volume
taker_quote_vol	float64	Taker buy quote volume
maker_base_vol	float64	Maker base volume, calculated as volume - taker_base_vol
maker_quote_vol	float64	Maker quote volume, calculated as quote_volume - taker_quote_vol

A complete UTC day of 1-minute candles should contain exactly:

1440 rows
Collector configuration inputs

The collector does not use command-line arguments. Edit the variables at the top of the collector script.

Path and storage inputs
BASE_DIR = Path(r"E:\online wannabequant website\get spot and futures klines live\data_dump")
BULK_CSV_DIR = BASE_DIR / "bulk_csv"
DAILY_BIN_DIR = BASE_DIR / "daily_bin"
LOG_DIR = BASE_DIR / "logs"
SYMBOLS_OUTPUT_DIR = BASE_DIR / "symbols_output"
LOG_PATH = LOG_DIR / "klines_collector.log"
Input	What it does
BASE_DIR	Main root folder for all collector output. Change this first.
BULK_CSV_DIR	Stores extracted Binance Vision CSV files.
DAILY_BIN_DIR	Stores daily .bin OHLCV files. The dashboard reads this folder.
LOG_DIR	Stores log files.
SYMBOLS_OUTPUT_DIR	Stores resolved symbol lists when using symbol discovery.
LOG_PATH	Full path of the main collector log file.

Example:

BASE_DIR = Path(r"C:\binance_ohlcv_data")

Linux/macOS example:

BASE_DIR = Path("/home/user/binance_ohlcv_data")
Run mode input
RUN_MODE = "all"

Choose one:

RUN_MODE	What it does
"symbols"	Resolves symbol lists and saves them to symbols_output, then exits.
"bulk"	Downloads Binance Vision historical CSV files and optionally imports them into .bin files.
"rest72"	Fetches the latest 72 hours of closed 1-minute candles using REST.
"live"	Runs live websocket collection only.
"all"	Starts live, REST72, and bulk together in separate processes.
"validate"	Checks .bin files for row count, duplicates, sorting, and out-of-day rows.

Recommended first test:

RUN_MODE = "rest72"

Then validate:

RUN_MODE = "validate"

For full collection:

RUN_MODE = "all"
Historical bulk symbol inputs
SYMBOLS_SPOT_BULK = ["AAVEUSDT", "BTCUSDT", "ETHUSDT", "LTCUSDT", "SOLUSDT", "XRPUSDT"]

SYMBOLS_FUTURES_UM_BULK = [
    "AAVEUSDT", "ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"
]

SYMBOLS_FUTURES_CM_BULK = ["AAVEUSD_PERP", "BTCUSD_PERP"]
Input	What it controls
SYMBOLS_SPOT_BULK	Spot symbols used for historical Binance Vision bulk downloads.
SYMBOLS_FUTURES_UM_BULK	USD-M futures symbols used for historical Binance Vision bulk downloads.
SYMBOLS_FUTURES_CM_BULK	COIN-M futures symbols used for historical Binance Vision bulk downloads.

Bulk lists can include old, dead, or delisted symbols if Binance Vision has historical files for them.

COIN-M bulk lists may include dated contracts such as:

"ADAUSD_200925"
Live and REST symbol inputs
SYMBOLS_SPOT_LIVE_REST = ["AAVEUSDT", "BTCUSDT", "ETHUSDT", "LTCUSDT", "SOLUSDT", "XRPUSDT"]

SYMBOLS_FUTURES_UM_LIVE_REST = [
    "AAVEUSDT", "ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"
]

SYMBOLS_FUTURES_CM_LIVE_REST = ["AAVEUSD_PERP", "BTCUSD_PERP"]
Input	What it controls
SYMBOLS_SPOT_LIVE_REST	Spot symbols used by REST72 and live websocket mode.
SYMBOLS_FUTURES_UM_LIVE_REST	USD-M futures symbols used by REST72 and live websocket mode.
SYMBOLS_FUTURES_CM_LIVE_REST	COIN-M futures symbols used by REST72 and live websocket mode.

Important:

Live/REST lists should contain only currently trading symbols.
Do not put expired COIN-M dated contracts in live/REST lists.
Use perpetual COIN-M symbols like BTCUSD_PERP for live/REST.
Auto symbol discovery inputs
AUTO_FIND_SYMBOLS_SPOT = False
AUTO_FIND_SYMBOLS_UM = False
AUTO_FIND_SYMBOLS_CM = False
Input	What it does
AUTO_FIND_SYMBOLS_SPOT	If True, automatically finds Spot symbols.
AUTO_FIND_SYMBOLS_UM	If True, automatically finds USD-M futures symbols.
AUTO_FIND_SYMBOLS_CM	If True, automatically finds COIN-M futures symbols.

When enabled:

Bulk symbols come from Binance Vision directories.
Live/REST symbols come from Binance exchangeInfo trading symbols.

Optional validation:

AUTO_VALIDATE_LIVE_SYMBOLS_WITH_KLINE = False
AUTO_VALIDATE_MAX_WORKERS = 25
AUTO_VALIDATE_MAX_AGE_MINUTES = 180
Input	What it does
AUTO_VALIDATE_LIVE_SYMBOLS_WITH_KLINE	If True, checks that each live/REST symbol has a recent closed 1-minute candle.
AUTO_VALIDATE_MAX_WORKERS	Thread count used for validation checks.
AUTO_VALIDATE_MAX_AGE_MINUTES	Maximum age allowed for a recent validation candle.

This is slower, but helps avoid stale or broken symbols.

Timeframe and REST inputs
INTERVAL = "1m"
INTERVAL_MS = 60_000
REST_LOOKBACK_HOURS = 72
REST_SYMBOL_THREADS = 10
Input	What it does
INTERVAL	Candle interval. This project is designed for "1m".
INTERVAL_MS	Interval length in milliseconds. 60_000 equals 1 minute.
REST_LOOKBACK_HOURS	How many recent hours REST72 mode fetches.
REST_SYMBOL_THREADS	Number of REST worker threads used across symbols.

Default behavior:

REST72 fetches the latest 72 hours of closed 1-minute candles.
Bulk download inputs
BULK_DOWNLOAD_THREADS = 40
BULK_PLAN_THREADS = 10
BULK_DEBUG_LOG_EVERY_CSV_CHECK = False
BULK_DEBUG_LOG_REMOTE_SUMMARY = False
Input	What it does
BULK_DOWNLOAD_THREADS	Number of concurrent Binance Vision CSV downloads.
BULK_PLAN_THREADS	Number of worker threads used to plan bulk downloads.
BULK_DEBUG_LOG_EVERY_CSV_CHECK	Logs every local CSV exists/missing decision if True.
BULK_DEBUG_LOG_REMOTE_SUMMARY	Logs monthly/daily remote archive summary if True.

Bulk date range:

BULK_START_DATE = date(2025, 6, 1)
BULK_END_DATE = date(2099, 12, 31)
BULK_IMPORT_TO_BINS = True
BULK_DELETE_ZIPS_AFTER_EXTRACT = True
Input	What it does
BULK_START_DATE	First historical date to download/import.
BULK_END_DATE	Last historical date to download/import.
BULK_IMPORT_TO_BINS	If True, imports downloaded CSV rows into .bin files.
BULK_DELETE_ZIPS_AFTER_EXTRACT	If True, deletes .zip files after extracting CSVs.

BULK_END_DATE = date(2099, 12, 31) does not download future data. It just allows the planner to use whatever Binance Vision archives currently exist.

Bulk import skip and repair inputs
BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS = True
BULK_EXPECTED_ROWS_PER_DAY = 1440
BULK_BIN_COMPLETENESS_WORKERS = 1
Input	What it does
BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS	If True, skips days where the .bin already has 1440 rows.
BULK_EXPECTED_ROWS_PER_DAY	Expected row count for a complete 1-minute UTC day.
BULK_BIN_COMPLETENESS_WORKERS	Workers used to check whether daily .bin files are complete.

If a day is missing, partial, corrupt, or has too many rows, bulk import repairs that day by importing and deduping rows from the CSV.

RUN_MODE="all" supervision inputs
RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S = 1.0
RUN_ALL_MONITOR_SLEEP_S = 5.0
RUN_ALL_RESTART_LIVE_IF_PROCESS_EXITS = True
Input	What it does
RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S	Delay after starting live process before starting REST72 and bulk.
RUN_ALL_MONITOR_SLEEP_S	How often the supervisor checks child processes.
RUN_ALL_RESTART_LIVE_IF_PROCESS_EXITS	If True, restarts live process if it exits.

In "all" mode:

Live starts first.
REST72 starts after a short delay.
Bulk starts in its own process.
Live can be restarted if it exits.
Websocket inputs
MAX_SYMBOLS_PER_STREAM = 200
STREAM_RECONNECT_MAX_BACKOFF_S = 120
STREAM_RECV_TIMEOUT_S = 600
WS_OPEN_TIMEOUT_S = 30
WS_CLOSE_TIMEOUT_S = 10
WS_MAX_QUEUE = 8192
Input	What it does
MAX_SYMBOLS_PER_STREAM	Maximum symbols per multiplex websocket connection.
STREAM_RECONNECT_MAX_BACKOFF_S	Maximum reconnect backoff delay.
STREAM_RECV_TIMEOUT_S	If no kline messages arrive for this many seconds, reconnect.
WS_OPEN_TIMEOUT_S	Timeout for opening websocket connection.
WS_CLOSE_TIMEOUT_S	Timeout for closing websocket connection.
WS_MAX_QUEUE	Websocket internal queue limit.

Ping/pong inputs:

WS_CLIENT_PING_INTERVAL_S = None
WS_CLIENT_PING_TIMEOUT_S = None
WS_UNSOLICITED_PONG_INTERVAL_S = 300
Input	What it does
WS_CLIENT_PING_INTERVAL_S	Client-side ping interval. Default None disables extra client pings.
WS_CLIENT_PING_TIMEOUT_S	Client-side ping timeout. Default None.
WS_UNSOLICITED_PONG_INTERVAL_S	Optional unsolicited pong keepalive interval.

The websockets library automatically replies to Binance server ping frames.

Network retry inputs
LOG_RECOVERABLE_NETWORK_TRACEBACKS = False
NETWORK_FAILURE_RETRY_MIN_S = 1.0
NETWORK_FAILURE_RETRY_MAX_S = 15.0
NETWORK_FAILURE_BACKOFF_MAX_S = 30.0
WINDOWS_USE_SELECTOR_EVENT_LOOP = True
Input	What it does
LOG_RECOVERABLE_NETWORK_TRACEBACKS	If True, logs full tracebacks for recoverable websocket/network errors.
NETWORK_FAILURE_RETRY_MIN_S	Minimum retry sleep after a network failure.
NETWORK_FAILURE_RETRY_MAX_S	Maximum random retry sleep after a network failure.
NETWORK_FAILURE_BACKOFF_MAX_S	Maximum network backoff delay.
WINDOWS_USE_SELECTOR_EVENT_LOOP	Uses selector event loop on Windows for websocket stability.

HTTP inputs:

HTTP_TIMEOUT_S = 30
HTTP_RETRIES = 8
Input	What it does
HTTP_TIMEOUT_S	Default HTTP request timeout.
HTTP_RETRIES	Default retry count for HTTP requests.

Bulk HTTP download inputs:

BULK_DOWNLOAD_CONNECT_TIMEOUT_S = 10
BULK_DOWNLOAD_READ_TIMEOUT_S = 180
BULK_DOWNLOAD_RETRIES = 12
BULK_DOWNLOAD_CHUNK_SIZE = 2 * 1024 * 1024
BULK_DOWNLOAD_BACKOFF_MAX_S = 180.0
Input	What it does
BULK_DOWNLOAD_CONNECT_TIMEOUT_S	Timeout for connecting to Binance Vision.
BULK_DOWNLOAD_READ_TIMEOUT_S	Read timeout while downloading large archives.
BULK_DOWNLOAD_RETRIES	Retry count for bulk downloads.
BULK_DOWNLOAD_CHUNK_SIZE	Download chunk size in bytes.
BULK_DOWNLOAD_BACKOFF_MAX_S	Maximum backoff delay for bulk downloads.

DNS inputs:

DNS_FAILURE_RETRY_MIN_S = 5.0
DNS_FAILURE_RETRY_MAX_S = 30.0
DNS_FORCE_IPV4 = True
Input	What it does
DNS_FAILURE_RETRY_MIN_S	Minimum retry delay for DNS failures.
DNS_FAILURE_RETRY_MAX_S	Maximum retry delay for DNS failures.
DNS_FORCE_IPV4	If True, forces IPv4 for DNS/network handling.
Validation input
VALIDATE_DATE = None
Input	What it does
VALIDATE_DATE	Date to validate. If None, validates today's UTC .bin files.

Example:

VALIDATE_DATE = date(2026, 5, 25)
RUN_MODE = "validate"
Binance endpoint inputs

These are advanced settings. Most users should not change them.

REST endpoints:

SPOT_REST_KLINES = "https://api.binance.com/api/v3/klines"
UM_REST_KLINES = "https://fapi.binance.com/fapi/v1/klines"
CM_REST_KLINES = "https://dapi.binance.com/dapi/v1/klines"
SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
UM_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
CM_EXCHANGE_INFO = "https://dapi.binance.com/dapi/v1/exchangeInfo"

Websocket endpoints:

SPOT_WS_BASE = "wss://stream.binance.com:443/stream?streams="
UM_WS_BASE = "wss://fstream.binance.com/market/stream?streams="
CM_WS_BASE = "wss://dstream.binance.com/stream?streams="

Fallback lists:

SPOT_WS_BASE_FALLBACKS
UM_WS_BASE_FALLBACKS
CM_WS_BASE_FALLBACKS

Binance Vision listing endpoint:

S3_LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
Dashboard web app inputs

The dashboard also does not use command-line arguments. Edit the CONFIG VARIABLES section at the top of the dashboard script.

Dashboard path inputs
BASE_DIR = Path(r"E:\online wannabequant website\get spot and futures klines live\data_dump")
DAILY_BIN_DIR = BASE_DIR / "daily_bin"
Input	What it does
BASE_DIR	Must point to the same data root used by the collector.
DAILY_BIN_DIR	Folder containing daily .bin files. Usually BASE_DIR / "daily_bin".

The dashboard expects files like:

daily_bin/<market>/<symbol>/<symbol>_<market>_1m_YYYY-MM-DD.bin

Examples:

daily_bin/spot/BTCUSDT/BTCUSDT_spot_1m_2026-05-21.bin
daily_bin/um/BTCUSDT/BTCUSDT_um_1m_2026-05-21.bin
daily_bin/cm/BTCUSD_PERP/BTCUSD_PERP_cm_1m_2026-05-21.bin
Dashboard server inputs
HOST = "127.0.0.1"
PORT = 8050
AUTO_OPEN_BROWSER = True
Input	What it does
HOST	Address the local server binds to. Use 127.0.0.1 for local-only access.
PORT	Port used by the dashboard. Default is 8050.
AUTO_OPEN_BROWSER	If True, opens the dashboard automatically when the app starts.

Local URL:

http://127.0.0.1:8050

If you change the port:

PORT = 8060

Then open:

http://127.0.0.1:8060
Dashboard market and interval inputs
INTERVAL = "1m"
INTERVAL_MS = 60_000
MARKETS = ("spot", "um", "cm")
DEFAULT_MARKET = "spot"
Input	What it does
INTERVAL	File interval expected by the dashboard. Should match the collector.
INTERVAL_MS	Interval length in milliseconds. 60_000 means 1 minute.
MARKETS	Markets shown in the first dropdown.
DEFAULT_MARKET	Market selected when the dashboard first loads.

Do not change INTERVAL unless the collector is also changed to write another timeframe.

Dashboard chart inputs
DEFAULT_BARS = 500
MAX_BARS = 250_000
ONLY_SYMBOLS_WITH_BIN_FILES = True
Input	What it does
DEFAULT_BARS	Initial number of recent bars plotted.
MAX_BARS	Maximum bars allowed in one chart/API request.
ONLY_SYMBOLS_WITH_BIN_FILES	If True, hides symbols that do not have readable .bin files.

Large values for MAX_BARS may use more memory and make the browser slower.

Dashboard browser controls

When the dashboard opens, the UI has these inputs:

UI input	What it does
Market	Selects spot, um, or cm.
Symbol	Selects a symbol found in the local data folder.
Rows	Number of recent bars to load and plot.
Plot type	Choose Candles + volume or Close line.
Refresh seconds	How often the dashboard checks for new data.
Plot	Manually reloads and redraws the chart.

The dashboard also displays:

Display	Meaning
Data root	The daily_bin folder currently being read.
Range	First and last available timestamps for the selected symbol.
Latest	Latest plotted candle.
Gaps plotted	Number of missing 1-minute bars detected in the plotted range.
Symbols in selected market	Summary table of symbols, files, rows, date range, and corrupt files.
Dashboard API endpoints

The FastAPI app exposes these endpoints:

Endpoint	Query inputs	What it returns
/	none	HTML dashboard page
/api/markets	none	Available markets, default market, counts, data paths, dtype item size
/api/symbols	market	Symbols found for the selected market
/api/summary	market	Summary table rows for all symbols in a market
/api/range	market, symbol	Available data range for one symbol
/api/ohlcv	market, symbol, bars	Latest OHLCV rows for plotting

Examples:

http://127.0.0.1:8050/api/markets
http://127.0.0.1:8050/api/symbols?market=spot
http://127.0.0.1:8050/api/summary?market=um
http://127.0.0.1:8050/api/range?market=spot&symbol=BTCUSDT
http://127.0.0.1:8050/api/ohlcv?market=spot&symbol=BTCUSDT&bars=500

API query input rules:

Input	Allowed values
market	One of the values in MARKETS, usually spot, um, or cm.
symbol	Binance symbol, for example BTCUSDT or BTCUSD_PERP.
bars	Integer from 1 to MAX_BARS.
Quick start
1. Configure the collector

Edit:

BASE_DIR = Path(r"C:\binance_ohlcv_data")
RUN_MODE = "rest72"

SYMBOLS_SPOT_LIVE_REST = ["BTCUSDT", "ETHUSDT"]
SYMBOLS_FUTURES_UM_LIVE_REST = ["BTCUSDT", "ETHUSDT"]
SYMBOLS_FUTURES_CM_LIVE_REST = ["BTCUSD_PERP"]

SYMBOLS_SPOT_BULK = ["BTCUSDT", "ETHUSDT"]
SYMBOLS_FUTURES_UM_BULK = ["BTCUSDT", "ETHUSDT"]
SYMBOLS_FUTURES_CM_BULK = ["BTCUSD_PERP"]

Run:

python binance_klines_cm_um_spot_NETWORK_ERRORS_HANDLED.py

This creates recent .bin files using REST.

2. Validate the output

Edit:

RUN_MODE = "validate"

Run:

python binance_klines_cm_um_spot_NETWORK_ERRORS_HANDLED.py

Check:

C:\binance_ohlcv_data\logs\klines_collector.log
3. Run live collection

Edit:

RUN_MODE = "live"

Run:

python binance_klines_cm_um_spot_NETWORK_ERRORS_HANDLED.py

For full live + REST + bulk collection:

RUN_MODE = "all"
4. Configure the dashboard

Edit the dashboard script:

BASE_DIR = Path(r"C:\binance_ohlcv_data")
DAILY_BIN_DIR = BASE_DIR / "daily_bin"

HOST = "127.0.0.1"
PORT = 8050
AUTO_OPEN_BROWSER = True

Run:

python dashboard_app_market_symbol_plotter.py

Open:

http://127.0.0.1:8050
Common setup examples
Small test setup

Use this before collecting many symbols:

RUN_MODE = "rest72"

SYMBOLS_SPOT_LIVE_REST = ["BTCUSDT"]
SYMBOLS_FUTURES_UM_LIVE_REST = ["BTCUSDT"]
SYMBOLS_FUTURES_CM_LIVE_REST = ["BTCUSD_PERP"]

SYMBOLS_SPOT_BULK = ["BTCUSDT"]
SYMBOLS_FUTURES_UM_BULK = ["BTCUSDT"]
SYMBOLS_FUTURES_CM_BULK = ["BTCUSD_PERP"]
Full automatic run
RUN_MODE = "all"

This starts:

live websocket collection,
latest 72-hour REST backfill,
historical Binance Vision bulk download/import.
Dashboard local-only mode
HOST = "127.0.0.1"
PORT = 8050

Use this if the dashboard is only for your own computer.
