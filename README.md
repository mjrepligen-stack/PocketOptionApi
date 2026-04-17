# PocketOption API (Python)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Unofficial Python client for the PocketOption WebSocket API. **Version 2.0** exposes a synchronous high-level class, `PocketOption`, built on a single WebSocket layer (no separate data-feed module).

## Topics

Suggested GitHub topics for discovery:

`pocketoption` · `trading` · `binary-options` · `websocket` · `socket-io` · `python` · `api-client` · `real-time` · `ohlc` · `financial-data`

## Features

### Connection and resilience

- **Auto-reconnection** — `WebsocketClient.connect()` walks the candidate server list with exponential backoff (base 1s, cap 10s) and up to **`max_reconnect_attempts` (5)** before raising `ConnectionError`. Failed attempts increment `reconnect_attempts`; auth failures (`41` / `NotAuthorized`) stop the loop immediately.
- **Backoff on the message loop** — `websocket_listener` is wrapped with **`backoff`** (exponential, max 5 tries / 30s) for transient `WebSocketException` / `ConnectionError` while reading frames.
- **Keepalive** — Background `42["ps"]` ping every ~20s while connected.
- **Clean disconnect** — `PocketOption.disconnect_websocket()` closes the socket on the shared asyncio loop (useful before swapping SSID).

### Account mode and endpoints (automatic)

- **Demo vs real from SSID** — On init, `WebsocketClient._parse_ssid()` reads **`isDemo`** and **`uid`** from the wire string and rebuilds auth with `json.dumps` (PHP-style session strings stay valid). If parsing fails, it **defaults to demo** and logs a warning.
- **Server selection** — **Demo:** EU demo endpoints only, fixed order (`constants.REGION.DEMO_SERVERS`). **Real:** all real endpoints (`REAL_SERVERS`); if there is more than one URL, the client **probes TCP latency in parallel** and tries **fastest-first** (unreachable hosts are still tried last). No manual region configuration is required for real accounts.

### Time and candles

- **Time synchronization** — Initial offset from the HTTP `Date` header on WebSocket upgrade; refined with `updateStream` ticks; `TimeSynchronizer` exposes synced time and **server-native** time for history requests.
- **Historical candles** — `loadHistoryPeriod` via `GetCandles`, optional **reconciliation** with `updateHistoryNewFast` and stream ticks (strip forming bar, replace stale bars, gap fill when tick coverage allows).
- **Auth handshake** — Payload includes `platform: 2`, `isFastHistory: true`, `isOptimized: true`. The client treats the session as ready on **`successauth`** (not on earlier events such as `updateAssets`).

### Market data and trading

- **Asset catalog** — After `updateAssets`, each symbol maps to `id`, `name`, `category`, **`payout`** (%), **`is_available`**, **`timeframes`**, and **`raw`** (full server row). Use `get_assets()`, `get_payout(asset)`.
- **Balance** — `successupdateBalance` updates `global_value.balance` / `balance_updated`; optional `balance_type` mirrors server `isDemo`.
- **Sentiment** — `chafor` messages fill per-asset values; `get_sentiment(asset)`.
- **Real-time ticks** — Ring buffer per asset (`subscribe` → `updateStream`); `get_realtime_ticks`.
- **Orders** — `buy` via `Buyv3` (includes account demo flag from the same `is_demo` as SSID); `check_win` / `get_order_result` for closures.

### Stack and dependencies

- **Single WebSocket stack** — `WebsocketClient` handles Socket.IO frames and binary payloads (`451-[type, binary]`); `PocketOptionAPI` holds deques/buffers per asset; `PocketOption` is the stable entry point.
- **Dependencies** — `websockets>=12`, `pandas`, `tzlocal`, `backoff` (see `requirements.txt`). The client uses `extra_headers` on websockets v12 and `additional_headers` on v13+.

The old **`datafeed`** module was removed; all streaming logic lives in `ws/client.py`.

## Architecture (v2.0)

| Layer | Role |
|--------|------|
| `pocketoptionapi.ws.client.WebsocketClient` | Connect, SSID parse, server selection, auth, reconnection, message routing, tick/history handlers. |
| `pocketoptionapi.api.PocketOptionAPI` | Per-asset stream buffers, `updateHistoryNewFast` cache, asset catalog, history/order events. |
| `pocketoptionapi.stable_api.PocketOption` | Public methods: `connect`, `subscribe`, `get_historical_candles`, `buy`, `check_win`, etc. |

## Requirements

- Python **3.9+** recommended.
- Install runtime deps:

```bash
pip install -r requirements.txt
```

Editable install from this repo:

```bash
pip install -e .
```

> **Note:** `examples/ml_trading_bot.py` uses `scikit-learn` and `numpy`, which are not listed in `requirements.txt`. Install them separately if you run that example.

## Quick start

The public API is **synchronous**: `connect()` runs the WebSocket on a background thread. Use `check_connect()` and `is_time_synced()` before requesting candles.

```python
import logging
import time
from pocketoptionapi import PocketOption

logging.basicConfig(level=logging.INFO)

SSID = r'''42["auth",{...}]'''  # Your captured Socket.IO auth string

api = PocketOption(SSID)
ok, err = api.connect()
if not ok:
    raise RuntimeError(err)

# Wait until socket is up and time sync is ready (required for history)
while not (api.check_connect() and api.is_time_synced()):
    time.sleep(0.1)

api.subscribe("EURUSD_otc", period=60)
time.sleep(1)

candles = api.get_historical_candles(
    "EURUSD_otc",
    period=60,
    offset=45000,  # Large offset avoids server timeouts on loadHistoryPeriod
    count_request=1,
)
if candles:
    print(len(candles), "bars")
```

See `test.py` for a minimal end-to-end script (balance, assets, payout, sentiment, ticks).

## SSID format

Pass the full wire string you would send after `42`, e.g.:

`42["auth",{"session":"...","isDemo":1,"uid":123456,"platform":2,...}]`

The client extracts `session`, `isDemo`, and `uid`, then builds a fresh auth message with the fields the server expects. Real-mode sessions may be long PHP-serialized strings; do not truncate or manually escape quotes—`json.dumps` handles them.

## Connection and time sync

1. Call `connect()` → returns `(True, None)` or `(False, reason)`.
2. Poll until `check_connect()` and `is_time_synced()` are true. **`get_historical_candles` returns `None` if time is not synced.**
3. For candle-aligned logic, use `get_server_datetime()` / `get_server_timestamp()` (synced UTC-based estimates).

### Reconnection behavior (implementation summary)

| Mechanism | Where | Behavior |
|-----------|--------|----------|
| Connect loop | `WebsocketClient.connect` | Tries each URL in the current list; on failure, backoff sleep and retry until connected, auth error, or **5** failed rounds across the outer loop. |
| Read loop | `websocket_listener` | `@backoff.on_exception` retries transient read errors before surfacing. |
| Auth failure | `41` / `NotAuthorized` | Sets `global_value.check_websocket_if_error`; no further server attempts for that session. |

To **close** the socket from your code (e.g. before a new SSID), call `disconnect_websocket()`.

## Main API (`PocketOption`)

### Connection

| Method | Description |
|--------|-------------|
| `connect()` | Start WebSocket thread; wait up to ~30s for connection. Returns `(success: bool, error: str \| None)`. |
| `disconnect_websocket(timeout=12.0)` | Schedule `websocket.close()` on the client event loop; returns whether a running loop was found. |
| `check_connect()` | Whether `global_value.websocket_is_connected` is true. |
| `is_time_synced()` | Whether `TimeSynchronizer` has references and offset (needed for history). |

### Time

| Method | Description |
|--------|-------------|
| `get_server_timestamp()` | Unix seconds (synced). |
| `get_server_datetime()` | Timezone-aware `datetime` (local zone via `tzlocal`). |

### Account and market info

| Method | Description |
|--------|-------------|
| `get_balance()` | Last balance if `balance_updated`, else `None`. |
| `get_assets()` | Dict keyed by symbol → `id`, `name`, `category`, `payout`, `is_available`, `timeframes`, `raw`. |
| `get_payout(asset)` | Payout % or `None`. |
| `get_sentiment(asset)` | Sentiment from `chafor` cache or `None`. |
| `get_open_deals()` / `get_closed_deals()` / `get_pending_deals()` | Shallow copies of server-fed lists. |

After successful auth, the server may send an account id; the client stores it in **`global_value.account_id`** when present in the `successauth` payload.

### Real-time data

| Method | Description |
|--------|-------------|
| `subscribe(asset, period=60)` | `changeSymbol` + `subfor` for `updateStream` / `updateHistoryNewFast`. |
| `get_realtime_ticks(asset=None, limit=100)` | Recent `(timestamp, price)` from the in-memory ring buffer. |

### Historical OHLC

| Method | Description |
|--------|-------------|
| `get_historical_candles(active, period, start_time=None, offset=None, count_request=1)` | Loads via `loadHistoryPeriod`; merges/reconciles with fast history and stream ticks. |

**`offset`:** Sent to the server as the `offset` field (candle window / count). **`None` sends null.** In practice, use a **large positive value** (e.g. `45000` or `9000`) so requests do not time out.

**`count_request`:** Multiple paginated rounds; after each response, the end time steps backward from the oldest candle.

After fetch, `_reconcile_with_realtime` may:

- Remove the **forming** candle.
- **Replace** recent server bars with tick-built OHLC when bars look non-finalized and tick coverage is above ~70%.
- **Fill gaps** using ticks when possible.

### Data helpers

| Method | Description |
|--------|-------------|
| `process_candles_data(data, period)` | Build a **pandas** `DataFrame` with local wall-clock `time` string and numeric `timestamp`. |
| `process_candle(candle_data, period)` | Sort, dedupe, forward-fill; returns `(df, is_continuous)`. |

### Trading

| Method | Description |
|--------|-------------|
| `buy(amount, active, direction, expiration)` | Sends `openOrder` (via `Buyv3`). `direction` is sent as `action` (e.g. `call` / `put`). `expiration` is duration in **seconds**. Returns `(order_dict, True)` on matching `requestId`, else `({}, False)`. |
| `check_win(id_number, callback=None)` | Queues resolution against `successcloseOrder` / `_order_results`; optional callback with deal dict. |
| `get_order_result(id_number)` | Snapshot of cached result if `deals` present. |

There is **no** high-level `close()` on `PocketOption`; use `disconnect_websocket()` or end the process. Lower-level `PocketOptionAPI.websocket` exposes async helpers if you extend the client.

## Error types

Defined in `pocketoptionapi.ws.client`:

- `WebSocketError`
- `AuthenticationError`
- `ConnectionError`
- `MessageError`

Connection failures are also surfaced via `connect()`’s second return value and `global_value.websocket_error_reason`.

## Logging

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
```

## Candle periods

Supported bar sizes (seconds) include:

`1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 604800, 2592000`

(see `PocketOption.CANDLE_SIZES`.)

## Assets

Forex, crypto, indices, commodities, and stock symbols depend on PocketOption’s catalog. Use `get_assets()` for live **`payout`**, **`is_available`**, and **`timeframes`**. Static name/id hints remain in `pocketoptionapi.constants.ACTIVES`.

## Examples

| File | Description |
|------|-------------|
| `test.py` | Connection, balance, assets, subscribe, sentiment, ticks. |
| `examples/ml_trading_bot.py` | Fetch candles, train a small classifier, loop (educational only). |
| `examples/real-time-data+chart.py` | Real-time chart demo. |
| `examples/buy+check.py` | Order placement pattern. |

## Security

- Do not commit or share live SSIDs or session strings.
- Prefer environment variables or secret stores for credentials in real apps.
- This library disables TLS verification on the bundled `requests` session in `PocketOptionAPI` (`session.verify = False`) for compatibility with some environments—harden this for production if you rely on HTTP helpers.

## Disclaimer

This is an **unofficial** API. It is not affiliated with PocketOption. Trading involves risk; the authors are not responsible for financial losses. Examples are for education and testing only.

## Contributing

Contributions are welcome. Please keep code style consistent, add tests when possible, and update this README when public behavior changes.

## License

MIT License.
