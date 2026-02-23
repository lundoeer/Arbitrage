# Websocket Implementation (Current)

This document describes the current websocket implementation used by:

- `scripts/run/start_engine.py` / `engine_loop.py` (production engine, default no file logging)
- `scripts/diagnostic/log_pair_price_depth_health.py` (diagnostic sampler)

It is intentionally implementation-specific. It focuses on how this code parses fields, mutates runtime state, and writes saved outputs.

## Upstream Spec References

These are the vendor docs this implementation maps from:

- Kalshi websocket connection: `https://docs.kalshi.com/websockets/websocket-connection`
- Kalshi market ticker channel: `https://docs.kalshi.com/websockets/market-ticker`
- Kalshi public trades channel: `https://docs.kalshi.com/websockets/public-trades`
- Polymarket websocket overview: `https://docs.polymarket.com/developers/CLOB/websocket/wss-overview`
- Polymarket market channel: `https://docs.polymarket.com/developers/CLOB/websocket/market-channel`

Mapping highlights in this codebase:

- Kalshi uses one authenticated connection and sends `subscribe` with `channels` + `market_ticker`.
- Engine default channels are `ticker,orderbook_delta`.
- Polymarket subscribes to `MARKET` with `asset_ids` + `assets_ids` + `custom_feature_enabled`.
- Polymarket `book`, `price_change`, `best_bid_ask`, and `last_trade_price` are normalized to internal events.

## Scope and Components

Runtime components:

- `PolymarketWsCollector` (`scripts/common/ws_collectors.py`)
- `KalshiWsCollector` (`scripts/common/ws_collectors.py`)
- `SharePriceRuntime` (`scripts/common/decision_runtime.py`)
- `DecisionRuntime` (`scripts/common/decision_runtime.py`)

Transport/health:

- `BaseWsCollector` (`scripts/common/ws_transport.py`)
- writer sink is pluggable (`NullWriter` or `JsonlWriter`)
- Kalshi uses `headers_factory` pattern: auth headers are regenerated on each `websockets.connect()` call to ensure fresh signatures on reconnect. Diagnostic scripts may use static `headers=` for short-lived captures.

Normalization:

- `normalize_polymarket_event(...)` and `normalize_kalshi_event(...)` (`scripts/common/ws_normalization.py`)

## End-to-End Pipeline (Raw -> Saved)

Per incoming websocket frame:

1. Collector receives raw text frame.
2. Frame is JSON parsed.
3. Venue normalizer emits normalized event(s).
4. Collector health state updates from normalized event (`source_timestamp_ms`, `lag_ms`).
5. Runtime state updates via `on_event(...)` callback.
6. If logging sink is enabled, raw and normalized payloads are written.

Decision loop (`engine_loop.py`):

1. Read stream health snapshots.
2. Read runtime quote snapshot.
3. Evaluate decision gate in `DecisionRuntime.evaluate(...)`.

Persistence behavior:

- default engine mode: no file output for raw/events
- `--log-raw-events`: writes raw/events files and one summary JSON

## Raw Frame Schema

Both venues use this raw envelope when logging is enabled:

- `ts`: local UTC write timestamp
- `recv_ms`: local receive epoch ms
- `collector`: `polymarket_ws` or `kalshi_ws`
- `raw`: original websocket payload string

## Normalized Event Schemas

All normalized events include:

- `ts`
- `recv_ms`
- `collector`
- event-specific fields

Important clarification:

- below are normalized fields in `events_*.jsonl`
- raw vendor payload remains the escaped `raw` string in `raw_*.jsonl`

### Polymarket normalized events

`book_snapshot`:

- `asset_id`
- `best_bid`, `best_bid_size`
- `best_ask`, `best_ask_size`
- `bids`, `asks` ladders (`[[price,size], ...]`)
- `source_event_type`
- `source_timestamp_ms`, `lag_ms`

`book_top_update` (from `price_change`):

- `asset_id`
- `best_bid`, `best_ask`
- `changed_side` (`BUY` -> `bid`, else `ask`)
- `changed_price`, `changed_size`
- `best_bid_size`, `best_ask_size` currently `null` for `price_change`
- `source_timestamp_ms`, `lag_ms`

`book_best_bid_ask` (from `best_bid_ask`):

- `asset_id`
- `best_bid`, `best_ask`, `spread`
- `source_timestamp_ms`, `lag_ms`

`last_trade_price`:

- `asset_id`
- `trade_side`
- `trade_price`, `trade_size`
- `fee_rate_bps`
- `source_timestamp_ms`, `lag_ms`

`control_or_unknown`:

- passthrough for control/unknown messages

### Kalshi normalized events

`ticker`:

- `yes_bid`, `yes_ask`, `yes_bid_size`, `yes_ask_size`
- `no_bid`, `no_ask`, `no_bid_size`, `no_ask_size`
- `source_timestamp_ms`, `lag_ms`

Notes:

- normalizer uses direct NO fields if present in raw
- if NO fields are absent, fallback is complement from YES:
  - `no_bid = 1 - yes_ask`
  - `no_ask = 1 - yes_bid`
  - `no_bid_size = yes_ask_size`
  - `no_ask_size = yes_bid_size`

`kalshi_trade`:

- `yes_price`, `no_price`, `count`, `taker_side`
- `source_timestamp_ms`, `lag_ms`
- documented for completeness; current engine default does not subscribe to `trade`

`orderbook_event`:

- emitted when raw type contains `orderbook` or `delta`
- includes `source_event_type`, `payload`, `source_timestamp_ms`, `lag_ms`
- supports `orderbook_snapshot` and `orderbook_delta`

`control_or_unknown`:

- passthrough for control/unknown

## Raw vs Normalized: Concrete Mappings

Examples below are from these recent engine runs:

- run id `20260219T120434Z` (historical ticker/trade examples)
- run id `20260219T144900Z` (orderbook-enabled examples)
- summary: `data/arbitrage_engine_summary__20260219T144900Z.json`

### Polymarket `asset_id`: where it comes from and how it is used

`asset_id` comes from raw Polymarket market-channel messages and is mapped by discovery-selected tokens:

- `token_yes = 103782393188970396365780442877266472145355134401211602840209846344821237214198`
- `token_no = 33226457550805856297825151152514875678165236996465223337270850561165079450410`

Mapping is created in `NormalizedBookRuntime`:

- `{token_yes: "yes", token_no: "no"}`

Event update behavior:

- if `asset_id` matches selected token -> mutate corresponding `yes`/`no` book
- if not matched -> ignore event for memory mutation

### Polymarket example 1: raw `book` snapshot -> `book_snapshot` normalized -> memory replace (with sizes)

Raw row (`data/websocket_poly/raw_engine__btc-updown-15m-1771502400__1392134__20260219T120434Z.jsonl`):

- `recv_ms=1771502677236`
- raw payload is an array with two `event_type="book"` objects (YES + NO assets)
- for NO asset (`...450410`), top levels include:
  - bid `0.64 @ 4.44`
  - ask `0.65 @ 5.0`
  - `timestamp="1771502675651"`

Normalized row (`data/websocket_poly/events_engine__btc-updown-15m-1771502400__1392134__20260219T120434Z.jsonl`):

- `kind="book_snapshot"`
- `asset_id="332264...450410"`
- `best_bid=0.64`, `best_bid_size=4.44`
- `best_ask=0.65`, `best_ask_size=5.0`
- `source_timestamp_ms=1771502675651`
- `lag_ms=1585`

Memory mutation:

- target leg = `books["polymarket"]["no"]`
- replace bids/asks from snapshot (truncated to runtime depth)
- update leg timestamps

### Polymarket example 2: `price_change` raw -> `book_top_update` normalized -> memory incremental update

Raw row:

- `recv_ms=1771502677237`
- raw `event_type="price_change"` with `price_changes` array
- one update:
  - `asset_id="332264...450410"`
  - `side="BUY"`, `price="0.62"`, `size="214"`
  - `best_bid="0.64"`, `best_ask="0.65"`

Normalized row:

- `kind="book_top_update"`
- `asset_id="332264...450410"`
- `changed_side="bid"`
- `changed_price=0.62`
- `changed_size=214.0`
- `best_bid=0.64`, `best_ask=0.65`
- `best_bid_size=null`, `best_ask_size=null`

Memory mutation:

- target leg = `books["polymarket"]["no"]`
- incremental level update:
  - `bids.levels[0.62] = 214.0`
- top sizes are not overwritten by this event because `best_*_size` is null

### Polymarket example 2b: `best_bid_ask` raw -> `book_best_bid_ask` -> memory top re-anchor

Raw row:

- `recv_ms=1771502677669`
- `event_type="best_bid_ask"`
- `asset_id="103782...214198"`
- `best_bid="0.35"`, `best_ask="0.37"`, `spread="0.02"`

Normalized row:

- `kind="book_best_bid_ask"`
- `asset_id="103782...214198"`
- `best_bid=0.35`, `best_ask=0.37`, `spread=0.02`

Memory mutation:

- target leg = `books["polymarket"]["yes"]`
- re-anchor top prices:
  - remove stale bid levels above `0.35`
  - remove stale ask levels below `0.37`
  - set anchored top levels with inferred size when size is not directly provided

### Polymarket example 3: raw `last_trade_price` -> `last_trade_price` -> memory

Raw row:

- `recv_ms=1771502678290`
- `event_type="last_trade_price"`
- `asset_id="332264...450410"`
- `side="SELL"`, `price="0.64"`, `size="0.62"`

Normalized row:

- `kind="last_trade_price"`
- `trade_side="sell"`
- `trade_price=0.64`
- `trade_size=0.62`

Memory mutation:

- target leg = `books["polymarket"]["no"]`
- sell trade consumes bid depth:
  - `bids.levels[0.64] -= 0.62`
- remove level if size becomes `<= 0`

### Kalshi example 1: raw `orderbook_snapshot` -> `orderbook_event` -> memory snapshot replace

Raw row (`data/websocket_kalshi/raw_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`):

- `recv_ms=1771512544188`
- `type="orderbook_snapshot"`, `sid=3`, `seq=1`
- payload contains `yes` and `no` bid ladders (integer prices in cents), for example:
  - `yes`: `[ [1,4537], [2,18309], ... [62,176] ]`
  - `no`: `[ [1,5009], [2,18303], ... [37,290] ]`

Normalized row (`data/websocket_kalshi/events_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`):

- `kind="orderbook_event"`
- `source_event_type="orderbook_snapshot"`
- `source_timestamp_ms=null` (snapshot payload has no source time field)

Memory mutation:

- mark Kalshi orderbook mode active and mark snapshot received
- normalize `yes` and `no` ladders to probability bids (`1` -> `0.01`, etc.)
- replace full Kalshi bid books from snapshot (truncated to runtime depth)
- rebuild asks by complement:
  - `ask_yes = 1 - bid_no`
  - `ask_no = 1 - bid_yes`
- update Kalshi leg receive timestamps

### Kalshi example 2: raw `orderbook_delta` -> `orderbook_event` -> memory incremental depth update

Raw row:

- `recv_ms=1771512544208`
- `type="orderbook_delta"`, `sid=3`, `seq=2`
- payload example:
  - `side="yes"`
  - `price_dollars="0.5900"` (or `price=59`)
  - `delta_fp="-400.00"` (or `delta=-400`)
  - `ts="2026-02-19T14:49:02.491265Z"`

Normalized row:

- `kind="orderbook_event"`
- `source_event_type="orderbook_delta"`
- `source_timestamp_ms=1771512542491`
- `lag_ms=1717`

Memory mutation:

- delta is ignored unless a snapshot has already been applied
- parse `side` (`yes`/`no`), `price`, and signed `delta`
- apply signed size delta to that side's bid ladder at that price
- if resulting size is `<= 0`, remove the level
- rebuild opposite asks from updated bids
- update timestamps on the mutated Kalshi side

### Kalshi example 3: raw `ticker` -> `ticker` normalized -> memory top refresh

Raw row (`data/websocket_kalshi/raw_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`):

- `recv_ms=1771512545188`
- `type="ticker"`
- sample fields:
  - `yes_bid_dollars="0.6000"`, `yes_ask_dollars="0.6200"`
  - `yes_bid_size_fp="177.00"`, `yes_ask_size_fp="194.00"`
  - `ts=1771512543`, `time="2026-02-19T14:49:03.388344Z"`

Normalized row (`data/websocket_kalshi/events_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`):

- `kind="ticker"`
- `yes_bid=0.6`, `yes_ask=0.62`
- `yes_bid_size=177.0`, `yes_ask_size=194.0`
- `no_bid=0.38`, `no_ask=0.4`
- `no_bid_size=194.0`, `no_ask_size=177.0`
- `source_timestamp_ms=1771512543000`
- `lag_ms=2188`

Memory mutation:

- ticker-only mode:
  - ticker is treated as L1 snapshot
  - clear Kalshi yes/no bids and asks
  - repopulate top levels from ticker fields
- orderbook-active mode (current engine default):
  - keep depth (do not clear books)
  - refresh top bids from ticker, prune stale higher bid levels
  - rebuild asks from opposite-side bids
  - update Kalshi receive/source timestamps

## What Is Actually Stored In Memory

`SharePriceRuntime` memory:

- `book_runtime.books`:
  - `books["polymarket"]["yes"]`
  - `books["polymarket"]["no"]`
  - `books["kalshi"]["yes"]`
  - `books["kalshi"]["no"]`
- each leg stores:
  - `bids.levels` (`price -> size`)
  - `asks.levels` (`price -> size`)
  - `source_timestamp_ms`
  - `recv_timestamp_ms`

## Memory -> Saved Output

Engine (`scripts/run/engine_loop.py`):

- default: no raw/events are written
- with `--log-raw-events`: raw/events and one summary are written

Diagnostic samplers still produce additional derived files:

- `scripts/diagnostic/capture_btc_15m_ws_price_feeds.py`
- `scripts/diagnostic/log_pair_price_depth_health.py`

LACKING EXAMPLE NOTICE:

- current example run `20260219T144900Z` was engine mode, not diagnostic sampler mode, so no `websocket_share_price_feed__*.jsonl` or `websocket_pair_price_depth_health__*.jsonl` rows were produced in that run.

## In-Memory Saved State (Runtime)

`SharePriceRuntime` keeps:

- `book_runtime` (4-leg normalized books)

### Order book data model

Each leg is `OutcomeBook`:

- `bids.levels`: dict of `price -> size`
- `asks.levels`: dict of `price -> size`
- `source_timestamp_ms`
- `recv_timestamp_ms`

Price keys are rounded to 6 decimals.

### Polymarket state update rules

`book_snapshot`:

- map `asset_id` to `yes`/`no`
- replace full bid/ask state for that leg
- update timestamps

`book_top_update`:

- apply changed side/price/size level update
- apply `best_bid`/`best_ask` only when sizes are present
- update timestamps

`book_best_bid_ask`:

- re-anchor top bid/ask prices without explicit sizes
- prune stale levels that violate new top

`last_trade_price`:

- buy trade consumes asks
- sell trade consumes bids
- remove depleted level when size `<= 0`

### Kalshi state update rules (price logger path)

Current behavior in shared runtime (used by engine and logger):

- engine default channels are `ticker,orderbook_delta`
- `orderbook_snapshot` handling:
  - set `kalshi_orderbook_snapshot_received=true` and orderbook mode active
  - replace full Kalshi `yes` and `no` bid books from snapshot ladders
  - derive asks from opposite bid books (`ask_yes = 1 - bid_no`, `ask_no = 1 - bid_yes`)
  - mark receive timestamps on both Kalshi outcomes
- `orderbook_delta` handling:
  - ignored unless `kalshi_orderbook_snapshot_received=true`
  - parse `side`, `price` (`price_dollars` or `price`), and signed `delta` (`delta_fp` or `delta`)
  - apply signed size delta to selected side bid level; remove level when resulting size `<= 0`
  - rebuild asks from opposite bids and mark timestamps on the mutated outcome
  - no separate quote object is mutated per event; quotes are derived from current book state in `book_runtime.executable_price_feed(...)`
- `ticker` handling:
  - ticker-only mode: reset Kalshi L1 state and repopulate top levels
  - orderbook-active mode: do not clear depth; refresh top bids, prune stale higher bids, rebuild asks
- `kalshi_trade` handling:
  - order books unchanged
  - ignored by `SharePriceRuntime`

## Quote Derivation Used By Saved Samples

Quote derivation comes from `book_runtime.executable_price_feed(...)`:

- `quotes` payload now contains only `legs` (no duplicated venue buckets)
- `best_bid`, `best_bid_size`, `best_ask`, `best_ask_size`
- `mid`, `spread`, `spread_bps` when both sides exist
- `source_timestamp_ms`, `recv_timestamp_ms`
- `quote_age_ms`

LACKING EXAMPLE NOTICE:

- current run `20260219T144900Z` did not emit periodic quote sample files; quote derivation is visible in `final_decision.quote_sanity` inside `data/arbitrage_engine_summary__20260219T144900Z.json`.

## Health State and Trade Gate

Collector health is tracked in `BaseWsCollector`:

- connection state/reconnect state
- transport heartbeat age (`last_transport_recv_ms`)
- market-data heartbeat age (`last_data_recv_ms`)
- source timestamp age/presence
- lag thresholds

Health snapshot returns:

- `transport_ok` with `transport_reasons`
- `market_data_ok` with `market_data_reasons`
- `decision_ok = transport_ok AND market_data_ok`

### Health Architecture (On-Demand Query)

Health is owned by each collector (`BaseWsCollector`) and updated event-by-event.
It is not pushed into `SharePriceRuntime` memory as a persistent health object.

Operational flow:

1. Collector receives frames and normalizes events.
2. Collector updates internal health fields from normalized events (`last_transport_recv_ms`, `last_data_recv_ms`, `last_source_timestamp_ms`, `last_lag_ms`).
3. Decision loop queries `kalshi_collector.health_snapshot(now_epoch_ms=...)` and `polymarket_collector.health_snapshot(now_epoch_ms=...)` on demand.
4. `DecisionRuntime.evaluate(...)` gates with `decision_ok` from those snapshots.

Implication:

- runtime memory snapshots (`share_price_runtime_memory`) contain books/quotes only
- stream health is evaluated at decision time, using latest collector state

Decision gate (`DecisionRuntime.evaluate(...)`):

- `health_can_trade = kalshi.decision_ok AND polymarket.decision_ok`
- `decision_ready` from canonical quote sanity checks
- `can_trade = health_can_trade AND decision_ready`

Health limits are loaded from `config/run_config.json` (`health` section):

- `transport_heartbeat_stale_seconds`
- `market_data_stale_seconds`
- `max_lag_ms`
- `require_source_timestamp`
- `max_source_age_ms`

From run `20260219T144900Z` final summary:

- `decision_ready=true`
- `health_can_trade=false`
- `can_trade=false`
- blocker was health (`transport_reasons` / `market_data_reasons`)

## Persisted Files and Update Cadence

Engine without logging:

- no raw/event files
- no summary JSON

Engine with `--log-raw-events`:

- `data/websocket_poly/raw_engine__*.jsonl` (every Polymarket frame)
- `data/websocket_poly/events_engine__*.jsonl` (every normalized Polymarket event)
- `data/websocket_kalshi/raw_engine__*.jsonl` (every Kalshi frame)
- `data/websocket_kalshi/events_engine__*.jsonl` (every normalized Kalshi event)
- `data/arbitrage_engine_summary__*.json` (final write once)

Current example files:

- `data/websocket_poly/raw_engine__btc-updown-15m-1771512300__1392370__20260219T144900Z.jsonl`
- `data/websocket_poly/events_engine__btc-updown-15m-1771512300__1392370__20260219T144900Z.jsonl`
- `data/websocket_kalshi/raw_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`
- `data/websocket_kalshi/events_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`
- `data/arbitrage_engine_summary__20260219T144900Z.json`

## Implementation Notes (Current)

- One canonical normalization path exists in `scripts/common/ws_normalization.py`.
- One canonical collector path exists in `scripts/common/ws_collectors.py`.
- Engine and diagnostics share the same collector/normalization/runtime code paths.
- Logging is sink-driven (`NullWriter` vs `JsonlWriter`) in the same engine process path.

## Kalshi Trade Channel (Reference Only, Not Currently Used)

Current default behavior:

- engine default channels are `ticker,orderbook_delta` (no `trade` subscription)
- `SharePriceRuntime` ignores `kalshi_trade` events
- order books are driven by `orderbook_snapshot` + `orderbook_delta` + ticker top refresh logic

Historical example kept for reference:

### Kalshi example: raw `trade` -> `kalshi_trade` normalized -> memory unchanged

Raw row (`data/websocket_kalshi/raw_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`):

- `recv_ms=1771512544290`
- `type="trade"`, `seq=1`
- `yes_price_dollars="0.6300"`, `no_price_dollars="0.3700"`
- `count=1`, `taker_side="yes"`, `ts=1771512542`

Normalized row (`data/websocket_kalshi/events_engine__KXBTC15M-26FEB191000-00__20260219T144900Z.jsonl`):

- `kind="kalshi_trade"`
- `yes_price=0.63`, `no_price=0.37`
- `count=1.0`, `taker_side="yes"`
- `source_timestamp_ms=1771512542000`
- `lag_ms=2290`

Memory mutation:

- order books unchanged
