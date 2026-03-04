# Log Reverse Strategy Implementation Plan

## Objective

Implement a persistent per-market run log for reverse strategy runs in:

`logs/log_reverse_strategy.jsonl`

Constraints from task #41:

- One persistent file.
- One row per market started during a run (derived 15m boundary).
- Row can remain open during run; if run ends early, write partial row with unresolved fields empty.
- Use internal engine reason codes for `next_order_decided_by`.

## Format Decision

Use JSONL (append-only, one object per market).

Why JSONL over CSV:

- Fields become available at different times.
- Some fields are naturally nullable/partial until finalize.
- Easier to include timestamps, diagnostics, and explicit nulls without fragile CSV patching.

## Row Schema (Planned)

Required fields:

- `market_start` (ISO UTC at 00/15/30/45 boundary)
- `previous_market_resolve` (`"yes"|"no"|null`)
- `order_side` (`"Up"|"Down"|null`)
- `order_price` (float|null)
- `order_size` (float|null)
- `orders_filled_when_first_seconds` (float|null)
- `orders_filled_when_full_seconds` (float|null)
- `orders_unfilled` (float|null)
- `result_usd` (float|null)
- `next_order_decided_by` (internal reason code|string|null)
- `account_balance` (float|null; polymarket cash only)

Recommended supporting fields for observability:

- `run_id`
- `market_slug`
- `market_id`
- `condition_id`
- `finalized_at`
- `finalization_status` (`"final"|"partial_on_shutdown"`)
- `account_balance_method` (`"cash_only"|"unavailable"`)

## Field-by-Field Data Source Plan

### 1) `market_start`

Source:

- Derived boundary from `_window_start_epoch_s(now_ms)` in reverse loop.

Implementation:

- Create row when window transitions to new `current_window_start_s`.
- Also support pre-start order fills by allowing early row creation at order decision time for the target market
  (upsert by `target_market.window_start_s`).
- When market-start transition occurs, merge with any pre-created row instead of creating a duplicate.
- Convert epoch to ISO UTC string (`datetime.fromtimestamp(..., tz=UTC).isoformat()` with `Z` formatting).

Write timing:

- At row creation (immediately when market starts).

### 2) `previous_market_resolve`

Source:

- Authoritative-only resolution source (no threshold/price-inference logic):
  - Primary: Polymarket market-channel resolution event, if available in subscribed feed.
  - Fallback: Polymarket closed-positions endpoint (`GET /closed-positions`) for the user, keyed by `conditionId`.
- Do not use `MarketWindow.official_resolution` from the current 0.99 threshold helper.

Implementation:

- Update row whenever authoritative resolution is observed.
- Keep `null` while unresolved/unavailable.

Write timing:

- Can be populated any time during active market.
- If still unavailable at shutdown: leave `null`.

### 3) `order_side`, `order_price`, `order_size`

Source:

- Existing order attempt payload from `_place_reverse_order` response (`order_result["request"]`).
- `order_side` mapping: strategy `yes -> Up`, `no -> Down`.

Implementation:

- On the first order attempt for target market, store:
  - side mapping,
  - limit price,
  - requested size.
- If no order attempted for a started market, keep nulls.

Write timing:

- At order attempt moment.

### 4) `orders_filled_when_first_seconds`

Source:

- Fill events from `PolymarketUserFillAdapter.drain_fill_events()`.
- Fallback: immediate submit-ack fill (`_is_fill_response` + normalized `filled_size`) if user WS arrives late.

Implementation:

- For each market row, track first observed positive fill timestamp.
- Compute: `(first_fill_ts_ms - market_start_ms) / 1000.0`.
- Negative value allowed (premarket fill).
- If fill arrives before market row is started, store it against the pre-created target-market row.

Write timing:

- First time any fill delta > 0 is observed.

### 5) `orders_filled_when_full_seconds`

Source:

- Same fill stream as above.

Implementation:

- Track cumulative filled shares per market row.
- When cumulative filled >= `order_size` (within epsilon), set full-fill timestamp.
- Compute seconds relative to market start.

Write timing:

- When full fill is first reached.

### 6) `orders_unfilled`

Source:

- Market close cutoff (`market_end + 10s`) and cumulative filled shares captured up to cutoff.

Implementation:

- At `market_end + 10s`, freeze close-fill quantity:
  - `filled_at_close = cumulative_filled_up_to_cutoff`.
- Compute `max(order_size - filled_at_close, 0.0)`.
- If no order existed, set null.

Write timing:

- Exactly once at close cutoff per market.

### 7) `result_usd`

Source:

- Authoritative resolution of the same market from the same source policy as `previous_market_resolve`
  (market-channel resolution event preferred, closed-positions fallback).
- Filled shares and realized fill cost from row-tracked fill data.

Formula:

- `payout = filled_shares * 1.0` if order side matches resolved outcome, else `0.0`.
- `result_usd = payout - fill_cost_usd`.
- `fill_cost_usd` from executed fills (`sum(fill_delta * fill_price)`); if per-fill price missing, fallback to `order_price * filled_shares` and mark method.

Implementation notes:

- `Up` wins when official resolution is `yes`.
- `Down` wins when official resolution is `no`.
- Ignore unfilled shares.

Write timing:

- When both close-cutoff fill state and official resolution are available.
- If unresolved at shutdown: null.

### 8) `next_order_decided_by`

Source:

- Existing `determination["reason"]` used at order decision time.

Implementation:

- Persist exact internal reason string from decision path:
  - e.g. `last_trade_threshold_upper`, `official_market_resolution`, `rtds_start_end_fallback`.

Write timing:

- At order attempt/decision.

### 9) `account_balance`

Source:

- Polymarket account collateral balance via `PolymarketAccountPollClient.fetch_balance_allowance()`.

Implementation:

- At `market_end + 10s`, fetch cash balance.
- Convert on-chain units to USD (`balance / 1_000_000`).
- Use cash only (ignore unclaimed winnings).
- Set `account_balance_method=\"cash_only\"` on success; `\"unavailable\"` on fetch failure.

Write timing:

- At close cutoff finalize pass.

## Runtime Design

Add a dedicated logger runtime in reverse strategy:

- `ReverseMarketLogRow` dataclass (in-memory row state).
- `ReverseStrategyMarketLogRuntime` manager:
  - `on_window_start(...)`
  - `on_order_attempt(...)`
  - `on_fill_event(...)`
  - `on_periodic_tick(...)` (resolution/account checks + finalize rows)
  - `flush_shutdown_partial(...)`

Resolution ingestion architecture (shared/common):

- Extend existing Polymarket normalization pipeline in
  `scripts/common/ws_normalization.py` with a dedicated normalized event kind
  (planned: `polymarket_market_resolution`) instead of creating a separate standalone script.
- Add a shared REST fallback helper under `scripts/common/` for
  `GET /closed-positions` parsing keyed by `conditionId`.
- Reverse strategy log runtime consumes both sources through one internal
  resolution-update path (WS-first, REST fallback).

Persistence model:

- Keep rows mutable in memory while run is active.
- Append one JSON object to `logs/log_reverse_strategy.jsonl` only when row is finalized.
- On shutdown, append all still-open rows as `finalization_status="partial_on_shutdown"` with missing fields null.

## Integration Points in `start_reverse_strategy.py`

1. Startup:

- Initialize log runtime with path `PROJECT_ROOT / "logs" / "log_reverse_strategy.jsonl"`.

2. Window transition block:

- Call `on_window_start(current_window_start_s, market metadata)`.

3. Candidate decision/order attempt block:

- When determination is resolved and order is attempted, call `on_order_attempt(...)`.

4. Fill drain block (already present for user WS):

- Forward each fill event to `on_fill_event(...)`.

5. Main loop periodic tick:

- Call `on_periodic_tick(now_ms, api_transport, account_client, user_address)` to perform:
  - previous-market official resolve updates,
  - close+10 unfilled freeze,
  - result_usd finalize,
  - account balance capture.

7. Common parser integration:

- Consume resolution events normalized by `scripts/common/ws_normalization.py`.
- Use shared closed-positions fallback helper when WS resolution event is missing.

6. Shutdown (`finally`):

- Call `flush_shutdown_partial(...)` before process exit.

## Finalization Conditions Per Row

A row is `final` when all are ready:

- `orders_unfilled` computed (close+10 reached for that market and order status frozen).
- `result_usd` computed or deterministically null due to no order.
- `account_balance` computed (or unavailable with explicit method marker).

A row is `partial_on_shutdown` when process exits before final conditions.

## Error Handling

- Never fail strategy execution due to log-write issues.
- Guard each external call (market/account queries) with try/except.
- Keep row fields null on temporary failures and retry next tick (until shutdown).
- Add lightweight counters in summary:
  - `reverse_log_rows_final`
  - `reverse_log_rows_partial`
  - `reverse_log_account_fetch_errors`
  - `reverse_log_resolution_fetch_errors`

## Validation Plan

1. Dry-run smoke:

- Confirm rows are created for each started market even with no orders.

2. Live short run:

- Verify one appended row per started market.
- Check first/full fill seconds with known filled orders.

3. Shutdown test:

- Interrupt run mid-market and confirm partial rows are appended with null unresolved fields.

4. Consistency check:

- For resolved winning rows: `result_usd` matches `(filled_shares*1) - fill_cost`.

## Implementation Sequence

1. Add row dataclass + log runtime class.
2. Wire lifecycle hooks in `start_reverse_strategy.py` main loop.
3. Add account balance fetch integration (Polymarket only).
4. Add resolve/finalize periodic checks and shutdown flush.
5. Add summary counters + targeted tests.
