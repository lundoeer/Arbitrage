# Buy Execution Design (Inline FSM) - Implementation (22/02/2026)

This document describes the buy path as it is currently implemented in:

- `scripts/common/decision_runtime.py`
- `scripts/common/buy_execution.py` (includes `_post_order_with_retry` for Polymarket retry)
- `scripts/common/buy_fsm.py` (`BuyFsmRuntime` + `BuyFsmState`)
- `scripts/run/engine_loop.py` (orchestration)

## Scope and Intent

Implemented:

- Inline buy FSM in engine loop.
- `execution_plan` emission from decision runtime.
- `can_trade -> FSM gate -> execute_cross_venue_buy` wiring.
- Parallel per-leg venue submits (threaded) inside `execute_cross_venue_buy(...)`.
- In-memory idempotency (`signal_id` based).
- Optional buy execution JSONL logging.
- Runtime safeguards for live submit:
  - explicit opt-in (`--enable-buy-execution`)
  - max attempts per run (default `1`)
  - FSM gate

## Current Order Policy

Planner now emits:

- `order_kind = "market"`
- `time_in_force = "fak"`

Notes:

- Kalshi create-order currently requires side price fields, so market intent is emulated with aggressive priced limit orders.
- Polymarket CLOB signing still requires a price in signed order args, so market is emulated with limit-style signed order + `FAK`.
- Planner limit price is computed from runtime quote memory for both venues:
  - `limit_price = min(0.99, current_leg_best_ask + 0.02)`
- Kalshi submit payload sends `yes_price_dollars` / `no_price_dollars`.
- Executor fallback cap env var still exists (`POLYMARKET_MARKET_BUY_PRICE_CAP`) but planner now provides limit price directly for polymarket legs.

## Current Sizing Policy

Sizing is conservative and intentionally constrained by both spend and top-of-book liquidity:

- size is shared across both buy legs.
- planner computes three caps (contracts):
  - `cap_by_spend = max_spend_per_market_usd / total_ask`
  - `cap_by_config_size = max_size_cap_per_leg`
  - `cap_by_top_ask_after_safety_factor = min(yes_best_ask_size, no_best_ask_size) * best_ask_size_safety_factor`
- raw size:
  - `raw_size = min(cap_by_spend, cap_by_config_size, cap_by_top_ask_after_safety_factor)`
- rounding:
  - `size = floor(raw_size)` (whole contracts only)
- minimum gates (when configured > 0):
  - `size >= min_size_per_leg_contracts`
  - `yes_best_ask * size >= min_notional_per_leg_usd`
  - `no_best_ask * size >= min_notional_per_leg_usd`
- if top-of-book liquidity is insufficient after safety factor, plan build fails and buy is blocked for that tick.

This is labeled in plan policy as `sizing_mode = "top_of_book_budget_caps_with_min_thresholds"`.

## Config Usage Status (Current)

### `buy` config

Yes, these are actively used by the live engine:

- `buy.min_gross_edge_threshold`
  - used in buy-signal gating (`DecisionRuntime._evaluate_buy_signal_gate`).
  - if best candidate edge is below threshold, decision adds reason `min_gross_edge_threshold_not_met`.
- `buy.max_spend_per_market_usd`
  - hard cap via `cap_by_spend`.
- `buy.max_size_cap_per_leg`
  - hard per-leg cap via `cap_by_config_size`.
- `buy.best_ask_size_safety_factor`
  - scales visible top-of-book ask size before sizing.
  - valid range `(0, 1]`.
- `buy.min_size_per_leg_contracts`
  - minimum shared size in contracts (`0` disables).
- `buy.min_notional_per_leg_usd`
  - minimum per-leg notional at `best_ask` (`0` disables).

Important nuance:

- `buy_signal` still reports `"spend_cap_enforced": false`; actual enforcement is in plan sizing.
- If any cap/min constraint fails, plan build fails and `can_trade` is forced to false.

### `buy_execution.api_retry` config

This block is loaded and applied when buy clients are built:

- `enabled`, `max_attempts`, `base_backoff_seconds`, `jitter_ratio`, `include_post`
  - parsed from `config/run_config.json` by `load_buy_execution_runtime_config_from_run_config(...)`.
  - fed into `build_buy_execution_transport(...)` to create `ApiTransport(RetryConfig(...))`.

How it applies today:

- Kalshi order submit path (`ApiTransport.request_json("POST", ...)`) honors `RetryConfig.retry_methods`, so `include_post` directly affects Kalshi POST retry behavior.
- Polymarket submit path (`_post_order_with_retry`) uses `transport.retry.max_attempts` and backoff values directly for `post_order` retries.

### `buy_execution.parallel_leg_timeout_ms` config

- controls per-attempt parallel submit wait budget for leg workers in `execute_cross_venue_buy(...)`.
- default is `4000` ms.
- if one or more legs exceed this wait budget, those legs are returned as failed with `error=timeout_after_ms:<value>`.
- timeout does not guarantee network cancel at venue SDK layer; it bounds engine wait time for that decision tick.

## Parallel Submit Architecture (Current)

This section documents the actual implementation used in `scripts/common/buy_execution.py`.

### High-level behavior

- `engine_loop` and `DecisionRuntime` remain non-async from a trading-decision perspective.
- `execute_cross_venue_buy(...)` is still a synchronous function call from `_decision_loop`.
- inside that function only, leg submissions are executed concurrently via `ThreadPoolExecutor`.
- this keeps the current control flow (FSM/idempotency/gates) unchanged while reducing dual-leg submit wall time.

### Function structure

Current helper structure:

- `_build_leg_request_payload(...)`
  - deterministic request snapshot used for result/log payloads.
- `_submit_leg(...) -> LegSubmitResult`
  - resolves venue client
  - records `submit_started_ms` and `submit_completed_ms`
  - calls `client.place_buy_order(...)`
  - catches exceptions and returns `submitted=False` with `error`
  - never raises to caller in normal path
- `execute_cross_venue_buy(...)`
  - idempotency check
  - `mark_in_flight(...)`
  - compute deterministic `client_order_ids`
  - dispatch one worker per leg
  - wait up to `parallel_leg_timeout_ms`
  - convert unfinished work into timeout leg failures
  - rebuild final results in original leg order
  - compute status (`submitted` / `partially_submitted` / `rejected`)
  - single `mark_final(...)`

### Ordering and determinism

- workers run concurrently, but output ordering is deterministic.
- each leg carries `leg_index`.
- results are first collected in `results_by_index`, then emitted by iterating original `plan.legs` order.
- this guarantees stable output ordering regardless of which venue returns first.

### Timeout and failure semantics

- timeout is an execution wait budget, not a guaranteed HTTP cancel.
- after timeout expiry, unfinished legs are recorded as:
  - `submitted=False`
  - `error=timeout_after_ms:<configured_ms>`
- `future.cancel()` is attempted, but venue SDK/HTTP calls may continue in background if already running.
- any worker exception that escapes helper safety is mapped to:
  - `error=worker_failure:<ExceptionType>:<message>`
- missing venue client is represented as:
  - `error=missing_client_for_venue:<venue>`

### Result payload fields relevant to parallelization

Each leg in `BuyExecutionResult.legs[*]` now includes:

- `leg_index`
- `submit_started_ms`
- `submit_completed_ms`
- `submitted`
- `error`
- `request_payload`
- `response_payload`

These fields are written to `buy_execution_log__*.jsonl` when `--log-buy-execution` is enabled.

### Why this design was chosen

- preserves existing FSM and idempotency behavior (one signal at a time).
- avoids reworking venue clients into async SDK calls.
- provides bounded wait and clear observability for submit skew between venues.
- minimizes blast radius: decision/gating logic remains unchanged.

## Execution Plan Contract (Current)

`DecisionRuntime.evaluate(...)` emits `execution_plan` only when it can build a valid plan. If plan build fails, `can_trade` is forced to `False`.

Plan fields:

- `signal_id`: deterministic hash-based id over market identity + candidate + window end.
- `market`: pair identifiers (`polymarket_event_slug`, `polymarket_market_id`, `polymarket_token_yes`, `polymarket_token_no`, `kalshi_ticker`, `market_window_end_epoch_ms`, candidate metadata).
- `created_at_ms`
- `execution_mode`: currently `two_leg_parallel`
- `legs`: exactly 2 legs for cross-venue buy candidate
- `max_quote_age_ms`
- `policy`

Leg fields:

- `venue`: `polymarket` or `kalshi`
- `side`: `yes` or `no`
- `action`: `buy`
- `instrument_id`: PM token id or Kalshi ticker
- `order_kind`: currently `market`
- `limit_price`:
  - polymarket legs: computed in planner from current ask + 0.02 (capped at 0.99)
  - kalshi legs: computed in planner from current ask + 0.02 (capped at 0.99)
- `size`
- `time_in_force`: currently `fak`
- `client_order_id_seed`
- `metadata.planning_reference_best_ask`

## Engine Safeguards (Current)

Execution is blocked unless all are satisfied:

1. `decision.can_trade` is true.
2. `--enable-buy-execution` is on and buy clients initialize successfully.
3. Per-run attempt cap not exceeded:
   - CLI `--max-buy-execution-attempts`
   - default `1`
   - `0` means unlimited.
4. FSM can accept new signal (`IDLE` only).
5. `execution_plan` exists and includes a non-empty `signal_id`.

Additional duplicate guard:

- Executor idempotency blocks repeat submits for same `signal_id` with `skipped_idempotent`.

### Position Health Exposure Gate (Current)

Buy execution is also gated by position health and per-plan market exposure checks.

In `engine_loop`, when position gating blocks a submit, execution is blocked with:

- `buy_execution.status = "blocked_position_health"`
- `buy_execution.reason = "position_buy_execution_not_allowed"`
- counter increment: `buy_execution_blocked_position_health`

Exposure component for this path is evaluated per execution plan (per active discovered market window):

- config:
  - `position_monitoring.max_exposure_per_market_usd`
  - `position_monitoring.include_pending_orders_in_exposure`
- health fields:
  - `exposure_by_market.polymarket.*`
  - `exposure_by_market.kalshi.*`
  - each includes:
    - `gross_position_exposure_usd`
    - `gross_open_order_exposure_usd`
    - `gross_total_exposure_usd`
    - `exposure_limit_exceeded`
    - `buy_blocked`

Computation:

- for each market bucket (`polymarket`, `kalshi`):
  - `gross_total_exposure_usd = gross_position_exposure_usd + gross_open_order_exposure_usd`
  - if `max_exposure_per_market_usd > 0` and `gross_total_exposure_usd > max_exposure_per_market_usd`, that market bucket is `buy_blocked=true`
- runtime blocks all buy legs whenever one or more market buckets are `buy_blocked=true`.
- no projection is applied for the candidate order itself; gate uses current known positions and tracked open orders only.

Pricing policy used now:

- filled positions:
  - use `position_exposure_usd` when present
  - otherwise `abs(net_contracts) * avg_entry_price` (fallback to `1.0` if avg price missing)
- open/pending buy orders: `remaining_size * limit_price`
  - if remaining is missing, derive from `requested_size - filled_size` when possible
  - if price is missing/invalid, fallback to `1.0` (conservative)

REST exposure normalization source for position polls:

- Polymarket: `initialValue` (fallback `size * avgPrice`)
- Kalshi: `market_exposure_dollars`

Notes:

- Scope is only the currently discovered market window (current segment runtime), not portfolio-wide.
- This gate blocks all buy legs while allowing sell-only flows.
- Open-order exposure is reconciled against authoritative orders polls. For
  complete snapshots (`truncated_by_max_pages=false`), missing local orders are
  pruned after a short grace window to avoid stale reservation carryover.
- Null/malformed positions poll payloads are treated as poll failures for that venue; reconciliation is skipped so prior position state is preserved.

## FSM Model (Current)

States:

- `IDLE`
- `SUBMITTING`
- `AWAITING_RESULT`
- `COOLDOWN`

Implemented transitions:

- `IDLE -> SUBMITTING` on accepted signal.
- `SUBMITTING -> AWAITING_RESULT` after submit attempt returns.
- `AWAITING_RESULT -> COOLDOWN` when configured cooldown > 0.
- `AWAITING_RESULT -> IDLE` when cooldown is 0.
- `COOLDOWN -> IDLE` when cooldown elapsed (`maybe_rearm`).
- Any submit exception path transitions back to `IDLE` via failure handler.

### `maybe_rearm(...)` behavior

`maybe_rearm(now_epoch_ms=...)` is a small guard function called every decision tick before evaluating a new trade signal.

Checks in order:

1. If FSM state is not `COOLDOWN`, it returns immediately (no-op).
2. If state is `COOLDOWN` but `cooldown_until_ms` is missing, it re-arms immediately:
   - transition to `IDLE`
   - reason: `cooldown_missing_until`
3. If current time is at/after `cooldown_until_ms`, it re-arms:
   - clears `cooldown_until_ms`
   - transition to `IDLE`
   - reason: `cooldown_elapsed`
4. Otherwise it stays in `COOLDOWN`.

Concrete example (matches `tests/test_buy_fsm_runtime.py`):

- submit completed at `1200`, cooldown `250ms` => `cooldown_until_ms=1450`.
- `maybe_rearm(now=1449)` => still `COOLDOWN`.
- `maybe_rearm(now=1450)` => transitions to `IDLE`, new signals allowed.

## BuyIdempotencyState (Current)

`BuyIdempotencyState` is process-local in-memory dedupe state keyed by `signal_id`.

Stored per signal:

- lifecycle `status` (`in_flight`, `submitted`, `partially_submitted`, `rejected`, `error`, etc.)
- deterministic `client_order_ids`
- optional final `result` payload
- timestamps (`created_at_ms`, `updated_at_ms`)

Execution flow:

1. Executor checks `is_in_flight_or_completed(signal_id)`.
2. If true, it returns `status="skipped_idempotent"` and does not place new orders.
3. If false, executor:
   - plans client order ids,
   - calls `mark_in_flight(...)`,
   - submits legs,
   - computes final status,
   - calls `mark_final(...)`.

Examples:

- First call with a new signal id:
  - state miss -> submit legs -> final status typically `submitted`.
- Second call with the same signal id:
  - state hit -> immediate `skipped_idempotent`.
- Pre-marked in-flight signal:
  - if already `in_flight`, executor also returns `skipped_idempotent` with `prior_status="in_flight"`.

Scope caveat:

- State is not persisted across process restarts.
- In the engine, a submission sequence suffix (`__sN`) is appended before execution, so idempotency is per submission attempt id, not a cross-restart durable guard.

## Logging (Current)

Optional decision logs:

- `--log-decisions` -> `data/decision_log__<run_id>.jsonl`
- `--log-buy-decisions` -> `data/buy_decision_log__<run_id>.jsonl`

Optional execution logs:

- `--log-buy-execution` -> `data/buy_execution_log__<run_id>.jsonl`
- Includes:
  - `execution_plan`
  - `execution_plan_reasons`
  - `buy_execution` event (`executed`, `blocked_fsm`, `blocked_max_attempts`, `skipped_disabled`, `error`)
  - per-leg submit timing fields (`submit_started_ms`, `submit_completed_ms`) inside `buy_execution.result.legs[*]`
  - `buy_fsm_before`, `buy_fsm_after`
  - market context + timestamps

Run summary JSON includes buy execution counters and final FSM snapshot.

## End-to-End Run Flow (Buy Decision -> API Response Handling)

1. Engine poll loop ticks in `_decision_loop` in `scripts/run/engine_loop.py`.
2. FSM executes `maybe_rearm(...)` to move `COOLDOWN` back to `IDLE` when elapsed.
3. Engine reads health snapshots from Kalshi and Polymarket collectors.
4. Engine reads latest normalized quotes from `SharePriceRuntime`.
5. Engine calls `DecisionRuntime.evaluate(...)`.
6. Decision runtime:
   - validates quotes
   - computes buy candidates and gross edge
   - applies execution/time window gates
   - applies buy-signal threshold gate
   - if passing, builds deterministic `execution_plan`.
7. If plan cannot be built, decision runtime sets `can_trade = False` and records reasons.
8. Engine receives decision snapshot.
9. If `can_trade` is false, no buy submit is attempted for this tick.
10. If `can_trade` is true, engine evaluates safeguards in order:
    - max attempts per run
    - execution enabled
    - FSM accepts signal (`IDLE`)
    - plan exists
    - plan has `signal_id`
11. On first accepted signal, engine transitions FSM to `SUBMITTING`.
12. Engine increments run-level attempt counter (`attempts_used`).
13. Engine converts plan dict into `BuyExecutionPlan` dataclass.
14. Engine calls `execute_cross_venue_buy(...)`.
15. Executor checks idempotency state:
    - if signal already in-flight/final, returns `skipped_idempotent`.
16. Executor computes deterministic per-leg client order ids.
17. Executor marks signal `in_flight` in idempotency state.
18. Executor launches one worker per leg and submits venue calls in parallel:
    - Kalshi client: POST `/trade-api/v2/portfolio/orders`
      - submits `type=limit` with `yes_price_dollars` / `no_price_dollars` for market emulation.
    - Polymarket client: POST `/order` with signed payload
      - for market legs, applies configured cap price internally before signing.
19. Per leg, executor captures request payload, response payload (if any), error, and timing:
    - `submit_started_ms`
    - `submit_completed_ms`
20. Executor computes overall status:
    - `submitted`
    - `partially_submitted`
    - `rejected`
    - `skipped_idempotent`
21. Executor writes final result into idempotency state and returns `BuyExecutionResult`.
22. Engine stores result as `last_buy_execution`.
23. Engine transitions FSM:
    - `AWAITING_RESULT`
    - then `COOLDOWN` or `IDLE` based on cooldown config.
24. Engine updates buy execution counters by result status.
25. Engine writes optional decision/buy-decision logs.
26. If `--log-buy-execution` is enabled, engine writes one execution log row with:
    - plan
    - execution event/result
    - FSM transition snapshots before/after submit path.
27. At run end, summary JSON records aggregate counters, final decision, final buy execution, and final FSM snapshot.

### Buy Execution Counters: Exact Cases

Counters are updated in `scripts/run/engine_loop.py` as follows.

When `decision.can_trade == true`:

- `buy_execution_blocked_max_attempts += 1`
  - when `attempts_used >= max_attempts` (and `max_attempts > 0`)
  - event status: `blocked_max_attempts`
- `buy_execution_disabled_signals += 1`
  - when buy execution is disabled
  - event status: `skipped_disabled`
- `buy_execution_blocked_position_health += 1`
  - when position runtime exists and `position_health.buy_execution_allowed` is false
  - event status: `blocked_position_health`
- `buy_execution_blocked_fsm_signals += 1`
  - when FSM cannot accept signal (not `IDLE`)
  - event status: `blocked_fsm`
- `buy_execution_errors += 1`
  - when `execution_plan` is missing
  - when `execution_plan.signal_id` is missing
  - when submit throws exception
  - when execution returns unknown status
- `buy_execution_attempts += 1`
  - once a submit attempt starts (before the API call)

After `execute_cross_venue_buy(...)` result:

- `buy_execution_submitted += 1` when `result.status == "submitted"`.
- `buy_execution_partially_submitted += 1` when `result.status == "partially_submitted"`.
- `buy_execution_rejected += 1` when `result.status == "rejected"`.
- `buy_execution_skipped_idempotent += 1` when `result.status == "skipped_idempotent"`.
- otherwise `buy_execution_errors += 1`.

## Current CLI Controls (Buy Path)

- `--enable-buy-execution` / `--no-enable-buy-execution` (default from `config.buy_execution.enabled`)
- `--max-buy-execution-attempts` (default from `config.buy_execution.max_attempts_per_run`)
- `--buy-execution-cooldown-ms` (default from `config.buy_execution.cooldown_ms`)
- `--log-buy-execution` / `--no-log-buy-execution` (default off)

Config section in `config/run_config.json`:

```json
"buy_execution": {
  "enabled": false,
  "cooldown_ms": 0,
  "max_attempts_per_run": 1,
  "parallel_leg_timeout_ms": 4000,
  "api_retry": {
    "enabled": true,
    "max_attempts": 3,
    "base_backoff_seconds": 0.5,
    "jitter_ratio": 0.2,
    "include_post": true
  }
}
```

CLI values override config when explicitly provided.

`buy_execution.api_retry.include_post=true` enables POST retries/backoff for order submission clients only.
`buy_execution.parallel_leg_timeout_ms` currently has config control only (no dedicated CLI override).

## Current Env Vars Used by Buy Clients

Kalshi:

- `KALSHI_RW_API_KEY` or fallback key envs
- `KALSHI_PRIVATEKEY` or `KALSHI_PRIVATEKEY_PATH`

Polymarket:

- `POLYMARKET_L1_APIKEY`
- `POLYMARKET_L2_API_KEY`
- `POLYMARKET_L2_API_SECRET`
- `POLYMARKET_L2_API_PASSPHRASE`
- optional `POLYMARKET_CHAIN_ID`
- optional `POLYMARKET_SIGNATURE_TYPE`
- optional `POLYMARKET_FUNDER`
  - for `POLYMARKET_SIGNATURE_TYPE` in `{1,2}`, buy client init for each discovered market segment derives funder on-chain and compares.
  - mismatch raises hard init error and buy execution remains disabled for that segment.
- optional `POLYMARKET_FUNDER_RPC_URL` (single URL or comma-separated list for on-chain funder lookup override)
- optional `POLYMARKET_MARKET_BUY_PRICE_CAP` (default `0.99`)

Diagnostic helper:

- `scripts/diagnostic/get_polymarket_funder.py` resolves the on-chain funder wallet for signature types `1/2`.
- Example:
  - `.venv\Scripts\python.exe scripts/diagnostic/get_polymarket_funder.py --signature-type 1`
