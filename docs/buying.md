# Buy Execution Design (Inline FSM) - Current Implementation

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
- In-memory idempotency (`signal_id` based).
- Optional buy execution JSONL logging.
- Runtime safeguards for live submit:
  - explicit opt-in (`--enable-buy-execution`)
  - max attempts per run (default `1`)
  - FSM gate

Out of scope (still):

- Hedge/unwind strategy.
- Portfolio follow-up logic after partial one-sided fills.
- Durable idempotency storage (state is process-local).

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

Sizing is conservative and intentionally small:

- `size = min(max_size_cap_per_leg, max_spend_per_market_usd / total_ask)`
- Then size is rounded down to a whole contract/share using floor:
  - `size = floor(size)`
- Same size is used for both legs.
- `max_spend_per_market_usd` comes from `config/run_config.json` (`buy.max_spend_per_market_usd`).
- `max_size_cap_per_leg` comes from `config/run_config.json` (`buy.max_size_cap_per_leg`, current default `20.0`).

This is labeled in plan policy as `sizing_mode = "budget_with_configured_cap_per_leg"`.

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
18. Executor iterates plan legs and submits each using venue client:
    - Kalshi client: POST `/trade-api/v2/portfolio/orders`
      - submits `type=limit` with `yes_price_dollars` / `no_price_dollars` for market emulation.
    - Polymarket client: POST `/order` with signed payload
      - for market legs, applies configured cap price internally before signing.
19. Per leg, executor captures request payload, response payload (if any), and error.
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
