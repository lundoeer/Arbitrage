# Implementation Plan: Task #24 (Two-Leg Selling)

Last updated: 2026-02-24  
Scope: Implement two-leg cross-venue selling based on bids (no one-leg sell; one-leg remains hedging task #26).

## Confirmed Requirements

1. Implement two-leg cross-venue sell only.
2. Sell pairings mirror buy pairings:
   - `sell_polymarket_yes_and_kalshi_no`
   - `sell_kalshi_yes_and_polymarket_no`
3. Sell signal rule:
   - `total_bid - 1.0 >= sell.min_gross_edge_threshold`
4. Sell size policy:
   - limited by available positions (authoritative runtime position state),
   - bid-liquidity cap with safety factor,
   - config cap (`max_size_cap_per_leg`, where `0` disables),
   - minimum size/notional gates (`0` disables).
5. Available position source:
   - `PositionRuntime` net contracts per leg (`max(0, net_contracts)`).
6. Sell order market emulation pricing:
   - `limit_price = max(0.01, best_bid - sell.market_emulation_slippage)`
   - default slippage `0.02` (configurable).
7. Use same global execution lock and lock semantics.
8. Cooldown is global and remains under `buy_execution.cooldown_ms`.
9. Buy max-attempt cap remains buy-only.
10. Add dedicated `sell` config block and default most gates disabled by `0`.
11. Reuse existing log sinks; add `action` field and avoid file renames.
12. Priority when both eligible in same tick:
   - sell first, then buy.
13. Fail-closed:
   - sell execution requires position monitoring/authoritative health path.

## Architecture Strategy

Use a minimal-extension approach that mirrors existing buy path and reuses infrastructure:

1. Keep existing `BuyFsmRuntime` for now as the submit lifecycle gate (global cooldown behavior unchanged).
2. Keep `ExecutionLockRuntime` as the cross-action serialization gate.
3. Extend decision runtime to emit both buy and sell candidates/plans.
4. Add sell execution plumbing parallel to buy execution while reusing shared primitives where possible.
5. Preserve existing logs and summary schemas; extend with sell-specific fields.

## Planned Code Changes

## 1) Config Model and Parsing

Files:

1. `scripts/common/run_config.py`
2. `config/run_config.json`

Changes:

1. Add `SellDecisionConfig` and wire into `DecisionConfig`.
2. New `sell` config keys:
   - `min_gross_edge_threshold` (default `0.0`)
   - `max_size_cap_per_leg` (default `0.0`, means disabled)
   - `min_size_per_leg_contracts` (default `0.0`, disabled)
   - `min_notional_per_leg_usd` (default `0.0`, disabled)
   - `best_bid_size_safety_factor` (default `1.0`)
   - `market_emulation_slippage` (default `0.02`)
3. Add `sell_execution` runtime config:
   - `enabled` (default false)
   - `parallel_leg_timeout_ms` (default 4000)
   - `api_retry` block (same shape as buy execution retry)
4. Keep cooldown under `buy_execution.cooldown_ms`.
5. Keep buy-only `max_attempts_per_run` under `buy_execution`.

## 2) Decision Runtime: Sell Signal + Plan

File:

1. `scripts/common/decision_runtime.py`

Changes:

1. Add sell candidate generation using bid legs:
   - `sell_polymarket_yes_and_kalshi_no`
   - `sell_kalshi_yes_and_polymarket_no`
2. Compute sell edge:
   - `gross_edge = total_bid - 1.0`
3. Add sell signal evaluation gate using `sell.min_gross_edge_threshold`.
4. Build `sell_execution_plan` analogous to buy plan:
   - `action="sell"`
   - `order_kind="market"`
   - `time_in_force="fak"`
   - sell limit price derived from bid/slippage policy
5. Sell size calculation (whole contracts):
   - `cap_by_position = min(available_position_leg1, available_position_leg2)`
   - `cap_by_top_bid_after_safety_factor`
   - `cap_by_config_size` (ignore when config `0`)
   - `size = floor(min(caps...))`
   - min size/notional gates (`0` disables)
6. Preserve existing buy output fields for compatibility; add sell fields:
   - `sell_signal`
   - `sell_signal_ready`
   - `sell_execution_plan`
   - `sell_execution_plan_reasons`

## 3) Execution Client Abstraction for Sell

Files:

1. `scripts/common/buy_execution.py` (rename deferred, internal extension now)
2. `scripts/common/engine_setup.py`

Changes:

1. Extend venue client protocol to support sell calls, either:
   - add `place_sell_order(...)`, or
   - unify to action-aware method (preferred long-term, larger refactor).
2. Implement sell submit for Kalshi and Polymarket with existing signing/transport paths.
3. Reuse parallel-leg submit executor structure and result schema.
4. Keep existing idempotency mechanism; signal ids remain deterministic and action-specific.

## 4) Engine Loop Integration (Sell-First)

File:

1. `scripts/run/engine_loop.py`

Changes:

1. Add sell execution enable/runtime config inputs from startup wiring.
2. Add sell-first branch in decision loop:
   - if sell plan eligible, process sell path before buy path.
   - if sell executes or blocks at execution gate level, buy is not attempted that tick.
3. Keep buy max attempts only on buy path.
4. Reuse global cooldown (`buy_execution.cooldown_ms`) for sell path transitions too.
5. Reuse execution lock for sell path exactly as buy path.
6. Reuse fail-closed requirement for position monitoring availability.

## 5) Position Runtime / Exposure Compatibility

File:

1. `scripts/common/position_runtime.py`

Changes:

1. Ensure sell submits are represented with `action="sell"` in order state.
2. Confirm open-order exposure calculation continues to include open buy-only reservation (current behavior) unless explicitly changed.
3. Reuse existing helper APIs (`has_open_orders_for_venue`, per-venue open checks) for lock release.

## 6) Logging and Summary (Reuse Existing Sinks)

Files:

1. `scripts/run/engine_loop.py`
2. `scripts/common/engine_logger.py` (only if payload helpers need small extension)

Changes:

1. Keep existing files:
   - `decision_log__*.jsonl`
   - `buy_decision_log__*.jsonl` (temporary compatibility; may hold sell-first selected action context)
   - `buy_execution_log__*.jsonl` (temporary compatibility; include `action`)
2. Add `action` field to execution event payloads.
3. Add sell counters in run stats:
   - attempts/submitted/partially_submitted/rejected/errors/blocked_*.
4. Add `selected_action` in decision payload to make routing decisions explicit.

## Acceptance Criteria

1. Engine can generate sell signals from bid-side opportunities.
2. Sell executes two-leg only; one-leg sell is not executed in #24.
3. Sell size never exceeds available position on either required leg.
4. Sell uses configurable slippage market emulation from bids.
5. Sell path is gated by:
   - position health,
   - global execution lock,
   - FSM cooldown gate.
6. Buy max-attempt cap applies only to buy path.
7. Sell is evaluated/executed before buy in same tick.
8. Existing log files remain; payloads include action-specific context.
9. If sell signal exists but sell plan cannot be built due to insufficient positions, sell is discarded for that tick and buy evaluation proceeds normally (no buy block from non-executable sell).

## Testing Plan

Add/extend tests:

1. `tests/test_decision_runtime_selling.py`
   - sell candidate scoring from bids
   - threshold pass/fail
   - plan build pass/fail reasons
   - sizing by position cap + bid-liquidity cap + config cap
   - slippage-derived limit price
2. `tests/test_sell_execution_parallel.py`
   - two-leg sell parallel submit success/partial/reject/timeout
3. `tests/test_engine_loop_sell_priority.py`
   - sell preferred over buy when both eligible
   - buy attempts unaffected when sell not eligible
   - buy max-attempt cap not applied to sell
4. Extend `tests/test_execution_lock_runtime.py` integration coverage if needed for sell action metadata.
5. Extend `tests/test_position_runtime.py` for sell submit ack/order-state handling.

## Rollout Steps

1. Introduce config + parser changes.
2. Implement decision-runtime sell logic and plan emission.
3. Implement sell execution path in client/executor layer.
4. Integrate sell-first routing in engine loop.
5. Add counters/log fields.
6. Add tests and run full suite.
7. Update docs (`trading.md`, `position_monitoring.md`, `run_scripts_overview.md`) to include sell path.

## Out of Scope for #24

1. One-leg hedging execution logic (#26).
2. Unified FSM refactor (deferred).
3. Log file renaming/migration.
4. Cancellation executor policy (#25_b).
