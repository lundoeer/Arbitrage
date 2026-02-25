# Unified FSM / Selling / Hedging Notes

Last updated: 2026-02-24

## Disclaimer

Current implementation is **not** a unified FSM.

What exists now is:

1. Existing buy submit FSM (`BuyFsmRuntime`) remains in place.
2. A new higher-level global execution lock (`ExecutionLockRuntime`) gates execution across the active market pair.

Unified buy/sell/hedge FSM is deferred until sell and hedge execution flows are implemented and stable.

## Purpose

Document the currently implemented high-level execution lock that enforces:

1. One execution flow at a time per active discovered market pair.
2. Strict fail-closed unlock criteria based on terminal order resolution plus authoritative positions poll success.

## Implemented Components

### `scripts/common/execution_lock.py`

Introduces:

1. `ExecutionLockState`
   - `UNLOCKED`
   - `LOCKED_AWAITING_TERMINAL`
   - `LOCKED_AWAITING_RECONCILE`
2. `ExecutionLockContext`
   - tracks lock identity, action, attempted/submitted venues, required venues, and satisfaction progress
3. `ExecutionLockRuntime`
   - `can_start_execution()`
   - `begin_execution(...)`
   - `record_execution_result(...)`
   - `refresh_terminal_status(...)`
   - `mark_positions_reconcile_success(...)`
   - `snapshot()`

### `scripts/run/engine_loop.py`

Integrated lock orchestration:

1. Instantiate runtime at segment start.
2. Block execution when lock is closed.
3. Acquire lock before buy submit attempt.
4. Record execution result to set required venues.
5. Refresh terminal status from `PositionRuntime` open-order view.
6. Feed positions poll success events into lock reconcile tracking.
7. Include lock fields in decision and buy-execution payloads.

### `scripts/common/position_runtime.py`

Added helpers for lock evaluation:

1. `has_open_orders_for_venue(venue)`
2. `open_order_counts_by_venue()`

These provide deterministic venue-level open/closed status for terminal gating.

## Lock Policy (As Implemented)

## Scope

Global lock is scoped to the active discovered pair segment.

## Allowed Parallelism

Two-leg same-action execution remains allowed within one execution attempt.
The lock governs starting another execution attempt, not per-leg parallel submission.

## Required Venues

After execution result is recorded:

1. If any legs were submitted: required venues = submitted venues only.
2. If no legs were submitted: required venues = attempted venues.

This matches the approved rules for partial-reject and full-reject outcomes.

## Unlock Criteria

Lock unlocks only when both are true for all required venues:

1. Terminal criterion:
   - no open orders remain for required venues.
2. Reconcile criterion:
   - at least one successful positions REST poll occurred at or after execution completion time.

## Fail-Closed Behavior

No timeout override is implemented.
If required terminal/reconcile conditions are not met, lock remains closed.

No persistence across restart is implemented (process-local state).

## Interaction With Existing Buy FSM

`BuyFsmRuntime` still controls buy submit lifecycle:

1. `IDLE -> SUBMITTING -> AWAITING_RESULT -> COOLDOWN -> IDLE`

`ExecutionLockRuntime` sits above this and blocks new execution attempts across actions (buy now, sell/hedge later).

## Decision Loop Guard Order (Current)

For `decision.can_trade == true`, engine currently checks:

1. max attempts per run
2. execution enabled
3. position health gate
4. execution lock prerequisites (position monitoring present) and lock open
5. buy FSM gate
6. execution plan presence and signal id
7. per-plan position exposure gate
8. submit attempt path

## Logging Behavior (Togglable)

No new always-on lock log file was added.

Lock data is emitted only through existing toggled sinks:

1. `decision_log` (`--log-decisions`)
2. `buy_execution_log` (`--log-buy-execution`)
3. `position_monitoring_log` (`--log-positions`) for lock transitions

## Current Limitations

1. Unified buy/sell/hedge FSM is not implemented yet.
2. Sell/hedge signal generation/execution is still pending (`#24`, `#26`).
3. Lock state is process-local only (no restart persistence).

## Next Phase (Planned)

1. Route selling and hedging execution through the same lock runtime.
2. Add action prioritization execution router (`sell > hedge > buy`).
3. Re-evaluate migration from split FSM+lock model to a true unified FSM.
