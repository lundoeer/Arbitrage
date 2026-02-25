# Selling/Hedging/Buying Execution Lock Plan

Last updated: 2026-02-24
Scope: `#27_a` (design) and `#27_b` (state/gating infrastructure only)

## Objective

Add a higher-level global execution lock so only one execution flow is active per discovered market pair at a time, while keeping the existing `BuyFsmRuntime` intact.

The lock must remain active after execution submit until:

1. Required orders are in terminal status.
2. At least one successful authoritative positions REST poll has occurred after execution time for required venue(s).

This is strict fail-closed behavior (no timeout override), with no persistence across process restart.

## Finalized Decisions

1. Lock scope: global per active market pair.
2. Two-leg same-action execution is allowed (parallel legs remain allowed).
3. Architecture: keep `BuyFsmRuntime`; add separate higher-level global execution lock runtime.
4. Order-resolved definition: terminal only (`filled`, `canceled`, `rejected`, `expired`, `failed`).
5. Authoritative reconciliation requirement: positions REST poll only (not orders poll), and only for venue(s) required by the last execution outcome.
6. Fail policy: strict fail-closed.
7. Cooldown: keep existing buy cooldown behavior.
8. Future action priority when multiple are eligible: `selling > hedging > buying`.
9. `#27_b` implementation scope: state/gating infrastructure only (no sell/hedge signal/execution plumbing yet).
10. Partial submit rule: if one venue rejects and one submits, wait only on submitted venue terminal + positions poll.
11. Zero-submit rule: if both attempted legs reject/no submit, require one positions poll cycle on both attempted venues before unlock.
12. Hedging will use the same global execution lock.

## Recommended Design

Keep existing `BuyFsmRuntime` as submit-lifecycle state for buy path. Add a new runtime dedicated to cross-action lock semantics.

Reasoning:

1. Minimal blast radius for `#27_b`.
2. Preserves tested buy submit/cooldown behavior.
3. Clean separation:
   - `BuyFsmRuntime`: buy submit lifecycle.
   - `ExecutionLockRuntime`: global pair-level sequencing + authoritative post-trade verification.
4. Unified FSM can be revisited after sell/hedge execution paths are implemented and stable.

## Proposed Runtime Model

Add `scripts/common/execution_lock.py`.

### State

1. `UNLOCKED`
2. `LOCKED_AWAITING_TERMINAL`
3. `LOCKED_AWAITING_RECONCILE`

### Lock Context Fields

1. `lock_id`
2. `action_type` (`buy` now; later `sell`, `hedge`)
3. `created_at_ms`
4. `execution_completed_at_ms`
5. `attempted_venues`
6. `submitted_venues`
7. `required_terminal_venues`
8. `required_reconcile_venues`
9. `terminal_satisfied_venues`
10. `reconcile_satisfied_venues`
11. `last_transition_reason`

### Core Behavior

1. `can_start_execution()` returns allowed/blocked + reason.
2. `begin_execution(action_type, attempted_venues, now_ms)` acquires global lock before submit.
3. `record_execution_result(result_payload, now_ms)` computes required venues:
   - if any legs submitted: required venues = submitted venues
   - if no legs submitted: required venues = attempted venues
4. Terminal completion is satisfied only when required venues have no open orders for that lock context.
5. Reconcile completion is satisfied only after at least one successful positions REST poll after `execution_completed_at_ms` for each required venue.
6. Unlock only when both terminal and reconcile criteria are satisfied for all required venues.

## Integration Plan (`#27_b`)

## 1. Add Runtime Module

Create `scripts/common/execution_lock.py` with:

1. lock state enum
2. lock context dataclass
3. runtime class with transition/snapshot/gate methods
4. deterministic reasons for blocked state

## 2. Wire Into Engine Decision Loop

File: `scripts/run/engine_loop.py`

1. Instantiate `ExecutionLockRuntime` at segment start.
2. Before attempting any execution path, gate on lock:
   - if locked: block execution and record blocked reason.
3. On accepted execution attempt:
   - call `begin_execution(...)` before submit.
4. After execution result:
   - call `record_execution_result(...)`.
5. Each decision tick:
   - refresh lock terminal status from `PositionRuntime` open-order state.
   - refresh lock status into decision payload.

## 3. Feed Poll Success Into Lock Runtime

File: `scripts/run/engine_loop.py` `_position_poll_loop`

1. On each venue positions poll success (`status == ok`), call:
   - `execution_lock.mark_positions_reconcile_success(venue, poll_at_ms)`.
2. Poll success timestamp source:
   - use the poll event `at_ms` from `PositionReconcileLoop.run_once(...)`.

## 4. PositionRuntime Helper Additions

File: `scripts/common/position_runtime.py`

Add helper APIs needed by lock runtime checks:

1. `has_open_orders_for_venue(venue: str) -> bool`
2. optional snapshot helper for open-order counts by venue for logging/debug

Terminal check policy for now is venue-level (not per-order-id binding), consistent with "one order per market pair at a time" and current runtime scope.

## 5. Logging and Stats

Files: `scripts/run/engine_loop.py`, `scripts/common/engine_logger.py` (if new event kind needed)

Add to decision/buy-execution payloads:

1. `execution_lock_before`
2. `execution_lock_after`
3. `execution_lock_block_reason` when blocked

Logging toggle requirement:

1. Lock-related writes must be togglable, consistent with current logging behavior.
2. Do not introduce always-on dedicated lock log files.
3. Lock fields should appear only in enabled sinks (decision log, buy-execution log, position log).

Add counters:

1. `execution_lock_blocked_signals`
2. `execution_lock_transitions`

Add position log event on transitions:

1. `kind=position_execution_lock_transition` (or equivalent structured sub-kind)

## 6. Priority Hook (Infrastructure Only)

Add placeholder routing point for future multi-action evaluation order:

1. `selling`
2. `hedging`
3. `buying`

For `#27_b`, keep buy as the only executable action but structure code so future action handlers pass through the same lock gate.

## Acceptance Criteria

1. While lock is active, no new execution attempt starts.
2. Submitted-leg success path stays locked until:
   - required venue(s) have terminal order state, and
   - required venue(s) each have at least one successful positions poll after execution completion time.
3. One-submit/one-reject path waits only on submitted venue.
4. Zero-submit path waits for both attempted venues positions poll success.
5. Buy cooldown still functions after lock unlock path (no regression).
6. Engine remains fail-closed if required reconcile success does not occur.
7. Restart behavior remains non-persistent (lock resets on process start by design).

## Test Plan

Add/extend tests:

1. `tests/test_execution_lock_runtime.py`
   - submitted both venues -> terminal + reconcile required both
   - submitted one/rejected one -> required only submitted venue
   - both rejected -> required both attempted venues reconcile
   - reconcile success before execution time does not satisfy condition
   - lock stays closed until both conditions met
2. `tests/test_engine_loop_execution_lock.py` (or extend existing loop tests)
   - decision is blocked when lock active
   - lock transitions recorded in payload/stats
3. `tests/test_position_runtime_open_order_gate.py`
   - `has_open_orders_for_venue` reflects terminal vs non-terminal states

## Out of Scope (This Plan)

1. Sell signal generation (`#24`)
2. Hedge signal generation (`#26`)
3. Unified cross-action FSM replacement
4. Lock persistence across restart

## Follow-Up

After `#24/#26` land:

1. Route sell/hedge execution through the same lock runtime.
2. Re-evaluate whether to merge `BuyFsmRuntime` into a unified execution FSM.
