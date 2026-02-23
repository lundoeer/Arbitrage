# tasks.md

Last updated: 2026-02-21

Purpose: Maintain the active execution queue with clear priorities, ownership, and status.

How this file is structured:

- `Done`: Recently completed tasks.
- `Now`: Immediate tasks that are actively being worked.
- `Next`: Ready tasks waiting for available capacity.
- `Later`: Lower-priority tasks or deferred work.
- `Blocked`: Tasks that need external input or dependency resolution.

## Done

- [x] Add `__init__.py` files to package dirs
- [x] Consolidate duplicate helpers → `scripts/common/utils.py`
- [x] Extract `BuyFsmRuntime` → `scripts/common/buy_fsm.py`
- [x] Extract edge snapshots → `scripts/common/edge_snapshots.py`
- [x] Extract CLI parser → `scripts/run/engine_cli.py`
- [x] Kalshi auth reconnect fix — `headers_factory` pattern
- [x] Polymarket `post_order` retry wrapper
- [x] Live integration test — 2 segments, 13K+ Kalshi / 27K+ Polymarket messages
- [x] #12 — Design position monitoring system (order status polling, fill tracking, net position)
- [x] #15 — Build `PositionRuntime` — tracks open positions, fills, and net exposure
- [x] #16 — Integrate position monitoring into engine loop
- [x] #18 — Refactor Engine Architecture
- [x] #19 — Refactor `PositionRuntime` to use `@dataclass` for state management
- [x] #20 — Clean up `position_polling.py` utility redundancies
- [x] #21 — Polish `docs/position_monitoring.md` documentation

## Now

awaiting user decision

## Next

- [ ] #29 - Create a script that looks at the currently avialable data\account_portfolio_snapshot_logs and shows all changes in the time, kalshi and polymarket balances and the total of the two with only these four fields pr. line an only new lines when there are changes. also kalshi and polymarket balances need to be converted to dollars currently kalshi is in cents and polymarket is in milicents i think.
- [ ] #22 - Implement sizing based on mainly on current best_size but maybe also book
- [ ] #23 -Implement minimum sizing so both sides of the trade is above a configurable threshold.
- [ ] #24 -Implement selling positions based on bids - this should also check size
- [ ] #25 -Implement order monitoring and cancellations - this is hopefully not super relevant with the current order types, but i need to know if i have any orders in place.
- [ ] #26 -Implement hedging based on positions and a check for unfilled orders. Integrated with drift_tolerance
- [ ] #27 -Implement states for what is open: {buying, selling, hedging}, remember only one order per market at a time. orders must be resolved before and resulting positions updated before a order is placed. Maybe the current FSM is already handeling this otherwise consider if this is new or a change to FSM where does this belong? Maybe it is logic to go from Awaiting_result to cooldown currently it just auto moves as far as i can see.
- [ ] #28 -Implement max exposture per market -> if over all buying blocked on that market.

## Later

- [ ] #9 — Lag timestamp precision detection (`Improve_lag.md` #1 + #6) — 1 hr
- [ ] #10 — Rolling lag stats in health snapshots (`Improve_lag.md` #3) — 1 hr, depends on #9
- [ ] #11 — Partial-submit alert (stderr on `partially_submitted`) — 10 min

## Blocked

- None
