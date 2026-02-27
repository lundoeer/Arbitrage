# tasks.md

Last updated: 2026-02-24

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
- [x] #29 - Create a script that looks at the currently avialable data\account_portfolio_snapshot_logs and shows all changes
- [x] #28 -Implement max exposture per market. This is each market in the active market pair. -> if over all buying blocked on that market.
- [x] #25_a order-state ingestion/status reconciliation.
- [x] #31: Corrections: I want to measure exposure based on market_exposure from kalshi and initial value from polymarket. If one market is above max exposure then buying should be closed.
- [x] #30 - Parallelized buy leg submits inside `execute_cross_venue_buy` with per-leg timeout + timing fields + tests.
- [x] #22 - Implemented buy sizing with top-of-book liquidity cap, safety factor, and configurable minimum contracts/notional thresholds.

- [x] #27_a -Design states for what is open: {buying, selling, hedging}, remember only one order per market at a time. orders must be resolved before and resulting positions updated by authoratative api calls before the next order is placed. Maybe the current FSM is already handeling this otherwise consider if this is new or a change to FSM where does this belong? Maybe it is logic to go from Awaiting_result to cooldown currently it just auto moves as far as i can see.

- [x] #27_b -Implement states for what is open: {buying, selling, hedging}, remember only one order per market at a time. orders must be resolved before and resulting positions updated before a order is placed.

- [x] #24 -Implement selling positions based on bids - this should also check size but have its own thresholds and such and the size is limited to current positions, and it is checking bid sizes not ask like buying i think. I would like this to mirror and reuse the implementations from buying as much as possible. I am thinking they are very similar. the main difference is determining sizing, I am unsure how different the order types are in selling for the venues. If these are very different this would cause more changes
- [x] #36 - Implement market_emulation_slippage config for buying. to replace the current hardcoded value.
- [x] #35 - Implement get trades today and yesterday, with optional flag for other period.

## Now

## Next

- [ ] #37 - Improve Polymarket order-failure diagnostics for `PolyApiException[status_code=None, error_message=Request exception!]`: log underlying transport exception context (type/message/trace), record per-attempt timing + retry metadata, and explicitly distinguish transport failures from API-limit responses (e.g., 425/429) in buy execution logs.
- [ ] #33 - Implement market surveillance - target price on market start and through chainlink - and regular current price surveillance. It should be evenly spread between target and price differences. investigate this also
- [ ] #36 - always log failed order and their responses.

- [ ] #34 unify locks for sell/hedge and buy
- [ ] #32 remove fallbacks from tests so the fail when they are meant to

## Later

- [ ] #26 -Implement hedging based on positions and a check for unfilled orders. Hedging is one-leg selling to avoid unbalanced positions
- [ ] #9 — Lag timestamp precision detection (`Improve_lag.md` #1 + #6) — 1 hr
- [ ] #10 — Rolling lag stats in health snapshots (`Improve_lag.md` #3) — 1 hr, depends on #9
- [ ] #11 — Partial-submit alert (stderr on `partially_submitted`) — 10 min

## Blocked

- None

## Implementation uncertain - under consideration

- [ ] #25_b cancellation policy + cancellation executor
