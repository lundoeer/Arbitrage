# Position Monitoring & Engine Cleanup Plan

**Target:** Refactor and clean up Tasks #12, #15, and #16 implementation.

## 1. Separation of Concerns: `start_engine.py` vs Core Engine

**The Issue:**
You are absolutely right—logging setup, writer management, CLI parsing, and the component wiring for websockets and position clients make up hundreds of lines in `arbitrage_engine.py`. This deeply obscures the core arbitrage logic and makes the engine hard to test in isolation.

**The Solution:**

- **Create `scripts/run/start_engine.py`:** This new script will serve as the actual entry point. It will handle `engine_cli.py` parsing, load `.env`, read configuration files, build all the websocket collectors and position clients, set up the log writers, and manage the `KeyboardInterrupt` graceful shutdown.
- **Refactor `arbitrage_engine.py`:** Convert the current massive script into a clean, reusable class or function (e.g., `class ArbitrageEngine` or a streamlined `run_engine` function). It will accept pre-configured components (collectors, runtime, log writers) and focus _exclusively_ on the trading loop, state management, and strategy execution.
- **Dynamic Stats Aggregation:** Inside the core engine loop, remove the massive 60-line block of hardcoded dictionary assignments for `stats`. Instead, iterate through the keys dynamically using `collections.Counter` or a simple loop: `for k, v in segment_stats.items(): stats[k] = stats.get(k, 0) + v`.

## 2. Refactor state management in `position_runtime.py`

**The Issue:**
Currently, `positions_by_key` and `orders_by_client_order_id` map to raw Python dictionaries. This requires manual dict-key lookups (`rec["net_contracts"]`) and lacks type safety.

**The Solution:**

- **Use `@dataclass`:** Introduce well-defined dataclasses like `PositionState` and `OrderState`.
- **Encapsulate Mutation:** Move logic like computing average entry price inside the `PositionState` class. Instead of manipulating dictionaries, `PositionRuntime` will just call `state.apply_fill(...)`.

## 3. Clean up `position_polling.py` utility redundancy

**The Issue:**
`position_polling.py` re-defines a few small casting helpers like `_to_float` and `_to_outcome_side` which already exist or overlap with `scripts/common/utils.py` and `ws_normalization.py`.

**The Solution:**

- Consolidate these parsing utilities into `scripts.common.utils` (or reuse existing ones) and import them cleanly.

## 4. Documentation Polish

**The Issue:**
`docs/position_monitoring.md` still reads like a planning document, talking about "Phased Implementation Steps" that are already done.

**The Solution:**

- Update `docs/position_monitoring.md`.
- Remove the phases section, update verb tenses from "Proposed" to standard description, and convert the "Test Plan" into an "Existing Test Coverage" section representing reality.

---

**Summary:**
Moving the CLI, logging, and component wiring into a dedicated `start_engine.py` script is a fantastic best practice. This refactoring process will significantly reduce the size of `arbitrage_engine.py`, make the in-memory states strictly typed and safer, and bring the documentation up to date as a final reflection of reality rather than a draft proposal.
