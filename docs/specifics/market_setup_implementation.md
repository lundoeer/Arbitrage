# Market Setup Implementation Plan (Tasks #40 and #39)

Last updated: 2026-03-02

Scope: Add a market listing workflow for manual cross-venue pairing (#40), then allow engine runs from a manually defined pair file instead of BTC15m discovery (#39).

## Goal

Implement two connected capabilities:

1. Task #40:
   - Discover active YES/NO markets from both venues.
   - Filter to markets ending within 24 hours from current time.
   - Provide dollar-volume context for manual evaluation.
   - Emit copy/paste-ready JSONL setup fragments in two files (one per venue).

2. Task #39:
   - Run arbitrage engine on one manually configured market pair.
   - Bypass discovery completely when setup file is provided (hard override).

## Confirmed Requirements

1. Market scope: any YES/NO markets (not only BTC 15m).
2. Pairing logic in #40: listing only, manual pairing by operator.
3. Time window: ending within next 24 hours from now.
4. Size metric: dollar volume only.
5. Status filter: active markets only.
6. Output format: JSONL.
7. Output split: separate files for Polymarket and Kalshi.
8. Setup file: one pair only for now.
9. Runtime behavior: run one pair per process.
10. Setup precedence: hard override discovery.
11. Trading defaults: use existing config behavior; if execution config is missing, execution remains disabled by default.
12. `condition_id`: optional in setup file.
13. For non-literal Polymarket outcomes (for example team-vs-team), setup must include explicit side orientation via `yes_outcome` or `token_yes`/`token_no`.
13. Manual mode startup enrichment failure: hard fail immediately (no fallback).
14. Manual mode with `--duration-seconds 0`: auto-stop at `window_end`.
15. Missing `dollar_volume`: keep row and normalize to `0.0`.

## Current-State Constraints

Current engine selection path is discovery-driven and BTC15m-specific:

1. `scripts/common/market_selection.py` loads selected markets only from discovery output.
2. `scripts/run/discover_active_btc_15m_markets.py` enforces strict BTC15m slug/ticker patterns.
3. `scripts/run/start_engine.py` repeatedly discovers per segment.

Engine runtime itself is mostly pair-agnostic once it gets:

1. Polymarket:
   - `event_slug`
   - `market_id`
   - `condition_id`
   - `token_yes`
   - `token_no`
2. Kalshi:
   - `ticker`

## Proposed Deliverables

1. New market listing script for #40:
   - `scripts/diagnostic/list_active_yesno_markets.py`

2. New setup file contract for #39:
   - `config/market_setup.json` (single pair)

3. Selection loader update:
   - Extend `scripts/common/market_selection.py` with setup-file loader and Polymarket enrichment.

4. Engine CLI/runtime integration:
   - `scripts/run/engine_cli.py`
   - `scripts/run/start_engine.py`

5. Documentation updates:
   - README usage snippet for manual setup run.
   - short note in `docs/run_scripts_overview.md`.

## Task #40 Design: Market Listing Script

## Script Interface (Proposed)

```powershell
.venv\Scripts\python.exe -m scripts.diagnostic.list_active_yesno_markets `
  --config config/run_config.json `
  --output-polymarket data/diagnostic/market_setup_candidates_polymarket.jsonl `
  --output-kalshi data/diagnostic/market_setup_candidates_kalshi.jsonl `
  --window-hours 24 `
  --active-only `
  --max-pages 50 `
  --page-size 200
```

## Output Contract (JSONL)

One row per market with minimal fields for decision making and setup linking.

Polymarket example row:

```json
{
  "venue": "polymarket",
  "end_time": "2026-03-02T20:15:00+00:00",
  "title": "Will ...?",
  "dollar_volume": 12345.67,
  "yes_no_detected": true,
  "setup_fragment": {
    "polymarket": {
      "market_id": "1234567",
      "condition_id": "0xabc..."
    }
  }
}
```

Kalshi example row:

```json
{
  "venue": "kalshi",
  "end_time": "2026-03-02T20:15:00+00:00",
  "title": "Will ...?",
  "dollar_volume": 12345.67,
  "yes_no_detected": true,
  "setup_fragment": {
    "kalshi": {
      "ticker": "KXEXAMPLE-02MAR2015-AB"
    }
  }
}
```

No `market_url` field is required.

## Filtering Rules

1. Include only `active` markets.
2. Include only markets with parseable end/close time where:
   - `0 < seconds_to_end <= 24h`.
3. Include only markets that resolve to binary YES/NO structure:
   - Polymarket: two outcomes and usable YES/NO token mapping.
   - Kalshi: binary market fields and YES/NO side semantics.
4. Keep rows when source `dollar_volume` is missing by normalizing it to `0.0`.

## Source/Field Strategy

1. Kalshi:
   - Use paginated `/markets` listing from configured trade API base URL.
   - Parse `ticker`, `status`, close/expiration time, title, `dollar_volume`.

2. Polymarket:
   - Use Gamma listing endpoint(s) with pagination.
   - Parse `market_id`, end time, title, optional `condition_id`, `dollar_volume`.

3. Timestamp and numeric parsing:
   - Reuse strict helper parsing style used across discovery scripts.
   - Reject malformed critical identifiers.

## Why JSONL

JSONL is the default because:

1. It is easy to append and diff.
2. It preserves nested setup fragments without lossy flattening.
3. It is easy to post-process later.

## Task #39 Design: Manual Pair Setup Run

## Setup File Contract (Single Pair)

Path: `config/market_setup.json`

Use a minimal contract. Engine enriches Polymarket fields from `market_id` at startup.

```json
{
  "mode": "manual_pair_v1",
  "pair": {
    "polymarket": {
      "market_id": "1234567",
      "condition_id": "0xabc...",
      "yes_outcome": "Weibo Gaming"
    },
    "kalshi": {
      "ticker": "KXEXAMPLE-02MAR2015-AB"
    }
  }
}
```

`condition_id` is optional.
`yes_outcome` is optional but recommended for non-literal outcomes; alternatively provide both `token_yes` and `token_no`.

## CLI Additions (Proposed)

Add to `scripts/run/engine_cli.py`:

1. `--market-setup-file` (default empty)
2. `--market-setup-strict` (default true)

Behavior:

1. When `--market-setup-file` is provided:
   - load manual pair.
   - do not call discovery.
2. When omitted:
   - keep current discovery flow unchanged.

## Selection Logic Changes

In `scripts/common/market_selection.py`:

1. Add `load_selected_markets_from_setup_file(...)`.
2. Resolve market definitions based on setup IDs:
   - Polymarket: fetch by `market_id` and derive `event_slug`, `token_yes`, `token_no`, `window_end`, `condition_id`.
   - Kalshi: validate ticker and resolve `window_end` when available.
3. Validate required fields:
   - Polymarket setup input: `market_id` required; `condition_id` optional.
   - Side orientation for non-literal outcomes: require one of:
     - `yes_outcome`, or
     - `token_yes` + `token_no`.
   - Kalshi setup input: `ticker` required.
4. Fail hard on enrichment errors (no fallback to discovery).
5. Return the same selection shape currently consumed by engine.

In `scripts/run/start_engine.py`:

1. If manual setup mode:
   - resolve selection once.
   - run single segment (one pair).
   - derive segment duration from:
     - `--duration-seconds` when bounded.
     - otherwise auto-stop at resolved `window_end`.
2. If discovery mode:
   - preserve existing loop.

## Safety and Defaults

1. Buy/sell execution enablement remains config-driven and CLI-overridable as today.
2. Missing execution sections in config keep execution disabled by existing defaults.
3. No automatic fallback to discovery in manual mode (hard override).
4. If setup enrichment cannot resolve required fields, exit with explicit error.

## Implementation Phases

## Phase 1: Listing Script (#40)

1. Create `list_active_yesno_markets.py`.
2. Implement venue clients and pagination loops.
3. Add normalization/filtering and two JSONL writers.
4. Add concise terminal summary (counts by venue, rows written).

## Phase 2: Manual Setup Loader (#39)

1. Add setup-file schema loader and validation.
2. Extend CLI with `--market-setup-file`.
3. Add Polymarket/Kalshi enrichment path.
4. Wire `start_engine.py` manual-path branch with hard override.

## Phase 3: Tests

1. Unit tests for setup file validation and conversion to engine selection shape.
2. Unit tests for setup enrichment:
   - Polymarket market_id to token mapping
   - optional condition_id behavior
   - hard-fail errors
3. Unit tests for listing filters:
   - active-only
   - 24h window
   - missing/invalid end times
   - `dollar_volume` normalization to `0.0`
4. Integration-style test for manual run path in `start_engine` selection flow.

## Phase 4: Docs and Examples

1. Add example commands to README.
2. Add setup file example and copy/paste flow.
3. Update `docs/agent/tasks.md` statuses for #40/#39 once implemented.

## Acceptance Criteria

1. Running listing script produces active YES/NO markets ending within 24h.
2. Script writes two JSONL files (Polymarket and Kalshi).
3. Each row includes venue, end time, `dollar_volume` (normalized to `0.0` if absent), `yes_no_detected`, and `setup_fragment`.
4. Manual setup file can launch engine for one pair without any discovery calls.
5. In manual mode, invalid setup file or failed enrichment fails fast with clear errors.
6. With `--duration-seconds 0` in manual mode, run stops automatically at market `window_end`.
7. Discovery mode behavior remains unchanged when setup file is not provided.

## Risks and Mitigations

1. Vendor payload shape drift:
   - Mitigation: strict parsing for required IDs, tolerant optional fields, clear skip reasons.
2. Polymarket token ordering ambiguity:
   - Mitigation: detect YES/NO token mapping from outcomes; fail when ambiguous.
3. Overly broad market listings:
   - Mitigation: keep strict filters (`active`, binary yes/no, end <= 24h) and `--max-pages`.

## Recommended Execution Order

1. Implement #40 first (listing + setup fragments).
2. Validate real output rows against intended manual pairing workflow.
3. Implement #39 using confirmed setup file shape from #40 output.
