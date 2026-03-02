# Market Surveillance Runbook

Last updated: 2026-02-27

## Purpose

Run and monitor the BTC 15m surveillance process that writes:

1. Per-second snapshot rows (`jsonl`)
2. A watchdog-friendly status file (`json`)
3. A structured run summary (`json`)

Underlying BTC sources in WS mode:

1. Polymarket RTDS `crypto_prices_chainlink` (`btc/usd`)
2. Kalshi `btc_current.json` rolling second series

Detailed source-to-row translation examples:

`docs/market_surveillance_data_flow.md`

## Entrypoint

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance [flags]
```

## Core Outputs

1. `data/diagnostic/market_surveillance/btc_15m_per_second.jsonl`
2. `data/diagnostic/market_surveillance/market_surveillance_status.json`
3. `data/diagnostic/market_surveillance/market_surveillance_summary__<run_id>.json`
4. `data/diagnostic/resolved_15m_pairs.log` (when resolved loop is enabled)

## Run Commands

### 1) Mock smoke test (bounded)

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance `
  --runtime-mode mock `
  --run-id ms-mock-smoke `
  --max-ticks 5 `
  --tick-seconds 0.2
```

### 2) WebSocket runtime (bounded)

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance `
  --runtime-mode ws `
  --run-id ms-ws-smoke `
  --max-ticks 120 `
  --tick-seconds 1.0
```

### 3) WebSocket runtime (continuous)

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance `
  --runtime-mode ws `
  --run-id ms-ws-live `
  --duration-seconds 0 `
  --tick-seconds 1.0
```

### 4) WebSocket runtime with explicit paths

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance `
  --runtime-mode ws `
  --run-id ms-ws-custom `
  --output-jsonl data/diagnostic/market_surveillance/custom_rows.jsonl `
  --status-json data/diagnostic/market_surveillance/custom_status.json `
  --summary-json data/diagnostic/market_surveillance/custom_summary.json `
  --resolved-pairs-log-path data/diagnostic/resolved_15m_pairs.log `
  --resolved-pairs-summary-path data/diagnostic/resolution_comparison_summary.json
```

## Key Runtime Flags

1. `--runtime-mode {mock|ws}`: select mock adapters or live websocket runtime.
2. `--max-ticks N`: stop after N snapshot rows (`0` disables).
3. `--duration-seconds N`: stop after N seconds (`0` means continuous).
4. `--pair-boundary-retry-seconds`: one safety retry delay after boundary refresh miss.
5. `--ws-start-stagger-seconds`: delay between starting RTDS and starting quote market websockets (default `10`).
6. `--resolved-pairs-enabled / --no-resolved-pairs-enabled`: toggle resolved-pair sync loop.
7. `--resolved-pairs-interval-seconds`: cadence for resolved-pair sync.
8. `--summary-json`: explicit summary path (optional; default auto path by run id).

## Status File Contract (watchdog-oriented)

`market_surveillance_status.json` includes:

1. `watchdog.state`: `running` or `stopped`
2. `watchdog.heartbeat_seq`: increments every status update
3. `watchdog.pid`: process id
4. `watchdog.started_at_ms`, `watchdog.updated_at_ms`, `watchdog.updated_at`
5. Tick counters and last error fields
6. Current pair and pair rotation/refresh counters (WS mode)
7. Resolved-pair loop counters (WS mode)

## Summary File Contract

Summary JSON includes:

1. `run_id`, `mode`, `ticks_written`
2. `started_at_ms`, `ended_at_ms`, `started_at`, `ended_at`
3. `output_jsonl`, `status_path`, `summary_path`
4. `watchdog.pid`, `watchdog.heartbeat_seq`
5. WS-only fields: pair rotation/refresh counters and resolved-pair summary

## Operational Checks

1. Verify status heartbeat moves forward:
   - `watchdog.heartbeat_seq` increases while running.
2. Verify status freshness:
   - `watchdog.updated_at_ms` advances at roughly the snapshot cadence.
3. Verify run completion:
   - status ends at `watchdog.state = "stopped"`
   - summary file exists and `ticks_written` matches expectation.

## Portability Validation (Minimal Folder Copy)

Run this after copying the minimal subset into another workspace:

1. `scripts/surveillance/market_surveillance.py`
2. `scripts/common/decision_runtime.py`
3. `scripts/common/ws_collectors.py`
4. `scripts/common/ws_transport.py`
5. `scripts/common/market_selection.py`
6. `scripts/common/run_config.py`
7. `scripts/common/kalshi_auth.py`
8. required `config/` and dependency environment

Validation command:

```powershell
.venv\Scripts\python.exe -m scripts.surveillance.market_surveillance `
  --runtime-mode mock `
  --run-id portability-check `
  --max-ticks 3 `
  --tick-seconds 0.1
```

Expected result:

1. Exit code `0`
2. Snapshot rows written
3. Status JSON with `watchdog.state = "stopped"`
4. Summary JSON present
