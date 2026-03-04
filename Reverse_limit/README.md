# Polymarket Reverse Premarket Strategy

Standalone repo split for the Polymarket-only reverse premarket strategy.

## Run

```powershell
.venv\Scripts\python.exe -m scripts.limit_premarket.start_reverse_strategy --config config/run_config.json
```

## Required env vars

```text
POLYMARKET_L1_APIKEY=
POLYMARKET_L2_API_KEY=
POLYMARKET_L2_API_SECRET=
POLYMARKET_L2_API_PASSPHRASE=
POLYMARKET_FUNDER=
```

Optional:

```text
POLYMARKET_CHAIN_ID=137
POLYMARKET_SIGNATURE_TYPE=1
POLYMARKET_FUNDER_RPC_URL=
```

## Key outputs

- `data/limit_market/reverse_strategy_events__<run_id>.jsonl`
- `data/limit_market/reverse_strategy_orders__<run_id>.jsonl`
- `data/limit_market/reverse_strategy_summary__<run_id>.json`
- `logs/log_reverse_strategy.jsonl`

## Documentation

- `docs/reverse_strategy.md` - strategy behavior and runtime flow
- `docs/common_scripts.md` - reference for `scripts/common/*` modules used by this repo
- `docs/limit_logs.md` - how strategy logs connect end-to-end
