# BTC 15m Arbitrage Engine

Cross-venue binary options arbitrage between **Polymarket** (CLOB) and **Kalshi** (exchange) on BTC 15-minute up/down markets.

> **Important**: Always use `.venv\Scripts\python.exe` — system Python lacks required dependencies (`websockets`, etc.).

## Quick Start

```powershell
# Discover currently active market pair
.venv\Scripts\python.exe -m scripts.run.discover_active_btc_15m_markets

# Run engine (no trading, with summary)
.venv\Scripts\python.exe -m scripts.run.arbitrage_engine --duration-seconds 300 --no-enable-buy-execution --log-summary

# Run engine with full logging
.venv\Scripts\python.exe -m scripts.run.arbitrage_engine --duration-seconds 300 --no-enable-buy-execution --log-summary --log-raw-events --log-decisions --log-edge-snapshots --log-runtime-memory
```

## Project Layout

```
scripts/
├── run/                    # Executable entry points
│   ├── arbitrage_engine.py       # Main engine — segment loop, WS lifecycle, decision loop
│   ├── engine_cli.py             # CLI argument parser (build_parser)
│   └── discover_active_btc_15m_markets.py
├── common/                 # Shared modules
│   ├── utils.py                  # Shared helpers (now_ms, as_dict, as_float, etc.)
│   ├── buy_fsm.py                # BuyFsmRuntime + BuyFsmState (FSM for buy execution)
│   ├── edge_snapshots.py         # Edge snapshot computation
│   ├── buy_execution.py          # Cross-venue order submission
│   ├── decision_runtime.py       # DecisionRuntime — quote sanity, candidate scoring
│   ├── normalized_books.py       # In-memory order books (4 legs)
│   ├── ws_transport.py           # BaseWsCollector — WS connection lifecycle
│   ├── ws_collectors.py          # KalshiWsCollector, PolymarketWsCollector
│   ├── ws_normalization.py       # Raw → normalized event mapping
│   ├── api_transport.py          # HTTP transport with retry/backoff
│   ├── kalshi_auth.py            # Kalshi WS auth header generation
│   └── run_config.py             # Config parsing
├── diagnostic/             # Analysis and capture tools
│   ├── capture_btc_15m_ws_market_data.py
│   ├── capture_btc_15m_ws_price_feeds.py
│   └── ...
config/
└── run_config.json         # Runtime configuration
data/                       # Engine output (gitignored)
docs/                       # Architecture docs and reviews
tests/                      # pytest tests
```

## Market Discovery

```powershell
.venv\Scripts\python.exe -m scripts.run.discover_active_btc_15m_markets
```

Outputs `data/market_discovery_latest.json` and `data/market_pair_cache.json`.

Selection logic: market status is active, `window_end` is in the future, and closes within the next 15 minutes.

## Arbitrage Engine

```powershell
# Bounded run, no trading
.venv\Scripts\python.exe -m scripts.run.arbitrage_engine --duration-seconds 60 --no-enable-buy-execution

# With raw WebSocket logging
.venv\Scripts\python.exe -m scripts.run.arbitrage_engine --duration-seconds 60 --log-raw-events

# Skip discovery (use cached pair — only if cache is fresh)
.venv\Scripts\python.exe -m scripts.run.arbitrage_engine --duration-seconds 60 --skip-discovery
```

**Caution**: `--skip-discovery` requires a fresh `data/market_pair_cache.json`. If the cache contains expired market windows, the engine will find no active segments and exit with 0 messages.

Engine outputs (in `data/`):

- `arbitrage_engine_summary__*.json` — run summary with message counts, health, quotes
- `decision_log__*.jsonl` — per-tick decision samples
- `gross_edge_snapshot__*.jsonl` — edge snapshot log
- `websocket_share_price_runtime__*.jsonl` — 1s book state snapshots
- `websocket_kalshi/raw_engine__*.jsonl` — raw Kalshi WS frames
- `websocket_poly/raw_engine__*.jsonl` — raw Polymarket WS frames

## Diagnostic Captures

```powershell
# Market data capture (raw + normalized + health)
.venv\Scripts\python.exe -m scripts.diagnostic.capture_btc_15m_ws_market_data --duration-seconds 120

# Share price feeds
.venv\Scripts\python.exe -m scripts.diagnostic.capture_btc_15m_ws_price_feeds --duration-seconds 120
```

Polymarket `custom_feature_enabled=true` is on by default. Use `--no-custom-feature-enabled` to disable.

## .env Variables

```
KALSHI_READONLY_API_KEY=
KALSHI_RW_API_KEY=
KALSHI_PRIVATEKEY=

POLYMARKET_L1_APIKEY=
POLYMARKET_L2_API_KEY=
POLYMARKET_L2_API_SECRET=
POLYMARKET_L2_API_PASSPHRASE=
POLYMARKET_FUNDER=
```

## Documentation

- `docs/ClaudeOpusReview.md` — Full architecture review
- `docs/ClaudeOpusSuggestions.md` — Prioritized improvement plan
- `docs/buying.md` — Buy execution design (FSM, execution flow)
- `docs/websocket.md` — WebSocket implementation details
- `docs/Improve_lag.md` — Health metric accuracy improvements
- `docs/agent/` — Agent-facing project context documents
