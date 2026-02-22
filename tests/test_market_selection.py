from __future__ import annotations

import json
from pathlib import Path

from scripts.common.market_selection import load_selected_markets


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_selected_markets_exposes_polymarket_condition_id(tmp_path: Path) -> None:
    config_path = tmp_path / "run_config.json"
    discovery_path = tmp_path / "market_discovery_latest.json"
    pair_cache_path = tmp_path / "market_pair_cache.json"
    _write_json(config_path, {})
    _write_json(pair_cache_path, {})
    _write_json(
        discovery_path,
        {
            "generated_at": "2026-02-22T00:00:00+00:00",
            "polymarket": {
                "selected_contract": {
                    "event_slug": "btc-updown-15m-1",
                    "market_id": "1399999",
                    "condition_id": "0xcond",
                    "token_yes": "pm_yes_token",
                    "token_no": "pm_no_token",
                    "window_end": "2026-02-22T00:15:00+00:00",
                    "normalization_signature": "sig-1",
                }
            },
            "kalshi": {
                "selected_contract": {
                    "ticker": "KXBTC15M-TEST",
                    "window_end": "2026-02-22T00:15:00+00:00",
                    "normalization_signature": "sig-1",
                }
            },
        },
    )
    selection = load_selected_markets(
        config_path=config_path,
        discovery_output=discovery_path,
        pair_cache_path=pair_cache_path,
        run_discovery_first=False,
    )
    assert selection["polymarket"]["condition_id"] == "0xcond"
