from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Sequence

from scripts.common.ws_transport import WsHealthConfig
from scripts.surveillance.market_surveillance import (
    CollectorRunner,
    MockChainlinkPriceAdapter,
    MockKalshiWebsitePriceAdapter,
    PairSelector,
    run_mock_market_surveillance,
    run_ws_market_surveillance,
)


class _StaticPairSelector:
    def __init__(self, pair: Dict[str, Any]) -> None:
        self._pair = dict(pair)

    def select(self, *, run_discovery_first: bool) -> Dict[str, Any]:
        del run_discovery_first
        return dict(self._pair)


class _NoopCollector:
    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(0)


def test_phase5_mock_writes_summary_and_watchdog_status(tmp_path: Path) -> None:
    out_path = tmp_path / "mock_phase5.jsonl"
    status_path = tmp_path / "mock_phase5_status.json"
    summary_path = tmp_path / "mock_phase5_summary.json"

    clock = {"value": 2_000_000}

    def _now_ms() -> int:
        current = int(clock["value"])
        clock["value"] = current + 1000
        return current

    def _now_iso(epoch_ms: int) -> str:
        return f"iso-{epoch_ms}"

    summary = run_mock_market_surveillance(
        output_jsonl_path=out_path,
        status_path=status_path,
        summary_path=summary_path,
        run_id="phase5-mock",
        tick_seconds=0.0,
        duration_seconds=0,
        max_ticks=3,
        now_ms_fn=_now_ms,
        now_iso_fn=_now_iso,
        sleep_fn=lambda _: None,
    )

    assert summary["summary_path"] == str(summary_path)
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["mode"] == "mock"
    assert summary_payload["run_id"] == "phase5-mock"
    assert summary_payload["ticks_written"] == 3
    assert int(summary_payload["watchdog"]["heartbeat_seq"]) >= 4

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload["run_id"] == "phase5-mock"
    assert status_payload["watchdog"]["state"] == "stopped"
    assert int(status_payload["watchdog"]["heartbeat_seq"]) >= 4


def test_phase5_ws_writes_summary_and_watchdog_status(tmp_path: Path) -> None:
    pair = {
        "kalshi_ticker": "KXBTC15M-PAIR-A",
        "polymarket_slug": "btc-updown-15m-pair-a",
        "polymarket_market_id": "market-a",
        "polymarket_token_yes": "pm_yes_a",
        "polymarket_token_no": "pm_no_a",
        "window_end": "1970-01-01T00:45:00+00:00",
    }
    selector: PairSelector = _StaticPairSelector(pair)

    def _collector_factory(
        pair_payload: Dict[str, Any],
        runtime: Any,
        health_config: WsHealthConfig,
    ) -> Sequence[CollectorRunner]:
        del pair_payload, runtime, health_config
        return [_NoopCollector(), _NoopCollector()]

    clock = {"value": 3_000_000}

    def _now_ms() -> int:
        current = int(clock["value"])
        clock["value"] = current + 1000
        return current

    async def _yield_sleep(_: float) -> None:
        await asyncio.sleep(0)

    out_path = tmp_path / "ws_phase5.jsonl"
    status_path = tmp_path / "ws_phase5_status.json"
    summary_path = tmp_path / "ws_phase5_summary.json"

    summary = asyncio.run(
        run_ws_market_surveillance(
            output_jsonl_path=out_path,
            status_path=status_path,
            summary_path=summary_path,
            config_path=Path("config/run_config.json"),
            discovery_output_path=Path("data/market_discovery_latest.json"),
            pair_cache_path=Path("data/market_pair_cache.json"),
            run_id="phase5-ws",
            tick_seconds=0.0,
            duration_seconds=0,
            max_ticks=3,
            resolved_pairs_enabled=False,
            chainlink_adapter=MockChainlinkPriceAdapter(),
            kalshi_adapter=MockKalshiWebsitePriceAdapter(),
            pair_selector=selector,
            collector_factory=_collector_factory,
            now_ms_fn=_now_ms,
            sleep_async_fn=_yield_sleep,
        )
    )

    assert summary["summary_path"] == str(summary_path)
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["mode"] == "ws"
    assert summary_payload["run_id"] == "phase5-ws"
    assert summary_payload["ticks_written"] == 3
    assert int(summary_payload["watchdog"]["heartbeat_seq"]) >= 4

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload["run_id"] == "phase5-ws"
    assert status_payload["watchdog"]["state"] == "stopped"
    assert int(status_payload["watchdog"]["heartbeat_seq"]) >= 4
