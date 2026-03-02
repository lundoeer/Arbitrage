from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

from scripts.common.ws_collectors import KalshiWsCollector, PolymarketWsCollector
from scripts.common.ws_transport import NullWriter, WsHealthConfig
from scripts.surveillance.market_surveillance import (
    CollectorRunner,
    MockChainlinkPriceAdapter,
    MockKalshiWebsitePriceAdapter,
    PairSelector,
    run_ws_market_surveillance,
)


class _FixtureReplayCollector:
    def __init__(self, *, collector: Any, fixture_path: Path) -> None:
        self.collector = collector
        self.fixture_path = fixture_path

    async def run(self, stop_event: asyncio.Event) -> None:
        lines = self.fixture_path.read_text(encoding="utf-8").splitlines()
        for raw in lines:
            if stop_event.is_set():
                break
            line = raw.strip()
            if not line:
                continue
            payload = json.loads(line)
            recv_ms = int(payload["recv_ms"])
            message = payload["message"]
            self.collector.message_count += 1
            for event in self.collector.normalize_event(message, recv_ms):
                self.collector.event_count += 1
                if callable(self.collector.on_event):
                    self.collector.on_event(event)
            await asyncio.sleep(0)
        while not stop_event.is_set():
            await asyncio.sleep(0.001)


class _StaticPairSelector:
    def __init__(self, pair: Dict[str, Any]) -> None:
        self._pair = dict(pair)

    def select(self, *, run_discovery_first: bool) -> Dict[str, Any]:
        del run_discovery_first
        return dict(self._pair)


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def test_phase4_resolved_pairs_loop_isolated_from_snapshot_loop(tmp_path: Path) -> None:
    fixture_dir = Path(__file__).resolve().parent / "fixtures" / "surveillance"
    pair = {
        "kalshi_ticker": "KXBTC15M-PAIR-A",
        "polymarket_slug": "btc-updown-15m-pair-a",
        "polymarket_market_id": "market-a",
        "polymarket_token_yes": "pm_yes_a",
        "polymarket_token_no": "pm_no_a",
        "window_end": "1970-01-01T00:30:00+00:00",
    }
    selector: PairSelector = _StaticPairSelector(pair)

    def _collector_factory(
        selected_pair: Dict[str, Any],
        runtime: Any,
        health_config: WsHealthConfig,
    ) -> Sequence[CollectorRunner]:
        ticker = str(selected_pair.get("kalshi_ticker") or "")
        polymarket = PolymarketWsCollector(
            token_yes=str(selected_pair.get("polymarket_token_yes") or ""),
            token_no=str(selected_pair.get("polymarket_token_no") or ""),
            custom_feature_enabled=True,
            raw_writer=NullWriter(),
            event_writer=NullWriter(),
            health_config=health_config,
            on_event=runtime.apply_polymarket_event,
        )
        kalshi = KalshiWsCollector(
            market_ticker=ticker,
            channels=["ticker", "orderbook_delta"],
            headers={},
            raw_writer=NullWriter(),
            event_writer=NullWriter(),
            health_config=health_config,
            on_event=runtime.apply_kalshi_event,
        )
        return [
            _FixtureReplayCollector(collector=kalshi, fixture_path=fixture_dir / "pair_a_kalshi.jsonl"),
            _FixtureReplayCollector(collector=polymarket, fixture_path=fixture_dir / "pair_a_polymarket.jsonl"),
        ]

    resolved_calls: List[str] = []

    def _resolved_runner() -> Dict[str, Any]:
        if not resolved_calls:
            resolved_calls.append("fail")
            raise RuntimeError("resolved sync temporary failure")
        resolved_calls.append("ok")
        return {
            "generated_at": "2026-02-27T00:00:00+00:00",
            "totals": {
                "pairs_in_log_total": 123,
                "new_pairs_appended_to_log": 1,
                "resolved_same": 100,
                "resolved_different": 23,
            },
        }

    clock = {"value": 1_000_000}

    def _now_ms() -> int:
        current = int(clock["value"])
        clock["value"] = current + 1000
        return current

    async def _yield_sleep(_: float) -> None:
        await asyncio.sleep(0)

    out_path = tmp_path / "phase4_output.jsonl"
    status_path = tmp_path / "phase4_status.json"

    summary = asyncio.run(
        run_ws_market_surveillance(
            output_jsonl_path=out_path,
            status_path=status_path,
            config_path=Path("config/run_config.json"),
            discovery_output_path=Path("data/market_discovery_latest.json"),
            pair_cache_path=Path("data/market_pair_cache.json"),
            run_id="phase4-test",
            tick_seconds=0.0,
            duration_seconds=0,
            max_ticks=16,
            pair_boundary_retry_seconds=20.0,
            resolved_pairs_enabled=True,
            resolved_pairs_interval_seconds=3.0,
            resolved_pairs_run_on_start=True,
            resolved_pairs_pair_log_path=tmp_path / "resolved_15m_pairs.log",
            resolved_pairs_summary_path=tmp_path / "resolution_comparison_summary.json",
            chainlink_adapter=MockChainlinkPriceAdapter(),
            kalshi_adapter=MockKalshiWebsitePriceAdapter(),
            pair_selector=selector,
            collector_factory=_collector_factory,
            resolved_pairs_runner=_resolved_runner,
            now_ms_fn=_now_ms,
            sleep_async_fn=_yield_sleep,
        )
    )

    assert summary["ticks_written"] == 16
    resolved = summary["resolved_pairs"]
    assert int(resolved["attempts"]) >= 2
    assert int(resolved["failures"]) >= 1
    assert int(resolved["successes"]) >= 1
    assert resolved["last_summary"]["new_pairs_appended_to_log"] == 1

    rows = _read_jsonl(out_path)
    assert len(rows) == 16

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload["ticks_written"] == 16
    assert int(status_payload["resolved_pairs"]["attempts"]) >= 2
    assert int(status_payload["resolved_pairs"]["failures"]) >= 1
    assert int(status_payload["resolved_pairs"]["successes"]) >= 1

