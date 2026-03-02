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
            text = raw.strip()
            if not text:
                continue
            payload = json.loads(text)
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


class _SequencedPairSelector:
    def __init__(self, *, pair_a: Dict[str, Any], pair_b: Dict[str, Any]) -> None:
        self._pair_a = dict(pair_a)
        self._pair_b = dict(pair_b)
        self._calls = 0

    def select(self, *, run_discovery_first: bool) -> Dict[str, Any]:
        self._calls += 1
        if self._calls == 1:
            return dict(self._pair_a)
        if self._calls == 2 and run_discovery_first:
            raise RuntimeError("temporary discovery miss at boundary")
        return dict(self._pair_b)


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _all_best_asks_present(row: Dict[str, Any]) -> bool:
    best_asks = row.get("best_asks") if isinstance(row, dict) else {}
    if not isinstance(best_asks, dict):
        return False
    for leg in ("polymarket_yes", "polymarket_no", "kalshi_yes", "kalshi_no"):
        payload = best_asks.get(leg)
        if not isinstance(payload, dict):
            return False
        if payload.get("price") is None or payload.get("size") is None:
            return False
    return True


def test_phase2_ws_runtime_boundary_rotation_with_retry_uses_fixture_collectors(tmp_path: Path) -> None:
    fixture_dir = Path(__file__).resolve().parent / "fixtures" / "surveillance"
    pair_a = {
        "kalshi_ticker": "KXBTC15M-PAIR-A",
        "polymarket_slug": "btc-updown-15m-pair-a",
        "polymarket_market_id": "market-a",
        "polymarket_token_yes": "pm_yes_a",
        "polymarket_token_no": "pm_no_a",
        "window_end": "1970-01-01T00:16:45+00:00",
    }
    pair_b = {
        "kalshi_ticker": "KXBTC15M-PAIR-B",
        "polymarket_slug": "btc-updown-15m-pair-b",
        "polymarket_market_id": "market-b",
        "polymarket_token_yes": "pm_yes_b",
        "polymarket_token_no": "pm_no_b",
        "window_end": "1970-01-01T00:31:00+00:00",
    }
    fixtures = {
        "KXBTC15M-PAIR-A": {
            "polymarket": fixture_dir / "pair_a_polymarket.jsonl",
            "kalshi": fixture_dir / "pair_a_kalshi.jsonl",
        },
        "KXBTC15M-PAIR-B": {
            "polymarket": fixture_dir / "pair_b_polymarket.jsonl",
            "kalshi": fixture_dir / "pair_b_kalshi.jsonl",
        },
    }

    selector: PairSelector = _SequencedPairSelector(pair_a=pair_a, pair_b=pair_b)

    def _collector_factory(
        pair: Dict[str, Any],
        runtime: Any,
        health_config: WsHealthConfig,
    ) -> Sequence[CollectorRunner]:
        ticker = str(pair.get("kalshi_ticker"))
        pair_fixtures = fixtures[ticker]
        polymarket = PolymarketWsCollector(
            token_yes=str(pair.get("polymarket_token_yes") or ""),
            token_no=str(pair.get("polymarket_token_no") or ""),
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
            _FixtureReplayCollector(collector=kalshi, fixture_path=pair_fixtures["kalshi"]),
            _FixtureReplayCollector(collector=polymarket, fixture_path=pair_fixtures["polymarket"]),
        ]

    clock = {"value": 1_000_000}

    def _now_ms() -> int:
        current = int(clock["value"])
        clock["value"] = current + 1000
        return current

    def _now_iso(epoch_ms: int) -> str:
        return f"iso-{epoch_ms}"

    async def _yield_sleep(_: float) -> None:
        await asyncio.sleep(0)

    out_path = tmp_path / "phase2_runtime.jsonl"
    status_path = tmp_path / "phase2_runtime_status.json"
    summary = asyncio.run(
        run_ws_market_surveillance(
            output_jsonl_path=out_path,
            status_path=status_path,
            config_path=Path("config/run_config.json"),
            discovery_output_path=Path("data/market_discovery_latest.json"),
            pair_cache_path=Path("data/market_pair_cache.json"),
            run_id="phase2-test",
            tick_seconds=0.0,
            duration_seconds=0,
            max_ticks=35,
            pair_boundary_retry_seconds=20.0,
            resolved_pairs_enabled=False,
            skip_discovery_initial=False,
            chainlink_adapter=MockChainlinkPriceAdapter(),
            kalshi_adapter=MockKalshiWebsitePriceAdapter(),
            pair_selector=selector,
            collector_factory=_collector_factory,
            now_ms_fn=_now_ms,
            now_iso_fn=_now_iso,
            sleep_async_fn=_yield_sleep,
        )
    )

    assert summary["ticks_written"] == 35
    assert summary["pair_rotations"] == 1
    assert summary["pair_refresh_failures"] >= 1

    rows = _read_jsonl(out_path)
    assert len(rows) == 35

    rows_a = [row for row in rows if row.get("pair", {}).get("kalshi_ticker") == "KXBTC15M-PAIR-A"]
    rows_b = [row for row in rows if row.get("pair", {}).get("kalshi_ticker") == "KXBTC15M-PAIR-B"]
    assert rows_a
    assert rows_b
    assert any(_all_best_asks_present(row) for row in rows_a)
    assert any(_all_best_asks_present(row) for row in rows_b)

    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert status_payload["pair_rotations"] == 1
    assert status_payload["pair_refresh_failures"] >= 1
    assert status_payload["current_pair"]["kalshi_ticker"] == "KXBTC15M-PAIR-B"
