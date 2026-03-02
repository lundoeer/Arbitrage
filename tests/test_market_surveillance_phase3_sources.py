from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from scripts.surveillance.market_surveillance import (
    CHAINLINK_QUERY_TIMESCALE_ENDPOINT,
    ChainlinkPriceAdapter,
    KalshiWebsitePriceAdapter,
    PolymarketRtdsChainlinkPriceAdapter,
    UnderlyingPricePoint,
    build_snapshot_row,
)


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        text: str = "",
        json_payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.status_code = int(status_code)
        self.text = str(text)
        self._json_payload = json_payload
        self.headers = dict(headers or {})

    def json(self) -> Dict[str, Any]:
        if self._json_payload is not None:
            return dict(self._json_payload)
        if self.text:
            return json.loads(self.text)
        return {}


class _FakeSession:
    def __init__(self, routes: Dict[str, _FakeResponse]) -> None:
        self.routes = dict(routes)
        self.calls: Dict[str, int] = {}

    def get(
        self,
        url: str,
        *,
        timeout: float,
        allow_redirects: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ) -> _FakeResponse:
        del timeout, allow_redirects, params
        if url not in self.routes:
            raise RuntimeError(f"No fake route configured for url={url}")
        self.calls[url] = int(self.calls.get(url, 0)) + 1
        return self.routes[url]


def _fixture_path(name: str) -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "surveillance" / name


def test_kalshi_website_adapter_parses_recorded_payload() -> None:
    payload = json.loads(_fixture_path("kalshi_btc_current_sample.json").read_text(encoding="utf-8"))
    response = _FakeResponse(
        status_code=200,
        json_payload=payload,
        headers={
            "Date": "Fri, 27 Feb 2026 13:25:07 GMT",
            "Last-Modified": "Fri, 27 Feb 2026 13:25:06 GMT",
        },
    )
    session = _FakeSession({"https://kalshi.test/btc_current.json": response})
    adapter = KalshiWebsitePriceAdapter(
        session=session,  # type: ignore[arg-type]
        url="https://kalshi.test/btc_current.json",
        max_age_ms=12000,
    )

    point = adapter.read(at_ms=1772198707000)
    assert point.source == "kalshi_btc_current_json"
    assert point.price_usd == pytest.approx(66021.548, rel=1e-12)
    assert point.source_timestamp_ms == 1772198705000
    assert point.max_age_ms == 12000
    assert isinstance(point.metadata, dict)
    assert point.metadata["price_field"] == "timeseries.second[rolling_buffer]"
    assert point.metadata["selected_second_exact"] is False
    assert point.metadata["selected_second_offset_ms"] == 2000


def test_kalshi_website_adapter_uses_second_cache_for_requested_second() -> None:
    payload = json.loads(_fixture_path("kalshi_btc_current_sample.json").read_text(encoding="utf-8"))
    url = "https://kalshi.test/btc_current.json"
    response = _FakeResponse(
        status_code=200,
        json_payload=payload,
        headers={
            "Date": "Fri, 27 Feb 2026 13:25:07 GMT",
            "Last-Modified": "Fri, 27 Feb 2026 13:25:06 GMT",
        },
    )
    session = _FakeSession({url: response})
    adapter = KalshiWebsitePriceAdapter(
        session=session,  # type: ignore[arg-type]
        url=url,
        fetch_min_interval_ms=30_000,
    )

    first = adapter.read(at_ms=1772198703000)
    assert first.price_usd == pytest.approx(66020.711, rel=1e-12)
    assert first.source_timestamp_ms == 1772198703000
    assert first.metadata["selected_second_exact"] is True
    assert session.calls[url] == 1

    second = adapter.read(at_ms=1772198704000)
    assert second.price_usd == pytest.approx(66021.1185, rel=1e-12)
    assert second.source_timestamp_ms == 1772198704000
    assert second.metadata["selected_second_exact"] is True
    assert session.calls[url] == 1


def test_chainlink_price_adapter_parses_live_query_payload() -> None:
    stream_html = _fixture_path("chainlink_stream_page_sample.html").read_text(encoding="utf-8")
    query_timescale_payload = json.loads(
        _fixture_path("chainlink_query_timescale_live_sample.json").read_text(encoding="utf-8")
    )
    session = _FakeSession(
        {
            "https://data.chain.link/streams/btc-usd": _FakeResponse(status_code=200, text=stream_html),
            CHAINLINK_QUERY_TIMESCALE_ENDPOINT: _FakeResponse(status_code=200, json_payload=query_timescale_payload),
        }
    )
    adapter = ChainlinkPriceAdapter(
        session=session,  # type: ignore[arg-type]
        stream_page_url="https://data.chain.link/streams/btc-usd",
        stream_live_max_age_ms=900000,
    )

    point = adapter.read(at_ms=1772200000000)
    expected_ts = int(datetime(2026, 2, 27, 13, 15, 5, tzinfo=timezone.utc).timestamp() * 1000)
    assert point.source == "chainlink_streams_query_timescale_live"
    assert point.price_usd == pytest.approx(66030.0, rel=1e-12)
    assert point.source_timestamp_ms == expected_ts
    assert point.max_age_ms == 900000
    assert isinstance(point.metadata, dict)
    assert point.metadata["feed_id"] == "0xfeedid123"
    assert point.metadata["price_field"] == "price"


def test_chainlink_adapter_has_no_fallback_when_live_query_fails() -> None:
    stream_html = _fixture_path("chainlink_stream_page_sample.html").read_text(encoding="utf-8")
    session = _FakeSession(
        {
            "https://data.chain.link/streams/btc-usd": _FakeResponse(status_code=200, text=stream_html),
            CHAINLINK_QUERY_TIMESCALE_ENDPOINT: _FakeResponse(status_code=500, text="upstream error"),
        }
    )
    adapter = ChainlinkPriceAdapter(
        session=session,  # type: ignore[arg-type]
        stream_page_url="https://data.chain.link/streams/btc-usd",
    )
    with pytest.raises(RuntimeError, match="query-timescale request failed"):
        adapter.read(at_ms=1772200000000)


def test_polymarket_rtds_adapter_parses_chainlink_message_shape() -> None:
    event = PolymarketRtdsChainlinkPriceAdapter._extract_chainlink_price_event(
        {
            "topic": "crypto_prices_chainlink",
            "event": "price_change",
            "payload": {
                "asset": "Crypto",
                "market": "Chainlink",
                "symbol": "BTC/USD",
                "timestamp": 1772210178123,
                "value": "65897.32070018168",
            },
        },
        symbol_key="btc/usd",
    )
    assert isinstance(event, dict)
    assert event["symbol"] == "btc/usd"
    assert event["price_usd"] == pytest.approx(65897.32070018168, rel=1e-12)
    assert event["source_timestamp_ms"] == 1772210178123
    assert event["second_timestamp_ms"] == 1772210178000


def test_polymarket_rtds_adapter_selects_latest_cached_second_at_or_before_target() -> None:
    selected = PolymarketRtdsChainlinkPriceAdapter._pick_second(
        {
            1772210178000: 65897.0,
            1772210179000: 65898.0,
            1772210181000: 65899.0,
        },
        target_second_ms=1772210180000,
    )
    assert selected == 1772210179000


def test_polymarket_rtds_adapter_unsubscription_payload_shape() -> None:
    payload = PolymarketRtdsChainlinkPriceAdapter._unsubscription_payload(symbol="BTC/USD")
    assert payload["action"] == "unsubscribe"
    subscriptions = payload.get("subscriptions")
    assert isinstance(subscriptions, list) and subscriptions
    first = subscriptions[0]
    assert first["topic"] == "crypto_prices_chainlink"
    assert first["type"] == "*"


def test_build_snapshot_row_marks_underlying_stale_when_age_exceeds_max() -> None:
    row = build_snapshot_row(
        run_id="stale-run",
        recv_ms=10_000,
        ts="iso-10000",
        pair={},
        chainlink_price=UnderlyingPricePoint(
            source="chainlink_test",
            price_usd=65000.0,
            source_timestamp_ms=8_500,
            max_age_ms=1000,
        ),
        kalshi_price=UnderlyingPricePoint(
            source="kalshi_test",
            price_usd=65001.0,
            source_timestamp_ms=9_500,
            max_age_ms=1000,
        ),
        best_asks={
            "polymarket_yes": {"price": 0.41, "size": 100.0},
            "polymarket_no": {"price": 0.60, "size": 100.0},
            "kalshi_yes": {"price": 0.42, "size": 100.0},
            "kalshi_no": {"price": 0.59, "size": 100.0},
        },
    )
    chainlink = row["underlying_prices"]["polymarket_chainlink"]
    kalshi = row["underlying_prices"]["kalshi_site"]
    assert chainlink["source_age_ms"] == 1500
    assert chainlink["is_stale"] is True
    assert kalshi["source_age_ms"] == 500
    assert kalshi["is_stale"] is False
    assert row["health"]["underlying_fresh"] is False
