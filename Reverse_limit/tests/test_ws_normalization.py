from __future__ import annotations

from scripts.common.ws_normalization import (
    normalize_polymarket_market_resolution_event,
    normalize_polymarket_user_event,
)


def test_normalize_polymarket_user_order_event() -> None:
    message = {
        "event_type": "order",
        "id": "0xcc5c",
        "type": "UPDATE",
        "timestamp": "1772558026207",
        "market": "0xb7d1",
        "asset_id": "1118398",
        "outcome": "Yes",
        "side": "BUY",
        "price": "0.49",
        "original_size": "10",
        "size_matched": "3",
    }
    events = list(normalize_polymarket_user_event(message, recv_ms=1772558027207, condition_id="0xb7d1"))
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "polymarket_user_order"
    assert event["order_id"] == "0xcc5c"
    assert event["client_order_id"] is None
    assert event["outcome_side"] == "yes"
    assert event["order_side"] == "buy"
    assert event["price"] == 0.49
    assert event["original_size"] == 10.0
    assert event["size_matched"] == 3.0
    assert event["remaining_size"] == 7.0
    assert event["status"] == "UPDATE"
    assert event["source_timestamp_ms"] == 1772558026207
    assert event["lag_ms"] == 1000


def test_normalize_polymarket_user_trade_confirmed() -> None:
    message = {
        "event_type": "trade",
        "id": "user-trade-1",
        "status": "CONFIRMED",
        "timestamp": "1772558026207",
        "market": "0xb7d1",
        "asset_id": "1118398",
        "outcome": "YES",
        "side": "BUY",
        "price": "0.49",
        "size": "1.5",
        "taker_order_id": "0xcc5c",
        "maker_orders": [{"maker_order_id": "0xmaker", "matched_size": "1.5"}],
    }
    events = list(normalize_polymarket_user_event(message, recv_ms=1772558027207, condition_id="0xb7d1"))
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "polymarket_user_trade"
    assert event["status"] == "CONFIRMED"
    assert event["is_confirmed"] is True
    assert event["outcome_side"] == "yes"
    assert event["order_side"] == "buy"
    assert event["price"] == 0.49
    assert event["size"] == 1.5
    assert event["taker_order_id"] == "0xcc5c"
    assert isinstance(event["maker_orders"], list)
    assert event["source_timestamp_ms"] == 1772558026207
    assert event["lag_ms"] == 1000


def test_normalize_polymarket_user_event_respects_condition_filter() -> None:
    message = {
        "event_type": "trade",
        "id": "trade-2",
        "status": "CONFIRMED",
        "timestamp": "1772558026207",
        "market": "0xother",
        "price": "0.49",
        "size": "1.0",
    }
    events = list(normalize_polymarket_user_event(message, recv_ms=1772558027207, condition_id="0xb7d1"))
    assert events == []


def test_normalize_polymarket_market_resolution_event() -> None:
    message = {
        "type": "market_resolved",
        "condition_id": "0xb7d1",
        "winner": "yes",
        "status": "resolved",
        "timestamp": "1772559000",
    }
    events = list(normalize_polymarket_market_resolution_event(message, recv_ms=1772559026207))
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "polymarket_market_resolution"
    assert event["condition_id"] == "0xb7d1"
    assert event["outcome"] == "yes"
    assert event["is_resolved"] is True
    assert event["resolution_status"] == "resolved"
    assert event["source_event_type"] == "market_resolved"
    assert event["source_timestamp_ms"] == 1772559000000
    assert event["lag_ms"] == 26207

