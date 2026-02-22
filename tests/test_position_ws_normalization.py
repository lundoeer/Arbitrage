from __future__ import annotations

from scripts.common.ws_collectors import KalshiMarketPositionsWsCollector, PolymarketUserWsCollector
from scripts.common.ws_normalization import normalize_kalshi_market_positions_event, normalize_polymarket_user_event
from scripts.common.ws_transport import NullWriter


def test_normalize_polymarket_user_trade_confirmed() -> None:
    message = {
        "event_type": "trade",
        "id": "1",
        "status": "CONFIRMED",
        "last_update": "1713871473000",
        "market": "0xabc",
        "asset_id": "token_yes",
        "outcome": "YES",
        "side": "BUY",
        "price": "0.52",
        "size": "3.0",
        "owner": "0xowner",
        "taker_order_id": "ord-1",
        "maker_orders": [],
    }
    events = list(normalize_polymarket_user_event(message, recv_ms=1_713_871_474_100, condition_id="0xabc"))
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "polymarket_user_trade"
    assert event["is_confirmed"] is True
    assert event["status"] == "CONFIRMED"
    assert event["outcome_side"] == "yes"
    assert event["order_side"] == "buy"
    assert event["size"] == 3.0
    assert event["price"] == 0.52
    assert event["market"] == "0xabc"
    assert event["lag_ms"] == 1100


def test_normalize_polymarket_user_trade_condition_filter() -> None:
    message = {
        "event_type": "trade",
        "id": "2",
        "status": "CONFIRMED",
        "last_update": "1713871473000",
        "market": "0xother",
        "asset_id": "token_yes",
        "outcome": "YES",
        "side": "BUY",
        "price": "0.52",
        "size": "3.0",
    }
    events = list(normalize_polymarket_user_event(message, recv_ms=1_713_871_474_100, condition_id="0xabc"))
    assert events == []


def test_normalize_polymarket_user_order_event() -> None:
    message = {
        "event_type": "order",
        "id": "4",
        "type": "UPDATE",
        "timestamp": "1713871473000",
        "market": "0xabc",
        "asset_id": "token_no",
        "outcome": "No",
        "side": "SELL",
        "price": "0.48",
        "original_size": "5",
        "size_matched": "2",
        "order_owner": "0xowner",
        "associate_trades": [{"id": "t1"}],
    }
    events = list(normalize_polymarket_user_event(message, recv_ms=1_713_871_474_000, condition_id="0xabc"))
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "polymarket_user_order"
    assert event["status"] == "UPDATE"
    assert event["outcome_side"] == "no"
    assert event["order_side"] == "sell"
    assert event["original_size"] == 5.0
    assert event["size_matched"] == 2.0
    assert event["market"] == "0xabc"


def test_normalize_kalshi_market_positions_event() -> None:
    message = {
        "id": 1,
        "type": "market_position",
        "msg": {
            "user_id": 1,
            "market_ticker": "KXBTC15M-TEST",
            "position": 100,
            "fees_paid": 100_000,
            "market_exposure": 1_000_000,
            "realized_pnl": 50_000,
            "position_cost": 2_000_000,
            "position_fee_cost": 100_000,
            "volume": 200,
        },
    }
    events = list(normalize_kalshi_market_positions_event(message, recv_ms=1_000, market_ticker="KXBTC15M-TEST"))
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "kalshi_market_position"
    assert event["market_ticker"] == "KXBTC15M-TEST"
    assert event["position"] == 100.0
    assert event["position_yes"] == 100.0
    assert event["position_no"] == 0.0
    assert event["fees_paid_usd"] == 10.0
    assert event["position_cost_usd"] == 200.0
    assert event["realized_pnl_usd"] == 5.0
    assert event["volume"] == 200.0


def test_normalize_kalshi_market_positions_ticker_filter() -> None:
    message = {
        "type": "market_position",
        "msg": {
            "market_ticker": "KXBTC15M-OTHER",
            "position": 10,
        },
    }
    events = list(normalize_kalshi_market_positions_event(message, recv_ms=1_000, market_ticker="KXBTC15M-TEST"))
    assert events == []


def test_position_collectors_subscription_payloads() -> None:
    null_writer = NullWriter()
    pm = PolymarketUserWsCollector(
        condition_id="0xcond",
        auth={"apiKey": "k", "secret": "s", "passphrase": "p"},
        raw_writer=null_writer,
        event_writer=null_writer,
    )
    pm_payloads = list(pm.subscription_payloads())
    assert len(pm_payloads) == 1
    assert pm_payloads[0]["type"] == "user"
    assert pm_payloads[0]["markets"] == ["0xcond"]
    assert pm_payloads[0]["auth"]["apiKey"] == "k"

    kx = KalshiMarketPositionsWsCollector(
        market_ticker="KXBTC15M-TEST",
        headers={},
        raw_writer=null_writer,
        event_writer=null_writer,
    )
    kx_payloads = list(kx.subscription_payloads())
    assert len(kx_payloads) == 1
    params = kx_payloads[0]["params"]
    assert params["channels"] == ["market_positions"]
    assert params["market_ticker"] == "KXBTC15M-TEST"
    assert params["market_tickers"] == ["KXBTC15M-TEST"]
