from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import pytest

import scripts.common.buy_execution as buy_execution
from scripts.common.buy_execution import KalshiApiBuyClient, PolymarketApiBuyClient


class _FakeTransport:
    def __init__(self) -> None:
        self.last_method: str | None = None
        self.last_url: str | None = None
        self.last_headers: Dict[str, Any] | None = None
        self.last_json: Dict[str, Any] | None = None

    def request_json(self, method: str, url: str, *, headers: Dict[str, Any], json: Dict[str, Any], allow_status: set[int]):
        self.last_method = str(method)
        self.last_url = str(url)
        self.last_headers = dict(headers)
        self.last_json = dict(json)
        return 201, {"order": {"status": "resting"}}


def test_kalshi_sell_schema_uses_action_sell() -> None:
    transport = _FakeTransport()
    client = KalshiApiBuyClient(
        api_key="test-key",
        private_key=object(),
        transport=transport,  # type: ignore[arg-type]
    )
    client._sign = lambda **kwargs: "sig"  # type: ignore[method-assign]

    result = client.place_sell_order(
        instrument_id="KXBTC15M-TEST",
        side="no",
        size=3.0,
        order_kind="market",
        limit_price=0.42,
        time_in_force="fak",
        client_order_id="cid-kx-sell-1",
    )

    assert result["ok"] is True
    assert transport.last_method == "POST"
    assert transport.last_url is not None and transport.last_url.endswith("/trade-api/v2/portfolio/orders")
    payload = transport.last_json or {}
    assert payload["action"] == "sell"
    assert payload["side"] == "no"
    assert payload["type"] == "limit"
    assert payload["time_in_force"] == "immediate_or_cancel"
    assert payload["no_price_dollars"] == "0.42"
    assert "yes_price_dollars" not in payload


@dataclass
class _FakeOrderArgs:
    token_id: str
    price: float
    size: float
    side: str
    nonce: int


@dataclass
class _FakeMarketOrderArgs:
    token_id: str
    amount: float
    side: str
    price: float
    nonce: int
    order_type: str


class _FakeSignedOrder:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = dict(payload)

    def dict(self) -> Dict[str, Any]:
        return dict(self._payload)


class _FakeClobClient:
    def __init__(self) -> None:
        self.create_order_calls: list[Dict[str, Any]] = []
        self.create_market_order_calls: list[Dict[str, Any]] = []
        self.post_order_calls: list[Dict[str, Any]] = []

    def create_order(self, order_args: _FakeOrderArgs) -> _FakeSignedOrder:
        payload = {
            "tokenId": str(order_args.token_id),
            "price": float(order_args.price),
            "size": float(order_args.size),
            "side": str(order_args.side),
            "nonce": int(order_args.nonce),
        }
        self.create_order_calls.append(dict(payload))
        return _FakeSignedOrder(payload)

    def create_market_order(self, market_order_args: _FakeMarketOrderArgs) -> _FakeSignedOrder:
        payload = {
            "tokenId": str(market_order_args.token_id),
            "amount": float(market_order_args.amount),
            "side": str(market_order_args.side),
            "price": float(market_order_args.price),
            "nonce": int(market_order_args.nonce),
            "orderType": str(market_order_args.order_type),
        }
        self.create_market_order_calls.append(dict(payload))
        return _FakeSignedOrder(payload)

    def post_order(self, signed_order: _FakeSignedOrder, orderType: str, post_only: bool) -> Dict[str, Any]:
        row = {
            "order": signed_order.dict(),
            "orderType": str(orderType),
            "postOnly": bool(post_only),
        }
        self.post_order_calls.append(dict(row))
        return row


class _FakeNonceManager:
    def __init__(self) -> None:
        self._nonce = 100

    def peek_nonce(self) -> int:
        return int(self._nonce)

    def mark_order_accepted(self, *, nonce: int) -> int:
        self._nonce = int(nonce + 1)
        return int(self._nonce)

    def refresh(self) -> int:
        self._nonce += 1
        return int(self._nonce)


def test_polymarket_sell_schema_signs_with_sell_side(monkeypatch) -> None:
    fake_clob = _FakeClobClient()
    client = PolymarketApiBuyClient(
        clob_client=fake_clob,
        order_args_cls=_FakeOrderArgs,
        market_order_args_cls=_FakeMarketOrderArgs,
        l2_api_key="l2",
        nonce_manager=_FakeNonceManager(),
    )
    monkeypatch.setattr(buy_execution, "_is_polymarket_invalid_nonce_error", lambda exc: False)

    result = client.place_sell_order(
        instrument_id="pm-token-yes",
        side="yes",
        size=2.0,
        order_kind="market",
        limit_price=0.61,
        time_in_force="fak",
        client_order_id="cid-pm-sell-1",
    )

    assert result["ok"] is True
    assert len(fake_clob.create_order_calls) == 1
    assert len(fake_clob.create_market_order_calls) == 0
    signed = fake_clob.create_order_calls[0]
    assert signed["side"] == "SELL"
    assert signed["price"] == 0.61
    post_call = fake_clob.post_order_calls[0]
    assert post_call["orderType"] == "FAK"


def test_polymarket_buy_market_uses_planning_reference_best_ask_for_quote_amount(monkeypatch) -> None:
    fake_clob = _FakeClobClient()
    client = PolymarketApiBuyClient(
        clob_client=fake_clob,
        order_args_cls=_FakeOrderArgs,
        market_order_args_cls=_FakeMarketOrderArgs,
        l2_api_key="l2",
        nonce_manager=_FakeNonceManager(),
    )
    monkeypatch.setattr(buy_execution, "_is_polymarket_invalid_nonce_error", lambda exc: False)

    result = client.place_buy_order(
        instrument_id="pm-token-no",
        side="no",
        size=20.0,
        order_kind="market",
        limit_price=0.70,
        time_in_force="fak",
        client_order_id="cid-pm-buy-1",
        planning_reference_best_ask=0.50,
    )

    assert result["ok"] is True
    assert len(fake_clob.create_market_order_calls) == 1
    signed = fake_clob.create_market_order_calls[0]
    assert signed["price"] == 0.70
    assert signed["amount"] == 10.0


def test_polymarket_buy_market_requires_planning_reference_best_ask(monkeypatch) -> None:
    fake_clob = _FakeClobClient()
    client = PolymarketApiBuyClient(
        clob_client=fake_clob,
        order_args_cls=_FakeOrderArgs,
        market_order_args_cls=_FakeMarketOrderArgs,
        l2_api_key="l2",
        nonce_manager=_FakeNonceManager(),
    )
    monkeypatch.setattr(buy_execution, "_is_polymarket_invalid_nonce_error", lambda exc: False)

    with pytest.raises(RuntimeError, match="planning_reference_best_ask"):
        client.place_buy_order(
            instrument_id="pm-token-no",
            side="no",
            size=5.0,
            order_kind="market",
            limit_price=0.60,
            time_in_force="fak",
            client_order_id="cid-pm-buy-2",
            planning_reference_best_ask=None,
        )
