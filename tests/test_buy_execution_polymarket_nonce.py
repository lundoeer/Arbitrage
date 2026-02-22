from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable

import pytest

import scripts.common.buy_execution as buy_execution
from scripts.common.buy_execution import PolymarketApiBuyClient, PolymarketNonceManager


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
    def __init__(self, *, nonce: int, token_id: str, price: float, size: float, side: str) -> None:
        self._payload = {
            "nonce": int(nonce),
            "tokenId": str(token_id),
            "price": float(price),
            "size": float(size),
            "side": str(side),
        }

    def dict(self) -> Dict[str, Any]:
        return dict(self._payload)


class _FakeClobClient:
    def __init__(self, *, fail_invalid_nonce_times: int = 0) -> None:
        self.fail_invalid_nonce_times = int(fail_invalid_nonce_times)
        self.created_nonces: list[int] = []
        self.post_calls = 0

    def create_order(self, order_args: _FakeOrderArgs) -> _FakeSignedOrder:
        self.created_nonces.append(int(order_args.nonce))
        return _FakeSignedOrder(
            nonce=int(order_args.nonce),
            token_id=str(order_args.token_id),
            price=float(order_args.price),
            size=float(order_args.size),
            side=str(order_args.side),
        )

    def post_order(self, signed_order: _FakeSignedOrder, orderType: str, post_only: bool) -> Dict[str, Any]:
        self.post_calls += 1
        if self.post_calls <= self.fail_invalid_nonce_times:
            raise RuntimeError("PolyApiException[status_code=400, error_message={'error': 'invalid nonce'}]")
        return {
            "ok": True,
            "orderType": str(orderType),
            "postOnly": bool(post_only),
            "accepted_nonce": int(signed_order.dict().get("nonce", 0)),
        }


def _patch_nonce_lookup(monkeypatch: pytest.MonkeyPatch, values: Iterable[int]) -> None:
    value_list = [int(v) for v in values]
    iterator = iter(value_list)
    fallback = int(value_list[-1]) if value_list else 0

    monkeypatch.setattr(
        buy_execution,
        "_resolve_polymarket_nonce_address",
        lambda clob_client: "0x0000000000000000000000000000000000000001",
    )

    def _fake_fetch(*, clob_client: Any, nonce_address: str) -> int:
        try:
            return int(next(iterator))
        except StopIteration:
            return int(fallback)

    monkeypatch.setattr(
        buy_execution,
        "_fetch_polymarket_exchange_nonce",
        _fake_fetch,
    )


def test_polymarket_nonce_manager_uses_chain_and_advances_on_accept(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_nonce_lookup(monkeypatch, [42, 84])
    manager = PolymarketNonceManager(clob_client=object())

    assert manager.peek_nonce() == 42
    assert manager.peek_nonce() == 42

    manager.mark_order_accepted(nonce=42)
    assert manager.peek_nonce() == 43

    manager.refresh()
    assert manager.peek_nonce() == 84

    manager.mark_order_accepted(nonce=84)
    assert manager.peek_nonce() == 85


def test_polymarket_api_buy_client_retries_once_on_invalid_nonce(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_nonce_lookup(monkeypatch, [500, 501])
    clob = _FakeClobClient(fail_invalid_nonce_times=1)
    manager = PolymarketNonceManager(clob_client=clob)
    client = PolymarketApiBuyClient(
        clob_client=clob,
        order_args_cls=_FakeOrderArgs,
        market_order_args_cls=_FakeMarketOrderArgs,
        l2_api_key="l2-key",
        nonce_manager=manager,
    )

    payload = client.place_buy_order(
        instrument_id="pm-token-1",
        side="yes",
        size=1.0,
        order_kind="limit",
        limit_price=0.55,
        time_in_force="fak",
        client_order_id="cid-1",
    )

    assert payload["ok"] is True
    assert clob.post_calls == 2
    assert clob.created_nonces == [500, 501]
    assert int(payload["response"]["accepted_nonce"]) == 501
    assert manager.peek_nonce() == 502


def test_polymarket_api_buy_client_fails_after_second_invalid_nonce(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_nonce_lookup(monkeypatch, [900, 901])
    clob = _FakeClobClient(fail_invalid_nonce_times=2)
    manager = PolymarketNonceManager(clob_client=clob)
    client = PolymarketApiBuyClient(
        clob_client=clob,
        order_args_cls=_FakeOrderArgs,
        market_order_args_cls=_FakeMarketOrderArgs,
        l2_api_key="l2-key",
        nonce_manager=manager,
    )

    with pytest.raises(RuntimeError, match="invalid nonce"):
        client.place_buy_order(
            instrument_id="pm-token-2",
            side="yes",
            size=2.0,
            order_kind="limit",
            limit_price=0.60,
            time_in_force="fak",
            client_order_id="cid-2",
        )

    assert clob.post_calls == 2
    assert clob.created_nonces == [900, 901]
    # Last attempt was rejected, so runtime should still hold that nonce.
    assert manager.peek_nonce() == 901
