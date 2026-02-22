from __future__ import annotations

from typing import Any, Dict, List

from scripts.common.position_polling import (
    capture_account_portfolio_snapshot,
    KalshiPositionsPollClient,
    PolymarketAccountPollClient,
    PolymarketPositionsPollClient,
    PositionPollClientConfig,
    PositionReconcileLoop,
    PositionReconcileLoopConfig,
)
from scripts.common.position_runtime import PositionRuntime


class _StubTransport:
    def __init__(self, responses: List[tuple[int, Any]]) -> None:
        self._responses = list(responses)
        self.calls: List[Dict[str, Any]] = []

    def request_json(self, method: str, url: str, **kwargs: Any) -> tuple[int, Any]:
        self.calls.append({"method": method, "url": url, "kwargs": kwargs})
        if not self._responses:
            raise RuntimeError("No stub response left")
        return self._responses.pop(0)


class _StaticClient:
    def __init__(self, payload: Dict[str, Any], *, should_raise: bool = False) -> None:
        self.payload = dict(payload)
        self.should_raise = bool(should_raise)
        self.calls = 0

    def fetch_positions(self) -> Dict[str, Any]:
        self.calls += 1
        if self.should_raise:
            raise RuntimeError("poll_failed")
        return dict(self.payload)


class _StubPolymarketClobClient:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = dict(payload)

    def get_balance_allowance(self, _params: Any) -> Dict[str, Any]:
        return dict(self.payload)


def _runtime(now_epoch_ms: int = 1_000) -> PositionRuntime:
    return PositionRuntime(
        polymarket_token_yes="pm_yes_token",
        polymarket_token_no="pm_no_token",
        kalshi_ticker="KXBTC15M-TEST",
        now_epoch_ms=now_epoch_ms,
    )


def test_polymarket_positions_poll_client_filters_selected_pair() -> None:
    transport = _StubTransport(
        [
            (
                200,
                [
                    {"conditionId": "0xcond", "asset": "pm_yes_token", "outcome": "Yes", "size": "2.5"},
                    {"conditionId": "0xcond", "asset": "pm_no_token", "outcome": "No", "size": "1.0"},
                    {"conditionId": "0xother", "asset": "pm_yes_token", "outcome": "Yes", "size": "99.0"},
                    {"conditionId": "0xcond", "asset": "other_asset", "outcome": "Yes", "size": "99.0"},
                ],
            )
        ]
    )
    client = PolymarketPositionsPollClient(
        user_address="0xabc",
        condition_id="0xcond",
        token_yes="pm_yes_token",
        token_no="pm_no_token",
        transport=transport,  # type: ignore[arg-type]
    )

    snapshot = client.fetch_positions()
    assert snapshot["venue"] == "polymarket"
    assert snapshot["raw_count"] == 4
    assert snapshot["filtered_count"] == 2
    assert snapshot["positions"][0]["net_contracts"] == 2.5
    assert snapshot["positions"][1]["net_contracts"] == 1.0
    assert transport.calls[0]["kwargs"]["params"]["user"] == "0xabc"


def test_kalshi_positions_poll_client_handles_cursor_pagination() -> None:
    transport = _StubTransport(
        [
            (
                200,
                {
                    "market_positions": [
                        {
                            "ticker": "KXBTC15M-TEST",
                            "position": 2,
                            "fees_paid": 100_000,
                            "realized_pnl": 50_000,
                        }
                    ],
                    "cursor": "next-page",
                },
            ),
            (
                200,
                {
                    "market_positions": [{"ticker": "KXBTC15M-TEST", "position": -1}],
                    "cursor": "",
                },
            ),
        ]
    )
    client = KalshiPositionsPollClient(
        market_ticker="KXBTC15M-TEST",
        transport=transport,  # type: ignore[arg-type]
        api_key="k",
        private_key=object(),
        headers_factory=lambda _path: {"X-Test": "1"},
        config=PositionPollClientConfig(kalshi_page_limit=100, kalshi_max_pages=5),
    )

    snapshot = client.fetch_positions()
    assert snapshot["venue"] == "kalshi"
    assert snapshot["pages"] == 2
    assert snapshot["raw_count"] == 2
    assert snapshot["positions"][0]["outcome_side"] == "yes"
    assert snapshot["positions"][0]["net_contracts"] == 1.0
    assert snapshot["positions"][1]["outcome_side"] == "no"
    assert snapshot["positions"][1]["net_contracts"] == 0.0
    assert snapshot["positions"][0]["fees"] == 10.0
    assert snapshot["positions"][0]["realized_pnl"] == 5.0
    assert len(transport.calls) == 2
    assert transport.calls[0]["kwargs"]["params"]["ticker"] == "KXBTC15M-TEST"
    assert transport.calls[1]["kwargs"]["params"]["cursor"] == "next-page"


def test_position_reconcile_loop_success_and_failure_flow() -> None:
    runtime = _runtime(now_epoch_ms=10_000)
    pm_client = _StaticClient(
        {
            "positions": [
                {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 1.0},
                {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
            ]
        }
    )
    kx_client_fail = _StaticClient({"positions": []}, should_raise=True)
    loop = PositionReconcileLoop(
        runtime=runtime,
        polymarket_client=pm_client,  # type: ignore[arg-type]
        kalshi_client=kx_client_fail,  # type: ignore[arg-type]
        config=PositionReconcileLoopConfig(polymarket_poll_seconds=10.0, kalshi_poll_seconds=20.0),
    )

    first = loop.run_once(now_epoch_ms=11_000, force=True)
    assert first["venues"]["polymarket"]["status"] == "ok"
    assert first["venues"]["kalshi"]["status"] == "error"
    assert loop.stats["polymarket_success"] == 1
    assert loop.stats["kalshi_failure"] == 1
    assert runtime.snapshot(now_epoch_ms=11_000)["health"]["buy_execution_allowed"] is False

    loop.kalshi_client = _StaticClient(
        {
            "positions": [
                {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0.0},
                {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0.0},
            ]
        }
    )  # type: ignore[assignment]
    second = loop.run_once(now_epoch_ms=12_000, force=True)
    assert second["venues"]["polymarket"]["status"] == "ok"
    assert second["venues"]["kalshi"]["status"] == "ok"
    assert loop.stats["kalshi_success"] == 1
    assert runtime.snapshot(now_epoch_ms=12_000)["health"]["buy_execution_allowed"] is True


def test_position_reconcile_loop_respects_poll_intervals_without_force() -> None:
    runtime = _runtime(now_epoch_ms=100_000)
    pm_client = _StaticClient(
        {
            "positions": [
                {"instrument_id": "pm_yes_token", "outcome_side": "yes", "net_contracts": 0.0},
                {"instrument_id": "pm_no_token", "outcome_side": "no", "net_contracts": 0.0},
            ]
        }
    )
    kx_client = _StaticClient(
        {
            "positions": [
                {"instrument_id": "KXBTC15M-TEST", "outcome_side": "yes", "net_contracts": 0.0},
                {"instrument_id": "KXBTC15M-TEST", "outcome_side": "no", "net_contracts": 0.0},
            ]
        }
    )
    loop = PositionReconcileLoop(
        runtime=runtime,
        polymarket_client=pm_client,  # type: ignore[arg-type]
        kalshi_client=kx_client,  # type: ignore[arg-type]
        config=PositionReconcileLoopConfig(polymarket_poll_seconds=10.0, kalshi_poll_seconds=20.0),
    )

    run1 = loop.run_once(now_epoch_ms=100_000, force=False)
    assert "polymarket" in run1["venues"]
    assert "kalshi" in run1["venues"]
    assert pm_client.calls == 1
    assert kx_client.calls == 1

    run2 = loop.run_once(now_epoch_ms=105_000, force=False)
    assert run2["venues"] == {}
    assert pm_client.calls == 1
    assert kx_client.calls == 1

    run3 = loop.run_once(now_epoch_ms=111_000, force=False)
    assert "polymarket" in run3["venues"]
    assert "kalshi" not in run3["venues"]
    assert pm_client.calls == 2
    assert kx_client.calls == 1

    run4 = loop.run_once(now_epoch_ms=121_000, force=False)
    assert "polymarket" in run4["venues"]
    assert "kalshi" in run4["venues"]
    assert pm_client.calls == 3
    assert kx_client.calls == 2


def test_kalshi_positions_poll_client_fetch_balance() -> None:
    transport = _StubTransport(
        [
            (
                200,
                {
                    "balance": "100.00",
                    "available_balance": "99.00",
                },
            )
        ]
    )
    client = KalshiPositionsPollClient(
        market_ticker="KXBTC15M-TEST",
        transport=transport,  # type: ignore[arg-type]
        api_key="k",
        private_key=object(),
        headers_factory=lambda _path: {"X-Test": "1"},
        config=PositionPollClientConfig(kalshi_page_limit=100, kalshi_max_pages=5),
    )

    snapshot = client.fetch_balance()
    assert snapshot["venue"] == "kalshi"
    assert snapshot["balance"]["balance"] == "100.00"
    assert snapshot["balance"]["available_balance"] == "99.00"
    assert transport.calls[0]["kwargs"]["allow_status"] == {200}


def test_capture_account_portfolio_snapshot_reports_venue_statuses() -> None:
    pm_transport = _StubTransport(
        [
            (
                200,
                [
                    {"conditionId": "0xcond", "asset": "pm_yes_token", "outcome": "Yes", "size": "1.0"},
                    {"conditionId": "0xcond", "asset": "pm_no_token", "outcome": "No", "size": "2.0"},
                ],
            )
        ]
    )
    pm_client = PolymarketPositionsPollClient(
        user_address="0xabc",
        condition_id="0xcond",
        token_yes="pm_yes_token",
        token_no="pm_no_token",
        transport=pm_transport,  # type: ignore[arg-type]
    )

    kx_transport = _StubTransport(
        [
            (
                200,
                {
                    "market_positions": [{"ticker": "KXBTC15M-TEST", "position": 2}],
                    "cursor": "",
                },
            ),
            (
                200,
                {
                    "balance": "100.00",
                    "available_balance": "99.00",
                },
            ),
        ]
    )
    kx_client = KalshiPositionsPollClient(
        market_ticker="KXBTC15M-TEST",
        transport=kx_transport,  # type: ignore[arg-type]
        api_key="k",
        private_key=object(),
        headers_factory=lambda _path: {"X-Test": "1"},
    )

    snapshot = capture_account_portfolio_snapshot(
        polymarket_client=pm_client,
        polymarket_account_client=None,
        kalshi_client=kx_client,
        now_epoch_ms=123,
    )
    assert snapshot["at_ms"] == 123
    assert snapshot["venues"]["polymarket"]["status"] == "ok"
    assert snapshot["venues"]["kalshi"]["status"] == "ok"
    assert snapshot["venues"]["kalshi"]["balance"]["balance"]["balance"] == "100.00"

    snapshot_unavailable = capture_account_portfolio_snapshot(
        polymarket_client=None,
        polymarket_account_client=None,
        kalshi_client=None,
        now_epoch_ms=124,
    )
    assert snapshot_unavailable["venues"]["polymarket"]["status"] == "unavailable"
    assert snapshot_unavailable["venues"]["kalshi"]["status"] == "unavailable"


def test_polymarket_account_poll_client_fetch_balance_allowance() -> None:
    client = PolymarketAccountPollClient(
        clob_client=_StubPolymarketClobClient(
            {
                "balance": "12.34",
                "allowance": "56.78",
            }
        ),
        signature_type=1,
    )

    snapshot = client.fetch_balance_allowance()
    assert snapshot["venue"] == "polymarket"
    assert snapshot["balance_allowance"]["balance"] == "12.34"
    assert snapshot["balance_allowance"]["allowance"] == "56.78"
