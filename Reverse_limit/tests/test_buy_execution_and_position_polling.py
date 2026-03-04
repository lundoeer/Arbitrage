from __future__ import annotations

from scripts.common.buy_execution import build_client_order_id
from scripts.common.position_polling import PolymarketAccountPollClient


class _StrictClobClient:
    def __init__(self) -> None:
        self.calls = 0

    def get_balance_allowance(self, params: object) -> dict:
        self.calls += 1
        assert params is not None
        return {
            "balance": "12.34",
            "allowance": "56.78",
        }


def test_build_client_order_id_is_deterministic_and_sensitive_to_inputs() -> None:
    cid_a = build_client_order_id(
        signal_id="reverse-premarket-1772555400-1483937",
        venue="polymarket",
        instrument_id="55713813804142270500445196993359201616144459307396863549709319905402600058822",
        side="yes",
        seed="reverse-limit-premarket",
    )
    cid_b = build_client_order_id(
        signal_id="reverse-premarket-1772555400-1483937",
        venue="polymarket",
        instrument_id="55713813804142270500445196993359201616144459307396863549709319905402600058822",
        side="yes",
        seed="reverse-limit-premarket",
    )
    cid_c = build_client_order_id(
        signal_id="reverse-premarket-1772555400-1483937",
        venue="polymarket",
        instrument_id="55713813804142270500445196993359201616144459307396863549709319905402600058822",
        side="no",
        seed="reverse-limit-premarket",
    )

    assert cid_a == cid_b
    assert cid_a != cid_c
    assert cid_a.startswith("arb-pm-")
    assert 16 <= len(cid_a) <= 64


def test_polymarket_account_poll_client_fetch_balance_allowance() -> None:
    client = PolymarketAccountPollClient(
        clob_client=_StrictClobClient(),
        signature_type=1,
    )
    snapshot = client.fetch_balance_allowance()

    assert snapshot["venue"] == "polymarket"
    assert snapshot["balance_allowance"]["balance"] == "12.34"
    assert snapshot["balance_allowance"]["allowance"] == "56.78"

