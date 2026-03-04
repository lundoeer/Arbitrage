from __future__ import annotations

import os
from typing import Any, Dict, Optional

from scripts.common.buy_execution import build_polymarket_clob_client_from_env
from scripts.common.utils import as_dict


def _resolve_polymarket_signature_type_from_env(default: int = 1) -> int:
    raw = str(os.getenv("POLYMARKET_SIGNATURE_TYPE", "") or "").strip()
    if not raw:
        return int(default)
    try:
        parsed = int(raw)
    except Exception:
        return int(default)
    if parsed not in {0, 1, 2}:
        return int(default)
    return int(parsed)


class PolymarketAccountPollClient:
    """
    Poll authenticated Polymarket account balance/allowance state.

    Uses py-clob-client L2 auth path and reports collateral allowance payload.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://clob.polymarket.com",
        clob_client: Optional[Any] = None,
        signature_type: Optional[int] = None,
    ) -> None:
        if clob_client is None:
            clob_client = build_polymarket_clob_client_from_env(base_url=base_url)
        self.clob_client = clob_client
        self.signature_type = (
            int(signature_type)
            if signature_type is not None
            else _resolve_polymarket_signature_type_from_env(default=1)
        )

    def fetch_balance_allowance(self) -> Dict[str, Any]:
        params: Any
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=int(self.signature_type),
            )
        except Exception:
            params = {
                "asset_type": "COLLATERAL",
                "signature_type": int(self.signature_type),
            }

        allowance = self.clob_client.get_balance_allowance(params)
        return {
            "venue": "polymarket",
            "balance_allowance": as_dict(allowance),
        }
