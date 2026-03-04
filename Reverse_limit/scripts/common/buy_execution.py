from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from scripts.common.api_transport import ApiTransport, RetryConfig


def _safe_id_segment(value: str, *, max_len: int = 12) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value or "").strip().lower()).strip("-")
    if not cleaned:
        return "x"
    return cleaned[: max(1, int(max_len))]


def build_client_order_id(
    *,
    signal_id: str,
    venue: str,
    instrument_id: str,
    side: str,
    seed: Optional[str] = None,
    max_length: int = 64,
) -> str:
    venue_short = {"polymarket": "pm", "kalshi": "kx"}.get(str(venue).strip().lower(), "vx")
    payload = {
        "signal_id": str(signal_id or "").strip(),
        "venue": str(venue or "").strip().lower(),
        "instrument_id": str(instrument_id or "").strip(),
        "side": str(side or "").strip().lower(),
        "seed": str(seed or "").strip(),
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    instrument_short = _safe_id_segment(str(instrument_id), max_len=10)
    cid = f"arb-{venue_short}-{instrument_short}-{digest[:20]}"
    return cid[: max(16, int(max_length))]


@dataclass(frozen=True)
class _PolymarketClobContext:
    clob_client: Any
    order_args_cls: Any
    market_order_args_cls: Any
    l2_api_key: str


def _normalize_eth_address(value: str) -> str:
    raw = str(value or "").strip()
    if not raw.startswith("0x"):
        raw = f"0x{raw}"
    lower = raw.lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", lower):
        raise RuntimeError(f"Invalid ethereum address: {value!r}")
    return lower


def _resolve_clob_signer_address(clob_client: Any) -> str:
    signer = getattr(clob_client, "signer", None)
    if signer is None:
        raise RuntimeError("Polymarket clob client signer is missing")
    address_attr = getattr(signer, "address", None)
    if not callable(address_attr):
        raise RuntimeError("Polymarket clob client signer.address is missing")
    address_value = address_attr()
    address = str(address_value or "").strip()
    if not address:
        raise RuntimeError("Polymarket signer address is empty")
    return address


def _polymarket_l1_private_key_from_env() -> str:
    value = str(os.getenv("POLYMARKET_L1_APIKEY", "") or "").strip()
    if value:
        return value
    raise RuntimeError("Missing Polymarket L1 private key in env (POLYMARKET_L1_APIKEY)")


def _polymarket_l2_creds_from_env() -> Tuple[str, str, str]:
    api_key = str(os.getenv("POLYMARKET_L2_API_KEY", "") or "").strip()
    api_secret = str(os.getenv("POLYMARKET_L2_API_SECRET", "") or "").strip()
    api_passphrase = str(os.getenv("POLYMARKET_L2_API_PASSPHRASE", "") or "").strip()
    missing: List[str] = []
    if not api_key:
        missing.append("POLYMARKET_L2_API_KEY")
    if not api_secret:
        missing.append("POLYMARKET_L2_API_SECRET")
    if not api_passphrase:
        missing.append("POLYMARKET_L2_API_PASSPHRASE")
    if missing:
        raise RuntimeError(f"Missing Polymarket L2 env vars: {', '.join(missing)}")
    return api_key, api_secret, api_passphrase


def _polymarket_chain_id_from_env(default: int = 137) -> int:
    raw = str(os.getenv("POLYMARKET_CHAIN_ID", "") or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except Exception as exc:
        raise RuntimeError(f"Invalid POLYMARKET_CHAIN_ID value: {raw}") from exc
    if value <= 0:
        raise RuntimeError(f"Invalid POLYMARKET_CHAIN_ID value: {raw}")
    return value


def _polymarket_signature_type_from_env(default: int = 1) -> int:
    raw = str(os.getenv("POLYMARKET_SIGNATURE_TYPE", "") or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(raw)
    except Exception as exc:
        raise RuntimeError(f"Invalid POLYMARKET_SIGNATURE_TYPE value: {raw}") from exc
    if value not in {0, 1, 2}:
        raise RuntimeError(f"Invalid POLYMARKET_SIGNATURE_TYPE value: {raw} (expected 0/1/2)")
    return value


def _polymarket_funder_from_env() -> Optional[str]:
    value = str(os.getenv("POLYMARKET_FUNDER", "") or "").strip()
    return value or None


def _polymarket_market_buy_price_cap_from_env(default: float = 0.99) -> float:
    raw = str(os.getenv("POLYMARKET_MARKET_BUY_PRICE_CAP", "") or "").strip()
    if not raw:
        return float(default)
    try:
        value = float(raw)
    except Exception as exc:
        raise RuntimeError(f"Invalid POLYMARKET_MARKET_BUY_PRICE_CAP value: {raw}") from exc
    if value <= 0.0 or value > 1.0:
        raise RuntimeError(f"Invalid POLYMARKET_MARKET_BUY_PRICE_CAP value: {raw} (expected >0 and <=1)")
    return float(value)


def _polymarket_market_sell_price_floor_from_env(default: float = 0.01) -> float:
    raw = str(os.getenv("POLYMARKET_MARKET_SELL_PRICE_FLOOR", "") or "").strip()
    if not raw:
        return float(default)
    try:
        value = float(raw)
    except Exception as exc:
        raise RuntimeError(f"Invalid POLYMARKET_MARKET_SELL_PRICE_FLOOR value: {raw}") from exc
    if value <= 0.0 or value > 1.0:
        raise RuntimeError(f"Invalid POLYMARKET_MARKET_SELL_PRICE_FLOOR value: {raw} (expected >0 and <=1)")
    return float(value)


def _polymarket_funder_rpc_urls_from_env() -> List[str]:
    raw = str(os.getenv("POLYMARKET_FUNDER_RPC_URL", "") or "").strip()
    if raw:
        urls = [part.strip() for part in raw.split(",") if part.strip()]
        if urls:
            return urls
    return [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon-rpc.com",
        "https://polygon.llamarpc.com",
        "https://rpc.ankr.com/polygon",
    ]


def _polymarket_order_type(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"gtc", "good_til_cancelled", "good_till_cancelled"}:
        return "GTC"
    if raw in {"gtd", "good_til_date", "good_till_date"}:
        return "GTD"
    if raw in {"fak", "ioc", "immediate_or_cancel"}:
        return "FAK"
    if raw == "market":
        return "FAK"
    return "FOK"


def derive_polymarket_funder_from_chain(
    *,
    l1_key: str,
    signature_type: int,
    chain_id: int = 137,
    base_url: str = "https://clob.polymarket.com",
    rpc_url: Optional[str] = None,
) -> str:
    sig_type = int(signature_type)
    if sig_type not in {1, 2}:
        raise RuntimeError(f"Funder chain lookup requires signature_type 1 or 2, got: {sig_type}")

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.config import get_contract_config
        from py_clob_client.constants import POLYGON
    except Exception as exc:
        raise RuntimeError("Missing py-clob-client components required for Polymarket funder lookup.") from exc

    probe = ClobClient(
        host=str(base_url).rstrip("/"),
        chain_id=int(chain_id),
        key=str(l1_key),
        signature_type=0,
    )
    signer_address = _normalize_eth_address(_resolve_clob_signer_address(probe))
    contract_config = get_contract_config(POLYGON)
    if contract_config is None:
        raise RuntimeError("Could not load Polymarket contract config for Polygon")
    exchange_address = str(getattr(contract_config, "exchange", "") or "").strip()
    if not exchange_address:
        raise RuntimeError("Polymarket contract config missing exchange address")

    method_selector = "edef7d8e" if sig_type == 1 else "a287bdf1"
    encoded_signer = signer_address.replace("0x", "").rjust(64, "0")
    data = f"0x{method_selector}{encoded_signer}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": exchange_address, "data": data}, "latest"],
    }
    transport = ApiTransport(
        timeout_seconds=10,
        retry_config=RetryConfig(
            max_attempts=3,
            base_backoff_seconds=0.5,
            jitter_ratio=0.2,
            retry_methods=frozenset({"POST"}),
        ),
    )
    rpc_urls = [str(rpc_url).strip()] if rpc_url else _polymarket_funder_rpc_urls_from_env()
    errors: List[str] = []
    body: Optional[Dict[str, Any]] = None
    for url in rpc_urls:
        if not url:
            continue
        try:
            _, response_payload = transport.request_json("POST", url, json=payload, allow_status={200})
            if not isinstance(response_payload, dict):
                raise RuntimeError("Invalid RPC response for Polymarket funder lookup")
            if response_payload.get("error") is not None:
                raise RuntimeError(f"RPC error during Polymarket funder lookup: {response_payload.get('error')}")
            body = response_payload
            break
        except Exception as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")
    if body is None:
        raise RuntimeError(
            "Polymarket funder lookup failed across RPC endpoints. "
            f"Errors: {' | '.join(errors) if errors else 'unknown'}"
        )
    result = body.get("result")
    if not isinstance(result, str) or len(result) < 66:
        raise RuntimeError("RPC result missing/invalid during Polymarket funder lookup")

    funder = _normalize_eth_address("0x" + result[-40:])
    if funder == "0x0000000000000000000000000000000000000000":
        raise RuntimeError("Derived Polymarket funder resolved to zero address")
    return funder


def _resolve_polymarket_nonce_address(clob_client: Any) -> str:
    builder = getattr(clob_client, "builder", None)
    funder = str(getattr(builder, "funder", "") or "").strip()
    if funder:
        return _normalize_eth_address(funder)
    return _normalize_eth_address(_resolve_clob_signer_address(clob_client))


def _fetch_polymarket_exchange_nonce(*, nonce_address: str) -> int:
    try:
        from py_clob_client.config import get_contract_config
        from py_clob_client.constants import POLYGON
    except Exception as exc:
        raise RuntimeError("Missing py-clob-client components required for Polymarket nonce lookup.") from exc

    contract_config = get_contract_config(POLYGON)
    if contract_config is None:
        raise RuntimeError("Could not load Polymarket contract config for Polygon")
    exchange_address = str(getattr(contract_config, "exchange", "") or "").strip()
    if not exchange_address:
        raise RuntimeError("Polymarket contract config missing exchange address")

    encoded_addr = str(nonce_address).replace("0x", "").lower().rjust(64, "0")
    data = f"0x7ecebe00{encoded_addr}"  # nonces(address)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": exchange_address, "data": data}, "latest"],
    }

    transport = ApiTransport(
        timeout_seconds=10,
        retry_config=RetryConfig(
            max_attempts=3,
            base_backoff_seconds=0.5,
            jitter_ratio=0.2,
            retry_methods=frozenset({"POST"}),
        ),
    )
    errors: List[str] = []
    for url in _polymarket_funder_rpc_urls_from_env():
        if not url:
            continue
        try:
            _, response_payload = transport.request_json("POST", url, json=payload, allow_status={200})
            if not isinstance(response_payload, dict):
                raise RuntimeError("Invalid RPC response for Polymarket nonce lookup")
            if response_payload.get("error") is not None:
                raise RuntimeError(f"RPC error during Polymarket nonce lookup: {response_payload.get('error')}")
            result = response_payload.get("result")
            if not isinstance(result, str) or not result.startswith("0x"):
                raise RuntimeError("RPC result missing/invalid during Polymarket nonce lookup")
            return int(result, 16)
        except Exception as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")

    raise RuntimeError(
        f"Polymarket nonce lookup failed across RPC endpoints. Errors: {' | '.join(errors) if errors else 'unknown'}"
    )


@dataclass
class PolymarketNonceManager:
    clob_client: Any

    def __post_init__(self) -> None:
        self.nonce_address = _resolve_polymarket_nonce_address(self.clob_client)
        self._next_nonce = int(_fetch_polymarket_exchange_nonce(nonce_address=self.nonce_address))

    def peek_nonce(self) -> int:
        return int(self._next_nonce)

    def mark_order_accepted(self, *, nonce: int) -> int:
        accepted = int(nonce)
        if accepted >= int(self._next_nonce):
            self._next_nonce = int(accepted + 1)
        return int(self._next_nonce)

    def refresh(self) -> int:
        self._next_nonce = int(_fetch_polymarket_exchange_nonce(nonce_address=self.nonce_address))
        return int(self._next_nonce)


def _is_polymarket_invalid_nonce_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return "invalid nonce" in text


def _build_polymarket_signed_order(
    *,
    clob_client: Any,
    order_args_cls: Any,
    market_order_args_cls: Any,
    l2_api_key: str,
    action: str,
    instrument_id: str,
    size: float,
    order_kind: str,
    limit_price: Optional[float],
    time_in_force: Optional[str],
    nonce: int,
    planning_reference_best_ask: Optional[float] = None,
) -> Tuple[Any, Dict[str, Any]]:
    token_id = str(instrument_id or "").strip()
    action_norm = str(action or "").strip().lower()
    kind = str(order_kind or "").strip().lower()
    if action_norm not in {"buy", "sell"}:
        raise RuntimeError(f"Polymarket action must be buy/sell, got: {action}")
    if not token_id:
        raise RuntimeError(f"Polymarket {action_norm} order requires instrument_id token_id")

    effective_limit_price = limit_price
    if kind == "market" and effective_limit_price is None:
        if action_norm == "buy":
            effective_limit_price = _polymarket_market_buy_price_cap_from_env(default=0.99)
        else:
            effective_limit_price = _polymarket_market_sell_price_floor_from_env(default=0.01)
    if effective_limit_price is None:
        raise RuntimeError("Polymarket order signing requires limit_price")
    if float(size) <= 0:
        raise RuntimeError(f"Polymarket {action_norm} order requires positive size")

    if kind == "market" and action_norm == "buy":
        if planning_reference_best_ask is None or float(planning_reference_best_ask) <= 0:
            raise RuntimeError("Polymarket market buy requires positive planning_reference_best_ask")
        quote_amount = float(size) * float(planning_reference_best_ask)
        signed_order = clob_client.create_market_order(
            market_order_args_cls(
                token_id=token_id,
                amount=float(quote_amount),
                side="BUY",
                price=float(effective_limit_price),
                nonce=int(nonce),
                order_type=_polymarket_order_type(time_in_force if time_in_force else kind),
            )
        )
    else:
        side = "BUY" if action_norm == "buy" else "SELL"
        signed_order = clob_client.create_order(
            order_args_cls(
                token_id=token_id,
                price=float(effective_limit_price),
                size=float(size),
                side=side,
                nonce=int(nonce),
            )
        )
    payload = {
        "order": signed_order.dict(),
        "owner": l2_api_key,
        "orderType": _polymarket_order_type(time_in_force if time_in_force else order_kind),
        "postOnly": False,
    }
    return signed_order, payload


def _build_polymarket_clob_context_from_env(*, base_url: str = "https://clob.polymarket.com") -> _PolymarketClobContext:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderArgs
    except Exception as exc:
        raise RuntimeError("Missing dependency py-clob-client. Install it to enable Polymarket order signing.") from exc

    l1_key = _polymarket_l1_private_key_from_env()
    l2_key, l2_secret, l2_passphrase = _polymarket_l2_creds_from_env()
    chain_id = _polymarket_chain_id_from_env()
    signature_type = _polymarket_signature_type_from_env(default=1)
    funder = _polymarket_funder_from_env()
    if int(signature_type) in {1, 2}:
        derived_funder = derive_polymarket_funder_from_chain(
            l1_key=l1_key,
            signature_type=int(signature_type),
            chain_id=int(chain_id),
            base_url=base_url,
        )
        if funder:
            configured = _normalize_eth_address(funder)
            if configured != derived_funder:
                raise RuntimeError(
                    "POLYMARKET_FUNDER mismatch: configured "
                    f"{configured} != on_chain {derived_funder}. Refusing to start trading."
                )
            funder = configured
        else:
            funder = derived_funder

    client_kwargs: Dict[str, Any] = {
        "host": str(base_url).rstrip("/"),
        "chain_id": int(chain_id),
        "key": l1_key,
        "signature_type": int(signature_type),
    }
    if funder:
        client_kwargs["funder"] = funder

    clob_client = ClobClient(**client_kwargs)
    clob_client.set_api_creds(
        ApiCreds(
            api_key=l2_key,
            api_secret=l2_secret,
            api_passphrase=l2_passphrase,
        )
    )
    return _PolymarketClobContext(
        clob_client=clob_client,
        order_args_cls=OrderArgs,
        market_order_args_cls=MarketOrderArgs,
        l2_api_key=l2_key,
    )


def build_polymarket_clob_client_from_env(*, base_url: str = "https://clob.polymarket.com") -> Any:
    return _build_polymarket_clob_context_from_env(base_url=base_url).clob_client


class PolymarketApiBuyClient:
    def __init__(
        self,
        *,
        base_url: str,
        clob_client: Any,
        order_args_cls: Any,
        market_order_args_cls: Any,
        l2_api_key: str,
        nonce_manager: Optional[PolymarketNonceManager] = None,
        transport: Optional[ApiTransport] = None,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.clob_client = clob_client
        self.order_args_cls = order_args_cls
        self.market_order_args_cls = market_order_args_cls
        self.l2_api_key = str(l2_api_key)
        self.nonce_manager = nonce_manager if nonce_manager is not None else PolymarketNonceManager(clob_client=clob_client)
        self.transport = transport or ApiTransport(timeout_seconds=10)

    def place_buy_order(
        self,
        *,
        instrument_id: str,
        side: str,
        size: float,
        order_kind: str,
        limit_price: Optional[float],
        time_in_force: Optional[str],
        client_order_id: str,
        planning_reference_best_ask: Optional[float] = None,
    ) -> Dict[str, Any]:
        return self._place_order(
            action="buy",
            instrument_id=instrument_id,
            side=side,
            size=size,
            order_kind=order_kind,
            limit_price=limit_price,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
            planning_reference_best_ask=planning_reference_best_ask,
        )

    def _place_order(
        self,
        *,
        action: str,
        instrument_id: str,
        side: str,
        size: float,
        order_kind: str,
        limit_price: Optional[float],
        time_in_force: Optional[str],
        client_order_id: str,
        planning_reference_best_ask: Optional[float] = None,
    ) -> Dict[str, Any]:
        def _submit_once() -> Tuple[int, Dict[str, Any], Any]:
            nonce = self.nonce_manager.peek_nonce()
            signed_order, payload = _build_polymarket_signed_order(
                clob_client=self.clob_client,
                order_args_cls=self.order_args_cls,
                market_order_args_cls=self.market_order_args_cls,
                l2_api_key=self.l2_api_key,
                action=action,
                instrument_id=instrument_id,
                size=float(size),
                order_kind=order_kind,
                limit_price=limit_price,
                time_in_force=time_in_force,
                nonce=nonce,
                planning_reference_best_ask=planning_reference_best_ask,
            )
            response_payload_inner = self._post_order_with_retry(signed_order, payload)
            return nonce, payload, response_payload_inner

        try:
            nonce, payload, response_payload = _submit_once()
            self.nonce_manager.mark_order_accepted(nonce=nonce)
        except Exception as exc:
            if not _is_polymarket_invalid_nonce_error(exc):
                raise
            self.nonce_manager.refresh()
            nonce, payload, response_payload = _submit_once()
            self.nonce_manager.mark_order_accepted(nonce=nonce)

        return {
            "ok": True,
            "venue": "polymarket",
            "status_code": 200,
            "request": payload,
            "response": response_payload,
        }

    def _post_order_with_retry(self, signed_order: Any, payload: Dict[str, Any]) -> Any:
        retry = self.transport.retry
        max_attempts = max(1, int(retry.max_attempts))
        last_exc: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                return self.clob_client.post_order(
                    signed_order,
                    orderType=str(payload.get("orderType") or "GTC"),
                    post_only=bool(payload.get("postOnly", False)),
                )
            except Exception as exc:
                last_exc = exc
                exc_name = type(exc).__name__
                is_transient = any(
                    keyword in exc_name.lower() or keyword in str(exc).lower()
                    for keyword in ("timeout", "connection", "503", "502", "504", "429")
                )
                if not is_transient or attempt >= max_attempts:
                    raise
                self.transport._sleep_backoff(attempt)

        raise RuntimeError(f"post_order failed: {last_exc}")


def build_polymarket_api_buy_client_from_env(
    *,
    base_url: str = "https://clob.polymarket.com",
    transport: Optional[ApiTransport] = None,
) -> PolymarketApiBuyClient:
    clob_context = _build_polymarket_clob_context_from_env(base_url=base_url)
    nonce_manager = PolymarketNonceManager(clob_client=clob_context.clob_client)
    return PolymarketApiBuyClient(
        base_url=base_url,
        clob_client=clob_context.clob_client,
        order_args_cls=clob_context.order_args_cls,
        market_order_args_cls=clob_context.market_order_args_cls,
        l2_api_key=clob_context.l2_api_key,
        nonce_manager=nonce_manager,
        transport=transport,
    )
