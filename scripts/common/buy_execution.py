from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from scripts.common.api_transport import ApiTransport, RetryConfig
from scripts.common.utils import as_float as _as_float, normalize_kalshi_pem as _normalize_kalshi_pem, now_ms


# _as_float imported from scripts.common.utils


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    return None


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
    """
    Deterministic, venue-aware client order id.

    Hash inputs include venue + instrument + side so one signal can safely
    produce distinct ids across legs and venues.
    """
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
class BuyExecutionLeg:
    venue: str
    side: str
    action: str
    instrument_id: str
    order_kind: str
    size: float
    limit_price: Optional[float] = None
    time_in_force: Optional[str] = None
    client_order_id_seed: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> BuyExecutionLeg:
        size = _as_float(payload.get("size"))
        if size is None or size <= 0:
            raise RuntimeError("Invalid execution leg: size must be positive")
        return BuyExecutionLeg(
            venue=str(payload.get("venue") or "").strip().lower(),
            side=str(payload.get("side") or "").strip().lower(),
            action=str(payload.get("action") or "").strip().lower(),
            instrument_id=str(payload.get("instrument_id") or "").strip(),
            order_kind=str(payload.get("order_kind") or "").strip().lower(),
            size=float(size),
            limit_price=_as_float(payload.get("limit_price")),
            time_in_force=(
                None
                if payload.get("time_in_force") is None
                else str(payload.get("time_in_force")).strip().lower()
            ),
            client_order_id_seed=(
                None
                if payload.get("client_order_id_seed") is None
                else str(payload.get("client_order_id_seed")).strip()
            ),
            metadata=dict(payload.get("metadata") or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue": self.venue,
            "side": self.side,
            "action": self.action,
            "instrument_id": self.instrument_id,
            "order_kind": self.order_kind,
            "size": float(self.size),
            "limit_price": None if self.limit_price is None else float(self.limit_price),
            "time_in_force": self.time_in_force,
            "client_order_id_seed": self.client_order_id_seed,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class BuyExecutionPlan:
    signal_id: str
    market: Dict[str, Any]
    created_at_ms: int
    execution_mode: str
    legs: List[BuyExecutionLeg]
    max_quote_age_ms: Optional[int] = None
    policy: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> BuyExecutionPlan:
        signal_id = str(payload.get("signal_id") or "").strip()
        if not signal_id:
            raise RuntimeError("Invalid execution plan: missing signal_id")
        legs_raw = payload.get("legs")
        if not isinstance(legs_raw, list) or not legs_raw:
            raise RuntimeError("Invalid execution plan: legs must be a non-empty list")
        legs = [BuyExecutionLeg.from_dict(dict(item or {})) for item in legs_raw]
        created_at = _as_int(payload.get("created_at_ms"))
        return BuyExecutionPlan(
            signal_id=signal_id,
            market=dict(payload.get("market") or {}),
            created_at_ms=int(created_at if created_at is not None else now_ms()),
            execution_mode=str(payload.get("execution_mode") or "one_leg").strip().lower(),
            legs=legs,
            max_quote_age_ms=_as_int(payload.get("max_quote_age_ms")),
            policy=dict(payload.get("policy") or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "market": dict(self.market),
            "created_at_ms": int(self.created_at_ms),
            "execution_mode": self.execution_mode,
            "legs": [leg.to_dict() for leg in self.legs],
            "max_quote_age_ms": None if self.max_quote_age_ms is None else int(self.max_quote_age_ms),
            "policy": dict(self.policy),
        }


@dataclass(frozen=True)
class LegSubmitResult:
    venue: str
    side: str
    instrument_id: str
    client_order_id: str
    request_payload: Dict[str, Any]
    response_payload: Optional[Dict[str, Any]]
    submitted: bool
    error: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "venue": self.venue,
            "side": self.side,
            "instrument_id": self.instrument_id,
            "client_order_id": self.client_order_id,
            "request_payload": dict(self.request_payload),
            "response_payload": None if self.response_payload is None else dict(self.response_payload),
            "submitted": bool(self.submitted),
            "error": self.error,
        }


@dataclass(frozen=True)
class BuyExecutionResult:
    signal_id: str
    status: str
    submitted_at_ms: int
    completed_at_ms: int
    legs: List[LegSubmitResult]
    idempotency: Dict[str, Any]
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "status": self.status,
            "submitted_at_ms": int(self.submitted_at_ms),
            "completed_at_ms": int(self.completed_at_ms),
            "legs": [leg.to_dict() for leg in self.legs],
            "idempotency": dict(self.idempotency),
            "error": self.error,
        }


@dataclass
class IdempotencyRecord:
    signal_id: str
    created_at_ms: int
    updated_at_ms: int
    status: str
    client_order_ids: Dict[str, str]
    result: Optional[Dict[str, Any]] = None


class BuyIdempotencyState:
    """
    In-memory idempotency state.

    First implementation is process-local by design. This can later move to a
    durable store for restart safety.
    """

    def __init__(self) -> None:
        self._records: Dict[str, IdempotencyRecord] = {}

    def get(self, signal_id: str) -> Optional[IdempotencyRecord]:
        key = str(signal_id or "").strip()
        if not key:
            return None
        return self._records.get(key)

    def has_signal(self, signal_id: str) -> bool:
        return self.get(signal_id) is not None

    def is_in_flight_or_completed(self, signal_id: str) -> bool:
        record = self.get(signal_id)
        if record is None:
            return False
        return record.status in {"in_flight", "completed", "submitted", "partially_submitted", "rejected", "error"}

    def mark_in_flight(self, *, signal_id: str, client_order_ids: Dict[str, str]) -> None:
        ts = now_ms()
        self._records[str(signal_id)] = IdempotencyRecord(
            signal_id=str(signal_id),
            created_at_ms=ts,
            updated_at_ms=ts,
            status="in_flight",
            client_order_ids=dict(client_order_ids),
            result=None,
        )

    def mark_final(self, *, signal_id: str, status: str, result: Dict[str, Any]) -> None:
        ts = now_ms()
        key = str(signal_id)
        prior = self._records.get(key)
        created = int(prior.created_at_ms) if prior is not None else ts
        client_order_ids = dict(prior.client_order_ids) if prior is not None else {}
        self._records[key] = IdempotencyRecord(
            signal_id=key,
            created_at_ms=created,
            updated_at_ms=ts,
            status=str(status),
            client_order_ids=client_order_ids,
            result=dict(result or {}),
        )


class BuyVenueClient(Protocol):
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
    ) -> Dict[str, Any]: ...


@dataclass(frozen=True)
class BuyExecutionClients:
    polymarket: Optional[BuyVenueClient] = None
    kalshi: Optional[BuyVenueClient] = None


# _normalize_kalshi_pem imported from scripts.common.utils


def _resolve_kalshi_api_key_from_env() -> str:
    for key in ("KALSHI_RW_API_KEY", "KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def _load_kalshi_private_key_from_env() -> Any:
    try:
        from cryptography.hazmat.primitives import serialization
    except Exception as exc:
        raise RuntimeError("Missing cryptography dependency required for Kalshi auth signing") from exc

    pem = (os.getenv("KALSHI_PRIVATEKEY", "") or os.getenv("KALSHI_PRIVATE_KEY", "")).strip()
    pem_path = (os.getenv("KALSHI_PRIVATEKEY_PATH", "") or os.getenv("KALSHI_PRIVATE_KEY_PATH", "")).strip()
    if not pem and pem_path:
        path = Path(pem_path)
        if not path.exists():
            raise RuntimeError(f"KALSHI private key path does not exist: {path}")
        pem = path.read_text(encoding="utf-8")
    pem = _normalize_kalshi_pem(pem)
    if not pem:
        raise RuntimeError("Missing KALSHI private key in env (KALSHI_PRIVATEKEY or KALSHI_PRIVATEKEY_PATH)")

    try:
        return serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    except Exception as exc:
        raise RuntimeError("Invalid KALSHI private key PEM") from exc


def _kalshi_time_in_force(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"fok", "fill_or_kill"}:
        return "fill_or_kill"
    if raw in {"fak", "ioc", "immediate_or_cancel"}:
        return "immediate_or_cancel"
    if raw in {"gtc", "good_til_cancelled", "good_till_cancelled"}:
        return "good_til_cancelled"
    return "fill_or_kill"


def _kalshi_price_to_dollars(price: float) -> float:
    """
    Convert probability-ish input price to Kalshi dollar-formatted probability.

    Accepts:
    - [0, 1] probability-like values
    - [1, 99] cent-like values
    """
    px = float(price)
    dollars = px if px <= 1.0 else (px / 100.0)
    clipped = max(0.01, min(0.99, dollars))
    return round(float(clipped), 2)


def _kalshi_price_dollars_str(price: float) -> str:
    return f"{_kalshi_price_to_dollars(price):.2f}"


class KalshiApiBuyClient(BuyVenueClient):
    """
    Kalshi REST buy client backed by ApiTransport.

    Endpoint reference:
    - POST /trade-api/v2/portfolio/orders
    """

    def __init__(
        self,
        *,
        base_host: str = "https://api.elections.kalshi.com",
        api_key: Optional[str] = None,
        private_key: Optional[Any] = None,
        transport: Optional[ApiTransport] = None,
    ) -> None:
        self.base_host = str(base_host).rstrip("/")
        self.path = "/trade-api/v2/portfolio/orders"
        self.api_key = str(api_key or _resolve_kalshi_api_key_from_env()).strip()
        self.private_key = private_key if private_key is not None else _load_kalshi_private_key_from_env()
        if not self.api_key:
            raise RuntimeError("Missing Kalshi API key for trading client")
        self.transport = transport or ApiTransport(timeout_seconds=10)

    def _sign(self, *, method: str, path: str, timestamp_ms: str) -> str:
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import padding
        except Exception as exc:
            raise RuntimeError("Missing cryptography dependency required for Kalshi auth signing") from exc

        message = f"{timestamp_ms}{method.upper()}{path}"
        signature = self.private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

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
    ) -> Dict[str, Any]:
        ticker = str(instrument_id or "").strip()
        side_norm = str(side or "").strip().lower()
        kind = str(order_kind or "").strip().lower()
        if not ticker:
            raise RuntimeError("Kalshi buy order requires instrument_id ticker")
        if side_norm not in {"yes", "no"}:
            raise RuntimeError(f"Kalshi side must be yes/no, got: {side}")

        count = int(math.floor(float(size)))
        if count <= 0:
            raise RuntimeError("Kalshi buy order requires positive size")

        # Kalshi create-order expects an explicit side price. We emulate
        # market intent by submitting aggressive priced limit orders.
        effective_limit_price = limit_price
        if kind == "market" and effective_limit_price is None:
            raise RuntimeError("Kalshi market order emulation requires limit_price")
        if effective_limit_price is None:
            raise RuntimeError("Kalshi order requires limit_price")
        price_dollars = _kalshi_price_dollars_str(float(effective_limit_price))

        payload: Dict[str, Any] = {
            "ticker": ticker,
            "client_order_id": str(client_order_id),
            "type": "limit",
            "action": "buy",
            "side": side_norm,
            "count": count,
            "time_in_force": _kalshi_time_in_force(time_in_force),
        }
        if side_norm == "yes":
            payload["yes_price_dollars"] = price_dollars
        else:
            payload["no_price_dollars"] = price_dollars

        ts = str(now_ms())
        headers = {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(method="POST", path=self.path, timestamp_ms=ts),
            "Content-Type": "application/json",
        }

        status_code, response_payload = self.transport.request_json(
            "POST",
            f"{self.base_host}{self.path}",
            headers=headers,
            json=payload,
            allow_status={200, 201},
        )
        return {
            "ok": True,
            "venue": "kalshi",
            "status_code": int(status_code),
            "request": payload,
            "response": response_payload,
        }


class PolymarketSignedOrderBuilder(Protocol):
    def __call__(
        self,
        *,
        instrument_id: str,
        side: str,
        size: float,
        order_kind: str,
        limit_price: Optional[float],
        time_in_force: Optional[str],
        client_order_id: str,
    ) -> Dict[str, Any]: ...


class PolymarketAuthHeadersProvider(Protocol):
    def __call__(self, *, request_path: str, method: str, body: Dict[str, Any]) -> Dict[str, str]: ...


@dataclass(frozen=True)
class _PolymarketClobContext:
    clob_client: Any
    order_args_cls: Any
    market_order_args_cls: Any
    l2_api_key: str
    l2_api_secret: str
    l2_api_passphrase: str
    signer_address: str


def _polymarket_l1_private_key_from_env() -> str:
    value = str(os.getenv("POLYMARKET_L1_APIKEY", "") or "").strip()
    if value:
        return value
    raise RuntimeError(
        "Missing Polymarket L1 private key in env (POLYMARKET_L1_APIKEY)"
    )


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
        raise RuntimeError(
            f"Invalid POLYMARKET_MARKET_BUY_PRICE_CAP value: {raw} (expected >0 and <=1)"
        )
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


def _polymarket_hmac_signature(
    *,
    api_secret: str,
    timestamp: str,
    method: str,
    request_path: str,
    serialized_body: Optional[str],
) -> str:
    secret_bytes = base64.urlsafe_b64decode(str(api_secret))
    message = f"{timestamp}{str(method).upper()}{request_path}"
    if serialized_body:
        message += serialized_body
    digest = hmac.new(secret_bytes, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8")


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


def _resolve_polymarket_nonce_address(clob_client: Any) -> str:
    builder = getattr(clob_client, "builder", None)
    funder = str(getattr(builder, "funder", "") or "").strip()
    if funder:
        return _normalize_eth_address(funder)
    return _normalize_eth_address(_resolve_clob_signer_address(clob_client))


def _fetch_polymarket_exchange_nonce(*, clob_client: Any, nonce_address: str) -> int:
    try:
        from py_clob_client.config import get_contract_config
        from py_clob_client.constants import POLYGON
    except Exception as exc:
        raise RuntimeError(
            "Missing py-clob-client components required for Polymarket nonce lookup."
        ) from exc

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
        "Polymarket nonce lookup failed across RPC endpoints. "
        f"Errors: {' | '.join(errors) if errors else 'unknown'}"
    )


@dataclass
class PolymarketNonceManager:
    clob_client: Any

    def __post_init__(self) -> None:
        self.nonce_address = _resolve_polymarket_nonce_address(self.clob_client)
        self._next_nonce = int(
            _fetch_polymarket_exchange_nonce(clob_client=self.clob_client, nonce_address=self.nonce_address)
        )

    def peek_nonce(self) -> int:
        return int(self._next_nonce)

    def reserve_nonce(self) -> int:
        nonce = int(self._next_nonce)
        self._next_nonce = int(nonce + 1)
        return int(nonce)

    def mark_order_accepted(self, *, nonce: int) -> int:
        accepted = int(nonce)
        if accepted >= int(self._next_nonce):
            self._next_nonce = int(accepted + 1)
        return int(self._next_nonce)

    def refresh(self) -> int:
        self._next_nonce = int(
            _fetch_polymarket_exchange_nonce(clob_client=self.clob_client, nonce_address=self.nonce_address)
        )
        return int(self._next_nonce)


def _build_polymarket_nonce_manager(*, clob_client: Any) -> PolymarketNonceManager:
    return PolymarketNonceManager(clob_client=clob_client)


def _is_polymarket_invalid_nonce_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return "invalid nonce" in text


def _build_polymarket_signed_order(
    *,
    clob_client: Any,
    order_args_cls: Any,
    market_order_args_cls: Any,
    l2_api_key: str,
    instrument_id: str,
    size: float,
    order_kind: str,
    limit_price: Optional[float],
    time_in_force: Optional[str],
    nonce: int,
) -> Tuple[Any, Dict[str, Any]]:
    token_id = str(instrument_id or "").strip()
    kind = str(order_kind or "").strip().lower()
    if not token_id:
        raise RuntimeError("Polymarket buy order requires instrument_id token_id")

    effective_limit_price = limit_price
    if kind == "market" and effective_limit_price is None:
        # Polymarket CLOB is signed limit-style; emulate market with a high
        # buy cap and FAK so the order does not rest.
        effective_limit_price = _polymarket_market_buy_price_cap_from_env(default=0.99)
    if effective_limit_price is None:
        raise RuntimeError("Polymarket order signing requires limit_price")
    if float(size) <= 0:
        raise RuntimeError("Polymarket buy order requires positive size")
    if kind == "market":
        # BUY market orders expect quote amount ($), while planner sizing is
        # in shares. Convert shares to notional using the chosen price cap.
        quote_amount = float(size) * float(effective_limit_price)
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
        signed_order = clob_client.create_order(
            order_args_cls(
                token_id=token_id,
                price=float(effective_limit_price),
                size=float(size),
                side="BUY",
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


def derive_polymarket_funder_from_chain(
    *,
    l1_key: str,
    signature_type: int,
    chain_id: int = 137,
    base_url: str = "https://clob.polymarket.com",
    rpc_url: Optional[str] = None,
) -> str:
    """
    Resolve proxy/safe funder wallet from Polymarket exchange contract.

    Supported signature types:
    - 1: proxy mode
    - 2: safe mode
    """
    sig_type = int(signature_type)
    if sig_type not in {1, 2}:
        raise RuntimeError(f"Funder chain lookup requires signature_type 1 or 2, got: {sig_type}")

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.config import get_contract_config
        from py_clob_client.constants import POLYGON
    except Exception as exc:
        raise RuntimeError(
            "Missing py-clob-client components required for Polymarket funder lookup."
        ) from exc

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


def _build_polymarket_clob_context_from_env(
    *,
    base_url: str = "https://clob.polymarket.com",
) -> _PolymarketClobContext:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderArgs
    except Exception as exc:
        raise RuntimeError(
            "Missing dependency py-clob-client. Install it to enable Polymarket order signing."
        ) from exc

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
    signer_address = _resolve_clob_signer_address(clob_client)
    return _PolymarketClobContext(
        clob_client=clob_client,
        order_args_cls=OrderArgs,
        market_order_args_cls=MarketOrderArgs,
        l2_api_key=l2_key,
        l2_api_secret=l2_secret,
        l2_api_passphrase=l2_passphrase,
        signer_address=signer_address,
    )


def build_polymarket_order_signing_providers_from_env(
    *,
    base_url: str = "https://clob.polymarket.com",
) -> Tuple[PolymarketSignedOrderBuilder, PolymarketAuthHeadersProvider]:
    """
    Build Polymarket signed-order + L2-header providers from env vars.

    This keeps HTTP submission in ApiTransport while delegating order signing to
    py-clob-client.
    """
    clob_context = _build_polymarket_clob_context_from_env(base_url=base_url)
    nonce_manager = _build_polymarket_nonce_manager(clob_client=clob_context.clob_client)

    def signed_order_builder(
        *,
        instrument_id: str,
        side: str,
        size: float,
        order_kind: str,
        limit_price: Optional[float],
        time_in_force: Optional[str],
        client_order_id: str,
    ) -> Dict[str, Any]:
        nonce = nonce_manager.reserve_nonce()
        _, payload = _build_polymarket_signed_order(
            clob_client=clob_context.clob_client,
            order_args_cls=clob_context.order_args_cls,
            market_order_args_cls=clob_context.market_order_args_cls,
            l2_api_key=clob_context.l2_api_key,
            instrument_id=instrument_id,
            size=float(size),
            order_kind=order_kind,
            limit_price=limit_price,
            time_in_force=time_in_force,
            nonce=nonce,
        )
        return payload

    def auth_headers_provider(
        *,
        request_path: str,
        method: str,
        body: Dict[str, Any],
    ) -> Dict[str, str]:
        serialized_body = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body is not None else None
        ts = str(int(time.time()))
        return {
            "POLY_ADDRESS": clob_context.signer_address,
            "POLY_SIGNATURE": _polymarket_hmac_signature(
                api_secret=clob_context.l2_api_secret,
                timestamp=ts,
                method=method,
                request_path=request_path,
                serialized_body=serialized_body,
            ),
            "POLY_TIMESTAMP": ts,
            "POLY_API_KEY": clob_context.l2_api_key,
            "POLY_PASSPHRASE": clob_context.l2_api_passphrase,
        }

    return signed_order_builder, auth_headers_provider


def build_polymarket_api_buy_client_from_env(
    *,
    base_url: str = "https://clob.polymarket.com",
    transport: Optional[ApiTransport] = None,
) -> "PolymarketApiBuyClient":
    clob_context = _build_polymarket_clob_context_from_env(base_url=base_url)
    nonce_manager = _build_polymarket_nonce_manager(clob_client=clob_context.clob_client)
    return PolymarketApiBuyClient(
        base_url=base_url,
        clob_client=clob_context.clob_client,
        order_args_cls=clob_context.order_args_cls,
        market_order_args_cls=clob_context.market_order_args_cls,
        l2_api_key=clob_context.l2_api_key,
        nonce_manager=nonce_manager,
        transport=transport,
    )


class PolymarketApiBuyClient(BuyVenueClient):
    """
    Polymarket CLOB API buy client backed by py-clob-client.

    Endpoint reference:
    - POST /order

    Uses py-clob-client create_order + post_order so submission follows the same
    authentication/signing path as official examples.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://clob.polymarket.com",
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
        self.nonce_manager = (
            nonce_manager if nonce_manager is not None else _build_polymarket_nonce_manager(clob_client=clob_client)
        )
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
    ) -> Dict[str, Any]:
        def _submit_once() -> Tuple[int, Dict[str, Any], Any]:
            nonce = self.nonce_manager.peek_nonce()
            signed_order, payload = _build_polymarket_signed_order(
                clob_client=self.clob_client,
                order_args_cls=self.order_args_cls,
                market_order_args_cls=self.market_order_args_cls,
                l2_api_key=self.l2_api_key,
                instrument_id=instrument_id,
                size=float(size),
                order_kind=order_kind,
                limit_price=limit_price,
                time_in_force=time_in_force,
                nonce=nonce,
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
        """Call post_order with retry/backoff from the configured transport."""
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
                # Only retry on transient / network-level errors
                exc_name = type(exc).__name__
                is_transient = any(
                    keyword in exc_name.lower() or keyword in str(exc).lower()
                    for keyword in ("timeout", "connection", "503", "502", "504", "429")
                )
                if not is_transient or attempt >= max_attempts:
                    raise
                self.transport._sleep_backoff(attempt)

        # Should not reach here, but just in case
        raise last_exc  # type: ignore[misc]


def plan_client_order_ids(plan: BuyExecutionPlan) -> List[str]:
    ids: List[str] = []
    used: set[str] = set()
    for idx, leg in enumerate(plan.legs):
        seed = leg.client_order_id_seed or f"leg-{idx + 1}"
        cid = build_client_order_id(
            signal_id=plan.signal_id,
            venue=leg.venue,
            instrument_id=leg.instrument_id,
            side=leg.side,
            seed=seed,
        )
        suffix = 1
        deduped = cid
        while deduped in used:
            suffix += 1
            deduped = f"{cid[:54]}-{suffix:02d}"
        used.add(deduped)
        ids.append(deduped)
    return ids


def _resolve_client(*, venue: str, clients: BuyExecutionClients) -> Optional[BuyVenueClient]:
    venue_norm = str(venue or "").strip().lower()
    if venue_norm == "polymarket":
        return clients.polymarket
    if venue_norm == "kalshi":
        return clients.kalshi
    return None


def execute_cross_venue_buy(
    plan: BuyExecutionPlan,
    clients: BuyExecutionClients,
    state: BuyIdempotencyState,
    now_epoch_ms: Optional[int] = None,
) -> BuyExecutionResult:
    submitted_at = int(now_epoch_ms if now_epoch_ms is not None else now_ms())

    if state.is_in_flight_or_completed(plan.signal_id):
        prior = state.get(plan.signal_id)
        return BuyExecutionResult(
            signal_id=plan.signal_id,
            status="skipped_idempotent",
            submitted_at_ms=submitted_at,
            completed_at_ms=submitted_at,
            legs=[],
            idempotency={
                "hit": True,
                "signal_id": plan.signal_id,
                "prior_status": None if prior is None else prior.status,
                "client_order_ids": {} if prior is None else dict(prior.client_order_ids),
            },
            error=None,
        )

    client_order_ids = plan_client_order_ids(plan)
    state.mark_in_flight(
        signal_id=plan.signal_id,
        client_order_ids={f"leg_{idx + 1}": client_order_ids[idx] for idx in range(len(client_order_ids))},
    )

    leg_results: List[LegSubmitResult] = []
    submitted_count = 0

    for idx, leg in enumerate(plan.legs):
        client_order_id = client_order_ids[idx]
        request_payload = {
            "signal_id": plan.signal_id,
            "venue": leg.venue,
            "side": leg.side,
            "action": leg.action,
            "instrument_id": leg.instrument_id,
            "order_kind": leg.order_kind,
            "size": float(leg.size),
            "limit_price": None if leg.limit_price is None else float(leg.limit_price),
            "time_in_force": leg.time_in_force,
            "client_order_id": client_order_id,
        }

        client = _resolve_client(venue=leg.venue, clients=clients)
        if client is None:
            leg_results.append(
                LegSubmitResult(
                    venue=leg.venue,
                    side=leg.side,
                    instrument_id=leg.instrument_id,
                    client_order_id=client_order_id,
                    request_payload=request_payload,
                    response_payload=None,
                    submitted=False,
                    error=f"missing_client_for_venue:{leg.venue}",
                )
            )
            continue

        try:
            response = client.place_buy_order(
                instrument_id=leg.instrument_id,
                side=leg.side,
                size=float(leg.size),
                order_kind=leg.order_kind,
                limit_price=leg.limit_price,
                time_in_force=leg.time_in_force,
                client_order_id=client_order_id,
            )
            leg_results.append(
                LegSubmitResult(
                    venue=leg.venue,
                    side=leg.side,
                    instrument_id=leg.instrument_id,
                    client_order_id=client_order_id,
                    request_payload=request_payload,
                    response_payload=dict(response or {}),
                    submitted=True,
                    error=None,
                )
            )
            submitted_count += 1
        except Exception as exc:
            leg_results.append(
                LegSubmitResult(
                    venue=leg.venue,
                    side=leg.side,
                    instrument_id=leg.instrument_id,
                    client_order_id=client_order_id,
                    request_payload=request_payload,
                    response_payload=None,
                    submitted=False,
                    error=f"{type(exc).__name__}:{exc}",
                )
            )

    total = len(plan.legs)
    if submitted_count == total:
        status = "submitted"
        error = None
    elif submitted_count > 0:
        status = "partially_submitted"
        error = "partial_submit"
    else:
        status = "rejected"
        error = "no_legs_submitted"

    completed_at = now_ms()
    result = BuyExecutionResult(
        signal_id=plan.signal_id,
        status=status,
        submitted_at_ms=submitted_at,
        completed_at_ms=completed_at,
        legs=leg_results,
        idempotency={
            "hit": False,
            "signal_id": plan.signal_id,
            "client_order_ids": {f"leg_{idx + 1}": cid for idx, cid in enumerate(client_order_ids)},
        },
        error=error,
    )
    state.mark_final(signal_id=plan.signal_id, status=status, result=result.to_dict())
    return result
