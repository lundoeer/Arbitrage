from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from dotenv import load_dotenv
from eth_abi import encode
from eth_account import Account
from eth_utils import function_signature_to_4byte_selector, to_hex

from scripts.common.api_transport import ApiTransport, RetryConfig
from scripts.common.utils import as_dict, as_float, as_int


ZERO_BYTES32 = "0x" + ("00" * 32)
REDEEM_POSITIONS_SIG = "redeemPositions(address,bytes32,bytes32,uint256[])"
REDEEM_POSITIONS_SELECTOR = to_hex(function_signature_to_4byte_selector(REDEEM_POSITIONS_SIG))


def _normalize_eth_address(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("ethereum_address_missing")
    if not raw.startswith("0x"):
        raw = f"0x{raw}"
    lower = raw.lower()
    if len(lower) != 42 or not lower.startswith("0x"):
        raise RuntimeError(f"invalid_ethereum_address:{value}")
    body = lower[2:]
    if any(ch not in "0123456789abcdef" for ch in body):
        raise RuntimeError(f"invalid_ethereum_address:{value}")
    return lower


def _normalize_bytes32(value: str, *, field: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        raise RuntimeError(f"{field}_missing")
    if not raw.startswith("0x"):
        raw = f"0x{raw}"
    if len(raw) != 66:
        raise RuntimeError(f"{field}_must_be_bytes32_hex")
    body = raw[2:]
    if any(ch not in "0123456789abcdef" for ch in body):
        raise RuntimeError(f"{field}_must_be_bytes32_hex")
    return raw


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _build_rpc_transport(*, timeout_seconds: int = 10) -> ApiTransport:
    return ApiTransport(
        timeout_seconds=max(1, int(timeout_seconds)),
        retry_config=RetryConfig(
            max_attempts=3,
            base_backoff_seconds=0.5,
            jitter_ratio=0.2,
            retry_methods=frozenset({"GET", "HEAD", "OPTIONS", "POST"}),
        ),
    )


def _rpc_urls_from_env() -> List[str]:
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


def _first_non_empty_env(keys: Sequence[str]) -> str:
    for key in keys:
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value
    return ""


def resolve_redeem_private_key_from_env() -> str:
    value = _first_non_empty_env(("POLYMARKET_L1_APIKEY", "POLYMARKET_PRIVATE_KEY", "PRIVATE_KEY"))
    if not value:
        raise RuntimeError("missing_private_key_env:POLYMARKET_L1_APIKEY|POLYMARKET_PRIVATE_KEY|PRIVATE_KEY")
    return value


def resolve_positions_user_address_from_env(*, private_key: Optional[str] = None) -> str:
    configured = _first_non_empty_env(("POLYMARKET_FUNDER", "POLYMARKET_ADDRESS", "POLYMARKET_SIGNER_ADDRESS"))
    if configured:
        return _normalize_eth_address(configured)
    if private_key:
        acct = Account.from_key(private_key)
        return _normalize_eth_address(str(acct.address))
    raise RuntimeError(
        "missing_positions_user_address_env:POLYMARKET_FUNDER|POLYMARKET_ADDRESS|POLYMARKET_SIGNER_ADDRESS"
    )


def resolve_tx_sender_address(*, private_key: str) -> str:
    acct = Account.from_key(private_key)
    return _normalize_eth_address(str(acct.address))


def resolve_ctf_contracts() -> Dict[str, str]:
    try:
        from py_clob_client.config import get_contract_config
        from py_clob_client.constants import POLYGON
    except Exception as exc:
        raise RuntimeError("missing_py_clob_client_dependency_for_contract_lookup") from exc

    cfg = get_contract_config(POLYGON)
    if cfg is None:
        raise RuntimeError("polymarket_contract_config_missing")
    ctf = _normalize_eth_address(str(getattr(cfg, "conditional_tokens", "") or ""))
    collateral = _normalize_eth_address(str(getattr(cfg, "collateral", "") or ""))

    ctf_override = str(os.getenv("POLYMARKET_CTF_CONTRACT", "") or "").strip()
    collateral_override = str(os.getenv("POLYMARKET_COLLATERAL_TOKEN", "") or "").strip()
    if ctf_override:
        ctf = _normalize_eth_address(ctf_override)
    if collateral_override:
        collateral = _normalize_eth_address(collateral_override)
    return {"ctf_contract": ctf, "collateral_token": collateral}


class PolygonRpcClient:
    def __init__(
        self,
        *,
        rpc_urls: Optional[Iterable[str]] = None,
        transport: Optional[ApiTransport] = None,
    ) -> None:
        urls = [str(u).strip() for u in (rpc_urls or []) if str(u).strip()]
        self.rpc_urls = urls or _rpc_urls_from_env()
        self.transport = transport or _build_rpc_transport()

    def call(self, method: str, params: Sequence[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": str(method),
            "params": list(params),
        }
        errors: List[str] = []
        for url in self.rpc_urls:
            try:
                _, body = self.transport.request_json("POST", url, json=payload, allow_status={200})
                result_body = as_dict(body)
                if result_body.get("error") is not None:
                    raise RuntimeError(f"rpc_error:{result_body.get('error')}")
                if "result" not in result_body:
                    raise RuntimeError("rpc_result_missing")
                return result_body.get("result")
            except Exception as exc:
                errors.append(f"{url}:{type(exc).__name__}:{exc}")
        raise RuntimeError(
            f"rpc_call_failed:{method}:"
            f"{' | '.join(errors) if errors else 'no_rpc_url_available'}"
        )


@dataclass(frozen=True)
class RedeemableRow:
    condition_id: str
    outcome_index: int
    index_set: int
    asset: str
    size: float
    redeemable: bool
    proxy_wallet: Optional[str]
    event_slug: Optional[str]


@dataclass(frozen=True)
class RedeemTarget:
    condition_id: str
    parent_collection_id: str
    collateral_token: str
    index_sets: Tuple[int, ...]
    rows: Tuple[RedeemableRow, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "condition_id": self.condition_id,
            "parent_collection_id": self.parent_collection_id,
            "collateral_token": self.collateral_token,
            "index_sets": [int(value) for value in self.index_sets],
            "rows": [
                {
                    "asset": row.asset,
                    "size": float(row.size),
                    "redeemable": bool(row.redeemable),
                    "condition_id": row.condition_id,
                    "outcome_index": int(row.outcome_index),
                    "index_set": int(row.index_set),
                    "proxy_wallet": row.proxy_wallet,
                    "event_slug": row.event_slug,
                }
                for row in self.rows
            ],
        }


def _outcome_index_to_index_set(outcome_index: int) -> int:
    idx = int(outcome_index)
    if idx < 0 or idx > 255:
        raise RuntimeError(f"invalid_outcome_index:{idx}")
    return 1 << idx


def parse_redeemable_rows(*, rows: Sequence[Dict[str, Any]]) -> List[RedeemableRow]:
    parsed: List[RedeemableRow] = []
    for raw in rows:
        item = as_dict(raw)
        if not item:
            continue
        redeemable = _parse_bool(item.get("redeemable"))
        if not redeemable:
            continue
        size = as_float(item.get("size"))
        if size is None or size <= 0:
            continue
        condition_id_raw = str(item.get("conditionId") or item.get("condition_id") or "").strip()
        if not condition_id_raw:
            continue
        try:
            condition_id = _normalize_bytes32(condition_id_raw, field="condition_id")
        except Exception:
            continue
        outcome_index = as_int(item.get("outcomeIndex"))
        if outcome_index is None:
            continue
        try:
            index_set = _outcome_index_to_index_set(outcome_index)
        except Exception:
            continue
        proxy_wallet = str(item.get("proxyWallet") or "").strip() or None
        if proxy_wallet is not None:
            try:
                proxy_wallet = _normalize_eth_address(proxy_wallet)
            except Exception:
                proxy_wallet = None
        parsed.append(
            RedeemableRow(
                condition_id=condition_id,
                outcome_index=int(outcome_index),
                index_set=int(index_set),
                asset=str(item.get("asset") or "").strip(),
                size=float(size),
                redeemable=True,
                proxy_wallet=proxy_wallet,
                event_slug=str(item.get("eventSlug") or item.get("slug") or "").strip() or None,
            )
        )
    return parsed


def group_redeem_targets(
    *,
    rows: Sequence[RedeemableRow],
    collateral_token: str,
    parent_collection_id: str = ZERO_BYTES32,
    condition_id_filter: Optional[str] = None,
) -> List[RedeemTarget]:
    collateral = _normalize_eth_address(collateral_token)
    parent_id = _normalize_bytes32(parent_collection_id, field="parent_collection_id")
    condition_filter = (
        _normalize_bytes32(str(condition_id_filter), field="condition_id_filter")
        if condition_id_filter is not None and str(condition_id_filter).strip()
        else None
    )

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if condition_filter is not None and row.condition_id != condition_filter:
            continue
        entry = grouped.setdefault(
            row.condition_id,
            {"row_items": [], "index_sets": set()},
        )
        entry["row_items"].append(row)
        index_sets = entry["index_sets"]
        if isinstance(index_sets, set):
            index_sets.add(int(row.index_set))

    targets: List[RedeemTarget] = []
    for condition_id, body in grouped.items():
        index_sets_value = body.get("index_sets")
        rows_value = body.get("row_items")
        if not isinstance(index_sets_value, set) or not isinstance(rows_value, list):
            continue
        if not index_sets_value or not rows_value:
            continue
        sorted_sets = tuple(sorted(int(v) for v in index_sets_value if int(v) > 0))
        if not sorted_sets:
            continue
        targets.append(
            RedeemTarget(
                condition_id=condition_id,
                parent_collection_id=parent_id,
                collateral_token=collateral,
                index_sets=sorted_sets,
                rows=tuple(rows_value),
            )
        )

    targets.sort(key=lambda item: item.condition_id)
    return targets


def build_redeem_positions_calldata(
    *,
    collateral_token: str,
    parent_collection_id: str,
    condition_id: str,
    index_sets: Sequence[int],
) -> str:
    collateral = _normalize_eth_address(collateral_token)
    parent_id = _normalize_bytes32(parent_collection_id, field="parent_collection_id")
    condition = _normalize_bytes32(condition_id, field="condition_id")
    packed_index_sets = [int(value) for value in index_sets if int(value) > 0]
    if not packed_index_sets:
        raise RuntimeError("index_sets_missing_or_invalid")
    encoded = encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [
            collateral,
            bytes.fromhex(parent_id[2:]),
            bytes.fromhex(condition[2:]),
            packed_index_sets,
        ],
    )
    return REDEEM_POSITIONS_SELECTOR + encoded.hex()


def fetch_polymarket_positions(
    *,
    user_address: str,
    base_url: str = "https://data-api.polymarket.com",
    transport: Optional[ApiTransport] = None,
) -> List[Dict[str, Any]]:
    url = f"{str(base_url).rstrip('/')}/positions"
    tx = transport or _build_rpc_transport()
    _, payload = tx.request_json(
        "GET",
        url,
        params={"user": _normalize_eth_address(user_address), "sizeThreshold": 0},
        allow_status={200},
    )
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise RuntimeError(f"positions_payload_invalid_type:{type(payload).__name__}")
    return [as_dict(item) for item in payload if isinstance(item, dict)]


def _rpc_hex_to_int(value: Any, *, field: str) -> int:
    text = str(value or "").strip().lower()
    if not text.startswith("0x"):
        raise RuntimeError(f"{field}_invalid_hex")
    try:
        return int(text, 16)
    except Exception as exc:
        raise RuntimeError(f"{field}_invalid_hex") from exc


def _int_to_rpc_hex(value: int) -> str:
    return hex(int(value))


def _estimate_gas_with_buffer(estimate: int, *, multiplier: float = 1.2) -> int:
    base = int(estimate)
    if base <= 0:
        raise RuntimeError("gas_estimate_non_positive")
    buffered = int(base * float(multiplier))
    return max(base, buffered)


def submit_redeem_target(
    *,
    rpc_client: PolygonRpcClient,
    private_key: str,
    chain_id: int,
    ctf_contract: str,
    target: RedeemTarget,
    nonce: int,
    gas_limit_override: Optional[int] = None,
) -> Dict[str, Any]:
    sender = resolve_tx_sender_address(private_key=private_key)
    calldata = build_redeem_positions_calldata(
        collateral_token=target.collateral_token,
        parent_collection_id=target.parent_collection_id,
        condition_id=target.condition_id,
        index_sets=list(target.index_sets),
    )

    gas_price = _rpc_hex_to_int(rpc_client.call("eth_gasPrice", []), field="gas_price")
    if gas_price <= 0:
        raise RuntimeError("gas_price_non_positive")

    estimate_params = {
        "from": sender,
        "to": _normalize_eth_address(ctf_contract),
        "data": calldata,
        "value": "0x0",
    }
    if gas_limit_override is None:
        gas_estimate = _rpc_hex_to_int(rpc_client.call("eth_estimateGas", [estimate_params]), field="gas_estimate")
        gas_limit = _estimate_gas_with_buffer(gas_estimate, multiplier=1.2)
    else:
        gas_limit = max(1, int(gas_limit_override))

    tx = {
        "chainId": int(chain_id),
        "nonce": int(nonce),
        "to": _normalize_eth_address(ctf_contract),
        "value": 0,
        "data": calldata,
        "gas": int(gas_limit),
        "gasPrice": int(gas_price),
    }
    signed = Account.sign_transaction(tx, private_key)
    raw_tx = signed.raw_transaction.hex()
    tx_hash = str(rpc_client.call("eth_sendRawTransaction", [raw_tx]))
    return {
        "sender": sender,
        "nonce": int(nonce),
        "gas_price_wei": int(gas_price),
        "gas_limit": int(gas_limit),
        "tx_hash": tx_hash,
        "target": target.to_dict(),
        "calldata": calldata,
    }


def wait_for_receipt(
    *,
    rpc_client: PolygonRpcClient,
    tx_hash: str,
    timeout_seconds: float = 120.0,
    poll_seconds: float = 2.0,
) -> Dict[str, Any]:
    deadline = time.time() + max(1.0, float(timeout_seconds))
    poll = max(0.2, float(poll_seconds))
    while time.time() < deadline:
        receipt = rpc_client.call("eth_getTransactionReceipt", [str(tx_hash)])
        if receipt is not None:
            body = as_dict(receipt)
            if body:
                return body
        time.sleep(poll)
    raise RuntimeError(f"receipt_timeout:{tx_hash}")


def redeem_positions(
    *,
    user_address: str,
    private_key: str,
    rpc_urls: Optional[Iterable[str]] = None,
    positions_base_url: str = "https://data-api.polymarket.com",
    dry_run: bool = True,
    parent_collection_id: str = ZERO_BYTES32,
    condition_id_filter: Optional[str] = None,
    max_targets: int = 0,
    wait_receipts: bool = False,
    receipt_timeout_seconds: float = 120.0,
    receipt_poll_seconds: float = 2.0,
    gas_limit_override: Optional[int] = None,
    enforce_sender_match: bool = True,
) -> Dict[str, Any]:
    chain_id_raw = str(os.getenv("POLYMARKET_CHAIN_ID", "137") or "137").strip()
    try:
        chain_id = int(chain_id_raw)
    except Exception as exc:
        raise RuntimeError(f"invalid_chain_id:{chain_id_raw}") from exc
    if chain_id <= 0:
        raise RuntimeError(f"invalid_chain_id:{chain_id}")

    contracts = resolve_ctf_contracts()
    ctf_contract = contracts["ctf_contract"]
    collateral_token = contracts["collateral_token"]
    rpc_client = PolygonRpcClient(rpc_urls=rpc_urls)

    raw_positions = fetch_polymarket_positions(
        user_address=user_address,
        base_url=positions_base_url,
    )
    redeemable_rows = parse_redeemable_rows(rows=raw_positions)
    targets = group_redeem_targets(
        rows=redeemable_rows,
        collateral_token=collateral_token,
        parent_collection_id=parent_collection_id,
        condition_id_filter=condition_id_filter,
    )
    if max_targets and int(max_targets) > 0:
        targets = targets[: int(max_targets)]

    tx_sender = resolve_tx_sender_address(private_key=private_key)
    user_norm = _normalize_eth_address(user_address)
    proxy_wallets: Set[str] = set()
    for row in redeemable_rows:
        if row.proxy_wallet:
            proxy_wallets.add(row.proxy_wallet)

    preflight = {
        "positions_user": user_norm,
        "tx_sender": tx_sender,
        "proxy_wallets_seen": sorted(proxy_wallets),
        "sender_matches_positions_user": tx_sender == user_norm,
        "sender_matches_any_proxy_wallet": tx_sender in proxy_wallets if proxy_wallets else None,
    }

    result: Dict[str, Any] = {
        "ok": True,
        "mode": "dry_run" if bool(dry_run) else "submit",
        "chain_id": int(chain_id),
        "contracts": contracts,
        "positions_total_rows": len(raw_positions),
        "positions_redeemable_rows": len(redeemable_rows),
        "targets_total": len(targets),
        "preflight": preflight,
        "targets": [target.to_dict() for target in targets],
        "submitted": [],
        "failures": [],
    }

    if (not bool(dry_run)) and bool(enforce_sender_match):
        allowed_senders: Set[str] = {user_norm}
        allowed_senders.update(proxy_wallets)
        if tx_sender not in allowed_senders:
            raise RuntimeError(
                "tx_sender_mismatch:"
                f"sender={tx_sender},positions_user={user_norm},"
                f"proxy_wallets={','.join(sorted(proxy_wallets)) if proxy_wallets else '<none>'}"
            )

    if bool(dry_run) or not targets:
        return result

    nonce_base = _rpc_hex_to_int(
        rpc_client.call("eth_getTransactionCount", [tx_sender, "pending"]),
        field="nonce",
    )

    for idx, target in enumerate(targets):
        nonce = int(nonce_base + idx)
        try:
            submit_payload = submit_redeem_target(
                rpc_client=rpc_client,
                private_key=private_key,
                chain_id=chain_id,
                ctf_contract=ctf_contract,
                target=target,
                nonce=nonce,
                gas_limit_override=gas_limit_override,
            )
            if bool(wait_receipts):
                receipt = wait_for_receipt(
                    rpc_client=rpc_client,
                    tx_hash=str(submit_payload.get("tx_hash") or ""),
                    timeout_seconds=receipt_timeout_seconds,
                    poll_seconds=receipt_poll_seconds,
                )
                submit_payload["receipt"] = receipt
            result["submitted"].append(submit_payload)
        except Exception as exc:
            result["ok"] = False
            result["failures"].append(
                {
                    "condition_id": target.condition_id,
                    "index_sets": [int(v) for v in target.index_sets],
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )

    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Redeem Polymarket positions by calling CTF redeemPositions on Polygon."
    )
    parser.add_argument("--user-address", default="", help="Polymarket positions user address. Defaults from env.")
    parser.add_argument(
        "--positions-base-url",
        default="https://data-api.polymarket.com",
        help="Polymarket data-api base URL for positions query.",
    )
    parser.add_argument(
        "--rpc-url",
        action="append",
        default=[],
        help="Polygon RPC URL. Can be passed multiple times; defaults to env/fallback list.",
    )
    parser.add_argument(
        "--condition-id",
        default="",
        help="Optional bytes32 condition id filter (0x...).",
    )
    parser.add_argument(
        "--parent-collection-id",
        default=ZERO_BYTES32,
        help="Parent collection id bytes32; default is zero root collection.",
    )
    parser.add_argument(
        "--max-targets",
        type=int,
        default=0,
        help="Max redeem targets per run. 0 means no limit.",
    )
    parser.add_argument(
        "--gas-limit",
        type=int,
        default=0,
        help="Optional fixed gas limit override. 0 means use estimate+buffer.",
    )
    parser.add_argument(
        "--wait-receipts",
        action="store_true",
        help="Wait for transaction receipts after submit.",
    )
    parser.add_argument(
        "--receipt-timeout-seconds",
        type=float,
        default=120.0,
        help="Receipt wait timeout when --wait-receipts is set.",
    )
    parser.add_argument(
        "--receipt-poll-seconds",
        type=float,
        default=2.0,
        help="Receipt poll interval when --wait-receipts is set.",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Submit transactions. Without this flag, script runs in dry-run mode.",
    )
    parser.add_argument(
        "--allow-sender-mismatch",
        action="store_true",
        help="Allow submit when tx sender does not match positions user/proxy wallet.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv(dotenv_path=".env", override=False)
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        private_key = resolve_redeem_private_key_from_env()
        user_address = (
            _normalize_eth_address(str(args.user_address).strip())
            if str(args.user_address or "").strip()
            else resolve_positions_user_address_from_env(private_key=private_key)
        )
        condition_id_filter = str(args.condition_id or "").strip() or None
        gas_limit_override = int(args.gas_limit) if int(args.gas_limit) > 0 else None
        result = redeem_positions(
            user_address=user_address,
            private_key=private_key,
            rpc_urls=list(args.rpc_url or []),
            positions_base_url=str(args.positions_base_url),
            dry_run=(not bool(args.submit)),
            parent_collection_id=str(args.parent_collection_id or ZERO_BYTES32),
            condition_id_filter=condition_id_filter,
            max_targets=int(args.max_targets),
            wait_receipts=bool(args.wait_receipts),
            receipt_timeout_seconds=float(args.receipt_timeout_seconds),
            receipt_poll_seconds=float(args.receipt_poll_seconds),
            gas_limit_override=gas_limit_override,
            enforce_sender_match=(not bool(args.allow_sender_mismatch)),
        )
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "error_type": type(exc).__name__, "error": str(exc)},
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, ensure_ascii=True, separators=(",", ":")))
    return 0 if bool(result.get("ok")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
