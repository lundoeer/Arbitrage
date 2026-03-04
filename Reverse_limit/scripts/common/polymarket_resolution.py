from __future__ import annotations

from typing import Any, Dict, List, Optional

from scripts.common.api_transport import ApiTransport
from scripts.common.utils import as_dict, as_float, as_non_empty_text


POLYMARKET_DATA_API = "https://data-api.polymarket.com"


def normalize_polymarket_outcome_side(raw: Any) -> Optional[str]:
    text = str(raw or "").strip().lower()
    if text in {"yes", "up"}:
        return "yes"
    if text in {"no", "down"}:
        return "no"
    return None


def _to_epoch_ms(raw: Any) -> Optional[int]:
    value = as_float(raw)
    if value is None:
        return None
    ivalue = int(value)
    if ivalue <= 0:
        return None
    # Assume seconds when precision is small.
    if ivalue < 10_000_000_000:
        return int(ivalue * 1000)
    return int(ivalue)


def parse_closed_positions_resolution_rows(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        item = as_dict(raw)
        condition_id = as_non_empty_text(item.get("conditionId")) or as_non_empty_text(item.get("condition_id"))
        if not condition_id:
            continue
        outcome = normalize_polymarket_outcome_side(
            item.get("outcome")
            or item.get("result")
            or item.get("resolution")
        )
        if outcome not in {"yes", "no"}:
            continue
        ts_ms = _to_epoch_ms(item.get("timestamp"))
        prev = as_dict(out.get(condition_id))
        prev_ts = _to_epoch_ms(prev.get("source_timestamp_ms")) if prev else None
        if prev and prev_ts is not None and ts_ms is not None and prev_ts > ts_ms:
            continue
        out[condition_id] = {
            "kind": "polymarket_market_resolution",
            "condition_id": str(condition_id),
            "outcome": str(outcome),
            "source": "closed_positions",
            "source_timestamp_ms": int(ts_ms) if ts_ms is not None else None,
            "raw": item,
        }
    return out


def fetch_closed_positions_resolution_map(
    *,
    transport: ApiTransport,
    user_address: str,
    base_url: str = POLYMARKET_DATA_API,
    page_size: int = 500,
    max_pages: int = 3,
) -> Dict[str, Dict[str, Any]]:
    user = str(user_address or "").strip().lower()
    if not user:
        return {}
    limit = max(1, min(1000, int(page_size)))
    pages = max(1, int(max_pages))
    offset = 0
    merged: Dict[str, Dict[str, Any]] = {}
    for _ in range(pages):
        _, payload = transport.request_json(
            "GET",
            f"{str(base_url).rstrip('/')}/closed-positions",
            params={
                "user": user,
                "limit": int(limit),
                "offset": int(offset),
            },
            allow_status={200},
        )
        rows = payload if isinstance(payload, list) else []
        if not rows:
            break
        parsed = parse_closed_positions_resolution_rows([as_dict(row) for row in rows if isinstance(row, dict)])
        for condition_id, value in parsed.items():
            prev = as_dict(merged.get(condition_id))
            prev_ts = _to_epoch_ms(prev.get("source_timestamp_ms")) if prev else None
            new_ts = _to_epoch_ms(value.get("source_timestamp_ms"))
            if prev and prev_ts is not None and new_ts is not None and prev_ts > new_ts:
                continue
            merged[str(condition_id)] = dict(value)
        if len(rows) < limit:
            break
        offset += int(limit)
    return merged

