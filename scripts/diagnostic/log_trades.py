#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.common.api_transport import ApiTransport, RetryConfig
from scripts.common.buy_execution import (
    _build_polymarket_clob_context_from_env,
    _load_kalshi_private_key_from_env,
    _resolve_kalshi_api_key_from_env,
)
from scripts.common.engine_setup import resolve_polymarket_position_user_address_from_env
from scripts.common.utils import as_dict


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch trades + market outcomes and append logs/trade_log rows.")
    p.add_argument("--output", default="logs/trade_log/trades.jsonl")
    p.add_argument("--period", default="today,yesterday", help="today,yesterday,last_24h,last_48h,last_7d")
    p.add_argument("--after", default="", help="ISO-8601 or unix seconds/ms")
    p.add_argument("--before", default="", help="ISO-8601 or unix seconds/ms")
    p.add_argument("--timezone", default="America/New_York")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--max-pages", type=int, default=200)
    p.add_argument("--polymarket-base-url", default="https://clob.polymarket.com")
    p.add_argument("--polymarket-data-api-base-url", default="https://data-api.polymarket.com")
    p.add_argument("--polymarket-maker-address", default="")
    p.add_argument("--polymarket-user-address", default="")
    p.add_argument("--polymarket-market", default="", help="Optional condition_id filter")
    p.add_argument("--kalshi-base-host", default="https://api.elections.kalshi.com")
    p.add_argument("--kalshi-api-prefix", default="/trade-api/v2")
    p.add_argument("--kalshi-ticker", default="")
    p.add_argument("--kalshi-subaccount", type=int, default=None)
    return p


def _flt(v: Any) -> Optional[float]:
    try:
        return None if v is None else float(v)
    except Exception:
        return None


def _int(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    try:
        return None if v is None else int(float(v))
    except Exception:
        return None


def _txt(v: Any) -> Optional[str]:
    t = str(v or "").strip()
    return t or None


def _parse_iso(v: str) -> Optional[datetime]:
    t = str(v or "").strip()
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        d = datetime.fromisoformat(t)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _parse_dt_or_epoch(v: str, tz: ZoneInfo) -> Optional[datetime]:
    t = str(v or "").strip()
    if not t:
        return None
    n = _flt(t)
    if n is not None:
        if abs(n) > 1_000_000_000_000:
            n /= 1000.0
        return datetime.fromtimestamp(float(n), tz=timezone.utc)
    d = _parse_iso(t)
    if d:
        return d
    try:
        d2 = datetime.strptime(t, "%Y-%m-%d")
    except ValueError:
        return None
    return d2.replace(tzinfo=tz).astimezone(timezone.utc)


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_ms(iso_value: Optional[str] = None, unix_value: Optional[Any] = None) -> Optional[int]:
    if iso_value:
        d = _parse_iso(iso_value)
        if d:
            return int(d.timestamp() * 1000)
    n = _flt(unix_value)
    if n is None:
        return None
    return int(n if abs(n) > 1_000_000_000_000 else n * 1000.0)


def _resolve_window(args: argparse.Namespace) -> Tuple[int, int, str]:
    tz = ZoneInfo(str(args.timezone))
    now_utc = datetime.now(timezone.utc)
    explicit_after = _parse_dt_or_epoch(str(args.after), tz) if str(args.after).strip() else None
    explicit_before = _parse_dt_or_epoch(str(args.before), tz) if str(args.before).strip() else None
    if explicit_after or explicit_before:
        start = explicit_after or (now_utc - timedelta(days=1))
        end = explicit_before or now_utc
        if end <= start:
            raise RuntimeError("--before must be later than --after")
        return int(start.timestamp()), int(end.timestamp()), "explicit"

    now_local = now_utc.astimezone(tz)
    today0 = datetime.combine(now_local.date(), time.min, tzinfo=tz)
    yday0 = today0 - timedelta(days=1)
    allowed = {"today", "yesterday", "last_24h", "last_48h", "last_7d"}
    parts = [x.strip().lower() for x in str(args.period or "").split(",") if x.strip()] or ["today", "yesterday"]
    for x in parts:
        if x not in allowed:
            raise RuntimeError(f"Invalid period: {x}")
    starts: List[datetime] = []
    ends: List[datetime] = []
    for x in parts:
        if x == "today":
            starts.append(today0.astimezone(timezone.utc))
            ends.append(now_utc)
        elif x == "yesterday":
            starts.append(yday0.astimezone(timezone.utc))
            ends.append(today0.astimezone(timezone.utc))
        elif x == "last_24h":
            starts.append(now_utc - timedelta(hours=24))
            ends.append(now_utc)
        elif x == "last_48h":
            starts.append(now_utc - timedelta(hours=48))
            ends.append(now_utc)
        elif x == "last_7d":
            starts.append(now_utc - timedelta(days=7))
            ends.append(now_utc)
    start = min(starts)
    end = max(ends)
    return int(start.timestamp()), int(end.timestamp()), ",".join(parts)


def _in_window(ms: Optional[int], start_s: int, end_s: int) -> bool:
    if ms is None:
        return False
    s = int(ms / 1000)
    return start_s <= s <= end_s


def _load_aliases() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {"pm_market": {}, "pm_condition": {}, "pm_asset": {}, "kx_ticker": {}}
    path = PROJECT_ROOT / "data" / "market_discovery_latest.json"
    if not path.exists():
        return out
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return out
    pm = as_dict(as_dict(as_dict(payload).get("polymarket")).get("selected_contract"))
    kx = as_dict(as_dict(as_dict(payload).get("kalshi")).get("selected_contract"))
    sig = _txt(pm.get("normalization_signature")) or _txt(kx.get("normalization_signature")) or "unknown_pair"
    if _txt(pm.get("market_id")):
        out["pm_market"][str(pm["market_id"]).lower()] = sig
    if _txt(pm.get("condition_id")):
        out["pm_condition"][str(pm["condition_id"]).lower()] = sig
    if _txt(pm.get("token_yes")):
        out["pm_asset"][str(pm["token_yes"])] = sig
    if _txt(pm.get("token_no")):
        out["pm_asset"][str(pm["token_no"])] = sig
    if _txt(kx.get("ticker")):
        out["kx_ticker"][str(kx["ticker"])] = sig
    return out


def _pair_for_pm(aliases: Dict[str, Dict[str, str]], market: Optional[str], asset: Optional[str]) -> str:
    m = str(market or "").strip().lower()
    a = str(asset or "").strip()
    if m in aliases["pm_condition"]:
        return aliases["pm_condition"][m]
    if m in aliases["pm_market"]:
        return aliases["pm_market"][m]
    if a in aliases["pm_asset"]:
        return aliases["pm_asset"][a]
    return f"polymarket:{m or a or 'unknown'}"


def _pair_for_kx(aliases: Dict[str, Dict[str, str]], ticker: Optional[str]) -> str:
    t = str(ticker or "").strip()
    if t in aliases["kx_ticker"]:
        return aliases["kx_ticker"][t]
    return f"kalshi:{t or 'unknown'}"


def _kalshi_sig(private_key: Any, method: str, path: str, ts_ms: str) -> str:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    msg = f"{ts_ms}{method.upper()}{path}"
    raw = private_key.sign(
        msg.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(raw).decode("utf-8")


def _kalshi_headers(api_key: str, private_key: Any, method: str, path: str) -> Dict[str, str]:
    ts_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": _kalshi_sig(private_key, method, path, ts_ms),
        "Content-Type": "application/json",
    }


def _fetch_kalshi_pages(
    transport: ApiTransport,
    api_key: str,
    private_key: Any,
    base_host: str,
    path: str,
    list_key: str,
    params: Dict[str, Any],
    max_pages: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    pm_trade_max_ts_by_condition: Dict[str, int] = {}
    cursor: Optional[str] = None
    pages = 0
    while pages < max_pages:
        pages += 1
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        _, payload = transport.request_json(
            "GET",
            f"{base_host}{path}",
            headers=_kalshi_headers(api_key, private_key, "GET", path),
            params=q,
            allow_status={200},
        )
        body = as_dict(payload)
        batch = body.get(list_key)
        if isinstance(batch, list):
            rows.extend([as_dict(x) for x in batch if isinstance(x, dict)])
        cursor = _txt(body.get("cursor"))
        if not cursor:
            break
    return rows


def _fetch_kalshi_ts_fallback(
    transport: ApiTransport,
    api_key: str,
    private_key: Any,
    base_host: str,
    path: str,
    list_key: str,
    start_s: int,
    end_s: int,
    ticker: Optional[str],
    subaccount: Optional[int],
    limit: int,
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], str]:
    q: Dict[str, Any] = {"limit": int(limit), "min_ts": int(start_s), "max_ts": int(end_s)}
    if ticker:
        q["ticker"] = ticker
    if subaccount is not None:
        q["subaccount"] = int(subaccount)
    rows = _fetch_kalshi_pages(transport, api_key, private_key, base_host, path, list_key, q, max_pages)
    if rows:
        return rows, "seconds"
    q["min_ts"] = int(start_s) * 1000
    q["max_ts"] = int(end_s) * 1000
    rows = _fetch_kalshi_pages(transport, api_key, private_key, base_host, path, list_key, q, max_pages)
    return rows, "milliseconds" if rows else "seconds"


def _load_existing_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    if not path.exists():
        return ids
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except Exception:
                continue
            if isinstance(row, dict) and _txt(row.get("row_id")):
                ids.add(str(row["row_id"]))
    return ids


def _append_rows(path: Path, rows: List[Dict[str, Any]]) -> int:
    seen = _load_existing_ids(path)
    new_rows = [r for r in rows if _txt(r.get("row_id")) and str(r["row_id"]) not in seen]
    if not new_rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as dst:
        for row in new_rows:
            dst.write(json.dumps(row, ensure_ascii=True) + "\n")
    return len(new_rows)


def _kalshi_fill_price_for_side(fill: Dict[str, Any], side: Optional[str]) -> Optional[float]:
    side_norm = str(side or "").strip().lower()
    if side_norm == "yes":
        px = _flt(fill.get("yes_price_fixed"))
        if px is None:
            px = _flt(fill.get("yes_price"))
    elif side_norm == "no":
        px = _flt(fill.get("no_price_fixed"))
        if px is None:
            px = _flt(fill.get("no_price"))
    else:
        px = _flt(fill.get("price"))
    if px is not None and px > 1.0:
        return float(px) / 100.0
    return px


def _polymarket_btc15m_fee(*, size: Optional[float], price: Optional[float]) -> Optional[float]:
    if size is None or price is None:
        return None
    contracts = float(size)
    p = float(price)
    if contracts <= 0.0:
        return None
    # BTC 15m fee formula requested by user (rebate ignored):
    # fee = C * feeRate * (p * (1 - p))^exponent
    fee_rate = 0.25
    exponent = 2.0
    p_clamped = min(1.0, max(0.0, p))
    return float(contracts * fee_rate * ((p_clamped * (1.0 - p_clamped)) ** exponent))


def _sort_rows(
    rows: List[Dict[str, Any]],
    *,
    pm_trade_max_ts_by_condition: Dict[str, int],
) -> List[Dict[str, Any]]:
    def key(row: Dict[str, Any]) -> Tuple[int, int, str]:
        row_type = str(row.get("row_type") or "").strip().lower()
        venue = str(row.get("venue") or "").strip().lower()
        market = str(row.get("market") or "").strip().lower()
        time_epoch_ms = int(_int(row.get("time_epoch_ms")) or 0)
        if row_type == "market_outcome" and venue == "polymarket":
            anchor = int(pm_trade_max_ts_by_condition.get(market, time_epoch_ms))
            # rank=1 keeps it after trade rows when anchor is the same.
            return (anchor, 1, str(row.get("row_id") or ""))
        return (time_epoch_ms, 0, str(row.get("row_id") or ""))

    return sorted(rows, key=key)


def main() -> None:
    args = _build_parser().parse_args()
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    start_s, end_s, source = _resolve_window(args)
    if end_s <= start_s:
        raise RuntimeError("Empty window")

    transport = ApiTransport(timeout_seconds=15, retry_config=RetryConfig(max_attempts=4, retry_methods=frozenset({"GET"})))
    aliases = _load_aliases()
    maker = _txt(args.polymarket_maker_address) or _txt(resolve_polymarket_position_user_address_from_env())
    user = _txt(args.polymarket_user_address) or _txt(resolve_polymarket_position_user_address_from_env())
    if not maker or not user:
        raise RuntimeError("Missing polymarket address. Set POLYMARKET_FUNDER/POLYMARKET_ADDRESS or CLI overrides.")

    pm_ctx = _build_polymarket_clob_context_from_env(base_url=str(args.polymarket_base_url).rstrip("/"))
    from py_clob_client.clob_types import TradeParams

    pm_trades_raw = pm_ctx.clob_client.get_trades(
        params=TradeParams(
            maker_address=maker,
            market=_txt(args.polymarket_market),
            after=int(start_s),
            before=int(end_s),
        ),
        next_cursor="MA==",
    )
    pm_closed_raw: List[Dict[str, Any]] = []
    offset = 0
    limit = max(1, min(200, int(args.limit)))
    max_pages = max(1, int(args.max_pages))
    while len(pm_closed_raw) < limit * max_pages:
        _, payload = transport.request_json(
            "GET",
            f"{str(args.polymarket_data_api_base_url).rstrip('/')}/closed-positions",
            params={"user": user, "limit": min(50, limit), "offset": offset, "sortBy": "TIMESTAMP", "sortDirection": "DESC"},
            allow_status={200},
        )
        batch = payload if isinstance(payload, list) else []
        if not batch:
            break
        pm_closed_raw.extend([as_dict(x) for x in batch if isinstance(x, dict)])
        if len(batch) < min(50, limit):
            break
        offset += min(50, limit)

    kx_key = _resolve_kalshi_api_key_from_env()
    if not kx_key:
        raise RuntimeError("Missing Kalshi API key env var.")
    kx_private = _load_kalshi_private_key_from_env()
    kx_prefix = "/" + str(args.kalshi_api_prefix or "").strip("/")
    kx_ticker = _txt(args.kalshi_ticker)

    kx_fills_raw, fills_mode = _fetch_kalshi_ts_fallback(
        transport,
        kx_key,
        kx_private,
        str(args.kalshi_base_host).rstrip("/"),
        f"{kx_prefix}/portfolio/fills",
        "fills",
        start_s,
        end_s,
        kx_ticker,
        args.kalshi_subaccount,
        limit,
        max_pages,
    )
    kx_sett_raw, sett_mode = _fetch_kalshi_ts_fallback(
        transport,
        kx_key,
        kx_private,
        str(args.kalshi_base_host).rstrip("/"),
        f"{kx_prefix}/portfolio/settlements",
        "settlements",
        start_s,
        end_s,
        kx_ticker,
        args.kalshi_subaccount,
        limit,
        max_pages,
    )

    rows: List[Dict[str, Any]] = []
    pm_trade_max_ts_by_condition: Dict[str, int] = {}
    for r in [as_dict(x) for x in list(pm_trades_raw or []) if isinstance(x, dict)]:
        ms = _ts_ms(unix_value=r.get("match_time")) or _ts_ms(unix_value=r.get("last_update"))
        if not _in_window(ms, start_s, end_s):
            continue
        market = _txt(r.get("market"))
        asset = _txt(r.get("asset_id"))
        size = _flt(r.get("size"))
        price = _flt(r.get("price"))
        fee = _polymarket_btc15m_fee(size=size, price=price)
        fee_bps = _flt(r.get("fee_rate_bps"))
        rid = _txt(r.get("id")) or f"{market}|{asset}|{r.get('match_time')}|{r.get('side')}|{size}|{price}"
        condition_key = str(market or "").strip().lower()
        if condition_key:
            current_max = pm_trade_max_ts_by_condition.get(condition_key)
            if current_max is None or int(ms) > int(current_max):
                pm_trade_max_ts_by_condition[condition_key] = int(ms)
        rows.append(
            {
                "row_type": "trade",
                "action": _txt(r.get("side")),
                "size": size,
                "price": price,
                "fee": fee,
                "outcome": _txt(r.get("outcome")),
                "outcomeIndex": _int(r.get("bucket_index")),
                "venue": "polymarket",
                "fee_rate_bps": fee_bps,
                "time": _iso(int(ms)),
                "source": "polymarket_trades",
                "row_id": f"pm_trade:{rid}",
                "time_epoch_ms": int(ms),
                "market_pair": _pair_for_pm(aliases, market, asset),
                "market": market,
            }
        )

    for r in pm_closed_raw:
        ms = _ts_ms(unix_value=r.get("timestamp"))
        if not _in_window(ms, start_s, end_s):
            continue
        market = _txt(r.get("conditionId"))
        market_key = str(market or "").strip().lower()
        # Only include outcomes for markets where we actually have trade rows.
        if not market_key or market_key not in pm_trade_max_ts_by_condition:
            continue
        asset = _txt(r.get("asset"))
        size = _flt(r.get("totalBought"))
        avg = _flt(r.get("avgPrice"))
        cost = (size * avg) if (size is not None and avg is not None) else None
        pnl = _flt(r.get("realizedPnl"))
        rev = (cost + pnl) if (cost is not None and pnl is not None) else None
        rid = f"{market}|{asset}|{r.get('timestamp')}|{r.get('outcomeIndex')}"
        rows.append(
            {
                "row_type": "market_outcome",
                "size": size,
                "price": avg,
                "fee": None,
                "total cost": cost,
                "revenue": rev,
                "outcome": _txt(r.get("outcome")),
                "outcomeIndex": _int(r.get("outcomeIndex")),
                "venue": "polymarket",
                "row_id": f"pm_closed:{rid}",
                "time": _iso(int(ms)),
                "time_epoch_ms": int(ms),
                "market_pair": _pair_for_pm(aliases, market, asset),
                "market": market,
                "action": "settled",
                "source": "polymarket_closed_positions",
            }
        )

    for r in kx_fills_raw:
        ms = _ts_ms(iso_value=_txt(r.get("created_time")), unix_value=r.get("ts"))
        if not _in_window(ms, start_s, end_s):
            continue
        side = (_txt(r.get("side")) or "").lower()
        action = (_txt(r.get("action")) or "").lower()
        effective_side = side
        # Kalshi fill "sell" semantics are opposite-outcome inventory reduction.
        if action == "sell":
            if side == "yes":
                effective_side = "no"
            elif side == "no":
                effective_side = "yes"
        ticker = _txt(r.get("market_ticker")) or _txt(r.get("ticker"))
        size = _flt(r.get("count_fp")) if _flt(r.get("count_fp")) is not None else _flt(r.get("count"))
        price = _kalshi_fill_price_for_side(r, effective_side)
        if price is None:
            price = _kalshi_fill_price_for_side(r, side)
        rid = _txt(r.get("fill_id")) or f"{ticker}|{r.get('order_id')}|{r.get('created_time')}|{side}|{size}|{price}"
        fee_rate_bps = _flt(r.get("fee_rate_bps"))
        rows.append(
            {
                "row_type": "trade",
                "action": action or _txt(r.get("action")),
                "size": size,
                "price": price,
                "fee": _flt(r.get("fee_cost")),
                "outcome": effective_side.upper() if effective_side else None,
                "outcomeIndex": 0 if effective_side == "yes" else (1 if effective_side == "no" else None),
                "venue": "kalshi",
                "fee_rate_bps": fee_rate_bps,
                "time": _iso(int(ms)),
                "source": "kalshi_fills",
                "row_id": f"kx_fill:{rid}",
                "time_epoch_ms": int(ms),
                "market_pair": _pair_for_kx(aliases, ticker),
                "market": ticker,
            }
        )

    for r in kx_sett_raw:
        ms = _ts_ms(iso_value=_txt(r.get("settled_time")))
        if not _in_window(ms, start_s, end_s):
            continue
        ticker = _txt(r.get("ticker"))
        result = (_txt(r.get("market_result")) or "").lower()
        y_count = _flt(r.get("yes_count_fp")) if _flt(r.get("yes_count_fp")) is not None else _flt(r.get("yes_count"))
        n_count = _flt(r.get("no_count_fp")) if _flt(r.get("no_count_fp")) is not None else _flt(r.get("no_count"))
        if result == "yes":
            size = y_count
            cost = _flt(r.get("yes_total_cost"))
            idx = 0
        elif result == "no":
            size = n_count
            cost = _flt(r.get("no_total_cost"))
            idx = 1
        else:
            size = y_count if y_count is not None else n_count
            cost = _flt(r.get("yes_total_cost")) if _flt(r.get("yes_total_cost")) is not None else _flt(r.get("no_total_cost"))
            idx = None
        rid = f"{ticker}|{r.get('settled_time')}|{result}|{r.get('value')}"
        rows.append(
            {
                "row_type": "market_outcome",
                "size": size,
                "price": None,
                "fee": _flt(r.get("fee_cost")),
                "total cost": cost,
                "revenue": _flt(r.get("revenue")),
                "outcome": result.upper() if result else None,
                "outcomeIndex": idx,
                "venue": "kalshi",
                "row_id": f"kx_settlement:{rid}",
                "time": _iso(int(ms)),
                "time_epoch_ms": int(ms),
                "market_pair": _pair_for_kx(aliases, ticker),
                "market": ticker,
                "action": "settled",
                "source": "kalshi_settlements",
            }
        )

    output = Path(str(args.output).strip() or "logs/trade_log/trades.jsonl")
    if not output.is_absolute():
        output = PROJECT_ROOT / output
    rows = _sort_rows(rows, pm_trade_max_ts_by_condition=pm_trade_max_ts_by_condition)
    appended = _append_rows(output, rows)

    print(f"Window (UTC): {_iso(start_s * 1000)} -> {_iso(end_s * 1000)}")
    print(f"Window source: {source}")
    print(
        "Fetched rows: "
        f"pm_trades={len(list(pm_trades_raw or []))}, "
        f"pm_closed_positions={len(pm_closed_raw)}, "
        f"kx_fills={len(kx_fills_raw)}(ts={fills_mode}), "
        f"kx_settlements={len(kx_sett_raw)}(ts={sett_mode})"
    )
    print(f"Normalized rows in window: {len(rows)}")
    if appended > 0:
        print(f"Appended {appended} new rows to {output}")
    else:
        print(f"No new rows to append ({output})")


if __name__ == "__main__":
    main()
