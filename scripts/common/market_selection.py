from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Mapping

from scripts.common.api_transport import ApiTransport


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("_")


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _validate_allowed_keys(
    *,
    scope: str,
    payload: Mapping[str, Any],
    allowed: set[str],
    strict: bool,
) -> None:
    if not strict:
        return
    extras = sorted(set(payload.keys()) - set(allowed))
    if extras:
        raise RuntimeError(f"Unknown keys in {scope}: {', '.join(extras)}")


def _parse_polymarket_token_ids(raw_ids: Any) -> list[str]:
    if isinstance(raw_ids, list):
        return [str(token_id) for token_id in raw_ids if str(token_id or "").strip()]
    if isinstance(raw_ids, str):
        try:
            parsed = json.loads(raw_ids)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [str(token_id) for token_id in parsed if str(token_id or "").strip()]
    return []


def _parse_polymarket_outcomes(raw_outcomes: Any) -> list[str]:
    if isinstance(raw_outcomes, list):
        return [str(item or "").strip() for item in raw_outcomes if str(item or "").strip()]
    if isinstance(raw_outcomes, str):
        try:
            parsed = json.loads(raw_outcomes)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [str(item or "").strip() for item in parsed if str(item or "").strip()]
    return []


def _resolve_literal_yes_no_indexes(outcomes: list[str]) -> tuple[int, int] | None:
    if len(outcomes) != 2:
        return None
    lowered = [str(item or "").strip().lower() for item in outcomes]
    if "yes" not in lowered or "no" not in lowered:
        return None
    return lowered.index("yes"), lowered.index("no")


def _resolve_polymarket_tokens_from_setup(
    *,
    market_id: str,
    market_payload: Dict[str, Any],
    setup_payload: Dict[str, Any],
) -> tuple[str, str, str, str | None, str | None]:
    pm_tokens = _parse_polymarket_token_ids(market_payload.get("clobTokenIds"))
    if len(pm_tokens) != 2:
        raise RuntimeError(
            f"Polymarket market {market_id} must have exactly 2 clobTokenIds; found {len(pm_tokens)}"
        )
    token_set = set(pm_tokens)
    outcomes = _parse_polymarket_outcomes(market_payload.get("outcomes"))
    outcome_yes_label: str | None = None
    outcome_no_label: str | None = None

    requested_pm_token_yes = str(setup_payload.get("token_yes") or "").strip()
    requested_pm_token_no = str(setup_payload.get("token_no") or "").strip()
    if requested_pm_token_yes or requested_pm_token_no:
        if not requested_pm_token_yes or not requested_pm_token_no:
            raise RuntimeError(
                "market_setup_file.pair.polymarket.token_yes and token_no must both be provided when one is set"
            )
        if requested_pm_token_yes == requested_pm_token_no:
            raise RuntimeError("market_setup_file.pair.polymarket token_yes and token_no must differ")
        if requested_pm_token_yes not in token_set:
            raise RuntimeError(
                f"market_setup_file.pair.polymarket.token_yes does not belong to market_id={market_id}"
            )
        if requested_pm_token_no not in token_set:
            raise RuntimeError(
                f"market_setup_file.pair.polymarket.token_no does not belong to market_id={market_id}"
            )
        if len(outcomes) == 2:
            by_token = {pm_tokens[idx]: outcomes[idx] for idx in range(2)}
            outcome_yes_label = by_token.get(requested_pm_token_yes)
            outcome_no_label = by_token.get(requested_pm_token_no)
        return requested_pm_token_yes, requested_pm_token_no, "setup_tokens", outcome_yes_label, outcome_no_label

    requested_pm_yes_outcome = str(setup_payload.get("yes_outcome") or "").strip()
    if requested_pm_yes_outcome:
        if len(outcomes) != 2:
            raise RuntimeError(
                "market_setup_file.pair.polymarket.yes_outcome requires exactly two Polymarket outcomes"
            )
        lowered_target = requested_pm_yes_outcome.lower()
        matches = [idx for idx, label in enumerate(outcomes) if str(label).strip().lower() == lowered_target]
        if len(matches) != 1:
            raise RuntimeError(
                "market_setup_file.pair.polymarket.yes_outcome did not match exactly one Polymarket outcome"
            )
        yes_idx = int(matches[0])
        no_idx = 1 - yes_idx
        return pm_tokens[yes_idx], pm_tokens[no_idx], "setup_yes_outcome", outcomes[yes_idx], outcomes[no_idx]

    yes_no_indexes = _resolve_literal_yes_no_indexes(outcomes)
    if yes_no_indexes is not None:
        yes_idx, no_idx = yes_no_indexes
        return pm_tokens[yes_idx], pm_tokens[no_idx], "literal_yes_no", outcomes[yes_idx], outcomes[no_idx]

    raise RuntimeError(
        "Polymarket market has non-literal outcomes. "
        "Provide pair.polymarket.token_yes/token_no or pair.polymarket.yes_outcome in market setup file."
    )


def _load_run_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    return _as_dict(json.loads(config_path.read_text(encoding="utf-8")))


def _resolve_kalshi_api_key(config_payload: Dict[str, Any]) -> str:
    env_candidates = ["KALSHI_READONLY_API_KEY", "KALSHI_API_KEY", "kalshiapi"]
    for env_key in env_candidates:
        value = str(os.getenv(env_key, "") or "").strip()
        if value:
            return value
    api_payload = _as_dict(config_payload.get("api"))
    return str(api_payload.get("readonly_api_key") or "").strip()


def _fetch_polymarket_market_by_id(*, market_id: str) -> Dict[str, Any]:
    tx = ApiTransport(
        default_headers={
            "User-Agent": "Arbitrage-Market-Selection/1.0",
            "Accept": "application/json, text/plain, */*",
        }
    )
    status, payload = tx.request_json(
        "GET",
        f"https://gamma-api.polymarket.com/markets/{market_id}",
        allow_status={200, 404},
    )
    if status == 404:
        raise RuntimeError(f"Polymarket market not found for market_id={market_id}")
    body = _as_dict(payload)
    if not body:
        raise RuntimeError(f"Unexpected Polymarket payload for market_id={market_id}")
    return body


def _fetch_kalshi_market_by_ticker(*, base_url: str, ticker: str, api_key: str) -> Dict[str, Any]:
    headers: Dict[str, str] = {
        "User-Agent": "Arbitrage-Market-Selection/1.0",
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    tx = ApiTransport(default_headers=headers)
    status, payload = tx.request_json(
        "GET",
        f"{base_url.rstrip('/')}/markets/{ticker}",
        allow_status={200, 404},
    )
    if status == 404:
        raise RuntimeError(f"Kalshi market not found for ticker={ticker}")
    body = _as_dict(payload)
    if not body:
        raise RuntimeError(f"Unexpected Kalshi payload for ticker={ticker}")
    market = _as_dict(body.get("market"))
    return market if market else body


def load_selected_markets_from_setup_file(
    *,
    config_path: Path,
    market_setup_file: Path,
    strict: bool = True,
) -> Dict[str, Any]:
    if not market_setup_file.exists():
        raise FileNotFoundError(f"Missing market setup file: {market_setup_file}")

    setup_payload = _as_dict(json.loads(market_setup_file.read_text(encoding="utf-8")))
    _validate_allowed_keys(
        scope="market_setup_file",
        payload=setup_payload,
        allowed={"mode", "pair"},
        strict=bool(strict),
    )
    mode = str(setup_payload.get("mode") or "").strip()
    if mode and mode != "manual_pair_v1":
        raise RuntimeError(f"Unsupported market setup mode: {mode}")

    pair_payload = _as_dict(setup_payload.get("pair"))
    if not pair_payload:
        raise RuntimeError("market_setup_file.pair is required")
    _validate_allowed_keys(
        scope="market_setup_file.pair",
        payload=pair_payload,
        allowed={"polymarket", "kalshi"},
        strict=bool(strict),
    )

    pm_setup = _as_dict(pair_payload.get("polymarket"))
    kx_setup = _as_dict(pair_payload.get("kalshi"))
    if not pm_setup or not kx_setup:
        raise RuntimeError("market_setup_file.pair.polymarket and market_setup_file.pair.kalshi are required")

    _validate_allowed_keys(
        scope="market_setup_file.pair.polymarket",
        payload=pm_setup,
        allowed={"market_id", "condition_id", "token_yes", "token_no", "yes_outcome"},
        strict=bool(strict),
    )
    _validate_allowed_keys(
        scope="market_setup_file.pair.kalshi",
        payload=kx_setup,
        allowed={"ticker"},
        strict=bool(strict),
    )

    requested_pm_market_id = str(pm_setup.get("market_id") or "").strip()
    if not requested_pm_market_id:
        raise RuntimeError("market_setup_file.pair.polymarket.market_id is required")
    requested_pm_condition_id = str(pm_setup.get("condition_id") or "").strip()

    requested_kx_ticker = str(kx_setup.get("ticker") or "").strip()
    if not requested_kx_ticker:
        raise RuntimeError("market_setup_file.pair.kalshi.ticker is required")

    pm_market = _fetch_polymarket_market_by_id(market_id=requested_pm_market_id)
    pm_market_id = str(pm_market.get("id") or "").strip()
    if pm_market_id != requested_pm_market_id:
        raise RuntimeError(
            f"Polymarket market_id mismatch. requested={requested_pm_market_id} fetched={pm_market_id}"
        )
    pm_event_slug = str(pm_market.get("slug") or "").strip()
    if not pm_event_slug:
        raise RuntimeError(f"Polymarket market {pm_market_id} missing slug")

    pm_condition_id = str(pm_market.get("conditionId") or "").strip()
    if requested_pm_condition_id:
        if pm_condition_id and pm_condition_id.lower() != requested_pm_condition_id.lower():
            raise RuntimeError(
                "Polymarket condition_id mismatch between setup and fetched market. "
                f"setup={requested_pm_condition_id} fetched={pm_condition_id}"
            )
        pm_condition_id = requested_pm_condition_id
    if not pm_condition_id:
        raise RuntimeError(f"Polymarket market {pm_market_id} missing conditionId")

    (
        pm_token_yes,
        pm_token_no,
        pm_token_orientation_source,
        pm_yes_outcome_label,
        pm_no_outcome_label,
    ) = _resolve_polymarket_tokens_from_setup(
        market_id=pm_market_id,
        market_payload=pm_market,
        setup_payload=pm_setup,
    )
    pm_window_end = pm_market.get("endDate") or pm_market.get("endDateIso")

    config_payload = _load_run_config(config_path=config_path)
    api_payload = _as_dict(config_payload.get("api"))
    kalshi_base_url = str(api_payload.get("base_url") or "https://api.elections.kalshi.com/trade-api/v2").rstrip("/")
    kalshi_api_key = _resolve_kalshi_api_key(config_payload)
    kx_market = _fetch_kalshi_market_by_ticker(
        base_url=kalshi_base_url,
        ticker=requested_kx_ticker,
        api_key=kalshi_api_key,
    )
    kx_ticker = str(kx_market.get("ticker") or "").strip()
    if kx_ticker != requested_kx_ticker:
        raise RuntimeError(
            f"Kalshi ticker mismatch. requested={requested_kx_ticker} fetched={kx_ticker}"
        )
    kx_window_end = kx_market.get("close_time") or kx_market.get("expiration_time")

    return {
        "discovery_generated_at": None,
        "selection_source": "manual_setup_file",
        "manual_setup_file": str(market_setup_file),
        "polymarket": {
            "event_slug": pm_event_slug,
            "market_id": pm_market_id,
            "condition_id": pm_condition_id,
            "token_yes": pm_token_yes,
            "token_no": pm_token_no,
            "yes_outcome_label": pm_yes_outcome_label,
            "no_outcome_label": pm_no_outcome_label,
            "token_orientation_source": pm_token_orientation_source,
            "window_end": pm_window_end,
            "normalization_signature": "",
        },
        "kalshi": {
            "ticker": kx_ticker,
            "window_end": kx_window_end,
            "normalization_signature": "",
        },
    }


def load_selected_markets(
    *,
    config_path: Path,
    discovery_output: Path,
    pair_cache_path: Path,
    run_discovery_first: bool,
) -> Dict[str, Any]:
    if run_discovery_first:
        # Delayed import keeps this module lightweight and avoids startup
        # dependency cycles when only static selection is needed.
        from scripts.run.discover_active_btc_15m_markets import run_discovery

        payload = run_discovery(
            config_path=config_path,
            output_path=discovery_output,
            pair_cache_path=pair_cache_path,
            pm_back_buckets=2,
            pm_forward_buckets=8,
            kalshi_back_buckets=2,
            kalshi_forward_buckets=8,
        )
    else:
        if not discovery_output.exists():
            raise FileNotFoundError(
                f"Missing discovery output file: {discovery_output}. Run discovery first or remove --skip-discovery."
            )
        payload = json.loads(discovery_output.read_text(encoding="utf-8"))

    selected_pm = payload.get("polymarket", {}).get("selected_contract")
    selected_kx = payload.get("kalshi", {}).get("selected_contract")
    if not isinstance(selected_pm, dict):
        raise RuntimeError("No selected Polymarket contract available from discovery.")
    if not isinstance(selected_kx, dict):
        raise RuntimeError("No selected Kalshi contract available from discovery.")

    pm_token_yes = str(selected_pm.get("token_yes") or "").strip()
    pm_token_no = str(selected_pm.get("token_no") or "").strip()
    pm_slug = str(selected_pm.get("event_slug") or "").strip()
    pm_market_id = str(selected_pm.get("market_id") or "").strip()
    pm_condition_id = str(selected_pm.get("condition_id") or "").strip()
    kx_ticker = str(selected_kx.get("ticker") or "").strip()

    if not pm_token_yes or not pm_token_no:
        raise RuntimeError("Selected Polymarket contract is missing token_yes/token_no.")
    if not pm_slug:
        raise RuntimeError("Selected Polymarket contract is missing event_slug.")
    if not pm_market_id:
        raise RuntimeError("Selected Polymarket contract is missing market_id.")
    if not kx_ticker:
        raise RuntimeError("Selected Kalshi contract is missing ticker.")

    pm_sig = str(selected_pm.get("normalization_signature") or "")
    kx_sig = str(selected_kx.get("normalization_signature") or "")
    if pm_sig and kx_sig and pm_sig != kx_sig:
        raise RuntimeError(
            f"Selected venue signatures do not match. polymarket={pm_sig} kalshi={kx_sig}"
        )

    return {
        "discovery_generated_at": payload.get("generated_at"),
        "polymarket": {
            "event_slug": pm_slug,
            "market_id": pm_market_id,
            "condition_id": pm_condition_id,
            "token_yes": pm_token_yes,
            "token_no": pm_token_no,
            "window_end": selected_pm.get("window_end"),
            "normalization_signature": pm_sig,
        },
        "kalshi": {
            "ticker": kx_ticker,
            "window_end": selected_kx.get("window_end"),
            "normalization_signature": kx_sig,
        },
    }
