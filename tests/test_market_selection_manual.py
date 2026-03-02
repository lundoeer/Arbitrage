from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.common import market_selection as ms


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _pm_payload(
    *,
    condition_id: str = "0xcond",
    end_date: str | None = "2026-03-02T15:00:00Z",
    outcomes: str = '["Yes","No"]',
    token_ids: str = '["pm_yes_token","pm_no_token"]',
) -> dict:
    body = {
        "id": "1465732",
        "slug": "lol-wb-tes-2026-03-02",
        "conditionId": condition_id,
        "outcomes": outcomes,
        "clobTokenIds": token_ids,
    }
    if end_date is not None:
        body["endDate"] = end_date
    return body


def _kx_payload(*, close_time: str = "2026-03-16T09:00:00Z") -> dict:
    return {
        "ticker": "KXLOLGAME-26MAR02TESWB-WB",
        "close_time": close_time,
    }


def test_load_selected_markets_from_setup_file_enriches_minimal_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {"api": {"base_url": "https://api.elections.kalshi.com/trade-api/v2"}})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {"market_id": "1465732"},
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(ms, "_fetch_polymarket_market_by_id", lambda *, market_id: _pm_payload())
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    selection = ms.load_selected_markets_from_setup_file(
        config_path=config_path,
        market_setup_file=setup_path,
        strict=True,
    )

    assert selection["selection_source"] == "manual_setup_file"
    assert selection["polymarket"]["event_slug"] == "lol-wb-tes-2026-03-02"
    assert selection["polymarket"]["market_id"] == "1465732"
    assert selection["polymarket"]["condition_id"] == "0xcond"
    assert selection["polymarket"]["token_yes"] == "pm_yes_token"
    assert selection["polymarket"]["token_no"] == "pm_no_token"
    assert selection["polymarket"]["window_end"] == "2026-03-02T15:00:00Z"
    assert selection["kalshi"]["ticker"] == "KXLOLGAME-26MAR02TESWB-WB"
    assert selection["kalshi"]["window_end"] == "2026-03-16T09:00:00Z"


def test_load_selected_markets_from_setup_file_hard_fails_on_unknown_keys_strict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {"market_id": "1465732", "unexpected": "value"},
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(ms, "_fetch_polymarket_market_by_id", lambda *, market_id: _pm_payload())
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    with pytest.raises(RuntimeError, match="Unknown keys in market_setup_file.pair.polymarket"):
        ms.load_selected_markets_from_setup_file(
            config_path=config_path,
            market_setup_file=setup_path,
            strict=True,
        )


def test_load_selected_markets_from_setup_file_allows_unknown_keys_when_not_strict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {"market_id": "1465732", "unexpected": "value"},
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(ms, "_fetch_polymarket_market_by_id", lambda *, market_id: _pm_payload())
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    selection = ms.load_selected_markets_from_setup_file(
        config_path=config_path,
        market_setup_file=setup_path,
        strict=False,
    )
    assert selection["polymarket"]["market_id"] == "1465732"


def test_load_selected_markets_from_setup_file_fails_on_condition_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {"market_id": "1465732", "condition_id": "0xsetup"},
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(ms, "_fetch_polymarket_market_by_id", lambda *, market_id: _pm_payload(condition_id="0xother"))
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    with pytest.raises(RuntimeError, match="condition_id mismatch"):
        ms.load_selected_markets_from_setup_file(
            config_path=config_path,
            market_setup_file=setup_path,
            strict=True,
        )


def test_load_selected_markets_from_setup_file_populates_kalshi_window_end_when_polymarket_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {"market_id": "1465732"},
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(ms, "_fetch_polymarket_market_by_id", lambda *, market_id: _pm_payload(end_date=None))
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload(close_time="2026-03-16T09:00:00Z"))

    selection = ms.load_selected_markets_from_setup_file(
        config_path=config_path,
        market_setup_file=setup_path,
        strict=True,
    )
    assert selection["polymarket"]["window_end"] is None
    assert selection["kalshi"]["window_end"] == "2026-03-16T09:00:00Z"


def test_load_selected_markets_from_setup_file_maps_non_literal_outcomes_from_yes_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {
                    "market_id": "1465732",
                    "yes_outcome": "Top Esports",
                },
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(
        ms,
        "_fetch_polymarket_market_by_id",
        lambda *, market_id: _pm_payload(
            outcomes='["Weibo Gaming","Top Esports"]',
            token_ids='["pm_weibo","pm_tes"]',
        ),
    )
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    selection = ms.load_selected_markets_from_setup_file(
        config_path=config_path,
        market_setup_file=setup_path,
        strict=True,
    )
    assert selection["polymarket"]["token_yes"] == "pm_tes"
    assert selection["polymarket"]["token_no"] == "pm_weibo"
    assert selection["polymarket"]["yes_outcome_label"] == "Top Esports"
    assert selection["polymarket"]["no_outcome_label"] == "Weibo Gaming"
    assert selection["polymarket"]["token_orientation_source"] == "setup_yes_outcome"


def test_load_selected_markets_from_setup_file_rejects_non_literal_without_orientation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {"market_id": "1465732"},
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(
        ms,
        "_fetch_polymarket_market_by_id",
        lambda *, market_id: _pm_payload(
            outcomes='["Weibo Gaming","Top Esports"]',
            token_ids='["pm_weibo","pm_tes"]',
        ),
    )
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    with pytest.raises(RuntimeError, match="non-literal outcomes"):
        ms.load_selected_markets_from_setup_file(
            config_path=config_path,
            market_setup_file=setup_path,
            strict=True,
        )


def test_load_selected_markets_from_setup_file_accepts_explicit_tokens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {
                    "market_id": "1465732",
                    "token_yes": "pm_tes",
                    "token_no": "pm_weibo",
                },
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(
        ms,
        "_fetch_polymarket_market_by_id",
        lambda *, market_id: _pm_payload(
            outcomes='["Weibo Gaming","Top Esports"]',
            token_ids='["pm_weibo","pm_tes"]',
        ),
    )
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    selection = ms.load_selected_markets_from_setup_file(
        config_path=config_path,
        market_setup_file=setup_path,
        strict=True,
    )
    assert selection["polymarket"]["token_yes"] == "pm_tes"
    assert selection["polymarket"]["token_no"] == "pm_weibo"
    assert selection["polymarket"]["token_orientation_source"] == "setup_tokens"


def test_load_selected_markets_from_setup_file_rejects_explicit_tokens_not_in_market(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "run_config.json"
    setup_path = tmp_path / "market_setup.json"
    _write_json(config_path, {})
    _write_json(
        setup_path,
        {
            "mode": "manual_pair_v1",
            "pair": {
                "polymarket": {
                    "market_id": "1465732",
                    "token_yes": "pm_not_present",
                    "token_no": "pm_weibo",
                },
                "kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"},
            },
        },
    )

    monkeypatch.setattr(
        ms,
        "_fetch_polymarket_market_by_id",
        lambda *, market_id: _pm_payload(
            outcomes='["Weibo Gaming","Top Esports"]',
            token_ids='["pm_weibo","pm_tes"]',
        ),
    )
    monkeypatch.setattr(ms, "_fetch_kalshi_market_by_ticker", lambda **kwargs: _kx_payload())

    with pytest.raises(RuntimeError, match="token_yes does not belong"):
        ms.load_selected_markets_from_setup_file(
            config_path=config_path,
            market_setup_file=setup_path,
            strict=True,
        )
