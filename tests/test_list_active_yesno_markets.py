from __future__ import annotations

from datetime import datetime, timezone

from scripts.diagnostic.list_active_yesno_markets import (
    _kalshi_market_row,
    _parse_kalshi_event_ticker_date,
    _polymarket_market_row,
)


def test_polymarket_row_matches_lol_wb_tes_example() -> None:
    market = {
        "id": "1465732",
        "conditionId": "0xaaaa9edf4c6ba43e6ad09cf204864f5c9864a7f575945215826aa9e3ca8bb97e",
        "question": "LoL: Weibo Gaming vs Top Esports (BO5) - LPL Playoffs",
        "active": True,
        "endDate": "2026-03-02T15:00:00Z",
        "outcomes": '["Weibo Gaming","Top Esports"]',
        "clobTokenIds": (
            '["45421371926973371632420213429141104724758579461092945506485156280429781269889",'
            '"55290959233602665163229355642073148228482747772488222386012293835080395856816"]'
        ),
        "volumeNum": 946184.873667,
        "events": [{"slug": "lol-wb-tes-2026-03-02"}],
    }
    now_utc = datetime(2026, 3, 2, 12, 0, 0, tzinfo=timezone.utc)
    row = _polymarket_market_row(
        market=market,
        now_utc=now_utc,
        window_seconds=24 * 3600,
        active_only=True,
    )
    assert row == {
        "venue": "polymarket",
        "event_slug": "lol-wb-tes-2026-03-02",
        "end_time": "2026-03-02T15:00:00+00:00",
        "title": "LoL: Weibo Gaming vs Top Esports (BO5) - LPL Playoffs",
        "dollar_volume": 946184.873667,
        "yes_no_detected": True,
        "setup_fragment": {
            "polymarket": {
                "market_id": "1465732",
                "condition_id": "0xaaaa9edf4c6ba43e6ad09cf204864f5c9864a7f575945215826aa9e3ca8bb97e",
            }
        },
    }


def test_kalshi_row_matches_lol_wb_tes_example_close_time_mode() -> None:
    market = {
        "ticker": "KXLOLGAME-26MAR02TESWB-WB",
        "event_ticker": "KXLOLGAME-26MAR02TESWB",
        "title": "Will Weibo Gaming win the Top Esports vs. Weibo Gaming League of Legends match?",
        "status": "active",
        "close_time": "2026-03-16T09:00:00Z",
        "expiration_time": "2026-03-16T09:00:00Z",
        "market_type": "binary",
    }
    now_utc = datetime(2026, 3, 2, 12, 0, 0, tzinfo=timezone.utc)
    row = _kalshi_market_row(
        market=market,
        now_utc=now_utc,
        window_seconds=30 * 24 * 3600,
        active_only=True,
        series_ticker_hint="KXLOLGAME",
        use_event_ticker_date=False,
    )
    assert row == {
        "venue": "kalshi",
        "series_ticker": "KXLOLGAME",
        "event_ticker": "KXLOLGAME-26MAR02TESWB",
        "end_time": "2026-03-16T09:00:00+00:00",
        "title": "Will Weibo Gaming win the Top Esports vs. Weibo Gaming League of Legends match?",
        "dollar_volume": 0.0,
        "yes_no_detected": True,
        "setup_fragment": {"kalshi": {"ticker": "KXLOLGAME-26MAR02TESWB-WB"}},
    }


def test_kalshi_event_ticker_date_mode_for_lol_filtering() -> None:
    parsed = _parse_kalshi_event_ticker_date("KXLOLGAME-26MAR02TESWB")
    assert parsed is not None
    assert parsed.isoformat() == "2026-03-02T23:59:59+00:00"

    market = {
        "ticker": "KXLOLGAME-26MAR02TESWB-WB",
        "event_ticker": "KXLOLGAME-26MAR02TESWB",
        "title": "Will Weibo Gaming win the Top Esports vs. Weibo Gaming League of Legends match?",
        "status": "active",
        "close_time": "2026-03-16T09:00:00Z",
        "expiration_time": "2026-03-16T09:00:00Z",
        "market_type": "binary",
    }
    now_utc = datetime(2026, 3, 2, 0, 0, 0, tzinfo=timezone.utc)

    without_event_date = _kalshi_market_row(
        market=market,
        now_utc=now_utc,
        window_seconds=24 * 3600,
        active_only=True,
        series_ticker_hint="KXLOLGAME",
        use_event_ticker_date=False,
    )
    assert without_event_date is None

    with_event_date = _kalshi_market_row(
        market=market,
        now_utc=now_utc,
        window_seconds=24 * 3600,
        active_only=True,
        series_ticker_hint="KXLOLGAME",
        use_event_ticker_date=True,
    )
    assert with_event_date is not None
    assert with_event_date["end_time"] == "2026-03-02T23:59:59+00:00"

