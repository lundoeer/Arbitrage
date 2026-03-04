from __future__ import annotations

from scripts.common.api_transport import ApiTransport
from scripts.common.polymarket_resolution import (
    fetch_closed_positions_resolution_map,
    parse_closed_positions_resolution_rows,
)


class _StaticTransport:
    def __init__(self, payloads: list[list[dict]]) -> None:
        self.payloads = list(payloads)
        self.calls = 0
        self.last_offsets: list[int] = []

    def request_json(self, method: str, url: str, **kwargs: object) -> tuple[int, object]:
        self.calls += 1
        assert method == "GET"
        assert url.endswith("/closed-positions")
        params = kwargs.get("params")
        assert isinstance(params, dict)
        assert "user" in params
        assert "limit" in params
        assert "offset" in params
        self.last_offsets.append(int(params["offset"]))
        if not self.payloads:
            return 200, []
        return 200, self.payloads.pop(0)


def test_parse_closed_positions_resolution_rows_latest_timestamp_wins() -> None:
    rows = [
        {"conditionId": "0xabc", "outcome": "yes", "timestamp": 1772554500},
        {"conditionId": "0xabc", "outcome": "no", "timestamp": 1772554600},
        {"conditionId": "0xdef", "outcome": "up", "timestamp": 1772554700},
    ]
    parsed = parse_closed_positions_resolution_rows(rows)

    assert parsed["0xabc"]["outcome"] == "no"
    assert parsed["0xabc"]["source_timestamp_ms"] == 1772554600000
    assert parsed["0xdef"]["outcome"] == "yes"
    assert parsed["0xdef"]["source_timestamp_ms"] == 1772554700000


def test_fetch_closed_positions_resolution_map_merges_pages() -> None:
    transport = _StaticTransport(
        payloads=[
            [
                {"conditionId": "0xabc", "outcome": "yes", "timestamp": 1772554500},
                {"conditionId": "0xdef", "outcome": "no", "timestamp": 1772554550},
            ],
            [
                {"conditionId": "0xabc", "outcome": "no", "timestamp": 1772554600},
            ],
        ]
    )

    result = fetch_closed_positions_resolution_map(
        transport=transport,  # type: ignore[arg-type]
        user_address="0xuser",
        base_url="http://example.local",
        page_size=2,
        max_pages=3,
    )

    assert transport.calls == 2
    assert transport.last_offsets == [0, 2]
    assert result["0xabc"]["outcome"] == "no"
    assert result["0xabc"]["source_timestamp_ms"] == 1772554600000
    assert result["0xdef"]["outcome"] == "no"


def test_fetch_closed_positions_resolution_map_returns_empty_for_missing_user() -> None:
    transport = ApiTransport(timeout_seconds=2)
    result = fetch_closed_positions_resolution_map(
        transport=transport,
        user_address="",
    )
    assert result == {}

