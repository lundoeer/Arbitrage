from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Protocol

from scripts.common.utils import now_ms, utc_now_iso

try:
    import websockets
except Exception:
    websockets = None


# now_ms and utc_now_iso imported from scripts.common.utils


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, payload: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class NullWriter:
    def write(self, payload: Dict[str, Any]) -> None:  # noqa: ARG002
        return

    def close(self) -> None:
        return


class CollectorWriter(Protocol):
    def write(self, payload: Dict[str, Any]) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class WsRetryConfig:
    reconnect_base_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    recv_timeout_seconds: float = 30.0


@dataclass(frozen=True)
class WsHealthConfig:
    transport_heartbeat_stale_seconds: float = 1.5
    market_data_stale_seconds: float = 1.5
    max_lag_ms: int = 700
    require_source_timestamp: bool = False
    max_source_age_ms: Optional[int] = None


class BaseWsCollector:
    def __init__(
        self,
        *,
        name: str,
        url: str,
        raw_writer: CollectorWriter,
        event_writer: CollectorWriter,
        headers: Optional[Dict[str, str]] = None,
        headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
        retry: Optional[WsRetryConfig] = None,
        health: Optional[WsHealthConfig] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.name = name
        self.url = url
        self.raw_writer = raw_writer
        self.event_writer = event_writer
        self.headers = headers or {}
        self.headers_factory = headers_factory
        self.retry = retry or WsRetryConfig()
        self.health = health or WsHealthConfig()
        self.on_event = on_event
        self.message_count = 0
        self.event_count = 0
        self.connection_state = "idle"
        self.reconnect_attempt = 0
        self.reconnect_count = 0
        self.disconnect_count = 0
        self.last_connect_ms: Optional[int] = None
        self.last_disconnect_ms: Optional[int] = None
        self.last_message_recv_ms: Optional[int] = None
        self.last_data_recv_ms: Optional[int] = None
        self.last_source_timestamp_ms: Optional[int] = None
        self.last_lag_ms: Optional[int] = None

    def subscription_payloads(self) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError

    def normalize_event(self, message: Dict[str, Any], recv_ms: int) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError

    def _set_connection_state(self, state: str, at_ms: Optional[int] = None) -> None:
        now = int(at_ms if at_ms is not None else now_ms())
        self.connection_state = str(state)
        if state == "connected":
            self.last_connect_ms = now
            self.reconnect_attempt = 0
        elif state in {"disconnected", "reconnecting"}:
            self.last_disconnect_ms = now

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(float(value))
            except Exception:
                return None
        return None

    @staticmethod
    def _is_market_data_event(event: Dict[str, Any]) -> bool:
        kind = str(event.get("kind") or "").strip().lower()
        return kind not in {"", "control_or_unknown"}

    def _update_health_from_event(self, event: Dict[str, Any], recv_ms: int) -> None:
        self.last_message_recv_ms = int(recv_ms)
        if not self._is_market_data_event(event):
            return

        self.last_data_recv_ms = int(recv_ms)
        source_ts = event.get("source_timestamp_ms")
        lag = event.get("lag_ms")

        source_ts_ms = self._to_optional_int(source_ts)
        lag_ms_value = self._to_optional_int(lag)

        if source_ts_ms is not None:
            self.last_source_timestamp_ms = int(source_ts_ms)
            if lag_ms_value is None:
                lag_ms_value = int(recv_ms - source_ts_ms)

        if lag_ms_value is not None:
            self.last_lag_ms = int(lag_ms_value)

    def health_snapshot(self, now_epoch_ms: Optional[int] = None) -> Dict[str, Any]:
        now_value = int(now_epoch_ms if now_epoch_ms is not None else now_ms())
        transport_heartbeat_age_ms: Optional[int] = None
        if self.last_message_recv_ms is not None:
            transport_heartbeat_age_ms = int(now_value - int(self.last_message_recv_ms))
        data_heartbeat_age_ms: Optional[int] = None
        if self.last_data_recv_ms is not None:
            data_heartbeat_age_ms = int(now_value - int(self.last_data_recv_ms))
        source_age_ms: Optional[int] = None
        if self.last_source_timestamp_ms is not None:
            source_age_ms = int(now_value - int(self.last_source_timestamp_ms))

        transport_reasons = []
        if self.connection_state != "connected":
            transport_reasons.append("not_connected")

        if self.last_message_recv_ms is None:
            transport_reasons.append("no_messages")
        elif (
            transport_heartbeat_age_ms is not None
            and transport_heartbeat_age_ms > int(self.health.transport_heartbeat_stale_seconds * 1000)
        ):
            transport_reasons.append("heartbeat_timeout")

        market_data_reasons = []
        if self.last_data_recv_ms is None:
            market_data_reasons.append("no_data_messages")
        elif (
            data_heartbeat_age_ms is not None
            and data_heartbeat_age_ms > int(self.health.market_data_stale_seconds * 1000)
        ):
            market_data_reasons.append("data_timeout")

        if self.health.require_source_timestamp and self.last_source_timestamp_ms is None:
            market_data_reasons.append("missing_source_timestamp")

        if (
            self.health.max_source_age_ms is not None
            and source_age_ms is not None
            and source_age_ms > int(self.health.max_source_age_ms)
        ):
            market_data_reasons.append("source_age_exceeded")

        if self.last_lag_ms is not None and int(self.last_lag_ms) > int(self.health.max_lag_ms):
            market_data_reasons.append("lag_exceeded")

        transport_ok = not bool(transport_reasons)
        market_data_ok = not bool(market_data_reasons)
        decision_ok = bool(transport_ok and market_data_ok)

        return {
            "collector": self.name,
            "connection_state": self.connection_state,
            "reconnect_attempt": int(self.reconnect_attempt),
            "reconnect_count": int(self.reconnect_count),
            "disconnect_count": int(self.disconnect_count),
            "last_connect_ms": self.last_connect_ms,
            "last_disconnect_ms": self.last_disconnect_ms,
            "last_transport_recv_ms": self.last_message_recv_ms,
            "last_data_recv_ms": self.last_data_recv_ms,
            "last_source_timestamp_ms": self.last_source_timestamp_ms,
            "last_lag_ms": self.last_lag_ms,
            "transport_heartbeat_age_ms": transport_heartbeat_age_ms,
            "data_heartbeat_age_ms": data_heartbeat_age_ms,
            "source_age_ms": source_age_ms,
            "transport_heartbeat_stale_seconds": float(self.health.transport_heartbeat_stale_seconds),
            "market_data_stale_seconds": float(self.health.market_data_stale_seconds),
            "max_lag_ms": int(self.health.max_lag_ms),
            "require_source_timestamp": bool(self.health.require_source_timestamp),
            "max_source_age_ms": (
                int(self.health.max_source_age_ms) if self.health.max_source_age_ms is not None else None
            ),
            "transport_reasons": transport_reasons,
            "market_data_reasons": market_data_reasons,
            "transport_ok": transport_ok,
            "market_data_ok": market_data_ok,
            "decision_ok": decision_ok,
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        if websockets is None:
            raise RuntimeError("Missing websocket dependency. Install requirements.txt in your .venv.")
        attempt = 0
        while not stop_event.is_set():
            try:
                self._set_connection_state("connecting")
                connect_headers = self.headers_factory() if self.headers_factory else self.headers
                self.event_writer.write(
                    {
                        "ts": utc_now_iso(),
                        "collector": self.name,
                        "kind": "state",
                        "state": "connecting",
                        "url": self.url,
                        "attempt": attempt + 1,
                    }
                )
                async with websockets.connect(
                    self.url,
                    additional_headers=connect_headers or None,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    attempt = 0
                    self._set_connection_state("connected")
                    self.event_writer.write(
                        {
                            "ts": utc_now_iso(),
                            "collector": self.name,
                            "kind": "state",
                            "state": "connected",
                            "url": self.url,
                        }
                    )
                    for payload in self.subscription_payloads():
                        await ws.send(json.dumps(payload))
                        self.event_writer.write(
                            {
                                "ts": utc_now_iso(),
                                "collector": self.name,
                                "kind": "subscribe",
                                "payload": payload,
                            }
                        )

                    while not stop_event.is_set():
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=self.retry.recv_timeout_seconds,
                        )
                        recv_ms = now_ms()
                        self.last_message_recv_ms = int(recv_ms)
                        self.message_count += 1
                        self.raw_writer.write(
                            {
                                "ts": utc_now_iso(),
                                "recv_ms": recv_ms,
                                "collector": self.name,
                                "raw": raw,
                            }
                        )

                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            self.event_writer.write(
                                {
                                    "ts": utc_now_iso(),
                                    "recv_ms": recv_ms,
                                    "collector": self.name,
                                    "kind": "non_json",
                                }
                            )
                            continue

                        if isinstance(parsed, list):
                            items = [item for item in parsed if isinstance(item, dict)]
                        elif isinstance(parsed, dict):
                            items = [parsed]
                        else:
                            items = []

                        for item in items:
                            for event in self.normalize_event(item, recv_ms):
                                self._update_health_from_event(event, recv_ms)
                                payload = {
                                    "ts": utc_now_iso(),
                                    "recv_ms": recv_ms,
                                    "collector": self.name,
                                    **event,
                                }
                                if self.on_event is not None:
                                    self.on_event(payload)
                                self.event_writer.write(payload)
                                self.event_count += 1

            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                attempt += 1
                self.disconnect_count += 1
                self.reconnect_count += 1
                self.reconnect_attempt = int(attempt)
                self._set_connection_state("reconnecting")
                delay = min(
                    self.retry.reconnect_max_seconds,
                    self.retry.reconnect_base_seconds * (2 ** min(attempt - 1, 10)),
                )
                self.event_writer.write(
                    {
                        "ts": utc_now_iso(),
                        "collector": self.name,
                        "kind": "state",
                        "state": "disconnected",
                        "error": str(exc),
                        "reconnect_delay_seconds": round(delay, 3),
                        "attempt": attempt,
                        "health": self.health_snapshot(),
                    }
                )
                await asyncio.sleep(delay)

    def close(self) -> None:
        self.raw_writer.close()
        self.event_writer.close()
