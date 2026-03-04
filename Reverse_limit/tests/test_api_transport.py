from __future__ import annotations

import json
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Callable, Dict, Iterator, Tuple

import pytest

from scripts.common.api_transport import ApiHTTPError, ApiResponseParseError, ApiTransport, RetryConfig


Responder = Callable[[str, str, bytes], Tuple[int, Dict[str, str], bytes]]


@contextmanager
def run_local_server(responder: Responder) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self._handle("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._handle("POST")

        def _handle(self, method: str) -> None:
            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length > 0 else b""
            status, headers, payload = responder(method, self.path, body)
            self.send_response(int(status))
            for key, value in headers.items():
                self.send_header(str(key), str(value))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_get_retries_on_transient_status_then_succeeds() -> None:
    attempts = {"count": 0}

    def responder(method: str, path: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        assert method == "GET"
        assert path == "/retry"
        assert body == b""
        attempts["count"] += 1
        if attempts["count"] < 3:
            return 503, {"Content-Type": "application/json"}, b'{"error":"temporary"}'
        return 200, {"Content-Type": "application/json"}, b'{"ok":true,"attempt":3}'

    with run_local_server(responder) as base_url:
        transport = ApiTransport(
            timeout_seconds=2,
            retry_config=RetryConfig(
                max_attempts=3,
                base_backoff_seconds=0.0,
                jitter_ratio=0.0,
            ),
        )
        status, payload = transport.request_json("GET", f"{base_url}/retry")

    assert status == 200
    assert payload == {"ok": True, "attempt": 3}
    assert attempts["count"] == 3


def test_post_is_not_retried_when_post_not_in_retry_methods() -> None:
    attempts = {"count": 0}

    def responder(method: str, path: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        assert method == "POST"
        assert path == "/submit"
        assert json.loads(body.decode("utf-8")) == {"x": 1}
        attempts["count"] += 1
        return 503, {"Content-Type": "application/json"}, b'{"error":"temporary"}'

    with run_local_server(responder) as base_url:
        transport = ApiTransport(
            timeout_seconds=2,
            retry_config=RetryConfig(
                max_attempts=5,
                base_backoff_seconds=0.0,
                jitter_ratio=0.0,
                retry_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
            ),
        )
        with pytest.raises(ApiHTTPError):
            transport.request_json("POST", f"{base_url}/submit", json={"x": 1})

    assert attempts["count"] == 1


def test_invalid_json_response_raises_parse_error() -> None:
    def responder(method: str, path: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        assert method == "GET"
        assert path == "/bad-json"
        assert body == b""
        return 200, {"Content-Type": "application/json"}, b"not-json"

    with run_local_server(responder) as base_url:
        transport = ApiTransport(timeout_seconds=2)
        with pytest.raises(ApiResponseParseError):
            transport.request_json("GET", f"{base_url}/bad-json")


def test_allow_status_accepts_non_200_and_empty_body_returns_none() -> None:
    def responder(method: str, path: str, body: bytes) -> Tuple[int, Dict[str, str], bytes]:
        assert method == "POST"
        assert path == "/created"
        assert json.loads(body.decode("utf-8")) == {"hello": "world"}
        return 201, {}, b""

    with run_local_server(responder) as base_url:
        transport = ApiTransport(timeout_seconds=2)
        status, payload = transport.request_json(
            "POST",
            f"{base_url}/created",
            allow_status={201},
            json={"hello": "world"},
        )

    assert status == 201
    assert payload is None
