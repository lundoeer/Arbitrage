from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Tuple

import requests


DEFAULT_TRANSIENT_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
DEFAULT_RETRY_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class ApiTransportError(RuntimeError):
    """Base transport error for HTTP request/response failures."""


class ApiHTTPError(ApiTransportError):
    """Raised when a response status is not in the allowed set."""

    def __init__(self, url: str, status_code: int, body_preview: str):
        super().__init__(f"HTTP {status_code} from {url}: {body_preview}")
        self.url = url
        self.status_code = status_code
        self.body_preview = body_preview


class ApiResponseParseError(ApiTransportError):
    """Raised when response body cannot be parsed as JSON."""

    def __init__(self, url: str, message: str):
        super().__init__(f"Invalid JSON from {url}: {message}")
        self.url = url


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_backoff_seconds: float = 0.5
    jitter_ratio: float = 0.2
    transient_status_codes: frozenset[int] = DEFAULT_TRANSIENT_HTTP_STATUS_CODES
    retry_methods: frozenset[str] = DEFAULT_RETRY_METHODS


class ApiTransport:
    """Reusable HTTP transport with retries/backoff and JSON parsing."""

    def __init__(
        self,
        *,
        default_headers: Optional[Mapping[str, str]] = None,
        timeout_seconds: int = 10,
        retry_config: Optional[RetryConfig] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.session = session or requests.Session()
        if default_headers:
            self.session.headers.update(dict(default_headers))
        self.timeout_seconds = timeout_seconds
        self.retry = retry_config or RetryConfig()

    def request_json(
        self,
        method: str,
        url: str,
        *,
        allow_status: Optional[Iterable[int]] = None,
        **kwargs: Any,
    ) -> Tuple[int, Any]:
        accepted_status = set(allow_status or {200})
        method_upper = method.upper()
        retryable_method = method_upper in self.retry.retry_methods

        last_exception: Optional[Exception] = None
        max_attempts = max(1, int(self.retry.max_attempts))

        for attempt in range(1, max_attempts + 1):
            try:
                response = self.session.request(
                    method_upper,
                    url,
                    timeout=self.timeout_seconds,
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_exception = exc
                if not retryable_method or attempt >= max_attempts:
                    raise ApiTransportError(f"Request failed for {url}: {exc}") from exc
                self._sleep_backoff(attempt)
                continue

            if response.status_code in accepted_status:
                if not response.text:
                    return response.status_code, None
                try:
                    return response.status_code, response.json()
                except ValueError as exc:
                    raise ApiResponseParseError(url, str(exc)) from exc

            should_retry = (
                retryable_method
                and attempt < max_attempts
                and response.status_code in self.retry.transient_status_codes
            )
            if should_retry:
                self._sleep_backoff(attempt)
                continue

            raise ApiHTTPError(url, response.status_code, response.text[:500])

        if last_exception is not None:
            raise ApiTransportError(f"Request failed for {url}: {last_exception}") from last_exception
        raise ApiTransportError(f"Request failed for {url}: exhausted retries")

    def _sleep_backoff(self, attempt: int) -> None:
        base = max(0.0, float(self.retry.base_backoff_seconds))
        wait = base * (2 ** max(0, attempt - 1))
        jitter_ratio = max(0.0, float(self.retry.jitter_ratio))
        if jitter_ratio > 0 and wait > 0:
            wait += random.uniform(0, wait * jitter_ratio)
        time.sleep(wait)
