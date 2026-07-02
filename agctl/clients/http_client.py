"""httpx-backed HTTP client for agctl commands (DESIGN §7 clients).

The ``httpx`` dependency is lazy-imported inside :meth:`HttpClient.__init__`
so that this module imports cleanly even when the optional ``http`` extra is
not installed. Only constructing a client requires httpx.
"""

import json
import time

from ..errors import ConfigError, ConnectionFailure, OperationTimeout


class HttpClient:
    """Thin wrapper around :class:`httpx.Client`.

    Produces result dicts matching the ``http.call`` schema in DESIGN §4.2::

        {"status_code", "response_time_ms", "headers", "body", "url", "method"}
    """

    def __init__(self, base_url, timeout_seconds, *, transport=None, headers=None):
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise ConfigError(
                "HTTP support requires the 'http' extra: pip install 'agctl[http]'"
            ) from exc

        self._httpx = httpx
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout_seconds,
            transport=transport,
        )
        self._default_headers = dict(headers) if headers else {}

    def request(self, method, path, *, headers=None, body=None, params=None) -> dict:
        """Send a single HTTP request and return the DESIGN §4.2 result dict."""
        httpx = self._httpx

        # Merge default + per-call headers case-insensitively; per-call wins.
        sent_headers = self._merge_headers(self._default_headers, headers)

        request_kwargs = {"headers": sent_headers or None, "params": params}
        if body is not None:
            request_kwargs["json"] = body

        try:
            start = time.monotonic()
            response = self._client.request(method, path, **request_kwargs)
            elapsed_ms = int((time.monotonic() - start) * 1000)
        except httpx.ConnectTimeout as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        except httpx.ConnectError as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        except httpx.ReadTimeout as exc:
            raise OperationTimeout(message=str(exc)) from exc
        except httpx.TimeoutException as exc:
            # Non-connect timeouts (write/pool/read beyond ReadTimeout, etc.)
            raise OperationTimeout(message=str(exc)) from exc
        except httpx.HTTPError as exc:
            # Best-effort mapping of remaining network errors.
            raise ConnectionFailure(message=str(exc)) from exc

        parsed_body = self._parse_body(response)

        return {
            "status_code": response.status_code,
            "response_time_ms": elapsed_ms,
            "headers": {k.lower(): v for k, v in response.headers.items()},
            "body": parsed_body,
            "url": str(response.request.url),
            "method": method,
        }

    @staticmethod
    def _merge_headers(default, per_call):
        """Merge two header dicts case-insensitively; per_call wins on clash."""
        if not default and not per_call:
            return {}

        merged = {}
        # Index defaults by lowercased key -> original-cased key + value.
        lower_defaults = {}
        for key, value in (default or {}).items():
            lower_defaults[key.lower()] = (key, value)
            merged[key] = value

        for key, value in (per_call or {}).items():
            low = key.lower()
            if low in lower_defaults:
                # Override: drop the original-cased default entry to avoid
                # sending duplicates, then set the per-call key.
                orig_key, _ = lower_defaults[low]
                merged.pop(orig_key, None)
            merged[key] = value

        return merged

    def _parse_body(self, response):
        text = response.text
        content_type = response.headers.get("content-type", "")
        is_json_ct = "json" in content_type.lower()

        if is_json_ct:
            try:
                return json.loads(text)
            except (ValueError, json.JSONDecodeError):
                return text

        # Fall back: try to parse as JSON anyway; if it fails, return text.
        try:
            return json.loads(text)
        except (ValueError, json.JSONDecodeError):
            return text
