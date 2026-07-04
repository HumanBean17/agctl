"""Tests for agctl.mock.http_server."""

from __future__ import annotations

import json
import socket
import threading
import time
from typing import Any

import httpx
import pytest

from agctl.config.models import HttpMatch, HttpStub, HttpResponse
from agctl.mock.http_server import MockHTTPServer


@pytest.fixture
def event_sink() -> list[dict[str, Any]]:
    """Capture all emitted events."""
    return []


@pytest.fixture
def emit_event(event_sink: list[dict[str, Any]]) -> callable:
    """Create an emit_event callable that appends to event_sink."""
    return lambda event: event_sink.append(event)


def start_server(
    stubs: dict[str, HttpStub],
    emit_event: callable,
    concurrency_cap: int = 64,
) -> MockHTTPServer:
    """Start a MockHTTPServer in a background thread and return it."""
    server = MockHTTPServer(
        ("127.0.0.1", 0),
        stubs=stubs,
        emit_event=emit_event,
        concurrency_cap=concurrency_cap,
    )

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    # Give server time to start
    time.sleep(0.01)

    return server


class TestMatchCaptureTemplate:
    """Test successful match with capture and template substitution."""

    def test_post_with_capture_and_template(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """POST /api/v1/orders with body filter and response template."""
        stubs = {
            "create-order": HttpStub(
                method="POST",
                path="/api/v1/orders",
                match=HttpMatch(body={"priority": "high"}),
                response=HttpResponse(
                    status=201,
                    body={"order_id": "{customer_id}-mock", "status": "PENDING"},
                ),
                delay_ms=0,
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/api/v1/orders",
                    json={"customer_id": "c1", "priority": "high"},
                    headers={"Content-Type": "application/json"},
                )

            assert response.status_code == 201
            assert response.json() == {"order_id": "c1-mock", "status": "PENDING"}
            assert response.headers["content-type"] == "application/json"

            assert len(event_sink) == 1
            event = event_sink[0]
            assert event["event"] == "http.hit"
            assert event["stub"] == "create-order"
            assert event["method"] == "POST"
            assert event["path"] == "/api/v1/orders"
            assert event["status"] == 201
            assert "duration_ms" in event
            assert "timestamp" not in event  # Engine adds timestamp
        finally:
            server.shutdown()


class TestMismatch:
    """Test various mismatch scenarios."""

    def test_body_filter_mismatch(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Body filter mismatch returns 404."""
        stubs = {
            "create-order": HttpStub(
                method="POST",
                path="/api/v1/orders",
                match=HttpMatch(body={"priority": "high"}),
                response=HttpResponse(status=201),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/api/v1/orders",
                    json={"customer_id": "c1", "priority": "low"},
                )

            assert response.status_code == 404
            assert response.json() == {"mock_error": "no matching stub"}

            assert len(event_sink) == 1
            event = event_sink[0]
            assert event["event"] == "http.unmatched"
            assert event["method"] == "POST"
            assert event["path"] == "/api/v1/orders"
            assert event["status"] == 404
        finally:
            server.shutdown()

    def test_method_mismatch(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Method mismatch returns 404."""
        stubs = {
            "create-order": HttpStub(
                method="POST",
                path="/api/v1/orders",
                response=HttpResponse(status=201),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/api/v1/orders")

            assert response.status_code == 404
            assert response.json() == {"mock_error": "no matching stub"}

            assert len(event_sink) == 1
            event = event_sink[0]
            assert event["event"] == "http.unmatched"
            assert event["method"] == "GET"
        finally:
            server.shutdown()


class TestPathCapture:
    """Test path parameter capture."""

    def test_path_capture(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """GET /api/v1/orders/{order_id} captures order_id."""
        stubs = {
            "get-order": HttpStub(
                method="GET",
                path="/api/v1/orders/{order_id}",
                response=HttpResponse(body={"order_id": "{order_id}"}),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/api/v1/orders/42")

            assert response.status_code == 200
            assert response.json() == {"order_id": "42"}

            assert len(event_sink) == 1
            event = event_sink[0]
            assert event["event"] == "http.hit"
            assert event["stub"] == "get-order"
        finally:
            server.shutdown()

    def test_query_string_strip(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Query string is stripped before matching."""
        stubs = {
            "get-order": HttpStub(
                method="GET",
                path="/api/v1/orders/{order_id}",
                response=HttpResponse(body={"order_id": "{order_id}"}),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(
                    f"http://127.0.0.1:{port}/api/v1/orders/42?x=1&y=2"
                )

            assert response.status_code == 200
            assert response.json() == {"order_id": "42"}
        finally:
            server.shutdown()


class TestChunkedBody:
    """Test Transfer-Encoding: chunked body handling."""

    def test_chunked_body_match(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """A genuine chunked POST (no Content-Length) is de-chunked and matched.

        Sends raw chunked bytes over a socket so the wire format is fully
        controlled: ``Transfer-Encoding: chunked`` with NO ``Content-Length``.
        Proves the read loop (readline + read(n)) neither deadlocks nor
        mis-parses: the de-chunked body satisfies the stub's ``match.body``
        filter and the templated response is returned. Under the old
        ``rfile.read(65536)`` impl this would hang until the socket timeout.
        """
        stubs = {
            "upload": HttpStub(
                method="POST",
                path="/upload",
                match=HttpMatch(body={"data": "test"}),
                response=HttpResponse(
                    status=200,
                    body={"received": "{data}"},
                ),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        payload = json.dumps({"data": "test"}).encode("utf-8")
        # "{hex}\\r\\n{data}\\r\\n0\\r\\n\\r\\n"
        chunked_body = b"%x\r\n%s\r\n0\r\n\r\n" % (len(payload), payload)
        request = (
            b"POST /upload HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Content-Type: application/json\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        ) + chunked_body

        try:
            with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                sock.sendall(request)
                response_data = b""
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response_data += chunk

            # Matched: status is 200 and the de-chunked {data} populated the
            # templated response body (proves body was de-chunked + filter hit).
            status_line = response_data.split(b"\r\n", 1)[0].decode("ascii")
            assert status_line.split()[1] == "200"
            assert b'"received": "test"' in response_data

            assert len(event_sink) == 1
            assert event_sink[0]["event"] == "http.hit"
            assert event_sink[0]["stub"] == "upload"
        finally:
            server.shutdown()


class TestHTTP11ContentLength:
    """Test HTTP/1.1 and Content-Length header."""

    def test_http11_and_content_length(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Response is HTTP/1.1 and includes Content-Length."""
        stubs = {
            "test": HttpStub(
                method="GET",
                path="/test",
                response=HttpResponse(body="hello"),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/test")

            assert response.http_version == "HTTP/1.1"
            assert "content-length" in response.headers
            assert response.headers["content-length"] == "5"  # "hello" = 5 bytes
        finally:
            server.shutdown()


class TestConcurrencyCap:
    """Test semaphore-based concurrency cap."""

    def test_concurrency_cap_overflow_returns_429(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Concurrency cap overflow returns 429 deterministically.

        Pre-acquires the single permit so the next ``acquire(blocking=False)``
        in the handler must fail immediately — no wall-clock timing dependency
        (the old 2-thread/50ms-window variant could let both win on slow CI).
        Also asserts the 429 returns well under ``delay_ms`` (prompt failure).
        """
        delay_ms = 50
        stubs = {
            "slow": HttpStub(
                method="GET",
                path="/slow",
                response=HttpResponse(body="done"),
                delay_ms=delay_ms,
            )
        }

        server = start_server(stubs, emit_event, concurrency_cap=1)
        port = server.server_port

        # Exhaust the single permit -> next request overflows immediately.
        server.semaphore.acquire()

        try:
            start = time.monotonic()
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/slow")
            elapsed_ms = (time.monotonic() - start) * 1000

            assert response.status_code == 429
            # 429 must be prompt, never wait for delay_ms
            assert elapsed_ms < delay_ms
        finally:
            server.semaphore.release()
            server.shutdown()


class TestBodyParseSkipped:
    """Test body_parse_skipped event."""

    def test_body_parse_skipped_event(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Non-JSON body with unresolved placeholder emits body_parse_skipped."""
        stubs = {
            "echo": HttpStub(
                method="POST",
                path="/echo",
                response=HttpResponse(body="Customer: {customer_id}"),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/echo",
                    content="plain text body",
                    headers={"Content-Type": "text/plain"},
                )

            # Response should contain literal placeholder
            assert response.status_code == 200
            assert response.text == "Customer: {customer_id}"

            # Should emit both body_parse_skipped and http.hit events
            assert len(event_sink) == 2
            parse_skipped = [e for e in event_sink if e["event"] == "http.body_parse_skipped"][0]
            assert parse_skipped["stub"] == "echo"
            assert parse_skipped["method"] == "POST"
            assert parse_skipped["path"] == "/echo"
            assert parse_skipped["reason"] == "body did not parse to a dict; response has unresolved placeholders"

            # Also verify http.hit was emitted
            hit_events = [e for e in event_sink if e["event"] == "http.hit"]
            assert len(hit_events) == 1
            assert hit_events[0]["stub"] == "echo"
        finally:
            server.shutdown()

    def test_body_parse_skipped_on_list_body(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """A JSON body that parses to a list (not dict) -> body_parse_skipped.

        A list/scalar body parses but exposes no capturable keys, so a
        {placeholder} referencing a body field stays unresolved -- the broadened
        condition (not a dict) must fire just like the non-JSON case.
        """
        stubs = {
            "echo": HttpStub(
                method="POST",
                path="/echo",
                response=HttpResponse(body="Item: {item}"),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/echo",
                    content=json.dumps(["a", "b"]),
                    headers={"Content-Type": "application/json"},
                )

            assert response.status_code == 200
            # Placeholder unresolved: list body has no capturable keys
            assert response.text == "Item: {item}"

            parse_skipped = [
                e for e in event_sink if e["event"] == "http.body_parse_skipped"
            ]
            assert len(parse_skipped) == 1
            assert parse_skipped[0]["stub"] == "echo"

            hit_events = [e for e in event_sink if e["event"] == "http.hit"]
            assert len(hit_events) == 1
        finally:
            server.shutdown()


class TestContentTypeDefault:
    """Test Content-Type defaulting."""

    def test_json_body_defaults_to_application_json(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Dict/list body defaults to application/json."""
        stubs = {
            "json": HttpStub(
                method="GET",
                path="/json",
                response=HttpResponse(body={"key": "value"}),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/json")

            assert response.status_code == 200
            assert response.headers["content-type"] == "application/json"
            assert response.json() == {"key": "value"}
        finally:
            server.shutdown()

    def test_scalar_body_defaults_to_text_plain(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Scalar body defaults to text/plain."""
        stubs = {
            "text": HttpStub(
                method="GET",
                path="/text",
                response=HttpResponse(body="plain text"),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/text")

            assert response.status_code == 200
            assert response.headers["content-type"] == "text/plain"
            assert response.text == "plain text"
        finally:
            server.shutdown()

    def test_none_body_is_empty(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """An unset (None) response.body yields an empty body, not the text "None"."""
        stubs = {
            "empty": HttpStub(
                method="GET",
                path="/empty",
                response=HttpResponse(),  # body defaults to None
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/empty")

            assert response.status_code == 200
            assert response.content == b""
            assert response.headers["content-length"] == "0"
            assert response.headers["content-type"] == "text/plain"
        finally:
            server.shutdown()

    def test_explicit_headers_override_defaults(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """Explicit response.headers always win."""
        stubs = {
            "custom": HttpStub(
                method="GET",
                path="/custom",
                response=HttpResponse(
                    body={"data": "value"},
                    headers={"Content-Type": "application/custom+json"},
                ),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.get(f"http://127.0.0.1:{port}/custom")

            assert response.status_code == 200
            assert response.headers["content-type"] == "application/custom+json"
        finally:
            server.shutdown()


class TestJqPredicate:
    """Test match.jq predicate evaluation in stub matching."""

    def test_jq_predicate_match(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """(a) POST amount:1500 against stub match.jq='.amount > 1000' -> 201 + hit."""
        stubs = {
            "big-payment": HttpStub(
                method="POST",
                path="/payments",
                match=HttpMatch(jq=".amount > 1000"),
                response=HttpResponse(status=201, body={"status": "OK"}),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/payments",
                    json={"amount": 1500},
                )

            assert response.status_code == 201

            assert len(event_sink) == 1
            event = event_sink[0]
            assert event["event"] == "http.hit"
            assert event["stub"] == "big-payment"
            assert event["method"] == "POST"
            assert event["path"] == "/payments"
            assert event["status"] == 201
        finally:
            server.shutdown()

    def test_jq_predicate_false_falls_through(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """(b) POST amount:500 against stub match.jq='.amount > 1000' -> 404 + unmatched."""
        stubs = {
            "big-payment": HttpStub(
                method="POST",
                path="/payments",
                match=HttpMatch(jq=".amount > 1000"),
                response=HttpResponse(status=201),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/payments",
                    json={"amount": 500},
                )

            assert response.status_code == 404
            assert response.json() == {"mock_error": "no matching stub"}

            assert len(event_sink) == 1
            event = event_sink[0]
            assert event["event"] == "http.unmatched"
            assert event["status"] == 404
        finally:
            server.shutdown()

    def test_two_stubs_distinguished_by_jq(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """(c) Two stubs same method+path, distinguished by jq.

        high-value (amount>1000) -> 201/APPROVED (first stub).
        low-value (else)         -> 202/QUEUED   (second stub, no jq).
        POST 1500 hits first; POST 500 falls through to second.
        """
        stubs = {
            "high-value": HttpStub(
                method="POST",
                path="/payments",
                match=HttpMatch(jq=".amount > 1000"),
                response=HttpResponse(status=201, body={"decision": "APPROVED"}),
            ),
            "low-value": HttpStub(
                method="POST",
                path="/payments",
                response=HttpResponse(status=202, body={"decision": "QUEUED"}),
            ),
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                r1 = client.post(
                    f"http://127.0.0.1:{port}/payments",
                    json={"amount": 1500},
                )
                r2 = client.post(
                    f"http://127.0.0.1:{port}/payments",
                    json={"amount": 500},
                )

            assert r1.status_code == 201
            assert r1.json() == {"decision": "APPROVED"}

            assert r2.status_code == 202
            assert r2.json() == {"decision": "QUEUED"}

            hit_events = [e for e in event_sink if e["event"] == "http.hit"]
            assert len(hit_events) == 2
            assert hit_events[0]["stub"] == "high-value"
            assert hit_events[1]["stub"] == "low-value"
        finally:
            server.shutdown()

    def test_body_and_jq_both_required(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """(d) Stub with match.body AND match.jq requires BOTH to pass (AND).

        A request matching only the body filter (priority=high, amount=500)
        falls through to 404; a request matching both (priority=high,
        amount=1500) hits.
        """
        stubs = {
            "priority-big": HttpStub(
                method="POST",
                path="/orders",
                match=HttpMatch(body={"priority": "high"}, jq=".amount > 1000"),
                response=HttpResponse(status=201, body={"ok": True}),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                # Body matches but jq fails -> 404.
                r_only_body = client.post(
                    f"http://127.0.0.1:{port}/orders",
                    json={"priority": "high", "amount": 500},
                )
                # Both pass -> 201.
                r_both = client.post(
                    f"http://127.0.0.1:{port}/orders",
                    json={"priority": "high", "amount": 1500},
                )

            assert r_only_body.status_code == 404
            assert r_both.status_code == 201
            assert r_both.json() == {"ok": True}

            hit_events = [e for e in event_sink if e["event"] == "http.hit"]
            unmatched_events = [
                e for e in event_sink if e["event"] == "http.unmatched"
            ]
            assert len(hit_events) == 1
            assert len(unmatched_events) == 1
        finally:
            server.shutdown()

    def test_jq_predicate_raises_is_soft_nonmatch(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """(e) match.jq='.a.b.c' on a body without .a -> 404 + unmatched (no 500).

        jq_bool swallows the predicate's runtime error to False, so the stub
        is a soft non-match and the request falls through cleanly.
        """
        stubs = {
            "deep": HttpStub(
                method="POST",
                path="/items",
                match=HttpMatch(jq=".a.b.c"),
                response=HttpResponse(status=200, body={"hit": True}),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/items",
                    json={"x": 1},  # no .a -> .a.b.c raises
                )

            assert response.status_code == 404
            assert response.json() == {"mock_error": "no matching stub"}

            assert len(event_sink) == 1
            assert event_sink[0]["event"] == "http.unmatched"
            assert event_sink[0]["status"] == 404
        finally:
            server.shutdown()

    def test_jq_predicate_non_json_body_is_nonmatch(
        self, emit_event: callable, event_sink: list[dict[str, Any]]
    ) -> None:
        """(f) match.jq on a non-JSON body -> 404 + unmatched.

        parsed_body is None for plain text; jq_bool(None, expr) -> False ->
        soft non-match -> fall through.
        """
        stubs = {
            "big-payment": HttpStub(
                method="POST",
                path="/payments",
                match=HttpMatch(jq=".amount > 1000"),
                response=HttpResponse(status=201),
            )
        }

        server = start_server(stubs, emit_event)
        port = server.server_port

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"http://127.0.0.1:{port}/payments",
                    content="plain text body",
                    headers={"Content-Type": "text/plain"},
                )

            assert response.status_code == 404
            assert response.json() == {"mock_error": "no matching stub"}

            assert len(event_sink) == 1
            assert event_sink[0]["event"] == "http.unmatched"
            assert event_sink[0]["status"] == 404
        finally:
            server.shutdown()
