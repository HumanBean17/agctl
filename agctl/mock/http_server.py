"""HTTP mock server using stdlib ThreadingHTTPServer (DESIGN §4.2)."""

from __future__ import annotations

import json
import re
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

from agctl.assertions import jq_bool, json_subset
from agctl.mock.capture import resolve_captures
from agctl.mock.routing import match_path
from agctl.resolution import CaptureValue, render_typed

__all__ = ["MockHTTPServer", "make_handler"]


def _decode_chunked(data: bytes) -> bytes:
    """Decode Transfer-Encoding: chunked body.

    Chunked format: "{hex_size}\\r\\n{chunk}\\r\\n...0\\r\\n\\r\\n"
    Returns the concatenated decoded chunks.
    """
    chunks = []
    pos = 0

    while pos < len(data):
        # Find chunk size line (ends with \r\n)
        line_end = data.find(b"\r\n", pos)
        if line_end == -1:
            break  # Malformed

        size_line = data[pos:line_end].decode("ascii", errors="ignore")
        try:
            chunk_size = int(size_line.strip(), 16)
        except ValueError:
            break  # Malformed

        if chunk_size == 0:
            # Last chunk marker
            break

        # Extract chunk data
        chunk_start = line_end + 2  # Skip \r\n
        chunk_end = chunk_start + chunk_size
        if chunk_end > len(data):
            break  # Incomplete

        chunks.append(data[chunk_start:chunk_end])
        pos = chunk_end + 2  # Skip trailing \r\n

    return b"".join(chunks)


def make_handler(
    stubs: dict[str, Any],
    emit_event: Callable[[dict[str, Any]], None],
    semaphore: threading.Semaphore,
) -> type[BaseHTTPRequestHandler]:
    """Factory that creates a BaseHTTPRequestHandler bound to stubs/emit_event/semaphore.

    Args:
        stubs: Ordered dict of stub_name -> HttpStub (insertion order matters)
        emit_event: Engine's single-writer event callable (already locked)
        semaphore: Concurrency cap semaphore (acquire before delay_ms)

    Returns:
        A BaseHTTPRequestHandler subclass configured for the mock server.
    """

    class MockHTTPHandler(BaseHTTPRequestHandler):
        """HTTP request handler for mock server."""

        protocol_version = "HTTP/1.1"

        def _read_body(self) -> bytes:
            """Read request body honoring Transfer-Encoding or Content-Length.

            Transfer-Encoding: chunked takes precedence over Content-Length
            (RFC 7230 §3.3.3): when both are present only the chunked body is
            read (the prior code computed/returned ``body`` twice in that case).
            """
            transfer_encoding = self.headers.get("Transfer-Encoding", "")
            if "chunked" in transfer_encoding.lower():
                return self._read_chunked_body()

            content_length = self.headers.get("Content-Length")
            if content_length:
                return self.rfile.read(int(content_length))

            return b""

        def _read_chunked_body(self) -> bytes:
            """Read a Transfer-Encoding: chunked body without blocking.

            Uses ``readline()`` for each hex chunk-size line and ``read(n)`` for
            the chunk data + trailing CRLF, looping until the 0-size terminator.
            This avoids the ``rfile.read(N)`` deadlock that blocks until N bytes
            or EOF arrive — neither comes under HTTP/1.1 keep-alive, so the old
            ``read(65536)`` hung the handler thread until the client timed out.
            The raw chunked bytes are reassembled and handed to
            :func:`_decode_chunked` (verified correct on well-formed input).
            """
            buf = bytearray()
            while True:
                size_line = self.rfile.readline()
                if not size_line:  # connection closed mid-stream
                    break
                buf += size_line
                try:
                    chunk_size = int(size_line.strip(), 16)
                except ValueError:
                    break  # malformed chunk-size line
                if chunk_size == 0:
                    # Drain the trailer section + terminating blank line so a
                    # subsequent keep-alive request on this connection aligns.
                    while True:
                        trailer = self.rfile.readline()
                        if trailer in (b"\r\n", b"\n", b""):
                            break
                    break
                # Chunk data + the trailing CRLF that follows it
                buf += self.rfile.read(chunk_size + 2)
            return _decode_chunked(bytes(buf))

        def _parse_json_body(self, body: bytes) -> Any:
            """Parse body as JSON; return None if empty or unparseable.

            Content-Type is intentionally ignored: a body is JSON iff it parses
            as JSON, so stubs can match JSON sent without the header (and a
            scalar/list body parses but yields no capturable keys).
            """
            if not body:
                return None
            try:
                return json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return None

        def _has_unresolved_placeholders(self, value: Any) -> bool:
            """Check if value contains unresolved {placeholder} tokens."""
            if isinstance(value, str):
                return bool(re.search(r"\{[A-Za-z_][A-Za-z0-9_]*\}", value))
            if isinstance(value, dict):
                return any(self._has_unresolved_placeholders(v) for v in value.values())
            if isinstance(value, list):
                return any(self._has_unresolved_placeholders(v) for v in value)
            return False

        def _send_json_response(
            self, status: int, body: Any, headers: dict[str, str] | None = None
        ) -> None:
            """Send HTTP response; always emit Content-Length (HTTP/1.1 keep-alive).

            Serialization: dict/list -> JSON bytes; ``None`` -> empty body (not
            the literal ``"None"``); other scalars -> ``str()`` utf-8. Content-Type
            defaults to application/json for dict/list and text/plain otherwise,
            unless the caller supplies an explicit Content-Type.
            """
            response_headers = (headers or {}).copy()

            if isinstance(body, (dict, list)):
                body_bytes = json.dumps(body).encode("utf-8")
                default_ct = "application/json"
            elif body is None:
                body_bytes = b""
                default_ct = "text/plain"
            else:
                body_bytes = str(body).encode("utf-8")
                default_ct = "text/plain"

            if "content-type" not in {k.lower() for k in response_headers}:
                response_headers["Content-Type"] = default_ct

            # Always send Content-Length (required for HTTP/1.1 keep-alive)
            response_headers["Content-Length"] = str(len(body_bytes))

            self.send_response(status)
            for header_name, header_value in response_headers.items():
                self.send_header(header_name, header_value)
            self.end_headers()
            self.wfile.write(body_bytes)

        def do_POST(self) -> None:
            """Handle POST request."""
            self._handle_request()

        def do_GET(self) -> None:
            """Handle GET request."""
            self._handle_request()

        def do_PUT(self) -> None:
            """Handle PUT request."""
            self._handle_request()

        def do_DELETE(self) -> None:
            """Handle DELETE request."""
            self._handle_request()

        def do_PATCH(self) -> None:
            """Handle PATCH request."""
            self._handle_request()

        def _handle_request(self) -> None:
            """Handle any HTTP request method."""
            start_time = time.time()

            # Step 1: Read request body
            raw_body = self._read_body()

            # Step 2: Parse body as JSON
            parsed_body = self._parse_json_body(raw_body)

            # Request envelope: built ONCE per request, shared by match.jq
            # (predicate input) and resolve_captures (capture input) so the two
            # features share a single root. Field set and casing are load-bearing
            # (capture tests pin them): method, path, headers (lowercased keys),
            # body (= parsed_body).
            envelope = {
                "method": self.command,
                "path": urlsplit(self.path).path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": parsed_body,
            }

            # Step 3: Match stubs
            matched_stub = None
            captures: dict[str, CaptureValue] = {}

            for stub_name, stub in stubs.items():
                # Match method (case-insensitive)
                if stub.method.upper() != self.command.upper():
                    continue

                # Match path
                path_captures = match_path(stub.path, self.path)
                if path_captures is None:
                    continue

                # Match body filter if present. json_subset (match.body) stays
                # rooted at parsed_body — only the jq predicate changed roots.
                if stub.match and stub.match.body is not None:
                    if not json_subset(stub.match.body, parsed_body):
                        continue  # Body filter failed

                # Match jq predicate if present. The predicate is rooted at the
                # request envelope (`.body.<field>` / `.headers.<name>` / `.method`
                # / `.path`), mirroring capture.from. jq_bool swallows compile/
                # runtime errors to False (soft non-match per DESIGN §3.2).
                if stub.match and stub.match.jq is not None:
                    if not jq_bool(envelope, stub.match.jq):
                        continue  # jq predicate failed

                # Found a match
                matched_stub = (stub_name, stub)
                captures = {
                    k: CaptureValue(v, "scalar") for k, v in path_captures.items()
                }
                break

            # Step 4: No match -> 404. The capture-context build is skipped on
            # this path: it is only needed for a matched stub, so building it
            # here would be wasted work whose result is immediately discarded.
            if matched_stub is None:
                self._send_json_response(
                    404,
                    {"mock_error": "no matching stub"},
                    headers={"Content-Type": "application/json"},
                )
                emit_event(
                    {
                        "event": "http.unmatched",
                        "method": self.command,
                        "path": self.path,
                        "status": 404,
                    }
                )
                return

            # Step 5: Build capture context for the matched stub only.
            # Implicit: top-level body keys as scalar CaptureValues (raw value;
            # render_typed applies str() for scalar — preserves today's coercion
            # of numerics/bools to strings).
            # Explicit: envelope-rooted jq extraction (stub.capture) overrides
            # implicit on name collision, including type promotion (e.g. an
            # implicit scalar promoted to object for true pass-through).
            stub_name, stub = matched_stub
            if isinstance(parsed_body, dict):
                for key, value in parsed_body.items():
                    captures[key] = CaptureValue(value, "scalar")

            if stub.capture is not None:
                explicit, missing = resolve_captures(envelope, stub.capture)
                captures.update(explicit)
                for cap_name, from_path in missing:
                    emit_event(
                        {
                            "event": "capture.missing",
                            "stub": stub_name,
                            "name": cap_name,
                            "from": from_path,
                        }
                    )

            # Step 6: React
            # Render response via typed capture substitution.
            rendered_body = render_typed(stub.response.body, captures)
            rendered_headers = (
                render_typed(stub.response.headers or {}, captures)
                if stub.response.headers
                else {}
            )

            # Emit body_parse_skipped when the body did not parse to a dict
            # (None, list, or scalar) AND a {placeholder} referencing a body
            # field remains unresolved in the rendered response. A list/scalar
            # body parses but exposes no capturable keys, so such placeholders
            # stay unresolved just like a non-JSON body.
            if not isinstance(parsed_body, dict) and self._has_unresolved_placeholders(
                rendered_body
            ):
                emit_event(
                    {
                        "event": "http.body_parse_skipped",
                        "stub": stub_name,
                        "method": self.command,
                        "path": self.path,
                        "reason": "body did not parse to a dict; response has unresolved placeholders",
                    }
                )

            # Acquire semaphore for concurrency cap
            if semaphore.acquire(blocking=False):
                try:
                    # Sleep for delay_ms
                    if stub.delay_ms > 0:
                        time.sleep(stub.delay_ms / 1000.0)

                    # Send response
                    self._send_json_response(
                        stub.response.status, rendered_body, rendered_headers
                    )

                    duration_ms = int((time.time() - start_time) * 1000)
                    emit_event(
                        {
                            "event": "http.hit",
                            "stub": stub_name,
                            "method": self.command,
                            "path": self.path,
                            "status": stub.response.status,
                            "duration_ms": duration_ms,
                        }
                    )
                finally:
                    semaphore.release()
            else:
                # Concurrency cap exhausted
                self._send_json_response(429, {"error": "Too many concurrent requests"})

        def log_message(self, format: str, *args: Any) -> None:
            """Suppress default stderr logging."""
            pass

    return MockHTTPHandler


class MockHTTPServer(ThreadingHTTPServer):
    """HTTP mock server using stdlib ThreadingHTTPServer.

    Args:
        server_address: (host, port) tuple. Use port 0 for auto-assignment.
        RequestHandlerClass: Handler class from make_handler() - pass None to auto-create.
        stubs: Ordered dict of stub_name -> HttpStub.
        emit_event: Engine's single-writer event callable.
        concurrency_cap: Max concurrent requests (default 64).
    """

    def __init__(
        self,
        server_address: tuple[str, int],
        RequestHandlerClass: type[BaseHTTPRequestHandler] | None = None,
        *,
        stubs: dict[str, Any],
        emit_event: Callable[[dict[str, Any]], None],
        concurrency_cap: int = 64,
    ):
        self.stubs = stubs
        self.emit_event = emit_event
        self.semaphore = threading.Semaphore(concurrency_cap)

        # Auto-create handler if not provided
        if RequestHandlerClass is None:
            RequestHandlerClass = make_handler(stubs, emit_event, self.semaphore)

        super().__init__(server_address, RequestHandlerClass)
