"""HTTP mock server using stdlib ThreadingHTTPServer (DESIGN §4.2)."""

from __future__ import annotations

import json
import re
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from agctl.assertions import json_subset
from agctl.mock.routing import match_path
from agctl.resolution import fill_placeholders

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
            """Read request body honoring Content-Length or Transfer-Encoding: chunked."""
            # Check for Transfer-Encoding: chunked
            transfer_encoding = self.headers.get("Transfer-Encoding", "")
            if "chunked" in transfer_encoding.lower():
                # Read all available data and de-chunk
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                # For chunked, we need to read until we get the terminal chunk
                # This is a simplification - stdlib doesn't give us raw access
                # We'll read based on Content-Length if available
                content_length = self.headers.get("Content-Length")
                if content_length:
                    body = self.rfile.read(int(content_length))
                else:
                    # Read until EOF (limited to avoid blocking)
                    body = self.rfile.read(65536)
                return _decode_chunked(body)

            # Use Content-Length if present
            content_length = self.headers.get("Content-Length")
            if content_length:
                return self.rfile.read(int(content_length))

            return b""

        def _parse_json_body(self, body: bytes) -> Any:
            """Parse body as JSON; return None if not JSON."""
            content_type = self.headers.get("Content-Type", "")

            # Check if content-type indicates JSON
            is_json_ct = "application/json" in content_type

            if not body:
                return None

            # Try to parse as JSON
            try:
                return json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # If content-type said JSON but parse failed, still return None
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
            """Send HTTP response with JSON body and Content-Length."""
            response_headers = (headers or {}).copy()

            # Default Content-Type based on body type
            if "content-type" not in {k.lower() for k in response_headers}:
                if isinstance(body, (dict, list)):
                    response_headers["Content-Type"] = "application/json"
                    body_bytes = json.dumps(body).encode("utf-8")
                else:
                    response_headers["Content-Type"] = "text/plain"
                    body_bytes = str(body).encode("utf-8")
            else:
                # Use explicit Content-Type
                if isinstance(body, (dict, list)):
                    body_bytes = json.dumps(body).encode("utf-8")
                else:
                    body_bytes = str(body).encode("utf-8")

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

            # Step 3: Match stubs
            matched_stub = None
            captures: dict[str, str] = {}

            for stub_name, stub in stubs.items():
                # Match method (case-insensitive)
                if stub.method.upper() != self.command.upper():
                    continue

                # Match path
                path_captures = match_path(stub.path, self.path)
                if path_captures is None:
                    continue

                # Match body filter if present
                if stub.match and stub.match.body is not None:
                    if not json_subset(stub.match.body, parsed_body):
                        continue  # Body filter failed

                # Found a match
                matched_stub = (stub_name, stub)
                captures = path_captures.copy()
                break

            # Step 4: Build capture context
            if isinstance(parsed_body, dict):
                # Merge top-level body keys via str(v) stringification
                for key, value in parsed_body.items():
                    captures[key] = str(value)

            # Step 5: No match -> 404
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

            # Step 6: React
            stub_name, stub = matched_stub

            # Render response via fill_placeholders
            rendered_body = fill_placeholders(stub.response.body, captures)
            rendered_headers = (
                fill_placeholders(stub.response.headers or {}, captures)
                if stub.response.headers
                else {}
            )

            # Check for body_parse_skipped condition (AFTER substitution)
            if parsed_body is None and self._has_unresolved_placeholders(
                rendered_body
            ):
                emit_event(
                    {
                        "event": "http.body_parse_skipped",
                        "stub": stub_name,
                        "method": self.command,
                        "path": self.path,
                        "reason": "non-JSON body; response has unresolved placeholders",
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
