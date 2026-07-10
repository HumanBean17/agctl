"""Unit tests for `grpc call` command (DESIGN §3.3, D6).

Tests inject a fake gRPC client via monkeypatch to `grpc_commands.new_grpc_client`,
avoiding any real gRPC connection. The fake mirrors the GrpcClient contract.
"""

from pathlib import Path
from typing import Any

import grpc
import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.clients.grpc_client import GrpcStatus, GrpcStreamMessage, GrpcUnaryResult, GrpcHealthResult
from agctl.commands import grpc_commands
from agctl.errors import ConfigError

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "SCHEMA_REGISTRY_URL": "",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "p",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
    "TEST_GRPC_ADDR": "localhost:50051",
}


# ---------------------------------------------------------------------------
# Fake gRPC client (test double)
# ---------------------------------------------------------------------------


class _FakeMethodDescriptor:
    """Fake method descriptor for call type inspection."""

    def __init__(self, client_streaming: bool = False, server_streaming: bool = False):
        self.client_streaming = client_streaming
        self.server_streaming = server_streaming


class _FakeGrpcClient:
    """Fake GrpcClient that returns canned results without touching gRPC."""

    def __init__(self):
        self._call_unary_result = GrpcUnaryResult(
            target="localhost:50051",
            service="echo.Echo",
            method="Unary",
            call_type="unary",
            status=GrpcStatus(code=0, name="OK", message=""),
            message={"msg": "hi"},
            initial_metadata={},
            trailers={},
        )
        self._call_client_stream_result = GrpcUnaryResult(
            target="localhost:50051",
            service="echo.Echo",
            method="ClientStream",
            call_type="client_stream",
            status=GrpcStatus(code=0, name="OK", message=""),
            message={"result": "ok"},
            initial_metadata={},
            trailers={},
        )
        self._healthcheck_result = GrpcHealthResult(
            target="localhost:50051",
            address="localhost:50051",
            status="SERVING",
            note=None,
        )
        self.terminal_status: GrpcStatus = GrpcStatus(0, "OK", "")
        self.find_method_calls = []
        self.call_unary_calls = []
        self.call_client_stream_calls = []
        self.call_server_stream_calls = []
        self.call_bidi_calls = []
        self.healthcheck_calls = []

    def find_method(self, service: str, method: str) -> _FakeMethodDescriptor:
        """Return a fake method descriptor."""
        self.find_method_calls.append((service, method))
        # Default to unary; tests can override by setting the descriptor type
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=False)

    def call_type_of(self, method_desc) -> str:
        """Determine call type from fake descriptor."""
        if method_desc.client_streaming and method_desc.server_streaming:
            return "bidi"
        elif method_desc.server_streaming:
            return "server_stream"
        elif method_desc.client_streaming:
            return "client_stream"
        else:
            return "unary"

    def call_unary(
        self,
        service: str,
        method: str,
        message: dict,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ) -> GrpcUnaryResult:
        """Return canned unary result."""
        self.call_unary_calls.append(
            {"service": service, "method": method, "message": message, "metadata": metadata, "timeout": timeout}
        )
        return self._call_unary_result

    def call_client_stream(
        self,
        service: str,
        method: str,
        request_json_iter,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ) -> GrpcUnaryResult:
        """Return canned client-stream result."""
        messages = list(request_json_iter)
        self.call_client_stream_calls.append(
            {"service": service, "method": method, "messages": messages, "metadata": metadata, "timeout": timeout}
        )
        return self._call_client_stream_result

    def call_server_stream(
        self,
        service: str,
        method: str,
        message: dict,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ):
        """Yield canned server-stream messages (tests override)."""
        self.call_server_stream_calls.append(
            {"service": service, "method": method, "message": message, "metadata": metadata, "timeout": timeout}
        )
        # Reset terminal status to OK (default for normal completion)
        self.terminal_status = GrpcStatus(0, "OK", "")
        # Default: yield nothing (tests override by setting the yield list)
        return
        yield  # Make it a generator

    def call_bidi(
        self,
        service: str,
        method: str,
        request_json_iter,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ):
        """Yield canned bidi stream messages (tests override)."""
        messages = list(request_json_iter)
        self.call_bidi_calls.append(
            {"service": service, "method": method, "messages": messages, "metadata": metadata, "timeout": timeout}
        )
        # Reset terminal status to OK (default for normal completion)
        self.terminal_status = GrpcStatus(0, "OK", "")
        # Default: yield nothing (tests override by setting the yield list)
        return
        yield  # Make it a generator

    def healthcheck(self, service_name: str = ""):
        """Fake healthcheck - returns canned result."""
        self.healthcheck_calls.append({"service": service_name})
        return self._healthcheck_result


def install_fake(monkeypatch, fake: _FakeGrpcClient | None = None):
    """Install a fake gRPC client via monkeypatch."""
    if fake is None:
        fake = _FakeGrpcClient()
    monkeypatch.setattr(grpc_commands, "new_grpc_client", lambda target, descriptors=None: fake)
    return fake


# ---------------------------------------------------------------------------
# Template mode: unary call
# ---------------------------------------------------------------------------


def test_grpc_call_template_unary(monkeypatch):
    """Template mode: grpc call <template> resolves target/service/method and invokes unary call."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(cli, ["--config", str(FIXTURE), "grpc", "call", "echo-unary", "--param", "m=hi"])

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["ok"] is True
    assert out["command"] == "grpc.call"
    assert out["result"]["message"] == {"msg": "hi"}
    assert out["result"]["status"]["name"] == "OK"
    assert out["result"]["call_type"] == "unary"


# ---------------------------------------------------------------------------
# Freeform mode: --address
# ---------------------------------------------------------------------------


def test_grpc_call_freeform_address(monkeypatch):
    """Freeform mode: --address with --service/--method invokes unary call."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--address", "localhost:50051", "--service", "echo.Echo", "--method", "Unary", "--message", '{"msg":"x"}'],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    assert fake.call_unary_calls == [
        {
            "service": "echo.Echo",
            "method": "Unary",
            "message": {"msg": "x"},
            "metadata": None,
            "timeout": None,
        }
    ]


# ---------------------------------------------------------------------------
# Error cases: target/address resolution
# ---------------------------------------------------------------------------


def test_grpc_call_target_unknown(monkeypatch):
    """Unknown --target raises ConfigError."""
    install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(cli, ["--config", str(FIXTURE), "grpc", "call", "--target", "nope", "--service", "S", "--method", "M", "--message", "{}"])

    assert result.exit_code == 2
    assert "Unknown gRPC target: nope" in result.stdout or "Unknown gRPC target" in result.stdout


def test_grpc_call_address_target_mutex(monkeypatch):
    """--address and --target are mutually exclusive."""
    install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--address", "h:1", "--service", "S", "--method", "M", "--message", "{}"],
    )

    assert result.exit_code == 2
    assert "mutually exclusive" in result.stdout or "--address is mutually exclusive with --target" in result.stdout


def test_grpc_call_template_mutex(monkeypatch):
    """Template positional is mutually exclusive with --target/--address/--service/--method."""
    install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(cli, ["--config", str(FIXTURE), "grpc", "call", "echo-unary", "--service", "x"])

    assert result.exit_code == 2
    assert "mutually exclusive" in result.stdout


# ---------------------------------------------------------------------------
# Non-OK status is a result (D6), not an error
# ---------------------------------------------------------------------------


def test_grpc_call_nonok_status_is_result(monkeypatch):
    """Non-OK gRPC status is returned as a result, not an error."""
    fake = install_fake(monkeypatch)
    # Override to return non-OK status
    fake._call_unary_result = GrpcUnaryResult(
        target="localhost:50051",
        service="echo.Echo",
        method="Unary",
        call_type="unary",
        status=GrpcStatus(code=5, name="NOT_FOUND", message="not found"),
        message=None,
        initial_metadata={},
        trailers={},
    )
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--address", "localhost:50051", "--service", "echo.Echo", "--method", "Unary", "--message", "{}"],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["ok"] is True
    assert out["result"]["status"]["name"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------


def test_grpc_call_assertion_status_fail(monkeypatch):
    """Assertion --status NOT_FOUND fails when actual status is OK."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        [
            "--config",
            str(FIXTURE),
            "grpc",
            "call",
            "--address",
            "localhost:50051",
            "--service",
            "echo.Echo",
            "--method",
            "Unary",
            "--message",
            "{}",
            "--status",
            "NOT_FOUND",
        ],
    )

    assert result.exit_code == 1
    import json

    out = json.loads(result.stdout)
    assert out["ok"] is False
    assert out["error"]["type"] == "AssertionError"
    assert any(f["mode"] == "status" for f in out["error"]["detail"]["failures"])


def test_grpc_call_assertion_match_pass(monkeypatch):
    """Assertion --match '.status.name == "OK"' passes when status is OK."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        [
            "--config",
            str(FIXTURE),
            "grpc",
            "call",
            "--address",
            "localhost:50051",
            "--service",
            "echo.Echo",
            "--method",
            "Unary",
            "--message",
            "{}",
            '--match',
            '.status.name == "OK"',
        ],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"


# ---------------------------------------------------------------------------
# Client-streaming envelope (stdin NDJSON)
# ---------------------------------------------------------------------------


def test_grpc_call_client_stream_envelope(monkeypatch):
    """Client-streaming call reads NDJSON from stdin and returns unary result."""
    fake = install_fake(monkeypatch)
    # Override find_method to return client-streaming descriptor
    def fake_find_method(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=True, server_streaming=False)

    fake.find_method = fake_find_method
    runner = CliRunner(env=ENV)

    stdin_data = '{"msg":"a"}\n{"msg":"b"}\n'
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--service", "echo.Echo", "--method", "ClientStream"],
        input=stdin_data,
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["result"]["call_type"] == "client_stream"
    assert len(fake.call_client_stream_calls) == 1
    assert fake.call_client_stream_calls[0]["messages"] == [{"msg": "a"}, {"msg": "b"}]


# ---------------------------------------------------------------------------
# Bad request JSON raises ConfigError
# ---------------------------------------------------------------------------


def test_grpc_call_bad_request_json_is_configerror(monkeypatch):
    """Malformed --message raises ConfigError."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--address", "localhost:50051", "--service", "echo.Echo", "--method", "Unary", "--message", "not json"],
    )

    assert result.exit_code == 2
    import json

    out = json.loads(result.stdout)
    assert out["error"]["type"] == "ConfigError"


# ---------------------------------------------------------------------------
# Streaming tests (Task 10)
# ---------------------------------------------------------------------------


def test_grpc_call_server_stream_emits_ndjson_and_summary(monkeypatch):
    """Server-streaming emits NDJSON messages + summary line."""
    fake = install_fake(monkeypatch)
    # Override find_method to return server-streaming descriptor
    def fake_find_method_server_stream(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method_server_stream

    # Override call_server_stream to yield canned messages
    def fake_call_server_stream(
        service, method, message, *, metadata=None, timeout=None
    ):
        yield GrpcStreamMessage(message={"msg": "a"}, trailers=None)
        yield GrpcStreamMessage(message={"msg": "b"}, trailers={"grpc-status": "0"})

    fake.call_server_stream = fake_call_server_stream

    runner = CliRunner(env=ENV)
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--service", "echo.Echo", "--method", "ServerStream", "--message", '{"msg":"x"}'],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    lines = result.stdout.strip().split("\n")
    assert len(lines) == 3, f"Expected 3 lines (2 messages + 1 summary), got {len(lines)}"

    import json

    msg1 = json.loads(lines[0])
    msg2 = json.loads(lines[1])
    summary = json.loads(lines[2])

    assert msg1["event"] == "message"
    assert msg1["message"] == {"msg": "a"}
    assert msg1["trailers"] is None

    assert msg2["event"] == "message"
    assert msg2["message"] == {"msg": "b"}
    assert msg2["trailers"] == {"grpc-status": "0"}

    assert summary["summary"] is True
    assert summary["messages"] == 2
    assert summary["matched"] == 2


def test_grpc_call_bidi_reads_stdin_and_streams(monkeypatch):
    """Bidirectional streaming reads stdin NDJSON and streams responses."""
    fake = install_fake(monkeypatch)
    # Override find_method to return bidi descriptor
    def fake_find_method_bidi(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=True, server_streaming=True)

    fake.find_method = fake_find_method_bidi

    # Override call_bidi to echo messages back
    def fake_call_bidi(service, method, request_json_iter, *, metadata=None, timeout=None):
        requests = list(request_json_iter)
        # Record the call
        fake.call_bidi_calls.append(
            {"service": service, "method": method, "messages": requests, "metadata": metadata, "timeout": timeout}
        )
        for req in requests:
            # Echo back each request
            yield GrpcStreamMessage(message=req, trailers=None)
        yield GrpcStreamMessage(message={}, trailers={"grpc-status": "0"})

    fake.call_bidi = fake_call_bidi

    runner = CliRunner(env=ENV)
    stdin_data = '{"msg":"a"}\n{"msg":"b"}\n'
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--service", "echo.Echo", "--method", "Bidi"],
        input=stdin_data,
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    lines = result.stdout.strip().split("\n")
    assert len(lines) == 4  # 3 messages (2 echo + 1 final) + 1 summary

    import json

    msg1 = json.loads(lines[0])
    msg2 = json.loads(lines[1])
    msg3 = json.loads(lines[2])
    summary = json.loads(lines[3])

    assert msg1["event"] == "message"
    assert msg1["message"] == {"msg": "a"}

    assert msg2["event"] == "message"
    assert msg2["message"] == {"msg": "b"}

    assert msg3["event"] == "message"
    assert msg3["message"] == {}
    assert msg3["trailers"] == {"grpc-status": "0"}

    assert summary["summary"] is True
    assert summary["messages"] == 3
    assert summary["matched"] == 3

    # Verify the fake client received both stdin messages
    assert len(fake.call_bidi_calls) == 1
    assert fake.call_bidi_calls[0]["messages"] == [{"msg": "a"}, {"msg": "b"}]


def test_grpc_call_stream_match_filters_and_expect_count(monkeypatch):
    """Server-streaming with --match filters and --expect-count validates matched count."""
    fake = install_fake(monkeypatch)
    # Override find_method to return server-streaming descriptor
    def fake_find_method_server_stream(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method_server_stream

    # Override call_server_stream to yield messages with different patterns
    def fake_call_server_stream(
        service, method, message, *, metadata=None, timeout=None
    ):
        yield GrpcStreamMessage(message={"msg": "x", "count": 1}, trailers=None)
        yield GrpcStreamMessage(message={"msg": "y", "count": 2}, trailers=None)
        yield GrpcStreamMessage(message={"msg": "x", "count": 3}, trailers={"grpc-status": "0"})

    fake.call_server_stream = fake_call_server_stream

    runner = CliRunner(env=ENV)

    # Test 1: --expect-count 2 with --match '.msg == "x"' (2 matches) → exit 0
    result = runner.invoke(
        cli,
        [
            "--config", str(FIXTURE),
            "grpc", "call", "--target", "echo",
            "--service", "echo.Echo", "--method", "ServerStream",
            "--message", '{"msg":"x"}',
            '--match', '.msg == "x"',
            "--expect-count", "2",
        ],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    lines = result.stdout.strip().split("\n")
    import json

    summary = json.loads(lines[-1])
    assert summary["matched"] == 2
    assert summary["messages"] == 3

    # Test 2: --expect-count 3 with --match '.msg == "x"' (only 2 matches) → exit 1
    result = runner.invoke(
        cli,
        [
            "--config", str(FIXTURE),
            "grpc", "call", "--target", "echo",
            "--service", "echo.Echo", "--method", "ServerStream",
            "--message", '{"msg":"x"}',
            '--match', '.msg == "x"',
            "--expect-count", "3",
        ],
    )

    assert result.exit_code == 1, f"stdout: {result.stdout}"
    lines = result.stdout.strip().split("\n")
    summary = json.loads(lines[-1])
    assert summary["matched"] == 2
    assert summary["messages"] == 3


def test_grpc_call_stream_rejects_unary_assertion_flags(monkeypatch):
    """Unary assertion flags (--status/--contains/--jq-path/--equals) are rejected on streaming calls."""
    fake = install_fake(monkeypatch)
    # Override find_method to return server-streaming descriptor
    def fake_find_method_server_stream(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method_server_stream

    runner = CliRunner(env=ENV)

    # Test --status (unary assertion flag)
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--service", "echo.Echo", "--method", "ServerStream", "--message", '{}', "--status", "OK"],
    )

    assert result.exit_code == 2, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["error"]["type"] == "ConfigError"
    assert "unary assertion" in out["error"]["message"].lower() or "not allowed" in out["error"]["message"].lower() or "mutually exclusive" in out["error"]["message"].lower()


def test_grpc_call_stream_bad_match_startup_error(monkeypatch):
    """Malformed --match expression causes ConfigError startup error (no messages streamed)."""
    fake = install_fake(monkeypatch)
    # Override find_method to return server-streaming descriptor
    def fake_find_method_server_stream(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method_server_stream

    # Track if streaming was attempted (should not be)
    call_server_stream_called = []

    def fake_call_server_stream(service, method, message, *, metadata=None, timeout=None):
        call_server_stream_called.append(True)
        yield GrpcStreamMessage(message={"msg": "a"}, trailers=None)

    fake.call_server_stream = fake_call_server_stream

    runner = CliRunner(env=ENV)
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--service", "echo.Echo", "--method", "ServerStream", "--message", '{}', '--match', '.msg =='],
    )

    assert result.exit_code == 2, f"stdout: {result.stdout}"
    # Should emit exactly one error envelope line (no streaming messages)
    lines = result.stdout.strip().split("\n")
    assert len(lines) == 1, f"Expected 1 line (error envelope), got {len(lines)}: {result.stdout}"
    import json

    out = json.loads(lines[0])
    assert out["error"]["type"] == "ConfigError"
    # Verify streaming was never attempted
    assert len(call_server_stream_called) == 0


def test_grpc_call_stream_unknown_target_startup_error(monkeypatch):
    """Unknown --target causes ConfigError startup error (single envelope line, no streaming)."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "nope", "--service", "S", "--method", "M", "--message", '{}'],
    )

    assert result.exit_code == 2, f"stdout: {result.stdout}"
    # Should emit exactly one error envelope line
    lines = result.stdout.strip().split("\n")
    assert len(lines) == 1, f"Expected 1 line (error envelope), got {len(lines)}: {result.stdout}"
    import json

    out = json.loads(lines[0])
    assert out["error"]["type"] == "ConfigError"
    assert "unknown" in out["error"]["message"].lower()


def test_grpc_call_server_stream_summary_carries_nonok_status(monkeypatch):
    """Server-streaming summary carries non-OK terminal status."""
    fake = install_fake(monkeypatch)
    # Override find_method to return server-streaming descriptor
    def fake_find_method_server_stream(service: str, method: str) -> _FakeMethodDescriptor:
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method_server_stream

    # Override call_server_stream to yield 2 messages then set terminal_status to NOT_FOUND
    def fake_call_server_stream_nonok(
        service, method, message, *, metadata=None, timeout=None
    ):
        yield GrpcStreamMessage(message={"msg": "a"}, trailers=None)
        yield GrpcStreamMessage(message={"msg": "b"}, trailers=None)
        # Set non-OK terminal status
        fake.terminal_status = GrpcStatus(code=5, name="NOT_FOUND", message="missing")

    fake.call_server_stream = fake_call_server_stream_nonok

    runner = CliRunner(env=ENV)
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--target", "echo", "--service", "echo.Echo", "--method", "ServerStream", "--message", '{"msg":"x"}'],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    lines = result.stdout.strip().split("\n")
    assert len(lines) == 3, f"Expected 3 lines (2 messages + 1 summary), got {len(lines)}"

    import json

    msg1 = json.loads(lines[0])
    msg2 = json.loads(lines[1])
    summary = json.loads(lines[2])

    assert msg1["event"] == "message"
    assert msg1["message"] == {"msg": "a"}

    assert msg2["event"] == "message"
    assert msg2["message"] == {"msg": "b"}

    assert summary["summary"] is True
    assert summary["messages"] == 2
    assert summary["matched"] == 2
    # Verify the summary carries the non-OK terminal status
    assert summary["status"]["name"] == "NOT_FOUND"
    assert summary["status"]["code"] == 5
    assert summary["status"]["message"] == "missing"


# ---------------------------------------------------------------------------
# Healthcheck tests (Task 11)
# ---------------------------------------------------------------------------


def test_grpc_healthcheck_single_target(monkeypatch):
    """Single target healthcheck returns SERVING status with all_serving=True."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "healthcheck", "--target", "echo", "--service", ""],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["ok"] is True
    assert out["command"] == "grpc.healthcheck"
    assert out["result"]["targets"]["echo"]["status"] == "SERVING"
    assert out["result"]["targets"]["echo"]["address"] == "localhost:50051"
    assert "note" not in out["result"]["targets"]["echo"]
    assert out["result"]["all_serving"] is True


def test_grpc_healthcheck_all(monkeypatch):
    """--all flag checks all configured targets."""
    fake = install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "healthcheck", "--all"],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["ok"] is True
    assert out["command"] == "grpc.healthcheck"
    # Should have one entry for the "echo" target
    assert "echo" in out["result"]["targets"]
    assert out["result"]["targets"]["echo"]["status"] == "SERVING"
    assert out["result"]["all_serving"] is True


def test_grpc_healthcheck_unknown_is_unknown_not_error(monkeypatch):
    """UNKNOWN status (UNIMPLEMENTED health service) is not an error - returns all_serving=False."""
    fake = install_fake(monkeypatch)
    # Override healthcheck to return UNKNOWN status with note
    fake._healthcheck_result = GrpcHealthResult(
        target="localhost:50051",
        address="localhost:50051",
        status="UNKNOWN",
        note="health service UNIMPLEMENTED",
    )
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "healthcheck", "--target", "echo"],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["ok"] is True
    assert out["command"] == "grpc.healthcheck"
    assert out["result"]["targets"]["echo"]["status"] == "UNKNOWN"
    assert out["result"]["targets"]["echo"]["note"] == "health service UNIMPLEMENTED"
    assert out["result"]["all_serving"] is False


def test_grpc_healthcheck_unknown_target(monkeypatch):
    """Unknown --target raises ConfigError."""
    install_fake(monkeypatch)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "healthcheck", "--target", "nope"],
    )

    assert result.exit_code == 2, f"stdout: {result.stdout}"
    import json

    out = json.loads(result.stdout)
    assert out["error"]["type"] == "ConfigError"
    assert "Unknown gRPC target" in out["error"]["message"] or "unknown" in out["error"]["message"].lower()


# ---------------------------------------------------------------------------
# Finding 2b: Streaming error handling tests
# ---------------------------------------------------------------------------


def test_grpc_call_server_stream_midstream_error_carries_status(monkeypatch):
    """Server-streaming: mid-stream RpcError is captured into terminal_status and emitted as final NDJSON line."""
    import json

    # Create fake client that yields one message then sets terminal_status to INTERNAL
    fake = install_fake(monkeypatch)

    # Override find_method to return a server-streaming descriptor
    def fake_find_method(service, method):
        fake.find_method_calls.append((service, method))
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method

    # Override call_server_stream to yield one message then set error terminal status
    def fake_call_server_stream(service, method, message, *, metadata=None, timeout=None):
        fake.call_server_stream_calls.append({"service": service, "method": method, "message": message, "metadata": metadata, "timeout": timeout})
        # Yield one message, then set terminal status to INTERNAL error
        yield GrpcStreamMessage(message={"msg": "first"}, trailers=None)
        fake.terminal_status = GrpcStatus(
            code=grpc.StatusCode.INTERNAL.value[0],
            name="INTERNAL",
            message="stream error",
        )

    fake.call_server_stream = fake_call_server_stream

    runner = CliRunner(env=ENV)

    # Use --message with a JSON string (no spaces to avoid quoting issues)
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--address", "localhost:50051", "--service", "echo.Echo", "--method", "ServerStream", "--message", "{\"msg\":\"x\"}"],
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"

    # Parse NDJSON output
    lines = [json.loads(line) for line in result.stdout.strip().split("\n")]

    # First line should be the message
    assert lines[0]["event"] == "message"
    assert lines[0]["message"]["msg"] == "first"

    # Final line should be the summary with the INTERNAL status
    assert lines[-1]["summary"] is True
    assert lines[-1]["status"]["name"] == "INTERNAL"
    assert lines[-1]["status"]["message"] == "stream error"
    assert lines[-1]["messages"] == 1


def test_grpc_call_stream_deadline_emits_summary_not_traceback(monkeypatch):
    """Server-streaming: OperationTimeout is emitted as structured NDJSON summary line, not traceback."""
    import json
    from agctl.errors import OperationTimeout

    # Create fake client that raises OperationTimeout
    fake = install_fake(monkeypatch)

    # Override find_method to return a server-streaming descriptor
    def fake_find_method(service, method):
        fake.find_method_calls.append((service, method))
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method

    # Override call_server_stream to raise OperationTimeout
    def fake_call_server_stream(service, method, message, *, metadata=None, timeout=None):
        fake.call_server_stream_calls.append({"service": service, "method": method, "message": message, "metadata": metadata, "timeout": timeout})
        raise OperationTimeout(message="timeout", detail={})

    fake.call_server_stream = fake_call_server_stream

    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--address", "localhost:50051", "--service", "echo.Echo", "--method", "ServerStream", "--message", "{\"msg\":\"x\"}"],
    )

    assert result.exit_code == 1, f"stdout: {result.stdout}"

    # Parse NDJSON output - should be a single error summary line
    lines = [json.loads(line) for line in result.stdout.strip().split("\n")]

    # Should have a single line with error and summary
    assert len(lines) == 1
    assert lines[0]["summary"] is True
    assert lines[0]["error"]["type"] == "TimeoutError"
    assert "timeout" in lines[0]["error"]["message"]
    assert lines[0]["messages"] == 0
    assert lines[0]["matched"] == 0
    # Valid gRPC status code on the error-summary (0-16), never the invented -1 (M1)
    assert lines[0]["status"]["code"] != -1
    assert 0 <= lines[0]["status"]["code"] <= 16

    # Verify no Python traceback in output (structured JSON only)
    assert "Traceback" not in result.stdout
    assert "OperationTimeout" not in result.stdout or lines[0]["error"]["type"] == "OperationTimeout"


def test_grpc_call_bidi_internal_error_emits_summary(monkeypatch):
    """Bidirectional streaming: InternalError during setup emits structured NDJSON summary."""
    import json

    # Create fake client that raises a generic exception during bidi setup
    fake = install_fake(monkeypatch)

    # Override find_method to return a bidi descriptor
    def fake_find_method(service, method):
        fake.find_method_calls.append((service, method))
        return _FakeMethodDescriptor(client_streaming=True, server_streaming=True)

    fake.find_method = fake_find_method

    # Override call_bidi to raise a generic exception
    def fake_call_bidi(service, method, request_json_iter, *, metadata=None, timeout=None):
        messages = list(request_json_iter)  # Consume the iterator like the real implementation
        fake.call_bidi_calls.append({"service": service, "method": method, "messages": messages, "metadata": metadata, "timeout": timeout})
        raise RuntimeError("unexpected internal error")

    fake.call_bidi = fake_call_bidi

    runner = CliRunner(env=ENV)

    # Use --address to avoid template mode stdin complexity
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "grpc", "call", "--address", "localhost:50051", "--service", "echo.Echo", "--method", "Bidi"],
        input='{"msg":"a"}\n',
    )

    assert result.exit_code == 2, f"stdout: {result.stdout}"

    # Parse NDJSON output - should be a single error summary line
    lines = [json.loads(line) for line in result.stdout.strip().split("\n")]

    # Should have a single line with InternalError
    assert len(lines) == 1
    assert lines[0]["summary"] is True
    assert lines[0]["error"]["type"] == "InternalError"
    assert "unexpected internal error" in lines[0]["error"]["message"]

    # Verify no Python traceback in output (structured JSON only)
    assert "Traceback" not in result.stdout
    assert lines[0]["messages"] == 0

    # Verify no Python traceback in output (structured JSON only)
    assert "Traceback" not in result.stdout


# ---------------------------------------------------------------------------
# Review-fix tests: --status numeric (I1), single-resolution (I3),
# streaming partial-count error-summary (M1)
# ---------------------------------------------------------------------------


def test_grpc_call_status_numeric(monkeypatch):
    """--status accepts numeric codes from the CLI (I1).

    Click declares --status as type=str, so '5' arrives as a string. The
    validator coerces numeric strings to int before lookup:
      - --status 5 against a NOT_FOUND (code 5) result -> match, exit 0
      - --status 0 against an OK (code 0) result -> match, exit 0
      - --status 99 -> out of range -> ConfigError exit 2 (before the call)
    """
    import json

    runner = CliRunner(env=ENV)
    base = [
        "--config", str(FIXTURE),
        "grpc", "call", "--address", "localhost:50051",
        "--service", "echo.Echo", "--method", "Unary", "--message", "{}",
    ]

    # NOT_FOUND result -> --status 5 matches -> exit 0
    fake = install_fake(monkeypatch)
    fake._call_unary_result = GrpcUnaryResult(
        target="localhost:50051",
        service="echo.Echo",
        method="Unary",
        call_type="unary",
        status=GrpcStatus(code=5, name="NOT_FOUND", message=""),
        message=None,
        initial_metadata={},
        trailers={},
    )
    r = runner.invoke(cli, base + ["--status", "5"])
    assert r.exit_code == 0, f"stdout: {r.stdout}"

    # OK result (default fake, code 0) -> --status 0 matches -> exit 0
    install_fake(monkeypatch)
    r = runner.invoke(cli, base + ["--status", "0"])
    assert r.exit_code == 0, f"stdout: {r.stdout}"

    # --status 99 -> out of range -> ConfigError exit 2 (validation, before call)
    install_fake(monkeypatch)
    r = runner.invoke(cli, base + ["--status", "99"])
    assert r.exit_code == 2, f"stdout: {r.stdout}"
    out = json.loads(r.stdout)
    assert out["error"]["type"] == "ConfigError"


def test_grpc_call_single_resolution(monkeypatch):
    """One ``grpc call`` resolves target/client/method EXACTLY ONCE (I3).

    Previously resolution was duplicated: _detect_call_type and _grpc_call_core
    each independently loaded config, built the client, and called find_method
    (two reflection round-trips for ``reflection: auto`` targets). Now a single
    _resolve_grpc_call produces one client + one find_method per invocation.
    """
    fake = _FakeGrpcClient()
    build_calls = []

    def counting_factory(target, descriptors=None):
        build_calls.append(target)
        return fake

    monkeypatch.setattr(grpc_commands, "new_grpc_client", counting_factory)
    runner = CliRunner(env=ENV)

    result = runner.invoke(
        cli, ["--config", str(FIXTURE), "grpc", "call", "echo-unary", "--param", "m=hi"]
    )

    assert result.exit_code == 0, f"stdout: {result.stdout}"
    assert len(build_calls) == 1, (
        f"new_grpc_client called {len(build_calls)} times, expected 1"
    )
    assert len(fake.find_method_calls) == 1, (
        f"find_method called {len(fake.find_method_calls)} times, expected 1"
    )


def test_grpc_call_stream_midstream_error_summary_carries_partial_counts(monkeypatch):
    """Server-streaming: a mid-stream AgctlError error-summary carries the PARTIAL
    message/matched counts actually emitted (not 0) and a valid gRPC status code
    (no invented -1). (M1)"""
    import json
    from agctl.errors import OperationTimeout

    fake = install_fake(monkeypatch)

    def fake_find_method(service, method):
        fake.find_method_calls.append((service, method))
        return _FakeMethodDescriptor(client_streaming=False, server_streaming=True)

    fake.find_method = fake_find_method

    # Yield two messages, THEN raise OperationTimeout mid-stream
    def fake_call_server_stream(service, method, message, *, metadata=None, timeout=None):
        yield GrpcStreamMessage(message={"msg": "a"}, trailers=None)
        yield GrpcStreamMessage(message={"msg": "b"}, trailers=None)
        raise OperationTimeout(message="stream timed out", detail={})

    fake.call_server_stream = fake_call_server_stream

    runner = CliRunner(env=ENV)
    result = runner.invoke(
        cli,
        [
            "--config", str(FIXTURE),
            "grpc", "call", "--address", "localhost:50051",
            "--service", "echo.Echo", "--method", "ServerStream",
            "--message", "{\"msg\":\"x\"}",
        ],
    )

    # OperationTimeout -> exit 1
    assert result.exit_code == 1, f"stdout: {result.stdout}"

    lines = [json.loads(line) for line in result.stdout.strip().split("\n")]
    # 2 streamed message lines + 1 error-summary line
    assert len(lines) == 3, f"Expected 3 lines, got {len(lines)}: {result.stdout}"
    assert lines[0]["event"] == "message"
    assert lines[1]["event"] == "message"

    # Error-summary carries the PARTIAL counts emitted before the error (M1)
    summary = lines[2]
    assert summary["summary"] is True
    assert summary["error"]["type"] == "TimeoutError"
    assert summary["messages"] == 2, (
        f"Expected partial messages=2, got {summary['messages']}"
    )
    assert summary["matched"] == 2, (
        f"Expected partial matched=2, got {summary['matched']}"
    )
    # Valid gRPC status code (0-16), never the invented -1 (M1)
    assert summary["status"]["code"] != -1
    assert 0 <= summary["status"]["code"] <= 16
    assert "Traceback" not in result.stdout

