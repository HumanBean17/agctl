"""Unit tests for `grpc call` inline template-variable generators + memo (Task 8).

Threads the per-invocation ``memo`` + ``template_vars_enabled`` flag through the
gRPC commands so ``{{uuid}}``/``{{ts}}``/``{{rand}}`` tokens in ``--message`` and
each ``--metadata`` value (and the template message/metadata via
``fill_placeholders``) resolve to generated values, with the SAME token resolving
to the SAME value within one invocation (shared memo). ``--no-template-vars``
leaves tokens literal; an unknown generator surfaces as ``ConfigError`` (exit 2)
before any RPC.

The recording seam is the existing ``new_grpc_client`` DI (the same one used by
``test_grpc_commands.py``): the fake client records every ``call_unary``
``(service, method, message, metadata, timeout)`` so tests assert on the
post-substitution serialized request without a real channel.
"""

from __future__ import annotations

import json
import uuid as uuidlib
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.clients.grpc_client import GrpcStatus, GrpcUnaryResult
from agctl.commands import grpc_commands

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
# Fake gRPC client (minimal unary recording double — mirrors test_grpc_commands)
# ---------------------------------------------------------------------------


class _FakeMethodDescriptor:
    def __init__(self, client_streaming=False, server_streaming=False):
        self.client_streaming = client_streaming
        self.server_streaming = server_streaming


class _RecordingGrpcClient:
    """Fake GrpcClient that records the unary request (message + metadata)."""

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
        self.call_unary_calls: list[dict] = []

    def find_method(self, service, method):
        return _FakeMethodDescriptor()

    def call_type_of(self, method_desc):
        return "unary"

    def call_unary(self, service, method, message, *, metadata=None, timeout=None):
        self.call_unary_calls.append(
            {
                "service": service,
                "method": method,
                "message": message,
                "metadata": metadata,
                "timeout": timeout,
            }
        )
        return self._call_unary_result


@pytest.fixture
def install_fake(monkeypatch):
    """Install a recording fake gRPC client; return the fake for assertions."""

    def _install():
        fake = _RecordingGrpcClient()
        monkeypatch.setattr(
            grpc_commands, "new_grpc_client", lambda target, descriptors=None: fake
        )
        return fake

    return _install


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


# ---------------------------------------------------------------------------
# grpc call (free-form): inline generators + shared memo
# ---------------------------------------------------------------------------


def test_grpc_call_message_and_metadata_share_uuid(install_fake):
    """``--message '{"id":"{{uuid}}","ref":"{{uuid}}"}'`` and
    ``--metadata trace={{uuid}}`` resolve to ONE uuid within one invocation
    (shared memo): ``id`` == ``ref`` == metadata ``trace``, all a valid UUID."""
    fake = install_fake()
    result = _run(
        [
            "--config", str(FIXTURE),
            "grpc", "call",
            "--address", "localhost:50051",
            "--service", "echo.Echo", "--method", "Unary",
            "--message", '{"id":"{{uuid}}","ref":"{{uuid}}"}',
            "--metadata", "trace={{uuid}}",
        ]
    )
    assert result.exit_code == 0, result.output

    call = fake.call_unary_calls[0]
    msg = call["message"]
    md = call["metadata"]

    # Same memo -> identical value for the repeated {{uuid}} token across
    # message body AND metadata.
    assert msg["id"] == msg["ref"] == md["trace"]
    # The shared value is a valid RFC-4122 UUID.
    uuidlib.UUID(msg["id"])
    uuidlib.UUID(md["trace"])


def test_grpc_call_no_template_vars_leaves_tokens_literal(install_fake):
    """``--no-template-vars`` disables substitution end-to-end: the recorded
    message and metadata carry the LITERAL token text, and no error is raised."""
    fake = install_fake()
    result = _run(
        [
            "--config", str(FIXTURE),
            "--no-template-vars",
            "grpc", "call",
            "--address", "localhost:50051",
            "--service", "echo.Echo", "--method", "Unary",
            "--message", '{"id":"{{uuid}}"}',
            "--metadata", "trace={{uuid}}",
        ]
    )
    assert result.exit_code == 0, result.output

    call = fake.call_unary_calls[0]
    assert call["message"]["id"] == "{{uuid}}"
    assert call["metadata"]["trace"] == "{{uuid}}"


def test_grpc_call_unknown_generator_is_config_error_before_rpc(install_fake):
    """``--message '{{nope}}'`` (unknown generator) surfaces as ``ConfigError``
    (exit 2) BEFORE any RPC — the recording fake recorded nothing / was not
    called."""
    fake = install_fake()
    result = _run(
        [
            "--config", str(FIXTURE),
            "grpc", "call",
            "--address", "localhost:50051",
            "--service", "echo.Echo", "--method", "Unary",
            "--message", "{{nope}}",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # The fake client was never called (no RPC fired).
    assert fake.call_unary_calls == []


# ---------------------------------------------------------------------------
# grpc call <template>: memo threaded into template message/metadata fills
# ---------------------------------------------------------------------------


def test_grpc_call_template_shares_uuid_across_message_and_param(install_fake):
    """Template mode: a ``{{uuid}}`` in the caller ``--message`` override and a
    ``{{uuid}}`` in a ``--metadata`` value resolve to ONE value (shared memo
    threads through the template fill path too)."""
    fake = install_fake()
    result = _run(
        [
            "--config", str(FIXTURE),
            "grpc", "call", "echo-unary",
            "--metadata", "trace={{uuid}}",
            "--message", '{"msg":"x","ref":"{{uuid}}"}',
        ]
    )
    assert result.exit_code == 0, result.output

    call = fake.call_unary_calls[0]
    msg = call["message"]
    md = call["metadata"]
    # Caller override ``ref`` and metadata ``trace`` share the memo; the
    # template ``{m}`` param is unfilled (no --param) and left literal.
    assert msg["ref"] == md["trace"]
    uuidlib.UUID(msg["ref"])
