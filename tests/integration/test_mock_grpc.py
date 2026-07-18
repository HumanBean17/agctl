"""End-to-end integration tests for the gRPC mock server (Task 12).

Spins a REAL in-process grpcio mock server via :class:`MockEngine`
(``run_grpc=True``; the default ``grpc_server_factory`` lazy-imports the real
:class:`MockGrpcServer`) and drives all four call types + Health + Reflection
against it through the ``agctl grpc call`` / ``agctl grpc healthcheck`` CLI
path (Click's :class:`CliRunner` invokes the real CLI in-process, which builds
a real :class:`GrpcClient` + real grpcio channel).

Self-contained — no Docker, no external broker, no ``AGCTL_TEST_LIVE``. The
gRPC mock is an in-process ``grpc.Server`` + client, so these tests are gated
ONLY on the gRPC extra (``grpc`` / ``grpc_tools`` / ``grpcio_health_checking``
/ ``grpcio_reflection``) via module-level :func:`pytest.importorskip`. The
existing ``tests/integration/conftest.py`` does NOT force-skip the integration
suite (``_live_services`` is a no-op when ``AGCTL_TEST_LIVE`` is unset, and no
test here uses a ``require_*`` fixture), so these tests run by default whenever
the grpc extra is installed.

The mock's descriptor pool is built from ``tests/fixtures/mock_grpc/echo.proto``
(Task 1): ``package echo; service EchoService { Unary / ServerStream /
ClientStream / Bidi }``, messages ``EchoRequest{msg,n}`` / ``EchoResponse{msg}``.

Approach: in-process engine (faster + more reliable than subprocess timing).
Exercises the real gRPC server + client + the engine lifecycle (start/run/
shutdown) + the dispatch core end-to-end. ``MockEngine.run()`` runs on a
background thread; ``engine.shutdown()`` emits the summary line whose
``grpc_hits`` / ``grpc_unmatched`` / ``grpc_errors`` counters the dedicated
summary test asserts on.
"""

from __future__ import annotations

import json
import pathlib
import threading

import pytest

# Self-skip guard: gate ONLY on the grpc extra. No AGCTL_TEST_LIVE / Docker.
# Module-level importorskip means a missing extra skips the WHOLE file cleanly
# (collection does not error); the tests below never execute without these.
pytest.importorskip("grpc")
pytest.importorskip("grpc_tools")
pytest.importorskip("grpc_health")
pytest.importorskip("grpc_reflection")

from click.testing import CliRunner

from agctl.cli import cli
from agctl.config.models import (
    CaptureSpec,
    GrpcDescriptorSource,
    GrpcMatch,
    GrpcMockConfig,
    GrpcResponse,
    GrpcResponseMessage,
    GrpcStub,
    MocksConfig,
)
from agctl.mock.engine import MockEngine

SERVICE = "echo.EchoService"

# Path to the echo.proto fixture (Task 1). Declared as a module constant
# because the session-scoped ``mock_grpc_echo_proto_path`` fixture lives in
# ``tests/unit/conftest.py`` and is not visible to the integration suite.
ECHO_PROTO_PATH = (
    pathlib.Path(__file__).parent.parent / "fixtures" / "mock_grpc" / "echo.proto"
)


# ---------------------------------------------------------------------------
# Stubs / engine / config helpers
# ---------------------------------------------------------------------------


def _default_stubs() -> dict:
    """Return insertion-ordered stubs covering all 4 call types + NOT_FOUND.

    Insertion order matters (first-match-wins): ``not-found`` (match
    ``msg=="fail"``) is first so its predicate gates the NOT_FOUND status;
    ``echo-unary`` (no match) is the catch-all for every other Unary request.
    """
    return {
        "not-found": GrpcStub(
            service=SERVICE,
            method="Unary",
            match=GrpcMatch(body={"msg": "fail"}),
            response=GrpcResponse(status="NOT_FOUND", message={}),
        ),
        "echo-unary": GrpcStub(
            service=SERVICE,
            method="Unary",
            capture={"msg": CaptureSpec(from_=".message.msg")},
            response=GrpcResponse(message={"msg": "echo-{msg}"}),
        ),
        "echo-server-stream": GrpcStub(
            service=SERVICE,
            method="ServerStream",
            capture={"msg": CaptureSpec(from_=".message.msg")},
            response=GrpcResponse(
                messages=[
                    GrpcResponseMessage(message={"msg": "{msg}-0"}),
                    GrpcResponseMessage(message={"msg": "{msg}-1"}),
                    GrpcResponseMessage(message={"msg": "{msg}-2"}),
                ]
            ),
        ),
        "echo-client-stream": GrpcStub(
            service=SERVICE,
            method="ClientStream",
            # ``.count`` lives on the client_stream envelope (built at request
            # stream close); capturing it lets the aggregated response template
            # on the number of requests received.
            capture={"count": CaptureSpec(from_=".count")},
            response=GrpcResponse(message={"msg": "aggregated-{count}"}),
        ),
        "echo-bidi": GrpcStub(
            service=SERVICE,
            method="Bidi",
            capture={"msg": CaptureSpec(from_=".message.msg")},
            response=GrpcResponse(message={"msg": "echo-{msg}"}),
        ),
    }


def _start_engine(stubs: dict, proto_path: str, capture_fn) -> MockEngine:
    """Build + start an in-process MockEngine (``run_grpc=True``, ephemeral port).

    The default ``grpc_server_factory`` (None) lazy-imports the real
    :class:`MockGrpcServer`. After ``start()`` returns the underlying
    ``grpc.Server`` is bound (OS-assigned port reported by ``actual_listen()``)
    and already handling requests; ``run()`` on a background thread exercises
    the full lifecycle (serve thread + duration loop) but is not required for
    traffic to flow.
    """
    config = MocksConfig(
        grpc=GrpcMockConfig(
            listen="127.0.0.1:0",
            descriptors=[GrpcDescriptorSource(proto=proto_path)],
            stubs=stubs,
        )
    )
    engine = MockEngine(
        mocks=config,
        run_http=False,
        run_kafka=False,
        run_grpc=True,
        http_listen="127.0.0.1:0",
        grpc_listen="127.0.0.1:0",
        emit_fn=capture_fn,
    )
    engine.start()
    return engine


def _run_engine_on_thread(engine: MockEngine) -> threading.Thread:
    """Start ``engine.run()`` on a daemon thread; stash it on ``engine._run_thread``."""
    thread = threading.Thread(target=engine.run, daemon=True)
    thread.start()
    engine._run_thread = thread
    return thread


def _stop_engine(engine: MockEngine) -> None:
    """Shutdown the engine + join the run thread (set by ``_run_engine_on_thread``)."""
    engine.shutdown()
    run_thread = getattr(engine, "_run_thread", None)
    if run_thread is not None:
        run_thread.join(timeout=5)


def _write_config(
    tmp_path,
    address: str,
    proto_path: str,
    *,
    reflection: str = "auto",
    descriptors: bool = True,
    name: str = "agctl.yaml",
) -> str:
    """Write an ``agctl.yaml`` pointing ``grpc.targets.mock`` at ``address``.

    Block-style YAML (no flow collections) so the templates map parses
    unambiguously. ``descriptors=False`` produces a reflection-only config
    (no ``grpc.descriptors`` block) used by the reflection test.
    """
    lines = [
        'version: "3"',
        "grpc:",
        "  targets:",
        "    mock:",
        f"      address: {address}",
        "      use_tls: false",
        f'      reflection: "{reflection}"',
        "  templates:",
        "    unary:",
        "      target: mock",
        f"      service: {SERVICE}",
        "      method: Unary",
        "    server-stream:",
        "      target: mock",
        f"      service: {SERVICE}",
        "      method: ServerStream",
        "    client-stream:",
        "      target: mock",
        f"      service: {SERVICE}",
        "      method: ClientStream",
        "    bidi:",
        "      target: mock",
        f"      service: {SERVICE}",
        "      method: Bidi",
    ]
    if descriptors:
        lines.extend(["  descriptors:", f"    - proto: {proto_path}"])
    config_path = tmp_path / name
    config_path.write_text("\n".join(lines) + "\n")
    return str(config_path)


def _capture_appender() -> tuple[list, object]:
    """Return ``(captured_list, append_copy_fn)`` for engine event capture."""
    captured: list = []

    def append_copy(line: dict) -> None:
        captured.append(line.copy())

    return captured, append_copy


# ---------------------------------------------------------------------------
# Fixture: default in-process mock (all 4 call types + NOT_FOUND)
# ---------------------------------------------------------------------------


@pytest.fixture
def grpc_mock(tmp_path):
    """Start the in-process gRPC mock engine with the default stubs set.

    Yields ``(address, captured_events, engine, config_path)``. The default
    config uses ``reflection: auto`` + client-side descriptors so calls resolve
    via descriptors (fast path; reflection is exercised by the dedicated test).
    ``MockEngine.run()`` runs on a background daemon thread to exercise the
    full lifecycle (serve thread + duration loop + signal handlers).

    Teardown: ``shutdown()`` (stops server, emits summary) + join run thread.
    """
    captured, append_copy = _capture_appender()
    engine = _start_engine(_default_stubs(), str(ECHO_PROTO_PATH), append_copy)
    address = engine._grpc_server.actual_listen()
    config_path = _write_config(tmp_path, address, str(ECHO_PROTO_PATH))
    _run_engine_on_thread(engine)

    try:
        yield address, captured, engine, config_path
    finally:
        _stop_engine(engine)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMockGrpcEndToEnd:
    """Drive the in-process gRPC mock via the ``agctl grpc`` CLI path.

    Every test uses :class:`CliRunner` to invoke the real ``agctl`` CLI
    in-process, so the envelope load, config parse, :class:`GrpcClient`
    construction, real grpcio channel, and mock dispatch are all exercised
    end-to-end.
    """

    def test_unary_stub_match_returns_templated_response(self, grpc_mock):
        """Unary stub with ``{msg}`` capture → response message is templated."""
        _, _, _, config_path = grpc_mock
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--config", config_path,
                "grpc", "call", "unary",
                "--message", '{"msg":"hi"}',
            ],
        )
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["result"]["message"]["msg"] == "echo-hi"
        assert envelope["result"]["status"]["name"] == "OK"

    def test_unary_not_found_stub_returns_not_found_status(self, grpc_mock):
        """``status: NOT_FOUND`` stub → call returns NOT_FOUND as a result
        (``ok: true`` — non-OK is a result field, not a call failure, per D6)."""
        _, _, _, config_path = grpc_mock
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--config", config_path,
                "grpc", "call", "unary",
                "--message", '{"msg":"fail"}',
            ],
        )
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["result"]["status"]["name"] == "NOT_FOUND"
        assert envelope["result"]["message"] is None

    def test_server_stream_returns_n_messages_in_order(self, grpc_mock):
        """Server-stream stub → 3 messages in authored order + 1 summary line."""
        _, _, _, config_path = grpc_mock
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--config", config_path,
                "grpc", "call", "server-stream",
                "--message", '{"msg":"x"}',
            ],
        )
        assert result.exit_code == 0, result.output
        lines = [l for l in result.output.strip().split("\n") if l]
        events = [json.loads(l) for l in lines]
        message_events = [e for e in events if e.get("event") == "message"]
        assert len(message_events) == 3, (
            f"expected 3 stream messages, got {len(message_events)}: {events}"
        )
        assert [e["message"]["msg"] for e in message_events] == ["x-0", "x-1", "x-2"]
        # Summary line present (server-stream emits NDJSON + a final summary).
        summaries = [e for e in events if e.get("summary") is True]
        assert len(summaries) == 1

    def test_client_stream_returns_aggregated_response(self, grpc_mock):
        """Client-stream stub → one aggregated response templated on ``count``."""
        _, _, _, config_path = grpc_mock
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config", config_path, "grpc", "call", "client-stream"],
            input='{"msg":"a"}\n{"msg":"b"}\n',
        )
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["result"]["message"]["msg"] == "aggregated-2"
        assert envelope["result"]["status"]["name"] == "OK"

    def test_bidi_returns_one_response_per_request(self, grpc_mock):
        """Bidi stub → one response per request, in order."""
        _, _, _, config_path = grpc_mock
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config", config_path, "grpc", "call", "bidi"],
            input='{"msg":"r1"}\n{"msg":"r2"}\n',
        )
        assert result.exit_code == 0, result.output
        lines = [l for l in result.output.strip().split("\n") if l]
        events = [json.loads(l) for l in lines]
        message_events = [e for e in events if e.get("event") == "message"]
        assert len(message_events) == 2, (
            f"expected 2 bidi responses, got {len(message_events)}: {events}"
        )
        assert [e["message"]["msg"] for e in message_events] == ["echo-r1", "echo-r2"]

    def test_healthcheck_returns_serving(self, grpc_mock):
        """``grpc healthcheck --target mock`` → SERVING (Health servicer auto-served)."""
        _, _, _, config_path = grpc_mock
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config", config_path, "grpc", "healthcheck", "--target", "mock"],
        )
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["result"]["targets"]["mock"]["status"] == "SERVING"

    def test_reflection_resolves_method_with_no_client_descriptors(
        self, grpc_mock, tmp_path
    ):
        """A reflection-only target (no client-side descriptors, ``reflection:
        on``) resolves the method via the mock's reflection service and returns
        the templated response — proving the client's reflection path works
        end-to-end against the mock's :func:`reflection.enable_server_reflection`.
        """
        address, _, _, _ = grpc_mock
        refl_config = _write_config(
            tmp_path,
            address,
            str(ECHO_PROTO_PATH),
            reflection="on",
            descriptors=False,
            name="reflection.yaml",
        )
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--config", refl_config,
                "grpc", "call", "unary",
                "--message", '{"msg":"reflect-hi"}',
            ],
        )
        assert result.exit_code == 0, result.output
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["result"]["message"]["msg"] == "echo-reflect-hi"
        assert envelope["result"]["status"]["name"] == "OK"

    def test_unmatched_method_returns_unimplemented(self, tmp_path):
        """A stub whose predicate does not match the request → dispatch returns
        unmatched → the handler aborts UNIMPLEMENTED and emits a
        ``grpc.unmatched`` event (the summary's ``grpc_unmatched`` counter is
        asserted in the dedicated summary test).

        Uses a dedicated engine with one body-match stub so the call to a
        non-matching message deterministically misses.
        """
        captured, append_copy = _capture_appender()
        stubs = {
            "picky": GrpcStub(
                service=SERVICE,
                method="Unary",
                match=GrpcMatch(body={"msg": "match-me"}),
                response=GrpcResponse(message={"msg": "matched"}),
            ),
        }
        engine = _start_engine(stubs, str(ECHO_PROTO_PATH), append_copy)
        address = engine._grpc_server.actual_listen()
        config_path = _write_config(tmp_path, address, str(ECHO_PROTO_PATH))
        _run_engine_on_thread(engine)

        try:
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "--config", config_path,
                    "grpc", "call", "unary",
                    "--message", '{"msg":"no-match"}',
                ],
            )
            assert result.exit_code == 0, result.output
            envelope = json.loads(result.output)
            assert envelope["ok"] is True
            assert envelope["result"]["status"]["name"] == "UNIMPLEMENTED"
            assert envelope["result"]["message"] is None
            # The mock emitted a grpc.unmatched event (dispatch-level miss).
            assert any(e.get("event") == "grpc.unmatched" for e in captured), (
                f"expected a grpc.unmatched event; captured={captured}"
            )
        finally:
            _stop_engine(engine)

    def test_summary_line_carries_grpc_counters(self, tmp_path):
        """Engine ``shutdown()`` emits a summary line carrying
        ``grpc_hits`` / ``grpc_unmatched`` / ``grpc_errors``. Drives a known
        mix of calls (1 hit + 1 unmatched) against a dedicated engine so the
        counters are deterministic.
        """
        captured, append_copy = _capture_appender()
        stubs = {
            "picky": GrpcStub(
                service=SERVICE,
                method="Unary",
                match=GrpcMatch(body={"msg": "match-me"}),
                response=GrpcResponse(message={"msg": "matched"}),
            ),
        }
        engine = _start_engine(stubs, str(ECHO_PROTO_PATH), append_copy)
        address = engine._grpc_server.actual_listen()
        config_path = _write_config(tmp_path, address, str(ECHO_PROTO_PATH))
        _run_engine_on_thread(engine)

        try:
            runner = CliRunner()
            # 1 matching call → 1 grpc.hit.
            r_hit = runner.invoke(
                cli,
                [
                    "--config", config_path,
                    "grpc", "call", "unary",
                    "--message", '{"msg":"match-me"}',
                ],
            )
            assert r_hit.exit_code == 0, r_hit.output
            # 1 non-matching call → 1 grpc.unmatched (handler aborts UNIMPLEMENTED).
            r_miss = runner.invoke(
                cli,
                [
                    "--config", config_path,
                    "grpc", "call", "unary",
                    "--message", '{"msg":"other"}',
                ],
            )
            assert r_miss.exit_code == 0, r_miss.output
        finally:
            _stop_engine(engine)

        summaries = [e for e in captured if e.get("event") == "summary"]
        assert len(summaries) == 1, (
            f"expected exactly 1 summary event, got {len(summaries)}: {summaries}"
        )
        summary = summaries[0]
        assert summary["grpc_hits"] == 1, (
            f"expected grpc_hits==1, got {summary['grpc_hits']}; full={summary}"
        )
        assert summary["grpc_unmatched"] == 1, (
            f"expected grpc_unmatched==1, got {summary['grpc_unmatched']}; full={summary}"
        )
        assert summary["grpc_errors"] == 0, (
            f"expected grpc_errors==0, got {summary['grpc_errors']}; full={summary}"
        )
