"""Tests for ``MockGrpcServer`` construction, validation, and method table
(Task 6 of the gRPC mock server).

These tests exercise the construction layer ONLY:

- Resolves the descriptor pool ONCE (via the shared kernel or an injected pool).
- Validates every stub's service/method against the pool.
- Validates response-shape-vs-call-type (server_stream -> ``messages``;
  unary/client_stream/bidi -> ``message``).
- Precomputes ``stubs_by_method`` (dict-of-dicts, insertion-ordered) and
  ``method_meta`` (per-method input/output descriptors + call type).

All gRPC-runtime wiring (generic servicer, Health, Reflection, lifecycle) is
Task 7 — these tests must NOT spawn a real grpc server. They build a real
``DescriptorPool`` from ``tests/fixtures/mock_grpc/echo.proto`` (via the
session-scoped ``mock_grpc_echo_pool`` fixture) and pass it as the injected
``descriptor_pool`` DI seam, so construction is exercised without binding.

The module is gated on the gRPC extra (``grpc_tools`` / ``google.protobuf``):
``pytest.importorskip`` at module top so a missing extra skips cleanly rather
than erroring at collection.
"""

from __future__ import annotations

# Gate the whole module on the gRPC extra: the pool fixture compiles echo.proto
# via grpc_tools.protoc + google.protobuf, so a missing extra must skip, not
# error. Both are required by the kernel (`build_descriptor_pool` /
# `find_method` / `call_type_of`) which the constructor calls.
import pytest

pytest.importorskip("grpc_tools")
pytest.importorskip("google.protobuf")

from agctl.config.models import (
    GrpcDescriptorSource,
    GrpcMockConfig,
    GrpcResponse,
    GrpcResponseMessage,
    GrpcStub,
)
from agctl.errors import ConfigError
from agctl.mock.grpc_server import MockGrpcServer


SERVICE = "echo.EchoService"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _no_op_emit(event: dict) -> None:
    """No-op event emitter for tests (Task 6 doesn't yet drive events)."""
    return None


def _unary_stub(
    *,
    service: str = SERVICE,
    method: str = "Unary",
    message: dict | None = None,
) -> GrpcStub:
    """Unary-style stub (``response.message`` set)."""
    return GrpcStub(
        service=service,
        method=method,
        response=GrpcResponse(message=message if message is not None else {}),
    )


def _stream_stub(
    *,
    service: str = SERVICE,
    method: str = "ServerStream",
    messages: list[GrpcResponseMessage] | None = None,
) -> GrpcStub:
    """Server-stream-style stub (``response.messages`` set)."""
    return GrpcStub(
        service=service,
        method=method,
        response=GrpcResponse(
            messages=messages
            if messages is not None
            else [GrpcResponseMessage(message={"chunk": 1})]
        ),
    )


def _build(
    stubs: dict[str, GrpcStub],
    pool,
    *,
    descriptors: list[GrpcDescriptorSource] | None = None,
    top_level_descriptors: list[GrpcDescriptorSource] | None = None,
    listen: str = "0.0.0.0:50051",
) -> MockGrpcServer:
    """Construct a ``MockGrpcServer`` against ``pool`` (DI seam)."""
    config = GrpcMockConfig(
        listen=listen,
        descriptors=descriptors,
        stubs=stubs,
    )
    return MockGrpcServer(
        config,
        top_level_descriptors=top_level_descriptors,
        emit_event=_no_op_emit,
        descriptor_pool=pool,
    )


# ---------------------------------------------------------------------------
# Step 1(a): successful unary construction
# ---------------------------------------------------------------------------


class TestConstructionSuccess:
    """A valid unary stub constructs cleanly and populates the tables."""

    def test_a_unary_stub_constructs_and_populates_tables(self, mock_grpc_echo_pool):
        """(a) unary stub for echo.EchoService/Unary + response.message:

        - server constructs without raising
        - services == ["echo.EchoService"]
        - stubs_by_method[("echo.EchoService","Unary")] has exactly 1 entry,
          keyed by the config stub name
        - method_meta[...][2] == "unary"
        """
        server = _build({"echo_unary": _unary_stub()}, mock_grpc_echo_pool)

        assert server.services == ["echo.EchoService"]

        key = ("echo.EchoService", "Unary")
        assert key in server.stubs_by_method
        # Inner dict is name -> stub; exactly one entry, keyed by config name.
        inner = server.stubs_by_method[key]
        assert list(inner.keys()) == ["echo_unary"]
        assert isinstance(inner["echo_unary"], GrpcStub)

        # method_meta carries input_desc, output_desc, call_type at indices 0/1/2
        assert key in server.method_meta
        input_desc, output_desc, call_type = server.method_meta[key]
        assert call_type == "unary"
        # Input is EchoRequest, output is EchoResponse (sanity via full_name).
        assert input_desc.name == "EchoRequest"
        assert output_desc.name == "EchoResponse"

    def test_server_initial_state_is_unbound(self, mock_grpc_echo_pool):
        """Construction does NOT start a grpc server: _server is None until Task 7."""
        server = _build({"s": _unary_stub()}, mock_grpc_echo_pool)
        assert server._server is None

    def test_listen_and_bind_addresses(self, mock_grpc_echo_pool):
        """listen_address -> 'host:port' string; bind_address -> (host, port)."""
        server = _build({"s": _unary_stub()}, mock_grpc_echo_pool, listen="127.0.0.1:50071")
        assert server.listen_address == "127.0.0.1:50071"
        assert server.bind_address == ("127.0.0.1", 50071)


# ---------------------------------------------------------------------------
# Step 1(b)/(c): unresolved service / method -> ConfigError
# ---------------------------------------------------------------------------


class TestUnresolvedServiceOrMethod:
    """Missing service or method surfaces as ConfigError at mocks.grpc.stubs.<name>."""

    def test_b_unknown_service_raises_config_error_naming_stub_path(
        self, mock_grpc_echo_pool
    ):
        """(b) service='echo.Missing' -> ConfigError mentioning 'mocks.grpc.stubs.echo_missing'."""
        with pytest.raises(ConfigError) as exc_info:
            _build({"echo_missing": _unary_stub(service="echo.Missing")}, mock_grpc_echo_pool)
        msg = str(exc_info.value)
        assert "mocks.grpc.stubs.echo_missing" in msg

    def test_c_unknown_method_raises_config_error_naming_stub_path(
        self, mock_grpc_echo_pool
    ):
        """(c) method='Missing' -> ConfigError mentioning 'mocks.grpc.stubs.<name>'."""
        with pytest.raises(ConfigError) as exc_info:
            _build({"bad_method": _unary_stub(method="Missing")}, mock_grpc_echo_pool)
        msg = str(exc_info.value)
        assert "mocks.grpc.stubs.bad_method" in msg

    def test_unknown_service_error_exits_two(self, mock_grpc_echo_pool):
        """ConfigError exit code is 2 (config-level, not runtime)."""
        with pytest.raises(ConfigError) as exc_info:
            _build({"x": _unary_stub(service="echo.Missing")}, mock_grpc_echo_pool)
        assert exc_info.value.exit_code == 2

    def test_validation_fails_before_any_bind(self, mock_grpc_echo_pool):
        """A ConfigError in construction must leave _server unset (no bind attempt)."""
        # The failed constructor never returns an instance, so this is structural:
        # the only way _server could be set is if binding happened first. Since
        # construction raises before binding is even conceivable (Task 7), we
        # simply assert the error path.
        with pytest.raises(ConfigError):
            _build({"x": _unary_stub(service="echo.Missing")}, mock_grpc_echo_pool)


# ---------------------------------------------------------------------------
# Step 1(d)/(e)/(f): response-shape vs call-type
# ---------------------------------------------------------------------------


class TestResponseShapeVsCallType:
    """Response shape (message vs messages) must match the derived call type."""

    def test_d_unary_stub_with_messages_raises_naming_unary(self, mock_grpc_echo_pool):
        """(d) unary stub using response.messages -> ConfigError at ...response
        mentioning 'unary'."""
        bad = GrpcStub(
            service=SERVICE,
            method="Unary",
            response=GrpcResponse(
                messages=[GrpcResponseMessage(message={"chunk": 1})]
            ),
        )
        with pytest.raises(ConfigError) as exc_info:
            _build({"unary_with_stream": bad}, mock_grpc_echo_pool)
        msg = str(exc_info.value)
        assert "unary" in msg
        assert "mocks.grpc.stubs.unary_with_stream.response" in msg

    def test_e_server_stream_stub_with_message_raises_naming_server_stream(
        self, mock_grpc_echo_pool
    ):
        """(e) server_stream stub using response.message -> ConfigError mentioning
        'server_stream'."""
        bad = GrpcStub(
            service=SERVICE,
            method="ServerStream",
            response=GrpcResponse(message={"single": "wrong"}),
        )
        with pytest.raises(ConfigError) as exc_info:
            _build({"stream_with_unary": bad}, mock_grpc_echo_pool)
        msg = str(exc_info.value)
        assert "server_stream" in msg
        assert "mocks.grpc.stubs.stream_with_unary.response" in msg

    def test_f_server_stream_stub_with_messages_constructs(self, mock_grpc_echo_pool):
        """(f) server_stream stub using response.messages -> constructs cleanly,
        method_meta call_type == 'server_stream'."""
        server = _build({"ss_ok": _stream_stub()}, mock_grpc_echo_pool)
        key = ("echo.EchoService", "ServerStream")
        assert key in server.stubs_by_method
        assert server.method_meta[key][2] == "server_stream"

    def test_server_stream_empty_messages_raises_config_error(
        self, mock_grpc_echo_pool
    ):
        """server_stream stub with ``response.messages: []`` -> ConfigError.

        An empty streaming sequence is almost always an authoring mistake
        (the model validator only enforces exactly-one-of message/messages,
        and ``[] is not None`` so it slips through); ``_check_response_shape``
        fails loud at construction so the operator fixes the stub instead of
        silently streaming nothing at runtime.
        """
        bad = GrpcStub(
            service=SERVICE,
            method="ServerStream",
            response=GrpcResponse(messages=[]),
        )
        with pytest.raises(ConfigError) as exc_info:
            _build({"ss_empty": bad}, mock_grpc_echo_pool)
        msg = str(exc_info.value)
        assert "mocks.grpc.stubs.ss_empty.response" in msg
        assert "server_stream" in msg
        assert "at least one" in msg.lower()

    def test_client_stream_requires_message(self, mock_grpc_echo_pool):
        """client_stream is response.message-shaped (single aggregated reply)."""
        # Correct shape -> ok
        ok = GrpcStub(
            service=SERVICE,
            method="ClientStream",
            response=GrpcResponse(message={"aggregated": True}),
        )
        server = _build({"cs_ok": ok}, mock_grpc_echo_pool)
        assert server.method_meta[("echo.EchoService", "ClientStream")][2] == "client_stream"

        # Wrong shape -> ConfigError naming 'client_stream'
        bad = GrpcStub(
            service=SERVICE,
            method="ClientStream",
            response=GrpcResponse(
                messages=[GrpcResponseMessage(message={"x": 1})]
            ),
        )
        with pytest.raises(ConfigError) as exc_info:
            _build({"cs_bad": bad}, mock_grpc_echo_pool)
        assert "client_stream" in str(exc_info.value)

    def test_bidi_requires_message(self, mock_grpc_echo_pool):
        """bidi is response.message-shaped per turn."""
        ok = GrpcStub(
            service=SERVICE,
            method="Bidi",
            response=GrpcResponse(message={"reply": "turn"}),
        )
        server = _build({"b_ok": ok}, mock_grpc_echo_pool)
        assert server.method_meta[("echo.EchoService", "Bidi")][2] == "bidi"


# ---------------------------------------------------------------------------
# Step 1(g): descriptor fallback / both-empty
# ---------------------------------------------------------------------------


class TestDescriptorFallback:
    """config.descriptors=None falls back to top_level_descriptors; both empty -> error."""

    def test_g_top_level_descriptors_used_when_config_descriptors_none(
        self, mock_grpc_echo_proto_path
    ):
        """(g) config.descriptors=None + top_level_descriptors=[proto] -> constructs
        via the fallback source (builds the pool itself, no DI injection)."""
        stub = _unary_stub()
        config = GrpcMockConfig(
            listen="0.0.0.0:50051",
            descriptors=None,
            stubs={"fallback": stub},
        )
        server = MockGrpcServer(
            config,
            top_level_descriptors=[
                GrpcDescriptorSource(proto=str(mock_grpc_echo_proto_path))
            ],
            emit_event=_no_op_emit,
            # descriptor_pool intentionally None: forces the build path.
        )
        assert server.services == ["echo.EchoService"]
        assert ("echo.EchoService", "Unary") in server.stubs_by_method

    def test_g_both_descriptor_sources_empty_raises_config_error(self):
        """(g) config.descriptors=None AND top_level_descriptors=None -> ConfigError
        (no descriptors available for mocks.grpc)."""
        config = GrpcMockConfig(
            listen="0.0.0.0:50051",
            descriptors=None,
            stubs={"orphan": _unary_stub()},
        )
        with pytest.raises(ConfigError) as exc_info:
            MockGrpcServer(
                config,
                top_level_descriptors=None,
                emit_event=_no_op_emit,
            )
        # Context label 'mocks.grpc' must appear (folded by build_descriptor_pool).
        assert "mocks.grpc" in str(exc_info.value)

    def test_config_descriptors_take_precedence_over_top_level(
        self, mock_grpc_echo_proto_path, mock_grpc_echo_pool
    ):
        """config.descriptors set -> used; top_level_descriptors ignored.

        Verified by passing a deliberately broken top_level (None) alongside a
        valid config.descriptors source — the build must succeed because
        config.descriptors wins.
        """
        config = GrpcMockConfig(
            listen="0.0.0.0:50051",
            descriptors=[GrpcDescriptorSource(proto=str(mock_grpc_echo_proto_path))],
            stubs={"prec": _unary_stub()},
        )
        server = MockGrpcServer(
            config,
            top_level_descriptors=None,
            emit_event=_no_op_emit,
        )
        assert server.services == ["echo.EchoService"]


# ---------------------------------------------------------------------------
# stubs_by_method shape (dict-of-dicts, insertion order)
# ---------------------------------------------------------------------------


class TestStubsByMethodShape:
    """``stubs_by_method`` is a dict[(svc,mtd)] of ordered dict[name -> GrpcStub]."""

    def test_two_stubs_same_method_preserve_insertion_order(self, mock_grpc_echo_pool):
        """First-match-wins dispatch depends on insertion order being preserved.

        Register two stubs for the same (svc, method); the inner dict must
        iterate in registration order.
        """
        first = _unary_stub(message={"who": "first"})
        second = _unary_stub(message={"who": "second"})
        server = _build({"first": first, "second": second}, mock_grpc_echo_pool)

        inner = server.stubs_by_method[("echo.EchoService", "Unary")]
        assert list(inner.keys()) == ["first", "second"]
        # The GrpcStub objects are the same instances we registered.
        assert inner["first"] is first
        assert inner["second"] is second

    def test_two_stubs_different_methods_build_separate_entries(self, mock_grpc_echo_pool):
        """Stubs on different methods land in separate inner dicts."""
        server = _build(
            {
                "u": _unary_stub(method="Unary"),
                "s": _stream_stub(method="ServerStream"),
            },
            mock_grpc_echo_pool,
        )
        assert set(server.stubs_by_method.keys()) == {
            ("echo.EchoService", "Unary"),
            ("echo.EchoService", "ServerStream"),
        }
        # method_meta parallel keys with correct call types.
        assert server.method_meta[("echo.EchoService", "Unary")][2] == "unary"
        assert server.method_meta[("echo.EchoService", "ServerStream")][2] == "server_stream"

    def test_services_sorted_and_unique(self, mock_grpc_echo_pool):
        """``services`` is the sorted unique set of fully-qualified service names."""
        # Two stubs on the same service -> one entry in services.
        server = _build(
            {
                "u": _unary_stub(),
                "s": _stream_stub(),
            },
            mock_grpc_echo_pool,
        )
        assert server.services == ["echo.EchoService"]

    def test_empty_stubs_construct_cleanly(self, mock_grpc_echo_pool):
        """An empty stubs map is structurally valid (server with no handlers).

        Task 7 may still reject this at bind time (no handlers); Task 6 only
        validates per-stub, so zero stubs is a successful no-op construction.
        """
        server = _build({}, mock_grpc_echo_pool)
        assert server.stubs_by_method == {}
        assert server.method_meta == {}
        assert server.services == []


# ---------------------------------------------------------------------------
# DI seam: injected descriptor_pool short-circuits the build
# ---------------------------------------------------------------------------


def test_injected_descriptor_pool_skips_build(monkeypatch, mock_grpc_echo_pool):
    """When descriptor_pool is injected, build_descriptor_pool is NOT called.

    Monkey-patch build_descriptor_pool to explode; the injected-pool path must
    short-circuit around it. This pins the DI seam contract relied on by every
    other test in this file.
    """
    import agctl.mock.grpc_server as mod

    def _explode(*args, **kwargs):
        raise AssertionError("build_descriptor_pool must not be called when descriptor_pool is injected")

    monkeypatch.setattr(mod, "build_descriptor_pool", _explode)

    config = GrpcMockConfig(stubs={"x": _unary_stub()})
    server = MockGrpcServer(
        config,
        top_level_descriptors=None,
        emit_event=_no_op_emit,
        descriptor_pool=mock_grpc_echo_pool,
    )
    assert server.services == ["echo.EchoService"]


# ---------------------------------------------------------------------------
# Module-level import discipline (load-bearing: construction must not import
# grpc at module top; the kernel handles all proto work via lazy imports).
# ---------------------------------------------------------------------------


def test_mock_grpc_server_module_remains_grpcio_free_at_module_top():
    """Re-asserted here alongside Task 6: the module that now hosts
    ``MockGrpcServer`` still bans ``grpc``/``grpcio``/``grpc_tools``/
    ``google``/``google.protobuf`` at module top — the class's ``__init__``
    must do its proto work through the kernel (which lazy-imports), not via
    module-top imports. Task 7's ``import grpc`` goes INSIDE ``serve_forever``.
    """
    import ast

    import agctl.mock.grpc_server as mod

    banned = ("grpc", "grpcio", "grpc_tools", "google", "google.protobuf")

    def _is_banned(dotted: str) -> bool:
        return any(dotted == b or dotted.startswith(b + ".") for b in banned)

    tree = ast.parse(open(mod.__file__).read())
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not _is_banned(alias.name), (
                    f"agctl/mock/grpc_server.py has a module-top "
                    f"`import {alias.name}` — must remain grpcio-free at "
                    f"module top; import grpc lazily inside MockGrpcServer "
                    f"methods (Task 7)."
                )
        elif isinstance(node, ast.ImportFrom):
            if node.module and _is_banned(node.module):
                raise AssertionError(
                    f"agctl/mock/grpc_server.py has a module-top "
                    f"`from {node.module} import ...` — must remain "
                    f"grpcio-free at module top."
                )


# ---------------------------------------------------------------------------
# Task 7: gRPC runtime wiring — generic servicer, 4 call types, Health,
# Reflection, lifecycle (start/serve_forever/shutdown/actual_listen).
#
# These tests spin up a REAL grpcio server on an ephemeral port and exercise
# it via raw ``grpc.insecure_channel`` calls. The whole section is gated on
# the gRPC extra (``grpc``/``grpcio-health-checking``/``grpcio-reflection``):
# ``pytest.importorskip`` so a missing extra skips cleanly rather than erroring.
# ---------------------------------------------------------------------------


# Gate the Task-7 section on the gRPC runtime extras. The Task-6 tests above
# already gated on ``grpc_tools``/``google.protobuf`` (the build-time extras);
# Task 7 additionally needs the runtime extras (``grpc``/``grpc_health``/
# ``grpc_reflection``). ``pytest.importorskip`` returns a "skip" marker object
# when missing — keep it as a module-level statement so collection skips.
pytest.importorskip("grpc")
pytest.importorskip("grpc_health")
pytest.importorskip("grpc_reflection")

import contextlib  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

import grpc  # noqa: E402

from agctl.clients.grpc_descriptors import (  # noqa: E402
    deserialize,
    serialize,
)
from agctl.config.models import (  # noqa: E402
    CaptureSpec,
    GrpcMatch,
)


SERVICE = "echo.EchoService"  # re-stated for proximity to the Task-7 tests below


def _start_server(
    stubs: dict[str, GrpcStub],
    pool,
    *,
    health: bool = True,
    reflection: bool = True,
    listen: str = "127.0.0.1:0",
    concurrency_cap: int = 8,
) -> tuple[MockGrpcServer, list[dict]]:
    """Build + start a ``MockGrpcServer`` against ``pool`` on an ephemeral port.

    Returns ``(server, events)``. Caller MUST shut the server down (the
    ``stop_and_wait`` context manager below does both).
    """
    events: list[dict] = []
    config = GrpcMockConfig(
        listen=listen,
        stubs=stubs,
        health=health,
        reflection=reflection,
        concurrency_cap=concurrency_cap,
    )
    server = MockGrpcServer(
        config,
        top_level_descriptors=None,
        emit_event=events.append,
        descriptor_pool=pool,
    )
    server.start()
    return server, events


@contextlib.contextmanager
def _running_server(
    stubs: dict[str, GrpcStub],
    pool,
    *,
    health: bool = True,
    reflection: bool = True,
    listen: str = "127.0.0.1:0",
):
    """Context manager: yields ``(server, events, channel)``; shuts down on exit."""
    server, events = _start_server(
        stubs,
        pool,
        health=health,
        reflection=reflection,
        listen=listen,
    )
    channel = grpc.insecure_channel(server.actual_listen())
    try:
        # Wait for the channel to become ready (bounded; ephemeral bind is up).
        grpc.channel_ready_future(channel).result(timeout=2.0)
        yield server, events, channel
    finally:
        channel.close()
        server.shutdown()


def _make_invoker(pool, channel, svc: str, mtd: str):
    """Build raw unary/stream invokers for /svc/mtd off the echo proto pool.

    Returns a 4-tuple ``(unary, server_stream, client_stream, bidi)`` of callables
    wired with serialize/deserialize for the method's input/output types.
    """
    from agctl.clients.grpc_descriptors import find_method

    md = find_method(pool, svc, mtd)
    ser = serialize(md.input_type)
    deser = deserialize(md.output_type)
    method_path = f"/{svc}/{mtd}"
    return (
        channel.unary_unary(method_path, request_serializer=ser, response_deserializer=deser),
        channel.unary_stream(method_path, request_serializer=ser, response_deserializer=deser),
        channel.stream_unary(method_path, request_serializer=ser, response_deserializer=deser),
        channel.stream_stream(method_path, request_serializer=ser, response_deserializer=deser),
    )


# ---------------------------------------------------------------------------
# Lifecycle: start / actual_listen / shutdown
# ---------------------------------------------------------------------------


class TestLifecycle:
    """start/actual_listen/shutdown wiring."""

    def test_start_builds_server_and_binds_ephemeral_port(self, mock_grpc_echo_pool):
        """start() -> _server is a grpc.Server; actual_listen() reflects the bound port."""
        with _running_server({"u": _unary_stub()}, mock_grpc_echo_pool) as (server, _, _):
            assert server._server is not None
            # actual_listen is "host:port" with port > 0 (ephemeral bind succeeded).
            host, _, port = server.actual_listen().rpartition(":")
            assert host == "127.0.0.1"
            assert int(port) > 0

    def test_actual_listen_uses_fixed_port_when_requested(self, mock_grpc_echo_pool):
        """A non-zero requested port is reflected verbatim in actual_listen()."""
        # Pick a likely-free high port. Bind + immediate teardown.
        with _running_server(
            {"u": _unary_stub()},
            mock_grpc_echo_pool,
            listen="127.0.0.1:50151",
        ) as (server, _, _):
            assert server.actual_listen() == "127.0.0.1:50151"

    def test_shutdown_is_idempotent(self, mock_grpc_echo_pool):
        """shutdown() can be called twice without raising."""
        server, _ = _start_server({"u": _unary_stub()}, mock_grpc_echo_pool)
        try:
            server.shutdown()
        finally:
            server.shutdown()  # second call must not raise

    def test_serve_forever_returns_when_stop_event_is_set(self, mock_grpc_echo_pool):
        """serve_forever blocks until stop_event.is_set(), then returns promptly."""
        import threading

        server, _ = _start_server({"u": _unary_stub()}, mock_grpc_echo_pool)
        try:
            stop_event = threading.Event()
            serve_thread = threading.Thread(
                target=server.serve_forever, args=(stop_event,), daemon=True
            )
            serve_thread.start()
            # Give serve_forever a moment to enter the wait_for_termination loop.
            time.sleep(0.05)
            stop_event.set()
            serve_thread.join(timeout=2.0)
            assert not serve_thread.is_alive(), "serve_forever did not return after stop_event"
        finally:
            server.shutdown()

    def test_serve_forever_without_start_is_a_noop(self, mock_grpc_echo_pool):
        """serve_forever on an un-started server returns immediately (no _server)."""
        import threading

        config = GrpcMockConfig(stubs={"u": _unary_stub()})
        server = MockGrpcServer(
            config,
            top_level_descriptors=None,
            emit_event=lambda _event: None,
            descriptor_pool=mock_grpc_echo_pool,
        )
        # Not started -> _server is None -> serve_forever returns immediately.
        stop_event = threading.Event()
        serve_thread = threading.Thread(
            target=server.serve_forever, args=(stop_event,), daemon=True
        )
        serve_thread.start()
        serve_thread.join(timeout=2.0)
        assert not serve_thread.is_alive()

    def test_port_in_use_raises_config_error(self, mock_grpc_echo_pool):
        """Binding on an already-bound port -> ConfigError.

        Uses a plain ``socket.socket`` to hold the port (without SO_REUSEPORT)
        so the grpc C core's bind genuinely fails with EADDRINUSE and
        ``add_insecure_port`` raises ``RuntimeError``. Two grpc servers can
        otherwise share a port via SO_REUSEPORT, which would not exercise the
        error path under test.
        """
        import socket

        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))  # ephemeral — discover via getsockname.
        blocker.listen(1)
        port = blocker.getsockname()[1]
        try:
            with pytest.raises(ConfigError) as exc_info:
                _start_server(
                    {"u": _unary_stub()},
                    mock_grpc_echo_pool,
                    listen=f"127.0.0.1:{port}",
                )
            msg = str(exc_info.value)
            # Message must mention the listen address and the in-use condition.
            assert f"127.0.0.1:{port}" in msg
            assert "in use" in msg.lower() or "already" in msg.lower()
        finally:
            blocker.close()


# ---------------------------------------------------------------------------
# Unary dispatch
# ---------------------------------------------------------------------------


class TestUnaryDispatch:
    """Unary RPC end-to-end: match, unmatched, capture, non-OK status."""

    def test_unary_match_returns_response_and_emits_hit(self, mock_grpc_echo_pool):
        """Echo/Unary with match.body {msg} + response.message {msg:"{msg}"}:
        call {msg:"hi"} -> {msg:"hi"}; exactly one grpc.hit event recorded."""
        stub = GrpcStub(
            service=SERVICE,
            method="Unary",
            match=GrpcMatch(body={"msg": "hi"}),
            capture={
                "msg": CaptureSpec(from_=".message.msg"),
            },
            response=GrpcResponse(message={"msg": "{msg}"}),
        )
        with _running_server({"echo_unary": stub}, mock_grpc_echo_pool) as (
            server,
            events,
            channel,
        ):
            unary, _, _, _ = _make_invoker(mock_grpc_echo_pool, channel, SERVICE, "Unary")
            resp, _call = unary.with_call({"msg": "hi"})
            assert resp == {"msg": "hi"}

            hits = [e for e in events if e.get("event") == "grpc.hit"]
            assert len(hits) == 1
            assert hits[0]["stub"] == "echo_unary"
            assert hits[0]["service"] == SERVICE
            assert hits[0]["method"] == "Unary"
            assert hits[0]["call_type"] == "unary"
            assert hits[0]["status"] == "OK"
            assert isinstance(hits[0]["duration_ms"], int)

    def test_unary_unmatched_aborts_unimplemented_and_emits_unmatched(
        self, mock_grpc_echo_pool
    ):
        """A request that matches no stub -> UNIMPLEMENTED + grpc.unmatched event."""
        stub = GrpcStub(
            service=SERVICE,
            method="Unary",
            match=GrpcMatch(body={"msg": "hi"}),
            response=GrpcResponse(message={"msg": "{msg}"}),
        )
        with _running_server({"echo_unary": stub}, mock_grpc_echo_pool) as (
            server,
            events,
            channel,
        ):
            unary, _, _, _ = _make_invoker(mock_grpc_echo_pool, channel, SERVICE, "Unary")
            with pytest.raises(grpc.RpcError) as exc_info:
                unary({"msg": "bye"})  # no stub matches
            assert exc_info.value.code() == grpc.StatusCode.UNIMPLEMENTED

            unmatched = [e for e in events if e.get("event") == "grpc.unmatched"]
            assert len(unmatched) == 1
            assert unmatched[0]["service"] == SERVICE
            assert unmatched[0]["method"] == "Unary"
            assert unmatched[0]["call_type"] == "unary"

    def test_unary_non_ok_status_aborts_with_authored_code(self, mock_grpc_echo_pool):
        """status: NOT_FOUND -> call aborts with NOT_FOUND (no grpc.hit for an
        unmatched-style miss; here we DO match, then abort with the stub's status)."""
        stub = GrpcStub(
            service=SERVICE,
            method="Unary",
            match=GrpcMatch(body={"msg": "ghost"}),
            response=GrpcResponse(message={"msg": "should-not-see"}, status="NOT_FOUND"),
        )
        with _running_server({"missing": stub}, mock_grpc_echo_pool) as (
            server,
            events,
            channel,
        ):
            unary, _, _, _ = _make_invoker(mock_grpc_echo_pool, channel, SERVICE, "Unary")
            with pytest.raises(grpc.RpcError) as exc_info:
                unary({"msg": "ghost"})
            assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

            # Matched: a grpc.hit event is recorded (status_name NOT_FOUND).
            hits = [e for e in events if e.get("event") == "grpc.hit"]
            assert len(hits) == 1
            assert hits[0]["status"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# Server-stream dispatch
# ---------------------------------------------------------------------------


class TestServerStreamDispatch:
    """Server-stream RPC: N rendered messages in authored order."""

    def test_server_stream_returns_n_messages_in_order(self, mock_grpc_echo_pool):
        stub = GrpcStub(
            service=SERVICE,
            method="ServerStream",
            response=GrpcResponse(
                messages=[
                    GrpcResponseMessage(message={"msg": "one"}),
                    GrpcResponseMessage(message={"msg": "two"}),
                    GrpcResponseMessage(message={"msg": "three"}),
                ]
            ),
        )
        with _running_server({"ss": stub}, mock_grpc_echo_pool) as (
            server,
            events,
            channel,
        ):
            _, server_stream, _, _ = _make_invoker(
                mock_grpc_echo_pool, channel, SERVICE, "ServerStream"
            )
            msgs = list(server_stream({"msg": "anything"}))
            assert [m["msg"] for m in msgs] == ["one", "two", "three"]

            # One grpc.hit per yielded message.
            hits = [e for e in events if e.get("event") == "grpc.hit"]
            assert len(hits) == 3
            assert [h["method"] for h in hits] == ["ServerStream"] * 3
            assert all(h["call_type"] == "server_stream" for h in hits)


# ---------------------------------------------------------------------------
# Client-stream dispatch
# ---------------------------------------------------------------------------


class TestClientStreamDispatch:
    """Client-stream RPC: aggregated match at iterator close, single reply."""

    def test_client_stream_aggregated_match_returns_single_reply(
        self, mock_grpc_echo_pool
    ):
        # Match on the LAST request's msg via jq (client_stream uses jq only).
        stub = GrpcStub(
            service=SERVICE,
            method="ClientStream",
            match=GrpcMatch(jq='.messages[-1].msg == "end"'),
            response=GrpcResponse(message={"msg": "aggregated"}),
        )
        with _running_server({"cs": stub}, mock_grpc_echo_pool) as (
            server,
            events,
            channel,
        ):
            _, _, client_stream, _ = _make_invoker(
                mock_grpc_echo_pool, channel, SERVICE, "ClientStream"
            )
            resp, _call = client_stream.with_call(
                iter([{"msg": "a"}, {"msg": "b"}, {"msg": "end"}])
            )
            assert resp == {"msg": "aggregated"}

            hits = [e for e in events if e.get("event") == "grpc.hit"]
            assert len(hits) == 1
            assert hits[0]["call_type"] == "client_stream"


# ---------------------------------------------------------------------------
# Bidi dispatch
# ---------------------------------------------------------------------------


class TestBidiDispatch:
    """Bidi RPC: one response per matched incoming request."""

    def test_bidi_echo_style_yields_one_response_per_request(self, mock_grpc_echo_pool):
        # Capture the incoming msg, echo it back. Per-turn unary-style envelope.
        stub = GrpcStub(
            service=SERVICE,
            method="Bidi",
            capture={"msg": CaptureSpec(from_=".message.msg")},
            response=GrpcResponse(message={"msg": "echo:{msg}"}),
        )
        with _running_server({"bidi": stub}, mock_grpc_echo_pool) as (
            server,
            events,
            channel,
        ):
            _, _, _, bidi = _make_invoker(
                mock_grpc_echo_pool, channel, SERVICE, "Bidi"
            )
            replies = list(bidi(iter([{"msg": "x"}, {"msg": "y"}, {"msg": "z"}])))
            assert [r["msg"] for r in replies] == ["echo:x", "echo:y", "echo:z"]

            # One grpc.hit per turn (3 requests -> 3 hits).
            hits = [e for e in events if e.get("event") == "grpc.hit"]
            assert len(hits) == 3
            assert all(h["call_type"] == "bidi" for h in hits)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:
    """health=True -> HealthStub.Check returns SERVING for overall + per service."""

    def test_overall_health_is_serving(self, mock_grpc_echo_pool):
        from grpc_health.v1 import health_pb2, health_pb2_grpc

        with _running_server({"u": _unary_stub()}, mock_grpc_echo_pool, health=True) as (
            server,
            events,
            channel,
        ):
            stub = health_pb2_grpc.HealthStub(channel)
            resp = stub.Check(health_pb2.HealthCheckRequest(service=""))
            assert resp.status == health_pb2.HealthCheckResponse.SERVING

    def test_per_service_health_is_serving(self, mock_grpc_echo_pool):
        from grpc_health.v1 import health_pb2, health_pb2_grpc

        with _running_server({"u": _unary_stub()}, mock_grpc_echo_pool, health=True) as (
            server,
            events,
            channel,
        ):
            stub = health_pb2_grpc.HealthStub(channel)
            resp = stub.Check(health_pb2.HealthCheckRequest(service=SERVICE))
            assert resp.status == health_pb2.HealthCheckResponse.SERVING

    def test_health_disabled_omits_health_service(self, mock_grpc_echo_pool):
        """health=False -> Check on the overall service aborts UNIMPLEMENTED."""
        from grpc_health.v1 import health_pb2, health_pb2_grpc

        with _running_server(
            {"u": _unary_stub()}, mock_grpc_echo_pool, health=False
        ) as (server, events, channel):
            stub = health_pb2_grpc.HealthStub(channel)
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.Check(health_pb2.HealthCheckRequest(service=""))
            assert exc_info.value.code() == grpc.StatusCode.UNIMPLEMENTED


# ---------------------------------------------------------------------------
# Reflection
# ---------------------------------------------------------------------------


class TestReflection:
    """reflection=True -> ServerReflectionStub.list_services returns configured services."""

    def test_reflection_lists_configured_services(self, mock_grpc_echo_pool):
        from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

        with _running_server(
            {"u": _unary_stub()}, mock_grpc_echo_pool, reflection=True
        ) as (server, events, channel):
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            resp_iter = stub.ServerReflectionInfo(
                iter([reflection_pb2.ServerReflectionRequest(list_services="")])
            )
            listed = []
            for r in resp_iter:
                listed.extend(s.name for s in r.list_services_response.service)
            # The configured service must be present.
            assert SERVICE in listed

    def test_reflection_disabled_omits_reflection_service(self, mock_grpc_echo_pool):
        """reflection=False -> the reflection bidi call aborts UNIMPLEMENTED."""
        from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

        with _running_server(
            {"u": _unary_stub()}, mock_grpc_echo_pool, reflection=False
        ) as (server, events, channel):
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            with pytest.raises(grpc.RpcError) as exc_info:
                # Consume the iterator to surface the terminal status.
                list(
                    stub.ServerReflectionInfo(
                        iter([reflection_pb2.ServerReflectionRequest(list_services="")])
                    )
                )
            assert exc_info.value.code() == grpc.StatusCode.UNIMPLEMENTED


# ---------------------------------------------------------------------------
# Missing-grpc-extra path: _build_server must surface ConfigError pointing at
# the ``grpc`` extra when grpc/grpc_health/grpc_reflection can't be imported.
# ---------------------------------------------------------------------------


class TestMissingGrpcExtra:
    """When ``import grpc`` fails inside _build_server, surface ConfigError."""

    def test_missing_grpc_extra_raises_config_error(self, mock_grpc_echo_pool, monkeypatch):
        """Simulate a missing grpc extra by hiding the module via sys.modules.

        ``sys.modules[name] = None`` makes Python raise ImportError on
        ``import name`` (the standard "halted; None in sys.modules" path).
        The lazy ``import grpc`` inside ``_build_server`` must translate that
        ImportError into a :class:`ConfigError` pointing at the ``grpc`` extra.
        """
        import sys

        monkeypatch.setitem(sys.modules, "grpc", None)

        events: list[dict] = []
        config = GrpcMockConfig(
            listen="127.0.0.1:0",
            stubs={"u": _unary_stub()},
        )
        server = MockGrpcServer(
            config,
            top_level_descriptors=None,
            emit_event=events.append,
            descriptor_pool=mock_grpc_echo_pool,
        )
        with pytest.raises(ConfigError) as exc_info:
            server.start()
        assert "agctl[grpc]" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Module stays importable without the grpc extra (the dispatch core path).
# This test runs unconditionally: it constructs the server (no grpc import)
# and verifies _server stays None without ever calling start().
# ---------------------------------------------------------------------------


def test_module_imports_and_constructs_without_grpc_runtime(mock_grpc_echo_pool):
    """The module + dispatch core remain importable without the grpc runtime.

    ``MockGrpcServer.__init__`` must not import ``grpc`` (only the kernel-level
    ``google.protobuf``/``grpc_tools`` already gated at module top). This test
    is a smoke test that construction succeeds and ``_server`` stays None until
    ``start()`` is actually called (which would lazy-import grpc).
    """
    server = _build({"u": _unary_stub()}, mock_grpc_echo_pool)
    assert server._server is None
