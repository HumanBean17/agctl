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
