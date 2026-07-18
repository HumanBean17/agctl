"""Transport-agnostic gRPC dispatch core (Task 5) + ``MockGrpcServer`` (Task 6).

This module is the **brain** of the gRPC mock: given a resolved method's call
type, the stub list registered for that ``(service, method)`` pair, and the
deserialized request envelope, it picks a stub (first-match-wins), runs
match/capture/render, and produces the response message(s) + terminal status,
or signals UNIMPLEMENTED (``matched=False``).

It is **pure Python with no ``grpc`` import** anywhere in the file. The
dispatch functions are importable and unit-testable without the grpcio extra
installed; this keeps the mock brain fully covered by the fast unit suite,
independent of the gRPC transport. Task 6 adds ``MockGrpcServer`` to this
same module, importing ``grpc`` **lazily inside its methods** (never at
module top) so this dispatch core stays grpcio-free.

Per-call-type behavior (DESIGN §8.1):

==================  =====================================================
call type           match envelope           response selection
==================  =====================================================
unary               ``{service, method,      ``response.message`` ->
                    metadata, message}``     one rendered message
server_stream       same as unary            each ``response.messages[*]``
                                             -> N rendered messages
client_stream       ``{service, method,      ``response.message`` ->
                    metadata, messages,      one rendered message
                    count}``                 (match.body skipped)
bidi                same as unary            ``response.message`` ->
                                             one rendered message per turn
==================  =====================================================

Dispatch trusts the ``call_type`` argument: response-shape correctness vs the
derived call type (e.g. ``messages`` on a unary method) is validated at server
construction in Task 6, where the descriptor pool is available.

**Signature note:** ``dispatch_grpc`` takes ``stubs: dict[str, GrpcStub]``
(insertion-ordered), not the ``list[GrpcStub]`` from the original brief.
``GrpcStub`` (Task 3) has no ``.name`` field — its identity is the dict key
in ``mocks.grpc.stubs``, exactly like ``HttpStub``. The dict form carries
that name through dispatch without modifying Task 3's model: Python 3.7+
dicts preserve insertion order, so first-match-wins semantics are identical
to the brief's list iteration.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from ..assertions import jq_bool, json_subset, parse_grpc_status
from ..clients.grpc_descriptors import (
    build_descriptor_pool,
    call_type_of,
    find_method,
)
from ..config.models import (
    GrpcDescriptorSource,
    GrpcMockConfig,
    GrpcStub,
    parse_listen,
)
from ..errors import ConfigError, TemplateNotFound
from ..resolution import render_typed
from .capture import resolve_captures

__all__ = [
    "GrpcDispatchOutcome",
    "MockGrpcServer",
    "build_envelope",
    "dispatch_grpc",
]


def build_envelope(
    service: str,
    method: str,
    metadata: dict[str, str],
    *,
    message: Any = None,
    messages: list[Any] | None = None,
) -> dict[str, Any]:
    """Build the per-call-type match envelope (DESIGN §8.1).

    The envelope is the single root shared by ``match.jq`` (predicate input)
    and ``capture.from`` (capture extraction) — mirroring the HTTP request
    envelope contract (method/path/headers/body).

    - ``metadata`` keys are lowercased (mirrors HTTP header convention).
    - ``message`` set (not ``None``) -> unary/server_stream/bidi shape:
      ``{service, method, metadata, message}``.
    - ``messages`` set (not ``None``) -> client_stream shape:
      ``{service, method, metadata, messages: [...], count: len}``. Built at
      request-stream close.

    Exactly one of ``message`` / ``messages`` should be supplied; if both are
    passed, ``messages`` wins — opt into the client_stream shape explicitly
    by passing ``messages=...`` and leaving ``message`` unset (which is how
    Task 6/7 will call this).
    """
    lowered = {k.lower(): v for k, v in metadata.items()}
    if messages is not None:
        return {
            "service": service,
            "method": method,
            "metadata": lowered,
            "messages": list(messages),
            "count": len(messages),
        }
    return {
        "service": service,
        "method": method,
        "metadata": lowered,
        "message": message,
    }


@dataclass
class GrpcDispatchOutcome:
    """Result of :func:`dispatch_grpc` for a single RPC turn.

    Fields:
        matched: True iff a stub matched. When False, every other field is
            cleared (``stub_name is None``, ``messages == []``,
            ``status is None``, ``missing_captures == []``) — the caller
            (Task 7) emits ``grpc.unmatched`` and aborts UNIMPLEMENTED.
        stub_name: Name (dict key) of the matched stub; None when unmatched.
        messages: Rendered response message dict(s). One entry for
            unary/client_stream/bidi; N entries for server_stream.
        status: Terminal ``(code, name)`` gRPC status tuple (e.g.
            ``(5, "NOT_FOUND")``). ``(0, "OK")`` is the success default.
            ``None`` only on the unmatched outcome.
        missing_captures: ``[(name, from_path), ...]`` for captures whose
            ``from`` path resolved to null on the live envelope. Each pair
            is also surfaced via the ``emit_capture_missing`` callback at
            dispatch time so the caller can emit ``capture.missing`` events
            in stream order.
    """

    matched: bool
    stub_name: str | None = None
    messages: list[Any] = field(default_factory=list)
    status: tuple[int, str] | None = None
    missing_captures: list[tuple[str, str]] = field(default_factory=list)


def dispatch_grpc(
    stubs: dict[str, GrpcStub],
    envelope: dict,
    call_type: str,
    *,
    emit_capture_missing: Callable[[str, str, str], None],
) -> GrpcDispatchOutcome:
    """Dispatch one RPC turn against ``stubs`` (first-match-wins).

    Iterates ``stubs`` in insertion order; for each ``(name, stub)``, evaluates
    every set predicate of ``stub.match``:

    - ``match.body``: :func:`json_subset` against ``envelope["message"]`` for
      unary/server_stream/bidi. **Skipped for client_stream** — a subset over
      an aggregated list is ill-defined; client_stream matches via
      ``match.jq`` only (rooted at ``.messages[-1].x`` etc.).
    - ``match.jq``: :func:`jq_bool` against the whole envelope. A runtime
      error against the envelope is swallowed by ``jq_bool`` to False (soft
      non-match per DESIGN §3.2) — dispatch never raises on a jq runtime
      error.

    Both set predicates must pass (AND). The first stub where all set
    predicates pass wins; subsequent stubs are not evaluated. An omitted
    ``match`` (``None``) matches unconditionally.

    On match:

    1. ``resolve_captures(envelope, stub.capture)`` -> ``(typed, missing)``.
       For each ``(cap_name, from_path)`` in ``missing``, invoke
       ``emit_capture_missing(stub_name, cap_name, from_path)`` — the caller
       (Task 7) emits the ``capture.missing`` event.
    2. Render each response message via :func:`render_typed`. Message source
       depends on ``call_type``:

       - ``unary`` / ``client_stream`` / ``bidi`` -> ``response.message``
         -> ``messages=[rendered]``.
       - ``server_stream`` -> ``response.messages`` -> one rendered entry
         per ``GrpcResponseMessage``.
    3. Resolve ``stub.response.status`` via :func:`parse_grpc_status`
       (default ``"OK"`` -> ``(0, "OK")``).

    Returns a populated :class:`GrpcDispatchOutcome`. If no stub matches,
    returns ``GrpcDispatchOutcome(matched=False)`` (all other fields cleared)
    — the caller emits ``grpc.unmatched`` and aborts UNIMPLEMENTED.

    Args:
        stubs: Insertion-ordered ``{name: GrpcStub}`` map for the resolved
            ``(service, method)`` pair (Task 7 pre-filters the global stub
            map by service/method). Dispatch does NOT re-check
            ``stub.service``/``stub.method`` against the envelope.
        envelope: Per-call-type envelope from :func:`build_envelope`.
        call_type: One of ``unary``/``server_stream``/``client_stream``/
            ``bidi``. Trusted: response-shape correctness vs derived call
            type is validated at server construction (Task 6).
        emit_capture_missing: Callback invoked once per capture whose
            ``from`` path resolved to null. Signature
            ``(stub_name, capture_name, from_path) -> None``.

    Returns:
        The dispatch outcome (matched or unmatched).
    """
    matched = _first_match(stubs, envelope, call_type)
    if matched is None:
        return GrpcDispatchOutcome(matched=False)

    stub_name, stub = matched

    # Capture resolution: ``resolve_captures`` returns the typed map AND the
    # missing list. A missing ``jq`` library surfaces as ``ConfigError`` from
    # ``jq_value`` (a configuration problem, exit 2) and propagates — that is
    # distinct from a per-path soft miss (path resolves to None), which the
    # resolver records in the missing list and we surface via the callback.
    captures, missing = resolve_captures(envelope, stub.capture)
    for cap_name, from_path in missing:
        emit_capture_missing(stub_name, cap_name, from_path)

    # Render the response. Per-call-type selection trusts the ``call_type``
    # arg; response-shape correctness vs derived call type is validated at
    # Task 6 server construction (where the descriptor pool is available).
    rendered = _render_response(stub, call_type, captures)

    status = parse_grpc_status(stub.response.status)

    return GrpcDispatchOutcome(
        matched=True,
        stub_name=stub_name,
        messages=rendered,
        status=status,
        missing_captures=missing,
    )


def _first_match(
    stubs: dict[str, GrpcStub], envelope: dict, call_type: str
) -> tuple[str, GrpcStub] | None:
    """Return ``(name, stub)`` for the first stub whose set predicates all pass."""
    for name, stub in stubs.items():
        if stub.match is None:
            return name, stub
        if _match_body(stub, envelope, call_type) and _match_jq(stub, envelope):
            return name, stub
    return None


def _match_body(stub: GrpcStub, envelope: dict, call_type: str) -> bool:
    """Evaluate ``match.body`` (json_subset).

    Skipped (treated as pass) when: ``match.body is None``, or the call type
    is ``client_stream`` (subset over an aggregated list is ill-defined).
    """
    if call_type == "client_stream":
        return True
    body = stub.match.body  # type: ignore[union-attr]
    if body is None:
        return True
    return json_subset(body, envelope.get("message"))


def _match_jq(stub: GrpcStub, envelope: dict) -> bool:
    """Evaluate ``match.jq`` (jq_bool against the envelope).

    ``jq_bool`` swallows compile/runtime errors to False (soft non-match per
    DESIGN §3.2); this wrapper adds no extra try/except so dispatch never
    raises on a jq runtime error. A missing ``jq`` library surfaces as
    ``ConfigError`` (exit 2) and propagates — distinct from a soft miss.
    """
    expr = stub.match.jq  # type: ignore[union-attr]
    if expr is None:
        return True
    return jq_bool(envelope, expr)


def _render_response(
    stub: GrpcStub, call_type: str, captures: dict
) -> list[Any]:
    """Render response messages per the call type.

    - server_stream: one rendered entry per ``response.messages[*].message``.
    - unary/client_stream/bidi: ``response.message`` -> single-entry list.

    The caller (Task 7) is responsible for honoring per-message ``delay_ms``
    and emitting one ``grpc.hit`` per yielded message; dispatch returns the
    full rendered list in authored order.
    """
    if call_type == "server_stream":
        return [
            render_typed(entry.message, captures)
            for entry in (stub.response.messages or [])
        ]
    return [render_typed(stub.response.message, captures)]


# ---------------------------------------------------------------------------
# MockGrpcServer — construction, validation, method table (Task 6)
# ---------------------------------------------------------------------------


class MockGrpcServer:
    """Validated, not-yet-bound gRPC mock server (Task 6 of the gRPC mock feature).

    Construction does everything that does NOT need the grpc runtime:

    - Resolves the protobuf ``DescriptorPool`` ONCE (via the shared kernel or
      an injected pool — the DI seam used by tests).
    - Validates every stub's ``(service, method)`` against the pool
      (unknown service/method -> :class:`ConfigError` at
      ``mocks.grpc.stubs.<name>``).
    - Validates response-shape-vs-call-type: ``server_stream`` requires
      ``response.messages``; ``unary``/``client_stream``/``bidi`` require
      ``response.message``. Violation -> :class:`ConfigError` at
      ``mocks.grpc.stubs.<name>.response`` naming the call type.
    - Precomputes :attr:`stubs_by_method` (dict-of-dicts keyed by
      ``(service, method)`` -> ordered ``{stub_name: GrpcStub}``) and
      :attr:`method_meta` (parallel ``{(service, method):
      (input_msg_desc, output_msg_desc, call_type)}``).

    Construction deliberately does NOT bind a port or build a
    ``grpc.Server`` — that is Task 7's ``serve_forever``. ``self._server``
    stays ``None`` until then, and every lazy ``import grpc`` lives inside
    Task 7's methods (this module remains grpcio-free at module top so the
    dispatch brain stays unit-testable without the gRPC extra).

    ``stubs_by_method`` shape (load-bearing): an ordered
    ``dict[tuple[str, str], dict[str, GrpcStub]]`` — the OUTER key is
    ``(service, method)``; the INNER dict is ``{config_key: GrpcStub}``
    preserving insertion order for first-match-wins dispatch (Task 5's
    ``dispatch_grpc`` takes ``dict[str, GrpcStub]`` because ``GrpcStub`` has
    no ``.name`` field — the dict key carries the name).
    """

    def __init__(
        self,
        config: GrpcMockConfig,
        *,
        top_level_descriptors: list[GrpcDescriptorSource] | None,
        emit_event: Callable[[dict], None],
        descriptor_pool: Any = None,
    ) -> None:
        # 1. Resolve the descriptor pool ONCE. Production passes
        #    ``descriptor_pool=None``; tests inject a prebuilt pool to skip
        #    the (slow) protoc compile and isolate construction from the
        #    kernel's source-resolution paths.
        if descriptor_pool is None:
            # config.descriptors wins; fall back to top_level (the engine's
            # ``grpc.descriptors``); an empty/None everywhere surfaces as a
            # context-labeled ConfigError from the kernel (mentions
            # "mocks.grpc") — no special-casing here.
            sources = config.descriptors or top_level_descriptors or []
            pool = build_descriptor_pool(sources, context_label="mocks.grpc")
        else:
            pool = descriptor_pool

        # 2. Validate each stub + build the per-method tables in one pass.
        # Partial state is discarded if any stub fails validation (the
        # constructor raises and never returns an instance), so we don't
        # need a two-phase build.
        stubs_by_method: dict[tuple[str, str], dict[str, GrpcStub]] = {}
        method_meta: dict[tuple[str, str], tuple[Any, Any, str]] = {}
        services_seen: set[str] = set()

        for name, stub in config.stubs.items():
            method_desc = self._resolve_method(pool, stub, name)
            call_type = call_type_of(method_desc)
            self._check_response_shape(stub, call_type, name)

            key = (stub.service, stub.method)
            # First stub for this (service, method) seeds both tables; later
            # stubs append to the existing inner dict. Insertion order is
            # preserved across stubs (Python 3.7+ dict semantics) and within
            # each inner dict — the contract Task 5's dispatch relies on.
            if key not in stubs_by_method:
                stubs_by_method[key] = {}
                method_meta[key] = (
                    method_desc.input_type,
                    method_desc.output_type,
                    call_type,
                )
            stubs_by_method[key][name] = stub
            services_seen.add(stub.service)

        # 3. Expose the resolved state. ``_server`` stays None until Task 7's
        #    serve_forever actually binds.
        self.stubs_by_method: dict[tuple[str, str], dict[str, GrpcStub]] = (
            stubs_by_method
        )
        self.method_meta: dict[tuple[str, str], tuple[Any, Any, str]] = (
            method_meta
        )
        self.services: list[str] = sorted(services_seen)
        # Stash the resolved pool so Task 7's reflection registration can pass
        # the SAME pool to ``reflection.enable_server`` — reflection needs the
        # proto descriptors to answer ``ServerReflectionInfo`` calls, and our
        # pool is a fresh ``DescriptorPool()`` (not the Default), so reflection
        # would otherwise return empty/unknown-symbol responses.
        self._descriptor_pool: Any = pool
        self._listen_host, self._listen_port = parse_listen(config.listen)
        self._config = config
        self._emit_event = emit_event
        self._server: Any = None  # grpc.Server — built in Task 7's serve_forever.

    # -- validation helpers -------------------------------------------------

    @staticmethod
    def _resolve_method(pool: Any, stub: GrpcStub, name: str) -> Any:
        """``find_method`` with a ConfigError wrap naming ``mocks.grpc.stubs.<name>``.

        The kernel raises :class:`TemplateNotFound` on a missing service or
        method; that is a config-shaped failure (exit 2), but the path prefix
        ``mocks.grpc.stubs.<name>`` is the contract Task 6 owns, so we wrap.
        """
        try:
            return find_method(pool, stub.service, stub.method)
        except TemplateNotFound as exc:
            raise ConfigError(
                f"mocks.grpc.stubs.{name}: {exc.message}",
                {
                    "stub": name,
                    "service": stub.service,
                    "method": stub.method,
                    "path": f"mocks.grpc.stubs.{name}",
                    **exc.detail,
                },
            ) from exc

    @staticmethod
    def _check_response_shape(
        stub: GrpcStub, call_type: str, name: str
    ) -> None:
        """Response-shape vs derived call type.

        - ``server_stream`` -> ``response.messages`` set (``message`` unset) and
          non-empty (an empty sequence is almost certainly an authoring
          mistake — fail loud rather than silently streaming nothing).
        - ``unary``/``client_stream``/``bidi`` -> ``response.message`` set
          (``messages`` unset).

        Structural exactly-one-of is already enforced at model parse time
        (Task 3's :class:`GrpcResponse`); this check pins the call-type side
        of the contract — the only place the descriptor pool is available.
        """
        response = stub.response
        path = f"mocks.grpc.stubs.{name}.response"
        method_loc = f"{stub.service}/{stub.method}"

        if call_type == "server_stream":
            if response.message is not None or response.messages is None:
                raise ConfigError(
                    f"{path}: server_stream method {method_loc} requires "
                    f"'response.messages' (got 'response.message')",
                    {
                        "stub": name,
                        "call_type": call_type,
                        "service": stub.service,
                        "method": stub.method,
                        "path": path,
                    },
                )
            if len(response.messages) == 0:
                raise ConfigError(
                    f"{path}: server_stream method {method_loc} requires at "
                    f"least one entry in 'response.messages' "
                    f"(got an empty list)",
                    {
                        "stub": name,
                        "call_type": call_type,
                        "service": stub.service,
                        "method": stub.method,
                        "path": path,
                    },
                )
            return

        # unary / client_stream / bidi -> message-shaped response.
        if response.message is None or response.messages is not None:
            raise ConfigError(
                f"{path}: {call_type} method {method_loc} requires "
                f"'response.message' (got 'response.messages')",
                {
                    "stub": name,
                    "call_type": call_type,
                    "service": stub.service,
                    "method": stub.method,
                    "path": path,
                },
            )

    # -- accessors ----------------------------------------------------------

    @property
    def listen_address(self) -> str:
        """Human-readable ``host:port`` (for logs / status lines)."""
        return f"{self._listen_host}:{self._listen_port}"

    @property
    def bind_address(self) -> tuple[str, int]:
        """``(host, port)`` tuple for the engine's started line / socket bind."""
        return (self._listen_host, self._listen_port)

    # ------------------------------------------------------------------
    # Task 7: gRPC runtime wiring — generic servicer, 4 call types,
    # Health, Reflection, lifecycle.
    #
    # Every ``import grpc`` / ``grpc_health`` / ``grpc_reflection`` lives
    # INSIDE a method body so the module stays grpcio-free at module top
    # (the AST test pins this). A missing optional dependency surfaces as
    # :class:`ConfigError` pointing at ``pip install 'agctl[grpc]'`` via
    # :meth:`_require_grpc`.
    # ------------------------------------------------------------------

    _GRPC_EXTRA_HINT = (
        "gRPC support requires the 'grpc' extra: pip install 'agctl[grpc]'"
    )

    def _require_grpc(self, module_name: str):
        """Lazy-import a grpc-runtime module or raise :class:`ConfigError`.

        Mirrors :func:`agctl.clients.grpc_descriptors._require` but local to
        the mock server so we don't reach across to a private name. Used for
        ``grpc`` / ``grpc_health.v1.health`` / ``grpc_reflection.v1alpha.reflection``
        inside :meth:`_build_server`.
        """
        import importlib

        try:
            return importlib.import_module(module_name)
        except ImportError as exc:
            raise ConfigError(
                self._GRPC_EXTRA_HINT,
                {"missing_module": module_name},
            ) from exc

    def _build_server(self):
        """Build the ``grpc.Server`` with one generic handler per service.

        For every ``(service, method)`` in :attr:`method_meta`, the right
        ``RpcMethodHandler`` shape (``unary_unary``/``unary_stream``/
        ``stream_unary``/``stream_stream``) is created and wired with the
        kernel's :func:`serialize`/:func:`deserialize` for that method's
        input/output message descriptors + a behavior callable that dispatches
        via :func:`dispatch_grpc`. Handlers are grouped per service and
        registered via ``grpc.method_handlers_generic_handler`` (the same
        pattern ``grpc_health``/``grpc_reflection`` use internally).

        Health (``config.health``) and Reflection (``config.reflection``) are
        auto-served on top. Returns the unbound ``grpc.Server`` (caller binds
        via ``add_insecure_port`` in :meth:`start`).
        """
        import concurrent.futures

        grpc = self._require_grpc("grpc")
        from ..clients.grpc_descriptors import deserialize, serialize

        server = grpc.server(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=self._config.concurrency_cap
            )
        )

        # Group methods by service so we register one generic handler per
        # service (the grpcio DictionaryGenericHandler keys methods by
        # fully-qualified "/svc/mtd" inside one service's namespace).
        methods_by_service: dict[str, dict[str, object]] = {}
        for (svc, mtd), (
            input_desc,
            output_desc,
            call_type,
        ) in self.method_meta.items():
            ser = serialize(output_desc)
            deser = deserialize(input_desc)
            behavior = self._make_behavior(grpc, svc, mtd, call_type)
            if call_type == "unary":
                handler = grpc.unary_unary_rpc_method_handler(
                    behavior,
                    request_deserializer=deser,
                    response_serializer=ser,
                )
            elif call_type == "server_stream":
                handler = grpc.unary_stream_rpc_method_handler(
                    behavior,
                    request_deserializer=deser,
                    response_serializer=ser,
                )
            elif call_type == "client_stream":
                handler = grpc.stream_unary_rpc_method_handler(
                    behavior,
                    request_deserializer=deser,
                    response_serializer=ser,
                )
            elif call_type == "bidi":
                handler = grpc.stream_stream_rpc_method_handler(
                    behavior,
                    request_deserializer=deser,
                    response_serializer=ser,
                )
            else:  # pragma: no cover - call_type_of only returns the four above
                raise ConfigError(
                    f"unknown call type {call_type!r} for {svc}/{mtd}",
                    {"service": svc, "method": mtd, "call_type": call_type},
                )
            methods_by_service.setdefault(svc, {})[mtd] = handler

        for svc, handlers in methods_by_service.items():
            generic = grpc.method_handlers_generic_handler(svc, handlers)
            server.add_generic_rpc_handlers((generic,))

        # Health: register the v1 Health servicer and mark every configured
        # service (plus the overall "" key) as SERVING.
        if self._config.health:
            health_module = self._require_grpc("grpc_health.v1.health")
            health_pb2 = self._require_grpc("grpc_health.v1.health_pb2")
            health_pb2_grpc = self._require_grpc(
                "grpc_health.v1.health_pb2_grpc"
            )
            health_servicer = health_module.HealthServicer()
            health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
            for service_name in self.services:
                health_servicer.set(
                    service_name, health_pb2.HealthCheckResponse.SERVING
                )
            # Overall server health (empty service name).
            health_servicer.set(
                "", health_pb2.HealthCheckResponse.SERVING
            )

        # Reflection: enable_server_reflection adds the v1alpha ServerReflection
        # servicer AND introspects the supplied service_names to answer
        # list_services. Pass our pool so reflection can resolve the proto
        # symbols we built from config.descriptors (we use a fresh
        # DescriptorPool, not Default()).
        if self._config.reflection:
            reflection = self._require_grpc("grpc_reflection.v1alpha.reflection")
            service_names = list(self.services) + [reflection.SERVICE_NAME]
            reflection.enable_server_reflection(
                service_names, server, pool=self._descriptor_pool
            )

        return server

    def _make_behavior(self, grpc_module, svc: str, mtd: str, call_type: str):
        """Build the per-call-type behavior callable for ``(svc, mtd)``.

        Each behavior closes over ``grpc_module`` (so handlers — which run
        after :meth:`_build_server` returns — can call ``context.abort`` with
        a ``grpc.StatusCode`` member), the dispatch glue, and the event
        emitter. The actual per-call-type logic lives in the ``_handle_*``
        methods to keep the closures thin.
        """
        emit_capture_missing = self._make_capture_missing_emitter(svc, mtd)

        if call_type == "unary":
            def behavior(request, context):
                return self._handle_unary(
                    grpc_module, svc, mtd, request, context, emit_capture_missing
                )
            return behavior

        if call_type == "server_stream":
            def behavior(request, context):
                yield from self._handle_server_stream(
                    grpc_module, svc, mtd, request, context, emit_capture_missing
                )
            return behavior

        if call_type == "client_stream":
            def behavior(request_iterator, context):
                return self._handle_client_stream(
                    grpc_module,
                    svc,
                    mtd,
                    request_iterator,
                    context,
                    emit_capture_missing,
                )
            return behavior

        if call_type == "bidi":
            def behavior(request_iterator, context):
                yield from self._handle_bidi(
                    grpc_module,
                    svc,
                    mtd,
                    request_iterator,
                    context,
                    emit_capture_missing,
                )
            return behavior

        raise ConfigError(  # pragma: no cover - guarded above
            f"unknown call type {call_type!r}",
            {"call_type": call_type},
        )

    def _make_capture_missing_emitter(self, svc: str, mtd: str):
        """Build the ``emit_capture_missing`` callback for ``(svc, mtd)``.

        Signature: ``(stub_name, capture_name, from_path) -> None``. Emits one
        ``capture.missing`` event per call, mirroring the HTTP mock's event
        shape so downstream assertion tooling can treat them uniformly.
        """

        def emit(stub_name: str, capture_name: str, from_path: str) -> None:
            self._emit_event(
                {
                    "event": "capture.missing",
                    "stub": stub_name,
                    "service": svc,
                    "method": mtd,
                    "name": capture_name,
                    "from": from_path,
                }
            )

        return emit

    # -- per-call-type handlers -----------------------------------------

    @staticmethod
    def _metadata_to_dict(context) -> dict[str, str]:
        """Read invocation metadata off the ServicerContext into a plain dict.

        Keys are lowercased to match :func:`build_envelope`'s convention (and
        the HTTP request envelope's lowercased header keys).
        """
        try:
            pairs = context.invocation_metadata() or ()
        except Exception:
            return {}
        return {str(k).lower(): str(v) for k, v in pairs}

    @staticmethod
    def _grpc_status_code(grpc_module, name: str):
        """Resolve a status NAME (e.g. 'NOT_FOUND') to a grpc.StatusCode member."""
        return getattr(grpc_module.StatusCode, name)

    def _handle_unary(
        self,
        grpc_module,
        svc,
        mtd,
        request,
        context,
        emit_capture_missing,
    ):
        """Unary behavior: match one stub, return one rendered message dict."""
        start = time.time()
        metadata = self._metadata_to_dict(context)
        envelope = build_envelope(svc, mtd, metadata, message=request)
        stubs = self.stubs_by_method.get((svc, mtd), {})
        try:
            outcome = dispatch_grpc(
                stubs,
                envelope,
                "unary",
                emit_capture_missing=emit_capture_missing,
            )
        except Exception as exc:
            self._emit_event(
                {
                    "event": "grpc.error",
                    "stub": None,
                    "service": svc,
                    "method": mtd,
                    "call_type": "unary",
                    "error": str(exc),
                    "fatal": True,
                }
            )
            context.abort(grpc_module.StatusCode.INTERNAL, str(exc))
            return None  # unreachable: abort raises

        if not outcome.matched:
            self._emit_event(
                {
                    "event": "grpc.unmatched",
                    "service": svc,
                    "method": mtd,
                    "call_type": "unary",
                }
            )
            context.abort(
                grpc_module.StatusCode.UNIMPLEMENTED,
                f"no stub matched {svc}/{mtd}",
            )
            return None  # unreachable

        stub_name = outcome.stub_name
        # Per-stub delay (unary/client_stream/bidi share the single-message form).
        stub = stubs.get(stub_name)
        if stub is not None and stub.delay_ms > 0:
            time.sleep(stub.delay_ms / 1000.0)

        duration_ms = int((time.time() - start) * 1000)
        self._emit_event(
            {
                "event": "grpc.hit",
                "stub": stub_name,
                "service": svc,
                "method": mtd,
                "call_type": "unary",
                "status": outcome.status[1] if outcome.status else "OK",
                "duration_ms": duration_ms,
            }
        )

        if outcome.status is not None and outcome.status[0] != 0:
            context.abort(
                self._grpc_status_code(grpc_module, outcome.status[1]),
                outcome.status[1],
            )
            return None  # unreachable

        return outcome.messages[0] if outcome.messages else {}

    def _handle_server_stream(
        self,
        grpc_module,
        svc,
        mtd,
        request,
        context,
        emit_capture_missing,
    ):
        """Server-stream behavior: yield each rendered message, then terminal status."""
        start = time.time()
        metadata = self._metadata_to_dict(context)
        envelope = build_envelope(svc, mtd, metadata, message=request)
        stubs = self.stubs_by_method.get((svc, mtd), {})
        try:
            outcome = dispatch_grpc(
                stubs,
                envelope,
                "server_stream",
                emit_capture_missing=emit_capture_missing,
            )
        except Exception as exc:
            self._emit_event(
                {
                    "event": "grpc.error",
                    "stub": None,
                    "service": svc,
                    "method": mtd,
                    "call_type": "server_stream",
                    "error": str(exc),
                    "fatal": True,
                }
            )
            context.abort(grpc_module.StatusCode.INTERNAL, str(exc))
            return

        if not outcome.matched:
            self._emit_event(
                {
                    "event": "grpc.unmatched",
                    "service": svc,
                    "method": mtd,
                    "call_type": "server_stream",
                }
            )
            context.abort(
                grpc_module.StatusCode.UNIMPLEMENTED,
                f"no stub matched {svc}/{mtd}",
            )
            return

        # Per-message delay_ms: read off the matched stub's authored sequence
        # (dispatch_grpc returned rendered dicts; the delay metadata stays on
        # stub.response.messages). If the user authored fewer entries than
        # dispatch rendered (shouldn't happen — same source), pad with 0.
        stub = stubs.get(outcome.stub_name)
        authored_delays = (
            [entry.delay_ms for entry in (stub.response.messages or [])]
            if stub is not None
            else []
        )

        for idx, rendered in enumerate(outcome.messages):
            delay_ms = (
                authored_delays[idx]
                if idx < len(authored_delays)
                else 0
            )
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
            duration_ms = int((time.time() - start) * 1000)
            self._emit_event(
                {
                    "event": "grpc.hit",
                    "stub": outcome.stub_name,
                    "service": svc,
                    "method": mtd,
                    "call_type": "server_stream",
                    "status": outcome.status[1] if outcome.status else "OK",
                    "duration_ms": duration_ms,
                }
            )
            yield rendered

        if outcome.status is not None and outcome.status[0] != 0:
            context.abort(
                self._grpc_status_code(grpc_module, outcome.status[1]),
                outcome.status[1],
            )

    def _handle_client_stream(
        self,
        grpc_module,
        svc,
        mtd,
        request_iterator,
        context,
        emit_capture_missing,
    ):
        """Client-stream behavior: aggregate requests, match once, return one reply."""
        start = time.time()
        # Drain the iterator into a list. Each element is a deserialized dict
        # (the handler's request_deserializer already converted bytes -> dict).
        messages: list = list(request_iterator)
        metadata = self._metadata_to_dict(context)
        envelope = build_envelope(svc, mtd, metadata, messages=messages)
        stubs = self.stubs_by_method.get((svc, mtd), {})
        try:
            outcome = dispatch_grpc(
                stubs,
                envelope,
                "client_stream",
                emit_capture_missing=emit_capture_missing,
            )
        except Exception as exc:
            self._emit_event(
                {
                    "event": "grpc.error",
                    "stub": None,
                    "service": svc,
                    "method": mtd,
                    "call_type": "client_stream",
                    "error": str(exc),
                    "fatal": True,
                }
            )
            context.abort(grpc_module.StatusCode.INTERNAL, str(exc))
            return None

        if not outcome.matched:
            self._emit_event(
                {
                    "event": "grpc.unmatched",
                    "service": svc,
                    "method": mtd,
                    "call_type": "client_stream",
                }
            )
            context.abort(
                grpc_module.StatusCode.UNIMPLEMENTED,
                f"no stub matched {svc}/{mtd}",
            )
            return None

        stub = stubs.get(outcome.stub_name)
        if stub is not None and stub.delay_ms > 0:
            time.sleep(stub.delay_ms / 1000.0)

        duration_ms = int((time.time() - start) * 1000)
        self._emit_event(
            {
                "event": "grpc.hit",
                "stub": outcome.stub_name,
                "service": svc,
                "method": mtd,
                "call_type": "client_stream",
                "status": outcome.status[1] if outcome.status else "OK",
                "duration_ms": duration_ms,
            }
        )

        if outcome.status is not None and outcome.status[0] != 0:
            context.abort(
                self._grpc_status_code(grpc_module, outcome.status[1]),
                outcome.status[1],
            )
            return None

        return outcome.messages[0] if outcome.messages else {}

    def _handle_bidi(
        self,
        grpc_module,
        svc,
        mtd,
        request_iterator,
        context,
        emit_capture_missing,
    ):
        """Bidi behavior: per incoming request, match+render; yield one response per turn."""
        # Per-turn start time; duration_ms is measured per yielded response.
        metadata = self._metadata_to_dict(context)
        stubs = self.stubs_by_method.get((svc, mtd), {})
        for request in request_iterator:
            start = time.time()
            envelope = build_envelope(svc, mtd, metadata, message=request)
            try:
                outcome = dispatch_grpc(
                    stubs,
                    envelope,
                    "bidi",
                    emit_capture_missing=emit_capture_missing,
                )
            except Exception as exc:
                self._emit_event(
                    {
                        "event": "grpc.error",
                        "stub": None,
                        "service": svc,
                        "method": mtd,
                        "call_type": "bidi",
                        "error": str(exc),
                        "fatal": True,
                    }
                )
                context.abort(grpc_module.StatusCode.INTERNAL, str(exc))
                return

            if not outcome.matched:
                # Per the brief: emit grpc.unmatched and SKIP this turn (no
                # response for this incoming request), continue the stream.
                self._emit_event(
                    {
                        "event": "grpc.unmatched",
                        "service": svc,
                        "method": mtd,
                        "call_type": "bidi",
                    }
                )
                continue

            stub = stubs.get(outcome.stub_name)
            if stub is not None and stub.delay_ms > 0:
                time.sleep(stub.delay_ms / 1000.0)

            duration_ms = int((time.time() - start) * 1000)
            self._emit_event(
                {
                    "event": "grpc.hit",
                    "stub": outcome.stub_name,
                    "service": svc,
                    "method": mtd,
                    "call_type": "bidi",
                    "status": outcome.status[1] if outcome.status else "OK",
                    "duration_ms": duration_ms,
                }
            )

            # Bidi yields exactly one response per matched request.
            for rendered in outcome.messages:
                yield rendered

            if outcome.status is not None and outcome.status[0] != 0:
                context.abort(
                    self._grpc_status_code(grpc_module, outcome.status[1]),
                    outcome.status[1],
                )
                return

    # -- lifecycle ------------------------------------------------------

    def start(self) -> None:
        """Build + bind + start the underlying ``grpc.Server``.

        - ``add_insecure_port`` returns the actually-bound port; for an
          ephemeral request (``listen`` port 0) we record the assigned port
          so :meth:`actual_listen` reports it.
        - Bind failure (port already in use, permission denied, etc.)
          surfaces as :class:`ConfigError` with the listen address in
          ``detail`` so the user can locate the stale process.
        """
        if self._server is not None:
            return  # already started; idempotent.
        self._server = self._build_server()
        try:
            bound_port = self._server.add_insecure_port(self.listen_address)
        except (OSError, RuntimeError) as exc:
            # Explicitly tear down the C-core grpc.Server we just built.
            # ``server.start()`` hasn't run, so the ThreadPoolExecutor has no
            # live workers — but the underlying C core still owns a Server
            # object whose cleanup we shouldn't defer to GC.
            self._server.stop(grace=0)
            self._server = None
            raise ConfigError(
                f"grpc listen address {self.listen_address} already in use; "
                f"kill the stale mock or pick another port: {exc}",
                {"listen": self.listen_address},
            ) from exc
        if self._listen_port == 0:
            self._listen_port = bound_port
        self._server.start()

    def serve_forever(self, stop_event: threading.Event) -> None:
        """Block until ``stop_event`` is set, polling termination every 200ms.

        Uses ``wait_for_termination(timeout=0.2)`` so the loop is interruptible
        by the engine's stop signal (a direct ``wait_for_termination()`` with
        no timeout would block forever and ignore the event).
        """
        if self._server is None:
            return
        while not stop_event.is_set():
            self._server.wait_for_termination(timeout=0.2)

    def shutdown(self) -> None:
        """Stop the underlying server with a 2s grace period; idempotent."""
        if self._server is None:
            return
        try:
            self._server.stop(grace=2)
            self._server.wait_for_termination(timeout=2)
        finally:
            self._server = None

    def actual_listen(self) -> str:
        """The bound ``host:port`` (after :meth:`start`, for ephemeral-port tests)."""
        return f"{self._listen_host}:{self._listen_port}"
