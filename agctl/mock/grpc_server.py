"""Transport-agnostic gRPC dispatch core (Task 5 of the gRPC mock server).

This module is the **brain** of the gRPC mock: given a resolved method's call
type, the stub list registered for that ``(service, method)`` pair, and the
deserialized request envelope, it picks a stub (first-match-wins), runs
match/capture/render, and produces the response message(s) + terminal status,
or signals UNIMPLEMENTED (``matched=False``).

It is **pure Python with no ``grpc`` import** anywhere in the file. The
dispatch functions are importable and unit-testable without the grpcio extra
installed; this keeps the mock brain fully covered by the fast unit suite,
independent of the gRPC transport. Task 6/7 will add ``MockGrpcServer`` to
this same module, importing ``grpc`` **lazily inside its methods** (never at
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

from dataclasses import dataclass, field
from typing import Any, Callable

from ..assertions import jq_bool, json_subset, parse_grpc_status
from ..config.models import GrpcStub
from ..resolution import render_typed
from .capture import resolve_captures

__all__ = [
    "GrpcDispatchOutcome",
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
