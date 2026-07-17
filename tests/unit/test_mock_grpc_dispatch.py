"""Tests for the transport-agnostic gRPC dispatch core (Task 5).

``dispatch_grpc`` is the pure brain that, given a resolved method's call type +
the stub list for that method + the deserialized request(s), picks a stub
(first-match-wins), runs match/capture/render, and produces the response
message(s) + terminal status, or signals UNIMPLEMENTED. ``build_envelope``
shapes the per-call-type envelope that ``match.jq`` and ``capture.from`` are
rooted at.

This module must remain grpcio-free: the module under test
(``agctl.mock.grpc_server``) imports nothing from ``grpc``/``grpcio`` so the
whole dispatch pipeline is unit-testable without the gRPC extra. Only the
``jq``-dependent tests skip when the optional ``jq`` library is missing — they
call ``pytest.importorskip("jq")`` per-test, mirroring
``test_grpc_assertions.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from agctl.config.models import (
    CaptureSpec,
    GrpcMatch,
    GrpcResponse,
    GrpcResponseMessage,
    GrpcStub,
)
from agctl.mock.grpc_server import (
    GrpcDispatchOutcome,
    build_envelope,
    dispatch_grpc,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_unary_stub(
    *,
    match: GrpcMatch | None = None,
    capture: dict[str, CaptureSpec] | None = None,
    message: Any = None,
    status: str | int = "OK",
) -> GrpcStub:
    """Build a minimal unary-style GrpcStub (response.message set)."""
    return GrpcStub(
        service="echo.EchoService",
        method="Unary",
        match=match,
        capture=capture,
        response=GrpcResponse(
            message=message if message is not None else {}, status=status
        ),
    )


def _make_stream_stub(
    *,
    match: GrpcMatch | None = None,
    capture: dict[str, CaptureSpec] | None = None,
    messages: list[GrpcResponseMessage] | None = None,
    status: str | int = "OK",
) -> GrpcStub:
    """Build a server-stream-style GrpcStub (response.messages set)."""
    return GrpcStub(
        service="echo.EchoService",
        method="ServerStream",
        match=match,
        capture=capture,
        response=GrpcResponse(
            messages=messages if messages is not None else [],
            status=status,
        ),
    )


def _recording_callback() -> tuple[list[tuple[str, str, str]], Any]:
    """Build a list-recording fake for ``emit_capture_missing``.

    Returns ``(calls, fn)`` where ``calls`` collects ``(stub_name, cap_name,
    from_path)`` triples in invocation order.
    """
    calls: list[tuple[str, str, str]] = []

    def _fn(stub_name: str, cap_name: str, from_path: str) -> None:
        calls.append((stub_name, cap_name, from_path))

    return calls, _fn


# ---------------------------------------------------------------------------
# build_envelope
# ---------------------------------------------------------------------------


class TestBuildEnvelope:
    """``build_envelope`` shapes the per-call-type envelope (§8.1)."""

    def test_unary_envelope_has_message(self):
        """unary/server_stream/bidi: envelope carries the single `message`."""
        env = build_envelope(
            "echo.EchoService",
            "Unary",
            {"Authorization": "Bearer x"},
            message={"msg": "hi"},
        )
        assert env == {
            "service": "echo.EchoService",
            "method": "Unary",
            "metadata": {"authorization": "Bearer x"},  # lowercased
            "message": {"msg": "hi"},
        }

    def test_metadata_keys_lowercased(self):
        """Multi-key metadata: every key lowercased (mirrors HTTP headers)."""
        env = build_envelope(
            "svc",
            "Method",
            {"X-Trace-Id": "abc", "CONTENT-TYPE": "application/grpc"},
            message={},
        )
        assert env["metadata"] == {
            "x-trace-id": "abc",
            "content-type": "application/grpc",
        }

    def test_client_stream_envelope_has_messages_and_count(self):
        """client_stream: envelope carries messages list + count (no `message`)."""
        env = build_envelope(
            "echo.EchoService",
            "ClientStream",
            {},
            messages=[{"x": 1}, {"x": 2}, {"x": 3}],
        )
        assert env == {
            "service": "echo.EchoService",
            "method": "ClientStream",
            "metadata": {},
            "messages": [{"x": 1}, {"x": 2}, {"x": 3}],
            "count": 3,
        }
        assert "message" not in env

    def test_client_stream_empty_messages_count_zero(self):
        """client_stream: empty messages list -> count == 0."""
        env = build_envelope("svc", "M", {}, messages=[])
        assert env["messages"] == []
        assert env["count"] == 0


# ---------------------------------------------------------------------------
# dispatch_grpc — match/body/jq
# ---------------------------------------------------------------------------


class TestDispatchMatch:
    """Match semantics: body subset, jq predicate, AND, first-match-wins."""

    def test_a_unary_body_subset_match_with_capture_substitution(self):
        """(a) match.body subset passes; {placeholder} substituted from capture.

        jq-gated: ``capture.from`` is evaluated by ``jq_value``, so this test
        transitively requires the jq extra (skips cleanly without it).
        """
        pytest.importorskip("jq")
        stub = _make_unary_stub(
            match=GrpcMatch(body={"order_id": "123"}),
            capture={"op_id": CaptureSpec.model_validate({"from": ".message.order_id"})},
            message={"echo": "order-{op_id}"},
        )
        env = build_envelope(
            "svc", "Unary", {}, message={"order_id": "123", "extra": "ignored"}
        )
        calls, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"echo": stub}, env, "unary", emit_capture_missing=cb
        )

        assert outcome.matched is True
        assert outcome.stub_name == "echo"
        assert outcome.messages == [{"echo": "order-123"}]
        assert calls == []  # capture resolved; no missing
        assert outcome.missing_captures == []

    def test_b_first_match_wins_when_first_predicate_fails(self):
        """(b) Two stubs, first.body fails, second matches -> second wins."""
        first = _make_unary_stub(
            match=GrpcMatch(body={"kind": "A"}),
            message={"echo": "from-strict"},
        )
        second = _make_unary_stub(
            match=GrpcMatch(body={"kind": "B"}),
            message={"echo": "from-fallback"},
        )
        env = build_envelope("svc", "Unary", {}, message={"kind": "B"})
        calls, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"strict": first, "fallback": second},
            env,
            "unary",
            emit_capture_missing=cb,
        )

        assert outcome.matched is True
        assert outcome.stub_name == "fallback"
        assert outcome.messages == [{"echo": "from-fallback"}]

    def test_c_match_jq_true_then_false(self):
        """(c) match.jq True -> match; False -> skip (no body to fall back on)."""
        pytest.importorskip("jq")
        stub_pass = _make_unary_stub(
            match=GrpcMatch(jq=".message.x == 1"),
            message={"echo": "pass"},
        )
        stub_fail = _make_unary_stub(
            match=GrpcMatch(jq=".message.x == 1"),
            message={"echo": "should-not-match"},
        )
        env_pass = build_envelope("svc", "Unary", {}, message={"x": 1})
        env_fail = build_envelope("svc", "Unary", {}, message={"x": 2})
        _, cb = _recording_callback()

        ok = dispatch_grpc(
            {"jp": stub_pass}, env_pass, "unary", emit_capture_missing=cb
        )
        no = dispatch_grpc(
            {"jf": stub_fail}, env_fail, "unary", emit_capture_missing=cb
        )

        assert ok.matched is True
        assert ok.stub_name == "jp"
        assert no.matched is False
        assert no.stub_name is None

    def test_d_both_body_and_jq_are_anded(self):
        """(d) body AND jq both set: both must pass; either fails -> skip."""
        pytest.importorskip("jq")
        stub = _make_unary_stub(
            match=GrpcMatch(body={"region": "us"}, jq=".message.amount > 100"),
            message={"echo": "ok"},
        )
        _, cb = _recording_callback()

        # Both pass
        both_pass = build_envelope(
            "svc", "Unary", {}, message={"region": "us", "amount": 200}
        )
        # body passes, jq fails
        jq_fails = build_envelope(
            "svc", "Unary", {}, message={"region": "us", "amount": 50}
        )
        # jq passes, body fails
        body_fails = build_envelope(
            "svc", "Unary", {}, message={"region": "eu", "amount": 200}
        )

        ok = dispatch_grpc({"both": stub}, both_pass, "unary", emit_capture_missing=cb)
        no1 = dispatch_grpc({"both": stub}, jq_fails, "unary", emit_capture_missing=cb)
        no2 = dispatch_grpc({"both": stub}, body_fails, "unary", emit_capture_missing=cb)

        assert ok.matched is True and ok.stub_name == "both"
        assert no1.matched is False
        assert no2.matched is False

    def test_e_no_match_returns_unmatched_outcome(self):
        """(e) All stubs fail -> matched=False, other fields cleared."""
        stub = _make_unary_stub(
            match=GrpcMatch(body={"missing": "key"}), message={"a": 1}
        )
        env = build_envelope("svc", "Unary", {}, message={"other": "value"})
        calls, cb = _recording_callback()

        outcome = dispatch_grpc({"x": stub}, env, "unary", emit_capture_missing=cb)

        assert outcome.matched is False
        assert outcome.stub_name is None
        assert outcome.messages == []
        assert outcome.status is None
        assert outcome.missing_captures == []
        assert calls == []

    def test_omitted_match_always_matches(self):
        """No match set -> stub matches unconditionally."""
        stub = _make_unary_stub(match=None, message={"echo": "ok"})
        env = build_envelope("svc", "Unary", {}, message={"anything": 1})
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"open": stub}, env, "unary", emit_capture_missing=cb
        )

        assert outcome.matched is True
        assert outcome.stub_name == "open"
        assert outcome.messages == [{"echo": "ok"}]

    def test_j_match_jq_runtime_error_is_soft_non_match(self):
        """(j) match.jq that raises at runtime -> non-match, no raise.

        ``jq_bool`` already swallows runtime errors to False; this test pins
        that the dispatcher trusts that contract (does not catch+re-raise) and
        proceeds to the next stub / falls through to unmatched.
        """
        pytest.importorskip("jq")
        stub = _make_unary_stub(
            match=GrpcMatch(jq='.message.x | error("forced")'),
            message={"echo": "should-not-reach"},
        )
        env = build_envelope("svc", "Unary", {}, message={"x": 1})
        _, cb = _recording_callback()

        # Must not raise; outcome is unmatched.
        outcome = dispatch_grpc(
            {"boom": stub}, env, "unary", emit_capture_missing=cb
        )
        assert outcome.matched is False


# ---------------------------------------------------------------------------
# dispatch_grpc — per-call-type response selection
# ---------------------------------------------------------------------------


class TestDispatchResponseTypeSelection:
    """Per-call-type response message selection trusts the call_type arg."""

    def test_f_server_stream_renders_each_message(self):
        """(f) server_stream: messages list -> N rendered entries."""
        stub = _make_stream_stub(
            messages=[
                GrpcResponseMessage(message={"chunk": 1}),
                GrpcResponseMessage(message={"chunk": 2}, delay_ms=5),
                GrpcResponseMessage(message={"chunk": 3}),
            ],
        )
        env = build_envelope("svc", "ServerStream", {}, message={"start": 0})
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"stream": stub}, env, "server_stream", emit_capture_missing=cb
        )

        assert outcome.matched is True
        assert outcome.stub_name == "stream"
        assert outcome.messages == [{"chunk": 1}, {"chunk": 2}, {"chunk": 3}]

    def test_client_stream_uses_response_message_singular(self):
        """client_stream: dispatch uses response.message -> messages=[rendered]."""
        stub = _make_unary_stub(
            message={"received": True},
        )
        env = build_envelope("svc", "ClientStream", {}, messages=[{"x": 1}, {"x": 2}])
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"agg": stub}, env, "client_stream", emit_capture_missing=cb
        )

        assert outcome.matched is True
        assert outcome.messages == [{"received": True}]

    def test_g_client_stream_match_jq_rooted_at_messages_last(self):
        """(g) client_stream: match.jq at .messages[-1].x works."""
        pytest.importorskip("jq")
        stub = _make_unary_stub(
            match=GrpcMatch(jq=".messages[-1].x == 2"),
            message={"ok": True},
        )
        env = build_envelope("svc", "ClientStream", {}, messages=[{"x": 1}, {"x": 2}])
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"last": stub}, env, "client_stream", emit_capture_missing=cb
        )
        assert outcome.matched is True
        assert outcome.messages == [{"ok": True}]

    def test_client_stream_skips_match_body(self):
        """client_stream: match.body is NOT applicable; only jq runs.

        A stub with match.body that would not subset any single message still
        matches via match.jq (the body predicate is skipped entirely per the
        design decision in the task brief).
        """
        pytest.importorskip("jq")
        stub = _make_unary_stub(
            match=GrpcMatch(
                body={"would_not_subset": "anything"}, jq=".count == 2"
            ),
            message={"ok": True},
        )
        env = build_envelope("svc", "ClientStream", {}, messages=[{"a": 1}, {"b": 2}])
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"cs": stub}, env, "client_stream", emit_capture_missing=cb
        )
        assert outcome.matched is True
        assert outcome.stub_name == "cs"

    def test_bidi_uses_response_message(self):
        """bidi: per-turn dispatch uses response.message -> one rendered message."""
        stub = _make_unary_stub(
            match=GrpcMatch(body={"turn": 1}),
            message={"reply": "turn-1"},
        )
        env = build_envelope("svc", "Bidi", {}, message={"turn": 1})
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"bidi-pair": stub}, env, "bidi", emit_capture_missing=cb
        )
        assert outcome.matched is True
        assert outcome.messages == [{"reply": "turn-1"}]


# ---------------------------------------------------------------------------
# dispatch_grpc — capture + status
# ---------------------------------------------------------------------------


class TestDispatchCaptureAndStatus:
    """Capture resolution, missing-capture callback, status resolution."""

    def test_h_capture_from_null_emits_missing_and_substitutes_empty(self):
        """(h) capture.from resolves to null -> callback called once with
        (stub_name, cap_name, from_path); rendered value substitutes ""."""
        pytest.importorskip("jq")
        stub = _make_unary_stub(
            capture={"missing_id": CaptureSpec.model_validate({"from": ".message.nope"})},
            message={"echo": "id=[{missing_id}]"},
        )
        env = build_envelope("svc", "Unary", {}, message={"other": 1})
        calls, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"cap-stub": stub}, env, "unary", emit_capture_missing=cb
        )

        assert outcome.matched is True
        # callback called exactly once with the right triple
        assert len(calls) == 1
        assert calls[0] == ("cap-stub", "missing_id", ".message.nope")
        # outcome carries the missing pair too
        assert outcome.missing_captures == [("missing_id", ".message.nope")]
        # rendered value substitutes "" for the null capture
        assert outcome.messages == [{"echo": "id=[]"}]

    def test_h_no_capture_means_no_callback(self):
        """Stub without capture: callback never invoked, missing_captures empty."""
        stub = _make_unary_stub(message={"a": 1})
        env = build_envelope("svc", "Unary", {}, message={"x": 1})
        calls, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"plain": stub}, env, "unary", emit_capture_missing=cb
        )
        assert outcome.matched is True
        assert calls == []
        assert outcome.missing_captures == []

    def test_i_status_name_resolved_to_tuple(self):
        """(i) status='NOT_FOUND' -> outcome.status == (5, 'NOT_FOUND')."""
        stub = _make_unary_stub(message={"ok": False}, status="NOT_FOUND")
        env = build_envelope("svc", "Unary", {}, message={})
        _, cb = _recording_callback()

        outcome = dispatch_grpc(
            {"nf": stub}, env, "unary", emit_capture_missing=cb
        )
        assert outcome.status == (5, "NOT_FOUND")

    def test_status_default_ok_resolves_to_zero_tuple(self):
        """Default status='OK' -> outcome.status == (0, 'OK')."""
        stub = _make_unary_stub(message={"a": 1})  # status defaults to "OK"
        env = build_envelope("svc", "Unary", {}, message={})
        _, cb = _recording_callback()

        outcome = dispatch_grpc({"ok": stub}, env, "unary", emit_capture_missing=cb)
        assert outcome.status == (0, "OK")

    def test_status_int_code_resolved(self):
        """status=5 (int) -> outcome.status == (5, 'NOT_FOUND')."""
        stub = _make_unary_stub(message={"a": 1}, status=5)
        env = build_envelope("svc", "Unary", {}, message={})
        _, cb = _recording_callback()

        outcome = dispatch_grpc({"code": stub}, env, "unary", emit_capture_missing=cb)
        assert outcome.status == (5, "NOT_FOUND")

    def test_status_digit_string_resolved(self):
        """status='5' (digit-string) -> coerced to 5 -> (5, 'NOT_FOUND')."""
        stub = _make_unary_stub(message={"a": 1}, status="5")
        env = build_envelope("svc", "Unary", {}, message={})
        _, cb = _recording_callback()

        outcome = dispatch_grpc({"ds": stub}, env, "unary", emit_capture_missing=cb)
        assert outcome.status == (5, "NOT_FOUND")


# ---------------------------------------------------------------------------
# GrpcDispatchOutcome dataclass shape
# ---------------------------------------------------------------------------


class TestGrpcDispatchOutcomeShape:
    """The outcome dataclass has the load-bearing fields and defaults."""

    def test_matched_outcome_carries_all_fields(self):
        """A matched outcome exposes matched/stub_name/messages/status/missing_captures."""
        stub = _make_unary_stub(message={"a": 1})
        env = build_envelope("svc", "Unary", {}, message={})
        _, cb = _recording_callback()

        outcome = dispatch_grpc({"s": stub}, env, "unary", emit_capture_missing=cb)

        assert isinstance(outcome, GrpcDispatchOutcome)
        assert outcome.matched is True
        assert outcome.stub_name == "s"
        assert isinstance(outcome.messages, list)
        assert outcome.status == (0, "OK")
        assert isinstance(outcome.missing_captures, list)

    def test_unmatched_outcome_fields_cleared(self):
        """An unmatched outcome: matched=False, stub_name=None, messages=[],
        status=None, missing_captures=[] per the brief's contract."""
        stub = _make_unary_stub(
            match=GrpcMatch(body={"x": 1}), message={"a": 1}
        )
        env = build_envelope("svc", "Unary", {}, message={"y": 2})
        _, cb = _recording_callback()

        outcome = dispatch_grpc({"never": stub}, env, "unary", emit_capture_missing=cb)

        assert outcome.matched is False
        assert outcome.stub_name is None
        assert outcome.messages == []
        assert outcome.status is None
        assert outcome.missing_captures == []

    def test_empty_stub_map_returns_unmatched(self):
        """Empty stub map (no stubs registered for method) -> unmatched."""
        env = build_envelope("svc", "Unary", {}, message={"x": 1})
        _, cb = _recording_callback()

        outcome = dispatch_grpc({}, env, "unary", emit_capture_missing=cb)
        assert outcome.matched is False
        assert outcome.stub_name is None


# ---------------------------------------------------------------------------
# Module is grpcio-free (load-bearing: dispatch must be unit-testable)
# ---------------------------------------------------------------------------


def test_grpc_server_module_does_not_import_grpc():
    """The dispatch module must NOT import grpc/grpcio at module top —
    the dispatcher is unit-testable without the gRPC extra. Task 6/7 will add
    a MockGrpcServer that imports grpc lazily INSIDE its methods."""
    import agctl.mock.grpc_server as mod

    src = open(mod.__file__).read()
    # Crude but load-bearing: any module-top ``import grpc``/``from grpc``/
    # ``import grpcio`` would break the unit-testable-without-grpcio contract.
    for forbidden in ("import grpc", "from grpc", "import grpcio"):
        assert forbidden not in src, (
            f"agctl/mock/grpc_server.py must remain grpcio-free at module "
            f"top (found {forbidden!r}); Task 6/7 imports grpc lazily inside "
            f"MockGrpcServer methods."
        )
