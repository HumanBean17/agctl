"""Tests for the mock jq-expression walker + validate-error collector (Task 4).

The walker ``iter_mock_jq_expressions`` walks a :class:`MocksConfig` and yields
every jq match expression (HTTP stub ``match.jq`` and Kafka reactor ``match``)
with a stable path label. ``collect_jq_compile_errors`` calls :func:`compile_jq`
on each, catching :class:`ConfigError` so ``config validate`` can report ALL
typos in one pass rather than stopping at the first.
"""

from agctl.config.models import (
    CaptureSpec,
    GrpcMatch,
    GrpcMockConfig,
    GrpcResponse,
    GrpcResponseMessage,
    GrpcStub,
    HttpMatch,
    HttpMockConfig,
    HttpResponse,
    HttpStub,
    KafkaMockConfig,
    KafkaReaction,
    KafkaReactor,
    MocksConfig,
)
from agctl.mock.jq_precompile import (
    collect_jq_compile_errors,
    iter_mock_jq_expressions,
)


# --- (a) None guard --------------------------------------------------------
def test_iter_none_yields_nothing():
    """iter_mock_jq_expressions(None) yields nothing (no http/kafka to walk)."""
    assert list(iter_mock_jq_expressions(None)) == []


def test_collect_none_returns_empty_list():
    """collect_jq_compile_errors(None) -> [] (nothing to validate)."""
    assert collect_jq_compile_errors(None) == []


# --- (b) one stub + one reactor -> two correct (label, expr) pairs ---------
def test_iter_one_stub_one_reactor_yields_two_pairs():
    """One HTTP stub with match.jq='.a>1' and one Kafka reactor with
    match='.b==2' -> exactly two (label, expr) pairs, stubs first then reactors,
    with the exact path labels from the contract."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "create-order": HttpStub(
                    method="POST",
                    path="/orders",
                    match=HttpMatch(jq=".a>1"),
                    response=HttpResponse(status=201),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "order-created": KafkaReactor(
                    topic="orders.created",
                    match=".b==2",
                    reaction=KafkaReaction(topic="orders.mock", value={"ok": True}),
                ),
            },
        ),
    )
    pairs = list(iter_mock_jq_expressions(mocks))
    assert pairs == [
        ("mocks.http.stubs.create-order.match.jq", ".a>1"),
        ("mocks.kafka.reactors.order-created.match", ".b==2"),
    ]


# --- (c) body-only stub / match=None reactor skipped -----------------------
def test_iter_skips_none_expressions():
    """A stub whose match has only ``body`` (jq None) and a reactor whose
    ``match`` is None are both skipped — neither yields a pair."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "body-only": HttpStub(
                    method="GET",
                    path="/x",
                    match=HttpMatch(body={"x": 1}),  # jq is None
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "no-match": KafkaReactor(
                    topic="t",
                    match=None,  # explicitly None
                    reaction=KafkaReaction(topic="out", value=1),
                ),
            },
        ),
    )
    assert list(iter_mock_jq_expressions(mocks)) == []


def test_iter_skips_stub_with_no_match_at_all():
    """A stub with ``match=None`` (no HttpMatch object at all) is skipped."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "bare": HttpStub(
                    method="GET",
                    path="/x",
                    match=None,
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=None,
    )
    assert list(iter_mock_jq_expressions(mocks)) == []


def test_iter_http_none_kafka_present():
    """``MocksConfig(http=None, kafka=...)`` walks only the kafka reactors."""
    mocks = MocksConfig(
        http=None,
        kafka=KafkaMockConfig(
            reactors={
                "r": KafkaReactor(
                    topic="t",
                    match=".v==1",
                    reaction=KafkaReaction(topic="out", value=1),
                ),
            },
        ),
    )
    assert list(iter_mock_jq_expressions(mocks)) == [
        ("mocks.kafka.reactors.r.match", ".v==1"),
    ]


# --- (d) collect returns one error for a malformed stub -------------------
def test_collect_one_malformed_stub():
    """collect_jq_compile_errors on a config with a malformed stub
    match.jq=')(' -> a one-element list whose ``path`` is the stub's jq label
    and whose ``message`` carries the underlying error text."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "bad": HttpStub(
                    method="POST",
                    path="/orders",
                    match=HttpMatch(jq=")("),
                    response=HttpResponse(),
                ),
            },
        ),
    )
    errors = collect_jq_compile_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.http.stubs.bad.match.jq"
    assert "invalid jq expression" in errors[0]["message"]
    assert ")(" in errors[0]["message"]


def test_collect_does_not_raise_on_malformed():
    """The collector must NEVER raise — it catches ConfigError and continues."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "bad": HttpStub(
                    method="POST",
                    path="/o",
                    match=HttpMatch(jq=")("),
                    response=HttpResponse(),
                ),
            },
        ),
    )
    # Should return a list, not raise
    result = collect_jq_compile_errors(mocks)
    assert isinstance(result, list)


# --- (e) fully-valid config -> [] -----------------------------------------
def test_collect_valid_config_returns_empty():
    """A config whose every expression compiles cleanly -> []."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "ok": HttpStub(
                    method="POST",
                    path="/orders",
                    match=HttpMatch(jq=".amount > 1000"),
                    response=HttpResponse(status=201),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "ok-reactor": KafkaReactor(
                    topic="orders.created",
                    match='.eventType == "ORDER_CREATED"',
                    reaction=KafkaReaction(topic="out", value={"ok": True}),
                ),
            },
        ),
    )
    assert collect_jq_compile_errors(mocks) == []


# --- (f) collects TWO errors when both malformed ---------------------------
def test_collect_two_errors_does_not_stop_at_first():
    """Both a stub and a reactor malformed -> two entries, one per offending
    expression. Proves the collector does NOT short-circuit on the first
    ConfigError (Task 10's ``config validate`` reports ALL typos in one pass)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "bad-stub": HttpStub(
                    method="POST",
                    path="/o",
                    match=HttpMatch(jq=")("),
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "bad-reactor": KafkaReactor(
                    topic="t",
                    match=".unclosed >",
                    reaction=KafkaReaction(topic="out", value=1),
                ),
            },
        ),
    )
    errors = collect_jq_compile_errors(mocks)
    assert len(errors) == 2
    paths = [e["path"] for e in errors]
    assert "mocks.http.stubs.bad-stub.match.jq" in paths
    assert "mocks.kafka.reactors.bad-reactor.match" in paths
    # each error has both keys
    for err in errors:
        assert set(err.keys()) == {"path", "message"}
        assert err["message"]


# --- stable order: stubs before reactors, dict order preserved -------------
def test_iter_stable_order_multiple_stubs_and_reactors():
    """Stubs are yielded before reactors; within each, dict insertion order
    is preserved (Python dicts are ordered, so this is deterministic)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "s1": HttpStub(
                    method="GET",
                    path="/a",
                    match=HttpMatch(jq=".a"),
                    response=HttpResponse(),
                ),
                "s2": HttpStub(
                    method="GET",
                    path="/b",
                    match=HttpMatch(jq=".b"),
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "r1": KafkaReactor(
                    topic="t",
                    match=".x",
                    reaction=KafkaReaction(topic="o", value=1),
                ),
                "r2": KafkaReactor(
                    topic="t",
                    match=".y",
                    reaction=KafkaReaction(topic="o", value=1),
                ),
            },
        ),
    )
    pairs = list(iter_mock_jq_expressions(mocks))
    assert pairs == [
        ("mocks.http.stubs.s1.match.jq", ".a"),
        ("mocks.http.stubs.s2.match.jq", ".b"),
        ("mocks.kafka.reactors.r1.match", ".x"),
        ("mocks.kafka.reactors.r2.match", ".y"),
    ]


# --- (g) capture.*.from yielded after match.jq / match (Task 4) -------------
def test_iter_yields_capture_from_for_stub_and_reactor():
    """A stub/reactor with a non-None ``capture`` contributes, AFTER its
    match.jq/match label, one ``capture.{cap}.from`` (label, expr) pair per
    capture entry — in dict insertion order, with the exact path label and the
    spec's ``from_`` expression verbatim."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "create-order": HttpStub(
                    method="POST",
                    path="/orders",
                    match=HttpMatch(jq=".amount > 100"),
                    capture={"order_id": CaptureSpec(from_=".body.variables.id")},
                    response=HttpResponse(status=201),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "order-created": KafkaReactor(
                    topic="orders.created",
                    match='.eventType == "ORDER_CREATED"',
                    capture={"event_key": CaptureSpec(from_=".key")},
                    reaction=KafkaReaction(topic="orders.mock", value={"ok": True}),
                ),
            },
        ),
    )
    pairs = list(iter_mock_jq_expressions(mocks))
    # match.jq/match still yielded first (regression), then capture.*.from.
    assert pairs == [
        ("mocks.http.stubs.create-order.match.jq", ".amount > 100"),
        ("mocks.http.stubs.create-order.capture.order_id.from", ".body.variables.id"),
        ("mocks.kafka.reactors.order-created.match", '.eventType == "ORDER_CREATED"'),
        ("mocks.kafka.reactors.order-created.capture.event_key.from", ".key"),
    ]


def test_iter_capture_order_within_stub_preserves_dict_order():
    """Multiple captures on one stub yield in dict insertion order, immediately
    after the stub's match.jq label (and before the next stub/reactor)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "stub-a": HttpStub(
                    method="POST",
                    path="/a",
                    match=HttpMatch(jq=".a"),
                    capture={
                        "first": CaptureSpec(from_=".body.a"),
                        "second": CaptureSpec(from_=".body.b"),
                    },
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=None,
    )
    pairs = list(iter_mock_jq_expressions(mocks))
    assert pairs == [
        ("mocks.http.stubs.stub-a.match.jq", ".a"),
        ("mocks.http.stubs.stub-a.capture.first.from", ".body.a"),
        ("mocks.http.stubs.stub-a.capture.second.from", ".body.b"),
    ]


def test_iter_skips_capture_labels_when_capture_is_none():
    """Stubs/reactors whose ``capture`` is None contribute NO capture labels,
    even when sibling stubs/reactors in the SAME config do carry a capture.
    Also covers a stub that has a capture but no match (jq=None): the capture
    labels are still yielded (capture is independent of match)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "with-cap": HttpStub(
                    method="POST",
                    path="/a",
                    match=HttpMatch(jq=".a"),
                    capture={"id": CaptureSpec(from_=".body.id")},
                    response=HttpResponse(),
                ),
                "no-cap": HttpStub(
                    method="POST",
                    path="/b",
                    match=HttpMatch(jq=".b"),
                    capture=None,  # explicitly None -> no capture labels
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "with-cap-r": KafkaReactor(
                    topic="t",
                    match=".x",
                    capture={"k": CaptureSpec(from_=".key")},
                    reaction=KafkaReaction(topic="o", value=1),
                ),
                "no-cap-r": KafkaReactor(
                    topic="t",
                    match=".y",
                    capture=None,
                    reaction=KafkaReaction(topic="o", value=1),
                ),
            },
        ),
    )
    pairs = list(iter_mock_jq_expressions(mocks))
    # Only with-cap / with-cap-r contribute capture labels; no-cap* do not.
    capture_labels = [label for label, _ in pairs if ".capture." in label]
    assert capture_labels == [
        "mocks.http.stubs.with-cap.capture.id.from",
        "mocks.kafka.reactors.with-cap-r.capture.k.from",
    ]
    # Pre-existing match.jq / match labels still present for all four entries.
    match_labels = [label for label, _ in pairs if ".capture." not in label]
    assert match_labels == [
        "mocks.http.stubs.with-cap.match.jq",
        "mocks.http.stubs.no-cap.match.jq",
        "mocks.kafka.reactors.with-cap-r.match",
        "mocks.kafka.reactors.no-cap-r.match",
    ]


def test_iter_capture_yielded_even_when_match_is_none():
    """A stub with ``match=None`` but a non-None ``capture`` still yields its
    capture.*.from labels — capture is independent of match. (A reactor's
    ``match`` and ``capture`` are likewise independent.)"""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "body-only-cap": HttpStub(
                    method="POST",
                    path="/a",
                    match=None,
                    capture={"id": CaptureSpec(from_=".body.id")},
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=None,
    )
    assert list(iter_mock_jq_expressions(mocks)) == [
        ("mocks.http.stubs.body-only-cap.capture.id.from", ".body.id"),
    ]


# --- (h) collect surfaces capture.from errors under the capture label -------
def test_collect_malformed_capture_from_surfaces_under_capture_label():
    """collect_jq_compile_errors on a config whose capture ``from`` is malformed
    (e.g. '.body[') returns one error whose ``path`` is the capture label, NOT
    a match.jq/match label. Both HTTP and Kafka capture paths are exercised."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "bad-cap": HttpStub(
                    method="POST",
                    path="/orders",
                    match=HttpMatch(jq=".amount > 0"),
                    capture={"order_id": CaptureSpec(from_=".body[")},
                    response=HttpResponse(status=201),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "bad-cap-r": KafkaReactor(
                    topic="t",
                    match=".x",
                    capture={"k": CaptureSpec(from_=".key[")},
                    reaction=KafkaReaction(topic="o", value=1),
                ),
            },
        ),
    )
    errors = collect_jq_compile_errors(mocks)
    paths = [e["path"] for e in errors]
    assert "mocks.http.stubs.bad-cap.capture.order_id.from" in paths
    assert "mocks.kafka.reactors.bad-cap-r.capture.k.from" in paths
    # Each error carries both keys and a non-empty message naming the bad expr.
    for err in errors:
        assert set(err.keys()) == {"path", "message"}
        assert "invalid jq expression" in err["message"]


def test_collect_valid_capture_froms_return_empty():
    """A config whose every match.jq/match AND capture ``from`` compiles cleanly
    -> [] (capture froms are validated by the same collect pass)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "ok": HttpStub(
                    method="POST",
                    path="/orders",
                    match=HttpMatch(jq=".amount > 1000"),
                    capture={"id": CaptureSpec(from_=".body.variables.id")},
                    response=HttpResponse(status=201),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "ok-r": KafkaReactor(
                    topic="orders.created",
                    match='.eventType == "ORDER_CREATED"',
                    capture={"k": CaptureSpec(from_=".key")},
                    reaction=KafkaReaction(topic="out", value={"ok": True}),
                ),
            },
        ),
    )
    assert collect_jq_compile_errors(mocks) == []


# --- (i) grpc stubs walked as a third block (Task 4) -----------------------
def test_iter_grpc_stub_match_jq_yielded():
    """A grpc stub with match.jq='.msg == "hi"' yields one pair labelled
    ``mocks.grpc.stubs.{name}.match.jq`` carrying the expression verbatim."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    match=GrpcMatch(jq='.msg == "hi"'),
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    assert list(iter_mock_jq_expressions(mocks)) == [
        ("mocks.grpc.stubs.s.match.jq", '.msg == "hi"'),
    ]


def test_iter_grpc_stub_capture_from_yielded():
    """A grpc stub with a capture yields ``capture.{cap}.from`` (verbatim) right
    after its match.jq label — mirroring HTTP stubs / Kafka reactors."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    match=GrpcMatch(jq=".a"),
                    capture={"id": CaptureSpec(from_=".msg.id")},
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    assert list(iter_mock_jq_expressions(mocks)) == [
        ("mocks.grpc.stubs.s.match.jq", ".a"),
        ("mocks.grpc.stubs.s.capture.id.from", ".msg.id"),
    ]


def test_iter_grpc_skips_none_match_and_none_capture():
    """A grpc stub with match=None / capture=None contributes no pairs."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "bare": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    match=None,
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    assert list(iter_mock_jq_expressions(mocks)) == []


def test_iter_grpc_block_comes_after_http_and_kafka():
    """Order is HTTP stubs -> Kafka reactors -> gRPC stubs (gRPC is the third
    block). Within gRPC, dict insertion order is preserved."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "h": HttpStub(
                    method="POST",
                    path="/x",
                    match=HttpMatch(jq=".h"),
                    response=HttpResponse(),
                ),
            },
        ),
        kafka=KafkaMockConfig(
            reactors={
                "k": KafkaReactor(
                    topic="t",
                    match=".k",
                    reaction=KafkaReaction(topic="o", value=1),
                ),
            },
        ),
        grpc=GrpcMockConfig(
            stubs={
                "g1": GrpcStub(
                    service="pkg.Svc",
                    method="A",
                    match=GrpcMatch(jq=".g1"),
                    response=GrpcResponse(message={}),
                ),
                "g2": GrpcStub(
                    service="pkg.Svc",
                    method="B",
                    match=GrpcMatch(jq=".g2"),
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    assert list(iter_mock_jq_expressions(mocks)) == [
        ("mocks.http.stubs.h.match.jq", ".h"),
        ("mocks.kafka.reactors.k.match", ".k"),
        ("mocks.grpc.stubs.g1.match.jq", ".g1"),
        ("mocks.grpc.stubs.g2.match.jq", ".g2"),
    ]


def test_collect_grpc_malformed_match_jq():
    """collect_jq_compile_errors on a grpc stub with malformed match.jq
    ('.msg ==') -> one error whose ``path`` is the grpc stub's jq label."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    match=GrpcMatch(jq=".msg =="),
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    errors = collect_jq_compile_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.grpc.stubs.s.match.jq"
    assert "invalid jq expression" in errors[0]["message"]
    assert set(errors[0].keys()) == {"path", "message"}


def test_collect_grpc_malformed_capture_from_under_capture_label():
    """A malformed grpc capture ``from`` surfaces under the capture label, not
    the match.jq label (same behaviour as HTTP/Kafka)."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    match=GrpcMatch(jq=".a"),
                    capture={"id": CaptureSpec(from_=".msg[")},
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    errors = collect_jq_compile_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.grpc.stubs.s.capture.id.from"


def test_collect_grpc_valid_returns_empty():
    """A grpc config whose match.jq and capture ``from`` both compile -> []."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    match=GrpcMatch(jq='.msg == "hi"'),
                    capture={"id": CaptureSpec(from_=".msg.id")},
                    response=GrpcResponse(message={}),
                ),
            },
        ),
    )
    assert collect_jq_compile_errors(mocks) == []
