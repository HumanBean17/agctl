"""Tests for the mock jq-expression walker + validate-error collector (Task 4).

The walker ``iter_mock_jq_expressions`` walks a :class:`MocksConfig` and yields
every jq match expression (HTTP stub ``match.jq`` and Kafka reactor ``match``)
with a stable path label. ``collect_jq_compile_errors`` calls :func:`compile_jq`
on each, catching :class:`ConfigError` so ``config validate`` can report ALL
typos in one pass rather than stopping at the first.
"""

from agctl.config.models import (
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
