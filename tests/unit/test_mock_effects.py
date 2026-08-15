"""Unit tests for EffectExecutor (mock cross-transport effects, Task 2).

The executor is the pure dispatch core: it renders effect templates against
the capture namespace, dispatches to injected kafka/http clients, chains HTTP
response captures back into the namespace, and short-circuits on the first
failure. Tests use fakes — no confluent_kafka/httpx anywhere (the resolvers
are closures returning recording fakes).
"""

import json

import pytest

from agctl.config.models import (
    CaptureSpec,
    HttpEffect,
    KafkaEffect,
    KafkaEffectMessage,
)
from agctl.mock.effects import EffectExecutor, EffectOutcome
from agctl.resolution import CaptureValue


# ---------------------------------------------------------------------------
# Fakes (duck-type KafkaClient.produce / HttpClient.request)
# ---------------------------------------------------------------------------


class FakeKafkaClient:
    """Fake KafkaClient for testing EffectExecutor.

    - produce: records calls (can raise to simulate an undelivered produce).
    """

    def __init__(self, produce_raises=None):
        self.produce_raises = produce_raises
        self.produce_calls = []

    def produce(self, topic, value, *, key=None, headers=None, _raw=False):
        """Record produce call or raise configured error."""
        self.produce_calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": headers,
                "_raw": _raw,
            }
        )
        if self.produce_raises:
            raise self.produce_raises


class FakeHttpClient:
    """Fake HttpClient for testing EffectExecutor.

    - request: records calls and returns the canned response dict (the DESIGN
      §4.2 shape HttpClient.request returns); can raise to simulate failure.
    """

    def __init__(self, response=None, request_raises=None):
        self.response = response or {
            "status_code": 200,
            "response_time_ms": 5,
            "headers": {},
            "body": {},
            "url": "u",
            "method": "GET",
        }
        self.request_raises = request_raises
        self.request_calls = []

    def request(self, method, path, *, headers=None, body=None):
        """Record request call; return canned response or raise."""
        self.request_calls.append(
            {"method": method, "path": path, "headers": headers, "body": body}
        )
        if self.request_raises:
            raise self.request_raises
        return self.response


def _no_kafka(effect):
    raise AssertionError("kafka_resolver must not be called for this run")


def _no_http(effect):
    raise AssertionError("http_resolver must not be called for this run")


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def emit_event():
    """Capture emitted events in a list."""
    events = []

    def emit(event_dict):
        events.append(event_dict)

    emit.events = events
    return emit


# ---------------------------------------------------------------------------
# Kafka effects
# ---------------------------------------------------------------------------


def test_kafka_single_value_produces_and_emits(emit_event):
    """Single-value kafka effect: rendered produce + one kafka.produced event."""
    client = FakeKafkaClient()
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (client, None),
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    outcome = executor.run(
        [KafkaEffect(type="kafka", topic="t", value={"a": "{x}"})],
        {"x": CaptureValue("v", "scalar")},
        "s",
    )

    assert outcome == EffectOutcome(ok=True)
    assert len(client.produce_calls) == 1
    call = client.produce_calls[0]
    assert call["topic"] == "t"
    # codec=None -> legacy json.dumps encode; published pre-encoded via _raw.
    assert call["value"] == json.dumps({"a": "v"}).encode("utf-8")
    assert call["key"] is None
    assert call["headers"] is None
    assert call["_raw"] is True

    assert len(emit_event.events) == 1
    event = emit_event.events[0]
    assert event["event"] == "kafka.produced"
    assert event["trigger"] == "s"
    assert event["topic"] == "t"
    assert event["key"] is None
    assert "duration_ms" in event


def test_kafka_multi_message_two_produces_in_order(emit_event):
    """values=[...] produces one message per element, in order, each rendered."""
    client = FakeKafkaClient()
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (client, None),
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    effect = KafkaEffect(
        type="kafka",
        topic="t",
        values=[
            KafkaEffectMessage(value=1),
            KafkaEffectMessage(value=2, key="{x}", headers={"tid": "{x}"}),
        ],
    )
    outcome = executor.run([effect], {"x": CaptureValue("k9", "scalar")}, "s")

    assert outcome.ok is True
    assert len(client.produce_calls) == 2
    first, second = client.produce_calls
    assert first["value"] == json.dumps(1).encode("utf-8")
    assert first["key"] is None
    assert first["headers"] is None
    assert second["value"] == json.dumps(2).encode("utf-8")
    assert second["key"] == b"k9"
    assert second["headers"] == {"tid": "k9"}
    assert all(call["_raw"] is True for call in client.produce_calls)

    produced = [e for e in emit_event.events if e["event"] == "kafka.produced"]
    assert len(produced) == 2
    assert produced[0]["key"] is None
    assert produced[1]["key"] == "k9"


def test_kafka_values_item_falls_back_to_top_level_key_and_headers(emit_event):
    """Spec §6.1 fallback: a values item omitting key/headers inherits the
    top-level ones; an item with its own keeps its own."""
    client = FakeKafkaClient()
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (client, None),
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    effect = KafkaEffect(
        type="kafka",
        topic="t",
        key="{x}",
        headers={"src": "mock"},
        values=[
            KafkaEffectMessage(value={"n": 1}),  # omits both -> top-level
            KafkaEffectMessage(value={"n": 2}, key="own", headers={"h": "own"}),
        ],
    )
    outcome = executor.run([effect], {"x": CaptureValue("k9", "scalar")}, "s")

    assert outcome.ok is True
    first, second = client.produce_calls
    assert first["key"] == b"k9"  # rendered top-level fallback
    assert first["headers"] == {"src": "mock"}
    assert second["key"] == b"own"  # item's own wins
    assert second["headers"] == {"h": "own"}


# ---------------------------------------------------------------------------
# HTTP effects + capture chaining
# ---------------------------------------------------------------------------


def test_http_call_capture_chains_into_later_kafka_effect(emit_event):
    """HTTP capture lands in the namespace and a later kafka effect sees it."""
    http_client = FakeHttpClient(
        response={
            "status_code": 200,
            "response_time_ms": 5,
            "headers": {},
            "body": {"id": "A1"},
            "url": "u",
            "method": "POST",
        }
    )
    kafka_client = FakeKafkaClient()
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (kafka_client, None),
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    effects = [
        HttpEffect(
            type="http",
            url="http://svc.internal/api",
            method="post",
            path="/api",
            body={"q": 1},
            capture={"ackId": CaptureSpec.model_validate({"from": ".body.id"})},
        ),
        KafkaEffect(type="kafka", topic="acks", value={"ack": "{ackId}"}),
    ]
    namespace = {}
    outcome = executor.run(effects, namespace, "s")

    assert outcome.ok is True
    assert len(http_client.request_calls) == 1
    assert http_client.request_calls[0]["method"] == "POST"  # normalized
    assert http_client.request_calls[0]["path"] == "/api"
    assert http_client.request_calls[0]["body"] == {"q": 1}

    called = [e for e in emit_event.events if e["event"] == "http.called"]
    assert len(called) == 1
    assert called[0]["trigger"] == "s"
    assert called[0]["url"] == "http://svc.internal/api"
    assert "service" not in called[0]  # service XOR url
    assert called[0]["method"] == "POST"
    assert called[0]["path"] == "/api"
    assert called[0]["status_code"] == 200
    assert called[0]["duration_ms"] == 5

    # Capture chained: the later kafka effect rendered "{ackId}" -> "A1".
    assert namespace["ackId"].value == "A1"
    assert len(kafka_client.produce_calls) == 1
    assert (
        kafka_client.produce_calls[0]["value"] == json.dumps({"ack": "A1"}).encode("utf-8")
    )


def test_http_service_event_carries_service_not_url(emit_event):
    """A service-based http effect emits `service` (never `url`)."""
    http_client = FakeHttpClient()
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [HttpEffect(type="http", service="orders", path="/health", method="GET")],
        {},
        "s",
    )

    assert outcome.ok is True
    called = [e for e in emit_event.events if e["event"] == "http.called"]
    assert len(called) == 1
    assert called[0]["service"] == "orders"
    assert "url" not in called[0]


def test_http_service_mode_path_renders_placeholders(emit_event):
    """Spec §6.2: a service-mode path's ``{placeholder}`` renders from the
    namespace before the request (the resolver hands back effect.path verbatim;
    the executor renders it)."""
    http_client = FakeHttpClient()
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [HttpEffect(type="http", service="orders", path="/orders/{orderId}")],
        {"orderId": CaptureValue("o-42", "scalar")},
        "s",
    )

    assert outcome.ok is True
    assert http_client.request_calls[0]["path"] == "/orders/o-42"
    called = [e for e in emit_event.events if e["event"] == "http.called"]
    assert called[0]["path"] == "/orders/o-42"


def test_http_url_mode_path_stays_literal(emit_event):
    """Url mode: the path was split from the literal url — no rendering."""
    http_client = FakeHttpClient()
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, "/legacy/{orderId}"),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [HttpEffect(type="http", url="http://svc/legacy/{orderId}")],
        {"orderId": CaptureValue("o-42", "scalar")},
        "s",
    )

    assert outcome.ok is True
    # The literal braces survive (nothing in url mode renders).
    assert http_client.request_calls[0]["path"] == "/legacy/{orderId}"


def test_http_capture_last_wins(emit_event):
    """Two http effects capturing the same name: the later capture wins."""
    first = FakeHttpClient(
        response={
            "status_code": 200,
            "response_time_ms": 3,
            "headers": {},
            "body": {"id": "A1"},
            "url": "u1",
            "method": "GET",
        }
    )
    second = FakeHttpClient(
        response={
            "status_code": 200,
            "response_time_ms": 4,
            "headers": {},
            "body": {"id": "B2"},
            "url": "u2",
            "method": "GET",
        }
    )
    clients = {"http://one": first, "http://two": second}
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (clients[effect.url], effect.path),
        emit_event=emit_event,
    )
    capture = {"x": CaptureSpec.model_validate({"from": ".body.id"})}

    namespace = {}
    outcome = executor.run(
        [
            HttpEffect(type="http", url="http://one", capture=capture),
            HttpEffect(type="http", url="http://two", capture=capture),
        ],
        namespace,
        "s",
    )

    assert outcome.ok is True
    assert namespace["x"].value == "B2"


def test_http_capture_missing_emits_event_and_empty_slot(emit_event):
    """A capture from-path resolving to nothing emits capture.missing."""
    http_client = FakeHttpClient(
        response={
            "status_code": 200,
            "response_time_ms": 3,
            "headers": {},
            "body": {"other": 1},
            "url": "u",
            "method": "GET",
        }
    )
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    namespace = {}
    outcome = executor.run(
        [
            HttpEffect(
                type="http",
                url="http://svc",
                capture={"x": CaptureSpec.model_validate({"from": ".body.nope"})},
            )
        ],
        namespace,
        "s",
    )

    assert outcome.ok is True  # a soft miss is not a failure
    missing = [e for e in emit_event.events if e["event"] == "capture.missing"]
    assert len(missing) == 1
    assert missing[0]["trigger"] == "s"
    assert missing[0]["name"] == "x"
    assert missing[0]["from"] == ".body.nope"
    # Soft-miss contract: the slot exists with value None (renders as "").
    assert namespace["x"].value is None


# ---------------------------------------------------------------------------
# Generator pre-pass ({{gen}} tokens)
# ---------------------------------------------------------------------------


_UUID_RE = __import__("re").compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def test_generator_token_in_kafka_value_resolves(emit_event):
    """``{{uuid}}`` in a kafka effect's value produces a UUID-shaped value —
    not the literal token."""
    client = FakeKafkaClient()
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (client, None),
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    outcome = executor.run(
        [KafkaEffect(type="kafka", topic="t", value={"id": "{{uuid}}"})],
        {},
        "s",
    )

    assert outcome.ok is True
    produced_value = json.loads(client.produce_calls[0]["value"])
    assert _UUID_RE.match(produced_value["id"]) is not None


def test_generator_token_resolves_once_per_run(emit_event):
    """The same ``{{uuid}}`` token in two effects of one run resolves to ONE
    value (per-run memo, single-invocation semantics)."""
    client = FakeKafkaClient()
    http_client = FakeHttpClient()
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (client, None),
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [
            KafkaEffect(type="kafka", topic="t1", value={"id": "{{uuid}}"}),
            KafkaEffect(type="kafka", topic="t2", key="{{uuid}}", value=1),
        ],
        {},
        "s",
    )

    assert outcome.ok is True
    first = json.loads(client.produce_calls[0]["value"])
    second_key = client.produce_calls[1]["key"].decode()
    assert first["id"] == second_key


def test_generator_token_in_http_body_and_service_path(emit_event):
    """``{{rand}}`` renders in an http effect's body and service-mode path."""
    http_client = FakeHttpClient()
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [
            HttpEffect(
                type="http",
                service="orders",
                path="/t/{{rand:4}}",
                body={"nonce": "{{rand:4}}"},
            )
        ],
        {},
        "s",
    )

    assert outcome.ok is True
    call = http_client.request_calls[0]
    # Both tokens resolved (4 hex chars each), and per-run memoing means the
    # SAME token text resolved to one value in both fields.
    assert call["path"] != "/t/{{rand:4}}"
    assert call["body"]["nonce"] != "{{rand:4}}"
    assert call["path"].endswith(call["body"]["nonce"])


def test_kafka_failure_short_circuits_remaining_effects(emit_event):
    """A raising produce emits one fatal effect.error; later effects never run."""
    failing = FakeKafkaClient(produce_raises=Exception("produce timeout"))
    never = FakeKafkaClient()
    clients = {"t1": failing, "t2": never}
    executor = EffectExecutor(
        kafka_resolver=lambda effect: (clients[effect.topic], None),
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    outcome = executor.run(
        [
            KafkaEffect(type="kafka", topic="t1", value={"a": 1}),
            KafkaEffect(type="kafka", topic="t2", value={"b": 2}),
        ],
        {},
        "s",
    )

    assert outcome == EffectOutcome(ok=False, error="produce timeout")
    assert len(never.produce_calls) == 0  # short-circuited

    errors = [e for e in emit_event.events if e["event"] == "effect.error"]
    assert len(errors) == 1
    assert errors[0]["trigger"] == "s"
    assert errors[0]["effect_type"] == "kafka"
    assert errors[0]["error"] == "produce timeout"
    assert errors[0]["fatal"] is True
    assert errors[0]["topic"] == "t1"
    assert not [e for e in emit_event.events if e["event"] == "kafka.produced"]


def test_http_failure_emits_effect_error_with_url_context(emit_event):
    """A raising request emits effect.error carrying the url context."""
    http_client = FakeHttpClient(request_raises=Exception("connection refused"))
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [HttpEffect(type="http", url="http://svc/api", method="GET")], {}, "s"
    )

    assert outcome.ok is False
    assert outcome.error == "connection refused"
    errors = [e for e in emit_event.events if e["event"] == "effect.error"]
    assert len(errors) == 1
    assert errors[0]["effect_type"] == "http"
    assert errors[0]["fatal"] is True
    assert errors[0]["url"] == "http://svc/api"
    assert "topic" not in errors[0]


def test_http_failure_with_service_context(emit_event):
    """A failing service-based effect reports `service` (not `url`)."""
    http_client = FakeHttpClient(request_raises=Exception("boom"))
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=lambda effect: (http_client, effect.path),
        emit_event=emit_event,
    )

    outcome = executor.run(
        [HttpEffect(type="http", service="orders", path="/x")], {}, "s"
    )

    assert outcome.ok is False
    errors = [e for e in emit_event.events if e["event"] == "effect.error"]
    assert len(errors) == 1
    assert errors[0]["effect_type"] == "http"
    assert errors[0]["service"] == "orders"
    assert "url" not in errors[0]


def test_resolver_failure_surfaces_as_effect_error(emit_event):
    """A raising kafka_resolver (unresolved cluster) is caught, not raised."""
    def unresolved(effect):
        raise Exception("no such cluster: prod")

    executor = EffectExecutor(
        kafka_resolver=unresolved,
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    outcome = executor.run([KafkaEffect(type="kafka", topic="t", value=1)], {}, "s")

    assert outcome == EffectOutcome(ok=False, error="no such cluster: prod")
    errors = [e for e in emit_event.events if e["event"] == "effect.error"]
    assert len(errors) == 1
    assert errors[0]["topic"] == "t"


# ---------------------------------------------------------------------------
# No-op
# ---------------------------------------------------------------------------


def test_run_without_effects_is_ok_and_silent(emit_event):
    """An empty effects list returns ok immediately and emits nothing."""
    executor = EffectExecutor(
        kafka_resolver=_no_kafka,
        http_resolver=_no_http,
        emit_event=emit_event,
    )

    outcome = executor.run([], {}, "s")

    assert outcome == EffectOutcome(ok=True)
    assert emit_event.events == []
