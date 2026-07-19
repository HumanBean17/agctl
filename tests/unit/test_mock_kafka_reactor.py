"""Unit tests for KafkaReactor (DESIGN §8.3, Task 6).

The reactor orchestrates match/capture/react mechanics with visible skips
and idempotent retries. Tests use a FakeKafkaClient that scripts message
delivery and records produce calls.
"""

import json
import threading
import time

import pytest

from agctl.clients.kafka_client import ReactionResult
from agctl.config.models import CaptureSpec, KafkaReaction, KafkaReactor
from agctl.mock.kafka_reactor import KafkaReactor as Reactor
from agctl.errors import ConnectionFailure, ConfigError


# ---------------------------------------------------------------------------
# Fake KafkaClient (duck-types KafkaClient.consume_loop/probe/produce)
# ---------------------------------------------------------------------------


class FakeKafkaClient:
    """Fake KafkaClient for testing KafkaReactor.

    - consume_loop: calls handle for scripted messages with attempt/final flags.
    - probe: records calls (can raise to test error propagation).
    - produce: records calls (can raise to test reaction failure).
    """

    def __init__(self, messages=None, probe_raises=None, produce_raises=None):
        self.messages = list(messages or [])
        self.probe_raises = probe_raises
        self.produce_raises = produce_raises
        self.consume_calls = []
        self.probe_calls = []
        self.produce_calls = []
        self.stop_event = None
        self.handle = None
        self.max_retries = 3

    def consume_loop(
        self,
        topic,
        *,
        group_id,
        stop_event,
        handle,
        poll_timeout=0.5,
        max_retries=3,
        on_assign=None,
        on_revoke=None,
        on_decode_error=None,
    ):
        """Scripted message delivery with retry logic."""
        self.consume_calls.append(
            {
                "topic": topic,
                "group_id": group_id,
                "max_retries": max_retries,
            }
        )
        self.stop_event = stop_event
        self.handle = handle
        self.max_retries = max_retries

        for msg in self.messages:
            if stop_event.is_set():
                break
            # Deliver with retry logic. Handler exceptions PROPAGATE — matching
            # the real KafkaClient.consume_loop, which does NOT wrap handle() in
            # try/except. The reactor's _handle catches its OWN reaction failures
            # (produce errors) and returns a ReactionResult, so reaction-failure
            # tests are unaffected; only unexpected exceptions propagate.
            for attempt in range(1, max_retries + 1):
                final = attempt >= max_retries
                result = handle(msg, attempt=attempt, final=final)
                if result == ReactionResult.COMMIT:
                    break
                elif result == ReactionResult.STOP:
                    stop_event.set()
                    return
                elif result == ReactionResult.RETRY:
                    if final:
                        # RETRY on final is treated as COMMIT (defensive)
                        break
                    # else: continue to next attempt

    def probe(self, topic, *, group_id, timeout=5.0):
        """Record probe call or raise configured error."""
        self.probe_calls.append({"topic": topic, "group_id": group_id, "timeout": timeout})
        if self.probe_raises:
            raise self.probe_raises

    def produce(self, topic, value, *, key=None, headers=None, _raw=False):
        """Record produce call or raise configured error.

        Accepts the ``_raw`` kwarg (recorded on the call) so the fake mirrors
        the real ``KafkaClient.produce`` post-collapse of the reactor's
        reaction publish path: the reactor ALWAYS publishes via
        ``produce(..., _raw=True)`` (pre-encoded by ``_encode_reaction``),
        even for the legacy JSON reaction (``reaction_codec=None``).
        """
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


@pytest.fixture
def stop_event():
    """Thread stop event."""
    return threading.Event()


@pytest.fixture
def sample_config():
    """Sample KafkaReactor config."""
    return KafkaReactor(
        description="Test reactor",
        topic="commands",
        consumer_group="test-group",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(
            topic="events",
            key="{orderId}",
            value={"eventType": "ORDER_CREATED", "orderId": "{orderId}"},
            headers={"source": "mock-server"},
        ),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_reactor_resolved_group_with_consumer_group(sample_config, stop_event):
    """resolved_group returns config.consumer_group when set."""
    client = FakeKafkaClient()
    reactor = Reactor(
        name="test",
        config=sample_config,
        client=client,
        emit_event=lambda _: None,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-123",
    )
    assert reactor.resolved_group() == "test-group"


def test_reactor_resolved_group_without_consumer_group(stop_event):
    """resolved_group generates agctl-mock-{name}-{run_id} when consumer_group omitted."""
    config = KafkaReactor(
        topic="commands",
        reaction=KafkaReaction(topic="events", value={}),
    )
    client = FakeKafkaClient()
    reactor = Reactor(
        name="my-reactor",
        config=config,
        client=client,
        emit_event=lambda _: None,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-456",
    )
    assert reactor.resolved_group() == "agctl-mock-my-reactor-run-456"


def test_reactor_prepare_calls_probe(sample_config, stop_event):
    """prepare() calls client.probe with resolved group."""
    client = FakeKafkaClient()
    reactor = Reactor(
        name="test",
        config=sample_config,
        client=client,
        emit_event=lambda _: None,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-123",
    )
    reactor.prepare()
    assert len(client.probe_calls) == 1
    assert client.probe_calls[0]["topic"] == "commands"
    assert client.probe_calls[0]["group_id"] == "test-group"


def test_reactor_prepare_propagates_connection_failure(sample_config, stop_event):
    """prepare() propagates ConnectionFailure from probe."""
    client = FakeKafkaClient(probe_raises=ConnectionFailure("broker unreachable", {}))
    reactor = Reactor(
        name="test",
        config=sample_config,
        client=client,
        emit_event=lambda _: None,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-123",
    )
    with pytest.raises(ConnectionFailure):
        reactor.prepare()


def test_reactor_close_is_callable_noop(sample_config, stop_event):
    """close() is callable and a documented no-op (teardown contract).

    prepare() opens no long-lived resource today (probe builds+closes its own
    consumer), so close() must not raise and must release nothing.
    """
    client = FakeKafkaClient()
    reactor = Reactor(
        name="test",
        config=sample_config,
        client=client,
        emit_event=lambda _: None,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-123",
    )
    assert reactor.close() is None  # callable, no-op


def test_reactor_match_capture_react(emit_event, stop_event):
    """Match + capture + template reaction → kafka.reacted event."""
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(
            topic="events",
            key="{orderId}",
            value={"eventType": "ORDER_CREATED", "orderId": "{orderId}"},
            headers={"source": "mock-server"},
        ),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"orderId": "ord-1", "command": "CREATE_ORDER"},
                "key": None,
                "partition": 0,
                "offset": 42,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ]
    )
    reactor = Reactor(
        name="order-reactor",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Verify produce was called with rendered templates. Post-fix the
    # reaction publish path is collapsed: the reactor ALWAYS encodes via
    # ``_encode_reaction`` (legacy ``json.dumps`` when ``reaction_codec is
    # None``) and ALWAYS publishes via ``produce(..., _raw=True)``. The
    # produce call thus receives pre-encoded JSON BYTES (not a dict), a
    # utf-8-encoded key, and ``_raw=True`` — preserving the test's intent
    # (verify the rendered reaction content) at the new wire boundary.
    assert len(client.produce_calls) == 1
    prod = client.produce_calls[0]
    assert prod["topic"] == "events"
    assert prod["value"] == json.dumps(
        {"eventType": "ORDER_CREATED", "orderId": "ord-1"}
    ).encode("utf-8")
    assert prod["key"] == b"ord-1"
    assert prod["headers"] == {"source": "mock-server"}
    assert prod["_raw"] is True

    # Verify kafka.reacted event was emitted
    assert len(emit_event.events) == 1
    event = emit_event.events[0]
    assert event["event"] == "kafka.reacted"
    assert event["reactor"] == "order-reactor"
    assert event["topic"] == "events"
    assert event["key"] == "ord-1"
    assert "duration_ms" in event


def test_reactor_numeric_capture_coercion(emit_event, stop_event):
    """Numeric values in message are stringified via str() for capture context."""
    config = KafkaReactor(
        topic="commands",
        reaction=KafkaReaction(
            topic="events",
            value={"orderId": "{orderId}"},
        ),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"orderId": 42, "command": "CREATE_ORDER"},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ]
    )
    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Verify orderId was stringified to "42". Post-fix the value is
    # published as pre-encoded JSON bytes (reaction_codec=None → legacy
    # json.dumps path through ``_encode_reaction``), with ``_raw=True``.
    assert len(client.produce_calls) == 1
    assert client.produce_calls[0]["value"] == json.dumps({"orderId": "42"}).encode(
        "utf-8"
    )
    assert client.produce_calls[0]["_raw"] is True


def test_reactor_jq_non_match_commits_no_event(emit_event, stop_event):
    """jq non-match → COMMIT, no event emitted."""
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "SHIP_ORDER"',
        reaction=KafkaReaction(topic="events", value={}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"orderId": "ord-1", "command": "CREATE_ORDER"},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ]
    )
    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # No produce call, no event
    assert len(client.produce_calls) == 0
    assert len(emit_event.events) == 0


def test_reactor_match_on_key_envelope(emit_event, stop_event):
    """Reactor match roots at the message envelope, so .key is reachable.

    Two messages with value={"command": "X"}: key="ord-1" matches '.key ==
    "ord-1"' → reaction emitted; key="other" does not → silent commit, no
    reaction. Under the prior value-rooted impl, .key against the value dict
    is null → no match for either → no reaction.
    """
    config = KafkaReactor(
        topic="commands",
        match='.key == "ord-1"',
        reaction=KafkaReaction(topic="out", value={}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"command": "X"},
                "key": "ord-1",
                "partition": 0,
                "offset": 0,
                "timestamp": 1719660000000,
                "headers": [],
            },
            {
                "value": {"command": "X"},
                "key": "other",
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": [],
            },
        ]
    )
    reactor = Reactor(
        name="key-match",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Exactly one reaction (key="ord-1" matched; key="other" did not).
    assert len(client.produce_calls) == 1
    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1
    assert reacted[0]["reactor"] == "key-match"
    assert reacted[0]["topic"] == "out"


def test_reactor_non_object_value_skipped(emit_event, stop_event):
    """Non-object value → kafka.skipped event + COMMIT."""
    config = KafkaReactor(
        topic="data",
        reaction=KafkaReaction(topic="events", value={}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": "not-json",  # String value, not dict
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ]
    )
    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Verify kafka.skipped event
    assert len(emit_event.events) == 1
    event = emit_event.events[0]
    assert event["event"] == "kafka.skipped"
    assert event["reactor"] == "test"
    assert event["topic"] == "data"
    assert event["reason"] == "non-object message value"
    assert event["count"] == 1

    # No produce call
    assert len(client.produce_calls) == 0


def test_reactor_array_value_skipped(emit_event, stop_event):
    """Array value → kafka.skipped event + COMMIT."""
    config = KafkaReactor(
        topic="data",
        reaction=KafkaReaction(topic="events", value={}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": [1, 2, 3],  # Array value, not dict
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ]
    )
    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Verify kafka.skipped event
    assert len(emit_event.events) == 1
    event = emit_event.events[0]
    assert event["event"] == "kafka.skipped"
    assert event["reason"] == "non-object message value"


def test_reactor_reaction_failure_retry_then_commit(emit_event, stop_event):
    """Reaction failure: not final → RETRY (no event); final → kafka.error once + COMMIT."""
    config = KafkaReactor(
        topic="commands",
        reaction=KafkaReaction(topic="events", value={"status": "created"}),
    )
    # Simulate produce that always fails (to force all retries)
    produce_call_count = [0]

    def failing_produce(topic, value, *, key=None, headers=None, _raw=False):
        produce_call_count[0] += 1
        raise Exception("produce timeout")

    client = FakeKafkaClient(
        messages=[
            {
                "value": {"orderId": "ord-1"},
                "key": None,
                "partition": 0,
                "offset": 10,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ],
    )
    # Override produce to simulate failure
    client.produce = failing_produce

    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Should have been called 3 times (max_retries=3)
    assert produce_call_count[0] == 3

    # One kafka.error event on final attempt
    assert len(emit_event.events) == 1
    event = emit_event.events[0]
    assert event["event"] == "kafka.error"
    assert event["reactor"] == "test"
    assert event["topic"] == "commands"
    assert event["offset"] == 10
    assert event["partition"] == 0
    assert "error" in event
    assert event["fatal"] is False  # fail_fast=False


def test_reactor_reaction_failure_fail_fast_stop(emit_event, stop_event):
    """Reaction failure with fail_fast=True: final → kafka.error + STOP."""
    config = KafkaReactor(
        topic="commands",
        reaction=KafkaReaction(topic="events", value={"status": "created"}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"orderId": "ord-1"},
                "key": None,
                "partition": 0,
                "offset": 10,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ],
        produce_raises=Exception("produce timeout"),
    )
    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=True,  # STOP on final failure
        run_id="run-1",
    )

    reactor.run()

    # One kafka.error event with fatal=True
    assert len(emit_event.events) == 1
    event = emit_event.events[0]
    assert event["event"] == "kafka.error"
    assert event["fatal"] is True  # fail_fast=True

    # Stop event should be set
    assert stop_event.is_set()


def test_reactor_run_calls_consume_loop_with_resolved_group(sample_config, stop_event):
    """run() calls consume_loop with resolved_group()."""
    client = FakeKafkaClient(messages=[])
    reactor = Reactor(
        name="test",
        config=sample_config,
        client=client,
        emit_event=lambda _: None,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-123",
    )

    reactor.run()

    assert len(client.consume_calls) == 1
    call = client.consume_calls[0]
    assert call["topic"] == "commands"
    assert call["group_id"] == "test-group"  # resolved_group()
    assert call["max_retries"] == 3

def test_reactor_no_match_config_processes_all(emit_event, stop_event):
    """When match is None, all messages are processed."""
    config = KafkaReactor(
        topic="commands",
        match=None,  # No filter
        reaction=KafkaReaction(topic="events", value={"processed": "true"}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"orderId": "ord-1"},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": [],
            }
        ]
    )
    reactor = Reactor(
        name="test",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )

    reactor.run()

    # Should process the message
    assert len(client.produce_calls) == 1
    assert len(emit_event.events) == 1
    assert emit_event.events[0]["event"] == "kafka.reacted"


def test_reactor_capture_from_key(emit_event, stop_event):
    """capture.from '.key' reads the message key (gap 2)."""
    config = KafkaReactor(
        topic="chatx.commands",
        match='.value.command == "SEARCH"',
        capture={"tid": CaptureSpec.model_validate({"from": ".key"})},
        reaction=KafkaReaction(topic="chatx.events", value={"threadId": "{tid}"}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"command": "SEARCH"},
                "key": "k-9",
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {},
            }
        ]
    )
    reactor = Reactor(
        name="chatx",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()
    assert len(client.produce_calls) == 1
    # Post-fix: reaction value is pre-encoded JSON bytes (reaction_codec=None
    # → legacy json.dumps via ``_encode_reaction``), published ``_raw=True``.
    assert client.produce_calls[0]["value"] == json.dumps({"threadId": "k-9"}).encode(
        "utf-8"
    )
    assert client.produce_calls[0]["_raw"] is True


def test_reactor_capture_from_header_case_sensitive(emit_event, stop_event):
    """capture.from '.headers.rqUID' (case-sensitive, as-produced) (gap 2)."""
    config = KafkaReactor(
        topic="chatx.commands",
        capture={"rqUID": CaptureSpec.model_validate({"from": ".headers.rqUID"})},
        reaction=KafkaReaction(
            topic="chatx.events",
            value={"rs_headers": {"rqUID": "{rqUID}"}},
        ),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"command": "SEARCH"},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {"rqUID": "r-1"},
            }
        ]
    )
    reactor = Reactor(
        name="chatx",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()
    assert len(client.produce_calls) == 1
    assert client.produce_calls[0]["value"] == json.dumps(
        {"rs_headers": {"rqUID": "r-1"}}
    ).encode("utf-8")
    assert client.produce_calls[0]["_raw"] is True


def test_reactor_capture_object_passthrough_context_echo(emit_event, stop_event):
    """type:object copies the context object from value to reaction (gap 3)."""
    config = KafkaReactor(
        topic="chatx.commands",
        capture={"ctx": CaptureSpec.model_validate({"from": ".value.context", "type": "object"})},
        reaction=KafkaReaction(topic="chatx.events", value={"context": "{ctx}"}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {
                    "command": "SEARCH",
                    "context": {"conversationId": "abc", "eventType": "X"},
                },
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {},
            }
        ]
    )
    reactor = Reactor(
        name="chatx",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()
    assert len(client.produce_calls) == 1
    # Real object, not a stringified dict (kafkaThreadHistoryFlow expectation).
    # Post-fix: the rendered object is published as pre-encoded JSON bytes
    # (reaction_codec=None → json.dumps via ``_encode_reaction``), preserving
    # the nested object structure byte-for-byte inside the JSON wire frame.
    assert client.produce_calls[0]["value"] == json.dumps(
        {"context": {"conversationId": "abc", "eventType": "X"}}
    ).encode("utf-8")
    assert client.produce_calls[0]["_raw"] is True


def test_reactor_capture_object_overrides_implicit_scalar(emit_event, stop_event):
    """Explicit object capture overrides an implicit same-name scalar."""
    config = KafkaReactor(
        topic="chatx.commands",
        capture={"context": CaptureSpec.model_validate({"from": ".value.context", "type": "object"})},
        reaction=KafkaReaction(topic="chatx.events", value={"context": "{context}"}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"context": {"k": 1}},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {},
            }
        ]
    )
    reactor = Reactor(
        name="chatx",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()
    assert client.produce_calls[0]["value"] == json.dumps({"context": {"k": 1}}).encode(
        "utf-8"
    )
    assert client.produce_calls[0]["_raw"] is True


def test_reactor_capture_missing_emits_event_and_empty(emit_event, stop_event):
    """A from resolving to nothing -> capture.missing + empty substitution."""
    config = KafkaReactor(
        topic="chatx.commands",
        capture={"x": CaptureSpec.model_validate({"from": ".value.nope"})},
        reaction=KafkaReaction(topic="chatx.events", value={"x": "{x}"}),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"command": "SEARCH"},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {},
            }
        ]
    )
    reactor = Reactor(
        name="chatx",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()
    assert len(client.produce_calls) == 1
    assert client.produce_calls[0]["value"] == json.dumps({"x": ""}).encode("utf-8")
    assert client.produce_calls[0]["_raw"] is True
    missing = [e for e in emit_event.events if e["event"] == "capture.missing"]
    assert len(missing) == 1
    assert missing[0]["reactor"] == "chatx"
    assert missing[0]["name"] == "x"
    assert missing[0]["from"] == ".value.nope"


def test_match_and_capture_share_envelope_root_kafka(emit_event, stop_event):
    """Acceptance test for #22: match and capture share one `.` root.

    A single reactor carries BOTH ``match='.value.command == "SEARCH"'`` AND
    ``capture.rq.from='.headers.rqUID'``, with a reaction value that renders
    the captured header. Feeding a message with
    ``value={"command":"SEARCH"}`` and ``headers={"rqUID":"abc-123"}`` produces
    one reaction whose rendered value carries ``rq == "abc-123"`` -- proving
    reactor ``match`` (``.value.command``) and ``capture`` (``.headers.rqUID``)
    share the message envelope (the ``.value`` and ``.headers`` subtrees hang
    off one root). A message with ``value={"command":"OTHER"}`` produces no
    reaction (match misses).

    Under the pre-#22 value-rooted ``match``, ``.value.command`` would resolve
    against the value as ``value.value.command`` -> ``null`` -> no SEARCH match
    -> no reaction, and the header echo would be impossible.
    """
    config = KafkaReactor(
        topic="chatx.commands",
        match='.value.command == "SEARCH"',
        capture={"rq": CaptureSpec.model_validate({"from": ".headers.rqUID"})},
        reaction=KafkaReaction(
            topic="chatx.events",
            value={"rq": "{rq}"},
        ),
    )
    client = FakeKafkaClient(
        messages=[
            {
                "value": {"command": "SEARCH"},
                "key": None,
                "partition": 0,
                "offset": 0,
                "timestamp": 1719660000000,
                "headers": {"rqUID": "abc-123"},
            },
            {
                "value": {"command": "OTHER"},
                "key": None,
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {"rqUID": "xyz"},
            },
        ]
    )
    reactor = Reactor(
        name="search-reactor",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()

    # Exactly one reaction: SEARCH matched and its rqUID was captured+rendered
    # (proves .value.command and .headers.rqUID agree on one envelope root).
    assert len(client.produce_calls) == 1
    assert client.produce_calls[0]["value"] == json.dumps({"rq": "abc-123"}).encode(
        "utf-8"
    )
    assert client.produce_calls[0]["_raw"] is True

    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1
    assert reacted[0]["reactor"] == "search-reactor"
