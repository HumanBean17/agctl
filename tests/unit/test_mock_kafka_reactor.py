"""Unit tests for KafkaReactor (DESIGN §8.3, Task 6).

The reactor orchestrates match/capture/react mechanics with visible skips
and idempotent retries. Tests use a FakeKafkaClient that scripts message
delivery and records produce calls.
"""

import threading
import time

import pytest

from agctl.clients.kafka_client import ReactionResult
from agctl.config.models import KafkaReaction, KafkaReactor
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

    def produce(self, topic, value, *, key=None, headers=None):
        """Record produce call or raise configured error."""
        self.produce_calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": headers,
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
        match='.command == "CREATE_ORDER"',
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
        match='.command == "CREATE_ORDER"',
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

    # Verify produce was called with rendered templates
    assert len(client.produce_calls) == 1
    prod = client.produce_calls[0]
    assert prod["topic"] == "events"
    assert prod["value"] == {"eventType": "ORDER_CREATED", "orderId": "ord-1"}
    assert prod["key"] == "ord-1"
    assert prod["headers"] == {"source": "mock-server"}

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

    # Verify orderId was stringified to "42"
    assert len(client.produce_calls) == 1
    assert client.produce_calls[0]["value"] == {"orderId": "42"}


def test_reactor_jq_non_match_commits_no_event(emit_event, stop_event):
    """jq non-match → COMMIT, no event emitted."""
    config = KafkaReactor(
        topic="commands",
        match='.command == "SHIP_ORDER"',
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

    def failing_produce(topic, value, *, key=None, headers=None):
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
