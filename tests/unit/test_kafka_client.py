"""Unit tests for KafkaClient produce + windowed consume (DESIGN §3.2, D6).

The lookback-window mechanics (D6) are exercised via a FakeConsumer that
implements the same ``offsets_for_times`` + ``seek`` + ``poll`` contract as the
real confluent_kafka Consumer. This lets the seek-by-timestamp logic be unit
tested without a broker.
"""

import enum
import json
import math
import threading
import time

import pytest
from confluent_kafka import OFFSET_END, TopicPartition

from agctl.clients.kafka_client import KafkaClient, ReactionResult
from agctl.errors import ConfigError, ConnectionFailure


# ---------------------------------------------------------------------------
# Fake seams
# ---------------------------------------------------------------------------


class FakeMsg:
    """Mimics confluent_kafka.Message for the produce delivery report."""

    def __init__(self, partition, offset, ts_ms):
        self._p = partition
        self._o = offset
        self._ts = ts_ms

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def timestamp(self):
        # (type, ms) — type 1 = CREATE_TIME
        return (1, self._ts)


class FakeProducer:
    """Records produce calls and immediately invokes the delivery callback."""

    def __init__(self, conf):
        self.conf = conf
        self.calls = []
        self._p = 0
        self._o = 100
        self._ts = 1719660000000  # 2024-06-29T...

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        if on_delivery is not None:
            on_delivery(None, FakeMsg(self._p, self._o, self._ts))
        self._o += 1

    def flush(self, timeout):
        return 0


class FakeErrProducer(FakeProducer):
    """Producer whose delivery callback always reports an error."""

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        if on_delivery is not None:
            on_delivery(object(), None)  # err is truthy/not None


class FakeTimeoutProducer(FakeProducer):
    """Producer that models an unreachable broker: the delivery callback never
    fires and ``flush`` times out with messages still queued."""

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        # do NOT invoke on_delivery — broker unreachable, callback never fires

    def flush(self, timeout):
        return 1  # one message still queued after the flush timeout


class FakeCMsg:
    """Mimics a consumed confluent_kafka.Message."""

    def __init__(self, topic, partition, offset, key, value, ts_ms, headers=None):
        self._topic = topic
        self._p = partition
        self._o = offset
        self._key = key
        self._value = value
        self._ts = ts_ms
        self._headers = headers

    def topic(self):
        return self._topic

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._key

    def value(self):
        return self._value

    def timestamp(self):
        return (1, self._ts)

    def headers(self):
        return self._headers

    def error(self):
        return None


class FakeConsumer:
    """Fake consumer that models the D6 lookback window.

    Canned messages are provided as a list of FakeCMsg. ``offsets_for_times``
    translates the requested timestamp (carried on the TP's ``.offset`` slot,
    which is how confluent_kafka accepts it) into the earliest offset at/after
    that timestamp for the partition. ``seek`` records the per-partition start
    offset; ``poll`` yields only canned messages whose offset >= the seek
    offset, one per call, then returns None.
    """

    def __init__(self, conf, messages=None, poll_error=False):
        self.conf = conf
        self._messages = list(messages or [])
        # sort by (partition, offset) so poll ordering is deterministic
        self._messages.sort(key=lambda m: (m.partition(), m.offset()))
        self._seek_offsets = {}  # (topic, partition) -> offset
        self._cursor = 0
        self._topics = []
        self.closed = False
        self.poll_error = poll_error
        # New for consume_loop/probe tests
        self.subscribe_calls = []
        self.store_offset_calls = []
        self.commit_calls = []
        self.seek_calls = []
        self.list_topics_calls = []
        self.list_topics_result = None
        self.on_assign = None
        self.on_revoke = None

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self._topics = list(topics)
        self.on_assign = on_assign
        self.on_revoke = on_revoke
        self.subscribe_calls.append({"topics": list(topics), "on_assign": on_assign, "on_revoke": on_revoke})
        # Simulate immediate assignment for tests
        if on_assign:
            t = topics[0]
            on_assign([TopicPartition(t, 0), TopicPartition(t, 1)])

    def assignment(self):
        # Two partitions for the subscribed topic, mirroring tests' setup.
        t = self._topics[0]
        return [TopicPartition(t, 0), TopicPartition(t, 1)]

    def offsets_for_times(self, tps):
        out = []
        for tp in tps:
            target_ms = tp.offset
            chosen = -1
            if target_ms is not None and target_ms >= 0:
                # earliest canned offset on this partition with ts >= target_ms
                for m in self._messages:
                    if m.partition() == tp.partition and m.timestamp()[1] >= target_ms:
                        chosen = m.offset()
                        break
            new_tp = TopicPartition(tp.topic, tp.partition, chosen)
            out.append(new_tp)
        return out

    def seek(self, tp):
        # OFFSET_END means "nothing at/after here" — model it as +inf so poll's
        # `offset >= seek_off` test yields nothing for that partition.
        off = math.inf if tp.offset == OFFSET_END else tp.offset
        self._seek_offsets[(tp.topic, tp.partition)] = off
        self.seek_calls.append(tp)
        # Reset cursor to the earliest message that meets all seek offsets
        # (for retry scenarios and multi-partition seeks).
        if tp.offset != OFFSET_END and tp.offset >= 0:
            # Find the earliest message that meets ALL current seek offsets
            for i, m in enumerate(self._messages):
                seek_off = self._seek_offsets.get((m.topic(), m.partition()), 0)
                if m.offset() >= seek_off:
                    self._cursor = i
                    break

    def seek_to_beginning(self, *tps):
        for tp in tps:
            self._seek_offsets[(tp.topic, tp.partition)] = 0

    def poll(self, timeout):
        if self.poll_error:
            return _ErrMsg()
        # Find next canned message at/after the seek offset for its partition.
        # After a seek, the cursor might be at a message that doesn't meet the seek
        # offset (for other partitions), so we need to skip those.
        while self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            seek_off = self._seek_offsets.get((m.topic(), m.partition()), 0)
            if m.offset() >= seek_off:
                self._cursor += 1  # Only advance cursor if we return this message
                return m
            self._cursor += 1  # Skip messages that don't meet seek offset
        return None

    def store_offset(self, msg):
        """Record store_offset calls (used in consume_loop commit path)."""
        self.store_offset_calls.append(msg)

    def commit(self, offsets=None):
        """Record commit calls (used in consume_loop commit path)."""
        self.commit_calls.append(offsets)

    def list_topics(self, topic=None, timeout=0):
        """Record list_topics calls (used in probe)."""
        self.list_topics_calls.append({"topic": topic, "timeout": timeout})
        if self.list_topics_result is not None:
            return self.list_topics_result
        # Return a fake successful result by default
        return type("obj", (object,), {"topics": [topic]})()

    def close(self):
        self.closed = True


class _ErrMsg:
    def error(self):
        return object()


# ---------------------------------------------------------------------------
# produce
# ---------------------------------------------------------------------------


def test_produce_returns_design_shape():
    fake = FakeProducer({})
    client = KafkaClient(["host:9092"], producer_factory=lambda c: fake)

    result = client.produce("t", {"a": 1}, key="k")

    assert result["topic"] == "t"
    assert result["partition"] == 0
    assert result["offset"] == 100
    assert result["key"] == "k"
    assert result["timestamp"].endswith("Z")
    # ISO8601 parseable
    from datetime import datetime

    parsed = datetime.strptime(result["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    assert parsed.year == 2024


def test_produce_json_encodes_value_and_key():
    fake = FakeProducer({})
    client = KafkaClient("host:9092", producer_factory=lambda c: fake)

    client.produce("t", {"a": 1}, key="k", headers={"h1": "v1"})

    call = fake.calls[0]
    assert call["value"] == b'{"a": 1}'
    assert call["key"] == b"k"
    assert call["headers"] == [("h1", b"v1")]


def test_produce_value_can_be_list():
    fake = FakeProducer({})
    client = KafkaClient("host:9092", producer_factory=lambda c: fake)

    client.produce("t", [1, 2, 3])

    assert fake.calls[0]["value"] == b"[1, 2, 3]"


def test_produce_delivery_error_raises_connection_failure():
    fake = FakeErrProducer({})
    client = KafkaClient(["host:9092"], producer_factory=lambda c: fake)

    with pytest.raises(ConnectionFailure):
        client.produce("t", {"a": 1})


def test_produce_flush_timeout_raises_connection_failure():
    """A flush that leaves messages undelivered (broker unreachable within the
    timeout) is a connection failure, not a silent null-partition/offset success."""
    fake = FakeTimeoutProducer({})
    client = KafkaClient(["host:9092"], producer_factory=lambda c: fake)

    with pytest.raises(ConnectionFailure):
        client.produce("t", {"a": 1})


def test_produce_offset_increments_across_calls():
    fake = FakeProducer({})
    client = KafkaClient(["host:9092"], producer_factory=lambda c: fake)

    r1 = client.produce("t", {"a": 1})
    r2 = client.produce("t", {"a": 2})

    assert r1["offset"] == 100
    assert r2["offset"] == 101


# ---------------------------------------------------------------------------
# extra_conf — TLS/extra librdkafka keys merged into producer + consumer confs
# ---------------------------------------------------------------------------


def test_produce_merges_extra_conf():
    """TLS/extra keys supplied via extra_conf land in the producer conf dict."""
    fake = FakeProducer({})
    extra = {
        "security.protocol": "SSL",
        "ssl.ca.location": "/ca.pem",
        "ssl.certificate.location": "/client.crt",
        "ssl.key.location": "/client.key",
    }

    def factory(conf):
        fake.conf = conf  # capture the conf the client builds
        return fake

    client = KafkaClient(["host:9092"], producer_factory=factory, extra_conf=extra)

    client.produce("t", {"a": 1})

    assert fake.conf["bootstrap.servers"] == "host:9092"
    assert fake.conf["security.protocol"] == "SSL"
    assert fake.conf["ssl.ca.location"] == "/ca.pem"
    assert fake.conf["ssl.certificate.location"] == "/client.crt"
    assert fake.conf["ssl.key.location"] == "/client.key"


def test_consume_merges_extra_conf():
    """TLS/extra keys supplied via extra_conf land in the consumer conf dict."""
    extra = {"security.protocol": "SSL", "ssl.ca.location": "/ca.pem"}
    captured = {}

    def factory(conf):
        captured["conf"] = conf
        return FakeConsumer(conf, messages=[])

    client = KafkaClient(["host:9092"], consumer_factory=factory, extra_conf=extra)

    client.consume_window(
        "t", lookback_seconds=30, timeout_seconds=0.02, from_beginning=True
    )

    conf = captured["conf"]
    assert conf["bootstrap.servers"] == "host:9092"
    assert conf["group.id"] == "agctl-consumer"
    assert conf["security.protocol"] == "SSL"
    assert conf["ssl.ca.location"] == "/ca.pem"


def test_no_extra_conf_keeps_plaintext_conf():
    """Without extra_conf the producer conf has no stray ssl.* keys — a
    plaintext-broker user must never see TLS leakage from a default client."""
    captured = {}

    def factory(conf):
        captured["conf"] = conf
        return FakeProducer(conf)

    client = KafkaClient(["host:9092"], producer_factory=factory)

    client.produce("t", {"a": 1})

    assert captured["conf"] == {"bootstrap.servers": "host:9092"}


# ---------------------------------------------------------------------------
# consume_window — from_beginning
# ---------------------------------------------------------------------------


def _canned(topic):
    now_ms = int(time.time() * 1000)
    return [
        FakeCMsg(topic, 0, 0, "k0", b'{"i":0}', now_ms - 5000),
        FakeCMsg(topic, 0, 1, "k1", b'{"i":1}', now_ms - 3000),
        FakeCMsg(topic, 1, 0, "k2", b'{"i":2}', now_ms - 1000),
    ]


def test_consume_window_from_beginning_returns_all():
    topic = "orders"
    messages = _canned(topic)
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=0.02, from_beginning=True
    )

    assert len(result) == 3
    assert consumer.closed is True
    keys = {m["key"] for m in result}
    assert keys == {"k0", "k1", "k2"}


# ---------------------------------------------------------------------------
# consume_window — lookback window (D6)
# ---------------------------------------------------------------------------


def test_consume_window_lookback_excludes_old_messages():
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        # oldest — older than the lookback window, must be excluded
        FakeCMsg(topic, 0, 0, "old", b'{"i":0}', now_ms - 60_000),
        # within window
        FakeCMsg(topic, 0, 1, "mid", b'{"i":1}', now_ms - 3_000),
        FakeCMsg(topic, 1, 0, "new", b'{"i":2}', now_ms - 1_000),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=10, timeout_seconds=0.02, from_beginning=False
    )

    keys = {m["key"] for m in result}
    assert "old" not in keys
    assert keys == {"mid", "new"}


def test_consume_window_lookback_stale_partition_seeked_to_end():
    """A partition whose every message is older than the window (offsets_for_times
    returns -1) must be seeked past, not re-read via auto.offset.reset=earliest."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        # partition 0 has an in-window message
        FakeCMsg(topic, 0, 0, "fresh0", b'{"i":0}', now_ms - 1_000),
        # partition 1 has ONLY a stale message (older than the window)
        FakeCMsg(topic, 1, 0, "stale1", b'{"i":1}', now_ms - 60_000),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=10, timeout_seconds=0.02, from_beginning=False
    )

    keys = {m["key"] for m in result}
    assert keys == {"fresh0"}
    assert "stale1" not in keys


# ---------------------------------------------------------------------------
# consume_window — value parsing
# ---------------------------------------------------------------------------


def test_consume_window_parses_json_value():
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k0", b'{"event": "X"}', now_ms - 500),
        FakeCMsg(topic, 1, 0, "k1", b"not-json", now_ms - 500),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=10, timeout_seconds=0.02, from_beginning=True
    )

    by_key = {m["key"]: m for m in result}
    assert by_key["k0"]["value"] == {"event": "X"}
    assert by_key["k1"]["value"] == "not-json"


def test_consume_window_normalizes_fields():
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(
            topic,
            0,
            5,
            b"mykey",
            b'{"a":1}',
            now_ms - 100,
            headers=[("trace", b"abc"), ("source", b"agctl")],
        ),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=10, timeout_seconds=0.02, from_beginning=True
    )

    m = result[0]
    assert m["key"] == "mykey"
    assert m["partition"] == 0
    assert m["offset"] == 5
    assert m["timestamp"].endswith("Z")
    assert m["headers"] == {"trace": "abc", "source": "agctl"}


# ---------------------------------------------------------------------------
# consume_window — empty window
# ---------------------------------------------------------------------------


def test_consume_window_empty_returns_empty_list():
    topic = "orders"
    consumer = FakeConsumer({}, messages=[])

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=10, timeout_seconds=0.02, from_beginning=True
    )

    assert result == []
    assert consumer.closed is True


# ---------------------------------------------------------------------------
# consume_window — early-stop on --expect-count (DESIGN §3.2 "whichever comes first")
# ---------------------------------------------------------------------------


def test_consume_window_stops_early_on_expect_count():
    """With expect_count=1, consume_window returns as soon as one matching message
    is collected — it does NOT drain the full window (which would yield all 3)."""
    topic = "orders"
    messages = _canned(topic)  # 3 canned messages
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=2.0,
        from_beginning=True, expect_count=1,
    )

    assert len(result) == 1  # early stop after the first match, not all 3


def test_consume_window_expect_count_returns_before_timeout():
    """A satisfied expect_count returns well before the full timeout elapses."""
    topic = "orders"
    messages = _canned(topic)
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    t0 = time.monotonic()
    client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=2.0,
        from_beginning=True, expect_count=1,
    )
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0  # did NOT wait the full 2.0s window


def test_consume_window_predicate_filters_and_early_stops():
    """A predicate filters messages AND expect_count short-circuits once enough
    matches are in hand — here 2 matches exist but expect_count=1 yields just 1."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "a", b'{"x":0}', now_ms - 1000),
        FakeCMsg(topic, 0, 1, "b", b'{"x":1}', now_ms - 900),
        FakeCMsg(topic, 1, 0, "c", b'{"x":1}', now_ms - 800),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=2.0, from_beginning=True,
        predicate=lambda m: m["value"].get("x") == 1, expect_count=1,
    )

    assert [m["key"] for m in result] == ["b"]  # stopped after the first match


# ---------------------------------------------------------------------------
# consume_loop
# ---------------------------------------------------------------------------


def test_consume_loop_commits_all_messages():
    """consume_loop with COMMIT for all messages → commit called for each."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
        FakeCMsg(topic, 0, 1, "k2", b'{"i":2}', now_ms + 100),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)
    stop_event = threading.Event()
    handle_calls = []

    def handle(msg, *, attempt, final):
        handle_calls.append((msg["key"], attempt, final))
        # Set stop_event after both messages are processed
        if len(handle_calls) >= 2:
            stop_event.set()
        return ReactionResult.COMMIT

    client.consume_loop(
        topic,
        group_id="test-group",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    # Both messages were handled (attempt=1, final=False for both)
    assert len(handle_calls) == 2
    assert handle_calls[0] == ("k1", 1, False)
    assert handle_calls[1] == ("k2", 1, False)
    # Both messages were committed
    assert len(consumer.commit_calls) == 2
    assert len(consumer.store_offset_calls) == 2
    assert consumer.closed is True


def test_consume_loop_retries_then_commits():
    """handle returns RETRY for attempts 1 and 2 on first message, then COMMIT at attempt 3.
    Second message is handled normally (no retries)."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
        FakeCMsg(topic, 0, 1, "k2", b'{"i":2}', now_ms + 100),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)
    stop_event = threading.Event()
    handle_calls = []

    def handle(msg, *, attempt, final):
        key = msg["key"]
        handle_calls.append((key, attempt, final))
        # Only retry the first message (k1), second message (k2) commits immediately
        if key == "k1" and attempt < 3:
            return ReactionResult.RETRY
        # Set stop_event after both messages are fully processed
        # (k1 gets 3 attempts, k2 gets 1 = 4 total handle calls)
        if len(handle_calls) >= 4:
            stop_event.set()
        return ReactionResult.COMMIT

    client.consume_loop(
        topic,
        group_id="test-group",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    # First message: attempts 1, 2 (RETRY), 3 (COMMIT/final=True)
    # Second message: handled once (attempt=1, final=False)
    assert len(handle_calls) == 4
    assert handle_calls[0] == ("k1", 1, False)
    assert handle_calls[1] == ("k1", 2, False)
    assert handle_calls[2] == ("k1", 3, True)  # final attempt
    assert handle_calls[3] == ("k2", 1, False)
    # Two seeks (retry back to k1 twice)
    assert len(consumer.seek_calls) == 2
    # Two commits (one per message)
    assert len(consumer.commit_calls) == 2
    assert consumer.closed is True


def test_consume_loop_retry_on_final_treated_as_commit():
    """handle returns RETRY on final attempt → forced COMMIT (defensive)."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)
    stop_event = threading.Event()
    handle_calls = []

    def handle(msg, *, attempt, final):
        handle_calls.append((msg["key"], attempt, final))
        # Set stop_event after both attempts (final attempt forced commit)
        if len(handle_calls) >= 2:
            stop_event.set()
        return ReactionResult.RETRY  # Even on final!

    client.consume_loop(
        topic,
        group_id="test-group",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=2,  # Only 2 attempts total
    )

    # Attempt 1 (RETRY, final=False), attempt 2 (RETRY treated as COMMIT, final=True)
    assert len(handle_calls) == 2
    assert handle_calls[0] == ("k1", 1, False)
    assert handle_calls[1] == ("k1", 2, True)
    # Message was committed despite RETRY on final (Fix 4: also assert store_offset)
    assert len(consumer.commit_calls) == 1
    assert len(consumer.store_offset_calls) == 1  # Fix 4: assert store_offset was called
    assert consumer.closed is True


def test_consume_loop_stop_exits_immediately():
    """handle returns STOP → loop exits, consumer closed."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
        FakeCMsg(topic, 0, 1, "k2", b'{"i":2}', now_ms + 100),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)
    stop_event = threading.Event()
    handle_calls = []

    def handle(msg, *, attempt, final):
        handle_calls.append((msg["key"], attempt, final))
        return ReactionResult.STOP

    client.consume_loop(
        topic,
        group_id="test-group",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    # Only first message was handled
    assert len(handle_calls) == 1
    assert handle_calls[0] == ("k1", 1, False)
    # No commits (STOP exits before commit)
    assert len(consumer.commit_calls) == 0
    assert consumer.closed is True


def test_consume_loop_stop_event_exits_without_handling():
    """stop_event set before first poll → loop exits without calling handle."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)
    stop_event = threading.Event()
    stop_event.set()  # Set before loop starts
    handle_calls = []

    def handle(msg, *, attempt, final):
        handle_calls.append((msg["key"], attempt, final))
        return ReactionResult.COMMIT

    client.consume_loop(
        topic,
        group_id="test-group",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    # Handle was never called
    assert len(handle_calls) == 0
    # No commits
    assert len(consumer.commit_calls) == 0
    assert consumer.closed is True


def test_consume_loop_uses_group_id_parameter():
    """consume_loop uses the group_id parameter, not self._group_id."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    # Create client with default group_id
    client = KafkaClient(["host:9092"], group_id="default-group", consumer_factory=factory)
    stop_event = threading.Event()

    def handle(msg, *, attempt, final):
        # Set stop_event after first message
        stop_event.set()
        return ReactionResult.COMMIT

    client.consume_loop(
        topic,
        group_id="override-group",  # Should use this, not "default-group"
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    # Consumer was built with the override group_id
    assert consumer.conf["group.id"] == "override-group"


def test_consume_loop_registers_rebalance_callbacks():
    """on_assign and on_revoke are registered with subscribe."""
    topic = "orders"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, "k1", b'{"i":1}', now_ms),
    ]
    consumer = FakeConsumer({}, messages=messages)

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)
    stop_event = threading.Event()

    assign_calls = []
    revoke_calls = []

    def on_assign(tps):
        assign_calls.append(tps)

    def on_revoke(tps):
        revoke_calls.append(tps)

    def handle(msg, *, attempt, final):
        return ReactionResult.STOP

    client.consume_loop(
        topic,
        group_id="test-group",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
        on_assign=on_assign,
        on_revoke=on_revoke,
    )

    # Callbacks were registered and on_assign was called (simulated immediate assignment)
    assert len(consumer.subscribe_calls) == 1
    assert consumer.subscribe_calls[0]["on_assign"] is on_assign
    assert consumer.subscribe_calls[0]["on_revoke"] is on_revoke
    assert len(assign_calls) == 1  # Called immediately by FakeConsumer


# ---------------------------------------------------------------------------
# probe
# ---------------------------------------------------------------------------


def test_probe_returns_on_success():
    """probe with successful list_topics → returns None, consumer closed."""
    topic = "orders"
    consumer = FakeConsumer({})

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    result = client.probe(topic, group_id="test-group", timeout=1.0)

    # probe returns None on success
    assert result is None
    # list_topics was called
    assert len(consumer.list_topics_calls) == 1
    assert consumer.list_topics_calls[0]["topic"] == topic
    assert consumer.list_topics_calls[0]["timeout"] == 1.0
    # Consumer was closed
    assert consumer.closed is True


def test_probe_raises_connection_failure_on_error():
    """probe with list_topics raising → ConnectionFailure raised, consumer closed."""
    topic = "orders"

    class BrokenConsumer(FakeConsumer):
        def list_topics(self, topic=None, timeout=0):
            raise Exception("broker unreachable")

    consumer = BrokenConsumer({})

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["host:9092"], consumer_factory=factory)

    with pytest.raises(ConnectionFailure, match="broker unreachable"):
        client.probe(topic, group_id="test-group", timeout=1.0)

    # Consumer was still closed
    assert consumer.closed is True


def test_probe_raises_config_error_on_missing_kafka_extra():
    """probe with _import_kafka raising ConfigError → propagates."""
    topic = "orders"

    client = KafkaClient(["host:9092"])

    # Patch _import_kafka to raise ConfigError
    import agctl.clients.kafka_client as kc_module
    original_import = kc_module._import_kafka

    def mock_import():
        raise ConfigError("missing kafka extra")

    kc_module._import_kafka = mock_import

    try:
        with pytest.raises(ConfigError, match="missing kafka extra"):
            client.probe(topic, group_id="test-group", timeout=1.0)
    finally:
        kc_module._import_kafka = original_import


def test_probe_uses_group_id_parameter():
    """probe uses the group_id parameter, not self._group_id."""
    topic = "orders"
    consumer = FakeConsumer({})

    def factory(conf):
        consumer.conf = conf
        return consumer

    # Create client with default group_id
    client = KafkaClient(["host:9092"], group_id="default-group", consumer_factory=factory)

    client.probe(topic, group_id="override-group", timeout=1.0)

    # Consumer was built with the override group_id
    assert consumer.conf["group.id"] == "override-group"


def test_probe_error_message_includes_brokers():
    """ConnectionFailure from probe includes broker list."""
    topic = "orders"

    class BrokenConsumer(FakeConsumer):
        def list_topics(self, topic=None, timeout=0):
            raise Exception("timeout")

    consumer = BrokenConsumer({})

    def factory(conf):
        consumer.conf = conf
        return consumer

    client = KafkaClient(["broker1:9092", "broker2:9092"], consumer_factory=factory)

    with pytest.raises(ConnectionFailure, match="broker1:9092.*broker2:9092"):
        client.probe(topic, group_id="test-group", timeout=1.0)
