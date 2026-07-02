"""Unit tests for KafkaClient produce + windowed consume (DESIGN §3.2, D6).

The lookback-window mechanics (D6) are exercised via a FakeConsumer that
implements the same ``offsets_for_times`` + ``seek`` + ``poll`` contract as the
real confluent_kafka Consumer. This lets the seek-by-timestamp logic be unit
tested without a broker.
"""

import json
import math
import time

import pytest
from confluent_kafka import OFFSET_END, TopicPartition

from agctl.clients.kafka_client import KafkaClient
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

    def subscribe(self, topics):
        self._topics = list(topics)

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

    def seek_to_beginning(self, *tps):
        for tp in tps:
            self._seek_offsets[(tp.topic, tp.partition)] = 0

    def poll(self, timeout):
        if self.poll_error:
            return _ErrMsg()
        # Find next canned message at/after the seek offset for its partition.
        while self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            self._cursor += 1
            seek_off = self._seek_offsets.get((m.topic(), m.partition()), 0)
            if m.offset() >= seek_off:
                return m
        return None

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
