"""Unit tests for the KafkaClient codec seam (Task 8).

The codec is a single dependency-injection kwarg ``codec`` on
``KafkaClient.__init__`` with shape::

    {"value": {"fmt": Format, "subject_strategy": str | None} | None,
     "key":   {"fmt": Format, "subject_strategy": str | None} | None,
     "sr":    SchemaRegistryClient | None}

When ``codec=None`` the client behaves byte-for-byte as before (raw JSON
values, string keys). When set, ``produce`` encodes via
:func:`encode_payload` (+ :func:`resolve_subject`) and the consume
methods decode via :func:`decode_payload` (per side, so a key failure
does not lose the value). Single-side decode failures are NON-fatal:
reported via an ``on_decode_error`` callback and the failed side becomes
``None``. Tombstones (``value=None``) decode to ``value=None`` and are
NOT counted as decode errors.

The Avro cases are gated on ``pytest.importorskip("fastavro")`` so CI
without the ``avro`` extra skips rather than errors; the JSON / tombstone
cases run unconditionally.
"""

import struct
import threading
import time

import pytest
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, TopicPartition

from agctl.clients.kafka_client import KafkaClient, ReactionResult
from agctl.serialization import Format
from agctl.serialization.avro_codec import encode_avro
from agctl.serialization.wire import build_wire


# ---------------------------------------------------------------------------
# Fakes (mirrors tests/unit/test_kafka_client.py; trimmed to what we need)
# ---------------------------------------------------------------------------


class FakeMsg:
    """confluent_kafka.Message stand-in for the produce delivery report."""

    def __init__(self, partition, offset, ts_ms):
        self._p, self._o, self._ts = partition, offset, ts_ms

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def timestamp(self):
        return (1, self._ts)


class FakeProducer:
    """Records produce calls and immediately fires the delivery callback."""

    def __init__(self, conf):
        self.conf = conf
        self.calls = []
        self._p, self._o, self._ts = 0, 100, 1719660000000

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append({"topic": topic, "value": value, "key": key, "headers": headers})
        if on_delivery is not None:
            on_delivery(None, FakeMsg(self._p, self._o, self._ts))
        self._o += 1

    def flush(self, timeout):
        return 0


class FakeCMsg:
    """confluent_kafka.Message stand-in for the consume path."""

    def __init__(self, topic, partition, offset, key, value, ts_ms, headers=None):
        self._topic = topic
        self._p, self._o = partition, offset
        self._key, self._value = key, value
        self._ts, self._headers = ts_ms, headers

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


_UNSET = object()


class FakeConsumer:
    """Minimal consumer fake; mirrors the contract in test_kafka_client.py."""

    def __init__(self, conf, messages=None):
        self.conf = conf
        self._messages = list(messages or [])
        self._messages.sort(key=lambda m: (m.partition(), m.offset()))
        self._seek_offsets = {}
        self._cursor = 0
        self._topics = []
        self.closed = False
        self.subscribe_calls = []
        self.store_offsets_calls = []
        self.commit_calls = []
        self.seek_calls = []
        self.poll_calls = 0
        self.on_assign = None
        self.on_revoke = None

    def subscribe(self, topics, on_assign=_UNSET, on_revoke=_UNSET):
        for _n, _c in (("on_assign", on_assign), ("on_revoke", on_revoke)):
            if _c is not _UNSET and not callable(_c):
                raise TypeError(f"{_n} expects a callable")
        self._topics = list(topics)
        self.on_assign = on_assign if on_assign is not _UNSET else None
        self.on_revoke = on_revoke if on_revoke is not _UNSET else None
        self.subscribe_calls.append({"topics": list(topics)})
        if callable(on_assign):
            t = topics[0]
            on_assign([TopicPartition(t, 0), TopicPartition(t, 1)])

    def assignment(self):
        t = self._topics[0]
        return [TopicPartition(t, 0), TopicPartition(t, 1)]

    def offsets_for_times(self, tps):
        out = []
        for tp in tps:
            target_ms = tp.offset
            chosen = -1
            if target_ms is not None and target_ms >= 0:
                for m in self._messages:
                    if m.partition() == tp.partition and m.timestamp()[1] >= target_ms:
                        chosen = m.offset()
                        break
            out.append(TopicPartition(tp.topic, tp.partition, chosen))
        return out

    def seek(self, tp):
        import math

        if tp.offset == OFFSET_BEGINNING:
            off = 0
        elif tp.offset == OFFSET_END:
            off = math.inf
        else:
            off = tp.offset
        self._seek_offsets[(tp.topic, tp.partition)] = off
        self.seek_calls.append(tp)
        if off != math.inf and off >= 0:
            for i, m in enumerate(self._messages):
                seek_off = self._seek_offsets.get((m.topic(), m.partition()), 0)
                if m.offset() >= seek_off:
                    self._cursor = i
                    break

    def poll(self, timeout):
        self.poll_calls += 1
        while self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            seek_off = self._seek_offsets.get((m.topic(), m.partition()), 0)
            if m.offset() >= seek_off:
                self._cursor += 1
                return m
            self._cursor += 1
        return None

    def store_offsets(self, msg):
        self.store_offsets_calls.append(msg)

    def commit(self, offsets=None):
        self.commit_calls.append(offsets)

    def close(self):
        self.closed = True


# Fake SR (mirrors tests/unit/test_serialization_api.py — duck-typed; only the
# methods decode_payload / encode_payload actually call).
SCHEMA_STR = '{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}'
SCHEMA_ID = 17


class FakeSR:
    def __init__(self):
        self.by_id = {SCHEMA_ID: ("AVRO", SCHEMA_STR)}
        self.latest = {"t-value": ("AVRO", SCHEMA_STR, SCHEMA_ID)}
        self.get_schema_calls = 0
        self.get_latest_schema_calls = 0

    def get_schema(self, schema_id):
        self.get_schema_calls += 1
        return self.by_id[schema_id]

    def get_latest_schema(self, subject):
        self.get_latest_schema_calls += 1
        return self.latest[subject]


def _avro_value_bytes(id_value):
    """Confluent-framed Avro bytes for ``{"id": id_value}`` against the test schema."""
    return build_wire(SCHEMA_ID, encode_avro({"id": id_value}, SCHEMA_STR))


# ===========================================================================
# (a, b) produce — encode path
# ===========================================================================


def test_produce_without_codec_publishes_json_bytes():
    """(b) codec=None: produce publishes json.dumps(value) bytes (today's behavior)."""
    fake = FakeProducer({})
    client = KafkaClient("host:9092", producer_factory=lambda c: fake)

    client.produce("t", {"a": 1}, key="k")

    call = fake.calls[0]
    assert call["value"] == b'{"a": 1}'  # legacy JSON encoding
    assert call["key"] == b"k"


def test_produce_with_avro_codec_publishes_confluent_framed_bytes():
    """(a) codec with AVRO value: produce publishes Confluent-framed bytes
    (magic 0x00 + 4-byte BE schema id + Avro payload), and the key is
    utf-8 encoded (KEY_STRING format)."""
    pytest.importorskip("fastavro")
    fake = FakeProducer({})
    sr = FakeSR()
    codec = {
        "value": {"fmt": Format.AVRO, "subject_strategy": "topic"},
        "key": {"fmt": Format.KEY_STRING},
        "sr": sr,
    }
    client = KafkaClient("host:9092", producer_factory=lambda c: fake, codec=codec)

    client.produce("t", {"id": "x"}, key="k")

    value_bytes = fake.calls[0]["value"]
    # Confluent wire frame: magic 0x00 + 4-byte big-endian schema id + payload.
    assert value_bytes[0] == 0
    schema_id = struct.unpack(">I", value_bytes[1:5])[0]
    assert schema_id == SCHEMA_ID
    # The framed bytes round-trip through decode_payload to the original record.
    from agctl.serialization import decode_payload

    assert decode_payload(value_bytes, Format.AVRO, sr) == {"id": "x"}
    # KEY_STRING key is utf-8 bytes — same as today's `key.encode("utf-8")`.
    assert fake.calls[0]["key"] == b"k"
    # The encode went through get_latest_schema (the registered subject).
    assert sr.get_latest_schema_calls == 1


def test_produce_with_codec_keeps_decoded_key_in_return_shape():
    """The returned kafka.produce shape keeps `key` as _decode_bytes(key_bytes)
    (today's behavior) even with a codec set — the return contract is unchanged."""
    pytest.importorskip("fastavro")
    fake = FakeProducer({})
    codec = {
        "value": {"fmt": Format.AVRO, "subject_strategy": "topic"},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    client = KafkaClient("host:9092", producer_factory=lambda c: fake, codec=codec)

    result = client.produce("t", {"id": "x"}, key="k")

    assert result["key"] == "k"  # _decode_bytes(b"k") == "k"
    assert result["topic"] == "t"


# ===========================================================================
# (c, d) consume_window — decode path
# ===========================================================================


def test_consume_window_with_codec_decodes_avro_value():
    """(c) consume_window with codec decodes framed Avro value to a dict."""
    pytest.importorskip("fastavro")
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, b"k0", _avro_value_bytes("x"), now_ms - 100),
    ]
    consumer = FakeConsumer({}, messages=messages)
    codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    client = KafkaClient("host:9092", consumer_factory=lambda c: consumer, codec=codec)

    result = client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=0.02, from_beginning=True
    )

    assert len(result) == 1
    assert result[0]["value"] == {"id": "x"}
    assert result[0]["key"] == "k0"


def test_consume_window_without_codec_decodes_json():
    """(d) codec=None: consume_window decodes value as JSON (today's behavior)."""
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, b"k0", b'{"i": 42}', now_ms - 100),
        # Non-JSON bytes still fall back to a utf-8 string (legacy behavior).
        FakeCMsg(topic, 0, 1, b"k1", b"not-json", now_ms - 50),
    ]
    consumer = FakeConsumer({}, messages=messages)
    client = KafkaClient("host:9092", consumer_factory=lambda c: consumer)

    result = client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=0.02, from_beginning=True
    )

    by_key = {m["key"]: m for m in result}
    assert by_key["k0"]["value"] == {"i": 42}
    assert by_key["k1"]["value"] == "not-json"


# ===========================================================================
# (e) corrupt framed message — non-fatal, counted via on_decode_error
# ===========================================================================


def test_consume_window_corrupt_message_is_non_fatal_and_counted():
    """(e) A single corrupt framed message increments on_decode_error,
    is null-valued in the result, and does NOT raise."""
    pytest.importorskip("fastavro")
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        # good Avro message
        FakeCMsg(topic, 0, 0, b"k0", _avro_value_bytes("x"), now_ms - 100),
        # corrupt: not a Confluent frame (no magic byte) -> decode_payload raises
        FakeCMsg(topic, 0, 1, b"k1", b"not-a-frame", now_ms - 50),
    ]
    consumer = FakeConsumer({}, messages=messages)
    codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    errors = []
    client = KafkaClient(
        "host:9092", consumer_factory=lambda c: consumer, codec=codec
    )

    result = client.consume_window(
        topic,
        lookback_seconds=30,
        timeout_seconds=0.02,
        from_beginning=True,
        on_decode_error=errors.append,
    )

    # No raise. Both messages are in the result; the corrupt one is null-valued.
    by_key = {m["key"]: m for m in result}
    assert by_key["k0"]["value"] == {"id": "x"}
    assert by_key["k1"]["value"] is None
    # Exactly one decode error was reported (labeled with the failed side).
    assert len(errors) == 1
    assert "value" in errors[0]


def test_consume_window_without_on_decode_error_callback_still_non_fatal():
    """on_decode_error defaults to None — a corrupt message is still null-valued
    and does not raise even when no callback is supplied."""
    pytest.importorskip("fastavro")
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, b"k-bad", b"not-a-frame", now_ms - 100),
    ]
    consumer = FakeConsumer({}, messages=messages)
    codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    client = KafkaClient(
        "host:9092", consumer_factory=lambda c: consumer, codec=codec
    )

    result = client.consume_window(
        topic, lookback_seconds=30, timeout_seconds=0.02, from_beginning=True
    )

    assert len(result) == 1
    assert result[0]["value"] is None  # null-valued, no raise


# ===========================================================================
# Tombstone guard (Task 7 review finding)
# ===========================================================================


def test_consume_window_tombstone_decodes_to_none_without_error():
    """A Kafka tombstone (value=None) decodes to value=None and is NOT counted
    as a decode error — it is a delete marker, not a corrupt payload."""
    pytest.importorskip("fastavro")
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        # tombstone — Kafka delete marker (value=None)
        FakeCMsg(topic, 0, 0, b"k-tomb", None, now_ms - 100),
        # healthy Avro message alongside it
        FakeCMsg(topic, 0, 1, b"k-data", _avro_value_bytes("x"), now_ms - 50),
    ]
    consumer = FakeConsumer({}, messages=messages)
    codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    errors = []
    client = KafkaClient(
        "host:9092", consumer_factory=lambda c: consumer, codec=codec
    )

    result = client.consume_window(
        topic,
        lookback_seconds=30,
        timeout_seconds=0.02,
        from_beginning=True,
        on_decode_error=errors.append,
    )

    by_key = {m["key"]: m for m in result}
    # Tombstone: value=None (delete marker), NOT counted as a decode error.
    assert by_key["k-tomb"]["value"] is None
    # The healthy Avro message decoded normally.
    assert by_key["k-data"]["value"] == {"id": "x"}
    # No decode errors recorded (tombstone is not a failure).
    assert errors == []


# ===========================================================================
# (f) consume_loop — decode before handler
# ===========================================================================


def test_consume_loop_with_codec_decodes_before_handler():
    """(f) consume_loop decodes each delivered message before invoking the
    handler — the handler receives the decoded dict, not raw Avro bytes."""
    pytest.importorskip("fastavro")
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, b"k0", _avro_value_bytes("x"), now_ms),
    ]
    consumer = FakeConsumer({}, messages=messages)
    codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    client = KafkaClient("host:9092", consumer_factory=lambda c: consumer, codec=codec)
    stop_event = threading.Event()
    captured = []

    def handle(msg, *, attempt, final):
        captured.append(msg)
        stop_event.set()
        return ReactionResult.COMMIT

    client.consume_loop(
        topic,
        group_id="g",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    assert len(captured) == 1
    # Handler received the DECODED value, not raw Confluent-framed bytes.
    assert captured[0]["value"] == {"id": "x"}
    assert captured[0]["key"] == "k0"


def test_consume_loop_without_codec_decodes_json():
    """consume_loop with codec=None keeps today's JSON decode (no regression)."""
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, b"k0", b'{"i": 1}', now_ms),
    ]
    consumer = FakeConsumer({}, messages=messages)
    client = KafkaClient("host:9092", consumer_factory=lambda c: consumer)
    stop_event = threading.Event()
    captured = []

    def handle(msg, *, attempt, final):
        captured.append(msg)
        stop_event.set()
        return ReactionResult.COMMIT

    client.consume_loop(
        topic,
        group_id="g",
        stop_event=stop_event,
        handle=handle,
        poll_timeout=0.1,
        max_retries=3,
    )

    assert captured[0]["value"] == {"i": 1}


# ===========================================================================
# find_in_window — same codec threading (consistency check)
# ===========================================================================


def test_find_in_window_with_codec_decodes_before_predicate():
    """find_in_window applies the codec before the predicate sees the message —
    so the predicate can match on the decoded dict."""
    pytest.importorskip("fastavro")
    topic = "t"
    now_ms = int(time.time() * 1000)
    messages = [
        FakeCMsg(topic, 0, 0, b"k0", _avro_value_bytes("x"), now_ms - 100),
    ]
    consumer = FakeConsumer({}, messages=messages)
    codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": FakeSR(),
    }
    client = KafkaClient("host:9092", consumer_factory=lambda c: consumer, codec=codec)

    found, scanned = client.find_in_window(
        topic,
        predicate=lambda m: m["value"] == {"id": "x"},
        lookback_seconds=30,
        timeout_seconds=0.02,
        from_beginning=True,
    )

    assert scanned == 1
    assert found is not None
    assert found["value"] == {"id": "x"}
