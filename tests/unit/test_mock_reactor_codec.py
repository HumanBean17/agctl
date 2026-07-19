"""Unit tests for KafkaReactor Avro/Protobuf decode-trigger + encode-reaction
plumbing (Task 12).

Per the task brief, four reactor-level scenarios driven by a fake
``KafkaClient`` whose ``consume_loop`` yields decoded messages (and can
script a decode-error delivery):

(a) AVRO trigger -> JSON reaction: ``match`` evaluates against the decoded
    dict and the reaction is published as a JSON dict (today's path — the
    reaction_codec is None so the reactor does not encode).
(b) JSON trigger -> AVRO reaction: the reactor encodes the rendered value
    via ``encode_payload`` against the reaction codec's SR client and
    publishes Confluent-framed bytes (``\\x00``-prefixed, matching the
    registered schema id) to ``client.produce(_raw=True)``.
(c) trigger decode failure -> exactly one ``kafka.skipped`` event with a
    ``decode failed: ...`` reason and NO ``kafka.reacted`` event (non-fatal,
    COMMIT past the corrupt message).
(d) reaction encode failure (payload violates schema) -> exactly one
    ``kafka.error`` event with ``fatal`` set (the encode raises
    ``SerializationError``; the existing retry-then-error flow emits the
    event on the final attempt).

The fake client mirrors ``tests/unit/test_listen_codec.py::_FakeKafkaClient``
but also records ``_raw`` on produce calls so the JSON-vs-Avro publish path
can be asserted directly.
"""

from __future__ import annotations

import struct
import threading

import pytest

from agctl.clients.kafka_client import ReactionResult
from agctl.config.models import KafkaReaction, KafkaReactor
from agctl.errors import SerializationError
from agctl.mock.kafka_reactor import KafkaReactor as Reactor


# ---------------------------------------------------------------------------
# Avro schema fixtures (mirror tests/unit/test_kafka_client_codec.py)
# ---------------------------------------------------------------------------

SCHEMA_STR = (
    '{"type":"record","name":"E",'
    '"fields":[{"name":"id","type":"string"},'
    '{"name":"qty","type":"int"}]}'
)
SCHEMA_ID = 17


class _FakeSR:
    """Duck-typed SR stand-in: the methods ``encode_payload`` actually calls."""

    def __init__(self):
        self.by_id = {SCHEMA_ID: ("AVRO", SCHEMA_STR)}
        # The reaction topic is "events"; encode resolves subject "events-value".
        self.latest = {"events-value": ("AVRO", SCHEMA_STR, SCHEMA_ID)}
        self.get_latest_schema_calls = 0

    def get_schema(self, schema_id):
        return self.by_id[schema_id]

    def get_latest_schema(self, subject):
        self.get_latest_schema_calls += 1
        return self.latest[subject]


# ---------------------------------------------------------------------------
# Fake KafkaClient (duck-types consume_loop/probe/produce + on_decode_error)
# ---------------------------------------------------------------------------


class _DecodeErr:
    """Scripted decode-error delivery: ``on_decode_error`` then ``handle``.

    Mirrors the real ``KafkaClient._normalize_message`` order: the codec
    seam fires ``on_decode_error`` BEFORE ``handle`` is called for the
    partially-decoded message (the failed side is None).
    """

    def __init__(self, error_label: str, msg: dict):
        self.error_label = error_label
        self.msg = msg


class _GoodMsg:
    """A scripted decoded message delivery (no decode error)."""

    def __init__(self, msg: dict):
        self.msg = msg


class FakeKafkaClient:
    """Fake ``KafkaClient``: scripts message + decode-error deliveries.

    - ``consume_loop``: iterates the script; for ``_GoodMsg`` calls
      ``handle(msg, ...)``; for ``_DecodeErr`` calls ``on_decode_error(label)``
      then ``handle(msg, ...)`` (matching the real client's order). Accepts
      and records the ``on_decode_error`` callback so the codec seam contract
      is exercised.
    - ``probe``: records the probe call (no-op).
    - ``produce``: records the call including the ``_raw`` flag so the
      encode-vs-raw publish path can be asserted.
    """

    def __init__(self, script=None, probe_raises=None):
        self.script = list(script or [])
        self.probe_raises = probe_raises
        self.consume_calls: list[dict] = []
        self.probe_calls: list[dict] = []
        self.produce_calls: list[dict] = []
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
        """Scripted delivery: iterate the script, honoring stop_event."""
        self.consume_calls.append(
            {
                "topic": topic,
                "group_id": group_id,
                "max_retries": max_retries,
                "on_decode_error": on_decode_error,
            }
        )
        self.stop_event = stop_event
        self.handle = handle
        self.max_retries = max_retries

        for item in self.script:
            if stop_event.is_set():
                break
            if isinstance(item, _GoodMsg):
                self._deliver(item.msg, handle, max_retries, stop_event)
            elif isinstance(item, _DecodeErr):
                if on_decode_error is not None:
                    on_decode_error(item.error_label)
                self._deliver(item.msg, handle, max_retries, stop_event)

    @staticmethod
    def _deliver(msg, handle, max_retries, stop_event):
        """Mirror the real consume_loop's retry-then-final delivery."""
        for attempt in range(1, max_retries + 1):
            if stop_event.is_set():
                return
            final = attempt >= max_retries
            result = handle(msg, attempt=attempt, final=final)
            if result == ReactionResult.COMMIT:
                return
            if result == ReactionResult.STOP:
                stop_event.set()
                return
            # RETRY (non-final): re-handle the same message.

    def probe(self, topic, *, group_id, timeout=5.0):
        self.probe_calls.append({"topic": topic, "group_id": group_id, "timeout": timeout})
        if self.probe_raises:
            raise self.probe_raises

    def produce(self, topic, value, *, key=None, headers=None, _raw=False):
        """Record the produce call (including the ``_raw`` bypass flag)."""
        self.produce_calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": headers,
                "_raw": _raw,
            }
        )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _msg(value, *, key=None, offset=0, partition=0, headers=None) -> dict:
    """Build a normalized message dict (KafkaClient._normalize_message shape)."""
    return {
        "key": key,
        "value": value,
        "partition": partition,
        "offset": offset,
        "timestamp": "2026-07-18T00:00:00Z",
        "headers": headers or {},
    }


@pytest.fixture
def emit_event():
    events = []

    def emit(event_dict):
        events.append(event_dict)

    emit.events = events
    return emit


@pytest.fixture
def stop_event():
    return threading.Event()


def _avro_reaction_codec() -> dict:
    """Reaction codec with AVRO value + KEY_STRING key + a fake SR client."""
    from agctl.serialization import Format

    return {
        "value": {"fmt": Format.AVRO, "subject_strategy": "topic"},
        "key": {"fmt": Format.KEY_STRING, "subject_strategy": "topic"},
        "sr": _FakeSR(),
    }


# ===========================================================================
# (a) AVRO trigger -> JSON reaction
# ===========================================================================


def test_avro_trigger_json_reaction_match_evaluates_against_decoded_dict(
    emit_event, stop_event
):
    """(a) Decoded Avro trigger (dict) -> match -> JSON reaction.

    The trigger client (codec set on the CLIENT, not the reactor) decodes
    Avro bytes to a dict before delivery; the reactor's ``match`` jq
    predicate evaluates against that decoded dict. The reaction is
    published as JSON bytes via ``produce(_raw=True)``: post-fix the
    reactor ALWAYS encodes via ``_encode_reaction`` (which delegates to
    ``_encode_payload_with_codec`` and returns legacy ``json.dumps``
    bytes when ``reaction_codec is None``) and ALWAYS publishes
    ``_raw=True`` so the trigger client's own (Avro) codec is never
    consulted on the reaction path — see
    ``test_avro_trigger_json_reaction_does_not_route_through_trigger_codec``
    for the regression that pinned this.
    """
    pytest.importorskip("fastavro")
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(
            topic="events",
            key="{orderId}",
            value={"eventType": "ORDER_CREATED", "orderId": "{orderId}"},
        ),
    )
    client = FakeKafkaClient(
        script=[
            _GoodMsg(
                _msg(
                    {"orderId": "ord-1", "command": "CREATE_ORDER"},
                    key="ord-1",
                )
            ),
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
        reaction_codec=None,  # JSON reaction -> legacy json.dumps bytes
    )

    reactor.run()

    # One JSON publish: value bytes are json.dumps(rendered) (NOT a dict,
    # NOT Confluent-Avro-framed bytes); key is utf-8 bytes; _raw True (the
    # fix collapsed the two-branch publish path).
    assert len(client.produce_calls) == 1
    prod = client.produce_calls[0]
    assert prod["topic"] == "events"
    assert prod["value"] == b'{"eventType": "ORDER_CREATED", "orderId": "ord-1"}'
    assert prod["key"] == b"ord-1"
    assert prod["_raw"] is True

    # kafka.reacted emitted; no skipped/error/decode events.
    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1
    assert reacted[0]["reactor"] == "order-reactor"
    assert not any(e["event"] == "kafka.skipped" for e in emit_event.events)
    assert not any(e["event"] == "kafka.error" for e in emit_event.events)


# ===========================================================================
# (a-regression) Avro TRIGGER + JSON reaction via a REAL KafkaClient
# ===========================================================================


class _RecordingProducer:
    """Minimal confluent_kafka.Producer stand-in that records produce calls.

    Used by the Avro-trigger regression test to capture the reaction's
    published bytes (so the test can assert the wire frame directly).
    Fires the delivery callback immediately, matching ``FakeProducer`` in
    ``tests/unit/test_kafka_client.py``.
    """

    def __init__(self, conf):
        self.conf = conf
        self.calls = []
        self._p, self._o, self._ts = 0, 100, 1719660000000

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        if on_delivery is not None:
            on_delivery(None, _DeliveryMsg(self._p, self._o, self._ts))
        self._o += 1

    def flush(self, timeout):
        return 0


class _DeliveryMsg:
    """confluent_kafka.Message stand-in for the produce delivery report."""

    def __init__(self, partition, offset, ts_ms):
        self._p, self._o, self._ts = partition, offset, ts_ms

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def timestamp(self):
        return (1, self._ts)


class _LoopCMsg:
    """confluent_kafka.Message stand-in ``consume_loop`` polls off a topic."""

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


class _LoopConsumer:
    """Minimal consumer for ``KafkaClient.consume_loop``.

    Implements only the methods ``consume_loop`` exercises: ``subscribe``,
    ``poll``, ``store_offsets``, ``commit``, ``close``. Yields each scripted
    message exactly once; once exhausted, sets the bound ``stop_event`` and
    returns ``None`` so the consume_loop's ``while not stop_event.is_set()``
    can terminate (without this the loop would poll forever).
    """

    def __init__(self, conf, messages, stop_event):
        self.conf = conf
        self._messages = list(messages)
        self._cursor = 0
        self._stop_event = stop_event
        self.store_offsets_calls = []
        self.commit_calls = []
        self.closed = False

    def subscribe(self, topics, **kwargs):
        self._topics = list(topics)

    def poll(self, timeout):
        if self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            self._cursor += 1
            return m
        # Out of messages: arm the stop_event so consume_loop exits, then
        # return None (mirrors "no more data" without an external setter).
        self._stop_event.set()
        return None

    def store_offsets(self, msg):
        self.store_offsets_calls.append(msg)

    def commit(self, offsets=None):
        self.commit_calls.append(offsets)

    def close(self):
        self.closed = True


def test_avro_trigger_json_reaction_does_not_route_through_trigger_codec(
    emit_event, stop_event
):
    """(a-regression) Avro TRIGGER client + JSON reaction publishes JSON bytes.

    The companion to (a) that uses a REAL ``KafkaClient`` constructed WITH
    the trigger Avro codec via the ``producer_factory``/``consumer_factory``
    seams (NOT a scripted Fake). The trigger client genuinely carries the
    Avro codec on both the consume (decode) AND the produce (encode) sides,
    so any leakage of the reaction through the trigger codec on the publish
    path is observed as Confluent-Avro-framed bytes at the producer.

    Pre-fix, the reactor's reaction publish had two branches. When
    ``reaction_codec is None`` it took the legacy branch:
    ``client.produce(topic, rendered_value, key=..., headers=...)`` with NO
    ``_raw=True``. That delegated encoding to the trigger client's own
    ``_encode_payload``, which — for an Avro-trigger reactor — mis-encoded
    the rendered JSON reaction dict as Confluent-Avro-framed bytes against
    ``<reaction_topic>-value`` (DESIGN §7.4 violation: formats must resolve
    INDEPENDENTLY per direction). Post-fix the reactor ALWAYS encodes via
    ``_encode_reaction`` (which returns legacy ``json.dumps`` bytes when
    ``reaction_codec is None``) and ALWAYS publishes via
    ``produce(..., _raw=True)``, so the trigger client's Avro codec is
    consulted ONLY on consume (decode) and never on the reaction (encode).

    Asserts the produced value bytes are EXACTLY the rendered JSON (and
    explicitly do NOT start with the Confluent magic byte ``b"\\x00"``),
    and that the Avro trigger value was nonetheless decoded correctly
    (the reactor's ``match`` saw the decoded dict → one ``kafka.reacted``).
    """
    pytest.importorskip("fastavro")
    import json as _json

    from agctl.clients.kafka_client import KafkaClient
    from agctl.serialization import Format
    from agctl.serialization.avro_codec import encode_avro
    from agctl.serialization.wire import build_wire

    # Trigger topic codec: Avro value (the bug surface). The reaction codec
    # is None (JSON reaction — the legacy byte-identical path the fix preserves).
    trigger_schema_str = (
        '{"type":"record","name":"Cmd",'
        '"fields":[{"name":"orderId","type":"string"},'
        '{"name":"command","type":"string"}]}'
    )
    trigger_schema_id = 71

    class _AvroTriggerSR:
        """SR double serving the trigger schema by id (decode-only path).

        ``get_latest_schema`` is unused: ``reaction_codec=None`` means the
        reactor never resolves a reaction subject, and ``_raw=True`` means
        the trigger client's produce path skips its own encode entirely.
        """

        def __init__(self):
            self.by_id = {trigger_schema_id: ("AVRO", trigger_schema_str)}
            self.latest = {}

        def get_schema(self, schema_id):
            return self.by_id[schema_id]

        def get_latest_schema(self, subject):
            return self.latest[subject]

    sr = _AvroTriggerSR()
    trigger_codec = {
        "value": {"fmt": Format.AVRO},
        "key": {"fmt": Format.KEY_STRING},
        "sr": sr,
    }

    trigger_topic = "commands"
    reaction_topic = "events"
    trigger_value_bytes = build_wire(
        trigger_schema_id,
        encode_avro(
            {"orderId": "ord-1", "command": "CREATE_ORDER"},
            trigger_schema_str,
        ),
    )

    now_ms = 1719660000000
    cmsg = _LoopCMsg(
        trigger_topic, 0, 0, b"ord-1", trigger_value_bytes, now_ms
    )
    # The consumer is built by the client's ``_build_consumer``; bind the
    # test's stop_event so poll() can terminate the loop when messages run out.
    consumer = _LoopConsumer({}, [cmsg], stop_event)
    producer = _RecordingProducer({})

    client = KafkaClient(
        "host:9092",
        consumer_factory=lambda conf: consumer,
        producer_factory=lambda conf: producer,
        codec=trigger_codec,
    )

    config = KafkaReactor(
        topic=trigger_topic,
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(
            topic=reaction_topic,
            key="{orderId}",
            value={"eventType": "ORDER_CREATED", "orderId": "{orderId}"},
        ),
    )
    reactor = Reactor(
        name="avro-trigger-json-reaction",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
        reaction_codec=None,  # JSON reaction — the legacy byte-identical path
    )

    reactor.run()

    # Exactly one produce reached the underlying producer (the reaction).
    assert len(producer.calls) == 1
    prod = producer.calls[0]
    assert prod["topic"] == reaction_topic

    value_bytes = prod["value"]
    # Regression core: the reaction is JSON, NOT Confluent-Avro-framed.
    # The Avro wire frame starts with the magic byte 0x00; JSON cannot
    # (it starts with ``{`` → 0x7B). The bug would have produced 0x00.
    assert isinstance(value_bytes, (bytes, bytearray))
    assert value_bytes[:1] != b"\x00", (
        f"reaction mis-encoded as Confluent-Avro-framed: "
        f"first bytes = {bytes(value_bytes[:5])!r}"
    )
    # The reaction bytes are EXACTLY the rendered JSON (the legacy path's
    # ``json.dumps(rendered_value).encode("utf-8")``).
    assert value_bytes == _json.dumps(
        {"eventType": "ORDER_CREATED", "orderId": "ord-1"}
    ).encode("utf-8")
    # The key is utf-8 bytes (KEY_STRING legacy path), not Avro-framed.
    assert prod["key"] == b"ord-1"

    # The trigger Avro value WAS decoded (match saw the dict → one reaction).
    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1
    assert reacted[0]["reactor"] == "avro-trigger-json-reaction"
    assert reacted[0]["topic"] == reaction_topic
    assert reacted[0]["key"] == "ord-1"
    # No errors/skips — the trigger decoded cleanly, the reaction encoded cleanly.
    assert not any(e["event"] == "kafka.error" for e in emit_event.events)
    assert not any(e["event"] == "kafka.skipped" for e in emit_event.events)


# ===========================================================================
# (b) JSON trigger -> AVRO reaction
# ===========================================================================


def test_json_trigger_avro_reaction_publishes_confluent_framed_bytes(
    emit_event, stop_event
):
    """(b) JSON trigger -> AVRO reaction published as Confluent-framed bytes.

    The reactor encodes the rendered value via ``encode_payload`` against
    the reaction codec's SR client (subject resolved to ``events-value``
    via the ``topic`` strategy) and publishes the framed bytes via
    ``client.produce(_raw=True)``. The fake SR's ``get_latest_schema`` is
    called exactly once.
    """
    pytest.importorskip("fastavro")
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(
            topic="events",
            key="{orderId}",
            value={"id": "{orderId}", "qty": 1},
        ),
    )
    codec = _avro_reaction_codec()
    client = FakeKafkaClient(
        script=[
            _GoodMsg(
                _msg(
                    {"orderId": "ord-1", "command": "CREATE_ORDER"},
                    key="ord-1",
                )
            ),
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
        reaction_codec=codec,
    )

    reactor.run()

    # Exactly one produce, raw bytes (encode happened in the reactor).
    assert len(client.produce_calls) == 1
    prod = client.produce_calls[0]
    assert prod["topic"] == "events"
    assert prod["_raw"] is True

    value_bytes = prod["value"]
    assert isinstance(value_bytes, (bytes, bytearray))
    # Confluent wire frame: magic 0x00 + 4-byte big-endian schema id + payload.
    assert value_bytes[0] == 0
    schema_id = struct.unpack(">I", bytes(value_bytes[1:5]))[0]
    assert schema_id == SCHEMA_ID

    # KEY_STRING key encoded as utf-8 bytes.
    assert prod["key"] == b"ord-1"

    # The encode went through get_latest_schema exactly once for "events-value".
    assert codec["sr"].get_latest_schema_calls == 1

    # Round-trip: decode the framed bytes back to the rendered record.
    from agctl.serialization import decode_payload

    assert decode_payload(bytes(value_bytes), codec["value"]["fmt"], codec["sr"]) == {
        "id": "ord-1",
        "qty": 1,
    }

    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1


# ===========================================================================
# (c) trigger decode failure -> kafka.skipped, no kafka.reacted
# ===========================================================================


def test_trigger_decode_failure_emits_skipped_with_decode_failed_reason(
    emit_event, stop_event
):
    """(c) Decode failure reported via ``on_decode_error`` -> one
    ``kafka.skipped`` event with a ``"decode failed: ..."`` reason and no
    ``kafka.reacted`` event.

    Mirrors the existing non-object skip semantics: non-fatal, COMMIT, no
    produce call. The reactor's ``_on_decode_error`` callback is armed
    BEFORE ``_handle`` runs (the codec seam contract); ``_handle`` checks
    the per-message flag at the top and COMMITs past the corrupt message.
    """
    pytest.importorskip("fastavro")
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(topic="events", value={"id": "{orderId}"}),
    )
    codec = _avro_reaction_codec()
    client = FakeKafkaClient(
        script=[
            _DecodeErr(
                "value: not a Confluent frame",
                # Partially-decoded message: value is None (decode failed).
                _msg(None, key="ord-1"),
            ),
        ]
    )
    reactor = Reactor(
        name="decode-skip-reactor",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
        reaction_codec=codec,
    )

    reactor.run()

    # Exactly one kafka.skipped event with the decode-failed reason.
    skipped = [e for e in emit_event.events if e["event"] == "kafka.skipped"]
    assert len(skipped) == 1
    evt = skipped[0]
    assert evt["reactor"] == "decode-skip-reactor"
    assert evt["topic"] == "commands"
    assert evt["reason"].startswith("decode failed")
    assert "not a Confluent frame" in evt["reason"]
    assert evt["count"] == 1

    # No reaction produced, no kafka.reacted, no kafka.error.
    assert len(client.produce_calls) == 0
    assert not any(e["event"] == "kafka.reacted" for e in emit_event.events)
    assert not any(e["event"] == "kafka.error" for e in emit_event.events)


# ===========================================================================
# (d) reaction encode failure (schema violation) -> kafka.error (fatal)
# ===========================================================================


def test_reaction_encode_failure_emits_kafka_error_with_fatal_set(
    emit_event, stop_event
):
    """(d) Rendered value violates the reaction schema -> one ``kafka.error``
    event with ``fatal`` set after the retry budget is exhausted.

    The encode raises ``SerializationError`` (fastavro rejects ``int`` for a
    ``string`` field); the existing ``except Exception`` retry-then-error
    flow retries up to ``max_retries`` (the encode is deterministic, so each
    attempt fails the same way), emits exactly one ``kafka.error`` on the
    final attempt, and returns STOP when ``fail_fast=True``.
    """
    pytest.importorskip("fastavro")
    # Schema requires id:string, qty:int. Rendered value has id:int -> violates.
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(topic="events", value={"id": 123, "qty": 1}),
    )
    codec = _avro_reaction_codec()
    encode_call_count = [0]

    # Sanity: encode_payload with this value raises SerializationError, so the
    # reactor's encode path will raise it inside _handle. (Verified separately
    # so a regression in encode_payload surfaces as a clear failure here.)
    from agctl.serialization import Format, encode_payload

    with pytest.raises(SerializationError):
        encode_payload(
            {"id": 123, "qty": 1},
            Format.AVRO,
            codec["sr"],
            subject="events-value",
        )

    client = FakeKafkaClient(
        script=[
            _GoodMsg(
                _msg(
                    {"orderId": "ord-1", "command": "CREATE_ORDER"},
                    key="ord-1",
                )
            ),
        ]
    )
    reactor = Reactor(
        name="encode-fail-reactor",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=True,  # fatal flag set on kafka.error
        run_id="run-1",
        reaction_codec=codec,
    )

    reactor.run()

    # The encode failed on every retry; no produce call ever reached the client.
    assert len(client.produce_calls) == 0

    # Exactly one kafka.error event on the final attempt, fatal=True.
    errors = [e for e in emit_event.events if e["event"] == "kafka.error"]
    assert len(errors) == 1
    evt = errors[0]
    assert evt["reactor"] == "encode-fail-reactor"
    assert evt["topic"] == "commands"
    assert evt["fatal"] is True
    # The error message references the Avro encode failure.
    assert "Avro encode failed" in evt["error"] or "SerializationError" in evt["error"] \
        or "encode" in evt["error"].lower()

    # No kafka.reacted; no kafka.skipped (the trigger decoded fine).
    assert not any(e["event"] == "kafka.reacted" for e in emit_event.events)
    assert not any(e["event"] == "kafka.skipped" for e in emit_event.events)

    # fail_fast + STOP -> stop_event set by the reactor's return value.
    assert stop_event.is_set()


# ===========================================================================
# (e) regression: key-side resolve_subject must receive the KEY (not value)
# ===========================================================================


def test_reaction_avro_key_resolves_record_subject_from_key_payload(
    emit_event, stop_event
):
    """(e) Regression: a non-string KEY format under ``subject_strategy="record"``
    resolves the key subject from the KEY's ``__record_name__`` (not the value's).

    A previous copy of the encode logic in ``KafkaReactor._encode_reaction``
    duplicated ~30 lines from ``KafkaClient._encode_payload`` and the copy
    diverged: the key-side ``resolve_subject(...)`` call passed ``value`` as
    the 4th argument instead of ``key``. The bug was dormant under the
    default ``"topic"`` strategy (which ignores the payload) but LIVE under
    ``"record"``/``"topic_record"``: a reactor whose reaction KEY uses a
    non-string format (Avro) would resolve the wrong subject (reading
    ``__record_name__`` off the VALUE instead of the KEY, falling back to
    the topic-name subject when the value has no record name).

    This pins the post-fix behavior via the full reactor flow: an
    object-typed capture injects a dict KEY carrying
    ``__record_name__="OrderKey"``; the VALUE has NO ``__record_name__``.
    The fake SR records every ``get_latest_schema`` query — the key encode
    MUST hit ``"OrderKey"`` (the key's record-name subject), not
    ``"events-key"`` (the fallback the bug produced by reading the value,
    which lacks a record name — and which the SR does NOT have, so the
    encode would have raised).
    """
    pytest.importorskip("fastavro")
    from agctl.config.models import CaptureSpec
    from agctl.serialization import Format

    # Key schema's record name is "OrderKey"; ``__record_name__`` is included
    # as a field so fastavro's strict=True accepts the rendered key datum
    # (which carries the reserved key to drive subject resolution). The value
    # schema reuses the module-level SCHEMA_STR (record name "E").
    KEY_SCHEMA_STR = (
        '{"type":"record","name":"OrderKey",'
        '"fields":[{"name":"orderId","type":"string"},'
        '{"name":"__record_name__","type":"string"}]}'
    )
    KEY_SCHEMA_ID = 23

    class _RecordSubjectSR(_FakeSR):
        """Extends the value-only fake with a separate key-record schema.

        Records every queried subject so the test can assert the key encode
        hit the key's record-name subject (not the value's, not the
        topic-name fallback).
        """

        def __init__(self):
            super().__init__()  # registers events-value -> value schema (E)
            self.by_id[KEY_SCHEMA_ID] = ("AVRO", KEY_SCHEMA_STR)
            self.latest["OrderKey"] = ("AVRO", KEY_SCHEMA_STR, KEY_SCHEMA_ID)
            self.queries: list[str] = []

        def get_latest_schema(self, subject):
            self.queries.append(subject)
            return self.latest[subject]

    sr = _RecordSubjectSR()
    codec = {
        "value": {"fmt": Format.AVRO, "subject_strategy": "record"},
        "key": {"fmt": Format.AVRO, "subject_strategy": "record"},
        "sr": sr,
    }

    # Object-typed capture injects the dict KEY (carrying its own record
    # name) into the reaction; the rendered VALUE has no __record_name__,
    # so a value-derived resolution would fall back to "events-key".
    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        capture={
            "keyPayload": CaptureSpec(from_=".value.keyPayload", type="object"),
        },
        reaction=KafkaReaction(
            topic="events",
            key="{keyPayload}",  # whole-field object substitution -> dict
            value={"id": "{orderId}", "qty": 1},
        ),
    )
    client = FakeKafkaClient(
        script=[
            _GoodMsg(
                _msg(
                    {
                        "orderId": "ord-1",
                        "command": "CREATE_ORDER",
                        "keyPayload": {
                            "orderId": "ord-1",
                            "__record_name__": "OrderKey",
                        },
                    },
                    key="ord-1",
                )
            ),
        ]
    )
    reactor = Reactor(
        name="record-key-reactor",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
        reaction_codec=codec,
    )

    reactor.run()

    # The key encode queried the SR at the KEY's record-name subject.
    assert "OrderKey" in sr.queries
    # The value (no __record_name__) fell back to the topic-name subject.
    assert "events-value" in sr.queries
    # The bug would have queried "events-key" (value has no record name so
    # resolve_subject("record") falls back to topic-name for the key) — the
    # SR does not have that subject, so the encode would have raised.
    assert "events-key" not in sr.queries

    # One produce call with both sides encoded as Confluent-framed bytes.
    assert len(client.produce_calls) == 1
    prod = client.produce_calls[0]
    assert prod["topic"] == "events"
    assert prod["_raw"] is True

    # The key bytes carry the KEY schema id (encoded against the OrderKey
    # subject's schema, not the value schema).
    key_bytes = prod["key"]
    assert isinstance(key_bytes, (bytes, bytearray))
    assert key_bytes[0] == 0
    key_schema_id = struct.unpack(">I", bytes(key_bytes[1:5]))[0]
    assert key_schema_id == KEY_SCHEMA_ID

    # The value bytes carry the VALUE schema id (events-value -> E schema).
    value_bytes = prod["value"]
    assert value_bytes[0] == 0
    value_schema_id = struct.unpack(">I", bytes(value_bytes[1:5]))[0]
    assert value_schema_id == SCHEMA_ID

    # Round-trip the key bytes to confirm the encoded datum is the rendered
    # key record (schema_id in the wire frame selects the key schema).
    from agctl.serialization import decode_payload

    decoded_key = decode_payload(bytes(key_bytes), Format.AVRO, sr)
    assert decoded_key == {"orderId": "ord-1", "__record_name__": "OrderKey"}

    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1
    assert reacted[0]["reactor"] == "record-key-reactor"
    assert not any(e["event"] == "kafka.error" for e in emit_event.events)


# ===========================================================================
# Protobuf coverage (Task 14): a JSON trigger -> Protobuf-encoded reaction.
# Verifies the reactor's ``_encode_reaction`` path (which delegates to the
# shared ``KafkaClient._encode_payload_with_codec`` helper) is format-agnostic
# — it must work identically for Protobuf without any Avro-specific branch.
# ===========================================================================


def test_json_trigger_protobuf_reaction_publishes_confluent_framed_bytes(
    emit_event, stop_event
):
    """JSON trigger -> Protobuf-encoded reaction published as Confluent-framed
    bytes. The reactor encodes via ``encode_payload`` against the reaction
    codec's Protobuf SR; the produced bytes carry magic 0x00, the registered
    schema id, and a payload that round-trips through ``decode_payload`` to
    the rendered record."""
    pytest.importorskip("google.protobuf")
    pytest.importorskip("grpc_tools")
    from agctl.serialization import Format

    # Single-message Protobuf schema with two fields so the rendered reaction
    # datum exercises more than one field.
    proto_schema = (
        'syntax = "proto3";'
        " message Reaction { string id = 1; int32 qty = 2; }"
    )
    proto_schema_id = 31

    class _ProtoSR:
        """Fake SR returning Protobuf schemas for the reaction subject."""

        def __init__(self):
            self.by_id = {proto_schema_id: ("PROTOBUF", proto_schema)}
            self.latest = {"events-value": ("PROTOBUF", proto_schema, proto_schema_id)}
            self.get_latest_schema_calls = 0

        def get_schema(self, schema_id):
            return self.by_id[schema_id]

        def get_latest_schema(self, subject):
            self.get_latest_schema_calls += 1
            return self.latest[subject]

    config = KafkaReactor(
        topic="commands",
        match='.value.command == "CREATE_ORDER"',
        reaction=KafkaReaction(
            topic="events",
            key="{orderId}",
            value={"id": "{orderId}", "qty": 1},
        ),
    )
    sr = _ProtoSR()
    codec = {
        "value": {"fmt": Format.PROTOBUF, "subject_strategy": "topic"},
        "key": {"fmt": Format.KEY_STRING, "subject_strategy": "topic"},
        "sr": sr,
    }
    client = FakeKafkaClient(
        script=[
            _GoodMsg(
                _msg(
                    {"orderId": "ord-1", "command": "CREATE_ORDER"},
                    key="ord-1",
                )
            ),
        ]
    )
    reactor = Reactor(
        name="pb-reactor",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-pb",
        reaction_codec=codec,
    )

    reactor.run()

    # Exactly one produce, raw bytes (encode happened in the reactor).
    assert len(client.produce_calls) == 1
    prod = client.produce_calls[0]
    assert prod["topic"] == "events"
    assert prod["_raw"] is True

    value_bytes = prod["value"]
    assert isinstance(value_bytes, (bytes, bytearray))
    # Confluent wire frame: magic 0x00 + 4-byte big-endian schema id + payload.
    assert value_bytes[0] == 0
    schema_id = struct.unpack(">I", bytes(value_bytes[1:5]))[0]
    assert schema_id == proto_schema_id

    # KEY_STRING key encoded as utf-8 bytes.
    assert prod["key"] == b"ord-1"

    # Encode resolved the latest schema exactly once for "events-value".
    assert sr.get_latest_schema_calls == 1

    # Round-trip: decode the framed bytes back to the rendered record.
    from agctl.serialization import decode_payload

    assert decode_payload(bytes(value_bytes), codec["value"]["fmt"], sr) == {
        "id": "ord-1",
        "qty": 1,
    }

    reacted = [e for e in emit_event.events if e["event"] == "kafka.reacted"]
    assert len(reacted) == 1
    assert reacted[0]["reactor"] == "pb-reactor"

