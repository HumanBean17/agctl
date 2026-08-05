"""Unit tests for kafka produce/assert inline template-variable generators (Task 6).

Threads the per-invocation ``memo`` + ``template_vars_enabled`` flag through the
kafka commands so ``{{uuid}}``/``{{ts}}``/``{{rand}}`` tokens in ``--key``,
``--message``, and ``--header`` resolve to generated values, with the SAME token
resolving to the SAME value within one invocation (shared memo). ``--no-template-vars``
leaves tokens literal; an unknown generator surfaces as ``ConfigError`` (exit 2)
before any produce call.

The fake producer (the ARCH §8 KafkaClient test seam) records every published
``(topic, key, value, headers)`` so tests can assert on the post-substitution
bytes without a real broker.
"""

from __future__ import annotations

import json
import re
import time
import uuid as uuidlib
from pathlib import Path

import pytest
from click.testing import CliRunner
from confluent_kafka import TopicPartition

from agctl.cli import cli
from agctl.clients.kafka_client import KafkaClient
from agctl.commands import kafka_commands

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "SCHEMA_REGISTRY_URL": "",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "p",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
}


# ---------------------------------------------------------------------------
# Fake seams (minimal copies of the ones in test_kafka_commands.py — a recording
# producer is all the produce tests need; an empty consumer stands in for the
# no-consume produce path).
# ---------------------------------------------------------------------------


class _FakeMsg:
    """Mimics confluent_kafka.Message for the produce delivery report."""

    def __init__(self, p, o, ts):
        self._p, self._o, self._ts = p, o, ts

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def timestamp(self):
        return (1, self._ts)


class RecordingProducer:
    """Records every ``produce()`` call and immediately invokes the delivery callback."""

    def __init__(self, conf):
        self.conf = conf
        self.calls: list[dict] = []

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        if on_delivery is not None:
            on_delivery(None, _FakeMsg(0, 100, 1719660000000))

    def flush(self, timeout):
        return 0


class _EmptyConsumer:
    """Minimal consumer double yielding no messages (produce tests don't consume)."""

    def __init__(self, conf):
        self.conf = conf

    def subscribe(self, topics):
        pass

    def assignment(self):
        return []

    def offsets_for_times(self, tps):
        return []

    def seek(self, tp):
        pass

    def poll(self, timeout):
        return None

    def close(self):
        pass


class _FakeCMsg:
    """Mimics a consumed confluent_kafka.Message (minimal copy of test_kafka_commands)."""

    def __init__(self, topic, partition, offset, key, value, ts_ms):
        self._topic = topic
        self._p = partition
        self._o = offset
        self._key = key
        self._value = value
        self._ts = ts_ms

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
        return None

    def error(self):
        return None


class _FakeConsumer:
    """Fake consumer modelling the D6 lookback window + seek mechanics.

    Minimal copy of the double in ``test_kafka_commands.py`` — partitions are
    only "in window" once seeked to a non-negative offset. Used by the assert
    tests so ``find_in_window`` can replay canned messages without a broker.
    """

    def __init__(self, conf, messages=None):
        self.conf = conf
        self._messages = list(messages or [])
        self._messages.sort(key=lambda m: (m.partition(), m.offset()))
        self._seek_offsets = {}
        self._cursor = 0
        self._topics = []

    def subscribe(self, topics):
        self._topics = list(topics)

    def assignment(self):
        if not self._topics:
            return []
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
        from confluent_kafka import OFFSET_BEGINNING, OFFSET_END
        import math as _math

        if tp.offset == OFFSET_BEGINNING:
            off = 0
        elif tp.offset == OFFSET_END:
            off = _math.inf
        else:
            off = tp.offset
        self._seek_offsets[(tp.topic, tp.partition)] = off

    def poll(self, timeout):
        while self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            self._cursor += 1
            key = (m.topic(), m.partition())
            seek_off = self._seek_offsets.get(key)
            if seek_off is None:
                continue
            if m.offset() >= seek_off:
                return m
        return None

    def close(self):
        pass


@pytest.fixture
def install_recording_producer(monkeypatch):
    """Install a fake client whose producer records every ``produce()`` call.

    Returns the ``cap`` dict; ``cap["producer"]`` is the recording double whose
    ``.calls`` list holds the published ``{topic, value, key, headers}``.
    """

    cap: dict = {}

    def _install():
        producer = RecordingProducer({})
        consumer = _EmptyConsumer({})

        def consumer_factory(conf):
            consumer.conf = conf
            return consumer

        def producer_factory(conf):
            producer.conf = conf
            return producer

        client = KafkaClient(
            ["host:9092"],
            consumer_factory=consumer_factory,
            producer_factory=producer_factory,
        )
        cap["producer"] = producer

        def factory(cluster, group_id=None, codec=None):
            return client

        monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)
        return cap

    return _install


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


@pytest.fixture
def install_fake_consumer(monkeypatch):
    """Install a fake-backed client whose consumer replays canned messages.

    Mirrors ``install_fake`` in test_kafka_commands.py. Returns the ``cap`` dict
    with ``cap["consumer"]`` / ``cap["producer"]`` so assert tests can verify the
    predicate/consume path. ``new_kafka_client`` is monkeypatched to return the
    wired client regardless of cluster (group_id/codec accepted and ignored).
    """

    cap: dict = {}

    def _install(messages):
        consumer = _FakeConsumer({}, messages=messages)
        producer = RecordingProducer({})

        def consumer_factory(conf):
            consumer.conf = conf
            return consumer

        def producer_factory(conf):
            producer.conf = conf
            return producer

        client = KafkaClient(
            ["host:9092"],
            consumer_factory=consumer_factory,
            producer_factory=producer_factory,
        )
        cap["consumer"] = consumer
        cap["producer"] = producer

        def factory(cluster, group_id=None, codec=None):
            return client

        monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)
        return cap

    return _install


def _cmsg(topic, value_obj, key, ms_ago=500, partition=0, offset=0):
    """Build a ``_FakeCMsg`` at (now - ms_ago) ms with a JSON value."""
    now_ms = int(time.time() * 1000)
    return _FakeCMsg(
        topic,
        partition,
        offset,
        key,
        json.dumps(value_obj).encode("utf-8"),
        now_ms - ms_ago,
    )


# ---------------------------------------------------------------------------
# kafka produce: inline generators + shared memo
# ---------------------------------------------------------------------------


def test_produce_key_and_message_share_uuid(install_recording_producer):
    """``--key {{uuid}}`` and ``--message '{"cid":"{{uuid}}"}'`` resolve to the
    SAME uuid within one invocation (shared memo), and that value is a valid uuid."""
    cap = install_recording_producer()
    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "produce",
            "--topic", "t",
            "--key", "{{uuid}}",
            "--message", '{"cid":"{{uuid}}"}',
        ]
    )
    assert result.exit_code == 0, result.output

    call = cap["producer"].calls[0]
    key = call["key"].decode("utf-8")
    body = json.loads(call["value"].decode("utf-8"))
    cid = body["cid"]

    # Same memo -> identical value for the repeated {{uuid}} token.
    assert key == cid
    # Both are valid RFC-4122 UUIDs.
    uuidlib.UUID(key)
    uuidlib.UUID(cid)


def test_produce_key_header_and_message_substituted(install_recording_producer):
    """``--key``, ``--header`` value, and ``--message`` all get generator
    substitution; ``--key {{uuid}}`` and ``--header trace={{uuid}}`` resolve to
    the SAME value (shared memo); ``--message {{ts}}`` resolves to a timestamp."""
    cap = install_recording_producer()
    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "produce",
            "--topic", "t",
            "--key", "{{uuid}}",
            "--header", "trace={{uuid}}",
            "--message", "{{ts}}",
        ]
    )
    assert result.exit_code == 0, result.output

    call = cap["producer"].calls[0]
    key = call["key"].decode("utf-8")

    # header trace == key (same {{uuid}} memo).
    headers = {k: v.decode("utf-8") for k, v in (call["headers"] or [])}
    assert headers["trace"] == key
    uuidlib.UUID(key)

    # message: '{{ts}}' substituted to a digit timestamp, JSON-encoded.
    msg = json.loads(call["value"].decode("utf-8"))
    assert re.fullmatch(r"\d+", str(msg))


def test_produce_no_template_vars_leaves_tokens_literal(install_recording_producer):
    """``--no-template-vars`` disables substitution end-to-end: the recorded key
    and value carry the LITERAL token text, and no error is raised."""
    cap = install_recording_producer()
    result = _run(
        [
            "--config", str(FIXTURE),
            "--no-template-vars",
            "kafka", "produce",
            "--topic", "t",
            "--key", "{{uuid}}",
            "--message", '{"cid":"{{uuid}}"}',
        ]
    )
    assert result.exit_code == 0, result.output

    call = cap["producer"].calls[0]
    assert call["key"].decode("utf-8") == "{{uuid}}"
    body = json.loads(call["value"].decode("utf-8"))
    assert body["cid"] == "{{uuid}}"


def test_produce_unknown_generator_is_config_error_before_produce(
    install_recording_producer,
):
    """``--message '{{nope}}'`` (unknown generator) surfaces as ``ConfigError``
    (exit 2) BEFORE the producer is called — the recording producer records
    nothing."""
    cap = install_recording_producer()
    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "produce",
            "--topic", "t",
            "--message", "{{nope}}",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # The producer double was never called.
    assert cap["producer"].calls == []


# ---------------------------------------------------------------------------
# kafka assert: --pattern generator substitution + memo (Task 6, Finding 2)
# ---------------------------------------------------------------------------


def test_assert_pattern_substitutes_generators_and_shares_memo(
    install_fake_consumer, tmp_path
):
    """A ``--pattern`` whose match expression references ``{{uuid}}`` has the
    token substituted with a generated value, and the SAME token used twice in
    the expr resolves to ONE value (shared per-invocation memo).

    Verified via the no-match failure detail, which echoes the FILLED match
    expression (``modes[].expr``) — so we can see exactly what the predicate
    evaluated without having to predict the generated uuid upstream.
    """
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers: [host:9092]\n"
        "  default_cluster: main\n"
        "  patterns:\n"
        "    gen:\n"
        "      topic: t\n"
        '      match: \'.value.trace == "{{uuid}}" and .key == "{{uuid}}"\'\n'
    )
    # Canned message that will NOT match the generated uuid -> no-match echoes
    # the filled expr.
    install_fake_consumer([_cmsg("t", {"trace": "no-match"}, "k1")])

    result = _run(
        [
            "--config", str(cfg),
            "kafka", "assert",
            "--pattern", "gen",
            "--lookback", "10",
            "--timeout", "0.02",
        ]
    )
    payload = json.loads(result.output)

    # No match (the canned message's trace/key are not the generated uuid).
    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"
    modes = payload["error"]["detail"]["modes"]
    assert len(modes) == 1 and modes[0]["mode"] == "pattern"
    expr = modes[0]["expr"]

    # The literal token must be gone, and BOTH positions hold the SAME uuid.
    assert "{{uuid}}" not in expr
    uuids = re.findall(r'"([0-9a-fA-F-]{36})"', expr)
    assert len(uuids) == 2
    assert uuids[0] == uuids[1]
    uuidlib.UUID(uuids[0])


def test_assert_pattern_unknown_generator_is_config_error_before_sr_probe(
    monkeypatch, install_fake_consumer, tmp_path
):
    """An unknown generator (``{{nope}}``) inside a ``--pattern`` against an
    SR-backed (avro) cluster surfaces as ``ConfigError`` (exit 2) BEFORE the
    Schema Registry reachability probe fires — i.e. generator substitution runs
    ahead of ``_resolve_codec``/``probe_schema_registry``. The SR client /
    consumer seam must record nothing.

    This is the ordering contract of Finding 1: without the hoist, the probe
    fires (recorded) before the ConfigError.
    """
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    sr:\n"
        "      brokers: [host:9092]\n"
        "      schema_registry_url: http://sr:8081\n"
        "      value_format: avro\n"
        "  default_cluster: sr\n"
        "  patterns:\n"
        "    bad:\n"
        "      topic: t\n"
        '      match: \'.value.x == "{{nope}}"\'\n'
    )

    # SR-backed avro path: _resolve_codec would build an SR client and probe.
    # Stub resolve_schema_registry_client (return a sentinel so the resolver
    # proceeds past the "no SR URL" guard) and RECORD probe_schema_registry so
    # the test can prove the probe never ran.
    probe_calls: list[str] = []
    monkeypatch.setattr(
        kafka_commands,
        "resolve_schema_registry_client",
        lambda cfg_, name: object(),  # non-None sentinel -> resolver proceeds
    )

    def _record_probe(sr, cluster):
        probe_calls.append(cluster)
        return None

    monkeypatch.setattr(kafka_commands, "probe_schema_registry", _record_probe)

    cap = install_fake_consumer([])

    result = _run(
        [
            "--config", str(cfg),
            "kafka", "assert",
            "--pattern", "bad",
            "--lookback", "10",
            "--timeout", "0.02",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # The SR probe MUST NOT have fired (generator substitution runs first).
    assert probe_calls == []
    # And the consume path was never reached either.
    assert cap["producer"].calls == []


# ---------------------------------------------------------------------------
# kafka assert: explicit --match/--path/--contains honor {{...}} generators
# (review fix — uniform with --pattern; fail-loud on unknown generators).
# ---------------------------------------------------------------------------


def test_assert_match_substitutes_uuid_and_shares_memo_with_contains(
    install_fake_consumer,
):
    """Explicit ``--match '.value.id == "{{uuid}}"'`` and ``--contains
    '{"id":"{{uuid}}"}'`` have their ``{{uuid}}`` tokens substituted BEFORE
    compile_jq / json.loads, and BOTH resolve to the SAME value (shared
    per-invocation memo — uniform with ``--pattern``).

    Verified via the no-match failure detail, which echoes the FILLED match
    expr (``modes[].expr``) and the parsed contains ``needle`` — so the test
    sees exactly what the predicate evaluated without predicting the uuid.
    """
    # Canned message that will NOT match the generated uuid -> no-match echoes
    # the filled expr + needle.
    install_fake_consumer([_cmsg("t", {"id": "no-match"}, "k1")])

    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--match", '.value.id == "{{uuid}}"',
            "--contains", '{"id": "{{uuid}}"}',
            "--lookback", "10",
            "--timeout", "0.02",
        ]
    )
    payload = json.loads(result.output)

    # No match (the canned id is not the generated uuid).
    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"
    modes = payload["error"]["detail"]["modes"]
    by_mode = {m["mode"]: m for m in modes}

    # --match expr: literal token gone, holds ONE valid uuid.
    match_expr = by_mode["match"]["expr"]
    assert "{{uuid}}" not in match_expr
    match_uuids = re.findall(r'"([0-9a-fA-F-]{36})"', match_expr)
    assert len(match_uuids) == 1
    uuidlib.UUID(match_uuids[0])

    # --contains needle: literal token gone, holds ONE valid uuid.
    needle = by_mode["contains"]["needle"]
    assert "{{uuid}}" not in json.dumps(needle)
    contains_uuid = needle["id"]
    uuidlib.UUID(contains_uuid)

    # Shared memo: both positions hold the SAME uuid.
    assert match_uuids[0] == contains_uuid


def test_assert_match_unknown_generator_is_config_error_before_consume(
    install_fake_consumer,
):
    """``--match '.value.id == "{{nope}}"'`` (unknown generator) surfaces as
    ``ConfigError`` (exit 2) BEFORE any consume — ``substitute_generators`` runs
    ahead of compile_jq / ``_resolve_codec`` / ``find_in_window``. The consumer
    seam records nothing (no seek offsets set), restoring the fail-loud promise
    that a typo'd generator can't silently match nothing.
    """
    cap = install_fake_consumer([_cmsg("t", {"id": "x"}, "k1")])

    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--match", '.value.id == "{{nope}}"',
            "--lookback", "10",
            "--timeout", "0.02",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # The consume path was never entered: the consumer double recorded no seeks.
    assert cap["consumer"]._seek_offsets == {}


def test_assert_match_substitutes_timestamp(install_fake_consumer):
    """``--match '{{ts}}'`` has the token substituted with a Unix-second timestamp
    BEFORE compile_jq — so the jq expression is a valid number literal rather than
    the invalid-jq ``{{ts}}`` (which ``compile_jq`` rejects as a syntax error).

    Verified via the no-match failure detail's ``expr`` against an empty window.
    """
    install_fake_consumer([])  # empty window -> no match -> expr echoed

    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--match", "{{ts}}",
            "--lookback", "10",
            "--timeout", "0.02",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"
    modes = payload["error"]["detail"]["modes"]
    assert len(modes) == 1 and modes[0]["mode"] == "match"
    expr = modes[0]["expr"]
    # Literal token gone, replaced by a Unix-second timestamp (digits only).
    assert "{{ts}}" not in expr
    assert re.fullmatch(r"\d+", expr)

