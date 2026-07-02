"""Unit tests for `kafka produce/consume/assert` commands (DESIGN §3.2, D6, D10).

The lookback-window mechanics (D6) and early-stop semantics are exercised via
FakeConsumer/FakeProducer doubles mirroring the real confluent_kafka contract.
``kafka_commands.new_kafka_client`` is monkeypatched to return a KafkaClient
built with these fakes, so no broker is required.
"""

import json
import math
import time
from pathlib import Path

import pytest
from click.testing import CliRunner
from confluent_kafka import OFFSET_END, TopicPartition

from agctl.cli import cli
from agctl.clients.kafka_client import KafkaClient
from agctl.config.models import KafkaConfig, KafkaSSL
from agctl.assertion_registry import Assertion
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
# _kafka_ssl_conf — typed kafka.ssl -> librdkafka conf dict translation
# ---------------------------------------------------------------------------


def _kcfg(**ssl_kwargs):
    return KafkaConfig(ssl=KafkaSSL(**ssl_kwargs)) if ssl_kwargs else KafkaConfig()


def test_ssl_conf_none_when_no_ssl():
    assert kafka_commands._kafka_ssl_conf(KafkaConfig()) == {}
    # An empty ssl block also yields nothing (no knobs -> no protocol inferred).
    assert kafka_commands._kafka_ssl_conf(_kcfg()) == {}


def test_ssl_conf_full_emits_all_keys_with_inferred_protocol():
    conf = kafka_commands._kafka_ssl_conf(
        _kcfg(
            ca_location="/ca.pem",
            certificate_location="/client.crt",
            key_location="/client.key",
            key_password="secret",
            endpoint_identification_algorithm="none",
        )
    )
    assert conf == {
        "ssl.ca.location": "/ca.pem",
        "ssl.certificate.location": "/client.crt",
        "ssl.key.location": "/client.key",
        "ssl.key.password": "secret",
        "ssl.endpoint.identification.algorithm": "none",
        # security.protocol inferred to SSL when any knob is set and not given.
        "security.protocol": "SSL",
    }


def test_ssl_conf_explicit_security_protocol_honored():
    conf = kafka_commands._kafka_ssl_conf(
        _kcfg(ca_location="/ca.pem", security_protocol="SASL_SSL")
    )
    assert conf["security.protocol"] == "SASL_SSL"


def test_ssl_conf_partial_emits_only_set_keys():
    conf = kafka_commands._kafka_ssl_conf(_kcfg(ca_location="/ca.pem"))
    # Only the CA knob plus the inferred protocol.
    assert conf == {"ssl.ca.location": "/ca.pem", "security.protocol": "SSL"}


def test_ssl_conf_skips_empty_string_values():
    """Empty strings (from unresolved ${VAR:-}) count as unset: they must not
    emit bogus ssl.*.location paths nor disable hostname verification via an
    empty endpoint_identification_algorithm (librdkafka treats "" == "none")."""
    conf = kafka_commands._kafka_ssl_conf(
        _kcfg(
            ca_location="/ca.pem",
            certificate_location="",
            key_location="",
            key_password="",
            endpoint_identification_algorithm="",
        )
    )
    assert conf == {"ssl.ca.location": "/ca.pem", "security.protocol": "SSL"}
    assert "ssl.endpoint.identification.algorithm" not in conf


def test_ssl_conf_all_empty_strings_is_noop():
    """A fully-unresolved ssl block (every field "") enables no TLS at all —
    no empty paths, no inferred security.protocol."""
    conf = kafka_commands._kafka_ssl_conf(
        _kcfg(
            ca_location="",
            certificate_location="",
            key_location="",
            key_password="",
            endpoint_identification_algorithm="",
        )
    )
    assert conf == {}


# ---------------------------------------------------------------------------
# Fake seams (mirror the real confluent_kafka contract; minimal copies of the
# ones in test_kafka_client.py).
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
        return (1, self._ts)


class FakeProducer:
    """Records produce calls and immediately invokes the delivery callback."""

    def __init__(self, conf):
        self.conf = conf
        self.calls = []
        self._p = 0
        self._o = 100
        self._ts = 1719660000000

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        if on_delivery is not None:
            on_delivery(None, FakeMsg(self._p, self._o, self._ts))
        self._o += 1

    def flush(self, timeout):
        return 0


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
    """Fake consumer modelling the D6 lookback window + seek mechanics.

    A partition is only "in window" once it has been seeked to a non-negative
    offset. ``offsets_for_times`` returns ``-1`` when no canned message falls
    inside the requested window; the real client then seeks that partition to
    ``OFFSET_END`` so it contributes nothing stale, and this fake mirrors that
    (an un-seeked partition likewise yields nothing).
    """

    def __init__(self, conf, messages=None):
        self.conf = conf
        self._messages = list(messages or [])
        self._messages.sort(key=lambda m: (m.partition(), m.offset()))
        self._seek_offsets = {}
        self._from_beginning = set()  # partitions seeked via seek_to_beginning
        self._cursor = 0
        self._topics = []
        self.closed = False

    def subscribe(self, topics):
        self._topics = list(topics)

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
        # OFFSET_END means "nothing at/after here" — model it as +inf so poll's
        # `offset >= seek_off` test yields nothing for that partition.
        off = math.inf if tp.offset == OFFSET_END else tp.offset
        self._seek_offsets[(tp.topic, tp.partition)] = off

    def seek_to_beginning(self, *tps):
        for tp in tps:
            self._seek_offsets[(tp.topic, tp.partition)] = 0
            self._from_beginning.add((tp.topic, tp.partition))

    def poll(self, timeout):
        while self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            self._cursor += 1
            key = (m.topic(), m.partition())
            if key in self._seek_offsets:
                seek_off = self._seek_offsets[key]
            elif key in self._from_beginning:
                seek_off = 0
            else:
                # Un-seeked partition (e.g. offsets_for_times returned -1): the
                # real consumer would have nothing in-window for it.
                continue
            if m.offset() >= seek_off:
                return m
        return None

    def close(self):
        self.closed = True


def _build_fake_client(messages, producer=None):
    """Build a KafkaClient backed by a FakeConsumer + (optional) FakeProducer.

    The consumer factory captures the canned messages; the producer factory
    captures the producer double so tests can inspect produce calls.
    """
    consumer = FakeConsumer({}, messages=messages)
    if producer is None:
        producer = FakeProducer({})

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
    return client, producer, consumer


@pytest.fixture
def install_fake(monkeypatch):
    """Return a factory: install_fake(messages=...) wires a fake-backed client."""

    captured = {}

    def _install(messages, producer=None):
        client, producer, consumer = _build_fake_client(messages, producer=producer)
        captured["client"] = client
        captured["producer"] = producer
        captured["consumer"] = consumer

        def factory(cfg_kafka, group_id=None):
            # Preserve the test's canned messages; honor a caller group_id by
            # simply returning the same client (group has no effect on fakes).
            return client

        monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)
        return captured

    return _install


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


def _payload(result):
    return json.loads(result.output)


# --------------------------------------------------------------------------- #
# kafka produce
# --------------------------------------------------------------------------- #


def test_kafka_produce_returns_design_shape(install_fake):
    cap = install_fake([])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "produce",
            "--topic",
            "t",
            "--message",
            '{"a":1}',
            "--key",
            "k",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "kafka.produce"
    assert payload["ok"] is True
    res = payload["result"]
    assert res["topic"] == "t"
    assert res["partition"] == 0
    assert res["offset"] == 100
    assert res["key"] == "k"
    # Fake producer received JSON-encoded value bytes.
    fake = cap["producer"]
    assert fake.calls[0]["value"] == b'{"a": 1}'
    assert fake.calls[0]["key"] == b"k"


def test_kafka_produce_with_headers(install_fake):
    cap = install_fake([])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "produce",
            "--topic",
            "t",
            "--message",
            '{"a":1}',
            "--header",
            "h1=v1",
            "--header",
            "h2=v2",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    fake = cap["producer"]
    headers = fake.calls[0]["headers"]
    assert ("h1", b"v1") in headers
    assert ("h2", b"v2") in headers


# --------------------------------------------------------------------------- #
# kafka consume
# --------------------------------------------------------------------------- #


def _msg(topic, value_obj, key, ms_ago=1000, partition=0, offset=0):
    """Build a FakeCMsg at (now - ms_ago) ms with a JSON value."""
    now_ms = int(time.time() * 1000)
    return FakeCMsg(
        topic,
        partition,
        offset,
        key,
        json.dumps(value_obj).encode("utf-8"),
        now_ms - ms_ago,
    )


def test_kafka_consume_expect_count_passes(install_fake):
    install_fake([_msg("t", {"a": 1}, "k1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "consume",
            "--topic",
            "t",
            "--timeout",
            "0.02",
            "--lookback",
            "10",
            "--expect-count",
            "1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "kafka.consume"
    assert payload["ok"] is True
    assert payload["result"]["count"] == 1
    assert payload["result"]["topic"] == "t"
    assert len(payload["result"]["messages"]) == 1


def test_kafka_consume_expect_count_miss_is_assertion_error(install_fake):
    # No messages -> expect-count 1 cannot be satisfied.
    install_fake([])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "consume",
            "--topic",
            "t",
            "--timeout",
            "0.02",
            "--lookback",
            "10",
            "--expect-count",
            "1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"
    assert payload["error"]["detail"]["expected"] == 1
    assert payload["error"]["detail"]["actual"] == 0


def test_kafka_consume_match_filters_messages(install_fake):
    install_fake(
        [
            _msg("t", {"x": 1}, "a", offset=0),
            _msg("t", {"x": 2}, "b", offset=1),
            _msg("t", {"x": 1}, "c", offset=2, partition=1),
        ]
    )
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "consume",
            "--topic",
            "t",
            "--timeout",
            "0.02",
            "--lookback",
            "10",
            "--match",
            ".x==1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    msgs = payload["result"]["messages"]
    assert payload["result"]["count"] == 2
    assert {m["key"] for m in msgs} == {"a", "c"}


def test_kafka_consume_filter_key_is_alias_of_match(install_fake):
    install_fake(
        [
            _msg("t", {"x": 1}, "a", offset=0),
            _msg("t", {"x": 2}, "b", offset=1),
        ]
    )
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "consume",
            "--topic",
            "t",
            "--timeout",
            "0.02",
            "--lookback",
            "10",
            "--filter-key",
            ".x==1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["count"] == 1
    assert payload["result"]["messages"][0]["key"] == "a"


def test_kafka_consume_match_and_filter_key_both_given_errors(install_fake):
    install_fake([])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "consume",
            "--topic",
            "t",
            "--timeout",
            "0.02",
            "--lookback",
            "10",
            "--match",
            ".x==1",
            "--filter-key",
            ".x==2",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"


def test_kafka_consume_expect_count_stops_early(install_fake):
    """DESIGN §3.2 'whichever comes first': consume returns as soon as
    --expect-count is satisfied, NOT after the full --timeout. A message is
    available immediately (FakeConsumer), expect-count 1, timeout 2s -> the
    command must return far sooner than 2s."""
    install_fake([_msg("t", {"a": 1}, "k1")])

    t0 = time.monotonic()
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "consume",
            "--topic",
            "t",
            "--timeout",
            "2",
            "--lookback",
            "10",
            "--expect-count",
            "1",
        ]
    )
    elapsed = time.monotonic() - t0
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["count"] == 1
    assert elapsed < 1.0  # did NOT wait the full 2s window


# --------------------------------------------------------------------------- #
# kafka assert
# --------------------------------------------------------------------------- #


def test_kafka_assert_contains_matches(install_fake):
    install_fake([_msg("t", {"a": 1, "b": 2}, "k1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--contains",
            '{"a":1}',
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "kafka.assert"
    assert payload["ok"] is True
    assert payload["result"]["matched"] is True
    assert payload["result"]["matching_message"]["key"] == "k1"
    assert payload["result"]["messages_scanned"] >= 1


def test_kafka_assert_no_match_is_assertion_error(install_fake):
    install_fake([_msg("t", {"a": 999}, "k1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--contains",
            '{"a":1}',
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"
    assert payload["error"]["detail"]["topic"] == "t"


def test_kafka_assert_pattern_fills_placeholder_and_infers_topic(install_fake):
    # Pattern order-created: match on eventType==ORDER_CREATED and
    # .payload.orderId=="{orderId}" -> fill orderId=o9; topic orders.created.
    install_fake(
        [
            _msg(
                "orders.created",
                {"eventType": "ORDER_CREATED", "payload": {"orderId": "o9"}},
                "k1",
            )
        ]
    )
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--pattern",
            "order-created",
            "--param",
            "orderId=o9",
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["matched"] is True
    # Topic inferred from the pattern.
    assert payload["result"]["topic"] == "orders.created"


def test_kafka_assert_pattern_missing_raises_template_missing():
    # No fake install: pattern lookup fails before the client is built.
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--pattern",
            "does-not-exist",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "TemplateNotFound"
    assert payload["error"]["detail"]["path"] == "kafka.patterns.does-not-exist"


def test_kafka_assert_match_predicate_errors_skips_message(install_fake):
    # A bad jq expr must skip the message (not crash); only-bad-expr msgs ->
    # no match -> AssertionError.
    install_fake([_msg("t", {"a": 1}, "k1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--match",
            ".a == ",  # malformed jq
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"


def test_kafka_assert_match_filters_with_valid_expr(install_fake):
    install_fake([_msg("t", {"status": "OK"}, "k1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--match",
            '.status=="OK"',
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["matched"] is True


# --------------------------------------------------------------------------- #
# D6: lookback window excludes old messages for assert too
# --------------------------------------------------------------------------- #


def test_kafka_assert_lookback_excludes_old_messages(install_fake):
    topic = "t"
    now_ms = int(time.time() * 1000)
    # Old message (60s ago) WOULD match the contains, but is outside a small
    # lookback window so offsets_for_times seek should skip it.
    old = FakeCMsg(
        topic,
        0,
        0,
        "old",
        json.dumps({"a": 1}).encode("utf-8"),
        now_ms - 60_000,
    )
    install_fake([old])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            topic,
            "--contains",
            '{"a":1}',
            "--lookback",
            "2",  # only the last 2s
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"


def test_kafka_assert_in_window_message_matches(install_fake):
    topic = "t"
    # Recent message well inside the default lookback.
    install_fake([_msg(topic, {"a": 1}, "fresh", ms_ago=500)])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            topic,
            "--contains",
            '{"a":1}',
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["matched"] is True
    assert payload["result"]["matching_message"]["key"] == "fresh"


# --------------------------------------------------------------------------- #
# find_in_window early-stop: first matching message returns promptly
# (implicit: a tiny timeout would be hit if early-stop didn't work, but the
# match succeeds before the window elapses).
# --------------------------------------------------------------------------- #


def test_kafka_assert_early_stops_on_first_match(install_fake):
    install_fake(
        [
            _msg("t", {"a": 1}, "first", offset=0),
            _msg("t", {"a": 1}, "second", offset=1),
        ]
    )
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--contains",
            '{"a":1}',
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["matched"] is True
    # Only one message was scanned (early stop after the first match).
    assert payload["result"]["messages_scanned"] == 1
    assert payload["result"]["matching_message"]["key"] == "first"


# --------------------------------------------------------------------------- #
# DESIGN §9.3: `kafka assert --assertion <name>` dispatches to a registered mode
# --------------------------------------------------------------------------- #


def _install_custom_registry(monkeypatch, mode_cls):
    import agctl.assertion_registry as ar
    from agctl.assertion_registry import AssertionRegistry

    reg = AssertionRegistry()
    reg.register(mode_cls)
    monkeypatch.setattr(ar, "get_default_registry", lambda: reg)


def test_kafka_assert_custom_mode_passes(monkeypatch, install_fake):
    class _AtLeastTwo(Assertion):
        name = "at_least_two"

        def evaluate(self, context):
            return {"passed": context["count"] >= 2, "count": context["count"]}

    _install_custom_registry(monkeypatch, _AtLeastTwo)
    install_fake([_msg("t", {"i": 1}, "a"), _msg("t", {"i": 2}, "b")])

    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--assertion", "at_least_two",
            "--timeout", "0.02",
            "--lookback", "10",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["assertion_type"] == "at_least_two"
    assert payload["result"]["count"] == 2


def test_kafka_assert_custom_mode_fails_exit1(monkeypatch, install_fake):
    class _NeedsThree(Assertion):
        name = "needs_three"

        def evaluate(self, context):
            return {"passed": context["count"] >= 3}

    _install_custom_registry(monkeypatch, _NeedsThree)
    install_fake([_msg("t", {"i": 1}, "a")])

    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--assertion", "needs_three",
            "--timeout", "0.02",
            "--lookback", "10",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"


def test_kafka_assert_custom_mode_mutually_exclusive_with_match(install_fake):
    install_fake([_msg("t", {"a": 1}, "k1")])
    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--match", ".a==1",
            "--assertion", "x",
            "--timeout", "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
