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
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, TopicPartition

from agctl.cli import cli
from agctl.clients.kafka_client import KafkaClient
from agctl.config.models import KafkaCluster, KafkaConfig, KafkaSSL
from agctl.assertion_registry import Assertion
from agctl.commands import kafka_commands
from agctl.errors import ConfigError

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


def _cluster(**ssl_kwargs):
    """Build a KafkaCluster carrying the given ssl knobs (or none)."""
    return KafkaCluster(ssl=KafkaSSL(**ssl_kwargs)) if ssl_kwargs else KafkaCluster()


def test_ssl_conf_none_when_no_ssl():
    assert kafka_commands._kafka_ssl_conf(KafkaCluster()) == {}
    # An empty ssl block also yields nothing (no knobs -> no protocol inferred).
    assert kafka_commands._kafka_ssl_conf(_cluster()) == {}


def test_ssl_conf_full_emits_all_keys_with_inferred_protocol():
    conf = kafka_commands._kafka_ssl_conf(
        _cluster(
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
        _cluster(ca_location="/ca.pem", security_protocol="SASL_SSL")
    )
    assert conf["security.protocol"] == "SASL_SSL"


def test_ssl_conf_partial_emits_only_set_keys():
    conf = kafka_commands._kafka_ssl_conf(_cluster(ca_location="/ca.pem"))
    # Only the CA knob plus the inferred protocol.
    assert conf == {"ssl.ca.location": "/ca.pem", "security.protocol": "SSL"}


def test_ssl_conf_skips_empty_string_values():
    """Empty strings (from unresolved ${VAR:-}) count as unset: they must not
    emit bogus ssl.*.location paths nor disable hostname verification via an
    empty endpoint_identification_algorithm (librdkafka treats "" == "none")."""
    conf = kafka_commands._kafka_ssl_conf(
        _cluster(
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
        _cluster(
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

    def __init__(self, conf, messages=None, empty_assignment=False):
        self.conf = conf
        self._messages = list(messages or [])
        self._messages.sort(key=lambda m: (m.partition(), m.offset()))
        self._seek_offsets = {}
        self._cursor = 0
        self._topics = []
        self.closed = False
        # When True, assignment() returns [] — models a non-existent topic or an
        # unreachable broker.
        self.empty_assignment = empty_assignment

    def subscribe(self, topics):
        self._topics = list(topics)

    def assignment(self):
        if self.empty_assignment:
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
        # Model librdkafka logical offsets the real Consumer resolves:
        #   OFFSET_BEGINNING -> partition start (0 here; fake has no retention)
        #   OFFSET_END       -> +inf so poll's `offset >= seek_off` yields nothing
        if tp.offset == OFFSET_BEGINNING:
            off = 0
        elif tp.offset == OFFSET_END:
            off = math.inf
        else:
            off = tp.offset
        self._seek_offsets[(tp.topic, tp.partition)] = off

    def poll(self, timeout):
        while self._cursor < len(self._messages):
            m = self._messages[self._cursor]
            self._cursor += 1
            key = (m.topic(), m.partition())
            if key in self._seek_offsets:
                seek_off = self._seek_offsets[key]
            else:
                # Un-seeked partition: the real consumer would have nothing
                # in-window for it.
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

        def factory(cluster, group_id=None):
            # Preserve the test's canned messages; honor a caller group_id by
            # simply returning the same client (group has no effect on fakes).
            # Capture the resolved cluster so multi-cluster tests can assert
            # which named cluster the flag/binding resolved to.
            captured["cluster"] = cluster
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
            ".value.x==1",
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
            ".value.x==1",
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
            ".value.x==1",
            "--filter-key",
            ".value.x==2",
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


def test_kafka_consume_invalid_match_is_config_error(install_fake):
    """Regression: a malformed --match jq expression must fail loudly (exit 2),
    not silently match nothing and report ok:0. jq_bool swallows per-message
    compile errors (returns False), so the expression is compiled once up front."""
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
            "--match",
            ".value.i ===",  # truncated/invalid jq
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "jq" in payload["error"]["message"].lower()


def test_kafka_consume_nonpositive_timeout_is_config_error(install_fake):
    """Regression: --timeout <= 0 makes the poll deadline already-passed, so the
    loop never runs and consume would return ok:0 (a silent no-op). Reject it."""
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
            "0",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "timeout" in payload["error"]["message"].lower()


def test_kafka_consume_nonexistent_topic_is_connection_error(monkeypatch):
    """Regression: a topic that yields no partition assignment after the grace
    window (non-existent topic with auto-create off, or unreachable broker) must
    surface as a ConnectionError (exit 2) — not a silent ok:0 nor a cryptic
    offsets_for_times([]) _INVALID_ARG."""
    consumer = FakeConsumer({}, messages=[], empty_assignment=True)
    client = KafkaClient(["host:9092"], consumer_factory=lambda conf: consumer)
    monkeypatch.setattr(
        kafka_commands, "new_kafka_client", lambda cluster, group_id=None: client
    )

    result = _run(
        ["--config", str(FIXTURE), "kafka", "consume", "--topic", "nope", "--timeout", "1"]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConnectionError"
    assert "nope" in payload["error"]["message"]



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
    # Self-debugging fields: echo the active modes with their jq roots so the
    # agent sees --contains roots at the message value (not the envelope).
    assert payload["error"]["detail"]["modes"] == [
        {"mode": "contains", "root": "message value", "needle": {"a": 1}}
    ]
    assert payload["error"]["detail"]["messages_scanned"] >= 1


def test_kafka_assert_no_match_modes_echo_path_narrowing_contains(install_fake):
    """When --path narrows --contains, the no-match detail must surface that path
    (and that --contains still roots at the message value) so the agent can see
    the exact target it AND-ed on."""
    install_fake([_msg("t", {"payload": {"event": "Y"}}, "k1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--contains",
            '{"event": "X"}',
            "--path",
            ".payload",
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 1
    assert payload["error"]["detail"]["modes"] == [
        {
            "mode": "contains",
            "root": "message value",
            "needle": {"event": "X"},
            "path": ".payload",
        }
    ]


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
    # A predicate that ERRORS AT RUNTIME on a message (valid syntax, but fails
    # against this value) must skip that message (not crash); an all-skip window
    # -> no match -> AssertionError. Note: a *compile* error (malformed syntax)
    # is now caught up front as a ConfigError (see
    # test_kafka_consume_invalid_match_is_config_error); only runtime errors
    # against specific messages reach this per-message skip path (DESIGN §3.2).
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
            ".value.a.b",  # valid syntax; .value.a==1 is a number -> runtime error
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
            '.value.status=="OK"',
            "--lookback",
            "10",
            "--timeout",
            "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["matched"] is True


def test_kafka_assert_match_on_key(install_fake):
    """Envelope-root reach: --match '.key == "..."' matches by message key, not
    a value field. Under the prior value-rooted predicate, .key evaluated
    against the value dict was null -> no match -> AssertionError."""
    install_fake([_msg("t", {"eventType": "X"}, "ord-1")])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "kafka",
            "assert",
            "--topic",
            "t",
            "--match",
            '.key == "ord-1"',
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
            "--match", ".value.a==1",
            "--assertion", "x",
            "--timeout", "0.02",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# v3 cluster resolution (Task 1: default / single-cluster path)
# --cluster flag and per-pattern bindings land in Task 2; here the resolution
# helper (the Task 1 contract) is exercised directly for the error cases that
# need an explicit name, and via the CLI for the no-flag paths.
# --------------------------------------------------------------------------- #


def test_kafka_produce_single_cluster_no_flag(install_fake):
    """A config with one cluster and no --cluster flag produces successfully —
    proving default/single-cluster resolution keeps the common case flagless."""
    cap = install_fake([])
    result = _run(
        [
            "--config", str(FIXTURE),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"a":1}',
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 0
    assert payload["ok"] is True
    # The fake producer received the produce call.
    assert cap["producer"].calls[0]["topic"] == "t"
    # The fixture's single default cluster was selected (broker resolved from
    # KAFKA_BROKER=localhost) — genuinely exercising single-cluster resolution.
    assert cap["cluster"].brokers == ["localhost"]


def test_kafka_consume_no_default_multi_cluster_error(install_fake, tmp_path):
    """A config with two clusters, no default_cluster, and no --cluster flag
    cannot resolve a cluster -> ConfigError (exit 2), message names the gap."""
    install_fake([])
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    a:\n"
        "      brokers: [ha:9092]\n"
        "    b:\n"
        "      brokers: [hb:9092]\n"
    )
    result = _run(
        ["--config", str(cfg), "kafka", "consume", "--topic", "t", "--timeout", "0.02"],
        env={},
    )
    payload = _payload(result)
    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "No kafka cluster specified" in payload["error"]["message"]


def test_resolve_cluster_name_explicit_wins():
    """Precedence: explicit > binding > default > single-cluster."""
    k = KafkaConfig(
        clusters={
            "a": KafkaCluster(brokers=["ha:9092"]),
            "b": KafkaCluster(brokers=["hb:9092"]),
        },
        default_cluster="a",
    )
    assert kafka_commands.resolve_cluster_name(k, "b") == "b"
    # binding_cluster beats default
    assert kafka_commands.resolve_cluster_name(k, None, "b") == "b"
    # default when no explicit/binding
    assert kafka_commands.resolve_cluster_name(k, None) == "a"


def test_resolve_cluster_name_single_cluster_auto_default():
    """One cluster defined, no default_cluster -> that cluster auto-resolves."""
    k = KafkaConfig(clusters={"only": KafkaCluster(brokers=["h:9092"])})
    assert kafka_commands.resolve_cluster_name(k, None) == "only"


def test_resolve_cluster_name_unknown_cluster_errors():
    """A resolved name absent from clusters -> ConfigError with detail.cluster
    (the Task 1 contract; the CLI --cluster wiring lands in Task 2)."""
    k = KafkaConfig(
        clusters={"a": KafkaCluster(brokers=["ha:9092"])},
        default_cluster="a",
    )
    with pytest.raises(ConfigError) as exc:
        kafka_commands.resolve_cluster_name(k, "ghost")
    assert "Unknown kafka cluster: ghost" in exc.value.message
    assert exc.value.detail["cluster"] == "ghost"


def test_resolve_cluster_name_no_cluster_specified_errors():
    """No explicit/binding/default and >1 cluster -> ConfigError."""
    k = KafkaConfig(
        clusters={
            "a": KafkaCluster(brokers=["ha:9092"]),
            "b": KafkaCluster(brokers=["hb:9092"]),
        }
    )
    with pytest.raises(ConfigError) as exc:
        kafka_commands.resolve_cluster_name(k, None)
    assert exc.value.message == "No kafka cluster specified"
    assert exc.value.detail == {}


# --------------------------------------------------------------------------- #
# Task 2: --cluster flag + per-pattern cluster binding
# Two-cluster configs: main (broker-a, default) + analytics (broker-b). The
# `install_fake` seam captures the resolved KafkaCluster passed to
# new_kafka_client, so tests assert which cluster was selected by inspecting
# ``captured["cluster"].brokers``.
# --------------------------------------------------------------------------- #


def _write_two_cluster_cfg(tmp_path, *, pattern_cluster=None):
    """Write a two-cluster v3 config: main (broker-a, default) + analytics
    (broker-b). When ``pattern_cluster`` is given, add a pattern ``ord``
    (topic=orders, match=.value.x) bound to that cluster."""
    lines = [
        'version: "3"',
        "kafka:",
        "  clusters:",
        "    main:",
        "      brokers: [broker-a:9092]",
        "    analytics:",
        "      brokers: [broker-b:9092]",
        "  default_cluster: main",
    ]
    if pattern_cluster is not None:
        lines += [
            "  patterns:",
            "    ord:",
            "      topic: orders",
            "      match: '.value.x'",
            f"      cluster: {pattern_cluster}",
        ]
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text("\n".join(lines) + "\n")
    return cfg


def test_kafka_produce_explicit_cluster(install_fake, tmp_path):
    """`--cluster analytics` selects the analytics cluster: the fake client is
    built from analytics's brokers (broker-b), not the default main (broker-a)."""
    cap = install_fake([])
    cfg = _write_two_cluster_cfg(tmp_path)
    result = _run(
        [
            "--config", str(cfg),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"a":1}',
            "--cluster", "analytics",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 0
    assert payload["ok"] is True
    # The factory received the analytics cluster (broker-b), not main (broker-a).
    assert cap["cluster"].brokers == ["broker-b:9092"]


def test_kafka_assert_pattern_resolves_cluster(install_fake, tmp_path):
    """`assert --pattern ord` (no --cluster, no --topic) resolves BOTH the topic
    and the cluster from the pattern binding (analytics). The fake client is
    built from analytics and consumes the pattern's topic `orders`."""
    cap = install_fake([_msg("orders", {"x": 1}, "k1")])
    cfg = _write_two_cluster_cfg(tmp_path, pattern_cluster="analytics")
    result = _run(
        [
            "--config", str(cfg),
            "kafka", "assert",
            "--pattern", "ord",
            "--timeout", "2",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 0
    assert payload["ok"] is True
    # Topic inferred from the pattern.
    assert payload["result"]["topic"] == "orders"
    # Cluster resolved from the pattern's binding (analytics / broker-b).
    assert cap["cluster"].brokers == ["broker-b:9092"]


def test_kafka_assert_cluster_flag_overrides_pattern(install_fake, tmp_path):
    """Precedence: `--cluster main` (explicit) beats the pattern's
    `cluster: analytics` (binding). Captured cluster is main (broker-a).

    Note: override of a pattern binding is only exercisable on `kafka assert`,
    the sole command that carries a pattern binding (produce/consume have no
    --pattern, so binding_cluster is always None there per the Task 2 spec)."""
    cap = install_fake([_msg("orders", {"x": 1}, "k1")])
    cfg = _write_two_cluster_cfg(tmp_path, pattern_cluster="analytics")
    result = _run(
        [
            "--config", str(cfg),
            "kafka", "assert",
            "--pattern", "ord",
            "--cluster", "main",
            "--timeout", "2",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 0
    assert payload["ok"] is True
    # Explicit --cluster main wins over the pattern's analytics binding.
    assert cap["cluster"].brokers == ["broker-a:9092"]


def test_kafka_assert_unknown_cluster_flag_error(install_fake, tmp_path):
    """`--cluster ghost` surfaces as a ConfigError at the CLI (flag path). The
    helper-level contract is covered by test_resolve_cluster_name_unknown_cluster_errors;
    this adds the flag-path coverage Task 1 deferred."""
    install_fake([])
    cfg = _write_two_cluster_cfg(tmp_path)
    result = _run(
        [
            "--config", str(cfg),
            "kafka", "assert",
            "--topic", "t",
            "--contains", '{"a":1}',
            "--timeout", "2",
            "--cluster", "ghost",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "Unknown kafka cluster: ghost" in payload["error"]["message"]
    assert payload["error"]["detail"]["cluster"] == "ghost"
