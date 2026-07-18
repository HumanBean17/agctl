"""Unit tests for Task 9: command-layer format resolver, SR startup probe,
``--value-format``/``--key-format`` CLI flags, and ``decode_errors`` surfacing.

Cases (a-h) from the task brief:

* (a) ``resolve_topic_format`` honors a topic-level ``value_format`` and falls
  back to JSON when neither topic nor cluster declares one.
* (b) cluster-level ``value_format: avro`` with no topic override -> AVRO.
* (c) topic override beats cluster default.
* (d) ``resolve_schema_registry_client`` returns a client when URL present,
  ``None`` when absent.
* (e) memoization: two calls for the same cluster return the same instance.
* (f) ``probe_schema_registry`` raises ``ConnectionFailure`` (naming BOTH the
  cluster and the URL) when ``check_reachable`` raises, returns ``None``
  otherwise.
* (g) Click-level ``--value-format avro`` threads ``Format.AVRO`` into the
  codec passed to ``new_kafka_client`` (captured via monkeypatched factory).
* (h) ``kafka consume`` result envelope carries ``decode_errors: 0`` on a
  clean run.
"""

import json
import time
from pathlib import Path

import pytest
from click.testing import CliRunner
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, TopicPartition

from agctl.cli import cli
from agctl.clients.kafka_client import KafkaClient
from agctl.commands import kafka_commands
from agctl.config.models import (
    Config,
    KafkaCluster,
    KafkaConfig,
    KafkaTopicConfig,
)
from agctl.errors import ConfigError, ConnectionFailure
from agctl.serialization import Format

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
# Test seams (mirrors tests/unit/test_kafka_commands.py — minimal copies).
# ---------------------------------------------------------------------------


class FakeMsg:
    def __init__(self, partition, offset, ts_ms):
        self._p, self._o, self._ts = partition, offset, ts_ms

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def timestamp(self):
        return (1, self._ts)


class FakeProducer:
    def __init__(self, conf):
        self.conf = conf
        self.calls = []
        self._p, self._o, self._ts = 0, 100, 1719660000000

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.calls.append({"topic": topic, "value": value, "key": key})
        if on_delivery is not None:
            on_delivery(None, FakeMsg(self._p, self._o, self._ts))
        self._o += 1

    def flush(self, timeout):
        return 0


class FakeCMsg:
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


class FakeConsumer:
    def __init__(self, conf, messages=None):
        self.conf = conf
        self._messages = list(messages or [])
        self._messages.sort(key=lambda m: (m.partition(), m.offset()))
        self._seek_offsets = {}
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
        import math

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
            seek_off = self._seek_offsets.get(key, 0)
            if m.offset() >= seek_off:
                return m
        return None

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_sr_cache():
    """SR-client cache is module-level; clear it before and after each test
    so memoization assertions are isolated."""
    kafka_commands._sr_client_cache.clear()
    yield
    kafka_commands._sr_client_cache.clear()


def _cfg(
    *,
    topic_fmt=None,
    topic_key_fmt=None,
    topic_subject_strategy=None,
    cluster_value_fmt=None,
    cluster_key_fmt=None,
    schema_registry_url=None,
    cluster_name="default",
    topic_name="orders.created",
):
    """Build a Config in-code with the requested topic/cluster format knobs."""
    topic_cfg = None
    if topic_fmt is not None or topic_key_fmt is not None or topic_subject_strategy is not None:
        topic_cfg = KafkaTopicConfig(
            value_format=topic_fmt,
            key_format=topic_key_fmt,
            subject_strategy=topic_subject_strategy,
        )
    cluster_kwargs = {"brokers": ["host:9092"]}
    if cluster_value_fmt is not None:
        cluster_kwargs["value_format"] = cluster_value_fmt
    if cluster_key_fmt is not None:
        cluster_kwargs["key_format"] = cluster_key_fmt
    if schema_registry_url is not None:
        cluster_kwargs["schema_registry_url"] = schema_registry_url
    return Config(
        version="3",
        kafka=KafkaConfig(
            clusters={cluster_name: KafkaCluster(**cluster_kwargs)},
            default_cluster=cluster_name,
            topics={topic_name: topic_cfg} if topic_cfg is not None else {},
        ),
    )


# ===========================================================================
# (a, b, c) resolve_topic_format — precedence
# ===========================================================================


def test_resolve_topic_format_topic_override_wins_over_cluster_default():
    """(c, part of a) Topic-level value_format beats cluster-level default."""
    cfg = _cfg(
        topic_fmt="avro",
        cluster_value_fmt="json",
        topic_name="orders.created",
    )
    assert (
        kafka_commands.resolve_topic_format(cfg, "orders.created", "default", "value")
        == Format.AVRO
    )


def test_resolve_topic_format_cluster_default_when_topic_unset():
    """(b) No topic override; cluster default ``avro`` -> Format.AVRO."""
    cfg = _cfg(cluster_value_fmt="avro")
    assert (
        kafka_commands.resolve_topic_format(cfg, "anything", "default", "value")
        == Format.AVRO
    )


def test_resolve_topic_format_default_json_when_nothing_declared():
    """(a) Nothing declared -> Format.JSON (value) default; never raises."""
    cfg = _cfg()
    assert (
        kafka_commands.resolve_topic_format(cfg, "unknown.topic", "default", "value")
        == Format.JSON
    )


def test_resolve_topic_format_key_defaults_to_string():
    """Key side defaults to Format.KEY_STRING when nothing is declared."""
    cfg = _cfg()
    assert (
        kafka_commands.resolve_topic_format(cfg, "unknown.topic", "default", "key")
        == Format.KEY_STRING
    )


def test_resolve_topic_format_key_topic_override_beats_cluster():
    """Key side: topic-level key_format wins over cluster-level default."""
    cfg = _cfg(topic_key_fmt="avro", cluster_key_fmt="string")
    assert (
        kafka_commands.resolve_topic_format(cfg, "orders.created", "default", "key")
        == Format.AVRO
    )


def test_resolve_topic_format_unknown_topic_falls_through_to_cluster_default():
    """Unknown topic -> cluster default -> JSON (no raise)."""
    cfg = _cfg(cluster_value_fmt="avro", topic_name="declared.topic")
    # Topic absent from the map: cluster default avro applies.
    assert (
        kafka_commands.resolve_topic_format(cfg, "not.in.map", "default", "value")
        == Format.AVRO
    )


# ===========================================================================
# resolve_subject_strategy
# ===========================================================================


def test_resolve_subject_strategy_topic_override():
    cfg = _cfg(topic_subject_strategy="record")
    assert (
        kafka_commands.resolve_subject_strategy(cfg, "orders.created", "default")
        == "record"
    )


def test_resolve_subject_strategy_defaults_to_topic():
    cfg = _cfg()
    assert (
        kafka_commands.resolve_subject_strategy(cfg, "unknown", "default") == "topic"
    )


# ===========================================================================
# (d, e) resolve_schema_registry_client
# ===========================================================================


class _FakeSRClient:
    """Stand-in for serialization.registry.SchemaRegistryClient.

    The real class lazy-imports ``confluent_kafka.schema_registry`` which pulls
    ``authlib`` (absent in this venv) — so we monkeypatch the class itself for
    resolver-level tests; the real construction is exercised in
    ``test_serialization_registry.py``.
    """

    instances = []

    def __init__(self, url, sr_config=None, **kwargs):
        self.url = url
        self.sr_config = sr_config
        type(self).instances.append(self)


@pytest.fixture
def patch_sr_class(monkeypatch):
    """Patch the SchemaRegistryClient symbol the resolver imports lazily."""
    _FakeSRClient.instances.clear()
    monkeypatch.setattr(
        "agctl.serialization.registry.SchemaRegistryClient",
        _FakeSRClient,
    )
    return _FakeSRClient


def test_resolve_schema_registry_client_returns_none_when_url_absent(patch_sr_class):
    """(d) No schema_registry_url on the cluster -> None (no client built)."""
    cfg = _cfg()  # no schema_registry_url
    assert kafka_commands.resolve_schema_registry_client(cfg, "default") is None
    assert patch_sr_class.instances == []  # constructor never called


def test_resolve_schema_registry_client_returns_client_when_url_present(patch_sr_class):
    """(d) URL present -> a SchemaRegistryClient is constructed and returned."""
    cfg = _cfg(schema_registry_url="http://sr:8081")
    client = kafka_commands.resolve_schema_registry_client(cfg, "default")
    assert client is not None
    assert client.url == "http://sr:8081"


def test_resolve_schema_registry_client_memoizes_per_cluster(patch_sr_class):
    """(e) Two calls for the same cluster name return the SAME instance."""
    cfg = _cfg(schema_registry_url="http://sr:8081")
    first = kafka_commands.resolve_schema_registry_client(cfg, "default")
    second = kafka_commands.resolve_schema_registry_client(cfg, "default")
    assert first is second
    # The constructor ran exactly once for the cluster.
    assert len(patch_sr_class.instances) == 1


def test_resolve_schema_registry_client_separate_clusters_separate_instances(
    patch_sr_class,
):
    """Different cluster names are cached independently."""
    cfg = Config(
        version="3",
        kafka=KafkaConfig(
            clusters={
                "a": KafkaCluster(brokers=["h:9092"], schema_registry_url="http://a:8081"),
                "b": KafkaCluster(brokers=["h:9092"], schema_registry_url="http://b:8081"),
            },
            default_cluster="a",
        ),
    )
    a = kafka_commands.resolve_schema_registry_client(cfg, "a")
    b = kafka_commands.resolve_schema_registry_client(cfg, "b")
    assert a is not b
    assert a.url == "http://a:8081"
    assert b.url == "http://b:8081"


# ===========================================================================
# (f) probe_schema_registry
# ===========================================================================


class _ProbeSR:
    """Minimal SR double for probe tests; only ``check_reachable`` and ``_url``."""

    def __init__(self, url, *, raise_exc=None):
        self._url = url
        self._raise = raise_exc
        self.calls = 0

    def check_reachable(self):
        self.calls += 1
        if self._raise is not None:
            raise self._raise


def test_probe_schema_registry_returns_none_on_success():
    sr = _ProbeSR("http://sr:8081")
    assert kafka_commands.probe_schema_registry(sr, "default") is None
    assert sr.calls == 1


def test_probe_schema_registry_raises_naming_cluster_and_url():
    """(f) On failure the ConnectionFailure message names BOTH the cluster
    and the URL (the T5 review point resolved here)."""
    inner = ConnectionFailure(message="get_subjects blew up")
    sr = _ProbeSR("http://sr:8081", raise_exc=inner)
    with pytest.raises(ConnectionFailure) as exc:
        kafka_commands.probe_schema_registry(sr, "prod-cluster")
    msg = exc.value.message
    assert "prod-cluster" in msg
    assert "http://sr:8081" in msg
    assert exc.value.detail["cluster"] == "prod-cluster"


# ===========================================================================
# (g) Click-level --value-format threads Format.AVRO into the codec
# ===========================================================================


SCHEMA_STR = '{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}'
SCHEMA_ID = 17


class _CodecFakeSR:
    """Fake SR for the produce Click test: returns a registered schema so the
    Avro encode path succeeds (no real registry / authlib required)."""

    def __init__(self):
        self.by_id = {SCHEMA_ID: ("AVRO", SCHEMA_STR)}
        self.latest = {"t-value": ("AVRO", SCHEMA_STR, SCHEMA_ID)}

    def get_schema(self, schema_id):
        return self.by_id[schema_id]

    def get_latest_schema(self, subject):
        return self.latest[subject]

    def check_reachable(self):
        return None


def test_kafka_produce_value_format_flag_threads_avro_into_codec(
    monkeypatch, tmp_path
):
    """(g) ``kafka produce --value-format avro`` resolves Format.AVRO from the
    flag and threads it into the codec captured by the monkeypatched client
    factory (precedence level 1: CLI flag > topic > cluster)."""
    pytest.importorskip("fastavro")

    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    default:\n"
        "      brokers: [localhost:9092]\n"
        "      schema_registry_url: http://sr:8081\n"
        "  default_cluster: default\n"
    )

    fake_sr = _CodecFakeSR()
    monkeypatch.setattr(
        "agctl.serialization.registry.SchemaRegistryClient",
        lambda url, sr_config=None, **kw: fake_sr,
    )

    fake_producer = FakeProducer({})
    captured = {}

    def factory(cluster, group_id=None, *, codec=None):
        captured["codec"] = codec
        captured["cluster"] = cluster
        return KafkaClient(
            cluster.brokers,
            producer_factory=lambda c: fake_producer,
            codec=codec,
        )

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(cfg),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"id":"x"}',
            "--value-format", "avro",
        ],
        env={},
    )

    assert result.exit_code == 0, result.output
    codec = captured["codec"]
    assert codec is not None
    assert codec["value"]["fmt"] == Format.AVRO
    # Key side stays on the default KEY_STRING when --key-format is omitted.
    assert codec["key"]["fmt"] == Format.KEY_STRING
    # The SR client is the one we patched in.
    assert codec["sr"] is fake_sr
    # Probe ran exactly once before the produce.
    # (No surface to count probe calls; check_reachable returning None is enough.)


def test_kafka_produce_no_format_flag_passes_codec_none_for_pure_json(monkeypatch, tmp_path):
    """Regression guard: with no --value-format and no topic/cluster format
    declared, the resolver returns codec=None so the legacy byte-identical
    JSON path is used (no Format.JSON-codec divergence)."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    default:\n"
        "      brokers: [localhost:9092]\n"
        "  default_cluster: default\n"
    )

    fake_producer = FakeProducer({})
    captured = {}

    def factory(cluster, group_id=None, *, codec=None):
        captured["codec"] = codec
        return KafkaClient(
            cluster.brokers,
            producer_factory=lambda c: fake_producer,
        )

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(cfg),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"a":1}',
        ],
        env={},
    )

    assert result.exit_code == 0, result.output
    # codec is None (pure-JSON topic) and the producer got legacy JSON bytes.
    assert captured["codec"] is None
    assert fake_producer.calls[0]["value"] == b'{"a": 1}'


def test_kafka_produce_value_format_flag_threads_protobuf_into_codec(
    monkeypatch, tmp_path
):
    """``kafka produce --value-format protobuf`` resolves Format.PROTOBUF from
    the flag and threads it into the codec captured by the monkeypatched
    client factory. Verifies the CLI flag layer is format-agnostic (no
    Avro-specific branch in the resolver path)."""
    pytest.importorskip("google.protobuf")
    pytest.importorskip("grpc_tools")

    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    default:\n"
        "      brokers: [localhost:9092]\n"
        "      schema_registry_url: http://sr:8081\n"
        "  default_cluster: default\n"
    )

    # Fake SR returns a PROTOBUF schema so the encode path succeeds.
    proto_schema = 'syntax = "proto3"; message E { string id = 1; }'

    class _ProtoFakeSR:
        def __init__(self):
            self.by_id = {29: ("PROTOBUF", proto_schema)}
            self.latest = {"t-value": ("PROTOBUF", proto_schema, 29)}

        def get_schema(self, schema_id):
            return self.by_id[schema_id]

        def get_latest_schema(self, subject):
            return self.latest[subject]

        def check_reachable(self):
            return None

    fake_sr = _ProtoFakeSR()
    monkeypatch.setattr(
        "agctl.serialization.registry.SchemaRegistryClient",
        lambda url, sr_config=None, **kw: fake_sr,
    )

    fake_producer = FakeProducer({})
    captured = {}

    def factory(cluster, group_id=None, *, codec=None):
        captured["codec"] = codec
        captured["cluster"] = cluster
        return KafkaClient(
            cluster.brokers,
            producer_factory=lambda c: fake_producer,
            codec=codec,
        )

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(cfg),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"id":"x"}',
            "--value-format", "protobuf",
        ],
        env={},
    )

    assert result.exit_code == 0, result.output
    codec = captured["codec"]
    assert codec is not None
    assert codec["value"]["fmt"] == Format.PROTOBUF
    assert codec["key"]["fmt"] == Format.KEY_STRING
    assert codec["sr"] is fake_sr
    # The produced value is Confluent-framed Protobuf bytes (magic 0x00 + 4-byte
    # schema id + payload), proving the CLI threaded the format through the
    # produce path end-to-end.
    value_bytes = fake_producer.calls[0]["value"]
    assert value_bytes[0] == 0
    import struct as _struct
    schema_id = _struct.unpack(">I", value_bytes[1:5])[0]
    assert schema_id == 29


# ===========================================================================
# (h) consume result envelope carries decode_errors: 0 on a clean run
# ===========================================================================


def _msg(topic, value_obj, key, ms_ago=1000, partition=0, offset=0):
    now_ms = int(time.time() * 1000)
    return FakeCMsg(
        topic,
        partition,
        offset,
        key,
        json.dumps(value_obj).encode("utf-8"),
        now_ms - ms_ago,
    )


def _build_fake_client(messages, producer=None):
    consumer = FakeConsumer({}, messages=messages)
    if producer is None:
        producer = FakeProducer({})

    def consumer_factory(conf):
        consumer.conf = conf
        return consumer

    def producer_factory(conf):
        producer.conf = conf
        return producer

    return KafkaClient(
        ["host:9092"],
        consumer_factory=consumer_factory,
        producer_factory=producer_factory,
    )


def test_kafka_consume_clean_run_has_decode_errors_zero(monkeypatch):
    """(h) A clean JSON consume run reports decode_errors: 0 in the envelope."""
    client = _build_fake_client([_msg("t", {"a": 1}, "k1")])

    # Pure-JSON path: the factory need not capture codec (Option B — codec is
    # only passed when non-None). Use a permissive factory that accepts codec
    # so the test is robust to future wiring changes.
    def factory(cluster, group_id=None, *, codec=None):
        return client

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(FIXTURE),
            "kafka", "consume",
            "--topic", "t",
            "--timeout", "0.02",
            "--lookback", "10",
        ],
        env=ENV,
    )
    payload = json.loads(result.output)
    assert result.exit_code == 0, result.output
    assert payload["result"]["decode_errors"] == 0
    assert payload["result"]["count"] == 1


def test_kafka_assert_clean_run_has_decode_errors_zero(monkeypatch):
    """Mirror of (h) for ``kafka assert`` — same wiring, same envelope field."""
    client = _build_fake_client([_msg("t", {"a": 1}, "k1")])

    def factory(cluster, group_id=None, *, codec=None):
        return client

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(FIXTURE),
            "kafka", "assert",
            "--topic", "t",
            "--contains", '{"a":1}',
            "--lookback", "10",
            "--timeout", "0.02",
        ],
        env=ENV,
    )
    payload = json.loads(result.output)
    assert result.exit_code == 0, result.output
    assert payload["result"]["decode_errors"] == 0


# ===========================================================================
# Codec wiring: SR probe runs once before the operation when avro resolves
# ===========================================================================


def test_kafka_produce_probes_sr_once_when_avro_resolves(monkeypatch, tmp_path):
    """When a non-JSON format resolves AND an SR client exists, the startup
    probe runs exactly once before the produce encodes."""
    pytest.importorskip("fastavro")

    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    default:\n"
        "      brokers: [localhost:9092]\n"
        "      schema_registry_url: http://sr:8081\n"
        "  default_cluster: default\n"
    )

    fake_sr = _CodecFakeSR()
    fake_sr.probe_calls = 0
    orig_check = fake_sr.check_reachable

    def counting_check():
        fake_sr.probe_calls += 1
        return orig_check()

    fake_sr.check_reachable = counting_check

    monkeypatch.setattr(
        "agctl.serialization.registry.SchemaRegistryClient",
        lambda url, sr_config=None, **kw: fake_sr,
    )

    fake_producer = FakeProducer({})

    def factory(cluster, group_id=None, *, codec=None):
        return KafkaClient(
            cluster.brokers,
            producer_factory=lambda c: fake_producer,
            codec=codec,
        )

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(cfg),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"id":"x"}',
            "--value-format", "avro",
        ],
        env={},
    )

    assert result.exit_code == 0, result.output
    assert fake_sr.probe_calls == 1


def test_kafka_produce_avro_without_sr_url_is_config_error(monkeypatch, tmp_path):
    """Defense-in-depth: a non-JSON format resolves but the cluster has no
    schema_registry_url -> ConfigError (validator already flags this; the
    command layer catches it here too in case config was built in-code)."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    default:\n"
        "      brokers: [localhost:9092]\n"
        "  default_cluster: default\n"
    )

    fake_producer = FakeProducer({})

    def factory(cluster, group_id=None, *, codec=None):
        return KafkaClient(
            cluster.brokers,
            producer_factory=lambda c: fake_producer,
            codec=codec,
        )

    monkeypatch.setattr(kafka_commands, "new_kafka_client", factory)

    result = CliRunner().invoke(
        cli,
        [
            "--config", str(cfg),
            "kafka", "produce",
            "--topic", "t",
            "--message", '{"id":"x"}',
            "--value-format", "avro",
        ],
        env={},
    )

    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "schema_registry_url" in payload["error"]["message"]
    assert payload["error"]["detail"]["cluster"] == "default"
