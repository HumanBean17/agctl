import pytest
from pydantic import ValidationError

from agctl.config.models import (
    Config,
    DatabaseConnection,
    DatabaseTemplate,
    GrpcConfig,
    GrpcDescriptorSource,
    GrpcTarget,
    GrpcTemplate,
    GrpcTls,
    KafkaCluster,
    KafkaConfig,
    KafkaPattern,
    KafkaReactor,
    KafkaReaction,
    LogSource,
    LogsConfig,
    LogsDefaults,
)


def _full_config_dict():
    return {
        "version": "3",
        "services": {
            "order-service": {"base_url": "http://localhost:8081", "health_path": "/health"}
        },
        "kafka": {
            "clusters": {
                "main": {"brokers": ["localhost:9092"]},
            },
            "default_cluster": "main",
            "patterns": {
                "order-created": {
                    "topic": "orders.created",
                    "match": '.value.eventType == "ORDER_CREATED"',
                }
            },
        },
        "database": {
            "connections": {
                "main-db": {"type": "postgresql", "host": "h", "default": True},
            },
            "templates": {
                "find-order": {"connection": "main-db", "sql": "SELECT 1 FROM orders WHERE id = :orderId"},
            },
        },
        "templates": {
            "create-order": {"method": "POST", "service": "order-service", "path": "/orders"},
        },
        "defaults": {"timeout_seconds": 10, "database_connection": "main-db"},
    }


def test_full_config_validates_with_connections_and_templates():
    """D1 regression: both database.connections and database.templates survive."""
    cfg = Config.model_validate(_full_config_dict())
    assert cfg.database.connections["main-db"].type == "postgresql"
    assert cfg.database.templates["find-order"].connection == "main-db"
    assert cfg.templates["create-order"].method == "POST"
    assert cfg.kafka.patterns["order-created"].topic == "orders.created"


def test_empty_sections_default():
    cfg = Config.model_validate({"version": "3"})
    assert cfg.services == {}
    assert cfg.database.connections == {}
    assert cfg.database.templates == {}
    assert cfg.kafka.clusters == {}
    assert cfg.kafka.default_cluster is None
    assert cfg.kafka.patterns == {}


def test_http_template_requires_method_service_path():
    with pytest.raises(ValidationError):
        Config.model_validate({"version": "1", "templates": {"x": {"method": "GET"}}})


def test_db_template_requires_sql():
    with pytest.raises(ValidationError):
        Config.model_validate(
            {"version": "1", "database": {"templates": {"x": {"connection": "c"}}}}
        )


def test_database_connection_writable_default_false():
    """DatabaseConnection.writable defaults to False."""
    conn = DatabaseConnection(type="postgresql")
    assert conn.writable is False


def test_database_connection_writable_explicit_true():
    """DatabaseConnection.writable can be set to True."""
    conn = DatabaseConnection(type="postgresql", writable=True)
    assert conn.writable is True


def test_database_template_mode_default_read():
    """DatabaseTemplate.mode defaults to 'read'."""
    tmpl = DatabaseTemplate(sql="SELECT 1")
    assert tmpl.mode == "read"


def test_database_template_mode_explicit_write():
    """DatabaseTemplate.mode can be set to 'write'."""
    tmpl = DatabaseTemplate(sql="...", mode="write")
    assert tmpl.mode == "write"


def test_database_template_mode_invalid_literal_raises_validation_error():
    """DatabaseTemplate.mode must be 'read' or 'write'."""
    with pytest.raises(ValidationError):
        DatabaseTemplate(sql="...", mode="bogus")


def test_database_connection_roundtrip():
    """DatabaseConnection.writable round-trips through model_dump()."""
    conn = DatabaseConnection(type="postgresql", writable=True)
    dumped = conn.model_dump()
    assert dumped["writable"] is True


def test_database_connection_url_defaults_none():
    """DatabaseConnection.url is optional and defaults to None."""
    conn = DatabaseConnection(type="postgresql", host="h")
    assert conn.url is None


def test_database_connection_url_roundtrips():
    """DatabaseConnection.url round-trips through model_dump()."""
    url = "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}/${DB_NAME}"
    conn = DatabaseConnection(type="postgresql", url=url)
    assert conn.model_dump()["url"] == url


def test_database_template_roundtrip():
    """DatabaseTemplate.mode round-trips through model_dump()."""
    tmpl = DatabaseTemplate(sql="x", mode="write")
    dumped = tmpl.model_dump()
    assert dumped["mode"] == "write"


def test_log_source_defaults():
    """LogSource() has type=='file', path is None, format=='logstash', service is None."""
    source = LogSource()
    assert source.type == "file"
    assert source.path is None
    assert source.format == "logstash"
    assert source.service is None


def test_logs_defaults_defaults():
    """LogsDefaults() has tail_lines==200, limit==50, timeout_seconds==10, poll_interval_ms==100."""
    defaults = LogsDefaults()
    assert defaults.tail_lines == 200
    assert defaults.limit == 50
    assert defaults.timeout_seconds == 10
    assert defaults.poll_interval_ms == 100


def test_logs_config_empty_default():
    """LogsConfig() has sources=={} and a LogsDefaults instance."""
    cfg = LogsConfig()
    assert cfg.sources == {}
    assert isinstance(cfg.defaults, LogsDefaults)
    assert cfg.defaults.tail_lines == 200
    assert cfg.defaults.limit == 50
    assert cfg.defaults.timeout_seconds == 10
    assert cfg.defaults.poll_interval_ms == 100


def test_config_has_logs_field():
    """Config(version='2') has .logs being a LogsConfig (empty sources)."""
    cfg = Config(version="2")
    assert isinstance(cfg.logs, LogsConfig)
    assert cfg.logs.sources == {}
    assert isinstance(cfg.logs.defaults, LogsDefaults)

    # Also test constructing with a dict via model_validate
    cfg2 = Config.model_validate(
        {
            "version": "2",
            "logs": {"sources": {"svc": {"path": "/tmp/x.log"}}},
        }
    )
    assert cfg2.logs.sources["svc"].path == "/tmp/x.log"
    assert cfg2.logs.sources["svc"].type == "file"  # default applied


# --- v3 kafka clusters schema -------------------------------------------------


def test_kafka_config_clusters_shape_parses():
    """A KafkaConfig built from a clusters dict parses; the cluster's brokers,
    default_cluster, and patterns are all reachable."""
    cfg = Config.model_validate(
        {
            "version": "3",
            "kafka": {
                "clusters": {"main": {"brokers": ["h:9092"]}},
                "default_cluster": "main",
                "patterns": {"p": {"topic": "t"}},
            },
        }
    )
    assert cfg.kafka.clusters["main"].brokers == ["h:9092"]
    assert cfg.kafka.default_cluster == "main"
    assert cfg.kafka.patterns["p"].topic == "t"


def test_kafka_cluster_carries_per_cluster_fields():
    """KafkaCluster carries brokers/ssl/timeout_seconds/default_consumer_group/
    schema_registry_url (the fields formerly on KafkaConfig)."""
    cluster = KafkaCluster(
        brokers=["h:9092"],
        timeout_seconds=30,
        default_consumer_group="g",
        schema_registry_url="http://sr:8081",
    )
    assert cluster.brokers == ["h:9092"]
    assert cluster.timeout_seconds == 30
    assert cluster.default_consumer_group == "g"
    assert cluster.schema_registry_url == "http://sr:8081"
    assert cluster.ssl is None


def test_kafka_config_default_is_empty():
    """KafkaConfig() default: empty clusters, default_cluster None, empty patterns."""
    k = KafkaConfig()
    assert k.clusters == {}
    assert k.default_cluster is None
    assert k.patterns == {}


def test_kafka_pattern_accepts_cluster_field():
    """KafkaPattern gains an optional cluster field (consumed in Task 2)."""
    pat = KafkaPattern(topic="t", cluster="analytics")
    assert pat.cluster == "analytics"
    # defaults to None when omitted
    assert KafkaPattern(topic="t").cluster is None


def test_kafka_reactor_accepts_cluster_field():
    """KafkaReactor gains an optional cluster field (consumed in Task 3)."""
    reactor = KafkaReactor(
        topic="t",
        cluster="analytics",
        reaction=KafkaReaction(topic="out", value={}),
    )
    assert reactor.cluster == "analytics"
    # defaults to None when omitted
    plain = KafkaReactor(topic="t", reaction=KafkaReaction(topic="out", value={}))
    assert plain.cluster is None


# gRPC model tests (Task 1)


def test_grpc_tls_defaults():
    """GrpcTls() has all four fields None."""
    tls = GrpcTls()
    assert tls.ca_location is None
    assert tls.certificate_location is None
    assert tls.key_location is None
    assert tls.override_authority is None


def test_grpc_target_defaults():
    """GrpcTarget(address='localhost:50051') has use_tls is False, tls is None, reflection == 'auto'."""
    target = GrpcTarget(address="localhost:50051")
    assert target.address == "localhost:50051"
    assert target.use_tls is False
    assert target.tls is None
    assert target.reflection == "auto"


def test_grpc_target_explicit_values():
    """GrpcTarget(address='x', use_tls=True, reflection='off') keeps those values."""
    target = GrpcTarget(address="x", use_tls=True, reflection="off")
    assert target.address == "x"
    assert target.use_tls is True
    assert target.reflection == "off"


def test_grpc_target_rejects_bad_reflection():
    """GrpcTarget(address='x', reflection='maybe') raises ValidationError."""
    with pytest.raises(ValidationError):
        GrpcTarget(address="x", reflection="maybe")


def test_grpc_descriptor_source_all_none():
    """GrpcDescriptorSource() has proto is None, descriptor_set is None, include_paths == []."""
    source = GrpcDescriptorSource()
    assert source.proto is None
    assert source.descriptor_set is None
    assert source.include_paths == []


def test_grpc_template_defaults():
    """GrpcTemplate(target='t', service='s.v1.S', method='M') has description is None, metadata == {}, message is None."""
    tmpl = GrpcTemplate(target="t", service="s.v1.S", method="M")
    assert tmpl.target == "t"
    assert tmpl.service == "s.v1.S"
    assert tmpl.method == "M"
    assert tmpl.description is None
    assert tmpl.metadata == {}
    assert tmpl.message is None


def test_grpc_config_empty_default():
    """GrpcConfig() has targets == {}, descriptors == [], templates == {}."""
    cfg = GrpcConfig()
    assert cfg.targets == {}
    assert cfg.descriptors == []
    assert cfg.templates == {}


def test_config_has_grpc_field():
    """Config(version='2') has .grpc a GrpcConfig (empty)."""
    cfg = Config(version="3")
    assert isinstance(cfg.grpc, GrpcConfig)
    assert cfg.grpc.targets == {}
    assert cfg.grpc.descriptors == []
    assert cfg.grpc.templates == {}

    # Also test constructing with a dict via model_validate
    cfg2 = Config.model_validate(
        {
            "version": "2",
            "grpc": {"targets": {"svc": {"address": "h:1"}}},
        }
    )
    assert cfg2.grpc.targets["svc"].address == "h:1"
    assert cfg2.grpc.targets["svc"].reflection == "auto"  # default applied
