"""Tests for the Kafka Schema Registry / format config models (Task 1).

These cover the additive v3 ``kafka:`` block extensions:
- ``SchemaRegistryConfig`` (auth / basic_auth / ssl) on ``KafkaCluster.schema_registry``.
- ``KafkaCluster`` cluster-level format defaults (``value_format`` / ``key_format``).
- ``KafkaTopicConfig`` and the ``KafkaConfig.topics`` map.

Cross-field validation (topic -> cluster, format-requires-SR, auth-shape) is
Task 2's job (``config/validator.py``); this module exercises only what Pydantic
``Literal`` types enforce at parse time.

The ``DatabaseConnection.options`` tests at the bottom cover Task 6's additive
``options: dict[str, Any] = Field(default_factory=dict)`` field on
``DatabaseConnection``.
"""

import pytest
from pydantic import ValidationError

from agctl.config.models import (
    DatabaseConnection,
    KafkaCluster,
    KafkaConfig,
    KafkaTopicConfig,
    SchemaRegistryConfig,
)


def test_kafka_config_topics_map_parses_value_format():
    """(a) KafkaConfig with topics {"orders.created": {value_format: "avro"}}
    parses; cfg.kafka.topics["orders.created"].value_format == "avro"."""
    cfg = KafkaConfig.model_validate(
        {"topics": {"orders.created": {"value_format": "avro"}}}
    )
    assert "orders.created" in cfg.topics
    assert cfg.topics["orders.created"].value_format == "avro"


def test_kafka_cluster_defaults_value_format_json_key_format_string_no_sr():
    """(b) KafkaCluster() defaults value_format == "json", key_format == "string",
    schema_registry is None."""
    cluster = KafkaCluster()
    assert cluster.value_format == "json"
    assert cluster.key_format == "string"
    assert cluster.schema_registry is None


def test_kafka_cluster_schema_registry_basic_auth_parses():
    """(c) KafkaCluster(schema_registry={"auth":"basic","basic_auth":{...}})
    parses to a SchemaRegistryConfig with auth == "basic"."""
    cluster = KafkaCluster(
        schema_registry={
            "auth": "basic",
            "basic_auth": {"username": "u", "password": "p"},
        }
    )
    sr = cluster.schema_registry
    assert isinstance(sr, SchemaRegistryConfig)
    assert sr.auth == "basic"
    assert sr.basic_auth is not None
    assert sr.basic_auth.username == "u"
    assert sr.basic_auth.password == "p"
    assert sr.ssl is None


def test_kafka_cluster_invalid_value_format_raises_validation_error():
    """(d) An invalid value_format ("yaml") raises Pydantic ValidationError."""
    with pytest.raises(ValidationError):
        KafkaCluster(value_format="yaml")


def test_kafka_topic_config_subject_strategy_with_json_parses():
    """(e) KafkaTopicConfig(subject_strategy="record", value_format="json")
    parses; strategy validity vs format is a Task-2 warning, not a model error."""
    topic = KafkaTopicConfig(subject_strategy="record", value_format="json")
    assert topic.subject_strategy == "record"
    assert topic.value_format == "json"


# --- additional shape / default coverage for the new models -------------------


def test_schema_registry_config_defaults_all_none():
    """SchemaRegistryConfig() with no args has auth/basic_auth/ssl all None."""
    sr = SchemaRegistryConfig()
    assert sr.auth is None
    assert sr.basic_auth is None
    assert sr.ssl is None


def test_schema_registry_config_rejects_invalid_auth():
    """SchemaRegistryConfig.auth is Literal["plaintext","basic","mtls"]; other
    values raise ValidationError at parse time."""
    with pytest.raises(ValidationError):
        SchemaRegistryConfig(auth="oauth")


def test_kafka_topic_config_defaults_all_none():
    """KafkaTopicConfig() with no args has cluster/value_format/key_format/
    subject_strategy all None."""
    topic = KafkaTopicConfig()
    assert topic.cluster is None
    assert topic.value_format is None
    assert topic.key_format is None
    assert topic.subject_strategy is None


def test_kafka_topic_config_rejects_invalid_subject_strategy():
    """KafkaTopicConfig.subject_strategy is Literal["topic","record",
    "topic_record"]; other values raise ValidationError at parse time."""
    with pytest.raises(ValidationError):
        KafkaTopicConfig(subject_strategy="bogus")


def test_kafka_config_topics_defaults_empty_dict():
    """KafkaConfig() defaults topics to an empty dict (not None)."""
    cfg = KafkaConfig()
    assert cfg.topics == {}


def test_kafka_topic_config_full_shape_round_trips():
    """A fully-populated KafkaTopicConfig round-trips through model_dump()."""
    topic = KafkaTopicConfig(
        cluster="analytics",
        value_format="protobuf",
        key_format="avro",
        subject_strategy="topic_record",
    )
    dumped = topic.model_dump()
    assert dumped == {
        "cluster": "analytics",
        "value_format": "protobuf",
        "key_format": "avro",
        "subject_strategy": "topic_record",
    }


# --- DatabaseConnection.options (Task 6) ----------------------------------


def test_database_connection_options_defaults_to_empty_dict():
    """DatabaseConnection(type="mysql", host="h") has options == {} (default factory)."""
    conn = DatabaseConnection(type="mysql", host="h")
    assert conn.options == {}


def test_database_connection_options_round_trips_via_model_dump():
    """Populated options round-trip through model_dump() verbatim."""
    conn = DatabaseConnection(
        type="mysql",
        host="h",
        options={"charset": "utf8mb4", "connect_timeout": 10},
    )
    dumped = conn.model_dump()
    assert dumped["options"] == {"charset": "utf8mb4", "connect_timeout": 10}


def test_database_connection_options_default_applies_for_sqlite():
    """Default options == {} even on a connection that only sets url."""
    conn = DatabaseConnection(type="sqlite", url=":memory:")
    assert conn.options == {}


def test_database_connection_options_rejects_non_dict():
    """Pydantic rejects a non-dict options value (e.g. a bare string)."""
    with pytest.raises(ValidationError):
        DatabaseConnection(type="mysql", host="h", options="not a dict")
