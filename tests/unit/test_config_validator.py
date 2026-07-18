"""Tests for config validator (DESIGN §3.5 dangling refs, §3.6 warnings)."""

import pytest

from agctl.config.models import Config
from agctl.config.validator import validate_config


def test_grpc_template_unknown_target_is_error():
    """A gRPC template referencing a missing target produces an error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"existing-target": {"address": "localhost:50051"}},
                "templates": {
                    "t1": {
                        "target": "missing-target",
                        "service": "s.Svc",
                        "method": "M",
                        "description": "A template",
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "grpc.templates.t1.target",
            "message": "gRPC template references unknown target 'missing-target'",
        }
    ]
    # No template-level warning for this config (description not involved)
    assert warnings == []


def test_grpc_template_known_target_no_error():
    """A gRPC template referencing an existing target produces no error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"existing-target": {"address": "localhost:50051"}},
                "templates": {
                    "t1": {
                        "target": "existing-target",
                        "service": "s.Svc",
                        "method": "M",
                        "description": "A template",
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


def test_grpc_descriptor_both_set_is_error():
    """A grpc.descriptors entry with both proto and descriptor_set set is an error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "descriptors": [
                    {"proto": "a.proto", "descriptor_set": "b.pb"}
                ],
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "grpc.descriptors[0]",
            "message": "each grpc.descriptors entry must set exactly one of 'proto' or 'descriptor_set'",
        }
    ]
    assert warnings == []


def test_grpc_descriptor_neither_set_is_error():
    """A grpc.descriptors entry with neither proto nor descriptor_set set is an error."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "descriptors": [{}],
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "grpc.descriptors[0]",
            "message": "each grpc.descriptors entry must set exactly one of 'proto' or 'descriptor_set'",
        }
    ]
    assert warnings == []


def test_grpc_descriptor_exactly_one_ok():
    """A grpc.descriptors entry with exactly one of proto or descriptor_set is valid."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "descriptors": [
                    {"proto": "a.proto", "include_paths": ["."]},
                    {"descriptor_set": "b.pb"},
                ],
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


def test_grpc_template_missing_description_warns():
    """A gRPC template with no description produces a warning."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"t1": {"address": "localhost:50051"}},
                "templates": {
                    "tmpl1": {
                        "target": "t1",
                        "service": "s.Svc",
                        "method": "M",
                        "description": None,
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == [
        {
            "path": "grpc.templates.tmpl1",
            "message": "missing description (discovery degrades without it)",
        }
    ]


def test_grpc_template_with_description_no_warning():
    """A gRPC template with a description produces no warning."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "grpc": {
                "targets": {"t1": {"address": "localhost:50051"}},
                "templates": {
                    "tmpl1": {
                        "target": "t1",
                        "service": "s.Svc",
                        "method": "M",
                        "description": "Call the method",
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


# --------------------------------------------------------------------------- #
# Task 2: kafka.topics cluster cross-refs + schema_registry auth shape
# (DESIGN §6.1 / §6.2). Cluster resolution mirrors resolve_cluster_name
# (topic.cluster -> default_cluster -> single-cluster auto-default) but is
# inlined here so config/ stays free of a commands/ import.
# --------------------------------------------------------------------------- #


def test_kafka_topic_unknown_cluster_is_error():
    """A kafka.topics entry naming an absent cluster is an error (case a)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {"real": {"brokers": ["localhost:9092"]}},
                "topics": {"t1": {"cluster": "nope"}},
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "kafka.topics.t1.cluster",
            "message": "Topic references unknown cluster 'nope'",
        }
    ]
    assert warnings == []


def test_kafka_topic_avro_without_schema_registry_url_is_error():
    """A topic resolving to avro on a cluster with no schema_registry_url (case b)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {"c1": {"brokers": ["localhost:9092"]}},
                "topics": {"t1": {"value_format": "avro"}},
            },
        }
    )
    errors, warnings = validate_config(cfg)
    # avro came from the topic override -> path is the topic, not the cluster.
    assert errors == [
        {
            "path": "kafka.topics.t1",
            "message": (
                "Topic 't1' format (value=avro) requires a schema registry "
                "but cluster 'c1' has no schema_registry_url"
            ),
        }
    ]
    assert warnings == []


def test_kafka_topic_avro_with_schema_registry_url_no_error():
    """Same as above but the cluster carries a schema_registry_url (case c)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {
                    "c1": {
                        "brokers": ["localhost:9092"],
                        "schema_registry_url": "http://localhost:8081",
                    }
                },
                "topics": {"t1": {"value_format": "avro"}},
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


def test_kafka_schema_registry_basic_without_basic_auth_is_error():
    """schema_registry.auth=basic with no basic_auth block is an error (case d)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {
                    "c1": {
                        "brokers": ["localhost:9092"],
                        "schema_registry": {"auth": "basic"},
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "kafka.clusters.c1.schema_registry.auth",
            "message": "auth 'basic' requires basic_auth on cluster 'c1'",
        }
    ]
    assert warnings == []


def test_kafka_schema_registry_mtls_without_ssl_is_error():
    """schema_registry.auth=mtls with no ssl block is an error (case e)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {
                    "c1": {
                        "brokers": ["localhost:9092"],
                        "schema_registry": {"auth": "mtls"},
                    }
                },
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == [
        {
            "path": "kafka.clusters.c1.schema_registry.auth",
            "message": "auth 'mtls' requires ssl on cluster 'c1'",
        }
    ]
    assert warnings == []


def test_kafka_topic_subject_strategy_with_json_is_warning():
    """subject_strategy on a topic whose resolved value_format is json (case f)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {"c1": {"brokers": ["localhost:9092"]}},
                "topics": {"t1": {"subject_strategy": "record"}},
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == [
        {
            "path": "kafka.topics.t1.subject_strategy",
            "message": (
                "subject_strategy 'record' has no effect on topic 't1' with "
                "resolved value_format 'json'"
            ),
        }
    ]


def test_kafka_no_topics_no_schema_registry_baseline_clean():
    """Baseline: no topics, no SR block -> zero errors/warnings (case g)."""
    cfg = Config.model_validate(
        {
            "version": "2",
            "kafka": {
                "clusters": {"c1": {"brokers": ["localhost:9092"]}},
            },
        }
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []
