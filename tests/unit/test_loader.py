from pathlib import Path

import pytest

from agctl.config import ConfigError, load_config

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"


def _env(**extra):
    env = {
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "PAYMENT_SERVICE_URL": "http://localhost:8082",
        "PAYMENT_SERVICE_TOKEN": "tok",
        "KAFKA_BROKER": "localhost",
        "DB_HOST": "h",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
        "ANALYTICS_DB_HOST": "ah",
        "ANALYTICS_DB_USER": "au",
        "ANALYTICS_DB_PASSWORD": "ap",
    }
    env.update(extra)
    return env


def test_load_full_config_keeps_connections_and_templates():
    """§5 verification test / D1 regression: both sections survive loading."""
    cfg = load_config(str(FIXTURE), env=_env())
    assert cfg.database.connections["main-db"].host == "h"
    assert cfg.database.templates["find-order"].connection == "main-db"
    assert cfg.services["order-service"].base_url == "http://localhost:8081"
    assert cfg.kafka.schema_registry_url == ""  # ${VAR:-} resolved to empty


def test_missing_required_env_raises(tmp_path):
    with pytest.raises(ConfigError) as exc:
        load_config(str(FIXTURE), env={})  # ORDER_SERVICE_URL etc. missing
    assert "DB_HOST" in exc.value.detail["variables"]


def test_agctl_override_applied(tmp_path):
    cfg = load_config(str(FIXTURE), env=_env(AGCTL_DEFAULTS__TIMEOUT_SECONDS="99"))
    assert cfg.defaults.timeout_seconds == 99


def test_version_mismatch_raises(tmp_path):
    bad = tmp_path / "agctl.yaml"
    bad.write_text('version: "2"\n')
    with pytest.raises(ConfigError) as exc:
        load_config(str(bad), env={})
    assert exc.value.detail["tool_major"] == "1"


def test_invalid_schema_raises(tmp_path):
    bad = tmp_path / "agctl.yaml"
    bad.write_text('version: "1"\ntemplates:\n  x:\n    method: GET\n')  # missing service/path
    with pytest.raises(ConfigError):
        load_config(str(bad), env={})


def test_kafka_ssl_loaded_from_config(tmp_path):
    """kafka.ssl sub-block loads into the typed KafkaSSL model (DESIGN §2.1)."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(
        'version: "1"\n'
        "kafka:\n"
        "  brokers: [host:9092]\n"
        "  ssl:\n"
        "    ca_location: /etc/ssl/kafka/ca.pem\n"
        "    certificate_location: /etc/ssl/kafka/client.crt\n"
        "    key_location: /etc/ssl/kafka/client.key\n"
        "    key_password: hunter2\n"
        "    endpoint_identification_algorithm: none\n"
    )
    cfg = load_config(str(cfg_file), env={})

    assert cfg.kafka.ssl is not None
    assert cfg.kafka.ssl.ca_location == "/etc/ssl/kafka/ca.pem"
    assert cfg.kafka.ssl.certificate_location == "/etc/ssl/kafka/client.crt"
    assert cfg.kafka.ssl.key_location == "/etc/ssl/kafka/client.key"
    assert cfg.kafka.ssl.key_password == "hunter2"
    assert cfg.kafka.ssl.endpoint_identification_algorithm == "none"
    # security_protocol is optional and defaults to None (inferred at use time).
    assert cfg.kafka.ssl.security_protocol is None


def test_kafka_ssl_env_override(tmp_path):
    """AGCTL_KAFKA__SSL__* overrides reach the typed model for free (§8)."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(
        'version: "1"\n'
        "kafka:\n"
        "  brokers: [host:9092]\n"
    )
    cfg = load_config(
        str(cfg_file),
        env={"AGCTL_KAFKA__SSL__CA_LOCATION": "/override/ca.pem"},
    )

    assert cfg.kafka.ssl is not None
    assert cfg.kafka.ssl.ca_location == "/override/ca.pem"


def test_kafka_ssl_path_interpolated(tmp_path):
    """${VAR} interpolation resolves into nested kafka.ssl fields (the documented
    primary TLS config mechanism); ${VAR:-} resolves to empty (== unset)."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(
        'version: "1"\n'
        "kafka:\n"
        "  brokers: [host:9092]\n"
        "  ssl:\n"
        '    ca_location: "${KAFKA_SSL_CA}"\n'        # bare var, required
        '    key_password: "${KAFKA_SSL_KEY_PASSWORD:-}"\n'  # default-to-empty
    )
    cfg = load_config(str(cfg_file), env={"KAFKA_SSL_CA": "/from/env/ca.pem"})

    assert cfg.kafka.ssl.ca_location == "/from/env/ca.pem"
    assert cfg.kafka.ssl.key_password == ""  # ${VAR:-} resolved to empty


def test_kafka_ssl_invalid_security_protocol_raises(tmp_path):
    """A typo'd security.protocol fails fast at config load (DESIGN §3.5),
    not as an opaque connect-time error."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(
        'version: "1"\n'
        "kafka:\n"
        "  brokers: [host:9092]\n"
        "  ssl:\n"
        "    security_protocol: SSLT\n"  # typo
    )
    with pytest.raises(ConfigError):
        load_config(str(cfg_file), env={})


def test_kafka_ssl_security_protocol_normalized(tmp_path):
    """security.protocol is normalized to librdkafka's uppercase form."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(
        'version: "1"\n'
        "kafka:\n"
        "  brokers: [host:9092]\n"
        "  ssl:\n"
        "    ca_location: /ca.pem\n"
        "    security_protocol: ssl\n"
    )
    cfg = load_config(str(cfg_file), env={})
    assert cfg.kafka.ssl.security_protocol == "SSL"
