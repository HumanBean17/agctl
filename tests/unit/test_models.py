import pytest
from pydantic import ValidationError

from agctl.config.models import (
    Config,
    DatabaseConnection,
    DatabaseTemplate,
    LogSource,
    LogsConfig,
    LogsDefaults,
)


def _full_config_dict():
    return {
        "version": "1",
        "services": {
            "order-service": {"base_url": "http://localhost:8081", "health_path": "/health"}
        },
        "kafka": {
            "brokers": ["localhost:9092"],
            "default_consumer_group": "agctl-consumer",
            "patterns": {
                "order-created": {
                    "topic": "orders.created",
                    "match": '.eventType == "ORDER_CREATED"',
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
    cfg = Config.model_validate({"version": "1"})
    assert cfg.services == {}
    assert cfg.database.connections == {}
    assert cfg.database.templates == {}
    assert cfg.kafka.brokers == []


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
