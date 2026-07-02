import pytest
from pydantic import ValidationError

from agctl.config.models import Config


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
