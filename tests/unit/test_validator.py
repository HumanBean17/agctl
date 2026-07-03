"""Unit tests for agctl.config.validator (DESIGN §3.5 dangling refs, §3.6 warnings)."""

from agctl.config.models import (
    Config,
    DatabaseConnection,
    DatabaseConfig,
    DatabaseTemplate,
    Defaults,
    HttpTemplate,
    KafkaConfig,
    KafkaPattern,
    ServiceConfig,
)
from agctl.config.validator import validate_config


def _cfg(**overrides) -> Config:
    base = dict(
        version="1",
        services={},
        kafka=KafkaConfig(),
        database=DatabaseConfig(),
        templates={},
        defaults=Defaults(),
    )
    base.update(overrides)
    return Config.model_validate(base)


# --- good config -----------------------------------------------------------


def test_good_config_no_errors():
    cfg = _cfg(
        services={"order-service": ServiceConfig(base_url="http://x")},
        templates={
            "create-order": HttpTemplate(
                method="POST",
                service="order-service",
                path="/orders",
                description="Create an order",
            )
        },
        database=DatabaseConfig(
            connections={"main-db": {"type": "postgresql"}},
            templates={
                "find-order": DatabaseTemplate(
                    connection="main-db",
                    sql="SELECT 1",
                    description="Find an order",
                )
            },
        ),
        kafka=KafkaConfig(
            patterns={
                "order-created": KafkaPattern(
                    topic="orders", description="order created"
                )
            }
        ),
        defaults=Defaults(database_connection="main-db"),
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


# --- dangling ref errors ---------------------------------------------------


def test_http_template_dangling_service_ref():
    cfg = _cfg(
        templates={
            "t1": HttpTemplate(method="GET", service="ghost", path="/x")
        }
    )
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "templates.t1.service"
    assert "ghost" in errors[0]["message"]
    # The error paths must not appear as errors elsewhere; the missing
    # description is reported as a warning, not an error.
    assert all(e["path"] != "templates.t1" for e in errors)


def test_db_template_dangling_connection_ref():
    cfg = _cfg(
        database=DatabaseConfig(
            templates={"q1": DatabaseTemplate(connection="ghost", sql="SELECT 1")}
        )
    )
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "database.templates.q1.connection"
    assert "ghost" in errors[0]["message"]


def test_default_connection_dangling_ref():
    cfg = _cfg(defaults=Defaults(database_connection="ghost"))
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "defaults.database_connection"


def test_db_template_connection_none_is_ok():
    cfg = _cfg(
        database=DatabaseConfig(
            templates={"q1": DatabaseTemplate(connection=None, sql="SELECT 1")}
        )
    )
    errors, warnings = validate_config(cfg)
    assert errors == []


# --- missing description warnings ------------------------------------------


def test_http_template_missing_description_warns():
    cfg = _cfg(
        templates={"t1": HttpTemplate(method="GET", service="s", path="/x")},
        services={"s": ServiceConfig(base_url="http://x")},
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert len(warnings) == 1
    assert warnings[0]["path"] == "templates.t1"
    assert "description" in warnings[0]["message"]


def test_db_template_missing_description_warns():
    cfg = _cfg(
        database=DatabaseConfig(
            templates={"q1": DatabaseTemplate(connection=None, sql="SELECT 1")}
        )
    )
    errors, warnings = validate_config(cfg)
    assert len(warnings) == 1
    assert warnings[0]["path"] == "database.templates.q1"


def test_kafka_pattern_missing_description_warns():
    cfg = _cfg(kafka=KafkaConfig(patterns={"p1": KafkaPattern(topic="t")}))
    errors, warnings = validate_config(cfg)
    assert len(warnings) == 1
    assert warnings[0]["path"] == "kafka.patterns.p1"


def test_multiple_missing_descriptions_multiple_warnings():
    cfg = _cfg(
        templates={
            "t1": HttpTemplate(method="GET", service="s", path="/x"),
            "t2": HttpTemplate(method="GET", service="s", path="/y"),
        },
        services={"s": ServiceConfig(base_url="http://x")},
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert len(warnings) == 2
    paths = {w["path"] for w in warnings}
    assert paths == {"templates.t1", "templates.t2"}


# --- composition: error + warning together --------------------------------


def test_both_error_and_warning():
    cfg = _cfg(
        templates={"t1": HttpTemplate(method="GET", service="ghost", path="/x")},
    )
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "templates.t1.service"
    assert len(warnings) == 1
    assert warnings[0]["path"] == "templates.t1"


# --- write template -> writable connection validation -----------------------


def test_write_template_with_writable_connection_passes():
    cfg = _cfg(
        database=DatabaseConfig(
            connections={
                "main-db": DatabaseConnection(type="postgresql", writable=True)
            },
            templates={
                "create-order": DatabaseTemplate(
                    connection="main-db",
                    sql="INSERT INTO orders",
                    mode="write",
                    description="Create an order",
                )
            },
        )
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert warnings == []


def test_write_template_with_non_writable_connection_errors():
    cfg = _cfg(
        database=DatabaseConfig(
            connections={
                "read-only-db": DatabaseConnection(type="postgresql", writable=False)
            },
            templates={
                "create-order": DatabaseTemplate(
                    connection="read-only-db",
                    sql="INSERT INTO orders",
                    mode="write",
                    description="Create an order",
                )
            },
        )
    )
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "database.templates.create-order"
    assert "writable" in errors[0]["message"] or "write target" in errors[0]["message"]
    assert warnings == []


def test_write_template_with_no_resolvable_connection_errors():
    cfg = _cfg(
        database=DatabaseConfig(
            connections={"main-db": DatabaseConnection(type="postgresql")},
            templates={
                "create-order": DatabaseTemplate(
                    connection=None,
                    sql="INSERT INTO orders",
                    mode="write",
                    description="Create an order",
                )
            },
        ),
        defaults=Defaults(database_connection=None),
    )
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "database.templates.create-order"
    assert warnings == []


def test_read_mode_template_with_read_only_connection_no_error():
    cfg = _cfg(
        database=DatabaseConfig(
            connections={
                "read-only-db": DatabaseConnection(type="postgresql", writable=False)
            },
            templates={
                "find-order": DatabaseTemplate(
                    connection="read-only-db", sql="SELECT * FROM orders"
                )
            },
        )
    )
    errors, warnings = validate_config(cfg)
    # read-mode templates should not trigger the writable connection rule
    assert not any(
        e["path"] == "database.templates.find-order"
        and ("writable" in e["message"] or "write target" in e["message"])
        for e in errors
    )
