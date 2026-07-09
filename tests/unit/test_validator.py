"""Unit tests for agctl.config.validator (DESIGN §3.5 dangling refs, §3.6 warnings)."""

from agctl.config.models import (
    Config,
    DatabaseConnection,
    DatabaseConfig,
    DatabaseTemplate,
    Defaults,
    HttpMatch,
    HttpMockConfig,
    HttpResponse,
    HttpStub,
    HttpTemplate,
    KafkaCluster,
    KafkaConfig,
    KafkaMockConfig,
    KafkaPattern,
    KafkaReaction,
    KafkaReactor,
    MocksConfig,
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


# --- mock server validation ---------------------------------------------------


def test_mock_kafka_requires_resolvable_default_cluster_error():
    """mocks.kafka.reactors non-empty but no resolvable cluster -> error at
    mocks.kafka (no default/single cluster resolves)."""
    cfg = _cfg(
        mocks=MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "r": KafkaReactor(
                        topic="test-topic",
                        reaction=KafkaReaction(
                            topic="test-topic", value='{"status": "ok"}'
                        ),
                    )
                }
            )
        ),
        kafka=KafkaConfig(),  # no clusters at all
    )
    errors, warnings = validate_config(cfg)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.kafka.reactors.r"
    assert "cluster" in errors[0]["message"]


def test_mock_kafka_with_cluster_no_error():
    """mocks.kafka.reactors with a single resolvable cluster -> no error."""
    cfg = _cfg(
        mocks=MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "r": KafkaReactor(
                        topic="test-topic",
                        reaction=KafkaReaction(
                            topic="test-topic", value='{"status": "ok"}'
                        ),
                    )
                }
            )
        ),
        kafka=KafkaConfig(
            clusters={"default": KafkaCluster(brokers=["localhost:9092"])}
        ),
    )
    errors, warnings = validate_config(cfg)
    # Should not have the mocks.kafka.reactors error
    assert not any(e["path"].startswith("mocks.kafka.reactors") for e in errors)


# --- v3 cluster cross-ref validation ----------------------------------------


def test_reactor_default_cluster_missing_brokers_errors():
    """A reactor whose resolved default cluster exists but has empty brokers ->
    error at mocks.kafka naming the cluster."""
    cfg = _cfg(
        mocks=MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "r": KafkaReactor(
                        topic="t",
                        reaction=KafkaReaction(topic="out", value={}),
                    )
                }
            )
        ),
        kafka=KafkaConfig(
            clusters={"default": KafkaCluster(brokers=[])},
            default_cluster="default",
        ),
    )
    errors, warnings = validate_config(cfg)
    mocks_errors = [e for e in errors if e["path"] == "mocks.kafka.reactors.r"]
    assert len(mocks_errors) == 1
    assert "kafka.clusters.default.brokers" in mocks_errors[0]["message"]


def test_reactor_cluster_dangling_ref_errors():
    """A KafkaReactor whose ``cluster`` names an unknown cluster -> error at
    mocks.kafka.reactors.<name>.cluster."""
    cfg = _cfg(
        mocks=MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "r": KafkaReactor(
                        topic="t",
                        cluster="ghost",
                        reaction=KafkaReaction(topic="out", value={}),
                    )
                }
            )
        ),
        kafka=KafkaConfig(
            clusters={"main": KafkaCluster(brokers=["h:9092"])},
            default_cluster="main",
        ),
    )
    errors, warnings = validate_config(cfg)
    paths = [e["path"] for e in errors]
    assert "mocks.kafka.reactors.r.cluster" in paths


def test_reactor_resolved_cluster_missing_brokers_errors():
    """A reactor binding ``cluster="main"`` where main.brokers is empty -> error
    at mocks.kafka.reactors.<name>, message names the cluster."""
    cfg = _cfg(
        mocks=MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "r": KafkaReactor(
                        topic="t",
                        cluster="main",
                        reaction=KafkaReaction(topic="out", value={}),
                    )
                }
            )
        ),
        kafka=KafkaConfig(
            clusters={"main": KafkaCluster(brokers=[])},
            default_cluster="main",
        ),
    )
    errors, warnings = validate_config(cfg)
    reactor_errors = [e for e in errors if e["path"] == "mocks.kafka.reactors.r"]
    assert len(reactor_errors) == 1
    assert "kafka.clusters.main.brokers" in reactor_errors[0]["message"]


def test_default_cluster_dangling_ref_errors():
    """cfg.kafka.default_cluster names a cluster absent from clusters -> error
    at kafka.default_cluster."""
    cfg = _cfg(
        kafka=KafkaConfig(clusters={}, default_cluster="ghost"),
    )
    errors, warnings = validate_config(cfg)
    paths = [e["path"] for e in errors]
    assert "kafka.default_cluster" in paths


def test_pattern_cluster_dangling_ref_errors():
    """A KafkaPattern whose cluster names an unknown cluster -> error at
    kafka.patterns.<name>.cluster."""
    cfg = _cfg(
        kafka=KafkaConfig(
            clusters={"main": KafkaCluster(brokers=["h:9092"])},
            patterns={"p": KafkaPattern(topic="t", cluster="ghost")},
        ),
    )
    errors, warnings = validate_config(cfg)
    paths = [e["path"] for e in errors]
    assert "kafka.patterns.p.cluster" in paths


def test_mock_http_stub_missing_description_warns():
    """HTTP stub without description → warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "get-order": HttpStub(
                        method="GET",
                        path="/orders/{order_id}",
                        response=HttpResponse(body={"id": 1}),
                    )
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert len(warnings) == 1
    assert warnings[0]["path"] == "mocks.http.stubs.get-order"
    assert warnings[0]["message"] == "missing description (discovery degrades without it)"


def test_mock_kafka_reactor_missing_description_warns():
    """Kafka reactor without description → warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "r": KafkaReactor(
                        topic="test-topic",
                        reaction=KafkaReaction(
                            topic="test-topic", value='{"status": "ok"}'
                        ),
                    )
                }
            )
        ),
        kafka=KafkaConfig(
            clusters={"default": KafkaCluster(brokers=["localhost:9092"])}
        ),
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    assert len(warnings) == 1
    assert warnings[0]["path"] == "mocks.kafka.reactors.r"
    assert warnings[0]["message"] == "missing description (discovery degrades without it)"


def test_mock_path_shadowing_warning():
    """Stub /orders/{order_id} shadows /orders/bulk → warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "order-by-id": HttpStub(
                        method="GET",
                        path="/orders/{order_id}",
                        response=HttpResponse(body={"id": 1}),
                    ),
                    "bulk-orders": HttpStub(
                        method="GET",
                        path="/orders/bulk",
                        response=HttpResponse(body={"orders": []}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    assert errors == []
    shadowing_warnings = [w for w in warnings if "shadow" in w["message"].lower()]
    assert len(shadowing_warnings) == 1
    assert shadowing_warnings[0]["path"] == "mocks.http.stubs.bulk-orders"
    assert "order-by-id" in shadowing_warnings[0]["message"]
    assert "bulk-orders" in shadowing_warnings[0]["message"]


def test_mock_no_shadowing_different_paths():
    """Stubs /a/{x} and /b/{y} → no shadowing warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "a-stub": HttpStub(
                        method="GET", path="/a/{x}", response=HttpResponse(body={})
                    ),
                    "b-stub": HttpStub(
                        method="GET", path="/b/{y}", response=HttpResponse(body={})
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    shadowing_warnings = [w for w in warnings if "shadow" in w["message"].lower()]
    assert len(shadowing_warnings) == 0


def test_mock_no_shadowing_literal_first():
    """Stub /orders/bulk before /orders/{order_id} → no warning (literal first wins)."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "bulk-orders": HttpStub(
                        method="GET",
                        path="/orders/bulk",
                        response=HttpResponse(body={"orders": []}),
                    ),
                    "order-by-id": HttpStub(
                        method="GET",
                        path="/orders/{order_id}",
                        response=HttpResponse(body={"id": 1}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    shadowing_warnings = [w for w in warnings if "shadow" in w["message"].lower()]
    assert len(shadowing_warnings) == 0


# --- Check 4: jq-shadowing warning (method-gated) ---------------------------


def _jq_shadowing_warnings(warnings: list[dict]) -> list[dict]:
    """Filter to Check 4 jq-shadowing warnings (distinguished from Check 3)."""
    return [w for w in warnings if "match.jq" in w["message"]]


def test_jq_shadowing_two_stubs_same_method_path_both_jq_warns():
    """(a) Two POST stubs same path, BOTH with match.jq → one warning on later stub."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "post1": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(jq=".status == 1"),
                        response=HttpResponse(body={}),
                    ),
                    "post2": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(jq=".status == 2"),
                        response=HttpResponse(body={}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    jq_warnings = _jq_shadowing_warnings(warnings)
    assert len(jq_warnings) == 1
    assert jq_warnings[0]["path"] == "mocks.http.stubs.post2"
    assert "post2" in jq_warnings[0]["message"]
    assert "post1" in jq_warnings[0]["message"]


def test_jq_shadowing_different_methods_no_warning():
    """(b) Same path, different methods (POST vs DELETE), both jq → no warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "post1": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(jq=".status == 1"),
                        response=HttpResponse(body={}),
                    ),
                    "del1": HttpStub(
                        method="DELETE",
                        path="/orders",
                        match=HttpMatch(jq=".status == 2"),
                        response=HttpResponse(body={}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    assert _jq_shadowing_warnings(warnings) == []


def test_jq_shadowing_jq_vs_body_no_warning():
    """(c) Same method+path, one match.jq + one match.body (no jq) → no warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "jq1": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(jq=".status == 1"),
                        response=HttpResponse(body={}),
                    ),
                    "body1": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(body={"x": 1}),
                        response=HttpResponse(body={}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    assert _jq_shadowing_warnings(warnings) == []


def test_jq_shadowing_different_paths_no_warning():
    """(d) Same method, different paths, both jq → no warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "jq1": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(jq=".status == 1"),
                        response=HttpResponse(body={}),
                    ),
                    "jq2": HttpStub(
                        method="POST",
                        path="/users",
                        match=HttpMatch(jq=".status == 2"),
                        response=HttpResponse(body={}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    assert _jq_shadowing_warnings(warnings) == []


def test_jq_shadowing_single_stub_no_warning():
    """(e) A single stub with match.jq → no warning."""
    cfg = _cfg(
        mocks=MocksConfig(
            http=HttpMockConfig(
                stubs={
                    "only1": HttpStub(
                        method="POST",
                        path="/orders",
                        match=HttpMatch(jq=".status == 1"),
                        response=HttpResponse(body={}),
                    ),
                }
            )
        )
    )
    errors, warnings = validate_config(cfg)
    assert _jq_shadowing_warnings(warnings) == []
