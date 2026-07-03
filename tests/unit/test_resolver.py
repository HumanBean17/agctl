import pytest
from agctl.config.resolver import apply_env_overrides
from agctl.errors import ConfigError


def test_sets_leaf_value():
    out = apply_env_overrides({"defaults": {"timeout_seconds": 10}}, {"AGCTL_DEFAULTS__TIMEOUT_SECONDS": "30"})
    assert out["defaults"]["timeout_seconds"] == "30"


def test_sets_nested_creating_intermediate_dicts():
    out = apply_env_overrides({}, {"AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP": "ci"})
    assert out["kafka"]["default_consumer_group"] == "ci"


def test_ignores_non_agctl_env():
    out = apply_env_overrides({"x": 1}, {"PATH": "/bin", "AGCTL_CONFIG": "/tmp/x"})
    assert out == {"x": 1}


def test_value_always_string():
    out = apply_env_overrides({}, {"AGCTL_DEFAULTS__TIMEOUT_SECONDS": "30"})
    assert out["defaults"]["timeout_seconds"] == "30"
    assert isinstance(out["defaults"]["timeout_seconds"], str)


def test_does_not_mutate_input():
    src = {"defaults": {"timeout_seconds": 10}}
    apply_env_overrides(src, {"AGCTL_DEFAULTS__TIMEOUT_SECONDS": "30"})
    assert src["defaults"]["timeout_seconds"] == 10


# --- DESIGN §5/§8: overrides must match hyphenated/cased existing keys -------


def test_override_matches_hyphenated_service_key():
    """AGCTL_SERVICES__ORDER_SERVICE__BASE_URL overrides the real `order-service`
    key (DESIGN §8: hyphens become underscores), not a phantom `order_service`
    sibling."""
    src = {"services": {"order-service": {"base_url": "http://orig:8080"}}}
    out = apply_env_overrides(
        src, {"AGCTL_SERVICES__ORDER_SERVICE__BASE_URL": "http://override:9090"}
    )
    assert out["services"]["order-service"]["base_url"] == "http://override:9090"
    # No phantom sibling created — the real key was the one updated.
    assert "order_service" not in out["services"]
    assert list(out["services"]) == ["order-service"]


def test_override_matches_hyphenated_nested_connection_key():
    """AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD reaches the `main-db`
    connection (the spec's flagship example)."""
    src = {"database": {"connections": {"main-db": {"password": "old"}}}}
    out = apply_env_overrides(
        src, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD": "s3cr3t"}
    )
    assert out["database"]["connections"]["main-db"]["password"] == "s3cr3t"
    assert "main_db" not in out["database"]["connections"]


def test_override_matches_case_insensitively():
    src = {"Defaults": {"TimeoutSeconds": 5}}
    out = apply_env_overrides(src, {"AGCTL_DEFAULTS__TIMEOUTSECONDS": "9"})
    assert out["Defaults"]["TimeoutSeconds"] == "9"


def test_override_through_existing_scalar_replaces_with_dict():
    """A path that routes through an existing scalar leaf replaces it with a
    dict so the nested override can still be recorded (overrides win, §5)."""
    src = {"kafka": "scalar-blocking"}
    out = apply_env_overrides(src, {"AGCTL_KAFKA__BROKERS": "host:9092"})
    assert out["kafka"] == {"brokers": "host:9092"}


# --- Task 2: Denylist for writable/mode leaves ---------------------------------


def test_writable_leaf_raises_config_error():
    """Override targeting a leaf named 'writable' raises ConfigError."""
    src = {"database": {"connections": {"main-db": {"writable": False}}}}
    with pytest.raises(ConfigError) as exc_info:
        apply_env_overrides(src, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__WRITABLE": "true"})
    assert "writable" in str(exc_info.value.message).lower()


def test_mode_leaf_raises_config_error():
    """Override targeting a leaf named 'mode' raises ConfigError."""
    src = {"database": {"templates": {"t": {"mode": "read"}}}}
    with pytest.raises(ConfigError) as exc_info:
        apply_env_overrides(src, {"AGCTL_DATABASE__TEMPLATES__T__MODE": "write"})
    assert "mode" in str(exc_info.value.message).lower()


def test_non_denylisted_override_applies():
    """Positive control: non-denylisted override on same connection applies."""
    src = {"database": {"connections": {"main-db": {"writable": False, "password": "old"}}}}
    out = apply_env_overrides(src, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD": "new"})
    assert out["database"]["connections"]["main-db"]["password"] == "new"
    # writable should remain unchanged since we can't override it
    assert out["database"]["connections"]["main-db"]["writable"] is False


def test_denylist_case_insensitive():
    """Denylist matches leaf names case-insensitively."""
    src = {"database": {"connections": {"main-db": {"writable": False}}}}
    # All these should raise regardless of case
    with pytest.raises(ConfigError):
        apply_env_overrides(src, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__WRITABLE": "true"})
    with pytest.raises(ConfigError):
        apply_env_overrides(src, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__writable": "true"})
    with pytest.raises(ConfigError):
        apply_env_overrides(src, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__WriTaBlE": "true"})


def test_non_leaf_writable_allowed():
    """When 'writable' is an intermediate segment (not the leaf), override is allowed."""
    src = {"x": {"writable": {"foo": 1}}}
    # The leaf is 'foo', not 'writable', so this should NOT raise
    out = apply_env_overrides(src, {"AGCTL_X__WRITABLE__FOO": "2"})
    assert out["x"]["writable"]["foo"] == "2"
