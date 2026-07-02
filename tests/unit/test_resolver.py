from agctl.config.resolver import apply_env_overrides


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
