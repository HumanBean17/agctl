import pytest

from agctl.config.loader import ConfigError, interpolate


def test_required_var_resolved():
    assert interpolate("${VAR}", {"VAR": "hello"}) == "hello"


def test_partial_substitution():
    assert interpolate("http://${HOST}:8080", {"HOST": "db"}) == "http://db:8080"


def test_required_var_missing_raises():
    with pytest.raises(ConfigError) as exc:
        interpolate("${MISSING}", {})
    assert "MISSING" in exc.value.detail["variables"]


def test_lists_all_unresolved():
    with pytest.raises(ConfigError) as exc:
        interpolate({"a": "${A}", "b": "${B}"}, {})
    assert set(exc.value.detail["variables"]) == {"A", "B"}


def test_optional_with_default():
    assert interpolate("${VAR:-fallback}", {}) == "fallback"


def test_optional_empty():
    assert interpolate("${VAR:-}", {}) == ""


def test_optional_ignored_when_set():
    assert interpolate("${VAR:-fallback}", {"VAR": "real"}) == "real"


def test_interpolates_nested_structures():
    data = {"kafka": {"brokers": ["${BROKER}:9092"]}, "n": 5}
    out = interpolate(data, {"BROKER": "kafka"})
    assert out == {"kafka": {"brokers": ["kafka:9092"]}, "n": 5}
