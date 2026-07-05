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


class TestNestedInterpolation:
    """Nested-default and chained-ref interpolation (DESIGN §2.1).

    These cover the recursive brace-counting parser that replaced the legacy
    single-pass regex (which broke on `${A:-${B}}` because `[^}]*` couldn't
    span the inner `}`).
    """

    def test_nested_default_outer_unset_inner_set(self):
        # DESIGN §2.1 example — the regression-of-record.
        assert interpolate("${DB_WRITE_USER:-${DB_USER}}", {"DB_USER": "alice"}) == "alice"

    def test_nested_default_outer_set_wins(self):
        assert (
            interpolate(
                "${DB_WRITE_USER:-${DB_USER}}",
                {"DB_WRITE_USER": "bob", "DB_USER": "alice"},
            )
            == "bob"
        )

    def test_deeply_nested_innermost_only(self):
        assert interpolate("${A:-${B:-${C}}}", {"C": "x"}) == "x"

    def test_deeply_nested_middle_wins(self):
        assert interpolate("${A:-${B:-${C}}}", {"B": "y", "C": "x"}) == "y"

    def test_deeply_nested_outer_wins(self):
        assert interpolate("${A:-${B:-${C}}}", {"A": "z", "B": "y", "C": "x"}) == "z"

    def test_chained_value_ref(self):
        # A var whose VALUE itself contains ${...} resolves through the
        # iterative pass in _interpolate_str.
        assert interpolate("${A}", {"A": "${B}", "B": "final"}) == "final"

    def test_self_reference_no_infinite_loop(self):
        # ${A:-${A}} with A unset: the inner ${A} is unresolved, so the whole
        # expression must raise ConfigError (not hang). The _MAX_INTERP_PASSES
        # cap guarantees termination.
        with pytest.raises(ConfigError) as exc:
            interpolate("${A:-${A}}", {})
        assert "A" in exc.value.detail["variables"]

    def test_literal_default_with_special_chars(self):
        assert interpolate("${MISSING:-some-default}", {}) == "some-default"

    def test_literal_default_empty(self):
        assert interpolate("${MISSING:-}", {}) == ""

    def test_nested_with_prefix_suffix(self):
        assert interpolate("pre-${A:-${B}}-post", {"B": "mid"}) == "pre-mid-post"
