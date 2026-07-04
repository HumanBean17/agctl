import datetime
import uuid
from decimal import Decimal

import pytest

from agctl.assertions import (
    coerce_db_value,
    compile_jq,
    jq_bool,
    jq_value,
    json_subset,
    parse_equals,
    type_aware_equal,
)


# --- jq_bool ---------------------------------------------------------------
def test_jq_bool_true():
    assert jq_bool({"a": 1}, ".a==1") is True


def test_jq_bool_false_predicate():
    assert jq_bool({"a": 2}, ".a==1") is False


def test_jq_bool_missing_path_no_raise():
    # missing path -> jq yields False; falsy -> False
    assert jq_bool({"a": 1}, ".b==1") is False


def test_jq_bool_bad_expr_no_raise():
    # compile/runtime error -> False, never raises
    assert jq_bool({}, ")(") is False


def test_jq_bool_truthy_from_list_iteration():
    assert jq_bool([{"x": 1}, {"x": 2}], ".[].x==2") is True


# --- jq_value --------------------------------------------------------------
def test_jq_value_simple():
    assert jq_value({"status": "OK"}, ".status") == "OK"


def test_jq_value_nested_path():
    assert jq_value({"a": {"b": 2}}, ".a.b") == 2


def test_jq_value_missing_path():
    assert jq_value({}, ".missing") is None


def test_jq_value_bad_expr():
    assert jq_value({}, ")(") is None


# --- compile_jq -------------------------------------------------------------
def test_compile_jq_valid_returns_none():
    # valid expression compiles without applying it; returns None
    assert compile_jq(".a == 1") is None


def test_compile_jq_syntax_error_raises_config_error():
    # malformed expression -> ConfigError (loud), not silently swallowed
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError):
        compile_jq(")(")


def test_compile_jq_truncated_expr_raises_config_error():
    # truncated expression (the case jq_bool silently swallows) -> ConfigError
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError):
        compile_jq(".amount >")


def test_compile_jq_message_includes_label():
    # the raised ConfigError.message includes the label when one is passed
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError) as exc_info:
        compile_jq(".amount >", label="order.amount match")
    assert "order.amount match" in exc_info.value.message


def test_compile_jq_contrast_jq_bool_swallows():
    # contrast: jq_bool wraps compile+eval in except Exception -> False,
    # so the SAME bad expression that compile_jq raises on yields False here.
    assert jq_bool({}, ")(") is False


def test_compile_jq_missing_jq_message_names_agctl_jq_extra(monkeypatch):
    # missing jq library -> ConfigError pointing at pip install 'agctl[jq]'
    # (the base _jq() message names only db/kafka; compile_jq MUST replace it).
    import sys
    from agctl.errors import ConfigError

    monkeypatch.setitem(sys.modules, "jq", None)  # block the lazy import
    from agctl import assertions

    with pytest.raises(ConfigError) as exc_info:
        assertions.compile_jq(".a")
    assert "agctl[jq]" in exc_info.value.message


# --- jq lazy import (Fix A) ------------------------------------------------
def test_jq_missing_raises_config_error(monkeypatch):
    """When the optional `jq` library is unavailable, jq_bool/jq_value surface a
    ConfigError (exit 2) rather than crashing at import time."""
    import sys
    from agctl.errors import ConfigError

    monkeypatch.setitem(sys.modules, "jq", None)  # block the lazy `import jq`
    from agctl import assertions

    with pytest.raises(ConfigError):
        assertions.jq_bool({"a": 1}, ".a==1")
    with pytest.raises(ConfigError):
        assertions.jq_value({"a": 1}, ".a")


# --- json_subset -----------------------------------------------------------
def test_json_subset_dict_extra_keys_ok():
    assert json_subset({"x": 1}, {"x": 1, "y": 2}) is True


def test_json_subset_dict_value_mismatch():
    assert json_subset({"x": 2}, {"x": 1}) is False


def test_json_subset_nested_dict_true():
    assert json_subset({"o": {"a": 1}}, {"o": {"a": 1, "b": 2}}) is True


def test_json_subset_nested_dict_false():
    assert json_subset({"o": {"a": 2}}, {"o": {"a": 1}}) is False


def test_json_subset_list_order_independent():
    assert json_subset([{"k": 1}], [{"k": 1}, {"k": 2}]) is True


def test_json_subset_list_no_match():
    assert json_subset([{"k": 9}], [{"k": 1}]) is False


def test_json_subset_needle_smaller_list():
    assert json_subset([1, 2], [1, 2, 3]) is True


def test_json_subset_scalar_equal():
    assert json_subset(1, 1) is True


def test_json_subset_scalar_unequal():
    assert json_subset(1, 2) is False


# --- parse_equals ----------------------------------------------------------
def test_parse_equals_int():
    assert parse_equals("0") == 0
    assert isinstance(parse_equals("0"), int)


def test_parse_equals_true():
    assert parse_equals("true") is True


def test_parse_equals_list():
    assert parse_equals("[1,2]") == [1, 2]


def test_parse_equals_null():
    assert parse_equals("null") is None


def test_parse_equals_bare_word_string():
    assert parse_equals("CONFIRMED") == "CONFIRMED"


def test_parse_equals_float():
    assert parse_equals("3.14") == 3.14


# --- coerce_db_value -------------------------------------------------------
def test_coerce_decimal_integral():
    assert coerce_db_value(Decimal("5")) == 5
    assert isinstance(coerce_db_value(Decimal("5")), int)


def test_coerce_decimal_fractional():
    assert coerce_db_value(Decimal("1.5")) == 1.5
    assert isinstance(coerce_db_value(Decimal("1.5")), float)


def test_coerce_datetime():
    assert coerce_db_value(datetime.datetime(2026, 6, 29, 14, 22, 0)) == (
        "2026-06-29T14:22:00"
    )


def test_coerce_date():
    assert coerce_db_value(datetime.date(2026, 6, 29)) == "2026-06-29"


def test_coerce_uuid():
    val = coerce_db_value(uuid.UUID(int=1))
    assert isinstance(val, str)
    assert val == "00000000-0000-0000-0000-000000000001"


def test_coerce_bool_before_int():
    assert coerce_db_value(True) is True


def test_coerce_none():
    assert coerce_db_value(None) is None


def test_coerce_int_unchanged():
    assert coerce_db_value(42) == 42


def test_coerce_str_unchanged():
    assert coerce_db_value("hi") == "hi"


# --- type_aware_equal ------------------------------------------------------
def test_tae_number_vs_string_number_first():
    assert type_aware_equal(0, "0") is False


def test_tae_number_vs_string_string_first():
    assert type_aware_equal("0", 0) is False


def test_tae_equal_numbers():
    assert type_aware_equal(0, 0) is True


def test_tae_equal_strings():
    assert type_aware_equal("CONFIRMED", "CONFIRMED") is True


def test_tae_int_float_numeric_equality():
    assert type_aware_equal(5, 5.0) is True


def test_tae_bool_int_numeric_equality():
    assert type_aware_equal(True, 1) is True


def test_tae_one_vs_string_one():
    assert type_aware_equal(1, "1") is False
