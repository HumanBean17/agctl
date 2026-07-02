import datetime
import uuid
from decimal import Decimal

import pytest

from agctl.assertions import (
    coerce_db_value,
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
