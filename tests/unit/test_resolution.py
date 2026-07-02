"""Tests for template resolution helpers (DESIGN D2, D5)."""

import copy

from agctl.resolution import convert_sql_params, deep_merge, fill_placeholders


# --- fill_placeholders ---


def test_fill_simple_string():
    assert fill_placeholders("hello {name}", {"name": "world"}) == "hello world"


def test_fill_multiple_in_string():
    assert fill_placeholders("{a}/{b}", {"a": "x", "b": "y"}) == "x/y"


def test_fill_missing_param_left_literal():
    # Name not in params -> literal {name} unchanged (DESIGN §10).
    assert fill_placeholders("hi {missing}", {"other": "1"}) == "hi {missing}"


def test_fill_partial_match_leaves_others_literal():
    assert (
        fill_placeholders("{a}-{b}", {"a": "1"}) == "1-{b}"
    )


def test_fill_non_string_scalar_passes_through():
    assert fill_placeholders(42, {"x": "y"}) == 42
    assert fill_placeholders(None, {"x": "y"}) is None


def test_fill_recurse_into_list():
    out = fill_placeholders(["{a}", "literal", "{b}"], {"a": "1", "b": "2"})
    assert out == ["1", "literal", "2"]


def test_fill_recurse_into_dict():
    out = fill_placeholders({"path": "/o/{id}", "body": {"k": "{id}"}}, {"id": "o9"})
    assert out == {"path": "/o/o9", "body": {"k": "o9"}}


def test_fill_nested_mixed():
    src = {"items": [{"url": "/{a}"}, "{b}"], "n": 3}
    out = fill_placeholders(src, {"a": "x", "b": "y"})
    assert out == {"items": [{"url": "/x"}, "y"], "n": 3}


def test_fill_does_not_mutate_input():
    src = {"path": "/{id}", "body": {"k": "{id}"}}
    snapshot = copy.deepcopy(src)
    fill_placeholders(src, {"id": "o9"})
    assert src == snapshot


# --- deep_merge (DESIGN D5) ---


def test_deep_merge_recursive_dict():
    assert deep_merge({"a": 1, "b": {"c": 2}}, {"b": {"d": 3}}) == {
        "a": 1,
        "b": {"c": 2, "d": 3},
    }


def test_deep_merge_array_replaced_wholesale():
    assert deep_merge({"x": [1, 2]}, {"x": [3]}) == {"x": [3]}


def test_deep_merge_scalar_wins():
    assert deep_merge({"a": 1}, {"a": 2}) == {"a": 2}


def test_deep_merge_base_preserved_when_empty_override():
    assert deep_merge({"a": 1}, {}) == {"a": 1}


def test_deep_merge_does_not_mutate_base():
    base = {"a": 1, "b": {"c": 2}}
    snapshot = copy.deepcopy(base)
    deep_merge(base, {"b": {"d": 3}})
    assert base == snapshot


def test_deep_merge_override_adds_new_keys():
    assert deep_merge({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_deep_merge_override_replaces_dict_with_scalar():
    assert deep_merge({"a": {"x": 1}}, {"a": 5}) == {"a": 5}


def test_deep_merge_returned_containers_are_independent():
    base = {"a": [1]}
    override = {"b": [2]}
    out = deep_merge(base, override)
    out["b"].append(99)
    assert override == {"b": [2]}


# --- convert_sql_params (DESIGN D2) ---


def test_convert_single_param():
    assert (
        convert_sql_params("WHERE id = :orderId")
        == "WHERE id = %(orderId)s"
    )


def test_convert_multiple_params():
    assert (
        convert_sql_params("WHERE id = :orderId AND s = :status")
        == "WHERE id = %(orderId)s AND s = %(status)s"
    )


def test_convert_does_not_touch_double_colon_cast():
    assert (
        convert_sql_params("SELECT '::text' AS x WHERE id = :orderId")
        == "SELECT '::text' AS x WHERE id = %(orderId)s"
    )


def test_convert_double_colon_in_expression():
    assert (
        convert_sql_params("SELECT (a::int)") == "SELECT (a::int)"
    )


def test_convert_no_params_unchanged():
    assert convert_sql_params("SELECT 1") == "SELECT 1"


def test_convert_underscore_and_digit_names():
    assert (
        convert_sql_params("WHERE x = :_user_id AND y = :id2")
        == "WHERE x = %(_user_id)s AND y = %(id2)s"
    )
