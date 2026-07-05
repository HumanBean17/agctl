"""Tests for template resolution helpers (DESIGN D2, D5)."""

import copy
import json

import pytest

from agctl.resolution import (
    CaptureValue,
    convert_sql_params,
    deep_merge,
    fill_placeholders,
    render_typed,
)


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


def test_fill_placeholders_regression_sanity():
    # Existing fill_placeholders semantics must remain byte-for-byte unchanged
    # when render_typed is added alongside it.
    assert fill_placeholders("hi {name}", {"name": "world"}) == "hi world"
    assert fill_placeholders({"k": "{id}"}, {"id": "o9"}) == {"k": "o9"}


# --- render_typed (typed CaptureValue renderer) ---


def test_render_scalar_int_to_string():
    assert render_typed("{op_id}", {"op_id": CaptureValue(42, "scalar")}) == "42"


def test_render_scalar_multiple_fields_in_dict():
    out = render_typed(
        {"id": "{op_id}", "n": "{n}"},
        {"op_id": CaptureValue(7, "scalar"), "n": CaptureValue(True, "scalar")},
    )
    assert out == {"id": "7", "n": "True"}


def test_render_json_type_emits_json_dumps_string():
    out = render_typed("{ctx}", {"ctx": CaptureValue({"a": 1}, "json")})
    assert out == json.dumps({"a": 1})


def test_render_json_type_preserves_non_ascii():
    # A json-typed capture must not \u-escape non-ASCII: the produced string is
    # embedded verbatim in the envelope, so escapes would survive even after
    # the emit-level ensure_ascii=False fix.
    out = render_typed("{ctx}", {"ctx": CaptureValue({"name": "Иван"}, "json")})
    assert out == '{"name": "Иван"}'
    assert "\\u" not in out


def test_render_object_whole_field_returns_live_object():
    out = render_typed({"context": "{ctx}"}, {"ctx": CaptureValue({"a": 1}, "object")})
    assert out == {"context": {"a": 1}}
    # Real object, not a stringified form.
    assert isinstance(out["context"], dict)


def test_render_object_inline_raises_value_error():
    with pytest.raises(ValueError):
        render_typed(
            {"context": "pre={ctx}"},
            {"ctx": CaptureValue({"a": 1}, "object")},
        )


def test_render_list_with_scalar_and_json():
    out = render_typed(
        ["{a}", "{b}"],
        {"a": CaptureValue("x", "scalar"), "b": CaptureValue([1, 2], "json")},
    )
    assert out == ["x", json.dumps([1, 2])]


def test_render_none_scalar_becomes_empty_string():
    assert render_typed("{op_id}", {"op_id": CaptureValue(None, "scalar")}) == ""


def test_render_none_object_becomes_empty_string():
    assert render_typed("{op_id}", {"op_id": CaptureValue(None, "object")}) == ""


def test_render_none_json_becomes_empty_string():
    # None→"" rule applies regardless of type.
    assert render_typed("{op_id}", {"op_id": CaptureValue(None, "json")}) == ""


def test_render_absent_name_left_as_literal():
    assert render_typed("{missing}", {}) == "{missing}"


def test_render_does_not_mutate_input():
    src = {"path": "/{id}", "body": {"k": "{id}"}}
    snapshot = copy.deepcopy(src)
    render_typed(src, {"id": CaptureValue("o9", "scalar")})
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
