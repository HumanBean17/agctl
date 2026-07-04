"""Tests for mock HTTP path-template router."""

from agctl.mock.routing import is_param_segment, match_path, param_name, split_segments


# --- split_segments ---


def test_split_simple_path():
    assert split_segments("/orders") == ["", "orders"]


def test_split_trailing_slash_significant():
    assert split_segments("/orders/") == ["", "orders", ""]


def test_split_complex_path():
    assert split_segments("/api/v1/orders/{order_id}") == [
        "",
        "api",
        "v1",
        "orders",
        "{order_id}",
    ]


def test_split_root():
    assert split_segments("/") == ["", ""]


def test_split_no_leading_slash():
    assert split_segments("orders") == ["orders"]


def test_split_empty_string():
    assert split_segments("") == []


# --- is_param_segment ---


def test_is_param_segment_true():
    assert is_param_segment("{order_id}")


def test_is_param_segment_underscore_start():
    assert is_param_segment("{_private}")


def test_is_param_segment_digit_after_first():
    assert is_param_segment("{id2}")


def test_is_param_segment_false_plain():
    assert not is_param_segment("orders")


def test_is_param_segment_false_curly_only():
    assert not is_param_segment("{}")


def test_is_param_segment_false_invalid_start():
    assert not is_param_segment("{2id}")


def test_is_param_segment_false_special_chars():
    assert not is_param_segment("{user-id}")


def test_is_param_segment_false_empty():
    assert not is_param_segment("")


# --- param_name ---


def test_param_name_simple():
    assert param_name("{order_id}") == "order_id"


def test_param_name_underscore():
    assert param_name("{_private}") == "_private"


def test_param_name_digits():
    assert param_name("{id2}") == "id2"


# --- match_path ---


def test_match_path_single_param():
    assert match_path("/api/v1/orders/{order_id}", "/api/v1/orders/42") == {
        "order_id": "42"
    }


def test_match_path_trailing_slash_significant_template_no_slash_request_has_slash():
    assert match_path("/orders", "/orders/") is None


def test_match_path_trailing_slash_significant_template_has_slash_request_no_slash():
    assert match_path("/orders/", "/orders") is None


def test_match_path_query_string_stripped():
    assert match_path("/api/v1/orders/{order_id}", "/api/v1/orders/42?x=1&y=2") == {
        "order_id": "42"
    }


def test_match_path_segment_count_mismatch():
    assert match_path("/orders/{id}", "/orders") is None


def test_match_path_literal_must_match_exactly():
    assert match_path("/orders/bulk", "/orders/BULK") is None


def test_match_path_multiple_captures():
    assert match_path("/{org}/{repo}", "/a/b") == {"org": "a", "repo": "b"}


def test_match_path_root_matches_root():
    assert match_path("/", "/") == {}


def test_match_path_root_not_match_non_root():
    assert match_path("/", "/x") is None


def test_match_path_literal_match():
    assert match_path("/orders/bulk", "/orders/bulk") == {}


def test_match_path_multiple_params_and_literals():
    assert match_path("/api/{version}/orders/{id}", "/api/v1/orders/42") == {
        "version": "v1",
        "id": "42",
    }


def test_match_path_empty_string_vs_slash():
    assert match_path("", "") == {}


def test_match_path_param_captures_empty_segment():
    assert match_path("/{x}", "/") == {"x": ""}


def test_match_path_param_in_middle():
    assert match_path("/a/{x}/b", "/a/xyz/b") == {"x": "xyz"}
