import datetime
import uuid
from decimal import Decimal

import pytest

from agctl.assertions import (
    coerce_db_value,
    compile_jq,
    evaluate_http_assertions,
    jq_bool,
    jq_value,
    json_subset,
    parse_equals,
    type_aware_equal,
    validate_http_assertion_args,
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


# --- compile_jq: hyphenated-key hint --------------------------------------
# jq parses `.headers.x-request-id` as subtraction (`.headers.x - request - id`)
# -> baffling "request/0 is not defined" compile error. compile_jq appends a
# targeted bracket-notation hint when the failing expression looks like a
# dotted path with a hyphenated final segment.
def test_compile_jq_hyphenated_header_key_hint_appended():
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError) as exc_info:
        compile_jq(".headers.x-request-id")
    assert "bracket notation" in exc_info.value.message
    # detail dict still carries expr/label
    assert exc_info.value.detail["expr"] == ".headers.x-request-id"


def test_compile_jq_hyphenated_body_field_hint_appended():
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError) as exc_info:
        compile_jq(".body.event-id", label="evt")
    assert "bracket notation" in exc_info.value.message
    assert exc_info.value.detail["label"] == "evt"


def test_compile_jq_bracket_notation_compiles_no_hint():
    # correct bracket-notation form compiles fine -> no raise, no hint.
    assert compile_jq('.headers["x-request-id"]') is None


def test_compile_jq_broken_expr_without_hyphen_has_no_hint():
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError) as exc_info:
        compile_jq(".foo bar")
    assert "bracket notation" not in exc_info.value.message


def test_compile_jq_event_type_field_compiles_due_to_type_builtin():
    # `.body.event-type` parses as `.body.event - type` where `type` IS a jq
    # builtin -> compiles fine, so no raise and no hint. (Hyphenated-key
    # hinting is gated on a compile failure; this case does not fail.)
    assert compile_jq(".body.event-type") is None


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


# --- type_aware_equal: ISO 8601 timestamp normalization -------------------
# DESIGN §3.3 tells users to write --equals "2026-06-29T14:22:00Z", but
# coerce_db_value emits ".isoformat()" -> "...+00:00". Both are valid ISO 8601
# for the same UTC instant; the comparison layer must treat them as equal.
def test_tae_timestamp_z_vs_utc_offset_equal():
    assert type_aware_equal("2026-06-29T14:22:00Z", "2026-06-29T14:22:00+00:00") is True


def test_tae_timestamp_z_vs_different_offset_not_equal():
    assert type_aware_equal("2026-06-29T14:22:00Z", "2026-06-29T14:22:00+05:00") is False


def test_tae_timestamp_naive_treated_as_utc_equal_z():
    assert type_aware_equal("2026-06-29T14:22:00", "2026-06-29T14:22:00Z") is True


def test_tae_timestamp_z_vs_utc_offset_end_to_end_via_coerce_db_value():
    # Regression-of-record: the exact pair from DESIGN §3.3 vs coerce_db_value.
    aware = datetime.datetime(2026, 6, 29, 14, 22, 0, tzinfo=datetime.timezone.utc)
    assert type_aware_equal(parse_equals("2026-06-29T14:22:00Z"), coerce_db_value(aware)) is True


def test_tae_non_timestamp_with_T_falls_back_to_string_equality():
    # Contains "T" but is not a valid datetime -> plain string compare.
    assert type_aware_equal("xTy", "xTy") is True
    assert type_aware_equal("xTy", "xTz") is False


def test_tae_date_only_string_unaffected():
    # No "T" -> never parsed as a datetime, plain string compare.
    assert type_aware_equal("2026-06-29", "2026-06-29") is True


def test_tae_lists_and_dicts_compare_elementwise():
    assert type_aware_equal([1, 2], [1, 2]) is True
    assert type_aware_equal([1, 2], [1, 3]) is False
    assert type_aware_equal({"a": 1}, {"a": 1}) is True


# --- validate_http_assertion_args -----------------------------------------
# Fixture shared by evaluate tests below.
RESULT = {
    "status_code": 201,
    "body": {"status": "PENDING", "items": [{"amount": 1500}]},
    "headers": {},
    "url": "u",
    "method": "POST",
    "response_time_ms": 5,
}


def test_validate_jq_path_only_no_equals_raises_config_error():
    """v1: --jq-path without --equals is a pairing violation (D8)."""
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError):
        validate_http_assertion_args(
            status=None, contains=None, match=None, jq_path=".status", equals=None
        )


def test_validate_equals_only_no_jq_path_raises_config_error():
    """v2: --equals without --jq-path is a pairing violation (D8)."""
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError):
        validate_http_assertion_args(
            status=None, contains=None, match=None, jq_path=None, equals="PENDING"
        )


def test_validate_contains_not_json_raises_config_error():
    """v3: --contains must parse as JSON."""
    from agctl.errors import ConfigError

    with pytest.raises(ConfigError) as exc_info:
        validate_http_assertion_args(
            status=None, contains="not json", match=None, jq_path=None, equals=None
        )
    assert "--contains must be valid JSON" in exc_info.value.message


def test_validate_all_none_returns_none():
    """v4: no active modes is a valid no-op."""
    assert (
        validate_http_assertion_args(
            status=None, contains=None, match=None, jq_path=None, equals=None
        )
        is None
    )


def test_validate_valid_args_returns_none():
    """v5: valid paired --jq-path/--equals and valid --contains JSON passes."""
    assert (
        validate_http_assertion_args(
            status=201,
            contains='{"x":1}',
            match=None,
            jq_path=".status",
            equals='"PENDING"',
        )
        is None
    )


# --- evaluate_http_assertions ---------------------------------------------
def test_evaluate_all_none_returns_none():
    """e1: no active modes returns immediately without raising."""
    assert (
        evaluate_http_assertions(
            RESULT, status=None, contains=None, match=None, jq_path=None, equals=None
        )
        is None
    )


def test_evaluate_status_pass_no_raise():
    """e2 pass: --status 201 matches the fixture status_code."""
    evaluate_http_assertions(
        RESULT, status=201, contains=None, match=None, jq_path=None, equals=None
    )


def test_evaluate_status_fail_raises_with_failure_entry():
    """e2 fail: --status 200 -> AssertionFailure with pinned failure shape."""
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            RESULT, status=200, contains=None, match=None, jq_path=None, equals=None
        )
    assert exc_info.value.detail["failures"] == [
        {"mode": "status", "expected": 200, "actual": 201}
    ]


def test_evaluate_contains_pass_no_raise():
    """e3 pass: needle is a subset of body."""
    evaluate_http_assertions(
        RESULT,
        status=None,
        contains='{"status":"PENDING"}',
        match=None,
        jq_path=None,
        equals=None,
    )


def test_evaluate_contains_fail_raises_with_failure_entry():
    """e3 fail: needle not present -> failure with parsed needle + matched:False."""
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            RESULT,
            status=None,
            contains='{"status":"PAID"}',
            match=None,
            jq_path=None,
            equals=None,
        )
    assert exc_info.value.detail["failures"] == [
        {
            "mode": "contains",
            "needle": {"status": "PAID"},
            "matched": False,
            "root": "response body",
            "body": RESULT["body"],
        }
    ]


def test_evaluate_match_pass_no_raise():
    """e4 pass: predicate truthy."""
    evaluate_http_assertions(
        RESULT,
        status=None,
        contains=None,
        match='.body.status=="PENDING"',
        jq_path=None,
        equals=None,
    )


def test_evaluate_match_fail_raises_with_failure_entry():
    """e4 fail: predicate falsy -> pinned failure shape."""
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            RESULT,
            status=None,
            contains=None,
            match='.body.status=="PAID"',
            jq_path=None,
            equals=None,
        )
    assert exc_info.value.detail["failures"] == [
        {
            "mode": "match",
            "expr": '.body.status=="PAID"',
            "result": False,
            "root": "response envelope",
            "body": RESULT["body"],
        }
    ]


def test_evaluate_match_any_truthy_pass_no_raise():
    """e5 pass: ANY-truthy semantics across list iteration (one item satisfies)."""
    evaluate_http_assertions(
        RESULT,
        status=None,
        contains=None,
        match=".body.items[].amount > 1000",
        jq_path=None,
        equals=None,
    )


def test_evaluate_match_any_truthy_fail_raises():
    """e5 fail: no item satisfies predicate."""
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure):
        evaluate_http_assertions(
            RESULT,
            status=None,
            contains=None,
            match=".body.items[].amount > 9999",
            jq_path=None,
            equals=None,
        )


def test_evaluate_jq_path_pass_no_raise():
    """e6 pass: jq value matches expected (bare-word equals -> string)."""
    evaluate_http_assertions(
        RESULT,
        status=None,
        contains=None,
        match=None,
        jq_path=".status",
        equals="PENDING",
    )


def test_evaluate_jq_path_fail_raises_with_failure_entry():
    """e6 fail: jq value mismatch -> pinned failure shape."""
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            RESULT,
            status=None,
            contains=None,
            match=None,
            jq_path=".status",
            equals='"PAID"',
        )
    assert exc_info.value.detail["failures"] == [
        {
            "mode": "jq-path",
            "path": ".status",
            "expected": "PAID",
            "actual": "PENDING",
            "root": "response body",
            "body": RESULT["body"],
        }
    ]


def test_evaluate_two_failures_no_short_circuit_and_response_preserved():
    """e7: two failing modes -> failures has TWO entries (no short-circuit)
    and detail['response'] equals the result dict."""
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            RESULT,
            status=200,
            contains=None,
            match='.body.status=="PAID"',
            jq_path=None,
            equals=None,
        )
    failures = exc_info.value.detail["failures"]
    assert len(failures) == 2
    assert exc_info.value.detail["response"] == RESULT


def test_evaluate_missing_jq_with_match_raises_config_error_mentioning_agctl_jq(
    monkeypatch,
):
    """e8: missing jq library + --match -> ConfigError whose message names agctl[jq]
    (the base _jq() message names only db/kafka; evaluate MUST rewrite it)."""
    import sys
    from agctl import assertions
    from agctl.errors import ConfigError

    monkeypatch.setitem(sys.modules, "jq", None)  # block the lazy import

    with pytest.raises(ConfigError) as exc_info:
        assertions.evaluate_http_assertions(
            RESULT,
            status=None,
            contains=None,
            match=".x",
            jq_path=None,
            equals=None,
        )
    assert "agctl[jq]" in exc_info.value.message


def test_evaluate_missing_jq_with_jq_path_raises_config_error_mentioning_agctl_jq(
    monkeypatch,
):
    """e9: missing jq library + --jq-path -> ConfigError whose message names agctl[jq]
    (symmetric to e8 for the --jq-path branch)."""
    import sys
    from agctl import assertions
    from agctl.errors import ConfigError

    monkeypatch.setitem(sys.modules, "jq", None)  # block the lazy import

    with pytest.raises(ConfigError) as exc_info:
        assertions.evaluate_http_assertions(
            RESULT,
            status=None,
            contains=None,
            match=None,
            jq_path=".status",
            equals="PENDING",
        )
    assert "agctl[jq]" in exc_info.value.message


def test_evaluate_non_json_body_contains_fails_matched_false():
    """e9a: non-JSON body (a string) + --contains -> json_subset is False on a
    scalar haystack, so failure with matched:False."""
    from agctl.errors import AssertionFailure

    body_string_result = {**RESULT, "body": "not-json"}
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            body_string_result,
            status=None,
            contains='{"x":1}',
            match=None,
            jq_path=None,
            equals=None,
        )
    assert exc_info.value.detail["failures"] == [
        {
            "mode": "contains",
            "needle": {"x": 1},
            "matched": False,
            "root": "response body",
            "body": body_string_result["body"],
        }
    ]


def test_evaluate_non_json_body_jq_path_actual_null():
    """e9b: non-JSON body + --jq-path -> jq_value yields nothing on a string,
    so actual is None."""
    from agctl.errors import AssertionFailure

    body_string_result = {**RESULT, "body": "not-json"}
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            body_string_result,
            status=None,
            contains=None,
            match=None,
            jq_path=".status",
            equals="whatever",
        )
    assert exc_info.value.detail["failures"] == [
        {
            "mode": "jq-path",
            "path": ".status",
            "expected": "whatever",
            "actual": None,
            "root": "response body",
            "body": body_string_result["body"],
        }
    ]


def test_evaluate_match_wrong_root_self_documents_envelope_and_body():
    """Regression (battle-test incident): an agent wrote `--match '.data.operator
    != null'` (body-rooted, trusting the stale 'response body' help wording) and
    got a silent result:false, then had to drop --match and re-run raw to find the
    path. The failure entry must now carry root='response envelope' + the actual
    body, so the agent derives `.body.data.operator` in one read."""
    from agctl.errors import AssertionFailure

    resp = {**RESULT, "body": {"data": {"operator": "ACME"}}}
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_http_assertions(
            resp,
            status=None,
            contains=None,
            match=".data.operator != null",
            jq_path=None,
            equals=None,
        )
    failure = exc_info.value.detail["failures"][0]
    assert failure["mode"] == "match"
    assert failure["result"] is False
    assert failure["root"] == "response envelope"
    assert failure["body"] == {"data": {"operator": "ACME"}}
