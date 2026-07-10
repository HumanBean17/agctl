"""Tests for gRPC assertion validation and evaluation."""

import json
import sys

import pytest

from agctl.assertions import evaluate_grpc_assertions, validate_grpc_assertion_args
from agctl.errors import AssertionFailure, ConfigError


@pytest.fixture
def grpc_result_ok():
    """A sample gRPC result dict with OK status and a message payload."""
    return {
        "target": "localhost:50051",
        "service": "test.Service",
        "method": "GetInfo",
        "call_type": "unary",
        "status": {"code": 0, "name": "OK", "message": ""},
        "message": {"a": 1, "b": {"c": 2}},
        "initial_metadata": [],
        "trailers": [],
    }


def test_validate_pairing():
    """jq_path/equals must be used together (XOR)."""
    with pytest.raises(ConfigError, match="--jq-path and --equals must be used together"):
        validate_grpc_assertion_args(jq_path=".x", equals=None)


def test_validate_contains_json():
    """--contains must be valid JSON."""
    with pytest.raises(ConfigError, match="--contains must be valid JSON"):
        validate_grpc_assertion_args(contains="{bad")


def test_validate_status_name_and_number():
    """--status accepts both code names and numbers 0-16."""
    # Valid forms
    validate_grpc_assertion_args(status="NOT_FOUND")
    validate_grpc_assertion_args(status=5)

    # Invalid forms
    with pytest.raises(ConfigError, match="--status must be a gRPC code name or number 0-16"):
        validate_grpc_assertion_args(status="NOPE")
    with pytest.raises(ConfigError, match="--status must be a gRPC code name or number 0-16"):
        validate_grpc_assertion_args(status=99)


def test_validate_all_none_ok():
    """All-None args return None (no-op)."""
    assert validate_grpc_assertion_args() is None


def test_eval_status_pass_and_fail(grpc_result_ok):
    """Status assertion: name or number -> compare to result['status']['code']."""
    # Pass: status matches
    evaluate_grpc_assertions(
        grpc_result_ok, status="OK", contains=None, match=None, jq_path=None, equals=None
    )

    # Fail: status mismatch
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_grpc_assertions(
            grpc_result_ok, status="NOT_FOUND", contains=None, match=None, jq_path=None, equals=None
        )
    failures = exc_info.value.detail["failures"]
    assert len(failures) == 1
    assert failures[0]["mode"] == "status"
    assert failures[0]["expected"] == "NOT_FOUND"
    assert failures[0]["actual"] == "OK"


def test_eval_contains(grpc_result_ok):
    """Contains assertion checks result['message'] subset match."""
    # Pass: needle exists in message
    evaluate_grpc_assertions(
        grpc_result_ok, contains='{"b":{"c":2}}', match=None, jq_path=None, equals=None, status=None
    )

    # Fail: needle missing
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_grpc_assertions(
            grpc_result_ok, contains='{"z":1}', match=None, jq_path=None, equals=None, status=None
        )
    failures = exc_info.value.detail["failures"]
    assert len(failures) == 1
    assert failures[0]["mode"] == "contains"
    assert failures[0]["needle"] == {"z": 1}
    assert failures[0]["matched"] is False
    assert failures[0]["root"] == "response message"


def test_eval_match_envelope_rooted(grpc_result_ok):
    """Match assertion evaluates jq against the whole result dict (envelope-rooted)."""
    jq = pytest.importorskip("jq")

    # Pass: envelope-rooted expression
    evaluate_grpc_assertions(grpc_result_ok, match='.status.name == "OK"', jq_path=None, equals=None, contains=None, status=None)

    # Pass: .message reaches into message
    evaluate_grpc_assertions(grpc_result_ok, match=".message.a == 1", jq_path=None, equals=None, contains=None, status=None)

    # Fail: wrong status name
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_grpc_assertions(
            grpc_result_ok, match='.status.name == "NOPE"', jq_path=None, equals=None, contains=None, status=None
        )
    failures = exc_info.value.detail["failures"]
    assert len(failures) == 1
    assert failures[0]["mode"] == "match"
    assert failures[0]["expr"] == '.status.name == "NOPE"'
    assert failures[0]["result"] is False
    assert failures[0]["root"] == "response envelope"


def test_eval_jq_path_equals(grpc_result_ok):
    """jq-path assertion: type-aware comparison against result['message'] value."""
    jq = pytest.importorskip("jq")

    # Pass: type-aware equal (number matches number)
    evaluate_grpc_assertions(grpc_result_ok, jq_path=".a", equals="1", match=None, contains=None, status=None)

    # Fail: number != string (type-aware)
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_grpc_assertions(grpc_result_ok, jq_path=".a", equals='"1"', match=None, contains=None, status=None)
    failures = exc_info.value.detail["failures"]
    assert len(failures) == 1
    assert failures[0]["mode"] == "jq-path"
    assert failures[0]["path"] == ".a"
    assert failures[0]["expected"] == "1"
    assert failures[0]["actual"] == 1
    assert failures[0]["root"] == "response message"


def test_eval_multiple_failures_no_shortcircuit(grpc_result_ok):
    """Multiple assertion failures are collected (no short-circuit)."""
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_grpc_assertions(
            grpc_result_ok,
            status="NOT_FOUND",
            contains='{"missing": "key"}',
            match=None,
            jq_path=None,
            equals=None,
        )
    failures = exc_info.value.detail["failures"]
    assert len(failures) == 2
    assert {f["mode"] for f in failures} == {"status", "contains"}


def test_eval_missing_jq_is_configerror(grpc_result_ok, monkeypatch):
    """Missing jq lib surfaces as ConfigError with grpc extra hint."""
    import agctl.assertions as assertions_module

    # Mock _jq to raise ConfigError (simulating missing jq library)
    def mock_jq_error():
        raise ConfigError(
            "jq is required for match/path assertions: pip install 'agctl[db]' or 'agctl[kafka]'",
            {},
        )

    monkeypatch.setattr(assertions_module, "_jq", mock_jq_error)

    # Now jq import should fail and surface ConfigError with grpc hint
    with pytest.raises(ConfigError, match="agctl\\[grpc\\]"):
        evaluate_grpc_assertions(grpc_result_ok, match=".x")
