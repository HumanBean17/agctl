"""Tests for the public ``parse_grpc_status`` helper.

This helper is the single source of truth for resolving a gRPC status arg
(name or number, digit-string or int) to a ``(code, name)`` tuple. It is
reused by ``validate_grpc_assertion_args`` (CLI flag validation),
``evaluate_grpc_assertions`` (post-call evaluation), the config model
(Task 3), and the mock server (Task 5).

Behavior pinned to the *pre-refactor* inline logic in ``assertions.py``:
  - Case-SENSITIVE name lookup. ``"NOT_FOUND"`` matches; ``"not_found"`` does
    NOT (the original code never upper-cased). The lower-case form raises
    ``ConfigError`` -- preserved on purpose so the helper is a faithful
    extraction, not a behavior change.
  - Digit-string -> int coercion (``"5"`` -> ``5``) via ``str.isdigit()``,
    so the Click CLI (which declares ``--status`` as ``type=str``) can pass
    the raw arg through.
  - Int-code lookup against the inverse map (``5`` -> ``"NOT_FOUND"``).
  - Anything else -> ``ConfigError`` with message
    ``"status must be a gRPC code name or number 0-16, got {status!r}"``.
    (Note: no ``--status `` prefix -- the helper is reusable / not CLI-bound;
    ``validate_grpc_assertion_args`` keeps its own CLI-prefixed message.)

The helper does NOT import ``grpc``; these tests run without the grpc extra.
"""

import pytest

from agctl.assertions import parse_grpc_status
from agctl.errors import ConfigError


# --- Happy path: name lookup (case-sensitive) -------------------------------


def test_parse_status_name_ok():
    """``"OK"`` -> ``(0, "OK")``."""
    assert parse_grpc_status("OK") == (0, "OK")


def test_parse_status_name_not_found():
    """``"NOT_FOUND"`` -> ``(5, "NOT_FOUND")``."""
    assert parse_grpc_status("NOT_FOUND") == (5, "NOT_FOUND")


def test_parse_status_name_unauthenticated():
    """Highest code: ``"UNAUTHENTICATED"`` -> ``(16, "UNAUTHENTICATED")``."""
    assert parse_grpc_status("UNAUTHENTICATED") == (16, "UNAUTHENTICATED")


# --- Happy path: int code lookup -------------------------------------------


def test_parse_status_int_zero():
    """``0`` -> ``(0, "OK")``."""
    assert parse_grpc_status(0) == (0, "OK")


def test_parse_status_int_mid():
    """``5`` -> ``(5, "NOT_FOUND")``."""
    assert parse_grpc_status(5) == (5, "NOT_FOUND")


def test_parse_status_int_max():
    """``16`` -> ``(16, "UNAUTHENTICATED")``."""
    assert parse_grpc_status(16) == (16, "UNAUTHENTICATED")


# --- Happy path: digit-string coercion -------------------------------------


def test_parse_status_digit_string_zero():
    """``"0"`` -> ``(0, "OK")`` (digit-string coercion path)."""
    assert parse_grpc_status("0") == (0, "OK")


def test_parse_status_digit_string_mid():
    """``"5"`` -> ``(5, "NOT_FOUND")`` -- the CLI path (Click declares
    ``--status`` as ``type=str``, so ``--status 5`` arrives as the string
    ``"5"`` and must be coerced BEFORE the int branch)."""
    assert parse_grpc_status("5") == (5, "NOT_FOUND")


def test_parse_status_digit_string_max():
    """``"16"`` -> ``(16, "UNAUTHENTICATED")``."""
    assert parse_grpc_status("16") == (16, "UNAUTHENTICATED")


# --- Error cases -----------------------------------------------------------


def test_parse_status_lowercase_name_raises():
    """Case-SENSITIVE name lookup: ``"not_found"`` is NOT matched.

    The pre-refactor inline code never upper-cased the status string -- only
    exact uppercase names hit ``_GRPC_STATUS_BY_NAME``. This helper preserves
    that behavior on purpose (faithful extraction, not a behavior change).
    If a caller wants case-insensitive matching, they must ``.upper()`` first.
    """
    with pytest.raises(ConfigError) as exc_info:
        parse_grpc_status("not_found")
    # Message drops the "--status " prefix (helper is reusable, not CLI-bound)
    assert "status must be a gRPC code name or number 0-16" in str(exc_info.value)
    assert "got 'not_found'" in str(exc_info.value)


def test_parse_status_unknown_name_raises():
    """``"FOO"`` is not a gRPC code name -> ``ConfigError``."""
    with pytest.raises(ConfigError, match="status must be a gRPC code name or number 0-16"):
        parse_grpc_status("FOO")


def test_parse_status_int_too_high_raises():
    """``17`` is outside the 0-16 range -> ``ConfigError``."""
    with pytest.raises(ConfigError, match="status must be a gRPC code name or number 0-16"):
        parse_grpc_status(17)


def test_parse_status_int_negative_raises():
    """``-1`` is outside the 0-16 range -> ``ConfigError``.

    (Negative ints are not digit-strings and never hit the coercion branch;
    they fail the int-code lookup directly.)"""
    with pytest.raises(ConfigError, match="status must be a gRPC code name or number 0-16"):
        parse_grpc_status(-1)


def test_parse_status_error_has_no_cli_prefix():
    """The helper's OWN error message drops the ``--status `` prefix so it is
    reusable beyond the CLI (config model, mock server). ``validate_grpc_assertion_args``
    keeps its CLI-prefixed message separately."""
    with pytest.raises(ConfigError) as exc_info:
        parse_grpc_status("FOO")
    # The message must NOT start with the CLI "--status " prefix
    assert not str(exc_info.value).startswith("--status ")


def test_parse_status_error_detail_is_empty_dict():
    """The raised ``ConfigError`` carries an empty detail dict (matches the
    contract pinned in the task brief: ``ConfigError(..., {})``)."""
    with pytest.raises(ConfigError) as exc_info:
        parse_grpc_status("FOO")
    assert exc_info.value.detail == {}
