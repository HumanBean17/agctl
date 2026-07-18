"""Tests for the AgctlError hierarchy (DESIGN §4.1)."""

import pytest

from agctl.errors import (
    AgctlError,
    AssertionFailure,
    ConfigError as ConfigErrorFromErrors,
    ConnectionFailure,
    OperationTimeout,
    SerializationError,
    TemplateNotFound,
)
from agctl.config import ConfigError as ConfigErrorFromConfig


# --- subclass type_name / exit_code table -------------------------------------

@pytest.mark.parametrize(
    "cls, type_name, exit_code",
    [
        (AssertionFailure, "AssertionError", 1),
        (ConfigErrorFromErrors, "ConfigError", 2),
        (ConnectionFailure, "ConnectionError", 2),
        (OperationTimeout, "TimeoutError", 1),
        (SerializationError, "SerializationError", 2),
        (TemplateNotFound, "TemplateNotFound", 2),
    ],
)
def test_subclass_table(cls, type_name, exit_code):
    assert cls.type_name == type_name
    assert cls.exit_code == exit_code


def test_base_defaults():
    assert AgctlError.type_name == "InternalError"
    assert AgctlError.exit_code == 2


# --- to_dict ------------------------------------------------------------------

def test_to_dict_shape_and_default_detail():
    err = AssertionFailure("boom")
    assert err.to_dict() == {"type": "AssertionError", "message": "boom", "detail": {}}


def test_to_dict_preserves_passed_detail():
    err = ConfigErrorFromErrors("nope", {"k": 1})
    d = err.to_dict()
    assert d == {"type": "ConfigError", "message": "nope", "detail": {"k": 1}}


def test_detail_none_becomes_empty_dict():
    err = OperationTimeout("slow", None)
    assert err.detail == {}
    assert err.to_dict()["detail"] == {}


def test_message_and_detail_attributes():
    err = TemplateNotFound("missing", {"name": "foo"})
    assert err.message == "missing"
    assert err.detail == {"name": "foo"}


# --- exit codes via instances -------------------------------------------------

def test_instance_exit_codes():
    assert AssertionFailure("x").exit_code == 1
    assert ConfigErrorFromErrors("y").exit_code == 2
    assert ConnectionFailure("z").exit_code == 2
    assert OperationTimeout("w").exit_code == 1
    assert SerializationError("x").exit_code == 2
    assert TemplateNotFound("v").exit_code == 2


# --- inheritance / identity ---------------------------------------------------

def test_assertion_failure_is_agctl_error():
    assert isinstance(AssertionFailure("x"), AgctlError)


def test_config_error_identity_across_import_paths():
    assert ConfigErrorFromConfig is ConfigErrorFromErrors


# --- SerializationError-specific tests -----------------------------------------

def test_serialization_error_to_dict_with_detail():
    """Test SerializationError.to_dict() with subject/topic detail (task brief requirement)."""
    err = SerializationError(
        "payload does not conform",
        {"subject": "orders.created-value", "topic": "orders.created"},
    )
    assert err.to_dict() == {
        "type": "SerializationError",
        "message": "payload does not conform",
        "detail": {"subject": "orders.created-value", "topic": "orders.created"},
    }


def test_serialization_error_exit_code():
    """Test SerializationError.exit_code == 2 (task brief requirement)."""
    assert SerializationError("x").exit_code == 2
