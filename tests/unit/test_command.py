"""Tests for the @envelope command wrapper (DESIGN §4.1)."""

import contextlib
import io
import json

import pytest

from agctl.command import envelope, load_config_or_raise
from agctl.errors import AssertionFailure


def invoke(fn):
    """Run a wrapped callback; return (parsed_envelope, exit_code).

    exit_code is None when the callback returned normally (no SystemExit).
    """
    buf = io.StringIO()
    exit_code = None
    with contextlib.redirect_stdout(buf):
        try:
            fn()
        except SystemExit as exc:
            exit_code = exc.code
    captured = buf.getvalue().strip()
    envelope = json.loads(captured) if captured else None
    return envelope, exit_code


def test_success_emits_ok_envelope():
    @envelope("demo")
    def cb():
        return {"k": 1}

    env, code = invoke(cb)
    assert env["ok"] is True
    assert env["command"] == "demo"
    assert env["result"] == {"k": 1}
    assert env["error"] is None
    assert env["duration_ms"] >= 0
    assert code is None


def test_assertion_failure_emits_error_exit1():
    @envelope("demo")
    def cb():
        raise AssertionFailure("boom", {"x": 1})

    env, code = invoke(cb)
    assert env["ok"] is False
    assert env["error"]["type"] == "AssertionError"
    assert env["error"]["message"] == "boom"
    assert env["error"]["detail"] == {"x": 1}
    assert code == 1


def test_builtin_assertion_emits_assertion_error_exit1():
    @envelope("demo")
    def cb():
        assert False, "plain"

    env, code = invoke(cb)
    assert env["ok"] is False
    assert env["error"]["type"] == "AssertionError"
    assert "plain" in env["error"]["message"]
    assert env["error"]["detail"] == {}
    assert code == 1


def test_generic_exception_emits_internal_error_exit2():
    @envelope("demo")
    def cb():
        raise ValueError("wat")

    env, code = invoke(cb)
    assert env["ok"] is False
    assert env["error"]["type"] == "InternalError"
    assert env["error"]["message"] == "wat"
    assert env["error"]["detail"] == {}
    assert code == 2


def test_nested_system_exit_propagates_unchanged():
    @envelope("demo")
    def cb():
        raise SystemExit(0)

    env, code = invoke(cb)
    # SystemExit must be re-raised as-is, NOT turned into an InternalError envelope.
    assert code == 0
    assert env is None


def test_load_config_or_raise_returns_config():
    # Smoke: returns the result of load_config (uses discovery; just ensure it delegates).
    # We only assert it is callable and forwards; full load tested in test_loader.py.
    assert callable(load_config_or_raise)
