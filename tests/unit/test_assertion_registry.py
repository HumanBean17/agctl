"""Tests for the assertion registry (DESIGN §9.3).

The registry is the extension point for pluggable assertion modes on
``db assert`` / ``kafka assert``. Built-in mode names are registered so they
are discoverable; unknown modes raise :class:`TemplateNotFound`. Third-party
modes are loaded from the ``agctl.assertions`` entry-point group, with each
load isolated so a broken entry point is skipped rather than fatal.
"""

import pytest

from agctl.assertion_registry import (
    Assertion,
    AssertionRegistry,
    evaluate_custom,
    get_default_registry,
)
from agctl.errors import AssertionFailure, ConfigError, TemplateNotFound


# --- built-in modes are present & discoverable ------------------------------


@pytest.mark.parametrize(
    "mode",
    ["expect_rows", "expect_value", "contains", "match", "pattern"],
)
def test_default_registry_resolves_built_in_mode(mode):
    reg = get_default_registry()
    instance = reg.get(mode)
    assert isinstance(instance, Assertion)
    assert instance.name == mode


def test_unknown_mode_raises_template_missing():
    reg = get_default_registry()
    with pytest.raises(TemplateNotFound) as exc_info:
        reg.get("no_such_mode")
    assert exc_info.value.message == "Unknown assertion mode: no_such_mode"
    assert exc_info.value.detail == {"mode": "no_such_mode"}


def test_default_registry_names_include_all_built_ins():
    names = get_default_registry().names()
    for built_in in ("expect_rows", "expect_value", "contains", "match", "pattern"):
        assert built_in in names
    # names() is sorted
    assert names == sorted(names)


# --- fresh registry: custom Assertion can be registered & resolved ----------


class _CustomMode(Assertion):
    name = "my_custom_mode"

    def evaluate(self, context: dict) -> dict:
        return {"passed": True, "echo": context.get("v")}


def test_register_and_get_custom_assertion():
    reg = AssertionRegistry()
    reg.register(_CustomMode)
    instance = reg.get("my_custom_mode")
    assert isinstance(instance, Assertion)
    assert instance.evaluate({"v": 42}) == {"passed": True, "echo": 42}


def test_register_accepts_instance_or_class():
    reg = AssertionRegistry()
    reg.register(_CustomMode())  # instance form
    assert reg.get("my_custom_mode").name == "my_custom_mode"


def test_empty_registry_get_raises_template_missing():
    reg = AssertionRegistry()
    with pytest.raises(TemplateNotFound):
        reg.get("anything")


def test_empty_registry_names_is_empty():
    assert AssertionRegistry().names() == []


# --- entry-point loading is isolated (broken EPs skipped) -------------------


class _FakeEntryPoint:
    """Minimal stand-in for importlib.metadata.EntryPoint."""

    def __init__(self, name, load_fn):
        self.name = name
        self._load_fn = load_fn

    def load(self):
        return self._load_fn()


def test_load_entry_points_skips_broken_entry_point(monkeypatch):
    """An entry point whose .load() raises is skipped, not fatal."""

    def boom():
        raise RuntimeError("broken plugin")

    broken_ep = _FakeEntryPoint("broken", boom)

    reg = AssertionRegistry()
    monkeypatch.setattr(
        "agctl.assertion_registry._entry_points",
        lambda group: [broken_ep],
    )

    # Must not raise.
    result = reg.load_entry_points()
    assert result is reg  # returns self for chaining
    assert "broken" not in reg.names()


def test_load_entry_points_registers_loaded_assertion(monkeypatch):
    """An entry point returning an Assertion subclass gets registered."""

    class _ThirdPartyMode(Assertion):
        name = "third_party_mode"

        def evaluate(self, context):
            return {"passed": True}

    good_ep = _FakeEntryPoint("third_party", lambda: _ThirdPartyMode)

    reg = AssertionRegistry()
    monkeypatch.setattr(
        "agctl.assertion_registry._entry_points",
        lambda group: [good_ep],
    )
    reg.load_entry_points()

    assert "third_party_mode" in reg.names()
    assert isinstance(reg.get("third_party_mode"), Assertion)


def test_load_entry_points_ignores_non_assertion_object(monkeypatch):
    """A loaded object that is not an Assertion subclass is skipped gracefully."""

    class NotAnAssertion:
        pass

    bogus_ep = _FakeEntryPoint("bogus", lambda: NotAnAssertion)

    reg = AssertionRegistry()
    monkeypatch.setattr(
        "agctl.assertion_registry._entry_points",
        lambda group: [bogus_ep],
    )
    reg.load_entry_points()  # no raise
    assert reg.names() == []


def test_default_registry_is_cached():
    """get_default_registry returns the same instance on repeat calls."""
    assert get_default_registry() is get_default_registry()


# --- evaluate_custom: the command-layer bridge to registered modes (§9.3) ---


class _PassMode(Assertion):
    name = "passmode"

    def evaluate(self, context):
        return {"passed": True, "row_count": context["row_count"]}


class _FailMode(Assertion):
    name = "failmode"

    def evaluate(self, context):
        return {"passed": False, "message": "no good", "row_count": context["row_count"]}


class _BoomMode(Assertion):
    name = "boommode"

    def evaluate(self, context):
        raise ValueError("kaboom")


def test_evaluate_custom_pass_returns_detail():
    reg = AssertionRegistry()
    reg.register(_PassMode)
    passed, detail = evaluate_custom("passmode", {"row_count": 3}, registry=reg)
    assert passed is True
    assert detail == {"row_count": 3}


def test_evaluate_custom_fail_raises_assertion_failure():
    reg = AssertionRegistry()
    reg.register(_FailMode)
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_custom("failmode", {"row_count": 1}, registry=reg)
    assert exc_info.value.message == "no good"
    assert exc_info.value.detail["mode"] == "failmode"
    assert exc_info.value.detail["row_count"] == 1


def test_evaluate_custom_unknown_raises_template_missing():
    with pytest.raises(TemplateNotFound):
        evaluate_custom("nope", {}, registry=AssertionRegistry())


def test_evaluate_custom_builtin_raises_config_error():
    """Built-in modes have dedicated flags; --assertion refuses to invoke them."""
    with pytest.raises(ConfigError):
        evaluate_custom("expect_rows", {}, registry=get_default_registry())


def test_evaluate_custom_raising_mode_is_assertion_failure():
    reg = AssertionRegistry()
    reg.register(_BoomMode)
    with pytest.raises(AssertionFailure) as exc_info:
        evaluate_custom("boommode", {}, registry=reg)
    assert "kaboom" in exc_info.value.message
