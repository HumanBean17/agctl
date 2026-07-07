"""Tests for PartialConfig overlay model."""

import pytest
from pydantic import ValidationError

from agctl.config.models import Config, HttpTemplate, PartialConfig


def test_partial_config_accepts_empty_dict():
    """PartialConfig.model_validate({}) returns instance with version=None and default-empty sections."""
    result = PartialConfig.model_validate({})
    assert result.version is None
    assert result.templates == {}
    assert result.services == {}
    assert result.mocks is None


def test_partial_config_accepts_version():
    """PartialConfig.model_validate({"version": "2"}) returns version="2"."""
    result = PartialConfig.model_validate({"version": "2"})
    assert result.version == "2"


def test_partial_config_validates_sections_without_version():
    """PartialConfig validates sections even without version key."""
    result = PartialConfig.model_validate(
        {
            "templates": {
                "t": {
                    "method": "GET",
                    "service": "svc",
                    "path": "/"
                }
            }
        }
    )
    assert isinstance(result.templates["t"], HttpTemplate)
    assert result.templates["t"].method == "GET"
    assert result.templates["t"].service == "svc"
    assert result.templates["t"].path == "/"


def test_base_config_requires_version():
    """Config.model_validate({}) raises ValidationError (version still required on base model)."""
    with pytest.raises(ValidationError) as exc_info:
        Config.model_validate({})
    # Verify the error is about the missing version field
    errors = exc_info.value.errors()
    assert any(err["loc"] == ("version",) for err in errors)


# Task 2: deep_merge tests
from agctl.config.loader import deep_merge


def test_deep_merge_addition():
    """Overlay key absent in base → merged has it; overrides empty."""
    base = {"a": 1}
    overlay = {"b": 2}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"a": 1, "b": 2}
    assert base == {"a": 1, "b": 2}  # mutated in place
    assert overrides == []


def test_deep_merge_scalar_override():
    """Both have scalar at same key → overlay wins; override recorded."""
    base = {"a": 1}
    overlay = {"a": 2}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"a": 2}
    assert overrides == [{"path": "a", "overlay": "sidecar.yaml"}]


def test_deep_merge_list_replace():
    """Lists replace (not extend); override recorded."""
    base = {"brokers": ["x"]}
    overlay = {"brokers": ["y", "z"]}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"brokers": ["y", "z"]}
    assert overrides == [{"path": "brokers", "overlay": "sidecar.yaml"}]


def test_deep_merge_nested_dict_merge():
    """Nested dicts merge key-by-key; only leaf override recorded."""
    base = {"templates": {"keep": 1, "shared": {"m": "GET"}}}
    overlay = {"templates": {"new": 2, "shared": {"m": "POST"}}}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {
        "templates": {
            "keep": 1,
            "new": 2,
            "shared": {"m": "POST"}
        }
    }
    assert overrides == [{"path": "templates.shared.m", "overlay": "sidecar.yaml"}]


def test_deep_merge_type_clash():
    """Dict vs scalar at same key → override wins and is recorded."""
    base = {"x": {"a": 1}}
    overlay = {"x": "scalar"}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"x": "scalar"}
    assert overrides == [{"path": "x", "overlay": "sidecar.yaml"}]


def test_deep_merge_dotted_path_nesting():
    """Dotted path builds correctly across multiple levels."""
    base = {"kafka": {"patterns": {"foo": {"match": "old"}}}}
    overlay = {"kafka": {"patterns": {"foo": {"match": "new"}}}}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"kafka": {"patterns": {"foo": {"match": "new"}}}}
    assert overrides == [{"path": "kafka.patterns.foo.match", "overlay": "sidecar.yaml"}]
