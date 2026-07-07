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
