"""Tests for --param tuple parsing (DESIGN D2)."""

import pytest

from agctl.errors import ConfigError
from agctl.params import parse_params


def test_empty_tuple_returns_empty_dict():
    assert parse_params(()) == {}


def test_multiple_params():
    assert parse_params(("a=1", "b=2")) == {"a": "1", "b": "2"}


def test_value_may_contain_equals():
    # Split on FIRST '=' only.
    assert parse_params(("x=a=b",)) == {"x": "a=b"}


def test_bare_value_without_equals_raises():
    with pytest.raises(ConfigError):
        parse_params(("bare",))


def test_mixed_valid_and_invalid_raises():
    with pytest.raises(ConfigError):
        parse_params(("a=1", "nope"))


def test_single_param():
    assert parse_params(("key=val",)) == {"key": "val"}
