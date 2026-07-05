"""Tests for the envelope capture resolver (Task 3).

``resolve_captures(envelope, captures)`` reads each :class:`CaptureSpec`'s
``from_`` jq path off the live envelope, wraps the result in a typed
:class:`CaptureValue`, and returns ``(typed_map, missing_list)``. Missing
paths (``raw is None``) appear in BOTH the typed map (as
``CaptureValue(None, type)``) and the missing list as ``(name, from_)``
pairs — the soft-miss contract.

These tests exercise ``jq_value`` (and thus the optional ``jq`` extra), so
the module is gated with ``pytest.importorskip("jq")``: it skips rather
than errors where the extra is unavailable, mirroring how other jq-dependent
tests behave.
"""

import pytest

pytest.importorskip("jq")  # skip (don't error) where the jq extra is missing

from agctl.config.models import CaptureSpec
from agctl.errors import ConfigError
from agctl.mock.capture import resolve_captures
from agctl.resolution import CaptureValue


# --- None / empty captures ------------------------------------------------


def test_resolve_captures_none_returns_empty_pair():
    """captures=None -> ({}, [])."""
    typed, missing = resolve_captures({"body": {"x": 1}}, None)
    assert typed == {}
    assert missing == []


def test_resolve_captures_empty_dict_returns_empty_pair():
    """captures={} -> ({}, [])."""
    typed, missing = resolve_captures({"body": {"x": 1}}, {})
    assert typed == {}
    assert missing == []


# --- nested-path extraction -----------------------------------------------


def test_resolve_nested_body_variables_id():
    """Envelope {body.variables.id=7}, capture from='.body.variables.id' ->
    typed map {op_id: CaptureValue(7, 'scalar')}, missing []."""
    envelope = {"body": {"variables": {"id": 7}}}
    captures = {"op_id": CaptureSpec.model_validate({"from": ".body.variables.id"})}
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {"op_id": CaptureValue(7, "scalar")}
    assert missing == []


def test_resolve_headers_authorization():
    """Envelope header extraction -> CaptureValue('Bearer x', 'scalar')."""
    envelope = {"headers": {"authorization": "Bearer x"}}
    captures = {"auth": CaptureSpec.model_validate({"from": ".headers.authorization"})}
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {"auth": CaptureValue("Bearer x", "scalar")}
    assert missing == []


def test_resolve_key_top_level():
    """Top-level '.key' extraction -> CaptureValue('k-1', 'scalar')."""
    envelope = {"key": "k-1"}
    captures = {"tid": CaptureSpec.model_validate({"from": ".key"})}
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {"tid": CaptureValue("k-1", "scalar")}
    assert missing == []


# --- object-typed spec ----------------------------------------------------


def test_resolve_object_type_returns_capture_value_with_object_type():
    """type='object' -> CaptureValue(<live dict>, 'object')."""
    envelope = {"value": {"context": {"conv": "abc"}}}
    captures = {
        "ctx": CaptureSpec.model_validate(
            {"from": ".value.context", "type": "object"}
        )
    }
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {"ctx": CaptureValue({"conv": "abc"}, "object")}
    assert missing == []


# --- missing path -> in BOTH map (as CaptureValue(None, type)) AND list ---


def test_resolve_missing_path_in_both_map_and_list():
    """Missing path '.body.nope' -> typed map {x: CaptureValue(None, 'scalar')}
    AND missing list [('x', '.body.nope')]."""
    envelope = {"body": {}}
    captures = {"x": CaptureSpec.model_validate({"from": ".body.nope"})}
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {"x": CaptureValue(None, "scalar")}
    assert missing == [("x", ".body.nope")]


# --- multiple captures, mixed present/missing -----------------------------


def test_resolve_multiple_captures_one_missing():
    """Two captures: one present, one missing. Both in typed map; only the
    missing one in the missing list, in insertion order."""
    envelope = {"headers": {"authorization": "Bearer y"}, "body": {}}
    captures = {
        "auth": CaptureSpec.model_validate({"from": ".headers.authorization"}),
        "missing_id": CaptureSpec.model_validate({"from": ".body.nope"}),
    }
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {
        "auth": CaptureValue("Bearer y", "scalar"),
        "missing_id": CaptureValue(None, "scalar"),
    }
    assert missing == [("missing_id", ".body.nope")]


def test_resolve_insertion_order_preserved_in_missing_list():
    """Multiple missing captures -> missing list preserves insertion order."""
    envelope = {}
    captures = {
        "a": CaptureSpec.model_validate({"from": ".a"}),
        "b": CaptureSpec.model_validate({"from": ".b"}),
        "c": CaptureSpec.model_validate({"from": ".c"}),
    }
    _, missing = resolve_captures(envelope, captures)
    assert missing == [("a", ".a"), ("b", ".b"), ("c", ".c")]


# --- jq expression error -> treated as missing (NOT a raise) --------------


def test_resolve_bad_jq_expression_treated_as_missing():
    """A malformed jq expression is swallowed by jq_value to None, surfacing
    as a missing entry — consistent with the soft-miss contract. The resolver
    must NOT raise."""
    envelope = {"body": {"x": 1}}
    captures = {"bad": CaptureSpec.model_validate({"from": ")("})}
    typed, missing = resolve_captures(envelope, captures)
    assert typed == {"bad": CaptureValue(None, "scalar")}
    assert missing == [("bad", ")(")]


# --- missing jq library -> ConfigError propagates (NOT swallowed) ---------


def test_resolve_missing_jq_library_propagates_config_error(monkeypatch):
    """When the optional jq library is unavailable, jq_value raises
    ConfigError; resolve_captures must let it propagate (NOT swallow it into
    a missing entry)."""
    import sys

    monkeypatch.setitem(sys.modules, "jq", None)  # block the lazy `import jq`
    envelope = {"body": {"x": 1}}
    captures = {"op_id": CaptureSpec.model_validate({"from": ".body.x"})}
    with pytest.raises(ConfigError):
        resolve_captures(envelope, captures)
