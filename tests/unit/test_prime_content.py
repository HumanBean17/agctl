"""Tests for :mod:`agctl.prime_content` — the prime content channel.

The prime topics are a *contract* (spec: 2026-08-19-agctl-prime), so these
tests pin the registry invariants (no dangling entries, no orphan files,
core indexes every topic) and the char budgets that keep prime output cheap:
rendered core ≤ 5,000, hook pointer ≤ 800, every topic ≤ 6,000 and > 200.
"""

from __future__ import annotations

import importlib.resources

import pytest

import agctl
from agctl.errors import ConfigError
from agctl.prime_content import TOPIC_ORDER, read_resource, render_core, topic_text


def _prime_dir():
    return importlib.resources.files("agctl").joinpath("data", "prime")


def _topic_file_stems() -> set[str]:
    return {p.stem for p in _prime_dir().iterdir() if p.is_file()}


def test_core_renders_with_version():
    """The core manual names the installed binary's version, so a stale
    copy-pasted prime output is self-evidently out of date."""
    core = render_core()
    assert f"agctl v{agctl.__version__}" in core
    assert core.startswith("# ")


def test_core_budget():
    """Default `agctl prime` output stays ~1.2k tokens (≤ 5,000 chars)."""
    assert len(render_core()) <= 5_000


def test_registry_no_dangling():
    """Every TOPIC_ORDER entry resolves to a packaged topic file."""
    for name in TOPIC_ORDER:
        assert topic_text(name) is not None, f"topic '{name}' has no file"


def test_registry_no_orphans():
    """Every packaged file under data/prime/ is core.md, hook.md, or a
    registered topic — no unlisted content can ship silently."""
    assert _topic_file_stems() - {"core", "hook"} == set(TOPIC_ORDER)


def test_registry_indexed_in_core():
    """core.md's depth index mentions every registered topic."""
    core = render_core()
    for name in TOPIC_ORDER:
        assert name in core, f"topic '{name}' missing from core index"


def test_read_resource_missing_raises_config_error():
    with pytest.raises(ConfigError) as excinfo:
        read_resource("data", "prime", "nope.md")
    assert "not found in the agctl package" in str(excinfo.value)
    assert excinfo.value.detail["resource"] == "data/prime/nope.md"


def test_topic_budgets_runtime():
    """Runtime topics ship real depth, bounded: 200 < chars ≤ 6,000 each."""
    for name in ("gotchas", "mock", "listen", "grpc"):
        t = topic_text(name)
        assert t is not None, f"topic '{name}' has no file"
        assert t.startswith("# agctl — ")
        assert 200 < len(t) <= 6_000, name
