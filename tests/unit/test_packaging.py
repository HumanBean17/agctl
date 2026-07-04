"""Tests for packaging configuration in pyproject.toml."""

import tomllib
from pathlib import Path


def test_jq_extra_exists():
    """Test that the 'jq' optional-dependency extra exists with the correct value."""
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    optional_deps = pyproject["project"]["optional-dependencies"]

    # Assert jq extra exists with exactly ["jq>=1.6"]
    assert "jq" in optional_deps, "jq extra should exist in optional-dependencies"
    assert optional_deps["jq"] == ["jq>=1.6"], "jq extra should equal ['jq>=1.6']"

    # Assert other extras are unchanged
    assert optional_deps["http"] == ["httpx>=0.27"], "http extra should be unchanged"
    assert optional_deps["kafka"] == ["confluent-kafka>=2.4", "jq>=1.6"], "kafka extra should be unchanged"
    assert optional_deps["db"] == ["psycopg[binary]>=3.1", "jq>=1.6"], "db extra should be unchanged"
