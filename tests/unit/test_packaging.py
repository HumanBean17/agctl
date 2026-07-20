"""Tests for packaging configuration in pyproject.toml."""

import subprocess
import sys

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
    # kafka extra gained the schemaregistry transitive deps (Task 15):
    # confluent_kafka.schema_registry imports authlib/cachetools/attrs/certifi/
    # httpx at module load, but confluent-kafka only declares these under its
    # own [schemaregistry] extra, not as core deps. Requesting
    # confluent-kafka[schemaregistry] makes SR work out of the box.
    assert optional_deps["kafka"] == ["confluent-kafka[schemaregistry]>=2.4", "jq>=1.6"], "kafka extra should pull confluent-kafka[schemaregistry]"
    assert optional_deps["db"] == ["psycopg[binary]>=3.1", "jq>=1.6"], "db extra should be unchanged"


def test_python_m_agctl_module_entry():
    """Test that `python -m agctl --help` works as a subprocess (enables daemon spawn)."""
    result = subprocess.run(
        [sys.executable, "-m", "agctl", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"python -m agctl --help failed: {result.stderr}"
    assert "mock" in result.stdout, "mock group should appear in help output"

