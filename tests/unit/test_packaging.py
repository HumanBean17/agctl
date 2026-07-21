"""Tests for packaging configuration in pyproject.toml."""

import importlib.metadata
import subprocess
import sys

import tomllib
from pathlib import Path


def _load_pyproject() -> dict:
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        return tomllib.load(f)


def test_jq_extra_exists():
    """Test that the 'jq' optional-dependency extra exists with the correct value."""
    pyproject = _load_pyproject()
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


def test_mysql_extra_exists():
    """Test that the 'mysql' optional-dependency extra exists with PyMySQL + jq."""
    pyproject = _load_pyproject()
    optional_deps = pyproject["project"]["optional-dependencies"]

    assert "mysql" in optional_deps, "mysql extra should exist in optional-dependencies"
    assert optional_deps["mysql"] == ["PyMySQL>=1.1", "jq>=1.6"], (
        "mysql extra should equal ['PyMySQL>=1.1', 'jq>=1.6']"
    )


def test_no_sqlite_extra_exists():
    """Test that NO 'sqlite' extra exists (sqlite3 is stdlib; no extra needed)."""
    pyproject = _load_pyproject()
    optional_deps = pyproject["project"]["optional-dependencies"]

    assert "sqlite" not in optional_deps, (
        "sqlite extra must NOT exist — sqlite3 is stdlib and needs no extra"
    )


def test_db_drivers_entry_points_registered():
    """Test that all three db_drivers entry points are registered in pyproject.toml.

    Reads pyproject.toml directly (not installed metadata) so this test fails
    fast when the source is missing a registration, even before reinstall.
    """
    pyproject = _load_pyproject()
    eps = pyproject["project"]["entry-points"]["agctl.db_drivers"]

    # Exactly three built-in drivers registered.
    assert set(eps.keys()) == {"postgresql", "mysql", "sqlite"}, (
        "db_drivers entry-point group must contain exactly "
        "postgresql, mysql, sqlite"
    )
    assert eps["postgresql"] == "agctl.clients.db_drivers.postgresql:PostgreSQLDriver"
    assert eps["mysql"] == "agctl.clients.db_drivers.mysql:MySQLDriver"
    assert eps["sqlite"] == "agctl.clients.db_drivers.sqlite:SQLiteDriver"


def test_db_drivers_entry_points_discoverable():
    """Test that installed metadata exposes all three db_drivers entry points.

    This complements the pyproject source check above by verifying the
    package's installed entry_points.txt was regenerated on reinstall and
    importlib.metadata can load each entry point to its concrete driver class.
    """
    eps = importlib.metadata.entry_points()
    group = {ep.name: ep for ep in eps.select(group="agctl.db_drivers")}

    assert set(group.keys()) >= {"postgresql", "mysql", "sqlite"}, (
        f"installed db_drivers entry points missing keys; got {sorted(group.keys())}"
    )

    # Each entry point must load to the correct concrete driver class.
    assert group["postgresql"].load().__name__ == "PostgreSQLDriver"
    assert group["mysql"].load().__name__ == "MySQLDriver"
    assert group["sqlite"].load().__name__ == "SQLiteDriver"


def test_db_client_imports_without_pymysql_or_psycopg(monkeypatch):
    """Import-order invariant: importing db_client.py must NOT import pymysql or psycopg.

    Both heavy deps are lazy-imported inside each driver's ``connect()`` /
    ``execute()`` / etc. methods. Populating ``BUILTIN_DRIVERS`` at module
    import time must NOT trigger those imports — otherwise ``import agctl``
    would crash in environments where neither extra is installed.

    Simulates absence by setting ``sys.modules[name] = None`` (Python's
    import machinery treats this as "module does not exist" and raises
    ModuleNotFoundError on ``import <name>``), then forces a fresh import of
    db_client + driver modules and asserts neither heavy dep leaked in.

    Manual state restoration in the ``finally`` block is required because
    ``monkeypatch.delitem`` only restores the modules it deleted — the
    reimport creates NEW module objects (NEW db_client + NEW driver modules)
    that monkeypatch never saw, and leaving them in ``sys.modules`` would
    contaminate downstream tests with phantom class-identity mismatches
    (NEW_DB_CLIENT.BUILTIN_DRIVERS would reference NEW driver classes while
    the OLD driver modules are still cached elsewhere in the session).
    """
    def _relevant(name: str) -> bool:
        return (
            name == "agctl.clients.db_client"
            or name.startswith("agctl.clients.db_drivers")
        )

    # Snapshot the current state of the modules we're about to disrupt so
    # we can restore it exactly after the assertion.
    saved = {name: mod for name, mod in sys.modules.items() if _relevant(name)}

    # Simulate both heavy deps being absent. monkeypatch restores these on
    # teardown (deletes the keys, since they were absent beforehand).
    monkeypatch.setitem(sys.modules, "pymysql", None)
    monkeypatch.setitem(sys.modules, "psycopg", None)

    try:
        # Remove cached modules so the import actually re-runs module
        # top-level code (where the lazy-import invariant lives).
        for name in list(saved.keys()):
            sys.modules.pop(name, None)

        # This must succeed (not raise ModuleNotFoundError) and must not
        # pull in either heavy dep.
        import agctl.clients.db_client  # noqa: F401  (import side-effect test)

        # If db_client or any of its driver modules had imported pymysql or
        # psycopg at module top, the None sentinel would have been replaced
        # with a real module object (or the import would have crashed).
        assert sys.modules.get("pymysql") is None, (
            "db_client.py import must not trigger pymysql import "
            "(lazy-import invariant)"
        )
        assert sys.modules.get("psycopg") is None, (
            "db_client.py import must not trigger psycopg import "
            "(lazy-import invariant)"
        )
    finally:
        # Restore the EXACT prior state. Drop any NEW modules the reimport
        # created that weren't in the snapshot, then put back the snapshot.
        for name in list(sys.modules):
            if _relevant(name) and name not in saved:
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def test_load_drivers_returns_all_three_builtins():
    """Test that DbClient.load_drivers() exposes postgresql, mysql, sqlite."""
    # Import here (not at module top) so the lazy-import invariant test above
    # can run first against a pristine module state.
    from agctl.clients.db_client import DbClient
    from agctl.clients.db_drivers.mysql import MySQLDriver
    from agctl.clients.db_drivers.postgresql import PostgreSQLDriver
    from agctl.clients.db_drivers.sqlite import SQLiteDriver

    drivers = DbClient.load_drivers()

    assert "postgresql" in drivers
    assert "mysql" in drivers
    assert "sqlite" in drivers
    assert drivers["postgresql"] is PostgreSQLDriver
    assert drivers["mysql"] is MySQLDriver
    assert drivers["sqlite"] is SQLiteDriver


def test_python_m_agctl_module_entry():
    """Test that `python -m agctl --help` works as a subprocess (enables daemon spawn)."""
    result = subprocess.run(
        [sys.executable, "-m", "agctl", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"python -m agctl --help failed: {result.stderr}"
    assert "mock" in result.stdout, "mock group should appear in help output"

