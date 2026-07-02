"""Unit tests for DbClient entry-point driver dispatch (DESIGN §9.1)."""

import pytest

from agctl.clients.db_client import DbClient
from agctl.clients.db_drivers.postgresql import PostgreSQLDriver
from agctl.config.models import DatabaseConnection
from agctl.errors import ConfigError


class FakeDriver:
    """Minimal DBDriver test double."""

    def __init__(self):
        self.connected_with = None
        self.executed = []
        self.closed = False

    def connect(self, config):
        self.connected_with = config

    def execute(self, sql, params):
        self.executed.append((sql, params))
        return [{"id": "o9", "status": "CONFIRMED"}]

    def close(self):
        self.closed = True


class FakeDriverSubclass(FakeDriver):
    """Distinct class so tests can assert which driver was selected."""


class TestDbClientDirectInjection:
    def test_execute_returns_driver_rows(self):
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"), driver=FakeDriver()
        )
        rows = client.execute("SELECT 1", {})
        assert rows == [{"id": "o9", "status": "CONFIRMED"}]
        assert client._driver.executed == [("SELECT 1", {})]

    def test_connect_forwards_config_dict(self):
        fake = FakeDriver()
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h", port=5432), driver=fake
        )
        client.connect()
        forwarded = fake.connected_with
        assert forwarded["type"] == "postgresql"
        assert forwarded["host"] == "h"
        assert forwarded["port"] == 5432

    def test_close_sets_driver_closed(self):
        fake = FakeDriver()
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"), driver=fake
        )
        client.close()
        assert fake.closed is True

    def test_accepts_plain_dict_connection(self):
        client = DbClient({"type": "postgresql", "host": "h"}, driver=FakeDriver())
        # Underlying connection dict is the plain dict as-is.
        assert client._conn_dict == {"type": "postgresql", "host": "h"}


class TestLoadDrivers:
    def test_includes_builtin_postgresql(self):
        drivers = DbClient.load_drivers()
        assert "postgresql" in drivers
        assert drivers["postgresql"] is PostgreSQLDriver

    def test_returns_dict(self):
        drivers = DbClient.load_drivers()
        assert isinstance(drivers, dict)


class TestDriverSelection:
    def test_unknown_type_raises_config_error(self):
        with pytest.raises(ConfigError) as exc_info:
            DbClient({"type": "mysql"})
        assert "mysql" in exc_info.value.message
        assert exc_info.value.detail.get("type") == "mysql"

    def test_missing_type_raises_config_error(self):
        with pytest.raises(ConfigError):
            DbClient({"host": "h"})

    def test_custom_drivers_dict_selected(self):
        # Provide a custom drivers map; the selected driver should be our
        # FakeDriverSubclass, instantiated by DbClient.
        client = DbClient(
            {"type": "mysql", "host": "h"},
            drivers={"mysql": FakeDriverSubclass},
        )
        assert isinstance(client._driver, FakeDriverSubclass)

        rows = client.execute("SELECT 1", {})
        assert rows == [{"id": "o9", "status": "CONFIRMED"}]

    def test_custom_drivers_dict_connect(self):
        client = DbClient(
            {"type": "mysql", "host": "h"},
            drivers={"mysql": FakeDriverSubclass},
        )
        client.connect()
        assert client._driver.connected_with == {"type": "mysql", "host": "h"}

    def test_custom_drivers_dict_close(self):
        client = DbClient(
            {"type": "mysql", "host": "h"},
            drivers={"mysql": FakeDriverSubclass},
        )
        client.close()
        assert client._driver.closed is True
