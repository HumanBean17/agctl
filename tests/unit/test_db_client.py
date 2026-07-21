"""Unit tests for DbClient entry-point driver dispatch (DESIGN §9.1)."""

import pytest

from agctl.clients.db_client import DbClient
from agctl.clients.db_driver_protocol import (
    ColumnInfo,
    ForeignKey,
    SchemaItem,
    SchemaMatch,
    UniqueConstraint,
    WriteResult,
)
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


class FakeDriverWithExecuteWrite(FakeDriver):
    """Fake driver that supports writes."""

    def execute_write(self, sql, params):
        return {"rows_affected": 2, "returning": [{"id": "x"}]}


class FakeDriverReadOnly(FakeDriver):
    """Read-only fake driver without execute_write."""


class FakeDriverWithNonCallableExecuteWrite(FakeDriver):
    """Fake driver with non-callable execute_write attribute."""

    def __init__(self):
        super().__init__()
        # Set execute_write to a non-callable value
        self.execute_write = "not a method"


class FakeDriverWithDescribeSchema(FakeDriver):
    """Fake driver that supports schema discovery."""

    SCHEMA = {
        "tables": [
            {"name": "users", "schema": "public", "columns": [{"name": "id"}]}
        ]
    }

    def describe_schema(self, table, schema):
        return self.SCHEMA


class FakeDriverWithNonCallableDescribeSchema(FakeDriver):
    """Fake driver with non-callable describe_schema attribute."""

    def __init__(self):
        super().__init__()
        # Set describe_schema to a non-callable value
        self.describe_schema = "not a method"


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

    def test_connect_forwards_url_field_to_driver(self):
        """url flows through the forwarded config dict so the driver sees it."""
        fake = FakeDriver()
        client = DbClient(
            DatabaseConnection(type="postgresql", url="postgresql://u:p@h/d"),
            driver=fake,
        )
        client.connect()
        assert fake.connected_with["url"] == "postgresql://u:p@h/d"

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


class TestExecuteWrite:
    """Tests for DbClient.execute_write with optional-capability probe."""

    def test_driver_with_execute_write_delegates_and_returns_dict(self):
        """Driver WITH execute_write: DbClient delegates and returns dict unchanged."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverWithExecuteWrite(),
        )
        result = client.execute_write("INSERT INTO t (x) VALUES (1)", {"x": 1})
        assert result == {"rows_affected": 2, "returning": [{"id": "x"}]}

    def test_driver_without_execute_write_raises_config_error(self):
        """Driver WITHOUT execute_write: raises ConfigError with driver type."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverReadOnly(),
        )
        with pytest.raises(ConfigError) as exc_info:
            client.execute_write("INSERT INTO t (x) VALUES (1)", {"x": 1})
        assert "does not support writes" in exc_info.value.message
        assert exc_info.value.detail.get("driver") == "postgresql"

    def test_driver_with_non_callable_execute_write_raises_config_error(self):
        """Driver with non-callable execute_write attribute: also raises ConfigError."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverWithNonCallableExecuteWrite(),
        )
        with pytest.raises(ConfigError) as exc_info:
            client.execute_write("INSERT INTO t (x) VALUES (1)", {"x": 1})
        assert "does not support writes" in exc_info.value.message
        assert exc_info.value.detail.get("driver") == "postgresql"


class TestDescribeSchema:
    """Tests for DbClient.supports_describe_schema + describe_schema probe."""

    def test_driver_with_describe_schema_reports_support_and_delegates(self):
        """Driver WITH callable describe_schema: probe True, delegation returns dict unchanged."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverWithDescribeSchema(),
        )
        assert client.supports_describe_schema() is True
        result = client.describe_schema(None, None)
        assert result == FakeDriverWithDescribeSchema.SCHEMA

    def test_driver_without_describe_schema_reports_no_support_and_raises(self):
        """Driver WITHOUT describe_schema: probe False, raises ConfigError with driver type."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverReadOnly(),
        )
        assert client.supports_describe_schema() is False
        with pytest.raises(ConfigError) as exc_info:
            client.describe_schema(None, None)
        assert "does not support schema discovery" in exc_info.value.message
        assert exc_info.value.detail.get("driver") == "postgresql"

    def test_driver_with_non_callable_describe_schema_raises_config_error(self):
        """Driver with non-callable describe_schema attribute: probe False, raises ConfigError."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverWithNonCallableDescribeSchema(),
        )
        assert client.supports_describe_schema() is False
        with pytest.raises(ConfigError) as exc_info:
            client.describe_schema(None, None)
        assert "does not support schema discovery" in exc_info.value.message
        assert exc_info.value.detail.get("driver") == "postgresql"

    def test_supports_describe_schema_does_not_open_connection(self):
        """Pre-connect probe must be side-effect-free: no connection opened."""
        fake = FakeDriverWithDescribeSchema()
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=fake,
        )
        # Probe before any connect(); the driver must not have been connected.
        assert client.supports_describe_schema() is True
        assert fake.connected_with is None


# --- DTO serialization (Task 3) -------------------------------------------
#
# Drivers return DTOs (WriteResult, SchemaItem, SchemaMatch) from their
# execute_write / describe_schema methods. DbClient serializes these to plain
# dicts at the boundary so callers (the `agctl db *` command layer, JSON
# output, agents consuming JSON) see the same shape they saw before the
# refactor — without ever knowing the internal return type changed.


class FakeDriverReturningWriteResult(FakeDriver):
    """Fake driver whose execute_write returns a WriteResult DTO."""

    def execute_write(self, sql, params):
        return WriteResult(rows_affected=3, returning=[{"id": "x"}])


class FakeDriverReturningPlainDict(FakeDriver):
    """Backward-compat fake: driver still returns a plain dict (no asdict crash)."""

    def execute_write(self, sql, params):
        return {"rows_affected": 1, "returning": []}


class FakeDriverReturningSchemaItems(FakeDriver):
    """Fake driver whose describe_schema returns DTOs in `items`."""

    def describe_schema(self, table, schema):
        return {
            "items": [
                SchemaItem(
                    schema="public",
                    name="orders",
                    kind="table",
                    column_count=3,
                )
            ]
        }


class FakeDriverReturningSchemaMatches(FakeDriver):
    """Fake driver whose describe_schema returns DTOs in `matches`."""

    def describe_schema(self, table, schema):
        return {
            "matches": [
                SchemaMatch(
                    schema="public",
                    table="t",
                    kind="table",
                    comment=None,
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="int",
                            nullable=False,
                            default=None,
                            generated=None,
                            enum_values=None,
                            comment=None,
                        )
                    ],
                    primary_key=[],
                    foreign_keys=[
                        ForeignKey(
                            name="t_fk",
                            columns=["id"],
                            references_schema="public",
                            references_table="other",
                            references_columns=["id"],
                        )
                    ],
                    unique_constraints=[
                        UniqueConstraint(name="t_uidx", columns=["code"])
                    ],
                )
            ]
        }


class TestDtoSerialization:
    """Verify DbClient serializes DTO returns into plain dicts at the boundary."""

    def test_execute_write_serializes_write_result_dto(self):
        """WriteResult DTO -> plain dict (same shape, not the dataclass instance)."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverReturningWriteResult(),
        )
        result = client.execute_write("INSERT INTO t (x) VALUES (1)", {"x": 1})

        # The result is a plain dict, not a WriteResult instance.
        assert isinstance(result, dict)
        assert not hasattr(result, "__dataclass_fields__")
        assert result == {"rows_affected": 3, "returning": [{"id": "x"}]}

    def test_execute_write_passes_through_plain_dict(self):
        """Backward compat: a driver that returns a plain dict is not crashed on."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverReturningPlainDict(),
        )
        result = client.execute_write("INSERT INTO t (x) VALUES (1)", {"x": 1})

        assert isinstance(result, dict)
        assert result == {"rows_affected": 1, "returning": []}

    def test_describe_schema_serializes_schema_item_dtos(self):
        """SchemaItem DTOs in `items` -> list of plain dicts."""
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverReturningSchemaItems(),
        )
        result = client.describe_schema(None, None)

        assert result == {
            "items": [
                {"schema": "public", "name": "orders", "kind": "table", "column_count": 3}
            ]
        }
        # No nested DTO survives the boundary.
        assert all(not hasattr(it, "__dataclass_fields__") for it in result["items"])

    def test_describe_schema_serializes_schema_match_dtos_recursively(self):
        """SchemaMatch DTOs in `matches` -> fully-nested dict equivalent.

        The nested ColumnInfo / ForeignKey / UniqueConstraint DTOs must also
        be serialized to plain dicts (dataclasses.asdict recurses).
        """
        client = DbClient(
            DatabaseConnection(type="postgresql", host="h"),
            driver=FakeDriverReturningSchemaMatches(),
        )
        result = client.describe_schema(None, None)

        assert result == {
            "matches": [
                {
                    "schema": "public",
                    "table": "t",
                    "kind": "table",
                    "comment": None,
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "int",
                            "nullable": False,
                            "default": None,
                            "generated": None,
                            "enum_values": None,
                            "comment": None,
                        }
                    ],
                    "primary_key": [],
                    "foreign_keys": [
                        {
                            "name": "t_fk",
                            "columns": ["id"],
                            "references_schema": "public",
                            "references_table": "other",
                            "references_columns": ["id"],
                        }
                    ],
                    "unique_constraints": [
                        {"name": "t_uidx", "columns": ["code"]}
                    ],
                }
            ]
        }
        # No DTO survives at any depth inside the returned structure.
        match = result["matches"][0]
        assert isinstance(match, dict)
        assert all(isinstance(c, dict) for c in match["columns"])
        assert all(isinstance(fk, dict) for fk in match["foreign_keys"])
        assert all(isinstance(u, dict) for u in match["unique_constraints"])
