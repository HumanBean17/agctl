"""DbClient with entry-point driver dispatch (DESIGN §9.1).

Selects a :class:`DBDriver` implementation by the connection's ``type`` field,
discovering third-party drivers via the ``agctl.db_drivers`` entry-point group
while always falling back to the built-in ``postgresql`` driver.
"""

from __future__ import annotations

import dataclasses
import importlib.metadata
from typing import Any

from ..errors import ConfigError
from .db_drivers.postgresql import PostgreSQLDriver

#: Entry-point group used to discover third-party DB drivers.
DB_DRIVER_ENTRY_POINT_GROUP = "agctl.db_drivers"

#: Built-in drivers always available even without entry-point registration.
BUILTIN_DRIVERS: dict[str, type] = {"postgresql": PostgreSQLDriver}


def _serialize_value(value: Any) -> Any:
    """Serialize a single top-level value from a driver's ``describe_schema`` dict.

    - A dataclass instance (e.g. a stray ``SchemaItem``/``SchemaMatch`` at the
      top of the dict) is converted via :func:`dataclasses.asdict`, which
      itself recurses into nested dataclasses / lists / dicts.
    - A list of dataclass instances (the normal case for ``items`` /
      ``matches``) is mapped elementwise through :func:`dataclasses.asdict`,
      leaving non-dataclass elements (strings, ints, plain dicts) untouched.
    - Any other value (plain dict, string, None) passes through unchanged.

    Non-dataclass elements inside a list are passed through as-is so a driver
    that still emits plain dicts at this level (forward-compat / partial
    migration) is not crashed on.
    """
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, list):
        return [
            dataclasses.asdict(item) if dataclasses.is_dataclass(item) else item
            for item in value
        ]
    return value


class DbClient:
    """High-level database client that delegates to a discovered driver.

    The driver is selected by ``connection["type"]``:

    - If ``driver`` is injected (DI), it is used directly and no lookup occurs.
    - Otherwise the driver class is looked up in ``drivers`` (or
      :meth:`load_drivers` by default) and instantiated.
    """

    def __init__(
        self,
        connection: Any,
        *,
        driver: Any = None,
        drivers: dict[str, type] | None = None,
    ) -> None:
        # Normalize the connection into a plain dict (pydantic models expose
        # model_dump(); plain dicts pass through unchanged).
        self._conn_dict = getattr(connection, "model_dump", lambda: connection)()

        if driver is not None:
            self._driver = driver
        else:
            available = drivers if drivers is not None else self.load_drivers()
            db_type = self._conn_dict.get("type")
            if not db_type or db_type not in available:
                raise ConfigError(
                    f"Unknown database type: {db_type}", {"type": db_type}
                )
            driver_class = available[db_type]
            self._driver = driver_class()

    @classmethod
    def load_drivers(cls) -> dict[str, type]:
        """Discover DB drivers via entry points, merging built-ins.

        Returns a ``{type_name: driver_class}`` mapping. The built-in
        ``postgresql`` driver is always present. Broken third-party drivers
        (``.load()`` raising) are skipped rather than crashing discovery.
        """
        drivers: dict[str, type] = {}
        try:
            eps = importlib.metadata.entry_points()
            group = (
                eps.select(group=DB_DRIVER_ENTRY_POINT_GROUP)
                if hasattr(eps, "select")
                else eps.get(DB_DRIVER_ENTRY_POINT_GROUP, [])
            )
        except Exception:  # pragma: no cover - defensive; shouldn't happen
            group = []

        for ep in group:
            try:
                driver_class = ep.load()
            except Exception:
                # A broken third-party driver must not break discovery.
                continue
            drivers[ep.name] = driver_class

        # Built-ins are always available and win over any registration gaps.
        drivers.update(BUILTIN_DRIVERS)
        return drivers

    def connect(self) -> None:
        self._driver.connect(self._conn_dict)

    def execute(self, sql: str, params: dict) -> list[dict]:
        return self._driver.execute(sql, params)

    def execute_write(self, sql: str, params: dict) -> dict:
        """Execute a write SQL statement via the driver's optional execute_write.

        Probes the selected driver for a callable ``execute_write`` attribute.
        Raises ConfigError if the driver lacks this optional capability.

        The driver may return either a plain dict (legacy/forward-compat) or a
        :class:`~agctl.clients.db_driver_protocol.WriteResult` dataclass
        instance; a dataclass instance is serialized to a plain dict via
        :func:`dataclasses.asdict` at this boundary so the JSON shape seen by
        callers is unchanged regardless of the driver's internal return type.

        Returns:
            The dict result of the driver's ``execute_write`` method.
        """
        execute_write_attr = getattr(self._driver, "execute_write", None)
        if not callable(execute_write_attr):
            raise ConfigError(
                f"connection's driver ({self._conn_dict['type']}) does not support writes",
                {"driver": self._conn_dict["type"]},
            )
        result = self._driver.execute_write(sql, params)
        if dataclasses.is_dataclass(result):
            return dataclasses.asdict(result)
        return result

    def supports_describe_schema(self) -> bool:
        """Return True if the driver offers a callable ``describe_schema``.

        This is a **pre-connect**, side-effect-free probe: it does not call
        ``describe_schema`` and does not require a connection. Callers
        (``agctl db schema``) use it to fail fast when the selected driver
        cannot introspect, without opening a connection.

        Returns:
            True if the driver has a callable ``describe_schema`` attribute.
        """
        return callable(getattr(self._driver, "describe_schema", None))

    def describe_schema(self, table: str | None, schema: str | None) -> dict:
        """Delegate to the driver's optional ``describe_schema`` capability.

        Probes :meth:`supports_describe_schema`; raises ConfigError if the
        driver lacks this optional capability. Otherwise delegates to the
        driver and serializes any DTO instances inside the returned dict
        (top-level values and nested-in-list values) into plain dicts via
        :func:`dataclasses.asdict` so the JSON shape seen by callers is
        unchanged regardless of the driver's internal return type.

        Returns:
            The dict returned by the driver's ``describe_schema`` method,
            with any dataclass instances serialized to plain dicts.
        """
        if not self.supports_describe_schema():
            raise ConfigError(
                f"connection's driver ({self._conn_dict['type']}) does not support schema discovery",
                {"driver": self._conn_dict["type"]},
            )
        result = self._driver.describe_schema(table, schema)
        if isinstance(result, dict):
            return {key: _serialize_value(value) for key, value in result.items()}
        return result

    def close(self) -> None:
        self._driver.close()
