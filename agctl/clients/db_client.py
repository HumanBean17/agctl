"""DbClient with entry-point driver dispatch (DESIGN §9.1).

Selects a :class:`DBDriver` implementation by the connection's ``type`` field,
discovering third-party drivers via the ``agctl.db_drivers`` entry-point group
while always falling back to the built-in ``postgresql`` driver.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any

from ..errors import ConfigError
from .db_drivers.postgresql import PostgreSQLDriver

#: Entry-point group used to discover third-party DB drivers.
DB_DRIVER_ENTRY_POINT_GROUP = "agctl.db_drivers"

#: Built-in drivers always available even without entry-point registration.
BUILTIN_DRIVERS: dict[str, type] = {"postgresql": PostgreSQLDriver}


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

        Returns:
            The dict returned by the driver's ``execute_write`` method.
        """
        execute_write_attr = getattr(self._driver, "execute_write", None)
        if not callable(execute_write_attr):
            raise ConfigError(
                f"connection's driver ({self._conn_dict['type']}) does not support writes",
                {"driver": self._conn_dict["type"]},
            )
        return self._driver.execute_write(sql, params)

    def close(self) -> None:
        self._driver.close()
