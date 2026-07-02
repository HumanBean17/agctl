"""DBDriver protocol (DESIGN §9.1).

A minimal structural protocol describing the contract every database driver
must satisfy. Drivers are registered as entry points (``agctl.db.drivers``)
and selected by the ``DbClient`` based on the ``driver`` config field.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DBDriver(Protocol):
    """Structural contract for a database driver.

    - :meth:`connect` opens (or retains an injected) connection from config.
    - :meth:`execute` runs a read-only SQL statement with named params and
      returns a list of dict rows (column name -> coerced value).
    - :meth:`close` releases the connection if the driver owns it.
    """

    def connect(self, config: dict) -> None: ...

    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]: ...

    def close(self) -> None: ...
