"""DBDriver protocol and canonical DTOs (DESIGN §9.1).

A minimal structural protocol describing the contract every database driver
must satisfy. Drivers are registered as entry points (``agctl.db_drivers``)
and selected by the ``DbClient`` based on the connection's ``type`` config field.

The DTOs defined below (``WriteResult`` and the schema-description family)
normalize driver-specific return shapes into a single canonical structure.
Later tasks (3, 5, 7) import these by name to replace ad-hoc dict contracts
in driver implementations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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


@dataclass
class WriteResult:
    """Result of an execute_write call.

    ``rows_affected`` is the count of rows touched (or None if the driver
    cannot report it). ``returning`` holds the rows produced by a RETURNING /
    OUTPUT clause (empty list when none).
    """

    rows_affected: int | None
    returning: list[dict] = field(default_factory=list)


@dataclass
class ColumnInfo:
    """Description of a single column.

    ``generated`` is None or one of ``"always_identity"``,
    ``"by_default_identity"``, ``"stored"``. ``enum_values`` is None for
    non-enum columns.
    """

    name: str
    data_type: str
    nullable: bool
    default: str | None = None
    generated: str | None = None
    enum_values: list[str] | None = None
    comment: str | None = None


@dataclass
class ForeignKey:
    """A single FOREIGN KEY constraint on a table."""

    name: str
    columns: list[str]
    references_schema: str | None
    references_table: str
    references_columns: list[str]


@dataclass
class UniqueConstraint:
    """A single UNIQUE constraint on a table."""

    name: str
    columns: list[str]


@dataclass
class SchemaItem:
    """One row of ``describe_schema``'s listing output.

    ``kind`` is ``"table"`` or ``"view"``. ``column_count`` is the number of
    columns in the table/view.
    """

    schema: str
    name: str
    kind: str
    column_count: int


@dataclass
class SchemaMatch:
    """Schema match for a single table or view from ``describe_schema``.

    ``columns`` is ordered to match the underlying catalog. ``primary_key``
    lists the PK column names (empty list if none). ``foreign_keys`` and
    ``unique_constraints`` carry the table's constraints.
    """

    schema: str
    table: str
    kind: str
    comment: str | None
    columns: list[ColumnInfo]
    primary_key: list[str]
    foreign_keys: list[ForeignKey]
    unique_constraints: list[UniqueConstraint]
