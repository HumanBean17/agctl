"""DBDriver protocol and canonical DTOs (DESIGN §9.1).

A minimal structural protocol describing the contract every database driver
must satisfy. Drivers are registered as entry points (``agctl.db_drivers``)
and selected by the ``DbClient`` based on the connection's ``type`` config field.

The DTOs defined below (``WriteResult`` and the schema-description family)
normalize driver-specific return shapes into a single canonical structure.
Later tasks (3, 5, 7) import these by name to replace ad-hoc dict contracts
in driver implementations.

:class:`BaseDBDriver` is a non-abstract mixin: drivers inherit from it to
gain access to :meth:`BaseDBDriver._redact_config` and
:meth:`BaseDBDriver._lazy_import_or_raise`. No required overrides.
"""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..errors import ConfigError


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

    Field declaration order mirrors the original dict insertion order used by
    the pre-refactor PostgreSQL driver (``schema, table, kind, columns,
    primary_key, foreign_keys, unique_constraints, comment``) so that
    ``dataclasses.asdict`` + ``json.dumps`` produces byte-identical output to
    the original implementation.
    """

    schema: str
    table: str
    kind: str
    columns: list[ColumnInfo]
    primary_key: list[str]
    foreign_keys: list[ForeignKey]
    unique_constraints: list[UniqueConstraint]
    comment: str | None


class BaseDBDriver:
    """Mixin of shared helpers for :class:`DBDriver` implementations.

    Non-abstract: nothing is required to override. Drivers inherit from this
    mixin only when they want the helpers — the :class:`DBDriver` protocol
    itself is unchanged. Two classmethod helpers plus three class-attribute
    patterns are provided:

    - :meth:`_redact_config` — produce a config dict safe to embed in an error
      ``detail`` (secret-key values masked, URL userinfo masked, original
      untouched).
    - :meth:`_lazy_import_or_raise` — defer an optional driver dependency
      import and surface a missing-extra as :class:`ConfigError` with a
      ``pip install 'agctl[<extra>]'`` hint.
    """

    # Substring match, case-insensitive. Deliberately broad: a key like
    # ``key_password`` or ``api_token`` is caught along with ``password``.
    _SECRET_KEY_PATTERN: re.Pattern = re.compile(
        r"password|secret|token|key", re.IGNORECASE
    )

    # Sentinel substituted in place of redacted secrets / userinfo.
    _REDACTED_SENTINEL: str = "***"

    # Scheme-agnostic URL userinfo prefix: any ``scheme://user:pass@`` (or
    # ``scheme://user@``). Capture group 1 holds ``scheme://`` so the
    # substitution can re-insert it. URLs without userinfo (no ``@`` before
    # the first ``/``) do not match and pass through unchanged.
    _URL_USERINFO_PATTERN: re.Pattern = re.compile(
        r"^([a-zA-Z][a-zA-Z0-9+]*://)[^@/]+@"
    )

    @classmethod
    def _redact_config(cls, config: dict) -> dict:
        """Return a copy of ``config`` safe to embed in an error ``detail``.

        The original ``config`` is NOT mutated (the caller still needs the real
        values for the actual connection attempt). Applied transformations:

        - Any key whose name matches :attr:`_SECRET_KEY_PATTERN`
          (``password``/``secret``/``token``/``key``, case-insensitive) ->
          :attr:`_REDACTED_SENTINEL`.
        - A string ``url`` value with leading ``scheme://user:pass@`` userinfo
          -> userinfo replaced by :attr:`_REDACTED_SENTINEL`
          (``scheme://***@host/...``). URLs without userinfo, or non-string
          ``url`` values, pass through unchanged.
        - All other keys are passed through unchanged.
        """
        redacted: dict = {}
        for key, value in config.items():
            if cls._SECRET_KEY_PATTERN.search(key):
                redacted[key] = cls._REDACTED_SENTINEL
            elif key == "url" and isinstance(value, str):
                redacted[key] = cls._URL_USERINFO_PATTERN.sub(
                    lambda m: f"{m.group(1)}{cls._REDACTED_SENTINEL}@", value
                )
            else:
                redacted[key] = value
        return redacted

    @classmethod
    def _lazy_import_or_raise(cls, module: str, extra: str):
        """Import ``module`` lazily, raising :class:`ConfigError` on ``ImportError``.

        Used by drivers whose backing library is an optional extra: the
        top-level driver module stays importable even when the dependency is
        absent, and the missing-extra surfaces as a :class:`ConfigError`
        pointing at the install command.

        On success returns the imported module. On ``ImportError`` raises
        :class:`ConfigError` with the message
        ``"Database support requires the '<extra>' extra: pip install 'agctl[<extra>]'"``
        chained from the original ``ImportError`` (``__cause__``).
        """
        try:
            return importlib.import_module(module)
        except ImportError as exc:
            raise ConfigError(
                f"Database support requires the '{extra}' extra: "
                f"pip install 'agctl[{extra}]'"
            ) from exc
