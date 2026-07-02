"""PostgreSQL DBDriver implementation backed by psycopg (DESIGN §9.1, §7).

``psycopg`` (the optional ``db`` extra) is lazy-imported inside :meth:`connect`,
so this module imports cleanly even when the dependency is absent. A pre-built
connection may be injected via ``connectable`` for testing.
"""

from __future__ import annotations

from typing import Any

from ...assertions import coerce_db_value
from ...errors import ConfigError, ConnectionFailure
from ...resolution import convert_sql_params


class PostgreSQLDriver:
    """psycopg-backed implementation of the :class:`DBDriver` protocol.

    ``connectable`` is an optional pre-built psycopg connection used for test
    injection. When provided, the driver does NOT own the connection and
    :meth:`close` will leave it open. When omitted, :meth:`connect` builds one
    from config and the driver owns its lifecycle.
    """

    def __init__(self, *, connectable=None):
        self._conn = connectable
        # We only own (and thus close) connections we create ourselves.
        self._owned = connectable is None

    def connect(self, config: dict) -> None:
        """Open a psycopg connection from ``config`` unless one was injected.

        Config keys: ``host``, ``port``, ``dbname``, ``user``, ``password``
        (any subset is forwarded; psycopg applies its own defaults). A
        ``psycopg`` import failure raises :class:`ConfigError` pointing at the
        ``db`` extra; a connection failure raises :class:`ConnectionFailure`.
        """
        if self._conn is not None:
            # Injected connection: nothing to do.
            return

        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise ConfigError(
                "Database support requires the 'db' extra: pip install 'agctl[db]'"
            ) from exc

        kwargs = {}
        for key in ("host", "port", "dbname", "user", "password"):
            if key in config and config[key] is not None:
                kwargs[key] = config[key]

        try:
            self._conn = psycopg.connect(**kwargs)
        except psycopg.Error as exc:
            raise ConnectionFailure(
                message=str(exc),
                detail={"driver": "postgresql", "config": dict(config)},
            ) from exc

    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]:
        """Run a read-only query, returning dict rows with coerced cell values.

        ``sql`` uses JDBC-style ``:name`` params; these are rewritten to psycopg
        ``%(name)s`` form via :func:`convert_sql_params` before execution. Query
        / connection errors surface as :class:`ConnectionFailure`. No commit is
        issued (read-only).
        """
        import psycopg  # local; module already imported in connect() normally

        rewrite = convert_sql_params(sql)
        cur = self._conn.cursor()
        try:
            cur.execute(rewrite, params)
            column_names = [desc.name for desc in (cur.description or [])]
            rows = cur.fetchall()
        except psycopg.Error as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        return [
            {col: coerce_db_value(value) for col, value in zip(column_names, row)}
            for row in rows
        ]

    def close(self) -> None:
        """Close the connection iff the driver owns it."""
        if self._conn is not None and self._owned:
            self._conn.close()
