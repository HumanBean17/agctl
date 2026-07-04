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

    def execute_write(self, sql: str, params: dict) -> dict:
        """Run a write query, returning rows affected and optional RETURNING data.

        ``sql`` uses JDBC-style ``:name`` params; these are rewritten to psycopg
        ``%(name)s`` form via :func:`convert_sql_params` before execution. Returns
        ``{"rows_affected": int | None, "returning": list[dict]}`` where
        ``rows_affected`` is ``None`` for statements that don't report a count
        (e.g., DDL) and ``returning`` contains coerced dict rows when the query
        includes a ``RETURNING`` clause. The transaction is committed after
        result materialization; any exception during execute/fetch/coercion/commit
        triggers a rollback and surfaces as :class:`ConnectionFailure`.
        """
        import psycopg

        rewrite = convert_sql_params(sql)
        cur = self._conn.cursor()
        try:
            # Execute the write query
            cur.execute(rewrite, params)

            # Materialize rows_affected before any fetch
            rowcount = cur.rowcount
            rows_affected = None if rowcount == -1 else rowcount

            # Materialize returning data if present
            returning = []
            if cur.description is not None:
                column_names = [desc.name for desc in cur.description]
                rows = cur.fetchall()
                returning = [
                    {col: coerce_db_value(value) for col, value in zip(column_names, row)}
                    for row in rows
                ]

            # Commit LAST, after materialization is complete
            self._conn.commit()

        except Exception as exc:
            # Rollback on ANY exception (not just psycopg.Error)
            try:
                self._conn.rollback()
            except Exception:
                pass  # Original exception surfaced below; failed rollback is safe to ignore
            # Surface as ConnectionFailure
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        return {"rows_affected": rows_affected, "returning": returning}

    def describe_schema(self, table: str | None, schema: str | None) -> dict:
        """Return the live schema of the connection (DESIGN §7, optional capability).

        This is an optional driver capability (the ``DBDriver`` Protocol is
        unchanged); introspection-capable drivers implement it and the command
        layer probes for a callable ``describe_schema`` before use.

        **Level 1** (``table is None``) lists the relations (tables and views)
        visible in the connection, normalized to::

            {"items": [ {"schema", "name", "kind", "column_count"} ... ]}

        Relations are read from ``pg_class`` joined to ``pg_namespace`` (schema
        name) with a non-dropped user-column count from ``pg_attribute``. The
        v1 scope (spec D5/D6) is enforced here, at the normalization seam:

        - ``relkind`` ordinary/partitioned table (``'r'``/``'p'``) -> ``"table"``,
          view (``'v'``) -> ``"view"``; every other ``relkind`` (materialized
          view ``'m'``, sequence ``'S'``, foreign table, index, TOAST) is
          excluded.
        - System schemas (name starting with ``pg_`` or equal to
          ``information_schema``) are excluded.
        - Partition *leaf* relations (``relispartition = true``) are excluded;
          only partitioned parents and plain tables appear.
        - Items are sorted by ``(schema, name)`` ascending for stable output.

        When ``schema`` is provided it restricts the namespace and is sent as a
        **bind parameter** (never interpolated). An unknown/empty ``schema``
        yields ``{"items": []}`` (not an error).

        ``psycopg`` is lazy-imported and any ``psycopg.Error`` during the catalog
        read surfaces as :class:`ConnectionFailure`; no ``commit()`` is issued
        (read-only catalog SELECTs). The ``table is not None`` (Level 2) branch
        is implemented in Task 3.
        """
        import psycopg

        if table is not None:
            # Level 2 (per-table detail) is implemented in Task 3.
            raise NotImplementedError(
                "describe_schema Level 2 (table detail) is implemented in Task 3"
            )

        # Level 1: relations from pg_class + pg_namespace + a non-dropped
        # user-column count from pg_attribute (attnum > 0 excludes system
        # columns; NOT attisdropped excludes dropped ones).
        base_query = (
            "SELECT n.nspname AS schema_name, c.relname AS relation_name, "
            "c.relkind AS relkind, c.relispartition AS relispartition, "
            "(SELECT count(*) FROM pg_attribute a "
            "WHERE a.attrelid = c.oid AND a.attnum > 0 "
            "AND NOT a.attisdropped) AS column_count "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace"
        )
        if schema is not None:
            sql = base_query + " WHERE n.nspname = %(schema)s"
            params = {"schema": schema}
        else:
            sql = base_query
            params = {}

        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            column_names = [desc.name for desc in (cur.description or [])]
            rows = cur.fetchall()
        except psycopg.Error as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        records = [dict(zip(column_names, row)) for row in rows]

        items = []
        for rec in records:
            relkind = rec.get("relkind")
            if relkind in ("r", "p"):
                kind = "table"
            elif relkind == "v":
                kind = "view"
            else:
                # Exclude matviews ('m'), sequences ('S'), foreign tables,
                # indexes, TOAST, etc. from v1.
                continue

            schema_name = rec.get("schema_name")
            if schema_name is None:
                continue
            if schema_name.startswith("pg_") or schema_name == "information_schema":
                continue
            if rec.get("relispartition"):
                # Partition leaves: only parents appear.
                continue
            if schema is not None and schema_name != schema:
                continue

            items.append(
                {
                    "schema": schema_name,
                    "name": rec.get("relation_name"),
                    "kind": kind,
                    "column_count": rec.get("column_count"),
                }
            )

        items.sort(key=lambda it: (it["schema"], it["name"]))
        return {"items": items}

    def close(self) -> None:
        """Close the connection iff the driver owns it."""
        if self._conn is not None and self._owned:
            self._conn.close()
