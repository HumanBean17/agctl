"""SQLite DBDriver implementation backed by stdlib ``sqlite3`` (DESIGN §9.1).

``sqlite3`` is in the Python standard library, so this module imports cleanly
with no optional-extra requirement and no lazy-import dance. A pre-built
connection may be injected via ``connectable`` for testing.

Unlike the PostgreSQL driver, no SQL parameter rewriting is performed: Python's
``sqlite3`` module natively accepts JDBC-style ``:name`` placeholders, so SQL
is passed through to ``cur.execute`` unchanged.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from ...assertions import coerce_db_value
from ...errors import ConnectionFailure
from ..db_driver_protocol import BaseDBDriver, WriteResult


class SQLiteDriver(BaseDBDriver):
    """``sqlite3``-backed implementation of the :class:`DBDriver` protocol.

    ``connectable`` is an optional pre-built ``sqlite3.Connection`` used for
    test injection. When provided, the driver does NOT own the connection and
    :meth:`close` will leave it open. When omitted, :meth:`connect` builds one
    from config and the driver owns its lifecycle.

    Inherits :meth:`BaseDBDriver._redact_config` (used in
    :class:`ConnectionFailure` detail). No SQL parameter rewriting is needed:
    ``sqlite3`` accepts ``:name`` placeholders natively.
    """

    def __init__(self, *, connectable=None):
        self._conn = connectable
        # We only own (and thus close) connections we create ourselves.
        self._owned = connectable is None

    def connect(self, config: dict) -> None:
        """Open a ``sqlite3`` connection from ``config`` unless one was injected.

        Config keys: ``url`` (optional database spec — a path, ``":memory:"``,
        or a ``file:`` URI). Defaults to ``":memory:"`` when absent. A
        ``file:``-prefixed ``url`` enables SQLite URI mode (so query-string
        options like ``?cache=shared`` are honored); any other value is treated
        as a plain path. Connection errors surface as :class:`ConnectionFailure`
        with the redacted config in ``detail``.
        """
        if self._conn is not None:
            # Injected connection: nothing to do.
            return

        url = config.get("url") or ":memory:"
        # Detect URI mode: only `file:`-prefixed URLs are passed with uri=True
        # so SQLite honors the query string (e.g. ?cache=shared). Anything else
        # — a filesystem path, ":memory:" — is treated as a plain database spec.
        uri_mode = isinstance(url, str) and url.startswith("file:")

        try:
            self._conn = sqlite3.connect(
                url,
                uri=uri_mode,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
        except sqlite3.Error as exc:
            raise ConnectionFailure(
                message=str(exc),
                detail={
                    "driver": "sqlite",
                    "config": self._redact_config(config),
                },
            ) from exc

    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]:
        """Run a read-only query, returning dict rows with coerced cell values.

        ``sql`` is passed through unchanged (``sqlite3`` accepts ``:name``
        placeholders natively — no ``convert_sql_params`` rewrite). Query /
        connection errors surface as :class:`ConnectionFailure`. No commit is
        issued (read-only).
        """
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            column_names = [desc[0] for desc in (cur.description or [])]
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        return [
            {col: coerce_db_value(value) for col, value in zip(column_names, row)}
            for row in rows
        ]

    def execute_write(self, sql: str, params: dict) -> WriteResult:
        """Run a write query, returning rows affected and optional RETURNING data.

        ``sql`` is passed through unchanged. Returns a :class:`WriteResult` with
        ``rows_affected`` (None for statements that don't report a count —
        ``cur.rowcount == -1`` — e.g. DDL) and ``returning`` (coerced dict rows
        when the query includes a ``RETURNING`` clause, SQLite >= 3.35; empty
        otherwise). The transaction is committed after result materialization;
        any exception during execute/fetch/coercion/commit triggers a rollback
        and surfaces as :class:`ConnectionFailure`.
        """
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)

            # Materialize RETURNING data first if present (SQLite >= 3.35).
            # Doing this before computing rows_affected lets the inferred
            # count below fall back to len(returning) when rowcount is
            # uninformative (the INSERT...RETURNING case).
            returning: list[dict] = []
            if cur.description is not None:
                column_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                returning = [
                    {col: coerce_db_value(value) for col, value in zip(column_names, row)}
                    for row in rows
                ]

            # Materialize rows_affected. SQLite reports -1 for statements
            # without a meaningful count (e.g. CREATE TABLE). For
            # INSERT/UPDATE/DELETE ... RETURNING, sqlite3 reports rowcount=0
            # (the cursor enters a SELECT-like code path and skips
            # sqlite3_changes()), diverging from psycopg which reports the
            # actual modified count. Normalize: when RETURNING data is
            # present AND rowcount is not positive, fall back to
            # len(returning) so the DBDriver contract is consistent across
            # drivers (a downstream agent can rely on rows_affected).
            rowcount = cur.rowcount
            if rowcount == -1:
                rows_affected = None
            elif rowcount <= 0 and cur.description is not None:
                rows_affected = len(returning)
            else:
                rows_affected = rowcount

            # Commit LAST, after materialization is complete.
            self._conn.commit()

        except Exception as exc:
            # Rollback on ANY exception (not just sqlite3.Error), mirroring the
            # PostgreSQL driver. A failed rollback is swallowed so the original
            # exception surfaces unchanged.
            try:
                self._conn.rollback()
            except Exception:
                pass
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        return WriteResult(rows_affected=rows_affected, returning=returning)

    def close(self) -> None:
        """Close the connection iff the driver owns it."""
        if self._conn is not None and self._owned:
            self._conn.close()
