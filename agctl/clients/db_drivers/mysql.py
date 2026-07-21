"""MySQL DBDriver implementation backed by PyMySQL (DESIGN §9.1).

``pymysql`` (the optional ``mysql`` extra) is lazy-imported inside
:meth:`MySQLDriver.connect`, so this module imports cleanly even when the
dependency is absent. A pre-built connection may be injected via
``connectable`` for testing.

Param style: PyMySQL natively accepts ``%(name)s`` placeholders, so this
driver rewrites JDBC-style ``:name`` params via :func:`convert_sql_params`
before execute — the same pattern used by the PostgreSQL driver.
``autocommit=False`` is forced at connect time so the commit / rollback
semantics match PostgreSQL (caller-driven transaction boundaries).
"""

from __future__ import annotations

import urllib.parse
from typing import Any

from ...assertions import coerce_db_value
from ...errors import ConnectionFailure
from ...resolution import convert_sql_params
from ..db_driver_protocol import BaseDBDriver, WriteResult


class MySQLDriver(BaseDBDriver):
    """PyMySQL-backed implementation of the :class:`DBDriver` protocol.

    ``connectable`` is an optional pre-built PyMySQL connection used for test
    injection. When provided, the driver does NOT own the connection and
    :meth:`close` will leave it open. When omitted, :meth:`connect` builds one
    from config and the driver owns its lifecycle.

    Inherits :meth:`BaseDBDriver._redact_config` (scheme-agnostic URL
    userinfo + secret-key redaction) and :meth:`BaseDBDriver._lazy_import_or_raise`
    (pymysql deferred import surfaced as :class:`ConfigError` with the
    ``mysql`` extra hint).

    ``autocommit=False`` is forced on connect so write transactions are
    caller-driven (matching PostgreSQL). SQL params use ``:name`` placeholders
    (the agctl authoring style); these are rewritten to PyMySQL's
    ``%(name)s`` form via :func:`convert_sql_params`.
    """

    def __init__(self, *, connectable=None):
        self._conn = connectable
        # We only own (and thus close) connections we create ourselves.
        self._owned = connectable is None

    def connect(self, config: dict) -> None:
        """Open a PyMySQL connection from ``config`` unless one was injected.

        Config keys:
        - ``url`` (optional) — a ``mysql://user:pass@host:port/dbname`` URI.
          Parsed via :func:`urllib.parse.urlparse`; query-string extras (e.g.
          ``?charset=utf8mb4``) are NOT auto-extracted into kwargs (use the
          ``options`` dict for those).
        - discrete ``host``/``port``/``dbname``/``user``/``password`` —
          override URL values when both are present.
        - ``options`` (dict) — driver-specific extras merged LAST so callers
          can force-override anything (e.g. ``charset``, ``collation``,
          ``connect_timeout``).

        ``autocommit=False`` is always set on the resulting connection (PyMySQL
        defaults to autocommit=False in recent versions, but the explicit kwarg
        makes the transaction semantics caller-driven and matches PostgreSQL).

        A ``pymysql`` import failure raises :class:`ConfigError` pointing at
        the ``mysql`` extra; a connection failure raises
        :class:`ConnectionFailure` with the redacted config in ``detail``.
        """
        if self._conn is not None:
            # Injected connection: nothing to do.
            return

        pymysql = self._lazy_import_or_raise("pymysql", "mysql")

        kwargs: dict[str, Any] = {}

        # URL values first (when present), so discrete fields below override.
        url = config.get("url")
        if url:  # non-empty string
            parsed = urllib.parse.urlparse(url)
            if parsed.hostname:
                kwargs["host"] = parsed.hostname
            if parsed.port:
                kwargs["port"] = parsed.port
            if parsed.username:
                kwargs["user"] = parsed.username
            if parsed.password:
                kwargs["password"] = parsed.password
            # urlparse path is "/<dbname>"; strip leading slash. An empty
            # resulting name (e.g. url had no path) is skipped.
            db_from_path = parsed.path.lstrip("/") if parsed.path else ""
            if db_from_path:
                kwargs["database"] = db_from_path

        # Discrete fields override URL values. PyMySQL uses ``database`` (not
        # ``dbname``) as the kwarg name; map accordingly. Skip Nones so we
        # don't clobber URL-derived values with absent overrides.
        if config.get("host") is not None:
            kwargs["host"] = config["host"]
        if config.get("port") is not None:
            kwargs["port"] = config["port"]
        if config.get("dbname") is not None:
            kwargs["database"] = config["dbname"]
        if config.get("user") is not None:
            kwargs["user"] = config["user"]
        if config.get("password") is not None:
            kwargs["password"] = config["password"]

        # Options merged LAST — caller force-override escape hatch. PyMySQL
        # accepts ``charset``, ``collation``, ``connect_timeout``, etc.
        options = config.get("options") or {}
        if options:
            kwargs.update(options)

        try:
            self._conn = pymysql.connect(autocommit=False, **kwargs)
        except pymysql.Error as exc:
            raise ConnectionFailure(
                message=str(exc),
                detail={
                    "driver": "mysql",
                    "config": self._redact_config(config),
                },
            ) from exc

    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]:
        """Run a read-only query, returning dict rows with coerced cell values.

        ``sql`` uses JDBC-style ``:name`` params; these are rewritten to
        PyMySQL's ``%(name)s`` form via :func:`convert_sql_params` before
        execution. Query / connection errors surface as
        :class:`ConnectionFailure`. No commit is issued (read-only).
        """
        pymysql = self._lazy_import_or_raise("pymysql", "mysql")

        rewrite = convert_sql_params(sql)
        cur = self._conn.cursor()
        try:
            cur.execute(rewrite, params)
            column_names = [desc.name for desc in (cur.description or [])]
            rows = cur.fetchall()
        except pymysql.Error as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        return [
            {col: coerce_db_value(value) for col, value in zip(column_names, row)}
            for row in rows
        ]

    def execute_write(self, sql: str, params: dict) -> WriteResult:
        """Run a write query, returning rows affected.

        ``sql`` uses JDBC-style ``:name`` params; these are rewritten to
        PyMySQL's ``%(name)s`` form via :func:`convert_sql_params` before
        execution. Returns a :class:`WriteResult` with ``rows_affected`` (None
        for statements that don't report a count, e.g. DDL — ``cur.rowcount
        == -1``) and ``returning`` (always ``[]`` — MySQL has no
        ``RETURNING`` clause). The transaction is committed after result
        materialization; any exception during execute/commit triggers a
        rollback and surfaces as :class:`ConnectionFailure`.
        """
        rewrite = convert_sql_params(sql)
        cur = self._conn.cursor()
        try:
            cur.execute(rewrite, params)

            # Materialize rows_affected. MySQL reports -1 for statements
            # without a meaningful count (e.g. CREATE TABLE) on some adapters;
            # normalize to None for the DBDriver contract.
            rowcount = cur.rowcount
            rows_affected = None if rowcount == -1 else rowcount

            # MySQL has no RETURNING clause; description is None for non-SELECT
            # statements, so ``returning`` is always empty.
            returning: list[dict] = []

            # Commit LAST, after materialization is complete.
            self._conn.commit()

        except Exception as exc:
            # Rollback on ANY exception (not just pymysql.Error), mirroring
            # the PostgreSQL driver. A failed rollback is swallowed so the
            # original exception surfaces unchanged.
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
