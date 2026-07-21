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
from ..db_driver_protocol import (
    BaseDBDriver,
    ColumnInfo,
    ForeignKey,
    SchemaItem,
    SchemaMatch,
    UniqueConstraint,
    WriteResult,
)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

# System schemas excluded from Level-1/Level-2 listings (DESIGN §9.1).
# MySQL exposes its own data dictionary plus three administrative schemas;
# none of them are user data, so the driver filters them out unconditionally.
_SYSTEM_SCHEMAS: frozenset[str] = frozenset(
    {"mysql", "performance_schema", "information_schema", "sys"}
)


def _parse_mysql_enum(column_type: str | None) -> list[str] | None:
    """Parse a MySQL ``enum('a','b','c')`` column_type into a list of literals.

    MySQL exposes an enum's declared values via the ``column_type`` string in
    ``information_schema.columns`` (e.g. ``enum('new','old','paid')``). There
    is no separate enum-value catalog (PostgreSQL's ``pg_enum`` has no MySQL
    analogue), so the values are parsed out of this string.

    The parser respects single-quote boundaries so an embedded comma inside
    a literal is preserved (``enum('a,b','c')`` -> ``["a,b", "c"]``). A
    doubled single quote inside a literal is decoded as one literal quote
    (``enum('it''s')`` -> ``["it's"]``), matching MySQL's escape convention.

    Returns ``None`` when:

    - ``column_type`` is not a string
    - it does not start with ``enum(`` and end with ``)``
    - any literal is not properly single-quoted
    - the body is empty (``enum()`` -- not a valid MySQL enum)
    - any other parse anomaly

    Never raises: callers can pass arbitrary catalog cells through without a
    try/except. Non-enum column types (``"int"``, ``"varchar(255)"``, etc.)
    return ``None`` -- the test for "is this an enum column?" is just
    ``_parse_mysql_enum(ct) is not None``.
    """
    if not isinstance(column_type, str):
        return None
    prefix = "enum("
    suffix = ")"
    if not column_type.startswith(prefix) or not column_type.endswith(suffix):
        return None
    # ``len(column_type) - len(suffix)`` rather than ``-len(suffix)`` slicing
    # so we don't crash on the prefix itself being shorter than the suffix
    # (defensive; ``enum()`` has body ``""`` here).
    body = column_type[len(prefix) : len(column_type) - len(suffix)]

    literals: list[str] = []
    pos = 0
    n = len(body)
    while pos < n:
        # Skip whitespace and commas between literals.
        while pos < n and body[pos] in " ,":
            pos += 1
        if pos >= n:
            break
        # Each literal must be single-quoted.
        if body[pos] != "'":
            return None
        pos += 1
        chars: list[str] = []
        closed = False
        while pos < n:
            ch = body[pos]
            if ch == "'":
                # Doubled quote -> escaped literal quote; else closing quote.
                if pos + 1 < n and body[pos + 1] == "'":
                    chars.append("'")
                    pos += 2
                    continue
                closed = True
                pos += 1
                break
            chars.append(ch)
            pos += 1
        if not closed:
            # Ran off the end without a closing quote.
            return None
        literals.append("".join(chars))

    # An empty enum (``enum()``) is not a valid MySQL declaration; surface
    # None so callers don't have to special-case ``[]``.
    return literals if literals else None


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
            # PyMySQL's cursor.description is a tuple of 7-tuples
            # (name, type_code, ...) per PEP 249 -- NOT objects with a .name
            # attribute. Index [0] is the column name. This matches the
            # SQLite driver's convention.
            column_names = [desc[0] for desc in (cur.description or [])]
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

    def describe_schema(self, table: str | None, schema: str | None) -> dict:
        """Return the live schema of the connection (DESIGN §7, optional capability).

        This is an optional driver capability (the ``DBDriver`` Protocol is
        unchanged); introspection-capable drivers implement it and the command
        layer probes for a callable ``describe_schema`` before use.

        Catalog reads use MySQL's ``information_schema`` (the ANSI-standard
        schema dictionary). All catalog SQL uses PyMySQL's native ``%s``
        paramstyle with **positional** params (a list/tuple) -- the catalog
        queries are NOT user SQL and do not pass through
        :func:`convert_sql_params` (which is only for JDBC-style ``:name``
        user params). The system schemas (``mysql``, ``performance_schema``,
        ``information_schema``, ``sys``) are filtered out of both Level-1 and
        Level-2 in Python post-filter so the contract is enforced at the
        normalization seam regardless of the SQL filter the server applies.

        **Level 1** (``table is None``) lists user tables and views::

            {"items": [SchemaItem, ...], "matches": []}

        Two catalog SELECTs are issued:

        - ``information_schema.tables`` for ``(table_schema, table_name,
          table_type)``. ``table_type == "BASE TABLE"`` maps to ``"table"``,
          every other value (``"VIEW"``, ``"SYSTEM VIEW"``) maps to ``"view"``.
        - ``information_schema.columns`` aggregated via ``COUNT(*)`` ...
          ``GROUP BY (table_schema, table_name)`` for column counts. Relations
          with zero columns surface with ``column_count=0`` (rare but valid).

        Items are sorted by ``(schema, name)`` ascending for stable output.
        When ``schema`` is provided it restricts both SELECTs (the test
        FakeCursor ignores the filter, but real MySQL enforces it).

        **Level 2** (``table is not None``) returns one :class:`SchemaMatch`
        per relation named ``table`` (exact stored case; views accepted),
        restricted to ``schema`` when given. The driver does NOT disambiguate:
        when ``schema`` is unset and the name exists in multiple schemas,
        every match is returned (the command layer raises on ambiguity).

        Each match carries:

        - ``columns`` from ``information_schema.columns`` (ordered by
          ``ordinal_position``); ``data_type`` is the bare type keyword
          (``"int"``, ``"enum"``); ``nullable`` inverted from ``is_nullable``
          (``"YES"`` -> ``True``); ``default`` from ``column_default``;
          ``generated`` mapped from ``extra`` (``"auto_increment"`` ->
          ``"by_default_identity"`` -- MySQL's AUTO_INCREMENT is closest to
          PostgreSQL's BY DEFAULT identity); ``enum_values`` parsed from
          ``column_type`` via :func:`_parse_mysql_enum` (None for non-enum
          columns); per-column ``comment`` from ``column_comment``.
        - ``primary_key`` columns from ``key_column_usage`` +
          ``table_constraints`` filtered to ``constraint_type='PRIMARY KEY'``.
        - ``unique_constraints`` from the same query filtered to
          ``constraint_type='UNIQUE'``.
        - ``foreign_keys`` from ``referential_constraints`` joined to
          ``key_column_usage`` (local side AND referenced side, joined on
          ``ordinal_position`` for positional pairing of composite FKs).

        The redaction rule from PostgreSQL carries over: when ``generated``
        is non-None, ``default`` is forced to None regardless of the catalog
        value (an auto_increment column has no literal default the agent may
        supply). Catalog cells are emitted verbatim
        (:func:`coerce_db_value` is deliberately not applied).

        ``pymysql`` is lazy-imported and any ``pymysql.Error`` during the
        catalog read surfaces as :class:`ConnectionFailure`; no ``commit()``
        is issued (read-only catalog SELECTs).
        """
        pymysql = self._lazy_import_or_raise("pymysql", "mysql")

        if table is not None:
            # Level 2: one relation's columns + keys. Find every relation
            # named ``table`` (restricted to ``schema`` when given; views
            # accepted), then read each relation's columns and constraints
            # from information_schema. The driver does NOT disambiguate;
            # ``items`` is kept (empty) so the return shape is uniform.
            cur = self._conn.cursor()
            try:
                rel_sql = (
                    "SELECT table_schema, table_name, table_type "
                    "FROM information_schema.tables "
                    "WHERE table_name = %s"
                )
                rel_params: list = [table]
                if schema is not None:
                    rel_sql += " AND table_schema = %s"
                    rel_params.append(schema)
                cur.execute(rel_sql, rel_params)
                # PyMySQL cursor.description is a tuple of 7-tuples per PEP 249;
                # index [0] is the column name (no .name attribute on tuples).
                rel_desc = [d[0] for d in (cur.description or [])]
                rel_rows = cur.fetchall()

                matches: list[SchemaMatch] = []
                for row in rel_rows:
                    rec = dict(zip(rel_desc, row))
                    schema_name = rec.get("table_schema")
                    if schema_name is None:
                        continue
                    if schema_name in _SYSTEM_SCHEMAS:
                        continue
                    if schema is not None and schema_name != schema:
                        continue
                    table_type = rec.get("table_type")
                    kind = "table" if table_type == "BASE TABLE" else "view"
                    matches.append(
                        self._describe_one_relation(
                            cur,
                            schema_name=schema_name,
                            table_name=rec.get("table_name"),
                            kind=kind,
                        )
                    )
            except pymysql.Error as exc:
                raise ConnectionFailure(message=str(exc)) from exc
            finally:
                cur.close()

            return {"items": [], "matches": matches}

        # Level 1: relations from information_schema.tables + a column count
        # from information_schema.columns (aggregated via GROUP BY). Two
        # separate SELECTs so zero-column relations still surface (a single
        # INNER JOIN would drop them).
        cur = self._conn.cursor()
        try:
            rel_sql = (
                "SELECT table_schema, table_name, table_type "
                "FROM information_schema.tables"
            )
            rel_params: list = []
            if schema is not None:
                rel_sql += " WHERE table_schema = %s"
                rel_params = [schema]
            cur.execute(rel_sql, rel_params)
            # PyMySQL cursor.description is a tuple of 7-tuples per PEP 249;
            # index [0] is the column name (no .name attribute on tuples).
            rel_desc = [d[0] for d in (cur.description or [])]
            rel_rows = cur.fetchall()

            count_sql = (
                "SELECT table_schema, table_name, COUNT(*) AS column_count "
                "FROM information_schema.columns"
            )
            count_params: list = []
            if schema is not None:
                count_sql += " WHERE table_schema = %s"
                count_params = [schema]
            count_sql += " GROUP BY table_schema, table_name"
            cur.execute(count_sql, count_params)
            # PyMySQL cursor.description is a tuple of 7-tuples per PEP 249;
            # index [0] is the column name (no .name attribute on tuples).
            count_desc = [d[0] for d in (cur.description or [])]
            count_rows = cur.fetchall()
        except pymysql.Error as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

        counts: dict[tuple[str, str], int] = {}
        for row in count_rows:
            rec = dict(zip(count_desc, row))
            counts[(rec.get("table_schema"), rec.get("table_name"))] = (
                rec.get("column_count") or 0
            )

        items: list[SchemaItem] = []
        for row in rel_rows:
            rec = dict(zip(rel_desc, row))
            schema_name = rec.get("table_schema")
            if schema_name is None:
                continue
            if schema_name in _SYSTEM_SCHEMAS:
                continue
            if schema is not None and schema_name != schema:
                continue
            table_type = rec.get("table_type")
            kind = "table" if table_type == "BASE TABLE" else "view"
            items.append(
                SchemaItem(
                    schema=schema_name,
                    name=rec.get("table_name"),
                    kind=kind,
                    column_count=counts.get((schema_name, rec.get("table_name")), 0),
                )
            )

        items.sort(key=lambda it: (it.schema or "", it.name or ""))
        return {"items": items, "matches": []}

    def _describe_one_relation(
        self, cur, *, schema_name: str, table_name: str, kind: str
    ) -> SchemaMatch:
        """Build the normalized Level-2 :class:`SchemaMatch` for one relation.

        Issues the per-relation catalog SELECTs (columns, PK/unique
        constraints, FKs) on the shared ``cur``. Any ``pymysql.Error``
        propagates to :meth:`describe_schema`'s mapping to
        :class:`ConnectionFailure`. Catalog cells are emitted verbatim
        (:func:`coerce_db_value` is deliberately not applied). Returns a
        :class:`SchemaMatch` DTO whose ``columns``/``foreign_keys``/
        ``unique_constraints`` carry :class:`ColumnInfo`, :class:`ForeignKey`,
        and :class:`UniqueConstraint` instances respectively.
        """
        # Columns (information_schema.columns). data_type is the bare type
        # keyword ("int", "enum", "varchar"); column_type is the full type
        # string ("int", "enum('a','b')", "varchar(255)"). extra holds
        # "auto_increment" for AUTO_INCREMENT columns (otherwise "").
        cur.execute(
            "SELECT column_name, data_type, is_nullable, column_default, "
            "extra, column_type, column_comment "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            [schema_name, table_name],
        )
        # PyMySQL cursor.description is a tuple of 7-tuples per PEP 249;
        # index [0] is the column name (no .name attribute on tuples).
        col_desc = [d[0] for d in (cur.description or [])]

        columns: list[ColumnInfo] = []
        for row in cur.fetchall():
            crec = dict(zip(col_desc, row))
            extra = crec.get("extra") or ""
            if extra == "auto_increment":
                generated = "by_default_identity"
            else:
                generated = None
            # Redaction rule (load-bearing): auto_increment columns have no
            # literal default the agent may supply, regardless of what
            # column_default returned.
            default = None if generated is not None else crec.get("column_default")
            data_type = crec.get("data_type")
            column_type = crec.get("column_type")
            enum_values = (
                _parse_mysql_enum(column_type) if data_type == "enum" else None
            )
            columns.append(
                ColumnInfo(
                    name=crec.get("column_name"),
                    data_type=data_type,
                    nullable=(crec.get("is_nullable") == "YES"),
                    default=default,
                    generated=generated,
                    enum_values=enum_values,
                    comment=crec.get("column_comment"),
                )
            )

        # Constraints (key_column_usage + table_constraints): PRIMARY KEY and
        # UNIQUE only. Other constraint types (FOREIGN KEY handled below,
        # CHECK not surfaced in v1) are filtered out. PK members are emitted
        # in ordinal_position order so composite PRIMARY KEY(a, b) -> ["a",
        # "b"] rather than catalog-storage order.
        cur.execute(
            "SELECT kcu.constraint_name AS constraint_name, "
            "tc.constraint_type AS constraint_type, "
            "kcu.column_name AS column_name, "
            "kcu.ordinal_position AS ordinal_position "
            "FROM information_schema.key_column_usage kcu "
            "JOIN information_schema.table_constraints tc "
            "ON kcu.constraint_name = tc.constraint_name "
            "AND kcu.table_schema = tc.table_schema "
            "WHERE kcu.table_schema = %s AND kcu.table_name = %s "
            "AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') "
            "ORDER BY kcu.constraint_name, kcu.ordinal_position",
            [schema_name, table_name],
        )
        # PyMySQL cursor.description is a tuple of 7-tuples per PEP 249;
        # index [0] is the column name (no .name attribute on tuples).
        con_desc = [d[0] for d in (cur.description or [])]
        primary_key: list[str] = []
        unique_constraints: list[UniqueConstraint] = []
        # Track unique-constraint columns by constraint name to preserve
        # ordinal_position order across rows.
        unique_cols_by_name: dict[str, list[str]] = {}
        unique_order: list[str] = []
        for row in cur.fetchall():
            crec = dict(zip(con_desc, row))
            ctype = crec.get("constraint_type")
            cname = crec.get("constraint_name")
            col = crec.get("column_name")
            if ctype == "PRIMARY KEY":
                primary_key.append(col)
            elif ctype == "UNIQUE":
                if cname not in unique_cols_by_name:
                    unique_cols_by_name[cname] = []
                    unique_order.append(cname)
                unique_cols_by_name[cname].append(col)
        for cname in unique_order:
            unique_constraints.append(
                UniqueConstraint(name=cname, columns=unique_cols_by_name[cname])
            )

        # Foreign keys (referential_constraints + key_column_usage on both
        # sides). The first JOIN to key_column_usage gives local columns;
        # the second JOIN to key_column_usage gives referenced columns. The
        # join condition ``kcu.ordinal_position = ku.ordinal_position``
        # preserves positional pairing for composite FKs (FOREIGN KEY(a, b)
        # REFERENCES t(x, y) -> columns=["a", "b"], references_columns=["x",
        # "y"] aligned by ordinal position within the constraint).
        cur.execute(
            "SELECT rc.constraint_name AS constraint_name, "
            "kcu.column_name AS column_name, "
            "kcu.ordinal_position AS ordinal_position, "
            "ku.table_schema AS ref_schema, "
            "ku.table_name AS ref_table, "
            "ku.column_name AS ref_column, "
            "ku.ordinal_position AS ref_ordinal "
            "FROM information_schema.referential_constraints rc "
            "JOIN information_schema.key_column_usage kcu "
            "ON rc.constraint_schema = kcu.table_schema "
            "AND rc.constraint_name = kcu.constraint_name "
            "JOIN information_schema.key_column_usage ku "
            "ON rc.unique_constraint_schema = ku.constraint_schema "
            "AND rc.unique_constraint_name = ku.constraint_name "
            "AND kcu.ordinal_position = ku.ordinal_position "
            "WHERE rc.constraint_schema = %s AND kcu.table_name = %s "
            "ORDER BY rc.constraint_name, kcu.ordinal_position",
            [schema_name, table_name],
        )
        # PyMySQL cursor.description is a tuple of 7-tuples per PEP 249;
        # index [0] is the column name (no .name attribute on tuples).
        fk_desc = [d[0] for d in (cur.description or [])]
        # Group FK columns by constraint name, preserving ordinal_position
        # order so composite FKs surface as a single ForeignKey DTO.
        fk_cols_by_name: dict[str, list[str]] = {}
        fk_ref_cols_by_name: dict[str, list[str]] = {}
        fk_ref_schema: dict[str, str | None] = {}
        fk_ref_table: dict[str, str | None] = {}
        fk_order: list[str] = []
        for row in cur.fetchall():
            frec = dict(zip(fk_desc, row))
            cname = frec.get("constraint_name")
            if cname not in fk_cols_by_name:
                fk_cols_by_name[cname] = []
                fk_ref_cols_by_name[cname] = []
                fk_ref_schema[cname] = frec.get("ref_schema")
                fk_ref_table[cname] = frec.get("ref_table")
                fk_order.append(cname)
            fk_cols_by_name[cname].append(frec.get("column_name"))
            fk_ref_cols_by_name[cname].append(frec.get("ref_column"))

        foreign_keys: list[ForeignKey] = []
        for cname in fk_order:
            foreign_keys.append(
                ForeignKey(
                    name=cname,
                    columns=fk_cols_by_name[cname],
                    references_schema=fk_ref_schema[cname],
                    references_table=fk_ref_table[cname],
                    references_columns=fk_ref_cols_by_name[cname],
                )
            )

        return SchemaMatch(
            schema=schema_name,
            table=table_name,
            kind=kind,
            comment=None,  # MySQL v1: no table-level comment surfaced.
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            unique_constraints=unique_constraints,
        )

    def close(self) -> None:
        """Close the connection iff the driver owns it."""
        if self._conn is not None and self._owned:
            self._conn.close()
