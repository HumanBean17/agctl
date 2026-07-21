"""SQLite DBDriver implementation backed by stdlib ``sqlite3`` (DESIGN §9.1).

``sqlite3`` is in the Python standard library, so this module imports cleanly
with no optional-extra requirement and no lazy-import dance. A pre-built
connection may be injected via ``connectable`` for testing.

Unlike the PostgreSQL driver, no SQL parameter rewriting is performed: Python's
``sqlite3`` module natively accepts JDBC-style ``:name`` placeholders, so SQL
is passed through to ``cur.execute`` unchanged.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

from ...assertions import coerce_db_value
from ...errors import ConnectionFailure
from ..db_driver_protocol import (
    BaseDBDriver,
    ColumnInfo,
    ForeignKey,
    SchemaItem,
    SchemaMatch,
    UniqueConstraint,
    WriteResult,
)

# Identifier whitelist for PRAGMA interpolation. PRAGMAs do NOT accept bind
# parameters in sqlite3, so any identifier that goes into a PRAGMA must first
# pass this regex. Rejection (not escaping) is the policy: a name that doesn't
# match raises ConnectionFailure before it reaches a PRAGMA. Permissive-but-
# strict: letters, digits, underscore; must start with letter or underscore.
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str) -> None:
    """Raise :class:`ConnectionFailure` if ``name`` is not a safe identifier.

    PRAGMAs cannot be parameterized in ``sqlite3``; identifiers are
    interpolated via f-string. This validator is the ONLY gate that an
    identifier passes through before reaching a PRAGMA. Anything outside
    ``^[a-zA-Z_][a-zA-Z0-9_]*$`` (whitespace, punctuation, quotes, semicolons)
    is REJECTED — there is no escaping fallback. The offending identifier is
    echoed in the error ``detail`` so downstream callers can self-correct.
    """
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ConnectionFailure(
            message=f"Invalid identifier: {name!r}",
            detail={"identifier": name},
        )


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

    def describe_schema(
        self, table: str | None, schema: str | None
    ) -> dict:
        """Return the live schema (DESIGN §7, optional capability).

        ``schema`` is accepted for protocol symmetry but IGNORED for v1:
        SQLite always reports the attached DB as ``"main"``, and multi-schema
        support (attached DBs) is deferred. A non-``None`` non-``"main"``
        ``schema`` value is silently accepted and the result still uses
        ``"main"``.

        **Level 1** (``table is None``): lists the relations visible in
        ``sqlite_master``, filtered to user-created tables and views and
        sorted by name ascending. Returns ``{"items": [SchemaItem, ...],
        "matches": []}`` (Level-1 entry). Internal ``sqlite_%`` tables
        (auto-created for FK/unique constraints) are excluded.

        **Level 2** (``table is not None``): returns one
        :class:`SchemaMatch` per relation named ``table`` (exact case match).
        SQLite identifiers are case-sensitive but relations named identically
        across types do not exist in practice (a name is either a table or a
        view), so the matches list holds at most one entry. Columns, PK, FKs,
        and unique constraints are read from PRAGMAs.

        **Identifier safety.** PRAGMAs do NOT accept bind parameters in
        ``sqlite3``, so identifiers are interpolated via f-string AFTER passing
        through :func:`_validate_identifier` (regex
        ``^[a-zA-Z_][a-zA-Z0-9_]*$``). Rejection is the policy; there is no
        escaping fallback. Any ``sqlite3.Error`` during the catalog read
        surfaces as :class:`ConnectionFailure`. No commit is issued (read-only).

        **SQLite-specific normalizations.** FKs are anonymous in
        ``PRAGMA foreign_key_list`` output → :class:`ForeignKey.name` is
        ``None``. SQLite has no native enum type or column/relation comments →
        ``enum_values`` and ``comment`` are always ``None``. Generated columns
        are detected via ``PRAGMA table_xinfo``'s ``hidden`` flag (2 or 3 →
        ``generated="stored"``; else ``None``).
        """
        cur = self._conn.cursor()
        try:
            if table is not None:
                # Level 2: validate BEFORE any interpolation; then look up.
                _validate_identifier(table)
                cur.execute(
                    "SELECT name, type FROM sqlite_master "
                    "WHERE name = ? AND type IN ('table', 'view')",
                    (table,),
                )
                rel_rows = cur.fetchall()
                matches = [
                    self._describe_one_relation(
                        cur, table_name=name, kind=kind
                    )
                    for name, kind in rel_rows
                ]
                return {"items": [], "matches": matches}

            # Level 1: relations from sqlite_master, sqlite_% filtered in SQL.
            cur.execute(
                "SELECT name, type FROM sqlite_master "
                "WHERE type IN ('table', 'view') "
                "AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name"
            )
            rel_rows = cur.fetchall()

            items: list[SchemaItem] = []
            for name, kind in rel_rows:
                _validate_identifier(name)
                col_count_cur = self._conn.cursor()
                try:
                    col_count_cur.execute(f"PRAGMA table_info({name})")
                    column_count = len(col_count_cur.fetchall())
                finally:
                    col_count_cur.close()
                items.append(
                    SchemaItem(
                        schema="main",
                        name=name,
                        kind=("table" if kind == "table" else "view"),
                        column_count=column_count,
                    )
                )
            return {"items": items, "matches": []}
        except sqlite3.Error as exc:
            raise ConnectionFailure(message=str(exc)) from exc
        finally:
            cur.close()

    def _describe_one_relation(
        self, cur, *, table_name: str, kind: str
    ) -> SchemaMatch:
        """Build the normalized Level-2 :class:`SchemaMatch` for one relation.

        Issues the per-relation PRAGMAs (columns, PK, FKs, unique indexes) on
        a cursor derived from the connection (PRAGMAs cannot share the looping
        cursor without clobbering the Level-2 iteration state). ``table_name``
        is assumed already validated by the caller
        (:func:`_validate_identifier`). Returns a :class:`SchemaMatch` with
        nested :class:`ColumnInfo` / :class:`ForeignKey` /
        :class:`UniqueConstraint` DTOs.

        SQLite PRAGMA output shapes (verified empirically):
          - ``table_xinfo``: ``(cid, name, type, notnull, dflt_value, pk, hidden)``
          - ``foreign_key_list``: ``(id, seq, table, from, to, on_update,
            on_delete, match)`` — no name column → FK is anonymous.
          - ``index_list``: ``(seq, name, unique, origin, partial)`` —
            ``origin`` is ``'u'`` for a UNIQUE constraint, ``'c'`` for a
            user-created UNIQUE INDEX, ``'p'`` for the implicit PK index.
            Both ``'u'`` and ``'c'`` are surfaced in ``unique_constraints``.
          - ``index_info``: ``(seqno, cid, name)``.
        """
        _validate_identifier(table_name)

        # Columns + hidden flag (generated-column detection). table_xinfo is a
        # superset of table_info with an extra trailing `hidden` column; using
        # it for both avoids a second PRAGMA round-trip for column metadata.
        xinfo_cur = self._conn.cursor()
        try:
            xinfo_cur.execute(f"PRAGMA table_xinfo({table_name})")
            xinfo_rows = xinfo_cur.fetchall()
        finally:
            xinfo_cur.close()

        columns: list[ColumnInfo] = []
        primary_key: list[str] = []
        for cid, name, data_type, notnull, dflt_value, pk, hidden in xinfo_rows:
            # nullable inverts notnull; PK flag drives primary_key ordering.
            nullable = not notnull
            if pk:
                primary_key.append(name)
            # hidden: 0 = normal, 1 = hidden (e.g. implicit), 2/3 = generated
            # (virtual/stored). Map 2/3 -> "stored" per DESIGN §7; else None.
            generated = "stored" if hidden in (2, 3) else None
            columns.append(
                ColumnInfo(
                    name=name,
                    data_type=data_type,
                    nullable=nullable,
                    default=dflt_value,
                    generated=generated,
                    enum_values=None,  # SQLite has no enum type.
                    comment=None,  # SQLite has no native comment metadata.
                )
            )

        # Foreign keys (anonymous in SQLite — foreign_key_list emits no name).
        fk_cur = self._conn.cursor()
        try:
            fk_cur.execute(f"PRAGMA foreign_key_list({table_name})")
            fk_rows = fk_cur.fetchall()
        finally:
            fk_cur.close()

        foreign_keys: list[ForeignKey] = [
            ForeignKey(
                name=None,
                columns=[from_col],
                references_schema=None,
                references_table=ref_table,
                references_columns=[to_col],
            )
            for (_id, _seq, ref_table, from_col, to_col, _on_upd, _on_del, _match)
            in fk_rows
        ]

        # Unique constraints + unique indexes: SQLite surfaces both via
        # index_list — origin 'u' is a named UNIQUE constraint (column-level
        # or table-level), origin 'c' is a user-created CREATE UNIQUE INDEX,
        # and origin 'p' is the implicit PK index. The Level-2 contract
        # (brief scenario 3) expects a CREATE UNIQUE INDEX to appear in
        # unique_constraints, so we include both 'u' and 'c' and exclude only
        # the PK ('p'). Each retained index gets its columns read via
        # index_info.
        unique_constraints: list[UniqueConstraint] = []
        idx_list_cur = self._conn.cursor()
        idx_info_cur = self._conn.cursor()
        try:
            idx_list_cur.execute(f"PRAGMA index_list({table_name})")
            for _seq, idx_name, _unique, origin, _partial in idx_list_cur.fetchall():
                if origin not in ("u", "c"):
                    continue
                _validate_identifier(idx_name)
                idx_info_cur.execute(f"PRAGMA index_info({idx_name})")
                cols = [row[2] for row in idx_info_cur.fetchall()]
                unique_constraints.append(
                    UniqueConstraint(name=idx_name, columns=cols)
                )
        finally:
            idx_list_cur.close()
            idx_info_cur.close()

        return SchemaMatch(
            schema="main",
            table=table_name,
            kind=kind,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            unique_constraints=unique_constraints,
            comment=None,
        )

    def close(self) -> None:
        """Close the connection iff the driver owns it."""
        if self._conn is not None and self._owned:
            self._conn.close()
