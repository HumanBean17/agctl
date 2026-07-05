"""PostgreSQL DBDriver implementation backed by psycopg (DESIGN §9.1, §7).

``psycopg`` (the optional ``db`` extra) is lazy-imported inside :meth:`connect`,
so this module imports cleanly even when the dependency is absent. A pre-built
connection may be injected via ``connectable`` for testing.
"""

from __future__ import annotations

import re
from typing import Any

from ...assertions import coerce_db_value
from ...errors import ConfigError, ConnectionFailure
from ...resolution import convert_sql_params


# Secret-style keys redacted wholesale before a config copy enters any error
# detail (substring match, case-insensitive). Deliberately broad: a key like
# `key_password` or `api_token` is caught along with `password`.
_SECRET_KEY_PATTERN = re.compile(r"password|secret|token|key", re.IGNORECASE)

# Matches a Postgres URL's ``user:pass@`` (or ``user@``) userinfo prefix so it
# can be replaced with a sentinel — e.g. ``postgres://u:p4ss@h:5432/db`` ->
# ``postgres://***@h:5432/db``. URLs without userinfo (no ``@`` before the first
# ``/``) are left untouched. ``postgres(ql)://`` scheme only.
_URL_USERINFO_PATTERN = re.compile(r"^(postgres(?:ql)?://)[^@/]+@")

_REDACTED_SENTINEL = "***"


def _redact_config(config: dict) -> dict:
    """Return a copy of ``config`` safe to embed in an error ``detail``.

    The original ``config`` is NOT mutated (the caller still needs the real
    values for the actual connection attempt). Applied transformations:

    - Any key whose name contains ``password``/``secret``/``token``/``key``
      (case-insensitive) -> ``"***"``.
    - A ``url`` string with leading ``user:pass@`` userinfo -> the userinfo is
      replaced by ``"***"`` (scheme + host kept). URLs without userinfo, or
      non-string ``url`` values, pass through unchanged.
    """
    redacted: dict = {}
    for key, value in config.items():
        if _SECRET_KEY_PATTERN.search(key):
            redacted[key] = _REDACTED_SENTINEL
        elif key == "url" and isinstance(value, str):
            redacted[key] = _URL_USERINFO_PATTERN.sub(
                lambda m: f"{m.group(1)}{_REDACTED_SENTINEL}@", value
            )
        else:
            redacted[key] = value
    return redacted


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

        Config keys: ``url`` (optional connection URI, e.g.
        ``postgresql://user:pass@host:port/dbname``) and/or the discrete
        ``host``, ``port``, ``dbname``, ``user``, ``password`` fields. When
        ``url`` is set it is passed to psycopg as the conninfo string; any
        discrete fields present are forwarded as kwargs and **override** the
        URI params (merge semantics — psycopg lets kwargs win). A ``psycopg``
        import failure raises :class:`ConfigError` pointing at the ``db`` extra;
        a connection failure raises :class:`ConnectionFailure`.
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

        # conninfo (positional) when a url is given; discrete fields override.
        # A missing or empty url (e.g. "${DB_URL:-}" resolving to "") falls back
        # to discrete fields — see DatabaseConnection.url.
        url = config.get("url")
        args = (url,) if url else ()

        try:
            self._conn = psycopg.connect(*args, **kwargs)
        except psycopg.Error as exc:
            raise ConnectionFailure(
                message=str(exc),
                detail={"driver": "postgresql", "config": _redact_config(config)},
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
        (read-only catalog SELECTs).

        **Level 2** (``table is not None``) returns one ``matches`` entry per
        relation named ``table`` (exact stored case; views accepted), restricted
        to ``schema`` when given. The driver does NOT disambiguate: when
        ``schema`` is unset and the name exists in multiple schemas, every match
        is returned (the command layer raises on ambiguity). Each entry carries
        columns (``name``, ``data_type`` verbatim from ``format_type``,
        ``nullable`` inverted from ``attnotnull``, ``default`` from
        ``pg_get_expr`` -- redacted to ``null`` when ``generated`` is non-null,
        ``generated`` from ``attidentity``/``attgenerated``, ``enum_values``
        from ``pg_enum`` for enum-typed columns, per-column ``comment``),
        ``primary_key``, ``foreign_keys`` (with positional ``conkey``/
        ``confkey`` pairing preserved, self-references handled), and
        ``unique_constraints`` (``contype == 'u'`` only). Catalog cells are
        emitted verbatim (``coerce_db_value`` is deliberately not applied).
        ``items`` is kept (empty) so the return shape is uniform across both
        branches.
        """
        import psycopg

        if table is not None:
            # Level 2: one relation's columns + keys. Find every relation named
            # ``table`` (restricted to ``schema`` when given; views accepted),
            # then read each relation's columns, defaults, enum values,
            # comments, and constraints from pg_catalog. The driver does NOT
            # disambiguate: when ``schema`` is unset and the name exists in
            # multiple schemas, every match is returned (the command layer
            # raises on ambiguity). ``items`` is kept (empty) so the return
            # shape is uniform across both branches.
            cur = self._conn.cursor()
            try:
                rel_sql = (
                    "SELECT c.oid AS oid, n.nspname AS schema_name, "
                    "c.relname AS relation_name, c.relkind AS relkind, "
                    "c.relispartition AS relispartition "
                    "FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relname = %(table)s"
                )
                rel_params: dict[str, Any] = {"table": table}
                if schema is not None:
                    rel_sql += " AND n.nspname = %(schema)s"
                    rel_params["schema"] = schema
                cur.execute(rel_sql, rel_params)
                rel_desc = [d.name for d in (cur.description or [])]
                rel_rows = cur.fetchall()

                matches = []
                for row in rel_rows:
                    rec = dict(zip(rel_desc, row))
                    relkind = rec.get("relkind")
                    if relkind in ("r", "p"):
                        kind = "table"
                    elif relkind == "v":
                        kind = "view"
                    else:
                        # Same v1 scope as Level 1 (matviews/sequences/etc.).
                        continue
                    schema_name = rec.get("schema_name")
                    if schema_name is None:
                        continue
                    if schema_name.startswith("pg_") or schema_name == "information_schema":
                        continue
                    if rec.get("relispartition"):
                        continue
                    if schema is not None and schema_name != schema:
                        continue
                    matches.append(
                        self._describe_one_relation(
                            cur,
                            oid=rec.get("oid"),
                            schema_name=schema_name,
                            table_name=rec.get("relation_name"),
                            kind=kind,
                        )
                    )
            except psycopg.Error as exc:
                raise ConnectionFailure(message=str(exc)) from exc
            finally:
                cur.close()

            return {"items": [], "matches": matches}

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

    def _describe_one_relation(
        self, cur, *, oid, schema_name, table_name, kind
    ) -> dict:
        """Build the normalized Level-2 dict for one relation.

        Issues the per-relation catalog SELECTs (columns, defaults, enum
        values, comments, constraints) on the shared ``cur``. Any
        ``psycopg.Error`` propagates to :meth:`describe_schema`'s mapping to
        :class:`ConnectionFailure`. Catalog cells are emitted verbatim
        (``coerce_db_value`` is deliberately not applied).
        """
        # Columns (pg_attribute + pg_type for data_type/typtype). attnum > 0
        # excludes system columns; NOT attisdropped excludes dropped ones.
        cur.execute(
            "SELECT a.attnum AS attnum, a.attname AS attname, "
            "format_type(a.atttypid, a.atttypmod) AS data_type, "
            "a.attnotnull AS attnotnull, a.attidentity AS attidentity, "
            "a.attgenerated AS attgenerated, t.typtype AS typtype, "
            "a.atttypid AS atttypid "
            "FROM pg_attribute a "
            "JOIN pg_type t ON t.oid = a.atttypid "
            "WHERE a.attrelid = %(oid)s AND a.attnum > 0 "
            "AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            {"oid": oid},
        )
        col_desc = [d.name for d in (cur.description or [])]
        attnum_to_name: dict[int, str] = {}
        raw_columns = []
        for row in cur.fetchall():
            crec = dict(zip(col_desc, row))
            attnum_to_name[crec.get("attnum")] = crec.get("attname")
            raw_columns.append(crec)

        # Defaults (pg_attrdef rendered through pg_get_expr -> already text).
        cur.execute(
            "SELECT adnum AS adnum, "
            "pg_get_expr(adbin, adrelid) AS default_expr "
            "FROM pg_attrdef WHERE adrelid = %(oid)s",
            {"oid": oid},
        )
        def_desc = [d.name for d in (cur.description or [])]
        defaults: dict[int, str] = {}
        for row in cur.fetchall():
            drec = dict(zip(def_desc, row))
            defaults[drec.get("adnum")] = drec.get("default_expr")

        # Enum values (pg_enum) — one batched query for all enum types on this
        # relation, in declared (enumsortorder) order, grouped by type oid.
        enum_values_by_typid: dict[int, list[str]] = {}
        enum_typids = sorted(
            {c.get("atttypid") for c in raw_columns if c.get("typtype") == "e"}
        )
        if enum_typids:
            cur.execute(
                "SELECT enumtypid AS enum_typid, enumlabel AS enum_label "
                "FROM pg_enum WHERE enumtypid = ANY(%(typids)s) "
                "ORDER BY enumtypid, enumsortorder",
                {"typids": enum_typids},
            )
            enum_desc = [d.name for d in (cur.description or [])]
            for row in cur.fetchall():
                erec = dict(zip(enum_desc, row))
                enum_values_by_typid.setdefault(
                    erec.get("enum_typid"), []
                ).append(erec.get("enum_label"))

        # Comments (pg_description): objsubid 0 -> table, >0 -> column attnum.
        cur.execute(
            "SELECT objsubid AS objsubid, description AS description "
            "FROM pg_description WHERE objoid = %(oid)s",
            {"oid": oid},
        )
        com_desc = [d.name for d in (cur.description or [])]
        column_comments: dict[int, str] = {}
        table_comment = None
        for row in cur.fetchall():
            corec = dict(zip(com_desc, row))
            subid = corec.get("objsubid")
            if subid == 0:
                table_comment = corec.get("description")
            elif subid:
                column_comments[subid] = corec.get("description")

        # Normalize columns (data_type verbatim, nullable inverted, generated
        # mapped, default redacted when generated is non-null, enum_values
        # only for enum types, column comment or None).
        columns = []
        for crec in raw_columns:
            attnum = crec.get("attnum")
            attidentity = crec.get("attidentity") or ""
            attgenerated = crec.get("attgenerated") or ""
            if attidentity == "a":
                generated = "always_identity"
            elif attidentity == "d":
                generated = "by_default_identity"
            elif attgenerated == "s":
                generated = "stored"
            else:
                generated = None
            # Redaction rule (load-bearing): identity/stored-generated columns
            # have no literal default the agent may supply, regardless of what
            # pg_attrdef returned.
            default = None if generated is not None else defaults.get(attnum)
            if crec.get("typtype") == "e":
                enum_values = list(enum_values_by_typid.get(crec.get("atttypid"), []))
            else:
                enum_values = None
            columns.append(
                {
                    "name": crec.get("attname"),
                    "data_type": crec.get("data_type"),
                    "nullable": not crec.get("attnotnull"),
                    "default": default,
                    "generated": generated,
                    "enum_values": enum_values,
                    "comment": column_comments.get(attnum),
                }
            )

        # Constraints (pg_constraint): PK/FK/unique. conkey/confkey are
        # attnum arrays resolved to names via the maps above/below; array
        # order is the constraint's column order and the FK pairing, preserved.
        cur.execute(
            "SELECT con.conname AS conname, con.contype AS contype, "
            "con.conkey AS conkey, con.confkey AS confkey, "
            "rn.nspname AS ref_schema, rf.relname AS ref_table, "
            "con.confrelid AS ref_oid "
            "FROM pg_constraint con "
            "LEFT JOIN pg_class rf ON rf.oid = con.confrelid "
            "LEFT JOIN pg_namespace rn ON rn.oid = rf.relnamespace "
            "WHERE con.conrelid = %(oid)s "
            "AND con.contype IN ('p', 'f', 'u')",
            {"oid": oid},
        )
        con_desc = [d.name for d in (cur.description or [])]
        primary_key: list[str] = []
        foreign_keys: list[dict] = []
        unique_constraints: list[dict] = []
        # Cache of referenced-relation attnum->name maps; pre-seeded with this
        # relation so self-referencing FKs (confrelid == own oid) reuse the
        # local map without an extra round trip.
        ref_attnum_cache: dict[Any, dict[int, str]] = {oid: dict(attnum_to_name)}
        for row in cur.fetchall():
            crec = dict(zip(con_desc, row))
            contype = crec.get("contype")
            conkey = crec.get("conkey") or []
            conname = crec.get("conname")
            if contype == "p":
                primary_key = [
                    attnum_to_name[k]
                    for k in conkey
                    if attnum_to_name.get(k) is not None
                ]
            elif contype == "u":
                cols = [
                    attnum_to_name[k]
                    for k in conkey
                    if attnum_to_name.get(k) is not None
                ]
                unique_constraints.append({"name": conname, "columns": cols})
            elif contype == "f":
                local_cols = [
                    attnum_to_name[k]
                    for k in conkey
                    if attnum_to_name.get(k) is not None
                ]
                ref_oid = crec.get("ref_oid")
                confkey = crec.get("confkey") or []
                if ref_oid not in ref_attnum_cache:
                    cur.execute(
                        "SELECT a.attnum AS ref_attnum, a.attname AS ref_attname "
                        "FROM pg_attribute a "
                        "WHERE a.attrelid = %(ref_oid)s AND a.attnum > 0 "
                        "AND NOT a.attisdropped",
                        {"ref_oid": ref_oid},
                    )
                    ref_desc = [d.name for d in (cur.description or [])]
                    ref_map: dict[int, str] = {}
                    for rrow in cur.fetchall():
                        rrec = dict(zip(ref_desc, rrow))
                        ref_map[rrec.get("ref_attnum")] = rrec.get("ref_attname")
                    ref_attnum_cache[ref_oid] = ref_map
                ref_map = ref_attnum_cache[ref_oid]
                ref_cols = [
                    ref_map[k]
                    for k in confkey
                    if ref_map.get(k) is not None
                ]
                foreign_keys.append(
                    {
                        "name": conname,
                        "columns": local_cols,
                        "references_schema": crec.get("ref_schema"),
                        "references_table": crec.get("ref_table"),
                        "references_columns": ref_cols,
                    }
                )

        # Standalone unique indexes (CREATE UNIQUE INDEX ...): these have NO
        # pg_constraint entry, so the constraint query above misses them.
        # Query pg_index for unique, non-primary, non-partial indexes that do
        # NOT back a pg_constraint (the NOT EXISTS excludes indexes already
        # captured above, preventing double-counting), and append each to
        # unique_constraints in the same {"name", "columns"} shape so the
        # constraints-then-indexes ordering is preserved. indkey is an
        # int2vector that psycopg may return as a string ("1 2") or a list
        # ([1, 2]); normalize defensively and map attnums to names via the
        # attnum_to_name map built above (skip any not found, mirroring the
        # PK/FK mapping).
        cur.execute(
            "SELECT c.relname AS indexname, i.indkey AS indkey "
            "FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE i.indrelid = %(oid)s "
            "AND i.indisunique "
            "AND NOT i.indisprimary "
            "AND i.indpred IS NULL "
            "AND NOT EXISTS ("
            "SELECT 1 FROM pg_constraint con "
            "WHERE con.conindid = i.indexrelid) "
            "ORDER BY c.relname",
            {"oid": oid},
        )
        idx_desc = [d.name for d in (cur.description or [])]
        for row in cur.fetchall():
            irec = dict(zip(idx_desc, row))
            raw_indkey = irec.get("indkey") or []
            if isinstance(raw_indkey, str):
                attnums = [int(part) for part in raw_indkey.split()]
            else:
                attnums = [int(k) for k in raw_indkey]
            cols = [
                attnum_to_name[k]
                for k in attnums
                if attnum_to_name.get(k) is not None
            ]
            unique_constraints.append(
                {"name": irec.get("indexname"), "columns": cols}
            )

        return {
            "schema": schema_name,
            "table": table_name,
            "kind": kind,
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys,
            "unique_constraints": unique_constraints,
            "comment": table_comment,
        }

    def close(self) -> None:
        """Close the connection iff the driver owns it."""
        if self._conn is not None and self._owned:
            self._conn.close()
