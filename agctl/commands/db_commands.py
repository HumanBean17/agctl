"""`db query` and `db assert` commands (DESIGN §3.3, D8).

- ``db query`` resolves a named DB template (or free-form SQL), runs it against
  a configured database connection via :class:`DbClient`, and returns the rows.
- ``db assert`` runs the same resolution+execution, then applies one of two
  assertion modes: ``--expect-rows <n>`` (row count) or ``--expect-value``
  (jq path + D8 type-aware value equality against the first row).

Both commands are wrapped in the success/error :func:`envelope`.
"""

from __future__ import annotations

from typing import Any

import click

from ..assertions import coerce_db_value, jq_value, parse_equals, type_aware_equal
from ..command import envelope, load_config_or_raise
from ..errors import AssertionFailure, ConfigError, TemplateNotFound
from ..params import parse_params

__all__ = [
    "db_query",
    "db_assert",
    "db_execute",
    "db_schema",
    "new_db_client",
    "resolve_db_request",
]


def new_db_client(connection_obj: Any):
    """Build a real :class:`DbClient` for a connection object.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(db_commands, "new_db_client", factory)``) to return a
    client wrapping a FakeDriver, avoiding any real DB connection.
    """
    from ..clients.db_client import DbClient

    return DbClient(connection_obj)


def resolve_connection_name(
    cfg,
    *,
    connection_name: str | None,
    template_connection: str | None = None,
) -> str:
    """Resolve the database connection name to use for a query/assert/execute.

    Precedence: explicit ``connection_name`` > ``template_connection`` >
    ``cfg.defaults.database_connection``. Raises :class:`ConfigError` if none
    of the three is set, or if the resolved name is absent from
    ``cfg.database.connections``.
    """
    resolved_name = connection_name
    if resolved_name is None:
        resolved_name = template_connection
    if resolved_name is None:
        resolved_name = cfg.defaults.database_connection
    if resolved_name is None:
        raise ConfigError("No database connection specified", {})

    if resolved_name not in cfg.database.connections:
        raise ConfigError(
            f"Unknown database connection: {resolved_name}",
            {"connection": resolved_name},
        )
    return resolved_name


def resolve_db_request(
    cfg,
    *,
    template: str | None,
    sql: str | None,
    param_tuple: tuple[str, ...],
    connection_name: str | None,
):
    """Resolve (sql_text, params, connection_name) for a query/assert.

    - ``template`` and ``sql`` are mutually exclusive: exactly one must be given
      (else :class:`ConfigError`).
    - If ``template``: look it up in ``cfg.database.templates``; missing ->
      :class:`TemplateNotFound`. ``sql_text`` comes from the template; the
      template's own ``connection`` may inform connection resolution.
    - If ``sql``: ``sql_text`` is the free-form caller SQL.
    - ``params = parse_params(param_tuple)``.
    - Connection resolution precedence: explicit ``connection_name`` arg >
      template's ``.connection`` (if a template) >
      ``cfg.defaults.database_connection``. None resolved -> ConfigError; a
      resolved name absent from ``cfg.database.connections`` -> ConfigError.
    """
    if template is not None and sql is not None:
        raise ConfigError(
            "--template and --sql are mutually exclusive", {}
        )
    if template is None and sql is None:
        raise ConfigError(
            "Either --template or --sql must be given", {}
        )

    template_connection: str | None = None
    if template is not None:
        if template not in cfg.database.templates:
            raise TemplateNotFound(
                f"Unknown database template: {template}",
                {"path": f"database.templates.{template}"},
            )
        tpl = cfg.database.templates[template]
        sql_text = tpl.sql
        template_connection = tpl.connection
    else:
        sql_text = sql  # type: ignore[assignment]

    params = parse_params(param_tuple)

    # Connection resolution: explicit arg > template connection > default.
    resolved_name = resolve_connection_name(
        cfg,
        connection_name=connection_name,
        template_connection=template_connection,
    )

    return sql_text, params, resolved_name


def _execute(cfg, sql_text: str, params: dict, conn_name: str):
    """Open a client, run the query, close it, and return the rows."""
    client = new_db_client(cfg.database.connections[conn_name])
    try:
        client.connect()
        rows = client.execute(sql_text, params)
    finally:
        client.close()
    return rows


def _check_template_mode(cfg, template_name: str | None, forbidden: str) -> None:
    """Check if a template's mode is forbidden.

    If ``template_name`` is not None and the template's ``mode`` equals
    ``forbidden``, raise :class:`ConfigError`. This is a shared helper for
    commands that need to reject read-mode or write-mode templates.
    """
    if template_name is None:
        return
    # template_name is guaranteed valid by resolve_db_request
    template = cfg.database.templates[template_name]
    if template.mode == forbidden:
        raise ConfigError(
            f"Template '{template_name}' has mode '{forbidden}', which is not allowed for this operation",
            {"template": template_name, "mode": forbidden},
        )


# --------------------------------------------------------------------------- #
# db query
# --------------------------------------------------------------------------- #


def _db_query_core(
    config_path: str | None,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
    overlay_paths: list[str] | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    sql_text, params, conn_name = resolve_db_request(
        cfg,
        template=template,
        sql=sql,
        param_tuple=param,
        connection_name=connection,
    )
    _check_template_mode(cfg, template, forbidden="write")
    rows = _execute(cfg, sql_text, params, conn_name)
    return {"rows": rows, "row_count": len(rows), "connection": conn_name}


@click.command("query")
@click.option("--template", "template", default=None, help="Named DB template")
@click.option("--sql", "sql", default=None, help="Free-form SQL text")
@click.option("--param", "param", multiple=True, help="k=v query parameter")
@click.option("--connection", "connection", default=None, help="Connection name override")
@click.pass_context
def db_query(
    ctx: click.Context,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
) -> None:
    """Run a DB query and return the rows."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _db_query_envelope(config_path, template, sql, param, connection, overlay_paths=list(ovs) if ovs else None)


_db_query_envelope = envelope("db.query")(_db_query_core)


# --------------------------------------------------------------------------- #
# db assert
# --------------------------------------------------------------------------- #


def _run_db_custom_assertion(name, rows, sql_text, params, conn_name):
    """DESIGN §9.3: dispatch ``--assertion <name>`` to a registered Assertion mode.

    The mode receives the query result as ``context`` and returns
    ``{"passed": bool, ...}``; see :func:`agctl.assertion_registry.evaluate_custom`.
    """
    from ..assertion_registry import evaluate_custom

    context = {
        "rows": rows,
        "row_count": len(rows),
        "sql": sql_text,
        "params": params,
        "connection": conn_name,
    }
    _, detail = evaluate_custom(name, context)
    return {
        "assertion_type": name,
        "passed": True,
        "sql": sql_text,
        "connection": conn_name,
        **detail,
    }


def _db_assert_core(
    config_path: str | None,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
    expect_rows: int | None,
    expect_value: bool,
    path: str | None,
    equals: str | None,
    assertion: str | None,
    overlay_paths: list[str] | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    sql_text, params, conn_name = resolve_db_request(
        cfg,
        template=template,
        sql=sql,
        param_tuple=param,
        connection_name=connection,
    )
    _check_template_mode(cfg, template, forbidden="write")

    # Validate assertion mode + required flags BEFORE hitting the database, so a
    # bad invocation fails fast with the right error category (ConfigError, exit 2)
    # rather than wasting a connection and surfacing a ConnectionError.
    rows_mode = expect_rows is not None
    value_mode = expect_value
    custom_mode = assertion is not None
    if rows_mode + value_mode + custom_mode != 1:
        raise ConfigError(
            "Exactly one of --expect-rows, --expect-value, or --assertion must be given",
            {},
        )
    if value_mode and (not path or equals is None):
        raise ConfigError("--expect-value requires --path and --equals", {})

    rows = _execute(cfg, sql_text, params, conn_name)

    if custom_mode:
        return _run_db_custom_assertion(assertion, rows, sql_text, params, conn_name)

    if rows_mode:
        expected = int(expect_rows)  # type: ignore[arg-type]
        actual = len(rows)
        if actual != expected:
            raise AssertionFailure(
                f"Expected {expected} rows, got {actual}",
                {
                    "expected": expected,
                    "actual": actual,
                    "sql": sql_text,
                    "connection": conn_name,
                    # Sample of the actual rows so the agent can see what the
                    # query returned without a follow-up `db query` (capped to
                    # the first 5; `actual` holds the true count).
                    "rows": rows[:5],
                },
            )
        return {
            "assertion_type": "expect_rows",
            "expected": expected,
            "actual": actual,
            "passed": True,
            "sql": sql_text,
            "connection": conn_name,
        }

    # expect-value mode (--path/--equals already validated above)
    if not rows:
        raise AssertionFailure(
            "Expected a row but query returned none",
            {"sql": sql_text, "connection": conn_name, "rows": []},
        )
    first_row = rows[0]
    actual = coerce_db_value(jq_value(first_row, path))
    expected = parse_equals(equals)
    if not type_aware_equal(expected, actual):
        raise AssertionFailure(
            f"Value mismatch at {path}: expected {expected!r}, got {actual!r}",
            {
                "path": path,
                "expected": expected,
                "actual": actual,
                "connection": conn_name,
                # --path is rooted at the FIRST ROW; echo it so the agent can see
                # the surrounding fields (issue #5) instead of re-running db query.
                "root": "first row",
                "row": first_row,
            },
        )
    return {
        "assertion_type": "expect_value",
        "path": path,
        "expected": expected,
        "actual": actual,
        "passed": True,
        "connection": conn_name,
    }


@click.command("assert")
@click.option("--template", "template", default=None, help="Named DB template")
@click.option("--sql", "sql", default=None, help="Free-form SQL text")
@click.option("--param", "param", multiple=True, help="k=v query parameter")
@click.option("--connection", "connection", default=None, help="Connection name override")
@click.option("--expect-rows", "expect_rows", type=int, default=None, help="Expected row count")
@click.option(
    "--expect-value",
    "expect_value",
    is_flag=True,
    default=False,
    help="Assert a cell value via --path/--equals",
)
@click.option("--path", "path", default=None, help="jq path into the FIRST ROW (expect-value), e.g. .status")
@click.option("--equals", "equals", default=None, help="Expected value (expect-value)")
@click.option(
    "--assertion",
    "assertion",
    default=None,
    help="Named custom assertion mode",
)
@click.pass_context
def db_assert(
    ctx: click.Context,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
    expect_rows: int | None,
    expect_value: bool,
    path: str | None,
    equals: str | None,
    assertion: str | None,
) -> None:
    """Run a DB query and assert on its result."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _db_assert_envelope(
        config_path,
        template,
        sql,
        param,
        connection,
        expect_rows,
        expect_value,
        path,
        equals,
        assertion,
        overlay_paths=list(ovs) if ovs else None,
    )


_db_assert_envelope = envelope("db.assert")(_db_assert_core)


# --------------------------------------------------------------------------- #
# db execute
# --------------------------------------------------------------------------- #


def _db_execute_core(
    config_path: str | None,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
    write: bool,
    overlay_paths: list[str] | None = None,
) -> dict:
    # Step 1: Load config
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)

    # Step 2: Resolve SQL, params, and connection (enforces template XOR sql,
    # neither-given, unknown template, unknown connection)
    sql_text, params, conn_name = resolve_db_request(
        cfg,
        template=template,
        sql=sql,
        param_tuple=param,
        connection_name=connection,
    )

    # Step 3: Explicit-target rule (refuse implicit write to default)
    if template is None and connection is None:
        raise ConfigError(
            "db execute requires --template or --connection to name the write target; refusing to write to the default connection implicitly (explicit target required)",
            {},
        )

    # Step 4: Invocation gate (--write is required)
    if not write:
        raise ConfigError(
            "db execute requires --write flag to confirm write intent",
            {},
        )

    # Step 5: Mode check (reject read-mode templates)
    _check_template_mode(cfg, template, forbidden="read")

    # Step 6: Connection gate (reject non-writable connections)
    if not cfg.database.connections[conn_name].writable:
        raise ConfigError(
            f"Connection '{conn_name}' is not writable (writable=false)",
            {"connection": conn_name},
        )

    # Step 7: Execute the write
    client = new_db_client(cfg.database.connections[conn_name])
    try:
        client.connect()
        result = client.execute_write(sql_text, params)
    finally:
        client.close()

    # Return result with connection and sql for echo/debug
    return {**result, "connection": conn_name, "sql": sql_text}


@click.command("execute")
@click.option("--template", "template", default=None, help="Named DB template")
@click.option("--sql", "sql", default=None, help="Free-form SQL text")
@click.option("--param", "param", multiple=True, help="k=v query parameter")
@click.option("--connection", "connection", default=None, help="Connection name override")
@click.option("--write", "write", is_flag=True, default=False, help="Confirm write intent")
@click.pass_context
def db_execute(
    ctx: click.Context,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
    write: bool,
) -> None:
    """Execute a write SQL statement and return affected row count."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _db_execute_envelope(config_path, template, sql, param, connection, write, overlay_paths=list(ovs) if ovs else None)


_db_execute_envelope = envelope("db.execute")(_db_execute_core)


# --------------------------------------------------------------------------- #
# db schema (Task 5: live, read-only schema discovery — two levels)
#
# ``db schema`` is read-only and ungated: no --write / --template / --sql /
# --param. It ignores ``writable`` / ``mode`` entirely. Level 1 (no --table)
# lists relations; Level 2 (--table <name>) returns one relation's columns and
# keys, disambiguating on the driver's ``matches`` list.
# --------------------------------------------------------------------------- #


# Verbatim hint strings from the spec (DESIGN §7 / spec D5–D7) — copied
# character-for-character. The agent reads these to chain its next call.
_SCHEMA_TABLES_HINT = (
    "Run 'agctl db schema --table <name> [--schema <name>] "
    "[--connection <name>]' for columns and keys"
)
_SCHEMA_TABLE_HINT = (
    "Use these columns in 'agctl db query' / 'db assert --sql' "
    "with :paramName bind params."
)


def _probe_and_describe(conn_obj, *, table, schema):
    """Build a client, probe schema-discovery support BEFORE connecting, then
    connect → describe_schema → close (try/finally).

    Returns the driver's raw result dict. Raises :class:`ConfigError` (exit 2)
    when the driver lacks ``describe_schema`` — and this happens BEFORE
    ``connect()``, so a non-introspection driver fails fast without opening a
    connection (the load-bearing lifecycle guarantee tested by Task 5).

    The close runs in ``finally`` even on the Level-2 not-found / ambiguity
    error paths, since those raise after ``describe_schema`` returned.
    """
    client = new_db_client(conn_obj)
    # Pre-connect probe: side-effect-free, never opens a connection.
    if not client.supports_describe_schema():
        driver_type = getattr(conn_obj, "type", None)
        raise ConfigError(
            f"connection's driver ({driver_type}) does not support schema discovery",
            {"driver": driver_type},
        )
    try:
        client.connect()
        return client.describe_schema(table=table, schema=schema)
    finally:
        client.close()


def _db_schema_tables_core(
    config_path: str | None,
    connection: str | None,
    schema: str | None,
    overlay_paths: list[str] | None = None,
) -> dict:
    """Level 1: list relations (tables/views) visible in the connection."""
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    # Schema has no template, so resolve inline with no template_connection.
    conn_name = resolve_connection_name(cfg, connection_name=connection)
    raw = _probe_and_describe(
        cfg.database.connections[conn_name], table=None, schema=schema
    )
    items = raw.get("items", [])
    return {
        "connection": conn_name,
        "schema_filter": schema,
        "count": len(items),
        "items": items,
        "hint": _SCHEMA_TABLES_HINT,
    }


def _db_schema_table_core(
    config_path: str | None,
    connection: str | None,
    schema: str | None,
    table: str,
    overlay_paths: list[str] | None = None,
) -> dict:
    """Level 2: return one relation's columns + keys, disambiguating matches."""
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    conn_name = resolve_connection_name(cfg, connection_name=connection)
    raw = _probe_and_describe(
        cfg.database.connections[conn_name], table=table, schema=schema
    )
    matches = raw.get("matches", [])

    if len(matches) == 0:
        raise ConfigError(
            f"Table '{table}' not found in connection '{conn_name}'; "
            "run 'agctl db schema' to list available tables",
            {"table": table},
        )
    if len(matches) > 1:
        candidates = [
            {"schema": m.get("schema"), "kind": m.get("kind")} for m in matches
        ]
        raise ConfigError(
            f"Table name '{table}' is ambiguous across schemas; "
            "pass --schema to disambiguate",
            {"table": table, "candidates": candidates},
        )

    # Single match: flatten it into the top-level result.
    match = matches[0]
    return {
        "connection": conn_name,
        "schema": match.get("schema"),
        "table": match.get("table"),
        "kind": match.get("kind"),
        "comment": match.get("comment"),
        "columns": match.get("columns"),
        "primary_key": match.get("primary_key"),
        "foreign_keys": match.get("foreign_keys"),
        "unique_constraints": match.get("unique_constraints"),
        "hint": _SCHEMA_TABLE_HINT,
    }


@click.command("schema")
@click.option(
    "--connection", "connection", default=None, help="Connection name override"
)
@click.option(
    "--schema", "schema", default=None, help="Schema filter (valid at both levels)"
)
@click.option(
    "--table",
    "table",
    default=None,
    help="Relation name for Level 2 detail (columns + keys); omit for Level 1 list",
)
@click.pass_context
def db_schema(
    ctx: click.Context,
    connection: str | None,
    schema: str | None,
    table: str | None,
) -> None:
    """Discover live DB schema: list relations, then columns/keys for one."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    overlay_paths = list(ovs) if ovs else None
    if table is None:
        _db_schema_tables_envelope(config_path, connection, schema, overlay_paths=overlay_paths)
    else:
        _db_schema_table_envelope(config_path, connection, schema, table, overlay_paths=overlay_paths)


_db_schema_tables_envelope = envelope("db.schema.tables")(_db_schema_tables_core)
_db_schema_table_envelope = envelope("db.schema.table")(_db_schema_table_core)
