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
from ..errors import AssertionFailure, ConfigError, TemplateMissing
from ..params import parse_params

__all__ = ["db_query", "db_assert", "new_db_client", "resolve_db_request"]


def new_db_client(connection_obj: Any):
    """Build a real :class:`DbClient` for a connection object.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(db_commands, "new_db_client", factory)``) to return a
    client wrapping a FakeDriver, avoiding any real DB connection.
    """
    from ..clients.db_client import DbClient

    return DbClient(connection_obj)


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
      :class:`TemplateMissing`. ``sql_text`` comes from the template; the
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
            raise TemplateMissing(
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


# --------------------------------------------------------------------------- #
# db query
# --------------------------------------------------------------------------- #


def _db_query_core(
    config_path: str | None,
    template: str | None,
    sql: str | None,
    param: tuple[str, ...],
    connection: str | None,
) -> dict:
    cfg = load_config_or_raise(config_path)
    sql_text, params, conn_name = resolve_db_request(
        cfg,
        template=template,
        sql=sql,
        param_tuple=param,
        connection_name=connection,
    )
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
    """Run a DB query and return the rows (DESIGN §3.3)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _db_query_envelope(config_path, template, sql, param, connection)


_db_query_envelope = envelope("db.query")(_db_query_core)


# --------------------------------------------------------------------------- #
# db assert
# --------------------------------------------------------------------------- #


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
) -> dict:
    cfg = load_config_or_raise(config_path)
    sql_text, params, conn_name = resolve_db_request(
        cfg,
        template=template,
        sql=sql,
        param_tuple=param,
        connection_name=connection,
    )
    rows = _execute(cfg, sql_text, params, conn_name)

    # Exactly one assertion mode must be active.
    rows_mode = expect_rows is not None
    value_mode = expect_value
    if rows_mode == value_mode:  # both True or both False
        raise ConfigError(
            "Exactly one of --expect-rows and --expect-value must be given", {}
        )

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

    # expect-value mode
    if not rows:
        raise AssertionFailure(
            "Expected a row but query returned none",
            {"sql": sql_text, "connection": conn_name},
        )
    if not path or equals is None:
        raise ConfigError(
            "--expect-value requires --path and --equals", {}
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
@click.option("--path", "path", default=None, help="jq path to the cell (expect-value)")
@click.option("--equals", "equals", default=None, help="Expected value (expect-value)")
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
) -> None:
    """Run a DB query and assert on its result (DESIGN §3.3, D8)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
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
    )


_db_assert_envelope = envelope("db.assert")(_db_assert_core)
