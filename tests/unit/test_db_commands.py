"""Unit tests for `db query` and `db assert` commands (DESIGN §3.3, D8)."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.clients.db_client import DbClient
from agctl.assertion_registry import Assertion
from agctl.commands import db_commands
from agctl.config.models import DatabaseConnection
from agctl.resolution import convert_sql_params

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "p",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
}


class FakeDriver:
    """Minimal DBDriver double.

    Records the (rewritten) SQL + params it was called with — mirroring the real
    PostgreSQLDriver, which rewrites ``:name`` -> ``%(name)s`` inside execute()
    via convert_sql_params — so the recorded SQL reflects what a real driver
    would dispatch. Also records ``describe_schema`` calls (``self.described``)
    and returns a canned ``schema_result`` dict, so ``db schema`` command tests
    avoid a real DB.
    """

    def __init__(self, rows=None, write_result=None, schema_result=None):
        self.rows = rows if rows is not None else []
        self.write_result = write_result if write_result is not None else {"rows_affected": 1, "returning": [{"id": "o1", "status": "PENDING"}]}
        # Default schema_result mirrors the Level-1 shape (empty items list).
        self.schema_result = schema_result if schema_result is not None else {"items": []}
        self.executed = []
        self.executed_write = []
        self.described = []
        self.connected = False
        self.closed = False

    def connect(self, config):
        self.connected = True

    def execute(self, sql, params):
        rewrite = convert_sql_params(sql)
        self.executed.append((rewrite, params))
        return list(self.rows)

    def execute_write(self, sql, params):
        rewrite = convert_sql_params(sql)
        self.executed_write.append((rewrite, params))
        return self.write_result

    def describe_schema(self, table, schema):
        self.described.append({"table": table, "schema": schema})
        return self.schema_result

    def close(self):
        self.closed = True


class FakeDriverNoSchema(FakeDriver):
    """Fake driver whose ``describe_schema`` attr is non-callable.

    The pre-connect probe (:meth:`DbClient.supports_describe_schema`) inspects
    ``callable(getattr(driver, "describe_schema", None))``; setting the
    attribute to ``None`` makes the probe report False without opening a
    connection, mirroring a read-only driver that never implemented
    introspection.
    """

    describe_schema = None


@pytest.fixture
def install_fake(monkeypatch):
    """Factory: wire a FakeDriver-backed DbClient so command tests avoid a real DB.

    Accepts the canned ``schema_result`` for ``db schema`` happy-path tests,
    and ``introspect=False`` to swap in :class:`FakeDriverNoSchema` for the
    pre-connect probe refusal test.
    """

    def _install(rows=None, write_result=None, schema_result=None, introspect=True):
        if introspect:
            fake = FakeDriver(rows=rows, write_result=write_result, schema_result=schema_result)
        else:
            fake = FakeDriverNoSchema(rows=rows, write_result=write_result)

        def factory(connection_obj):
            return DbClient(connection_obj, driver=fake)

        monkeypatch.setattr(db_commands, "new_db_client", factory)
        return fake

    return _install


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


def _payload(result):
    return json.loads(result.output)


# --------------------------------------------------------------------------- #
# db query
# --------------------------------------------------------------------------- #


def test_db_query_template_returns_rows(install_fake):
    fake = install_fake([{"id": "o9", "status": "CONFIRMED"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "query",
            "--template",
            "find-order",
            "--param",
            "orderId=o9",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.query"
    assert payload["ok"] is True
    assert payload["result"]["row_count"] == 1
    assert payload["result"]["rows"][0]["status"] == "CONFIRMED"
    # template's connection is main-db
    assert payload["result"]["connection"] == "main-db"
    # FakeDriver received the rewritten SQL (placeholder :orderId -> %(orderId)s)
    # and the params dict.
    assert len(fake.executed) == 1
    recorded_sql, recorded_params = fake.executed[0]
    assert "%(orderId)s" in recorded_sql
    assert ":orderId" not in recorded_sql
    assert recorded_params == {"orderId": "o9"}


def test_db_query_freeform_sql_falls_back_to_default_connection(install_fake):
    install_fake([{"x": 1}])
    result = _run(
        ["--config", str(FIXTURE), "db", "query", "--sql", "SELECT 1"]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.query"
    assert payload["ok"] is True
    # defaults.database_connection == main-db
    assert payload["result"]["connection"] == "main-db"


def test_db_query_template_and_sql_mutually_exclusive(install_fake):
    install_fake([])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "query",
            "--template",
            "find-order",
            "--sql",
            "SELECT 1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_db_query_unknown_template_raises_template_missing(install_fake):
    install_fake([])
    result = _run(
        ["--config", str(FIXTURE), "db", "query", "--template", "nope"]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "TemplateNotFound"


def test_db_query_neither_template_nor_sql_raises(install_fake):
    install_fake([])
    result = _run(["--config", str(FIXTURE), "db", "query"])
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# db assert — expect-rows
# --------------------------------------------------------------------------- #


def test_db_assert_expect_rows_pass(install_fake):
    install_fake([{"id": "o9"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--param",
            "order_id=o9",
            "--expect-rows",
            "1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["assertion_type"] == "expect_rows"
    assert payload["result"]["passed"] is True


def test_db_assert_expect_rows_fail(install_fake):
    install_fake([])  # 0 rows
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-rows",
            "1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"
    assert payload["error"]["detail"]["expected"] == 1
    assert payload["error"]["detail"]["actual"] == 0


# --------------------------------------------------------------------------- #
# db assert — expect-value
# --------------------------------------------------------------------------- #


def test_db_assert_expect_value_pass(install_fake):
    install_fake([{"status": "CONFIRMED"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-value",
            "--path",
            ".status",
            "--equals",
            "CONFIRMED",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["assertion_type"] == "expect_value"
    assert payload["result"]["passed"] is True


def test_db_assert_expect_value_fail(install_fake):
    install_fake([{"status": "PENDING"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-value",
            "--path",
            ".status",
            "--equals",
            "CONFIRMED",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"
    assert payload["error"]["detail"]["expected"] == "CONFIRMED"
    assert payload["error"]["detail"]["actual"] == "PENDING"


# --------------------------------------------------------------------------- #
# db assert — D8 type-aware coercion
# --------------------------------------------------------------------------- #


def test_db_assert_d8_numeric_zero_passes(install_fake):
    """parse_equals('0') == 0 (int); coerce(0) == 0; type_aware_equal(0, 0) True."""
    install_fake([{"cnt": 0}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-value",
            "--path",
            ".cnt",
            "--equals",
            "0",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True


def test_db_assert_d8_string_zero_vs_number_fails(install_fake):
    """DB cell is the string '0'; --equals '0' parses to int 0; 0 != '0' -> fail."""
    install_fake([{"status": "0"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-value",
            "--path",
            ".status",
            "--equals",
            "0",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"
    assert payload["error"]["detail"]["expected"] == 0
    assert payload["error"]["detail"]["actual"] == "0"


# --------------------------------------------------------------------------- #
# db assert — mode selection + edge cases
# --------------------------------------------------------------------------- #


def test_db_assert_both_modes_is_config_error(install_fake):
    install_fake([{"status": "CONFIRMED"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-rows",
            "1",
            "--expect-value",
            "--path",
            ".status",
            "--equals",
            "CONFIRMED",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_db_assert_neither_mode_is_config_error(install_fake):
    install_fake([{"status": "CONFIRMED"}])
    result = _run(
        ["--config", str(FIXTURE), "db", "assert", "--sql", "SELECT 1"]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_db_assert_expect_value_zero_rows(install_fake):
    install_fake([])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-value",
            "--path",
            ".status",
            "--equals",
            "CONFIRMED",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"


# --------------------------------------------------------------------------- #
# db assert — fail fast: invalid invocation must NOT hit the DB (Fix C)
# --------------------------------------------------------------------------- #


def test_db_assert_expect_value_missing_path_fails_fast_without_executing(install_fake):
    """--expect-value WITHOUT --path -> ConfigError (exit 2) AND the query is
    never executed (no DB round-trip on a malformed invocation)."""
    fake = install_fake([{"status": "CONFIRMED"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-value",
            "--equals",
            "X",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Fail-fast: validation happened BEFORE _execute, so the driver was untouched.
    assert fake.executed == []


def test_db_assert_both_modes_fails_fast_without_executing(install_fake):
    """--expect-rows + --expect-value together -> ConfigError (exit 2) WITHOUT
    executing the query (mutual exclusion is validated up front)."""
    fake = install_fake([{"status": "CONFIRMED"}])
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "db",
            "assert",
            "--sql",
            "SELECT 1",
            "--expect-rows",
            "1",
            "--expect-value",
            "--path",
            ".status",
            "--equals",
            "CONFIRMED",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert fake.executed == []


# --------------------------------------------------------------------------- #
# DESIGN §9.3: `db assert --assertion <name>` dispatches to a registered mode
# --------------------------------------------------------------------------- #


def _install_custom_registry(monkeypatch, mode_cls):
    """Point the default registry at a fresh registry containing `mode_cls`."""
    import agctl.assertion_registry as ar
    from agctl.assertion_registry import AssertionRegistry

    reg = AssertionRegistry()
    reg.register(mode_cls)
    monkeypatch.setattr(ar, "get_default_registry", lambda: reg)


def test_db_assert_custom_mode_passes(monkeypatch, install_fake):
    class _StatusOK(Assertion):
        name = "status_ok"

        def evaluate(self, context):
            ok = bool(context["rows"]) and context["rows"][0].get("status") == "OK"
            return {"passed": ok, "row_count": context["row_count"]}

    _install_custom_registry(monkeypatch, _StatusOK)
    install_fake([{"status": "OK"}])

    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "assert",
            "--sql", "SELECT 'OK' AS status",
            "--assertion", "status_ok",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["assertion_type"] == "status_ok"
    assert payload["result"]["passed"] is True
    assert payload["result"]["row_count"] == 1


def test_db_assert_custom_mode_fails_exit1(monkeypatch, install_fake):
    class _StatusOK(Assertion):
        name = "status_ok"

        def evaluate(self, context):
            return {"passed": False, "message": "status not OK"}

    _install_custom_registry(monkeypatch, _StatusOK)
    install_fake([{"status": "BAD"}])

    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "assert",
            "--sql", "SELECT 'BAD' AS status",
            "--assertion", "status_ok",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"
    assert payload["error"]["detail"]["mode"] == "status_ok"


def test_db_assert_custom_mode_unknown_is_template_not_found(install_fake):
    install_fake([{"a": 1}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "assert",
            "--sql", "SELECT 1",
            "--assertion", "does_not_exist",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "TemplateNotFound"


def test_db_assert_assertion_mutually_exclusive_with_expect_rows(install_fake):
    fake = install_fake([{"a": 1}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "assert",
            "--sql", "SELECT 1",
            "--expect-rows", "1",
            "--assertion", "status_ok",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert fake.executed == []  # mode validation fails fast before any query


# --------------------------------------------------------------------------- #
# db execute
# --------------------------------------------------------------------------- #


def test_db_execute_happy_path_template(install_fake):
    """Happy path with template: seed-order write succeeds."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": [{"id": "o1", "status": "PENDING"}]})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--template", "seed-order",
            "--param", "orderId=o1",
            "--param", "status=PENDING",
            "--write",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.execute"
    assert payload["ok"] is True
    assert payload["result"]["rows_affected"] == 1
    assert payload["result"]["returning"] == [{"id": "o1", "status": "PENDING"}]
    assert payload["result"]["connection"] == "main-db-writable"
    # The SQL in the result should have placeholders intact (:orderId, :status)
    assert ":orderId" in payload["result"]["sql"]
    assert ":status" in payload["result"]["sql"]
    # The fake driver received the rewritten SQL (%(orderId)s, %(status)s) and params
    assert len(fake.executed_write) == 1
    recorded_sql, recorded_params = fake.executed_write[0]
    assert "%(orderId)s" in recorded_sql
    assert "%(status)s" in recorded_sql
    assert ":orderId" not in recorded_sql
    assert ":status" not in recorded_sql
    assert recorded_params == {"orderId": "o1", "status": "PENDING"}


def test_db_execute_happy_path_freeform_with_explicit_connection(install_fake):
    """Happy path with free-form SQL and explicit connection."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--connection", "main-db-writable",
            "--sql", "DELETE FROM t WHERE id = :i",
            "--param", "i=9",
            "--write",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.execute"
    assert payload["ok"] is True
    assert payload["result"]["connection"] == "main-db-writable"


def test_db_execute_zero_affected_success(install_fake):
    """0 rows affected is still a successful write (no-op)."""
    fake = install_fake(write_result={"rows_affected": 0, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--template", "seed-order",
            "--param", "orderId=o1",
            "--param", "status=PENDING",
            "--write",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["rows_affected"] == 0
    assert payload["result"]["returning"] == []


def test_db_execute_missing_write_flag_fails_fast(install_fake):
    """Missing --write flag fails fast without touching the DB."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--template", "seed-order",
            "--param", "orderId=o1",
            "--param", "status=PENDING",
            # Missing --write flag
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Fail-fast: execute_write was never called
    assert fake.executed_write == []


def test_db_execute_connection_gate_non_writable(install_fake):
    """Connection gate: main-db is read-only, so execute is rejected."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--connection", "main-db",
            "--sql", "DELETE FROM t",
            "--write",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Fail-fast: execute_write was never called
    assert fake.executed_write == []


def test_db_execute_explicit_target_rule_refuses_implicit_write(install_fake):
    """Explicit-target rule: refuse to write to default connection implicitly."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--sql", "DELETE FROM t",
            "--write",
            # No --template or --connection, should refuse to write to default
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert "explicit target" in payload["error"]["message"].lower()
    # Fail-fast: execute_write was never called
    assert fake.executed_write == []


def test_db_execute_mode_check_rejects_read_template(install_fake):
    """Mode check: read-mode template (find-order) is rejected for execute."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--template", "find-order",
            "--write",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Fail-fast: execute_write was never called
    assert fake.executed_write == []


def test_db_execute_template_and_sql_mutually_exclusive(install_fake):
    """Template and SQL are mutually exclusive (enforced by resolve_db_request)."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--template", "seed-order",
            "--sql", "DELETE FROM t",
            "--write",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Fail-fast: execute_write was never called
    assert fake.executed_write == []


def test_db_execute_bare_write_flag_needs_template_or_sql(install_fake):
    """Bare --write flag without template or sql should fail (neither given)."""
    fake = install_fake(write_result={"rows_affected": 1, "returning": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--write",
            # No --template or --sql
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Fail-fast: execute_write was never called
    assert fake.executed_write == []


# --------------------------------------------------------------------------- #
# Task 7: Read-side mode checks (db query / db assert refuse write-mode templates)
# --------------------------------------------------------------------------- #


def test_db_query_rejects_write_mode_template(install_fake):
    """db query with write-mode template (seed-order) should raise ConfigError (exit 2)."""
    fake = install_fake([{"id": "o1", "status": "PENDING"}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "query",
            "--template", "seed-order",
            "--param", "orderId=o1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert "mode 'write'" in payload["error"]["message"].lower()
    # Fail-fast: execute was never called
    assert fake.executed == []


def test_db_assert_rejects_write_mode_template(install_fake):
    """db assert with write-mode template (seed-order) should raise ConfigError (exit 2)."""
    fake = install_fake([{"id": "o1", "status": "PENDING"}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "assert",
            "--template", "seed-order",
            "--param", "orderId=o1",
            "--expect-rows", "1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert "mode 'write'" in payload["error"]["message"].lower()
    # Fail-fast: execute was never called
    assert fake.executed == []


def test_db_query_read_mode_template_still_works(install_fake):
    """Regression guard: db query with read-mode template (find-order) should still work."""
    fake = install_fake([{"id": "o1", "status": "PENDING"}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "query",
            "--template", "find-order",
            "--param", "orderId=o1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["row_count"] == 1
    # The query should have executed
    assert len(fake.executed) == 1


def test_db_query_freeform_sql_still_works(install_fake):
    """Regression guard: db query with free-form SQL should still work."""
    fake = install_fake([{"x": 1}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "query",
            "--sql", "SELECT 1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    # The query should have executed
    assert len(fake.executed) == 1


# --------------------------------------------------------------------------- #
# db schema (Task 5: live, read-only schema discovery — two levels)
# --------------------------------------------------------------------------- #

# Verbatim hint strings from the spec — copy character-for-character.
_SCHEMA_TABLES_HINT = (
    "Run 'agctl db schema --table <name> [--schema <name>] "
    "[--connection <name>]' for columns and keys"
)
_SCHEMA_TABLE_HINT = (
    "Use these columns in 'agctl db query' / 'db assert --sql' "
    "with :paramName bind params."
)

_LEVEL1_ITEMS = [
    {"schema": "public", "name": "orders", "kind": "table", "column_count": 4},
    {"schema": "public", "name": "order_items", "kind": "table", "column_count": 3},
]


def _level2_match(schema="public", table="orders"):
    """Build a canned Level-2 match dict (one relation's full description)."""
    return {
        "schema": schema,
        "table": table,
        "kind": "table",
        "comment": None,
        "columns": [
            {
                "name": "id",
                "data_type": "integer",
                "nullable": False,
                "default": None,
                "generated": None,
                "enum_values": None,
                "comment": None,
            },
            {
                "name": "status",
                "data_type": "text",
                "nullable": False,
                "default": "'PENDING'",
                "generated": None,
                "enum_values": None,
                "comment": None,
            },
        ],
        "primary_key": ["id"],
        "foreign_keys": [
            {
                "name": "order_items_order_id_fkey",
                "columns": ["order_id"],
                "references_schema": "public",
                "references_table": "orders",
                "references_columns": ["id"],
            }
        ],
        "unique_constraints": [
            {"name": "orders_status_key", "columns": ["status"]},
        ],
    }


# --------------------------------------------------------------------------- #
# Level 1 (list tables)
# --------------------------------------------------------------------------- #


def test_db_schema_level1_happy_path(install_fake):
    """Level 1, no flags: resolves to default main-db, lists all items."""
    fake = install_fake(schema_result={"items": _LEVEL1_ITEMS})
    result = _run(["--config", str(FIXTURE), "db", "schema"])
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.schema.tables"
    assert payload["ok"] is True
    assert payload["result"]["connection"] == "main-db"
    assert payload["result"]["schema_filter"] is None
    assert payload["result"]["count"] == len(_LEVEL1_ITEMS)
    assert payload["result"]["items"] == _LEVEL1_ITEMS
    assert payload["result"]["hint"] == _SCHEMA_TABLES_HINT
    # FakeDriver received describe_schema(table=None, schema=None).
    assert fake.described == [{"table": None, "schema": None}]


def test_db_schema_level1_with_schema_filter(install_fake):
    """Level 1 with --schema: schema_filter echoed; describe_schema gets schema."""
    fake = install_fake(schema_result={"items": _LEVEL1_ITEMS})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "main-db",
            "--schema", "public",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.schema.tables"
    assert payload["result"]["schema_filter"] == "public"
    assert fake.described == [{"table": None, "schema": "public"}]


def test_db_schema_level1_empty_items_is_not_error(install_fake):
    """Level 1 with canned items == []: count 0, ok True (NOT an error)."""
    install_fake(schema_result={"items": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--schema", "empty_ns",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["count"] == 0
    assert payload["result"]["items"] == []


# --------------------------------------------------------------------------- #
# Level 2 (single table detail)
# --------------------------------------------------------------------------- #


def test_db_schema_level2_happy_path(install_fake):
    """Level 2 happy path: single match flattened into top-level result."""
    match = _level2_match()
    fake = install_fake(schema_result={"items": [], "matches": [match]})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "main-db",
            "--table", "orders",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.schema.table"
    assert payload["ok"] is True
    res = payload["result"]
    # Flattened single-match fields
    assert res["schema"] == "public"
    assert res["table"] == "orders"
    assert res["kind"] == "table"
    assert res["comment"] is None
    assert res["columns"] == match["columns"]
    assert res["primary_key"] == ["id"]
    assert res["foreign_keys"] == match["foreign_keys"]
    assert res["unique_constraints"] == match["unique_constraints"]
    assert res["connection"] == "main-db"
    assert res["hint"] == _SCHEMA_TABLE_HINT
    # FakeDriver received describe_schema(table="orders", schema=None).
    assert fake.described == [{"table": "orders", "schema": None}]


def test_db_schema_level2_with_schema_filter(install_fake):
    """Level 2 with --schema: describe_schema gets both table and schema."""
    match = _level2_match()
    fake = install_fake(schema_result={"items": [], "matches": [match]})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "main-db",
            "--schema", "public",
            "--table", "orders",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "db.schema.table"
    assert payload["result"]["table"] == "orders"
    assert payload["result"]["schema"] == "public"
    assert fake.described == [{"table": "orders", "schema": "public"}]


# --------------------------------------------------------------------------- #
# Pre-connect probe + error paths
# --------------------------------------------------------------------------- #


def test_db_schema_pre_connect_probe_refusal_never_connects(install_fake):
    """Pre-connect probe refusal: unsupported driver -> exit 2 ConfigError,
    and connect() was NEVER called (the load-bearing lifecycle guarantee)."""
    fake = install_fake(introspect=False)
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "main-db",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert "does not support schema discovery" in payload["error"]["message"]
    assert payload["error"]["detail"]["driver"] == "postgresql"
    # The probe fires BEFORE connect(); the driver must not have been touched.
    assert fake.connected is False


def test_db_schema_level2_not_found(install_fake):
    """Level 2 not-found: 0 matches -> ConfigError telling agent to list tables."""
    install_fake(schema_result={"items": [], "matches": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "main-db",
            "--table", "nope",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Message tells the agent the table was not found AND to list tables.
    msg = payload["error"]["message"].lower()
    assert "not found" in msg
    assert "agctl db schema" in payload["error"]["message"]
    assert payload["error"]["detail"] == {"table": "nope"}


def test_db_schema_level2_ambiguity(install_fake):
    """Level 2 ambiguity: >1 matches, no --schema -> ConfigError with candidates."""
    matches = [_level2_match(schema="public"), _level2_match(schema="legacy")]
    install_fake(schema_result={"items": [], "matches": matches})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "main-db",
            "--table", "orders",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    msg = payload["error"]["message"].lower()
    assert "ambig" in msg
    assert "--schema" in payload["error"]["message"]
    detail = payload["error"]["detail"]
    assert detail["table"] == "orders"
    assert {"schema": "public", "kind": "table"} in detail["candidates"]
    assert {"schema": "legacy", "kind": "table"} in detail["candidates"]
    assert len(detail["candidates"]) == 2


def test_db_schema_no_default_no_connection_raises_config_error(install_fake, tmp_path):
    """Connection resolution: no default and no --connection -> exit 2
    ConfigError 'No database connection specified'."""
    install_fake(schema_result={"items": []})
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "2"\n'
        "database:\n"
        "  connections:\n"
        "    main-db:\n"
        "      type: postgresql\n"
        '      host: "h"\n'
    )
    result = _run(["--config", str(cfg), "db", "schema"], env=ENV)
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert payload["error"]["message"] == "No database connection specified"


def test_db_schema_unknown_connection_raises_config_error(install_fake):
    """Connection resolution: unknown --connection foo -> exit 2 ConfigError."""
    install_fake(schema_result={"items": []})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "schema",
            "--connection", "foo",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert payload["error"]["message"] == "Unknown database connection: foo"
    assert payload["error"]["detail"] == {"connection": "foo"}
