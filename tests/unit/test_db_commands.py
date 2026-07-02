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
    would dispatch.
    """

    def __init__(self, rows=None):
        self.rows = rows if rows is not None else []
        self.executed = []
        self.connected = False
        self.closed = False

    def connect(self, config):
        self.connected = True

    def execute(self, sql, params):
        rewrite = convert_sql_params(sql)
        self.executed.append((rewrite, params))
        return list(self.rows)

    def close(self):
        self.closed = True


@pytest.fixture
def install_fake(monkeypatch):
    """Return a factory: install_fake(rows=...) wires a FakeDriver-backed client."""

    def _install(rows):
        fake = FakeDriver(rows=rows)

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
