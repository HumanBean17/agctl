"""Unit tests for `db query/assert/execute` inline template-variable generators (Task 9).

Threads the per-invocation ``memo`` + ``template_vars_enabled`` flag through the
db commands so ``{{uuid}}``/``{{ts}}``/``{{rand}}`` tokens in the FREE-FORM
``--sql`` string resolve to generated values, with the SAME token resolving to
the SAME value within one invocation (shared memo). ``--no-template-vars``
leaves tokens literal; an unknown generator surfaces as ``ConfigError`` (exit 2)
before the driver's ``execute`` is ever called.

The fake driver (the existing ``db_commands`` test seam) records every executed
``(sql, params)`` pair via ``convert_sql_params``, so tests assert on the
post-substitution SQL string without any real DB.
"""

import json
import re
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.clients.db_client import DbClient
from agctl.commands import db_commands
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


class _RecDriver:
    """Minimal DBDriver double that records the (rewritten) SQL + params.

    Mirrors :class:`FakeDriver` in ``test_db_commands`` — rewrites via
    ``convert_sql_params`` so the recorded SQL reflects what a real driver
    dispatches. Used only to capture the executed SQL string; rows are canned.
    """

    def __init__(self, rows=None, write_result=None):
        self.rows = list(rows or [])
        self.write_result = write_result or {
            "rows_affected": 1,
            "returning": [{"id": "o1"}],
        }
        self.executed: list[tuple[str, dict]] = []
        self.executed_write: list[tuple[str, dict]] = []

    def connect(self, config):
        pass

    def execute(self, sql, params):
        self.executed.append((convert_sql_params(sql), params))
        return list(self.rows)

    def execute_write(self, sql, params):
        self.executed_write.append((convert_sql_params(sql), params))
        return self.write_result

    def close(self):
        pass


@pytest.fixture
def install_recording_driver(monkeypatch):
    """Install a :class:`_RecDriver`-backed DbClient; return the driver double."""

    def _install(rows=None, write_result=None):
        driver = _RecDriver(rows=rows, write_result=write_result)

        def factory(connection_obj):
            return DbClient(connection_obj, driver=driver)

        monkeypatch.setattr(db_commands, "new_db_client", factory)
        return driver

    return _install


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


def _payload(result):
    return json.loads(result.output)


# ---------------------------------------------------------------------------
# db query: free-form --sql generator substitution
# ---------------------------------------------------------------------------


def test_db_query_substitutes_ts_in_freeform_sql(install_recording_driver):
    """``db query --sql "SELECT {{ts}} AS t"`` reaches the driver with the
    ``{{ts}}`` token replaced by a 10-digit Unix-seconds timestamp."""
    driver = install_recording_driver(rows=[{"t": 1}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "query",
            "--sql", "SELECT {{ts}} AS t",
            "--connection", "main-db",
        ]
    )
    assert result.exit_code == 0, result.output

    assert len(driver.executed) == 1
    recorded_sql, _ = driver.executed[0]
    assert re.fullmatch(r"SELECT \d{10} AS t", recorded_sql), recorded_sql


def test_db_query_no_template_vars_leaves_token_literal(install_recording_driver):
    """``--no-template-vars`` disables substitution end-to-end: the driver
    receives the LITERAL ``{{ts}}`` token, and no error is raised."""
    driver = install_recording_driver(rows=[{"t": 1}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "--no-template-vars",
            "db", "query",
            "--sql", "SELECT {{ts}} AS t",
            "--connection", "main-db",
        ]
    )
    assert result.exit_code == 0, result.output

    assert len(driver.executed) == 1
    recorded_sql, _ = driver.executed[0]
    assert recorded_sql == "SELECT {{ts}} AS t"


def test_db_query_unknown_generator_is_config_error_before_execute(
    install_recording_driver,
):
    """``--sql "{{nope}}"`` (unknown generator) surfaces as ``ConfigError``
    (exit 2) BEFORE the driver is called — the recording driver records nothing."""
    driver = install_recording_driver(rows=[])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "query",
            "--sql", "{{nope}}",
            "--connection", "main-db",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # Driver.execute was never called.
    assert driver.executed == []


def test_db_query_same_token_one_value_within_call(install_recording_driver):
    """The SAME ``{{token}}`` appearing twice in one ``--sql`` resolves to ONE
    value (per-invocation memoization). Verified by capturing the value via a
    round-trip query that echoes it back as two columns."""
    driver = install_recording_driver(rows=[{"a": "x", "b": "x"}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "query",
            "--sql", "SELECT {{ts}} AS a, {{ts}} AS b",
            "--connection", "main-db",
        ]
    )
    assert result.exit_code == 0, result.output

    recorded_sql, _ = driver.executed[0]
    # Both positions hold the SAME 10-digit timestamp.
    m = re.fullmatch(r"SELECT (\d{10}) AS a, (\d{10}) AS b", recorded_sql)
    assert m is not None, recorded_sql
    assert m.group(1) == m.group(2)


# ---------------------------------------------------------------------------
# db assert: free-form --sql generator substitution
# ---------------------------------------------------------------------------


def test_db_assert_substitutes_generator_in_freeform_sql(install_recording_driver):
    """``db assert --sql`` substitution surfaces in the failure detail's ``sql``
    field when the assertion misses (echo of the executed SQL)."""
    driver = install_recording_driver(rows=[{"x": 1}])
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "assert",
            "--sql", "SELECT {{ts}} AS x",
            "--connection", "main-db",
            "--expect-rows", "2",  # rows=[{"x":1}] -> 1 != 2 -> AssertionError
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"
    echoed_sql = payload["error"]["detail"]["sql"]
    assert re.fullmatch(r"SELECT \d{10} AS x", echoed_sql), echoed_sql


# ---------------------------------------------------------------------------
# db execute: free-form --sql generator substitution
# ---------------------------------------------------------------------------


def test_db_execute_substitutes_generator_in_freeform_sql(install_recording_driver):
    """``db execute --sql`` substitution reaches the driver's ``execute_write``
    with the token replaced."""
    driver = install_recording_driver(write_result={"rows_affected": 1})
    result = _run(
        [
            "--config", str(FIXTURE),
            "db", "execute",
            "--sql", "INSERT INTO t (ts) VALUES ({{ts}})",
            "--connection", "main-db-writable",
            "--write",
        ]
    )
    assert result.exit_code == 0, result.output

    assert len(driver.executed_write) == 1
    recorded_sql, _ = driver.executed_write[0]
    assert re.fullmatch(r"INSERT INTO t \(ts\) VALUES \(\d{10}\)", recorded_sql), recorded_sql
