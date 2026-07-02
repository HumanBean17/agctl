import json
from pathlib import Path

from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "KAFKA_BROKER": "localhost",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "secret",
}


def test_validate_ok():
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["command"] == "config.validate"
    assert payload["result"] == {"valid": True}


def test_validate_fails_on_missing_env():
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(FIXTURE)], env={})
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_show_masks_password():
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["database"]["connections"]["main-db"]["password"] == "***"


def test_show_unmask_exposes_password():
    args = ["config", "show", "--config", str(FIXTURE), "--unmask"]
    result = CliRunner().invoke(cli, args, env=ENV)
    payload = json.loads(result.output)
    assert payload["result"]["database"]["connections"]["main-db"]["password"] == "secret"


def test_global_config_flag_is_honored():
    args = ["--config", str(FIXTURE), "config", "validate"]
    result = CliRunner().invoke(cli, args, env=ENV)
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"] == {"valid": True}


def test_show_preserves_non_secret_values():
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    conn = payload["result"]["database"]["connections"]["main-db"]
    assert conn["password"] == "***"
    assert conn["host"] == "h"
    assert conn["dbname"] == "n"
