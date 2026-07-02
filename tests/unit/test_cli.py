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
    assert payload["result"]["valid"] is True
    assert payload["result"]["warnings"] is not None


def test_validate_fails_on_missing_env():
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(FIXTURE)], env={})
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def _write_config(tmp_path, text):
    p = tmp_path / "agctl.yaml"
    p.write_text(text)
    return p


def test_validate_structural_error_envelope(tmp_path):
    cfg_path = _write_config(
        tmp_path,
        """
version: "1"
services:
  order-service:
    base_url: "http://localhost:8081"
templates:
  create-order:
    method: POST
    service: ghost
    path: "/api/v1/orders"
""",
    )
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(cfg_path)])
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["command"] == "config.validate"
    assert payload["result"]["valid"] is False
    assert payload["result"]["errors"]  # non-empty
    assert payload["result"]["warnings"] is not None
    assert payload["error"]["type"] == "ConfigError"
    assert "structural error" in payload["error"]["message"]


def test_validate_good_fixture_no_structural_errors():
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["valid"] is True
    assert isinstance(payload["result"]["warnings"], list)


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
    assert payload["result"]["valid"] is True


def test_show_preserves_non_secret_values():
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(FIXTURE)], env=ENV)
    payload = json.loads(result.output)
    conn = payload["result"]["database"]["connections"]["main-db"]
    assert conn["password"] == "***"
    assert conn["host"] == "h"
    assert conn["dbname"] == "n"
