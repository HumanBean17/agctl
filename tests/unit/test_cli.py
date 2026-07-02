import json
from pathlib import Path

from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "secret",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
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
    assert "error" in payload["error"]["message"]


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


def test_show_does_not_mask_ssl_key_path(tmp_path):
    """kafka.ssl.key_location is a file path, not a secret — it must NOT be
    masked, while key_password (a real secret) must be. Regression guard: the
    'key' fragment in _is_secret must not match the key_* prefix."""
    cfg_path = _write_config(
        tmp_path,
        'version: "1"\n'
        "kafka:\n"
        "  brokers: [host:9092]\n"
        "  ssl:\n"
        "    ca_location: /etc/ssl/ca.pem\n"
        "    certificate_location: /etc/ssl/client.crt\n"
        "    key_location: /etc/ssl/client.key\n"
        "    key_password: hunter2\n",
    )
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(cfg_path)])
    payload = json.loads(result.output)
    ssl = payload["result"]["kafka"]["ssl"]
    assert ssl["ca_location"] == "/etc/ssl/ca.pem"          # path, not masked
    assert ssl["certificate_location"] == "/etc/ssl/client.crt"
    assert ssl["key_location"] == "/etc/ssl/client.key"      # path, NOT masked
    assert ssl["key_password"] == "***"                      # secret, masked
