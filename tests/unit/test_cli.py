import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands.config_commands import _load_sample

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
version: "3"
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
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    default:\n"
        "      brokers: [host:9092]\n"
        "      ssl:\n"
        "        ca_location: /etc/ssl/ca.pem\n"
        "        certificate_location: /etc/ssl/client.crt\n"
        "        key_location: /etc/ssl/client.key\n"
        "        key_password: hunter2\n",
    )
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(cfg_path)])
    payload = json.loads(result.output)
    ssl = payload["result"]["kafka"]["clusters"]["default"]["ssl"]
    assert ssl["ca_location"] == "/etc/ssl/ca.pem"          # path, not masked
    assert ssl["certificate_location"] == "/etc/ssl/client.crt"
    assert ssl["key_location"] == "/etc/ssl/client.key"      # path, NOT masked
    assert ssl["key_password"] == "***"                      # secret, masked


# --- config init -----------------------------------------------------------


def test_config_init_writes_sample(tmp_path):
    dest = tmp_path / "agctl.yaml"
    result = CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["command"] == "config.init"
    assert payload["result"]["created"] is True
    assert payload["result"]["path"] == str(dest)
    # file content is the packaged sample, verbatim, and parses as YAML
    assert dest.read_text(encoding="utf-8") == _load_sample()
    yaml.safe_load(dest.read_text(encoding="utf-8"))


def test_config_init_generates_valid_config(tmp_path):
    """The generated sample is a clean baseline: it validates with no env vars."""
    dest = tmp_path / "agctl.yaml"
    CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(dest)], env={})
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["valid"] is True


def test_config_init_refuses_overwrite(tmp_path):
    dest = tmp_path / "agctl.yaml"
    dest.write_text("existing: real-config\n")
    result = CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["result"]["created"] is False
    assert "--force" in payload["error"]["message"]
    # existing file is left untouched
    assert dest.read_text() == "existing: real-config\n"


def test_config_init_force_overwrites(tmp_path):
    dest = tmp_path / "agctl.yaml"
    dest.write_text("OLD\n")
    result = CliRunner().invoke(cli, ["config", "init", "-o", str(dest), "--force"])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["created"] is True
    assert dest.read_text(encoding="utf-8") == _load_sample()


def test_config_init_default_path():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["config", "init"])
        payload = json.loads(result.output)
        assert result.exit_code == 0
        assert payload["result"]["path"].endswith("agctl.yaml")
        assert Path("agctl.yaml").exists()


def test_sample_matches_readme_block():
    """Drift guard: the packaged sample must stay byte-identical to the
    copy-paste block in README.md, so users never see two diverging samples."""
    readme = Path(__file__).parent.parent.parent / "README.md"
    text = readme.read_text(encoding="utf-8")
    start = text.index("```yaml", text.index("Complete, copy-paste-ready config"))
    fence_start = start + len("```yaml")
    fence_end = text.index("```", fence_start)
    readme_block = text[fence_start:fence_end]
    assert readme_block.strip() == _load_sample().strip()


def test_mock_run_help_exits_zero():
    """mock run --help exits 0 and lists the flags."""
    result = CliRunner().invoke(cli, ["mock", "run", "--help"])
    assert result.exit_code == 0
    assert "--only" in result.output
    assert "--fail-fast" in result.output
    assert "--http-listen" in result.output
    assert "--duration" in result.output
    assert "--until-stopped" in result.output


def test_version_flag():
    """--version prints 'agctl <version>' and exits 0 (DESIGN: CLI plumbing)."""
    from agctl import __version__

    result = CliRunner().invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output
    assert result.output.strip().startswith("agctl ")
