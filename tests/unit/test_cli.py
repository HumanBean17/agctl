import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands.config_commands import _load_sample
from agctl.prime_content import HOOK_SETTINGS_SNIPPET, stub_text

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


def test_config_init_writes_sample(tmp_path, monkeypatch):
    # chdir so the default-on stub install lands in tmp_path, not the repo
    monkeypatch.chdir(tmp_path)
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


def test_config_init_generates_valid_config(tmp_path, monkeypatch):
    """The generated sample is a clean baseline: it validates with no env vars."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    result = CliRunner().invoke(cli, ["config", "validate", "--config", str(dest)], env={})
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["valid"] is True


def test_config_init_refuses_overwrite(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
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


def test_config_init_force_overwrites(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
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
    assert "--grpc-listen" in result.output


def test_version_flag():
    """--version prints 'agctl <version>' and exits 0 (DESIGN: CLI plumbing)."""
    from agctl import __version__

    result = CliRunner().invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output
    assert result.output.strip().startswith("agctl ")


# --- config init: skill stub installation (prime spec) ----------------------


def _stub_path() -> Path:
    return Path.cwd() / ".claude" / "skills" / "agctl" / "SKILL.md"


def test_config_init_installs_stub_by_default(tmp_path, monkeypatch):
    """Default init writes the config AND the router skill stub (byte-identical
    to the packaged one), and carries the hook snippet in the result."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    result = CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    stub = _stub_path()
    assert stub.exists()
    assert stub.read_text(encoding="utf-8") == stub_text()
    assert payload["result"]["skills_status"] == "created"
    assert payload["result"]["skills_path"] == str(stub)
    assert payload["result"]["hook_snippet"] == HOOK_SETTINGS_SNIPPET


def test_config_init_idempotent_identical_stub(tmp_path, monkeypatch):
    """Re-running over an identical stub is a no-op success ('unchanged')."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    result = CliRunner().invoke(
        cli, ["config", "init", "-o", str(dest), "--force", "--skills-only"]
    )
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["skills_status"] == "unchanged"
    assert _stub_path().read_text(encoding="utf-8") == stub_text()


def test_config_init_refuses_modified_stub(tmp_path, monkeypatch):
    """A consumer-modified stub is never silently clobbered: refuse with a
    pointer at --force, leave the file untouched, write nothing else."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    stub = _stub_path()
    stub.write_text("---\nname: agctl\ndescription: consumer-edited\n---\nlocal edits\n")
    dest.write_text("existing: config\n")  # also pre-stage config for --skills-only
    result = CliRunner().invoke(cli, ["config", "init", "--skills-only"])
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert "--force" in payload["error"]["message"]
    assert payload["result"]["skills_status"] == "refused"
    # consumer edits preserved
    assert "consumer-edited" in stub.read_text(encoding="utf-8")


def test_config_init_force_overwrites_modified_stub(tmp_path, monkeypatch):
    """--force surrenders consumer edits back to the packaged stub."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    CliRunner().invoke(cli, ["config", "init", "-o", str(dest)])
    _stub_path().write_text("consumer edits\n")
    result = CliRunner().invoke(cli, ["config", "init", "--skills-only", "--force"])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["result"]["skills_status"] == "overwritten"
    assert _stub_path().read_text(encoding="utf-8") == stub_text()


def test_config_init_no_skills(tmp_path, monkeypatch):
    """--no-skips skips the stub write entirely; no .claude/ tree appears."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    result = CliRunner().invoke(cli, ["config", "init", "-o", str(dest), "--no-skills"])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert not (tmp_path / ".claude").exists()
    assert payload["result"]["skills_status"] == "skipped"
    assert payload["result"]["skills_path"] is None


def test_config_init_skills_only_skips_config(tmp_path, monkeypatch):
    """--skills-only installs/refreshes the stub while leaving an existing
    config byte-untouched (the upgrade path for existing consumers)."""
    monkeypatch.chdir(tmp_path)
    sentinel = tmp_path / "agctl.yaml"
    sentinel.write_text("existing: real-config\n")
    result = CliRunner().invoke(cli, ["config", "init", "--skills-only"])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert sentinel.read_text() == "existing: real-config\n"
    assert _stub_path().read_text(encoding="utf-8") == stub_text()
    assert payload["result"]["path"] is None
    assert payload["result"]["created"] is False
    assert payload["result"]["skills_status"] == "created"


def test_config_init_existing_config_refusal_untouched(tmp_path, monkeypatch):
    """Plain init with an existing config keeps today's refusal AND does not
    write the stub either — no partial bootstrap."""
    monkeypatch.chdir(tmp_path)
    dest = tmp_path / "agctl.yaml"
    dest.write_text("existing: real-config\n")
    result = CliRunner().invoke(cli, ["config", "init"])
    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["result"]["created"] is False
    assert not (tmp_path / ".claude").exists()


def test_stub_packaged_shape():
    """The packaged stub is a thin router: ≤ 20 lines, frontmatter intact,
    points at prime, and carries zero domain content."""
    text = stub_text()
    assert len(text.splitlines()) <= 20
    assert text.startswith("---")
    assert "name: agctl" in text
    assert "description:" in text
    assert "agctl prime" in text
    # zero-domain-content guard: depth lives in prime topics, not the stub
    assert "--match" not in text
    assert "kafka listen" not in text
