from pathlib import Path

import pytest

from agctl.config import ConfigError, load_config

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"


def _env(**extra):
    env = {
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "KAFKA_BROKER": "localhost",
        "DB_HOST": "h",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
    }
    env.update(extra)
    return env


def test_load_full_config_keeps_connections_and_templates():
    """§5 verification test / D1 regression: both sections survive loading."""
    cfg = load_config(str(FIXTURE), env=_env())
    assert cfg.database.connections["main-db"].host == "h"
    assert cfg.database.templates["find-order"].connection == "main-db"
    assert cfg.services["order-service"].base_url == "http://localhost:8081"
    assert cfg.kafka.schema_registry_url == ""  # ${VAR:-} resolved to empty


def test_missing_required_env_raises(tmp_path):
    with pytest.raises(ConfigError) as exc:
        load_config(str(FIXTURE), env={})  # ORDER_SERVICE_URL etc. missing
    assert "DB_HOST" in exc.value.detail["variables"]


def test_agctl_override_applied(tmp_path):
    cfg = load_config(str(FIXTURE), env=_env(AGCTL_DEFAULTS__TIMEOUT_SECONDS="99"))
    assert cfg.defaults.timeout_seconds == 99


def test_version_mismatch_raises(tmp_path):
    bad = tmp_path / "agctl.yaml"
    bad.write_text('version: "2"\n')
    with pytest.raises(ConfigError) as exc:
        load_config(str(bad), env={})
    assert exc.value.detail["tool_major"] == "1"


def test_invalid_schema_raises(tmp_path):
    bad = tmp_path / "agctl.yaml"
    bad.write_text('version: "1"\ntemplates:\n  x:\n    method: GET\n')  # missing service/path
    with pytest.raises(ConfigError):
        load_config(str(bad), env={})
