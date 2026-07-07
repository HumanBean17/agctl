"""Tests for PartialConfig overlay model."""

import pytest
from pydantic import ValidationError

from agctl.config.models import Config, HttpTemplate, PartialConfig


def test_partial_config_accepts_empty_dict():
    """PartialConfig.model_validate({}) returns instance with version=None and default-empty sections."""
    result = PartialConfig.model_validate({})
    assert result.version is None
    assert result.templates == {}
    assert result.services == {}
    assert result.mocks is None


def test_partial_config_accepts_version():
    """PartialConfig.model_validate({"version": "2"}) returns version="2"."""
    result = PartialConfig.model_validate({"version": "2"})
    assert result.version == "2"


def test_partial_config_validates_sections_without_version():
    """PartialConfig validates sections even without version key."""
    result = PartialConfig.model_validate(
        {
            "templates": {
                "t": {
                    "method": "GET",
                    "service": "svc",
                    "path": "/"
                }
            }
        }
    )
    assert isinstance(result.templates["t"], HttpTemplate)
    assert result.templates["t"].method == "GET"
    assert result.templates["t"].service == "svc"
    assert result.templates["t"].path == "/"


def test_base_config_requires_version():
    """Config.model_validate({}) raises ValidationError (version still required on base model)."""
    with pytest.raises(ValidationError) as exc_info:
        Config.model_validate({})
    # Verify the error is about the missing version field
    errors = exc_info.value.errors()
    assert any(err["loc"] == ("version",) for err in errors)


# Task 2: deep_merge tests
from agctl.config.loader import deep_merge


def test_deep_merge_addition():
    """Overlay key absent in base → merged has it; overrides empty."""
    base = {"a": 1}
    overlay = {"b": 2}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"a": 1, "b": 2}
    assert base == {"a": 1, "b": 2}  # mutated in place
    assert overrides == []


def test_deep_merge_scalar_override():
    """Both have scalar at same key → overlay wins; override recorded."""
    base = {"a": 1}
    overlay = {"a": 2}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"a": 2}
    assert overrides == [{"path": "a", "overlay": "sidecar.yaml"}]


def test_deep_merge_list_replace():
    """Lists replace (not extend); override recorded."""
    base = {"brokers": ["x"]}
    overlay = {"brokers": ["y", "z"]}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"brokers": ["y", "z"]}
    assert overrides == [{"path": "brokers", "overlay": "sidecar.yaml"}]


def test_deep_merge_nested_dict_merge():
    """Nested dicts merge key-by-key; only leaf override recorded."""
    base = {"templates": {"keep": 1, "shared": {"m": "GET"}}}
    overlay = {"templates": {"new": 2, "shared": {"m": "POST"}}}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {
        "templates": {
            "keep": 1,
            "new": 2,
            "shared": {"m": "POST"}
        }
    }
    assert overrides == [{"path": "templates.shared.m", "overlay": "sidecar.yaml"}]


def test_deep_merge_type_clash():
    """Dict vs scalar at same key → override wins and is recorded."""
    base = {"x": {"a": 1}}
    overlay = {"x": "scalar"}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"x": "scalar"}
    assert overrides == [{"path": "x", "overlay": "sidecar.yaml"}]


def test_deep_merge_dotted_path_nesting():
    """Dotted path builds correctly across multiple levels."""
    base = {"kafka": {"patterns": {"foo": {"match": "old"}}}}
    overlay = {"kafka": {"patterns": {"foo": {"match": "new"}}}}
    overrides = []
    result = deep_merge(base, overlay, "sidecar.yaml", overrides)
    assert result == {"kafka": {"patterns": {"foo": {"match": "new"}}}}
    assert overrides == [{"path": "kafka.patterns.foo.match", "overlay": "sidecar.yaml"}]


# Task 3: compose_config tests
import pathlib
from agctl.config import load_config
from agctl.config.loader import compose_config, ComposedConfig
from agctl.errors import ConfigError


def test_compose_config_overlay_adds_template(tmp_path):
    """Overlay adds a template → config has both; overrides empty."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /api/v1/orders/{id}
""")
    result = compose_config(str(base), [str(ov)])
    assert "create-order" in result.config.templates
    assert "extra" in result.config.templates
    assert result.config.templates["extra"].path == "/api/v1/orders/{id}"
    assert result.overrides == []


def test_compose_config_override_recorded(tmp_path):
    """Base and overlay both define templates.create-order → overlay wins; override recorded."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  create-order:
    method: PUT
    service: orders
    path: /api/v1/orders/{id}
""")
    result = compose_config(str(base), [str(ov)])
    assert result.config.templates["create-order"].method == "PUT"
    assert result.config.templates["create-order"].path == "/api/v1/orders/{id}"
    assert len(result.overrides) == 3
    assert any(o["path"] == "templates.create-order.method" and o["overlay"] == str(ov) for o in result.overrides)
    assert any(o["path"] == "templates.create-order.path" and o["overlay"] == str(ov) for o in result.overrides)


def test_compose_config_version_inherited(tmp_path):
    """Overlay has no version → no error; config.version == '2'."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /api/v1/orders/{id}
""")
    result = compose_config(str(base), [str(ov)])
    assert result.config.version == "2"


def test_compose_config_overlay_version_mismatch(tmp_path):
    """Overlay has version: '3' → ConfigError with detail['overlay'] set."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""version: "3"
templates:
  extra:
    method: GET
    service: orders
    path: /api/v1/orders/{id}
""")
    with pytest.raises(ConfigError) as exc_info:
        compose_config(str(base), [str(ov)])
    assert "version mismatch" in exc_info.value.message.lower()
    assert exc_info.value.detail["overlay"] == str(ov)
    assert exc_info.value.detail["found"] == "3"


def test_compose_config_bad_overlay_fragment(tmp_path):
    """Overlay has templates.bad missing required method → ConfigError with detail['overlay'] set."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  bad:
    service: orders
    path: /api/v1/orders/{id}
""")
    with pytest.raises(ConfigError) as exc_info:
        compose_config(str(base), [str(ov)])
    assert "invalid overlay" in exc_info.value.message.lower()
    assert exc_info.value.detail["overlay"] == str(ov)
    assert "validation_errors" in exc_info.value.detail


def test_compose_config_type_clash_final_validate(tmp_path):
    """Base templates.x is valid; overlay replaces with scalar → caught at PartialConfig validation."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  x:
    method: GET
    service: orders
    path: /x
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  x: not-a-template
""")
    with pytest.raises(ConfigError) as exc_info:
        compose_config(str(base), [str(ov)])
    assert "invalid overlay" in exc_info.value.message.lower()
    assert exc_info.value.detail["overlay"] == str(ov)
    assert "validation_errors" in exc_info.value.detail


def test_compose_config_env_wins_over_overlay(tmp_path):
    """Base sets path=/a, overlay sets /b, env sets /env → final value is /env."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  x:
    method: GET
    service: orders
    path: /a
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  x:
    method: GET
    service: orders
    path: /b
""")
    result = compose_config(str(base), [str(ov)], env={"AGCTL_TEMPLATES__X__PATH": "/env"})
    assert result.config.templates["x"].path == "/env"


def test_compose_config_missing_overlay_file(tmp_path):
    """Overlay file not found → ConfigError with detail['path'] set."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    with pytest.raises(ConfigError) as exc_info:
        compose_config(str(base), [str(tmp_path / "nope.yaml")])
    assert "overlay file not found" in exc_info.value.message.lower()
    assert exc_info.value.detail["path"] == str(tmp_path / "nope.yaml")


def test_load_config_forwards_overlays(tmp_path):
    """load_config(path, overlays=[...]) returns Config with overlay's added template."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /api/v1/orders/{id}
""")
    result = load_config(str(base), overlays=[str(ov)])
    assert "create-order" in result.templates
    assert "extra" in result.templates
    assert result.templates["extra"].path == "/api/v1/orders/{id}"


def test_load_config_no_overlay_back_compat(tmp_path):
    """load_config(path) and load_config(path, env={}) work as before (back-compat)."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    # 1-arg form
    result1 = load_config(str(base))
    assert result1.templates["create-order"].path == "/api/v1/orders"
    # 2-arg form
    result2 = load_config(str(base), env={})
    assert result2.templates["create-order"].path == "/api/v1/orders"


# Task 4: CLI --overlay flag and config show --overlay
import json
from click.testing import CliRunner
from agctl.cli import cli


def test_global_overlay_flag_threads_to_show(tmp_path):
    """Scenario 1: Global --overlay flag threads to config show."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /api/v1/orders/{id}
""")
    result = CliRunner().invoke(cli, ["--overlay", str(ov), "config", "show", "--config", str(base)])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    # With --overlay, result should be wrapped: {"config": ..., "overrides": ...}
    assert "config" in payload["result"]
    assert "overrides" in payload["result"]
    # Base template present
    assert "create-order" in payload["result"]["config"]["templates"]
    # Overlay template added
    assert "extra" in payload["result"]["config"]["templates"]
    assert payload["result"]["config"]["templates"]["extra"]["path"] == "/api/v1/orders/{id}"
    # No overrides (addition only)
    assert payload["result"]["overrides"] == []


def test_override_surfaced_in_show(tmp_path):
    """Scenario 2: Override surfaced in show result."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  create-order:
    method: PUT
    service: orders
    path: /api/v1/orders/{id}
""")
    result = CliRunner().invoke(cli, ["--overlay", str(ov), "config", "show", "--config", str(base)])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert "config" in payload["result"]
    assert "overrides" in payload["result"]
    # Overlay value wins
    assert payload["result"]["config"]["templates"]["create-order"]["method"] == "PUT"
    assert payload["result"]["config"]["templates"]["create-order"]["path"] == "/api/v1/orders/{id}"
    # Override recorded (deep_merge records leaf overrides)
    assert len(payload["result"]["overrides"]) > 0
    # Check that some override under templates.create-order exists
    assert any(o["path"].startswith("templates.create-order.") for o in payload["result"]["overrides"])
    assert all(o["overlay"] == str(ov) for o in payload["result"]["overrides"])


def test_no_overlay_shape_unchanged(tmp_path):
    """Scenario 3: No --overlay shape unchanged (back-compat)."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(base)])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    # Without --overlay, result is the config dict directly (no wrapper)
    assert "templates" in payload["result"]
    assert "config" not in payload["result"]
    assert "overrides" not in payload["result"]
    assert "create-order" in payload["result"]["templates"]


def test_post_command_overlay_form(tmp_path):
    """Scenario 4: Post-command --overlay form (own option precedence)."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "2"
services:
  orders:
    base_url: http://localhost:8081
templates:
  create-order:
    method: POST
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /api/v1/orders/{id}
""")
    result = CliRunner().invoke(cli, ["config", "show", "--config", str(base), "--overlay", str(ov)])
    payload = json.loads(result.output)
    assert result.exit_code == 0
    # Same behavior as global form
    assert "config" in payload["result"]
    assert "overrides" in payload["result"]
    assert "extra" in payload["result"]["config"]["templates"]
    assert payload["result"]["overrides"] == []

