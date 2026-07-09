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
    """PartialConfig.model_validate({"version": "3"}) returns version="3"."""
    result = PartialConfig.model_validate({"version": "3"})
    assert result.version == "3"


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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    """Overlay has no version → no error; config.version == '3'."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
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
    assert result.config.version == "3"


def test_compose_config_overlay_version_mismatch(tmp_path):
    """Overlay has a stale version ('2' under the v3 tool) → ConfigError with
    detail['overlay'] set."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
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
    ov.write_text("""version: "2"
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
    assert exc_info.value.detail["found"] == "2"


def test_compose_config_bad_overlay_fragment(tmp_path):
    """Overlay has templates.bad missing required method → ConfigError with detail['overlay'] set."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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
    base.write_text("""version: "3"
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


# Fix 1: overlay version-only test
def test_compose_config_overlay_version_only_no_override_recorded(tmp_path):
    """Overlay with only version: '2' on v2 base → no override recorded, version inherited."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
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
""")
    result = compose_config(str(base), [str(ov)])
    # No overrides should be recorded for version
    assert result.overrides == []
    # Version should be inherited from base
    assert result.config.version == "3"


# Fix 2: multiple overlays later wins test
def test_compose_config_multiple_overlays_later_wins(tmp_path):
    """Two overlays both setting templates.x.path → later wins, single override record per path."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
templates:
  x:
    method: GET
    service: orders
    path: /original
""")
    ov1 = tmp_path / "overlay1.yaml"
    ov1.write_text("""templates:
  x:
    method: GET
    service: orders
    path: /a
""")
    ov2 = tmp_path / "overlay2.yaml"
    ov2.write_text("""templates:
  x:
    method: GET
    service: orders
    path: /b
""")
    result = compose_config(str(base), [str(ov1), str(ov2)])
    # Later overlay wins for the value
    assert result.config.templates["x"].path == "/b"
    # All three fields have overrides (method, service, path), but all should be from ov2 (deduped)
    assert len(result.overrides) == 3
    # Verify no override references ov1 - all should be from ov2 (last writer wins)
    assert all(o["overlay"] == str(ov2) for o in result.overrides)
    # Verify the specific path override is from ov2
    path_override = [o for o in result.overrides if o["path"] == "templates.x.path"]
    assert len(path_override) == 1
    assert path_override[0]["overlay"] == str(ov2)


# Task 7: Thread --overlay into runtime commands
from pathlib import Path
from agctl.config.models import Config, ServiceConfig


def test_http_call_forwards_overlay(tmp_path, monkeypatch):
    """http call forwards overlay_paths from ctx.obj to load_config_or_raise."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
templates:
  get-order:
    method: GET
    service: orders
    path: /api/v1/orders
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /extra
""")

    # Track what overlay_paths was passed to load_config_or_raise
    captured_overlays = []

    def fake_load_config(config_path, overlay_paths=None):
        captured_overlays.append(overlay_paths)
        from agctl.config.models import HttpTemplate
        return Config(
            version="2",
            services={"orders": ServiceConfig(base_url="http://localhost:8081")},
            templates={
                "get-order": HttpTemplate(
                    method="GET",
                    service="orders",
                    path="/api/v1/orders"
                )
            }
        )

    # Patch at the module level where it's imported
    monkeypatch.setattr("agctl.commands.http_commands.load_config_or_raise", fake_load_config)

    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "http", "call", "get-order"]
    )

    # Debug output
    if len(captured_overlays) != 1:
        print(f"\nExit code: {result.exit_code}")
        print(f"Output: {result.output}")
        if result.exception:
            import traceback
            print(f"Exception: {''.join(traceback.format_exception(type(result.exception), result.exception, result.exception.__traceback__))}")

    # Should have attempted to load config with overlay
    assert len(captured_overlays) == 1
    assert captured_overlays[0] == [str(ov)]


def test_db_query_forwards_overlay(tmp_path, monkeypatch):
    """db query forwards overlay_paths from ctx.obj to load_config_or_raise."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
database:
  connections:
    main:
      type: sqlite
      path: /tmp/db.sqlite
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""database:
  templates:
    extra:
      sql: SELECT 1
""")

    captured_overlays = []

    def fake_load_config(config_path, overlay_paths=None):
        captured_overlays.append(overlay_paths)
        from agctl.config.models import DatabaseConfig, ConnectionConfig
        return Config(
            version="2",
            services={"orders": ServiceConfig(base_url="http://localhost:8081")},
            database=DatabaseConfig(
                connections={"main": ConnectionConfig(type="sqlite", path="/tmp/db.sqlite")}
            )
        )

    monkeypatch.setattr("agctl.commands.db_commands.load_config_or_raise", fake_load_config)

    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "db", "query", "--sql", "SELECT 1"]
    )

    assert len(captured_overlays) == 1
    assert captured_overlays[0] == [str(ov)]


def test_kafka_produce_forwards_overlay(tmp_path, monkeypatch):
    """kafka produce forwards overlay_paths from ctx.obj to load_config_or_raise."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
kafka:
  brokers:
    - localhost:9092
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""kafka:
  patterns:
    extra:
      topic: test
      match: .event == "test"
""")

    captured_overlays = []

    def fake_load_config(config_path, overlay_paths=None):
        captured_overlays.append(overlay_paths)
        from agctl.config.models import KafkaCluster, KafkaConfig
        return Config(
            version="3",
            services={"orders": ServiceConfig(base_url="http://localhost:8081")},
            kafka=KafkaConfig(
                clusters={"default": KafkaCluster(brokers=["localhost:9092"])}
            ),
        )

    monkeypatch.setattr("agctl.commands.kafka_commands.load_config_or_raise", fake_load_config)

    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "kafka", "produce", "--topic", "test", "--message", "{}"]
    )

    assert len(captured_overlays) == 1
    assert captured_overlays[0] == [str(ov)]


def test_check_ready_forwards_overlay(tmp_path, monkeypatch):
    """check ready forwards overlay_paths from ctx.obj to load_config_or_raise."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
    health_path: /health
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""services:
  orders:
    health_path: /health2
""")

    captured_overlays = []

    def fake_load_config(config_path, overlay_paths=None):
        captured_overlays.append(overlay_paths)
        return Config(
            version="2",
            services={"orders": ServiceConfig(base_url="http://localhost:8081", health_path="/health")}
        )

    monkeypatch.setattr("agctl.commands.check_commands.load_config_or_raise", fake_load_config)

    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "check", "ready"]
    )

    assert len(captured_overlays) == 1
    assert captured_overlays[0] == [str(ov)]


def test_mock_run_forwards_overlay(tmp_path, monkeypatch):
    """mock run forwards overlay_paths from ctx.obj to load_config_or_raise."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""mocks:
  http:
    listen: "0.0.0.0:18080"
""")

    captured_overlays = []

    def fake_load_config(config_path, overlay_paths=None):
        captured_overlays.append(overlay_paths)
        # Return minimal valid config (no mocks needed for this test)
        return Config(
            version="2",
            services={"orders": ServiceConfig(base_url="http://localhost:8081")}
        )

    # For mock run we need to patch at the command level since it's not @envelope-wrapped
    monkeypatch.setattr("agctl.commands.mock_commands.load_config_or_raise", fake_load_config)

    # Also need to patch new_mock_engine to prevent actual engine startup
    from unittest.mock import MagicMock
    fake_engine = MagicMock()
    fake_engine.start = MagicMock()
    fake_engine.run = MagicMock(return_value=0)
    fake_engine.shutdown = MagicMock()
    monkeypatch.setattr("agctl.commands.mock_commands.new_mock_engine", lambda **kwargs: fake_engine)

    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "mock", "run"],
        catch_exceptions=False
    )

    assert len(captured_overlays) == 1
    assert captured_overlays[0] == [str(ov)]


def test_http_ping_forwards_overlay(tmp_path, monkeypatch):
    """http ping forwards overlay_paths from ctx.obj to load_config_or_raise."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
templates:
  health-check:
    method: GET
    service: orders
    path: /health
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""templates:
  extra:
    method: GET
    service: orders
    path: /extra
""")

    captured_overlays = []

    def fake_load_config(config_path, overlay_paths=None):
        captured_overlays.append(overlay_paths)
        from agctl.config.models import HttpTemplate
        return Config(
            version="2",
            services={"orders": ServiceConfig(base_url="http://localhost:8081")},
            templates={
                "health-check": HttpTemplate(
                    method="GET",
                    service="orders",
                    path="/health"
                )
            }
        )

    # Patch at the module level where it's imported
    monkeypatch.setattr("agctl.commands.http_commands.load_config_or_raise", fake_load_config)

    # Use --url mode to avoid template resolution complexity (the monkeypatch short-circuits config load)
    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "http", "ping", "--url", "http://localhost:8081/health", "--interval", "1", "--duration", "0.1"]
    )

    # Should have attempted to load config with overlay
    assert len(captured_overlays) == 1
    assert captured_overlays[0] == [str(ov)]

