"""Tests for `agctl config validate` command-level behavior (Task 10).

Scope: the ``config validate`` Click command surfaces jq-compile errors from
``collect_jq_compile_errors`` (Task 4) alongside schema/cross-reference errors.
A malformed HTTP stub ``match.jq`` or Kafka reactor ``match`` is reported as a
validation error ``{path, message}`` with exit code 2. This is the
``config validate`` half of D5 (the ``mock run`` half is Task 5).

These tests drive the real Click command via :class:`click.testing.CliRunner`
against a temp ``agctl.yaml`` written via ``tmp_path`` (the temp-config pattern
from ``tests/unit/test_loader.py``). Layering: the jq merge lives in the
command layer (``config_commands.py``), not in ``validator.py``.
"""

import json

from click.testing import CliRunner

from agctl.cli import cli


def _validate(tmp_path, yaml_text):
    """Run `agctl config validate` against a temp config; return the CliRunner result."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(yaml_text)
    return CliRunner().invoke(
        cli,
        ["config", "validate", "--config", str(cfg_file)],
    )


# --- (a) malformed HTTP stub match.jq -----------------------------------------


def test_validate_malformed_http_stub_jq_exits_2(tmp_path):
    """A stub whose match.jq is ')(' -> exit 2 and an error whose path is
    ``mocks.http.stubs.<name>.match.jq`` (the jq-compile error surfaced by
    collect_jq_compile_errors)."""
    yaml_text = """
version: "2"
mocks:
  http:
    stubs:
      bad-stub:
        description: malformed jq stub
        method: POST
        path: /orders
        match:
          jq: ")("
        response:
          status: 200
"""
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is False
    paths = [e["path"] for e in payload["result"]["errors"]]
    assert "mocks.http.stubs.bad-stub.match.jq" in paths


# --- (b) malformed Kafka reactor match ----------------------------------------


def test_validate_malformed_kafka_reactor_match_exits_2(tmp_path):
    """A Kafka reactor whose match is malformed -> exit 2 and an error whose
    path is ``mocks.kafka.reactors.<name>.match``."""
    yaml_text = """
version: "2"
kafka:
  brokers:
    - localhost:9092
mocks:
  kafka:
    reactors:
      bad-reactor:
        description: malformed reactor match
        topic: orders
        match: ".unclosed >"
        reaction:
          topic: out
          value: 1
"""
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is False
    paths = [e["path"] for e in payload["result"]["errors"]]
    assert "mocks.kafka.reactors.bad-reactor.match" in paths


# --- (c) fully-valid config ---------------------------------------------------


def test_validate_fully_valid_config_exits_0(tmp_path):
    """A config with a well-formed match.jq and reactor match -> exit 0,
    ``valid: true`` (no jq-compile errors)."""
    yaml_text = """
version: "2"
kafka:
  brokers:
    - localhost:9092
mocks:
  http:
    stubs:
      ok-stub:
        description: valid jq stub
        method: POST
        path: /orders
        match:
          jq: ".amount > 1000"
        response:
          status: 201
  kafka:
    reactors:
      ok-reactor:
        description: valid reactor match
        topic: orders.created
        match: '.eventType == "ORDER_CREATED"'
        reaction:
          topic: out
          value:
            ok: true
"""
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True


# --- (d) no mocks section -----------------------------------------------------


def test_validate_no_mocks_section_exits_0(tmp_path):
    """A config with no ``mocks`` section -> exit 0 (collector returns [])."""
    result = _validate(tmp_path, 'version: "2"\n')
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True


# --- Task 5: config validate --overlay (override warnings) --------------------


def test_validate_override_warning_emitted(tmp_path):
    """Scenario 1: Override warning emitted when overlay overrides templates.create-order."""
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
    result = CliRunner().invoke(
        cli,
        ["config", "validate", "--config", str(base), "--overlay", str(ov)],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True
    # Check that override warnings are present
    warnings = payload["result"].get("warnings", [])
    override_warnings = [w for w in warnings if "overridden by overlay" in w.get("message", "")]
    assert len(override_warnings) > 0
    # Check that at least one warning mentions templates.create-order path
    override_warnings_templates = [w for w in override_warnings if "templates.create-order" in w.get("path", "")]
    assert len(override_warnings_templates) > 0
    # Check that the warning contains the overlay filename
    assert any("overlay.yaml" in w.get("message", "") for w in override_warnings)


def test_validate_no_override_no_warning(tmp_path):
    """Scenario 2: No override warning when overlay only adds a new template."""
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
    result = CliRunner().invoke(
        cli,
        ["config", "validate", "--config", str(base), "--overlay", str(ov)],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True
    # Check that no override warnings are present
    warnings = payload["result"].get("warnings", [])
    override_warnings = [w for w in warnings if "overridden by overlay" in w.get("message", "")]
    assert len(override_warnings) == 0


def test_validate_cross_file_dangling_ref_error(tmp_path):
    """Scenario 3: Cross-file dangling ref is still an error."""
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
  x:
    method: GET
    service: ghost
    path: /x
""")
    result = CliRunner().invoke(
        cli,
        ["config", "validate", "--config", str(base), "--overlay", str(ov)],
    )
    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is False
    # Check that the error path is templates.x.service
    errors = payload["result"].get("errors", [])
    service_errors = [e for e in errors if e.get("path") == "templates.x.service"]
    assert len(service_errors) > 0


def test_validate_global_overlay_form_threads(tmp_path):
    """Scenario 4: Global --overlay form threads to config validate (same as scenario 1)."""
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
    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "config", "validate", "--config", str(base)],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True
    # Check that override warnings are present
    warnings = payload["result"].get("warnings", [])
    override_warnings = [w for w in warnings if "overridden by overlay" in w.get("message", "")]
    assert len(override_warnings) > 0
    # Check that at least one warning mentions templates.create-order path
    override_warnings_templates = [w for w in override_warnings if "templates.create-order" in w.get("path", "")]
    assert len(override_warnings_templates) > 0
