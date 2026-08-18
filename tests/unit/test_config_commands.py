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
from agctl.commands.config_commands import collect_unknown_template_errors
from agctl.config import load_config


def _validate(tmp_path, yaml_text):
    """Run `agctl config validate` against a temp config; return the CliRunner result."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(yaml_text)
    return CliRunner().invoke(
        cli,
        ["config", "validate", "--config", str(cfg_file)],
    )


# --- Task 9: unknown ``{{...}}`` tokens in effect fields ----------------------

#: Minimal named-cluster block so kafka effects / reactors load cleanly.
_KAFKA = """
kafka:
  clusters:
    default:
      brokers:
        - localhost:9092
  default_cluster: default
"""


def _load(tmp_path, yaml_text):
    """Write a temp agctl.yaml and load it into a ``Config`` (temp-config pattern)."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(yaml_text)
    return load_config(str(cfg_file), env={})


def _messages_at(errors, path):
    """Join the messages of all errors attributed at ``path`` (empty if none)."""
    return " | ".join(e["message"] for e in errors if e["path"] == path)


def test_collect_unknown_template_errors_stub_kafka_effect_fields(tmp_path):
    """A stub's kafka effect fields are walked: ``{{foo}}`` in ``value`` is
    flagged at ``mocks.http.stubs.<s>.effects[0].value`` while ``{{uuid}}`` in
    the same dict is not; ``topic``/``key``/``headers`` and each ``values[j]``
    element are flagged at their own paths."""
    cfg = _load(
        tmp_path,
        'version: "3"\n'
        + _KAFKA
        + """
mocks:
  http:
    stubs:
      s1:
        method: POST
        path: /orders
        response:
          status: 200
        effects:
          - type: kafka
            topic: "out-{{topy}}"
            key: "{{keyy}}"
            headers:
              h: "{{heady}}"
            value:
              id: "{{uuid}}"
              bad: "{{foo}}"
          - type: kafka
            topic: out2
            values:
              - value: "{{valy}}"
                key: "{{keyy}}"
                headers:
                  h: "{{heady}}"
""",
    )
    errors = collect_unknown_template_errors(cfg)
    base = "mocks.http.stubs.s1.effects"
    assert "{{foo}}" in _messages_at(errors, f"{base}[0].value")
    assert "{{topy}}" in _messages_at(errors, f"{base}[0].topic")
    assert "{{keyy}}" in _messages_at(errors, f"{base}[0].key")
    assert "{{heady}}" in _messages_at(errors, f"{base}[0].headers")
    assert "{{valy}}" in _messages_at(errors, f"{base}[1].values[0].value")
    assert "{{keyy}}" in _messages_at(errors, f"{base}[1].values[0].key")
    assert "{{heady}}" in _messages_at(errors, f"{base}[1].values[0].headers")
    # A known generator riding in the same value dict is NOT flagged.
    assert not any("{{uuid}}" in e["message"] for e in errors)


def test_collect_unknown_template_errors_stub_http_effect_fields(tmp_path):
    """A stub's http effect fields (url/path/body/headers) are walked; each
    unknown token is flagged at its own ``effects[i].<field>`` path."""
    cfg = _load(
        tmp_path,
        'version: "3"\n'
        + _KAFKA
        + """
mocks:
  http:
    stubs:
      s1:
        method: POST
        path: /orders
        response:
          status: 200
        effects:
          - type: http
            url: "https://x/{{bary}}"
            path: "/p/{{pathy}}"
            body:
              note: "{{bodyy}}"
            headers:
              X-H: "{{heady}}"
""",
    )
    errors = collect_unknown_template_errors(cfg)
    base = "mocks.http.stubs.s1.effects[0]"
    assert "{{bary}}" in _messages_at(errors, f"{base}.url")
    assert "{{pathy}}" in _messages_at(errors, f"{base}.path")
    assert "{{bodyy}}" in _messages_at(errors, f"{base}.body")
    assert "{{heady}}" in _messages_at(errors, f"{base}.headers")


def test_collect_unknown_template_errors_reactor_effect_fields(tmp_path):
    """A reactor's effect list is walked under the ``mocks.kafka.reactors``
    prefix: kafka ``effects[0].value`` and http ``effects[1].url`` tokens are
    each flagged at their own path."""
    cfg = _load(
        tmp_path,
        'version: "3"\n'
        + _KAFKA
        + """
mocks:
  kafka:
    reactors:
      r1:
        topic: in
        effects:
          - type: kafka
            topic: out
            value: "{{foo}}"
          - type: http
            url: "https://y/{{bar}}"
""",
    )
    errors = collect_unknown_template_errors(cfg)
    assert "{{foo}}" in _messages_at(errors, "mocks.kafka.reactors.r1.effects[0].value")
    assert "{{bar}}" in _messages_at(errors, "mocks.kafka.reactors.r1.effects[1].url")


def test_collect_unknown_template_errors_effects_only_reactor_does_not_raise(tmp_path):
    """An effects-only reactor (``reaction`` is None — legal since T1) must not
    crash the collector: the reaction-field checks are skipped for it and a
    clean effect list yields no errors."""
    cfg = _load(
        tmp_path,
        'version: "3"\n'
        + _KAFKA
        + """
mocks:
  kafka:
    reactors:
      r1:
        topic: in
        effects:
          - type: http
            url: "https://y/{{uuid}}"
""",
    )
    assert collect_unknown_template_errors(cfg) == []


# --- (a) malformed HTTP stub match.jq -----------------------------------------


def test_validate_malformed_http_stub_jq_exits_2(tmp_path):
    """A stub whose match.jq is ')(' -> exit 2 and an error whose path is
    ``mocks.http.stubs.<name>.match.jq`` (the jq-compile error surfaced by
    collect_jq_compile_errors)."""
    yaml_text = """
version: "3"
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
version: "3"
kafka:
  clusters:
    default:
      brokers:
        - localhost:9092
  default_cluster: default
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
version: "3"
kafka:
  clusters:
    default:
      brokers:
        - localhost:9092
  default_cluster: default
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
    result = _validate(tmp_path, 'version: "3"\n')
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True


# --- Task 5: config validate --overlay (override warnings) --------------------


def test_validate_override_warning_emitted(tmp_path):
    """Scenario 1: Override warning emitted when overlay overrides templates.create-order."""
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
