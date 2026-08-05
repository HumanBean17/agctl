"""Tests for `agctl config validate` unknown-template-token detection (Task 10).

Scope: ``config validate`` walks the resolved config's string-bearing fields
and rejects unknown ``{{...}}`` tokens (typos like ``{{uuids}}``) via exit
code 2, while valid generators (``{{uuid}}``) and non-matching text
(``{{user.name}}``) pass. This runs alongside the existing
``collect_jq_compile_errors`` / ``collect_capture_placement_errors`` checks.

These tests drive the real Click command via :class:`click.testing.CliRunner`
against a temp ``agctl.yaml`` (the temp-config pattern from
``tests/unit/test_config_commands.py``).
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


# A minimal services block so HTTP templates pass the §3.5.1 service
# cross-reference check performed by validator.py.
_SVC = """
version: "3"
services:
  svc:
    base_url: http://example.com
"""


# --- (a) typo in an HTTP template body ---------------------------------------


def test_validate_unknown_template_token_in_http_body_exits_2(tmp_path):
    """An HTTP template body containing ``{{uuids}}`` (typo) -> exit 2 and an
    error whose ``path`` is the template-body path and whose ``message`` names
    the token (``{{uuids}}``) and the valid generator names."""
    yaml_text = (
        _SVC
        + """
templates:
  bad:
    description: typo template
    method: POST
    service: svc
    path: /things
    body:
      id: "{{uuids}}"
"""
    )
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is False
    errs = payload["result"]["errors"]
    body_errs = [e for e in errs if e["path"] == "templates.bad.body"]
    assert body_errs, (
        f"no error attributed at templates.bad.body; got paths="
        f"{[e['path'] for e in errs]}"
    )
    msg = body_errs[0]["message"]
    assert "{{uuids}}" in msg
    # A valid generator name appears so the operator knows what to fix.
    assert "uuid" in msg


# --- (b) valid generator passes ---------------------------------------------


def test_validate_known_template_token_in_http_body_passes(tmp_path):
    """An HTTP template body containing ``{{uuid}}`` (valid) -> exit 0,
    ``valid: true`` (no template-token error)."""
    yaml_text = (
        _SVC
        + """
templates:
  ok:
    description: valid-generator template
    method: POST
    service: svc
    path: /things
    body:
      id: "{{uuid}}"
"""
    )
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True


# --- (c) non-matching text (dots) is not flagged -----------------------------


def test_validate_nonmatching_template_text_passes(tmp_path):
    """An HTTP template body containing ``{{user.name}}`` (dots -> non-matching,
    skipped by ``find_unknown_templates``) -> exit 0, ``valid: true`` (not
    flagged as an unknown token)."""
    yaml_text = (
        _SVC
        + """
templates:
  ok:
    description: non-matching text template
    method: POST
    service: svc
    path: /things
    body:
      who: "{{user.name}}"
"""
    )
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True


# --- (d) breadth: other config-defined string fields are walked too ----------


def test_validate_unknown_template_token_in_db_sql_exits_2(tmp_path):
    """A DB template ``sql`` containing ``{{uuids}}`` (typo) -> exit 2 and an
    error attributed at the SQL config path. Demonstrates the walker covers DB
    templates, not just HTTP bodies."""
    yaml_text = """
version: "3"
database:
  connections:
    main:
      type: sqlite
      url: "sqlite:///./test.db"
  templates:
    bad:
      description: typo sql
      sql: "SELECT '{{uuids}}'"
"""
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is False
    errs = payload["result"]["errors"]
    sql_errs = [e for e in errs if e["path"] == "database.templates.bad.sql"]
    assert sql_errs, (
        f"no error attributed at database.templates.bad.sql; got paths="
        f"{[e['path'] for e in errs]}"
    )
    assert "{{uuids}}" in sql_errs[0]["message"]


def test_validate_known_template_token_in_kafka_pattern_match_passes(tmp_path):
    """A kafka pattern ``match`` containing ``{{ts}}`` (valid) -> exit 0,
    ``valid: true``. Demonstrates the walker covers kafka patterns and that a
    valid generator with opts is accepted."""
    yaml_text = """
version: "3"
kafka:
  clusters:
    default:
      brokers:
        - localhost:9092
  default_cluster: default
  patterns:
    pat:
      topic: events
      match: '.ts == "{{ts:iso}}"'
"""
    result = _validate(tmp_path, yaml_text)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["result"]["valid"] is True
