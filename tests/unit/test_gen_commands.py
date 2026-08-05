"""Unit tests for `agctl gen uuid|ts|rand` (template-vars Task 3).

These commands are **config-free**: they call the Task-1 generators directly and
must succeed with no ``agctl.yaml`` present. Each subcommand is envelope-wrapped
(``gen.uuid`` / ``gen.ts`` / ``gen.rand``); bad flags become ``ConfigError``
(exit 2) through the envelope.
"""

from __future__ import annotations

import json
import re
import uuid as _uuid

import pytest
from click.testing import CliRunner

from agctl.cli import cli

# UUID v4: 8-4-4-4-12 hex, version nibble 4, variant nibble in [89ab].
# Case-insensitive so an uppercased (--upper) UUID validates just as well.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _run(args):
    """Invoke the root ``cli`` with args; the CliRunner captures stdout."""
    return CliRunner().invoke(cli, args)


def _payload(result):
    """Parse the envelope JSON printed to stdout by ``emit``."""
    return json.loads(result.output)


# --------------------------------------------------------------------------- #
# gen uuid
# --------------------------------------------------------------------------- #


def test_gen_uuid_single():
    result = _run(["gen", "uuid"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["command"] == "gen.uuid"
    assert _UUID_RE.match(payload["result"]["value"]), payload["result"]["value"]


def test_gen_uuid_explicit_count_one_is_value_shape():
    result = _run(["gen", "uuid", "--count", "1"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    # count == 1 -> single "value" key, NOT a "values" list.
    assert "value" in payload["result"]
    assert "values" not in payload["result"]
    assert _UUID_RE.match(payload["result"]["value"])


def test_gen_uuid_count_three_distinct():
    result = _run(["gen", "uuid", "--count", "3"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    values = payload["result"]["values"]
    assert isinstance(values, list)
    assert len(values) == 3
    # All valid v4 UUIDs.
    for v in values:
        assert _UUID_RE.match(v), v
    # All distinct.
    assert len(set(values)) == 3


def test_gen_uuid_upper_single():
    result = _run(["gen", "uuid", "--upper"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    value = payload["result"]["value"]
    assert _UUID_RE.match(value), value
    assert value == value.upper()


def test_gen_uuid_upper_count_two():
    result = _run(["gen", "uuid", "--upper", "--count", "2"])
    assert result.exit_code == 0
    payload = _payload(result)
    values = payload["result"]["values"]
    assert len(values) == 2
    for v in values:
        assert _UUID_RE.match(v), v
        assert v == v.upper()


def test_gen_uuid_count_zero_is_config_error():
    result = _run(["gen", "uuid", "--count", "0"])
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_gen_uuid_count_negative_is_config_error():
    result = _run(["gen", "uuid", "--count", "-1"])
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_gen_uuid_count_non_integer_is_config_error():
    result = _run(["gen", "uuid", "--count", "abc"])
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# gen ts
# --------------------------------------------------------------------------- #


def test_gen_ts_seconds_default():
    result = _run(["gen", "ts"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["command"] == "gen.ts"
    assert re.match(r"^\d{10}$", payload["result"]["value"]), payload["result"]["value"]


def test_gen_ts_ms():
    result = _run(["gen", "ts", "--ms"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert re.match(r"^\d{13}$", payload["result"]["value"]), payload["result"]["value"]


def test_gen_ts_iso():
    result = _run(["gen", "ts", "--iso"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert _ISO_RE.match(payload["result"]["value"]), payload["result"]["value"]


def test_gen_ts_ms_and_iso_mutually_exclusive():
    result = _run(["gen", "ts", "--ms", "--iso"])
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# gen rand
# --------------------------------------------------------------------------- #


def test_gen_rand_default_length_16():
    result = _run(["gen", "rand"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["command"] == "gen.rand"
    assert re.match(r"^[0-9a-f]{16}$", payload["result"]["value"]), payload["result"]["value"]


def test_gen_rand_length_eight():
    result = _run(["gen", "rand", "--length", "8"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert re.match(r"^[0-9a-f]{8}$", payload["result"]["value"]), payload["result"]["value"]


def test_gen_rand_length_non_integer_is_config_error():
    result = _run(["gen", "rand", "--length", "abc"])
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


def test_gen_rand_length_zero_is_config_error():
    result = _run(["gen", "rand", "--length", "0"])
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# Config-free: no agctl.yaml required.
# --------------------------------------------------------------------------- #


def test_gen_uuid_config_free_no_agctl_yaml(tmp_path, monkeypatch):
    """`gen uuid` must succeed with no agctl.yaml and no AGCTL_CONFIG."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGCTL_CONFIG", raising=False)
    assert not (tmp_path / "agctl.yaml").exists()

    result = _run(["gen", "uuid"])
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    # Specifically NOT a config-missing error.
    assert "No agctl.yaml found" not in payload["error"].get("message", "") \
        if not payload["ok"] else True
    # Validate the value is a real UUID (sanity).
    _uuid.UUID(payload["result"]["value"])


def test_gen_group_help_lists_subcommands():
    result = _run(["gen", "--help"])
    assert result.exit_code == 0
    # All three subcommands appear in the help output.
    for sub in ("uuid", "ts", "rand"):
        assert sub in result.output


def test_root_help_lists_gen_group():
    result = _run(["--help"])
    assert result.exit_code == 0
    # Match the command-listing line for `gen` (not the "gen" inside "agent-facing").
    assert re.search(r"(?m)^\s+gen\s", result.output), result.output
