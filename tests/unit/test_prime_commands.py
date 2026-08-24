"""Tests for the ``agctl prime`` command (spec:
``2026-08-19-agctl-prime-skills-replacement-design``).

prime is a config-free documentation command: raw markdown on stdout, exit 0,
no JSON envelope (a ``--help``-class exception). The tests pin the default
output, the config-free property, and root-group registration.
"""

from __future__ import annotations

import json

from click.testing import CliRunner

from agctl.cli import cli
from agctl.errors import ConfigError
from agctl.prime_content import (
    TOPIC_ORDER,
    hook_pointer,
    render_all,
    render_core,
    topic_text,
)


def test_prime_default_outputs_core():
    """`agctl prime` prints exactly the rendered core manual and exits 0 —
    in an empty directory, proving it needs no agctl.yaml."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["prime"])
    assert result.exit_code == 0
    assert result.output == render_core() + "\n"


def test_prime_registered_on_root_help():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "prime" in result.output


def test_prime_topic_single():
    """`--topic <name>` emits that topic verbatim."""
    result = CliRunner().invoke(cli, ["prime", "--topic", "mock"])
    assert result.exit_code == 0
    assert result.output == topic_text("mock") + "\n"


def test_prime_topic_repeatable_argument_order():
    """Repeated `--topic` concatenates in argument order, not registry order."""
    result = CliRunner().invoke(cli, ["prime", "--topic", "grpc", "--topic", "mock"])
    assert result.exit_code == 0
    assert result.output == f"{topic_text('grpc')}\n\n{topic_text('mock')}\n"


def test_prime_topic_unknown_exit2():
    """Unknown topic (incl. the empty string) is a usage error: exit 2,
    message on stderr, stdout empty, valid topics listed."""
    runner = CliRunner()
    for bad in ("bogus", ""):
        result = runner.invoke(cli, ["prime", "--topic", bad])
        assert result.exit_code == 2
        assert result.stdout == ""
        assert "Unknown topic" in result.stderr
        for name in TOPIC_ORDER:
            assert name in result.stderr


def test_prime_all_equals_concatenation():
    """`--all` = core + every topic in registry order (the relation, not a
    fixture blob)."""
    result = CliRunner().invoke(cli, ["prime", "--all"])
    assert result.exit_code == 0
    assert result.output == render_all() + "\n"
    expected_topics = "\n\n".join(topic_text(name) for name in TOPIC_ORDER)
    assert render_all() == f"{render_core()}\n\n{expected_topics}"


def test_prime_all_with_topic_rejected():
    """`--all` and `--topic` together are redundant/ambiguous → usage error."""
    result = CliRunner().invoke(cli, ["prime", "--all", "--topic", "mock"])
    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_prime_hook_json_valid_envelope():
    """`--hook-json` emits the Claude Code SessionStart envelope with the
    pointer as additionalContext."""
    result = CliRunner().invoke(cli, ["prime", "--hook-json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    spec = payload["hookSpecificOutput"]
    assert spec["hookEventName"] == "SessionStart"
    assert spec["additionalContext"] == hook_pointer()
    assert len(spec["additionalContext"]) <= 800


def test_prime_hook_json_exclusive():
    """`--hook-json` cannot combine with --topic or --all."""
    runner = CliRunner()
    for args in (
        ["prime", "--hook-json", "--topic", "mock"],
        ["prime", "--hook-json", "--all"],
    ):
        result = runner.invoke(cli, args)
        assert result.exit_code == 2
        assert result.stdout == ""
        assert "mutually exclusive" in result.stderr


def test_prime_missing_resource_is_usage_error(monkeypatch):
    """prime never exits 1: a broken packaged resource surfaces as a usage
    error (exit 2, clean stderr message), never a traceback."""
    import agctl.commands.prime_commands as pc

    def boom(*args, **kwargs):
        raise ConfigError("Resource not found in the agctl package: gone")

    monkeypatch.setattr(pc, "render_core", boom)
    result = CliRunner().invoke(cli, ["prime"])
    assert result.exit_code == 2
    assert result.stdout == ""
    assert "Resource not found in the agctl package" in result.stderr
