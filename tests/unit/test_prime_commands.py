"""Tests for the ``agctl prime`` command (spec:
``2026-08-19-agctl-prime-skills-replacement-design``).

prime is a config-free documentation command: raw markdown on stdout, exit 0,
no JSON envelope (a ``--help``-class exception). The tests pin the default
output, the config-free property, and root-group registration.
"""

from __future__ import annotations

from click.testing import CliRunner

from agctl.cli import cli
from agctl.prime_content import TOPIC_ORDER, render_core, topic_text


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
    """Unknown topic is a usage error: exit 2, stderr lists valid topics."""
    result = CliRunner().invoke(cli, ["prime", "--topic", "bogus"])
    assert result.exit_code == 2
    assert "Unknown topic 'bogus'" in result.output
    for name in TOPIC_ORDER:
        assert name in result.output
