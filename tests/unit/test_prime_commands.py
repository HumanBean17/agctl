"""Tests for the ``agctl prime`` command (spec:
``2026-08-19-agctl-prime-skills-replacement-design``).

prime is a config-free documentation command: raw markdown on stdout, exit 0,
no JSON envelope (a ``--help``-class exception). The tests pin the default
output, the config-free property, and root-group registration.
"""

from __future__ import annotations

from click.testing import CliRunner

from agctl.cli import cli
from agctl.prime_content import render_core


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
