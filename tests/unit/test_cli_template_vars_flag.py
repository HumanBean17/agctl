"""Task 4 (template-vars): the global ``--no-template-vars`` flag.

The flag lives on the root ``agctl`` group and populates
``ctx.obj["no_template_vars"]`` (bool, default False). It is NOT yet consumed
by any command's ``_core`` (that wiring is Tasks 6-9); ``agctl gen`` ignores it
entirely. These tests only assert the flag exists and lands in ``ctx.obj``.
"""

import click
from click.testing import CliRunner

from agctl.cli import cli


@click.command(name="_probe_template_vars")
@click.pass_context
def _probe(ctx: click.Context) -> None:
    """Test-only command: echo ``ctx.obj["no_template_vars"]`` for inspection."""
    click.echo(str(ctx.obj.get("no_template_vars", "<missing>")))


def _register_probe() -> None:
    cli.add_command(_probe, name="_probe_template_vars")


def _unregister_probe() -> None:
    cli.commands.pop("_probe_template_vars", None)


def test_flag_recognized_in_root_help():
    """``agctl --no-template-vars --help`` exits 0 -> the flag is a recognized
    global option (not 'no such option')."""
    result = CliRunner().invoke(cli, ["--no-template-vars", "--help"])
    assert result.exit_code == 0
    assert "--no-template-vars" in result.output


def test_flag_accepted_before_subcommand(tmp_path):
    """Passing the flag before a subcommand must not raise a Click UsageError
    (no 'no such option'). ``config init`` runs config-free, so it is a safe
    probe that does not need env vars."""
    dest = tmp_path / "agctl.yaml"
    result = CliRunner().invoke(
        cli, ["--no-template-vars", "config", "init", "-o", str(dest)]
    )
    assert result.exit_code == 0
    assert "no such option" not in result.output
    assert not isinstance(result.exception, click.UsageError)


def test_flag_populates_ctx_obj_true_when_set():
    """When the flag is passed, ``ctx.obj["no_template_vars"]`` is True."""
    _register_probe()
    try:
        result = CliRunner().invoke(cli, ["--no-template-vars", "_probe_template_vars"])
        assert result.exit_code == 0
        assert result.output.strip() == "True"
    finally:
        _unregister_probe()


def test_flag_default_false_in_ctx_obj():
    """Without the flag, ``ctx.obj["no_template_vars"]`` is False (default)."""
    _register_probe()
    try:
        result = CliRunner().invoke(cli, ["_probe_template_vars"])
        assert result.exit_code == 0
        assert result.output.strip() == "False"
    finally:
        _unregister_probe()
