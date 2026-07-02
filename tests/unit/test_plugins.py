"""Tests for plugin / entry-point registration (DESIGN §9.1 db_drivers, §9.2 plugins)."""

import importlib.metadata as metadata
import sys
import types

import click
from click.testing import CliRunner

from agctl.cli import _entry_points, _load_plugins, cli
from agctl.clients.db_client import DbClient
from agctl.clients.db_drivers.postgresql import PostgreSQLDriver


def _select(group):
    eps = metadata.entry_points()
    if hasattr(eps, "select"):
        return list(eps.select(group=group))
    return list(eps.get(group, []))


# --- §9.1: db_drivers entry point ------------------------------------------


def test_postgresql_driver_registered_via_entry_point():
    """The postgresql driver is discoverable via the real agctl.db_drivers group."""
    group = _select("agctl.db_drivers")
    names = {ep.name for ep in group}
    assert "postgresql" in names

    ep = next(e for e in group if e.name == "postgresql")
    assert ep.load() is PostgreSQLDriver

    # And DbClient.load_drivers resolves it (entry point is the primary source).
    assert DbClient.load_drivers()["postgresql"] is PostgreSQLDriver


# --- §9.2: plugin command_group loading ------------------------------------


def _make_plugin():
    """A synthetic third-party plugin object exposing a `.command_group`."""
    @click.group(name="grpc")
    def grpc_group():
        """Synthetic grpc plugin commands."""

    @grpc_group.command("ping")
    def grpc_ping():
        click.echo("grpc pong")

    plugin = types.SimpleNamespace(command_group=grpc_group)
    return plugin


def test_plugin_command_group_registered(monkeypatch):
    """A well-formed third-party plugin's command_group is registered on cli."""
    plugin = _make_plugin()
    fake_ep = types.SimpleNamespace(name="grpc", load=lambda: plugin)

    monkeypatch.setattr(
        "agctl.cli._entry_points",
        lambda group: [fake_ep] if group == "agctl.plugins" else [],
    )

    target = click.Group(name="root")
    _load_plugins(target)

    assert "grpc" in target.commands
    assert isinstance(target.commands["grpc"], click.Group)


def test_broken_plugin_does_not_crash(monkeypatch):
    """A plugin whose .load() raises is skipped without bricking the CLI."""
    def _boom():
        raise RuntimeError("plugin explosion")

    fake_ep = types.SimpleNamespace(name="broken", load=_boom)
    monkeypatch.setattr(
        "agctl.cli._entry_points",
        lambda group: [fake_ep] if group == "agctl.plugins" else [],
    )

    target = click.Group(name="root")
    # Must not raise.
    _load_plugins(target)
    assert "broken" not in target.commands

    # The real CLI still works.
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0


def test_broken_plugin_command_group_attribute_skipped(monkeypatch):
    """A plugin object lacking a usable command_group is silently skipped."""
    # Has .command_group but it's not a click.Group.
    bad_plugin = types.SimpleNamespace(command_group="not-a-group")
    fake_ep = types.SimpleNamespace(name="bad", load=lambda: bad_plugin)
    monkeypatch.setattr(
        "agctl.cli._entry_points",
        lambda group: [fake_ep] if group == "agctl.plugins" else [],
    )

    target = click.Group(name="root")
    _load_plugins(target)  # no raise
    assert "bad" not in target.commands


def test_no_plugins_is_noop(monkeypatch):
    """With no plugins registered, _load_plugins is a clean no-op."""
    monkeypatch.setattr("agctl.cli._entry_points", lambda group: [])
    target = click.Group(name="root")
    _load_plugins(target)  # no raise
    assert target.commands == {}


def test_load_plugins_import_time_guard(monkeypatch):
    """An empty/missing agctl.plugins group is a no-op via _entry_points default."""
    # The real default for agctl.plugins today is empty.
    eps = _entry_points("agctl.plugins")
    assert isinstance(eps, list)


# --- §9.2: plugin.validate_config consulted by `config validate` -------------


def test_plugin_validation_errors_collects_strings():
    """A plugin's validate_config error strings become {path, message} records."""
    from agctl.commands.config_commands import _plugin_validation_errors

    plugin = types.SimpleNamespace(
        name="grpc",
        validate_config=lambda cfg: ["grpc section missing", "port out of range"],
    )
    errors = _plugin_validation_errors([plugin], {"version": "1"})
    assert errors == [
        {"path": "plugin.grpc", "message": "grpc section missing"},
        {"path": "plugin.grpc", "message": "port out of range"},
    ]


def test_plugin_validation_errors_skips_plugins_without_validate():
    """Plugins lacking validate_config are ignored (duck-typed loading)."""
    from agctl.commands.config_commands import _plugin_validation_errors

    plugin = types.SimpleNamespace(name="noop")  # no validate_config
    assert _plugin_validation_errors([plugin], {}) == []


def test_plugin_validation_errors_isolates_raising_plugin():
    """A plugin whose validate_config raises is isolated, not fatal."""
    from agctl.commands.config_commands import _plugin_validation_errors

    def _boom(cfg):
        raise RuntimeError("boom")

    plugin = types.SimpleNamespace(name="bad", validate_config=_boom)
    errors = _plugin_validation_errors([plugin], {})
    assert len(errors) == 1
    assert errors[0]["path"] == "plugin.bad"
    assert "boom" in errors[0]["message"]


def test_plugin_command_name_from_name_attribute(monkeypatch):
    """DESIGN §9.2: `.name` is used as the subcommand name when present."""
    import click

    @click.group(name="internal-group-name")
    def grp():
        pass

    plugin = types.SimpleNamespace(name="grpc", command_group=grp)
    fake_ep = types.SimpleNamespace(name="entrypoint-name", load=lambda: plugin)
    monkeypatch.setattr(
        "agctl.cli._entry_points",
        lambda group: [fake_ep] if group == "agctl.plugins" else [],
    )

    target = click.Group(name="root")
    _load_plugins(target)
    # `.name` ("grpc") wins over command_group.name and the entry-point name.
    assert "grpc" in target.commands


def test_config_validate_surfaces_plugin_errors(monkeypatch):
    """DESIGN §9.2: a loaded plugin's validate_config errors appear in
    `agctl config validate` output."""
    import json
    from pathlib import Path

    from click.testing import CliRunner

    from agctl.cli import cli
    import agctl.cli as cli_module

    fixture = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"
    env = {
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "PAYMENT_SERVICE_URL": "http://localhost:8082",
        "PAYMENT_SERVICE_TOKEN": "tok",
        "KAFKA_BROKER": "localhost",
        "DB_HOST": "h",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
        "ANALYTICS_DB_HOST": "ah",
        "ANALYTICS_DB_USER": "au",
        "ANALYTICS_DB_PASSWORD": "ap",
    }

    plugin = types.SimpleNamespace(
        name="grpc",
        command_group=click.Group(name="grpc"),
        validate_config=lambda cfg: ["grpc requires a broker URL"],
    )
    fake_ep = types.SimpleNamespace(name="grpc", load=lambda: plugin)
    monkeypatch.setattr(
        "agctl.cli._entry_points",
        lambda group: [fake_ep] if group == "agctl.plugins" else [],
    )
    # Populate the module-global _LOADED_PLUGINS via the loader.
    _load_plugins(click.Group(name="root"))
    try:
        result = CliRunner().invoke(
            cli, ["config", "validate", "--config", str(fixture)], env=env
        )
    finally:
        # Reset global state so other tests are not polluted.
        cli_module._LOADED_PLUGINS = []

    payload = json.loads(result.output)
    assert result.exit_code == 2
    assert payload["result"]["valid"] is False
    messages = [e["message"] for e in payload["result"]["errors"]]
    assert "grpc requires a broker URL" in messages
