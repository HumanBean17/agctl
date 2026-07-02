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
