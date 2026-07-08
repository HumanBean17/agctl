"""Click entry point (DESIGN §3, §7). Wires command groups and emits envelopes.

The ``config`` command group lives in :mod:`agctl.commands.config_commands`;
plugin loading (``_load_plugins`` / ``_LOADED_PLUGINS``) stays here because it
is a CLI-bootstrap concern. Config validation reads the live plugin list via a
thunk injected into ``config_commands`` (see :func:`set_plugins_provider`), so
the dependency runs one way: ``cli → config_commands``.
"""

import importlib.metadata
import sys
from typing import Any

import click

from . import __version__
from .commands.check_commands import check_ready
from .commands.config_commands import (
    config_init,
    config_migrate,
    config_show,
    config_validate,
    set_plugins_provider,
)
from .commands.db_commands import db_assert, db_execute, db_query, db_schema
from .commands.discover_commands import discover
from .commands.http_commands import http_call, http_ping, http_request
from .commands.kafka_commands import kafka_assert, kafka_consume, kafka_produce
from .commands.logs_commands import logs_assert, logs_query, logs_tail
from .commands.mock_commands import mock_run, mock_start, mock_stop, mock_status

#: Entry-point group for third-party protocol plugins (DESIGN §9.2).
PLUGIN_ENTRY_POINT_GROUP = "agctl.plugins"

#: Plugins successfully loaded at import time (DESIGN §9.2). Populated by
#: :func:`_load_plugins`; read by ``agctl config validate`` via the thunk passed
#: to :func:`agctl.commands.config_commands.set_plugins_provider` so each plugin
#: can validate its own config section.
_LOADED_PLUGINS: list[Any] = []


def _entry_points(group: str) -> list:
    """Return the entry points registered under ``group`` (3.11+ shim).

    Factored out so tests can monkeypatch discovery. Returns ``[]`` on any
    failure (a broken importlib state must never crash the CLI).
    """
    try:
        eps = importlib.metadata.entry_points()
        if hasattr(eps, "select"):
            return list(eps.select(group=group))
        return list(eps.get(group, []))
    except Exception:
        return []


def _load_plugins(cli_group: click.Group) -> None:
    """Load ``agctl.plugins`` entry points onto ``cli_group`` (DESIGN §9.2).

    Each plugin object exposes ``.command_group`` (a :class:`click.Group`) and
    optionally ``.name`` (subcommand name) and ``.validate_config(config)``
    (consulted by ``agctl config validate`` via the provider set below). Each
    load+register is wrapped in try/except so a broken plugin logs to stderr
    and is skipped rather than bricking the CLI. An empty/missing
    ``agctl.plugins`` group is a clean no-op. Successfully loaded plugins are
    recorded in ``_LOADED_PLUGINS`` so config validation can delegate to them.
    """
    global _LOADED_PLUGINS
    _LOADED_PLUGINS = []
    try:
        for ep in _entry_points(PLUGIN_ENTRY_POINT_GROUP):
            try:
                obj = ep.load()
            except Exception as exc:  # noqa: BLE001 - plugin isolation
                print(f"agctl: failed to load plugin {ep.name}: {exc}", file=sys.stderr)
                continue
            command_group = getattr(obj, "command_group", None)
            if isinstance(command_group, click.Group):
                # DESIGN §9.2: `.name` is the subcommand name; fall back to the
                # group's own name, then the entry-point name.
                name = getattr(obj, "name", None) or command_group.name or ep.name
                cli_group.add_command(command_group, name=name)
                _LOADED_PLUGINS.append(obj)
            else:
                print(
                    f"agctl: plugin {ep.name} has no valid command_group; skipping",
                    file=sys.stderr,
                )
    except Exception as exc:  # noqa: BLE001 - never let the loader crash the CLI
        print(f"agctl: plugin loader error: {exc}", file=sys.stderr)


def _ensure_utf8_streams() -> None:
    """Force stdout/stderr to UTF-8 so non-ASCII JSON renders instead of crashing.

    The envelope emits raw UTF-8 (``ensure_ascii=False``, RFC 8259-valid) rather
    than ``\\uXXXX`` escapes. On a non-UTF-8 stdout (``PYTHONIOENCODING=ascii``,
    the ``C`` locale, legacy Windows) that would raise ``UnicodeEncodeError`` —
    turning a successful invocation into a leaked traceback. Reconfigure once at
    bootstrap so every emitter (``emit``, ``mock`` NDJSON, ``http ping`` NDJSON)
    is covered. No-op on streams we don't own (pytest capsys, Click CliRunner
    ``StringIO``) — they lack ``reconfigure``.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8")


@click.group()
@click.version_option(version=__version__, message="agctl %(version)s")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--overlay", "overlay_paths", multiple=True, help="Overlay config fragment (repeatable; later wins)")
@click.pass_context
def cli(ctx: click.Context, config_path: str | None, overlay_paths: tuple[str, ...]) -> None:
    """agctl — agent-facing CLI harness for testing distributed systems."""
    _ensure_utf8_streams()
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path
    ctx.obj["overlay_paths"] = tuple(overlay_paths) or None


@cli.group(name="config")
def config_group() -> None:
    """Config introspection."""


@cli.group(name="http")
def http_group() -> None:
    """HTTP request commands."""


@cli.group(name="db")
def db_group() -> None:
    """Database query/assert commands."""


@cli.group(name="kafka")
def kafka_group() -> None:
    """Kafka produce/consume/assert commands."""


@cli.group(name="logs")
def logs_group() -> None:
    """Log query/assert/tail commands."""


@cli.group(name="check")
def check_group() -> None:
    """Health/readiness checks."""


@cli.group(name="mock")
def mock_group() -> None:
    """Mock server commands."""


# Register subcommands on the config group (commands live in config_commands.py).
config_group.add_command(config_validate)
config_group.add_command(config_show)
config_group.add_command(config_init)
config_group.add_command(config_migrate)

# Register subcommands on the http group.
http_group.add_command(http_call)
http_group.add_command(http_request)
http_group.add_command(http_ping)


# Register subcommands on the db group.
db_group.add_command(db_query)
db_group.add_command(db_assert)
db_group.add_command(db_execute)
db_group.add_command(db_schema)


# Register subcommands on the kafka group.
kafka_group.add_command(kafka_produce)
kafka_group.add_command(kafka_consume)
kafka_group.add_command(kafka_assert)


# Register subcommands on the logs group.
logs_group.add_command(logs_query)
logs_group.add_command(logs_assert)
logs_group.add_command(logs_tail)


# Register subcommands on the check group.
check_group.add_command(check_ready)


# Register subcommands on the mock group.
mock_group.add_command(mock_run)
mock_group.add_command(mock_start)
mock_group.add_command(mock_stop)
mock_group.add_command(mock_status)


# Register the top-level `discover` command directly on the root group.
cli.add_command(discover)


# Bridge config validation to the live loaded-plugins list. The thunk reads this
# module's ``_LOADED_PLUGINS`` global at call time, so it stays correct even after
# :func:`_load_plugins` reassigns that global.
set_plugins_provider(lambda: _LOADED_PLUGINS)


# Load third-party protocol plugins (DESIGN §9.2). A clean no-op today since no
# plugins are registered; guarded so a broken plugin/importlib never bricks the CLI.
_load_plugins(cli)


if __name__ == "__main__":
    cli()
