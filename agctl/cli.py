"""Click entry point (DESIGN §3, §7). Wires command groups and emits envelopes."""

import importlib.metadata
import sys
import time
from typing import Any

import click

from .commands.check_commands import check_ready
from .commands.db_commands import db_assert, db_query
from .commands.discover_commands import discover
from .commands.http_commands import http_call, http_ping, http_request
from .commands.kafka_commands import kafka_assert, kafka_consume, kafka_produce
from .config import ConfigError, load_config
from .config.validator import validate_config
from .output import emit

_SECRET_FRAGMENTS = ("password", "token", "secret")

#: Entry-point group for third-party protocol plugins (DESIGN §9.2).
PLUGIN_ENTRY_POINT_GROUP = "agctl.plugins"

#: Plugins successfully loaded at import time (DESIGN §9.2). Populated by
#: :func:`_load_plugins`; consulted by ``agctl config validate`` so each plugin
#: can validate its own config section (see :func:`_plugin_validation_errors`).
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
    (consulted by ``agctl config validate`` — see :func:`_plugin_validation_errors`).
    Each load+register is wrapped in try/except so a broken plugin logs to stderr
    and is skipped rather than bricking the CLI. An empty/missing ``agctl.plugins``
    group is a clean no-op. Successfully loaded plugins are recorded in
    ``_LOADED_PLUGINS`` so config validation can delegate to them.
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


def _plugin_validation_errors(plugins: list, config_dict: dict) -> list[dict]:
    """Ask each loaded plugin to validate its own config section (DESIGN §9.2).

    Each plugin's ``validate_config(config_dict)`` (if present) returns a list of
    human-readable error strings. Those are folded into ``{path, message}`` error
    records under ``plugin.<name>``. A plugin that raises is isolated: its error
    becomes a single record rather than crashing ``config validate``.
    """
    errors: list[dict] = []
    for plugin in plugins:
        validate = getattr(plugin, "validate_config", None)
        if not callable(validate):
            continue
        plugin_name = getattr(plugin, "name", None) or "unknown"
        try:
            returned = validate(config_dict) or []
        except Exception as exc:  # noqa: BLE001 - plugin isolation
            errors.append(
                {"path": f"plugin.{plugin_name}", "message": f"plugin raised: {exc}"}
            )
            continue
        for msg in returned:
            errors.append({"path": f"plugin.{plugin_name}", "message": str(msg)})
    return errors


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def _mask(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("***" if _is_secret(k) and v else _mask(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mask(v) for v in obj]
    return obj


def _is_secret(key: str) -> bool:
    lowered = key.lower()
    if any(frag in lowered for frag in _SECRET_FRAGMENTS):
        return True
    # "key" is too broad as a substring (would mask non-secret paths like
    # kafka.ssl.key_location, or names like key_id). Treat it as a secret only
    # when it names the key itself: a bare ``key`` or an ``*_key`` suffix.
    return lowered == "key" or lowered.endswith("_key")


def _emit_config_error(command: str, err: ConfigError, start: float) -> None:
    errors = [{"message": err.message, **(err.detail or {})}]
    emit(
        ok=False,
        command=command,
        result={"valid": False, "errors": errors},
        error={"type": "ConfigError", "message": err.message, "detail": err.detail},
        duration_ms=_ms(start),
    )


@click.group()
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.pass_context
def cli(ctx: click.Context, config_path: str | None) -> None:
    """agctl — agent-facing CLI harness for testing distributed systems."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@cli.group(name="config")
def config_group() -> None:
    """Config introspection."""


@cli.group(name="http")
def http_group() -> None:
    """HTTP request commands (DESIGN §3.1)."""


@cli.group(name="db")
def db_group() -> None:
    """Database query/assert commands (DESIGN §3.3)."""


@cli.group(name="kafka")
def kafka_group() -> None:
    """Kafka produce/consume/assert commands (DESIGN §3.2)."""


@cli.group(name="check")
def check_group() -> None:
    """Health/readiness checks (DESIGN §3.4)."""


# Register subcommands on the http group.
http_group.add_command(http_call)
http_group.add_command(http_request)
http_group.add_command(http_ping)


# Register subcommands on the db group.
db_group.add_command(db_query)
db_group.add_command(db_assert)


# Register subcommands on the kafka group.
kafka_group.add_command(kafka_produce)
kafka_group.add_command(kafka_consume)
kafka_group.add_command(kafka_assert)


# Register subcommands on the check group.
check_group.add_command(check_ready)


# Register the top-level `discover` command directly on the root group.
cli.add_command(discover)


# Load third-party protocol plugins (DESIGN §9.2). A clean no-op today since no
# plugins are registered; guarded so a broken plugin/importlib never bricks the CLI.
_load_plugins(cli)


@config_group.command("validate")
@click.option("--config", "config_path", default=None)
@click.pass_context
def config_validate(ctx: click.Context, config_path: str | None) -> None:
    """Parse and validate agctl.yaml (DESIGN §3.5). Exit 2 on any error."""
    start = time.monotonic()
    path = config_path or ctx.obj.get("config_path")
    try:
        cfg = load_config(path)
    except ConfigError as err:
        _emit_config_error("config.validate", err, start)
        raise SystemExit(2)
    errors, warnings = validate_config(cfg)
    # Let loaded plugins validate their own config sections (DESIGN §9.2).
    errors = errors + _plugin_validation_errors(_LOADED_PLUGINS, cfg.model_dump())
    if errors:
        summary = f"Configuration has {len(errors)} error(s)"
        emit(
            ok=False,
            command="config.validate",
            result={"valid": False, "errors": errors, "warnings": warnings},
            error={
                "type": "ConfigError",
                "message": summary,
                "detail": {"errors": errors, "warnings": warnings},
            },
            duration_ms=_ms(start),
        )
        raise SystemExit(2)
    emit(
        ok=True,
        command="config.validate",
        result={"valid": True, "warnings": warnings},
        duration_ms=_ms(start),
    )


@config_group.command("show")
@click.option("--config", "config_path", default=None)
@click.option("--unmask", is_flag=True, default=False)
@click.pass_context
def config_show(ctx: click.Context, config_path: str | None, unmask: bool) -> None:
    """Dump the resolved config as JSON, secrets masked (DESIGN §3.5)."""
    start = time.monotonic()
    path = config_path or ctx.obj.get("config_path")
    try:
        cfg = load_config(path)
    except ConfigError as err:
        _emit_config_error("config.show", err, start)
        raise SystemExit(2)
    data = cfg.model_dump()
    if not unmask:
        data = _mask(data)
    emit(ok=True, command="config.show", result=data, duration_ms=_ms(start))


if __name__ == "__main__":
    cli()
