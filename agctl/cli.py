"""Click entry point (DESIGN §3, §7). Wires command groups and emits envelopes."""

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

_SECRET_FRAGMENTS = ("password", "token", "secret", "key")


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
    return any(frag in lowered for frag in _SECRET_FRAGMENTS)


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
    if errors:
        summary = f"Configuration has {len(errors)} structural error(s)"
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
