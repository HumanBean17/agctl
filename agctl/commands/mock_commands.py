"""mock run command (DESIGN §3.3, §6).

The mock run command starts HTTP mock server and/or Kafka reactors, streaming
NDJSON events to stdout. It is a streaming exception like http ping: NOT wrapped
in @envelope, instead hand-rolling try/except → emit + SystemExit.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import click

from ..command import load_config_or_raise
from ..config.models import MocksConfig, parse_listen
from ..errors import AgctlError, ConfigError, ConnectionFailure
from ..output import emit

if TYPE_CHECKING:
    from ..config.models import KafkaConfig
    from ..clients.kafka_client import KafkaClient

__all__ = ["mock_run", "new_mock_engine"]

# Import from kafka_commands to avoid duplication (no circular import)
from .kafka_commands import new_kafka_client


# Test seam: tests monkeypatch this to return a fake MockEngine
def new_mock_engine(
    mocks: MocksConfig | None,
    *,
    run_http: bool,
    run_kafka: bool,
    http_listen: str,
    kafka_client: KafkaClient | None,
    fail_fast: bool = False,
    duration: float | None = None,
    until_stopped: bool = True,
):
    """Build a MockEngine (test seam — monkeypatched in tests)."""
    from ..mock.engine import MockEngine

    return MockEngine(
        mocks=mocks,
        run_http=run_http,
        run_kafka=run_kafka,
        http_listen=http_listen,
        kafka_client=kafka_client,
        fail_fast=fail_fast,
        duration=duration,
        until_stopped=until_stopped,
    )


def _resolve_engines(
    only: str | None,
    mocks: MocksConfig | None,
) -> tuple[bool, bool]:
    """Resolve which engines to run based on --only and config presence.

    Returns (run_http, run_kafka).

    Runtime guards (from brief):
    - --only http ⇒ run_http=mocks.http present, run_kafka=False
    - --only kafka ⇒ run_kafka=mocks.kafka.reactors non-empty, run_http=False
    - neither ⇒ run_http = mocks and mocks.http is not None
               run_kafka = mocks and mocks.kafka is not None and bool(mocks.kafka.reactors)
    """
    if only == "http":
        # Guard: --only http with no mocks.http → ConfigError
        if mocks is None or mocks.http is None:
            raise ConfigError("--only http but no mocks.http configured", {})
        return True, False

    if only == "kafka":
        # Guard: --only kafka with no mocks.kafka.reactors → ConfigError
        if mocks is None or mocks.kafka is None or not mocks.kafka.reactors:
            raise ConfigError("--only kafka but no mocks.kafka.reactors configured", {})
        return False, True

    # No --only: resolve from mocks presence
    run_http = mocks is not None and mocks.http is not None
    run_kafka = mocks is not None and mocks.kafka is not None and bool(mocks.kafka.reactors)
    return run_http, run_kafka


@click.command("run")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--http-listen", "http_listen", default=None, help="HTTP listen address (host:port)")
@click.option(
    "--only",
    "only",
    type=click.Choice(["http", "kafka"]),
    default=None,
    help="Run only HTTP or Kafka mock engine",
)
@click.option("--fail-fast", "fail_fast", is_flag=True, default=False, help="Exit on first reactor error")
@click.option("--duration", "duration", type=float, default=None, help="Stop after N seconds")
@click.option("--until-stopped", "until_stopped", is_flag=True, default=False, help="Run until stopped")
@click.pass_context
def mock_run(
    ctx: click.Context,
    config_path: str | None,
    http_listen: str | None,
    only: str | None,
    fail_fast: bool,
    duration: float | None,
    until_stopped: bool,
) -> None:
    """Run HTTP mock server and/or Kafka reactors, streaming NDJSON events."""
    # Fall back to ctx.obj["config_path"] if --config not provided
    if config_path is None:
        config_path = ctx.obj.get("config_path") if ctx.obj else None

    start = time.monotonic()

    # Guard: --duration and --until-stopped are mutually exclusive
    if duration is not None and until_stopped:
        emit(
            ok=False,
            command="mock.run",
            error={
                "type": "ConfigError",
                "message": "--duration and --until-stopped are mutually exclusive",
                "detail": {},
            },
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    try:
        # Guard 1: Load config (ConfigError → envelope + exit 2)
        cfg = load_config_or_raise(config_path)

        # Guard 2+3: Resolve engines to run
        run_http, run_kafka = _resolve_engines(only, cfg.mocks)

        # Guard 5: If run_kafka, require non-empty kafka.brokers
        kafka_client = None
        if run_kafka:
            if not cfg.kafka.brokers:
                raise ConfigError("kafka.brokers is required when running Kafka reactors", {})
            # Build KafkaClient (may raise ConfigError if kafka extra missing)
            kafka_client = new_kafka_client(cfg.kafka)

        # Guard 6: Resolve http_listen
        if http_listen is not None:
            # Parse the CLI override (literal — no ${} interpolation)
            try:
                parse_listen(http_listen)
            except ValueError as e:
                raise ConfigError(f"Invalid --http-listen: {e}", {})
        elif cfg.mocks and cfg.mocks.http:
            http_listen = cfg.mocks.http.listen
        else:
            # Default if no HTTP config and no override
            http_listen = "0.0.0.0:18080"

        # Build the engine (via the test seam)
        engine = new_mock_engine(
            mocks=cfg.mocks,
            run_http=run_http,
            run_kafka=run_kafka,
            http_listen=http_listen,
            kafka_client=kafka_client,
            fail_fast=fail_fast,
            duration=duration,
            until_stopped=until_stopped,
        )

        # Start the engine (probes + binds — may raise ConfigError/ConnectionFailure)
        engine.start()

    except AgctlError as err:
        # Startup errors → structured envelope + exit code
        emit(
            ok=False,
            command="mock.run",
            error=err.to_dict(),
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(err.exit_code)
    except Exception as exc:
        # Non-agctl startup errors → InternalError envelope + exit 2
        emit(
            ok=False,
            command="mock.run",
            error={"type": "InternalError", "message": str(exc), "detail": {}},
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    # Run the engine (blocks until stop)
    code = engine.run()

    # Exit with the engine's exit code
    raise SystemExit(code)
