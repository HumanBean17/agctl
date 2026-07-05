"""mock run command (DESIGN §3.3, §6).

The mock run command starts HTTP mock server and/or Kafka reactors, streaming
NDJSON events to stdout. It is a streaming exception like http ping: NOT wrapped
in @envelope, instead hand-rolling try/except → emit + SystemExit.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import click

from ..command import envelope, load_config_or_raise
from ..config.models import MocksConfig, parse_listen
from ..errors import AgctlError, ConfigError, ConnectionFailure
from ..output import emit

if TYPE_CHECKING:
    from ..config.models import KafkaConfig
    from ..clients.kafka_client import KafkaClient

__all__ = ["mock_run", "new_mock_engine", "mock_start"]

# Import from kafka_commands to avoid duplication (no circular import)
from .kafka_commands import new_kafka_client

# Import daemon lifecycle helpers (Task 2: pidfile, liveness; Task 3: log parser)
from ..mock.daemon import (
    is_alive,
    log_path,
    parse_log,
    pidfile_path,
    read_pidfile,
    remove_pidfile,
    write_pidfile,
)

# Readiness poll timeout (Task 4)
_START_BUDGET_SECONDS: float = 30.0


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


def spawn_daemon(argv: list[str], log_path: str, env: dict | None = None) -> int:
    """Spawn a detached daemon process (Task 4).

    This is the test seam: tests monkeypatch this to return a fake pid and
    optionally write a canned log line.

    Args:
        argv: Command-line arguments to pass to the daemon (e.g., ["mock", "run", ...]).
        log_path: Path to the log file where stdout+stderr will be redirected.
        env: Environment variables (if None, inherits parent environment).

    Returns:
        The PID of the spawned daemon process.

    Raises:
        OSError: If the subprocess fails to start.
    """
    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Open log file for append (daemon writes here)
    log_handle = open(log_file, "ab")

    # Build the daemon command: python -m agctl <argv...>
    # Use sys.executable to ensure same interpreter
    daemon_cmd = [sys.executable, "-m", "agctl"] + argv

    # Spawn the daemon in a new session (detached from parent terminal)
    # stdout+stderr both go to the log file
    proc = subprocess.Popen(
        daemon_cmd,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,  # Detach: new session/process group
        env=env,  # Inherit parent env if None
    )

    return proc.pid


def _mock_start_core(
    config_path: str | None,
    http_listen: str | None,
    only: str | None,
    fail_fast: bool,
    duration: float | None,
    state_dir: str,
) -> dict:
    """Core logic for `mock start` (Task 4).

    Spawns a detached mock daemon, writes a pidfile, and polls for readiness.

    Returns:
        Dict with keys: pid, listen, log_path, stubs, reactors, started_at.

    Raises:
        ConfigError: If already running, startup error, or timeout.
    """
    # Step 1: Load config
    cfg = load_config_or_raise(config_path)

    # Step 2: Resolve which engines to run
    run_http, run_kafka = _resolve_engines(only, cfg.mocks)

    # Step 3: Resolve http_listen
    if http_listen is not None:
        # Parse the CLI override
        try:
            parsed_listen = parse_listen(http_listen)
        except ValueError as e:
            raise ConfigError(f"Invalid --http-listen: {e}", {})
    elif cfg.mocks and cfg.mocks.http:
        parsed_listen = parse_listen(cfg.mocks.http.listen)
        http_listen = cfg.mocks.http.listen
    else:
        # Default if no HTTP config and no override
        http_listen = "0.0.0.0:18080"
        parsed_listen = parse_listen(http_listen)

    # Extract port for pidfile keying (None when not running HTTP)
    port = None
    if run_http:
        host, port = parsed_listen
        if port <= 0:
            raise ConfigError(
                "start requires a concrete --http-listen port (got 0)",
                {},
            )
    else:
        port = None

    # Step 4: Compute pidfile and log paths
    state_path = Path(state_dir)
    pid = pidfile_path(state_path, port)
    logp = log_path(state_path, port)

    # Step 5: Already-running pre-check
    existing = read_pidfile(pid)
    if existing is not None:
        existing_pid = existing.get("pid")
        if existing_pid is not None and is_alive(existing_pid):
            raise ConfigError(
                f"mock already running on {http_listen} (pid {existing_pid}); "
                "run 'agctl mock stop' first or use a different --http-listen",
                {"pid": existing_pid, "listen": http_listen},
            )

    # Step 6: Build daemon argv
    daemon_argv = ["mock", "run"]
    if config_path is not None:
        daemon_argv.extend(["--config", str(Path(config_path).absolute())])
    if run_http:
        daemon_argv.extend(["--http-listen", http_listen])
    if only is not None:
        daemon_argv.extend(["--only", only])
    if fail_fast:
        daemon_argv.append("--fail-fast")
    if duration is not None:
        daemon_argv.extend(["--duration", str(duration)])

    # Step 7: Spawn the daemon
    child_pid = spawn_daemon(daemon_argv, str(logp))

    # Step 8: Write pidfile
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    write_pidfile(
        pid,
        {
            "pid": child_pid,
            "listen": http_listen if run_http else None,
            "port": port,
            "log_path": str(logp),
            "config_path": config_path,
            "started_at": started_at,
            "run_id": str(child_pid),
        },
    )

    # Step 9: Readiness poll
    start_time = time.monotonic()
    started = None

    try:
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > _START_BUDGET_SECONDS:
                raise ConfigError(
                    f"mock daemon did not become ready within {_START_BUDGET_SECONDS}s",
                    {"pid": child_pid, "log_path": str(logp)},
                )

            parsed = parse_log(logp)

            if parsed.started is not None:
                started = parsed.started
                break
            elif parsed.startup_error is not None:
                # Cleanup: kill daemon and remove pidfile
                try:
                    os.kill(child_pid, signal.SIGTERM)
                except (ProcessLookupError, OSError):
                    pass  # Already dead
                remove_pidfile(pid)

                # Extract error details from the startup_error envelope
                error = parsed.startup_error.get("error", {})
                message = error.get("message", "startup failed")
                detail = error.get("detail", {})
                detail["listen"] = http_listen
                raise ConfigError(message, detail)

            # Sleep briefly before next poll
            time.sleep(0.05)

    except Exception:
        # Cleanup on any error
        try:
            os.kill(child_pid, signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass  # Already dead
        remove_pidfile(pid)
        raise

    # Step 10: Build result
    stubs = None
    reactors = []
    if started.get("http") is not None:
        stubs = started["http"].get("stubs")
    if started.get("kafka") is not None:
        reactors = [r["name"] for r in started["kafka"].get("reactors", [])]

    return {
        "pid": child_pid,
        "listen": http_listen if run_http else None,
        "log_path": str(logp),
        "stubs": stubs,
        "reactors": reactors,
        "started_at": started_at,
    }


@click.command("start")
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
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for mock state (pidfiles, logs)")
@click.pass_context
def mock_start(
    ctx: click.Context,
    config_path: str | None,
    http_listen: str | None,
    only: str | None,
    fail_fast: bool,
    duration: float | None,
    state_dir: str,
) -> None:
    """Start a detached mock daemon with HTTP server and/or Kafka reactors."""
    # Fall back to ctx.obj["config_path"] if --config not provided
    if config_path is None:
        config_path = ctx.obj.get("config_path") if ctx.obj else None

    _mock_start_envelope(config_path, http_listen, only, fail_fast, duration, state_dir)


_mock_start_envelope = envelope("mock.start")(_mock_start_core)


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

    # Run the engine (blocks until stop); ensure shutdown always runs
    try:
        code = engine.run()
    finally:
        # Shutdown to emit summary line (runs even if run() raises)
        engine.shutdown()

    # Exit with the engine's exit code
    raise SystemExit(code)
