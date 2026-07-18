"""mock run command (DESIGN §3.3, §6).

The mock run command starts HTTP mock server and/or Kafka reactors, streaming
NDJSON events to stdout. It is a streaming exception like http ping: NOT wrapped
in @envelope, instead hand-rolling try/except → emit + SystemExit.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import click

from ..command import envelope, load_config_or_raise
from ..config.models import MocksConfig, parse_listen
from ..daemon import (
    require_posix_daemon as _require_posix_daemon,
    spawn_daemon,
    terminate as _terminate,
)
from ..errors import AgctlError, AssertionFailure, ConfigError, ConnectionFailure
from ..output import emit

if TYPE_CHECKING:
    from ..config.models import GrpcDescriptorSource, KafkaConfig
    from ..clients.kafka_client import KafkaClient
    from typing import Any, Callable

__all__ = ["mock_run", "new_mock_engine", "mock_start", "mock_stop", "mock_status"]

# Import from kafka_commands to avoid duplication (no circular import)
from .kafka_commands import new_kafka_client, resolve_cluster_name

# Import daemon lifecycle helpers (Task 2: pidfile, liveness; Task 3: log parser)
from ..mock.daemon import (
    FATAL_FAILURE_EVENTS,
    is_alive,
    log_path,
    parse_log,
    pidfile_path,
    read_pidfile,
    remove_pidfile,
    resolve_target,
    write_pidfile,
)

# Readiness poll timeout (Task 4)
_START_BUDGET_SECONDS: float = 30.0

# Termination grace period for mock start cleanup (short - daemon won't emit useful summary yet)
_START_CLEANUP_GRACE_SECONDS: float = 2.0


# Test seam: tests monkeypatch this to return a fake MockEngine
def new_mock_engine(
    mocks: MocksConfig | None,
    *,
    run_http: bool,
    run_kafka: bool,
    http_listen: str,
    kafka_clients: dict[str, "KafkaClient"] | None = None,
    fail_fast: bool = False,
    duration: float | None = None,
    until_stopped: bool = True,
    run_grpc: bool = False,
    grpc_listen: str | None = None,
    grpc_server_factory: "Callable[..., Any] | None" = None,
    top_level_descriptors: "list[GrpcDescriptorSource] | None" = None,
):
    """Build a MockEngine (test seam — monkeypatched in tests).

    Forwards the gRPC engine knobs (Task 8) and the top-level descriptor
    fallback (Task 10 obligation) to MockEngine. ``grpc_server_factory`` and
    ``top_level_descriptors`` default to None so pre-Task-10 callers keep
    working; production passes the real descriptors list, tests inject a fake.
    """
    from ..mock.engine import MockEngine

    return MockEngine(
        mocks=mocks,
        run_http=run_http,
        run_kafka=run_kafka,
        http_listen=http_listen,
        kafka_clients=kafka_clients,
        fail_fast=fail_fast,
        duration=duration,
        until_stopped=until_stopped,
        run_grpc=run_grpc,
        grpc_listen=grpc_listen,
        grpc_server_factory=grpc_server_factory,
        top_level_descriptors=top_level_descriptors,
    )


def _resolve_engines(
    only: str | None,
    mocks: MocksConfig | None,
) -> tuple[bool, bool, bool]:
    """Resolve which engines to run based on --only and config presence.

    Returns (run_http, run_kafka, run_grpc).

    Runtime guards (from brief):
    - --only http ⇒ run_http=mocks.http present, run_kafka/run_grpc=False
    - --only kafka ⇒ run_kafka=mocks.kafka.reactors non-empty, run_http/run_grpc=False
    - --only grpc ⇒ run_grpc=mocks.grpc.stubs non-empty, run_http/run_kafka=False
    - neither ⇒ run_http = mocks and mocks.http is not None
               run_kafka = mocks and mocks.kafka is not None and bool(mocks.kafka.reactors)
               run_grpc = mocks and mocks.grpc is not None and bool(mocks.grpc.stubs)
    """
    if only == "http":
        # Guard: --only http with no mocks.http → ConfigError
        if mocks is None or mocks.http is None:
            raise ConfigError("--only http but no mocks.http configured", {})
        return True, False, False

    if only == "kafka":
        # Guard: --only kafka with no mocks.kafka.reactors → ConfigError
        if mocks is None or mocks.kafka is None or not mocks.kafka.reactors:
            raise ConfigError("--only kafka but no mocks.kafka.reactors configured", {})
        return False, True, False

    if only == "grpc":
        # Guard: --only grpc with no mocks.grpc.stubs → ConfigError. An empty
        # ``stubs`` dict is the same as missing (a grpc server with no stubs
        # has nothing to serve); the truthiness check covers both.
        if mocks is None or mocks.grpc is None or not mocks.grpc.stubs:
            raise ConfigError("--only grpc but no mocks.grpc.stubs configured", {})
        return False, False, True

    # No --only: resolve from mocks presence
    run_http = mocks is not None and mocks.http is not None
    run_kafka = mocks is not None and mocks.kafka is not None and bool(mocks.kafka.reactors)
    run_grpc = mocks is not None and mocks.grpc is not None and bool(mocks.grpc.stubs)
    return run_http, run_kafka, run_grpc


def _mock_start_core(
    config_path: str | None,
    http_listen: str | None,
    only: str | None,
    fail_fast: bool,
    duration: float | None,
    state_dir: str,
    overlay_paths: list[str] | None = None,
    env_file: str | None = None,
    grpc_listen: str | None = None,
) -> dict:
    """Core logic for `mock start` (Task 4).

    Spawns a detached mock daemon, writes a pidfile, and polls for readiness.

    Returns:
        Dict with keys: pid, listen, log_path, stubs, reactors, started_at,
        and (when run_grpc) grpc. HTTP listen/stubs are None when HTTP is not
        running; the grpc block carries {listen, stubs, services, reflection,
        health}.

    Raises:
        ConfigError: If already running, startup error, or timeout.
    """
    _require_posix_daemon()
    # Step 1: Load config
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths, env_file=env_file)

    # Step 2: Resolve which engines to run
    run_http, run_kafka, run_grpc = _resolve_engines(only, cfg.mocks)

    # Step 3: Resolve http_listen (still parsed even when run_http=False so a
    # bad override surfaces as a ConfigError — the daemon's child re-loads the
    # flag and would reject it anyway; better to fail fast in the parent).
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

    # Step 3b: Resolve grpc_listen (--grpc-listen overrides mocks.grpc.listen).
    # Parsed even when run_grpc=False so a malformed override fails fast in the
    # parent rather than disappearing into the daemon child.
    if grpc_listen is not None:
        try:
            parse_listen(grpc_listen)
        except ValueError as e:
            raise ConfigError(f"Invalid --grpc-listen: {e}", {})
    elif cfg.mocks and cfg.mocks.grpc:
        grpc_listen = cfg.mocks.grpc.listen

    # Extract port for pidfile keying. HTTP drives the legacy mock-<port>.pid
    # naming when present. A grpc-ONLY daemon (no HTTP) keys off the grpc port
    # under mock-grpc-<port>.pid (Task 9's ``engine="grpc"`` kwarg) so it
    # doesn't collide with an HTTP daemon on the same numeric port. Kafka-only
    # daemons still fall through to mock-kafka.pid (port None, no engine hint).
    port = None
    engine_hint: str | None = None
    if run_http:
        host, port = parsed_listen
        if port <= 0:
            raise ConfigError(
                "start requires a concrete --http-listen port (got 0)",
                {},
            )
    elif run_grpc and grpc_listen is not None:
        # grpc-only daemon: derive port from grpc_listen, key via engine="grpc".
        _gh, port = parse_listen(grpc_listen)
        if port <= 0:
            raise ConfigError(
                "start requires a concrete --grpc-listen port (got 0)",
                {},
            )
        engine_hint = "grpc"
    else:
        port = None

    # Step 4: Compute pidfile and log paths
    state_path = Path(state_dir)
    pid = pidfile_path(state_path, port, engine=engine_hint)
    logp = log_path(state_path, port, engine=engine_hint)

    # Step 5: Already-running pre-check
    existing = read_pidfile(pid)
    if existing is not None:
        existing_pid = existing.get("pid")
        if existing_pid is not None and is_alive(existing_pid):
            if run_http:
                raise ConfigError(
                    f"mock already running on {http_listen} (pid {existing_pid}); "
                    "run 'agctl mock stop' first or use a different --http-listen",
                    {"pid": existing_pid, "listen": http_listen},
                )
            elif run_grpc:
                raise ConfigError(
                    f"mock already running on {grpc_listen} (pid {existing_pid}); "
                    "run 'agctl mock stop' first or use a different --grpc-listen",
                    {"pid": existing_pid, "listen": grpc_listen},
                )
            else:
                raise ConfigError(
                    f"mock already running (kafka-only, pid {existing_pid}); "
                    "run 'agctl mock stop' first",
                    {"pid": existing_pid},
                )

    # Step 6: Build daemon argv
    daemon_argv = []
    # Global flags (parsed by root cli group) must come BEFORE the subcommand
    if config_path is not None:
        daemon_argv.extend(["--config", str(Path(config_path).absolute())])
    # Forward overlay paths to the daemon
    if overlay_paths is not None:
        for ov in overlay_paths:
            daemon_argv.extend(["--overlay", str(Path(ov).absolute())])
    # Forward --env-file: the daemon re-loads config from scratch, so without
    # this it would silently fall back to the default .env sibling and ignore
    # the user's flag (the parent's readiness load uses the right file, the
    # server that actually serves traffic would not).
    if env_file is not None:
        daemon_argv.extend(["--env-file", str(Path(env_file).absolute())])
    # Subcommand and mock-run-specific options
    daemon_argv.extend(["mock", "run"])
    if run_http:
        daemon_argv.extend(["--http-listen", http_listen])
    if run_grpc:
        daemon_argv.extend(["--grpc-listen", grpc_listen])
    if only is not None:
        daemon_argv.extend(["--only", only])
    if fail_fast:
        daemon_argv.append("--fail-fast")
    if duration is not None:
        daemon_argv.extend(["--duration", str(duration)])

    # Step 7: Spawn the daemon
    child_pid = spawn_daemon(daemon_argv, str(logp))

    # Step 8: Write pidfile. ``listen`` stays the legacy primary identity
    # (HTTP address when present, else None for kafka/grpc-only daemons).
    # ``http_listen``/``grpc_listen`` are the engine-specific addresses recorded
    # so ``mock status``/``mock stop`` selection by EITHER listen works
    # end-to-end (Task 9 obligation discharged here).
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
            "http_listen": http_listen if run_http else None,
            "grpc_listen": grpc_listen if run_grpc else None,
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
                # Cleanup: terminate daemon and remove pidfile
                _terminate(child_pid, _START_CLEANUP_GRACE_SECONDS)
                remove_pidfile(pid)

                # Extract error details from the startup_error envelope
                error = parsed.startup_error.get("error", {})
                message = error.get("message", "startup failed")
                detail = error.get("detail", {})
                # Point detail.listen at the engine that actually failed: the
                # HTTP address when run_http, else the gRPC address when run_grpc,
                # else None (kafka-only). For a grpc-only daemon http_listen
                # defaults to 0.0.0.0:18080 (the HTTP default), so the previous
                # unconditional ``detail["listen"] = http_listen`` pointed a
                # gRPC startup error at the wrong address (the message text still
                # named the gRPC port). (Fix 5)
                detail["listen"] = (
                    http_listen if run_http else (grpc_listen if run_grpc else None)
                )
                raise ConfigError(message, detail)

            # Sleep briefly before next poll
            time.sleep(0.05)

    except Exception:
        # Cleanup on any error
        _terminate(child_pid, _START_CLEANUP_GRACE_SECONDS)
        remove_pidfile(pid)
        raise

    # Step 10: Build result. HTTP listen/stubs stay as HTTP values (None when
    # HTTP not running, as before). A grpc block is added ONLY when run_grpc
    # so HTTP-only/kafka-only results stay unchanged.
    stubs = None
    reactors = []
    if started.get("http") is not None:
        stubs = started["http"].get("stubs")
    if started.get("kafka") is not None:
        reactors = [r["name"] for r in started["kafka"].get("reactors", [])]

    result = {
        "pid": child_pid,
        "listen": http_listen if run_http else None,
        "log_path": str(logp),
        "stubs": stubs,
        "reactors": reactors,
        "started_at": started_at,
    }
    if run_grpc:
        # ``started["grpc"]`` is emitted by MockEngine (Task 8) as
        # {listen, stubs, services, reflection, health}; pass through verbatim.
        grpc_started = started.get("grpc")
        if grpc_started is not None:
            result["grpc"] = grpc_started
        else:
            # Defensive: daemon declared run_grpc but emitted no grpc block —
            # surface an explicit None rather than dropping the key so consumers
            # can distinguish "grpc ran, no data" from "grpc not running".
            result["grpc"] = None
    return result


@click.command("start")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--http-listen", "http_listen", default=None, help="HTTP listen address (host:port)")
@click.option("--grpc-listen", "grpc_listen", default=None, help="gRPC listen address (host:port)")
@click.option(
    "--only",
    "only",
    type=click.Choice(["http", "kafka", "grpc"]),
    default=None,
    help="Run only HTTP, Kafka, or gRPC mock engine",
)
@click.option("--fail-fast", "fail_fast", is_flag=True, default=False, help="Exit on first reactor error")
@click.option("--duration", "duration", type=float, default=None, help="Stop after N seconds")
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for mock state (pidfiles, logs)")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def mock_start(
    ctx: click.Context,
    env_file: str | None,
    config_path: str | None,
    http_listen: str | None,
    grpc_listen: str | None,
    only: str | None,
    fail_fast: bool,
    duration: float | None,
    state_dir: str,
) -> None:
    """Start a detached mock daemon with HTTP server, Kafka reactors, and/or gRPC server."""
    # Fall back to ctx.obj["config_path"] if --config not provided
    if config_path is None:
        config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)

    _mock_start_envelope(config_path, http_listen, only, fail_fast, duration, state_dir, overlay_paths=list(ovs) if ovs else None, env_file=env_file, grpc_listen=grpc_listen)


_mock_start_envelope = envelope("mock.start")(_mock_start_core)


@click.command("run")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--http-listen", "http_listen", default=None, help="HTTP listen address (host:port)")
@click.option("--grpc-listen", "grpc_listen", default=None, help="gRPC listen address (host:port)")
@click.option(
    "--only",
    "only",
    type=click.Choice(["http", "kafka", "grpc"]),
    default=None,
    help="Run only HTTP, Kafka, or gRPC mock engine",
)
@click.option("--fail-fast", "fail_fast", is_flag=True, default=False, help="Exit on first reactor error")
@click.option("--duration", "duration", type=float, default=None, help="Stop after N seconds")
@click.option("--until-stopped", "until_stopped", is_flag=True, default=False, help="Run until stopped")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def mock_run(
    ctx: click.Context,
    env_file: str | None,
    config_path: str | None,
    http_listen: str | None,
    grpc_listen: str | None,
    only: str | None,
    fail_fast: bool,
    duration: float | None,
    until_stopped: bool,
) -> None:
    """Run HTTP mock server, Kafka reactors, and/or gRPC server, streaming NDJSON events."""
    # Fall back to ctx.obj["config_path"] if --config not provided
    if config_path is None:
        config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)

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
        cfg = load_config_or_raise(config_path, overlay_paths=list(ovs) if ovs else None, env_file=env_file)

        # Guard 2+3: Resolve engines to run
        run_http, run_kafka, run_grpc = _resolve_engines(only, cfg.mocks)

        # Guard 5: If run_kafka, resolve each reactor's cluster and build one
        # KafkaClient per DISTINCT cluster (reactors sharing a cluster reuse the
        # same client). The per-reactor client map is keyed by reactor name.
        kafka_clients: dict[str, KafkaClient] | None = None
        if run_kafka:
            kafka_clients = {}
            clients_by_cluster: dict[str, KafkaClient] = {}
            for reactor_name, reactor in cfg.mocks.kafka.reactors.items():
                try:
                    cluster_name = resolve_cluster_name(
                        cfg.kafka, binding_cluster=reactor.cluster
                    )
                except ConfigError as e:
                    # load_config does NOT run validate_config, so mock_run is
                    # the primary error surface for `agctl mock run`. Re-raise
                    # with reactor context so a dangling reactor.cluster or
                    # unresolvable (no default/single) cluster names the reactor.
                    raise ConfigError(
                        e.message, {**e.detail, "reactor": reactor_name}
                    ) from e
                cluster = cfg.kafka.clusters[cluster_name]
                # Per-reactor brokers guard (spec §11): a reactor whose resolved
                # cluster has empty brokers must fail fast with a clear
                # ConfigError at mock run, BEFORE any client build/probe. This
                # restores the runtime guarantee dropped in Task 3 — but per
                # reactor (honoring the brief's per-reactor resolution), not via
                # the old single-cluster guard.
                if not cluster.brokers:
                    raise ConfigError(
                        f"kafka.clusters.{cluster_name}.brokers is required when running Kafka reactors",
                        {"reactor": reactor_name, "cluster": cluster_name},
                    )
                if cluster_name not in clients_by_cluster:
                    clients_by_cluster[cluster_name] = new_kafka_client(cluster)
                kafka_clients[reactor_name] = clients_by_cluster[cluster_name]

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

        # Guard 7: Resolve grpc_listen (--grpc-listen overrides mocks.grpc.listen).
        # Parsed even when run_grpc=False so a malformed override fails loudly
        # instead of being silently dropped (mirrors --http-listen's discipline).
        if grpc_listen is not None:
            try:
                parse_listen(grpc_listen)
            except ValueError as e:
                raise ConfigError(f"Invalid --grpc-listen: {e}", {})
        elif cfg.mocks and cfg.mocks.grpc:
            grpc_listen = cfg.mocks.grpc.listen

        # Build the engine (via the test seam). Threads the top-level
        # Config.grpc.descriptors fallback (Task 8 obligation) so MockGrpcServer
        # can fall back from mocks.grpc.descriptors to grpc.descriptors when the
        # per-mock list is None. ``grpc_server_factory`` is left to its default
        # (None) — MockEngine lazy-imports the real factory on demand.
        descriptors = cfg.grpc.descriptors if cfg.grpc.descriptors else None
        engine = new_mock_engine(
            mocks=cfg.mocks,
            run_http=run_http,
            run_kafka=run_kafka,
            http_listen=http_listen,
            kafka_clients=kafka_clients,
            fail_fast=fail_fast,
            duration=duration,
            until_stopped=until_stopped,
            run_grpc=run_grpc,
            grpc_listen=grpc_listen,
            top_level_descriptors=descriptors,
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


# ----------------------------------------------------------------------------
# Task 5: mock stop command
# ----------------------------------------------------------------------------


def _mock_stop_core(
    listen: str | None,
    pid: int | None,
    all_: bool,
    timeout: float,
    state_dir: str,
) -> dict:
    """Core logic for `mock stop` (Task 5).

    Stops running mock daemon(s), sends SIGTERM/SIGKILL, parses log for verdict.

    Returns:
        Dict with keys: stopped (bool or list), signal, summary, failures, etc.
        For single target: {stopped: True, pid: ..., signal: ..., summary: ..., failures: [...]}
        For --all: {stopped: [{...}, {...}]}
        For not-running (single): {stopped: False}
        For not-running (--all): {stopped: []}

    Raises:
        AssertionFailure: When any stopped mock had fatal failure events.
    """
    _require_posix_daemon()
    state_path = Path(state_dir)

    # Step 1: Resolve targets
    targets = resolve_target(state_path, listen, pid, all_)

    # Step 2: Handle not-running case
    if not targets:
        if all_:
            return {"stopped": []}
        return {"stopped": False}

    # Step 3: Stop each target and collect entries
    entries = []
    for target in targets:
        sig = "SIGTERM"
        warning = None

        # Step 3a: Check if process is alive before attempting termination
        was_alive_before = is_alive(target.pid)

        # Step 3b: Try to reap zombie child if this is our child process (unit test context)
        # This dual behavior is intentional: in unit tests, the sleeper is the test process's child,
        # so we reap it here. In production, the daemon is not our child, so this raises ChildProcessError
        # and we fall through to _terminate which handles non-child processes.
        try:
            pid, status = os.waitpid(target.pid, os.WNOHANG)
            if pid == target.pid:
                # Child has exited and been reaped
                was_alive_before = False
        except (ChildProcessError, OSError):
            # Not our child process - continue to _terminate below
            pass

        # Step 3c: Terminate the process (sends SIGTERM, waits, sends SIGKILL if timeout)
        if was_alive_before:
            sig = _terminate(target.pid, timeout)
            if sig == "SIGKILL":
                timeout_str = str(int(timeout)) if timeout == int(timeout) else str(timeout)
                warning = f"process did not exit on SIGTERM within {timeout_str}s; sent SIGKILL; summary may be incomplete"

        # Step 3d: Parse log
        parsed = parse_log(Path(target.log_path))

        # Step 3e: Build entry
        entry = {
            "stopped": True,
            "pid": target.pid,
            "signal": sig,
            "summary": parsed.summary or {},
            "failures": parsed.failures,
        }
        if warning is not None:
            entry["warning"] = warning

        entries.append(entry)

        # Step 3f: Remove pidfile (after parsing log)
        remove_pidfile(target.pidfile_path)

    # Step 4: Aggregate and check for fatal failures
    if not all_:
        # Single target
        verdict = entries[0]
        fatal = [f for f in verdict["failures"] if f.get("event") in FATAL_FAILURE_EVENTS]
        if fatal:
            raise AssertionFailure(
                f"mock run had {len(fatal)} fatal failure event(s)",
                verdict,
            )
        return verdict
    else:
        # --all: check for any bad entries
        bad = [e for e in entries if any(f.get("event") in FATAL_FAILURE_EVENTS for f in e["failures"])]
        if bad:
            raise AssertionFailure(
                f"{len(bad)} of {len(entries)} mock(s) had fatal failures",
                {"stopped": entries},
            )
        return {"stopped": entries}


@click.command("stop")
@click.option("--listen", "listen", type=str, default=None, help="Listen address (e.g., 127.0.0.1:18080)")
@click.option("--pid", "pid", type=int, default=None, help="Process ID")
@click.option("--all", "all_", is_flag=True, default=False, help="Stop all running mocks")
@click.option("--timeout", "timeout", type=float, default=10.0, help="Seconds to wait for SIGTERM before SIGKILL")
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for mock state (pidfiles, logs)")
@click.pass_context
def mock_stop(
    ctx: click.Context,
    listen: str | None,
    pid: int | None,
    all_: bool,
    timeout: float,
    state_dir: str,
) -> None:
    """Stop a running mock daemon with SIGTERM/SIGKILL and parse verdict."""
    _mock_stop_envelope(listen, pid, all_, timeout, state_dir)


_mock_stop_envelope = envelope("mock.stop")(_mock_stop_core)


# ----------------------------------------------------------------------------
# Task 6: mock status command
# ----------------------------------------------------------------------------


def _mock_status_core(
    listen: str | None,
    state_dir: str,
) -> dict:
    """Core logic for `mock status` (Task 6).

    Returns live snapshot of a running mock by resolving via pidfile and
    parsing the NDJSON log. Never signals the daemon and never removes the
    pidfile.

    Returns:
        Dict with keys: running (bool), pid, listen, uptime_ms, summary_so_far,
        failures_so_far. When not running: {"running": False}.

    Raises:
        ConfigError: If multiple mocks running and no selector, or if
            specified --listen doesn't match any running mock.
    """
    _require_posix_daemon()
    state_path = Path(state_dir)

    # Step 1: Resolve targets (no --all flag for status)
    targets = resolve_target(state_path, listen, None, all_=False)

    # Step 2: Handle not-running case
    if not targets:
        return {"running": False}

    # Step 3: Get the single target
    target = targets[0]

    # Step 4: Parse the log
    parsed = parse_log(Path(target.log_path))

    # Step 5: Compute uptime_ms from started_at (ISO-8601 Z)
    uptime_ms = None
    try:
        # Parse ISO-8601 with Z suffix (UTC)
        started_at_str = target.started_at
        if started_at_str.endswith("Z"):
            started_at_str = started_at_str.replace("Z", "+00:00")
        started_at = datetime.fromisoformat(started_at_str)
        now_utc = datetime.now(timezone.utc)
        uptime_ms = int((now_utc - started_at).total_seconds() * 1000)
    except (ValueError, TypeError):
        # If parsing fails, leave uptime_ms as None
        uptime_ms = None

    # Step 6: Return live snapshot
    return {
        "running": True,
        "pid": target.pid,
        "listen": target.listen,
        "uptime_ms": uptime_ms,
        "summary_so_far": parsed.summary_so_far,
        "failures_so_far": parsed.failures,
    }


@click.command("status")
@click.option("--listen", "listen", type=str, default=None, help="Listen address (e.g., 127.0.0.1:18080)")
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for mock state (pidfiles, logs)")
@click.pass_context
def mock_status(
    ctx: click.Context,
    listen: str | None,
    state_dir: str,
) -> None:
    """Show live status of a running mock daemon (no signal)."""
    _mock_status_envelope(listen, state_dir)


_mock_status_envelope = envelope("mock.status")(_mock_status_core)
