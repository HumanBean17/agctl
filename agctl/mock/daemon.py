"""Pure-function helpers for mock daemon lifecycle (pidfile, liveness, target resolution).

This module provides the foundational utilities for managing mock daemon processes:
- Pidfile read/write operations with graceful error handling
- Process liveness detection via os.kill(0)
- Target resolution for mock commands (start/stop/status)
- Automatic cleanup of stale pidfiles

No dependency on the mock engine — fully unit-testable with temporary directories.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..daemon import is_alive, read_pidfile, remove_pidfile, write_pidfile
from ..errors import ConfigError


@dataclass(frozen=True)
class RunningMock:
    """A running mock instance, reconstructed from a pidfile.

    Attributes:
        pid: Process ID of the running mock.
        listen: Legacy primary listen address (e.g., "127.0.0.1:18080") or
            None for kafka-only / grpc-only daemons that don't expose HTTP.
        port: Primary port (e.g., 18080) or None for kafka-only mocks.
        log_path: Absolute path to the mock's NDJSON log file.
        config_path: Absolute path to the mock's config.yaml, or None if no config.
        started_at: ISO-8601 timestamp when the mock was started (UTC, Z suffix).
        run_id: Unique run identifier for this mock invocation.
        pidfile_path: Path to the pidfile this data was read from.
        http_listen: HTTP listen address (e.g., "127.0.0.1:18080") when an HTTP
            engine is attached, else None. Recorded in the pidfile so multi-engine
            (HTTP+gRPC) and HTTP-only daemons can be targeted by their HTTP address.
        grpc_listen: gRPC listen address (e.g., "127.0.0.1:50051") when a gRPC
            engine is attached, else None. Recorded in the pidfile so grpc-only
            and HTTP+gRPC daemons can be targeted by their gRPC address.
    """

    pid: int
    listen: str | None
    port: int | None
    log_path: str
    config_path: str | None
    started_at: str
    run_id: str
    pidfile_path: Path
    http_listen: str | None = None
    grpc_listen: str | None = None


def pidfile_path(
    state_dir: Path, port: int | None, *, engine: str | None = None
) -> Path:
    """Return the pidfile path for a mock with the given port/engine.

    Args:
        state_dir: Directory where mock state is stored.
        port: Primary port number, or None for kafka-only mocks.
        engine: Optional engine hint. ``"grpc"`` keys a grpc-only daemon under
            ``mock-grpc-<port>.pid`` so it doesn't collide with an HTTP daemon on
            the same numeric port. ``None`` (default) preserves the legacy
            ``mock-<port>.pid`` / ``mock-kafka.pid`` naming so existing HTTP and
            kafka call sites are unaffected.

    Returns:
        Path to the pidfile:
        - ``<state_dir>/mock-grpc-<port>.pid`` when ``engine == "grpc"``
        - ``<state_dir>/mock-kafka.pid`` when ``engine is None`` and port is None
        - ``<state_dir>/mock-<port>.pid`` otherwise (HTTP-present default)
    """
    if engine == "grpc":
        return state_dir / f"mock-grpc-{port}.pid"
    if port is None:
        return state_dir / "mock-kafka.pid"
    return state_dir / f"mock-{port}.pid"


def log_path(
    state_dir: Path, port: int | None, *, engine: str | None = None
) -> Path:
    """Return the log file path for a mock with the given port/engine.

    Mirrors :func:`pidfile_path` naming so a daemon's pidfile and log share a
    stem.

    Args:
        state_dir: Directory where mock state is stored.
        port: Primary port number, or None for kafka-only mocks.
        engine: Optional engine hint (see :func:`pidfile_path`).

    Returns:
        Path to the log file:
        - ``<state_dir>/mock-grpc-<port>.log`` when ``engine == "grpc"``
        - ``<state_dir>/mock-kafka.log`` when ``engine is None`` and port is None
        - ``<state_dir>/mock-<port>.log`` otherwise (HTTP-present default)
    """
    if engine == "grpc":
        return state_dir / f"mock-grpc-{port}.log"
    if port is None:
        return state_dir / "mock-kafka.log"
    return state_dir / f"mock-{port}.log"


def list_running_mocks(state_dir: Path) -> list[RunningMock]:
    """List all running mocks in the state directory, cleaning up stale pidfiles.

    Creates the state_dir if it doesn't exist (no error on missing/empty dirs).

    Args:
        state_dir: Directory containing mock pidfiles.

    Returns:
        A list of RunningMock instances, one for each live mock. Empty if none.
    """
    state_dir.mkdir(parents=True, exist_ok=True)

    running = []
    for pidfile in state_dir.glob("mock-*.pid"):
        data = read_pidfile(pidfile)
        if data is None:
            continue

        pid = data.get("pid")
        if not isinstance(pid, int):
            continue

        if is_alive(pid):
            running.append(
                RunningMock(
                    pid=pid,
                    listen=data.get("listen"),
                    port=data.get("port"),
                    log_path=data.get("log_path", ""),
                    config_path=data.get("config_path"),
                    started_at=data.get("started_at", ""),
                    run_id=data.get("run_id", ""),
                    pidfile_path=pidfile,
                    http_listen=data.get("http_listen"),
                    grpc_listen=data.get("grpc_listen"),
                )
            )
        else:
            # Stale pidfile — clean it up
            remove_pidfile(pidfile)

    return running


def resolve_target(
    state_dir: Path,
    listen: str | None,
    pid: int | None,
    all_: bool,
) -> list[RunningMock]:
    """Resolve the target mock(s) for a start/stop/status operation.

    Args:
        state_dir: Directory containing mock pidfiles.
        listen: Listen address string to match (e.g., "127.0.0.1:18080"), or None.
        pid: Process ID to match, or None.
        all_: If True, return all running mocks (ignores listen/pid).

    Returns:
        A list of RunningMock instances to operate on. Empty if no mocks are
        running and no specific target was requested.

    Raises:
        ConfigError: If multiple mocks are running and no target is specified,
            or if the specified listen/pid doesn't match any running mock.
    """
    if all_:
        return list_running_mocks(state_dir)

    if pid is not None:
        candidates = list_running_mocks(state_dir)
        for mock in candidates:
            if mock.pid == pid:
                return [mock]
        raise ConfigError(f"no running mock with pid {pid}", {"pid": pid})

    if listen is not None:
        candidates = list_running_mocks(state_dir)
        for mock in candidates:
            # Match against any of the engine listen addresses a daemon exposes:
            # legacy ``listen`` (primary identity) plus explicit ``http_listen``
            # / ``grpc_listen`` for multi-engine and engine-only daemons.
            if listen in (mock.listen, mock.http_listen, mock.grpc_listen):
                return [mock]
        raise ConfigError(f"no running mock on {listen}", {"listen": listen})

    # No target specified — use implicit behavior
    candidates = list_running_mocks(state_dir)
    if len(candidates) == 0:
        return []
    if len(candidates) == 1:
        return [candidates[0]]

    # Multiple running — require explicit target
    raise ConfigError(
        "multiple mocks running; specify --listen or --pid",
        {"candidates": [r.listen for r in candidates]},
    )


# ----------------------------------------------------------------------------
# NDJSON log parser and failure taxonomy (Task 3)
# ----------------------------------------------------------------------------

FATAL_FAILURE_EVENTS: frozenset[str] = frozenset(
    {
        "http.unmatched",
        "http.body_parse_skipped",
        "kafka.skipped",
        "kafka.error",
        "grpc.unmatched",
        "grpc.error",
    }
)

ALL_FAILURE_EVENTS: frozenset[str] = FATAL_FAILURE_EVENTS | {"capture.missing"}

EVENT_TO_COUNTER: dict[str, str] = {
    "http.hit": "http_hits",
    "http.unmatched": "http_unmatched",
    "http.body_parse_skipped": "http_body_parse_skipped",
    "kafka.reacted": "kafka_reactions",
    "kafka.skipped": "kafka_skipped",
    "kafka.error": "kafka_errors",
    "grpc.hit": "grpc_hits",
    "grpc.unmatched": "grpc_unmatched",
    "grpc.error": "grpc_errors",
}


@dataclass
class ParsedLog:
    """Parsed NDJSON log from a mock daemon.

    Attributes:
        started: The "started" event object, if present.
        startup_error: The startup-error envelope (ok=false), if present.
        summary: The "summary" event object, if present.
        summary_so_far: Counter increments accumulated during parsing.
        failures: List of all failure events in order of appearance.
    """

    started: dict[str, Any] | None
    startup_error: dict[str, Any] | None
    summary: dict[str, Any] | None
    summary_so_far: dict[str, int]
    failures: list[dict[str, Any]]


def parse_log(path: Path) -> ParsedLog:
    """Read and parse an NDJSON log file from a mock daemon.

    Args:
        path: Path to the log file (may not exist).

    Returns:
        A ParsedLog with started, startup_error, summary, summary_so_far,
        and failures populated from the log lines.
    """
    started: dict[str, Any] | None = None
    startup_error: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    failures: list[dict[str, Any]] = []

    # Initialize summary_so_far with all counters at zero
    summary_so_far = {field: 0 for field in EVENT_TO_COUNTER.values()}

    # Return empty ParsedLog if file doesn't exist
    if not path.exists():
        return ParsedLog(
            started=started,
            startup_error=startup_error,
            summary=summary,
            summary_so_far=summary_so_far,
            failures=failures,
        )

    try:
        lines = path.read_text().splitlines()
    except OSError:
        # File exists but is unreadable
        return ParsedLog(
            started=started,
            startup_error=startup_error,
            summary=summary,
            summary_so_far=summary_so_far,
            failures=failures,
        )

    for line in lines:
        line = line.strip()
        if not line:
            continue

        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            # Skip unparseable lines silently
            continue

        # Check if this is an event line
        if "event" in obj:
            event = obj["event"]

            # Handle specific event types
            if event == "started":
                started = obj
            elif event == "summary":
                summary = obj
            elif event in ALL_FAILURE_EVENTS:
                failures.append(obj)

            # Update summary_so_far if this event maps to a counter
            if event in EVENT_TO_COUNTER:
                counter_field = EVENT_TO_COUNTER[event]
                summary_so_far[counter_field] += 1

        else:
            # No "event" key — check for startup-error envelope
            if obj.get("ok") is False:
                startup_error = obj

    return ParsedLog(
        started=started,
        startup_error=startup_error,
        summary=summary,
        summary_so_far=summary_so_far,
        failures=failures,
    )


def has_fatal_failure(parsed: ParsedLog) -> bool:
    """Check if a parsed log contains any fatal failure events.

    Args:
        parsed: A ParsedLog instance.

    Returns:
        True if any entry in parsed.failures has an event in FATAL_FAILURE_EVENTS.
    """
    for failure in parsed.failures:
        event = failure.get("event")
        if event in FATAL_FAILURE_EVENTS:
            return True
    return False
