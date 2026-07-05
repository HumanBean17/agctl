"""Pure-function helpers for mock daemon lifecycle (pidfile, liveness, target resolution).

This module provides the foundational utilities for managing mock daemon processes:
- Pidfile read/write operations with graceful error handling
- Process liveness detection via os.kill(0)
- Target resolution for mock commands (start/stop/status)
- Automatic cleanup of stale pidfiles

No dependency on the mock engine — fully unit-testable with temporary directories.
"""

import errno
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..errors import ConfigError


@dataclass(frozen=True)
class RunningMock:
    """A running mock instance, reconstructed from a pidfile.

    Attributes:
        pid: Process ID of the running mock.
        listen: Listen address (e.g., "127.0.0.1:18080") or None for kafka-only.
        port: HTTP port (e.g., 18080) or None for kafka-only.
        log_path: Absolute path to the mock's NDJSON log file.
        config_path: Absolute path to the mock's config.yaml, or None if no config.
        started_at: ISO-8601 timestamp when the mock was started (UTC, Z suffix).
        run_id: Unique run identifier for this mock invocation.
        pidfile_path: Path to the pidfile this data was read from.
    """

    pid: int
    listen: str | None
    port: int | None
    log_path: str
    config_path: str | None
    started_at: str
    run_id: str
    pidfile_path: Path


def pidfile_path(state_dir: Path, port: int | None) -> Path:
    """Return the pidfile path for a mock with the given port.

    Args:
        state_dir: Directory where mock state is stored.
        port: HTTP port number, or None for kafka-only mocks.

    Returns:
        Path to the pidfile: `<state_dir>/mock-<port>.pid` for HTTP mocks,
        `<state_dir>/mock-kafka.pid` for kafka-only mocks.
    """
    if port is None:
        return state_dir / "mock-kafka.pid"
    return state_dir / f"mock-{port}.pid"


def log_path(state_dir: Path, port: int | None) -> Path:
    """Return the log file path for a mock with the given port.

    Args:
        state_dir: Directory where mock state is stored.
        port: HTTP port number, or None for kafka-only mocks.

    Returns:
        Path to the log file: `<state_dir>/mock-<port>.log` for HTTP mocks,
        `<state_dir>/mock-kafka.log` for kafka-only mocks.
    """
    if port is None:
        return state_dir / "mock-kafka.log"
    return state_dir / f"mock-{port}.log"


def read_pidfile(path: Path) -> dict[str, Any] | None:
    """Read a pidfile and return its contents as a dict.

    Never raises: returns None if the file is missing or unparseable.

    Args:
        path: Path to the pidfile.

    Returns:
        The pidfile contents as a dict, or None if the file is missing or
        contains invalid JSON.
    """
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def write_pidfile(path: Path, data: dict[str, Any]) -> None:
    """Write mock process data to a pidfile as JSON.

    Args:
        path: Path to the pidfile (will be overwritten if it exists).
        data: Dictionary with keys: pid, listen, port, log_path, config_path,
              started_at (ISO-8601 Z), run_id.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def remove_pidfile(path: Path) -> None:
    """Remove a pidfile, ignoring FileNotFoundError if it doesn't exist.

    Args:
        path: Path to the pidfile to remove.
    """
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def is_alive(pid: int) -> bool:
    """Check if a process with the given PID is alive.

    Uses os.kill(pid, 0) which sends no signal but checks process existence.

    Args:
        pid: Process ID to check.

    Returns:
        True if the process exists, False otherwise.
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except OSError as e:
        if e.errno == errno.ESRCH:
            return False
        # PermissionError means the process exists but we don't own it
        return True


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
            if mock.listen == listen:
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
