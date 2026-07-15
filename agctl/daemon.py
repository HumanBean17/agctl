"""Generic process/pidfile lifecycle primitives shared by `mock` and `listen`.

This module provides foundational, transport-agnostic utilities for managing
detached daemon processes:
- Spawn a detached daemon subprocess (``spawn_daemon``)
- Terminate a process with SIGTERM → SIGKILL escalation (``terminate``)
- POSIX-only gating for the managed daemon surface (``require_posix_daemon``)
- Pidfile read/write/remove with graceful error handling
- Process liveness detection via ``os.kill(pid, 0)`` (``is_alive``)

These primitives were moved verbatim from ``agctl/commands/mock_commands.py``
(``spawn_daemon``, ``_terminate``, ``_require_posix_daemon``) and
``agctl/mock/daemon.py`` (``is_alive``, ``read_pidfile``, ``write_pidfile``,
``remove_pidfile``) so the upcoming ``kafka listen`` capture daemon can reuse
them without coupling to ``mock``.

No dependency on ``mock``, ``listen``, or ``commands`` — only ``errors``.
Fully unit-testable with temporary directories.
"""

from __future__ import annotations

import errno
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from .errors import ConfigError


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

    # Close the parent's copy of the log file handle (the child holds its own dup)
    log_handle.close()

    return proc.pid


def terminate(pid: int, timeout: float) -> str:
    """Terminate a process with SIGTERM, wait for exit, SIGKILL if timeout.

    Args:
        pid: Process ID to terminate.
        timeout: Seconds to wait for graceful exit after SIGTERM before SIGKILL.

    Returns:
        The signal that was used: "SIGTERM" if process exited on SIGTERM,
        "SIGKILL" if timeout elapsed and SIGKILL was sent.

    This is the shared discipline for both mock start cleanup (short grace) and
    mock stop (user-configurable timeout). A daemon hung in a blocking C call
    (e.g., Kafka broker TCP connect) will ignore SIGTERM; SIGKILL ensures cleanup.
    """
    # Step 1: Send SIGTERM (best-effort)
    try:
        os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, OSError):
        return "SIGTERM"  # Already dead or doesn't exist

    # Step 2: Wait for process to exit or timeout
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start

        # Try to reap zombie child first (non-blocking)
        # This is critical in unit test context where the sleeper is our child:
        # if it exits on SIGTERM but we don't reap it, is_alive() still returns True
        # because the zombie process entry exists. Reaping turns is_alive() to False.
        try:
            reaped_pid, status = os.waitpid(pid, os.WNOHANG)
            if reaped_pid == pid:
                # Child has exited and been reaped
                return "SIGTERM"
        except (ChildProcessError, OSError):
            # Not our child or doesn't exist - check with is_alive
            pass

        # Check if process is gone using is_alive (for non-child processes)
        if not is_alive(pid):
            return "SIGTERM"  # Exited gracefully

        # Timeout - send SIGKILL
        if elapsed >= timeout:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass  # Already dead
            # Brief wait after SIGKILL to ensure process is reaped by the system
            time.sleep(0.1)
            return "SIGKILL"

        # Sleep briefly before next poll
        time.sleep(0.05)


def require_posix_daemon() -> None:
    """Gate the managed daemon surface to POSIX.

    On native Windows (``os.name == "nt"``) the managed daemon
    (``mock start``/``stop``/``status`` and the upcoming ``listen`` daemon) is
    unsupported; raise ``ConfigError`` pointing at the foreground command or WSL.
    WSL reports ``"posix"`` and passes through. Detection reads ``os.name`` via
    this module's ``os`` binding so a unit test can force the branch without
    mutating the global ``os`` module.
    """
    if os.name == "nt":
        raise ConfigError(
            "the managed mock daemon (mock start/stop/status) is supported on "
            "Linux, macOS, and WSL; on native Windows use 'agctl mock run' or "
            "run inside WSL",
            {
                "platform": sys.platform,
                "hint": (
                    "use 'agctl mock run' (foreground) or run agctl inside WSL "
                    "for the managed daemon"
                ),
            },
        )


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
    """Write process data to a pidfile as JSON.

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
