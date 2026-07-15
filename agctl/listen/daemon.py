"""Pure-function helpers for the `kafka listen` capture daemon lifecycle.

This module is the listen-specific, key-by-``run_id`` analogue of
``agctl/mock/daemon.py``. It provides the lifecycle/pidfile/events-log layer
that the later ``kafka listen`` command tasks consume:

- ``run_id`` generation (``secrets.token_hex(4)``) and state-path derivation
- ``RunningListener`` frozen dataclass reconstructed from a pidfile
- Pidfile listing + stale-pid cleanup + target resolution
- ``meta.json`` and ``asserts.jsonl`` read/append helpers
- ``events.log`` NDJSON parser (``ParsedEvents``)

It is deliberately pure: no ``confluent_kafka``, no ``jq``, no network. The
generic primitives (``is_alive``/``read_pidfile``/``remove_pidfile``) are reused
from ``agctl.daemon`` (Task 1) — not reimplemented here. ``write_pidfile`` is
also a Task-1 primitive; it is consumed by the ``listen start`` command (Task 8),
not by this pure-function layer. Fully unit-testable with temporary directories.
"""

from __future__ import annotations

import json
import secrets
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ..daemon import is_alive, read_pidfile, remove_pidfile
from ..errors import ConfigError


# ----------------------------------------------------------------------------
# run_id + state-path derivation (Shared Types contract)
# ----------------------------------------------------------------------------


def new_run_id() -> str:
    """Return a fresh 8-hex-char run id (``secrets.token_hex(4)``).

    The run id keys all per-run state: the pidfile name, the run directory, and
    the consumer group string (``agctl-listen-<run_id>``).
    """
    return secrets.token_hex(4)


def pidfile_path(state_dir: Path, run_id: str) -> Path:
    """Return the pidfile path for a run: ``<state_dir>/listen-<run_id>.pid``."""
    return state_dir / f"listen-{run_id}.pid"


def run_dir(state_dir: Path, run_id: str) -> Path:
    """Return the run directory: ``<state_dir>/listen-<run_id>/``."""
    return state_dir / f"listen-{run_id}"


def events_log_path(run_dir: Path) -> Path:
    """Return the events-log path: ``<run_dir>/events.log``."""
    return run_dir / "events.log"


def capture_path(run_dir: Path, topic: str) -> Path:
    """Return the per-topic capture path: ``<run_dir>/<topic>.ndjson``."""
    return run_dir / f"{topic}.ndjson"


def meta_path(run_dir: Path) -> Path:
    """Return the metadata path: ``<run_dir>/meta.json``."""
    return run_dir / "meta.json"


def asserts_path(run_dir: Path) -> Path:
    """Return the attached-expectations path: ``<run_dir>/asserts.jsonl``."""
    return run_dir / "asserts.jsonl"


# ----------------------------------------------------------------------------
# RunningListener + pidfile enumeration / target resolution
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class RunningListener:
    """A running ``kafka listen`` capture daemon, reconstructed from a pidfile.

    Attributes:
        pid: Process ID of the running listener.
        run_id: 8-hex-char run id (keys the pidfile + run dir).
        topics: Topic list the listener subscribed to.
        group: Consumer group string (``agctl-listen-<run_id>``).
        cluster: Cluster name the listener connected to.
        started_at: ISO-8601 timestamp when the listener was started (UTC, Z).
        state_dir: Absolute path to the state directory holding the pidfile.
        log_path: Absolute path to the listener's ``events.log``.
        pidfile_path: Path to the pidfile this data was read from.
    """

    pid: int
    run_id: str
    topics: list[str]
    group: str
    cluster: str
    started_at: str
    state_dir: str
    log_path: str
    pidfile_path: Path


def list_running_listeners(state_dir: Path) -> list[RunningListener]:
    """List running listeners, cleaning stale (dead-pid) pidfiles.

    Globs ``listen-*.pid``; for each parseable pidfile whose ``pid`` is alive,
    builds a :class:`RunningListener`. Pidfiles whose pid is dead are removed.
    Does not error on a missing/empty directory.

    Args:
        state_dir: Directory containing listen pidfiles.

    Returns:
        A list of :class:`RunningListener`, one per live listener. Empty if none.
    """
    if not state_dir.exists():
        return []

    running: list[RunningListener] = []
    for pidfile in state_dir.glob("listen-*.pid"):
        data = read_pidfile(pidfile)
        if data is None:
            continue

        pid = data.get("pid")
        if not isinstance(pid, int):
            continue

        if is_alive(pid):
            running.append(
                RunningListener(
                    pid=pid,
                    run_id=data.get("run_id", ""),
                    topics=data.get("topics", []),
                    group=data.get("group", ""),
                    cluster=data.get("cluster", ""),
                    started_at=data.get("started_at", ""),
                    state_dir=data.get("state_dir", ""),
                    log_path=data.get("log_path", ""),
                    pidfile_path=pidfile,
                )
            )
        else:
            # Stale pidfile — clean it up.
            remove_pidfile(pidfile)

    return running


def resolve_listener_target(
    state_dir: Path,
    *,
    run_id: str | None,
    pid: int | None,
    all_: bool,
) -> list[RunningListener]:
    """Resolve the target listener(s) for a listen subcommand.

    Args:
        state_dir: Directory containing listen pidfiles.
        run_id: Run id to match, or None.
        pid: Process id to match, or None.
        all_: If True, return every running listener (ignores run_id/pid).

    Returns:
        A list of :class:`RunningListener` to operate on. Empty if no listeners
        are running and no specific target was requested.

    Raises:
        ConfigError: If multiple listeners are running and no selector is given,
            or if the given selector matches nothing.
    """
    if all_:
        return list_running_listeners(state_dir)

    if pid is not None:
        candidates = list_running_listeners(state_dir)
        for listener in candidates:
            if listener.pid == pid:
                return [listener]
        raise ConfigError(f"no running listener with pid {pid}", {"pid": pid})

    if run_id is not None:
        candidates = list_running_listeners(state_dir)
        for listener in candidates:
            if listener.run_id == run_id:
                return [listener]
        raise ConfigError(f"no running listener with run_id {run_id}", {"run_id": run_id})

    # No selector — implicit singleton.
    candidates = list_running_listeners(state_dir)
    if len(candidates) == 0:
        return []
    if len(candidates) == 1:
        return [candidates[0]]

    raise ConfigError(
        "multiple listeners running; specify --run-id, --pid, or --all",
        {"candidates": [r.run_id for r in candidates]},
    )


# ----------------------------------------------------------------------------
# meta.json + asserts.jsonl helpers
# ----------------------------------------------------------------------------


def write_meta(run_dir: Path, meta: dict[str, Any]) -> None:
    """Write ``meta.json`` into the run dir (creating parents as needed).

    Args:
        run_dir: The run directory (``<state_dir>/listen-<run_id>``).
        meta: Metadata dict to serialize as JSON.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    meta_path(run_dir).write_text(json.dumps(meta))


def read_meta(run_dir: Path) -> dict[str, Any] | None:
    """Read ``meta.json`` from the run dir.

    Never raises: returns None if the file is missing or unparseable.
    """
    path = meta_path(run_dir)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


@dataclass(frozen=True)
class ExpectationSpec:
    """One attached-expectation line in ``asserts.jsonl``.

    Attributes:
        id: Stable id for this expectation (defaults to ``exp-<n>`` in the
            command layer).
        topic: Topic whose ``<topic>.ndjson`` capture file this expectation scans.
        modes: Match-mode dict with optional keys ``contains``/``match``/
            ``pattern``/``path`` (any absent mode is treated as unused).
        params: ``--param`` substitutions for filling named patterns.
        expect_count: Minimum matching message count for a passing verdict.
    """

    id: str
    topic: str
    modes: dict[str, Any]
    params: dict[str, str]
    expect_count: int


def append_expectation(run_dir: Path, spec: ExpectationSpec) -> None:
    """Append one expectation spec as a JSON line to ``asserts.jsonl``.

    Creates the run dir (and parents) if absent.

    Args:
        run_dir: The run directory (``<state_dir>/listen-<run_id>``).
        spec: The :class:`ExpectationSpec` to append.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    with asserts_path(run_dir).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(spec)))
        fh.write("\n")


def read_expectations(run_dir: Path) -> list[dict[str, Any]]:
    """Read ``asserts.jsonl``, skipping blank/unparseable lines.

    Args:
        run_dir: The run directory (``<state_dir>/listen-<run_id>``).

    Returns:
        A list of expectation dicts in file order. Empty if the file is absent.
    """
    path = asserts_path(run_dir)
    if not path.exists():
        return []

    results: list[dict[str, Any]] = []
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        results.append(obj)

    return results


# ----------------------------------------------------------------------------
# events.log NDJSON parser
# ----------------------------------------------------------------------------


@dataclass
class ParsedEvents:
    """Parsed ``events.log`` NDJSON from a listen capture daemon.

    Attributes:
        started: The ``started`` event object, if present.
        startup_error: The startup-error envelope (``ok: false``, no ``event``
            key), if present.
        summary: The ``summary`` event object, if present.
        overflow_topics: Topics that emitted ``capture.overflow``, in order of
            appearance (duplicates preserved — one entry per overflow event).
        errors: ``kafka.error`` event objects, in order of appearance.
    """

    started: dict[str, Any] | None = None
    startup_error: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    overflow_topics: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)


def parse_events_log(path: Path) -> ParsedEvents:
    """Read and parse an ``events.log`` NDJSON file from a listen daemon.

    Recognized event types (the ``event`` key): ``started``, ``summary``,
    ``capture.overflow`` (its ``topic`` is appended to ``overflow_topics``), and
    ``kafka.error`` (appended to ``errors``). A line whose JSON has ``ok`` set
    to ``False`` and no ``event`` key is the startup-error envelope.

    Blank and unparseable lines are skipped. A missing file yields an empty
    :class:`ParsedEvents`.

    Args:
        path: Path to the events.log file (may not exist).

    Returns:
        A :class:`ParsedEvents` populated from the log lines.
    """
    parsed = ParsedEvents()

    if not path.exists():
        return parsed

    try:
        lines = path.read_text().splitlines()
    except OSError:
        return parsed

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue

        if "event" in obj:
            event = obj["event"]
            if event == "started":
                parsed.started = obj
            elif event == "summary":
                parsed.summary = obj
            elif event == "capture.overflow":
                topic = obj.get("topic")
                if isinstance(topic, str):
                    parsed.overflow_topics.append(topic)
            elif event == "kafka.error":
                parsed.errors.append(obj)
        else:
            # No "event" key — check for a startup-error envelope.
            if obj.get("ok") is False:
                parsed.startup_error = obj

    return parsed
