"""``agctl kafka listen`` command group + ``run`` foreground streaming command.

This is the listen analog of :mod:`agctl.commands.mock_commands`. ``run`` is the
foreground streaming command: it is the daemon's spawn target (Task 8 will add
``start``/``status``/``stop``) and the cross-platform fallback. Like
:func:`mock_run`, it is NOT wrapped in :func:`~agctl.command.envelope`; instead
it hand-rolls the try/except → emit + :class:`SystemExit` streaming structure:

* a mutual-exclusion guard for ``--duration`` / ``--until-stopped`` emits a
  ConfigError envelope BEFORE any event line;
* startup errors (raised by :meth:`ListenEngine.start`) become a single
  ``{"ok": False, "command": "kafka.listen.run", ...}`` envelope, exit code;
* on a clean start the engine streams NDJSON events to stdout (``started`` →
  per-topic capture/overflow → ``summary``) and the command exits with the
  engine's exit code.

Tasks 8 and 9 will add ``start``/``status``/``stop``/``assert``/``results``/
``messages`` to :data:`kafka_listen_group`; ``__all__`` is intentionally left
open so those names can be appended without churn.
"""

from __future__ import annotations

import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import click

from ..command import envelope, load_config_or_raise
from ..daemon import (
    is_alive,
    read_pidfile,
    remove_pidfile,
    require_posix_daemon,
    spawn_daemon,
    terminate,
    write_pidfile,
)
from ..errors import AgctlError, AssertionFailure, ConfigError, TemplateNotFound
from ..listen.daemon import (
    capture_path,
    events_log_path,
    new_run_id,
    parse_events_log,
    pidfile_path,
    read_expectations,
    resolve_listener_target,
    run_dir,
    write_meta,
)
from ..output import emit
from .kafka_commands import new_kafka_client, resolve_cluster_name

if TYPE_CHECKING:
    from ..clients.kafka_client import KafkaClient
    from ..config.models import Config, KafkaConfig

__all__ = [
    "kafka_listen_group",
    "kafka_listen_run",
    "kafka_listen_start",
    "kafka_listen_status",
    "kafka_listen_stop",
    "new_listen_engine",
    "resolve_subscriptions",
]

# Readiness poll budget (mirrors mock start) + cleanup grace for start abort.
_START_BUDGET_SECONDS: float = 30.0
_START_CLEANUP_GRACE_SECONDS: float = 2.0


# ---------------------------------------------------------------------------
# Test seam: module-level factory defaulting to ListenEngine
# ---------------------------------------------------------------------------


def new_listen_engine(
    *,
    topics: list[str],
    client: "KafkaClient",
    run_id: str,
    group: str,
    cluster: str,
    run_dir: Path,
    capture_match: str | None,
    max_bytes: int,
    duration: float | None,
    until_stopped: bool,
    emit_fn=None,
):
    """Build a :class:`ListenEngine` (test seam — monkeypatched in tests).

    Mirrors :func:`agctl.commands.mock_commands.new_mock_engine`: tests patch
    this attribute to return a fake engine so the streaming command can be driven
    end-to-end without a real broker. ``until_stopped`` is accepted for parity
    with ``new_mock_engine``; :class:`ListenEngine` treats ``duration is None`` as
    run-until-stopped, so the flag is informational and not forwarded.
    """
    from ..listen.engine import ListenEngine

    kwargs: dict = {
        "topics": topics,
        "client": client,
        "run_id": run_id,
        "group": group,
        "cluster": cluster,
        "run_dir": run_dir,
        "capture_match": capture_match,
        "max_bytes": max_bytes,
        "duration": duration,
    }
    if emit_fn is not None:
        kwargs["emit_fn"] = emit_fn
    return ListenEngine(**kwargs)


# ---------------------------------------------------------------------------
# resolve_subscriptions: pure helper (unit-tested)
# ---------------------------------------------------------------------------


def resolve_subscriptions(
    cfg: "Config",
    topics: list[str],
    patterns: list[str],
    capture_match: str | None,
) -> tuple[list[str], str | None, str | None]:
    """Merge ``--topic``/``--pattern``/``--capture-match`` into the engine inputs.

    For each named pattern:

    * look up ``cfg.kafka.patterns[name]`` (missing → :class:`TemplateNotFound`
      pointing at ``kafka.patterns.<name>``);
    * append the pattern's ``topic`` to the topic list;
    * if ``capture_match`` is unset AND the pattern has a ``match``, adopt the
      pattern's ``match`` as the capture filter (the FIRST pattern with a match
      wins; an explicit ``--capture-match`` always wins over every pattern);
    * if ``binding_cluster`` is unset AND the pattern has a ``cluster``, adopt
      the pattern's ``cluster`` as the binding (an explicit ``--cluster`` still
      wins via :func:`resolve_cluster_name`).

    The ``topics`` list is de-duplicated preserving first-seen order so a bare
    ``--topic`` that matches a pattern's topic collapses to one entry.

    Returns:
        ``(topics_out, effective_capture_match, binding_cluster)``.
    """
    topics_out: list[str] = list(topics)
    effective_capture_match = capture_match
    binding_cluster: str | None = None

    for name in patterns:
        if name not in cfg.kafka.patterns:
            raise TemplateNotFound(
                f"Unknown kafka pattern: {name}",
                {"path": f"kafka.patterns.{name}"},
            )
        pat = cfg.kafka.patterns[name]
        topics_out.append(pat.topic)
        if effective_capture_match is None and pat.match is not None:
            effective_capture_match = pat.match
        if binding_cluster is None and pat.cluster is not None:
            binding_cluster = pat.cluster

    # De-dup preserving order (a bare --topic matching a pattern's topic).
    seen: set[str] = set()
    deduped: list[str] = []
    for topic in topics_out:
        if topic in seen:
            continue
        seen.add(topic)
        deduped.append(topic)

    return deduped, effective_capture_match, binding_cluster


# ---------------------------------------------------------------------------
# kafka listen group
# ---------------------------------------------------------------------------


@click.group(name="listen")
def kafka_listen_group() -> None:
    """Kafka long-lived capture listener."""


# ---------------------------------------------------------------------------
# kafka listen run (foreground streaming)
# ---------------------------------------------------------------------------


@click.command("run")
@click.option("--topic", "topics", multiple=True, help="Kafka topic to capture (repeatable)")
@click.option("--pattern", "patterns", multiple=True, help="Named kafka pattern (repeatable; contributes its topic/match/cluster)")
@click.option("--cluster", "cluster", default=None, help="Cluster name override")
@click.option("--capture-match", "capture_match", default=None, help="jq predicate for capture filtering (overrides a pattern's match)")
@click.option(
    "--max-bytes-per-topic",
    "max_bytes",
    type=int,
    default=268435456,
    help="Per-topic capture file byte ceiling (0 disables the overflow valve)",
)
@click.option("--duration", "duration", type=float, default=None, help="Stop after N seconds")
@click.option("--until-stopped", "until_stopped", is_flag=True, default=False, help="Run until stopped (mutually exclusive with --duration)")
@click.option("--run-id", "run_id_arg", default=None, help="Run id (default: generated)")
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for listen state (run dirs, capture files)")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def kafka_listen_run(
    ctx: click.Context,
    topics: tuple[str, ...],
    patterns: tuple[str, ...],
    cluster: str | None,
    capture_match: str | None,
    max_bytes: int,
    duration: float | None,
    until_stopped: bool,
    run_id_arg: str | None,
    state_dir: str,
    config_path: str | None,
    env_file: str | None,
) -> None:
    """Run the listener in the FOREGROUND, streaming NDJSON events to stdout.

    The daemon spawn target (Task 8 ``start`` reuses this command) and the
    cross-platform fallback. Streams one JSON object per event: ``started`` →
    per-topic capture/overflow → ``summary``. Exit code is the engine's
    (1 if any ``kafka.error`` occurred, else 0).
    """
    # Fall back to ctx.obj globals (same pattern as mock_run).
    if config_path is None:
        config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)

    start = time.monotonic()

    # Guard: --duration and --until-stopped are mutually exclusive (mirrors mock_run).
    if duration is not None and until_stopped:
        emit(
            ok=False,
            command="kafka.listen.run",
            error={
                "type": "ConfigError",
                "message": "--duration and --until-stopped are mutually exclusive",
                "detail": {},
            },
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    try:
        cfg = load_config_or_raise(
            config_path,
            overlay_paths=list(ovs) if ovs else None,
            env_file=env_file,
        )

        # Resolve the topic/match/cluster subscription from --topic/--pattern.
        topics_out, effective_capture_match, binding_cluster = resolve_subscriptions(
            cfg,
            topics=list(topics),
            patterns=list(patterns),
            capture_match=capture_match,
        )
        if not topics_out:
            raise ConfigError(
                "kafka listen run requires at least one --topic or --pattern",
                {},
            )

        # Resolve the cluster: --cluster (explicit) > pattern.cluster (binding) >
        # default > single-cluster.
        name = resolve_cluster_name(
            cfg.kafka, explicit=cluster, binding_cluster=binding_cluster
        )
        client = new_kafka_client(cfg.kafka.clusters[name])

        # Run id + per-run state directory + consumer group.
        run_id = run_id_arg or new_run_id()
        group = f"agctl-listen-{run_id}"
        rdir = run_dir(Path(state_dir), run_id)
        rdir.mkdir(parents=True, exist_ok=True)

        engine = new_listen_engine(
            topics=topics_out,
            client=client,
            run_id=run_id,
            group=group,
            cluster=name,
            run_dir=rdir,
            capture_match=effective_capture_match,
            max_bytes=max_bytes,
            duration=duration,
            until_stopped=(duration is None),
        )
        engine.start()

    except AgctlError as err:
        # Startup errors → structured envelope + exit code, BEFORE any event line.
        emit(
            ok=False,
            command="kafka.listen.run",
            error=err.to_dict(),
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(err.exit_code)
    except Exception as exc:
        # Non-agctl startup errors → InternalError envelope + exit 2.
        emit(
            ok=False,
            command="kafka.listen.run",
            error={"type": "InternalError", "message": str(exc), "detail": {}},
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    # Run the engine (blocks until stop); ensure shutdown always runs so the
    # ``summary`` line is emitted even when run() raises.
    try:
        code = engine.run()
    finally:
        engine.shutdown()

    raise SystemExit(code)


# ---------------------------------------------------------------------------
# kafka listen start (managed daemon)
# ---------------------------------------------------------------------------


def _kafka_listen_start_core(
    config_path: str | None,
    topics: list[str],
    patterns: list[str],
    cluster: str | None,
    capture_match: str | None,
    max_bytes: int,
    state_dir: str,
    overlay_paths: list[str] | None = None,
    env_file: str | None = None,
) -> dict:
    """Core logic for ``kafka listen start`` (Task 8).

    Spawns a detached ``kafka listen run`` daemon (run-id-keyed), writes the
    pidfile + ``meta.json``, and readiness-polls ``events.log`` for the
    ``started`` event. Mirrors :func:`agctl.commands.mock_commands._mock_start_core`.

    Returns:
        Dict with keys: pid, run_id, state_dir, topics, group, cluster,
        started_at.

    Raises:
        ConfigError: If already running, no topic resolved, startup error, or
            the daemon did not become ready within the budget.
    """
    require_posix_daemon()

    cfg = load_config_or_raise(
        config_path, overlay_paths=overlay_paths, env_file=env_file
    )

    topics_out, eff_capture_match, binding_cluster = resolve_subscriptions(
        cfg,
        topics=topics,
        patterns=patterns,
        capture_match=capture_match,
    )
    if not topics_out:
        raise ConfigError(
            "kafka listen start requires at least one --topic or --pattern",
            {},
        )

    name = resolve_cluster_name(
        cfg.kafka, explicit=cluster, binding_cluster=binding_cluster
    )

    run_id = new_run_id()
    group = f"agctl-listen-{run_id}"
    state_path = Path(state_dir)
    pid = pidfile_path(state_path, run_id)
    rdir = run_dir(state_path, run_id)
    logp = events_log_path(rdir)

    # Already-running pre-check (run-id-keyed pidfile + liveness).
    existing = read_pidfile(pid)
    if existing is not None:
        existing_pid = existing.get("pid")
        if existing_pid is not None and is_alive(existing_pid):
            raise ConfigError(
                "listener already running; run 'agctl kafka listen stop' first",
                {"run_id": run_id, "pid": existing_pid},
            )

    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )

    # meta.json (also creates the run dir so spawn_daemon can write events.log).
    write_meta(
        rdir,
        {
            "run_id": run_id,
            "topics": topics_out,
            "group": group,
            "cluster": name,
            "started_at": started_at,
            "capture_match": eff_capture_match,
            "max_bytes_per_topic": max_bytes,
        },
    )

    # Build the daemon argv: global flags (absolute paths) BEFORE the subcommand,
    # exactly as _mock_start_core does; then ``kafka listen run`` + listen opts.
    daemon_argv: list[str] = []
    if config_path is not None:
        daemon_argv.extend(["--config", str(Path(config_path).absolute())])
    if overlay_paths is not None:
        for ov in overlay_paths:
            daemon_argv.extend(["--overlay", str(Path(ov).absolute())])
    if env_file is not None:
        daemon_argv.extend(["--env-file", str(Path(env_file).absolute())])
    daemon_argv.extend(["kafka", "listen", "run", "--run-id", run_id])
    daemon_argv.extend(["--state-dir", str(state_path.absolute())])
    for topic in topics_out:
        daemon_argv.extend(["--topic", topic])
    daemon_argv.extend(["--cluster", name])
    if eff_capture_match is not None:
        daemon_argv.extend(["--capture-match", eff_capture_match])
    daemon_argv.extend(["--max-bytes-per-topic", str(max_bytes)])

    child_pid = spawn_daemon(daemon_argv, str(logp))

    write_pidfile(
        pid,
        {
            "pid": child_pid,
            "run_id": run_id,
            "topics": topics_out,
            "group": group,
            "cluster": name,
            "started_at": started_at,
            "state_dir": str(state_path),
            "log_path": str(logp),
        },
    )

    # Readiness poll: wait for ``started`` or bail on startup_error / timeout.
    start_time = time.monotonic()
    started_event: dict | None = None
    try:
        while True:
            elapsed = time.monotonic() - start_time
            if elapsed > _START_BUDGET_SECONDS:
                raise ConfigError(
                    f"listener did not become ready within {_START_BUDGET_SECONDS}s",
                    {"pid": child_pid, "log_path": str(logp)},
                )
            parsed = parse_events_log(logp)
            if parsed.started is not None:
                started_event = parsed.started
                break
            if parsed.startup_error is not None:
                error = parsed.startup_error.get("error", {})
                message = error.get("message", "startup failed")
                detail = error.get("detail", {})
                raise ConfigError(message, detail)
            time.sleep(0.05)
    except Exception:
        # Cleanup on any error: terminate the child + drop the pidfile.
        terminate(child_pid, _START_CLEANUP_GRACE_SECONDS)
        remove_pidfile(pid)
        raise

    return {
        "pid": child_pid,
        "run_id": run_id,
        "state_dir": str(state_path),
        "topics": topics_out,
        "group": group,
        "cluster": name,
        "started_at": (started_event or {}).get("started_at", started_at),
    }


@click.command("start")
@click.option("--topic", "topics", multiple=True, help="Kafka topic to capture (repeatable)")
@click.option("--pattern", "patterns", multiple=True, help="Named kafka pattern (repeatable; contributes its topic/match/cluster)")
@click.option("--cluster", "cluster", default=None, help="Cluster name override")
@click.option("--capture-match", "capture_match", default=None, help="jq predicate for capture filtering (overrides a pattern's match)")
@click.option(
    "--max-bytes-per-topic",
    "max_bytes",
    type=int,
    default=268435456,
    help="Per-topic capture file byte ceiling (0 disables the overflow valve)",
)
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for listen state (run dirs, capture files)")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def kafka_listen_start(
    ctx: click.Context,
    topics: tuple[str, ...],
    patterns: tuple[str, ...],
    cluster: str | None,
    capture_match: str | None,
    max_bytes: int,
    state_dir: str,
    config_path: str | None,
    env_file: str | None,
) -> None:
    """Start a detached ``kafka listen`` capture daemon (run-id-keyed)."""
    if config_path is None:
        config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)

    _kafka_listen_start_envelope(
        config_path,
        topics=list(topics),
        patterns=list(patterns),
        cluster=cluster,
        capture_match=capture_match,
        max_bytes=max_bytes,
        state_dir=state_dir,
        overlay_paths=list(ovs) if ovs else None,
        env_file=env_file,
    )


_kafka_listen_start_envelope = envelope("kafka.listen.start")(_kafka_listen_start_core)


# ---------------------------------------------------------------------------
# kafka listen status (managed daemon)
# ---------------------------------------------------------------------------


def _kafka_listen_status_core(
    run_id: str | None,
    pid: int | None,
    state_dir: str,
) -> dict:
    """Core logic for ``kafka listen status`` (Task 8).

    Live snapshot of a running listener: per-topic captured line count + byte
    size + overflow flag, plus uptime. Never signals the daemon and never
    removes the pidfile.

    Returns:
        ``{"running": False}`` when nothing is running, else
        ``{"running": True, "pid", "run_id", "uptime_ms", "topics": [...]}``.
    """
    require_posix_daemon()
    state_path = Path(state_dir)

    targets = resolve_listener_target(state_path, run_id=run_id, pid=pid, all_=False)
    if not targets:
        return {"running": False}

    t = targets[0]
    parsed = parse_events_log(Path(t.log_path))

    rdir = run_dir(Path(t.state_dir), t.run_id)
    overflow_set = set(parsed.overflow_topics)
    topic_rows: list[dict] = []
    for topic in t.topics:
        cp = capture_path(rdir, topic)
        captured = 0
        size = 0
        if cp.exists():
            try:
                captured = sum(
                    1 for line in cp.read_text().splitlines() if line.strip()
                )
                size = cp.stat().st_size
            except OSError:
                captured = 0
                size = 0
        topic_rows.append(
            {
                "topic": topic,
                "captured": captured,
                "bytes": size,
                "overflowed": topic in overflow_set,
            }
        )

    uptime_ms = None
    try:
        started_at_str = t.started_at
        if started_at_str.endswith("Z"):
            started_at_str = started_at_str.replace("Z", "+00:00")
        started_at_dt = datetime.fromisoformat(started_at_str)
        uptime_ms = int(
            (datetime.now(timezone.utc) - started_at_dt).total_seconds() * 1000
        )
    except (ValueError, TypeError):
        uptime_ms = None

    return {
        "running": True,
        "pid": t.pid,
        "run_id": t.run_id,
        "uptime_ms": uptime_ms,
        "topics": topic_rows,
    }


@click.command("status")
@click.option("--run-id", "run_id", default=None, help="Run id selector")
@click.option("--pid", "pid", type=int, default=None, help="Process id selector")
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for listen state (run dirs, capture files)")
@click.pass_context
def kafka_listen_status(
    ctx: click.Context,
    run_id: str | None,
    pid: int | None,
    state_dir: str,
) -> None:
    """Show live status of a running ``kafka listen`` daemon (no signal)."""
    _kafka_listen_status_envelope(run_id, pid, state_dir)


_kafka_listen_status_envelope = envelope("kafka.listen.status")(_kafka_listen_status_core)


# ---------------------------------------------------------------------------
# kafka listen stop (managed daemon)
# ---------------------------------------------------------------------------


def _kafka_listen_stop_core(
    run_id: str | None,
    pid: int | None,
    all_: bool,
    timeout: float,
    state_dir: str,
) -> dict:
    """Core logic for ``kafka listen stop`` (Task 8).

    Stops running listener(s), parses ``events.log`` for the verdict, and removes
    the run dir + pidfile. Fatal failures — ``kafka.error`` events, or
    ``capture.overflow`` on a topic with an attached expectation — raise
    :class:`AssertionFailure`. Cleanup (rmtree + remove pidfile) always runs.

    Returns:
        Single target: ``{"stopped": True, "pid", "signal", "summary",
        "cleaned": True, "failures": [...]}``; ``--all``: ``{"stopped": [...]}``;
        not-running: ``{"stopped": False}`` (or ``{"stopped": []}`` for ``--all``).
    """
    require_posix_daemon()
    state_path = Path(state_dir)

    targets = resolve_listener_target(
        state_path, run_id=run_id, pid=pid, all_=all_
    )
    if not targets:
        if all_:
            return {"stopped": []}
        return {"stopped": False}

    verdicts: list[dict] = []
    for t in targets:
        sig = terminate(t.pid, timeout)
        parsed = parse_events_log(Path(t.log_path))

        # Read expectations BEFORE deleting the run dir: overflow on an asserted
        # topic is fatal; overflow on an un-asserted topic is informational.
        rdir = run_dir(Path(t.state_dir), t.run_id)
        asserted_topics = {
            exp.get("topic")
            for exp in read_expectations(rdir)
            if exp.get("topic")
        }

        failures: list[dict] = list(parsed.errors)
        for ov_topic in parsed.overflow_topics:
            if ov_topic in asserted_topics:
                failures.append({"event": "capture.overflow", "topic": ov_topic})

        # Cleanup always runs (rmtree + remove pidfile), even on fatal failures.
        shutil.rmtree(rdir, ignore_errors=True)
        remove_pidfile(t.pidfile_path)

        verdicts.append(
            {
                "stopped": True,
                "pid": t.pid,
                "signal": sig,
                "summary": parsed.summary or {},
                "cleaned": True,
                "failures": failures,
            }
        )

    if all_:
        bad = [v for v in verdicts if v["failures"]]
        if bad:
            raise AssertionFailure(
                f"{len(bad)} of {len(verdicts)} listener(s) had fatal failures",
                {"stopped": verdicts},
            )
        return {"stopped": verdicts}

    verdict = verdicts[0]
    if verdict["failures"]:
        raise AssertionFailure(
            f"kafka listen run had {len(verdict['failures'])} fatal failure event(s)",
            verdict,
        )
    return verdict


@click.command("stop")
@click.option("--run-id", "run_id", default=None, help="Run id selector")
@click.option("--pid", "pid", type=int, default=None, help="Process id selector")
@click.option("--all", "all_", is_flag=True, default=False, help="Stop all running listeners")
@click.option("--timeout", "timeout", type=float, default=10.0, help="Seconds to wait for SIGTERM before SIGKILL")
@click.option("--state-dir", "state_dir", default="./.agctl", help="Directory for listen state (run dirs, capture files)")
@click.pass_context
def kafka_listen_stop(
    ctx: click.Context,
    run_id: str | None,
    pid: int | None,
    all_: bool,
    timeout: float,
    state_dir: str,
) -> None:
    """Stop a running ``kafka listen`` daemon with SIGTERM/SIGKILL and parse verdict."""
    _kafka_listen_stop_envelope(run_id, pid, all_, timeout, state_dir)


_kafka_listen_stop_envelope = envelope("kafka.listen.stop")(_kafka_listen_stop_core)


# ---------------------------------------------------------------------------
# Command registration
# ---------------------------------------------------------------------------

# Register ``run`` (foreground streaming) + the managed-daemon trio on the
# ``listen`` group. Task 9 will add assert/results/messages alongside these.
kafka_listen_group.add_command(kafka_listen_run)
kafka_listen_group.add_command(kafka_listen_start)
kafka_listen_group.add_command(kafka_listen_status)
kafka_listen_group.add_command(kafka_listen_stop)
