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

import time
from pathlib import Path
from typing import TYPE_CHECKING

import click

from ..command import load_config_or_raise
from ..errors import AgctlError, ConfigError, TemplateNotFound
from ..listen.daemon import new_run_id, run_dir
from ..output import emit
from .kafka_commands import new_kafka_client, resolve_cluster_name

if TYPE_CHECKING:
    from ..clients.kafka_client import KafkaClient
    from ..config.models import Config, KafkaConfig

__all__ = [
    "kafka_listen_group",
    "kafka_listen_run",
    "new_listen_engine",
    "resolve_subscriptions",
]
# NOTE: Tasks 8/9 will append start/status/stop/assert/results/messages here.


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


# Register ``run`` on the ``listen`` group. Tasks 8/9 will add
# start/status/stop/assert/results/messages alongside this registration.
kafka_listen_group.add_command(kafka_listen_run)
