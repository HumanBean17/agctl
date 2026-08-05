"""`logs query/assert/tail` commands (DESIGN §6.2, §6.3, §6.4).

- ``logs query`` scans logs within a time window, optionally filtering by
  level/logger/message, and returns a paginated result set.
- ``logs assert`` polls for a matching entry and raises an AssertionError if
  no match appears within the timeout (or if ``--not`` is set and a match
  does appear).
- ``logs tail`` streams log entries in real-time until stopped (NDJSON,
  signal-driven, summary line) — the streaming exception (D9), mirroring
  ``http ping``.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import re
import signal
import threading
import time
from typing import TYPE_CHECKING, Any, Callable

import click

from ..assertions import _parse_iso_datetime, _to_utc, compile_jq
from ..command import envelope, load_config_or_raise
from ..errors import AgctlError, AssertionFailure, ConfigError
from ..output import emit
from ..params import parse_params
from ..resolution import fill_placeholders

if TYPE_CHECKING:
    from ..config.models import LogSource

__all__ = [
    "logs_query",
    "logs_assert",
    "logs_tail",
    "new_logs_client",
]


def new_logs_client(source: "LogSource"):
    """Build a real :class:`LogClient` from a log source config.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(logs_commands, "new_logs_client", factory)``) to
    return a fake client, avoiding any real file access.
    """
    from ..clients.log_client import LogClient

    return LogClient(source)


_DURATION_RE = re.compile(r"^(\d+)([smh])$")


def _parse_since_until(value: str) -> datetime.datetime:
    """Parse ``value`` to an aware UTC datetime.

    - If it matches ``^(\\d+)([smh])$``, compute ``now(UTC) - duration``.
    - Else if it parses as ISO-8601 (contains ``"T"``), parse and normalize via
      :func:`_to_utc`.
    - Else raise :class:`ConfigError`.
    """
    # Duration form: 30s, 5m, 1h
    m = _DURATION_RE.match(value)
    if m:
        count = int(m.group(1))
        unit = m.group(2)
        now = datetime.datetime.now(datetime.timezone.utc)
        if unit == "s":
            return now - datetime.timedelta(seconds=count)
        if unit == "m":
            return now - datetime.timedelta(minutes=count)
        if unit == "h":
            return now - datetime.timedelta(hours=count)

    # ISO-8601 form
    if "T" in value:
        dt = _parse_iso_datetime(value)
        if dt is not None:
            return _to_utc(dt)

    raise ConfigError(
        f"invalid --since/--until value: {value!r}",
        {"value": value},
    )


def _build_log_filter(
    *,
    level: str | None,
    logger: str | None,
    message: str | None,
    match: str | None,
    params: dict[str, str],
):
    """Build a :class:`LogFilter` from CLI arguments.

    - ``level`` is upper-cased if present.
    - ``match`` is filled with ``params`` and compiled via ``compile_jq`` (raises
      ``ConfigError`` on malformed expression or missing jq).
    """
    from ..clients.log_backend_protocol import LogFilter

    level_norm = level.upper() if level else None
    match_jq: str | None = None
    if match is not None:
        filled = fill_placeholders(match, params)
        try:
            compile_jq(filled, label="logs --match")
        except ConfigError as exc:
            # Rewrite the jq library hint to point at agctl[logs] (D11)
            msg = str(exc)
            if "jq is required" in msg or "pip install" in msg:
                # Extract the expression from the error detail if available
                expr = getattr(exc, "detail", {}).get("expr", filled) if hasattr(exc, "detail") else filled
                raise ConfigError(
                    f"jq is required for logs --match: pip install 'agctl[logs]'",
                    {"expr": expr},
                ) from None
            # Re-raise malformed expression errors as-is
            raise
        match_jq = filled

    return LogFilter(
        level=level_norm,
        logger_glob=logger,
        message_substring=message,
        match_jq=match_jq,
        params=params,
    )


def _resolve_source(cfg, name: str) -> "LogSource":
    """Resolve a log source by name from config."""
    if name not in cfg.logs.sources:
        raise ConfigError(
            f"Unknown logs source: {name}",
            {"source": name},
        )
    return cfg.logs.sources[name]


# --------------------------------------------------------------------------- #
# logs query
# --------------------------------------------------------------------------- #


def _logs_query_core(
    config_path: str | None,
    source: str,
    level: str | None,
    logger: str | None,
    message: str | None,
    match: str | None,
    param: tuple[str, ...],
    since: str | None,
    until: str | None,
    limit: int | None,
    env_file: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, env_file=env_file)
    src = _resolve_source(cfg, source)
    params = parse_params(param)
    filt = _build_log_filter(
        level=level,
        logger=logger,
        message=message,
        match=match,
        params=params,
    )

    since_dt = _parse_since_until(since) if since else None
    until_dt = _parse_since_until(until) if until else datetime.datetime.now(datetime.timezone.utc)

    client = new_logs_client(src)
    res = client.scan(
        filt,
        since=since_dt,
        until=until_dt,
        limit=limit or cfg.logs.defaults.limit,
        tail_lines=cfg.logs.defaults.tail_lines,
    )

    return {
        "source": source,
        "matched": res.matched,
        "scanned": res.scanned,
        "truncated": res.truncated,
        "entries": [dataclasses.asdict(e) for e in res.entries],
    }


@click.command("query")
@click.option("--source", "source", required=True, help="Log source name")
@click.option("--level", "level", default=None, help="Log level (case-insensitive)")
@click.option("--logger", "logger", default=None, help="Logger name glob")
@click.option("--message", "message", default=None, help="Message substring")
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate against canonical entry fields",
)
@click.option("--param", "param", multiple=True, help="k=v placeholder for --match")
@click.option("--since", "since", default=None, help="Start time (ISO-8601 or duration like 30s/5m/1h)")
@click.option("--until", "until", default=None, help="End time (ISO-8601 or duration)")
@click.option("--limit", "limit", type=int, default=None, help="Max entries to return")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def logs_query(
    ctx: click.Context,
    env_file: str | None,
    source: str,
    level: str | None,
    logger: str | None,
    message: str | None,
    match: str | None,
    param: tuple[str, ...],
    since: str | None,
    until: str | None,
    limit: int | None,
    config_path: str | None,
) -> None:
    """Query logs within a time window."""
    # Resolve config_path from click context
    config_path_resolved = config_path
    if ctx.obj and ctx.obj.get("config_path"):
        config_path_resolved = ctx.obj.get("config_path")
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)
    _logs_query_envelope(
        config_path_resolved,
        source,
        level,
        logger,
        message,
        match,
        param,
        since,
        until,
        limit,
        env_file=env_file,
    )


_logs_query_envelope = envelope("logs.query")(_logs_query_core)


# --------------------------------------------------------------------------- #
# logs assert
# --------------------------------------------------------------------------- #


def _logs_assert_core(
    config_path: str | None,
    source: str,
    level: str | None,
    logger: str | None,
    message: str | None,
    match: str | None,
    param: tuple[str, ...],
    since: str | None,
    not_: bool,
    timeout: float | None,
    env_file: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, env_file=env_file)
    src = _resolve_source(cfg, source)
    params = parse_params(param)
    filt = _build_log_filter(
        level=level,
        logger=logger,
        message=message,
        match=match,
        params=params,
    )

    if since is None:
        raise ConfigError(
            "--since is required for logs assert",
            {},
        )

    since_dt = _parse_since_until(since)
    timeout_s = float(timeout) if timeout is not None else 0.0

    client = new_logs_client(src)
    res = client.await_one(
        filt,
        since=since_dt,
        timeout_s=timeout_s,
        poll_interval_ms=cfg.logs.defaults.poll_interval_ms,
        tail_lines=cfg.logs.defaults.tail_lines,
    )

    matched = res.entry is not None
    succeeded = matched if not not_ else (not matched)

    if not succeeded:
        if not_:
            message = "Matching log entry found"
        else:
            message = f"No matching log entry found within {timeout_s}s"

        detail = {
            "source": source,
            "not": bool(not_),
            "filter": {
                "level": filt.level,
                "logger": filt.logger_glob,
                "message": filt.message_substring,
                "match": filt.match_jq,
            },
            "since": since_dt.isoformat(),
            "entries_scanned": res.scanned,
            "elapsed_ms": res.elapsed_ms,
        }
        if res.entry is not None:
            detail["matching_entry"] = dataclasses.asdict(res.entry)

        raise AssertionFailure(message, detail)

    return {
        "source": source,
        "matched": True,
        "matching_entry": dataclasses.asdict(res.entry) if res.entry is not None else None,
        "entries_scanned": res.scanned,
        "elapsed_ms": res.elapsed_ms,
    }


@click.command("assert")
@click.option("--source", "source", required=True, help="Log source name")
@click.option("--level", "level", default=None, help="Log level (case-insensitive)")
@click.option("--logger", "logger", default=None, help="Logger name glob")
@click.option("--message", "message", default=None, help="Message substring")
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate against canonical entry fields",
)
@click.option("--param", "param", multiple=True, help="k=v placeholder for --match")
@click.option("--since", "since", default=None, help="Start time (ISO-8601 or duration)")
@click.option("--not", "not_", is_flag=True, default=False, help="Invert: fail if a match IS found")
@click.option("--timeout", "timeout", type=float, default=None, help="Poll timeout (seconds); omit for one-shot")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def logs_assert(
    ctx: click.Context,
    env_file: str | None,
    source: str,
    level: str | None,
    logger: str | None,
    message: str | None,
    match: str | None,
    param: tuple[str, ...],
    since: str | None,
    not_: bool,
    timeout: float | None,
    config_path: str | None,
) -> None:
    """Assert a matching log entry exists (or not, with --not)."""
    # Resolve config_path from click context
    config_path_resolved = config_path
    if ctx.obj and ctx.obj.get("config_path"):
        config_path_resolved = ctx.obj.get("config_path")
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)
    _logs_assert_envelope(
        config_path_resolved,
        source,
        level,
        logger,
        message,
        match,
        param,
        since,
        not_,
        timeout,
        env_file=env_file,
    )


_logs_assert_envelope = envelope("logs.assert")(_logs_assert_core)


# --------------------------------------------------------------------------- #
# logs tail (DESIGN §6.4, D9 streaming exception)
# --------------------------------------------------------------------------- #


def _emit_stdout_line(line: dict) -> None:
    """Write one NDJSON line directly to stdout (NOT via emit)."""
    import sys

    sys.stdout.write(json.dumps(line, ensure_ascii=False))
    sys.stdout.write("\n")
    sys.stdout.flush()


def _tail_run(
    client,
    filt,
    *,
    stop_event: threading.Event,
    emit_line: Callable[[dict], None],
    poll_interval_ms: int,
) -> tuple[int, int]:
    """Drive the tail streaming loop.

    Calls ``client.follow(filt, stop_event=stop_event)`` and yields each
    entry via ``emit_line`` (as ``dataclasses.asdict(entry)``). Returns
    ``(emitted_count, duration_ms)``. Factored out so the streaming loop
    is testable separately from signal plumbing (mirrors ``_run_pings``).
    """
    start = time.monotonic()
    emitted = 0

    for entry in client.follow(filt, stop_event=stop_event, poll_interval_ms=poll_interval_ms):
        emit_line(dataclasses.asdict(entry))
        emitted += 1

    duration_ms = int((time.monotonic() - start) * 1000)
    return emitted, duration_ms


@click.command("tail")
@click.option("--source", "source", required=True, help="Log source name")
@click.option("--level", "level", default=None, help="Log level (case-insensitive)")
@click.option("--logger", "logger", default=None, help="Logger name glob")
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate against canonical entry fields",
)
@click.option("--param", "param", multiple=True, help="k=v placeholder for --match")
@click.option("--duration", "duration", type=float, default=None, help="Stop after N seconds")
@click.option("--until-stopped", "until_stopped", is_flag=True, default=False)
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.option("--env-file", "env_file", default=None, help="Path to .env file (default: .env next to agctl.yaml)")
@click.pass_context
def logs_tail(
    ctx: click.Context,
    env_file: str | None,
    source: str,
    level: str | None,
    logger: str | None,
    match: str | None,
    param: tuple[str, ...],
    duration: float | None,
    until_stopped: bool,
    config_path: str | None,
) -> None:
    """Stream log entries in real-time (NDJSON, signal-driven stop, summary line)."""
    # Resolve config_path from click context
    config_path_resolved = config_path
    if ctx.obj and ctx.obj.get("config_path"):
        config_path_resolved = ctx.obj.get("config_path")
    env_file = env_file or (ctx.obj.get("env_file") if ctx.obj else None)

    start = time.monotonic()

    if duration is not None and until_stopped:
        # Mutex check (mirrors http_ping)
        emit(
            ok=False,
            command="logs.tail",
            error={
                "type": "ConfigError",
                "message": "--duration and --until-stopped are mutually exclusive",
                "detail": {},
            },
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    try:
        cfg = load_config_or_raise(config_path_resolved, env_file=env_file)
        src = _resolve_source(cfg, source)
        params = parse_params(param)
        filt = _build_log_filter(
            level=level,
            logger=logger,
            message=None,  # Tail takes no --message per spec §6.4
            match=match,
            params=params,
        )
        # Build the client INSIDE the startup try so a validate-time error from
        # ``LogClient.__init__`` -> ``backend.validate_config()`` (e.g. a Loki
        # source missing its ``query``) is caught and emitted as a startup
        # envelope rather than leaking a traceback (DESIGN §10).
        client = new_logs_client(src)
    except AgctlError as err:
        # Startup config errors -> structured envelope + exit code (mirrors http_ping)
        emit(
            ok=False,
            command="logs.tail",
            error=err.to_dict(),
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(err.exit_code)
    except Exception as exc:
        # Non-agctl startup errors -> InternalError envelope + exit 2
        emit(
            ok=False,
            command="logs.tail",
            error={"type": "InternalError", "message": str(exc), "detail": {}},
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    # Signal handling: install SIGTERM/SIGINT handlers that set a stop event.
    # Guard for non-main-thread contexts (degrades gracefully).
    stop_event = threading.Event()
    prev_term = None
    prev_int = None

    def _handler(signum, frame):
        stop_event.set()

    try:
        prev_term = signal.getsignal(signal.SIGTERM)
        prev_int = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)
    except (ValueError, OSError):
        pass

    # If --duration is set, start a daemon timer that sets stop_event after N seconds.
    duration_timer: threading.Timer | None = None
    if duration is not None:
        duration_timer = threading.Timer(duration, stop_event.set)
        duration_timer.daemon = True
        duration_timer.start()

    try:
        emitted, total_ms = _tail_run(
            client,
            filt,
            stop_event=stop_event,
            emit_line=_emit_stdout_line,
            poll_interval_ms=cfg.logs.defaults.poll_interval_ms,
        )
    except AgctlError as err:
        # A streaming error raised inside ``_tail_run`` (notably the FIRST
        # ``follow()`` fetch, which deliberately propagates startup errors --
        # bad auth / unreachable / bad LogQL) must emit a structured
        # ``logs.tail`` envelope + exit code, NOT leak a traceback. Mirrors the
        # startup-error handlers above; one-envelope contract (DESIGN §10).
        emit(
            ok=False,
            command="logs.tail",
            error=err.to_dict(),
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(err.exit_code)
    finally:
        # Restore previous signal handlers (mirrors http_ping).
        try:
            if prev_term is not None:
                signal.signal(signal.SIGTERM, prev_term)
            if prev_int is not None:
                signal.signal(signal.SIGINT, prev_int)
        except (ValueError, OSError):
            pass
        # Clean up duration timer if it was started.
        if duration_timer is not None:
            duration_timer.cancel()

    summary = {
        "summary": True,
        "total_emitted": emitted,
        "duration_ms": total_ms,
    }
    _emit_stdout_line(summary)

    raise SystemExit(0)
