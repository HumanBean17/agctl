"""`logs query/assert` commands (DESIGN §6.2, §6.3).

- ``logs query`` scans logs within a time window, optionally filtering by
  level/logger/message, and returns a paginated result set.
- ``logs assert`` polls for a matching entry and raises an AssertionError if
  no match appears within the timeout (or if ``--not`` is set and a match
  does appear).
"""

from __future__ import annotations

import dataclasses
import datetime
import re
from typing import TYPE_CHECKING

import click

from ..assertions import _parse_iso_datetime, _to_utc, compile_jq
from ..command import envelope, load_config_or_raise
from ..errors import AssertionFailure, ConfigError
from ..params import parse_params
from ..resolution import fill_placeholders

if TYPE_CHECKING:
    from ..config.models import LogSource

__all__ = [
    "logs_query",
    "logs_assert",
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
) -> dict:
    cfg = load_config_or_raise(config_path)
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
@click.pass_context
def logs_query(
    ctx: click.Context,
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
) -> dict:
    cfg = load_config_or_raise(config_path)
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
@click.option("--since", "since", default=None, required=True, help="Start time (ISO-8601 or duration)")
@click.option("--not", "not_", is_flag=True, default=False, help="Invert: fail if a match IS found")
@click.option("--timeout", "timeout", type=float, default=None, help="Poll timeout (seconds); omit for one-shot")
@click.option("--config", "config_path", default=None, help="Path to agctl.yaml")
@click.pass_context
def logs_assert(
    ctx: click.Context,
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
    )


_logs_assert_envelope = envelope("logs.assert")(_logs_assert_core)
