"""`http call` and `http request` commands (DESIGN §3.1, D5 body-merge).

- ``http call <template>`` resolves a named HTTP template from config, fills
  ``{name}`` placeholders, merges any ``--body``/``--header`` overrides (D5),
  and dispatches the request through :class:`HttpClient`.
- ``http request`` is the free-form variant: caller supplies service, method,
  path, body, headers directly.

Both commands are wrapped in the success/error :func:`envelope`. A 4xx/5xx HTTP
response is a *successful* request and yields ``ok: true`` — HTTP status is a
result, not an assertion failure.
"""

from __future__ import annotations

import json
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

import click

from ..assertions import evaluate_http_assertions, validate_http_assertion_args
from ..command import envelope, load_config_or_raise
from ..errors import AgctlError, ConfigError, TemplateNotFound
from ..output import emit
from ..params import parse_params
from ..resolution import deep_merge, fill_placeholders

__all__ = [
    "http_call",
    "http_request",
    "http_ping",
    "ping_loop",
    "resolve_timeout",
    "set_default_transport",
]

# Test seam: tests inject an ``httpx.MockTransport`` here without touching the
# network. ``None`` (the default) means "use the real transport".
_default_transport: Any = None


def set_default_transport(transport: Any) -> None:
    """Inject the transport used by :func:`new_client` (test seam)."""
    global _default_transport
    _default_transport = transport


def new_client(base_url: str, timeout: float, headers: dict | None = None):
    """Build an :class:`HttpClient` bound to the active (mock or real) transport."""
    from ..clients.http_client import HttpClient

    return HttpClient(base_url, timeout, transport=_default_transport, headers=headers)


def resolve_timeout(
    cli_timeout: float | None,
    service_timeout: float | None,
    defaults_timeout: float | None,
) -> float:
    """First non-None of (cli, service, defaults, 10s); coerced to float."""
    for candidate in (cli_timeout, service_timeout, defaults_timeout, 10):
        if candidate is not None:
            return float(candidate)
    return 10.0  # pragma: no cover - unreachable given the literal 10 above


def _parse_headers(values: tuple[str, ...]) -> dict[str, str]:
    """Turn ``--header k=v`` tuples into a dict (same rule as ``--param``)."""
    return parse_params(values)


# --------------------------------------------------------------------------- #
# http call
# --------------------------------------------------------------------------- #


def _http_call_core(
    config_path: str | None,
    template_name: str,
    param: tuple[str, ...],
    body: str | None,
    header: tuple[str, ...],
    timeout: float | None,
    status: int | None = None,
    contains: str | None = None,
    match: str | None = None,
    jq_path: str | None = None,
    equals: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path)

    if template_name not in cfg.templates:
        raise TemplateNotFound(
            f"Unknown HTTP template: {template_name}", {"name": template_name}
        )
    tpl = cfg.templates[template_name]

    if tpl.service not in cfg.services:
        raise ConfigError(
            f"Template '{template_name}' references unknown service '{tpl.service}'",
            {"service": tpl.service},
        )

    params = parse_params(param)

    path = fill_placeholders(tpl.path, params)

    # Body: start from the (filled) template body, then D5-merge --body on top.
    filled_body = fill_placeholders(tpl.body, params)
    if body is not None:
        caller_body = json.loads(body)
        if filled_body is None:
            resolved_body = caller_body
        else:
            resolved_body = deep_merge(filled_body, caller_body)
    else:
        resolved_body = filled_body

    # Headers: fill template headers, then overlay caller --header (caller wins).
    base_headers = fill_placeholders(dict(tpl.headers), params)
    caller_headers = _parse_headers(header)
    resolved_headers = {**base_headers, **caller_headers}

    service = cfg.services[tpl.service]
    effective_timeout = resolve_timeout(
        timeout, service.timeout_seconds, cfg.defaults.timeout_seconds
    )

    # Pre-request gate: fail pairing/bad-JSON misuse BEFORE the request is sent
    # so no wasted side-effect (load-bearing for the validate/evaluate split).
    validate_http_assertion_args(
        status=status,
        contains=contains,
        match=match,
        jq_path=jq_path,
        equals=equals,
    )

    client = new_client(service.base_url, effective_timeout)
    result = client.request(
        tpl.method, path, headers=resolved_headers, body=resolved_body
    )
    evaluate_http_assertions(
        result,
        status=status,
        contains=contains,
        match=match,
        jq_path=jq_path,
        equals=equals,
    )
    return result


@click.command("call")
@click.argument("template_name")
@click.option("--param", "param", multiple=True, help="k=v path/body placeholder")
@click.option("--body", "body", default=None, help="JSON body (merged over template)")
@click.option("--header", "header", multiple=True, help="k=v header override")
@click.option("--timeout", "timeout", type=float, default=None)
@click.option("--status", "status", type=int, default=None, help="Expected HTTP status code")
@click.option(
    "--contains",
    "contains",
    default=None,
    help="JSON needle that must be present in the response body (subset match)",
)
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate that must evaluate truthy against the response body",
)
@click.option(
    "--jq-path",
    "jq_path",
    default=None,
    help="jq path expression (used with --equals) to extract a value from the body",
)
@click.option(
    "--equals",
    "equals",
    default=None,
    help="Expected value for --jq-path (type-aware comparison; paired with --jq-path)",
)
@click.pass_context
def http_call(
    ctx: click.Context,
    template_name: str,
    param: tuple[str, ...],
    body: str | None,
    header: tuple[str, ...],
    timeout: float | None,
    status: int | None,
    contains: str | None,
    match: str | None,
    jq_path: str | None,
    equals: str | None,
) -> None:
    """Resolve and send a named HTTP template."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _http_call_envelope(
        config_path,
        template_name,
        param,
        body,
        header,
        timeout,
        status,
        contains,
        match,
        jq_path,
        equals,
    )


_http_call_envelope = envelope("http.call")(_http_call_core)


# --------------------------------------------------------------------------- #
# http request
# --------------------------------------------------------------------------- #


def _http_request_core(
    config_path: str | None,
    service: str,
    method: str,
    path: str,
    body: str | None,
    header: tuple[str, ...],
    timeout: float | None,
    status: int | None = None,
    contains: str | None = None,
    match: str | None = None,
    jq_path: str | None = None,
    equals: str | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path)

    if service not in cfg.services:
        raise ConfigError(
            f"Unknown service: {service}", {"service": service}
        )

    resolved_body = json.loads(body) if body is not None else None
    resolved_headers = _parse_headers(header)

    service_cfg = cfg.services[service]
    effective_timeout = resolve_timeout(
        timeout, service_cfg.timeout_seconds, cfg.defaults.timeout_seconds
    )

    # Pre-request gate: fail pairing/bad-JSON misuse BEFORE the request is sent
    # so no wasted side-effect (load-bearing for the validate/evaluate split).
    validate_http_assertion_args(
        status=status,
        contains=contains,
        match=match,
        jq_path=jq_path,
        equals=equals,
    )

    client = new_client(service_cfg.base_url, effective_timeout)
    result = client.request(
        method, path, headers=resolved_headers or None, body=resolved_body
    )
    evaluate_http_assertions(
        result,
        status=status,
        contains=contains,
        match=match,
        jq_path=jq_path,
        equals=equals,
    )
    return result


@click.command("request")
@click.option("--service", "service", required=True)
@click.option(
    "--method",
    "method",
    required=True,
    type=click.Choice(["GET", "POST", "PUT", "PATCH", "DELETE"]),
)
@click.option("--path", "path", required=True)
@click.option("--body", "body", default=None, help="JSON body")
@click.option("--header", "header", multiple=True, help="k=v header")
@click.option("--timeout", "timeout", type=float, default=None)
@click.option("--status", "status", type=int, default=None, help="Expected HTTP status code")
@click.option(
    "--contains",
    "contains",
    default=None,
    help="JSON needle that must be present in the response body (subset match)",
)
@click.option(
    "--match",
    "match",
    default=None,
    help="jq predicate that must evaluate truthy against the response body",
)
@click.option(
    "--jq-path",
    "jq_path",
    default=None,
    help="jq path expression (used with --equals) to extract a value from the body",
)
@click.option(
    "--equals",
    "equals",
    default=None,
    help="Expected value for --jq-path (type-aware comparison; paired with --jq-path)",
)
@click.pass_context
def http_request(
    ctx: click.Context,
    service: str,
    method: str,
    path: str,
    body: str | None,
    header: tuple[str, ...],
    timeout: float | None,
    status: int | None,
    contains: str | None,
    match: str | None,
    jq_path: str | None,
    equals: str | None,
) -> None:
    """Send a free-form HTTP request against a configured service."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _http_request_envelope(
        config_path,
        service,
        method,
        path,
        body,
        header,
        timeout,
        status,
        contains,
        match,
        jq_path,
        equals,
    )


_http_request_envelope = envelope("http.request")(_http_request_core)


# --------------------------------------------------------------------------- #
# http ping  (DESIGN §3.1, M5 streaming exception)
# --------------------------------------------------------------------------- #


def _emit_stdout_line(line: dict) -> None:
    """Write one NDJSON ping/summary line directly to stdout (NOT via emit)."""
    import sys

    sys.stdout.write(json.dumps(line))
    sys.stdout.write("\n")
    sys.stdout.flush()


def ping_loop(
    send_one: Callable[[int], dict],
    *,
    interval: float,
    max_pings: int | None = None,
    stop_event: threading.Event | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    duration: float | None = None,
    emit_line: Callable[[dict], None] = _emit_stdout_line,
) -> tuple[list[dict], int]:
    """Drive the ping loop.

    ``send_one(i)`` performs a single request and returns the per-ping line
    dict (already including ``ping: i``). Between pings the loop sleeps
    ``interval`` seconds via ``sleep_fn``. It stops when:

    - ``max_pings`` pings have been sent (if set), OR
    - ``duration`` wall-clock seconds have elapsed (if set), OR
    - ``stop_event`` is set (e.g. by a signal handler).

    Each produced line is passed to ``emit_line`` as it happens (streaming).
    Returns ``(ping_lines, total_duration_ms)``.
    """
    start = monotonic()
    ping_lines: list[dict] = []
    i = 0
    while True:
        i += 1
        line = send_one(i)
        ping_lines.append(line)
        emit_line(line)

        # Stop conditions evaluated after each ping.
        if max_pings is not None and i >= max_pings:
            break
        if stop_event is not None and stop_event.is_set():
            break
        if duration is not None and (monotonic() - start) >= duration:
            break

        # Sleep between pings. When a stop_event is wired (the signal-handler
        # path), wait on it so a SIGTERM/SIGINT received DURING the sleep is
        # acted on promptly (DESIGN §3.1: emit the summary line without blocking
        # up to a full interval). Without a stop_event, fall back to the
        # injectable sleep_fn (the test seam).
        if stop_event is not None:
            if stop_event.wait(interval):
                break  # event set during the wait -> stop promptly
        else:
            sleep_fn(interval)

    total_ms = int((monotonic() - start) * 1000)
    return ping_lines, total_ms


def _resolve_ping_request(
    config_path: str | None,
    template_name: str | None,
    service: str | None,
    path: str | None,
    method: str | None,
    body: str | None,
    header: tuple[str, ...],
    param: tuple[str, ...],
    timeout: float | None,
):
    """Resolve the request components for a ping (template OR free-form).

    Returns ``(client, method, path, headers, body_dict_or_None)``.
    """
    cfg = load_config_or_raise(config_path)

    if template_name is not None:
        if template_name not in cfg.templates:
            raise TemplateNotFound(
                f"Unknown HTTP template: {template_name}", {"name": template_name}
            )
        tpl = cfg.templates[template_name]
        if tpl.service not in cfg.services:
            raise ConfigError(
                f"Template '{template_name}' references unknown service '{tpl.service}'",
                {"service": tpl.service},
            )

        params = parse_params(param)
        resolved_path = fill_placeholders(tpl.path, params)

        filled_body = fill_placeholders(tpl.body, params)
        if body is not None:
            caller_body = json.loads(body)
            resolved_body = (
                caller_body if filled_body is None else deep_merge(filled_body, caller_body)
            )
        else:
            resolved_body = filled_body

        base_headers = fill_placeholders(dict(tpl.headers), params)
        caller_headers = _parse_headers(header)
        resolved_headers = {**base_headers, **caller_headers}

        resolved_method = method or tpl.method
        svc = cfg.services[tpl.service]
    else:
        if not service or not path:
            raise ConfigError(
                "http ping requires either a template name or --service + --path",
                {},
            )
        if service not in cfg.services:
            raise ConfigError(f"Unknown service: {service}", {"service": service})

        resolved_body = json.loads(body) if body is not None else None
        resolved_headers = _parse_headers(header)
        resolved_path = path
        resolved_method = method or "GET"
        svc = cfg.services[service]

    effective_timeout = resolve_timeout(
        timeout, svc.timeout_seconds, cfg.defaults.timeout_seconds
    )
    client = new_client(svc.base_url, effective_timeout)
    return client, resolved_method, resolved_path, resolved_headers, resolved_body


def _run_pings(send_one, *, emit_line=_emit_stdout_line, **loop_kwargs):
    """Default loop runner used by the Click command.

    Runs :func:`ping_loop` (which streams each ping line via ``emit_line``) and
    returns ``(total_pings, failed_pings, total_duration_ms)``. The final
    summary line is emitted by the Click command, NOT here, so test fakes only
    need to emit ping lines and return counts.
    """
    ping_lines, total_ms = ping_loop(
        send_one, emit_line=emit_line, **loop_kwargs
    )
    failed = sum(1 for p in ping_lines if not p.get("ok"))
    return len(ping_lines), failed, total_ms


@click.command("ping")
@click.argument("template_name", required=False)
@click.option("--service", "service", default=None)
@click.option("--path", "path", default=None)
@click.option("--interval", "interval", type=float, required=True, help="Seconds between pings")
@click.option(
    "--method",
    "method",
    type=click.Choice(["GET", "POST", "PUT", "PATCH", "DELETE"]),
    default=None,
)
@click.option("--body", "body", default=None, help="JSON body")
@click.option("--header", "header", multiple=True, help="k=v header")
@click.option("--duration", "duration", type=float, default=None, help="Stop after N seconds")
@click.option("--until-stopped", "until_stopped", is_flag=True, default=False)
@click.option("--timeout", "timeout", type=float, default=None, help="Per-request timeout")
@click.option("--param", "param", multiple=True, help="k=v placeholder")
@click.pass_context
def http_ping(
    ctx: click.Context,
    template_name: str | None,
    service: str | None,
    path: str | None,
    interval: float,
    method: str | None,
    body: str | None,
    header: tuple[str, ...],
    duration: float | None,
    until_stopped: bool,
    timeout: float | None,
    param: tuple[str, ...],
) -> None:
    """Repeatedly send an HTTP request, streaming NDJSON ping lines."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    start = time.monotonic()

    if duration is not None and until_stopped:
        # http ping is not wrapped in @envelope, so emit the error envelope directly.
        emit(
            ok=False,
            command="http.ping",
            error={
                "type": "ConfigError",
                "message": "--duration and --until-stopped are mutually exclusive",
                "detail": {},
            },
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    try:
        client, rmethod, rpath, rheaders, rbody = _resolve_ping_request(
            config_path,
            template_name,
            service,
            path,
            method,
            body,
            header,
            param,
            timeout,
        )
    except AgctlError as err:
        # Startup config/template errors (ConfigError, TemplateNotFound) -> structured
        # envelope + exit code, before any ping line is streamed.
        emit(ok=False, command="http.ping", error=err.to_dict(),
             duration_ms=int((time.monotonic() - start) * 1000))
        raise SystemExit(err.exit_code)
    except Exception as exc:
        # Non-agctl startup errors (e.g. malformed ``--body`` JSON, which
        # json.loads raises as ValueError) -> InternalError envelope + exit 2,
        # mirroring the @envelope fallback so http ping never leaks a raw
        # traceback like its sibling commands do not.
        emit(
            ok=False,
            command="http.ping",
            error={"type": "InternalError", "message": str(exc), "detail": {}},
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    def send_one(i: int) -> dict:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        req_start = time.monotonic()
        try:
            result = client.request(
                rmethod, rpath, headers=rheaders or None, body=rbody
            )
        except Exception as exc:  # connection/timeout failure for this ping
            elapsed = int((time.monotonic() - req_start) * 1000)
            return {
                "ping": i,
                "ok": False,
                "status_code": None,
                "duration_ms": elapsed,
                "timestamp": ts,
                "error": str(exc),
            }
        elapsed = int((time.monotonic() - req_start) * 1000)
        status = result["status_code"]
        ok = 200 <= status < 300
        line = {
            "ping": i,
            "ok": ok,
            "status_code": status,
            "duration_ms": elapsed,
            "timestamp": ts,
        }
        if not ok:
            line["error"] = f"Unexpected status {status}"
        return line

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

    loop_kwargs: dict[str, Any] = {
        "interval": interval,
        "stop_event": stop_event,
        "sleep_fn": time.sleep,
    }
    if duration is not None:
        loop_kwargs["duration"] = duration
    # If neither duration nor until-stopped is given, until-stopped is implied:
    # the loop runs until a signal flips stop_event (no max_pings set).

    try:
        total_pings, failed, total_ms = _run_pings(
            send_one, emit_line=_emit_stdout_line, **loop_kwargs
        )
    finally:
        # Restore previous signal handlers.
        try:
            if prev_term is not None:
                signal.signal(signal.SIGTERM, prev_term)
            if prev_int is not None:
                signal.signal(signal.SIGINT, prev_int)
        except (ValueError, OSError):
            pass

    summary = {
        "summary": True,
        "total_pings": total_pings,
        "failed_pings": failed,
        "duration_ms": total_ms,
    }
    _emit_stdout_line(summary)

    raise SystemExit(0 if failed == 0 else 1)
