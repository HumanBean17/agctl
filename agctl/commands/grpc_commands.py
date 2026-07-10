"""`grpc call` command (DESIGN §3.3, D6).

- ``grpc call <template>`` resolves a named gRPC template from config, fills
  ``{name}`` placeholders, and dispatches a unary or client-streaming call.
- ``grpc call --target <name>`` / ``--address host:port`` is the free-form variant.

Both unary and client-streaming return a single result (the ``grpc.call`` envelope).
Server-streaming and bidirectional streaming emit NDJSON (D9 streaming exception).

A non-OK gRPC status is a *successful* call and yields ``ok: true`` — status is a
result field, not an assertion failure (D6). Assertions (``--status``, ``--match``,
etc.) are evaluated separately and raise ``AssertionFailure`` on mismatch.
"""

from __future__ import annotations

import json
import signal
import sys
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

import click

from ..assertions import compile_jq, evaluate_grpc_assertions, jq_bool, validate_grpc_assertion_args
from ..clients.grpc_client import GrpcStatus
from ..command import envelope, load_config_or_raise
from ..errors import AgctlError, AssertionFailure, ConfigError, TemplateNotFound
from ..output import emit
from ..params import parse_params
from ..resolution import deep_merge, fill_placeholders

if TYPE_CHECKING:
    from ..config.models import GrpcConfig, GrpcTarget, GrpcTemplate

__all__ = [
    "grpc_call",
    "grpc_healthcheck",
    "new_grpc_client",
]


def new_grpc_client(target, *, descriptors=None):
    """Build a real :class:`GrpcClient` from a :class:`GrpcTarget`.

    Test seam: tests monkeypatch this attribute
    (``monkeypatch.setattr(grpc_commands, "new_grpc_client", factory)``) to
    return a fake GrpcClient, avoiding any real gRPC connection.
    """
    from ..clients.grpc_client import GrpcClient

    return GrpcClient(target, descriptors=descriptors)


def _resolve_target(cfg, name: str | None, address: str | None) -> tuple["GrpcTarget", str]:
    """Resolve a gRPC target from either a named target or an address.

    Args:
        cfg: Loaded config object.
        name: Target name from ``--target``.
        address: Raw address string from ``--address`` (``host:port`` format).

    Returns:
        Tuple of ``(GrpcTarget, resolved_name)`` where ``resolved_name`` is the
        address string if ``--address`` was used, otherwise the target name.

    Raises:
        ConfigError: If both ``--address`` and ``--target`` are given, if
            address format is invalid, if the target is unknown, or if neither
            is provided.
    """
    if address is not None:
        if name is not None:
            raise ConfigError("--address is mutually exclusive with --target", {})
        # Validate host:port format (single colon, non-empty host and port)
        if address.count(":") != 1:
            raise ConfigError(f"--address must be host:port: {address!r}", {"address": address})
        host, port = address.split(":", 1)
        if not host or not port:
            raise ConfigError(f"--address must be host:port: {address!r}", {"address": address})
        from ..config.models import GrpcTarget

        return GrpcTarget(address=address), address
    elif name is not None:
        grpc_cfg: "GrpcConfig" = cfg.grpc
        if name not in grpc_cfg.targets:
            raise ConfigError(f"Unknown gRPC target: {name}", {"target": name})
        return grpc_cfg.targets[name], name
    else:
        raise ConfigError("grpc call requires --target <name> or --address host:port", {})


def _parse_metadata(metadata: tuple[str, ...]) -> dict[str, str]:
    """Turn ``--metadata k=v`` tuples into a dict (same rule as ``--param``)."""
    return parse_params(metadata)


def _stdin_request_iter(params: dict[str, str]) -> Callable[[], dict]:
    """NDJSON iterator over stdin, filling placeholders per line.

    Yields:
        Parsed JSON dicts with placeholders filled from ``params``.

    Raises:
        ConfigError: If a line is not valid JSON.
    """

    def _iter():
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue  # Skip empty lines
            try:
                msg_dict = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ConfigError(
                    f"Invalid NDJSON on stdin: {exc.msg}",
                    {"line": line, "lineno": getattr(exc, "lineno", None)},
                ) from exc
            yield fill_placeholders(msg_dict, params)

    return _iter()


def _emit_stdout_line(line: dict) -> None:
    """Write one NDJSON line directly to stdout (NOT via emit)."""
    sys.stdout.write(json.dumps(line, ensure_ascii=False))
    sys.stdout.write("\n")
    sys.stdout.flush()


@dataclass
class _ResolvedGrpcCall:
    """Fully resolved gRPC call context (produced exactly once per invocation).

    ``grpc_call`` resolves the target/template/client/method/call-type a single
    time and hands this context to BOTH the unary/client-stream path
    (``_grpc_call_core``) and the streaming path — neither re-resolves (no second
    ``load_config``/``new_grpc_client``/``find_method``), which matters for
    ``reflection: auto`` targets where each ``find_method`` is a reflection
    round-trip and each client build constructs a channel.
    """

    call_type: str
    client: Any  # GrpcClient (or a test double)
    service: str | None
    method: str | None
    message: str | None  # resolved JSON request string (None for stdin-input call types)
    metadata: tuple[str, ...]
    params: dict[str, str]


def _resolve_grpc_call(
    cfg,
    *,
    target_name: str | None,
    address: str | None,
    service: str | None,
    method: str | None,
    template_name: str | None = None,
    caller_message: str | None = None,
    caller_metadata: tuple[str, ...] = (),
    param: tuple[str, ...] = (),
) -> _ResolvedGrpcCall:
    """Resolve a gRPC call EXACTLY ONCE: template/free-form, target, client,
    method descriptor, and call type.

    Returns a :class:`_ResolvedGrpcCall` carrying the client + resolved
    service/method/message/metadata/params. Raises :class:`ConfigError` /
    :class:`TemplateNotFound` for config errors.
    """
    resolved_target: str | None = target_name
    resolved_address: str | None = address
    resolved_service: str | None = service
    resolved_method: str | None = method
    resolved_message: str | None = caller_message
    resolved_metadata: tuple[str, ...] = caller_metadata

    # Mode resolution: template vs free-form
    if template_name is not None:
        # Template mode: --target/--address/--service/--method are mutually exclusive
        if target_name is not None or address is not None or service is not None or method is not None:
            raise ConfigError(
                "grpc call <template> is mutually exclusive with --target/--address/--service/--method",
                {},
            )

        if template_name not in cfg.grpc.templates:
            raise TemplateNotFound(
                f"Unknown gRPC template: {template_name}",
                {"name": template_name},
            )
        tpl: "GrpcTemplate" = cfg.grpc.templates[template_name]

        # Resolve template target
        tpl_target_name = tpl.target
        if tpl_target_name not in cfg.grpc.targets:
            raise ConfigError(
                f"Template '{template_name}' references unknown target '{tpl_target_name}'",
                {"target": tpl_target_name},
            )

        # Use template's service/method
        resolved_service = tpl.service
        resolved_method = tpl.method

        # Fill placeholders in template message
        params = parse_params(param)
        tpl_message = tpl.message or {}
        filled_message = fill_placeholders(tpl_message, params)

        # Merge caller --message if provided (caller wins, deep merge)
        if caller_message is not None:
            try:
                caller_msg_dict = json.loads(caller_message)
            except json.JSONDecodeError as exc:
                raise ConfigError(
                    f"Invalid JSON in --message: {exc.msg}",
                    {"message": caller_message},
                ) from exc
            resolved_message_dict = deep_merge(filled_message, caller_msg_dict)
        else:
            resolved_message_dict = filled_message

        # Fill metadata placeholders
        tpl_metadata = tpl.metadata or {}
        filled_metadata = fill_placeholders(dict(tpl_metadata), params)

        # Overlay caller --metadata (caller wins)
        caller_metadata_dict = _parse_metadata(caller_metadata)
        resolved_metadata_dict = {**filled_metadata, **caller_metadata_dict}

        # Serialize message back to JSON string
        resolved_message = json.dumps(resolved_message_dict) if resolved_message_dict else None

        # Convert metadata back to tuples
        resolved_metadata = tuple(f"{k}={v}" for k, v in resolved_metadata_dict.items())

        # Set target name for resolution
        resolved_target = tpl_target_name
        resolved_address = None
    else:
        # Free-form mode: service/method are required
        if service is None or method is None:
            raise ConfigError(
                "grpc call requires --service and --method (or a template name)",
                {},
            )
        params = parse_params(param)

    # Resolve target, build the client ONCE, find method + call type ONCE
    final_target, _ = _resolve_target(cfg, resolved_target, resolved_address)
    client = new_grpc_client(final_target, descriptors=cfg.grpc.descriptors)
    md = client.find_method(resolved_service, resolved_method)
    call_type = client.call_type_of(md)

    return _ResolvedGrpcCall(
        call_type=call_type,
        client=client,
        service=resolved_service,
        method=resolved_method,
        message=resolved_message,
        metadata=resolved_metadata,
        params=params,
    )


def _grpc_stream_run(
    client,
    *,
    service: str,
    method: str,
    call_type: str,
    request_input: dict | Callable[[], dict],
    metadata: dict | None,
    timeout: float | None,
    match: str | None,
    params: dict[str, str],
    stop_event: threading.Event,
    emit_line: Callable[[dict], None],
    stats: dict[str, int],
) -> dict:
    """Drive a streaming gRPC call (server-streaming or bidi).

    Args:
        client: GrpcClient instance.
        service: Service name.
        method: Method name.
        call_type: "server_stream" or "bidi".
        request_input: For server_stream, a single dict message; for bidi, an
            iterator (from ``_stdin_request_iter``) of request dicts.
        metadata: Metadata dict.
        timeout: Timeout in seconds.
        match: Compiled jq expression for filtering messages (optional).
        params: Placeholder values for filling match expression.
        stop_event: Threading event to stop streaming.
        emit_line: Function to emit NDJSON lines.
        stats: Mutable ``{"messages": int, "matched": int}`` updated as messages
            stream, so a mid-run error can report the PARTIAL counts emitted so
            far rather than dropping them to 0.

    Returns:
        Summary dict with keys: summary (True), messages (int), matched (int), status (dict), duration_ms (int).
    """
    start = time.monotonic()

    # Build the match filter if provided
    match_filter = None
    if match is not None:
        filled_match = fill_placeholders(match, params)

        def match_filter(msg_dict: dict) -> bool:
            return jq_bool(msg_dict, filled_match)

    # Drive the appropriate call type.
    # request_input is always a dict (server_stream) or a generator (bidi,
    # from _stdin_request_iter) — never a callable, so no call() branch.
    if call_type == "server_stream":
        stream_iter = client.call_server_stream(service, method, request_input, metadata=metadata, timeout=timeout)
    elif call_type == "bidi":
        stream_iter = client.call_bidi(service, method, request_input, metadata=metadata, timeout=timeout)
    else:
        raise ConfigError(f"Unknown call type for streaming: {call_type}", {"call_type": call_type})

    # Stream messages
    for stream_msg in stream_iter:
        if stop_event.is_set():
            break

        stats["messages"] += 1
        msg_dict = stream_msg.message

        # Apply match filter if provided
        if match_filter is not None:
            try:
                if match_filter(msg_dict):
                    stats["matched"] += 1
                    emit_line({"event": "message", "message": msg_dict, "trailers": stream_msg.trailers})
            except Exception:
                # If match evaluation fails, count as not matched
                pass
        else:
            # No filter: all messages match
            stats["matched"] += 1
            emit_line({"event": "message", "message": msg_dict, "trailers": stream_msg.trailers})

    duration_ms = int((time.monotonic() - start) * 1000)

    # Read the terminal status from the client
    terminal = getattr(client, "terminal_status", None) or GrpcStatus(0, "OK", "")

    # Return summary with the terminal status
    return {
        "summary": True,
        "messages": stats["messages"],
        "matched": stats["matched"],
        "status": {"code": terminal.code, "name": terminal.name, "message": terminal.message},
        "duration_ms": duration_ms,
    }


def _grpc_healthcheck_core(
    config_path: str | None,
    target: str | None,
    service: str | None,
    all_: bool,
    overlay_paths: list[str] | None = None,
) -> dict:
    """Core gRPC healthcheck logic.

    Args:
        config_path: Path to config file.
        target: Named gRPC target from config.
        service: Optional service name to check.
        all_: If True, check all targets.
        overlay_paths: Optional config overlay paths.

    Returns:
        Dict with keys: targets (dict of target results), all_serving (bool).

    Raises:
        ConfigError: If target is unknown.
    """
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)

    # Select targets: if all_ OR neither target given → all targets; else [target]
    if all_ or target is None:
        selected_targets = list(cfg.grpc.targets.keys())
    else:
        selected_targets = [target]

    # Validate targets exist
    for target_name in selected_targets:
        if target_name not in cfg.grpc.targets:
            raise ConfigError(f"Unknown gRPC target: {target_name}", {"target": target_name})

    # Check health for each target
    targets_result = {}
    for target_name in selected_targets:
        target_obj = cfg.grpc.targets[target_name]
        client = new_grpc_client(target_obj, descriptors=cfg.grpc.descriptors)
        res = client.healthcheck(service or "")

        # Build result dict for this target
        target_dict = {
            "address": res.address,
            "status": res.status,
        }
        if res.note is not None:
            target_dict["note"] = res.note
        targets_result[target_name] = target_dict

    # Compute all_serving: True only if all statuses are SERVING
    all_serving = all(result["status"] == "SERVING" for result in targets_result.values())

    return {
        "targets": targets_result,
        "all_serving": all_serving,
    }


_grpc_healthcheck_envelope = envelope("grpc.healthcheck")(_grpc_healthcheck_core)


@click.command("healthcheck")
@click.option("--target", "target", default=None, help="Named gRPC target from config")
@click.option("--service", "service", default=None, help="Service name to check (empty string checks overall health)")
@click.option("--all", "all_", is_flag=True, help="Check all configured gRPC targets")
@click.pass_context
def grpc_healthcheck(
    ctx: click.Context,
    target: str | None,
    service: str | None,
    all_: bool,
) -> None:
    """Check health of gRPC services.

    Without --target or --all, checks all configured targets.
    With --target, checks only the specified target.
    With --all, checks all configured targets explicitly.

    Examples:
        agctl grpc healthcheck                    # Check all targets
        agctl grpc healthcheck --all              # Check all targets (explicit)
        agctl grpc healthcheck --target echo       # Check specific target
        agctl grpc healthcheck --target echo --service echo.Echo  # Check specific service
    """
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None

    _grpc_healthcheck_envelope(
        config_path,
        target,
        service,
        all_,
        overlay_paths=list(ovs) if ovs else None,
    )


def _grpc_call_core(
    resolved: _ResolvedGrpcCall,
    *,
    timeout: float | None,
    status: str | None,
    contains: str | None,
    match: str | None,
    jq_path: str | None,
    equals: str | None,
) -> dict:
    """Core gRPC call logic for unary + client-streaming (single-result) paths.

    Receives an ALREADY-RESOLVED context (``resolved``) produced once by
    :func:`_resolve_grpc_call` — this function does NOT reload config, rebuild
    the client, or re-run ``find_method``/``call_type_of``. The Click command
    routes server-streaming/bidi to the streaming branch before this is called,
    so only unary/client_stream reach here.

    Raises:
        ConfigError: For invalid JSON in ``--message`` or a streaming-input call
            type given ``--message``.
        AssertionFailure: When assertions fail (via ``evaluate_grpc_assertions``).
    """
    call_type = resolved.call_type
    client = resolved.client

    # Build message_json based on call type
    if call_type == "unary":
        if resolved.message is not None:
            try:
                message_json = json.loads(resolved.message)
            except json.JSONDecodeError as exc:
                raise ConfigError(
                    f"Invalid JSON in --message: {exc.msg}",
                    {"message": resolved.message},
                ) from exc
            message_json = fill_placeholders(message_json, resolved.params)
        else:
            message_json = {}
    elif call_type == "client_stream":
        # Client-streaming reads from stdin; --message is invalid
        if resolved.message is not None:
            raise ConfigError(
                "--message is invalid for streaming-input call types; pipe request messages on stdin",
                {"call_type": call_type},
            )
        message_json = None
    else:
        # Defensive: the Click command routes server_stream/bidi to the streaming
        # branch, so this is unreachable in normal flow.
        raise ConfigError(f"Unsupported call type for single-result path: {call_type}", {"call_type": call_type})

    # Pre-call assertion validation (fail fast before the call)
    validate_grpc_assertion_args(status=status, contains=contains, match=match, jq_path=jq_path, equals=equals)

    metadata_dict = _parse_metadata(resolved.metadata) if resolved.metadata else None

    # Dispatch on call type
    if call_type == "unary":
        result = client.call_unary(resolved.service, resolved.method, message_json, metadata=metadata_dict, timeout=timeout)
    else:  # client_stream
        result = client.call_client_stream(
            resolved.service, resolved.method, _stdin_request_iter(resolved.params), metadata=metadata_dict, timeout=timeout
        )

    # Build the grpc.call result dict
    result_dict = {
        "target": result.target,
        "service": result.service,
        "method": result.method,
        "call_type": result.call_type,
        "status": {
            "code": result.status.code,
            "name": result.status.name,
            "message": result.status.message,
        },
        "message": result.message,
        "initial_metadata": result.initial_metadata,
        "trailers": result.trailers,
    }

    # Post-call assertions (only when any assertion kwarg is set)
    if any(arg is not None for arg in (status, contains, match, jq_path, equals)):
        evaluate_grpc_assertions(result_dict, status=status, contains=contains, match=match, jq_path=jq_path, equals=equals)

    return result_dict


_grpc_call_envelope = envelope("grpc.call")(_grpc_call_core)


@click.command("call")
@click.argument("template_name", required=False)
@click.option("--target", "target", default=None, help="Named gRPC target from config")
@click.option("--address", "address", default=None, help="Raw gRPC address (host:port)")
@click.option("--service", "service", default=None, help="Fully-qualified service name (e.g. echo.Echo)")
@click.option("--method", "method", default=None, help="Method name within the service")
@click.option("--message", "message", default=None, help="JSON request message body")
@click.option("--metadata", "metadata", multiple=True, help="k=v metadata header")
@click.option("--param", "param", multiple=True, help="k=v placeholder")
@click.option("--timeout", "timeout", type=float, default=None, help="Request timeout (seconds)")
@click.option("--status", "status", type=str, default=None, help="Expected gRPC status code (name or number)")
@click.option("--contains", "contains", default=None, help="JSON needle that must be present in the response (subset match)")
@click.option("--match", "match", default=None, help="jq predicate against the result envelope")
@click.option("--jq-path", "jq_path", default=None, help="jq path expression (used with --equals)")
@click.option("--equals", "equals", default=None, help="Expected value for --jq-path")
@click.option("--expect-count", "expect_count", type=int, default=None, help="Expected number of matching messages (for streaming calls)")
@click.pass_context
def grpc_call(
    ctx: click.Context,
    template_name: str | None,
    target: str | None,
    address: str | None,
    service: str | None,
    method: str | None,
    message: str | None,
    metadata: tuple[str, ...],
    param: tuple[str, ...],
    timeout: float | None,
    status: str | None,
    contains: str | None,
    match: str | None,
    jq_path: str | None,
    equals: str | None,
    expect_count: int | None,
) -> None:
    """Make a gRPC call (unary, client-streaming, server-streaming, or bidirectional).

    Either invoke a template:

        agctl grpc call <template> --param name=value

    Or provide free-form arguments:

        agctl grpc call --target <name> --service <Service> --method <Method> --message '{"msg":"hello"}'
        agctl grpc call --address host:port --service <Service> --method <Method> --message '{"msg":"hello"}'

    Client-streaming and bidirectional calls read NDJSON from stdin (one JSON object per line).
    Server-streaming and bidirectional calls emit NDJSON to stdout (one message per line).
    """
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    start = time.monotonic()

    # Step 1: Resolve EXACTLY ONCE — config load, target/template resolution,
    # client build, find_method, and call-type detection all happen here. The
    # resulting context is handed to both the single-result path
    # (``_grpc_call_core``) and the streaming path; neither re-resolves.
    try:
        cfg = load_config_or_raise(config_path, overlay_paths=list(ovs) if ovs else None)
        resolved = _resolve_grpc_call(
            cfg,
            target_name=target,
            address=address,
            service=service,
            method=method,
            template_name=template_name,
            caller_message=message,
            caller_metadata=metadata,
            param=param,
        )
    except AgctlError as err:
        # Startup config errors -> structured envelope + exit code
        emit(
            ok=False,
            command="grpc.call",
            error=err.to_dict(),
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(err.exit_code)
    except Exception as exc:
        # Non-agctl startup errors -> InternalError envelope + exit 2
        emit(
            ok=False,
            command="grpc.call",
            error={"type": "InternalError", "message": str(exc), "detail": {}},
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)

    call_type = resolved.call_type

    # Step 2: Route based on call type
    if call_type in ("unary", "client_stream"):
        # Unary and client-streaming use the envelope path (single JSON result)
        _grpc_call_envelope(
            resolved,
            timeout=timeout,
            status=status,
            contains=contains,
            match=match,
            jq_path=jq_path,
            equals=equals,
        )
    elif call_type in ("server_stream", "bidi"):
        # Server-streaming and bidi use the streaming path (NDJSON output),
        # reusing the already-built client (no second new_grpc_client).
        # Input validation
        if call_type == "server_stream":
            # Server-streaming requires a single request message
            if resolved.message is None:
                emit(
                    ok=False,
                    command="grpc.call",
                    error={"type": "ConfigError", "message": "Server-streaming requires --message", "detail": {}},
                    duration_ms=int((time.monotonic() - start) * 1000),
                )
                raise SystemExit(2)
        else:  # bidi
            # Bidi reads from stdin; --message is invalid
            if resolved.message is not None:
                emit(
                    ok=False,
                    command="grpc.call",
                    error={"type": "ConfigError", "message": "--message is invalid for bidi; pipe request messages on stdin", "detail": {}},
                    duration_ms=int((time.monotonic() - start) * 1000),
                )
                raise SystemExit(2)

        # Reject unary assertion flags on streaming calls
        if any(arg is not None for arg in (status, contains, jq_path, equals)):
            emit(
                ok=False,
                command="grpc.call",
                error={
                    "type": "ConfigError",
                    "message": "Unary assertion flags (--status, --contains, --jq-path, --equals) are not allowed on streaming calls",
                    "detail": {},
                },
                duration_ms=int((time.monotonic() - start) * 1000),
            )
            raise SystemExit(2)

        # Validate and compile --match if provided
        compiled_match = None
        if match is not None:
            filled_match = fill_placeholders(match, resolved.params)
            try:
                compile_jq(filled_match, label="grpc --match")
                compiled_match = filled_match
            except ConfigError as exc:
                emit(
                    ok=False,
                    command="grpc.call",
                    error=exc.to_dict(),
                    duration_ms=int((time.monotonic() - start) * 1000),
                )
                raise SystemExit(exc.exit_code)

        # Build request input
        if call_type == "server_stream":
            # Parse the single message
            try:
                request_input = json.loads(resolved.message) if resolved.message else {}
                request_input = fill_placeholders(request_input, resolved.params)
            except json.JSONDecodeError as exc:
                emit(
                    ok=False,
                    command="grpc.call",
                    error={"type": "ConfigError", "message": f"Invalid JSON in --message: {exc.msg}", "detail": {"message": resolved.message}},
                    duration_ms=int((time.monotonic() - start) * 1000),
                )
                raise SystemExit(2)
        else:  # bidi
            # Bidi uses stdin iterator
            request_input = _stdin_request_iter(resolved.params)

        # Build metadata dict
        metadata_dict = _parse_metadata(resolved.metadata) if resolved.metadata else None

        # Install signal handlers for graceful shutdown
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

        # Partial counters shared with _grpc_stream_run so a mid-run error can
        # report the counts actually emitted so far (M1: no dropped counts).
        stats = {"messages": 0, "matched": 0}

        try:
            # Run the streaming call on the already-built client
            summary = _grpc_stream_run(
                resolved.client,
                service=resolved.service,
                method=resolved.method,
                call_type=call_type,
                request_input=request_input,
                metadata=metadata_dict,
                timeout=timeout,
                match=compiled_match,
                params=resolved.params,
                stop_event=stop_event,
                emit_line=_emit_stdout_line,
                stats=stats,
            )

            # Emit summary
            _emit_stdout_line(summary)

            # Exit based on expect_count
            if expect_count is not None and summary["matched"] < expect_count:
                raise SystemExit(1)
            else:
                raise SystemExit(0)
        except AgctlError as err:
            # Emit final error summary line with PARTIAL counts (M1) and a valid
            # gRPC status code (no invented -1).
            _emit_stdout_line({
                "error": err.to_dict(),
                "summary": True,
                "messages": stats["messages"],
                "matched": stats["matched"],
                "status": {"code": 2, "name": "UNKNOWN", "message": err.message},
                "duration_ms": int((time.monotonic() - start) * 1000),
            })
            raise SystemExit(err.exit_code)
        except Exception as exc:
            # Internal error - emit structured error with PARTIAL counts (M1)
            # and a valid gRPC status code.
            _emit_stdout_line({
                "error": {
                    "type": "InternalError",
                    "message": str(exc),
                    "detail": {},
                },
                "summary": True,
                "messages": stats["messages"],
                "matched": stats["matched"],
                "status": {"code": 2, "name": "UNKNOWN", "message": str(exc)},
                "duration_ms": int((time.monotonic() - start) * 1000),
            })
            raise SystemExit(2)
        finally:
            # Restore previous signal handlers
            try:
                if prev_term is not None:
                    signal.signal(signal.SIGTERM, prev_term)
                if prev_int is not None:
                    signal.signal(signal.SIGINT, prev_int)
            except (ValueError, OSError):
                pass
    else:
        # Unknown call type (shouldn't happen)
        emit(
            ok=False,
            command="grpc.call",
            error={"type": "ConfigError", "message": f"Unknown call type: {call_type}", "detail": {"call_type": call_type}},
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        raise SystemExit(2)
