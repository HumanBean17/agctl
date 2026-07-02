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
from typing import Any

import click

from ..command import envelope, load_config_or_raise
from ..errors import ConfigError, TemplateMissing
from ..params import parse_params
from ..resolution import deep_merge, fill_placeholders

__all__ = ["http_call", "http_request", "resolve_timeout", "set_default_transport"]

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
) -> dict:
    cfg = load_config_or_raise(config_path)

    if template_name not in cfg.templates:
        raise TemplateMissing(
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

    client = new_client(service.base_url, effective_timeout)
    return client.request(tpl.method, path, headers=resolved_headers, body=resolved_body)


@click.command("call")
@click.argument("template_name")
@click.option("--param", "param", multiple=True, help="k=v path/body placeholder")
@click.option("--body", "body", default=None, help="JSON body (merged over template)")
@click.option("--header", "header", multiple=True, help="k=v header override")
@click.option("--timeout", "timeout", type=float, default=None)
@click.pass_context
def http_call(
    ctx: click.Context,
    template_name: str,
    param: tuple[str, ...],
    body: str | None,
    header: tuple[str, ...],
    timeout: float | None,
) -> None:
    """Resolve and send a named HTTP template (DESIGN §3.1)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _http_call_envelope(config_path, template_name, param, body, header, timeout)


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

    client = new_client(service_cfg.base_url, effective_timeout)
    return client.request(
        method, path, headers=resolved_headers or None, body=resolved_body
    )


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
@click.pass_context
def http_request(
    ctx: click.Context,
    service: str,
    method: str,
    path: str,
    body: str | None,
    header: tuple[str, ...],
    timeout: float | None,
) -> None:
    """Send a free-form HTTP request against a configured service (DESIGN §3.1)."""
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _http_request_envelope(config_path, service, method, path, body, header, timeout)


_http_request_envelope = envelope("http.request")(_http_request_core)
