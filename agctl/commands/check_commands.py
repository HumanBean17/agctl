"""`check ready` command (DESIGN §3.4, D7).

Probes one or all configured services by issuing a GET against each service's
health path (or ``<base_url>/`` when ``health_path`` is unset). A 2xx response
means the service is *ready*; connection/timeout failures and non-2xx statuses
mean it is *not ready* — but they are NOT command-level failures: ``check ready``
returns ``ok: true`` overall whenever it successfully ran the probes. Only the
per-service ``ready`` flags reflect health.
"""

from __future__ import annotations

from typing import Any

import click

from ..command import envelope, load_config_or_raise
from ..config.models import Config
from ..errors import ConfigError, ConnectionFailure, OperationTimeout
from .http_commands import new_client, resolve_timeout

__all__ = ["check_ready"]


def _select_services(cfg: Config, service: str | None, all_services: bool) -> list[str]:
    """D7 service selection: --service picks one; otherwise probe all configured."""
    if service is not None and all_services:
        raise ConfigError(
            "Specify either --service or --all, not both",
            {},
        )
    if service is not None:
        if service not in cfg.services:
            raise ConfigError(f"Unknown service: {service}", {"service": service})
        return [service]
    # D7: neither flag (or just --all) means "check all configured services".
    return list(cfg.services.keys())


def _probe_service(cfg: Config, name: str, timeout: float | None) -> dict:
    """Probe a single service; never raises on network failure."""
    svc = cfg.services[name]
    health_path = svc.health_path if svc.health_path else "/"
    effective_timeout = resolve_timeout(
        timeout, svc.timeout_seconds, cfg.defaults.timeout_seconds
    )
    client = new_client(svc.base_url, effective_timeout)
    try:
        result = client.request("GET", health_path)
    except (ConnectionFailure, OperationTimeout) as err:
        full_url = f"{svc.base_url.rstrip('/')}/{health_path.lstrip('/')}"
        return {
            "ready": False,
            "status_code": None,
            "url": full_url,
            "response_time_ms": None,
            "error": err.message,
        }

    status = result["status_code"]
    ready = isinstance(status, int) and 200 <= status < 300
    entry = {
        "ready": ready,
        "status_code": status,
        "url": result["url"],
        "response_time_ms": result["response_time_ms"],
    }
    if not ready:
        entry["error"] = f"Unexpected status {status}"
    return entry


def _check_ready_core(
    config_path: str | None,
    service: str | None,
    all_services: bool,
    timeout: float | None,
    overlay_paths: list[str] | None = None,
) -> dict:
    cfg = load_config_or_raise(config_path, overlay_paths=overlay_paths)
    return _check_ready_with_config(cfg, service, all_services, timeout)


def _check_ready_with_config(
    cfg: Config,
    service: str | None,
    all_services: bool,
    timeout: float | None,
) -> dict:
    names = _select_services(cfg, service, all_services)
    services: dict[str, Any] = {}
    for name in names:
        services[name] = _probe_service(cfg, name, timeout)
    all_ready = all(entry["ready"] for entry in services.values()) if services else True
    return {"services": services, "all_ready": all_ready}


@click.command("ready")
@click.option("--service", "service", default=None, help="Check a single service")
@click.option("--all", "all_services", is_flag=True, default=False, help="Check all services")
@click.option("--timeout", "timeout", type=float, default=None)
@click.option("--config", "config_path", default=None)
@click.pass_context
def check_ready(
    ctx: click.Context,
    service: str | None,
    all_services: bool,
    timeout: float | None,
    config_path: str | None,
) -> None:
    """Probe configured service health endpoints."""
    path = config_path or (ctx.obj.get("config_path") if ctx.obj else None)
    ovs = ctx.obj.get("overlay_paths") if ctx.obj else None
    _check_ready_envelope(path, service, all_services, timeout, overlay_paths=list(ovs) if ovs else None)


_check_ready_envelope = envelope("check.ready")(_check_ready_core)
