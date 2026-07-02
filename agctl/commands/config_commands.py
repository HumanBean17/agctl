"""`agctl config validate` and `agctl config show` commands (DESIGN §3.5).

- ``validate`` loads the fully-resolved config and reports schema / cross-reference
  / plugin errors (exit 2 on any). It also folds in each loaded protocol plugin's
  own ``validate_config`` (DESIGN §9.2).
- ``show`` dumps the resolved config as JSON with secret-looking values masked
  (unless ``--unmask``).

These commands are plain ``@click.command``s registered onto the ``config`` group
in :mod:`agctl.cli`. The list of loaded plugins is injected from
:mod:`agctl.cli` via :func:`set_plugins_provider`: :mod:`agctl.cli` owns plugin
loading (``_load_plugins`` / ``_LOADED_PLUGINS``), and hands config validation a
thunk that reads that module global at call time. This keeps the dependency
one-directional (``cli → config_commands``) and avoids a circular import.
"""

from __future__ import annotations

import time
from typing import Any, Callable

import click

from ..config import ConfigError, load_config
from ..config.validator import validate_config
from ..output import emit

__all__ = ["config_validate", "config_show", "set_plugins_provider"]

# --- masking ---------------------------------------------------------------

_SECRET_FRAGMENTS = ("password", "token", "secret")


def _is_secret(key: str) -> bool:
    lowered = key.lower()
    if any(frag in lowered for frag in _SECRET_FRAGMENTS):
        return True
    # "key" is too broad as a substring (would mask non-secret paths like
    # kafka.ssl.key_location, or names like key_id). Treat it as a secret only
    # when it names the key itself: a bare ``key`` or an ``*_key`` suffix.
    return lowered == "key" or lowered.endswith("_key")


def _mask(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("***" if _is_secret(k) and v else _mask(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mask(v) for v in obj]
    return obj


# --- plugin-validation bridge (injected from agctl.cli) --------------------

#: Yields the plugins loaded by ``agctl.cli._load_plugins``. Set by
#: :func:`set_plugins_provider` (called from :mod:`agctl.cli`) so this module
#: never imports ``cli``. Defaults to "no plugins" until injected.
_plugins_provider: Callable[[], list] = lambda: []


def set_plugins_provider(fn: Callable[[], list]) -> None:
    """Inject the callable that returns the currently-loaded protocol plugins.

    ``agctl.cli`` passes a thunk that reads its ``_LOADED_PLUGINS`` module global
    at call time, so validation always sees the live list — even after
    :func:`agctl.cli._load_plugins` reassigns that global to a new list.
    """
    global _plugins_provider
    _plugins_provider = fn


def _plugin_validation_errors(plugins: list, config_dict: dict) -> list[dict]:
    """Ask each loaded plugin to validate its own config section (DESIGN §9.2).

    Each plugin's ``validate_config(config_dict)`` (if present) returns a list of
    human-readable error strings. Those are folded into ``{path, message}``
    error records under ``plugin.<name>``. A plugin that raises is isolated: its
    error becomes a single record rather than crashing ``config validate``.
    """
    errors: list[dict] = []
    for plugin in plugins:
        validate = getattr(plugin, "validate_config", None)
        if not callable(validate):
            continue
        plugin_name = getattr(plugin, "name", None) or "unknown"
        try:
            returned = validate(config_dict) or []
        except Exception as exc:  # noqa: BLE001 - plugin isolation
            errors.append(
                {"path": f"plugin.{plugin_name}", "message": f"plugin raised: {exc}"}
            )
            continue
        for msg in returned:
            errors.append({"path": f"plugin.{plugin_name}", "message": str(msg)})
    return errors


# --- shared helpers --------------------------------------------------------


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def _emit_config_error(command: str, err: ConfigError, start: float) -> None:
    errors = [{"message": err.message, **(err.detail or {})}]
    emit(
        ok=False,
        command=command,
        result={"valid": False, "errors": errors},
        error={"type": "ConfigError", "message": err.message, "detail": err.detail},
        duration_ms=_ms(start),
    )


# --- config validate -------------------------------------------------------


@click.command("validate")
@click.option("--config", "config_path", default=None)
@click.pass_context
def config_validate(ctx: click.Context, config_path: str | None) -> None:
    """Parse and validate agctl.yaml (DESIGN §3.5). Exit 2 on any error."""
    start = time.monotonic()
    path = config_path or ctx.obj.get("config_path")
    try:
        cfg = load_config(path)
    except ConfigError as err:
        _emit_config_error("config.validate", err, start)
        raise SystemExit(2)
    errors, warnings = validate_config(cfg)
    # Let loaded plugins validate their own config sections (DESIGN §9.2).
    errors = errors + _plugin_validation_errors(_plugins_provider(), cfg.model_dump())
    if errors:
        summary = f"Configuration has {len(errors)} error(s)"
        emit(
            ok=False,
            command="config.validate",
            result={"valid": False, "errors": errors, "warnings": warnings},
            error={
                "type": "ConfigError",
                "message": summary,
                "detail": {"errors": errors, "warnings": warnings},
            },
            duration_ms=_ms(start),
        )
        raise SystemExit(2)
    emit(
        ok=True,
        command="config.validate",
        result={"valid": True, "warnings": warnings},
        duration_ms=_ms(start),
    )


# --- config show -----------------------------------------------------------


@click.command("show")
@click.option("--config", "config_path", default=None)
@click.option("--unmask", is_flag=True, default=False)
@click.pass_context
def config_show(ctx: click.Context, config_path: str | None, unmask: bool) -> None:
    """Dump the resolved config as JSON, secrets masked (DESIGN §3.5)."""
    start = time.monotonic()
    path = config_path or ctx.obj.get("config_path")
    try:
        cfg = load_config(path)
    except ConfigError as err:
        _emit_config_error("config.show", err, start)
        raise SystemExit(2)
    data = cfg.model_dump()
    if not unmask:
        data = _mask(data)
    emit(ok=True, command="config.show", result=data, duration_ms=_ms(start))
