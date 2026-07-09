"""`agctl config validate`, `show`, and `init` commands (DESIGN §3.5).

- ``validate`` loads the fully-resolved config and reports schema / cross-reference
  / plugin errors (exit 2 on any). It also folds in each loaded protocol plugin's
  own ``validate_config`` (DESIGN §9.2).
- ``show`` dumps the resolved config as JSON with secret-looking values masked
  (unless ``--unmask``).
- ``init`` writes a starter ``agctl.yaml`` (a clean baseline that validates as-is)
  for the user to edit, so no one has to copy-paste the sample from the README.

These commands are plain ``@click.command``s registered onto the ``config`` group
in :mod:`agctl.cli`. The list of loaded plugins is injected from
:mod:`agctl.cli` via :func:`set_plugins_provider`: :mod:`agctl.cli` owns plugin
loading (``_load_plugins`` / ``_LOADED_PLUGINS``), and hands config validation a
thunk that reads that module global at call time. This keeps the dependency
one-directional (``cli → config_commands``) and avoids a circular import.
"""

from __future__ import annotations

import importlib.resources
import os
import time
from pathlib import Path
from typing import Any, Callable

import click
import yaml

from ..config import ConfigError, load_config
from ..config.loader import compose_config, discover_config_path
from ..config.migrate import migrate_config
from ..config.validator import validate_config
from ..mock.capture_validate import collect_capture_placement_errors
from ..mock.jq_precompile import collect_jq_compile_errors
from ..output import emit

__all__ = ["config_init", "config_validate", "config_show", "config_migrate", "set_plugins_provider"]

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
@click.option("--overlay", "overlay_paths", multiple=True, default=None)
@click.pass_context
def config_validate(ctx: click.Context, config_path: str | None, overlay_paths: tuple[str, ...] | None) -> None:
    """Parse and validate agctl.yaml. Exit 2 on any error."""
    start = time.monotonic()
    path = config_path or ctx.obj.get("config_path")
    # Resolve overlay paths: own option takes precedence over ctx.obj
    ovs = tuple(overlay_paths) or ctx.obj.get("overlay_paths")
    try:
        composed = compose_config(path, list(ovs) if ovs else None)
    except ConfigError as err:
        _emit_config_error("config.validate", err, start)
        raise SystemExit(2)
    cfg = composed.config
    errors, warnings = validate_config(cfg)
    # Append override warnings from compose_config
    for override in composed.overrides:
        p = override["path"]
        ov = override["overlay"]
        warnings.append({"path": p, "message": f"overridden by overlay {Path(ov).name}"})
    # Let loaded plugins validate their own config sections (DESIGN §9.2).
    errors = errors + _plugin_validation_errors(_plugins_provider(), cfg.model_dump())
    # Surface malformed match.jq / reactor match as validation errors (D5,
    # Task 10). The collector lives in mock.jq_precompile (not validator.py)
    # so config/* stays free of an assertions dependency.
    errors = errors + collect_jq_compile_errors(cfg.mocks)
    # Surface object-capture placement violations (Task 5): an object-typed
    # capture used inline / in a string-only slot has no honest render. Same
    # pure-Python, no-assertions constraint as the jq collector above.
    errors = errors + collect_capture_placement_errors(cfg.mocks)
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
@click.option("--overlay", "overlay_paths", multiple=True, default=None)
@click.option("--unmask", is_flag=True, default=False)
@click.pass_context
def config_show(ctx: click.Context, config_path: str | None, overlay_paths: tuple[str, ...] | None, unmask: bool) -> None:
    """Dump the resolved config as JSON, secrets masked."""
    start = time.monotonic()
    path = config_path or ctx.obj.get("config_path")
    # Resolve overlay paths: own option takes precedence over ctx.obj
    ovs = tuple(overlay_paths) or ctx.obj.get("overlay_paths")
    try:
        composed = compose_config(path, list(ovs) if ovs else None)
    except ConfigError as err:
        _emit_config_error("config.show", err, start)
        raise SystemExit(2)
    data = composed.config.model_dump()
    if not unmask:
        data = _mask(data)
    # Branch result shape based on whether overlays are active
    if ovs:
        # With --overlay: wrap in {"config": ..., "overrides": ...}
        result = {"config": data, "overrides": composed.overrides}
    else:
        # Without --overlay: back-compat (config dict directly)
        result = data
    emit(ok=True, command="config.show", result=result, duration_ms=_ms(start))


# --- config init -----------------------------------------------------------

#: Packaged starter config — the single source of truth for the sample
#: ``agctl.yaml``. Kept byte-identical to the block in README.md (a drift-guard
#: test enforces this) and written as a clean baseline that validates with no
#: environment variables, so ``config init && config validate`` passes as-is.
_SAMPLE_RESOURCE = ("data", "sample-config.yaml")


def _load_sample() -> str:
    """Read the packaged sample config text.

    Read via :mod:`importlib.resources` so it resolves under both an installed
    wheel and an editable (``-e``) install. Raises if the package data is missing
    (e.g. an incomplete build); callers surface that as an exit-2 error.
    """
    try:
        root = importlib.resources.files("agctl")
        return (root.joinpath(*_SAMPLE_RESOURCE)).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError) as err:
        raise ConfigError(
            f"Sample config not found in the agctl package: {err}",
            detail={"resource": "/".join(_SAMPLE_RESOURCE)},
        ) from err


@click.command("init")
@click.option(
    "--output",
    "-o",
    "output",
    default=None,
    help="Destination path (default: ./agctl.yaml).",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Overwrite an existing file instead of refusing.",
)
@click.pass_context
def config_init(ctx: click.Context, output: str | None, force: bool) -> None:
    """Write a sample agctl.yaml to edit. Exit 2 if it already exists (use --force)."""
    start = time.monotonic()
    dest = Path(output) if output else Path.cwd() / "agctl.yaml"
    if dest.exists() and not force:
        msg = f"Refusing to overwrite existing {dest} (pass --force to overwrite)."
        emit(
            ok=False,
            command="config.init",
            result={"path": str(dest), "created": False},
            error={"type": "ConfigError", "message": msg},
            duration_ms=_ms(start),
        )
        raise SystemExit(2)
    try:
        content = _load_sample()
    except ConfigError as err:
        _emit_config_error("config.init", err, start)
        raise SystemExit(2)
    dest.write_text(content, encoding="utf-8")
    emit(
        ok=True,
        command="config.init",
        result={
            "path": str(dest),
            "created": True,
            "bytes": len(content.encode("utf-8")),
        },
        duration_ms=_ms(start),
    )


# --- config migrate --------------------------------------------------------

#: Base operator-facing reminder, always emitted: CLI ``--match`` flags are
#: NOT rewritten by ``agctl config migrate`` (the migration only walks the
#: config file). Note ``agctl mock run`` has NO ``--match`` CLI flag (mock
#: matchers are config-file only — and ARE rewritten); the deprecated
#: ``--filter-key`` alias on ``agctl kafka consume`` is, like ``--match``,
#: envelope-rooted under v2 and equally out of this command's reach. This base
#: text contains NO prefix instruction — that is appended (see below) only for
#: v1 sources, whose CLI flags genuinely still need a manual envelope prefix.
_CLI_FLAGS_NOTE_BASE = (
    "CLI --match flags (and the deprecated --filter-key alias) on "
    "`agctl http` / `agctl kafka` live in shell scripts and agent prompts — "
    "this command cannot reach them (it rewrites the config file only). Mock "
    "`match` expressions live in the config file and ARE rewritten by this "
    "command."
)

#: Appended to :data:`_CLI_FLAGS_NOTE_BASE` ONLY for v1 sources. ``migrate``
#: jq-prefixes match expressions solely for v1 inputs (v2/v3 exprs are already
#: envelope-rooted); telling a v2->v3 migrator to prefix would double-prefix
#: working scripts. Composed in :func:`config_migrate` gated on
#: ``from_version.split(".")[0] == "1"`` (mirroring ``migrate_config``'s own
#: ``source_major == "1"`` gate).
_CLI_FLAGS_NOTE_V1_PREFIX = (
    " For v1 sources being lifted to v3, prefix those CLI flags manually with "
    "`.body | ` (HTTP) or `.value | ` (Kafka); v2/v3 exprs are already "
    "envelope-rooted and need no prefix."
)

#: yaml.safe_dump reformats the file and drops comments; the original is
#: preserved in ``<path>.bak``. Surfaced in the result so the operator is not
#: surprised that the "prepend + bump version" migration touches most lines.
_FORMATTING_NOTE = (
    "yaml.safe_dump reformats the file and drops comments; the original is "
    "preserved in <path>.bak. Review the full diff before committing."
)


@click.command("migrate")
@click.option("--config", "config_path", default=None)
@click.option("--dry-run", is_flag=True, default=False, help="Preview; do not write.")
@click.pass_context
def config_migrate(
    ctx: click.Context, config_path: str | None, dry_run: bool
) -> None:
    """Rewrite a v1/v2 agctl.yaml to v3 (named kafka clusters; envelope-rooted match).

    Backs up the original to ``<path>.bak`` and writes the rewritten config
    back to ``<path>``. With ``--dry-run`` the rewrite is reported but nothing
    is written. A config already at dialect \"3\" is a clean no-op.
    """
    start = time.monotonic()
    explicit = config_path or ctx.obj.get("config_path")
    try:
        path = discover_config_path(explicit=explicit, env=os.environ)
        raw = path.read_text(encoding="utf-8")
        parsed = yaml.safe_load(raw) or {}
        result = migrate_config(parsed)
        # Write only when migrating for real (not already_current, not --dry-run).
        will_write = not result.already_current and not dry_run
        # The manual-prefix instruction applies ONLY to v1 sources (those are
        # the inputs whose match expressions migrate_config actually jq-prefixed).
        # For v2->v3 the exprs are already envelope-rooted, so emitting the
        # prefix guidance would steer operators into double-prefixing working
        # scripts. Gate on the from_version major, exactly as migrate_config
        # gates its own jq-prefix walkers on source_major == "1".
        is_v1_source = str(result.from_version).split(".")[0] == "1"
        cli_flags_note = _CLI_FLAGS_NOTE_BASE + (
            _CLI_FLAGS_NOTE_V1_PREFIX if is_v1_source else ""
        )
        base_result = {
            "path": str(path),
            "already_current": result.already_current,
            "from_version": result.from_version,
            "to_version": result.to_version,
            "rewritten": result.rewrites,
            "cli_flags_note": cli_flags_note,
            # Surfaced only when the file is actually rewritten — on --dry-run
            # and already_current nothing is reformatted, so the note would be noise.
            "formatting_note": _FORMATTING_NOTE if will_write else None,
        }
        if will_write:
            backup = path.with_suffix(path.suffix + ".bak")
            # Refuse to clobber an existing backup — silently overwriting would
            # destroy the safety net this command promises. Surfaces as the
            # standard ConfigError envelope (exit 2), consistent with the rest.
            if backup.exists():
                raise ConfigError(
                    f"Backup {backup} already exists; remove or rename it first, "
                    f"then re-run.",
                    {"backup": str(backup)},
                )
            backup.write_text(raw, encoding="utf-8")
            path.write_text(
                yaml.safe_dump(result.config, sort_keys=False, default_flow_style=False),
                encoding="utf-8",
            )
    except ConfigError as err:
        _emit_config_error("config.migrate", err, start)
        raise SystemExit(2)
    emit(ok=True, command="config.migrate", result=base_result, duration_ms=_ms(start))
