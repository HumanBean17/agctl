"""Config loading pipeline: discovery, interpolation, validation (DESIGN §2.2, §5)."""

import os
import pathlib
import re
from typing import Any, NamedTuple

import yaml
from pydantic import ValidationError

from ..errors import ConfigError
from .env_file import resolve_dotenv_values
from .models import Config, PartialConfig
from .resolver import apply_env_overrides

# Matches the *inner* text of a ${...} expression: VAR, VAR:-default, or VAR:-.
# DOTALL lets a default span newlines. The default is `.*` (not `[^}]*`) because
# by the time we match, the brace-counting walker has already extracted the
# balanced inner text — so stray `}` inside a default aren't a concern (an
# earlier literal `}` would have closed the outer expression instead, same as
# the legacy single-pass regex).
_VAR_INNER_RE = re.compile(r"^([A-Z_][A-Z0-9_]*)(:-(.*))?$", re.DOTALL)

# Cap on iterative substitution passes. Lets a value that itself contains
# ${...} (chained refs) resolve, while guaranteeing termination on pathological
# self-reference like ${A:-${A}} (which resolves to ${A} then stabilises —
# never spins). 25 is far above any realistic chain depth.
_MAX_INTERP_PASSES = 25


def deep_merge(base: dict, overlay: dict, overlay_name: str, overrides: list[dict], path: str = "") -> dict:
    """Merge overlay dict into base dict, with overlay winning in conflicts.

    For each key in overlay:
      - If key absent from base: base[key] = overlay[key] (addition, no record).
      - If key in base and both base[key] and overlay[key] are dict: recurse.
      - Else: record override and base[key] = overlay[key] (overlay wins).

    Args:
        base: Base dict to merge into (mutated in place).
        overlay: Overlay dict to merge from.
        overlay_name: Name of overlay source (e.g., "sidecar.yaml").
        overrides: List to append override records to.
        path: Current dotted path (used internally for recursion).

    Returns:
        The mutated base dict.
    """
    for key, overlay_value in overlay.items():
        # Build the dotted path for this key
        key_path = f"{path}.{key}" if path else key

        if key not in base:
            # Addition: key not in base, just add it
            base[key] = overlay_value
        elif isinstance(base[key], dict) and isinstance(overlay_value, dict):
            # Both are dicts: recurse
            deep_merge(base[key], overlay_value, overlay_name, overrides, key_path)
        else:
            # Scalar/list leaf or type clash: record override and replace
            overrides.append({"path": key_path, "overlay": overlay_name})
            base[key] = overlay_value

    return base


def interpolate(obj: Any, env: dict[str, str]) -> Any:
    """Resolve ${VAR}, ${VAR:-default}, ${VAR:-} in all string scalars.

    Bare ${VAR} missing -> ConfigError listing all unresolved vars.
    ${VAR:-default} missing -> the literal default. ${VAR:-} missing -> empty.
    """
    unresolved: list[str] = []
    resolved = _interpolate(obj, env, unresolved)
    if unresolved:
        raise ConfigError(
            "Unresolved environment variables",
            {"variables": sorted(set(unresolved))},
        )
    return resolved


def _interpolate(obj: Any, env: dict[str, str], unresolved: list[str]) -> Any:
    if isinstance(obj, str):
        return _interpolate_str(obj, env, unresolved)
    if isinstance(obj, dict):
        return {k: _interpolate(v, env, unresolved) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v, env, unresolved) for v in obj]
    return obj


def _interpolate_str(s: str, env: dict[str, str], unresolved: list[str]) -> str:
    """Resolve ${VAR}, ${VAR:-default}, ${VAR:-} with nested-default support.

    Two layers cooperate:

    * `_substitute_once` does a single left-to-right pass that matches each
      ${...} by *brace depth*, so a nested expression like ${A:-${B}} is
      treated as ONE outer expression whose default text is `${B}`.
    * This outer loop re-runs the pass until the string stops changing, so a
      var whose VALUE itself contains ${...} (chained refs like
      A -> ${B} -> "final") also resolves. The `_MAX_INTERP_PASSES` cap turns
      self-reference into best-effort rather than an infinite loop.

    Defaults are resolved by recursing through `_interpolate_str` (inside
    `_resolve_var`), so deeply nested defaults ${A:-${B:-${C}}} resolve
    inside-out on the first pass already.
    """
    for _ in range(_MAX_INTERP_PASSES):
        if "${" not in s:
            return s
        new_s = _substitute_once(s, env, unresolved)
        if new_s == s:
            return s
        s = new_s
    return s  # cap reached: best-effort, leave remaining ${...} literal


def _substitute_once(s: str, env: dict[str, str], unresolved: list[str]) -> str:
    """One left-to-right pass. Matches ${...} by counting ${ / } depth so a
    nested ${A:-${B}} binds as a single outer expression (the legacy
    single-pass regex used `[^}]*` which could not span the inner `}`).

    A `${` with no matching `}` is emitted literally (and we stop, since the
    remainder can no longer contain a balanced expression).
    """
    out: list[str] = []
    i = 0
    n = len(s)
    while i < n:
        if s[i] == "$" and i + 1 < n and s[i + 1] == "{":
            close = _find_matching_brace(s, i + 2)
            if close == -1:
                # Unbalanced ${ with no close: emit the rest verbatim.
                out.append(s[i:])
                return "".join(out)
            inner = s[i + 2 : close]
            out.append(_resolve_var(inner, env, unresolved))
            i = close + 1
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def _find_matching_brace(s: str, start: int) -> int:
    """Index of the `}` that closes the `${` whose content begins at `start`,
    counting ${ / } depth so nested expressions belong to the outer one.
    Returns -1 if unbalanced (no matching close)."""
    depth = 1
    i = start
    n = len(s)
    while i < n:
        if s[i] == "$" and i + 1 < n and s[i + 1] == "{":
            depth += 1
            i += 2
            continue
        if s[i] == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _resolve_var(inner: str, env: dict[str, str], unresolved: list[str]) -> str:
    """Resolve the text between one matched `${ ... }`.

    inner is e.g. `VAR`, `VAR:-default`, or `VAR:-${OTHER}`. A default is
    itself passed back through `_interpolate_str` so nested defaults resolve.
    Non-var `${...}` (e.g. lowercase `${foo}`, or `${}`) is left literal,
    preserving the legacy "UPPER_CASE only" rule.
    """
    m = _VAR_INNER_RE.match(inner)
    if not m:
        return "${" + inner + "}"
    var = m.group(1)
    if var in env:
        return env[var]
    if m.group(2) is not None:  # the `:-` separator is present (incl. `VAR:-`)
        default = m.group(3) or ""
        return _interpolate_str(default, env, unresolved)
    # Bare ${VAR}, no default, var unset.
    unresolved.append(var)
    return "${" + inner + "}"


def discover_config_path(explicit: str | None = None, env: dict[str, str] | None = None) -> pathlib.Path:
    """Resolve the config path per DESIGN §5: --config > AGCTL_CONFIG > walk up."""
    env = env if env is not None else os.environ

    if explicit:
        path = pathlib.Path(explicit)
        if not path.is_file():
            raise ConfigError(f"Config file not found: {explicit}", {"path": explicit})
        return path

    if "AGCTL_CONFIG" in env:
        path = pathlib.Path(env["AGCTL_CONFIG"])
        if not path.is_file():
            raise ConfigError(f"Config file not found: {env['AGCTL_CONFIG']}", {"path": str(path)})
        return path

    cwd = pathlib.Path.cwd()
    for directory in [cwd, *cwd.parents]:
        candidate = directory / "agctl.yaml"
        if candidate.is_file():
            return candidate
        if (directory / ".git").exists():
            break

    raise ConfigError("No agctl.yaml found (use --config or AGCTL_CONFIG, or add agctl.yaml)", {})


TOOL_MAJOR_VERSION = "3"


class ComposedConfig(NamedTuple):
    """Result of composing a base config with overlay fragments.

    Attributes:
        config: The final merged and validated Config.
        overrides: List of override records, each with "path" (dotted path) and
            "overlay" (overlay file path).
    """
    config: Config
    overrides: list[dict]


def compose_config(
    path: str | None = None,
    overlays: list[str] | None = None,
    env: dict[str, str] | None = None,
    env_file: str | None = None,
) -> ComposedConfig:
    """Compose base config with overlay fragments.

    Pipeline:
      1. Resolve env (defaults to os.environ).
      2. Discover base config path.
      3. Merge `.env` defaults into env (real env wins; DESIGN §2.2).
      4. Load and interpolate base YAML.
      5. Check base version (must be v3).
      6. For each overlay (in order):
         - Verify file exists.
         - Load and interpolate overlay YAML.
         - Validate fragment with PartialConfig.
         - Check overlay version major matches TOOL_MAJOR_VERSION if present.
         - Deep merge into base, recording overrides.
      7. Apply environment variable overrides to merged dict.
      8. Validate final Config.
      9. Return ComposedConfig(config, overrides).

    Args:
        path: Explicit base config path (discovery if None).
        overlays: List of overlay file paths to apply in order.
        env: Environment dict (defaults to os.environ).
        env_file: Explicit `.env` path (``--env-file``). None → fall back to
            ``AGCTL_ENV_FILE``, then the `.env` sibling of the resolved config.

    Returns:
        ComposedConfig with final Config and override records.

    Raises:
        ConfigError: On discovery, validation, or version mismatch errors.
    """
    env = env if env is not None else os.environ
    base_path = discover_config_path(explicit=path, env=env)
    # Merge `.env` defaults BEFORE interpolation so ${VAR} can resolve from it.
    # Done after config discovery so the sibling .env sits next to the RESOLVED
    # agctl.yaml, and so AGCTL_CONFIG / AGCTL_ENV_FILE set inside .env cannot
    # steer their own resolution (only real-env values do — no cycle). Real env
    # wins: .env only fills keys not already set. We build a NEW dict rather than
    # mutate os.environ, so config loading has no process-env side effects.
    dotenv = resolve_dotenv_values(env_file, env, base_path)
    if dotenv:
        env = {**dotenv, **env}
    base_raw = interpolate(yaml.safe_load(base_path.read_text()) or {}, env)
    _check_version(base_raw)

    overrides: list[dict] = []
    for ov in overlays or []:
        ov_path = pathlib.Path(ov)
        if not ov_path.is_file():
            raise ConfigError(f"Overlay file not found: {ov}", {"path": ov})

        raw_ov = interpolate(yaml.safe_load(ov_path.read_text()) or {}, env)

        try:
            PartialConfig.model_validate(raw_ov)
        except ValidationError as exc:
            raise ConfigError(
                f"Invalid overlay: {ov}",
                {"overlay": ov, "validation_errors": exc.errors()},
            ) from exc

        ov_version = raw_ov.get("version")
        if ov_version is not None:
            ov_major = str(ov_version).split(".")[0]
            if ov_major != TOOL_MAJOR_VERSION:
                raise ConfigError(
                    f"Overlay version mismatch in {ov}: major must be {TOOL_MAJOR_VERSION}",
                    {"overlay": ov, "found": ov_major},
                )

        # Drop version from overlay before merge to prevent spurious override warning
        raw_ov.pop("version", None)

        deep_merge(base_raw, raw_ov, ov, overrides)

    # Dedupe overrides by path, keeping last occurrence (last writer wins)
    deduped_overrides: list[dict] = {}
    for override in overrides:
        deduped_overrides[override["path"]] = override
    overrides = list(deduped_overrides.values())

    with_env = apply_env_overrides(base_raw, env)
    try:
        config = Config.model_validate(with_env)
    except ValidationError as exc:
        raise ConfigError("Invalid configuration", {"validation_errors": exc.errors()}) from exc

    return ComposedConfig(config, overrides)


def load_config(
    path: str | None = None,
    env: dict[str, str] | None = None,
    overlays: list[str] | None = None,
    env_file: str | None = None,
) -> Config:
    """Full pipeline: discover -> parse -> interpolate -> merge overlays -> override -> validate.

    Args:
        path: Explicit config path (discovery if None).
        env: Environment dict (defaults to os.environ).
        overlays: Optional list of overlay file paths to compose.
        env_file: Explicit `.env` path (``--env-file``); None → AGCTL_ENV_FILE → sibling.

    Returns:
        Validated Config object.

    Raises:
        ConfigError: On any pipeline error.
    """
    return compose_config(path, overlays, env, env_file).config


def _check_version(data: dict) -> None:
    # A bare `version:` parses as YAML None — treat it as missing (clear message)
    # rather than stringifying to "None" ("Config dialect vNone ..."). A real
    # `0` / `False` / string still stringifies normally (so `or ""` is wrong here
    # — it would swallow `version: 0`).
    raw = data.get("version")
    version = "" if raw is None else str(raw).strip()
    major = version.split(".")[0] if version else ""
    if major != TOOL_MAJOR_VERSION:
        if not version:
            message = (
                f"Config is missing a `version`. agctl speaks dialect v{TOOL_MAJOR_VERSION}; "
                f"add `version: \"{TOOL_MAJOR_VERSION}\"` (or run `agctl config migrate`)."
            )
        else:
            message = (
                f"Config dialect v{major} is no longer supported by agctl v{TOOL_MAJOR_VERSION} "
                f"(config_version='{version}'). Run `agctl config migrate` to upgrade."
            )
        raise ConfigError(
            message,
            {"config_version": version, "tool_major": TOOL_MAJOR_VERSION},
        )
