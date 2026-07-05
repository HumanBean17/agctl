"""Config loading pipeline: discovery, interpolation, validation (DESIGN §2.2, §5)."""

import os
import pathlib
import re
from typing import Any

import yaml
from pydantic import ValidationError

from ..errors import ConfigError
from .models import Config
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


TOOL_MAJOR_VERSION = "2"


def load_config(path: str | None = None, env: dict[str, str] | None = None):
    """Full pipeline: discover -> parse -> interpolate -> override -> validate."""
    env = env if env is not None else os.environ
    config_path = discover_config_path(explicit=path, env=env)
    raw = yaml.safe_load(config_path.read_text()) or {}
    interpolated = interpolate(raw, env)
    with_overrides = apply_env_overrides(interpolated, env)
    _check_version(with_overrides)
    try:
        return Config.model_validate(with_overrides)
    except ValidationError as exc:
        raise ConfigError("Invalid configuration", {"validation_errors": exc.errors()}) from exc


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
                f"add `version: \"{TOOL_MAJOR_VERSION}\"` (or run `agctl config migrate` "
                f"on a v1 config)."
            )
        else:
            message = (
                f"Config dialect v{major} is no longer supported by agctl v{TOOL_MAJOR_VERSION} "
                f"(config_version='{version}'). Run `agctl config migrate` to upgrade, "
                f"or manually bump `version: \"{TOOL_MAJOR_VERSION}\"` and prefix each HTTP "
                f"`match` expression with `.body | ` and each Kafka `match` expression with "
                f"`.value | `."
            )
        raise ConfigError(
            message,
            {"config_version": version, "tool_major": TOOL_MAJOR_VERSION},
        )
