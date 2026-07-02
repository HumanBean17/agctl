"""Config loading pipeline: discovery, interpolation, validation (DESIGN §2.2, §5)."""

import re
from typing import Any

_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(:-([^}]*))?\}")


class ConfigError(Exception):
    """Raised for any config problem. Maps to exit code 2 (DESIGN §4.1)."""

    def __init__(self, message: str, detail: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}


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
    def repl(match: re.Match[str]) -> str:
        var, has_default, default = match.group(1), match.group(2), match.group(3)
        if var in env:
            return env[var]
        if has_default is not None:
            return default  # "" for ${VAR:-}, the literal for ${VAR:-x}
        unresolved.append(var)
        return match.group(0)

    return _VAR_RE.sub(repl, s)
