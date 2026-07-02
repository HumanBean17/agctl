"""Config loading pipeline: discovery, interpolation, validation (DESIGN §2.2, §5)."""

import os
import pathlib
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
