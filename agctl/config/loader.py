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

_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(:-([^}]*))?\}")


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
