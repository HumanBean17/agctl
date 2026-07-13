"""Optional `.env` file loading — env defaults before interpolation (DESIGN §2.2).

agctl reads `${VAR}` from the process environment. A `.env` file lets a project
commit those values as **defaults** for local dev / CI without forcing users to
`source` the file first. We merge the parsed values into the config env dict
with **real environment winning** (`.env` only fills keys not already set) — the
universal convention (docker compose, python-dotenv default, rails, django), so
CI/production can inject a real env var to override a committed default.

Resolution order (highest precedence first):

1. `--env-file <path>` (explicit).
2. `AGCTL_ENV_FILE` env var.
3. `.env` sitting next to the **resolved** `agctl.yaml` (best-effort: a missing
   sibling is a normal no-op, not an error).

Explicit sources (1, 2) are *required*: a missing file is a user-intent error
and raises ``ConfigError`` (mirrors `--config`). Only the auto-load sibling (3)
is allowed to be absent silently.

We disable python-dotenv's *own* ``${VAR}`` expansion (``interpolate=False``) so
the raw values flow into the env dict and agctl's single ``interpolate()`` engine
(``loader.py``) owns all ``${...}`` resolution — including chains, e.g. a `.env`
line ``FOO=a-${B}`` plus env ``B=b`` yields ``a-b`` through agctl's multi-pass
interpolator. One engine, no double-substitution ambiguity.
"""

import pathlib

from dotenv import dotenv_values

from ..errors import ConfigError


def load_env_file(path: pathlib.Path, *, required: bool) -> dict[str, str]:
    """Parse a `.env` file into a ``{KEY: VALUE}`` dict.

    Args:
        path: Path to the `.env` file.
        required: If True, a missing file raises ``ConfigError`` (used for
            explicit ``--env-file`` / ``AGCTL_ENV_FILE``). If False, a missing
            file returns ``{}`` (used for the best-effort sibling auto-load).

    Returns:
        Parsed key→value mapping. Bare keys (``KEY`` with no ``=``) are dropped
        — python-dotenv returns ``None`` for them and they carry no value to
        interpolate. ``KEY=`` is kept as the empty string.

    Raises:
        ConfigError: ``required`` is True and the file is missing, or the file
            exists but cannot be read.
    """
    if not path.is_file():
        if required:
            raise ConfigError(f"Env file not found: {path}", {"path": str(path)})
        return {}
    try:
        # interpolate=False: raw values — agctl's interpolate() owns ${...}.
        raw = dotenv_values(path, interpolate=False)
    except (OSError, UnicodeDecodeError) as exc:
        # OSError: unreadable / permission / etc.
        # UnicodeDecodeError: file is not valid UTF-8 (dotenv decodes internally;
        # a ValueError subclass, NOT an OSError, so it needs explicit catching).
        raise ConfigError(
            f"Could not read env file {path}: {exc}", {"path": str(path)}
        ) from exc
    # dotenv_values -> Dict[str, str | None]; drop bare-key Nones.
    return {key: value for key, value in raw.items() if value is not None}


def resolve_dotenv_values(
    explicit: str | None, env: dict[str, str], base_path: pathlib.Path
) -> dict[str, str]:
    """Pick the `.env` source per the precedence order and return its values.

    Args:
        explicit: Value of ``--env-file`` (None if not given).
        env: The config env dict (real environment, pre-merge). ``AGCTL_ENV_FILE``
            is read from here — so it must be set in the *real* environment, not
            inside the `.env` itself (that would be circular and is ignored).
        base_path: The resolved ``agctl.yaml`` path; its parent dir holds the
            auto-load sibling `.env`.

    Returns:
        Parsed values from the chosen source (may be empty). Real-env-wins
        merging is the caller's responsibility (``{**dotenv, **env}``).
    """
    if explicit:
        return load_env_file(pathlib.Path(explicit), required=True)
    if "AGCTL_ENV_FILE" in env:
        return load_env_file(pathlib.Path(env["AGCTL_ENV_FILE"]), required=True)
    return load_env_file(base_path.parent / ".env", required=False)
