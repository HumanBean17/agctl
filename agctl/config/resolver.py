"""AGCTL_* environment-override layer (DESIGN §5, §8 — D4: __ nesting delimiter).

Convention: AGCTL_<SECTION>__<KEY> — double-underscore separates path segments;
a single underscore stays within a key segment. Overrides are write-oriented:
hyphenated YAML keys (e.g. order-service) are not reconstructed from the
underscored env name, so prefer overrides on hyphen-free keys.
"""

import copy
from typing import Any

_PREFIX = "AGCTL_"


def apply_env_overrides(data: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    """Deep-merge AGCTL_* overrides into a copy of data. Highest precedence."""
    data = copy.deepcopy(data)
    for key, value in env.items():
        if not key.startswith(_PREFIX):
            continue
        path = _parse_path(key[len(_PREFIX):])
        if path is None:
            continue
        _deep_set(data, path, value)
    return data


def _parse_path(suffix: str) -> list[str] | None:
    segments = suffix.split("__")
    # Require at least one __ separator (i.e., at least 2 segments)
    if len(segments) < 2 or any(seg == "" for seg in segments):
        return None  # malformed (e.g. trailing __); skip
    return [seg.lower() for seg in segments]


def _deep_set(data: dict[str, Any], path: list[str], value: str) -> None:
    cur = data
    for part in path[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[path[-1]] = value
