"""AGCTL_* environment-override layer (DESIGN §5, §8 — D4: __ nesting delimiter).

Convention: AGCTL_<SECTION>__<KEY> — double-underscore separates path segments;
a single underscore stays within a key segment.

Override resolution matches existing config keys **case- and
hyphen-insensitively** (DESIGN §8: hyphens within a segment become underscores,
e.g. ``main-db`` ~ ``MAIN_DB``). So ``AGCTL_SERVICES__ORDER_SERVICE__BASE_URL``
overrides the real ``services.order-service.base_url`` entry rather than
creating a phantom ``order_service`` sibling. When no existing key matches, a
new key is written under the lowercased segment name (write-oriented: hyphen
reconstruction from an underscored env name is not guaranteed — DESIGN §5).
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


def _norm(segment: str) -> str:
    """Case- and hyphen-insensitive key form: lowercase with '-' -> '_'."""
    return segment.lower().replace("-", "_")


def _match_key(data: dict, segment: str) -> str | None:
    """Find an existing key in ``data`` matching ``segment`` case- and
    hyphen-insensitively. Returns the real key, or None if no existing key
    matches (so the caller can create a new one)."""
    target = _norm(segment)
    for key in data:
        if isinstance(key, str) and _norm(key) == target:
            return key
    return None


def _parse_path(suffix: str) -> list[str] | None:
    segments = suffix.split("__")
    # Require at least one "__" separator (>= 2 segments). The double-underscore
    # delimiter is what distinguishes a real SECTION__KEY override from other
    # AGCTL_* env vars that must NOT be treated as overrides — notably
    # AGCTL_CONFIG (config-file path, DESIGN §5) and AGCTL_TEST_* test flags.
    # A trailing/double "__" or the bare AGCTL_ prefix yields an empty segment
    # -> malformed -> skip. (Top-level scalars like `version` are therefore not
    # overridable; that is acceptable — overriding version to anything but the
    # tool major would just trip the version guard anyway.)
    if len(segments) < 2 or any(seg == "" for seg in segments):
        return None  # malformed (e.g. AGCTL_CONFIG, trailing __); skip
    return [seg.lower() for seg in segments]


def _deep_set(data: dict[str, Any], path: list[str], value: str) -> None:
    cur = data
    for part in path[:-1]:
        key = _match_key(cur, part)
        if key is None:
            # No existing key: create a new intermediate dict under the lowercased
            # segment name (write-oriented — hyphen reconstruction is not guaranteed).
            key = part.lower()
            cur[key] = {}
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            # An existing leaf (scalar/list) sits at this segment; replace it with
            # a dict so a nested override can still be recorded. Overrides have the
            # highest precedence (DESIGN §5), so clobbering is the intended behavior.
            nxt = {}
            cur[key] = nxt
        cur = nxt
    last = path[-1]
    key = _match_key(cur, last)
    cur[key if key is not None else last.lower()] = value
