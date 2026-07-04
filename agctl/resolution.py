"""Template resolution helpers (DESIGN D2, D5).

- :func:`fill_placeholders` substitutes ``{name}`` tokens in strings and
  recurses into dict/list containers.
- :func:`render_typed` is the typed counterpart used by capture-aware mocks:
  it consumes :class:`CaptureValue` (scalar/object/json) and emits values whose
  shape follows the declared type.
- :func:`deep_merge` implements the D5 body-merge algorithm (dict+dict
  recursive merge; everything else replaced wholesale).
- :func:`convert_sql_params` rewrites JDBC-style ``:name`` params to psycopg
  ``%(name)s`` form, leaving ``::`` casts untouched.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from typing import Any

__all__ = [
    "CaptureValue",
    "fill_placeholders",
    "render_typed",
    "deep_merge",
    "convert_sql_params",
]

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
# Negative lookbehind on ':' avoids converting the second colon of a
# PostgreSQL ``::`` cast (the first ':' is not a valid name-start, the second
# is preceded by ':' and is thus skipped). SQL string literals are not parsed;
# ``:foo`` inside a literal may be converted (acceptable for v1).
_SQL_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


def fill_placeholders(value, params: dict[str, str]):
    """Substitute ``{name}`` placeholders in ``value`` using ``params``.

    - Only literal ``{name}`` tokens (name = ``[A-Za-z_][A-Za-z0-9_]*``) are
      substituted; a name absent from ``params`` is left as the literal
      ``{name}`` (validation deferred per DESIGN §10).
    - Dict values and list elements are recursed into; new containers are
      returned (input is never mutated).
    - Non-string scalars (int, None, ...) pass through unchanged.
    """
    if isinstance(value, str):
        def _sub(match: re.Match[str]) -> str:
            name = match.group(1)
            return params[name] if name in params else match.group(0)

        return _PLACEHOLDER_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: fill_placeholders(v, params) for k, v in value.items()}
    if isinstance(value, list):
        return [fill_placeholders(v, params) for v in value]
    return value


@dataclass
class CaptureValue:
    """A resolved capture value paired with its declared type.

    Runtime artifact produced by the capture resolver and consumed by
    :func:`render_typed`. Distinct from ``CaptureSpec`` (the config model):
    ``CaptureValue`` carries no path/source metadata, just the live value and
    one of ``"scalar"``, ``"object"``, ``"json"``. ``value is None`` is the
    resolver-set marker for a missing path.
    """

    value: Any
    type: str


def render_typed(value: Any, captures: dict[str, CaptureValue]) -> Any:
    """Substitute ``{name}`` placeholders using typed :class:`CaptureValue`s.

    Mirrors :func:`fill_placeholders` for absent names (left as literal
    ``{name}``) and container recursion (dict/list rebuilt; input never
    mutated; non-string scalars passed through). Differs in that each capture
    carries a ``type`` controlling how its value is rendered:

    - ``scalar`` -> ``str(capture.value)``.
    - ``json`` -> ``json.dumps(capture.value)``.
    - ``object`` -> the live ``capture.value``, but ONLY when the containing
      field string is exactly ``"{name}"`` (whole-field). Used inline (e.g.
      ``"pre={name}"``) -> ``ValueError``. Valid configs never reach this —
      Task 5's startup check rejects object-typed captures used inline — but
      the renderer is defensive so behavior stays honest if it does.

    When ``capture.value is None`` (missing path), the substitution is the
    empty string ``""`` regardless of type (never ``"None"``/``"null"``/``{}``).
    """
    if isinstance(value, str):
        return _render_typed_str(value, captures)
    if isinstance(value, dict):
        return {k: render_typed(v, captures) for k, v in value.items()}
    if isinstance(value, list):
        return [render_typed(v, captures) for v in value]
    return value


def _render_typed_str(s: str, captures: dict[str, CaptureValue]) -> Any:
    """Apply typed substitution to a single string field.

    Whole-field object substitution (``"{name}"`` alone) may return a non-str
    value (the live object); every other path returns a ``str``.
    """
    whole = _PLACEHOLDER_RE.fullmatch(s)
    if whole is not None:
        name = whole.group(1)
        if name not in captures:
            return s  # absent name -> literal "{name}"
        capture = captures[name]
        if capture.value is None:
            return ""
        if capture.type == "object":
            return capture.value
        if capture.type == "scalar":
            return str(capture.value)
        if capture.type == "json":
            return json.dumps(capture.value)
        raise ValueError(f"unknown capture type for {name!r}: {capture.type!r}")

    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in captures:
            return match.group(0)
        capture = captures[name]
        if capture.value is None:
            return ""
        if capture.type == "scalar":
            return str(capture.value)
        if capture.type == "json":
            return json.dumps(capture.value)
        if capture.type == "object":
            raise ValueError(
                f"capture {name!r} of type 'object' must occupy the whole "
                f"field ('{{{name}}}'); cannot be used inline in {s!r}"
            )
        raise ValueError(f"unknown capture type for {name!r}: {capture.type!r}")

    return _PLACEHOLDER_RE.sub(_sub, s)


def deep_merge(base, override):
    """Merge ``override`` onto ``base`` per DESIGN D5.

    - Both dicts -> recursive per-key merge (override's keys added/overridden).
    - Any other type combination -> ``override`` replaces ``base`` entirely
      (arrays replaced wholesale, scalars win). A deep copy of container
      overrides is returned so callers cannot mutate the source.
    - ``base`` is never mutated.
    """
    if isinstance(base, dict) and isinstance(override, dict):
        merged = copy.deepcopy(base)
        for key, val in override.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], val)
            else:
                merged[key] = copy.deepcopy(val)
        return merged
    # Mismatch / scalar / array -> override wins wholesale.
    return copy.deepcopy(override)


def convert_sql_params(sql: str) -> str:
    """Rewrite JDBC-style ``:name`` named params to psycopg ``%(name)s``.

    Name chars: ``[A-Za-z_][A-Za-z0-9_]*``. ``::`` (PostgreSQL casts) are NOT
    converted because the char following the first ``:`` is another ``:``,
    which is not a valid name-start. SQL string literals are not parsed.
    """
    return _SQL_PARAM_RE.sub(r"%(\1)s", sql)
