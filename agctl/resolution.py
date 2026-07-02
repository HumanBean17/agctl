"""Template resolution helpers (DESIGN D2, D5).

- :func:`fill_placeholders` substitutes ``{name}`` tokens in strings and
  recurses into dict/list containers.
- :func:`deep_merge` implements the D5 body-merge algorithm (dict+dict
  recursive merge; everything else replaced wholesale).
- :func:`convert_sql_params` rewrites JDBC-style ``:name`` params to psycopg
  ``%(name)s`` form, leaving ``::`` casts untouched.
"""

from __future__ import annotations

import copy
import re

__all__ = ["fill_placeholders", "deep_merge", "convert_sql_params"]

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
