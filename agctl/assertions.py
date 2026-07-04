"""Assertion primitives backing Kafka/DB checks.

DESIGN §3.2 (jq predicate/value evaluation, silent skip on error),
and D8 (equals coercion + type-aware comparison rules).
"""

import datetime
import decimal
import json
import uuid

from .errors import ConfigError


def _jq():
    """Lazily import jq so the package imports without the optional extra.
    jq is only needed for match/path assertions (Kafka --match/--path, DB --path).
    A missing library is a configuration problem -> ConfigError (exit 2), distinct
    from a jq *expression* error (handled by callers as a silent skip per §3.2)."""
    try:
        import jq
    except ImportError as exc:  # pragma: no cover - exercised via sys.modules in tests
        raise ConfigError(
            "jq is required for match/path assertions: pip install 'agctl[db]' or 'agctl[kafka]'",
            {},
        ) from exc
    return jq


def jq_bool(value, expr: str) -> bool:
    """Evaluate a jq predicate against value; True only if the result is truthy.

    A jq compile/runtime error OR a falsy/empty result -> False (silently skipped
    per DESIGN §3.2). A missing jq library -> ConfigError (propagates, exit 2).
    """
    try:
        outputs = _jq().compile(expr).input(value).all()
    except ConfigError:
        raise
    except Exception:
        return False
    return any(bool(o) for o in outputs)


def jq_value(value, expr: str):
    """Evaluate a jq path/value expression (e.g. '.status'). Returns the first
    output value, or None if the expression errors or yields nothing. A missing jq
    library -> ConfigError (propagates, exit 2)."""
    try:
        outputs = _jq().compile(expr).input(value).all()
    except ConfigError:
        raise
    except Exception:
        return None
    if not outputs:
        return None
    return outputs[0]


def compile_jq(expr: str, *, label: str | None = None) -> None:
    """Compile a jq expression WITHOUT evaluating it against any value.

    Compile-only guard, distinct from ``jq_bool``/``jq_value``: those helpers wrap
    compile+eval in ``except Exception: return False/None`` (correct for runtime
    matching, where a partial match must never crash). ``compile_jq`` instead
    surfaces a malformed expression loudly as a :class:`ConfigError` (exit 2), so
    authoring typos are caught at startup / ``config validate`` rather than
    silently mis-matching every request.

    On a missing jq library, re-raises ``ConfigError`` pointing at the ``jq``
    extra (the base ``_jq()`` message names only db/kafka and is rewritten here
    for the HTTP/mock context). On any other compile-time exception (e.g.
    ``ValueError`` from a truncated expression), raises ``ConfigError`` whose
    message includes ``label``, the expression, and the underlying error.
    """
    context = f"[{label}] " if label else ""
    try:
        jq_lib = _jq()
    except ConfigError as exc:
        raise ConfigError(
            f"{context}jq is required for jq assertions: pip install 'agctl[jq]'",
            {"expr": expr, "label": label},
        ) from exc
    try:
        jq_lib.compile(expr)
    except Exception as exc:
        raise ConfigError(
            f"{context}invalid jq expression {expr!r}: {exc}",
            {"expr": expr, "label": label},
        ) from exc
    return None


def json_subset(needle, haystack) -> bool:
    """DESIGN --contains: True if every key/element in needle is present-and-equal
    in haystack, recursively for nested dict/list. Subset, not equality.

    - dict needle: every key in needle must exist in haystack with a
      json_subset-equal value.
    - list needle: every element of needle must be json_subset-matched by SOME
      element of haystack (order-independent).
    - scalar needle: needle == haystack.
    """
    if isinstance(needle, dict):
        if not isinstance(haystack, dict):
            return False
        return all(
            k in haystack and json_subset(v, haystack[k]) for k, v in needle.items()
        )
    if isinstance(needle, list):
        if not isinstance(haystack, list):
            return False
        # each needle element must match at least one haystack element
        return all(
            any(json_subset(n, h) for h in haystack) for n in needle
        )
    # scalar
    return needle == haystack


def parse_equals(text: str):
    """DESIGN D8 step 1-2: try json.loads(text); if it parses, return the typed
    value ('0'->int 0, 'true'->bool True, 'null'->None, '[1,2]'->[1,2]); else
    return the raw string."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


def coerce_db_value(value):
    """DESIGN D8 step 3: coerce a DB cell to a JSON-native type before comparison.

    - None -> None
    - bool -> bool (checked BEFORE int; bool is a subclass of int in Python)
    - decimal.Decimal -> int if integral else float
    - datetime.datetime/datetime.date/datetime.time -> .isoformat()
    - uuid.UUID -> str(value)
    - int/float/str -> unchanged
    - everything else -> unchanged
    """
    if value is None:
        return None
    # bool MUST be checked before int (bool subclasses int)
    if isinstance(value, bool):
        return value
    if isinstance(value, decimal.Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def type_aware_equal(expected, actual) -> bool:
    """DESIGN D8 step 4: strict, type-aware equality. 0 != '0' (number vs string).

    A number never equals a string of the same digits. Otherwise compares with ==,
    recursing element-wise for dict/list.
    """
    # number-vs-string mismatch (in either order) -> never equal
    if isinstance(expected, (int, float, decimal.Decimal, bool)) and isinstance(actual, str):
        return False
    if isinstance(actual, (int, float, decimal.Decimal, bool)) and isinstance(expected, str):
        return False
    return expected == actual
