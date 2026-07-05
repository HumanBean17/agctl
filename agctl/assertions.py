"""Assertion primitives backing Kafka/DB checks.

DESIGN §3.2 (jq predicate/value evaluation, silent skip on error),
and D8 (equals coercion + type-aware comparison rules).
"""

import datetime
import decimal
import json
import re
import uuid

from .errors import AssertionFailure, ConfigError

# A dotted jq path whose final segment contains a hyphen, e.g.
# ``.headers.x-request-id`` or ``.body.event-type``. jq parses such segments
# as subtraction, producing a baffling compile error; this lets ``compile_jq``
# append a targeted bracket-notation hint.
_HYPHEN_KEY_PATH = re.compile(r"\.[\w.]*\.[A-Za-z_][\w-]*-[\w-]+")


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


def _missing_jq_config_error(kind: str, detail: dict) -> ConfigError:
    """Build a :class:`ConfigError` pointing at the ``jq`` extra for the given
    assertion ``kind`` (e.g. ``"match"``, ``"jq-path"``).

    The base :func:`_jq` install hint names only db/kafka; HTTP/mock callers
    must point at ``pip install 'agctl[jq]'`` instead (DESIGN D7). Call sites
    raise the result ``from None`` so the stale db/kafka hint doesn't linger
    in the exception chain.
    """
    return ConfigError(
        f"jq is required for {kind} assertions: pip install 'agctl[jq]'",
        detail,
    )


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
    except ConfigError:
        raise ConfigError(
            f"{context}jq is required for jq assertions: pip install 'agctl[jq]'",
            {"expr": expr, "label": label},
        ) from None
    try:
        jq_lib.compile(expr)
    except Exception as exc:
        msg = f"{context}invalid jq expression {expr!r}: {exc}"
        if _HYPHEN_KEY_PATH.search(expr):
            msg += (
                " (header/field keys containing '-' need bracket notation, "
                'e.g. .headers["x-request-id"])'
            )
        raise ConfigError(
            msg,
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


def _parse_iso_datetime(s):
    """Parse ``s`` as an ISO 8601 datetime string, or return ``None``.

    The ``'T'`` gate avoids mis-treating date-only (``"2026-06-29"``) or
    pure-time strings as timestamps. Both ``'...Z'`` and ``'...+00:00'`` are
    accepted (Python's ``fromisoformat`` pre-3.11 doesn't handle ``'Z'``
    directly, so it's normalized first)."""
    if not isinstance(s, str) or "T" not in s:
        return None
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _to_utc(dt):
    """Normalize ``dt`` to UTC. Naive datetimes are treated as UTC (matches
    DESIGN's ``Z`` == UTC convention). Two aware datetimes compared in UTC
    never raise."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def type_aware_equal(expected, actual) -> bool:
    """DESIGN D8 step 4: strict, type-aware equality. 0 != '0' (number vs string).

    A number never equals a string of the same digits. For two strings that
    both parse as ISO 8601 datetimes, comparison normalizes via ``_to_utc``
    so ``'...Z'`` and ``'...+00:00'`` for the same UTC instant compare equal
    (the comparison layer tolerates the ``Z`` vs ``+00:00`` spelling
    difference; ``coerce_db_value``'s ``.isoformat()`` output is unchanged).
    Otherwise compares with ==, recursing element-wise for dict/list.
    """
    # number-vs-string mismatch (in either order) -> never equal
    if isinstance(expected, (int, float, decimal.Decimal, bool)) and isinstance(actual, str):
        return False
    if isinstance(actual, (int, float, decimal.Decimal, bool)) and isinstance(expected, str):
        return False
    if isinstance(expected, str) and isinstance(actual, str):
        te, ta = _parse_iso_datetime(expected), _parse_iso_datetime(actual)
        if te is not None and ta is not None:
            return _to_utc(te) == _to_utc(ta)
    return expected == actual


def validate_http_assertion_args(
    *,
    status: int | None,
    contains: str | None,
    match: str | None,
    jq_path: str | None,
    equals: str | None,
) -> None:
    """Pre-request gate for HTTP assertions (DESIGN D8 + --contains JSON shape).

    Pure arg validation only -- never touches the network, so misuse fails
    BEFORE the request side-effect is triggered (load-bearing for the
    validate/evaluate split).

    Raises :class:`ConfigError` (exit 2) on:
      - **pairing (D8)**: exactly one of ``jq_path``/``equals`` set ->
        ``--jq-path and --equals must be used together``.
      - ``--contains`` present but not valid JSON ->
        ``--contains must be valid JSON``.

    Returns ``None`` (no-op) when args are sound -- including the all-None case.
    """
    # pairing: jq_path and equals must be used together (XOR on None-ness)
    if (jq_path is None) != (equals is None):
        raise ConfigError("--jq-path and --equals must be used together", {})
    # --contains must parse as JSON when present (safe to re-parse in evaluate)
    if contains is not None:
        try:
            json.loads(contains)
        except (json.JSONDecodeError, ValueError):
            raise ConfigError("--contains must be valid JSON", {})
    return None


def evaluate_http_assertions(
    result: dict,
    *,
    status: int | None,
    contains: str | None,
    match: str | None,
    jq_path: str | None,
    equals: str | None,
) -> None:
    """Post-request evaluation of an HTTP response against active assertion modes.

    Assumes :func:`validate_http_assertion_args` has already run on the same
    args (so pairing is satisfied and ``--contains``, if present, is valid JSON).

    Evaluates each active mode, collecting a failure entry per failing mode
    (NO short-circuit -- all modes run, all failures are reported). If any
    failures were collected, raises :class:`AssertionFailure` whose
    ``detail`` carries BOTH the full ``response`` dict and the ``failures``
    list (so callers can render context and pinpoint each failed mode).

    For ``--match`` / ``--jq-path``, a missing ``jq`` library surfaces from
    ``_jq()`` as a :class:`ConfigError` whose message names only db/kafka;
    it is re-raised here pointing at ``pip install 'agctl[jq]'`` (DESIGN D7,
    mandatory rewrite -- HTTP/mock context).

    Per-mode failure entry shapes (pinned, parsed by downstream agents):
      - ``status``:   ``{"mode":"status","expected":<status>,"actual":<status_code>}``
      - ``contains``: ``{"mode":"contains","needle":<parsed>,"matched":False,
                        "root":"response body","body":<result["body"]>}``
      - ``match``:    ``{"mode":"match","expr":<match>,"result":False,
                        "root":"response envelope","body":<result["body"]>}``
      - ``jq-path``:  ``{"mode":"jq-path","path":<jq_path>,
                        "expected":<parse_equals(equals)>,"actual":<jq_value or None>,
                        "root":"response body","body":<result["body"]>}``

    The ``root`` label + ``body`` snapshot make a failed assertion self-debugging:
    an agent sees *what* the expression was evaluated against and the actual payload,
    so it can correct a mis-rooted expression (e.g. ``.x`` → ``.body.x``) without
    dropping the flag and re-running to inspect raw output. ``root`` differs per mode
    because the modes root differently (DESIGN §3.1): ``--match`` at the response
    envelope, ``--contains``/``--jq-path`` at the response body.
    """
    if all(arg is None for arg in (status, contains, match, jq_path, equals)):
        return None

    failures = []

    if status is not None:
        actual_status = result["status_code"]
        if actual_status != status:
            failures.append(
                {"mode": "status", "expected": status, "actual": actual_status}
            )

    if contains is not None:
        needle = json.loads(contains)  # validated safe by validate_http_assertion_args
        if not json_subset(needle, result["body"]):
            failures.append(
                {
                    "mode": "contains",
                    "needle": needle,
                    "matched": False,
                    "root": "response body",
                    "body": result["body"],
                }
            )

    if match is not None:
        try:
            ok = jq_bool(result, match)
        except ConfigError:
            raise _missing_jq_config_error("match", {"expr": match}) from None
        if not ok:
            failures.append(
                {
                    "mode": "match",
                    "expr": match,
                    "result": False,
                    "root": "response envelope",
                    "body": result["body"],
                }
            )

    if jq_path is not None:  # equals is non-None too (validated pairing)
        expected = parse_equals(equals)
        try:
            actual = jq_value(result["body"], jq_path)
        except ConfigError:
            raise _missing_jq_config_error("jq-path", {"path": jq_path}) from None
        if not type_aware_equal(actual, expected):
            failures.append(
                {
                    "mode": "jq-path",
                    "path": jq_path,
                    "expected": expected,
                    "actual": actual,
                    "root": "response body",
                    "body": result["body"],
                }
            )

    if failures:
        raise AssertionFailure(
            f"HTTP response failed {len(failures)} assertion(s)",
            {"response": result, "failures": failures},
        )
