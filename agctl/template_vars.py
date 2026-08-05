"""Template variable value generators and token substitution.

This module is the foundation of the template-variables feature. It provides:

* ``BUILTIN_GENERATORS`` — a registry of named value generators
  (``uuid`` / ``ts`` / ``rand``). Each generator takes an ``opts: str | None``
  and returns a ``list[str]`` (always a list; single-value generators return a
  1-element list).
* ``parse_token`` — splits a token's interior (the text between ``{{`` and
  ``}}``) into ``(name, opts)`` on the first ``:``.
* ``TEMPLATE_RE`` — the regex recognizing ``{{name[:opts]}}`` tokens.
* ``substitute_generators`` — recursive walker that replaces tokens with
  generated values, memoizing by full matched text.
* ``find_unknown_templates`` — recursive scan that returns tokens whose name is
  not in ``BUILTIN_GENERATORS`` (used by config validation; never raises).

The character set of every generator's output is restricted to
``[0-9a-fA-FT:Z -]`` (the "charset invariant"), pinned by a parametrized
property test in ``tests/unit/test_template_vars.py``.
"""

from __future__ import annotations

import datetime
import re
import secrets
import time
import uuid as _uuid
from collections.abc import Callable

from agctl.errors import ConfigError


def _parse_positive_int(opts: str, what: str) -> int:
    """Parse ``opts`` as an integer ``>= 1`` or raise ``ConfigError``.

    ``what`` names the option for the error message (e.g. ``"count"``,
    ``"length"``).
    """
    try:
        n = int(opts)
    except (TypeError, ValueError):
        raise ConfigError(
            f"invalid {what} {opts!r}: expected a positive integer"
        )
    if n < 1:
        raise ConfigError(f"invalid {what} {opts!r}: must be >= 1")
    return n


def gen_uuid(opts: str | None) -> list[str]:
    """Return ``n`` distinct lowercase RFC-4122 v4 UUIDs.

    ``opts=None`` returns a single UUID in a 1-element list. ``opts="<n>"``
    (integer ``>= 1``) returns ``n`` distinct UUIDs. Non-integer / ``< 1`` opts
    raise ``ConfigError``.
    """
    if opts is None:
        n = 1
    else:
        n = _parse_positive_int(opts, "count")
    return [str(_uuid.uuid4()) for _ in range(n)]


def gen_ts(opts: str | None) -> list[str]:
    """Return a 1-element list with a timestamp string.

    ``opts=None`` → Unix seconds (10 digits today). ``opts="ms"`` → Unix
    milliseconds (13 digits today). ``opts="iso"`` → ISO-8601 UTC seconds,
    e.g. ``2026-08-05T12:00:00Z``. Any other value raises ``ConfigError``.
    """
    if opts is None:
        return [str(int(time.time()))]
    if opts == "ms":
        return [str(int(time.time() * 1000))]
    if opts == "iso":
        return [
            datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        ]
    raise ConfigError(f"invalid ts opts {opts!r}: expected one of ms, iso")


def gen_rand(opts: str | None) -> list[str]:
    """Return a 1-element list of ``n`` lowercase hex chars.

    ``opts=None`` → 16 hex chars. ``opts="<n>"`` (integer ``>= 1``) → ``n`` hex
    chars. ``opts`` is the *character length* (not a count); this generator
    always returns a single string. Non-integer / ``< 1`` opts raise
    ``ConfigError``.
    """
    if opts is None:
        n = 16
    else:
        n = _parse_positive_int(opts, "length")
    # ``secrets.token_hex`` emits 2 hex chars per byte and is always even
    # length; generate enough bytes and truncate to exactly ``n`` chars so odd
    # lengths work too.
    return [secrets.token_hex((n + 1) // 2)[:n]]


BUILTIN_GENERATORS: dict[str, Callable[[str | None], list[str]]] = {
    "uuid": gen_uuid,
    "ts": gen_ts,
    "rand": gen_rand,
}


def parse_token(interior: str) -> tuple[str, str | None]:
    """Split a token's interior into ``(name, opts)`` on the first ``:``.

    The token *interior* is the text between ``{{`` and ``}}`` (matched by the
    caller — this function does no template scanning). Everything after the
    first ``:`` is ``opts`` verbatim; the named generator validates it.

    Examples::

        parse_token("uuid")    -> ("uuid", None)
        parse_token("uuid:2")  -> ("uuid", "2")
        parse_token("ts:ms")   -> ("ts", "ms")
    """
    if ":" in interior:
        name, _, opts = interior.partition(":")
        return name, opts
    return interior, None


# Matches ``{{name[:opts]}}``. The name is ``[A-Za-z_][A-Za-z0-9_]*``; opts is
# an optional ``:`` followed by any run of non-``}`` chars. The brace-boundary
# lookarounds (``(?<!\{)`` / ``(?!\})``) ensure triple-braced tokens such as
# ``{{{x}}}`` / ``{{{uuid}}}`` do NOT match — the inner ``{{x}}`` is bracketed
# by extra braces and must be left literal. Tokens containing dots or spaces
# (e.g. ``{{user.name}}``, ``{{ x }}``) also do not match.
TEMPLATE_RE = re.compile(
    r"(?<!\{)\{\{[A-Za-z_][A-Za-z0-9_]*(?::[^}]*)?\}\}(?!\})"
)


def substitute_generators(value, memo: dict[str, str], *, enabled: bool = True):
    """Recursively substitute ``{{name[:opts]}}`` tokens in ``value``.

    ``value`` may be a ``str``, ``dict``, or ``list``; containers are rebuilt
    with the same type, recursing into their values. For a string, every
    ``TEMPLATE_RE`` match is replaced:

    * The **full matched text** (including ``{{`` and ``}}``) is the memo key.
      A repeat occurrence reuses the cached value.
    * Otherwise ``parse_token`` extracts ``(name, opts)``; the generator
      ``BUILTIN_GENERATORS[name]`` is called with ``opts``; its returned list
      is joined to a single string (1-element → that element; n-element →
      joined by a single space) and stored under the memo key.
    * A name absent from ``BUILTIN_GENERATORS`` raises ``ConfigError`` whose
      message names the token and lists the valid generators.
    * Text that does not match ``TEMPLATE_RE`` is left unchanged.

    ``enabled=False`` returns ``value`` unchanged (no scan, no errors) — this is
    the ``--no-template-vars`` path.
    """
    if not enabled:
        return value
    if isinstance(value, dict):
        return {k: substitute_generators(v, memo) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute_generators(v, memo) for v in value]
    if isinstance(value, str):
        return _substitute_str(value, memo)
    return value


def _substitute_str(s: str, memo: dict[str, str]) -> str:
    """Replace every ``TEMPLATE_RE`` match in ``s`` using ``memo``."""

    def repl(match: re.Match[str]) -> str:
        full = match.group(0)
        if full in memo:
            return memo[full]
        interior = full[2:-2]  # strip the surrounding {{ }}
        name, opts = parse_token(interior)
        gen = BUILTIN_GENERATORS.get(name)
        if gen is None:
            valid = ", ".join(sorted(BUILTIN_GENERATORS))
            raise ConfigError(
                f"unknown template generator {name!r} in token {full!r}; "
                f"valid generators: {valid}"
            )
        parts = gen(opts)
        joined = parts[0] if len(parts) == 1 else " ".join(parts)
        memo[full] = joined
        return joined

    return TEMPLATE_RE.sub(repl, s)


def find_unknown_templates(value) -> list[str]:
    """Return full token texts in ``value`` whose name is not a known generator.

    Recurses the value tree (``str`` / ``dict`` / ``list``). Returns the full
    matched token texts (including ``{{`` and ``}}``), deduped and in
    first-occurrence order. This is a pure scan for the config validator: it
    performs **no generation and never raises** — invalid opts or unknown
    generators are reported as-is without calling any generator.
    """
    found: list[str] = []
    seen: set[str] = set()

    def scan(node) -> None:
        if isinstance(node, dict):
            for item in node.values():
                scan(item)
        elif isinstance(node, list):
            for item in node:
                scan(item)
        elif isinstance(node, str):
            for match in TEMPLATE_RE.finditer(node):
                full = match.group(0)
                if full in seen:
                    continue
                name, _ = parse_token(full[2:-2])
                if name not in BUILTIN_GENERATORS:
                    seen.add(full)
                    found.append(full)

    scan(value)
    return found
