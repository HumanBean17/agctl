"""Template variable value generators and token parsing (Task 1).

This module is the foundation of the template-variables feature. It provides:

* ``BUILTIN_GENERATORS`` — a registry of named value generators
  (``uuid`` / ``ts`` / ``rand``). Each generator takes an ``opts: str | None``
  and returns a ``list[str]`` (always a list; single-value generators return a
  1-element list).
* ``parse_token`` — splits a token's interior (the text between ``{{`` and
  ``}}``) into ``(name, opts)`` on the first ``:``.

Substitution (iterating a template, finding tokens, calling generators, and
splicing results back) is Task 2 and is deliberately not implemented here.

The character set of every generator's output is restricted to
``[0-9a-fA-FT:Z -]`` (the "charset invariant"), pinned by a parametrized
property test in ``tests/unit/test_template_vars.py``.
"""

from __future__ import annotations

import datetime
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
