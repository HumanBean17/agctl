"""Pure-function reader for a topic's captured ``<topic>.ndjson`` file.

The companion to :mod:`agctl.listen.daemon`: where that module owns the
listen-daemon *lifecycle* (pidfiles, run dirs, events log), this module owns
the *payload* — the per-topic NDJSON capture that the daemon appends to while
running and that later ``listen assert``/``listen messages`` tasks read back.

Each line of the capture file is one **CapturedEnvelope**::

    {topic, key, value, partition, offset, timestamp, headers, captured_at}

``value`` is JSON-decoded when parseable, else the raw decoded string. This is
the SAME envelope root as ``kafka assert`` consumes, so the predicate machinery
(``--contains`` / ``--match`` / ``--path`` / ``--pattern``) is reused verbatim:
:func:`build_predicate` validates each present jq expression up front
(loud-on-typo → :class:`ConfigError`) and then delegates to
:func:`agctl.commands.kafka_commands._build_assert_predicate`.

The module is deliberately pure — only ``pathlib.Path`` + ``json`` + the
assertion helpers. No Kafka client, no daemon process, no network. A missing
capture file is treated as "no messages captured yet": every function returns
an empty/zero result rather than raising (the daemon has not necessarily
flushed anything to disk when a reader first asks).

Functions:

- :func:`iter_messages` — yield parsed envelopes, skipping blank/unparseable.
- :func:`count_matching` — ``(matched, scanned)``; counts ALL, no short-circuit.
- :func:`first_matching` — ``(envelope|None, scanned)``; stops at first match.
- :func:`read_messages` — ``{matched, truncated, messages}``; optional
  predicate then capped at ``limit``.
- :func:`build_predicate` — translate a resolved modes dict into the predicate
  used by the readers above.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Iterator

from ..assertions import compile_jq
from ..commands.kafka_commands import _build_assert_predicate
from ..errors import ConfigError

__all__ = [
    "iter_messages",
    "count_matching",
    "first_matching",
    "read_messages",
    "build_predicate",
]


def iter_messages(path: Path) -> Iterator[dict[str, Any]]:
    """Yield each parsed CapturedEnvelope from an NDJSON capture file.

    Blank and unparseable lines are skipped silently (the daemon's append
    loop may have written a partial line on a crash, and the capture file is
    not authoritative line-wise). A missing file yields nothing — it means
    the daemon has not yet captured any messages for this topic.

    Args:
        path: Path to ``<run_dir>/<topic>.ndjson`` (need not exist).

    Yields:
        Parsed CapturedEnvelope dicts in file order.
    """
    if not path.exists():
        return

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        yield obj


def _safe_predicate(predicate: Callable[[dict[str, Any]], bool]) -> Callable[[dict[str, Any]], bool]:
    """Wrap a predicate so a per-message exception is swallowed → non-match.

    Mirrors ``kafka assert`` (DESIGN §3.2): a predicate that raises on a given
    message is treated as a non-match for that message, not propagated. The
    predicate is still validated up front via :func:`build_predicate`, so this
    only absorbs runtime failures (e.g. a jq expression hitting an unexpected
    message shape).
    """

    def wrapped(msg: dict[str, Any]) -> bool:
        try:
            return bool(predicate(msg))
        except Exception:
            return False

    return wrapped


def count_matching(
    path: Path, predicate: Callable[[dict[str, Any]], bool]
) -> tuple[int, int]:
    """Count every matching envelope in a capture file (no short-circuit).

    Unlike :func:`first_matching`, this scans the WHOLE file so the matched
    count is exact — used by ``listen assert`` to verify an ``expect_count``
    floor. A missing file returns ``(0, 0)``.

    Args:
        path: Path to the NDJSON capture (need not exist).
        predicate: Boolean predicate over a CapturedEnvelope. Per-message
            exceptions are swallowed → non-match.

    Returns:
        ``(matched_count, scanned_count)`` where ``scanned_count`` is the
        number of successfully parsed envelopes inspected.
    """
    safe = _safe_predicate(predicate)
    matched = 0
    scanned = 0
    for msg in iter_messages(path):
        scanned += 1
        if safe(msg):
            matched += 1
    return matched, scanned


def first_matching(
    path: Path, predicate: Callable[[dict[str, Any]], bool]
) -> tuple[dict[str, Any] | None, int]:
    """Return the first matching envelope and the number of envelopes scanned.

    Stops at the first match (``scanned`` includes the matching envelope). A
    missing file or no-match returns ``(None, <scanned>)``.

    Args:
        path: Path to the NDJSON capture (need not exist).
        predicate: Boolean predicate over a CapturedEnvelope. Per-message
            exceptions are swallowed → non-match.

    Returns:
        ``(matching_envelope | None, scanned_count)``.
    """
    safe = _safe_predicate(predicate)
    scanned = 0
    for msg in iter_messages(path):
        scanned += 1
        if safe(msg):
            return msg, scanned
    return None, scanned


def read_messages(
    path: Path,
    *,
    predicate: Callable[[dict[str, Any]], bool] | None,
    limit: int,
) -> dict[str, Any]:
    """Read up to ``limit`` matching messages from a capture file.

    Applies the optional ``predicate`` first (matching envelopes counted), then
    caps the returned ``messages`` list at ``limit``. ``truncated`` is True iff
    MORE envelopes matched than ``limit`` allows back — i.e. there is additional
    data the caller did not see.

    Args:
        path: Path to the NDJSON capture (need not exist).
        predicate: Optional boolean predicate; ``None`` admits every envelope.
        limit: Maximum number of envelopes to return.

    Returns:
        ``{"matched": int, "truncated": bool, "messages": list[dict]}`` where
        ``matched`` is the total number of matching envelopes in the file
        (independent of ``limit``).
    """
    safe = _safe_predicate(predicate) if predicate is not None else None
    matched = 0
    messages: list[dict[str, Any]] = []
    for msg in iter_messages(path):
        if safe is not None and not safe(msg):
            continue
        matched += 1
        if len(messages) < limit:
            messages.append(msg)
    truncated = matched > len(messages)
    return {"matched": matched, "truncated": truncated, "messages": messages}


def build_predicate(
    spec: dict[str, Any],
) -> Callable[[dict[str, Any]], bool]:
    """Build a predicate over a CapturedEnvelope from a resolved modes dict.

    Translates the (already-filled) expectation modes into the keyword args for
    :func:`agctl.commands.kafka_commands._build_assert_predicate`, after:

    - parsing ``contains`` (a JSON string) into ``needle`` via ``json.loads``;
    - compile-validating each present jq expression (``match`` / ``path`` /
      ``filled_pattern_match``) with clear labels so a typo raises
      :class:`ConfigError` LOUDLY up front, before any message is scanned.

    Pattern resolution (named-pattern lookup + placeholder fill) happens in the
    command layer, NOT here: callers pass the already-filled jq expression as
    ``filled_pattern_match``. This mirrors ``kafka assert``'s split between arg
    gathering and predicate construction.

    Args:
        spec: ``{contains: any|None, match: str|None, path: str|None,
            filled_pattern_match: str|None}``. ``contains`` may be a pre-parsed
            value OR a JSON string (a JSON string is parsed to keep parity with
            the ``--contains`` CLI flag); ``None`` means the mode is unused.

    Returns:
        A predicate ``Callable[[dict], bool]``. Per-message exceptions raised
        by the predicate are swallowed by the readers above (not here) — this
        builder only compiles/validates, it does not evaluate against any
        message.

    Raises:
        json.JSONDecodeError: If ``contains`` is a string that is not valid
            JSON (matches ``kafka assert``'s ``json.loads(contains)`` behavior;
            the command layer surfaces this as a :class:`ConfigError`).
        ConfigError: If ``match``, ``path``, or ``filled_pattern_match`` is a
            malformed jq expression.
    """
    contains = spec.get("contains")
    match = spec.get("match")
    path = spec.get("path")
    filled_pattern_match = spec.get("filled_pattern_match")

    # --contains: parse a JSON string into the needle (a pre-parsed value
    # passes through unchanged). Matches `_kafka_assert_core`'s
    # `json.loads(contains)`, which is what the predicate's --contains mode
    # expects (a dict/list/scalar needle, not a string).
    needle: Any = None
    if contains is not None:
        if isinstance(contains, str):
            needle = json.loads(contains)
        else:
            needle = contains

    # Validate each present jq expression ONCE up front. The predicate swallows
    # per-message jq errors (returns False, DESIGN §3.2), so a typo'd
    # --match/--path/--pattern would otherwise silently never match.
    if match is not None:
        compile_jq(match, label="kafka listen --match")
    if path is not None:
        compile_jq(path, label="kafka listen --path")
    if filled_pattern_match is not None:
        compile_jq(filled_pattern_match, label="kafka listen --pattern")

    return _build_assert_predicate(
        needle=needle,
        match=match,
        path=path,
        filled_pattern_match=filled_pattern_match,
    )
