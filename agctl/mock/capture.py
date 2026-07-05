"""Envelope capture resolver for mock stubs and Kafka reactors.

:func:`resolve_captures` reads each :class:`CaptureSpec`'s ``from_`` jq path
off the live envelope, wraps the result in a typed :class:`CaptureValue`,
and returns ``(typed_map, missing_list)``. It is the bridge between the
config model (Task 1) and the typed renderer (Task 2): the caller hands in
the parsed envelope plus a stub/reactor's ``capture`` mapping, and gets
back exactly what :func:`render_typed` consumes.

Soft-miss contract: a path that yields nothing (or a jq expression that
errors at runtime) sets ``CaptureValue(None, type)`` in the typed map AND
appends ``(name, from_)`` to the missing list — downstream rendering turns
``None`` into ``""`` (the empty string). A *missing jq library* is a
configuration problem, not a soft miss: :func:`jq_value` raises
:class:`ConfigError` and this resolver propagates it (exit 2).

Dependency direction is one-way: ``mock`` may consume ``assertions`` (the
jq evaluator), ``config.models`` (the spec), and ``resolution`` (the typed
value), but none of those depend on ``mock``.
"""

from ..assertions import jq_value
from ..config.models import CaptureSpec
from ..resolution import CaptureValue


def resolve_captures(
    envelope: dict,
    captures: dict[str, CaptureSpec] | None,
) -> tuple[dict[str, CaptureValue], list[tuple[str, str]]]:
    """Resolve a capture spec map against ``envelope`` into typed values + missing.

    For each ``(name, spec)`` in ``captures`` (insertion order):

    1. ``raw = jq_value(envelope, spec.from_)`` — the first jq output, or
       ``None`` if the expression errors or yields nothing.
    2. Place ``CaptureValue(raw, spec.type)`` in the typed map under ``name``
       (so a missing path still occupies its slot as
       ``CaptureValue(None, type)`` — the renderer turns that into ``""``).
    3. When ``raw is None``, also append ``(name, spec.from_)`` to the
       missing list (callers may surface misses without re-querying).

    ``captures is None`` or empty short-circuits to ``({}, [])``.

    A missing ``jq`` library surfaces from :func:`jq_value` as a
    :class:`ConfigError` and is re-raised here (NOT swallowed) — that is a
    configuration problem (exit 2), distinct from a per-path soft miss.
    """
    if not captures:
        return {}, []

    typed: dict[str, CaptureValue] = {}
    missing: list[tuple[str, str]] = []
    for name, spec in captures.items():
        # jq_value raises ConfigError ONLY on a missing jq library (propagate).
        # jq expression/runtime errors are swallowed to None inside jq_value,
        # so they surface below as a missing entry — the intended soft-miss.
        raw = jq_value(envelope, spec.from_)
        typed[name] = CaptureValue(raw, spec.type)
        if raw is None:
            missing.append((name, spec.from_))
    return typed, missing
