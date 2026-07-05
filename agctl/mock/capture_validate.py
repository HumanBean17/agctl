"""Static placement check for ``object``-typed captures (Task 5).

An ``object``-typed capture name ``N`` may only be used as a WHOLE-FIELD
placeholder — a string whose value is exactly ``"{N}"`` — inside ``response.body``
(HTTP) / ``reaction.value`` (Kafka). Used anywhere else it would render
incorrectly at request time (an object forced into a string slot has no honest
string form). Rather than discover this on the first matching request, this
module scans a :class:`MocksConfig` at startup / validate time and reports each
violation as a ``{"path", "message"}`` record, mirroring
:func:`collect_jq_compile_errors`.

Placement rule for an ``object``-typed name ``N`` (per stub/reactor):

- VALID: some field in ``response.body`` / ``reaction.value`` is exactly ``"{N}"``.
- VIOLATION (one error each):
  - (a) ``"{N}"`` appears inline within a larger string anywhere in the
    ``response.body`` / ``reaction.value`` tree.
  - (b) ``reaction.key`` is or contains ``"{N}"`` (Kafka only — string-only slot).
  - (c) any ``reaction.headers`` value is or contains ``"{N}"`` (Kafka only).
- ``scalar``/``json``-typed names are NEVER flagged.

Pure Python: imports only :mod:`config.models` and inlines a placeholder regex
(no :mod:`resolution` import) — no jq, no ``assertions`` dependency. That keeps
``config/*`` free of an assertions dependency when ``config_commands.py`` calls this.
"""

from __future__ import annotations

import re
from typing import Any

from ..config.models import MocksConfig

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

__all__ = ["collect_capture_placement_errors"]


def _classify(s: str, name: str) -> str:
    """Classify string ``s`` w.r.t. the ``{name}`` placeholder.

    Returns ``"whole"`` when ``s`` is exactly ``"{name}"``, ``"inline"`` when
    ``{name}`` appears within a larger string, ``"none"`` when ``{name}`` is
    absent. The whole-field case is the one valid placement for an object
    capture inside body/value; inline is a violation; key/headers treat any
    occurrence (whole or inline) as a violation (string-only slots).
    """
    full = _PLACEHOLDER_RE.fullmatch(s)
    if full is not None and full.group(1) == name:
        return "whole"
    for m in _PLACEHOLDER_RE.finditer(s):
        if m.group(1) == name:
            return "inline"
    return "none"


def _walk_tree(value: Any, name: str) -> bool:
    """Walk a body/value tree; return True if ``{name}`` appears inline anywhere.

    A whole-field occurrence (a string exactly ``"{name}"``) is the valid
    placement and does NOT set the flag — only inline appearances do. Dict
    values and list elements are recursed into.
    """
    if isinstance(value, str):
        return _classify(value, name) == "inline"
    if isinstance(value, dict):
        return any(_walk_tree(v, name) for v in value.values())
    if isinstance(value, list):
        return any(_walk_tree(v, name) for v in value)
    return False


def collect_capture_placement_errors(mocks: MocksConfig | None) -> list[dict]:
    """Scan ``mocks`` for object-capture misplacement; return one record per violation.

    For each HTTP stub / Kafka reactor whose ``capture`` declares a ``type ==
    "object"`` name ``N``, scans the relevant template tree(s) and appends a
    ``{"path": str, "message": str}`` per violation category (inline-in-body/value,
    ``reaction.key``, ``reaction.headers``). At most one error per category per
    name; up to three errors per name (inline + key + headers). ``scalar``/``json``
    captures and stubs/reactors with ``capture=None`` contribute nothing.

    ``mocks is None`` (or its ``http``/``kafka`` subsections None) -> ``[]``.
    Never raises — callers (``config validate``, ``MockEngine.start()``) decide
    whether to collect-and-report or fail-fast on the first record.
    """
    if mocks is None:
        return []

    errors: list[dict] = []

    if mocks.http is not None:
        for name, stub in mocks.http.stubs.items():
            if stub.capture is None:
                continue
            for cap_name, spec in stub.capture.items():
                if spec.type != "object":
                    continue
                path = f"mocks.http.stubs.{name}"
                # (a) inline within response.body (HTTP has no key/headers slot).
                if _walk_tree(stub.response.body, cap_name):
                    errors.append({
                        "path": path,
                        "message": (
                            f'capture {cap_name!r} of type "object" must occupy '
                            f'the whole field ("{{{cap_name}}}"); it appears inline '
                            f"within a larger string in response.body"
                        ),
                    })

    if mocks.kafka is not None:
        for name, reactor in mocks.kafka.reactors.items():
            if reactor.capture is None:
                continue
            for cap_name, spec in reactor.capture.items():
                if spec.type != "object":
                    continue
                path = f"mocks.kafka.reactors.{name}"
                reaction = reactor.reaction

                # (a) inline within reaction.value.
                if _walk_tree(reaction.value, cap_name):
                    errors.append({
                        "path": path,
                        "message": (
                            f'capture {cap_name!r} of type "object" must occupy '
                            f'the whole field ("{{{cap_name}}}"); it appears inline '
                            f"within a larger string in reaction.value"
                        ),
                    })

                # (b) reaction.key — string-only slot, any occurrence is a
                # violation (even a whole-field "{N}" cannot hold an object).
                if reaction.key is not None and _classify(reaction.key, cap_name) in ("whole", "inline"):
                    errors.append({
                        "path": path,
                        "message": (
                            f'capture {cap_name!r} of type "object" cannot be used '
                            f"in reaction.key (a string-only slot); object captures "
                            f"must occupy a whole field in reaction.value"
                        ),
                    })

                # (c) reaction.headers — string-only slot, any value is-or-contains
                # "{N}" -> one error (one per name for this category).
                if reaction.headers is not None:
                    for h_key, h_val in reaction.headers.items():
                        if _classify(h_val, cap_name) in ("whole", "inline"):
                            errors.append({
                                "path": path,
                                "message": (
                                    f'capture {cap_name!r} of type "object" cannot '
                                    f"be used in reaction.headers.{h_key} "
                                    f"(a string-only slot)"
                                ),
                            })
                            break

    return errors
