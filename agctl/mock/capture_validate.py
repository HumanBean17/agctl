"""Static placement check for ``object``-typed captures (Task 5).

An ``object``-typed capture name ``N`` may only be used as a WHOLE-FIELD
placeholder — a string whose value is exactly ``"{N}"`` — inside ``response.body``
(HTTP) / ``reaction.value`` (Kafka) / ``response.message`` and
``response.messages[*].message`` (gRPC). Used anywhere else it would render
incorrectly at request time (an object forced into a string slot has no honest
string form). Rather than discover this on the first matching request, this
module scans a :class:`MocksConfig` at startup / validate time and reports each
violation as a ``{"path", "message"}`` record, mirroring
:func:`collect_jq_compile_errors`.

Placement rule for an ``object``-typed name ``N`` (per stub/reactor):

- VALID: some field in ``response.body`` / ``reaction.value`` / gRPC
  ``response.message`` (or any ``response.messages[*].message``) is exactly
  ``"{N}"``.
- VIOLATION (one error each):
  - (a) ``"{N}"`` appears inline within a larger string anywhere in the
    ``response.body`` / ``reaction.value`` / gRPC message tree(s).
  - (b) ``reaction.key`` is or contains ``"{N}"`` (Kafka only — string-only slot).
  - (c) any ``reaction.headers`` value is or contains ``"{N}"`` (Kafka only).
- ``scalar``/``json``-typed names are NEVER flagged.

Effects rule (DESIGN: mock effects): for each HTTP effect capture of type
``object`` on a stub/reactor, the whole-object ``"{N}"`` placeholder may appear
as a WHOLE field in a LATER effect's dict-capable slots — ``value`` /
``values[*].value`` / ``body`` / ``path``. The string-only slots — a later
effect's ``key`` / ``headers`` (top-level and per-``values[*]``-item) — treat
ANY ``{N}`` occurrence (whole or nested) as a violation, mirroring
``reaction.key`` / ``reaction.headers``: ``render_typed`` would hand the live
dict to the producer as a non-str key / header value. A non-whole occurrence
(inline within a larger string, or a nested ``"{N.inner}"`` token, which the
placeholder regex does not match and would render literally) is one violation
per name at ``...effects[i].capture.{N}``. An object capture no later effect
uses is ALLOWED (chained captures may go unused). Effects-only reactors
(``reaction is None``) skip the reaction checks — there is no reaction to
misplace captures in.

Pure Python: imports only :mod:`config.models` and inlines a placeholder regex
(no :mod:`resolution` import) — no jq, no ``assertions`` dependency. That keeps
``config/*`` free of an assertions dependency when ``config_commands.py`` calls this.
"""

from __future__ import annotations

import re
from typing import Any

from ..config.models import Effect, MocksConfig

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
# Same shape as _PLACEHOLDER_RE but the name may carry a dotted suffix
# (``{ctx.inner}``). Used ONLY by the effects walk: a dotted token is never a
# whole-object placement, and it would silently render literally at runtime
# (the plain regex does not match it) — so it is flagged as a violation.
_NESTED_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z0-9_]+)+)\}")

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


def _classify_effect(s: str, name: str) -> str:
    """Classify string ``s`` w.r.t. the ``{name}`` placeholder (effects walk).

    Returns ``"whole"`` when ``s`` is exactly ``"{name}"``, ``"inline"`` when
    ``{name}`` appears any other way — inline within a larger string
    (``"pre={name}"``) or as a nested token (``"{name.inner}"``, matched by
    ``_NESTED_PLACEHOLDER_RE`` because the plain regex does not see dotted
    names, which would render literally at runtime) — and ``"none"`` when
    ``{name}`` is absent. Mirrors :func:`_classify` but folds nested tokens
    into ``"inline"``.
    """
    if s == f"{{{name}}}":
        return "whole"
    for m in _PLACEHOLDER_RE.finditer(s):
        if m.group(1) == name:
            return "inline"
    for m in _NESTED_PLACEHOLDER_RE.finditer(s):
        if m.group(1).split(".", 1)[0] == name:
            return "inline"
    return "none"


def _walk_effect_tree(value: Any, name: str, *, string_only: bool = False) -> bool:
    """Walk a later effect's slot tree; True when ``{name}`` is misplaced.

    Mirrors :func:`_walk_tree` (dict values / list elements recursed;
    whole-field strings allowed) but flags nested ``{name.inner}`` tokens too.
    With ``string_only`` the slot cannot carry an object at all — ANY
    occurrence (whole or inline/nested) is a violation, mirroring the
    ``reaction.key``/``reaction.headers`` checks.
    """
    if isinstance(value, str):
        c = _classify_effect(value, name)
        return c in ("whole", "inline") if string_only else c == "inline"
    if isinstance(value, dict):
        return any(_walk_effect_tree(v, name, string_only=string_only) for v in value.values())
    if isinstance(value, list):
        return any(_walk_effect_tree(v, name, string_only=string_only) for v in value)
    return False


def _effect_slot_values(later: Effect) -> list[tuple[Any, bool]]:
    """Collect ``(tree, string_only)`` pairs for a LATER effect's renderable slots.

    ``string_only`` marks slots that cannot carry an object at all — ``key`` /
    ``headers`` (top-level and per-``values[*]``-item), mirroring
    ``reaction.key`` / ``reaction.headers``. kafka effect: ``value`` plus every
    ``values[*]`` item's ``value``/``key``/``headers`` (when ``values`` is set,
    top-level ``key``/``headers`` are defaults, not rendered slots — the
    per-item ones are). http effect: ``body``, ``path``, ``headers`` (``url``
    is a literal base — placeholders ride on ``path``). Trees may be None; the
    walker ignores non-str leaves.
    """
    if later.type == "kafka":
        slots: list[tuple[Any, bool]] = [(later.key, True), (later.headers, True)]
        if later.values is not None:
            for item in later.values:
                slots.extend(
                    ((item.value, False), (item.key, True), (item.headers, True))
                )
        else:
            slots.append((later.value, False))
        return slots
    return [(later.body, False), (later.path, False), (later.headers, True)]


def _effect_capture_errors(
    carrier_label: str, effects: list[Effect] | None
) -> list[dict]:
    """Collect object-capture placement errors for HTTP effects on one carrier.

    For each HTTP effect (index ``i``) capture of ``type == "object"``, every
    SUBSEQUENT effect in the same list is scanned: a whole-field ``"{name}"``
    placement is valid in the dict-capable slots (``value`` / ``values[*].value``
    / ``body`` / ``path``), but ``key`` / ``headers`` are string-only — ANY
    occurrence there is a violation; any non-whole occurrence (inline or nested
    ``{name.x}``) is a violation everywhere. One violation for that name at
    ``{carrier}.effects[i].capture.{name}``.
    An object capture no later effect references is allowed (soft posture).
    Kafka effects carry no capture. Never raises; ``effects is None`` -> [].
    """
    if effects is None:
        return []
    errors: list[dict] = []
    for i, effect in enumerate(effects):
        if effect.type != "http" or effect.capture is None:
            continue
        for cap_name, spec in effect.capture.items():
            if spec.type != "object":
                continue
            if not any(
                _walk_effect_tree(slot, cap_name, string_only=string_only)
                for later in effects[i + 1 :]
                for slot, string_only in _effect_slot_values(later)
            ):
                continue
            errors.append({
                "path": f"{carrier_label}.effects[{i}].capture.{cap_name}",
                "message": (
                    f'capture {cap_name!r} of type "object" must occupy '
                    f"the whole field (\"{{{cap_name}}}\") in a later effect's "
                    f"value/body/path and cannot be used in key/headers "
                    f"(string-only slots)"
                ),
            })
    return errors


def collect_capture_placement_errors(mocks: MocksConfig | None) -> list[dict]:
    """Scan ``mocks`` for object-capture misplacement; return one record per violation.

    For each HTTP stub / Kafka reactor / gRPC stub whose ``capture`` declares a
    ``type == "object"`` name ``N``, scans the relevant template tree(s) and
    appends a ``{"path": str, "message": str}`` per violation category
    (inline-in-body/value/message, ``reaction.key``, ``reaction.headers``).
    At most one error per category per name; up to three errors per name
    (inline + key + headers). For gRPC, only the inline-in-message category
    applies: both the unary ``response.message`` tree and every streaming
    ``response.messages[*].message`` tree are walked. ``scalar``/``json``
    captures and stubs/reactors with ``capture=None`` contribute nothing.

    Additionally walks each HTTP effect's ``capture`` (both carriers): an
    object capture misplaced in a SUBSEQUENT effect's fields (non-whole
    anywhere, or any occurrence in the string-only ``key``/``headers`` slots)
    is one violation per name, reported at ``...effects[i].capture.{N}``
    (unused object captures are allowed). Effects-only reactors
    (``reaction is None``) skip the reaction checks. ``scalar``/``json``
    effect captures contribute nothing.

    ``mocks is None`` (or its ``http``/``kafka``/``grpc`` subsections None)
    -> ``[]``. Never raises — callers (``config validate``,
    ``MockEngine.start()``) decide whether to collect-and-report or fail-fast
    on the first record.
    """
    if mocks is None:
        return []

    errors: list[dict] = []

    if mocks.http is not None:
        for name, stub in mocks.http.stubs.items():
            # Effects first: object captures on this stub's http effects are
            # checked against SUBSEQUENT effects (a captured value only exists
            # after the capturing effect runs) — independent of the stub's own
            # capture (which may be None).
            errors.extend(
                _effect_capture_errors(f"mocks.http.stubs.{name}", stub.effects)
            )
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
            errors.extend(
                _effect_capture_errors(f"mocks.kafka.reactors.{name}", reactor.effects)
            )
            if reactor.capture is None:
                continue
            for cap_name, spec in reactor.capture.items():
                if spec.type != "object":
                    continue
                path = f"mocks.kafka.reactors.{name}"
                reaction = reactor.reaction

                # Effects-only reactor (legal since T1): no reaction, so there
                # is nothing to misplace a capture in — skip the checks below.
                if reaction is None:
                    continue

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

    if mocks.grpc is not None:
        for name, stub in mocks.grpc.stubs.items():
            if stub.capture is None:
                continue
            for cap_name, spec in stub.capture.items():
                if spec.type != "object":
                    continue
                path = f"mocks.grpc.stubs.{name}"
                response = stub.response
                # gRPC has no key/headers analogue — only the inline-in-message
                # tree category (like HTTP's response.body). Walk BOTH the unary
                # response.message tree and every streaming messages[*].message
                # tree; a single inline occurrence anywhere is one violation.
                trees: list[Any] = []
                if response.message is not None:
                    trees.append(response.message)
                if response.messages is not None:
                    trees.extend(m.message for m in response.messages)
                if any(_walk_tree(tree, cap_name) for tree in trees):
                    errors.append({
                        "path": path,
                        "message": (
                            f'capture {cap_name!r} of type "object" must occupy '
                            f'the whole field ("{{{cap_name}}}"); it appears inline '
                            f"within a larger string in response.message"
                        ),
                    })

    return errors
