"""Walk a :class:`MocksConfig` and pre-validate every jq match expression.

Two helpers consumed by the mock engine (Task 5, ``MockEngine.start()`` Step 0
— raises on the first error) and by ``config validate`` (Task 10 — collects
ALL errors for a single report):

- :func:`iter_mock_jq_expressions` — generator yielding ``(path_label, expr)``
  for every HTTP stub with a non-None ``match.jq``, every Kafka reactor with
  a non-None ``match``, and every gRPC stub with a non-None ``match.jq``.
  HTTP stubs are yielded first (in dict order), then Kafka reactors, then
  gRPC stubs. HTTP effects riding on a stub/reactor contribute one
  ``effects[i].capture.{cap}.from`` label per capture (kafka effects have no
  capture).
- :func:`collect_jq_compile_errors` — drives :func:`compile_jq` over every
  expression the walker emits, catching :class:`ConfigError` so a single pass
  surfaces every authoring typo (rather than stopping at the first).

Dependency direction is intentionally one-way: ``mock`` may depend on
``config.models`` (read-only traversal) and ``assertions`` (the compile guard),
but neither of those depends on ``mock``.
"""

from collections.abc import Iterator

from ..assertions import compile_jq
from ..config.models import Effect, MocksConfig
from ..errors import ConfigError


def iter_mock_jq_expressions(
    mocks: MocksConfig | None,
) -> Iterator[tuple[str, str]]:
    """Yield ``(path_label, expr)`` for every jq expression in ``mocks``.

    Iterates HTTP stubs first (in dict order), then Kafka reactors (in dict
    order), then gRPC stubs (in dict order). For each stub/reactor, yields its
    ``match.jq`` / ``match`` (when not None) and then, when it carries a
    non-None ``capture``, one ``capture.{cap}.from`` entry per capture (in dict
    order) — the capture ``from`` is itself a jq expression that must compile.
    gRPC stubs mirror HTTP stubs: ``match.jq`` first, then each
    ``capture.{cap}.from``. A stub/reactor with ``capture=None`` (or no match)
    contributes no capture labels.

    Each HTTP effect in a stub/reactor's ``effects`` list (in list order)
    appends one ``effects[i].capture.{cap}.from`` label per capture (in dict
    order) after the carrier's own labels — an HTTP effect's capture ``from``
    is a jq expression rooted in the call's response envelope, so a malformed
    one must fail at startup, not silently capture empty. Kafka effects carry
    no capture and contribute nothing; an ``effects`` of None is skipped.

    Walking the capture ``from`` here means :func:`collect_jq_compile_errors`
    (used by ``config validate``) and the engine's Step 0 pre-compile (used by
    ``mock run``) both surface a malformed ``from`` at the capture label,
    automatically — no caller changes needed.

    ``mocks is None`` (or its ``http``/``kafka``/``grpc`` subsections are None)
    yields nothing — the caller may pass a Config with mocks disabled without
    guarding.
    """
    if mocks is None:
        return

    if mocks.http is not None:
        for name, stub in mocks.http.stubs.items():
            if stub.match is not None and stub.match.jq is not None:
                yield f"mocks.http.stubs.{name}.match.jq", stub.match.jq
            if stub.capture is not None:
                for cap, spec in stub.capture.items():
                    yield f"mocks.http.stubs.{name}.capture.{cap}.from", spec.from_
            yield from _iter_http_effect_expressions(
                f"mocks.http.stubs.{name}", stub.effects
            )

    if mocks.kafka is not None:
        for name, reactor in mocks.kafka.reactors.items():
            if reactor.match is not None:
                yield f"mocks.kafka.reactors.{name}.match", reactor.match
            if reactor.capture is not None:
                for cap, spec in reactor.capture.items():
                    yield (
                        f"mocks.kafka.reactors.{name}.capture.{cap}.from",
                        spec.from_,
                    )
            yield from _iter_http_effect_expressions(
                f"mocks.kafka.reactors.{name}", reactor.effects
            )

    if mocks.grpc is not None:
        for name, stub in mocks.grpc.stubs.items():
            if stub.match is not None and stub.match.jq is not None:
                yield f"mocks.grpc.stubs.{name}.match.jq", stub.match.jq
            if stub.capture is not None:
                for cap, spec in stub.capture.items():
                    yield f"mocks.grpc.stubs.{name}.capture.{cap}.from", spec.from_


def _iter_http_effect_expressions(
    carrier_label: str,
    effects: list[Effect] | None,
) -> Iterator[tuple[str, str]]:
    """Yield ``(label, expr)`` for every HTTP effect capture on one carrier.

    ``carrier_label`` is the owning stub/reactor path (e.g.
    ``mocks.http.stubs.s``); each HTTP effect at index ``i`` contributes one
    ``{carrier}.effects[i].capture.{cap}.from`` per capture, in list/dict
    order. Kafka effects have no ``capture`` field and contribute nothing;
    ``effects is None`` yields nothing. Shared by both carriers so the label
    format stays identical (DESIGN: mock effects).
    """
    if effects is None:
        return
    for i, effect in enumerate(effects):
        if effect.type != "http" or effect.capture is None:
            continue
        for cap, spec in effect.capture.items():
            yield (
                f"{carrier_label}.effects[{i}].capture.{cap}.from",
                spec.from_,
            )


def collect_jq_compile_errors(mocks: MocksConfig | None) -> list[dict]:
    """Compile every jq expression in ``mocks``; collect ALL errors.

    Calls :func:`compile_jq` (compile-only — no value applied) on each
    ``(label, expr)`` from :func:`iter_mock_jq_expressions`. On
    :class:`ConfigError` appends ``{"path": label, "message": err.message}``
    and continues — never raises, so ``config validate`` can report every
    typo in a single run. Returns ``[]`` when ``mocks`` is None or every
    expression compiles cleanly.
    """
    errors: list[dict] = []
    for label, expr in iter_mock_jq_expressions(mocks):
        try:
            compile_jq(expr, label=label)
        except ConfigError as exc:
            errors.append({"path": label, "message": exc.message})
    return errors
