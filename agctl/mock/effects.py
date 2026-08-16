"""Cross-transport effect executor (DESIGN: mock effects, Task 2).

The executor is the pure dispatch core behind a stub's/reactor's ``effects``
list: resolve ``{{gen}}`` tokens (one memo per run), render each effect's
templates against the live capture namespace, dispatch to a transport client,
chain HTTP response captures back into the namespace, and stop at the first
failure. It is dependency-injected and extra-free — the resolvers hand in the
real clients (Kafka/HTTP), so this module imports no
``confluent_kafka``/``httpx`` anywhere.

Events (shapes are contracts; downstream consumers grep them):

- ``kafka.produced`` {event, trigger, topic, key, duration_ms} — one per
  message (a ``values`` effect emits one per element).
- ``http.called`` {event, trigger, service XOR url, method, path,
  status_code, duration_ms} — exactly one of ``service``/``url``.
- ``capture.missing`` {event, trigger, name, from} — an HTTP response capture
  path that resolved to nothing (soft miss, non-fatal).
- ``effect.error`` {event, trigger, effect_type, error, fatal: True, and
  ``topic`` (kafka) or ``url``/``service`` (http)} — the executor NEVER
  raises: any exception during an effect is emitted here and short-circuits
  the remaining effects.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from ..clients.kafka_client import _encode_payload_with_codec
from ..config.models import HttpEffect, KafkaEffect, KafkaEffectMessage
from ..resolution import CaptureValue, render_typed
from ..template_vars import substitute_generators
from .capture import resolve_captures


@dataclass(frozen=True)
class EffectOutcome:
    """Result of running an effect list.

    ``ok=False`` means an effect raised: the matching ``effect.error`` event
    was emitted and no effect after the failing one ran.
    """

    ok: bool
    error: str | None = None


class EffectExecutor:
    """Dispatch an ordered effect list against injected transport clients.

    The executor owns render → dispatch → capture-chain → short-circuit and
    nothing else. Transport resolution (which cluster, which service base
    URL, which codec) lives behind the injected resolvers so this class stays
    free of client construction and heavy imports.
    """

    def __init__(
        self,
        *,
        kafka_resolver: Callable[[KafkaEffect], tuple[Any, Any]],
        http_resolver: Callable[[HttpEffect], tuple[Any, str]],
        emit_event: Callable[[dict], None],
    ):
        """Initialize the executor.

        Args:
            kafka_resolver: Returns ``(client, codec)`` for an effect's
                resolved cluster+topic+format. ``client`` duck-types
                ``produce(topic, value, *, key=None, headers=None, _raw=False)``;
                ``codec`` is the encode codec dict or ``None`` (legacy JSON).
            http_resolver: Returns ``(client, path)``. ``client`` duck-types
                ``request(method, path, *, headers=None, body=None) -> dict``
                (the DESIGN §4.2 result shape).
            emit_event: Sink for the event dicts above (no timestamp — the
                engine adds it).
        """
        self._kafka_resolver = kafka_resolver
        self._http_resolver = http_resolver
        self._emit_event = emit_event

    def run(
        self,
        effects: list[KafkaEffect | HttpEffect],
        namespace: dict[str, CaptureValue],
        trigger_label: str,
    ) -> EffectOutcome:
        """Run ``effects`` in order; short-circuit on the first failure.

        Args:
            effects: Ordered effect models (empty/None → immediate ok).
            namespace: Live capture namespace, extended IN PLACE with values
                captured from HTTP effect responses (last-wins: a later
                effect capturing the same name overwrites). Later effects
                render against captures made by earlier ones. This in-place
                extension is the ONLY caller-visible mutation.
            trigger_label: Trigger identifier stamped on every event.

        Returns:
            :class:`EffectOutcome` — ``ok=True`` when every effect ran,
            ``ok=False`` (with the error text) after emitting a fatal
            ``effect.error`` for the first failure. Never raises.
        """
        if not effects:
            return EffectOutcome(ok=True)

        # Generator pre-pass (per-run memo): resolve ``{{gen}}`` tokens across
        # every effect's renderable fields BEFORE the ``{capture}`` pass. One
        # memo per run() call = the same token resolves to one value within one
        # trigger's effect list (§2.5 single-invocation semantics). The two
        # brace syntaxes never collide; a generated value is then literal to
        # render_typed. Done as a copy-pass over the model fields rather than
        # mutating the config models — effects may run on every trigger.
        memo: dict[str, str] = {}
        for effect in effects:
            try:
                if effect.type == "kafka":
                    effect = self._pregen_kafka(effect, memo)
                elif effect.type == "http":
                    effect = self._pregen_http(effect, memo)
                else:  # pragma: no cover - discriminated union admits no else
                    continue
                if effect.type == "kafka":
                    self._run_kafka(effect, namespace, trigger_label)
                elif effect.type == "http":
                    self._run_http(effect, namespace, trigger_label)
            except Exception as exc:
                self._emit_event(
                    {
                        "event": "effect.error",
                        "trigger": trigger_label,
                        "effect_type": effect.type,
                        "error": str(exc),
                        "fatal": True,
                        **self._error_context(effect),
                    }
                )
                return EffectOutcome(ok=False, error=str(exc))
        return EffectOutcome(ok=True)

    # ------------------------------------------------------------------
    # generator pre-pass ({{gen}} tokens)
    # ------------------------------------------------------------------

    @staticmethod
    def _pregen_kafka(effect: KafkaEffect, memo: dict[str, str]) -> KafkaEffect:
        """Return a copy of ``effect`` with ``{{gen}}`` tokens resolved.

        Scans the renderable payload fields: ``value`` / ``values[*].value`` /
        ``key`` / ``headers`` (top-level and per-item). ``topic`` / ``cluster``
        / format fields are targeting metadata, not payload — left verbatim.
        """
        updates: dict[str, Any] = {}
        if effect.values is not None:
            updates["values"] = [
                KafkaEffectMessage(
                    value=substitute_generators(item.value, memo),
                    key=(
                        substitute_generators(item.key, memo)
                        if item.key is not None
                        else None
                    ),
                    headers=(
                        substitute_generators(item.headers, memo)
                        if item.headers is not None
                        else None
                    ),
                )
                for item in effect.values
            ]
        else:
            updates["value"] = substitute_generators(effect.value, memo)
        # Top-level key/headers substitute in BOTH modes: under ``values`` they
        # are the per-message fallback (§6.1), so a token there must resolve
        # before the fallback hands it to a message — not ship literally.
        if effect.key is not None:
            updates["key"] = substitute_generators(effect.key, memo)
        if effect.headers is not None:
            updates["headers"] = substitute_generators(effect.headers, memo)
        return effect.model_copy(update=updates)

    @staticmethod
    def _pregen_http(effect: HttpEffect, memo: dict[str, str]) -> HttpEffect:
        """Return a copy of ``effect`` with ``{{gen}}`` tokens resolved.

        Scans ``body`` / ``headers`` / and ``path`` in service mode (service
        mode's path is rendered against the namespace at dispatch, so a token
        there is meaningful); url mode's ``url`` is a literal base and its path
        is split from it — left verbatim.
        """
        updates: dict[str, Any] = {}
        if effect.body is not None:
            updates["body"] = substitute_generators(effect.body, memo)
        if effect.headers is not None:
            updates["headers"] = substitute_generators(effect.headers, memo)
        if effect.service is not None:
            updates["path"] = substitute_generators(effect.path, memo)
        if not updates:
            return effect
        return effect.model_copy(update=updates)

    # ------------------------------------------------------------------
    # kafka
    # ------------------------------------------------------------------

    def _run_kafka(
        self,
        effect: KafkaEffect,
        namespace: dict[str, CaptureValue],
        trigger_label: str,
    ) -> None:
        """Render and produce one kafka effect (one message per element)."""
        client, codec = self._kafka_resolver(effect)
        if effect.values is not None:
            # Spec §6.1: top-level key/headers are per-message defaults —
            # a values item that omits its own falls back to the top-level one.
            messages = [
                KafkaEffectMessage(
                    value=item.value,
                    key=item.key if item.key is not None else effect.key,
                    headers=(
                        item.headers if item.headers is not None else effect.headers
                    ),
                )
                for item in effect.values
            ]
        else:
            messages = [
                KafkaEffectMessage(
                    value=effect.value, key=effect.key, headers=effect.headers
                )
            ]

        for item in messages:
            rendered_value = render_typed(item.value, namespace)
            rendered_key = (
                render_typed(item.key, namespace) if item.key is not None else None
            )
            rendered_headers = (
                render_typed(item.headers, namespace)
                if item.headers is not None
                else None
            )
            value_bytes, key_bytes = _encode_payload_with_codec(
                codec, effect.topic, rendered_value, rendered_key
            )
            start = time.perf_counter()
            client.produce(
                effect.topic,
                value_bytes,
                key=key_bytes,
                headers=rendered_headers,
                _raw=True,
            )
            duration_ms = (time.perf_counter() - start) * 1000
            self._emit_event(
                {
                    "event": "kafka.produced",
                    "trigger": trigger_label,
                    "topic": effect.topic,
                    "key": rendered_key,
                    "duration_ms": round(duration_ms, 2),
                }
            )

    # ------------------------------------------------------------------
    # http
    # ------------------------------------------------------------------

    def _run_http(
        self,
        effect: HttpEffect,
        namespace: dict[str, CaptureValue],
        trigger_label: str,
    ) -> None:
        """Render, call, and capture-chain one http effect."""
        client, path = self._http_resolver(effect)
        # Service-mode path carries {placeholder}s (spec §6.2); url mode's path
        # was split from the literal url and stays fully literal.
        if effect.service is not None:
            path = render_typed(path, namespace)
        rendered_headers = (
            render_typed(effect.headers, namespace)
            if effect.headers is not None
            else None
        )
        rendered_body = (
            render_typed(effect.body, namespace) if effect.body is not None else None
        )
        resp = client.request(
            effect.method, path, headers=rendered_headers, body=rendered_body
        )

        # Exactly one of service/url rides on the event (model enforces
        # exactly-one-of; this keeps the payload XOR for consumers).
        target = {"service": effect.service} if effect.service else {"url": effect.url}
        self._emit_event(
            {
                "event": "http.called",
                "trigger": trigger_label,
                **target,
                "method": effect.method,
                "path": path,
                "status_code": resp["status_code"],
                "duration_ms": resp["response_time_ms"],
            }
        )

        if effect.capture:
            typed, missing = resolve_captures(resp, effect.capture)
            for name, from_ in missing:
                self._emit_event(
                    {
                        "event": "capture.missing",
                        "trigger": trigger_label,
                        "name": name,
                        "from": from_,
                    }
                )
            # Last-wins chaining: later effects (and later captures of the
            # same name) see these values.
            namespace.update(typed)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _error_context(effect: KafkaEffect | HttpEffect) -> dict[str, str]:
        """Transport context for an ``effect.error`` event.

        Kafka errors carry the target ``topic``; http errors carry the
        ``url`` (absolute) or ``service`` (named) — whichever is set, mirroring
        the XOR on ``http.called``.
        """
        if effect.type == "kafka":
            return {"topic": effect.topic}
        if effect.service:
            return {"service": effect.service}
        return {"url": effect.url}
