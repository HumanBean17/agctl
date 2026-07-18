"""Kafka reactor: match, capture, react with visible skip semantics (DESIGN §8.3).

The reactor consumes messages from a topic, matches them with a jq predicate,
captures top-level values, renders reaction templates, and produces result
messages. Non-object values are visibly skipped (kafka.skipped). Reaction
failures are retried by the client; final failures emit kafka.error once
before committing (non-fatal) or stopping (fail_fast).
"""

import threading
import time
from typing import Callable

from ..assertions import jq_bool
from ..clients.kafka_client import (
    KafkaClient,
    ReactionResult,
    _encode_payload_with_codec,
)
from ..config.models import KafkaReactor as KafkaReactorConfig
from ..resolution import CaptureValue, render_typed
from .capture import resolve_captures


class KafkaReactor:
    """Kafka consumer reactor with jq match, capture, and reaction mechanics.

    The reactor runs a single consume_loop on its own thread. Each message is:
    1. Validated: non-object values → kafka.skipped + COMMIT (visible, not silent).
    2. Matched: if config.match is set and jq_bool(msg, match) is False → COMMIT
       (predicate rooted at the whole message envelope — peer of capture).
    3. Captured: implicit top-level value keys (scalar) + explicit envelope-rooted
       capture (jq over the whole message; overrides implicit incl. type). A from
       resolving to nothing emits capture.missing.
    4. Reacted: templates rendered via render_typed, message produced.
    5. On success: kafka.reacted event + COMMIT.
    6. On failure: RETRY (not final) or kafka.error + COMMIT/STOP (final).

    The reactor keeps NO per-message attempt counter; the client's consume_loop
    passes attempt/final flags and manages retry budget.
    """

    def __init__(
        self,
        name: str,
        config: KafkaReactorConfig,
        client: KafkaClient,
        *,
        emit_event: Callable[[dict], None],
        stop_event: threading.Event,
        fail_fast: bool,
        run_id: str,
        reaction_codec: dict | None = None,
    ):
        """Initialize the reactor.

        Args:
            name: Reactor name (for events/logging).
            config: KafkaReactor config model (from agctl.yaml).
            client: KafkaClient instance (or fake for testing). Wired with
                the TRIGGER topic's codec (Task 12) so ``consume_loop``
                decodes trigger values per the trigger format before
                ``_handle`` runs. The same client is used to PRODUCE the
                reaction — see ``reaction_codec`` for the per-direction
                format independence.
            emit_event: Callable[[dict], None] — emit event WITHOUT timestamp.
            stop_event: threading.Event — set to stop the reactor.
            fail_fast: If True, final reaction failure → STOP (engine-wide).
            run_id: Engine-provided run identifier.
            reaction_codec: Optional codec dict for the REACTION topic's
                format (Task 12). ``None`` keeps today's byte-identical
                JSON path (the reactor hands ``render_typed``'s output to
                ``client.produce`` and the client json.dumps's it). When
                set, ``_handle`` encodes value/key via
                :func:`encode_payload` against the codec's SR client and
                resolved subject BEFORE calling ``client.produce(_raw=True)``
                — the trigger client's own codec is for DECODE only and is
                not consulted on the reaction path. A reactor may thus
                decode a JSON trigger and emit an Avro reaction, or any
                other combination.
        """
        self._name = name
        self._config = config
        self._client = client
        self._emit_event = emit_event
        self._stop_event = stop_event
        self._fail_fast = fail_fast
        self._run_id = run_id
        self._reaction_codec = reaction_codec

        # Per-message decode-error flag (Task 12 codec seam). The trigger
        # client's ``consume_loop`` invokes ``on_decode_error`` BEFORE
        # ``_handle`` for any message whose value or key failed to decode
        # (the failed side becomes None in the envelope). The callback arms
        # this flag and records the codec's error label; ``_handle`` reads
        # it at the top, emits a ``kafka.skipped`` event with a
        # ``"decode failed: ..."`` reason (non-fatal — consistent with the
        # existing non-object skip), and COMMITs past the corrupt message.
        # Cleared on each ``_handle`` invocation's decode-error branch.
        self._decode_failed_for_msg: bool = False
        self._last_decode_error: str | None = None

    def resolved_group(self) -> str:
        """Return the consumer group ID for this reactor.

        If config.consumer_group is set, use it. Otherwise, generate a unique
        group ID as "agctl-mock-{name}-{run_id}" so each run gets its own group
        (prevents cross-run offset pollution).
        """
        if self._config.consumer_group is not None:
            return self._config.consumer_group
        return f"agctl-mock-{self._name}-{self._run_id}"

    def prepare(self) -> None:
        """Probe the broker and topic to verify connectivity.

        Calls client.probe(config.topic, group_id=self.resolved_group()).
        Raises ConnectionFailure or ConfigError from the probe.
        """
        self._client.probe(
            self._config.topic,
            group_id=self.resolved_group(),
        )

    def close(self) -> None:
        """Close any resource opened by :meth:`prepare`.

        Currently a documented no-op: ``prepare()`` is probe-only (the probe
        builds and closes its own consumer), and ``consume_loop`` owns its own
        consumer. This method establishes the teardown contract so a future
        ``prepare()`` that opens a long-lived resource has a documented place
        to release it. ``MockEngine._shutdown_reactors`` calls this on each
        prepared reactor when startup fails mid-probe.
        """
        # Intentionally empty: nothing owned across prepare()/run() today.
        return None

    def run(self) -> None:
        """Run the consume loop until stop_event is set.

        Calls client.consume_loop with self._handle as the message handler.
        The consume_loop builds, closes, and owns the consumer on this thread.

        ``on_decode_error`` (Task 12) wires the codec seam's per-side decode
        failure callback so a corrupt trigger payload emits a
        ``kafka.skipped`` event with a ``"decode failed: ..."`` reason
        (non-fatal, COMMIT). Legacy JSON-only reactors pass a codec-less
        client; the callback is still forwarded and is simply never invoked
        by the client in that mode.
        """
        self._client.consume_loop(
            self._config.topic,
            group_id=self.resolved_group(),
            stop_event=self._stop_event,
            handle=self._handle,
            max_retries=3,
            on_decode_error=self._on_decode_error,
        )

    def _on_decode_error(self, error_label: str) -> None:
        """Per-side decode failure callback (Task 8 codec seam).

        The trigger client's ``consume_loop`` invokes this once per failed
        SIDE (value or key) from inside ``_normalize_message`` BEFORE
        ``_handle`` is called for that message. The reactor records the
        failure (per-message flag + most-recent label) so ``_handle`` can
        emit a single ``kafka.skipped`` event with a ``"decode failed: ..."``
        reason and COMMIT past the corrupt message (consistent with today's
        non-object skip semantics: non-fatal, COMMIT).

        A message with BOTH sides failing invokes this twice (once per
        side); the per-message flag stays armed and ``_handle`` clears it
        once. The label is the most-recent call's, which is acceptable —
        the reactor skips the whole message regardless.
        """
        self._decode_failed_for_msg = True
        self._last_decode_error = error_label

    def _handle(self, msg: dict, *, attempt: int, final: bool) -> ReactionResult:
        """Handle a single Kafka message with match/capture/react logic.

        Args:
            msg: Normalized message dict with keys: value, key, partition, offset,
                 timestamp, headers.
            attempt: 1-based attempt number from the client.
            final: True if this is the last attempt (attempt >= max_retries).

        Returns:
            ReactionResult.COMMIT/RETRY/STOP.
        """
        # Step 0: Per-message decode-error skip (Task 12 codec seam). The
        # trigger client's ``consume_loop`` invoked ``on_decode_error`` BEFORE
        # this call for any side (value or key) that failed to decode. The
        # message is corrupt — proceed no further (no match, no capture, no
        # reaction). Emit a ``kafka.skipped`` event with a ``"decode failed"``
        # reason (non-fatal, COMMIT — consistent with today's non-object
        # skip), then clear the per-message flag so the next message starts
        # clean.
        if self._decode_failed_for_msg:
            label = self._last_decode_error or "unknown"
            self._decode_failed_for_msg = False
            self._last_decode_error = None
            self._emit_event(
                {
                    "event": "kafka.skipped",
                    "reactor": self._name,
                    "topic": self._config.topic,
                    "reason": f"decode failed: {label}",
                    "count": 1,
                }
            )
            return ReactionResult.COMMIT

        value = msg.get("value")

        # Step 1: Validate value is a dict (object)
        if not isinstance(value, dict):
            self._emit_event(
                {
                    "event": "kafka.skipped",
                    "reactor": self._name,
                    "topic": self._config.topic,
                    "reason": "non-object message value",
                    "count": 1,
                }
            )
            return ReactionResult.COMMIT

        # Step 2: Match if configured
        if self._config.match is not None:
            if not jq_bool(msg, self._config.match):
                # Non-match → silent commit (like `kafka consume --match`)
                return ReactionResult.COMMIT

        # Step 3: Capture context. Implicit: top-level value keys as scalar
        # CaptureValues (raw value; render_typed applies str() for scalar —
        # preserves today's coercion of numerics/bools to strings).
        capture_context = {
            k: CaptureValue(v, "scalar") for k, v in value.items()
        }

        # Explicit capture: envelope-rooted jq extraction over the whole
        # normalized message (key/value/headers/...) overrides implicit on
        # name collision, including type promotion (e.g. scalar -> object for
        # true pass-through). A from resolving to nothing emits a non-fatal
        # capture.missing event.
        if self._config.capture is not None:
            explicit, missing = resolve_captures(msg, self._config.capture)
            capture_context.update(explicit)
            for cap_name, from_path in missing:
                self._emit_event(
                    {
                        "event": "capture.missing",
                        "reactor": self._name,
                        "name": cap_name,
                        "from": from_path,
                    }
                )

        # Step 4: React (render templates and produce)
        try:
            rendered_value = render_typed(
                self._config.reaction.value, capture_context
            )
            rendered_key = None
            if self._config.reaction.key is not None:
                rendered_key = render_typed(
                    self._config.reaction.key, capture_context
                )
            rendered_headers = None
            if self._config.reaction.headers is not None:
                rendered_headers = render_typed(
                    self._config.reaction.headers, capture_context
                )

            # Produce the reaction message. When a reaction codec is set
            # (Task 12), the rendered value/key are encoded via
            # :func:`encode_payload` against the codec's SR client and the
            # resolved subject (topic strategy) BEFORE publish, and the
            # bytes are handed to ``produce(_raw=True)`` so the trigger
            # client's own codec (which is the TRIGGER topic's format, for
            # decode) does not re-encode them. ``reaction_codec=None``
            # keeps today's byte-identical JSON path.
            start = time.perf_counter()
            if self._reaction_codec is not None:
                value_bytes, key_bytes = self._encode_reaction(
                    rendered_value, rendered_key
                )
                self._client.produce(
                    self._config.reaction.topic,
                    value_bytes,
                    key=key_bytes,
                    headers=rendered_headers,
                    _raw=True,
                )
            else:
                self._client.produce(
                    self._config.reaction.topic,
                    rendered_value,
                    key=rendered_key,
                    headers=rendered_headers,
                )
            duration_ms = (time.perf_counter() - start) * 1000

            # Step 5: Emit kafka.reacted event on success
            self._emit_event(
                {
                    "event": "kafka.reacted",
                    "reactor": self._name,
                    "topic": self._config.reaction.topic,
                    "key": rendered_key,
                    "duration_ms": round(duration_ms, 2),
                }
            )
            return ReactionResult.COMMIT

        except Exception as exc:
            # Step 6: Reaction failure handling
            if not final:
                # Not final → retry (emit nothing yet)
                return ReactionResult.RETRY

            # Final → emit kafka.error once and decide COMMIT/STOP
            self._emit_event(
                {
                    "event": "kafka.error",
                    "reactor": self._name,
                    "topic": self._config.topic,
                    "offset": msg["offset"],
                    "partition": msg["partition"],
                    "error": str(exc),
                    "fatal": self._fail_fast,
                }
            )

            if self._fail_fast:
                return ReactionResult.STOP
            else:
                return ReactionResult.COMMIT

    # ------------------------------------------------------------------
    # reaction encode (Task 12)
    # ------------------------------------------------------------------

    def _encode_reaction(self, value, key):
        """Encode the rendered reaction value/key per ``self._reaction_codec``.

        Thin delegate over the shared module-level
        :func:`_encode_payload_with_codec` (the same implementation
        :meth:`KafkaClient._encode_payload` uses), scoped to the REACTION
        codec and reaction topic. Centralizing the encode logic prevents
        the class of copy-paste divergence that previously bit the
        key-side :func:`resolve_subject` call here (a duplicate passed
        ``value`` instead of ``key`` — dormant under the default
        ``"topic"`` strategy but LIVE under ``"record"``/``"topic_record"``,
        where it resolved the wrong subject for non-string KEY formats).

        The REACTION codec is independent of the trigger client's own
        codec (which is the trigger topic's format used for DECODE only):
        a reactor may decode a JSON trigger and emit an Avro reaction,
        or any other combination.

        Returns ``(value_bytes, key_bytes)`` ready for
        ``client.produce(..., _raw=True)``. JSON / unset value formats and
        KEY_STRING / unset key formats keep today's byte-for-byte legacy
        encoding (``json.dumps`` for the value, utf-8 for a string key) —
        only non-JSON value formats and non-string key formats route
        through :func:`encode_payload`.

        :class:`SerializationError` from the codec propagates unchanged
        (the produce path is the write side — a schema-violating record is
        a fatal contract/config bug, not a per-message skip). The caller's
        ``except Exception`` arm retries per the existing flow and emits
        ``kafka.error`` on the final attempt.
        """
        return _encode_payload_with_codec(
            self._reaction_codec, self._config.reaction.topic, value, key
        )

        return value_bytes, key_bytes
