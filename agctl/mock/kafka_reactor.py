"""Kafka reactor: match, capture, react with visible skip semantics (DESIGN §8.3).

The reactor consumes messages from a topic, matches them with a jq predicate,
captures top-level values, renders reaction templates, and produces result
messages. Non-object values are visibly skipped (kafka.skipped). Reaction
failures are retried by the client; final failures emit kafka.error once
before committing (non-fatal) or stopping (fail_fast).
"""

import os
from typing import Any

from ..assertions import jq_bool
from ..resolution import fill_placeholders
from ..clients.kafka_client import ReactionResult


class KafkaReactor:
    """Kafka consumer reactor with jq match, capture, and reaction mechanics.

    The reactor runs a single consume_loop on its own thread. Each message is:
    1. Validated: non-object values → kafka.skipped + COMMIT (visible, not silent).
    2. Matched: if config.match is set and jq_bool(value, match) is False → COMMIT.
    3. Captured: top-level keys of value dict, each stringified via str().
    4. Reacted: templates rendered via fill_placeholders, message produced.
    5. On success: kafka.reacted event + COMMIT.
    6. On failure: RETRY (not final) or kafka.error + COMMIT/STOP (final).

    The reactor keeps NO per-message attempt counter; the client's consume_loop
    passes attempt/final flags and manages retry budget.
    """

    def __init__(
        self,
        name: str,
        config,
        client,
        *,
        emit_event,
        stop_event,
        fail_fast: bool,
        run_id: str | None = None,
    ):
        """Initialize the reactor.

        Args:
            name: Reactor name (for events/logging).
            config: KafkaReactor config model (from agctl.yaml).
            client: KafkaClient instance (or fake for testing).
            emit_event: Callable[[dict], None] — emit event WITHOUT timestamp.
            stop_event: threading.Event — set to stop the reactor.
            fail_fast: If True, final reaction failure → STOP (engine-wide).
            run_id: Engine-provided run identifier (default: str(os.getpid())).
        """
        self._name = name
        self._config = config
        self._client = client
        self._emit_event = emit_event
        self._stop_event = stop_event
        self._fail_fast = fail_fast
        self._run_id = run_id if run_id is not None else str(os.getpid())

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

    def run(self) -> None:
        """Run the consume loop until stop_event is set.

        Calls client.consume_loop with self._handle as the message handler.
        The consume_loop builds, closes, and owns the consumer on this thread.
        """
        self._client.consume_loop(
            self._config.topic,
            group_id=self.resolved_group(),
            stop_event=self._stop_event,
            handle=self._handle,
            max_retries=3,
        )

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
            if not jq_bool(value, self._config.match):
                # Non-match → silent commit (like `kafka consume --match`)
                return ReactionResult.COMMIT

        # Step 3: Capture context (top-level keys, stringified)
        capture_context = {k: str(v) for k, v in value.items()}

        # Step 4: React (render templates and produce)
        try:
            rendered_value = fill_placeholders(
                self._config.reaction.value, capture_context
            )
            rendered_key = None
            if self._config.reaction.key is not None:
                rendered_key = fill_placeholders(
                    self._config.reaction.key, capture_context
                )
            rendered_headers = None
            if self._config.reaction.headers is not None:
                rendered_headers = fill_placeholders(
                    self._config.reaction.headers, capture_context
                )

            # Produce the reaction message
            import time

            start = time.perf_counter()
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
