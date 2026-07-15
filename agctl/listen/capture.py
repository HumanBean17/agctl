"""Per-topic capture loop for ``kafka listen`` (DESIGN §8.4 listen capture).

:class:`CaptureLoop` wraps :meth:`KafkaClient.consume_loop` to capture every
message produced AFTER the listener starts into a per-topic NDJSON file. It is
**capture-only** (no reaction) and owns three mechanics:

1. **Seek-to-latest on assignment (load-bearing invariant).**
   :meth:`KafkaClient._build_consumer` hardcodes ``auto.offset.reset: earliest``,
   so the consumer would otherwise replay the ENTIRE backlog from each
   partition's start. :meth:`CaptureLoop._on_assign` is forwarded to
   ``consumer.subscribe`` and invoked by librdkafka during the first poll's
   rebalance — BEFORE any data from the newly-assigned partitions is delivered.
   It seeks every assigned partition to ``OFFSET_END`` so the listener starts at
   the head: only messages produced AFTER ``start`` are captured. This is what
   makes ``kafka listen`` immune to scan-window misses, volume truncation, and
   broker retention cleanup.

2. **Optional jq ``--capture-match`` filter.** A non-matching message is
   ``COMMIT``-skipped (offset advanced, nothing written) — the same predicate
   semantics as ``kafka assert`` / the reactor match step.

3. **Byte-bound overflow valve.** Once the capture file's size reaches
   ``max_bytes`` (``max_bytes=0`` disables the valve), emit ``capture.overflow``
   exactly once and ``STOP`` (cease capturing this topic; do NOT truncate).

CaptureLoop owns NO consumer lifecycle: ``consume_loop`` builds, closes, and
owns the consumer on this thread. The appended line is one **CapturedEnvelope**::

    {topic, key, value, partition, offset, timestamp, headers, captured_at}

— the same envelope root ``listen assert`` / ``listen messages`` read back, so
the predicate machinery is reused verbatim across capture and evaluation.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from ..assertions import jq_bool
from ..clients.kafka_client import KafkaClient, ReactionResult

__all__ = ["CaptureLoop"]


def _now_iso_z() -> str:
    """Return the current UTC instant as an ISO-8601 ``Z`` string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CaptureLoop:
    """Per-topic capture loop wrapping :meth:`KafkaClient.consume_loop`.

    The loop runs on a single thread (one CaptureLoop per topic). ``run()``
    delegates to ``consume_loop`` with ``self._handle`` as the message handler
    and ``self._on_assign`` as the rebalance callback; the client owns the
    consumer's build/close lifecycle. See the module docstring for the
    seek-to-latest invariant, the capture-match filter, and the overflow valve.
    """

    def __init__(
        self,
        *,
        topic: str,
        client: KafkaClient,
        group_id: str,
        capture_path: Path,
        capture_match: str | None,
        max_bytes: int,
        emit_event: Callable[[dict], None],
        ready_event: threading.Event,
        stop_event: threading.Event,
    ) -> None:
        """Initialize the capture loop.

        Args:
            topic: Kafka topic to capture.
            client: KafkaClient (or fake) exposing ``consume_loop``.
            group_id: Consumer group id for this listener.
            capture_path: Path to the per-topic ``<topic>.ndjson`` file. The
                parent directory must exist (the listen daemon creates the run
                dir before starting the loop).
            capture_match: Optional jq predicate over the normalized message
                envelope; non-matches are silently skipped (COMMIT, no write).
            max_bytes: Capture-file byte ceiling. ``0`` disables the overflow
                valve; otherwise ``capture.overflow`` fires once and the loop
                STOPs when the file reaches this size.
            emit_event: Event sink (e.g. the daemon's events-log appender).
            ready_event: Signaled once on the first partition assignment (after
                the seek-to-end), so the daemon knows capture has begun at the
                head. Later rebalances re-seek but do not re-signal.
            stop_event: Set by the daemon to stop the consume loop.
        """
        self._topic = topic
        self._client = client
        self._group_id = group_id
        self._capture_path = capture_path
        self._capture_match = capture_match
        self._max_bytes = max_bytes
        self._emit_event = emit_event
        self._ready_event = ready_event
        self._stop_event = stop_event

        # Per-topic append lock. consume_loop is single-threaded per call; the
        # lock keeps the append atomic and is robust to a future shared-file
        # refactor (e.g. multiple capture loops writing the same envelope file).
        self._write_lock = threading.Lock()
        # Overflow guard: capture.overflow must fire at most once per topic.
        # STOP exits the loop on the first overflow, but the flag pins the
        # once-only contract defensively.
        self._overflowed = False

    def run(self) -> None:
        """Run the consume loop until ``stop_event`` is set or the valve STOPs it.

        ``max_retries=1`` because capture is append-only and idempotent:
        :meth:`_handle` never returns ``RETRY`` (a failed local append is a
        hard failure, not a transient broker condition worth re-handling).
        """
        self._client.consume_loop(
            self._topic,
            group_id=self._group_id,
            stop_event=self._stop_event,
            handle=self._handle,
            on_assign=self._on_assign,
            max_retries=1,
        )

    # ------------------------------------------------------------------
    # rebalance: seek-to-latest + readiness signal (load-bearing)
    # ------------------------------------------------------------------

    def _on_assign(self, consumer, partitions) -> None:
        """Seek every assigned partition to ``OFFSET_END``, then signal ready ONCE.

        librdkafka invokes ``on_assign(consumer, partitions)`` during the first
        poll's rebalance — AFTER the assignment is established but BEFORE any
        message from the (re)assigned partitions is delivered to the fetch
        loop. Seeking here repositions each partition's fetch position to the
        head so the consumer does not replay the backlog that
        ``auto.offset.reset: earliest`` would otherwise pull. No pre-start
        message can be delivered: delivery only begins on subsequent polls,
        by which point every assigned partition is positioned at ``OFFSET_END``.

        On later rebalances (partition revoke/reassign during the run)
        ``on_assign`` fires again. Every (re)assigned partition is re-seeked to
        the head (a revoked-then-reassigned partition must not resume from a
        stale committed offset below the head), but the ``ready_event`` guard
        restricts the readiness signal to the first assignment only.

        ``confluent_kafka`` is lazy-imported here because this module lives in
        the optional ``kafka`` extra and must import cleanly without it.
        """
        from confluent_kafka import OFFSET_END, TopicPartition

        for tp in partitions:
            consumer.seek(TopicPartition(tp.topic, tp.partition, OFFSET_END))

        if not self._ready_event.is_set():
            self._ready_event.set()

    # ------------------------------------------------------------------
    # message handler: filter -> overflow valve -> append
    # ------------------------------------------------------------------

    def _handle(self, msg: dict, *, attempt: int, final: bool) -> ReactionResult:
        """Filter, apply the overflow valve, else append one envelope and COMMIT.

        Order is load-bearing:

        1. **capture-match filter** — when ``capture_match`` is set and the
           message does not satisfy the jq predicate, ``COMMIT`` (skip the
           write, advance the offset). Non-matches never count toward overflow.
        2. **overflow valve** — when ``max_bytes > 0`` and the capture file's
           current byte size is ``>= max_bytes`` and the valve has not already
           fired, emit ``capture.overflow`` once (guarded by ``_overflowed``)
           and return ``STOP``. The message that trips the valve is NOT
           captured; the loop ceases capturing this topic.
        3. **append** — otherwise append one CapturedEnvelope NDJSON line under
           the per-topic lock and return ``COMMIT``.
        """
        # Step 1: optional jq capture-match filter (skip non-matches silently).
        if self._capture_match is not None and not jq_bool(msg, self._capture_match):
            return ReactionResult.COMMIT

        # Step 2: byte-bound overflow valve.
        if self._max_bytes > 0 and not self._overflowed:
            try:
                size = self._capture_path.stat().st_size
            except OSError:
                # Capture file not created yet -> size 0 (no overflow possible).
                size = 0
            if size >= self._max_bytes:
                # Guard BEFORE emit so the once-only contract holds even if a
                # later caller resurrects the loop after a STOP.
                self._overflowed = True
                self._emit_event(
                    {
                        "event": "capture.overflow",
                        "topic": self._topic,
                        "bytes": size,
                    }
                )
                return ReactionResult.STOP

        # Step 3: append one CapturedEnvelope line.
        envelope = {
            "topic": self._topic,
            "key": msg.get("key"),
            "value": msg.get("value"),
            "partition": msg.get("partition"),
            "offset": msg.get("offset"),
            "timestamp": msg.get("timestamp"),
            "headers": msg.get("headers"),
            "captured_at": _now_iso_z(),
        }
        line = json.dumps(envelope, ensure_ascii=False) + "\n"
        with self._write_lock:
            with self._capture_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()

        return ReactionResult.COMMIT
