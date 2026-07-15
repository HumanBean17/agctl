"""Tests for agctl/listen/capture.py — per-topic CaptureLoop (Task 5).

CaptureLoop wraps KafkaClient.consume_loop with three mechanics:

- **seek-to-latest on assignment** — the consumer's ``auto.offset.reset`` is
  ``earliest``, which would otherwise replay the entire backlog. ``_on_assign``
  seeks every assigned partition to ``OFFSET_END`` during the first poll's
  rebalance (BEFORE any data is delivered), so only messages produced AFTER
  ``start`` are captured. This is the load-bearing invariant.
- **optional jq capture-match filter** — non-matching messages are COMMIT-skipped.
- **byte-bound overflow valve** — once the capture file reaches ``max_bytes``,
  emit ``capture.overflow`` exactly once and STOP (cease, not truncate).

Tests inject a fake KafkaClient whose consume_loop records ``on_assign``,
invokes it against a fake consumer (``seek`` spy), then delivers canned
normalized dicts. No real broker is touched.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from confluent_kafka import OFFSET_END, TopicPartition

from agctl.clients.kafka_client import ReactionResult
from agctl.listen.capture import CaptureLoop

_TOPIC = "orders"
# The partition librdkafka would hand to on_assign during rebalance. Its offset
# is OFFSET_INVALID (-1001) at assignment time — _on_assign must construct a NEW
# TopicPartition(tp.topic, tp.partition, OFFSET_END) and seek to THAT.
_ASSIGNED_TP = TopicPartition(_TOPIC, 0)


# ---------------------------------------------------------------------------
# Fakes (mirror tests/unit/test_mock_kafka_reactor.py's plain-fake style)
# ---------------------------------------------------------------------------


class _FakeConsumer:
    """Minimal consumer stand-in: ``seek`` is a recording spy."""

    def __init__(self):
        self.seeks: list[TopicPartition] = []

    def seek(self, tp):
        self.seeks.append(tp)


class _RecordingEvent:
    """threading.Event stand-in that counts ``set()`` calls.

    Used to verify the readiness guard: ``ready_event.set()`` must fire exactly
    once across multiple rebalances.
    """

    def __init__(self):
        self._inner = threading.Event()
        self.set_count = 0

    def set(self):
        self._inner.set()
        self.set_count += 1

    def is_set(self) -> bool:
        return self._inner.is_set()


class _FakeKafkaClient:
    """Fake KafkaClient.consume_loop.

    Records the ``on_assign`` callback, invokes it once against the fake
    consumer (simulating the first poll's rebalance), then delivers each canned
    normalized message to ``handle``. Honors ``ReactionResult.STOP`` by
    returning immediately (mirrors the real client's STOP semantics); COMMIT
    advances to the next message. ``_handle`` never returns RETRY, so no retry
    loop is simulated.
    """

    def __init__(self, messages):
        self.messages = list(messages)
        self.consumer = _FakeConsumer()
        self.consume_calls: list[dict] = []

    def consume_loop(
        self,
        topic,
        *,
        group_id,
        stop_event,
        handle,
        poll_timeout=0.5,
        max_retries=3,
        on_assign=None,
        on_revoke=None,
    ):
        self.consume_calls.append(
            {
                "topic": topic,
                "group_id": group_id,
                "max_retries": max_retries,
                "on_assign": on_assign,
            }
        )
        if on_assign is not None:
            on_assign(self.consumer, [_ASSIGNED_TP])
        for msg in self.messages:
            if stop_event.is_set():
                return
            result = handle(msg, attempt=1, final=True)
            if result is ReactionResult.STOP:
                return
            # COMMIT (or RETRY@final treated as COMMIT) -> next message.


def _msg(value, *, key=None, offset=0, partition=0, headers=None):
    """Build a normalized message dict (KafkaClient._normalize_message shape)."""
    return {
        "key": key,
        "value": value,
        "partition": partition,
        "offset": offset,
        "timestamp": "2026-07-15T00:00:00Z",
        "headers": headers or {},
    }


# ---------------------------------------------------------------------------
# Seek-to-latest invariant (the load-bearing keystone)
# ---------------------------------------------------------------------------


class TestSeekToLatest:
    """_on_assign seeks every assigned partition to OFFSET_END before delivery."""

    def test_consume_loop_forwarded_with_on_assign_and_max_retries_1(
        self, tmp_path: Path
    ):
        ready = threading.Event()
        client = _FakeKafkaClient(messages=[])
        loop = CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=tmp_path / "orders.ndjson",
            capture_match=None,
            max_bytes=0,
            emit_event=lambda _e: None,
            ready_event=ready,
            stop_event=threading.Event(),
        )
        loop.run()

        call = client.consume_calls[0]
        assert call["topic"] == _TOPIC
        assert call["group_id"] == "g1"
        # on_assign is forwarded so librdkafka invokes it during rebalance.
        # Compare __func__ because each attribute access of a bound method
        # produces a fresh wrapper object (so `is` on two reads always fails).
        assert call["on_assign"] is not None
        assert call["on_assign"].__func__ is CaptureLoop._on_assign
        # max_retries=1: capture is append-only/idempotent; _handle never RETRYs.
        assert call["max_retries"] == 1

    def test_assigned_partition_seeked_to_offset_end(self, tmp_path: Path):
        ready = threading.Event()
        client = _FakeKafkaClient(messages=[])
        loop = CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=tmp_path / "orders.ndjson",
            capture_match=None,
            max_bytes=0,
            emit_event=lambda _e: None,
            ready_event=ready,
            stop_event=threading.Event(),
        )
        loop.run()

        # The single assigned partition was seeked to OFFSET_END (not
        # OFFSET_BEGINNING / earliest). This is what prevents backlog replay.
        assert len(client.consumer.seeks) == 1
        seek_tp = client.consumer.seeks[0]
        assert seek_tp.topic == _TOPIC
        assert seek_tp.partition == 0
        assert seek_tp.offset == OFFSET_END

    def test_ready_event_set_after_first_assignment(self, tmp_path: Path):
        ready = threading.Event()
        client = _FakeKafkaClient(messages=[])
        CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=tmp_path / "orders.ndjson",
            capture_match=None,
            max_bytes=0,
            emit_event=lambda _e: None,
            ready_event=ready,
            stop_event=threading.Event(),
        ).run()
        assert ready.is_set()


class TestReadyGuard:
    """ready_event fires ONCE; later rebalances re-seek but don't re-signal."""

    def test_ready_set_once_and_seek_every_rebalance(self, tmp_path: Path):
        """Two rebalances: seek runs both times, ready.set() fires once."""
        ready = _RecordingEvent()
        client = _FakeKafkaClient(messages=[])
        loop = CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=tmp_path / "orders.ndjson",
            capture_match=None,
            max_bytes=0,
            emit_event=lambda _e: None,
            ready_event=ready,
            stop_event=threading.Event(),
        )
        # Simulate back-to-back rebalances (librdkafka fires on_assign per
        # assignment; a partition revoke+reassign during the run is normal).
        loop._on_assign(client.consumer, [_ASSIGNED_TP])
        loop._on_assign(client.consumer, [_ASSIGNED_TP])

        # Seek is called on EVERY assignment — each rebalance must re-seek the
        # (re)assigned partitions to the head so no stale-offset fetch leaks in.
        assert len(client.consumer.seeks) == 2
        for tp in client.consumer.seeks:
            assert tp.offset == OFFSET_END
        # But the readiness signal fires exactly ONCE (first assignment only).
        assert ready.set_count == 1
        assert ready.is_set()


# ---------------------------------------------------------------------------
# Capture + capture-match filter
# ---------------------------------------------------------------------------


class TestCaptureAndFilter:
    """_handle appends matching envelopes and skips non-matching ones."""

    def test_captures_only_matching_messages(self, tmp_path: Path):
        capture = tmp_path / "orders.ndjson"
        msgs = [
            _msg({"eventType": "ORDER_CREATED", "id": "a"}, key="k1", offset=0),
            _msg({"eventType": "ORDER_CANCELLED", "id": "b"}, key="k2", offset=1),
            _msg({"eventType": "ORDER_CREATED", "id": "c"}, key="k3", offset=2),
        ]
        client = _FakeKafkaClient(messages=msgs)
        CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=capture,
            capture_match='.value.eventType == "ORDER_CREATED"',
            max_bytes=0,
            emit_event=lambda _e: None,
            ready_event=threading.Event(),
            stop_event=threading.Event(),
        ).run()

        lines = capture.read_text(encoding="utf-8").splitlines()
        # The non-match (offset 1) was COMMIT-skipped; the two matches captured.
        assert len(lines) == 2

        env0 = json.loads(lines[0])
        env1 = json.loads(lines[1])
        # CapturedEnvelope shape: topic + normalized fields + captured_at.
        assert env0["topic"] == _TOPIC
        assert env0["key"] == "k1"
        assert env0["value"] == {"eventType": "ORDER_CREATED", "id": "a"}
        assert env0["partition"] == 0
        assert env0["offset"] == 0
        assert env0["timestamp"] == "2026-07-15T00:00:00Z"
        assert env0["headers"] == {}
        assert env0["captured_at"]  # ISO-Z string is present
        assert env1["offset"] == 2  # offset 1 (non-match) skipped

    def test_no_filter_captures_everything(self, tmp_path: Path):
        capture = tmp_path / "orders.ndjson"
        msgs = [
            _msg({"id": "a"}, offset=0),
            _msg({"id": "b"}, offset=1),
        ]
        client = _FakeKafkaClient(messages=msgs)
        CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=capture,
            capture_match=None,
            max_bytes=0,
            emit_event=lambda _e: None,
            ready_event=threading.Event(),
            stop_event=threading.Event(),
        ).run()

        lines = capture.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        assert [json.loads(ln)["offset"] for ln in lines] == [0, 1]


# ---------------------------------------------------------------------------
# Overflow valve
# ---------------------------------------------------------------------------


class TestOverflowValve:
    """max_bytes bound: capture.overflow emitted once, then STOP."""

    def test_overflow_emits_once_and_stops(self, tmp_path: Path):
        events: list[dict] = []
        capture = tmp_path / "orders.ndjson"
        msgs = [
            _msg({"id": "a"}, offset=0),
            _msg({"id": "b"}, offset=1),
            _msg({"id": "c"}, offset=2),
        ]
        client = _FakeKafkaClient(messages=msgs)
        CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=capture,
            capture_match=None,
            # max_bytes=1: the first message (file size 0 < 1) is captured; the
            # second (file size >= 1) trips the valve. This faithfully exercises
            # the once-only emit + STOP path (real max_bytes is typically MB-sized).
            max_bytes=1,
            emit_event=events.append,
            ready_event=threading.Event(),
            stop_event=threading.Event(),
        ).run()

        # Exactly one line captured (the first message); the looping STOPped on
        # the second, so the third was never delivered to _handle.
        lines = capture.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        assert json.loads(lines[0])["value"] == {"id": "a"}

        # Exactly one capture.overflow event, with topic + bytes.
        overflows = [e for e in events if e.get("event") == "capture.overflow"]
        assert len(overflows) == 1
        assert overflows[0]["topic"] == _TOPIC
        assert isinstance(overflows[0]["bytes"], int)
        assert overflows[0]["bytes"] >= 1

    def test_max_bytes_zero_disables_valve(self, tmp_path: Path):
        """max_bytes=0 means no overflow check ever fires."""
        events: list[dict] = []
        capture = tmp_path / "orders.ndjson"
        msgs = [_msg({"id": "a"}, offset=0), _msg({"id": "b"}, offset=1)]
        client = _FakeKafkaClient(messages=msgs)
        CaptureLoop(
            topic=_TOPIC,
            client=client,
            group_id="g1",
            capture_path=capture,
            capture_match=None,
            max_bytes=0,
            emit_event=events.append,
            ready_event=threading.Event(),
            stop_event=threading.Event(),
        ).run()

        # Both messages captured; no overflow event.
        lines = capture.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        assert not any(e.get("event") == "capture.overflow" for e in events)
