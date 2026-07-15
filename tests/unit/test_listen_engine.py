"""Tests for agctl/listen/engine.py — ListenEngine lifecycle (Task 6).

ListenEngine is the HTTP-free analog of MockEngine: it owns per-topic
CaptureLoop threads, single-writer NDJSON emission, signal-driven shutdown,
and a ready-wait startup gate. These tests inject a fake CaptureLoop via the
``capture_loop_factory`` seam (mirroring how reactor tests inject fakes) so no
real broker is touched.

Three scenarios:
1. **Clean lifecycle** — fake sets ready + appends one canned line; start→run→
   shutdown emits exactly one ``started`` then one ``summary`` whose
   ``topics[].captured`` reflects the canned line; ``run()`` returns 0.
2. **Thread error** — fake's ``run()`` raises after signaling ready; the thread
   wrapper emits a fatal ``kafka.error`` and ``run()`` returns 1.
3. **Never-ready** — fake never signals ready; ``start()`` raises
   ``ConnectionFailure`` within a tiny budget (no ``started``/``summary``).
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from agctl.errors import ConnectionFailure
from agctl.listen.engine import ListenEngine


# ---------------------------------------------------------------------------
# Fake CaptureLoop (signature-compatible with the real one)
# ---------------------------------------------------------------------------


class _FakeCaptureLoop:
    """Base fake CaptureLoop storing the kwargs the engine builds it with.

    Subclasses override ``run()`` for each scenario. The engine instantiates the
    configured ``capture_loop_factory`` with the same keyword arguments as the
    real :class:`CaptureLoop`, so the fake accepts them verbatim.
    """

    def __init__(
        self,
        *,
        topic: str,
        client,
        group_id: str,
        capture_path: Path,
        capture_match: str | None,
        max_bytes: int,
        emit_event,
        ready_event: threading.Event,
        stop_event: threading.Event,
    ) -> None:
        self.topic = topic
        self.capture_path = capture_path
        self.emit_event = emit_event
        self.ready_event = ready_event
        self.stop_event = stop_event

    def run(self) -> None:  # pragma: no cover - overridden per scenario
        raise NotImplementedError


class _ReadyFake(_FakeCaptureLoop):
    """Append one canned envelope line, signal ready, and return cleanly."""

    def run(self) -> None:
        line = json.dumps(
            {"topic": self.topic, "value": {"id": "a"}, "captured_at": "now"},
            ensure_ascii=False,
        )
        with self.capture_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        self.ready_event.set()


class _RaisingFake(_FakeCaptureLoop):
    """Signal ready, then raise (exercises the fatal kafka.error thread wrapper)."""

    def run(self) -> None:
        self.ready_event.set()
        raise ConnectionFailure("broker died mid-run")


class _NeverReadyFake(_FakeCaptureLoop):
    """Block on stop_event without ever signaling ready (start must time out)."""

    def run(self) -> None:
        self.stop_event.wait(timeout=5.0)


def _make_engine(tmp_path: Path, emitted: list, factory, *, topics=("orders",)):
    """Build a ListenEngine wired to a recording emit_fn and a fake factory."""

    def recording_emit(line: dict) -> None:
        emitted.append(line.copy())

    engine = ListenEngine(
        topics=list(topics),
        client=object(),
        run_id="run-test",
        group="agctl-listen-run-test",
        cluster="default",
        run_dir=tmp_path,
        capture_match=None,
        max_bytes=0,
        duration=None,
        emit_fn=recording_emit,
    )
    engine.capture_loop_factory = factory
    return engine


# ---------------------------------------------------------------------------
# Scenario 1: clean start → run → shutdown
# ---------------------------------------------------------------------------


def test_clean_lifecycle_emits_started_then_summary_and_run_returns_0(tmp_path):
    """Fake sets ready + appends one line; one started, one summary, exit 0."""
    emitted: list[dict] = []
    engine = _make_engine(tmp_path, emitted, _ReadyFake)

    engine.start()
    # Stop is set before run() so run()'s block-on-stop returns promptly
    # (mirrors the MockEngine unit test pattern).
    engine._stop.set()
    code = engine.run()
    engine.shutdown()

    assert code == 0

    started = [e for e in emitted if e.get("event") == "started"]
    assert len(started) == 1
    assert started[0]["topics"] == ["orders"]
    assert started[0]["group"] == "agctl-listen-run-test"
    assert started[0]["cluster"] == "default"
    assert started[0]["run_id"] == "run-test"
    assert "started_at" in started[0]
    assert "timestamp" in started[0]

    summary = [e for e in emitted if e.get("event") == "summary"]
    assert len(summary) == 1
    assert summary[0]["topics"] == [
        {"topic": "orders", "captured": 1, "overflowed": False}
    ]
    assert summary[0]["errors"] == 0
    assert summary[0]["duration_ms"] >= 0
    assert "timestamp" in summary[0]

    # No fatal errors on a clean run.
    assert not any(e.get("event") == "kafka.error" for e in emitted)


# ---------------------------------------------------------------------------
# Scenario 2: capture-loop thread raises → fatal kafka.error → run returns 1
# ---------------------------------------------------------------------------


def test_capture_loop_thread_error_emits_fatal_kafka_error_and_run_returns_1(tmp_path):
    """A CaptureLoop thread that raises emits a fatal kafka.error; run() exits 1."""
    emitted: list[dict] = []
    engine = _make_engine(tmp_path, emitted, _RaisingFake)

    engine.start()
    engine._stop.set()
    code = engine.run()
    engine.shutdown()

    assert code == 1

    errors = [e for e in emitted if e.get("event") == "kafka.error"]
    assert len(errors) == 1
    assert errors[0]["topic"] == "orders"
    assert errors[0]["fatal"] is True
    assert "broker died mid-run" in errors[0]["error"]

    summary = [e for e in emitted if e.get("event") == "summary"]
    assert len(summary) == 1
    assert summary[0]["errors"] == 1


# ---------------------------------------------------------------------------
# Scenario 3: never-ready topic → start() raises ConnectionFailure
# ---------------------------------------------------------------------------


def test_never_ready_topic_raises_connection_failure_within_budget(tmp_path):
    """A topic whose ready_event never sets makes start() raise ConnectionFailure."""
    emitted: list[dict] = []
    engine = _make_engine(tmp_path, emitted, _NeverReadyFake)
    engine._startup_budget = 0.1  # tiny budget seam

    with pytest.raises(ConnectionFailure) as exc_info:
        engine.start()

    assert "did not become ready" in str(exc_info.value)
    assert "orders" in str(exc_info.value)

    # A failed start never emitted started, so shutdown (called by start's
    # except handler) must NOT emit a spurious summary (started gate).
    assert not any(e.get("event") == "started" for e in emitted)
    assert not any(e.get("event") == "summary" for e in emitted)


# ---------------------------------------------------------------------------
# Extra: multi-topic summary shape + overflow tally via emit_event
# ---------------------------------------------------------------------------


def test_emit_event_tallies_overflow_and_multi_topic_summary(tmp_path):
    """emit_event tallies capture.overflow topics; multi-topic summary reflects each."""
    emitted: list[dict] = []
    engine = _make_engine(tmp_path, emitted, _ReadyFake, topics=("orders", "payments"))

    engine.start()
    # Simulate an overflow on orders via the public emit_event path.
    engine.emit_event({"event": "capture.overflow", "topic": "orders", "bytes": 100})
    engine._stop.set()
    code = engine.run()
    engine.shutdown()

    assert code == 0
    summary = [e for e in emitted if e.get("event") == "summary"][0]
    topics_by_name = {t["topic"]: t for t in summary["topics"]}
    assert set(topics_by_name) == {"orders", "payments"}
    assert topics_by_name["orders"] == {
        "topic": "orders",
        "captured": 1,
        "overflowed": True,
    }
    assert topics_by_name["payments"] == {
        "topic": "payments",
        "captured": 1,
        "overflowed": False,
    }
