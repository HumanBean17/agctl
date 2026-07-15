"""ListenEngine: lifecycle owner for the ``kafka listen`` capture daemon (DESIGN §8.4).

The engine is the HTTP-free analog of :class:`agctl.mock.engine.MockEngine`. It
coordinates:

- Per-topic :class:`CaptureLoop` threads (one per topic), each owning its own
  consumer via ``consume_loop`` and signaling a per-topic ``ready_event`` once
  seek-to-end has positioned it at the head.
- Single-writer NDJSON emission to stdout (one JSON object per event), guarded by
  a threading lock so handler/thread emission never interleaves.
- Ready-wait startup gate: ``start()`` blocks until EVERY topic's capture loop
  has signaled ready (or a startup budget elapses → ``ConnectionFailure``),
  THEN emits the ``started`` line.
- Signal-driven shutdown (``SIGTERM``/``SIGINT``) with a final ``summary`` line
  carrying per-topic captured counts and the overflow/error tallies.

``started`` gates ``summary``: a failed start (which never emitted ``started``)
cannot emit a spurious ``summary``.
"""

from __future__ import annotations

import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..errors import ConnectionFailure
from ..output import emit_ndjson_line
from .capture import CaptureLoop
from .daemon import capture_path

__all__ = ["ListenEngine"]


def _now_iso_z() -> str:
    """Return the current UTC instant as an ISO-8601 ``Z`` string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ListenEngine:
    """Lifecycle owner for the ``kafka listen`` capture daemon.

    The engine manages:
    - Startup: spawn one CaptureLoop thread per topic and wait until every loop
      is ready (seek-to-end done) before emitting ``started``.
    - Runtime: CaptureLoop threads capture messages; the engine blocks on a stop
      event (set by signal handlers, a duration timer, or shutdown).
    - Shutdown: join threads, emit a ``summary`` line with per-topic captured
      counts and the overflow/error tallies.

    All event emission goes through :meth:`emit_event` (single-writer with lock).
    """

    # Test seam: the class used to build each per-topic capture loop. Unit tests
    # inject a fake to drive start→run→shutdown without a real broker. Mirrors
    # how MockEngine reactor tests inject fakes.
    capture_loop_factory = CaptureLoop

    # Test seam: startup ready-wait budget (seconds). 30s in production; tests
    # shrink it to assert the never-ready ConnectionFailure path quickly.
    _startup_budget: float = 30.0

    def __init__(
        self,
        *,
        topics: list[str],
        client: Any,
        run_id: str,
        group: str,
        cluster: str,
        run_dir: Path,
        capture_match: str | None,
        max_bytes: int,
        duration: float | None,
        emit_fn: Callable[[dict], None] = emit_ndjson_line,
    ) -> None:
        """Initialize the listen engine.

        Args:
            topics: Kafka topics to capture (one CaptureLoop thread per topic).
            client: KafkaClient (or fake) exposing ``consume_loop``.
            run_id: Run identifier (keys the run dir + consumer group).
            group: Consumer group string for this listener.
            cluster: Cluster name the listener connected to (reported in
                ``started``).
            run_dir: Run directory holding each topic's ``<topic>.ndjson``.
            capture_match: Optional jq predicate forwarded to each CaptureLoop.
            max_bytes: Per-topic capture-file byte ceiling (0 disables the
                overflow valve).
            duration: If set, stop after this many seconds.
            emit_fn: Callable to emit one NDJSON line (default: stdout).
        """
        self._topics = list(topics)
        self._client = client
        self._run_id = run_id
        self._group = group
        self._cluster = cluster
        self._run_dir = run_dir
        self._capture_match = capture_match
        self._max_bytes = max_bytes
        self._duration = duration
        self._emit_fn = emit_fn

        # Shutdown coordination.
        self._stop = threading.Event()

        # Single-writer emission lock.
        self._emit_lock = threading.Lock()

        # One ready_event per topic; set by each CaptureLoop after its first
        # seek-to-end-on-assignment. Populated during start().
        self._ready_events: dict[str, threading.Event] = {}

        # Summary tallies (protected by _emit_lock).
        self.overflowed_topics: list[str] = []
        self.errors: int = 0

        # Set True only after the started line is emitted; gates summary so a
        # failed start (which never emitted started) cannot emit a spurious
        # summary line.
        self._started = False

        # Engine components (set during start()).
        self._capture_loops: list[Any] = []
        self._capture_threads: list[threading.Thread] = []
        # Cancellable duration timer; cancelled on shutdown so an early shutdown
        # doesn't leave a sleeping timer for the full duration.
        self._duration_timer: threading.Timer | None = None

        # Startup time for duration_ms calculation.
        self._start_time: float | None = None

    # ------------------------------------------------------------------
    # single-writer emission
    # ------------------------------------------------------------------

    def emit_event(self, line: dict) -> None:
        """Emit a single NDJSON line with timestamp (single-writer).

        Called by CaptureLoop threads (and the start/run/shutdown methods on the
        main thread). Acquires the lock, adds a timestamp if absent, tallies
        summary counters (``capture.overflow`` → append topic;
        ``kafka.error`` → ``errors += 1``), writes via ``emit_fn``, and
        releases the lock.

        Args:
            line: Event dict (mutated to add ``timestamp`` if absent).
        """
        with self._emit_lock:
            if "timestamp" not in line:
                line["timestamp"] = _now_iso_z()

            event = line.get("event")
            if event == "capture.overflow":
                topic = line.get("topic")
                if isinstance(topic, str):
                    self.overflowed_topics.append(topic)
            elif event == "kafka.error":
                self.errors += 1

            self._emit_fn(line)

    # ------------------------------------------------------------------
    # startup: spawn capture threads, wait for ready, emit started
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the engine: spawn CaptureLoop threads, wait for ready, emit started.

        For each topic, build a CaptureLoop (via :attr:`capture_loop_factory`)
        with its own ``ready_event`` and the shared ``stop_event``, and spawn it
        on a daemon thread whose target wraps ``capture_loop.run()`` so any
        exception emits a fatal ``kafka.error`` (mirrors MockEngine's
        reactor-thread error handling). Then wait until EVERY topic's
        ``ready_event`` is set or the startup budget elapses (→
        :class:`ConnectionFailure`). Once ready, emit the ``started`` line and
        THEN mark ``_started`` (so a failed emit cannot leave ``_started`` True,
        which would let the except handler's ``shutdown()`` emit a spurious
        ``summary`` for a stream that never received a started line).

        On any exception, run :meth:`shutdown` to release threads, then re-raise.
        """
        try:
            for topic in self._topics:
                ready_event = threading.Event()
                self._ready_events[topic] = ready_event

                loop = self.capture_loop_factory(
                    topic=topic,
                    client=self._client,
                    group_id=self._group,
                    capture_path=capture_path(self._run_dir, topic),
                    capture_match=self._capture_match,
                    max_bytes=self._max_bytes,
                    emit_event=self.emit_event,
                    ready_event=ready_event,
                    stop_event=self._stop,
                )
                self._capture_loops.append(loop)

                def _run_loop(cl=loop, t=topic):
                    try:
                        cl.run()
                    except Exception as exc:
                        # CaptureLoop thread death (e.g. ConnectionFailure from
                        # consume_loop's commit/seek/subscribe, or any exception
                        # the loop didn't catch): emit the fatal kafka.error so
                        # the run exits 1. Do NOT set self._stop here — sibling
                        # topics must continue. emit_event increments ``errors``
                        # under the lock (→ exit 1 at run()).
                        self.emit_event(
                            {
                                "event": "kafka.error",
                                "topic": t,
                                "error": str(exc),
                                "fatal": True,
                            }
                        )

                thread = threading.Thread(target=_run_loop, daemon=True)
                self._capture_threads.append(thread)
                thread.start()

            # Wait until every topic's ready_event is set OR the startup budget
            # elapses. Poll at a small interval so the budget is honored promptly.
            deadline = time.monotonic() + self._startup_budget
            while True:
                if all(ev.is_set() for ev in self._ready_events.values()):
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    not_ready = [
                        t for t in self._topics if not self._ready_events[t].is_set()
                    ]
                    raise ConnectionFailure(
                        f"listener did not become ready for topic {not_ready[0]}"
                    )
                time.sleep(min(0.02, remaining))

            # Emit the started line FIRST, then mark _started. Mirrors
            # MockEngine's rationale: if the emit raises, _started is still
            # False, so the except handler's shutdown() will NOT emit a spurious
            # summary for a stream that never received a started line.
            self.emit_event(
                {
                    "event": "started",
                    "run_id": self._run_id,
                    "topics": list(self._topics),
                    "group": self._group,
                    "cluster": self._cluster,
                    "started_at": _now_iso_z(),
                }
            )
            # Mark started: only now may shutdown emit a summary.
            self._started = True
            self._start_time = time.monotonic()

        except Exception:
            # On any exception, release what we acquired (join spawned threads).
            self.shutdown()
            raise

    # ------------------------------------------------------------------
    # runtime: signals, duration timer, block on stop
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Run the engine until the stop event is set.

        - Installs ``SIGTERM``/``SIGINT`` handlers that set the stop event
          (guard for non-main-thread ``ValueError``).
        - Arms a ``threading.Timer(duration, ...)`` if ``duration`` is set.
        - Blocks on the stop event.
        - Joins capture threads (timeout 2s) before reading the error tally so
          the exit code and the summary share one post-join snapshot (a late
          fatal ``kafka.error`` from a winding-down thread must not produce a
          false-green exit 0).
        - Returns ``1`` if any ``kafka.error`` occurred, else ``0``.
        - Restores prior signal handlers in ``finally``.
        """
        prev_term = None
        prev_int = None

        def _handler(signum, frame):
            self._stop.set()

        # Install signal handlers (guard for non-main-thread).
        try:
            prev_term = signal.signal(signal.SIGTERM, _handler)
            prev_int = signal.signal(signal.SIGINT, _handler)
        except (ValueError, OSError):
            pass

        try:
            # Arm the duration timer (cancellable; cancelled on shutdown so an
            # early shutdown doesn't leave a sleeping timer for the full duration).
            if self._duration is not None:
                self._duration_timer = threading.Timer(self._duration, self._stop.set)
                self._duration_timer.daemon = True
                self._duration_timer.start()

            # Block until stop is set (signal, duration timer, or shutdown).
            # Bounded loop (matches MockEngine.run): a short timed wait is
            # reliably interruptible across platforms, where an indefinite
            # ``wait()`` can block past an already-set flag on some Windows
            # runtimes (the mock engine uses the same pattern and passes on
            # Windows; the prior unbounded ``self._stop.wait()`` hung CI there).
            while not self._stop.is_set():
                self._stop.wait(0.1)

            # Signal stop so still-running capture threads begin winding down,
            # then JOIN before deciding the exit code. A loop finishing its
            # final append after _stop was set can emit a fatal kafka.error
            # during this window; reading ``errors`` before joining would miss
            # it and return 0 while the summary snapshot shows errors > 0 — a
            # false-green exit 0. Joining first makes the exit code and the
            # summary share one post-join snapshot. Joining an already-finished
            # thread is a no-op, so shutdown()'s later join is harmless.
            self._stop.set()
            for t in self._capture_threads:
                t.join(timeout=2.0)

            with self._emit_lock:
                errors = self.errors
            return 1 if errors > 0 else 0

        finally:
            try:
                if prev_term is not None:
                    signal.signal(signal.SIGTERM, prev_term)
                if prev_int is not None:
                    signal.signal(signal.SIGINT, prev_int)
            except (ValueError, OSError):
                pass

    # ------------------------------------------------------------------
    # shutdown: stop, cancel timer, join, emit summary
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Shutdown the engine: stop threads, emit the summary line.

        - Sets stop (in case not already set).
        - Cancels the duration timer (if armed).
        - Joins capture threads with timeout (stuck threads don't hang shutdown).
        - Emits the ``summary`` line ONLY if ``start()`` actually emitted
          ``started`` — a failed start must not produce a spurious summary for a
          stream that never received a started line.
        """
        # Signal stop (in case not already set).
        self._stop.set()

        # Cancel the duration timer (if armed).
        if self._duration_timer is not None:
            self._duration_timer.cancel()
            self._duration_timer = None

        # Capture loops exit on stop event (consume_loop returns once stop_event
        # is set). Join with a timeout so a stuck thread doesn't hang shutdown.
        for t in self._capture_threads:
            t.join(timeout=2.0)

        # Emit summary only if start() actually emitted started.
        if self._started:
            self._emit_summary()

    def _emit_summary(self) -> None:
        """Emit the summary line with per-topic captured counts and tallies.

        ``captured`` is the line count of each topic's ``<topic>.ndjson`` at
        shutdown (read from disk; a missing file → 0). ``overflowed`` is whether
        the topic appears in :attr:`overflowed_topics`.
        """
        duration_ms = 0
        if self._start_time is not None:
            duration_ms = int((time.monotonic() - self._start_time) * 1000)

        # Snapshot the tallies under the lock so a thread still emitting after a
        # join-timeout can't produce a torn snapshot. The summary line itself is
        # emitted atomically by emit_event (which re-acquires the lock).
        with self._emit_lock:
            overflowed = list(self.overflowed_topics)
            errors = self.errors

        topics_summary = []
        for topic in self._topics:
            try:
                text = capture_path(self._run_dir, topic).read_text(encoding="utf-8")
                captured = sum(1 for line in text.splitlines() if line.strip())
            except OSError:
                # Missing capture file → 0 (e.g. no messages arrived).
                captured = 0
            topics_summary.append(
                {
                    "topic": topic,
                    "captured": captured,
                    "overflowed": topic in overflowed,
                }
            )

        self.emit_event(
            {
                "event": "summary",
                "topics": topics_summary,
                "errors": errors,
                "duration_ms": duration_ms,
            }
        )
