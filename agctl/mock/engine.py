"""MockEngine: lifecycle owner for HTTP mock and Kafka reactors (DESIGN §4.2, §8.3).

The engine coordinates:
- HTTP mock server (Task 5) with stub matching and response templating
- Kafka reactors (Task 6) with jq match, capture, and reaction mechanics
- Single-writer NDJSON emission to stdout with a threading lock
- Probe-then-bind startup ordering (probes Kafka brokers before binding HTTP)
- Signal-driven shutdown (SIGTERM/SIGINT) with summary line
- Fail-fast mode (reactor STOP → immediate exit 1)
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from ..config.models import MocksConfig, parse_listen
from ..errors import ConfigError
from .http_server import MockHTTPServer
from .kafka_reactor import KafkaReactor as KafkaReactorClass


def _default_emit(line: dict) -> None:
    """Default emit function: write NDJSON line to stdout with flush.

    This is the stdout write pattern from http_commands._emit_stdout_line.
    The lock lives in MockEngine.emit_event, not here.
    """
    sys.stdout.write(json.dumps(line))
    sys.stdout.write("\n")
    sys.stdout.flush()


class MockEngine:
    """Lifecycle owner for HTTP mock and Kafka reactors.

    The engine manages:
    - Startup: probe Kafka brokers (if run_kafka), then bind HTTP server (if run_http)
    - Runtime: serve HTTP requests, consume Kafka topics, emit NDJSON events
    - Shutdown: stop threads, emit summary line, clean up resources

    All event emission goes through emit_event (single-writer with lock).
    """

    def __init__(
        self,
        mocks: MocksConfig | None,
        *,
        run_http: bool,
        run_kafka: bool,
        http_listen: str,
        kafka_client: Any,  # KafkaClient or None (only used if run_kafka=True)
        fail_fast: bool = False,
        duration: float | None = None,
        until_stopped: bool = True,
        emit_fn: Callable[[dict], None] = _default_emit,
        run_id: str | None = None,
    ):
        """Initialize the mock engine.

        Args:
            mocks: MocksConfig with http/kafka definitions (None for no-op engine).
            run_http: If True, start HTTP mock server.
            run_kafka: If True, start Kafka reactors.
            http_listen: HTTP listen address string (host:port).
            kafka_client: KafkaClient instance (required if run_kafka=True).
            fail_fast: If True, reactor STOP → immediate exit 1.
            duration: If set, stop after this many seconds.
            until_stopped: Ignored (kept for API compatibility).
            emit_fn: Callable to emit one NDJSON line (default: stdout).
            run_id: Engine run identifier (defaults to PID if None).
        """
        self._mocks = mocks
        self._run_http = run_http
        self._run_kafka = run_kafka
        self._http_listen = http_listen
        self._kafka_client = kafka_client
        self._fail_fast = fail_fast
        self._duration = duration
        self._emit_fn = emit_fn
        self._run_id = run_id if run_id is not None else str(os.getpid())

        # Event for coordinating shutdown
        self._stop = threading.Event()

        # Single-writer emission lock
        self._emit_lock = threading.Lock()

        # Summary counters (protected by _emit_lock)
        self._http_hits = 0
        self._http_unmatched = 0
        self._http_body_parse_skipped = 0
        self._kafka_reactions = 0
        self._kafka_skipped = 0
        self._kafka_errors = 0
        self._runtime_error = False  # Track if any runtime error occurred
        # Set True only after the started line is emitted; gates summary so a
        # failed start (which never emitted started) cannot emit a spurious
        # summary line.
        self._started = False

        # Engine components (set during start())
        self._http_server: MockHTTPServer | None = None
        self._reactors: list[KafkaReactorClass] = []
        self._http_thread: threading.Thread | None = None
        self._reactor_threads: list[threading.Thread] = []
        # Cancellable duration timer (threading.Timer); cancelled on shutdown
        # so an early shutdown doesn't leave a sleeping timer for the duration.
        self._duration_timer: threading.Timer | None = None

        # Startup time for duration_ms calculation
        self._start_time: float | None = None

    def emit_event(self, line: dict) -> None:
        """Emit a single NDJSON line with timestamp (single-writer).

        This method is called by HTTP handler threads and Kafka reactor threads.
        It acquires a lock, adds timestamp if absent, tallies summary counters,
        writes via emit_fn, flushes, and releases the lock.

        Args:
            line: Event dict (will be mutated to add timestamp if absent).
        """
        with self._emit_lock:
            # Add timestamp if absent (ISO-8601 Z)
            if "timestamp" not in line:
                line["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Tally summary counters by event name
            event_name = line.get("event")
            if event_name == "http.hit":
                self._http_hits += 1
            elif event_name == "http.unmatched":
                self._http_unmatched += 1
            elif event_name == "http.body_parse_skipped":
                self._http_body_parse_skipped += 1
            elif event_name == "kafka.reacted":
                self._kafka_reactions += 1
            elif event_name == "kafka.skipped":
                self._kafka_skipped += 1
            elif event_name == "kafka.error":
                self._kafka_errors += 1
                # Track fatal errors for fail-fast
                if line.get("fatal"):
                    self._runtime_error = True

            # Write and flush via injected emit_fn
            self._emit_fn(line)

    def start(self) -> None:
        """Start the engine: probe Kafka, bind HTTP, emit started line.

        Probe-then-bind ordering (DESIGN §9):
        1. If run_kafka, build reactors and call prepare() (probes brokers).
           On any failure, close already-prepared reactors and re-raise.
        2. If run_http, bind MockHTTPServer (raises ConfigError if port in use).
        3. Emit the started line with http/kafka details.

        On any exception, run shutdown() to release acquired resources.
        """
        try:
            # Step 1: Probe Kafka brokers (if run_kafka)
            if self._run_kafka:
                if self._mocks is None or self._mocks.kafka is None:
                    raise ConfigError("run_kafka=True but no Kafka mocks configured", {})

                if self._kafka_client is None:
                    raise ConfigError("run_kafka=True but kafka_client is None", {})

                # Build reactors and prepare (probe) each
                for name, reactor_config in self._mocks.kafka.reactors.items():
                    reactor = KafkaReactorClass(
                        name=name,
                        config=reactor_config,
                        client=self._kafka_client,
                        emit_event=self.emit_event,
                        stop_event=self._stop,
                        fail_fast=self._fail_fast,
                        run_id=self._run_id,
                    )
                    self._reactors.append(reactor)

                # Prepare (probe) all reactors
                for reactor in self._reactors:
                    try:
                        reactor.prepare()
                    except Exception:
                        # On failure, close already-prepared reactors and re-raise
                        self._shutdown_reactors()
                        raise

            # Step 2: Bind HTTP server (if run_http)
            if self._run_http:
                if self._mocks is None or self._mocks.http is None:
                    raise ConfigError("run_http=True but no HTTP mocks configured", {})

                http_config = self._mocks.http
                try:
                    listen_addr = parse_listen(self._http_listen)
                except ValueError as e:
                    raise ConfigError(f"Invalid http_listen: {e}", {})

                # Build stubs dict (preserve insertion order)
                stubs = dict(http_config.stubs)

                # Bind the server (may raise OSError with EADDRINUSE)
                try:
                    self._http_server = MockHTTPServer(
                        server_address=listen_addr,
                        RequestHandlerClass=None,  # Auto-create
                        stubs=stubs,
                        emit_event=self.emit_event,
                        concurrency_cap=64,
                    )
                except OSError as e:
                    # Check for EADDRINUSE (port already in use)
                    if e.errno == 48 or e.errno == 98 or "address already in use" in str(e).lower():
                        raise ConfigError(
                            f"HTTP bind failed: port {self._http_listen} already in use. "
                            f"Kill the stale mock with: agctl mock stop",
                            {},
                        )
                    raise

            # Step 3: Emit started line
            self._emit_started_line()
            # Mark started: only now may shutdown emit a summary. If start()
            # fails before this point, the except handler calls shutdown(),
            # which must NOT emit a spurious summary for a stream that never
            # received a started line.
            self._started = True

            # Record start time for duration_ms
            self._start_time = time.monotonic()

        except Exception:
            # On any exception, release what we acquired
            self.shutdown()
            raise

    def run(self) -> int:
        """Run the engine until stop event is set.

        - Installs SIGTERM/SIGINT handlers that set the stop event.
        - Starts HTTP serve thread and one thread per reactor.
        - If duration is set, arms a timer that sets stop.
        - Blocks until stop is set.
        - Restores previous signal handlers in finally.
        - Returns 0 if no errors, 1 if runtime errors occurred.

        Under fail_fast, returns 1 as soon as a reactor signals STOP.
        """
        # Save previous signal handlers
        prev_term = None
        prev_int = None

        def _handler(signum, frame):
            self._stop.set()

        # Install signal handlers (guard for non-main-thread)
        try:
            prev_term = signal.signal(signal.SIGTERM, _handler)
            prev_int = signal.signal(signal.SIGINT, _handler)
        except (ValueError, OSError):
            pass

        try:
            # Start HTTP serve thread (if run_http)
            if self._run_http and self._http_server is not None:
                def _serve_http():
                    self._http_server.serve_forever()

                self._http_thread = threading.Thread(target=_serve_http, daemon=True)
                self._http_thread.start()

            # Start reactor threads (if run_kafka)
            if self._run_kafka:
                for reactor in self._reactors:
                    def _run_reactor(r=reactor):
                        try:
                            r.run()
                        except Exception as exc:
                            # Reactor thread death (e.g. ConnectionFailure from
                            # consume_loop's commit/seek/subscribe, or any
                            # exception the reactor's _handle didn't catch): emit
                            # the spec §11 fatal kafka.error so the run exits 1.
                            # Do NOT set self._stop here — sibling reactors must
                            # continue. emit_event's fatal-handling increments
                            # _kafka_errors and sets _runtime_error (→ exit 1 at
                            # shutdown; under fail_fast the run loop breaks
                            # immediately via the _runtime_error check above).
                            self.emit_event({
                                "event": "kafka.error",
                                "reactor": r._name,
                                "topic": r._config.topic,
                                "error": str(exc),
                                "fatal": True,
                            })

                    t = threading.Thread(target=_run_reactor, daemon=True)
                    self._reactor_threads.append(t)
                    t.start()

            # Arm duration timer (if set) — a cancellable threading.Timer so an
            # early shutdown cancels it rather than sleeping the full duration.
            if self._duration is not None:
                self._duration_timer = threading.Timer(self._duration, self._stop.set)
                self._duration_timer.daemon = True
                self._duration_timer.start()

            # Block until stop is set (or fail-fast triggered)
            while not self._stop.is_set():
                # Check for fail-fast (reactor set _runtime_error via fatal kafka.error)
                if self._fail_fast and self._runtime_error:
                    break

                # Wait for stop with a small timeout to check fail-fast condition
                self._stop.wait(0.1)

            # Determine exit code (DESIGN §11): exit 1 if ANY runtime error
            # occurred (any kafka.error, fatal or not) or a fatal/fail-fast
            # stop was triggered; else 0. The fail_fast immediate-exit-on-fatal
            # mid-run behavior is preserved by the loop break above.
            if self._kafka_errors > 0 or self._runtime_error:
                return 1
            return 0

        finally:
            # Restore previous signal handlers
            try:
                if prev_term is not None:
                    signal.signal(signal.SIGTERM, prev_term)
                if prev_int is not None:
                    signal.signal(signal.SIGINT, prev_int)
            except (ValueError, OSError):
                pass

    def shutdown(self) -> None:
        """Shutdown the engine: stop threads, emit summary line.

        - Stops HTTP server (if running).
        - Reactor loops exit on stop event (their consumers close internally).
        - Joins all threads with timeout (so stuck threads don't hang shutdown).
        - Emits summary line with counters.
        """
        # Signal stop (in case not already set)
        self._stop.set()

        # Cancel the duration timer (if armed) so an early shutdown doesn't
        # leave a sleeping timer for the full duration.
        if self._duration_timer is not None:
            self._duration_timer.cancel()
            self._duration_timer = None

        # Stop HTTP server
        if self._http_server is not None:
            self._http_server.shutdown()
            self._http_server.server_close()

        # Reactor loops exit on stop event (no explicit shutdown needed)

        # Join HTTP thread with timeout
        if self._http_thread is not None:
            self._http_thread.join(timeout=2.0)

        # Join reactor threads with timeout
        for t in self._reactor_threads:
            t.join(timeout=2.0)

        # Emit summary line only if start() actually emitted started — a failed
        # start (probe/bind error) must not produce a spurious summary for a
        # stream that never received a started line.
        if self._started:
            self._emit_summary_line()

    def _shutdown_reactors(self) -> None:
        """Close reactor resources (called during startup probe failure).

        Invokes ``reactor.close()`` on each prepared reactor so any resource
        ``prepare()`` opened is released. Today ``close()`` is a documented
        no-op (probe builds+closes its own consumer), but the contract is
        established for future reactor lifecycle changes.
        """
        for reactor in self._reactors:
            reactor.close()

    def _emit_started_line(self) -> None:
        """Emit the started line with http/kafka details."""
        started_line: dict[str, Any] = {"event": "started"}

        # Add HTTP details
        if self._run_http and self._http_server is not None:
            # Report the BOUND address: if the caller bound port 0, the real
            # port lives in server_address (updated by socketserver after
            # bind). Fall back to the input string if no server is present.
            bound_host, bound_port = self._http_server.server_address
            started_line["http"] = {
                "listen": f"{bound_host}:{bound_port}",
                "stubs": len(self._http_server.stubs),
            }
        else:
            started_line["http"] = None

        # Add Kafka details
        if self._run_kafka and self._reactors:
            started_line["kafka"] = {
                "reactors": [
                    {
                        "name": r._name,
                        "topic": r._config.topic,
                        "consumer_group": r.resolved_group(),
                    }
                    for r in self._reactors
                ]
            }
        else:
            started_line["kafka"] = None

        self.emit_event(started_line)

    def _emit_summary_line(self) -> None:
        """Emit the summary line with counters."""
        duration_ms = 0
        if self._start_time is not None:
            duration_ms = int((time.monotonic() - self._start_time) * 1000)

        # Snapshot the six counters under the lock so a reactor still emitting
        # after a join-timeout can't produce a torn snapshot (counters read at
        # different instants). The summary line itself is emitted atomically by
        # emit_event (which re-acquires the lock) outside this block.
        with self._emit_lock:
            http_hits = self._http_hits
            http_unmatched = self._http_unmatched
            http_body_parse_skipped = self._http_body_parse_skipped
            kafka_reactions = self._kafka_reactions
            kafka_skipped = self._kafka_skipped
            kafka_errors = self._kafka_errors

        summary_line = {
            "event": "summary",
            "http_hits": http_hits,
            "http_unmatched": http_unmatched,
            "http_body_parse_skipped": http_body_parse_skipped,
            "kafka_reactions": kafka_reactions,
            "kafka_skipped": kafka_skipped,
            "kafka_errors": kafka_errors,
            "duration_ms": duration_ms,
        }

        self.emit_event(summary_line)
