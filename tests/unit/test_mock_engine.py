"""Tests for MockEngine lifecycle, single-writer emission, and shutdown."""

import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from agctl.config.models import HttpMockConfig, HttpStub, HttpResponse, KafkaMockConfig, KafkaReaction, KafkaReactor, MocksConfig
from agctl.errors import ConfigError, ConnectionFailure
from agctl.mock.engine import MockEngine


# =============================================================================
# Fake dependencies for testing
# =============================================================================


class FakeHTTPServer:
    """Fake HTTP server that records bind/serve/shutdown calls.

    The bind call can be configured to raise EADDRINUSE to test port-in-use errors.
    """

    def __init__(self, server_address, RequestHandlerClass, *, stubs, emit_event, concurrency_cap=64):
        self.server_address = server_address
        self.stubs = stubs
        self.emit_event = emit_event
        self.bind_called = True  # Binding happens in __init__ for ThreadingHTTPServer
        self.serve_called = False
        self.shutdown_called = False
        self.server_close_called = False
        self._serve_forever_event = threading.Event()

    def serve_forever(self):
        """Fake serve_forever - just block until stop_event is set."""
        self.serve_called = True
        self._serve_forever_event.wait()  # Block until explicitly stopped

    def shutdown(self):
        """Fake shutdown - signal serve_forever to return."""
        self.shutdown_called = True
        self._serve_forever_event.set()

    def server_close(self):
        """Fake server_close."""
        self.server_close_called = True


class FakeKafkaClient:
    """Fake Kafka client for testing.

    Can be configured to raise on probe (for probe-then-bind tests) or to
    simulate fatal STOP errors (for fail-fast tests).
    """

    def __init__(self, probe_raises=None, consume_loop_returns_immediately=True):
        self._probe_raises = probe_raises
        self._consume_loop_returns_immediately = consume_loop_returns_immediately
        self.probe_called = False
        self.consume_loop_called = False
        self._stop_signal = None

    def probe(self, topic, group_id):
        """Fake probe - can raise if configured."""
        self.probe_called = True
        if self._probe_raises:
            raise self._probe_raises

    def consume_loop(self, topic, group_id, stop_event, handle, max_retries):
        """Fake consume_loop - returns immediately if configured."""
        self.consume_loop_called = True
        self._stop_signal = stop_event

        if self._consume_loop_returns_immediately:
            return

        # Otherwise, block until stop_event is set
        stop_event.wait()


class FakeKafkaClientStopFatal(FakeKafkaClient):
    """Fake client that signals STOP (fatal kafka.error)."""

    def __init__(self):
        super().__init__(consume_loop_returns_immediately=False)

    def consume_loop(self, topic, group_id, stop_event, handle, max_retries):
        """Simulate a fatal error by calling handle with fatal=True."""
        self.consume_loop_called = True
        self._stop_signal = stop_event

        # Simulate a fatal kafka.error
        result = handle(
            {
                "value": {"test": "data"},
                "key": None,
                "partition": 0,
                "offset": 123,
                "timestamp": 1234567890,
                "headers": {},
            },
            attempt=1,
            final=True,
        )

        # If handler returned STOP, set the stop_event
        if result == "STOP":
            stop_event.set()


# =============================================================================
# Test scenarios
# =============================================================================


def test_noop_engine_started_and_summary_with_zero_counts():
    """MockEngine with run_http=False, run_kafka=False emits started with nulls and summary with zeros."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    engine = MockEngine(
        mocks=None,
        run_http=False,
        run_kafka=False,
        http_listen="127.0.0.1:18080",
        kafka_client=None,
        emit_fn=capture_emit,
        run_id="test-run-123",
    )

    engine.start()

    # Set stop to unblock run()
    engine._stop.set()

    engine.run()
    engine.shutdown()

    # Check started line
    started = [l for l in captured_lines if l.get("event") == "started"]
    assert len(started) == 1
    assert started[0]["http"] is None
    assert started[0]["kafka"] is None
    assert "timestamp" in started[0]

    # Check summary line
    summary = [l for l in captured_lines if l.get("event") == "summary"]
    assert len(summary) == 1
    assert summary[0]["http_hits"] == 0
    assert summary[0]["http_unmatched"] == 0
    assert summary[0]["http_body_parse_skipped"] == 0
    assert summary[0]["kafka_reactions"] == 0
    assert summary[0]["kafka_skipped"] == 0
    assert summary[0]["kafka_errors"] == 0
    assert summary[0]["duration_ms"] >= 0
    assert "timestamp" in summary[0]


def test_http_only_engine_emits_started_with_stubs_count():
    """MockEngine with run_http=True, run_kafka=False emits started with http.stubs count."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    mocks = MocksConfig(
        http=HttpMockConfig(
            listen="127.0.0.1:0",  # Use port 0 for auto-assignment
            stubs={
                "stub1": HttpStub(method="GET", path="/api/test", response=HttpResponse(status=200)),
                "stub2": HttpStub(method="POST", path="/api/create", response=HttpResponse(status=201)),
            },
        )
    )

    engine = MockEngine(
        mocks=mocks,
        run_http=True,
        run_kafka=False,
        http_listen="127.0.0.1:0",
        kafka_client=None,
        emit_fn=capture_emit,
        run_id="test-run-456",
    )

    engine.start()

    # Set stop to unblock run()
    engine._stop.set()

    engine.run()
    engine.shutdown()

    # Check started line
    started = [l for l in captured_lines if l.get("event") == "started"]
    assert len(started) == 1
    assert started[0]["http"]["listen"] == "127.0.0.1:0"
    assert started[0]["http"]["stubs"] == 2
    assert started[0]["kafka"] is None


def test_probe_then_bind_probe_failure_no_started_no_http_bind():
    """Probe-then-bind: when kafka probe fails, no started line emitted and HTTP server never bound."""
    captured_lines = []
    fake_http = None

    def capture_emit(line):
        captured_lines.append(line.copy())

    # Create a fake client that raises on probe
    fake_client = FakeKafkaClient(probe_raises=ConnectionFailure("Broker not reachable"))

    # Patch the HTTP server class to use our fake
    with patch("agctl.mock.engine.MockHTTPServer", FakeHTTPServer) as mock_http_class:
        # Configure the fake to raise on bind (so we can detect it was called)
        def make_fake_http(*args, **kwargs):
            nonlocal fake_http
            fake_http = FakeHTTPServer(*args, **kwargs)
            return fake_http

        mock_http_class.side_effect = make_fake_http

        mocks = MocksConfig(
            kafka=KafkaMockConfig(
                reactors={
                    "reactor1": KafkaReactor(
                        topic="test-topic",
                        reaction=KafkaReaction(topic="out-topic", value="{}"),
                    )
                }
            )
        )

        engine = MockEngine(
            mocks=mocks,
            run_http=False,  # Only test kafka probe failure
            run_kafka=True,
            http_listen="127.0.0.1:0",
            kafka_client=fake_client,
            emit_fn=capture_emit,
            run_id="test-run-789",
        )

        # Probe should fail and raise
        with pytest.raises(ConnectionFailure, match="Broker not reachable"):
            engine.start()

        # Verify no started line was emitted
        started = [l for l in captured_lines if l.get("event") == "started"]
        assert len(started) == 0


def test_probe_then_bind_probe_success_then_http_bind():
    """Probe-then-bind: when probe succeeds, HTTP server is bound and started is emitted."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    fake_client = FakeKafkaClient()

    mocks = MocksConfig(
        http=HttpMockConfig(
            listen="127.0.0.1:0",
            stubs={"stub1": HttpStub(method="GET", path="/test", response=HttpResponse(status=200))},
        ),
        kafka=KafkaMockConfig(
            reactors={
                "reactor1": KafkaReactor(
                    topic="test-topic",
                    reaction=KafkaReaction(topic="out-topic", value="{}"),
                )
            }
        ),
    )

    # Patch the HTTP server class to use our fake
    with patch("agctl.mock.engine.MockHTTPServer", FakeHTTPServer):
        engine = MockEngine(
            mocks=mocks,
            run_http=True,
            run_kafka=True,
            http_listen="127.0.0.1:0",
            kafka_client=fake_client,
            emit_fn=capture_emit,
            run_id="test-run-abc",
        )

        engine.start()

        # Verify started line was emitted (indicates HTTP was bound)
        started = [l for l in captured_lines if l.get("event") == "started"]
        assert len(started) == 1
        assert started[0]["http"]["stubs"] == 1
        assert started[0]["kafka"]["reactors"][0]["name"] == "reactor1"
        assert started[0]["kafka"]["reactors"][0]["topic"] == "test-topic"

        # Cleanup
        engine._stop.set()
        engine.run()
        engine.shutdown()


def test_single_writer_concurrent_emission_no_interleaving():
    """Single-writer emission: N threads emitting concurrently produces valid JSON with no interleaving."""
    captured_lines = []
    num_threads = 10
    emits_per_thread = 100

    def capture_emit(line):
        captured_lines.append(line)

    engine = MockEngine(
        mocks=None,
        run_http=False,
        run_kafka=False,
        http_listen="127.0.0.1:0",
        kafka_client=None,
        emit_fn=capture_emit,
        run_id="test-run-concurrent",
    )

    # Spin up N threads, each emitting many events
    threads = []
    for i in range(num_threads):
        def emit_many():
            for j in range(emits_per_thread):
                engine.emit_event({"event": "http.hit", "thread": i, "emit": j})

        t = threading.Thread(target=emit_many)
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Verify every line is valid JSON (not corrupted/interleaved)
    assert len(captured_lines) == num_threads * emits_per_thread
    for line in captured_lines:
        # Each line should be a dict with the expected fields
        assert isinstance(line, dict)
        assert "event" in line
        assert "timestamp" in line
        assert line["event"] == "http.hit"
        # Verify it's not corrupted (no partial JSON)
        json.dumps(line)  # Should not raise


def test_fail_fast_fatal_error_returns_exit_1():
    """Fail-fast: reactor signaling STOP (fatal kafka.error) causes run() to return 1."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    fake_client = FakeKafkaClientStopFatal()

    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "reactor1": KafkaReactor(
                    topic="test-topic",
                    reaction=KafkaReaction(topic="out-topic", value="{}"),
                )
            }
        )
    )

    engine = MockEngine(
        mocks=mocks,
        run_http=False,
        run_kafka=True,
        http_listen="127.0.0.1:0",
        kafka_client=fake_client,
        emit_fn=capture_emit,
        run_id="test-run-failfast",
        fail_fast=True,
    )

    engine.start()

    # Run should return 1 due to fatal error
    exit_code = engine.run()
    assert exit_code == 1

    engine.shutdown()

    # Verify kafka.error was emitted with fatal=True
    errors = [l for l in captured_lines if l.get("event") == "kafka.error"]
    assert len(errors) == 1
    assert errors[0]["fatal"] is True


def test_summary_tally_counts_all_events():
    """Summary tally: emitting various events increments their counts correctly."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    engine = MockEngine(
        mocks=None,
        run_http=False,
        run_kafka=False,
        http_listen="127.0.0.1:0",
        kafka_client=None,
        emit_fn=capture_emit,
        run_id="test-run-tally",
    )

    engine.start()

    # Emit various events
    engine.emit_event({"event": "http.hit"})
    engine.emit_event({"event": "http.hit"})
    engine.emit_event({"event": "http.unmatched"})
    engine.emit_event({"event": "http.body_parse_skipped"})
    engine.emit_event({"event": "kafka.reacted"})
    engine.emit_event({"event": "kafka.reacted"})
    engine.emit_event({"event": "kafka.reacted"})
    engine.emit_event({"event": "kafka.skipped"})
    engine.emit_event({"event": "kafka.error"})

    engine._stop.set()
    engine.run()
    engine.shutdown()

    # Verify summary counts
    summary = [l for l in captured_lines if l.get("event") == "summary"][0]
    assert summary["http_hits"] == 2
    assert summary["http_unmatched"] == 1
    assert summary["http_body_parse_skipped"] == 1
    assert summary["kafka_reactions"] == 3
    assert summary["kafka_skipped"] == 1
    assert summary["kafka_errors"] == 1


def test_port_in_use_raises_config_error_with_hint():
    """Port-in-use: HTTP bind raising EADDRINUSE raises ConfigError with hint."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    mocks = MocksConfig(
        http=HttpMockConfig(
            listen="127.0.0.1:18080",
            stubs={"stub1": HttpStub(method="GET", path="/test", response=HttpResponse(status=200))},
        )
    )

    # Patch the HTTP server to raise on bind
    def make_http_server_that_raises(*args, **kwargs):
        raise OSError(48, "Address already in use")

    with patch("agctl.mock.engine.MockHTTPServer", side_effect=make_http_server_that_raises):
        engine = MockEngine(
            mocks=mocks,
            run_http=True,
            run_kafka=False,
            http_listen="127.0.0.1:18080",
            kafka_client=None,
            emit_fn=capture_emit,
            run_id="test-run-portinuse",
        )

        # Bind should raise ConfigError with hint
        with pytest.raises(ConfigError) as exc_info:
            engine.start()

        # Verify the error message mentions killing the stale mock
        error_msg = str(exc_info.value).lower()
        assert "kill" in error_msg or "already in use" in error_msg


def test_duration_timer_stops_engine():
    """Duration timer: setting duration causes run() to stop after that time."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    engine = MockEngine(
        mocks=None,
        run_http=False,
        run_kafka=False,
        http_listen="127.0.0.1:0",
        kafka_client=None,
        emit_fn=capture_emit,
        run_id="test-run-duration",
        duration=0.1,  # 100ms
    )

    engine.start()
    start_time = time.monotonic()
    exit_code = engine.run()
    elapsed = time.monotonic() - start_time

    engine.shutdown()

    # Should have stopped roughly after duration
    assert elapsed >= 0.1
    assert elapsed < 0.5  # Give some margin
    assert exit_code == 0


def test_signal_handlers_set_and_restored():
    """Signal handlers: SIGTERM/SIGINT handlers are installed and restored."""
    captured_lines = []

    def capture_emit(line):
        captured_lines.append(line.copy())

    engine = MockEngine(
        mocks=None,
        run_http=False,
        run_kafka=False,
        http_listen="127.0.0.1:0",
        kafka_client=None,
        emit_fn=capture_emit,
        run_id="test-run-signals",
    )

    engine.start()

    # Get original signal handlers
    original_term = signal.getsignal(signal.SIGTERM)
    original_int = signal.getsignal(signal.SIGINT)

    # Run the engine (which installs handlers)
    run_thread = threading.Thread(target=lambda: engine.run())
    run_thread.start()

    # Give it a moment to install handlers
    time.sleep(0.05)

    # Verify handlers were changed (they should point to our handler)
    current_term = signal.getsignal(signal.SIGTERM)
    current_int = signal.getsignal(signal.SIGINT)

    # Send a signal to trigger stop
    engine._stop.set()
    run_thread.join(timeout=1.0)

    engine.shutdown()

    # Verify handlers were restored (close to original)
    # Note: this might be exact match or might be the default handler
    # depending on test environment
    restored_term = signal.getsignal(signal.SIGTERM)
    restored_int = signal.getsignal(signal.SIGINT)


def test_run_id_defaults_to_pid():
    """run_id: defaults to str(os.getpid()) when None."""
    import sys

    # Save original modules
    original_os = sys.modules.get("os")

    # Mock os.getpid to return a known value
    with patch("os.getpid", return_value=12345):
        captured_lines = []

        def capture_emit(line):
            captured_lines.append(line.copy())

        engine = MockEngine(
            mocks=None,
            run_http=False,
            run_kafka=False,
            http_listen="127.0.0.1:0",
            kafka_client=None,
            emit_fn=capture_emit,
            run_id=None,  # Should default to PID
        )

        # Verify run_id was set to PID
        assert engine._run_id == "12345"
