"""Live integration tests: cross-transport mock effects over a real broker.

Composes the unit-tested seams (Tasks 1-9) end-to-end: ``agctl mock run`` as a
real subprocess, real HTTP sockets, and a real plaintext Kafka broker (via the
session's ``require_kafka`` fixture — testcontainers under ``AGCTL_TEST_LIVE=1``,
manual via ``AGCTL_TEST_KAFKA_BROKER``, else a clean self-skip, exactly like
``test_mock_commands.py``).

Scenarios (brief Task 10, Step 1):

1. HTTP→Kafka single — a stub whose kafka effect produces one message.
2. HTTP→Kafka multi — one ``values`` list of two ⇒ two ``kafka.produced``.
3. Kafka→HTTP — a reactor whose http effect calls a sink stub on the SAME
   mock; the sink's ``http.hit`` proves the rendered body arrived (the sink
   body-MATCHES on the rendered values, so an unrendered template would miss).
4. Chaining — reactor effects [http (captures ``ackId``), kafka (renders it)]
   ⇒ the audit topic carries the CAPTURED ack id.
5. Fatal effect — an http effect against an unreachable service ⇒
   ``effect.error`` fatal:true in the log and ``mock run`` exits 1.
6. Daemon fatal verdict — same failure under ``mock start``: ``mock stop``
   raises (CLI envelope: exit 1, ``error.type == "AssertionError"``).
7. Back-compat — a legacy ``reaction`` reactor still emits ``kafka.reacted``
   (never ``kafka.produced``) and the summary keeps the pre-change keys.

Harness discipline (mirrors ``test_mock_commands.py``): start ``mock run`` with
stdout→PIPE, poll stdout for the ``started`` line before driving anything,
drive via the REAL CLI (``agctl kafka produce`` / ``agctl http request`` /
``agctl kafka consume``), then SIGTERM + wait (never SIGKILL) and grep the
NDJSON log. Topics carry a per-run unique suffix (the existing suite's
``<prefix>.<uuid8>`` convention) so parallel/serial runs never cross-
contaminate the shared broker.

Scenarios 3/4 need the mock's own address inside its config (the effect calls
back into the same mock). ``mock run`` can bind an ephemeral port but the
config must name a concrete one, so those tests pre-allocate a free port
(bind :0, read, close) and write it into the config before starting — the same
trick ``test_mock_daemon.py`` uses for ``mock start``.
"""

from __future__ import annotations

import json
import select
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest


def _build_config(broker: str, mocks_config: dict, services: dict | None = None) -> str:
    """Serialize a minimal agctl.yaml whose default cluster is the live broker."""
    config: dict = {
        "version": "3",
        "kafka": {
            "clusters": {"default": {"brokers": [broker]}},
            "default_cluster": "default",
        },
        "mocks": mocks_config,
    }
    if services:
        config["services"] = services
    return json.dumps(config)


def _free_port() -> int:
    """Allocate a free TCP port (bind :0, read the assigned port, close)."""
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _run_cli(
    config_file: Path, args: list[str], timeout: float = 90.0
) -> subprocess.CompletedProcess:
    """Run one real agctl CLI command against the test's config file."""
    return subprocess.run(
        [sys.executable, "-m", "agctl", "--config", str(config_file), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )


class MockRunHandle:
    """A running ``agctl mock run`` subprocess plus its parsed NDJSON lines.

    The constructor polls stdout until the ``started`` line arrives (never a
    fixed sleep) and records the bound HTTP base URL. Kafka-only runs emit
    ``started`` with ``http: null`` — ``base_url`` is then None, which those
    tests ignore.
    """

    def __init__(self, config_file: Path):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "agctl", "--config", str(config_file),
             "mock", "run", "--until-stopped"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.lines: list[str] = []
        self.base_url: str | None = None
        self._wait_for_started(timeout=30.0)

    def _wait_for_started(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break  # died early; drain what we have and let asserts fail
            line = self.proc.stdout.readline()
            if not line:
                time.sleep(0.1)
                continue
            line = line.strip()
            if not line:
                continue
            self.lines.append(line)
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") == "started":
                listen = (event.get("http") or {}).get("listen", "")
                if listen:
                    self.base_url = f"http://{listen}"
                return

    def pump(self, budget: float = 0.3) -> None:
        """Drain currently-available stdout lines into ``self.lines``.

        The engine flushes each event line under its emit lock, so a line
        reported ready by select() is complete. Used while polling for
        asynchronous events (reactor effects land on reactor threads).
        """
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.proc.stdout], [], [], 0.05)
            if not ready:
                continue
            line = self.proc.stdout.readline()
            if not line:
                return  # EOF
            line = line.strip()
            if line:
                self.lines.append(line)

    def events(self) -> list[dict]:
        """Parse every NDJSON line collected so far into event dicts."""
        out = []
        for line in self.lines:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return out

    def events_of(self, name: str) -> list[dict]:
        return [e for e in self.events() if e.get("event") == name]

    def wait_for(self, description: str, predicate, timeout: float = 20.0):
        """Pump stdout and poll ``predicate(events)`` until truthy.

        Returns the first truthy result. Raises AssertionError (with the
        events seen so far) on timeout — async effects must be polled, not
        read once.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.pump()
            result = predicate(self.events())
            if result:
                return result
            time.sleep(0.1)
        self.pump()
        raise AssertionError(
            f"timed out waiting for {description}; events so far: "
            f"{json.dumps(self.events())}"
        )

    def stop_and_collect(self, timeout: float = 15.0) -> list[dict]:
        """SIGTERM, wait for exit (never SIGKILL), drain stdout, return events."""
        self.proc.terminate()
        self.proc.wait(timeout=timeout)
        remaining = self.proc.stdout.read() if self.proc.stdout else ""
        for line in remaining.splitlines():
            line = line.strip()
            if line:
                self.lines.append(line)
        return self.events()

    def summary(self) -> dict:
        summaries = self.events_of("summary")
        assert len(summaries) == 1, f"expected 1 summary line, got {summaries}"
        return summaries[0]


class TestHttpToKafkaEffects:
    """HTTP stub → kafka effect over real sockets + a real broker."""

    def test_http_stub_kafka_effect_single_message(self, require_kafka, tmp_path):
        """POST /fire (stub kafka effect, single ``value``) produces exactly
        one message on the events topic; the log carries one matching
        ``kafka.produced`` and the summary tallies ``kafka_produced == 1``.

        Asserts:
        - ``agctl http request`` against the stub returns 200 (trigger OK)
        - ``agctl kafka consume --expect-count 1 --match '.value.id == "o1"'``
          exits 0 (the produced message carries the rendered body)
        - exactly one ``kafka.produced`` for the events topic
        - summary: kafka_produced=1, http_hits=1, effect_errors=0; exit 0
        """
        broker = require_kafka
        run = uuid.uuid4().hex[:8]
        events_topic = f"t10.fx.orders-events.{run}"

        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(broker, {
            "http": {
                "listen": "127.0.0.1:0",
                "stubs": {
                    "fire": {
                        "description": "fire an order event",
                        "method": "POST",
                        "path": "/fire",
                        "response": {"status": 200, "body": {"ok": True}},
                        "effects": [{
                            "type": "kafka",
                            "topic": events_topic,
                            "value": {"id": "{id}", "event": "OrderCreated"},
                        }],
                    },
                },
            },
        }))

        mock = MockRunHandle(config_file)
        assert mock.base_url is not None, f"mock did not start: {mock.lines}"

        request = _run_cli(config_file, [
            "http", "request",
            "--url", f"{mock.base_url}/fire",
            "--method", "POST",
            "--body", json.dumps({"id": "o1"}),
        ])
        assert request.returncode == 0, (
            f"http request failed: {request.stdout} {request.stderr}"
        )
        assert json.loads(request.stdout)["result"]["status_code"] == 200

        consume = _run_cli(config_file, [
            "kafka", "consume",
            "--topic", events_topic,
            "--timeout", "15",
            "--expect-count", "1",
            "--match", '.value.id == "o1"',
        ])
        assert consume.returncode == 0, (
            f"kafka consume failed: rc={consume.returncode} "
            f"out={consume.stdout} err={consume.stderr}"
        )

        events = mock.stop_and_collect()
        produced = [e for e in events
                    if e.get("event") == "kafka.produced"
                    and e.get("topic") == events_topic]
        assert len(produced) == 1, f"expected 1 kafka.produced, got {produced}"
        assert produced[0]["trigger"] == "fire"

        summary = mock.summary()
        assert summary["kafka_produced"] == 1
        assert summary["http_hits"] == 1
        assert summary["effect_errors"] == 0
        assert mock.proc.returncode == 0, (
            f"stderr: {mock.proc.stderr.read()}"
        )

    def test_http_stub_kafka_effect_multi_values(self, require_kafka, tmp_path):
        """One ``values`` list of two produces two messages (expect-count 2,
        both seqs present) and two ``kafka.produced`` events."""
        broker = require_kafka
        run = uuid.uuid4().hex[:8]
        events_topic = f"t10.fx.orders-events.{run}"

        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(broker, {
            "http": {
                "listen": "127.0.0.1:0",
                "stubs": {
                    "fanout": {
                        "description": "produce a fanout pair",
                        "method": "POST",
                        "path": "/fanout",
                        "response": {"status": 200, "body": {"ok": True}},
                        "effects": [{
                            "type": "kafka",
                            "topic": events_topic,
                            "values": [
                                {"value": {"id": "{id}", "seq": 1}},
                                {"value": {"id": "{id}", "seq": 2}},
                            ],
                        }],
                    },
                },
            },
        }))

        mock = MockRunHandle(config_file)
        assert mock.base_url is not None, f"mock did not start: {mock.lines}"

        request = _run_cli(config_file, [
            "http", "request",
            "--url", f"{mock.base_url}/fanout",
            "--method", "POST",
            "--body", json.dumps({"id": "m1"}),
        ])
        assert request.returncode == 0, (
            f"http request failed: {request.stdout} {request.stderr}"
        )

        consume = _run_cli(config_file, [
            "kafka", "consume",
            "--topic", events_topic,
            "--timeout", "15",
            "--expect-count", "2",
            "--match", '.value.id == "m1"',
        ])
        assert consume.returncode == 0, (
            f"kafka consume failed: rc={consume.returncode} "
            f"out={consume.stdout} err={consume.stderr}"
        )
        messages = json.loads(consume.stdout)["result"]["messages"]
        seqs = sorted(m["value"]["seq"] for m in messages)
        assert seqs == [1, 2], f"expected both values produced, got {seqs}"

        events = mock.stop_and_collect()
        produced = [e for e in events
                    if e.get("event") == "kafka.produced"
                    and e.get("topic") == events_topic]
        assert len(produced) == 2, f"expected 2 kafka.produced, got {produced}"

        assert mock.summary()["kafka_produced"] == 2


class TestKafkaToHttpEffects:
    """Kafka reactor → http effect calling a sink stub on the same mock.

    The effect must name the mock's address in the config, so these tests
    pre-allocate a concrete port (the config cannot express "this mock's own
    ephemeral port").
    """

    def test_reactor_http_effect_calls_sink_stub(self, require_kafka, tmp_path):
        """A reactor on a commands topic fires an http effect at a sink stub
        served by the SAME mock. The sink stub BODY-MATCHES on the values the
        effect must render (``{command}`` / ``{order_id}`` captured from the
        trigger message), so its ``http.hit`` proves the rendered body was
        delivered — an unrendered template would fall through to the catch-all
        stub (418) instead. The log also carries exactly one ``http.called``.
        """
        broker = require_kafka
        run = uuid.uuid4().hex[:8]
        commands_topic = f"t10.fx.commands.{run}"
        port = _free_port()

        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(broker, {
            "http": {
                "listen": f"127.0.0.1:{port}",
                "stubs": {
                    # Body-matching sink: only a correctly rendered effect body
                    # hits this stub (json_subset on the parsed request body).
                    "sink": {
                        "description": "callback sink (matches rendered body)",
                        "method": "POST",
                        "path": "/sink",
                        "match": {"body": {"command": "ship", "order_id": "ord-42"}},
                        "response": {"status": 200, "body": {"received": True}},
                    },
                    # Catch-all for the same method+path: if the effect body did
                    # not render, the hit lands HERE (418) and the assertions
                    # below fail loudly on the stub name.
                    "sink-miss": {
                        "description": "catch-all for an unrendered body",
                        "method": "POST",
                        "path": "/sink",
                        "response": {"status": 418, "body": {"received": False}},
                    },
                },
            },
            "kafka": {
                "reactors": {
                    "callbacker": {
                        "description": "call the sink on each command",
                        "topic": commands_topic,
                        "effects": [{
                            "type": "http",
                            "url": f"http://127.0.0.1:{port}/sink",
                            "method": "POST",
                            "body": {"command": "{command}", "order_id": "{order_id}"},
                        }],
                    },
                },
            },
        }))

        mock = MockRunHandle(config_file)
        assert mock.base_url is not None, f"mock did not start: {mock.lines}"

        produce = _run_cli(config_file, [
            "kafka", "produce",
            "--topic", commands_topic,
            "--message", json.dumps({"command": "ship", "order_id": "ord-42"}),
            "--key", "ord-42",
        ])
        assert produce.returncode == 0, (
            f"kafka produce failed: {produce.stdout} {produce.stderr}"
        )

        try:
            # The effect fires asynchronously on the reactor thread — poll the
            # live log for the sink's http.hit before tearing anything down.
            hit = mock.wait_for(
                "sink http.hit",
                lambda events: next(
                    (e for e in events
                     if e.get("event") == "http.hit" and e.get("path") == "/sink"),
                    None,
                ),
            )
            assert hit["stub"] == "sink", (
                f"effect body did not render (hit fell through to "
                f"{hit['stub']!r}): {hit}"
            )
            assert hit["method"] == "POST"
            assert hit["status"] == 200

            called = mock.wait_for(
                "http.called",
                lambda events: next(
                    (e for e in events
                     if e.get("event") == "http.called"
                     and e.get("path") == "/sink"),
                    None,
                ),
            )
            assert called["trigger"] == "callbacker"
            assert called["method"] == "POST"
            assert called["status_code"] == 200
            # url mode: the event carries `url` (service XOR url).
            assert called["url"] == f"http://127.0.0.1:{port}/sink"
        finally:
            mock.stop_and_collect()

        assert len(mock.events_of("http.called")) == 1, (
            f"expected exactly 1 http.called, got {mock.events_of('http.called')}"
        )
        assert len(mock.events_of("http.unmatched")) == 0
        summary = mock.summary()
        assert summary["http_called"] == 1
        assert summary["http_hits"] == 1
        assert summary["effect_errors"] == 0
        assert mock.proc.returncode == 0, (
            f"stderr: {mock.proc.stderr.read()}"
        )

    def test_reactor_http_capture_chains_into_kafka_effect(self, require_kafka, tmp_path):
        """Chaining: reactor effects [http (captures ``ackId`` from the sink
        response), kafka (value renders ``{ackId}``)] ⇒ the audit topic carries
        a message with the CAPTURED ack id — proving response captures chain
        into later effects across the real transports.

        The sink renders ``ack-{order_id}`` from its own body captures, so the
        expected captured value (``ack-ord-7``) is known in advance and a
        template-literal leak (``{ackId}`` unrendered) cannot pass the consume
        match.
        """
        broker = require_kafka
        run = uuid.uuid4().hex[:8]
        commands_topic = f"t10.fx.commands.{run}"
        audit_topic = f"t10.fx.audit.{run}"
        port = _free_port()

        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(broker, {
            "http": {
                "listen": f"127.0.0.1:{port}",
                "stubs": {
                    "sink": {
                        "description": "acks what it receives",
                        "method": "POST",
                        "path": "/sink",
                        "response": {
                            "status": 200,
                            "body": {"ackId": "ack-{order_id}"},
                        },
                    },
                },
            },
            "kafka": {
                "reactors": {
                    "chained": {
                        "description": "http ack then audit the captured id",
                        "topic": commands_topic,
                        "effects": [
                            {
                                "type": "http",
                                "url": f"http://127.0.0.1:{port}/sink",
                                "method": "POST",
                                "body": {"order_id": "{order_id}"},
                                "capture": {"ackId": {"from": ".body.ackId"}},
                            },
                            {
                                "type": "kafka",
                                "topic": audit_topic,
                                "value": {"order_id": "{order_id}", "ack": "{ackId}"},
                            },
                        ],
                    },
                },
            },
        }))

        mock = MockRunHandle(config_file)
        assert mock.base_url is not None, f"mock did not start: {mock.lines}"

        produce = _run_cli(config_file, [
            "kafka", "produce",
            "--topic", commands_topic,
            "--message", json.dumps({"command": "audit", "order_id": "ord-7"}),
            "--key", "ord-7",
        ])
        assert produce.returncode == 0, (
            f"kafka produce failed: {produce.stdout} {produce.stderr}"
        )

        try:
            mock.wait_for(
                "chained kafka.produced on the audit topic",
                lambda events: next(
                    (e for e in events
                     if e.get("event") == "kafka.produced"
                     and e.get("topic") == audit_topic),
                    None,
                ),
            )
        finally:
            events = mock.stop_and_collect()

        consume = _run_cli(config_file, [
            "kafka", "consume",
            "--topic", audit_topic,
            "--timeout", "15",
            "--expect-count", "1",
            "--match", '.value.ack == "ack-ord-7" and .value.order_id == "ord-7"',
        ])
        assert consume.returncode == 0, (
            f"chained audit message not found: rc={consume.returncode} "
            f"out={consume.stdout} err={consume.stderr}"
        )

        # Effect ordering in the log: the http effect ran (and its capture
        # chained) BEFORE the kafka effect produced — both events are emitted
        # from the same reactor thread, so the order is deterministic.
        called_idx = next(i for i, e in enumerate(events)
                          if e.get("event") == "http.called")
        produced_idx = next(i for i, e in enumerate(events)
                            if e.get("event") == "kafka.produced"
                            and e.get("topic") == audit_topic)
        assert called_idx < produced_idx, (
            f"expected http.called before kafka.produced in {events}"
        )

        summary = mock.summary()
        assert summary["http_called"] == 1
        assert summary["kafka_produced"] == 1
        assert summary["effect_errors"] == 0
        assert mock.proc.returncode == 0, (
            f"stderr: {mock.proc.stderr.read()}"
        )


class TestFatalEffectError:
    """A failing effect is fatal: effect.error in the log, run exit 1."""

    def test_unreachable_http_effect_is_fatal(self, require_kafka, tmp_path):
        """An http effect against an unreachable service: the trigger request
        still completes (200 — the failure surfaces at the run level, not in
        the caller's response), the log contains exactly one ``effect.error``
        with ``fatal: true`` naming the service, the summary tallies
        ``effect_errors == 1``, and the ``mock run`` subprocess exits 1.
        """
        broker = require_kafka
        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(
            broker,
            {
                "http": {
                    "listen": "127.0.0.1:0",
                    "stubs": {
                        "boom": {
                            "description": "calls a dead service",
                            "method": "POST",
                            "path": "/boom",
                            "response": {"status": 200, "body": {"ok": True}},
                            "effects": [{
                                "type": "http",
                                "service": "dead-service",
                                "method": "POST",
                                "path": "/sink",
                            }],
                        },
                    },
                },
            },
            services={"dead-service": {"base_url": "http://127.0.0.1:1"}},
        ))

        mock = MockRunHandle(config_file)
        assert mock.base_url is not None, f"mock did not start: {mock.lines}"

        request = _run_cli(config_file, [
            "http", "request",
            "--url", f"{mock.base_url}/boom",
            "--method", "POST",
            "--body", json.dumps({"id": "x1"}),
        ])
        # The trigger completes normally even though its effect failed — the
        # failure is fatal to the RUN, not to the request.
        assert request.returncode == 0, (
            f"trigger request should complete: {request.stdout} {request.stderr}"
        )
        assert json.loads(request.stdout)["result"]["status_code"] == 200

        # The effect executor emits effect.error BEFORE http.hit/response, so
        # by the time the request returned the event is already flushed.
        mock.stop_and_collect()
        errors = mock.events_of("effect.error")
        assert len(errors) == 1, f"expected 1 effect.error, got {errors}"
        assert errors[0]["fatal"] is True
        assert errors[0]["effect_type"] == "http"
        assert errors[0]["service"] == "dead-service"
        assert errors[0]["trigger"] == "boom"

        summary = mock.summary()
        assert summary["effect_errors"] == 1
        assert summary["http_hits"] == 1
        # The fatal taxonomy (T3): a runtime error flag makes run() exit 1.
        assert mock.proc.returncode == 1, (
            f"expected exit 1 after fatal effect.error, got "
            f"{mock.proc.returncode}; stderr: {mock.proc.stderr.read()}"
        )


class TestDaemonFatalStopVerdict:
    """Daemon-mode companion to the fatal test: ``mock stop`` raises.

    The brief's AssertionFailure surfaces through the CLI envelope as exit 1
    with ``error.type == "AssertionError"`` — asserted end-to-end here
    (start → fatal effect → stop), mirroring ``test_mock_daemon.py``'s
    with-failure variant for the new effect.error taxonomy.
    """

    def test_mock_stop_raises_on_fatal_effect_error(self, require_kafka, tmp_path):
        """``mock start`` a daemon whose stub has a failing effect, trigger it,
        then ``mock stop`` ⇒ exit 1, ``error.type == "AssertionError"``, and
        the verdict's failures list carries the fatal ``effect.error``."""
        broker = require_kafka
        port = _free_port()
        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(
            broker,
            {
                "http": {
                    "listen": f"127.0.0.1:{port}",
                    "stubs": {
                        "boom": {
                            "description": "calls a dead service",
                            "method": "POST",
                            "path": "/boom",
                            "response": {"status": 200, "body": {"ok": True}},
                            "effects": [{
                                "type": "http",
                                "service": "dead-service",
                                "method": "POST",
                                "path": "/sink",
                            }],
                        },
                    },
                },
            },
            services={"dead-service": {"base_url": "http://127.0.0.1:1"}},
        ))
        state_dir = tmp_path / ".agctl"
        state_dir.mkdir()

        start = subprocess.run(
            [sys.executable, "-m", "agctl", "--config", str(config_file),
             "mock", "start", "--only", "http", "--state-dir", str(state_dir)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=90,
        )
        assert start.returncode == 0, f"mock start failed: {start.stdout}{start.stderr}"
        started = json.loads(start.stdout)
        assert started["ok"] is True
        pid = started["result"]["pid"]

        try:
            self._drive_fatal_effect(port)
            self._wait_for_effect_error(state_dir)
        finally:
            stop = subprocess.run(
                [sys.executable, "-m", "agctl",
                 "mock", "stop", "--state-dir", str(state_dir)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                timeout=90,
            )

        assert stop.returncode == 1, (
            f"mock stop should exit 1 on fatal effect.error; out={stop.stdout} "
            f"err={stop.stderr}"
        )
        envelope = json.loads(stop.stdout)
        assert envelope["ok"] is False
        assert envelope["error"]["type"] == "AssertionError"
        failures = envelope["error"]["detail"]["failures"]
        assert any(f.get("event") == "effect.error" and f.get("fatal") is True
                   for f in failures), failures

        # The daemon is gone and took its pidfile with it.
        with pytest.raises(OSError):
            import os
            os.kill(pid, 0)
        assert not (state_dir / f"mock-{port}.pid").exists()

    @staticmethod
    def _drive_fatal_effect(port: int) -> None:
        """POST /boom against the daemon, polling until the server accepts."""
        import http.client

        deadline = time.monotonic() + 20
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                conn.request("POST", "/boom", body="{}",
                             headers={"Content-Type": "application/json"})
                resp = conn.getresponse()
                body = resp.read()
                conn.close()
                assert resp.status == 200, f"/boom returned {resp.status}: {body}"
                return
            except Exception as exc:  # not accepting connections yet
                last_exc = exc
                time.sleep(0.3)
        raise AssertionError(f"daemon never served /boom: {last_exc}")

    @staticmethod
    def _wait_for_effect_error(state_dir: Path) -> None:
        """Poll ``mock status`` until the daemon's log shows the effect.error."""
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            status = subprocess.run(
                [sys.executable, "-m", "agctl",
                 "mock", "status", "--state-dir", str(state_dir)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                timeout=60,
            )
            assert status.returncode == 0, status.stdout + status.stderr
            failures = json.loads(status.stdout)["result"].get("failures_so_far")
            if any(f.get("event") == "effect.error" for f in failures):
                return
            time.sleep(0.3)
        raise AssertionError("effect.error never appeared in mock status")


class TestLegacyReactionBackCompat:
    """A legacy ``reaction`` reactor keeps its pre-change event vocabulary."""

    def test_legacy_reaction_reactor_emits_kafka_reacted_not_produced(
        self, require_kafka, tmp_path
    ):
        """A reactor configured with ``reaction`` (no effects) still emits
        ``kafka.reacted`` and NEVER ``kafka.produced``; the summary keeps the
        pre-change keys with ``kafka_reactions`` counted and the new effect
        counters at zero. The reaction message itself still lands on the
        events topic with the rendered key."""
        broker = require_kafka
        run = uuid.uuid4().hex[:8]
        commands_topic = f"t10.fx.commands.{run}"
        events_topic = f"t10.fx.orders-events.{run}"

        config_file = tmp_path / "agctl.yaml"
        config_file.write_text(_build_config(broker, {
            "kafka": {
                "reactors": {
                    "legacy-processor": {
                        "description": "legacy reaction reactor",
                        "topic": commands_topic,
                        "consumer_group": f"t10-fx-{run}",
                        "match": '.value.command == "create"',
                        "reaction": {
                            "topic": events_topic,
                            "key": "{order_id}",
                            "value": {
                                "event": "OrderCreated",
                                "order_id": "{order_id}",
                            },
                        },
                    },
                },
            },
        }))

        mock = MockRunHandle(config_file)
        # Kafka-only run: started has http:null; base_url stays None.

        produce = _run_cli(config_file, [
            "kafka", "produce",
            "--topic", commands_topic,
            "--message", json.dumps({"command": "create", "order_id": "ord-legacy"}),
            "--key", "ord-legacy",
        ])
        assert produce.returncode == 0, (
            f"kafka produce failed: {produce.stdout} {produce.stderr}"
        )

        try:
            mock.wait_for(
                "kafka.reacted",
                lambda events: next(
                    (e for e in events if e.get("event") == "kafka.reacted"),
                    None,
                ),
            )
        finally:
            mock.stop_and_collect()

        reacted = mock.events_of("kafka.reacted")
        assert len(reacted) == 1, f"expected 1 kafka.reacted, got {reacted}"
        assert reacted[0]["reactor"] == "legacy-processor"
        assert reacted[0]["topic"] == events_topic
        assert reacted[0]["key"] == "ord-legacy"

        # The legacy path never emits the new effect events.
        assert mock.events_of("kafka.produced") == [], (
            "legacy reaction reactor must not emit kafka.produced"
        )
        assert mock.events_of("http.called") == []

        summary = mock.summary()
        # Pre-change keys still present and correct.
        assert summary["kafka_reactions"] == 1
        assert summary["http_hits"] == 0
        assert summary["http_unmatched"] == 0
        assert summary["kafka_skipped"] == 0
        assert summary["kafka_errors"] == 0
        # New effect counters ride along at zero.
        assert summary["kafka_produced"] == 0
        assert summary["http_called"] == 0
        assert summary["effect_errors"] == 0
        assert mock.proc.returncode == 0, (
            f"stderr: {mock.proc.stderr.read()}"
        )

        # The reaction message is on the events topic (legacy path intact).
        consume = _run_cli(config_file, [
            "kafka", "consume",
            "--topic", events_topic,
            "--timeout", "15",
            "--expect-count", "1",
            "--match", '.value.order_id == "ord-legacy"',
        ])
        assert consume.returncode == 0, (
            f"legacy reaction not found: rc={consume.returncode} "
            f"out={consume.stdout} err={consume.stderr}"
        )
        messages = json.loads(consume.stdout)["result"]["messages"]
        assert messages[0]["key"] == "ord-legacy"
        assert messages[0]["value"]["event"] == "OrderCreated"
