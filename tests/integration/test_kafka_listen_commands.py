"""Live integration tests: ``agctl kafka listen`` capture daemon (self-skipping).

Mirrors the Kafka self-skipping pattern in :mod:`tests.integration.test_mock_commands`
and the ``require_kafka`` fixture convention in
:mod:`tests.integration.conftest`: under ``AGCTL_TEST_LIVE=1`` the session
fixture spins a Kafka+KRaft testcontainer wiring ``AGCTL_TEST_KAFKA_BROKER``;
without it (or when Docker is unavailable), ``require_kafka`` ``pytest.skip()``s
cleanly and these tests NEVER fail because the broker is absent.

The happy path drives the full managed-daemon lifecycle against a real broker
via the ``_core`` functions (the same callable the ``@envelope``-wrapped Click
commands delegate to; the Click layer is unit-covered, so the integration value
is the real subprocess daemon + real broker + real on-disk capture):

    produce(pre)  ->  kafka listen start  ->  produce(post)  ->
    assert  ->  results (pass)  ->  messages  ->  stop

It verifies the two load-bearing invariants:

1. **Seek-to-latest** — a message produced BEFORE ``start`` is NOT captured
   (``CaptureLoop._on_assign`` seeks every assigned partition to
   ``OFFSET_END``). The pre-start message id must be absent from ``messages``
   and the post-start message id must be present.
2. **Retention-immunity is structural** — ``assert``/``results``/``messages``
   read the on-disk capture file, not the live topic, so broker retention
   cleanup cannot cause a false negative. This is a structural property
   (exercised unit-side in ``test_listen_capture_file`` /
   ``test_listen_assert_eval``); it is NOT simulated here because doing so
   would mean racing the broker's retention thread, which is both flaky and
   redundant — a code-level comment is sufficient per the task brief.

POSIX note: ``start``/``stop`` are POSIX-gated (``require_posix_daemon``); the
integration run lives under ``AGCTL_TEST_LIVE=1`` on a Linux testcontainer, so
the gate passes. The module-level ``pytestmark`` below mirrors the mock daemon
integration test as a belt-and-braces guard against a Windows host run.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest

# Belt-and-braces: the managed daemon (start/stop) is POSIX-only. The
# integration run executes under AGCTL_TEST_LIVE=1 (Linux testcontainer) where
# this never trips; mirroring tests/integration/test_mock_commands.py's guard.
pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="managed listen daemon is POSIX-only; use 'kafka listen run' or WSL on Windows",
)


# jq predicate over the captured envelope: value is JSON-parsed by the kafka
# client, so .value.eventType reaches the produced message's eventType field.
MATCH_ORDER_CREATED = '.value.eventType == "ORDER_CREATED"'


def _write_config(broker: str, tmp_path: Path) -> Path:
    """Write a one-cluster v3 config pointing at the live broker."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        "\n".join(
            [
                'version: "3"',
                "kafka:",
                "  clusters:",
                "    default:",
                f"      brokers: [{broker}]",
                "  default_cluster: default",
                "",
            ]
        )
    )
    return cfg


def _produce(broker: str, topic: str, value: dict, key: str) -> None:
    """Produce one JSON message to ``topic`` (auto-creates the topic)."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": broker, "client.id": "agctl-test-producer"})
    producer.produce(topic, key=key, value=json.dumps(value))
    producer.flush(timeout=10)


def _capture_path(state_dir: Path, run_id: str, topic: str) -> Path:
    """Return the per-topic capture file path for a running listener."""
    return state_dir / f"listen-{run_id}" / f"{topic}.ndjson"


def _wait_for_capture(
    state_dir: Path,
    run_id: str,
    topic: str,
    predicate,
    *,
    timeout: float = 20.0,
) -> None:
    """Poll the capture file until ``predicate(envelope_dict)`` is True.

    Each captured message is flushed to disk synchronously inside
    :meth:`CaptureLoop._handle`, so this is a tight poll for the post-start
    message landing. Raises ``AssertionError`` on timeout — but never skips:
    by this point the broker is reachable (``require_kafka`` already proved it).
    """
    cap = _capture_path(state_dir, run_id, topic)
    deadline = time.monotonic() + timeout
    last_seen: list = []
    while time.monotonic() < deadline:
        if cap.exists():
            for raw in cap.read_text().splitlines():
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    env = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                last_seen.append(env.get("value"))
                if predicate(env):
                    return
        time.sleep(0.2)
    raise AssertionError(
        f"capture file {cap} did not satisfy predicate within {timeout}s; "
        f"last_seen={last_seen!r}"
    )


class TestKafkaListenLifecycle:
    """End-to-end ``kafka listen`` lifecycle against a live Kafka broker."""

    def test_start_assert_results_messages_stop_seek_to_latest(
        self, require_kafka, tmp_path
    ):
        """Core happy path + the seek-to-latest invariant.

        Flow:
        1. Produce a PRE-start matching message to topic T.
        2. ``kafka listen start`` — positions every assigned partition at
           OFFSET_END (readiness fires only after the seek).
        3. Produce a POST-start matching message.
        4. ``kafka listen assert --match <jq> --expect-count 1``.
        5. ``kafka listen results`` → pass (no AssertionFailure).
        6. ``kafka listen messages`` → includes post-start, EXCLUDES pre-start
           (seek-to-latest).
        7. ``kafka listen stop`` → clean (no AssertionFailure).
        """
        from agctl.commands.kafka_listen_commands import (
            _kafka_listen_assert_core,
            _kafka_listen_messages_core,
            _kafka_listen_results_core,
            _kafka_listen_start_core,
            _kafka_listen_stop_core,
        )

        broker = require_kafka
        cfg_path = _write_config(broker, tmp_path)
        state_dir = tmp_path / "state"

        test_id = uuid.uuid4().hex[:8]
        topic = f"orders.created.{test_id}"
        pre_id = f"pre-{test_id}"
        post_id = f"post-{test_id}"

        # 1. Pre-start message. Producing auto-creates topic T with one message
        #    at offset 0; the listener will seek past it.
        _produce(
            broker,
            topic,
            {"eventType": "ORDER_CREATED", "id": pre_id},
            key=pre_id,
        )
        # Settle so the broker has the pre-start message durable + the topic
        # metadata known before the consumer subscribes and seeks.
        time.sleep(0.5)

        # 2. Start the listener (spawns a real detached `kafka listen run`
        #    daemon; readiness-polls events.log for the `started` event, which
        #    fires only after assignment + seek-to-end).
        start = _kafka_listen_start_core(
            config_path=str(cfg_path),
            topics=[topic],
            patterns=[],
            cluster=None,
            capture_match=None,
            max_bytes=268435456,
            state_dir=str(state_dir),
            overlay_paths=None,
            env_file=None,
        )
        run_id = start["run_id"]
        assert start["topics"] == [topic]
        assert start["cluster"] == "default"

        try:
            # 3. Post-start matching message — this one must be captured.
            _produce(
                broker,
                topic,
                {"eventType": "ORDER_CREATED", "id": post_id},
                key=post_id,
            )

            # Wait for the capture loop to flush the post-start message. The
            # daemon writes each envelope under a per-topic lock + flush, so
            # this resolves within a poll or two.
            _wait_for_capture(
                state_dir,
                run_id,
                topic,
                lambda env: env.get("value", {}).get("id") == post_id,
                timeout=20.0,
            )

            # 4. assert — attach one expectation (idempotent file append; the
            #    running daemon never reads asserts.jsonl).
            assert_result = _kafka_listen_assert_core(
                topic=topic,
                contains=None,
                match=MATCH_ORDER_CREATED,
                pattern=None,
                path=None,
                param=(),
                expect_count=1,
                id=None,
                run_id=None,
                pid=None,
                state_dir=str(state_dir),
            )
            assert assert_result == {
                "attached": True,
                "id": "exp-1",
                "topic": topic,
                "modes": ["match"],
                "expect_count": 1,
            }

            # 5. results → pass. AssertionFailure would propagate (exit 1 at the
            #    CLI layer); here it would raise. matched_count (1) >= 1.
            results = _kafka_listen_results_core(
                run_id=None,
                pid=None,
                state_dir=str(state_dir),
                config_path=str(cfg_path),
            )
            assert results["failed"] == 0
            assert results["passed"] == 1
            assert results["evaluated"] == 1

            # 6. messages — includes post-start, EXCLUDES pre-start. This is
            #    the seek-to-latest assertion: only post-start offsets are
            #    captured. (Retention-immunity is structural here — messages
            #    reads the on-disk capture file, not the live topic, so broker
            #    retention cleanup cannot truncate the verdict. That property
            #    is unit-covered in test_listen_capture_file /
            #    test_listen_assert_eval; it is not simulated live because
            #    racing the broker's retention thread would be flaky and
            #    redundant.)
            msgs = _kafka_listen_messages_core(
                topic=topic,
                match=None,
                param=(),
                limit=50,
                run_id=None,
                pid=None,
                state_dir=str(state_dir),
            )
            captured_ids = {
                m.get("value", {}).get("id") for m in msgs["messages"]
            }
            assert post_id in captured_ids, (
                f"post-start message not captured: {captured_ids!r}"
            )
            assert pre_id not in captured_ids, (
                f"pre-start message leaked into capture "
                f"(seek-to-latest broken): {captured_ids!r}"
            )
        finally:
            # 7. stop — clean. Runs unconditionally so a mid-test failure still
            #    tears the daemon down. stop parses events.log for the verdict;
            #    it raises AssertionFailure only on kafka.error / overflow-on-
            #    asserted-topic, neither of which occur here.
            stop = _kafka_listen_stop_core(
                run_id=None,
                pid=None,
                all_=False,
                timeout=10.0,
                state_dir=str(state_dir),
            )
            assert stop["stopped"] is True
            assert stop["cleaned"] is True
            assert stop["failures"] == []
            # stop removes the run dir + pidfile.
            assert not (state_dir / f"listen-{run_id}").exists()

    def test_results_failure_path_raises_assertion_failure(
        self, require_kafka, tmp_path
    ):
        """``kafka listen results`` raises AssertionFailure when an attached
        expectation is not satisfied by the capture (exit 1 at the CLI layer).

        Composes cleanly with the fixture: start a listener, attach an
        expectation whose match predicate nothing satisfies, evaluate → the
        result is ``matched_count=0 < expect_count=1`` → AssertionFailure.
        ``stop`` afterwards is unaffected (it does not evaluate expectations).
        """
        from agctl.commands.kafka_listen_commands import (
            _kafka_listen_assert_core,
            _kafka_listen_results_core,
            _kafka_listen_start_core,
            _kafka_listen_stop_core,
        )
        from agctl.errors import AssertionFailure

        broker = require_kafka
        cfg_path = _write_config(broker, tmp_path)
        state_dir = tmp_path / "state"

        test_id = uuid.uuid4().hex[:8]
        topic = f"orders.created.{test_id}"

        start = _kafka_listen_start_core(
            config_path=str(cfg_path),
            topics=[topic],
            patterns=[],
            cluster=None,
            capture_match=None,
            max_bytes=268435456,
            state_dir=str(state_dir),
            overlay_paths=None,
            env_file=None,
        )
        run_id = start["run_id"]

        try:
            # Attach an unsatisfiable expectation (no message will ever match
            # this eventType). No production needed: an empty capture file is
            # "no messages yet" → zero matches (listen/assert_eval contract).
            _kafka_listen_assert_core(
                topic=topic,
                contains=None,
                match='.value.eventType == "NONEXISTENT"',
                pattern=None,
                path=None,
                param=(),
                expect_count=1,
                id=None,
                run_id=None,
                pid=None,
                state_dir=str(state_dir),
            )

            # Give the capture loop a moment to confirm it is positioned at the
            # head (no backlog to drain) — not strictly required, but keeps the
            # empty-capture verdict deterministic.
            time.sleep(0.5)

            with pytest.raises(AssertionFailure) as ei:
                _kafka_listen_results_core(
                    run_id=None,
                    pid=None,
                    state_dir=str(state_dir),
                    config_path=str(cfg_path),
                )
            detail = ei.value.detail or {}
            results = detail.get("results", [])
            assert results, (
                f"AssertionFailure.detail.results empty: {ei.value.detail!r}"
            )
            assert results[0]["passed"] is False
            assert results[0]["matched_count"] == 0
        finally:
            # stop is clean — expectations are NOT evaluated at stop time.
            stop = _kafka_listen_stop_core(
                run_id=None,
                pid=None,
                all_=False,
                timeout=10.0,
                state_dir=str(state_dir),
            )
            assert stop["stopped"] is True
            assert stop["failures"] == []
