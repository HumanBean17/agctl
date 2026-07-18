"""Tests for Avro/Protobuf decode in ``kafka listen`` (Task 11).

Per the task brief, four scenarios driven by a fake ``KafkaClient`` whose
``consume_loop`` yields one decoded Avro message and one decode-error:

(a) the capture file contains exactly the decoded Avro message;
(b) ``emit_event`` received a ``decode.error`` event for the bad message
    with ``fatal: false`` and the bad message was NOT written;
(c) the ``summary`` event's per-topic entry carries ``decode_errors: 1``;
(d) ``ListenEngine.start`` calls ``probe_schema_registry`` when a topic
    resolves to AVRO and an SR client exists; a probe failure surfaces as
    a startup error before any ``started`` line.

The fake client mirrors ``tests/unit/test_listen_capture.py::_FakeKafkaClient``
but also drives the ``on_decode_error`` callback (the Task 8 codec seam).
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pytest
from confluent_kafka import OFFSET_END, TopicPartition

from agctl.clients.kafka_client import ReactionResult
from agctl.config.models import (
    Config,
    KafkaConfig,
    KafkaCluster,
    KafkaTopicConfig,
)
from agctl.errors import ConnectionFailure
from agctl.listen.engine import ListenEngine

# Same Windows-skip rationale as test_listen_engine.py: the engine lifecycle
# installs real SIGTERM/SIGINT handlers and joins capture threads, which hangs
# on Windows CI. The codec plumbing itself is platform-independent; the
# cross-platform CLI surface is covered by test_kafka_listen_run.py.
pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason=(
        "ListenEngine lifecycle test (real signal handlers + thread joins) "
        "hangs on Windows CI; kafka listen run is covered via test_kafka_listen_run.py"
    ),
)


_TOPIC = "orders"
# The partition librdkafka would hand to on_assign during rebalance.
_ASSIGNED_TP = TopicPartition(_TOPIC, 0)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeConsumer:
    """Minimal consumer stand-in: ``seek`` is a recording spy."""

    def __init__(self):
        self.seeks: list[TopicPartition] = []

    def seek(self, tp):
        self.seeks.append(tp)


class _DecodeErrScript:
    """A scripted decode-error delivery: ``on_decode_error`` then ``handle``.

    The fake client's ``consume_loop`` iterates these in order after firing
    ``on_assign`` so the readiness gate opens before any capture happens.
    """

    def __init__(self, error_label: str, msg: dict):
        self.error_label = error_label
        self.msg = msg


class _GoodMsg:
    """A scripted decoded message delivery (no decode error)."""

    def __init__(self, msg: dict):
        self.msg = msg


class _FakeKafkaClient:
    """Fake ``KafkaClient.consume_loop`` driving on_assign + on_decode_error.

    Script:
        1. Fire ``on_assign`` once (opens the readiness gate).
        2. For each scripted item:
            - ``_GoodMsg``  -> ``handle(msg)`` (CaptureLoop writes it).
            - ``_DecodeErrScript`` -> ``on_decode_error(label)`` then
              ``handle(msg)`` (CaptureLoop emits decode.error + skips write).
        3. Signal ``script_done`` (tests wait on this before setting stop, so
           the script is guaranteed to have run).
        4. Block on ``stop_event`` so the consume_loop doesn't return until the
           engine sets stop (mirrors the real client's stop-event contract).

    The script runs atomically: no mid-script stop_event check (the broker
    already delivered these messages). Mid-script polling would race the
    engine's ``_stop.set()`` after ``start()`` returns and drop scripted
    deliveries nondeterministically.
    """

    def __init__(self, script):
        self.script = list(script)
        self.consumer = _FakeConsumer()
        self.consume_calls: list[dict] = []
        self.script_done = threading.Event()

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
        on_decode_error=None,
    ):
        self.consume_calls.append(
            {
                "topic": topic,
                "group_id": group_id,
                "max_retries": max_retries,
                "on_assign": on_assign,
                "on_decode_error": on_decode_error,
            }
        )
        if on_assign is not None:
            on_assign(self.consumer, [_ASSIGNED_TP])
        for item in self.script:
            if isinstance(item, _GoodMsg):
                result = handle(item.msg, attempt=1, final=True)
                if result == ReactionResult.STOP:
                    break
            elif isinstance(item, _DecodeErrScript):
                # Mirror KafkaClient._normalize_message: on_decode_error fires
                # BEFORE handle, then handle is called with the partially-decoded
                # message (the failed side is None).
                if on_decode_error is not None:
                    on_decode_error(item.error_label)
                result = handle(item.msg, attempt=1, final=True)
                if result == ReactionResult.STOP:
                    break
        # Signal that the script has fully iterated. Tests wait on this before
        # setting stop so the assertions see the complete side-effect set.
        self.script_done.set()
        # Drain: keep the loop alive until the engine signals stop, so the
        # engine's run() -> shutdown() sequence exercises the join path.
        stop_event.wait(timeout=2.0)


def _decoded_avro_value():
    """The decoded Avro value the fake client 'delivered'."""
    return {"eventType": "ORDER_CREATED", "id": "abc", "qty": 3}


def _msg(value, *, key=None, offset=0, partition=0, headers=None):
    """Build a normalized message dict (KafkaClient._normalize_message shape)."""
    return {
        "key": key,
        "value": value,
        "partition": partition,
        "offset": offset,
        "timestamp": "2026-07-18T00:00:00Z",
        "headers": headers or {},
    }


# ---------------------------------------------------------------------------
# Engine harness
# ---------------------------------------------------------------------------


def _make_engine_with_fake_client(
    tmp_path: Path,
    emitted: list,
    fake_client: _FakeKafkaClient,
    *,
    topics=(_TOPIC,),
    cfg: Config | None = None,
) -> ListenEngine:
    """Build a ListenEngine wired to a recording emit_fn + a fake client.

    Uses the real CaptureLoop (so the codec plumbing is exercised) and the
    real engine; only the KafkaClient is faked.
    """

    def recording_emit(line: dict) -> None:
        emitted.append(line.copy())

    engine = ListenEngine(
        topics=list(topics),
        client=fake_client,
        run_id="run-codec",
        group="agctl-listen-run-codec",
        cluster="default",
        run_dir=tmp_path,
        capture_match=None,
        max_bytes=0,
        duration=None,
        emit_fn=recording_emit,
        cfg=cfg,
    )
    return engine


def _drive_to_summary(
    engine: ListenEngine, fake_client: _FakeKafkaClient | None = None
) -> int:
    """start -> (wait for script) -> set stop -> run -> shutdown.

    When ``fake_client`` is supplied, wait for its ``script_done`` event
    between ``start()`` and ``_stop.set()`` so the assertions see the full
    scripted side-effect set (no race with the main thread setting stop).
    """
    engine.start()
    if fake_client is not None:
        assert fake_client.script_done.wait(timeout=2.0), (
            "fake consume_loop script did not complete within 2s"
        )
    engine._stop.set()
    code = engine.run()
    engine.shutdown()
    return code


# ---------------------------------------------------------------------------
# (a) decoded Avro message written to the capture file
# ---------------------------------------------------------------------------


def test_decoded_avro_message_is_written_to_capture_file(tmp_path: Path):
    """One decoded Avro message -> capture file contains exactly that envelope."""
    emitted: list[dict] = []
    fake = _FakeKafkaClient(
        script=[_GoodMsg(_msg(_decoded_avro_value(), key="k1", offset=0))]
    )
    engine = _make_engine_with_fake_client(tmp_path, emitted, fake)

    _drive_to_summary(engine, fake)

    capture = tmp_path / f"{_TOPIC}.ndjson"
    lines = [json.loads(ln) for ln in capture.read_text().splitlines() if ln.strip()]
    assert len(lines) == 1
    env = lines[0]
    # The decoded Avro value flowed through unchanged (the client already
    # decoded it; CaptureLoop writes the envelope verbatim).
    assert env["topic"] == _TOPIC
    assert env["value"] == _decoded_avro_value()
    assert env["key"] == "k1"
    assert env["offset"] == 0
    assert env["captured_at"]  # ISO-Z string is present
    # consume_loop was called with the on_decode_error seam (None for the
    # legacy client; the real KafkaClient would supply a callable).
    assert fake.consume_calls[0]["on_decode_error"] is not None


# ---------------------------------------------------------------------------
# (b) decode failure emits decode.error (fatal: false) AND skips the write
# ---------------------------------------------------------------------------


def test_decode_failure_emits_event_and_skips_write(tmp_path: Path):
    """One good Avro message + one decode-error -> only the good one captured."""
    emitted: list[dict] = []
    fake = _FakeKafkaClient(
        script=[
            _GoodMsg(_msg(_decoded_avro_value(), key="k1", offset=0)),
            _DecodeErrScript(
                "value: not a Confluent frame",
                _msg(None, key="k2", offset=1),  # value=None (decode failed)
            ),
        ]
    )
    engine = _make_engine_with_fake_client(tmp_path, emitted, fake)

    _drive_to_summary(engine, fake)

    # The bad message was NOT written: the capture file has exactly one line
    # (the good Avro message at offset 0).
    capture = tmp_path / f"{_TOPIC}.ndjson"
    lines = [json.loads(ln) for ln in capture.read_text().splitlines() if ln.strip()]
    assert len(lines) == 1
    assert lines[0]["offset"] == 0
    assert lines[0]["value"] == _decoded_avro_value()

    # A decode.error event was emitted with fatal: false, naming the topic
    # and carrying the callback's error label verbatim.
    decode_errors = [e for e in emitted if e.get("event") == "decode.error"]
    assert len(decode_errors) == 1
    evt = decode_errors[0]
    assert evt["topic"] == _TOPIC
    assert evt["error"] == "value: not a Confluent frame"
    assert evt["fatal"] is False
    assert "timestamp" in evt


# ---------------------------------------------------------------------------
# (c) summary carries decode_errors per topic
# ---------------------------------------------------------------------------


def test_summary_carries_decode_errors_per_topic(tmp_path: Path):
    """summary.topics[topic].decode_errors == 1 after one decode failure."""
    emitted: list[dict] = []
    fake = _FakeKafkaClient(
        script=[
            _GoodMsg(_msg(_decoded_avro_value(), key="k1", offset=0)),
            _DecodeErrScript(
                "value: bad magic byte",
                _msg(None, key="k2", offset=1),
            ),
        ]
    )
    engine = _make_engine_with_fake_client(tmp_path, emitted, fake)

    _drive_to_summary(engine, fake)

    summary = next(e for e in emitted if e.get("event") == "summary")
    assert len(summary["topics"]) == 1
    entry = summary["topics"][0]
    assert entry["topic"] == _TOPIC
    assert entry["captured"] == 1  # only the good one
    assert entry["decode_errors"] == 1


# ---------------------------------------------------------------------------
# (d) ListenEngine.start calls probe_schema_registry when AVRO + SR exists
# ---------------------------------------------------------------------------


def _avro_config(tmp_path: Path) -> Config:
    """A v3 Config where ``orders`` is AVRO + the cluster has an SR URL."""
    return Config(
        version="3",
        kafka=KafkaConfig(
            clusters={
                "default": KafkaCluster(
                    brokers=["broker-a:9092"],
                    schema_registry_url="http://sr:8081",
                )
            },
            default_cluster="default",
            topics={
                _TOPIC: KafkaTopicConfig(value_format="avro"),
            },
        ),
    )


def test_start_calls_probe_schema_registry_when_avro_and_sr_exist(
    tmp_path: Path, monkeypatch
):
    """A topic resolving to AVRO + an SR URL -> probe_schema_registry called once."""
    cfg = _avro_config(tmp_path)

    probe_calls: list[dict] = []
    fake_sr = object()  # stand-in; the engine forwards it to the codec dict

    def _fake_resolve_sr(_cfg, _cluster):
        return fake_sr

    def _fake_probe(sr, cluster: str) -> None:
        probe_calls.append({"cluster": cluster, "sr": sr})

    # Patch the source module so the engine's lazy import picks up the fakes.
    import agctl.commands.kafka_commands as cmds

    monkeypatch.setattr(cmds, "resolve_schema_registry_client", _fake_resolve_sr)
    monkeypatch.setattr(cmds, "probe_schema_registry", _fake_probe)
    monkeypatch.setattr(cmds, "_sr_client_cache", {})

    # A fake client factory so the engine builds a fake (no real broker).
    fake_client_holder: dict[str, _FakeKafkaClient] = {}

    def _factory(_cfg, _cluster, _codec):
        client = _FakeKafkaClient(
            script=[_GoodMsg(_msg(_decoded_avro_value(), key="k1", offset=0))]
        )
        fake_client_holder["client"] = client
        return client

    emitted: list[dict] = []
    engine = ListenEngine(
        topics=[_TOPIC],
        client=object(),  # unused when kafka_client_factory is set
        run_id="run-probe",
        group="agctl-listen-run-probe",
        cluster="default",
        run_dir=tmp_path,
        capture_match=None,
        max_bytes=0,
        duration=None,
        emit_fn=lambda e: emitted.append(e.copy()),
        cfg=cfg,
    )
    engine.kafka_client_factory = _factory

    _drive_to_summary(engine, fake_client_holder.get("client"))

    # Probe was called exactly once for the cluster, with the fake SR.
    assert len(probe_calls) == 1
    assert probe_calls[0]["cluster"] == "default"
    assert probe_calls[0]["sr"] is fake_sr
    # The codec-aware client was built (the consume_call wired on_decode_error).
    assert fake_client_holder["client"].consume_calls[0]["on_decode_error"] is not None


def test_probe_failure_surfaces_as_startup_error_before_started_line(
    tmp_path: Path, monkeypatch
):
    """probe_schema_registry raising -> ConnectionFailure before any started line."""
    cfg = _avro_config(tmp_path)

    def _fake_resolve_sr(_cfg, _cluster):
        return object()  # present so the engine proceeds to probe

    def _failing_probe(sr, cluster: str) -> None:
        raise ConnectionFailure("sr unreachable", {"cluster": cluster})

    import agctl.commands.kafka_commands as cmds

    monkeypatch.setattr(cmds, "resolve_schema_registry_client", _fake_resolve_sr)
    monkeypatch.setattr(cmds, "probe_schema_registry", _failing_probe)
    monkeypatch.setattr(cmds, "_sr_client_cache", {})

    emitted: list[dict] = []
    engine = ListenEngine(
        topics=[_TOPIC],
        client=object(),
        run_id="run-fail",
        group="agctl-listen-run-fail",
        cluster="default",
        run_dir=tmp_path,
        capture_match=None,
        max_bytes=0,
        duration=None,
        emit_fn=lambda e: emitted.append(e.copy()),
        cfg=cfg,
    )
    engine.kafka_client_factory = lambda *_a, **_k: _FakeKafkaClient(script=[])

    # start() surfaces the probe failure as ConnectionFailure. The started
    # line is NEVER emitted (probe runs before the ready gate).
    with pytest.raises(ConnectionFailure):
        engine.start()

    assert not any(e.get("event") == "started" for e in emitted)
    assert not any(e.get("event") == "summary" for e in emitted)
