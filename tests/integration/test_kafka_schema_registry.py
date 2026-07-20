"""Self-skipping integration tests: Avro + Protobuf round-trips via Schema Registry.

Exercises the full codec stack end-to-end against a real Confluent Schema
Registry and Kafka broker. Each test depends on BOTH the
:func:`tests.integration.conftest.require_kafka` AND
:func:`tests.integration.conftest.require_schema_registry` fixtures; when
either is absent (no ``AGCTL_TEST_LIVE=1`` / Docker unavailable / SR failed
to start) the test SKIPS, never FAILS.

Coverage (per Task 16 brief):

(a) Avro round-trip ``produce`` -> ``consume`` -> ``assert`` (value + key).
(b) Avro capture via ``kafka listen start``/``assert``/``results``.
(c) Avro mock reactor decode-trigger + encode-reaction
    (``mocks.kafka.reactors``).
(d) Protobuf equivalents of (a).
(e) ``--value-format`` CLI override case.
(f) Plaintext SR auth path (basic-auth/mTLS are deferred — see ``DEFERRED``
    note at the bottom of this module).

Each test registers the schemas it needs (or lets ``kafka produce`` register
via the codec) and asserts decoded JSON appears in the consumed/asserted
results.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli

# POSIX note: ``kafka listen start``/``stop`` are POSIX-gated (the managed
# daemon uses subprocess + pidfiles). Integration live runs are under
# ``AGCTL_TEST_LIVE=1`` on a Linux testcontainer; the guard is belt-and-braces
# against a Windows host run. Mirrors ``test_kafka_listen_commands.py``.
pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="managed listen daemon is POSIX-only; use 'kafka listen run' or WSL on Windows",
)


# ---------------------------------------------------------------------------
# Schema fixtures (Avro + Protobuf) — kept in sync with the unit codec suites.
# ---------------------------------------------------------------------------

# Avro record with an ``id`` (string) + ``amount`` (int) — wide enough to prove
# field-by-field round-trip, narrow enough to read at a glance.
AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "int"},
    ],
})

# Avro key record (separate schema for the ``-key`` subject).
AVRO_KEY_SCHEMA = json.dumps({
    "type": "record",
    "name": "EventKey",
    "fields": [{"name": "keyId", "type": "string"}],
})

# Protobuf record with the same shape (id + amount). Single-message v1 codec.
PROTO_SCHEMA = 'syntax = "proto3"; message Event { string id = 1; int32 amount = 2; }'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _register_schema(sr_url: str, subject: str, schema_str: str, schema_type: str) -> int:
    """Register ``schema_str`` under ``subject`` via the SR REST API.

    Returns the schema id. Uses the REST API directly (no ``confluent_kafka``
    import) so the helper is portable across environments. Idempotent: a
    re-registration of the same schema returns the existing id (SR semantics).
    """
    body = json.dumps({"schemaType": schema_type, "schema": schema_str}).encode()
    req = urllib.request.Request(
        f"{sr_url}/subjects/{subject}/versions",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return int(json.loads(resp.read().decode())["id"])


def _write_config(
    tmp_path: Path,
    broker: str,
    sr_url: str,
    *,
    topics: dict | None = None,
    reactors: dict | None = None,
) -> Path:
    """Write a one-cluster v3 config pointing at the live broker + SR.

    ``topics`` is an optional ``{name: {value_format, key_format, ...}}`` map
    plumbed under ``kafka.topics``. ``reactors`` is an optional
    ``mocks.kafka.reactors`` map for reactor tests. The cluster carries the
    SR URL at the cluster level so every test topic inherits it.
    """
    cfg = {
        "version": "3",
        "kafka": {
            "clusters": {
                "default": {
                    "brokers": [broker],
                    "schema_registry_url": sr_url,
                    "default_consumer_group": "agctl-consumer",
                }
            },
            "default_cluster": "default",
            "topics": topics or {},
        },
    }
    if reactors is not None:
        cfg["mocks"] = {"kafka": {"reactors": reactors}}
    out = tmp_path / "agctl.yaml"
    out.write_text(json.dumps(cfg))
    return out


def _env(broker: str, sr_url: str) -> dict:
    """Environment for CliRunner: the broker + SR URL + dummy service vars."""
    return {
        "KAFKA_BROKER": broker,
        "SCHEMA_REGISTRY_URL": sr_url,
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "PAYMENT_SERVICE_URL": "http://localhost:8082",
        "PAYMENT_SERVICE_TOKEN": "tok",
        "DB_HOST": "localhost",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
        "ANALYTICS_DB_HOST": "localhost",
        "ANALYTICS_DB_USER": "au",
        "ANALYTICS_DB_PASSWORD": "ap",
    }


def _free_port() -> int:
    """Allocate a free TCP port (bind to :0 and read the assigned port)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _run_cli(args: list[str], env: dict) -> dict:
    """Invoke the agctl CLI, parse the envelope, assert exit 0.

    Returns the parsed ``result`` payload. Failure output is echoed in the
    AssertionError so live-run failures are self-diagnosing.
    """
    res = CliRunner().invoke(cli, args, env=env)
    assert res.exit_code == 0, f"command failed: {args}\noutput: {res.output}"
    envelope = json.loads(res.output)
    assert envelope["ok"] is True, f"envelope not ok: {envelope}"
    return envelope["result"]


# ---------------------------------------------------------------------------
# (a) Avro round-trip: produce -> consume -> assert (value + key)
# ---------------------------------------------------------------------------


def test_avro_produce_consume_assert_value_and_key(
    require_kafka, require_schema_registry, tmp_path
):
    """Avro value + Avro key round-trip through produce/consume/assert.

    Registers the schemas under the ``<topic>-value`` and ``<topic>-key``
    subjects (Confluent ``topic`` strategy — the default), produces a message
    via the codec path, and asserts the decoded JSON appears in both the
    ``consume`` window and the ``assert`` verdict.
    """
    broker = require_kafka
    sr_url = require_schema_registry
    test_id = uuid.uuid4().hex[:8]
    topic = f"avro.events.{test_id}"

    # Register the value + key schemas BEFORE producing (v1 encode contract:
    # the subject must exist; ``produce`` does not auto-register).
    _register_schema(sr_url, f"{topic}-value", AVRO_SCHEMA, "AVRO")
    _register_schema(sr_url, f"{topic}-key", AVRO_KEY_SCHEMA, "AVRO")

    cfg = _write_config(
        tmp_path,
        broker,
        sr_url,
        topics={
            topic: {"value_format": "avro", "key_format": "avro"},
        },
    )
    env = _env(broker, sr_url)

    payload = {"id": f"avro-{test_id}", "amount": 42}
    key_payload = {"keyId": f"key-{test_id}"}

    # 1. Produce (encodes value+key via the Avro codec against the SR).
    _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "produce",
            "--topic",
            topic,
            "--message",
            json.dumps(payload),
            "--key",
            json.dumps(key_payload),
        ],
        env,
    )

    # Broker settle: produced bytes must be durable + topic metadata known
    # before the consume window opens.
    time.sleep(1)

    # 2. Consume (decodes via the codec; messages arrive as decoded JSON).
    consumed = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "consume",
            "--topic",
            topic,
            "--from-beginning",
            "--timeout",
            "10",
        ],
        env,
    )
    assert consumed["count"] >= 1, f"no messages consumed: {consumed}"
    last = consumed["messages"][-1]
    assert last["value"] == payload, (
        f"decoded value mismatch: got {last.get('value')!r}, want {payload!r}"
    )
    assert last["key"] == key_payload, (
        f"decoded key mismatch: got {last.get('key')!r}, want {key_payload!r}"
    )
    assert consumed["decode_errors"] == 0, (
        f"decode errors during consume: {consumed['decode_errors']}"
    )

    # 3. Assert via --contains on the decoded value (subset match) and via
    #    --match on the envelope. Both root differently: --contains at the
    #    message value, --match at the envelope.
    assert_env = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "assert",
            "--topic",
            topic,
            "--contains",
            json.dumps({"id": payload["id"]}),
            "--match",
            f'.value.amount == {payload["amount"]}',
            "--timeout",
            "10",
            "--from-beginning",
        ],
        env,
    )
    assert assert_env["matched"] is True
    assert assert_env["decode_errors"] == 0


# ---------------------------------------------------------------------------
# (b) Avro capture via kafka listen start / assert / results / messages / stop
# ---------------------------------------------------------------------------


def test_avro_listen_capture_round_trip(
    require_kafka, require_schema_registry, tmp_path
):
    """Avro capture through the ``kafka listen`` daemon.

    Registers the value schema, produces a message, starts the listener
    (which decodes via the codec), waits for the capture, then drives
    assert/results/messages/stop. Asserts the decoded payload appears in the
    capture and that ``decode_errors`` stayed at zero across the lifecycle.
    """
    broker = require_kafka
    sr_url = require_schema_registry
    test_id = uuid.uuid4().hex[:8]
    topic = f"avro.listen.{test_id}"

    _register_schema(sr_url, f"{topic}-value", AVRO_SCHEMA, "AVRO")

    cfg = _write_config(
        tmp_path,
        broker,
        sr_url,
        topics={topic: {"value_format": "avro"}},
    )
    env = _env(broker, sr_url)
    state_dir = tmp_path / "state"

    from agctl.commands.kafka_listen_commands import (
        _kafka_listen_assert_core,
        _kafka_listen_messages_core,
        _kafka_listen_results_core,
        _kafka_listen_start_core,
        _kafka_listen_stop_core,
    )

    payload = {"id": f"listen-{test_id}", "amount": 7}

    # Produce the trigger BEFORE start. ``kafka listen`` seeks every assigned
    # partition to OFFSET_END on assignment (seek-to-latest invariant), so a
    # message produced BEFORE start would be SKIPPED. To make this test
    # deterministic without racing the seek, we start the listener FIRST and
    # produce AFTER — the post-start message is the one captured.
    start = _kafka_listen_start_core(
        config_path=str(cfg),
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
        # Produce post-start so the listener's seek-to-latest doesn't skip it.
        _run_cli(
            [
                "--config",
                str(cfg),
                "kafka",
                "produce",
                "--topic",
                topic,
                "--message",
                json.dumps(payload),
            ],
            env,
        )

        # Poll the on-disk capture file for the decoded payload. The capture
        # loop writes each envelope under a per-topic lock + flush, so this
        # resolves within a poll or two.
        cap = state_dir / f"listen-{run_id}" / f"{topic}.ndjson"
        deadline = time.monotonic() + 30.0
        seen: list = []
        while time.monotonic() < deadline:
            if cap.exists():
                for raw in cap.read_text().splitlines():
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        env_entry = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    seen.append(env_entry.get("value"))
                    if env_entry.get("value") == payload:
                        break
                else:
                    time.sleep(0.2)
                    continue
                break
            time.sleep(0.2)
        assert payload in seen, (
            f"decoded Avro payload not captured within 30s; last_seen={seen!r}"
        )

        # Attach an expectation + evaluate. The decoded value flows through
        # the assert/results path the same as a JSON message would.
        _kafka_listen_assert_core(
            topic=topic,
            contains=None,
            match=f'.value.id == "{payload["id"]}"',
            pattern=None,
            path=None,
            param=(),
            expect_count=1,
            id=None,
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
        )
        results = _kafka_listen_results_core(
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
            config_path=str(cfg),
        )
        assert results["failed"] == 0, f"unexpected listen failures: {results}"
        assert results["passed"] == 1
        assert results["evaluated"] == 1

        # messages returns the decoded payload (not the wire bytes).
        msgs = _kafka_listen_messages_core(
            topic=topic,
            match=None,
            param=(),
            limit=50,
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
        )
        captured_values = [m.get("value") for m in msgs["messages"]]
        assert payload in captured_values, (
            f"decoded payload missing from messages(): {captured_values!r}"
        )
    finally:
        stop = _kafka_listen_stop_core(
            run_id=None,
            pid=None,
            all_=False,
            timeout=10.0,
            state_dir=str(state_dir),
        )
        assert stop["stopped"] is True
        assert stop["failures"] == []


# ---------------------------------------------------------------------------
# (c) Avro mock reactor: decode trigger + encode reaction
# ---------------------------------------------------------------------------


def test_avro_mock_reactor_decode_encode(
    require_kafka, require_schema_registry, tmp_path
):
    """Avro trigger decoded by the reactor + Avro reaction encoded back.

    Configures a ``mocks.kafka.reactors`` entry where BOTH the trigger topic
    and the reaction topic have ``value_format: avro``. Produces an Avro
    trigger via the codec, runs ``mock run --only kafka`` (subprocess) which
    decodes the trigger, fires the reaction, and encodes the reaction value
    via the codec. Then ``kafka consume`` decodes the reaction for the
    assertion.

    This is the load-bearing cross-codec test for the reactor surface: it
    proves the trigger decode path AND the reaction encode path both compose
    against a live SR.
    """
    broker = require_kafka
    sr_url = require_schema_registry
    test_id = uuid.uuid4().hex[:8]
    trigger_topic = f"avro.cmd.{test_id}"
    reaction_topic = f"avro.evt.{test_id}"
    reactor_group = f"agctl-reactor-{test_id}"

    # Reaction value schema: the reactor's reaction.value template yields an
    # ``{id, amount}`` shape (copies from the trigger) — the same Event
    # record works for both sides.
    _register_schema(sr_url, f"{trigger_topic}-value", AVRO_SCHEMA, "AVRO")
    _register_schema(sr_url, f"{reaction_topic}-value", AVRO_SCHEMA, "AVRO")

    cfg = _write_config(
        tmp_path,
        broker,
        sr_url,
        topics={
            trigger_topic: {"value_format": "avro"},
            reaction_topic: {"value_format": "avro"},
        },
        reactors={
            "echo-reactor": {
                "description": "Echo the trigger id+amount as an Avro event",
                "topic": trigger_topic,
                "consumer_group": reactor_group,
                "match": '.value.id == "reactor-trigger"',
                "reaction": {
                    "topic": reaction_topic,
                    "value": {
                        "id": "{id}",
                        "amount": "{amount}",
                    },
                },
            }
        },
    )
    env = _env(broker, sr_url)

    trigger = {"id": "reactor-trigger", "amount": 99}

    # 1. Produce the Avro trigger (codec-encoded). The reactor's fresh
    #    consumer group will reset to earliest and pick this up.
    _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "produce",
            "--topic",
            trigger_topic,
            "--message",
            json.dumps(trigger),
        ],
        env,
    )
    # Settle so the trigger is durable before the reactor subscribes.
    time.sleep(1)

    # 2. Run the reactor for a few seconds (subprocess, --only kafka, bounded
    #    --duration). The daemon decodes the trigger, fires the reaction,
    #    encodes the reaction value via the codec, produces it.
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "agctl.cli",
            "--config",
            str(cfg),
            "mock",
            "run",
            "--only",
            "kafka",
            "--duration",
            "4",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, **env},
    )
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        pytest.fail("mock run --only kafka did not exit within 15s")
    assert proc.returncode == 0, (
        f"mock run failed (rc={proc.returncode}):\n"
        f"stdout: {proc.stdout.read() if proc.stdout else ''}\n"
        f"stderr: {proc.stderr.read() if proc.stderr else ''}"
    )

    # 3. Consume the reaction (decoded) and assert.
    consumed = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "consume",
            "--topic",
            reaction_topic,
            "--from-beginning",
            "--timeout",
            "10",
        ],
        env,
    )
    assert consumed["decode_errors"] == 0, (
        f"decode errors on reaction topic: {consumed['decode_errors']}"
    )
    reaction_values = [m.get("value") for m in consumed["messages"]]
    assert trigger in reaction_values, (
        f"Avro reaction {trigger!r} not found in consumed {reaction_values!r}"
    )


# ---------------------------------------------------------------------------
# (d) Protobuf equivalents of (a)
# ---------------------------------------------------------------------------


def test_protobuf_produce_consume_assert_value(
    require_kafka, require_schema_registry, tmp_path
):
    """Protobuf value round-trip through produce/consume/assert.

    Mirrors :func:`test_avro_produce_consume_assert_value_and_key` on the
    Protobuf codec (value-only; the key remains the default KEY_STRING).
    """
    broker = require_kafka
    sr_url = require_schema_registry
    test_id = uuid.uuid4().hex[:8]
    topic = f"proto.events.{test_id}"

    _register_schema(sr_url, f"{topic}-value", PROTO_SCHEMA, "PROTOBUF")

    cfg = _write_config(
        tmp_path,
        broker,
        sr_url,
        topics={topic: {"value_format": "protobuf"}},
    )
    env = _env(broker, sr_url)

    payload = {"id": f"proto-{test_id}", "amount": 17}

    _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "produce",
            "--topic",
            topic,
            "--message",
            json.dumps(payload),
        ],
        env,
    )
    time.sleep(1)

    consumed = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "consume",
            "--topic",
            topic,
            "--from-beginning",
            "--timeout",
            "10",
        ],
        env,
    )
    assert consumed["count"] >= 1, f"no messages consumed: {consumed}"
    last = consumed["messages"][-1]
    assert last["value"] == payload, (
        f"decoded protobuf value mismatch: got {last.get('value')!r}, "
        f"want {payload!r}"
    )
    assert consumed["decode_errors"] == 0

    assert_env = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "assert",
            "--topic",
            topic,
            "--contains",
            json.dumps({"id": payload["id"]}),
            "--timeout",
            "10",
            "--from-beginning",
        ],
        env,
    )
    assert assert_env["matched"] is True
    assert assert_env["decode_errors"] == 0


# ---------------------------------------------------------------------------
# (e) --value-format CLI override
# ---------------------------------------------------------------------------


def test_value_format_cli_override_avro(
    require_kafka, require_schema_registry, tmp_path
):
    """``--value-format avro`` CLI flag overrides an unset (json-default) topic.

    The config declares NO ``value_format`` for the topic (so it defaults to
    ``json``); the CLI ``--value-format avro`` flag at level-1 precedence
    flips the codec on for this single invocation. Asserts the override path
    composes with both produce and consume.
    """
    broker = require_kafka
    sr_url = require_schema_registry
    test_id = uuid.uuid4().hex[:8]
    topic = f"override.events.{test_id}"

    _register_schema(sr_url, f"{topic}-value", AVRO_SCHEMA, "AVRO")

    # NOTE: topic intentionally has NO value_format — the default (json)
    # would apply if the CLI override were absent.
    cfg = _write_config(tmp_path, broker, sr_url)
    env = _env(broker, sr_url)

    payload = {"id": f"override-{test_id}", "amount": 5}

    # 1. Produce with the override.
    _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "produce",
            "--topic",
            topic,
            "--message",
            json.dumps(payload),
            "--value-format",
            "avro",
        ],
        env,
    )
    time.sleep(1)

    # 2. Consume with the override (else the default json codec would
    #    mis-decode the Avro wire-frame and surface a decode error).
    consumed = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "consume",
            "--topic",
            topic,
            "--from-beginning",
            "--timeout",
            "10",
            "--value-format",
            "avro",
        ],
        env,
    )
    assert consumed["decode_errors"] == 0, (
        f"override produced decode errors: {consumed['decode_errors']}"
    )
    last = consumed["messages"][-1]
    assert last["value"] == payload, (
        f"override round-trip mismatch: got {last.get('value')!r}, "
        f"want {payload!r}"
    )


# ---------------------------------------------------------------------------
# (f) Plaintext SR auth path (basic-auth / mTLS matrix is deferred)
# ---------------------------------------------------------------------------


def test_plaintext_schema_registry_auth_path(
    require_kafka, require_schema_registry, tmp_path
):
    """Smoke test for the plaintext Schema Registry auth mode.

    The ``require_schema_registry`` fixture (under ``AGCTL_TEST_LIVE=1``)
    starts ``cp-schema-registry`` with NO auth — plaintext HTTP on :8081.
    This test exercises the plaintext path end-to-end: register a schema,
    produce+consume through the codec, confirm the cluster's
    ``schema_registry.auth`` is effectively ``plaintext`` (no basic_auth /
    ssl block in the config).

    DEFERRED from the auth matrix:
      * HTTPS + basic-auth SR (Confluent-Cloud-style): requires standing up
        an SR container with a TLS cert + ``authentication.type=BEARER`` or
        basic-auth-configured reverse proxy; not reliable against a local
        container. The unit suite (``test_serialization_*`` +
        ``build_schema_registry_conf``) covers the conf translation; the
        integration confirmation against real basic-auth SR is deferred.
      * mTLS SR: requires a client cert + CA wired into both the SR container
        and agctl's SR client. Container-side mTLS is finicky to stand up
        cleanly; deferred for the same reason.

    Both deferred paths are tracked as a single follow-up: a basic-auth SR
    test will be added once the test harness can provision a TLS-capable SR
    container reliably across Docker Desktop and Linux Docker.
    """
    broker = require_kafka
    sr_url = require_schema_registry

    # 1. The SR /subjects endpoint answers with no auth header required ->
    #    plaintext mode confirmed reachable.
    with urllib.request.urlopen(f"{sr_url}/subjects", timeout=2) as resp:
        assert resp.status == 200

    # 2. End-to-end: register a schema + produce + consume with the cluster's
    #    SR URL set to plaintext (no schema_registry auth block). Mirrors the
    #    other tests' shape but with an explicit assertion that the SR config
    #    block is absent (plaintext is the resolver's default).
    test_id = uuid.uuid4().hex[:8]
    topic = f"auth.events.{test_id}"
    _register_schema(sr_url, f"{topic}-value", AVRO_SCHEMA, "AVRO")

    cfg = _write_config(
        tmp_path,
        broker,
        sr_url,
        topics={topic: {"value_format": "avro"}},
    )
    # Sanity: the YAML has no schema_registry auth block (plaintext default).
    raw = cfg.read_text()
    assert '"schema_registry":' not in raw, (
        f"plaintext SR config should not carry a schema_registry auth block: {raw}"
    )

    env = _env(broker, sr_url)
    payload = {"id": f"auth-{test_id}", "amount": 1}

    _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "produce",
            "--topic",
            topic,
            "--message",
            json.dumps(payload),
        ],
        env,
    )
    time.sleep(1)
    consumed = _run_cli(
        [
            "--config",
            str(cfg),
            "kafka",
            "consume",
            "--topic",
            topic,
            "--from-beginning",
            "--timeout",
            "10",
        ],
        env,
    )
    assert consumed["decode_errors"] == 0
    assert consumed["messages"][-1]["value"] == payload


# ---------------------------------------------------------------------------
# DEFERRED auth-matrix follow-up (documented; not exercised here)
# ---------------------------------------------------------------------------
#
# The brief allows lighter coverage of basic-auth / mTLS SR against containers.
# The unit suite fully covers ``build_schema_registry_conf`` (basic_auth +
# ssl fields → librdkafka conf keys). The integration gap: standing up an SR
# container with TLS + auth reliably. Follow-up work:
#
#   1. basic-auth: run cp-schema-registry with
#      ``authentication.method=BASIC`` + ``authentication.roles`` + a paired
#      ``schema_registry.basic_auth`` block in the cluster config. Workable
#      but adds container config surface (JAAS / role file) that is finicky
#      to make portable across Docker Desktop and Linux Docker.
#
#   2. mTLS: requires a generated CA + client cert, mounting both into the SR
#      container and the agctl SR client. Feasible via testcontainers volume
#      mounts but the cert-generation step is a non-trivial harness addition.
#
# Both are tracked under the same follow-up: a future task adds
# ``require_schema_registry_basic_auth`` + ``require_schema_registry_mtls``
# fixtures once the TLS-provisioning harness lands.
