"""Unit tests for ``kafka listen assert`` / ``results`` / ``messages`` (Task 9).

These three commands are ``@envelope``-wrapped file readers: they resolve the
running listener via its pidfile, then read/append the run dir's on-disk state
(``asserts.jsonl`` + per-topic ``<topic>.ndjson`` captures). No daemon IPC, no
Kafka client, no signaling — so the tests drive the ``_core`` functions with
nothing more than a temp state dir, a live-pid pidfile (``os.getpid()``), and a
canned capture file.

Cross-platform at the command layer (no ``require_posix_daemon`` — the commands
only read files), but the TESTS plant a live-pid pidfile (``os.getpid()``) that
the commands resolve via ``is_alive`` → ``os.kill(getpid(), 0)``. On Windows
``os.kill`` of the live pid destabilizes the process at shutdown (the same
issue documented for ``test_mock_daemon.py`` / ``test_kafka_listen_daemon_cmds.py``),
so this module skips on Windows (see ``pytestmark`` below).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from agctl.commands.kafka_listen_commands import (
    _kafka_listen_assert_core,
    _kafka_listen_messages_core,
    _kafka_listen_results_core,
)
from agctl.errors import AssertionFailure, ConfigError

pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason=(
        "tests plant a live-pid pidfile (os.getpid()) resolved via is_alive "
        "(os.kill), which destabilizes the process on Windows shutdown — same "
        "as test_mock_daemon / test_kafka_listen_daemon_cmds"
    ),
)


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_config(tmp_path: Path) -> Path:
    """One-cluster v3 config with no patterns (results needs cfg.kafka.patterns)."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        "\n".join(
            [
                'version: "3"',
                "kafka:",
                "  clusters:",
                "    default:",
                "      brokers: [broker-a:9092]",
                "  default_cluster: default",
                "",
            ]
        )
    )
    return cfg


def _envelope(value: dict, *, topic: str, offset: int = 0) -> dict:
    """Build a minimal CapturedEnvelope as written by the listen daemon."""
    return {
        "topic": topic,
        "key": None,
        "value": value,
        "partition": 0,
        "offset": offset,
        "timestamp": None,
        "headers": {},
        "captured_at": "2026-07-15T00:00:00Z",
    }


# Three envelopes on orders.created: 2x ORDER_CREATED, 1x ORDER_CANCELLED.
ORDER_A = _envelope({"eventType": "ORDER_CREATED", "id": "a"}, topic="orders.created", offset=0)
ORDER_B = _envelope({"eventType": "ORDER_CREATED", "id": "b"}, topic="orders.created", offset=1)
ORDER_C = _envelope({"eventType": "ORDER_CANCELLED", "id": "c"}, topic="orders.created", offset=2)

MATCH_CREATED = '.value.eventType == "ORDER_CREATED"'


def _plant_listener(tmp_path: Path, *, run_id: str = "aa112233") -> tuple[Path, Path]:
    """Plant a state dir with one live-pid pidfile + run dir + orders.created.ndjson.

    Returns ``(state_dir, run_dir)``.
    """
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    rdir = state_dir / f"listen-{run_id}"
    rdir.mkdir(parents=True)
    logp = rdir / "events.log"
    logp.write_text("")  # events.log content is irrelevant to assert/results/messages

    # Capture file: 3 envelopes, 2 matching MATCH_CREATED.
    (rdir / "orders.created.ndjson").write_text(
        "\n".join(json.dumps(env) for env in (ORDER_A, ORDER_B, ORDER_C)) + "\n"
    )

    pidfile = state_dir / f"listen-{run_id}.pid"
    pidfile.write_text(
        json.dumps(
            {
                "pid": os.getpid(),  # live pid → resolve_listener_target finds it
                "run_id": run_id,
                "topics": ["orders.created"],
                "group": f"agctl-listen-{run_id}",
                "cluster": "default",
                "started_at": _now_iso_z(),
                "state_dir": str(state_dir),
                "log_path": str(logp),
            }
        )
    )
    return state_dir, rdir


# ---------------------------------------------------------------------------
# assert
# ---------------------------------------------------------------------------


class TestAssert:
    def test_assert_attaches_expectation_returns_dict(self, tmp_path):
        """--match with expect-count 1 appends one line + returns attached dict."""
        state_dir, rdir = _plant_listener(tmp_path)
        result = _kafka_listen_assert_core(
            topic="orders.created",
            contains=None,
            match=MATCH_CREATED,
            pattern=None,
            path=None,
            param=(),
            expect_count=1,
            id=None,
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
        )
        assert result == {
            "attached": True,
            "id": "exp-1",
            "topic": "orders.created",
            "modes": ["match"],
            "expect_count": 1,
        }
        # One line appended to asserts.jsonl.
        lines = (rdir / "asserts.jsonl").read_text().splitlines()
        assert len(lines) == 1
        spec = json.loads(lines[0])
        assert spec["id"] == "exp-1"
        assert spec["topic"] == "orders.created"
        assert spec["modes"] == {
            "contains": None,
            "match": MATCH_CREATED,
            "pattern": None,
            "path": None,
        }
        assert spec["expect_count"] == 1
        assert spec["params"] == {}

    def test_assert_default_id_increments_from_existing_count(self, tmp_path):
        """With one existing expectation, the auto id is exp-2."""
        state_dir, rdir = _plant_listener(tmp_path)
        # Pre-write one expectation.
        (rdir / "asserts.jsonl").write_text(
            json.dumps(
                {"id": "exp-1", "topic": "orders.created", "modes": {"match": MATCH_CREATED},
                 "params": {}, "expect_count": 1}
            )
            + "\n"
        )
        result = _kafka_listen_assert_core(
            topic="orders.created", contains=None, match=MATCH_CREATED, pattern=None,
            path=None, param=(), expect_count=1, id=None,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        assert result["id"] == "exp-2"

    def test_assert_explicit_id_is_used(self, tmp_path):
        state_dir, _ = _plant_listener(tmp_path)
        result = _kafka_listen_assert_core(
            topic="orders.created", contains=None, match=MATCH_CREATED, pattern=None,
            path=None, param=(), expect_count=2, id="my-custom-id",
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        assert result["id"] == "my-custom-id"

    def test_assert_zero_modes_raises_configerror(self, tmp_path):
        """No --contains/--match/--pattern (path alone is not a mode) → ConfigError."""
        state_dir, _ = _plant_listener(tmp_path)
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_assert_core(
                topic="orders.created", contains=None, match=None, pattern=None,
                path=".value.eventType",  # path alone does NOT count as a mode
                param=(), expect_count=1, id=None,
                run_id=None, pid=None, state_dir=str(state_dir),
            )
        assert "at least one of --contains/--match/--pattern" in ei.value.message

    def test_assert_no_running_listener_raises_configerror(self, tmp_path):
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_assert_core(
                topic="orders.created", contains=None, match=MATCH_CREATED, pattern=None,
                path=None, param=(), expect_count=1, id=None,
                run_id=None, pid=None, state_dir=str(tmp_path / "empty"),
            )
        assert "no running kafka listener" in ei.value.message

    def test_assert_contains_stored_raw(self, tmp_path):
        """--contains is stored as the raw JSON string (evaluate_expectations json.loads it)."""
        state_dir, rdir = _plant_listener(tmp_path)
        _kafka_listen_assert_core(
            topic="orders.created", contains='{"eventType": "ORDER_CREATED"}',
            match=None, pattern=None, path=None, param=(), expect_count=1, id=None,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        spec = json.loads((rdir / "asserts.jsonl").read_text().splitlines()[0])
        # Stored RAW (string), not pre-parsed — evaluate_expectations handles both.
        assert spec["modes"]["contains"] == '{"eventType": "ORDER_CREATED"}'
        assert "match" in spec["modes"] and spec["modes"]["match"] is None


# ---------------------------------------------------------------------------
# results
# ---------------------------------------------------------------------------


class TestResults:
    def test_results_pass_when_expect_count_met(self, tmp_path):
        """Attach one expectation (expect-count 1, 2 match) → passed:1, failed:0."""
        state_dir, _ = _plant_listener(tmp_path)
        cfg_path = _write_config(tmp_path)
        _kafka_listen_assert_core(
            topic="orders.created", contains=None, match=MATCH_CREATED, pattern=None,
            path=None, param=(), expect_count=1, id=None,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        result = _kafka_listen_results_core(
            run_id=None, pid=None, state_dir=str(state_dir),
            config_path=str(cfg_path), overlay_paths=None, env_file=None,
        )
        assert result["evaluated"] == 1
        assert result["passed"] == 1
        assert result["failed"] == 0
        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is True
        assert result["results"][0]["matched_count"] == 2
        assert result["results"][0]["expect_count"] == 1

    def test_results_fail_raises_assertionfailure_with_per_result_detail(self, tmp_path):
        """expect-count 3 (only 2 match) → AssertionFailure; detail.results[0].matched_count==2."""
        state_dir, _ = _plant_listener(tmp_path)
        cfg_path = _write_config(tmp_path)
        _kafka_listen_assert_core(
            topic="orders.created", contains=None, match=MATCH_CREATED, pattern=None,
            path=None, param=(), expect_count=3, id=None,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        with pytest.raises(AssertionFailure) as ei:
            _kafka_listen_results_core(
                run_id=None, pid=None, state_dir=str(state_dir),
                config_path=str(cfg_path), overlay_paths=None, env_file=None,
            )
        assert "1/1 expectation(s) failed" in ei.value.message
        results = ei.value.detail["results"]
        assert len(results) == 1
        assert results[0]["matched_count"] == 2
        assert results[0]["expect_count"] == 3
        assert results[0]["passed"] is False

    def test_results_no_expectations_raises_configerror(self, tmp_path):
        """No asserts.jsonl → ConfigError pointing at 'kafka listen assert'."""
        state_dir, _ = _plant_listener(tmp_path)
        cfg_path = _write_config(tmp_path)
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_results_core(
                run_id=None, pid=None, state_dir=str(state_dir),
                config_path=str(cfg_path), overlay_paths=None, env_file=None,
            )
        assert "no expectations attached" in ei.value.message

    def test_results_no_running_listener_raises_configerror(self, tmp_path):
        cfg_path = _write_config(tmp_path)
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_results_core(
                run_id=None, pid=None, state_dir=str(tmp_path / "empty"),
                config_path=str(cfg_path), overlay_paths=None, env_file=None,
            )
        assert "no running kafka listener" in ei.value.message


# ---------------------------------------------------------------------------
# messages
# ---------------------------------------------------------------------------


class TestMessages:
    def test_messages_match_with_limit_truncates(self, tmp_path):
        """--match (2 match) --limit 1 → matched:2, truncated:True, one message."""
        state_dir, _ = _plant_listener(tmp_path)
        result = _kafka_listen_messages_core(
            topic="orders.created", match=MATCH_CREATED, param=(), limit=1,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        assert result["topic"] == "orders.created"
        assert result["matched"] == 2
        assert result["truncated"] is True
        assert len(result["messages"]) == 1
        # The one returned message is one of the matching envelopes.
        assert result["messages"][0]["value"]["eventType"] == "ORDER_CREATED"

    def test_messages_no_match_returns_all(self, tmp_path):
        """No --match → predicate None → every envelope matches (up to limit)."""
        state_dir, _ = _plant_listener(tmp_path)
        result = _kafka_listen_messages_core(
            topic="orders.created", match=None, param=(), limit=50,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        assert result["matched"] == 3
        assert result["truncated"] is False
        assert len(result["messages"]) == 3

    def test_messages_validates_jq_up_front(self, tmp_path):
        """A malformed --match raises ConfigError before scanning."""
        state_dir, _ = _plant_listener(tmp_path)
        with pytest.raises(ConfigError):
            _kafka_listen_messages_core(
                topic="orders.created", match=".value.== broken", param=(), limit=50,
                run_id=None, pid=None, state_dir=str(state_dir),
            )

    def test_messages_no_running_listener_raises_configerror(self, tmp_path):
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_messages_core(
                topic="orders.created", match=None, param=(), limit=50,
                run_id=None, pid=None, state_dir=str(tmp_path / "empty"),
            )
        assert "no running kafka listener" in ei.value.message

    def test_messages_missing_capture_file_is_empty(self, tmp_path):
        """A topic with no capture file yet → matched:0, truncated:False, messages:[]."""
        state_dir, _ = _plant_listener(tmp_path)
        result = _kafka_listen_messages_core(
            topic="no.such.topic", match=None, param=(), limit=50,
            run_id=None, pid=None, state_dir=str(state_dir),
        )
        assert result["matched"] == 0
        assert result["truncated"] is False
        assert result["messages"] == []

    def test_messages_fills_placeholders_from_params(self, tmp_path):
        """--param fills {name} tokens in --match before building the predicate."""
        state_dir, _ = _plant_listener(tmp_path)
        result = _kafka_listen_messages_core(
            topic="orders.created",
            match='.value.eventType == "{etype}"',
            param=("etype=ORDER_CREATED",),
            limit=5,
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
        )
        assert result["matched"] == 2
