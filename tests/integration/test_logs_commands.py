"""Always-on integration tests for ``logs`` commands over real temp NDJSON files.

These tests drive the real ``NdjsonFileBackend`` through the Click CLI against
temporary log files. No external services (broker/DB/HTTP) are required, so they
run always-on without ``AGCTL_TEST_LIVE`` gating.

Each test writes a temporary ``agctl.yaml`` whose ``logs.sources.svc.path`` points
at a temporary ``.log`` file it also writes with real logstash NDJSON lines,
then invokes the real ``cli`` via ``CliRunner``.
"""

import json
import threading
import time
from datetime import datetime, timezone

import pytest
from click.testing import CliRunner

from agctl.cli import cli


def _write_config(tmp_path, log_path):
    """Write a minimal agctl.yaml config with a logs source pointing at log_path."""
    config_path = tmp_path / "agctl.yaml"
    config_content = f"""version: "2"
services:
  demo:
    base_url: "http://localhost:9999"
logs:
  sources:
    svc:
      path: "{log_path}"
      format: logstash
      service: demo
  defaults:
    timeout_seconds: 10
    poll_interval_ms: 100
"""
    config_path.write_text(config_content)
    return config_path


def _write_log_lines(log_path, lines):
    """Write NDJSON lines to a log file. Each line is a dict that gets JSON-encoded."""
    log_path.write_text("\n".join(json.dumps(line) for line in lines))


def test_query_real_file_filters_and_window(tmp_path):
    """Query filters by level and time window, returning only matching entries."""
    log_path = tmp_path / "svc.log"
    now = datetime.now(timezone.utc)
    # Write 3 lines: 2 ERROR, 1 INFO, spanning 2 hours
    lines = [
        {
            "@timestamp": (now.replace(microsecond=0)).isoformat(),
            "level": "ERROR",
            "logger_name": "order.service",
            "message": "Order failed",
            "orderId": "ord-1",
        },
        {
            "@timestamp": (now.replace(microsecond=0)).isoformat(),
            "level": "INFO",
            "logger_name": "order.service",
            "message": "Order created",
            "orderId": "ord-2",
        },
        {
            "@timestamp": (now.replace(microsecond=0)).isoformat(),
            "level": "ERROR",
            "logger_name": "order.service",
            "message": "Payment failed",
            "orderId": "ord-3",
        },
    ]
    _write_log_lines(log_path, lines)

    config_path = _write_config(tmp_path, log_path)
    result = CliRunner().invoke(
        cli,
        ["--config", str(config_path), "logs", "query", "--source", "svc", "--level", "ERROR", "--since", "1h"],
    )

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    entries = envelope["result"]["entries"]
    assert len(entries) == 2
    assert all(e["level"] == "ERROR" for e in entries)
    assert envelope["result"]["matched"] == 2


def test_assert_positive_then_not(tmp_path):
    """Assert positive (INFO present) and negative (FATAL not present) cases."""
    log_path = tmp_path / "svc.log"
    now = datetime.now(timezone.utc)
    lines = [
        {
            "@timestamp": now.isoformat(),
            "level": "INFO",
            "logger_name": "order.service",
            "message": "Order created",
        }
    ]
    _write_log_lines(log_path, lines)

    config_path = _write_config(tmp_path, log_path)
    runner = CliRunner()

    # 1. Assert INFO is present -> exit 0
    result = runner.invoke(
        cli,
        ["--config", str(config_path), "logs", "assert", "--source", "svc", "--level", "INFO", "--since", "1h"],
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["matched"] is True

    # 2. Assert ERROR is present -> exit 1 (not found)
    result = runner.invoke(
        cli,
        ["--config", str(config_path), "logs", "assert", "--source", "svc", "--level", "ERROR", "--since", "1h"],
    )
    assert result.exit_code == 1, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is False

    # 3. Assert INFO is NOT present (with --not) -> exit 1 (found, but forbidden)
    result = runner.invoke(
        cli,
        ["--config", str(config_path), "logs", "assert", "--source", "svc", "--level", "INFO", "--since", "1h", "--not"],
    )
    assert result.exit_code == 1, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is False

    # 4. Assert FATAL is NOT present (with --not) -> exit 0 (correctly absent)
    # matched=True means the assertion succeeded (i.e., no FATAL was found as required by --not)
    result = runner.invoke(
        cli,
        ["--config", str(config_path), "logs", "assert", "--source", "svc", "--level", "FATAL", "--since", "1h", "--not"],
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["matched"] is True  # Assertion succeeded: no FATAL found


def test_assert_poll_finds_appended_line(tmp_path):
    """Assert with --timeout polls and finds a line appended after the command starts."""
    log_path = tmp_path / "svc.log"
    # Start with an empty file
    log_path.write_text("")

    config_path = _write_config(tmp_path, log_path)

    # Start the assert command in a background thread with a 3s timeout
    assert_result = {"done": False, "exit_code": None, "output": None}
    append_delay = 0.4  # seconds

    def run_assert():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--config",
                str(config_path),
                "logs",
                "assert",
                "--source",
                "svc",
                '--match',
                '.fields.orderId == "ord-2"',
                "--since",
                "1h",
                "--timeout",
                "3",  # short timeout for fast test
            ],
        )
        assert_result["exit_code"] = result.exit_code
        assert_result["output"] = result.output
        assert_result["done"] = True

    thread = threading.Thread(target=run_assert, daemon=True)
    thread.start()

    # Wait a bit, then append the matching line
    time.sleep(append_delay)
    now = datetime.now(timezone.utc)
    matching_line = {
        "@timestamp": now.isoformat(),
        "level": "INFO",
        "logger_name": "order.service",
        "message": "Order created",
        "orderId": "ord-2",
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(matching_line) + "\n")

    # Wait for the assert to complete
    thread.join(timeout=5)
    assert assert_result["done"], "Assert thread did not complete in time"
    assert assert_result["exit_code"] == 0, assert_result["output"]
    envelope = json.loads(assert_result["output"])
    assert envelope["ok"] is True
    assert envelope["result"]["matched"] is True


def test_tail_streams_appended_lines(tmp_path):
    """Tail with --duration streams lines appended during the run, then exits cleanly."""
    log_path = tmp_path / "svc.log"
    # Start with an empty file
    log_path.write_text("")

    config_path = _write_config(tmp_path, log_path)

    # Start tail in background with 1s duration
    tail_result = {"done": False, "exit_code": None, "output": None}

    def run_tail():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--config", str(config_path), "logs", "tail", "--source", "svc", "--duration", "1"],
        )
        tail_result["exit_code"] = result.exit_code
        tail_result["output"] = result.output
        tail_result["done"] = True

    thread = threading.Thread(target=run_tail, daemon=True)
    thread.start()

    # Append 2 lines after a short delay
    time.sleep(0.2)
    now = datetime.now(timezone.utc)
    lines = [
        {
            "@timestamp": now.isoformat(),
            "level": "INFO",
            "logger_name": "order.service",
            "message": "Line 1",
            "seq": 1,
        },
        {
            "@timestamp": now.isoformat(),
            "level": "INFO",
            "logger_name": "order.service",
            "message": "Line 2",
            "seq": 2,
        },
    ]
    with open(log_path, "a") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
            time.sleep(0.1)  # small gap between lines

    # Wait for tail to complete
    thread.join(timeout=5)
    assert tail_result["done"], "Tail thread did not complete in time"
    assert tail_result["exit_code"] == 0, tail_result["output"]

    # Output should have 2 entry lines + 1 summary line (all JSON)
    output_lines = [line for line in tail_result["output"].strip().split("\n") if line.strip()]
    assert len(output_lines) == 3, f"Expected 3 lines (2 entries + 1 summary), got {len(output_lines)}"

    # Parse the summary line (last line, JSON with "summary": true)
    summary = json.loads(output_lines[-1])
    assert summary.get("summary") is True
    assert summary.get("total_emitted") == 2


def test_missing_file_is_empty_not_error(tmp_path):
    """Query against a missing log file exits 0 with matched==0 (D10)."""
    # Point config at a path that doesn't exist
    nonexistent_log = tmp_path / "does_not_exist.log"
    config_path = _write_config(tmp_path, nonexistent_log)

    result = CliRunner().invoke(
        cli,
        ["--config", str(config_path), "logs", "query", "--source", "svc"],
    )

    # Should exit 0, not error
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    # Should report 0 matched (empty result)
    assert envelope["result"]["matched"] == 0
    assert envelope["result"]["entries"] == []


def test_discover_item_real_schema(tmp_path):
    """Discover log-sources item returns sampled schema_fields from real log file."""
    log_path = tmp_path / "svc.log"
    now = datetime.now(timezone.utc)
    # Write lines with varied fields to exercise schema sampling
    lines = [
        {
            "@timestamp": now.isoformat(),
            "@version": "1",
            "level": "INFO",
            "logger_name": "order.service",
            "message": "Order created",
            "orderId": "ord-1",
            "amount_cents": 1000,
        },
        {
            "@timestamp": now.isoformat(),
            "@version": "1",
            "level": "ERROR",
            "logger_name": "payment.service",
            "message": "Payment failed",
            "orderId": "ord-2",
            "stack_trace": "java.lang.Exception: oops\n\tat Foo.java:42",
        },
        {
            "@timestamp": now.isoformat(),
            "@version": "1",
            "level": "WARN",
            "logger_name": "order.service",
            "message": "Order delayed",
            "orderId": "ord-3",
            "delay_seconds": 5,
        },
    ]
    _write_log_lines(log_path, lines)

    config_path = _write_config(tmp_path, log_path)

    result = CliRunner().invoke(
        cli,
        ["--config", str(config_path), "discover", "--category", "log-sources", "--name", "svc"],
    )

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["command"] == "discover.item"

    res = envelope["result"]
    assert res["category"] == "log-sources"
    assert res["name"] == "svc"
    assert res["type"] == "file"
    assert res["format"] == "logstash"
    assert res["path"] == str(log_path)

    # Check schema_fields
    schema = res["schema_fields"]
    # Standard fields present in any sampled entry (union)
    assert set(schema["standard"]) == {"timestamp", "level", "logger", "message"}
    # Conditional fields present in SOME entries
    assert "stack_trace" in schema["conditional"]
    # Observed includes all unique field names (excluding internal @version)
    observed = schema["observed"]
    assert "orderId" in observed
    assert "amount_cents" in observed
    assert "delay_seconds" in observed
    assert "@version" not in observed  # internal fields excluded
    # Example command starts with "agctl logs query"
    assert res["example"].startswith("agctl logs query --source svc")
