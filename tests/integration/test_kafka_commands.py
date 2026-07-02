"""Live integration test: kafka produce then assert against a real broker.

Requires env:
- ``AGCTL_TEST_KAFKA_BROKER`` — ``host:port`` of the live Kafka broker (also
  used as the ``KAFKA_BROKER`` env var for config resolution).

Skips (via the ``require_kafka`` fixture) when Kafka is unavailable.
"""

import json
import os
import time

from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = (
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    + "/fixtures/agctl.yaml"
)


def _env(broker):
    return {
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "PAYMENT_SERVICE_URL": "http://localhost:8082",
        "PAYMENT_SERVICE_TOKEN": "tok",
        "KAFKA_BROKER": broker.split(":")[0],
        "DB_HOST": "localhost",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
        "ANALYTICS_DB_HOST": "localhost",
        "ANALYTICS_DB_USER": "au",
        "ANALYTICS_DB_PASSWORD": "ap",
    }


def test_kafka_produce_then_assert(require_kafka):
    """Produce a message, then assert it appears in the consume window."""
    broker = require_kafka
    env = _env(broker)
    topic = "agctl-it"
    body = json.dumps({"event": "IT", "nonce": "abc123"})
    runner = CliRunner()

    # 1. Produce.
    produce = runner.invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "kafka",
            "produce",
            "--topic",
            topic,
            "--message",
            body,
        ],
        env=env,
    )
    assert produce.exit_code == 0, produce.output
    prod_env = json.loads(produce.output)
    assert prod_env["ok"] is True

    # Allow the broker a moment to make the produced message readable.
    time.sleep(1)

    # 2. Assert the message is present in the window.
    assert_env = runner.invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "kafka",
            "assert",
            "--topic",
            topic,
            "--contains",
            body,
            "--timeout",
            "10",
            "--from-beginning",
        ],
        env=env,
    )
    assert assert_env.exit_code == 0, assert_env.output
    envelope = json.loads(assert_env.output)
    assert envelope["ok"] is True
    assert envelope["result"]["matched"] is True
