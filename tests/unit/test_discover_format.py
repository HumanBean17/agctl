"""Unit tests for kafka-pattern discover value_format/key_format resolution (Task 10)."""

import json
from pathlib import Path

from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "SCHEMA_REGISTRY_URL": "",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "p",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
}


def _run_with(args, config_yaml, tmp_path, monkeypatch):
    """Run discover against an ad-hoc config written to ``tmp_path``."""
    cfg_file = tmp_path / "agctl.yaml"
    cfg_file.write_text(config_yaml)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--config", str(cfg_file), "discover", *args],
        env=ENV,
        standalone_mode=False,
    )
    return result


def test_kafka_pattern_item_shows_resolved_value_format_from_topic(tmp_path, monkeypatch):
    """Pattern topic has value_format: avro -> item value_format == 'avro'."""
    config = (
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers:\n"
        '        - "localhost:9092"\n'
        "  default_cluster: main\n"
        "  patterns:\n"
        "    order-created:\n"
        '      description: "An order created event"\n'
        "      topic: orders.created\n"
        "  topics:\n"
        "    orders.created:\n"
        "      value_format: avro\n"
    )
    result = _run_with(
        ["--category", "kafka-patterns", "--name", "order-created"],
        config,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["name"] == "order-created"
    # Topic-level value_format wins
    assert res["value_format"] == "avro"
    # Key format falls back to default (string) when not set
    assert res["key_format"] == "string"


def test_kafka_pattern_item_shows_resolved_value_format_from_cluster_default(tmp_path, monkeypatch):
    """Pattern topic absent from kafka.topics -> cluster default value_format == 'json'."""
    config = (
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers:\n"
        '        - "localhost:9092"\n'
        "      value_format: json\n"
        "  default_cluster: main\n"
        "  patterns:\n"
        "    order-created:\n"
        '      description: "An order created event"\n'
        "      topic: orders.created\n"
    )
    result = _run_with(
        ["--category", "kafka-patterns", "--name", "order-created"],
        config,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["name"] == "order-created"
    # Cluster-level value_format wins when topic not in kafka.topics
    assert res["value_format"] == "json"
    # Key format falls back to default (string)
    assert res["key_format"] == "string"


def test_kafka_pattern_item_shows_resolved_key_format_from_topic(tmp_path, monkeypatch):
    """Pattern topic has key_format: avro -> item key_format == 'avro'."""
    config = (
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers:\n"
        '        - "localhost:9092"\n'
        "  default_cluster: main\n"
        "  patterns:\n"
        "    order-created:\n"
        '      description: "An order created event"\n'
        "      topic: orders.created\n"
        "  topics:\n"
        "    orders.created:\n"
        "      value_format: json\n"
        "      key_format: avro\n"
    )
    result = _run_with(
        ["--category", "kafka-patterns", "--name", "order-created"],
        config,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["name"] == "order-created"
    assert res["value_format"] == "json"
    assert res["key_format"] == "avro"


def test_kafka_pattern_item_shows_defaults_when_no_formats_configured(tmp_path, monkeypatch):
    """No topic/cluster formats -> defaults: value_format='json', key_format='string'."""
    config = (
        'version: "3"\n'
        "kafka:\n"
        "  clusters:\n"
        "    main:\n"
        "      brokers:\n"
        '        - "localhost:9092"\n'
        "  default_cluster: main\n"
        "  patterns:\n"
        "    order-created:\n"
        '      description: "An order created event"\n'
        "      topic: orders.created\n"
    )
    result = _run_with(
        ["--category", "kafka-patterns", "--name", "order-created"],
        config,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["name"] == "order-created"
    # Final fallback defaults
    assert res["value_format"] == "json"
    assert res["key_format"] == "string"
