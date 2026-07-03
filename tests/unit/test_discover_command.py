"""Unit tests for the top-level `discover` command (DESIGN §3.6, D9)."""

import json
import os
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


def _run(args, monkeypatch):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--config", str(FIXTURE), "discover", *args],
        env=ENV,
        standalone_mode=False,
    )
    return result


def _payload(result):
    return json.loads(result.output)


# --------------------------------------------------------------------------- #
# Level 0 — summary
# --------------------------------------------------------------------------- #


def test_summary_counts_and_hint(monkeypatch):
    result = _run([], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["ok"] is True
    assert payload["command"] == "discover.summary"
    counts = payload["result"]
    assert counts["services"] == 2
    assert counts["http_templates"] == 4
    assert counts["kafka_patterns"] == 2
    assert counts["db_templates"] == 4
    assert "hint" in counts


# --------------------------------------------------------------------------- #
# Level 1 — category listing
# --------------------------------------------------------------------------- #


def test_category_http_templates(monkeypatch):
    result = _run(["--category", "http-templates"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["command"] == "discover.category"
    res = payload["result"]
    assert res["category"] == "http-templates"
    assert res["count"] == 4
    names = {item["name"] for item in res["items"]}
    assert names == {"create-order", "get-order", "charge-payment", "get-payment-status"}
    # Each item is name + description only (no params/sql/method).
    for item in res["items"]:
        assert set(item.keys()) == {"name", "description"}
        assert isinstance(item["description"], str)


def test_category_services_omit_description(monkeypatch):
    result = _run(["--category", "services"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["count"] == 2
    for item in res["items"]:
        # Services have no description — emit name only.
        assert set(item.keys()) == {"name"}


def test_category_db_templates(monkeypatch):
    result = _run(["--category", "db-templates"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["count"] == 4
    # Listing is name + description only — no sql/params.
    for item in res["items"]:
        assert set(item.keys()) == {"name", "description"}


def test_category_kafka_patterns(monkeypatch):
    result = _run(["--category", "kafka-patterns"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["count"] == 2


def test_unknown_category_config_error(monkeypatch):
    result = _run(["--category", "bogus"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# Level 2 — item detail
# --------------------------------------------------------------------------- #


def test_item_db_template_includes_sql(monkeypatch):
    result = _run(["--category", "db-templates", "--name", "find-order"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["category"] == "db-templates"
    assert res["name"] == "find-order"
    # D9: SQL is included verbatim.
    assert res["sql"] == (
        "SELECT id, status, total_cents, created_at FROM orders WHERE id = :orderId"
    )
    assert res["params"] == ["orderId"]
    assert res["connection"] == "main-db"
    assert res["example"].startswith("agctl db query --template find-order")


def test_item_http_template(monkeypatch):
    result = _run(["--category", "http-templates", "--name", "create-order"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["method"] == "POST"
    assert res["service"] == "order-service"
    assert res["path"] == "/api/v1/orders"
    assert res["params"] == ["customer_id", "sku"]
    # Example lists both params.
    assert "--param customer_id=X" in res["example"]
    assert "--param sku=Y" in res["example"]
    assert res["example"].startswith("agctl http call create-order")


def test_item_kafka_pattern(monkeypatch):
    result = _run(["--category", "kafka-patterns", "--name", "order-created"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["topic"] == "orders.created"
    assert res["params"] == ["orderId"]
    assert res["example"] == (
        "agctl kafka assert --pattern order-created --param orderId=X --timeout 10"
    )


def test_item_service(monkeypatch):
    result = _run(["--category", "services", "--name", "order-service"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["name"] == "order-service"
    assert "base_url" in res
    assert res.get("health_path") == "/actuator/health"


def test_item_unknown_template_missing(monkeypatch):
    result = _run(["--category", "http-templates", "--name", "nope"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "TemplateNotFound"


def test_item_unknown_db_template_missing(monkeypatch):
    result = _run(["--category", "db-templates", "--name", "nope"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["error"]["type"] == "TemplateNotFound"


# --------------------------------------------------------------------------- #
# Search
# --------------------------------------------------------------------------- #


def test_search_payment_across_categories(monkeypatch):
    result = _run(["--search", "payment"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["command"] == "discover.search"
    res = payload["result"]
    assert res["query"] == "payment"
    matches = res["matches"]
    by_key = {(m["category"], m["name"]) for m in matches}
    # At least these three categories appear.
    assert ("http-templates", "get-payment-status") in by_key
    assert ("kafka-patterns", "payment-failed") in by_key
    assert ("db-templates", "count-failed-payments") in by_key
    for m in matches:
        assert "category" in m
        assert "name" in m


# --------------------------------------------------------------------------- #
# Mode-mutual-exclusion errors
# --------------------------------------------------------------------------- #


def test_name_without_category_errors(monkeypatch):
    result = _run(["--name", "find-order"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["error"]["type"] == "ConfigError"


def test_category_and_search_mutually_exclusive(monkeypatch):
    result = _run(["--category", "http-templates", "--search", "x"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["error"]["type"] == "ConfigError"
