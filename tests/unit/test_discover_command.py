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


def _run_with(args, config_yaml, tmp_path, monkeypatch):
    """Run discover against an ad-hoc config written to ``tmp_path``.

    Used for cases that don't fit the shared fixture: the absent-mocks
    zero-count path and item detail with a capture (kept out of the shared
    fixture to avoid its finicky placement validation).
    """
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
    assert counts["mock_http_stubs"] == 2
    assert counts["mock_kafka_reactors"] == 2
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
    # Listing is name + description + mode only — no sql/params.
    for item in res["items"]:
        assert set(item.keys()) == {"name", "description", "mode"}
        assert isinstance(item["description"], str)
        assert item["mode"] in ["read", "write"]


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


# --------------------------------------------------------------------------- #
# Task 8: mode marker tests
# --------------------------------------------------------------------------- #


def test_db_templates_category_mode_field(monkeypatch):
    """Task 8 Test 1: discover --category db-templates Level-1: every item has a mode key."""
    result = _run(["--category", "db-templates"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["count"] == 4

    # Build a lookup of item -> mode
    modes_by_name = {item["name"]: item["mode"] for item in res["items"]}

    # Specific templates have specific modes
    assert modes_by_name["find-order"] == "read"
    assert modes_by_name["orders-by-status"] == "read"
    assert modes_by_name["count-failed-payments"] == "read"
    assert modes_by_name["seed-order"] == "write"


def test_db_template_item_write_mode_example(monkeypatch):
    """Task 8 Test 2: discover --category db-templates --name seed-order Level-2: mode is write, example uses db execute."""
    result = _run(["--category", "db-templates", "--name", "seed-order"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "db-templates"
    assert res["name"] == "seed-order"
    assert res["mode"] == "write"
    # Write mode example starts with db execute --write
    assert res["example"].startswith("agctl db execute --template seed-order --write")
    # sql is present
    assert "sql" in res
    assert res["sql"] == (
        "INSERT INTO agctl_seed (id, status) VALUES (:orderId, :status) ON CONFLICT (id) DO NOTHING RETURNING id, status"
    )
    # params are extracted
    assert res["params"] == ["orderId", "status"]


def test_db_template_item_read_mode_example(monkeypatch):
    """Task 8 Test 3: discover --category db-templates --name find-order Level-2: mode is read, example uses db query."""
    result = _run(["--category", "db-templates", "--name", "find-order"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "db-templates"
    assert res["name"] == "find-order"
    assert res["mode"] == "read"
    # Read mode example starts with db query (unchanged behavior)
    assert res["example"].startswith("agctl db query --template find-order")


def test_search_includes_mode_field(monkeypatch):
    """Task 8 Test 4: discover --search order: matches include mode field for db-templates."""
    result = _run(["--search", "order"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["query"] == "order"
    matches = res["matches"]

    # Build a lookup for db-templates
    db_matches = {m["name"]: m for m in matches if m["category"] == "db-templates"}

    # seed-order and find-order should both appear
    assert "seed-order" in db_matches
    assert "find-order" in db_matches

    # seed-order has mode write
    assert db_matches["seed-order"]["mode"] == "write"
    # find-order has mode read
    assert db_matches["find-order"]["mode"] == "read"


# --------------------------------------------------------------------------- #
# Mock categories (mock-http-stubs / mock-kafka-reactors)
# --------------------------------------------------------------------------- #


def test_category_mock_http_stubs(monkeypatch):
    result = _run(["--category", "mock-http-stubs"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["command"] == "discover.category"
    res = payload["result"]
    assert res["category"] == "mock-http-stubs"
    assert res["count"] == 2
    by_name = {item["name"]: item for item in res["items"]}
    assert set(by_name) == {"charge-ok", "healthz"}
    for item in res["items"]:
        # Listing is name + description + method + path only.
        assert set(item.keys()) == {"name", "description", "method", "path"}
    assert by_name["charge-ok"]["method"] == "POST"
    assert by_name["charge-ok"]["path"] == "/v1/charge"
    assert by_name["healthz"]["method"] == "GET"


def test_category_mock_kafka_reactors(monkeypatch):
    result = _run(["--category", "mock-kafka-reactors"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "mock-kafka-reactors"
    assert res["count"] == 2
    by_name = {item["name"]: item for item in res["items"]}
    assert set(by_name) == {"order-confirmation", "payment-audit"}
    for item in res["items"]:
        assert set(item.keys()) == {"name", "description", "topic", "consumer_group"}
    assert by_name["order-confirmation"]["topic"] == "orders.created"
    assert by_name["order-confirmation"]["consumer_group"] == "mock-order-reactor"


def test_item_mock_http_stub_with_match(monkeypatch):
    result = _run(["--category", "mock-http-stubs", "--name", "charge-ok"], monkeypatch)
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["category"] == "mock-http-stubs"
    assert res["name"] == "charge-ok"
    assert res["method"] == "POST"
    assert res["path"] == "/v1/charge"
    assert res["response"]["status"] == 200
    assert res["response"]["body"] == {"status": "ok"}
    assert res["match"] == {"body": {"amount_cents": 1000}}
    assert "capture" not in res  # fixture stub has no capture
    assert res["delay_ms"] == 0
    # 0.0.0.0 bind normalized to localhost for a copy-pasteable URL.
    assert res["example"] == "curl -i -X POST http://localhost:18080/v1/charge"
    assert "mock run" in res["note"]


def test_item_mock_http_stub_minimal(monkeypatch):
    result = _run(["--category", "mock-http-stubs", "--name", "healthz"], monkeypatch)
    assert result.exit_code == 0
    res = _payload(result)["result"]
    assert res["name"] == "healthz"
    assert res["method"] == "GET"
    assert res["path"] == "/healthz"
    # No match/capture on this stub — those keys are omitted.
    assert "match" not in res
    assert "capture" not in res
    assert res["example"] == "curl -i -X GET http://localhost:18080/healthz"


def test_item_mock_kafka_reactor_with_match(monkeypatch):
    result = _run(
        ["--category", "mock-kafka-reactors", "--name", "order-confirmation"],
        monkeypatch,
    )
    assert result.exit_code == 0
    payload = _payload(result)
    assert payload["command"] == "discover.item"
    res = payload["result"]
    assert res["category"] == "mock-kafka-reactors"
    assert res["name"] == "order-confirmation"
    assert res["topic"] == "orders.created"
    assert res["consumer_group"] == "mock-order-reactor"
    assert res["match"] == '.value.eventType == "ORDER_CREATED"'
    assert res["reaction"] == {
        "topic": "orders.confirmed",
        "value": {"event": "ORDER_CONFIRMED"},
    }
    assert res["example"].startswith("agctl kafka produce --topic orders.created")
    assert "orders.confirmed" in res["example"]
    assert "mock run" in res["note"]


def test_item_mock_kafka_reactor_no_match(monkeypatch):
    result = _run(
        ["--category", "mock-kafka-reactors", "--name", "payment-audit"], monkeypatch
    )
    assert result.exit_code == 0
    res = _payload(result)["result"]
    assert res["name"] == "payment-audit"
    assert res["topic"] == "payments.events"
    # No match on this reactor — omitted.
    assert "match" not in res
    assert res["reaction"]["topic"] == "audit.events"


def test_item_mock_http_stub_unknown(monkeypatch):
    result = _run(["--category", "mock-http-stubs", "--name", "nope"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "TemplateNotFound"


def test_item_mock_kafka_reactor_unknown(monkeypatch):
    result = _run(["--category", "mock-kafka-reactors", "--name", "nope"], monkeypatch)
    assert result.exit_code == 2
    payload = _payload(result)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "TemplateNotFound"


def test_search_finds_mock_http_stub(monkeypatch):
    result = _run(["--search", "charge"], monkeypatch)
    assert result.exit_code == 0
    res = _payload(result)["result"]
    by_key = {(m["category"], m["name"]) for m in res["matches"]}
    assert ("mock-http-stubs", "charge-ok") in by_key


def test_search_finds_mock_kafka_reactor(monkeypatch):
    # "audit" matches the payment-audit reactor (name + description).
    result = _run(["--search", "audit"], monkeypatch)
    assert result.exit_code == 0
    res = _payload(result)["result"]
    by_key = {(m["category"], m["name"]) for m in res["matches"]}
    assert ("mock-kafka-reactors", "payment-audit") in by_key


_NO_MOCKS_CONFIG = (
    'version: "2"\n'
    "services:\n"
    "  demo:\n"
    '    base_url: "http://localhost:9999"\n'
)


def test_summary_absent_mocks_counts_zero(tmp_path, monkeypatch):
    result = _run_with([], _NO_MOCKS_CONFIG, tmp_path, monkeypatch)
    assert result.exit_code == 0
    counts = _payload(result)["result"]
    assert counts["mock_http_stubs"] == 0
    assert counts["mock_kafka_reactors"] == 0


def test_category_mock_http_stubs_absent_is_empty(tmp_path, monkeypatch):
    result = _run_with(["--category", "mock-http-stubs"], _NO_MOCKS_CONFIG, tmp_path, monkeypatch)
    assert result.exit_code == 0
    res = _payload(result)["result"]
    assert res["count"] == 0
    assert res["items"] == []


_CAPTURE_CONFIG = (
    'version: "2"\n'
    "services:\n"
    "  demo:\n"
    '    base_url: "http://localhost:9999"\n'
    "mocks:\n"
    "  http:\n"
    "    stubs:\n"
    "      echo-ctx:\n"
    '        description: "Echo the request ctx"\n'
    "        method: POST\n"
    "        path: /echo\n"
    "        capture:\n"
    "          ctx:\n"
    "            from: .body.ctx\n"
    "            type: object\n"
    "        response:\n"
    "          status: 200\n"
    "          body:\n"
    '            context: "{ctx}"\n'
)


def test_item_mock_http_stub_with_capture(tmp_path, monkeypatch):
    """Capture branch: a stub with a capture surfaces it under ``capture``
    with the YAML-facing ``from`` alias preserved."""
    result = _run_with(
        ["--category", "mock-http-stubs", "--name", "echo-ctx"],
        _CAPTURE_CONFIG,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    res = _payload(result)["result"]
    assert res["name"] == "echo-ctx"
    assert res["capture"] == {"ctx": {"from": ".body.ctx", "type": "object"}}


_IPV6_CONFIG = (
    'version: "2"\n'
    "services:\n"
    "  demo:\n"
    '    base_url: "http://localhost:9999"\n'
    "mocks:\n"
    "  http:\n"
    '    listen: "[::1]:19090"\n'
    "    stubs:\n"
    "      ping:\n"
    "        method: GET\n"
    "        path: /ping\n"
    "        response:\n"
    "          status: 204\n"
)


def test_item_mock_http_stub_ipv6_listen(tmp_path, monkeypatch):
    """A bracketed IPv6 listen address yields a valid bracketed example URL."""
    result = _run_with(
        ["--category", "mock-http-stubs", "--name", "ping"],
        _IPV6_CONFIG,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    res = _payload(result)["result"]
    assert res["example"] == "curl -i -X GET http://[::1]:19090/ping"


def test_item_mock_http_stub_ipv6_wildcard_normalized(tmp_path, monkeypatch):
    """The IPv6 wildcard ``[::]`` is normalized to localhost, mirroring ``0.0.0.0``."""
    config_yaml = _IPV6_CONFIG.replace('listen: "[::1]:19090"', 'listen: "[::]:19090"')
    result = _run_with(
        ["--category", "mock-http-stubs", "--name", "ping"],
        config_yaml,
        tmp_path,
        monkeypatch,
    )
    assert result.exit_code == 0
    res = _payload(result)["result"]
    assert res["example"] == "curl -i -X GET http://localhost:19090/ping"


# --------------------------------------------------------------------------- #
# Task 6: overlay support
# --------------------------------------------------------------------------- #


_BASE_CONFIG = (
    'version: "2"\n'
    "services:\n"
    "  demo:\n"
    '    base_url: "http://localhost:9999"\n'
    "templates:\n"
    "  base-template:\n"
    '    description: "Base template"\n'
    "    method: GET\n"
    "    service: demo\n"
    "    path: /base\n"
    "mocks:\n"
    "  http:\n"
    "    listen: \"0.0.0.0:18080\"\n"
    "    stubs:\n"
    "      base-stub:\n"
    "        description: \"Base stub\"\n"
    "        method: GET\n"
    "        path: /base\n"
    "        response:\n"
    "          status: 200\n"
)


_OVERLAY_CONFIG = (
    'version: "2"\n'
    "templates:\n"
    "  overlay-template:\n"
    '    description: "Overlay template"\n'
    "    method: POST\n"
    "    service: demo\n"
    "    path: /overlay\n"
    "    body:\n"
    "      foo: bar\n"
    "mocks:\n"
    "  http:\n"
    "    listen: \"0.0.0.0:18080\"\n"
    "    stubs:\n"
    "      overlay-stub:\n"
    "        description: \"Overlay stub\"\n"
    "        method: POST\n"
    "        path: /overlay\n"
    "        response:\n"
    "          status: 201\n"
    "          body:\n"
    "            created: true\n"
)


def _run_with_overlay(args, base_config_yaml, overlay_config_yaml, tmp_path, monkeypatch):
    """Run discover against a base config + overlay config written to ``tmp_path``."""
    base_file = tmp_path / "base.yaml"
    base_file.write_text(base_config_yaml)

    overlay_file = tmp_path / "overlay.yaml"
    overlay_file.write_text(overlay_config_yaml)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--config", str(base_file), "--overlay", str(overlay_file), "discover", *args],
        env=ENV,
        standalone_mode=False,
    )
    return result


def test_discover_overlay_composed_category_listing(tmp_path, monkeypatch):
    """Task 6 Test 1: discover --category http-templates with --overlay shows both base and overlay items."""
    result = _run_with_overlay(
        ["--category", "http-templates"],
        _BASE_CONFIG,
        _OVERLAY_CONFIG,
        tmp_path,
        monkeypatch,
    )

    # The test should fail initially since --overlay is not implemented
    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "http-templates"
    assert res["count"] == 2
    names = {item["name"] for item in res["items"]}
    assert names == {"base-template", "overlay-template"}


def test_discover_overlay_mock_stubs_visible(tmp_path, monkeypatch):
    """Task 6 Test 2: discover --category mock-http-stubs with --overlay includes overlay-added stub."""
    result = _run_with_overlay(
        ["--category", "mock-http-stubs"],
        _BASE_CONFIG,
        _OVERLAY_CONFIG,
        tmp_path,
        monkeypatch,
    )

    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "mock-http-stubs"
    assert res["count"] == 2
    names = {item["name"] for item in res["items"]}
    assert names == {"base-stub", "overlay-stub"}


def test_discover_overlay_name_resolves_overlay_only_item(tmp_path, monkeypatch):
    """Task 6 Test 3: discover --category http-templates --name overlay-template with --overlay returns full detail."""
    result = _run_with_overlay(
        ["--category", "http-templates", "--name", "overlay-template"],
        _BASE_CONFIG,
        _OVERLAY_CONFIG,
        tmp_path,
        monkeypatch,
    )

    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "http-templates"
    assert res["name"] == "overlay-template"
    assert res["method"] == "POST"
    assert res["path"] == "/overlay"
    assert res["service"] == "demo"
    assert "params" in res
    assert "example" in res


def test_discover_without_overlay_base_only(tmp_path, monkeypatch):
    """Task 6 Test 4: discover --category http-templates without --overlay shows only base items."""
    result = _run_with(
        ["--category", "http-templates"],
        _BASE_CONFIG,
        tmp_path,
        monkeypatch,
    )

    assert result.exit_code == 0
    payload = _payload(result)
    res = payload["result"]
    assert res["category"] == "http-templates"
    assert res["count"] == 1
    names = {item["name"] for item in res["items"]}
    assert names == {"base-template"}
