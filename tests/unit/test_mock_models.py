"""Tests for mock server config models (Task 1)."""

import pytest
from pydantic import ValidationError

from agctl.config.models import (
    Config,
    HttpMatch,
    HttpMockConfig,
    HttpResponse,
    HttpStub,
    KafkaMockConfig,
    KafkaReaction,
    KafkaReactor,
    MocksConfig,
    parse_listen,
)


def test_mocks_config_empty():
    """MocksConfig() with no data -> both http and kafka are None."""
    mocks = MocksConfig()
    assert mocks.http is None
    assert mocks.kafka is None


def test_config_with_empty_mocks():
    """A Config(version="1", mocks={}) validates (mocks present but empty is allowed)."""
    cfg = Config.model_validate({"version": "1", "mocks": {}})
    assert cfg.mocks is not None
    assert cfg.mocks.http is None
    assert cfg.mocks.kafka is None


def test_http_stub_method_uppercase():
    """HttpStub(method="post", path="/x", response={"status": 201}) -> method == "POST", response.status == 201."""
    stub = HttpStub(
        method="post", path="/x", response={"status": 201, "body": {"order_id": "123"}}
    )
    assert stub.method == "POST"
    assert stub.response.status == 201


def test_http_response_status_bounds():
    """HttpResponse(status=99) and HttpResponse(status=600) -> both raise ValidationError."""
    with pytest.raises(ValidationError):
        HttpResponse(status=99)
    with pytest.raises(ValidationError):
        HttpResponse(status=600)


def test_http_response_status_599_ok():
    """HttpResponse(status=599) -> ok."""
    resp = HttpResponse(status=599)
    assert resp.status == 599


def test_http_response_defaults():
    """HttpResponse() -> status == 200, body is None."""
    resp = HttpResponse()
    assert resp.status == 200
    assert resp.body is None


def test_kafka_reaction_json_value():
    """KafkaReaction(topic="t", value={"a":1}) ok."""
    reaction = KafkaReaction(topic="t", value={"a": 1})
    assert reaction.topic == "t"
    assert reaction.value == {"a": 1}


def test_kafka_reaction_scalar_value():
    """KafkaReaction(topic="t", value=42) ok (JSON-serializable scalar)."""
    reaction = KafkaReaction(topic="t", value=42)
    assert reaction.topic == "t"
    assert reaction.value == 42


def test_kafka_reaction_object_value():
    """KafkaReaction(topic="t", value=object()) is fine at model time (JSON-encoding failure surfaces later)."""
    # This should NOT raise a ValidationError at model time
    reaction = KafkaReaction(topic="t", value=object())
    assert reaction.topic == "t"
    # The value itself is an object instance - JSON encoding will fail later


def test_kafka_reaction_headers_non_string_rejected():
    """KafkaReaction(topic="t", value=1, headers={"x": 5}) -> ValidationError (non-string header value rejected)."""
    with pytest.raises(ValidationError):
        KafkaReaction(topic="t", value=1, headers={"x": 5})


def test_parse_listen_valid():
    """parse_listen("0.0.0.0:18080") == ("0.0.0.0", 18080); parse_listen("[::1]:18080") == ("::1", 18080)."""
    assert parse_listen("0.0.0.0:18080") == ("0.0.0.0", 18080)
    assert parse_listen("[::1]:18080") == ("::1", 18080)


def test_parse_listen_invalid():
    """parse_listen("host"), parse_listen("host:abc"), parse_listen("") each raise ValueError."""
    with pytest.raises(ValueError, match="port"):
        parse_listen("host")
    with pytest.raises(ValueError, match="port"):
        parse_listen("host:abc")
    with pytest.raises(ValueError, match="empty|parse"):
        parse_listen("")


def test_parse_listen_rejects_unbracketed_ipv6():
    """Unbracketed IPv6 (host contains ':') → ValueError per spec §7.2 (must be bracketed)."""
    with pytest.raises(ValueError, match="IPv6|bracket"):
        parse_listen("::1:8080")
    with pytest.raises(ValueError, match="IPv6|bracket"):
        parse_listen("::1")


def test_parse_listen_rejects_port_out_of_range():
    """Out-of-range ports → ValueError; port 0 (ephemeral) is allowed."""
    with pytest.raises(ValueError, match="range|0-65535"):
        parse_listen("0.0.0.0:99999")
    with pytest.raises(ValueError, match="range|0-65535"):
        parse_listen("0.0.0.0:-1")
    # Port 0 is valid (ephemeral bind — the engine reports the OS-assigned port).
    assert parse_listen("0.0.0.0:0") == ("0.0.0.0", 0)


def test_http_mock_config_listen_invalid():
    """HttpMockConfig(listen="0.0.0.0:notaport") -> ValidationError (the listen validator runs parse_listen)."""
    with pytest.raises(ValidationError):
        HttpMockConfig(listen="0.0.0.0:notaport")


def test_end_to_end_via_load_config():
    """End-to-end via load_config: a YAML string containing the spec §7.1 mocks: block parses into a Config."""
    # Simulating a parsed YAML dict with ${...} already resolved to literals
    config_dict = {
        "version": "1",
        "mocks": {
            "http": {
                "listen": "0.0.0.0:18080",
                "stubs": {
                    "create-order": {
                        "description": "Create order mock",
                        "method": "POST",
                        "path": "/orders",
                        "match": {"body": {"amount": 100}},
                        "response": {
                            "status": 201,
                            "headers": {"Content-Type": "application/json"},
                            "body": {"order_id": "{customer_id}-mock"},
                        },
                        "delay_ms": 10,
                    }
                },
            },
            "kafka": {
                "reactors": {
                    "order-created": {
                        "description": "React to order-created events",
                        "topic": "orders.created",
                        "consumer_group": "mock-reactor",
                        "match": '.eventType == "ORDER_CREATED"',
                        "reaction": {
                            "topic": "orders.mock",
                            "key": "mock-key",
                            "value": {"mock": True},
                            "headers": {"source": "mock-server"},
                        },
                    }
                }
            },
        },
    }

    cfg = Config.model_validate(config_dict)
    assert cfg.mocks is not None
    assert cfg.mocks.http is not None
    assert cfg.mocks.http.listen == "0.0.0.0:18080"
    assert cfg.mocks.http.stubs["create-order"].method == "POST"
    assert cfg.mocks.http.stubs["create-order"].path == "/orders"
    assert cfg.mocks.http.stubs["create-order"].response.status == 201
    assert cfg.mocks.http.stubs["create-order"].response.body["order_id"] == "{customer_id}-mock"
    assert cfg.mocks.http.stubs["create-order"].delay_ms == 10

    assert cfg.mocks.kafka is not None
    assert cfg.mocks.kafka.reactors["order-created"].topic == "orders.created"
    assert cfg.mocks.kafka.reactors["order-created"].consumer_group == "mock-reactor"
    assert cfg.mocks.kafka.reactors["order-created"].reaction.topic == "orders.mock"
    assert cfg.mocks.kafka.reactors["order-created"].reaction.value == {"mock": True}
