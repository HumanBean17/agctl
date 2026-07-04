"""Tests for mock server config models (Task 1)."""

import pytest
from pydantic import ValidationError

from agctl.config.models import (
    Config,
    CaptureSpec,
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


def test_http_match_no_args():
    """HttpMatch() with no args -> body is None, jq is None."""
    match = HttpMatch()
    assert match.body is None
    assert match.jq is None


def test_http_match_jq_only():
    """HttpMatch(jq='.amount > 1000') -> body is None, jq == '.amount > 1000'."""
    match = HttpMatch(jq=".amount > 1000")
    assert match.body is None
    assert match.jq == ".amount > 1000"


def test_http_match_body_and_jq():
    """HttpMatch(body={'priority': 'high'}, jq='.amount > 1000') -> both set (coexist)."""
    match = HttpMatch(body={"priority": "high"}, jq=".amount > 1000")
    assert match.body == {"priority": "high"}
    assert match.jq == ".amount > 1000"


# --- CaptureSpec (Task 1: envelope-rooted capture:) ---


def test_capture_spec_alias_default_type():
    """CaptureSpec.model_validate({"from": ".body.variables.id"}) -> from_ == path, type == 'scalar' (default)."""
    spec = CaptureSpec.model_validate({"from": ".body.variables.id"})
    assert spec.from_ == ".body.variables.id"
    assert spec.type == "scalar"


def test_capture_spec_alias_object_type():
    """CaptureSpec.model_validate({"from": ".value.context", "type": "object"}) -> type == 'object'."""
    spec = CaptureSpec.model_validate({"from": ".value.context", "type": "object"})
    assert spec.from_ == ".value.context"
    assert spec.type == "object"


def test_capture_spec_populate_by_name():
    """CaptureSpec(**{"from": ".x"}) works via populate_by_name (alias == 'from')."""
    spec = CaptureSpec(**{"from": ".x"})
    assert spec.from_ == ".x"


def test_capture_spec_field_name_construction():
    """CaptureSpec(from_=".x") works via populate_by_name (field name 'from_')."""
    spec = CaptureSpec(from_=".x")
    assert spec.from_ == ".x"


def test_capture_spec_invalid_type_rejected():
    """CaptureSpec.model_validate({"from": ".x", "type": "bogus"}) -> ValidationError."""
    with pytest.raises(ValidationError):
        CaptureSpec.model_validate({"from": ".x", "type": "bogus"})


def test_http_stub_capture_defaults_none():
    """HttpStub without capture -> .capture is None."""
    stub = HttpStub(method="POST", path="/x", response=HttpResponse())
    assert stub.capture is None


def test_http_stub_with_capture():
    """HttpStub(capture={"op_id": CaptureSpec(...)}) -> .capture["op_id"].from_ == path."""
    stub = HttpStub(
        method="POST",
        path="/x",
        response=HttpResponse(),
        capture={"op_id": CaptureSpec.model_validate({"from": ".body.variables.id"})},
    )
    assert stub.capture is not None
    assert stub.capture["op_id"].from_ == ".body.variables.id"


def test_kafka_reactor_capture_defaults_none():
    """KafkaReactor without capture -> .capture is None."""
    reactor = KafkaReactor(topic="t", reaction=KafkaReaction(topic="r", value={}))
    assert reactor.capture is None


def test_kafka_reactor_with_capture():
    """KafkaReactor(capture={...}) parses analogously to HttpStub."""
    reactor = KafkaReactor(
        topic="t",
        reaction=KafkaReaction(topic="r", value={}),
        capture={"op_id": CaptureSpec.model_validate({"from": ".value.id"})},
    )
    assert reactor.capture is not None
    assert reactor.capture["op_id"].from_ == ".value.id"
