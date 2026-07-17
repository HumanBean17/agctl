"""Tests for mock server config models (Task 1, Task 3)."""

import pytest
from pydantic import ValidationError

from agctl.config.models import (
    Config,
    CaptureSpec,
    GrpcMatch,
    GrpcMockConfig,
    GrpcResponse,
    GrpcResponseMessage,
    GrpcStub,
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


# --- gRPC mock models (Task 3: mocks.grpc structural config) ---
#
# Offline structural validation only: exactly-one-of message/messages, status
# validity via parse_grpc_status, listen parsing, concurrency_cap >= 1.
# Response-shape-vs-call-type (e.g. `messages` on a unary method) is deferred to
# Task 6 (needs the descriptor pool) and intentionally NOT exercised here.


def test_grpc_mock_config_defaults():
    """GrpcMockConfig() -> listen=='0.0.0.0:50051', reflection is True, health is True, concurrency_cap==64, stubs empty."""
    cfg = GrpcMockConfig()
    assert cfg.listen == "0.0.0.0:50051"
    assert cfg.reflection is True
    assert cfg.health is True
    assert cfg.concurrency_cap == 64
    assert cfg.stubs == {}


def test_grpc_mock_config_listen_accepts_ipv4_and_ipv6():
    """listen='0.0.0.0:50051' and '[::1]:50051' both parse (delegate to parse_listen)."""
    cfg4 = GrpcMockConfig(listen="0.0.0.0:50051")
    assert cfg4.listen == "0.0.0.0:50051"
    cfg6 = GrpcMockConfig(listen="[::1]:50051")
    assert cfg6.listen == "[::1]:50051"


def test_grpc_mock_config_listen_rejects_noport():
    """listen='noport' -> ValidationError (mirrors HttpMockConfig.listen validator)."""
    with pytest.raises(ValidationError):
        GrpcMockConfig(listen="noport")


def test_grpc_mock_config_concurrency_cap_zero_rejected():
    """concurrency_cap=0 -> ValidationError (must be >= 1)."""
    with pytest.raises(ValidationError):
        GrpcMockConfig(concurrency_cap=0)


def test_grpc_mock_config_concurrency_cap_one_ok():
    """concurrency_cap=1 -> ok (boundary)."""
    cfg = GrpcMockConfig(concurrency_cap=1)
    assert cfg.concurrency_cap == 1


def test_grpc_mock_config_full_stub_parse():
    """GrpcMockConfig parses a full stub: service/method/match/capture/response.message/status/metadata."""
    cfg = GrpcMockConfig(
        listen="0.0.0.0:50051",
        reflection=True,
        health=True,
        concurrency_cap=8,
        stubs={
            "get-order": {
                "description": "mock GetOrder",
                "service": "shop.OrderService",
                "method": "GetOrder",
                "match": {"body": {"order_id": "123"}, "jq": ".order_id == \"123\""},
                "capture": {"op_id": {"from": ".body.order_id"}},
                "response": {
                    "status": "OK",
                    "message": {"order_id": "123", "total": 999},
                    "metadata": {"x-trace-id": "abc"},
                },
                "delay_ms": 5,
            }
        },
    )
    stub = cfg.stubs["get-order"]
    assert stub.service == "shop.OrderService"
    assert stub.method == "GetOrder"
    assert stub.description == "mock GetOrder"
    assert stub.match is not None
    assert stub.match.body == {"order_id": "123"}
    assert stub.match.jq == '.order_id == "123"'
    assert stub.capture is not None
    assert stub.capture["op_id"].from_ == ".body.order_id"
    assert stub.response.status == "OK"
    assert stub.response.message == {"order_id": "123", "total": 999}
    assert stub.response.messages is None
    assert stub.response.metadata == {"x-trace-id": "abc"}
    assert stub.delay_ms == 5


def test_grpc_match_defaults():
    """GrpcMatch() -> body is None, jq is None."""
    match = GrpcMatch()
    assert match.body is None
    assert match.jq is None


def test_grpc_response_message_defaults():
    """GrpcResponseMessage(message={...}) -> delay_ms==0."""
    msg = GrpcResponseMessage(message={"a": 1})
    assert msg.message == {"a": 1}
    assert msg.delay_ms == 0


def test_grpc_response_message_delay_negative_rejected():
    """GrpcResponseMessage(message=..., delay_ms=-1) -> ValidationError."""
    with pytest.raises(ValidationError):
        GrpcResponseMessage(message={"a": 1}, delay_ms=-1)


def test_grpc_response_default_status_ok():
    """GrpcResponse(message={...}) -> status=='OK' default, metadata None."""
    resp = GrpcResponse(message={"a": 1})
    assert resp.status == "OK"
    assert resp.message == {"a": 1}
    assert resp.messages is None
    assert resp.metadata is None


def test_grpc_response_message_alone_ok():
    """GrpcResponse(message={...}) -> ok (exactly-one-of satisfied)."""
    resp = GrpcResponse(message={"a": 1})
    assert resp.message == {"a": 1}
    assert resp.messages is None


def test_grpc_response_messages_alone_ok():
    """GrpcResponse(messages=[...]) -> ok (exactly-one-of satisfied)."""
    resp = GrpcResponse(
        messages=[
            GrpcResponseMessage(message={"chunk": 1}, delay_ms=10),
            GrpcResponseMessage(message={"chunk": 2}),
        ]
    )
    assert resp.message is None
    assert resp.messages is not None
    assert len(resp.messages) == 2
    assert resp.messages[0].message == {"chunk": 1}
    assert resp.messages[0].delay_ms == 10
    assert resp.messages[1].delay_ms == 0


def test_grpc_response_rejects_both_message_and_messages():
    """Both message and messages set -> ValidationError."""
    with pytest.raises(ValidationError):
        GrpcResponse(
            message={"a": 1},
            messages=[GrpcResponseMessage(message={"chunk": 1})],
        )


def test_grpc_response_rejects_neither_message_nor_messages():
    """Neither message nor messages set -> ValidationError."""
    with pytest.raises(ValidationError):
        GrpcResponse()


def test_grpc_response_status_name_string_preserved():
    """status='NOT_FOUND' parses; the original string is preserved verbatim (code/name resolution is render-time)."""
    resp = GrpcResponse(message={"a": 1}, status="NOT_FOUND")
    assert resp.status == "NOT_FOUND"


def test_grpc_response_status_int_code_preserved():
    """status=5 parses; the original int is preserved verbatim."""
    resp = GrpcResponse(message={"a": 1}, status=5)
    assert resp.status == 5


def test_grpc_response_status_digit_string_preserved():
    """status='5' parses (digit-string coercion in parse_grpc_status); stored as the original string '5'."""
    resp = GrpcResponse(message={"a": 1}, status="5")
    assert resp.status == "5"


def test_grpc_response_status_zero_ok():
    """status=0 / 'OK' boundary -> ok (gRPC OK code is 0)."""
    assert GrpcResponse(message={"a": 1}, status=0).status == 0
    assert GrpcResponse(message={"a": 1}, status="OK").status == "OK"


def test_grpc_response_status_invalid_name_rejected():
    """status='FOO' -> ValidationError (invalid name; case-sensitive lookup)."""
    with pytest.raises(ValidationError):
        GrpcResponse(message={"a": 1}, status="FOO")


def test_grpc_response_status_out_of_range_code_rejected():
    """status=17 -> ValidationError (out of gRPC 0-16 range)."""
    with pytest.raises(ValidationError):
        GrpcResponse(message={"a": 1}, status=17)


def test_grpc_response_status_lowercase_name_rejected():
    """status='not_found' -> ValidationError (case-sensitive; caller must upper-case)."""
    with pytest.raises(ValidationError):
        GrpcResponse(message={"a": 1}, status="not_found")


def test_grpc_stub_minimal():
    """GrpcStub(service=..., method=..., response={...}) -> defaults: description/capture None, delay_ms 0."""
    stub = GrpcStub(
        service="shop.OrderService",
        method="GetOrder",
        response=GrpcResponse(message={"a": 1}),
    )
    assert stub.description is None
    assert stub.capture is None
    assert stub.delay_ms == 0
    assert stub.match is None


def test_grpc_stub_delay_negative_rejected():
    """GrpcStub(... delay_ms=-1) -> ValidationError."""
    with pytest.raises(ValidationError):
        GrpcStub(
            service="shop.OrderService",
            method="GetOrder",
            response=GrpcResponse(message={"a": 1}),
            delay_ms=-1,
        )


def test_mocks_config_grpc_only():
    """MocksConfig(grpc={...}) -> grpc set, http/kafka None."""
    mocks = MocksConfig(
        grpc={"listen": "0.0.0.0:50051", "stubs": {"s": {
            "service": "S", "method": "M", "response": {"message": {"x": 1}},
        }}}
    )
    assert mocks.grpc is not None
    assert mocks.grpc.listen == "0.0.0.0:50051"
    assert "s" in mocks.grpc.stubs
    assert mocks.http is None
    assert mocks.kafka is None


def test_mocks_config_empty_has_grpc_none():
    """MocksConfig() -> grpc is None (default)."""
    assert MocksConfig().grpc is None


def test_config_with_grpc_mocks_end_to_end():
    """End-to-end: a Config with mocks.grpc parses into typed GrpcMockConfig."""
    cfg = Config.model_validate({
        "version": "1",
        "mocks": {
            "grpc": {
                "listen": "0.0.0.0:50051",
                "reflection": True,
                "health": False,
                "concurrency_cap": 4,
                "stubs": {
                    "get-order": {
                        "service": "shop.OrderService",
                        "method": "GetOrder",
                        "response": {"status": "OK", "message": {"id": "1"}},
                    }
                },
            }
        },
    })
    assert cfg.mocks is not None
    assert cfg.mocks.grpc is not None
    assert cfg.mocks.grpc.health is False
    assert cfg.mocks.grpc.concurrency_cap == 4
    stub = cfg.mocks.grpc.stubs["get-order"]
    assert stub.method == "GetOrder"
    assert stub.response.status == "OK"
