"""Unit tests for the shared gRPC proto kernel (clients/grpc_descriptors.py).

The kernel is the single source of truth shared by the gRPC client and the
upcoming gRPC mock server: descriptor resolution, service/method lookup, call
type detection, and JSON<->protobuf (de)serialization. Pure functions over
``DescriptorPool`` / ``MessageDescriptor``; no grpcio dependency.

The whole module is gated on the ``grpc`` extra because every test exercises
``grpc_tools`` / ``google.protobuf``. Mirror the gating pattern in
``test_grpc_client.py`` so the unit suite stays collectable without extras.
"""

import pathlib

import pytest

# Module-level skip: preserves the project invariant that the unit suite is
# collectable without optional extras installed.
pytest.importorskip("grpc_tools")
pytest.importorskip("google.protobuf")

from agctl.clients import grpc_descriptors
from agctl.config.models import GrpcDescriptorSource
from agctl.errors import ConfigError, TemplateNotFound

SERVICE = "echo.EchoService"


# --- find_method / call_type_of --------------------------------------------


def test_find_method_unary(mock_grpc_echo_pool):
    md = grpc_descriptors.find_method(mock_grpc_echo_pool, SERVICE, "Unary")
    assert grpc_descriptors.call_type_of(md) == "unary"


def test_find_method_server_stream(mock_grpc_echo_pool):
    md = grpc_descriptors.find_method(mock_grpc_echo_pool, SERVICE, "ServerStream")
    assert grpc_descriptors.call_type_of(md) == "server_stream"


def test_find_method_client_stream(mock_grpc_echo_pool):
    md = grpc_descriptors.find_method(mock_grpc_echo_pool, SERVICE, "ClientStream")
    assert grpc_descriptors.call_type_of(md) == "client_stream"


def test_find_method_bidi(mock_grpc_echo_pool):
    md = grpc_descriptors.find_method(mock_grpc_echo_pool, SERVICE, "Bidi")
    assert grpc_descriptors.call_type_of(md) == "bidi"


def test_find_method_unknown_method_raises_template_not_found(mock_grpc_echo_pool):
    with pytest.raises(TemplateNotFound) as exc_info:
        grpc_descriptors.find_method(mock_grpc_echo_pool, SERVICE, "Missing")
    assert exc_info.value.detail.get("method") == "Missing"
    assert exc_info.value.detail.get("service") == SERVICE


def test_find_method_unknown_service_raises_template_not_found(mock_grpc_echo_pool):
    with pytest.raises(TemplateNotFound) as exc_info:
        grpc_descriptors.find_method(mock_grpc_echo_pool, "echo.Missing", "Unary")
    assert exc_info.value.detail.get("service") == "echo.Missing"


# --- build_descriptor_pool --------------------------------------------------


def test_build_descriptor_pool_empty_raises_configerror_mentions_label():
    with pytest.raises(ConfigError) as exc_info:
        grpc_descriptors.build_descriptor_pool([], context_label="x")
    # context_label must appear in the message so callers can identify the source
    assert "x" in str(exc_info.value)


def test_build_descriptor_pool_empty_does_not_require_grpc_extra(monkeypatch):
    """Empty-sources validation runs before any lazy import (input gate first).

    Ensures callers get a context-labeled ConfigError even when the gRPC extra
    is missing — the error names the source, not just the missing library.
    """
    import importlib

    real_import = importlib.import_module

    def blocking_import(name, *args, **kwargs):
        if name.startswith(("grpc_tools", "google.protobuf")):
            raise ImportError(f"simulated-missing: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", blocking_import)
    with pytest.raises(ConfigError) as exc_info:
        grpc_descriptors.build_descriptor_pool([], context_label="my-target")
    assert "my-target" in str(exc_info.value)
    assert "grpc.descriptors" in str(exc_info.value)


def test_build_descriptor_pool_from_proto(mock_grpc_echo_proto_path):
    """build_descriptor_pool compiles a .proto via GrpcDescriptorSource.proto."""
    pool = grpc_descriptors.build_descriptor_pool(
        [GrpcDescriptorSource(proto=str(mock_grpc_echo_proto_path))],
        context_label="test",
    )
    md = grpc_descriptors.find_method(pool, SERVICE, "Unary")
    assert grpc_descriptors.call_type_of(md) == "unary"


def test_build_descriptor_pool_from_descriptor_set(tmp_path, mock_grpc_echo_proto_path):
    """build_descriptor_pool loads a pre-compiled descriptor_set .pb file."""
    from google.protobuf import descriptor_pb2
    from grpc_tools import protoc

    descriptor_set_path = tmp_path / "echo.pb"
    rc = protoc.main(
        [
            "protoc",
            "--include_imports",
            "--proto_path",
            str(mock_grpc_echo_proto_path.parent),
            "--descriptor_set_out",
            str(descriptor_set_path),
            str(mock_grpc_echo_proto_path),
        ]
    )
    assert rc == 0
    assert descriptor_set_path.exists()

    pool = grpc_descriptors.build_descriptor_pool(
        [GrpcDescriptorSource(descriptor_set=str(descriptor_set_path))],
        context_label="test",
    )
    md = grpc_descriptors.find_method(pool, SERVICE, "Bidi")
    assert grpc_descriptors.call_type_of(md) == "bidi"


# --- serialize / deserialize -----------------------------------------------


def test_serialize_deserialize_round_trip_is_stable(mock_grpc_echo_pool):
    """Round-trip dict -> bytes -> dict and bytes -> dict -> bytes is stable."""
    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName("echo.EchoRequest")
    ser = grpc_descriptors.serialize(msg_desc)
    deser = grpc_descriptors.deserialize(msg_desc)

    payload = {"msg": "hi"}
    encoded = ser(payload)
    assert isinstance(encoded, bytes)

    decoded = deser(encoded)
    # ``deserialize`` preserves default-valued scalars (Fix 1): ``n`` defaults
    # to 0 and is now PRESENT in the envelope (always_print_fields_with_no_presence).
    assert decoded == {"msg": "hi", "n": 0}

    # bytes -> dict -> bytes is stable
    re_encoded = ser(decoded)
    assert re_encoded == encoded


def test_serialize_bytes_passthrough(mock_grpc_echo_pool):
    """Bytes input is returned unchanged (no re-serialization)."""
    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName("echo.EchoRequest")
    ser = grpc_descriptors.serialize(msg_desc)
    raw = b"\x0a\x02hi"  # field 1 (msg), len 2, "hi"
    assert ser(raw) is raw


def test_serialize_unknown_field_raises(mock_grpc_echo_pool):
    """ignore_unknown_fields=False: unknown field surfaces as ParseError."""
    from google.protobuf.json_format import ParseError

    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName("echo.EchoRequest")
    ser = grpc_descriptors.serialize(msg_desc)
    with pytest.raises(ParseError):
        ser({"nope": 1})


def test_message_class_returns_protobuf_class(mock_grpc_echo_pool):
    """message_class returns a concrete protobuf message class."""
    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName("echo.EchoRequest")
    cls = grpc_descriptors.message_class(msg_desc)
    instance = cls()
    instance.msg = "hello"
    assert instance.SerializeToString() == b"\x0a\x05hello"


# --- deserialize: snake_case + default-preserving contract (Fix 1) -----------
#
# ``MessageToDict`` defaults to lowerCamelCase + dropping scalar defaults, which
# silently broke ``match.body`` (a stub authored ``user_id`` never matched) and
# ``capture.from`` (``.message.user_id`` resolved to null). The fix passes
# ``preserving_proto_field_name=True`` + ``always_print_fields_with_no_presence=True``.
# These tests use ``echo.UserService/UserRequest`` (a snake_case ``user_id`` field
# + a scalar int ``n``) added to echo.proto for exactly this regression.


USER_REQUEST = "echo.UserRequest"
USER_SERVICE = "echo.UserService"


def test_deserialize_preserves_snake_case_field_names(mock_grpc_echo_pool):
    """(Fix 1) ``user_id`` round-trips as ``user_id`` — NOT lowerCamelCase ``userId``.

    Without ``preserving_proto_field_name=True`` the deserialized dict would key
    the field as ``userId``, silently breaking ``match.body``/``capture.from``
    authored against the proto's field names.
    """
    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName(USER_REQUEST)
    ser = grpc_descriptors.serialize(msg_desc)
    deser = grpc_descriptors.deserialize(msg_desc)

    encoded = ser({"user_id": "u-123", "n": 7})
    decoded = deser(encoded)
    assert decoded["user_id"] == "u-123", (
        "snake_case field must be preserved (would be 'userId' without the fix)"
    )
    assert "userId" not in decoded


def test_deserialize_preserves_zero_valued_int(mock_grpc_echo_pool):
    """(Fix 1) a scalar int equal to its default (n=0) is PRESENT in the envelope.

    Without ``always_print_fields_with_no_presence=True`` the zero-valued ``n``
    is dropped, so a ``match.body: {n: 0}`` stub never matches it.
    """
    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName(USER_REQUEST)
    ser = grpc_descriptors.serialize(msg_desc)
    deser = grpc_descriptors.deserialize(msg_desc)

    # n omitted on the wire == default 0; deserializer must surface it.
    encoded = ser({"user_id": "u-1"})
    decoded = deser(encoded)
    assert decoded == {"user_id": "u-1", "n": 0}, (
        "zero-valued int must be present (would be dropped without the fix)"
    )

    # Explicit n=0 round-trips identically.
    encoded0 = ser({"user_id": "u-2", "n": 0})
    assert deser(encoded0) == {"user_id": "u-2", "n": 0}


def test_deserialize_envelope_is_matchable_on_snake_case_and_zero_int(
    mock_grpc_echo_pool,
):
    """(Fix 1, (a)+(c)) a deserialized request is matchable by ``match.body``
    using the snake_case key AND a zero-valued int.

    Proves the full match path: serialize -> deserialize -> json_subset (the
    predicate ``match.body`` uses). On the pre-fix dict (``{userId: ...}``,
    ``n`` dropped) BOTH subset checks would fail — a silent correctness miss.
    """
    from agctl.assertions import json_subset

    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName(USER_REQUEST)
    ser = grpc_descriptors.serialize(msg_desc)
    deser = grpc_descriptors.deserialize(msg_desc)

    decoded = deser(ser({"user_id": "u-42", "n": 0}))

    # (a) snake_case key subset-matches the deserialized message.
    assert json_subset({"user_id": "u-42"}, decoded)
    # (c) a zero-valued int field is present and matchable.
    assert json_subset({"n": 0}, decoded)
    # Both together still subset-match.
    assert json_subset({"user_id": "u-42", "n": 0}, decoded)


def test_deserialize_capture_resolves_snake_case_via_dispatch(mock_grpc_echo_pool):
    """(Fix 1, (b)) ``capture.from: .message.user_id`` resolves off a REAL
    deserialized message (not a hand-built dict).

    Builds the envelope ``message`` from the deserializer's output, then runs
    ``dispatch_grpc`` with a capture rooted at the snake_case field. On the
    pre-fix lowerCamelCase dict the capture resolves to null → ``capture.missing``
    + empty substitution. With the fix the capture resolves and the rendered
    response substitutes the captured value.
    """
    pytest.importorskip("jq")
    from agctl.config.models import CaptureSpec, GrpcResponse, GrpcStub
    from agctl.mock.grpc_server import build_envelope, dispatch_grpc

    msg_desc = mock_grpc_echo_pool.FindMessageTypeByName(USER_REQUEST)
    ser = grpc_descriptors.serialize(msg_desc)
    deser = grpc_descriptors.deserialize(msg_desc)

    # Real deserialize path produces the envelope message.
    decoded_message = deser(ser({"user_id": "u-99", "n": 3}))

    stub = GrpcStub(
        service=USER_SERVICE,
        method="GetUser",
        capture={"uid": CaptureSpec(from_=".message.user_id")},
        response=GrpcResponse(message={"echo": "got-{uid}"}),
    )
    env = build_envelope(USER_SERVICE, "GetUser", {}, message=decoded_message)
    missing: list[tuple[str, str]] = []

    def cb(stub_name, cap_name, from_path):
        missing.append((cap_name, from_path))

    outcome = dispatch_grpc({"u": stub}, env, "unary", emit_capture_missing=cb)

    assert outcome.matched is True
    assert missing == [], "capture.from .message.user_id must resolve (non-null)"
    assert outcome.messages == [{"echo": "got-u-99"}]

