"""Tests for GrpcClient DTOs and skeleton construction."""

import sys
from unittest.mock import patch

import pytest
# Module-level skip guards: this test suite requires grpc and protobuf extras.
# pytest.importorskip at module level causes pytest to skip the entire module
# at collection time if the extras are absent, preserving the project invariant
# that the unit suite is collectable without optional extras installed.
pytest.importorskip("grpc")
pytest.importorskip("google.protobuf")

from agctl.clients.grpc_client import (
    GrpcClient,
    GrpcHealthResult,
    GrpcStatus,
    GrpcStreamMessage,
    GrpcUnaryResult,
)
from agctl.config.models import GrpcTarget, GrpcTls
from agctl.errors import ConfigError


def test_status_and_result_dataclasses():
    """Test that all DTOs construct correctly and defaults apply."""
    # GrpcStatus with default message
    status = GrpcStatus(code=0, name="OK")
    assert status.code == 0
    assert status.name == "OK"
    assert status.message == ""  # default

    # GrpcUnaryResult with all fields
    status = GrpcStatus(code=0, name="OK", message="success")
    result = GrpcUnaryResult(
        target="localhost:50051",
        service="MyService",
        method="MyMethod",
        call_type="unary",
        status=status,
        message={"data": "test"},
        initial_metadata={"header": "value"},
        trailers={"trailer": "end"},
    )
    assert result.target == "localhost:50051"
    assert result.service == "MyService"
    assert result.method == "MyMethod"
    assert result.call_type == "unary"
    assert result.status.code == 0
    assert result.status.name == "OK"
    assert result.status.message == "success"
    assert result.message == {"data": "test"}
    assert result.initial_metadata == {"header": "value"}
    assert result.trailers == {"trailer": "end"}

    # GrpcUnaryResult with None message (allowed)
    result_no_msg = GrpcUnaryResult(
        target="localhost:50051",
        service="MyService",
        method="MyMethod",
        call_type="unary",
        status=status,
        message=None,
        initial_metadata={},
        trailers={},
    )
    assert result_no_msg.message is None

    # GrpcStreamMessage with None trailers (allowed)
    stream_msg = GrpcStreamMessage(
        message={"chunk": "data"},
        trailers=None,
    )
    assert stream_msg.message == {"chunk": "data"}
    assert stream_msg.trailers is None

    # GrpcHealthResult with default note
    health = GrpcHealthResult(
        target="localhost:50051",
        address="localhost:50051",
        status="SERVING",
    )
    assert health.target == "localhost:50051"
    assert health.address == "localhost:50051"
    assert health.status == "SERVING"
    assert health.note is None  # default


def test_client_uses_injected_channel_and_skips_grpcio_import():
    """Test that injected channel/pool are stored and grpcio is NOT imported."""
    target = GrpcTarget(address="localhost:50051")
    injected_channel = object()
    injected_pool = object()

    client = GrpcClient(
        target,
        channel=injected_channel,
        descriptor_pool=injected_pool,
        timeout_seconds=5.0,
    )

    # Should store injected objects
    assert client._channel is injected_channel
    assert client._pool is injected_pool
    assert client._target is target
    assert client._timeout == 5.0

    # Should NOT have imported grpcio (no _grpc attribute set)
    assert getattr(client, "_grpc", None) is None


def test_client_missing_extra_when_no_channel():
    """Test that missing grpcio extra raises ConfigError when no channel injected."""
    target = GrpcTarget(address="localhost:50051")

    # Mock the import of grpc by making it raise ImportError
    import builtins

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "grpc":
            raise ImportError("No module named 'grpc'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        with pytest.raises(ConfigError) as exc_info:
            GrpcClient(target)

    # Error message should mention the grpc extra
    assert "agctl[grpc]" in str(exc_info.value)


# Task 5: Descriptor resolution tests (require protobuf)
pytest.importorskip("google.protobuf")

# Import descriptor-related modules only when protobuf is available
from google.protobuf import descriptor_pool


def test_resolve_descriptors_uses_injected_pool():
    """Test that injected pool is returned as-is without reflection call."""
    target = GrpcTarget(address="localhost:50051")
    injected_channel = object()
    sentinel_pool = object()  # Sentinel to verify exact object returned

    client = GrpcClient(
        target,
        channel=injected_channel,
        descriptor_pool=sentinel_pool,
        timeout_seconds=5.0,
    )

    # Should return the injected pool without any reflection
    pool = client.resolve_descriptors()
    assert pool is sentinel_pool


def test_find_method_from_echo_descriptor():
    """Test find_method and call_type_of with echo descriptor pool."""
    # Load the echo descriptor and build a pool
    import pathlib
    from google.protobuf import descriptor_pb2

    descriptor_path = (
        pathlib.Path(__file__).parent.parent / "fixtures" / "echo_descriptor.pb"
    )
    descriptor_bytes = descriptor_path.read_bytes()

    # Parse the FileDescriptorSet
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_bytes)

    pool = descriptor_pool.DescriptorPool()
    for file_desc in file_descriptor_set.file:
        pool.Add(file_desc)

    target = GrpcTarget(address="localhost:50051", reflection="off")
    injected_channel = object()

    client = GrpcClient(
        target,
        channel=injected_channel,
        descriptor_pool=pool,
        timeout_seconds=5.0,
    )

    # Test each method and its call type
    unary_method = client.find_method("echo.Echo", "Unary")
    assert client.call_type_of(unary_method) == "unary"

    server_stream_method = client.find_method("echo.Echo", "ServerStream")
    assert client.call_type_of(server_stream_method) == "server_stream"

    client_stream_method = client.find_method("echo.Echo", "ClientStream")
    assert client.call_type_of(client_stream_method) == "client_stream"

    bidi_method = client.find_method("echo.Echo", "Bidi")
    assert client.call_type_of(bidi_method) == "bidi"


def test_find_method_unknown_service_and_method():
    """Test that unknown service/method raises TemplateNotFound."""
    # Load the echo descriptor and build a pool
    import pathlib
    from google.protobuf import descriptor_pb2

    descriptor_path = (
        pathlib.Path(__file__).parent.parent / "fixtures" / "echo_descriptor.pb"
    )
    descriptor_bytes = descriptor_path.read_bytes()

    # Parse the FileDescriptorSet
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_bytes)

    pool = descriptor_pool.DescriptorPool()
    for file_desc in file_descriptor_set.file:
        pool.Add(file_desc)

    target = GrpcTarget(address="localhost:50051", reflection="off")
    injected_channel = object()

    client = GrpcClient(
        target,
        channel=injected_channel,
        descriptor_pool=pool,
        timeout_seconds=5.0,
    )

    # Unknown service should raise TemplateNotFound with detail "service"
    from agctl.errors import TemplateNotFound

    with pytest.raises(TemplateNotFound) as exc_info:
        client.find_method("echo.Missing", "Unary")
    assert exc_info.value.detail.get("service") == "echo.Missing"

    # Unknown method should raise TemplateNotFound with detail "method"
    with pytest.raises(TemplateNotFound) as exc_info:
        client.find_method("echo.Echo", "Nope")
    assert exc_info.value.detail.get("method") == "Nope"
    assert exc_info.value.detail.get("service") == "echo.Echo"


def test_resolve_reflection_unimplemented_off_path_no_descriptors_is_configerror():
    """Test that with reflection off and no descriptors, ConfigError is raised."""
    target = GrpcTarget(address="localhost:50051", reflection="off")
    injected_channel = object()

    client = GrpcClient(
        target,
        channel=injected_channel,
        descriptor_pool=None,
        descriptors=None,
        timeout_seconds=5.0,
    )

    # Should raise ConfigError mentioning grpc.descriptors
    with pytest.raises(ConfigError) as exc_info:
        client.resolve_descriptors()
    assert "grpc.descriptors" in str(exc_info.value)


def test_resolve_descriptors_with_fake_reflection_stub():
    """Test resolve_descriptors with fake reflection stub using file_containing_symbol."""
    pytest.importorskip("grpc_reflection")

    import pathlib
    from google.protobuf import descriptor_pb2
    from grpc_reflection.v1alpha import reflection_pb2

    # Load the echo descriptor fixture
    descriptor_path = (
        pathlib.Path(__file__).parent.parent / "fixtures" / "echo_descriptor.pb"
    )
    descriptor_bytes = descriptor_path.read_bytes()

    # Parse the FileDescriptorSet
    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_bytes)

    # Create fake reflection stub that responds to file_containing_symbol requests
    class FakeReflectionStub:
        def ServerReflectionInfo(self, request_iterator):
            """Yield responses for list_services and file_containing_symbol requests."""
            for request in request_iterator:
                if request.HasField("list_services"):
                    # Respond with the echo.Echo service
                    list_response = reflection_pb2.ServerReflectionResponse(
                        list_services_response=reflection_pb2.ListServiceResponse(
                            service=[reflection_pb2.ServiceResponse(name="echo.Echo")]
                        )
                    )
                    yield list_response
                elif request.HasField("file_containing_symbol"):
                    # Respond with the echo descriptor file
                    file_descriptor_response = reflection_pb2.FileDescriptorResponse(
                        file_descriptor_proto=[fd.SerializeToString() for fd in file_descriptor_set.file]
                    )
                    yield reflection_pb2.ServerReflectionResponse(
                        file_descriptor_response=file_descriptor_response
                    )

    # Create a fake channel with our reflection stub
    class FakeChannel:
        def __init__(self):
            self._reflection_stub = FakeReflectionStub()

    # Monkey-patch the ServerReflectionStub creation to use our fake
    import grpc_reflection.v1alpha.reflection_pb2_grpc as reflection_grpc
    original_stub = reflection_grpc.ServerReflectionStub

    def fake_stub_factory(channel):
        if isinstance(channel, FakeChannel):
            return channel._reflection_stub
        return original_stub(channel)

    target = GrpcTarget(address="localhost:50051", reflection="on")
    fake_channel = FakeChannel()

    client = GrpcClient(
        target,
        channel=fake_channel,
        descriptor_pool=None,
        descriptors=None,
        timeout_seconds=5.0,
    )

    with patch.object(reflection_grpc, "ServerReflectionStub", side_effect=fake_stub_factory):
        # Should resolve descriptors via reflection
        pool = client.resolve_descriptors()

        # Verify the pool contains the echo.Echo service
        service_desc = pool.FindServiceByName("echo.Echo")
        assert service_desc is not None
        assert "Unary" in service_desc.methods_by_name
        assert "ServerStream" in service_desc.methods_by_name

        # Verify find_method works with the resolved pool
        unary_method = client.find_method("echo.Echo", "Unary")
        assert client.call_type_of(unary_method) == "unary"

        server_stream_method = client.find_method("echo.Echo", "ServerStream")
        assert client.call_type_of(server_stream_method) == "server_stream"


# Task 6: Call type tests (require protobuf and grpcio)
pytest.importorskip("google.protobuf")
pytest.importorskip("grpc")

# Import grpc-related modules only when available
import grpc
from google.protobuf import descriptor_pool, descriptor_pb2, message_factory
import pathlib


class _FakeRpcError(grpc.RpcError):
    """Fake RpcError for testing."""

    def __init__(self, code, details=""):
        self._code = code
        self._details = details
        super().__init__(details)

    def code(self):
        return self._code

    def details(self):
        return self._details


class _FakeCall:
    """Fake call object for metadata capture."""

    def __init__(self, initial_metadata=(), trailing_metadata=()):
        self._initial_metadata = initial_metadata
        self._trailing_metadata = trailing_metadata

    def initial_metadata(self):
        return self._initial_metadata

    def trailing_metadata(self):
        return self._trailing_metadata


class _FakeStreamIterator:
    """Fake stream iterator that models real grpcio.

    Real grpcio's stream iterable (_MultiThreadedRendezvous) inherits
    trailing_metadata() from Call DIRECTLY on the iterable; there is no
    _call sub-object (the cython IntegratedCall does not expose it). Mirror
    that so the trailer-capture code path is exercised against the production
    behavior. See review finding I2.
    """

    def __init__(self, items, trailing_metadata=()):
        self._items = items
        self._trailing_metadata = trailing_metadata

    def __iter__(self):
        return iter(self._items)

    def trailing_metadata(self):
        return self._trailing_metadata


class _FakeChannel:
    """Fake gRPC channel for testing call methods."""

    def __init__(self, response_data=None, error=None, stream_responses=None):
        self._response_data = response_data
        self._error = error
        self._stream_responses = stream_responses or []
        self._initial_metadata = (("x-request-id", "abc"),)
        self._trailing_metadata = (("x-trailer", "end"),)

    def _fake_serializer(self, req_bytes):
        """Fake serializer that just passes bytes through."""
        return req_bytes

    def _fake_deserializer(self, resp_bytes):
        """Fake deserializer that just passes bytes through."""
        return resp_bytes

    def unary_unary(self, full_method, *, request_serializer=None, response_deserializer=None):
        """Fake unary_unary callable."""

        def invoker(request_bytes, metadata=None, timeout=None):
            if self._error:
                raise self._error
            # Deserialize the response bytes if deserializer provided
            if response_deserializer and self._response_data is not None:
                resp_dict = response_deserializer(self._response_data)
            else:
                resp_dict = self._response_data
            return resp_dict, _FakeCall(self._initial_metadata, self._trailing_metadata)

        invoker.with_call = invoker  # Alias for with_call to same function
        return invoker

    def unary_stream(self, full_method, *, request_serializer=None, response_deserializer=None):
        """Fake unary_stream callable."""

        def invoker(request_bytes, metadata=None, timeout=None):
            if self._error:
                raise self._error
            # Deserialize and yield response dicts
            items = []
            for resp_bytes in self._stream_responses:
                if response_deserializer:
                    items.append(response_deserializer(resp_bytes))
                else:
                    items.append(resp_bytes)
            return _FakeStreamIterator(items, self._trailing_metadata)

        return invoker

    def stream_unary(self, full_method, *, request_serializer=None, response_deserializer=None):
        """Fake stream_unary callable."""

        def invoker(request_iter, metadata=None, timeout=None):
            if self._error:
                raise self._error
            # Consume iterator and return single deserialized response
            list(request_iter)  # Drain the iterator
            if response_deserializer and self._response_data is not None:
                return response_deserializer(self._response_data)
            return self._response_data

        return invoker

    def stream_stream(self, full_method, *, request_serializer=None, response_deserializer=None):
        """Fake stream_stream callable."""

        def invoker(request_iter, metadata=None, timeout=None):
            if self._error:
                raise self._error
            # Consume iterator and yield deserialized responses
            list(request_iter)  # Drain the iterator
            items = []
            for resp_bytes in self._stream_responses:
                if response_deserializer:
                    items.append(response_deserializer(resp_bytes))
                else:
                    items.append(resp_bytes)
            return _FakeStreamIterator(items, self._trailing_metadata)

        return invoker


def _setup_echo_client(fake_channel):
    """Create a client with echo descriptor pool and fake channel."""
    descriptor_path = (
        pathlib.Path(__file__).parent.parent / "fixtures" / "echo_descriptor.pb"
    )
    descriptor_bytes = descriptor_path.read_bytes()

    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_bytes)

    pool = descriptor_pool.DescriptorPool()
    for file_desc in file_descriptor_set.file:
        pool.Add(file_desc)

    target = GrpcTarget(address="localhost:50051", reflection="off")

    client = GrpcClient(
        target,
        channel=fake_channel,
        descriptor_pool=pool,
        timeout_seconds=5.0,
    )
    return client


def _serialize_response_message(response_dict):
    """Serialize a response dict to bytes using the Response message class."""
    # Load descriptor to get message class
    descriptor_path = (
        pathlib.Path(__file__).parent.parent / "fixtures" / "echo_descriptor.pb"
    )
    descriptor_bytes = descriptor_path.read_bytes()

    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(descriptor_bytes)

    pool = descriptor_pool.DescriptorPool()
    for file_desc in file_descriptor_set.file:
        pool.Add(file_desc)

    # Get Response message descriptor and class
    response_desc = pool.FindMessageTypeByName("echo.Response")
    response_class = message_factory.GetMessageClass(response_desc)

    # Serialize dict to bytes via protobuf
    from google.protobuf import json_format

    msg = response_class()
    json_format.ParseDict(response_dict, msg, ignore_unknown_fields=False)
    return msg.SerializeToString()


def test_call_unary_success():
    """Test unary call success with metadata and trailers."""
    # Serialize the expected response
    response_dict = {"msg": "hi", "n": 3}
    response_bytes = _serialize_response_message(response_dict)

    fake_channel = _FakeChannel(response_data=response_bytes)
    client = _setup_echo_client(fake_channel)

    result = client.call_unary("echo.Echo", "Unary", {"msg": "hi"})

    assert result.call_type == "unary"
    assert result.status.code == 0
    assert result.status.name == "OK"
    assert result.message == {"msg": "hi", "n": 3}
    assert result.initial_metadata == {"x-request-id": "abc"}
    assert result.trailers == {"x-trailer": "end"}


def test_call_unary_nonok_status_is_result():
    """Test that non-OK status is returned as result, not raised."""
    # Create fake RpcError with NOT_FOUND status
    error = _FakeRpcError(code=grpc.StatusCode.NOT_FOUND, details="nope")

    fake_channel = _FakeChannel(error=error)
    client = _setup_echo_client(fake_channel)

    result = client.call_unary("echo.Echo", "Unary", {"msg": "hi"})

    # Should return result with error status, NOT raise
    assert result.status.name == "NOT_FOUND"
    assert result.status.code == grpc.StatusCode.NOT_FOUND.value[0]
    assert result.status.message == "nope"
    assert result.message is None


def test_call_unary_deadline_is_operation_timeout():
    """Test that DEADLINE_EXCEEDED raises OperationTimeout."""
    from agctl.errors import OperationTimeout

    # Create fake RpcError with DEADLINE_EXCEEDED status
    error = _FakeRpcError(code=grpc.StatusCode.DEADLINE_EXCEEDED, details="timeout")

    fake_channel = _FakeChannel(error=error)
    client = _setup_echo_client(fake_channel)

    # Should raise OperationTimeout
    with pytest.raises(OperationTimeout):
        client.call_unary("echo.Echo", "Unary", {"msg": "hi"})


def test_call_unary_bad_request_json_is_configerror():
    """Test that unknown field in request JSON raises ConfigError."""
    fake_channel = _FakeChannel(response_data=b"")  # Won't be called
    client = _setup_echo_client(fake_channel)

    # Request with unknown field should fail during serialization
    with pytest.raises(ConfigError):
        client.call_unary("echo.Echo", "Unary", {"unknown_field": 1})


def test_call_server_stream_yields_messages():
    """Test server streaming yields multiple messages with trailers on final."""
    # Create two response messages
    response1_bytes = _serialize_response_message({"msg": "first", "n": 1})
    response2_bytes = _serialize_response_message({"msg": "second", "n": 2})

    fake_channel = _FakeChannel(stream_responses=[response1_bytes, response2_bytes])
    client = _setup_echo_client(fake_channel)

    messages = list(client.call_server_stream("echo.Echo", "ServerStream", {"msg": "x"}))

    assert len(messages) == 2
    assert messages[0].message == {"msg": "first", "n": 1}
    assert messages[0].trailers is None  # First message has no trailers
    assert messages[1].message == {"msg": "second", "n": 2}
    assert messages[1].trailers == {"x-trailer": "end"}  # Final message has trailers


def test_call_client_stream_returns_single_result():
    """Test client streaming returns single result from request iterator."""
    # Serialize the expected response
    response_dict = {"msg": "done", "n": 2}
    response_bytes = _serialize_response_message(response_dict)

    fake_channel = _FakeChannel(response_data=response_bytes)
    client = _setup_echo_client(fake_channel)

    result = client.call_client_stream(
        service="echo.Echo", method="ClientStream", request_json_iter=iter([{"msg": "a"}, {"msg": "b"}])
    )

    assert result.call_type == "client_stream"
    assert result.status.code == 0
    assert result.status.name == "OK"
    assert result.message == {"msg": "done", "n": 2}


def test_call_bidi_yields_messages():
    """Test bidirectional streaming yields multiple messages with trailers on final."""
    # Create two response messages
    response1_bytes = _serialize_response_message({"msg": "echo1", "n": 1})
    response2_bytes = _serialize_response_message({"msg": "echo2", "n": 2})

    fake_channel = _FakeChannel(stream_responses=[response1_bytes, response2_bytes])
    client = _setup_echo_client(fake_channel)

    messages = list(client.call_bidi(service="echo.Echo", method="Bidi", request_json_iter=iter([{"msg": "a"}])))

    assert len(messages) == 2
    assert messages[0].message == {"msg": "echo1", "n": 1}
    assert messages[0].trailers is None  # First message has no trailers
    assert messages[1].message == {"msg": "echo2", "n": 2}
    assert messages[1].trailers == {"x-trailer": "end"}  # Final message has trailers


# Task 7: healthcheck tests (require grpc_health)
def test_healthcheck_serving():
    """Test healthcheck returns SERVING status on success."""
    pytest.importorskip("grpc_health")

    from grpc_health.v1 import health_pb2

    # Create serialized HealthCheckResponse with SERVING status
    response = health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.ServingStatus.SERVING)
    response_bytes = response.SerializeToString()

    # Create fake channel that returns the canned response
    class _FakeHealthChannel:
        def unary_unary(self, full_method, *, request_serializer=None, response_deserializer=None, _registered_method=False):
            """Fake unary_unary that returns canned health check response."""
            def invoker(request_bytes, metadata=None, timeout=None):
                # Deserialize using the health protobuf
                if response_deserializer:
                    return response_deserializer(response_bytes)
                return response_bytes
            return invoker

        def unary_stream(self, full_method, *, request_serializer=None, response_deserializer=None, _registered_method=False):
            """Fake unary_stream (not used by healthcheck but required by HealthStub)."""
            def invoker(request_bytes, metadata=None, timeout=None):
                return iter([])
            return invoker

    target = GrpcTarget(address="localhost:50051")
    client = GrpcClient(target, channel=_FakeHealthChannel(), timeout_seconds=5.0)

    result = client.healthcheck()

    assert result.target == "localhost:50051"
    assert result.address == "localhost:50051"
    assert result.status == "SERVING"
    assert result.note is None


def test_healthcheck_unimplemented_is_unknown():
    """Test healthcheck returns UNKNOWN status on UNIMPLEMENTED RpcError."""
    pytest.importorskip("grpc_health")

    # Create fake RpcError with UNIMPLEMENTED status
    error = _FakeRpcError(code=grpc.StatusCode.UNIMPLEMENTED, details="health not implemented")

    # Create fake channel that raises UNIMPLEMENTED error
    class _FakeHealthChannel:
        def unary_unary(self, full_method, *, request_serializer=None, response_deserializer=None, _registered_method=False):
            """Fake unary_unary that raises UNIMPLEMENTED error."""
            def invoker(request_bytes, metadata=None, timeout=None):
                raise error
            return invoker

        def unary_stream(self, full_method, *, request_serializer=None, response_deserializer=None, _registered_method=False):
            """Fake unary_stream (not used by healthcheck but required by HealthStub)."""
            def invoker(request_bytes, metadata=None, timeout=None):
                return iter([])
            return invoker

    target = GrpcTarget(address="localhost:50051")
    client = GrpcClient(target, channel=_FakeHealthChannel(), timeout_seconds=5.0)

    result = client.healthcheck()

    assert result.target == "localhost:50051"
    assert result.address == "localhost:50051"
    assert result.status == "UNKNOWN"
    assert result.note == "health service UNIMPLEMENTED"


def test_healthcheck_deadline_is_timeout():
    """Test healthcheck raises OperationTimeout on DEADLINE_EXCEEDED."""
    pytest.importorskip("grpc_health")

    from agctl.errors import OperationTimeout

    # Create fake RpcError with DEADLINE_EXCEEDED status
    error = _FakeRpcError(code=grpc.StatusCode.DEADLINE_EXCEEDED, details="timeout")

    # Create fake channel that raises DEADLINE_EXCEEDED error
    class _FakeHealthChannel:
        def unary_unary(self, full_method, *, request_serializer=None, response_deserializer=None, _registered_method=False):
            """Fake unary_unary that raises DEADLINE_EXCEEDED error."""
            def invoker(request_bytes, metadata=None, timeout=None):
                raise error
            return invoker

        def unary_stream(self, full_method, *, request_serializer=None, response_deserializer=None, _registered_method=False):
            """Fake unary_stream (not used by healthcheck but required by HealthStub)."""
            def invoker(request_bytes, metadata=None, timeout=None):
                return iter([])
            return invoker

    target = GrpcTarget(address="localhost:50051")
    client = GrpcClient(target, channel=_FakeHealthChannel(), timeout_seconds=5.0)

    # Should raise OperationTimeout
    with pytest.raises(OperationTimeout):
        client.healthcheck()


# Finding 1: mTLS config tests
def test_mtls_honors_ca_location_file_bytes():
    """Test that mTLS ca_location field is read and passed to ssl_channel_credentials."""
    import tempfile

    # Create a temporary PEM file
    ca_pem = b"-----BEGIN CERTIFICATE-----\nfake CA cert\n-----END CERTIFICATE-----"
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".pem", delete=False) as f:
        f.write(ca_pem)
        ca_path = f.name

    try:
        # Mock ssl_channel_credentials to capture arguments
        import grpc
        original_ssl_cred = grpc.ssl_channel_credentials
        ssl_creds_calls = []

        def fake_ssl_creds(root_certificates=None, certificate_chain=None, private_key=None):
            ssl_creds_calls.append({
                "root_certificates": root_certificates,
                "certificate_chain": certificate_chain,
                "private_key": private_key,
            })
            return original_ssl_cred(root_certificates, certificate_chain, private_key)

        with patch.object(grpc, "ssl_channel_credentials", side_effect=fake_ssl_creds):
            target = GrpcTarget(
                address="localhost:50051",
                use_tls=True,
                tls=GrpcTls(ca_location=ca_path)
            )

            # This should trigger reading the CA file and passing bytes to ssl_channel_credentials
            GrpcClient(target)

            # Verify ssl_channel_credentials was called with the CA file bytes
            assert len(ssl_creds_calls) == 1
            assert ssl_creds_calls[0]["root_certificates"] == ca_pem
            assert ssl_creds_calls[0]["certificate_chain"] is None
            assert ssl_creds_calls[0]["private_key"] is None
    finally:
        import os
        os.unlink(ca_path)


def test_mtls_ca_location_nonexistent_file_raises_configerror():
    """Test that non-existent ca_location file raises ConfigError."""
    target = GrpcTarget(
        address="localhost:50051",
        use_tls=True,
        tls=GrpcTls(ca_location="/nonexistent/ca.pem")
    )

    with pytest.raises(ConfigError) as exc_info:
        GrpcClient(target)

    assert "tls.ca_location file not readable" in str(exc_info.value)
    assert exc_info.value.detail.get("path") == "/nonexistent/ca.pem"


def test_mtls_empty_string_counts_as_unset():
    """Test that empty string for TLS fields counts as unset (no error)."""
    import grpc

    # Mock ssl_channel_credentials to verify it's called with None
    original_ssl_cred = grpc.ssl_channel_credentials
    ssl_creds_calls = []

    def fake_ssl_creds(root_certificates=None, certificate_chain=None, private_key=None):
        ssl_creds_calls.append({
            "root_certificates": root_certificates,
            "certificate_chain": certificate_chain,
            "private_key": private_key,
        })
        return original_ssl_cred(root_certificates, certificate_chain, private_key)

    with patch.object(grpc, "ssl_channel_credentials", side_effect=fake_ssl_creds):
        target = GrpcTarget(
            address="localhost:50051",
            use_tls=True,
            tls=GrpcTls(ca_location="", certificate_location="", key_location="")
        )

        GrpcClient(target)

        # Verify ssl_channel_credentials was called with None for all fields
        assert len(ssl_creds_calls) == 1
        assert ssl_creds_calls[0]["root_certificates"] is None
        assert ssl_creds_calls[0]["certificate_chain"] is None
        assert ssl_creds_calls[0]["private_key"] is None


def test_mtls_override_authority_applied_to_channel():
    """Test that override_authority is applied to the channel."""
    import grpc

    # Mock secure_channel to capture options
    original_secure_channel = grpc.secure_channel
    secure_channel_calls = []

    def fake_secure_channel(address, credentials, options=None):
        secure_channel_calls.append({
            "address": address,
            "credentials": credentials,
            "options": options,
        })
        # Return a fake channel object
        class FakeChannel:
            pass
        return FakeChannel()

    with patch.object(grpc, "secure_channel", side_effect=fake_secure_channel):
        target = GrpcTarget(
            address="localhost:50051",
            use_tls=True,
            tls=GrpcTls(override_authority="example.com")
        )

        GrpcClient(target)

        # Verify secure_channel was called with override options
        assert len(secure_channel_calls) == 1
        options = secure_channel_calls[0]["options"]
        assert options is not None
        assert ("grpc.ssl_target_name_override", "example.com") in options
        assert ("grpc.authority", "example.com") in options


# Finding 2a: Streaming mid-run error tests
def test_server_stream_midstream_rpcerror_captures_terminal_status():
    """Test that mid-stream RpcError is captured into terminal_status and ends iteration."""
    import grpc

    # Create a fake stream that yields one message then raises RpcError
    class _FakeStreamWithError:
        def __init__(self):
            self._yield_count = 0

        def __iter__(self):
            return self

        def __next__(self):
            self._yield_count += 1
            if self._yield_count == 1:
                return {"msg": "first"}
            else:
                # Raise INTERNAL error on second iteration
                raise _FakeRpcError(code=grpc.StatusCode.INTERNAL, details="stream error")

    # Create fake channel that raises error during iteration
    class _FakeChannelWithStreamError:
        def unary_stream(self, full_method, *, request_serializer=None, response_deserializer=None):
            def invoker(request_bytes, metadata=None, timeout=None):
                return _FakeStreamWithError()
            return invoker

    client = _setup_echo_client(_FakeChannelWithStreamError())

    # Stream should capture the error in terminal_status and stop iteration
    messages = list(client.call_server_stream("echo.Echo", "ServerStream", {"msg": "x"}))

    # Should have received the first message before the error
    assert len(messages) == 1
    assert messages[0].message == {"msg": "first"}

    # Terminal status should capture the INTERNAL error
    assert client.terminal_status.name == "INTERNAL"
    assert client.terminal_status.message == "stream error"


def test_server_stream_deadline_raises_operation_timeout():
    """Test that DEADLINE_EXCEEDED during server stream iteration raises OperationTimeout."""
    from agctl.errors import OperationTimeout
    import grpc

    # Create a fake stream that raises DEADLINE_EXCEEDED immediately
    class _FakeStreamWithDeadline:
        def __iter__(self):
            raise _FakeRpcError(code=grpc.StatusCode.DEADLINE_EXCEEDED, details="timeout")

    # Create fake channel that raises error during iteration
    class _FakeChannelWithDeadline:
        def unary_stream(self, full_method, *, request_serializer=None, response_deserializer=None):
            def invoker(request_bytes, metadata=None, timeout=None):
                return _FakeStreamWithDeadline()
            return invoker

    client = _setup_echo_client(_FakeChannelWithDeadline())

    # Should raise OperationTimeout
    with pytest.raises(OperationTimeout):
        list(client.call_server_stream("echo.Echo", "ServerStream", {"msg": "x"}))


def test_bidi_midstream_rpcerror_captures_terminal_status():
    """Test that mid-stream RpcError in bidi is captured into terminal_status and ends iteration."""
    import grpc

    # Create a fake stream that yields one message then raises RpcError
    class _FakeStreamWithError:
        def __init__(self):
            self._yield_count = 0

        def __iter__(self):
            return self

        def __next__(self):
            self._yield_count += 1
            if self._yield_count == 1:
                return {"msg": "first"}
            else:
                # Raise RESOURCE_EXHAUSTED error on second iteration
                raise _FakeRpcError(code=grpc.StatusCode.RESOURCE_EXHAUSTED, details="out of resources")

    # Create fake channel that raises error during iteration
    class _FakeChannelWithStreamError:
        def stream_stream(self, full_method, *, request_serializer=None, response_deserializer=None):
            def invoker(request_iter, metadata=None, timeout=None):
                list(request_iter)  # Drain the iterator
                return _FakeStreamWithError()
            return invoker

    client = _setup_echo_client(_FakeChannelWithStreamError())

    # Stream should capture the error in terminal_status and stop iteration
    messages = list(client.call_bidi(service="echo.Echo", method="Bidi", request_json_iter=iter([{"msg": "a"}])))

    # Should have received the first message before the error
    assert len(messages) == 1
    assert messages[0].message == {"msg": "first"}

    # Terminal status should capture the RESOURCE_EXHAUSTED error
    assert client.terminal_status.name == "RESOURCE_EXHAUSTED"
    assert client.terminal_status.message == "out of resources"


# --- Review fix: C1 - eager stream pre-call serialization --------------------
def test_call_client_stream_bad_request_json_is_configerror():
    """C1: malformed request in client-stream iterator raises ConfigError pre-call.

    Eager list-serialization makes the try/except guard live; the channel
    invoker must never be reached (a generator expr would defer ser() past
    the guard and surface as a false-green INTERNAL result inside grpcio).
    """
    class _TrackingChannel:
        def __init__(self):
            self.invoker_called = False

        def stream_unary(self, full_method, *, request_serializer=None, response_deserializer=None):
            def invoker(request_iter, metadata=None, timeout=None):
                self.invoker_called = True
                raise AssertionError("invoker must not be reached on bad request JSON")

            return invoker

    channel = _TrackingChannel()
    client = _setup_echo_client(channel)

    with pytest.raises(ConfigError) as exc_info:
        client.call_client_stream(
            service="echo.Echo",
            method="ClientStream",
            request_json_iter=iter([{"msg": "ok"}, {"unknown_field": 1}]),
        )
    assert "Failed to serialize request message" in str(exc_info.value)
    assert channel.invoker_called is False


def test_call_bidi_bad_request_json_is_configerror():
    """C1: malformed request in bidi iterator raises ConfigError pre-call."""
    class _TrackingChannel:
        def __init__(self):
            self.invoker_called = False

        def stream_stream(self, full_method, *, request_serializer=None, response_deserializer=None):
            def invoker(request_iter, metadata=None, timeout=None):
                self.invoker_called = True
                raise AssertionError("invoker must not be reached on bad request JSON")

            return invoker

    channel = _TrackingChannel()
    client = _setup_echo_client(channel)

    # call_bidi is a generator function: drive iteration to trigger the
    # (eager, pre-RPC) serialization guard inside its body.
    with pytest.raises(ConfigError) as exc_info:
        list(
            client.call_bidi(
                service="echo.Echo",
                method="Bidi",
                request_json_iter=iter([{"unknown_field": 1}]),
            )
        )
    assert "Failed to serialize request message" in str(exc_info.value)
    assert channel.invoker_called is False


# --- Review fix: I2 - stream trailers read off the iterable ------------------
def test_call_server_stream_captures_trailers():
    """I2: final server-stream message carries trailers read off the iterable.

    Real grpcio exposes trailing_metadata() on the stream iterable itself
    (_MultiThreadedRendezvous), not via a _call sub-object. The updated
    _FakeStreamIterator models that shape; trailers must be the captured dict.
    """
    response1 = _serialize_response_message({"msg": "first", "n": 1})
    response2 = _serialize_response_message({"msg": "second", "n": 2})
    fake_channel = _FakeChannel(stream_responses=[response1, response2])
    client = _setup_echo_client(fake_channel)

    messages = list(client.call_server_stream("echo.Echo", "ServerStream", {"msg": "x"}))

    assert len(messages) == 2
    assert messages[0].trailers is None
    assert messages[1].trailers == {"x-trailer": "end"}
    assert messages[1].trailers is not None  # explicit: not None in production


def test_call_bidi_captures_trailers():
    """I2: final bidi message carries trailers read off the iterable."""
    response1 = _serialize_response_message({"msg": "echo1", "n": 1})
    response2 = _serialize_response_message({"msg": "echo2", "n": 2})
    fake_channel = _FakeChannel(stream_responses=[response1, response2])
    client = _setup_echo_client(fake_channel)

    messages = list(
        client.call_bidi(service="echo.Echo", method="Bidi", request_json_iter=iter([{"msg": "a"}]))
    )

    assert len(messages) == 2
    assert messages[0].trailers is None
    assert messages[1].trailers == {"x-trailer": "end"}
    assert messages[1].trailers is not None


# --- Review fix: I6 - unit coverage for _resolve_via_descriptors -------------
from agctl.config.models import GrpcDescriptorSource


def test_resolve_descriptors_descriptor_set_branch():
    """I6: descriptor_set source builds a pool via resolve_descriptors()."""
    descriptor_path = pathlib.Path(__file__).parent.parent / "fixtures" / "echo_descriptor.pb"

    target = GrpcTarget(address="h:1", reflection="off")
    client = GrpcClient(
        target,
        channel=object(),
        descriptors=[GrpcDescriptorSource(descriptor_set=str(descriptor_path))],
    )

    pool = client.resolve_descriptors()
    method = client.find_method("echo.Echo", "Unary")
    assert client.call_type_of(method) == "unary"
    # Pool resolves the service directly too
    assert pool.FindServiceByName("echo.Echo") is not None


def test_resolve_descriptors_proto_branch(tmp_path):
    """I6: proto source compiles via protoc (honoring include_paths) and resolves."""
    import shutil

    src = pathlib.Path(__file__).parent.parent / "fixtures" / "echo.proto"
    proto_file = tmp_path / "echo.proto"
    shutil.copy(src, proto_file)

    target = GrpcTarget(address="h:1", reflection="off")
    client = GrpcClient(
        target,
        channel=object(),
        descriptors=[
            GrpcDescriptorSource(proto=str(proto_file), include_paths=[str(tmp_path)])
        ],
    )

    client.resolve_descriptors()
    method = client.find_method("echo.Echo", "Unary")
    assert client.call_type_of(method) == "unary"


def test_resolve_descriptors_protoc_failure_is_configerror(tmp_path):
    """I4: a malformed proto yields ConfigError mentioning protoc (rc != 0)."""
    # Unterminated service block -> protoc returns nonzero rc (does not raise)
    proto_file = tmp_path / "bad.proto"
    proto_file.write_text('syntax = "proto3";\npackage demo;\nservice X {\n')

    target = GrpcTarget(address="h:1", reflection="off")
    client = GrpcClient(
        target,
        channel=object(),
        descriptors=[GrpcDescriptorSource(proto=str(proto_file))],
    )

    with pytest.raises(ConfigError) as exc_info:
        client.resolve_descriptors()
    assert "protoc" in str(exc_info.value)
    assert exc_info.value.detail.get("proto") == str(proto_file)


def test_resolve_descriptors_multi_file_out_of_order(tmp_path):
    """I5: multi-file descriptor_set loads regardless of file arrival order.

    protoc emits dependency-first order, so we deliberately reverse the
    FileDescriptorSet.file list (importer before imported). The reversed order
    breaks pool.Add with a TypeError ("Depends on file 'b.proto'..."); the
    AddSerializedFile path tolerates it.
    """
    from google.protobuf import descriptor_pb2
    from grpc_tools import protoc

    (tmp_path / "b.proto").write_text(
        'syntax = "proto3";\npackage demo;\nmessage B { string v = 1; }\n'
    )
    (tmp_path / "a.proto").write_text(
        'syntax = "proto3";\npackage demo;\nimport "b.proto";\n'
        "message A { B b = 1; }\n"
        "service Demo { rpc Get(A) returns (A); }\n"
    )
    raw = tmp_path / "raw.pb"
    rc = protoc.main(
        [
            "protoc",
            "--include_imports",
            "--proto_path",
            str(tmp_path),
            "--descriptor_set_out",
            str(raw),
            str(tmp_path / "a.proto"),
        ]
    )
    assert rc == 0

    # Reverse file order to force importer-before-imported.
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(raw.read_bytes())
    reversed_set = descriptor_pb2.FileDescriptorSet()
    reversed_set.file.extend(reversed(fds.file))
    descriptor_set = tmp_path / "reversed.pb"
    descriptor_set.write_bytes(reversed_set.SerializeToString())

    target = GrpcTarget(address="h:1", reflection="off")
    client = GrpcClient(
        target,
        channel=object(),
        descriptors=[GrpcDescriptorSource(descriptor_set=str(descriptor_set))],
    )

    pool = client.resolve_descriptors()
    method = client.find_method("demo.Demo", "Get")
    assert client.call_type_of(method) == "unary"
    assert pool.FindMessageTypeByName("demo.A") is not None
    assert pool.FindMessageTypeByName("demo.B") is not None
