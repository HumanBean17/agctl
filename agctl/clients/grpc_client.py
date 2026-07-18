"""gRPC client DTOs and skeleton with lazy grpcio import.

Proto-only helpers (descriptor resolution, service/method lookup, JSON<->protobuf
translation) live in :mod:`agctl.clients.grpc_descriptors` — the shared kernel
consumed by both this client and the upcoming gRPC mock server. This module
keeps the reflection-resolution path (client-only) and the grpcio channel/call
plumbing; everything else delegates to the kernel.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

from agctl.clients import grpc_descriptors
from agctl.config.models import GrpcTarget
from agctl.errors import ConfigError


@dataclass
class GrpcStatus:
    """gRPC status code and metadata."""

    code: int
    name: str
    message: str = ""


@dataclass
class GrpcUnaryResult:
    """Result of a unary or client-streaming gRPC call."""

    target: str
    service: str
    method: str
    call_type: str
    status: GrpcStatus
    message: dict | None
    initial_metadata: dict
    trailers: dict


@dataclass
class GrpcStreamMessage:
    """A single message from a server-streaming or bidirectional stream."""

    message: dict
    trailers: dict | None


@dataclass
class GrpcHealthResult:
    """Result of a gRPC health check."""

    target: str
    address: str
    status: str
    note: str | None = None


class GrpcClient:
    """gRPC client with lazy grpcio import and DI support."""

    def __init__(
        self,
        target: GrpcTarget,
        *,
        channel=None,
        descriptor_pool=None,
        descriptors=None,
        timeout_seconds: float | None = None,
    ):
        """Initialize gRPC client.

        Args:
            target: gRPC target configuration.
            channel: Optional injected gRPC channel (bypasses grpcio import).
            descriptor_pool: Optional injected descriptor pool.
            descriptors: Optional list of GrpcDescriptorSource for fallback resolution.
            timeout_seconds: Optional timeout for calls in seconds.
        """
        self._target = target
        self._channel = channel
        self._pool = descriptor_pool
        self._descriptors = descriptors
        self._timeout = timeout_seconds
        self.terminal_status: GrpcStatus = GrpcStatus(0, "OK", "")

        # Lazy import grpcio only when no channel is injected
        if channel is None:
            try:
                import grpc
            except ImportError as exc:
                raise ConfigError(
                    "gRPC support requires the 'grpc' extra: pip install 'agctl[grpc]'",
                    {},
                ) from exc
            self._grpc = grpc

            # Build channel from target
            if not target.use_tls:
                # Plaintext h2c channel
                self._channel = grpc.insecure_channel(target.address)
            else:
                # TLS channel with credentials (mTLS support)
                # Read file bytes for each TLS field (empty-string-counts-as-unset convention)
                ca_bytes = None
                cert_bytes = None
                key_bytes = None

                if target.tls.ca_location:
                    try:
                        ca_bytes = pathlib.Path(target.tls.ca_location).read_bytes()
                    except Exception as exc:
                        raise ConfigError(
                            f"gRPC tls.ca_location file not readable: {target.tls.ca_location}: {exc}",
                            {"path": target.tls.ca_location},
                        ) from exc

                if target.tls.certificate_location:
                    try:
                        cert_bytes = pathlib.Path(target.tls.certificate_location).read_bytes()
                    except Exception as exc:
                        raise ConfigError(
                            f"gRPC tls.certificate_location file not readable: {target.tls.certificate_location}: {exc}",
                            {"path": target.tls.certificate_location},
                        ) from exc

                if target.tls.key_location:
                    try:
                        key_bytes = pathlib.Path(target.tls.key_location).read_bytes()
                    except Exception as exc:
                        raise ConfigError(
                            f"gRPC tls.key_location file not readable: {target.tls.key_location}: {exc}",
                            {"path": target.tls.key_location},
                        ) from exc

                # Build credentials with the provided file bytes
                credentials = grpc.ssl_channel_credentials(
                    root_certificates=ca_bytes,
                    certificate_chain=cert_bytes,
                    private_key=key_bytes,
                )

                # Apply override_authority if set
                if target.tls.override_authority:
                    self._channel = grpc.secure_channel(
                        target.address,
                        credentials,
                        options=(
                            ("grpc.ssl_target_name_override", target.tls.override_authority),
                            ("grpc.authority", target.tls.override_authority),
                        ),
                    )
                else:
                    self._channel = grpc.secure_channel(target.address, credentials)

    def resolve_descriptors(self):
        """Resolve service/method descriptors from proto files.

        Resolution order:
        1. Injected pool (self._pool is not None) → return it directly
        2. Reflection (if reflection in ("auto", "on")) → query server
        3. Descriptor fallback (from self._descriptors) → load from files

        Returns:
            DescriptorPool: A pool containing all resolved descriptors.

        Raises:
            ConfigError: If reflection is requested but unavailable and no descriptors.
        """
        # Path 1: Injected pool (test seam / pre-resolved)
        if self._pool is not None:
            return self._pool

        # Path 2: Reflection
        if self._target.reflection in ("auto", "on"):
            # Local import: on the channel-injected DI path self._grpc is None,
            # so referencing self._grpc here would AttributeError before the
            # UNIMPLEMENTED handling (see review finding M2).
            import grpc

            try:
                return self._resolve_via_reflection()
            except grpc.RpcError as exc:
                # Check if this is an UNIMPLEMENTED error via status code
                if exc.code() == grpc.StatusCode.UNIMPLEMENTED:
                    if self._target.reflection == "on":
                        # Reflection explicitly requested but not available
                        raise ConfigError(
                            "reflection requested but the server does not implement it; "
                            "set grpc.targets.<name>.reflection: off and supply grpc.descriptors",
                            {"target": self._target.address},
                        ) from exc
                    # If reflection == "auto", fall through to descriptor path
                    pass
                else:
                    # Other reflection errors should propagate
                    raise

        # Path 3: Descriptor fallback
        return self._resolve_via_descriptors()

    def _resolve_via_reflection(self):
        """Resolve descriptors via server reflection service.

        Returns:
            DescriptorPool: Pool built from reflection responses.

        Raises:
            Exception: If reflection query fails (including UNIMPLEMENTED).
        """
        from grpc_reflection.v1alpha import reflection_pb2
        from grpc_reflection.v1alpha import reflection_pb2_grpc

        # Create reflection stub
        reflection_stub = reflection_pb2_grpc.ServerReflectionStub(
            self._channel
        )

        # List all services via reflection
        request = reflection_pb2.ServerReflectionRequest(
            list_services=""
        )
        response = reflection_stub.ServerReflectionInfo(iter([request]))

        # Collect all file descriptor protos
        from google.protobuf import descriptor_pool

        pool = descriptor_pool.DescriptorPool()
        seen_files = set()  # Track files we've already collected
        all_file_protos = []

        # Collect service names from list_services_response
        service_names = []
        for response_msg in response:
            if response_msg.HasField("list_services_response"):
                for service in response_msg.list_services_response.service:
                    # Skip reflection and health services
                    if service.name not in ("grpc.reflection.v1alpha.ServerReflection", "grpc.health.v1.Health"):
                        service_names.append(service.name)

        # Request file descriptor for each service by symbol name
        for service_name in service_names:
            # Request file descriptor containing this service symbol
            symbol_request = reflection_pb2.ServerReflectionRequest(
                file_containing_symbol=service_name
            )
            symbol_response = reflection_stub.ServerReflectionInfo(
                iter([symbol_request])
            )

            for response_msg in symbol_response:
                if response_msg.HasField("file_descriptor_response"):
                    from google.protobuf import descriptor_pb2
                    for fd_bytes in response_msg.file_descriptor_response.file_descriptor_proto:
                        # Deserialize the bytes into a FileDescriptorProto
                        fd_proto = descriptor_pb2.FileDescriptorProto()
                        fd_proto.ParseFromString(fd_bytes)
                        # Collect each unique file descriptor; added to the pool
                        # in dependency order below (reflection responses are
                        # not guaranteed dependency-first).
                        fd_name = fd_proto.name
                        if fd_name not in seen_files:
                            all_file_protos.append(fd_proto)
                            seen_files.add(fd_name)

        # Add collected files in a dependency-order-tolerant pass.
        grpc_descriptors.add_file_protos_order_tolerant(pool, all_file_protos)
        return pool

    def _resolve_via_descriptors(self):
        """Resolve descriptors from configured descriptor sources.

        Thin delegate to :func:`grpc_descriptors.build_descriptor_pool` so the
        client and the upcoming mock server share one source of truth.

        Returns:
            DescriptorPool: Pool built from all descriptor sources.

        Raises:
            ConfigError: If no descriptors configured and reflection unavailable.
        """
        return grpc_descriptors.build_descriptor_pool(
            self._descriptors,
            context_label=self._target.address,
        )

    def find_method(self, service: str, method: str):
        """Find a method descriptor by service and method name.

        Thin delegate to :func:`grpc_descriptors.find_method`.

        Args:
            service: Fully-qualified service name (e.g., "echo.Echo").
            method: Method name within the service (e.g., "Unary").

        Returns:
            MethodDescriptor: The method descriptor.

        Raises:
            TemplateNotFound: If service or method not found.
        """
        return grpc_descriptors.find_method(
            self.resolve_descriptors(), service, method
        )

    @staticmethod
    def call_type_of(method_desc):
        """Determine the call type from a method descriptor.

        Delegating staticmethod preserved for existing call sites (e.g.
        ``GrpcClient.call_type_of(md)``) — routes to
        :func:`grpc_descriptors.call_type_of`.

        Args:
            method_desc: A protobuf MethodDescriptor.

        Returns:
            str: One of "unary", "server_stream", "client_stream", "bidi".
        """
        return grpc_descriptors.call_type_of(method_desc)

    def _msg_class(self, message_desc):
        """Build a protobuf message class from a descriptor.

        Thin delegate to :func:`grpc_descriptors.message_class`.
        """
        return grpc_descriptors.message_class(message_desc)

    def _serialize(self, message_desc):
        """Build a serializer callable: dict | bytes -> bytes.

        Thin delegate to :func:`grpc_descriptors.serialize`.
        """
        return grpc_descriptors.serialize(message_desc)

    def _deserialize(self, message_desc):
        """Build a deserializer callable: bytes -> dict.

        Thin delegate to :func:`grpc_descriptors.deserialize`.
        """
        return grpc_descriptors.deserialize(message_desc)

    def _metadata_to_items(self, metadata: dict | None) -> list | None:
        """Convert metadata dict to list of (key, value) tuples or None."""
        if metadata is None:
            return None
        return list(metadata.items())

    def _metadata_to_dict(self, metadata_tuple) -> dict:
        """Convert gRPC metadata tuple to dict with lowercased keys."""
        if not metadata_tuple:
            return {}
        return {k.lower(): v for k, v in metadata_tuple}

    def call_unary(
        self,
        service: str,
        method: str,
        message: dict,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ) -> GrpcUnaryResult:
        """Make a unary gRPC call."""
        from agctl.errors import ConnectionFailure, OperationTimeout
        import grpc

        md = self.find_method(service, method)

        # Build serializers
        ser = self._serialize(md.input_type)
        deser = self._deserialize(md.output_type)

        # Build invoker
        fn = self._channel.unary_unary(
            f"/{service}/{method}",
            request_serializer=ser,
            response_deserializer=deser,
        )

        try:
            req_bytes = ser(message)
        except Exception as e:
            from agctl.errors import ConfigError

            raise ConfigError(
                f"Failed to serialize request message: {e}", {"service": service, "method": method}
            ) from e

        try:
            resp, call = fn.with_call(
                req_bytes,
                metadata=self._metadata_to_items(metadata),
                timeout=timeout or self._timeout,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e), detail={}) from e
            else:
                # Non-OK status is returned as result, not raised
                code_enum = e.code()
                status = GrpcStatus(
                    code=code_enum.value[0],
                    name=code_enum.name,
                    message=e.details() or "",
                )
                return GrpcUnaryResult(
                    target=self._target.address,
                    service=service,
                    method=method,
                    call_type="unary",
                    status=status,
                    message=None,
                    initial_metadata={},
                    trailers={},
                )
        except Exception as e:
            raise ConnectionFailure(message=str(e)) from e

        # Success path
        return GrpcUnaryResult(
            target=self._target.address,
            service=service,
            method=method,
            call_type="unary",
            status=GrpcStatus(code=0, name="OK"),
            message=resp,
            initial_metadata=self._metadata_to_dict(call.initial_metadata()),
            trailers=self._metadata_to_dict(call.trailing_metadata()),
        )

    def call_server_stream(
        self,
        service: str,
        method: str,
        message: dict,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ):
        """Make a server-streaming gRPC call."""
        from agctl.errors import OperationTimeout
        import grpc

        md = self.find_method(service, method)

        # Build serializers
        ser = self._serialize(md.input_type)
        deser = self._deserialize(md.output_type)

        # Build invoker
        fn = self._channel.unary_stream(
            f"/{service}/{method}",
            request_serializer=ser,
            response_deserializer=deser,
        )

        # Initialize/reset terminal status to OK
        self.terminal_status = GrpcStatus(0, "OK", "")

        try:
            req_bytes = ser(message)
        except Exception as e:
            from agctl.errors import ConfigError

            raise ConfigError(
                f"Failed to serialize request message: {e}", {"service": service, "method": method}
            ) from e

        try:
            response_iter = fn(
                req_bytes,
                metadata=self._metadata_to_items(metadata),
                timeout=timeout or self._timeout,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e), detail={}) from e
            else:
                # Capture the terminal status from the RPC error
                code_enum = e.code()
                self.terminal_status = GrpcStatus(
                    code=code_enum.value[0],
                    name=code_enum.name,
                    message=e.details() or "",
                )
                return
        except Exception:
            # Non-RpcError - stream ends
            return

        # Collect messages and yield them, attaching trailers to final message
        messages = []
        try:
            for resp in response_iter:
                messages.append(resp)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e), detail={}) from e
            else:
                # Capture the terminal status from the RPC error
                code_enum = e.code()
                self.terminal_status = GrpcStatus(
                    code=code_enum.value[0],
                    name=code_enum.name,
                    message=e.details() or "",
                )
                # Stop iteration - terminal_status carries the error
        except Exception:
            # Non-RpcError - stream ends
            pass

        # Capture trailers after stream ends. On real grpcio the stream
        # iterable is a _MultiThreadedRendezvous that inherits
        # trailing_metadata() from Call directly; the old _call indirection
        # targeted a cython IntegratedCall that does NOT expose it, so trailers
        # were always None in production. Read off the iterable itself.
        # See review finding I2.
        trailers = None
        try:
            if hasattr(response_iter, "trailing_metadata"):
                trailers = self._metadata_to_dict(response_iter.trailing_metadata())
        except Exception:
            # Ignore trailer metadata errors
            pass

        # Yield all messages with trailers=None except the final one
        for i, msg in enumerate(messages):
            if i == len(messages) - 1 and trailers:
                # Final message gets trailers
                yield GrpcStreamMessage(message=msg, trailers=trailers)
            else:
                yield GrpcStreamMessage(message=msg, trailers=None)

    def call_client_stream(
        self,
        service: str,
        method: str,
        request_json_iter,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ) -> GrpcUnaryResult:
        """Make a client-streaming gRPC call."""
        from agctl.errors import ConnectionFailure, OperationTimeout
        import grpc

        md = self.find_method(service, method)

        # Build serializers
        ser = self._serialize(md.input_type)
        deser = self._deserialize(md.output_type)

        # Build invoker
        fn = self._channel.stream_unary(
            f"/{service}/{method}",
            request_serializer=ser,
            response_deserializer=deser,
        )

        try:
            # Eagerly serialize all requests into a list so a malformed request
            # raises ConfigError BEFORE the call. A generator expression would
            # defer ser(req) past this try/except (serialization then happens
            # inside grpcio, surfacing as a false-green INTERNAL result). The
            # list is then wrapped in iter() because grpcio's request consumer
            # calls next() on it (a raw list is not an iterator). See C1.
            req_iter = iter([ser(req) for req in request_json_iter])
        except Exception as e:
            from agctl.errors import ConfigError

            raise ConfigError(
                f"Failed to serialize request message: {e}", {"service": service, "method": method}
            ) from e

        try:
            resp = fn(
                req_iter,
                metadata=self._metadata_to_items(metadata),
                timeout=timeout or self._timeout,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e), detail={}) from e
            else:
                # Non-OK status is returned as result, not raised
                code_enum = e.code()
                status = GrpcStatus(
                    code=code_enum.value[0],
                    name=code_enum.name,
                    message=e.details() or "",
                )
                return GrpcUnaryResult(
                    target=self._target.address,
                    service=service,
                    method=method,
                    call_type="client_stream",
                    status=status,
                    message=None,
                    initial_metadata={},
                    trailers={},
                )
        except Exception as e:
            raise ConnectionFailure(message=str(e)) from e

        # Success path - stream_unary doesn't expose metadata
        return GrpcUnaryResult(
            target=self._target.address,
            service=service,
            method=method,
            call_type="client_stream",
            status=GrpcStatus(code=0, name="OK"),
            message=resp,
            initial_metadata={},
            trailers={},
        )

    def call_bidi(
        self,
        service: str,
        method: str,
        request_json_iter,
        *,
        metadata: dict | None = None,
        timeout: float | None = None,
    ):
        """Make a bidirectional-streaming gRPC call."""
        from agctl.errors import OperationTimeout
        import grpc

        md = self.find_method(service, method)

        # Build serializers
        ser = self._serialize(md.input_type)
        deser = self._deserialize(md.output_type)

        # Build invoker
        fn = self._channel.stream_stream(
            f"/{service}/{method}",
            request_serializer=ser,
            response_deserializer=deser,
        )

        # Initialize/reset terminal status to OK
        self.terminal_status = GrpcStatus(0, "OK", "")

        try:
            # Eagerly serialize all requests into a list so a malformed request
            # raises ConfigError BEFORE the call. A generator expression would
            # defer ser(req) past this try/except (serialization then happens
            # inside grpcio, surfacing as a false-green INTERNAL result). The
            # list is then wrapped in iter() because grpcio's request consumer
            # calls next() on it (a raw list is not an iterator). See C1.
            req_iter = iter([ser(req) for req in request_json_iter])
        except Exception as e:
            from agctl.errors import ConfigError

            raise ConfigError(
                f"Failed to serialize request message: {e}", {"service": service, "method": method}
            ) from e

        try:
            response_iter = fn(
                req_iter,
                metadata=self._metadata_to_items(metadata),
                timeout=timeout or self._timeout,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e), detail={}) from e
            else:
                # Capture the terminal status from the RPC error
                code_enum = e.code()
                self.terminal_status = GrpcStatus(
                    code=code_enum.value[0],
                    name=code_enum.name,
                    message=e.details() or "",
                )
                return
        except Exception:
            # Non-RpcError - stream ends
            return

        # Collect messages and yield them, attaching trailers to final message
        messages = []
        try:
            for resp in response_iter:
                messages.append(resp)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e), detail={}) from e
            else:
                # Capture the terminal status from the RPC error
                code_enum = e.code()
                self.terminal_status = GrpcStatus(
                    code=code_enum.value[0],
                    name=code_enum.name,
                    message=e.details() or "",
                )
                # Stop iteration - terminal_status carries the error
        except Exception:
            # Non-RpcError - stream ends
            pass

        # Capture trailers after stream ends. On real grpcio the stream
        # iterable is a _MultiThreadedRendezvous that inherits
        # trailing_metadata() from Call directly; the old _call indirection
        # targeted a cython IntegratedCall that does NOT expose it, so trailers
        # were always None in production. Read off the iterable itself.
        # See review finding I2.
        trailers = None
        try:
            if hasattr(response_iter, "trailing_metadata"):
                trailers = self._metadata_to_dict(response_iter.trailing_metadata())
        except Exception:
            # Ignore trailer metadata errors
            pass

        # Yield all messages with trailers=None except the final one
        for i, msg in enumerate(messages):
            if i == len(messages) - 1 and trailers:
                # Final message gets trailers
                yield GrpcStreamMessage(message=msg, trailers=trailers)
            else:
                yield GrpcStreamMessage(message=msg, trailers=None)

    def healthcheck(self, service_name: str = "") -> GrpcHealthResult:
        """Perform a gRPC health check.

        Args:
            service_name: Optional service name to check. Empty string checks overall
                server health.

        Returns:
            GrpcHealthResult: Health check result with status and optional note.

        Raises:
            OperationTimeout: If the health check request times out.
            ConnectionFailure: If the connection fails or other RPC error occurs.
        """
        from agctl.errors import ConnectionFailure, OperationTimeout

        # Lazy import grpc_health.v1
        from grpc_health.v1 import health_pb2
        from grpc_health.v1.health_pb2_grpc import HealthStub
        import grpc

        # Build HealthStub
        health_stub = HealthStub(self._channel)

        # Call health check
        try:
            response = health_stub.Check(
                health_pb2.HealthCheckRequest(service=service_name),
                timeout=self._timeout,
            )
            # Success: map status enum name to string
            status_name = health_pb2.HealthCheckResponse.ServingStatus.Name(response.status)
            return GrpcHealthResult(
                target=self._target.address,
                address=self._target.address,
                status=status_name,
                note=None,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                # Health service not implemented - return UNKNOWN (not an error)
                return GrpcHealthResult(
                    target=self._target.address,
                    address=self._target.address,
                    status="UNKNOWN",
                    note="health service UNIMPLEMENTED",
                )
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise OperationTimeout(message=str(e)) from e
            else:
                # Other RPC errors are connection failures
                raise ConnectionFailure(message=str(e)) from e
        except Exception as exc:
            # Non-RPC errors are connection failures
            raise ConnectionFailure(message=str(exc)) from exc
