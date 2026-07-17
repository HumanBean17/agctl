"""Shared gRPC proto kernel — descriptor resolution + JSON<->protobuf helpers.

This module is the single source of truth shared by the gRPC client
(:mod:`agctl.clients.grpc_client`) and the upcoming gRPC mock server so the two
paths agree on how service/method descriptors are resolved and how protobuf
messages are (de)serialized.

Pure functions over ``DescriptorPool`` / ``MessageDescriptor``. No grpcio
dependency: this module never opens a channel.

**Lazy-import discipline (load-bearing):** this module MUST NOT ``import grpc``,
``grpc_tools``, or ``google.protobuf`` at module top. Every such import lives
inside the function that needs it so that merely importing the kernel is cheap
and never requires the gRPC extra. A missing optional dependency surfaces as
:class:`agctl.errors.ConfigError` pointing at ``pip install 'agctl[grpc]'``
(never a bare :class:`ModuleNotFoundError`).
"""

from __future__ import annotations

import pathlib
from typing import Callable

from agctl.config.models import GrpcDescriptorSource
from agctl.errors import ConfigError, TemplateNotFound


_GRPC_EXTRA_HINT = "gRPC support requires the 'grpc' extra: pip install 'agctl[grpc]'"


def _require(module_name: str):
    """Lazily import a gRPC/protobuf submodule or raise ConfigError.

    Used to enforce the lazy-import discipline: missing libraries surface as
    ``ConfigError`` (exit 2) pointing at the ``grpc`` extra, mirroring how
    :class:`agctl.clients.grpc_client.GrpcClient` handles missing ``grpc``.

    Only call this for submodules (``google.protobuf.descriptor_pool``,
    ``grpc_tools.protoc``, etc.). Attribute access on the returned module is
    the caller's responsibility.
    """
    import importlib

    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigError(
            _GRPC_EXTRA_HINT,
            {"missing_module": module_name},
        ) from exc


def add_file_protos_order_tolerant(pool, file_protos) -> None:
    """Add FileDescriptorProtos to the pool regardless of dependency order.

    protoc's FileDescriptorSet.file order is NOT guaranteed dependency-first,
    and the descriptor pool validates dependencies eagerly (raising TypeError
    for a file whose import is not yet loaded — even via AddSerializedFile).
    Make repeated passes, deferring any file whose dependency is not yet loaded,
    until all are added. If no file can be added in a pass, surface the genuine
    error by re-adding the first remaining file. See review finding I5.

    Moved verbatim from ``clients.grpc_client._add_file_protos_order_tolerant``.
    """
    remaining = list(file_protos)
    while remaining:
        progress = False
        deferred = []
        for fd in remaining:
            try:
                pool.AddSerializedFile(fd.SerializeToString())
                progress = True
            except TypeError:
                deferred.append(fd)
        if not progress:
            # No file could be added -> genuine error (missing import, etc.).
            # Re-raise so the real cause surfaces.
            pool.AddSerializedFile(remaining[0].SerializeToString())
        remaining = deferred


def build_descriptor_pool(
    sources: list[GrpcDescriptorSource], *, context_label: str
):
    """Build a ``DescriptorPool`` from a list of descriptor sources.

    The body of the former ``GrpcClient._resolve_via_descriptors``, lifted to a
    function taking the source list plus ``context_label`` (folded into error
    messages so callers can identify which target/template failed).

    Args:
        sources: Ordered list of :class:`GrpcDescriptorSource`. Each source
            contributes via its ``descriptor_set`` (pre-compiled .pb) or
            ``proto`` (.proto source compiled in-memory via ``grpc_tools.protoc``).
        context_label: Free-form identifier folded into error messages.

    Returns:
        google.protobuf.descriptor_pool.DescriptorPool: Pool containing every
        file from every source.

    Raises:
        ConfigError: ``sources`` is empty, a referenced file is missing /
          unreadable, protoc fails to compile a .proto, or the gRPC extra is
          not installed.

    Lazy-imports ``google.protobuf.descriptor_pool`` /
    ``google.protobuf.descriptor_pb2`` / ``grpc_tools.protoc`` inside.
    """
    # Input validation FIRST: the empty-sources error must mention the
    # context_label regardless of whether the gRPC extra is installed.
    if not sources:
        raise ConfigError(
            f"no gRPC descriptors available for {context_label}; "
            "configure grpc.descriptors",
            {"context": context_label},
        )

    descriptor_pool = _require("google.protobuf.descriptor_pool")
    descriptor_pb2 = _require("google.protobuf.descriptor_pb2")

    pool = descriptor_pool.DescriptorPool()

    for descriptor_source in sources:
        if descriptor_source.descriptor_set:
            descriptor_path = pathlib.Path(descriptor_source.descriptor_set)
            try:
                descriptor_bytes = descriptor_path.read_bytes()
            except OSError as exc:
                raise ConfigError(
                    f"unable to read gRPC descriptor set "
                    f"{descriptor_path} for {context_label}: {exc}",
                    {"descriptor_set": str(descriptor_path)},
                ) from exc

            file_descriptor_set = descriptor_pb2.FileDescriptorSet()
            file_descriptor_set.ParseFromString(descriptor_bytes)

            # Dependency-order tolerant: FileDescriptorSet.file order is not
            # guaranteed dependency-first (and the pool validates deps eagerly).
            add_file_protos_order_tolerant(pool, file_descriptor_set.file)

        elif descriptor_source.proto:
            protoc = _require("grpc_tools.protoc")

            proto_path = pathlib.Path(descriptor_source.proto)

            import os
            import tempfile

            with tempfile.TemporaryDirectory() as tmpdir:
                descriptor_set_path = os.path.join(tmpdir, "descriptor_set.pb")

                # Build proto search paths: the proto's own directory first,
                # then any configured include_paths.
                proto_paths = ["--proto_path", str(proto_path.parent)]
                for include_path in descriptor_source.include_paths:
                    proto_paths.extend(["--proto_path", str(include_path)])

                # Call protoc to compile. protoc.main returns a nonzero int on
                # failure (it does NOT raise); surface as ConfigError pointing
                # at the proto. See review finding I4.
                rc = protoc.main(
                    [
                        "protoc",
                        "--include_imports",
                        *proto_paths,
                        "--descriptor_set_out",
                        descriptor_set_path,
                        str(proto_path),
                    ]
                )
                if rc != 0:
                    raise ConfigError(
                        f"protoc failed to compile {proto_path} "
                        f"for {context_label}: rc={rc}",
                        {"proto": str(proto_path)},
                    )

                descriptor_bytes = pathlib.Path(descriptor_set_path).read_bytes()
                file_descriptor_set = descriptor_pb2.FileDescriptorSet()
                file_descriptor_set.ParseFromString(descriptor_bytes)

                add_file_protos_order_tolerant(pool, file_descriptor_set.file)

    return pool


def find_method(pool, service: str, method: str):
    """Find a method descriptor by service and method name.

    Args:
        pool: A ``DescriptorPool`` containing the service.
        service: Fully-qualified service name (e.g., "echo.EchoService").
        method: Method name within the service (e.g., "Unary").

    Returns:
        MethodDescriptor: The resolved method descriptor.

    Raises:
        TemplateNotFound: If service or method is not present in the pool.
    """
    try:
        service_desc = pool.FindServiceByName(service)
    except KeyError:
        raise TemplateNotFound(
            f"Unknown gRPC service: {service}",
            {"service": service},
        )

    method_desc = service_desc.methods_by_name.get(method)
    if method_desc is None:
        raise TemplateNotFound(
            f"Unknown gRPC method: {method} on {service}",
            {"service": service, "method": method},
        )

    return method_desc


def call_type_of(method_desc) -> str:
    """Determine the call type from a method descriptor.

    Args:
        method_desc: A protobuf ``MethodDescriptor``.

    Returns:
        One of ``"unary"``, ``"server_stream"``, ``"client_stream"``, ``"bidi"``.
    """
    is_client_streaming = method_desc.client_streaming
    is_server_streaming = method_desc.server_streaming

    if is_client_streaming and is_server_streaming:
        return "bidi"
    elif is_server_streaming:
        return "server_stream"
    elif is_client_streaming:
        return "client_stream"
    else:
        return "unary"


def message_class(message_desc) -> type:
    """Build a concrete protobuf message class from a message descriptor.

    Lazy-imports ``google.protobuf.message_factory`` so the kernel stays cheap
    to import. Raises ``ConfigError`` if the gRPC extra is missing.
    """
    message_factory = _require("google.protobuf.message_factory")
    return message_factory.GetMessageClass(message_desc)


def serialize(message_desc) -> Callable[[dict | bytes], bytes]:
    """Build a serializer callable: ``dict | bytes -> bytes``.

    Dict path uses ``json_format.ParseDict(d, msg, ignore_unknown_fields=False)``
    so an unknown field raises (callers translate that to ``ConfigError``).
    Bytes input passes through unchanged.
    """

    def serializer(d: dict | bytes) -> bytes:
        if isinstance(d, bytes):
            return d

        json_format = _require("google.protobuf.json_format")

        cls = message_class(message_desc)
        msg = cls()
        json_format.ParseDict(d, msg, ignore_unknown_fields=False)
        return msg.SerializeToString()

    return serializer


def deserialize(message_desc) -> Callable[[bytes], dict]:
    """Build a deserializer callable: ``bytes -> dict``."""

    def deserializer(b: bytes) -> dict:
        json_format = _require("google.protobuf.json_format")

        cls = message_class(message_desc)
        msg = cls.FromString(b)
        return json_format.MessageToDict(msg)

    return deserializer
