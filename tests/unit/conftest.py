"""Shared fixtures for unit tests.

Currently scoped to gRPC-mock fixtures reused across the kernel, client,
mock-server and assertion suites (Tasks 1/6/7/12 of the gRPC mock server
feature). Add protocol-agnostic fixtures here as needed.
"""

from __future__ import annotations

import os
import pathlib
import tempfile

import pytest


@pytest.fixture(scope="session")
def mock_grpc_echo_proto_path() -> pathlib.Path:
    """Absolute path to tests/fixtures/mock_grpc/echo.proto.

    The single proto used by the gRPC mock tests: defines ``echo.EchoService``
    with Unary/ServerStream/ClientStream/Bidi and ``EchoRequest``/``EchoResponse``.
    Reused by the kernel (Task 1), client, mock server (Tasks 6/7), and
    assertions (Task 12) suites.
    """
    return (
        pathlib.Path(__file__).parent.parent
        / "fixtures"
        / "mock_grpc"
        / "echo.proto"
    )


@pytest.fixture(scope="session")
def mock_grpc_echo_pool(mock_grpc_echo_proto_path):
    """Compile ``echo.proto`` to a session-scoped ``DescriptorPool``.

    Compilation goes through ``grpc_tools.protoc`` + ``FileDescriptorSet`` (NOT
    through ``grpc_descriptors.build_descriptor_pool``) so kernel tests of
    ``build_descriptor_pool`` stay independent of this fixture.

    The fixture itself is gated lazily on ``grpc_tools`` / ``google.protobuf``:
    a test file that uses it must still module-level ``importorskip`` those
    extras, otherwise collection without the extras would error instead of skip.
    """
    from google.protobuf import descriptor_pb2, descriptor_pool
    from grpc_tools import protoc

    with tempfile.TemporaryDirectory() as tmpdir:
        descriptor_set_path = os.path.join(tmpdir, "echo_descriptor_set.pb")
        rc = protoc.main(
            [
                "protoc",
                "--include_imports",
                "--proto_path",
                str(mock_grpc_echo_proto_path.parent),
                "--descriptor_set_out",
                descriptor_set_path,
                str(mock_grpc_echo_proto_path),
            ]
        )
        assert rc == 0, f"protoc failed to compile {mock_grpc_echo_proto_path}"

        file_descriptor_set = descriptor_pb2.FileDescriptorSet()
        file_descriptor_set.ParseFromString(
            pathlib.Path(descriptor_set_path).read_bytes()
        )

    pool = descriptor_pool.DescriptorPool()
    for file_desc in file_descriptor_set.file:
        pool.Add(file_desc)
    return pool
