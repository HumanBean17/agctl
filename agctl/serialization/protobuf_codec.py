"""Lazy Protobuf codec (``DynamicMessage`` via the grpc_descriptors kernel).

Operates on the Protobuf payload AFTER :func:`agctl.serialization.wire.parse_wire`
has stripped the Confluent magic byte + schema id (decode) and BEFORE
:func:`agctl.serialization.wire.build_wire` re-adds them (encode). It does
NOT do wire-framing itself — that is the API layer's job (Task 14).

A Protobuf schema fetched from Confluent Schema Registry is a ``.proto``
**source string** (e.g. ``'syntax = "proto3"; message E { string id = 1; }'``)
— not a file path, not a ``FileDescriptorProto``. To decode/encode, this
module compiles that string into a :class:`DescriptorPool`, resolves the
single top-level message descriptor, and uses the kernel's
``DynamicMessage`` + ``json_format`` helpers.

The compile path reuses the kernel's order-tolerant loader
(:func:`agctl.clients.grpc_descriptors.add_file_protos_order_tolerant`) and
the kernel's :func:`serialize` / :func:`deserialize` helpers; it does NOT
reimplement protoc orchestration beyond the single ``grpc_tools.protoc.main``
call that the kernel itself makes internally.

``protobuf`` / ``grpc_tools`` / ``google.protobuf`` are lazy-imported inside
the functions that need them via :func:`_require_protobuf`. The module top
imports only stdlib + the kernel + :mod:`agctl.errors`, so it imports cleanly
even when the ``protobuf`` extra is absent; a missing extra surfaces as
:class:`ConfigError` pointing at ``pip install 'agctl[protobuf]'``, never a
bare :class:`ImportError`.

**v1 limitation (documented, fail-loud):** fully supports single-file,
self-contained ``.proto`` schemas (one top-level message, no imports).
Multi-file schemas with ``import`` statements are best-effort via the
kernel's order-tolerant loader; if resolution fails, the codec raises
:class:`SerializationError` with a truncated ``schema_snippet`` in the
detail — NEVER silently.
"""

from __future__ import annotations

import os
import pathlib
import tempfile

from ..clients import grpc_descriptors
from ..errors import ConfigError, SerializationError

# Module-level cache: schema_str -> MessageDescriptor. Same pattern as the
# Avro codec's ``_parsed`` dict; a MessageDescriptor is immutable so a
# process-wide cache is safe. The codec functions are pure functions of
# (payload, schema_str) with no per-instance state, so module scope is right.
_descriptors: dict[str, object] = {}

_PROTOBUF_EXTRA_HINT = (
    "Protobuf codec requires the 'protobuf' extra: pip install 'agctl[protobuf]'"
)

# Truncation limit for schema_snippet in error detail. Long schemas just
# bloat error envelopes; the first 200 chars are enough to identify the file.
_SNIPPET_LIMIT = 200


def _require_protobuf():
    """Lazy-import and return the modules this codec needs.

    Returns a ``(descriptor_pool, descriptor_pb2, protoc)`` triple — the three
    modules the compile path touches. Other kernel helpers
    (``message_factory``, ``json_format``) are lazy-imported by the kernel
    itself via its own ``_require`` once these are present.

    Raises :class:`ConfigError` with the install hint when the ``protobuf``
    extra is absent — never a bare :class:`ImportError`. Called from inside
    :func:`_message_descriptor` so module import stays cheap and extra-free.
    """
    import importlib  # noqa: PLC0415 — deliberate lazy import

    try:
        descriptor_pool = importlib.import_module(
            "google.protobuf.descriptor_pool"
        )
        descriptor_pb2 = importlib.import_module("google.protobuf.descriptor_pb2")
        protoc = importlib.import_module("grpc_tools.protoc")
    except ImportError as exc:  # pragma: no cover - env-dependent
        raise ConfigError(_PROTOBUF_EXTRA_HINT, {"missing_module": str(exc)}) from exc
    return descriptor_pool, descriptor_pb2, protoc


def _snippet(schema_str: str) -> str:
    """Truncate a schema string for inclusion in error detail."""
    s = schema_str.strip()
    if len(s) <= _SNIPPET_LIMIT:
        return s
    return s[:_SNIPPET_LIMIT] + "..."


def _compile_proto_string(schema_str: str):
    """Compile a ``.proto`` source string into a list of FileDescriptorProtos.

    Writes the schema to a temp file and invokes ``grpc_tools.protoc`` to
    produce a ``FileDescriptorSet`` (with ``--include_imports`` so transitive
    dependencies, if any, are present), then returns the ``file`` list.

    This mirrors the kernel's
    :func:`agctl.clients.grpc_descriptors.build_descriptor_pool` protoc step
    (single source of truth for the invocation). The kernel's own
    ``build_descriptor_pool`` takes file *paths* via ``GrpcDescriptorSource``
    and does not expose the ``FileDescriptorProto`` list needed to discover
    top-level message names, so we call protoc directly here and then hand
    the result to the kernel's order-tolerant loader.

    Raises :class:`SerializationError` (with ``schema_snippet``) when protoc
    fails — malformed schema, unknown type, or unresolved ``import``.
    """
    _descriptor_pool_module, descriptor_pb2, protoc = _require_protobuf()

    # Append grpc_tools' bundled well-known-type protos (google/protobuf/*.proto)
    # to --proto_path so single-file schemas that import e.g.
    # ``google/protobuf/timestamp.proto`` (ubiquitous) resolve. Without this
    # protoc fails on the import and the schema is unusable.
    import grpc_tools  # noqa: PLC0415 — deliberate lazy import (extra-gated)

    grpc_tools_proto_root = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")

    with tempfile.TemporaryDirectory() as tmpdir:
        proto_path = pathlib.Path(tmpdir) / "schema.proto"
        proto_path.write_text(schema_str)
        descriptor_set_path = os.path.join(tmpdir, "descriptor_set.pb")

        # protoc.main returns a nonzero int on failure (it does NOT raise);
        # surface as SerializationError so callers see a typed failure
        # rather than a bare rc. Mirrors the kernel's build_descriptor_pool
        # error handling, just at the codec's altitude.
        rc = protoc.main(
            [
                "protoc",
                "--include_imports",
                "--proto_path",
                str(proto_path.parent),
                "--proto_path",
                grpc_tools_proto_root,
                "--descriptor_set_out",
                descriptor_set_path,
                str(proto_path),
            ]
        )
        if rc != 0:
            raise SerializationError(
                "cannot compile protobuf schema (protoc failed)",
                {"schema_snippet": _snippet(schema_str), "protoc_rc": rc},
            )

        file_descriptor_set = descriptor_pb2.FileDescriptorSet()
        file_descriptor_set.ParseFromString(
            pathlib.Path(descriptor_set_path).read_bytes()
        )
        return list(file_descriptor_set.file)


def _pick_message_name(file_protos) -> str:
    """Pick the message name to decode against from FileDescriptorProtos.

    v1 assumes one top-level message per schema; for multi-message schemas
    the brief says to pick the LAST declared message (the schema's declared
    message). Returns the fully-qualified name (``package.MessageName`` when
    a package is declared, bare ``MessageName`` otherwise).

    Raises :class:`SerializationError` if no top-level message is declared.
    """
    candidates: list[str] = []
    for fd in file_protos:
        package = fd.package  # "" for no package, else "foo.bar"
        prefix = f"{package}." if package else ""
        for mt in fd.message_type:
            candidates.append(f"{prefix}{mt.name}")
    if not candidates:
        raise SerializationError(
            "cannot resolve protobuf schema (no top-level message declared)",
            {},
        )
    return candidates[-1]


def _message_descriptor(schema_str: str):
    """Build and cache a ``MessageDescriptor`` for the ``.proto`` schema string.

    Compiles the schema to ``FileDescriptorProto`` s, adds them to a fresh
    ``DescriptorPool`` via the kernel's order-tolerant loader, and resolves
    the single top-level message descriptor. Cached module-level keyed by
    ``schema_str`` so repeated decode/encode of the same schema does not
    re-invoke protoc per message.

    Raises :class:`SerializationError` on any compilation or resolution
    failure (malformed schema, unresolved multi-file imports, missing
    message) — never silent. The detail carries a truncated
    ``schema_snippet`` for diagnosis.
    """
    cached = _descriptors.get(schema_str)
    if cached is not None:
        return cached

    descriptor_pool_module, _descriptor_pb2, _protoc = _require_protobuf()

    try:
        file_protos = _compile_proto_string(schema_str)
        pool = descriptor_pool_module.DescriptorPool()
        # Reuse the kernel's order-tolerant add — protoc's FileDescriptorSet
        # file order is NOT guaranteed dependency-first, and the pool
        # validates dependencies eagerly. This is the load-bearing piece.
        grpc_descriptors.add_file_protos_order_tolerant(pool, file_protos)
        msg_name = _pick_message_name(file_protos)
        msg_desc = pool.FindMessageTypeByName(msg_name)
    except SerializationError:
        # Already typed (compile failure / no message); pass through with
        # its detail intact.
        raise
    except Exception as exc:
        # ImportError here means a missing extra slipped past _require_protobuf
        # (defensive — should not happen); everything else is a resolution
        # failure (multi-file imports the kernel's tolerant loader could not
        # satisfy, a missing message, etc.). Map uniformly to the brief's
        # "cannot resolve protobuf schema (multi-file imports?)" message.
        if isinstance(exc, ImportError):  # pragma: no cover - defensive
            raise ConfigError(_PROTOBUF_EXTRA_HINT, {}) from exc
        raise SerializationError(
            "cannot resolve protobuf schema (multi-file imports?)",
            {"schema_snippet": _snippet(schema_str), "error": str(exc)},
        ) from exc

    _descriptors[schema_str] = msg_desc
    return msg_desc


def decode_protobuf(raw: bytes, schema_str: str) -> dict:
    """Decode ``raw`` Protobuf bytes against ``schema_str`` to a JSON-native dict.

    No wire-framing: ``raw`` is the payload AFTER
    :func:`agctl.serialization.wire.parse_wire` has stripped the magic byte
    and schema id. Uses the kernel's :func:`deserialize` helper, which calls
    ``MessageToDict`` with ``preserving_proto_field_name=True`` (snake_case
    stays snake_case) and ``always_print_fields_with_no_presence=True``
    (zero-valued scalars stay present) — so the result is matchable by
    ``match.body`` / ``capture.from`` authored against the proto's field
    names. The parsed descriptor is cached via :func:`_message_descriptor`.

    Errors (truncated payload, unknown field, schema-resolution failure)
    surface as :class:`SerializationError` with a ``schema_snippet`` detail.
    """
    msg_desc = _message_descriptor(schema_str)
    try:
        return grpc_descriptors.deserialize(msg_desc)(raw)
    except SerializationError:
        raise
    except Exception as exc:
        raise SerializationError(
            "protobuf decode failed",
            {"schema_snippet": _snippet(schema_str), "error": str(exc)},
        ) from exc


def encode_protobuf(value: dict, schema_str: str) -> bytes:
    """Encode ``value`` against ``schema_str`` to Protobuf bytes (no framing).

    No wire-framing: the caller (Task 14 API layer) wraps the returned bytes
    with :func:`agctl.serialization.wire.build_wire` to add the magic byte
    and schema id. Uses the kernel's :func:`serialize` helper, which calls
    ``ParseDict`` with ``ignore_unknown_fields=False`` so an unknown field
    surfaces as a typed error rather than being silently dropped. The parsed
    descriptor is cached via :func:`_message_descriptor`.

    Errors (unknown field, type mismatch, schema-resolution failure) surface
    as :class:`SerializationError` with a ``schema_snippet`` detail.
    """
    msg_desc = _message_descriptor(schema_str)
    try:
        return grpc_descriptors.serialize(msg_desc)(value)
    except SerializationError:
        raise
    except Exception as exc:
        raise SerializationError(
            "protobuf encode failed",
            {"schema_snippet": _snippet(schema_str), "error": str(exc)},
        ) from exc
