"""Unit tests for the lazy Protobuf codec (DynamicMessage via grpc_descriptors).

The codec (``agctl/serialization/protobuf_codec.py``) decodes/encodes the
Protobuf payload AFTER the Confluent wire-frame kernel has stripped the
magic byte + schema id (decode) and BEFORE it adds them back (encode). It
does NOT do wire-framing itself; that is the API layer's job (Task 14).

``protobuf`` / ``grpc_tools`` are lazy-imported inside the codec functions,
so the module imports cleanly without the extra. These tests are gated on
``pytest.importorskip("google.protobuf")`` AND ``grpc_tools`` so CI without
the extra sees skips, not errors; locally (with the extra installed) they
run and pass.

The fixed single-message ``.proto`` source string and the round-trip /
encode-decode / malformed-schema / cache cases mirror the task brief
verbatim.
"""

import pytest

# Module-level skip: preserves the project invariant that the unit suite
# stays collectable without optional extras installed. Both ship with the
# ``grpc`` extra, already in the venv.
pytest.importorskip("google.protobuf")
pytest.importorskip("grpc_tools")

from agctl.serialization.protobuf_codec import (  # noqa: E402
    _compile_proto_string,
    _descriptors,
    _message_descriptor,
    _require_protobuf,
    decode_protobuf,
    encode_protobuf,
)
from agctl.errors import ConfigError, SerializationError  # noqa: E402


# Fixed single-message .proto schema (the brief's ``E`` with one ``id`` field).
SCHEMA_STR = 'syntax = "proto3"; message E { string id = 1; }'


# --- _require_protobuf ------------------------------------------------------


def test_require_protobuf_returns_modules_when_installed():
    """With the extra installed the helper returns the lazy-imported modules."""
    descriptor_pool, descriptor_pb2, protoc = _require_protobuf()
    assert descriptor_pool.__name__ == "google.protobuf.descriptor_pool"
    assert descriptor_pb2.__name__ == "google.protobuf.descriptor_pb2"
    assert protoc.__name__ == "grpc_tools.protoc"


def test_require_protobuf_raises_config_error_when_missing(monkeypatch):
    """A missing extra must surface as ConfigError with the 'protobuf' hint."""

    import builtins
    import sys

    real_import = builtins.__import__

    def _block_protobuf(name, *args, **kwargs):
        if name.startswith(("google.protobuf", "grpc_tools")):
            raise ImportError(f"simulated: no module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_protobuf)
    for mod in list(sys.modules):
        if mod.startswith(("google.protobuf", "grpc_tools")):
            monkeypatch.delitem(sys.modules, mod, raising=False)

    with pytest.raises(ConfigError, match="protobuf' extra"):
        _require_protobuf()


# --- round-trip: case (a) ---------------------------------------------------


def test_round_trip_encode_then_decode_returns_original_record():
    """decode_protobuf(encode_protobuf({"id":"x"}, proto), proto) == {"id":"x"}."""
    encoded = encode_protobuf({"id": "x"}, SCHEMA_STR)
    assert decode_protobuf(encoded, SCHEMA_STR) == {"id": "x"}


# --- encode-decode: case (b) ------------------------------------------------


def test_encode_returns_bytes():
    """Encoded output is bytes (no wire framing — caller's responsibility)."""
    encoded = encode_protobuf({"id": "abc"}, SCHEMA_STR)
    assert isinstance(encoded, bytes)
    # Protobuf field-1 string "abc": tag 0x0a (field 1, wire-type 2) + len 3.
    assert encoded == b"\x0a\x03abc"


def test_decode_of_encoded_bytes_returns_original_record():
    """Round-trip via plain bytes: decode_protobuf(encoded) == original."""
    encoded = encode_protobuf({"id": "abc"}, SCHEMA_STR)
    assert decode_protobuf(encoded, SCHEMA_STR) == {"id": "abc"}


def test_decode_of_handwritten_wire_bytes_succeeds():
    """Decode accepts hand-written protobuf wire bytes (not just our encoder)."""
    # field 1 (string), len 2, "hi" — same encoding the kernel tests use.
    raw = b"\x0a\x02hi"
    assert decode_protobuf(raw, SCHEMA_STR) == {"id": "hi"}


# --- malformed proto: case (c) ----------------------------------------------


def test_malformed_proto_string_raises_serialization_error():
    """A deliberately malformed .proto must surface as SerializationError.

    Fail-loud invariant: a schema that protoc rejects NEVER silently passes —
    the codec raises SerializationError with a schema_snippet in the detail.
    """
    bad = 'syntax = "proto3"; message { not valid'
    with pytest.raises(SerializationError) as exc_info:
        encode_protobuf({"id": "x"}, bad)
    # The detail must carry a schema_snippet so callers can diagnose.
    assert "schema_snippet" in exc_info.value.detail


def test_malformed_proto_string_raises_serialization_error_on_decode():
    """Same fail-loud invariant on the decode path."""
    bad = "this is not a valid proto"
    with pytest.raises(SerializationError):
        decode_protobuf(b"\x0a\x01x", bad)


def test_proto_with_unknown_field_type_raises_serialization_error():
    """A schema referencing an unknown type fails compilation -> SerializationError."""
    bad = 'syntax = "proto3"; message E { NoSuchType id = 1; }'
    with pytest.raises(SerializationError):
        encode_protobuf({"id": "x"}, bad)


# --- cached _message_descriptor: case (d) -----------------------------------


def test_message_descriptor_returns_same_object_for_same_schema_string():
    """A repeated call with the same schema string hits the cache."""
    _descriptors.clear()
    first = _message_descriptor(SCHEMA_STR)
    second = _message_descriptor(SCHEMA_STR)
    assert first is second  # cached: identical object identity
    assert SCHEMA_STR in _descriptors


def test_message_descriptor_caches_distinct_schemas_independently():
    """Two different schema strings get distinct cache entries."""
    _descriptors.clear()
    other = 'syntax = "proto3"; message F { string id = 1; }'
    a = _message_descriptor(SCHEMA_STR)
    b = _message_descriptor(other)
    assert a is not b
    assert SCHEMA_STR in _descriptors
    assert other in _descriptors


def test_repeated_decode_does_not_recompile_schema(monkeypatch):
    """decode_protobuf must not re-compile on every call (cache keyed by schema).

    Asserted by wrapping the protoc entrypoint with a counter and ensuring the
    second decode does NOT invoke it again.
    """
    import grpc_tools.protoc as _protoc

    _descriptors.clear()
    calls = {"n": 0}
    real = _protoc.main

    def counting(*argv, **kwargs):
        calls["n"] += 1
        return real(*argv, **kwargs)

    monkeypatch.setattr(_protoc, "main", counting)

    decode_protobuf(encode_protobuf({"id": "x"}, SCHEMA_STR), SCHEMA_STR)
    decode_protobuf(encode_protobuf({"id": "y"}, SCHEMA_STR), SCHEMA_STR)

    assert calls["n"] == 1, f"expected protoc.main once, got {calls['n']}"


# --- _compile_proto_string (low-level entry) --------------------------------


def test_compile_proto_string_returns_file_descriptor_protos():
    """The low-level compile helper returns FileDescriptorProto objects."""
    file_protos = _compile_proto_string(SCHEMA_STR)
    assert len(file_protos) >= 1
    # The single message ``E`` appears as a top-level message_type.
    names = [mt.name for fd in file_protos for mt in fd.message_type]
    assert "E" in names


def test_compile_proto_string_raises_serialization_error_on_malformed():
    """protoc failure (rc != 0) surfaces as SerializationError, not a bare rc."""
    with pytest.raises(SerializationError) as exc_info:
        _compile_proto_string("not a proto")
    assert "schema_snippet" in exc_info.value.detail


# --- multi-message schema: pick last ----------------------------------------


def test_multi_message_schema_picks_last_message():
    """v1 fallback: a multi-message schema picks the last declared message.

    The brief specifies this behavior for multi-message schemas when no record
    name is supplied. Verifies the round-trip resolves against the LAST message.
    """
    multi = (
        'syntax = "proto3";'
        " message First { string a = 1; }"
        " message Last { string b = 1; }"
    )
    # Encoding a ``Last`` record (field ``b``) round-trips through the schema.
    encoded = encode_protobuf({"b": "hi"}, multi)
    assert decode_protobuf(encoded, multi) == {"b": "hi"}


# --- module purity: lazy import --------------------------------------------


def test_module_does_not_import_protobuf_at_top_level():
    """The codec module top-level must import only stdlib + the kernel + errors.

    Importing it must not bind ``google``, ``grpc_tools`` etc. as a module
    attribute — those are lazy-loaded inside the functions via _require_protobuf.
    """
    import agctl.serialization.protobuf_codec as mod

    # These may have been imported by *our* test code by now, but the codec
    # module itself must not have them as attributes.
    assert "google" not in dir(mod)
    assert "grpc_tools" not in dir(mod)
    assert "google protobuf" not in " ".join(dir(mod))
    # Stdlib + kernel + errors ARE expected at top level.
    assert "tempfile" in dir(mod)
    assert "grpc_descriptors" in dir(mod)
    assert "SerializationError" in dir(mod)


# --- well-known-type imports (google/protobuf/*.proto) ----------------------
#
# protoc invocation must put grpc_tools' bundled WKT protos on --proto_path so
# a single-file schema that imports ``google/protobuf/timestamp.proto``
# (ubiquitous in real schemas) resolves. Without it protoc fails on the import
# and the codec raises SerializationError — a regression this test pins.


def test_schema_importing_well_known_timestamp_proto_compiles_and_decodes():
    """A single-file schema importing ``google/protobuf/timestamp.proto``
    compiles and decodes — proving the WKT protos ship on the protoc
    ``--proto_path`` via ``grpc_tools._proto``.

    Pre-fix: protoc returned non-zero (``google/protobuf/timestamp.proto:
    File not found."``), the codec raised SerializationError, and every
    schema with a WKT import was unusable.

    The F6 fix targets protoc compilation (the ``--proto_path`` append);
    the json_format WKT-string coercion (RFC3339 for Timestamp) is a
    json_format behavior beyond F6's scope, so this test exercises the
    COMPILE + raw-wire DECODE path rather than the encode round-trip.
    """
    _descriptors.clear()  # avoid cache hits across tests
    schema = (
        'syntax = "proto3";\n'
        "import \"google/protobuf/timestamp.proto\";\n"
        "message E {\n"
        "  string id = 1;\n"
        "  google.protobuf.Timestamp ts = 2;\n"
        "}\n"
    )

    # (1) Schema compiles: protoc finds the WKT import on --proto_path.
    #     Pre-fix this raised SerializationError("cannot compile ...").
    file_protos = _compile_proto_string(schema)
    names = [mt.name for fd in file_protos for mt in fd.message_type]
    assert "E" in names

    # (2) Descriptor resolves in a real DescriptorPool — the imported
    #     ``google.protobuf.Timestamp`` symbol is findable.
    desc = _message_descriptor(schema)
    assert desc.full_name == "E"
    ts_field = next(f for f in desc.fields if f.name == "ts")
    assert ts_field.message_type.full_name == "google.protobuf.Timestamp"

    # (3) Decode raw wire bytes: field 1 (id, string) = "x"; field 2 (ts)
    #     omitted -> default empty Timestamp. The codec decodes without
    #     raising and the decoded id round-trips.
    raw = b"\x0a\x01x"  # field 1: tag 0x0a, len 1, "x"
    decoded = decode_protobuf(raw, schema)
    assert decoded["id"] == "x"
