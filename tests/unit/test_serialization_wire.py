"""Tests for the Confluent wire-frame kernel (pure Python, no extras).

The Confluent Schema Registry wire format is:

    1 byte magic (0x00) | 4 byte big-endian schema id | payload

These tests pin the exact byte expectations from the task brief; the
byte-level literals (e.g. ``b"\\x00\\x00\\x00\\x00\\x2a\\x18\\x00"``) are
load-bearing and must match the implementation verbatim.
"""

import pytest

from agctl.serialization.wire import (
    MAGIC_BYTE,
    build_wire,
    is_confluent_frame,
    parse_wire,
)


# --- MAGIC_BYTE constant ------------------------------------------------------

def test_magic_byte_is_zero():
    assert MAGIC_BYTE == 0x00


# --- build_wire: exact expected bytes -----------------------------------------

def test_build_wire_42_with_two_byte_payload():
    # magic=0x00, schema_id=42 (0x0000002a big-endian), payload=b"\x18\x00"
    assert build_wire(42, b"\x18\x00") == b"\x00\x00\x00\x00\x2a\x18\x00"


def test_build_wire_zero_schema_id_empty_payload():
    # magic=0x00, schema_id=0, payload=b""
    assert build_wire(0, b"") == b"\x00\x00\x00\x00\x00"


def test_build_wire_starts_with_magic_byte():
    assert build_wire(7, b"x")[0] == MAGIC_BYTE


def test_build_wire_packs_schema_id_big_endian():
    # 0x12345678 packed big-endian -> b"\x12\x34\x56\x78"
    assert build_wire(0x12345678, b"")[1:5] == b"\x12\x34\x56\x78"


def test_build_wire_preserves_payload_verbatim():
    payload = b"\x00\x01\x02\xff random bytes"
    framed = build_wire(99, payload)
    assert framed[5:] == payload


# --- parse_wire: exact expected values ----------------------------------------

def test_parse_wire_known_frame():
    assert parse_wire(b"\x00\x00\x00\x00\x2a\x18\x00") == (42, b"\x18\x00")


def test_parse_wire_zero_schema_id_empty_payload():
    assert parse_wire(b"\x00\x00\x00\x00\x00") == (0, b"")


def test_parse_wire_round_trip_typical_schema_id():
    # 305419896 == 0x12345678 — exercises all four schema-id bytes.
    assert parse_wire(build_wire(305419896, b"payload")) == (305419896, b"payload")


def test_parse_wire_round_trip_preserves_arbitrary_payload():
    payload = b"\x00\x01\x02\xff arbitrary \x00 payload"
    schema_id, parsed_payload = parse_wire(build_wire(0x0A0B0C0D, payload))
    assert schema_id == 0x0A0B0C0D
    assert parsed_payload == payload


def test_parse_wire_big_endian_schema_id():
    # magic + 0x00\x00\x00\x2a -> schema_id 42, payload b"hi"
    schema_id, payload = parse_wire(b"\x00\x00\x00\x00\x2ahi")
    assert schema_id == 42
    assert payload == b"hi"


# --- is_confluent_frame -------------------------------------------------------

def test_is_confluent_frame_true_for_minimal_valid_frame():
    assert is_confluent_frame(b"\x00\x00\x00\x00\x00") is True


def test_is_confluent_frame_true_for_longer_valid_frame():
    assert is_confluent_frame(b"\x00\x00\x00\x00\x2a\x18\x00") is True


def test_is_confluent_frame_false_for_json_payload():
    # No magic byte — plain JSON message, not wire-framed.
    assert is_confluent_frame(b'{"a":1}') is False


def test_is_confluent_frame_false_for_empty_input():
    assert is_confluent_frame(b"") is False


def test_is_confluent_frame_false_for_too_short_input():
    # Starts with magic but only 3 bytes total (< 5).
    assert is_confluent_frame(b"\x00\x00\x00") is False


def test_is_confluent_frame_false_for_non_magic_first_byte():
    assert is_confluent_frame(b"\x01\x00\x00\x00\x00") is False


# --- parse_wire error paths ---------------------------------------------------

def test_parse_wire_raises_on_non_framed_input():
    with pytest.raises(ValueError, match="not a Confluent frame"):
        parse_wire(b"not framed")


def test_parse_wire_raises_on_empty_input():
    with pytest.raises(ValueError, match="not a Confluent frame"):
        parse_wire(b"")


def test_parse_wire_raises_on_short_input_starting_with_magic():
    # 3 bytes starting with magic — not enough to hold a schema id.
    with pytest.raises(ValueError, match="not a Confluent frame"):
        parse_wire(b"\x00\x00\x00")


def test_parse_wire_raises_on_wrong_magic_byte():
    with pytest.raises(ValueError, match="not a Confluent frame"):
        parse_wire(b"\x01\x00\x00\x00\x00extra")


# --- module purity: no heavy imports -----------------------------------------

def test_module_does_not_import_heavy_serialization_libs():
    """The wire kernel must stay import-light (pure stdlib).

    Importing it must not pull in fastavro, protobuf, or confluent_kafka —
    those are lazy-loaded by the Avro/Protobuf codecs built on top of it.
    """
    import agctl.serialization.wire as wire_mod  # noqa: F401

    # Top-level heavy imports would manifest as attributes on the module.
    assert "fastavro" not in dir(wire_mod)
    assert "protobuf" not in dir(wire_mod)
    assert "confluent_kafka" not in dir(wire_mod)
    assert "google" not in dir(wire_mod)
    # Sanity: struct is what we expect to be used.
    assert "struct" in dir(wire_mod)
