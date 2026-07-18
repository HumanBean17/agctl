"""Unit tests for the lazy fastavro Avro codec.

The codec (``agctl/serialization/avro_codec.py``) decodes/encodes the
Avro payload AFTER the Confluent wire-frame kernel has stripped the
magic byte + schema id (decode) and BEFORE it adds them back (encode).
It does NOT do wire-framing itself; that is the API layer's job (Task 7).

``fastavro`` is lazy-imported inside the codec functions, so the module
imports cleanly without the extra. These tests are gated on
``pytest.importorskip("fastavro")`` so CI without the extra sees skips,
not errors; locally (with the extra installed) they run and pass.

The fixed Avro schema and round-trip / encode-decode / extra-field /
cache cases mirror the task brief verbatim.
"""

import pytest

# Gated: the entire file is skipped when the 'avro' extra (fastavro) is
# absent. Locally with the extra installed, every case below runs.
fastavro = pytest.importorskip("fastavro")

from agctl.serialization.avro_codec import (  # noqa: E402
    _parsed,
    _require_fastavro,
    decode_avro,
    encode_avro,
    parse_schema,
)
from agctl.errors import ConfigError  # noqa: E402


# Fixed Avro schema (the brief's ``E`` record with a single ``id`` field).
SCHEMA_STR = '{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}'


# --- _require_fastavro ------------------------------------------------------


def test_require_fastavro_returns_module_when_installed():
    """With the extra installed the helper returns the fastavro module."""
    import fastavro as _fa

    assert _require_fastavro() is _fa


def test_require_fastavro_raises_config_error_when_missing(monkeypatch):
    """A missing extra must surface as ConfigError, never bare ImportError."""

    import builtins

    real_import = builtins.__import__

    def _block_fastavro(name, *args, **kwargs):
        if name == "fastavro":
            raise ImportError("simulated: no module named 'fastavro'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_fastavro)
    # Also purge any cached import so the blocked __import__ is exercised.
    monkeypatch.delitem(
        __import__("sys").modules, "fastavro", raising=False
    )

    with pytest.raises(ConfigError, match="avro' extra"):
        _require_fastavro()


# --- round-trip: case (a) ---------------------------------------------------


def test_round_trip_encode_then_decode_returns_original_record():
    """decode_avro(encode_avro({"id":"x"}, schema), schema) == {"id":"x"}."""
    encoded = encode_avro({"id": "x"}, SCHEMA_STR)
    assert decode_avro(encoded, SCHEMA_STR) == {"id": "x"}


# --- encode-decode: case (b) ------------------------------------------------


def test_encode_returns_bytes_starting_with_avro_payload_not_magic():
    """Encoded output is bytes; the first byte is NOT the Confluent magic 0x00.

    The codec does no framing, so the bytes are the raw Avro payload —
    the magic byte / schema id are the caller's responsibility (Task 7).
    """
    encoded = encode_avro({"id": "abc"}, SCHEMA_STR)
    assert isinstance(encoded, bytes)
    # Avro string payload begins with a zigzag length marker (0x06 for
    # "abc" -> 3 chars -> 6 in zigzag), never the 0x00 wire magic byte.
    assert encoded[0] != 0x00
    assert encoded == b"\x06abc"


def test_decode_of_encoded_bytes_returns_original_record():
    """Round-trip via plain bytes: decode_avro(encoded) == original."""
    encoded = encode_avro({"id": "abc"}, SCHEMA_STR)
    assert decode_avro(encoded, SCHEMA_STR) == {"id": "abc"}


# --- extra-field raises: case (c) -------------------------------------------


def test_encode_raises_on_field_not_in_schema():
    """fastavro rejects a record with a field absent from the schema.

    The codec surfaces the fastavro error directly (Task 7's API layer
    wraps encode failures as SerializationError); here we just assert it
    propagates rather than silently dropping the field.
    """
    with pytest.raises(Exception):
        encode_avro({"id": "abc", "extra": "no"}, SCHEMA_STR)


# --- cached parse_schema: case (d) ------------------------------------------


def test_parse_schema_returns_same_object_for_same_schema_string():
    """A repeated call with the same schema string hits the cache."""
    # Prime the cache for the fixed schema (decode_avro/encode_avro may
    # already have done so; clear it to make the test self-contained).
    _parsed.clear()
    first = parse_schema(SCHEMA_STR)
    second = parse_schema(SCHEMA_STR)
    assert first is second  # cached: identical object identity
    assert SCHEMA_STR in _parsed


def test_parse_schema_caches_distinct_schemas_independently():
    """Two different schema strings get distinct cache entries."""
    _parsed.clear()
    other = '{"type":"record","name":"F","fields":[{"name":"id","type":"string"}]}'
    a = parse_schema(SCHEMA_STR)
    b = parse_schema(other)
    assert a is not b
    assert SCHEMA_STR in _parsed
    assert other in _parsed


def test_repeated_decode_uses_cached_parsed_schema(monkeypatch):
    """decode_avro must not re-parse on every call (cache keyed by schema str).

    Asserted by wrapping fastavro.parse_schema with a counter and ensuring
    the second decode does NOT call it again.
    """
    import fastavro as _fa

    _parsed.clear()
    calls = {"n": 0}
    real = _fa.parse_schema

    def counting(parsed_schema):
        calls["n"] += 1
        return real(parsed_schema)

    monkeypatch.setattr(_fa, "parse_schema", counting)

    decode_avro(encode_avro({"id": "x"}, SCHEMA_STR), SCHEMA_STR)
    decode_avro(encode_avro({"id": "y"}, SCHEMA_STR), SCHEMA_STR)

    assert calls["n"] == 1, f"expected parse_schema once, got {calls['n']}"


# --- schema-string handling -------------------------------------------------


def test_parse_schema_accepts_json_schema_string():
    """parse_schema takes the SR-style JSON string (json.loads then parse)."""
    _parsed.clear()
    parsed = parse_schema(SCHEMA_STR)
    # The parsed schema object carries the record name; identity check is
    # covered by the cache tests. Here just confirm it is usable.
    assert parsed is not None


def test_decode_avro_handles_scalar_schema_string():
    """A scalar Avro schema (e.g. "string") decodes to a Python scalar."""
    scalar_schema = '"string"'
    encoded = encode_avro("hello", scalar_schema)
    assert decode_avro(encoded, scalar_schema) == "hello"


# --- module purity: lazy import --------------------------------------------


def test_module_does_not_import_fastavro_at_top_level():
    """The codec module top-level must import only stdlib.

    Importing it must not bind ``fastavro`` as a module attribute —
    fastavro is lazy-loaded inside the functions via _require_fastavro().
    """
    import agctl.serialization.avro_codec as mod

    # fastavro may have been imported by *our* test code by now, but the
    # codec module itself must not have it as an attribute.
    assert "fastavro" not in dir(mod)
    # Stdlib modules ARE expected at top level.
    assert "io" in dir(mod)
    assert "json" in dir(mod)
