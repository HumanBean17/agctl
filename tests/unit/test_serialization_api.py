"""Unit tests for the public serialization API (DESIGN §6.3).

``agctl/serialization/api.py`` composes:

* the wire kernel (:mod:`agctl.serialization.wire`: ``parse_wire`` /
  ``build_wire``) — strips/re-adds the Confluent magic byte + schema id;
* the Schema Registry client (:class:`SchemaRegistryClient`:
  ``get_schema`` / ``get_latest_schema``); and
* the Avro codec (:mod:`agctl.serialization.avro_codec`: ``decode_avro``
  / ``encode_avro``).

The JSON cases (a) and (f)/(g) do not need fastavro and run unconditionally;
the Avro round-trip cases (b)-(e) are gated on
``pytest.importorskip("fastavro")`` so CI without the ``avro`` extra skips
rather than errors. Cases (a)-(g) mirror the task brief verbatim.
"""

import json

import pytest

# JSON-only cases are defined above the importorskip, but the *Avro* test
# bodies need the codec — gate the whole test collection on fastavro by
# skipping the file when the extra is absent. The brief explicitly allows
# JSON cases to run without fastavro, so we keep this file importable and
# use per-test importorskip below instead of a module-level skip.
from agctl.errors import ConfigError, SerializationError  # noqa: F401
from agctl.serialization.api import (  # noqa: E402
    Format,
    decode_message,
    decode_payload,
    encode_payload,
    resolve_subject,
)
from agctl.serialization.avro_codec import encode_avro  # noqa: E402
from agctl.serialization.wire import build_wire, parse_wire  # noqa: E402


# Fixed Avro schema (mirrors the codec tests; record ``E`` with one field).
SCHEMA_STR = '{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}'
SCHEMA_ID = 17


# --- Fake SR (duck-typed; only the methods the api actually calls) ----------


class _FakeSR:
    """Minimal in-memory SR for the api tests.

    Exposes the three methods :func:`decode_payload` / :func:`encode_payload`
    touch: ``get_schema(id) -> (type, str)`` and
    ``get_latest_schema(subject) -> (type, str, id)``. Pre-seed
    :attr:`by_id` / :attr:`latest` before calling the api.

    A ``register_schema`` spy is also exposed — NOT because the v1 api
    should call it, but so tests can pin the no-auto-registration encode
    contract by asserting ``register_schema_calls == 0``. The
    :attr:`get_schema_calls` / :attr:`get_latest_schema_calls` /
    :attr:`register_schema_calls` counters record every call (including
    ones that raise) so a regression that swaps the resolved method, or
    silently auto-registers, trips an assertion.
    """

    def __init__(self):
        self.by_id: dict[int, tuple[str, str]] = {
            SCHEMA_ID: ("AVRO", SCHEMA_STR),
        }
        self.latest: dict[str, tuple[str, str, int]] = {
            "t-value": ("AVRO", SCHEMA_STR, SCHEMA_ID),
        }
        self.get_schema_calls = 0
        self.get_latest_schema_calls = 0
        self.register_schema_calls = 0

    def get_schema(self, schema_id):
        self.get_schema_calls += 1
        return self.by_id[schema_id]

    def get_latest_schema(self, subject):
        self.get_latest_schema_calls += 1
        return self.latest[subject]

    def register_schema(self, *args, **kwargs):
        # Present so tests can assert it is NEVER called on the encode
        # path (v1 contract: no auto-registration). A real SR client has
        # this method; a regression that silently auto-registers would
        # land here and trip ``register_schema_calls == 0``.
        self.register_schema_calls += 1
        return SCHEMA_ID


# --- (g) Format enum --------------------------------------------------------


def test_format_enum_lookup_by_value():
    # (g) Format("avro") is Format.AVRO.
    assert Format("avro") is Format.AVRO
    assert Format("json") is Format.JSON
    assert Format("protobuf") is Format.PROTOBUF


def test_format_enum_is_str():
    # Format inherits str so it serialises/compares as plain text.
    assert Format.AVRO == "avro"
    assert f"{Format.JSON}" in ("Format.JSON", "json")


# --- (a) JSON decode --------------------------------------------------------


def test_decode_payload_json_returns_parsed_dict():
    # (a) JSON: a valid JSON document decodes to the parsed object.
    raw = json.dumps({"id": "x", "n": 3}).encode()
    assert decode_payload(raw, Format.JSON, None) == {"id": "x", "n": 3}


def test_decode_payload_json_returns_string_for_non_json_bytes():
    # (a) JSON: bytes that fail to parse as JSON fall back to today's
    # _decode_bytes behavior (utf-8 with errors="replace").
    raw = b"not json at all"
    assert decode_payload(raw, Format.JSON, None) == "not json at all"


def test_decode_payload_json_handles_utf8_replacement():
    # Invalid UTF-8 in a non-JSON payload still decodes via the replace
    # policy rather than raising.
    raw = b"\xff\xfe garbage"
    result = decode_payload(raw, Format.JSON, None)
    assert isinstance(result, str)
    # Replacement char must appear (the exact rest is not load-bearing).
    assert "�" in result


# --- (b) AVRO decode --------------------------------------------------------
# Gated: requires the 'avro' extra (fastavro) for encode_avro.


def test_decode_payload_avro_returns_decoded_record():
    # (b) decode_payload(build_wire(sid, encode_avro({"id":"x"}, schema)),
    #     Format.AVRO, fake_sr) returns {"id":"x"}.
    pytest.importorskip("fastavro")
    encoded = encode_avro({"id": "x"}, SCHEMA_STR)
    raw = build_wire(SCHEMA_ID, encoded)
    fake = _FakeSR()
    assert decode_payload(raw, Format.AVRO, fake) == {"id": "x"}


# --- (c) AVRO decode rejects non-frame --------------------------------------


def test_decode_payload_avro_raises_on_non_frame():
    # (c) A non-framed raw (no magic byte / too short) raises SerializationError.
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    with pytest.raises(SerializationError) as exc_info:
        decode_payload(b"plain bytes", Format.AVRO, fake)
    detail = exc_info.value.detail
    # The detail must record which format failed.
    assert "fmt" in detail and detail["fmt"] == "avro"


def test_decode_payload_avro_raises_on_short_bytes():
    # 4 bytes is too short for a Confluent frame (need >=5).
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    with pytest.raises(SerializationError):
        decode_payload(b"\x00\x00\x00", Format.AVRO, fake)


# --- (d) AVRO encode round-trips -------------------------------------------


def test_encode_payload_avro_round_trips():
    # (d) encode_payload({"id":"x"}, Format.AVRO, fake_sr, subject="t-value")
    #     returns bytes whose parse_wire yields the registered id and whose
    #     decode_payload(...) round-trips to {"id":"x"}.
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    wire_bytes = encode_payload({"id": "x"}, Format.AVRO, fake, subject="t-value")

    sid, payload = parse_wire(wire_bytes)
    assert sid == SCHEMA_ID
    # The payload portion is non-empty Avro bytes.
    assert len(payload) > 0
    # And the wire frame round-trips through decode_payload to the original.
    assert decode_payload(wire_bytes, Format.AVRO, fake) == {"id": "x"}


def test_encode_payload_avro_uses_get_latest_schema():
    # Confirm encode resolves schema via get_latest_schema(subject) — if the
    # subject has no schema registered, encode fails (v1 contract: no
    # auto-registration).
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    fake.latest.clear()  # no subject registered
    with pytest.raises(KeyError):
        # _FakeSR raises KeyError on missing subject; a real SR surfaces
        # ConfigError (covered in test_serialization_registry). Either way,
        # encode does NOT silently succeed.
        encode_payload({"id": "x"}, Format.AVRO, fake, subject="t-value")
    # Even on the missing-subject path, encode MUST attempt
    # get_latest_schema exactly once and MUST NOT silently fall back to
    # register_schema (v1: no auto-registration).
    assert fake.get_latest_schema_calls == 1
    assert fake.register_schema_calls == 0


def test_encode_payload_avro_resolves_via_get_latest_schema_no_auto_register():
    # v1 encode contract (pinned on the happy path via call counters):
    # encode_payload resolves the schema via get_latest_schema(subject)
    # exactly once and NEVER calls register_schema (or get_schema). A
    # regression that silently auto-registers, or resolves via the wrong
    # SR method, would trip these counts.
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    encode_payload({"id": "x"}, Format.AVRO, fake, subject="t-value")
    assert fake.get_latest_schema_calls == 1
    assert fake.register_schema_calls == 0


# --- (e) AVRO encode schema-violation -> SerializationError -----------------


def test_encode_payload_avro_raises_on_schema_violation():
    # (e) encode_payload with a value violating the schema raises
    #     SerializationError carrying `subject` in its detail.
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    # fastavro in strict mode rejects records with extra fields.
    bad_value = {"id": "x", "not_in_schema": True}
    with pytest.raises(SerializationError) as exc_info:
        encode_payload(bad_value, Format.AVRO, fake, subject="t-value")
    assert exc_info.value.detail.get("subject") == "t-value"


# --- JSON encode -----------------------------------------------------------


def test_encode_payload_json_returns_encoded_bytes():
    # JSON encode is the trivial inverse of decode.
    out = encode_payload({"id": "x"}, Format.JSON, None, subject="ignored")
    assert json.loads(out) == {"id": "x"}


# --- PROTOBUF dispatch (Task 14) -------------------------------------------
#
# Mirrors the Avro cases (b)/(d) but for Protobuf: the codec lives in
# ``protobuf_codec`` and is wired into the api in Task 14. Gated on
# ``google.protobuf`` (the codec lazy-imports it via ``_require_protobuf``)
# AND ``grpc_tools`` (protoc — the compile step needs it). The schema is a
# ``.proto`` source string; the fake SR returns ``("PROTOBUF", proto)``.

PROTO_SCHEMA_STR = 'syntax = "proto3"; message E { string id = 1; }'
# A distinct id from the Avro SCHEMA_ID so a regression that hardcodes 17
# surfaces immediately; the fake SR maps this id to the PROTOBUF schema.
PROTO_SCHEMA_ID = 29


class _FakeProtobufSR:
    """Minimal SR double whose ``by_id``/``latest`` return PROTOBUF schemas.

    Duck-typed: only the two methods the api touches are implemented. The
    schema_type token is ``"PROTOBUF"`` (matches Confluent SR's
    ``schemaType`` field); the api dispatches on this token to
    :mod:`protobuf_codec`.
    """

    def __init__(self):
        self.by_id: dict[int, tuple[str, str]] = {
            PROTO_SCHEMA_ID: ("PROTOBUF", PROTO_SCHEMA_STR),
        }
        self.latest: dict[str, tuple[str, str, int]] = {
            "t-value": ("PROTOBUF", PROTO_SCHEMA_STR, PROTO_SCHEMA_ID),
        }
        self.get_schema_calls = 0
        self.get_latest_schema_calls = 0
        self.register_schema_calls = 0

    def get_schema(self, schema_id):
        self.get_schema_calls += 1
        return self.by_id[schema_id]

    def get_latest_schema(self, subject):
        self.get_latest_schema_calls += 1
        return self.latest[subject]

    def register_schema(self, *args, **kwargs):
        # Present so the v1 no-auto-registration contract can be pinned.
        self.register_schema_calls += 1
        return PROTO_SCHEMA_ID


def test_decode_payload_protobuf_returns_decoded_record():
    # decode_payload(build_wire(sid, encode_protobuf({"id":"x"}, proto)),
    #                Format.PROTOBUF, fake_sr) returns {"id":"x"}.
    pytest.importorskip("google.protobuf")
    pytest.importorskip("grpc_tools")
    from agctl.serialization.protobuf_codec import encode_protobuf

    encoded = encode_protobuf({"id": "x"}, PROTO_SCHEMA_STR)
    raw = build_wire(PROTO_SCHEMA_ID, encoded)
    fake = _FakeProtobufSR()
    assert decode_payload(raw, Format.PROTOBUF, fake) == {"id": "x"}
    # Decode resolved the writer schema via get_schema (Confluent contract).
    assert fake.get_schema_calls == 1


def test_encode_payload_protobuf_round_trips():
    # encode_payload({"id":"x"}, Format.PROTOBUF, fake_sr, subject="t-value")
    # returns wire-framed bytes whose parse_wire yields the registered id and
    # whose decode_payload(...) round-trips to {"id":"x"}.
    pytest.importorskip("google.protobuf")
    pytest.importorskip("grpc_tools")
    fake = _FakeProtobufSR()
    wire_bytes = encode_payload(
        {"id": "x"}, Format.PROTOBUF, fake, subject="t-value"
    )

    sid, payload = parse_wire(wire_bytes)
    assert sid == PROTO_SCHEMA_ID
    assert len(payload) > 0  # non-empty protobuf payload
    # Round-trip through decode_payload to the original record.
    assert decode_payload(wire_bytes, Format.PROTOBUF, fake) == {"id": "x"}
    # Encode resolved the latest schema for the subject exactly once and
    # never auto-registered.
    assert fake.get_latest_schema_calls == 1
    assert fake.register_schema_calls == 0


def test_encode_payload_protobuf_resolves_via_get_latest_schema_no_auto_register():
    # Pin the v1 encode contract for Protobuf the same way the Avro test
    # does: get_latest_schema(subject) is called exactly once, register_schema
    # never.
    pytest.importorskip("google.protobuf")
    pytest.importorskip("grpc_tools")
    fake = _FakeProtobufSR()
    encode_payload({"id": "x"}, Format.PROTOBUF, fake, subject="t-value")
    assert fake.get_latest_schema_calls == 1
    assert fake.register_schema_calls == 0


def test_decode_payload_protobuf_without_sr_raises_config_error():
    # The `sr is None` -> ConfigError path fires BEFORE any codec call, so
    # this test deliberately does NOT importorskip("google.protobuf"): it
    # must run on protobuf-less CI to cover the config-error branch.
    with pytest.raises(ConfigError):
        decode_payload(b"\x00\x00\x00\x00\x00x", Format.PROTOBUF, None)


def test_encode_payload_protobuf_without_sr_raises_config_error():
    # As above: no google.protobuf needed — ConfigError is raised before the
    # codec/SR resolution path is entered.
    with pytest.raises(ConfigError):
        encode_payload({"id": "x"}, Format.PROTOBUF, None, subject="t-value")


def test_decode_payload_protobuf_raises_serialization_error_on_non_frame():
    # A non-framed raw (no magic byte) surfaces as SerializationError, same
    # shape as the Avro branch.
    pytest.importorskip("google.protobuf")
    pytest.importorskip("grpc_tools")
    fake = _FakeProtobufSR()
    with pytest.raises(SerializationError) as exc_info:
        decode_payload(b"plain bytes", Format.PROTOBUF, fake)
    detail = exc_info.value.detail
    assert detail.get("fmt") == "protobuf"


# --- missing SR for non-JSON -----------------------------------------------


def test_decode_payload_avro_without_sr_raises_config_error():
    # The `sr is None` -> ConfigError path fires BEFORE any codec call,
    # so this test deliberately does NOT importorskip("fastavro"): it
    # must run on fastavro-less CI to cover the config-error branch.
    with pytest.raises(ConfigError):
        decode_payload(b"\x00\x00\x00\x00\x00x", Format.AVRO, None)


def test_encode_payload_avro_without_sr_raises_config_error():
    # As above: no fastavro needed — ConfigError is raised before the
    # codec/SR resolution path is entered.
    with pytest.raises(ConfigError):
        encode_payload({"id": "x"}, Format.AVRO, None, subject="t-value")


# --- (f) resolve_subject ---------------------------------------------------


def test_resolve_subject_topic_strategy():
    # (f) resolve_subject("orders.created","value","topic",None)
    #     == "orders.created-value".
    assert resolve_subject("orders.created", "value", "topic", None) == (
        "orders.created-value"
    )
    assert resolve_subject("orders.created", "key", "topic", None) == (
        "orders.created-key"
    )


def test_resolve_subject_record_falls_back_when_no_record_name():
    # In v1 the schema is not available inside resolve_subject, so the
    # record strategy falls back to topic-style when no record name is
    # attached to the value.
    assert resolve_subject("orders", "value", "record", None) == "orders-value"


def test_resolve_subject_record_uses_attached_record_name():
    # A caller may attach the record name (e.g. from a known Avro schema)
    # via the reserved ``__record_name__`` key.
    value = {"id": "x", "__record_name__": "OrderEvent"}
    assert resolve_subject("orders", "value", "record", value) == "OrderEvent"
    assert resolve_subject("orders", "value", "topic_record", value) == (
        "orders-OrderEvent"
    )


def test_resolve_subject_unknown_strategy_raises():
    with pytest.raises(ConfigError):
        resolve_subject("t", "value", "bogus", None)


# --- decode_message convenience --------------------------------------------


def test_decode_message_avro_value_string_key():
    # Value goes through Avro decode; key is a utf-8 string.
    pytest.importorskip("fastavro")
    fake = _FakeSR()
    value_raw = build_wire(SCHEMA_ID, encode_avro({"id": "x"}, SCHEMA_STR))
    key_raw = b"order-123"

    value, key = decode_message(
        value_raw,
        key_raw,
        value_fmt=Format.AVRO,
        key_fmt=Format.KEY_STRING,
        sr=fake,
    )

    assert value == {"id": "x"}
    assert key == "order-123"


def test_decode_message_json_value_json_key():
    value_raw = json.dumps({"a": 1}).encode()
    key_raw = b"the-key"
    value, key = decode_message(
        value_raw,
        key_raw,
        value_fmt=Format.JSON,
        key_fmt=Format.KEY_STRING,
        sr=None,
    )
    assert value == {"a": 1}
    assert key == "the-key"


# --- module shape ---------------------------------------------------------


def test_api_re_exports_public_names_from_package():
    # The package __init__ re-exports the api surface so consumers import
    # from `agctl.serialization` directly.
    import agctl.serialization as pkg

    for name in ("Format", "decode_payload", "encode_payload", "resolve_subject", "decode_message"):
        assert hasattr(pkg, name), f"missing public re-export: {name}"
