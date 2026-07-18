"""Public serialization API — the agent-facing decode/encode entry points.

This module composes the building blocks merged in Tasks 3-6:

* :class:`agctl.errors.SerializationError` — codec / schema-conformance
  failures (exit 2); propagated to the caller which decides fatal-vs-skip;
* :mod:`agctl.serialization.wire` — the pure Confluent wire-frame kernel
  (:func:`parse_wire` / :func:`build_wire`);
* :class:`agctl.serialization.registry.SchemaRegistryClient` — the cached,
  error-mapped SR wrapper (:meth:`get_schema`, :meth:`get_latest_schema`);
* :mod:`agctl.serialization.avro_codec` — the lazy ``fastavro`` codec
  (:func:`decode_avro` / :func:`encode_avro`).

The codec for Protobuf lands in Task 13 and is wired here in Task 14;
until then the ``Format.PROTOBUF`` branch raises :class:`ConfigError`
pointing at the ``protobuf`` extra (no half-implementation).

Design invariants:

* JSON never needs a Schema Registry client (``sr`` may be ``None``).
* Avro always needs an SR client — decode to resolve the writer schema
  by id, encode to resolve the latest schema for the subject.
* v1 encode contract: no auto-registration. The subject must already
  have a schema; :meth:`SchemaRegistryClient.get_latest_schema` surfaces
  a missing subject as :class:`ConfigError`.
* Codec failures (truncated payload, schema-violating record) propagate
  as :class:`SerializationError` so the caller can decide whether the
  failure is fatal or per-message-skippable.
"""

from __future__ import annotations

import enum
import json
from typing import Any

from ..errors import ConfigError, SerializationError
from . import avro_codec
from .registry import SchemaRegistryClient
from .wire import build_wire, parse_wire


class Format(str, enum.Enum):
    """Payload format for a Kafka value or key.

    ``str``-based so it serialises and compares as plain text:
    ``Format("avro") is Format.AVRO``. ``KEY_STRING`` is the format used
    for plain-string keys (the default for keys); it bypasses the SR /
    codec path and decodes/encodes as UTF-8 like the legacy
    ``_decode_bytes`` behavior.
    """

    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    KEY_STRING = "string"


_PROTOBUF_EXTRA_MSG = (
    "Protobuf codec requires the 'protobuf' extra: pip install 'agctl[protobuf]'"
)


def _decode_bytes(raw):
    """Mirror :func:`agctl.clients.kafka_client._decode_bytes` for non-JSON bytes.

    Returns ``None`` for ``None`` input; otherwise UTF-8 decodes with
    ``errors="replace"`` so an undecodable tail never crashes a consume
    loop. Kept here (rather than imported from the kafka client) so the
    serialization surface has no upstream dependency on the kafka client
    module — the wire/codec/SR stack is reusable independent of Kafka.
    """
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return raw


def decode_payload(
    raw: bytes, fmt: Format, sr: SchemaRegistryClient | None
) -> Any:
    """Decode ``raw`` per ``fmt``, resolving schemas via ``sr`` when needed.

    * ``Format.JSON`` — :func:`json.loads` ``raw``; if it does not parse,
      fall back to :func:`_decode_bytes` (preserve today's behavior for
      non-JSON bytes — UTF-8 with ``errors="replace"``).
    * ``Format.KEY_STRING`` — :func:`_decode_bytes` (plain string).
    * ``Format.AVRO`` — split the Confluent wire frame
      (:func:`wire.parse_wire`); a non-frame raises
      :class:`SerializationError`. Then ``sr.get_schema(schema_id)`` to
      resolve the writer schema and dispatch to
      :func:`avro_codec.decode_avro`. A missing ``sr`` is
      :class:`ConfigError`. Codec failures propagate as
      :class:`SerializationError` so the caller decides fatal-vs-skip.
    * ``Format.PROTOBUF`` — :class:`ConfigError` until Task 14.
    """
    if fmt == Format.JSON:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            return _decode_bytes(raw)
    if fmt == Format.KEY_STRING:
        return _decode_bytes(raw)
    if fmt == Format.PROTOBUF:
        # Wired in Task 14; until then this is a deliberate placeholder.
        raise ConfigError(_PROTOBUF_EXTRA_MSG, {"fmt": "protobuf"})
    if fmt == Format.AVRO:
        if sr is None:
            raise ConfigError(
                "Cannot decode Avro payload without a Schema Registry client",
                {"fmt": "avro"},
            )
        try:
            schema_id, payload = parse_wire(raw)
        except ValueError as exc:
            raise SerializationError(
                "not a Confluent frame", {"fmt": "avro"}
            ) from exc
        schema_type, schema_str = sr.get_schema(schema_id)
        try:
            return avro_codec.decode_avro(payload, schema_str)
        except SerializationError:
            raise
        except Exception as exc:  # noqa: BLE001 - codec boundary: wrap to SerializationError
            raise SerializationError(
                f"Avro decode failed: {exc}",
                {"fmt": "avro", "schema_id": schema_id},
            ) from exc
    raise ConfigError(f"Unsupported decode format: {fmt!r}", {"fmt": str(fmt)})


def encode_payload(
    value: Any,
    fmt: Format,
    sr: SchemaRegistryClient | None,
    *,
    subject: str,
) -> bytes:
    """Encode ``value`` per ``fmt`` against the latest schema for ``subject``.

    * ``Format.JSON`` — ``json.dumps(value).encode()``.
    * ``Format.KEY_STRING`` — UTF-8 encode of ``str(value)``.
    * ``Format.AVRO`` — resolve the schema via
      ``sr.get_latest_schema(subject)`` (v1 contract: the subject must
      already have a schema; missing subjects surface as
      :class:`ConfigError` from the SR client), encode via
      :func:`avro_codec.encode_avro`, then wrap with
      :func:`wire.build_wire`. Encode-time codec failures (e.g. fields
      not in the schema, picked up via fastavro's ``strict=True``)
      surface as :class:`SerializationError` with ``subject`` (and
      ``schema_id``) in the detail.
    * ``Format.PROTOBUF`` — :class:`ConfigError` until Task 14.
    """
    if fmt == Format.JSON:
        return json.dumps(value).encode()
    if fmt == Format.KEY_STRING:
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")
    if fmt == Format.PROTOBUF:
        raise ConfigError(_PROTOBUF_EXTRA_MSG, {"fmt": "protobuf"})
    if fmt == Format.AVRO:
        if sr is None:
            raise ConfigError(
                "Cannot encode Avro payload without a Schema Registry client",
                {"fmt": "avro", "subject": subject},
            )
        schema_type, schema_str, schema_id = sr.get_latest_schema(subject)
        try:
            encoded = avro_codec.encode_avro(value, schema_str)
        except SerializationError:
            raise
        except Exception as exc:  # noqa: BLE001 - codec boundary: wrap to SerializationError
            raise SerializationError(
                f"Avro encode failed for subject {subject!r}: {exc}",
                {"fmt": "avro", "subject": subject, "schema_id": schema_id},
            ) from exc
        return build_wire(schema_id, encoded)
    raise ConfigError(f"Unsupported encode format: {fmt!r}", {"fmt": str(fmt)})


def resolve_subject(
    topic: str, which: str, strategy: str, value: dict | None
) -> str:
    """Return the encode subject for ``topic`` / ``which`` under ``strategy``.

    Mirrors the Confluent subject-name strategies:

    * ``"topic"`` → ``f"{topic}-{which}"`` (``which`` is ``"value"`` or
      ``"key"``).
    * ``"record"`` → the record name (Avro ``schema.name``). For v1 the
      schema is not available inside this function, so the record name
      is taken from ``value["__record_name__"]`` if attached; otherwise
      the strategy falls back to ``f"{topic}-{which}"`` (a logged event
      is deferred to Task 9's resolver integration).
    * ``"topic_record"`` → ``f"{topic}-{record_name}"`` (same v1
      fallback as ``"record"``).

    The fallback keeps encode predictable when the schema is not yet
    known: callers that want true record-name subjects must attach the
    name to ``value`` (Task 9's resolver will do this once it has the
    schema in hand).
    """
    if strategy == "topic":
        return f"{topic}-{which}"
    if strategy in ("record", "topic_record"):
        record_name = _record_name_from_value(value)
        if record_name is None:
            return f"{topic}-{which}"
        if strategy == "record":
            return record_name
        return f"{topic}-{record_name}"
    raise ConfigError(
        f"Unknown subject strategy: {strategy!r}", {"strategy": strategy}
    )


def _record_name_from_value(value: dict | None) -> str | None:
    """Best-effort Avro record-name extraction for v1 subject strategies.

    The record name lives on the schema, not the datum; without the
    schema in scope here we look at a reserved ``__record_name__`` key
    the caller may attach. Returns ``None`` when absent, signaling the
    caller should fall back to the topic-name strategy.
    """
    if isinstance(value, dict):
        name = value.get("__record_name__")
        if isinstance(name, str) and name:
            return name
    return None


def decode_message(
    value_raw,
    key_raw,
    *,
    value_fmt: Format,
    key_fmt: Format,
    sr: SchemaRegistryClient | None,
) -> tuple[Any, Any]:
    """Convenience wrapper: decode value and key per their formats.

    Returns ``(value, key)``. A ``Format.KEY_STRING`` key bypasses the
    codec/SR path and decodes as UTF-8 via :func:`_decode_bytes`
    (preserving today's Kafka key handling). All other key formats go
    through :func:`decode_payload` with the same SR client.
    """
    if key_fmt == Format.KEY_STRING:
        key = _decode_bytes(key_raw)
    else:
        key = decode_payload(key_raw, key_fmt, sr)
    value = decode_payload(value_raw, value_fmt, sr)
    return value, key
