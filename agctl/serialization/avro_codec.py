"""Lazy ``fastavro`` Avro codec (DESIGN §6.2).

Operates on the Avro payload AFTER :func:`agctl.serialization.wire.parse_wire`
has stripped the Confluent magic byte + schema id (decode) and BEFORE
:func:`agctl.serialization.wire.build_wire` re-adds them (encode). It does
NOT do wire-framing itself — that is the API layer's job (Task 7).

``fastavro`` is lazy-imported inside the functions that need it via
:func:`_require_fastavro`. The module top imports only stdlib (``io``,
``json``) so it imports cleanly even when the ``avro`` extra is absent;
a missing extra surfaces as :class:`ConfigError` pointing at
``pip install 'agctl[avro]'``, never a bare :class:`ImportError`.

An Avro schema string from Confluent Schema Registry is a JSON string
(e.g. ``'{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}'``)
— :func:`parse_schema` :func:`json.loads` it before handing to
:func:`fastavro.parse_schema`. The parsed schema is cached module-level
keyed by the schema string so repeated decode/encode of the same schema
does not re-parse.
"""

from __future__ import annotations

import io
import json

from ..errors import ConfigError

# Module-level cache of parsed schemas keyed by the schema string. The
# parsed-schema object is conceptually immutable (it is a dict produced
# by fastavro.parse_schema), so a process-wide cache is safe. The wire
# codecs are pure functions of (payload, schema_str) and have no
# per-instance state, so a module-level cache is also the right scope.
_parsed: dict[str, object] = {}


def _require_fastavro():
    """Lazy-import and return :mod:`fastavro`.

    Raises :class:`ConfigError` with an install hint when the ``avro``
    extra is absent — never a bare :class:`ImportError`. Called from
    inside :func:`decode_avro` / :func:`encode_avro` so module import
    stays cheap and extra-free.
    """
    try:
        import fastavro  # noqa: PLC0415 — deliberate lazy import
    except ImportError as exc:  # pragma: no cover - env-dependent
        raise ConfigError(
            "Avro codec requires the 'avro' extra: pip install 'agctl[avro]'",
            {},
        ) from exc
    return fastavro


def parse_schema(schema_str: str):
    """Parse and cache an Avro schema string.

    ``schema_str`` is the JSON string Confluent Schema Registry stores
    (e.g. ``'{"type":"record",...}'`` or a scalar like ``'"string"'``).
    It is :func:`json.loads`-ed then handed to
    :func:`fastavro.parse_schema`; the result is cached module-level
    keyed by ``schema_str`` so repeated decode/encode of the same schema
    does not re-parse per message.
    """
    cached = _parsed.get(schema_str)
    if cached is not None:
        return cached

    fastavro = _require_fastavro()
    parsed = fastavro.parse_schema(json.loads(schema_str))
    _parsed[schema_str] = parsed
    return parsed


def decode_avro(raw: bytes, schema_str: str):
    """Decode ``raw`` Avro bytes against ``schema_str`` to a JSON-native object.

    No wire-framing: ``raw`` is the payload AFTER
    :func:`agctl.serialization.wire.parse_wire` has stripped the magic
    byte and schema id. Returns a ``dict`` (record), ``list`` (array),
    or scalar (int/float/str/bool/None) depending on the schema. The
    parsed schema is cached via :func:`parse_schema`.

    fastavro errors (e.g. truncated payload) propagate unchanged; the
    Task 7 API layer wraps them as :class:`SerializationError`.
    """
    fastavro = _require_fastavro()
    parsed = parse_schema(schema_str)
    return fastavro.schemaless_reader(io.BytesIO(raw), parsed)


def encode_avro(value, schema_str: str) -> bytes:
    """Encode ``value`` against ``schema_str`` to Avro bytes (no framing).

    No wire-framing: the caller (Task 7 API layer) wraps the returned
    bytes with :func:`agctl.serialization.wire.build_wire` to add the
    magic byte and schema id. The parsed schema is cached via
    :func:`parse_schema`.

    ``strict=True`` makes fastavro reject records with fields not in the
    schema (a config/contract bug) instead of silently dropping them;
    the resulting fastavro error propagates unchanged so the Task 7 API
    layer can wrap encode failures as :class:`SerializationError`.
    """
    fastavro = _require_fastavro()
    parsed = parse_schema(schema_str)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, value, strict=True)
    return buf.getvalue()
