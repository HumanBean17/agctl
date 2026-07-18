"""Serialization surface for agctl (DESIGN §6).

Public API (re-exported here so consumers import from
``agctl.serialization`` directly):

* :class:`Format` — payload format enum (``json`` / ``avro`` / ``protobuf``
  / ``string``);
* :func:`decode_payload`, :func:`encode_payload` — single-message codecs
  that compose the wire kernel, the Schema Registry client, and the
  Avro codec;
* :func:`resolve_subject` — Confluent subject-name strategies
  (``topic`` / ``record`` / ``topic_record``);
* :func:`decode_message` — convenience wrapper decoding a ``(value, key)``
  pair per their formats.

The lower-level building blocks (wire kernel, SR client, Avro codec)
remain importable from their submodules; only the agent-facing surface
is re-exported here.
"""

from .api import (
    Format,
    decode_message,
    decode_payload,
    encode_payload,
    resolve_subject,
)

__all__ = [
    "Format",
    "decode_message",
    "decode_payload",
    "encode_payload",
    "resolve_subject",
]
