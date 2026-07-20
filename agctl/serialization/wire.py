"""Confluent Schema Registry wire-frame kernel (pure Python).

The Confluent wire format is::

    +---+-------------------+---------+
    | 0 | 4-byte schema id | payload |
    +---+-------------------+---------+
      ^        ^ big-endian
      magic (0x00)

This module is the foundation for the Avro / Protobuf codecs (which
lazy-import ``fastavro`` / ``protobuf``). It deliberately keeps *no*
heavy imports — only stdlib ``struct`` — so it is unit-testable with no
extras installed and cheap to import from anywhere in the CLI.
"""

import struct

MAGIC_BYTE: int = 0x00


def is_confluent_frame(raw: bytes) -> bool:
    """Return True iff ``raw`` could be a Confluent wire frame.

    A frame is plausible when it has at least 5 bytes (1 magic + 4 schema
    id) and the first byte equals :data:`MAGIC_BYTE`. The payload may be
    empty, so a 5-byte input with magic 0x00 still qualifies.
    """
    return len(raw) >= 5 and raw[0] == MAGIC_BYTE


def parse_wire(raw: bytes) -> tuple[int, bytes]:
    """Split a Confluent wire frame into ``(schema_id, payload)``.

    ``schema_id`` is the 4-byte big-endian int at ``raw[1:5]`` and the
    payload is everything after ``raw[5:]``. Raises ``ValueError`` when
    ``raw`` is not a plausible Confluent frame (see
    :func:`is_confluent_frame`).
    """
    if not is_confluent_frame(raw):
        raise ValueError("not a Confluent frame")
    schema_id = struct.unpack(">I", raw[1:5])[0]
    payload = raw[5:]
    return schema_id, payload


def build_wire(schema_id: int, payload: bytes) -> bytes:
    """Build a Confluent wire frame from ``schema_id`` and ``payload``.

    Returns ``magic (0x00) || struct.pack(">I", schema_id) || payload``.
    """
    return bytes([MAGIC_BYTE]) + struct.pack(">I", schema_id) + payload
