"""JSON output envelope — the only permitted stdout write path (DESIGN §4.1)."""

import json
import sys
from typing import Any


def emit(
    ok: bool,
    command: str,
    result: Any = None,
    error: dict | None = None,
    duration_ms: int = 0,
) -> None:
    """Write exactly one JSON envelope to stdout and flush. Call once per invocation."""
    payload = {
        "ok": ok,
        "command": command,
        "result": result,
        "error": error,
        "duration_ms": duration_ms,
    }
    # ensure_ascii=False: non-ASCII (e.g. Cyrillic) must render as readable
    # UTF-8 in stdout, not as \uXXXX escapes. Valid JSON per RFC 8259, and
    # downstream JSON consumers decode both forms identically.
    sys.stdout.write(json.dumps(payload, default=str, ensure_ascii=False))
    sys.stdout.write("\n")
    sys.stdout.flush()


def emit_ndjson_line(line: dict) -> None:
    """Write one NDJSON line to stdout and flush (streaming event sink).

    Sibling to :func:`emit`: where ``emit`` writes the single command envelope
    once per invocation, ``emit_ndjson_line`` is the recurring event sink used by
    streaming daemons (e.g. ``ListenEngine``) that emit one JSON object per event
    as they occur. The emission lock lives in the caller (e.g.
    ``ListenEngine.emit_event``), not here.
    """
    sys.stdout.write(json.dumps(line, ensure_ascii=False))
    sys.stdout.write("\n")
    sys.stdout.flush()
