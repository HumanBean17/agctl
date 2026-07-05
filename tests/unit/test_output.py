import json

import pytest

from agctl.output import emit


def test_emit_writes_envelope(capsys):
    emit(ok=True, command="http.call", result={"status_code": 200}, duration_ms=12)
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload == {
        "ok": True,
        "command": "http.call",
        "result": {"status_code": 200},
        "error": None,
        "duration_ms": 12,
    }
    assert out.endswith("\n")


def test_emit_defaults(capsys):
    emit(ok=False, command="db.assert", error={"type": "AssertionError", "message": "x"})
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"] is None
    assert payload["duration_ms"] == 0
    assert payload["error"] == {"type": "AssertionError", "message": "x"}


def test_emit_serializes_non_json_via_default_str(capsys):
    class Thing:
        def __str__(self):
            return "THING"

    emit(ok=True, command="x", result=Thing())
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"] == "THING"


@pytest.mark.parametrize(
    "glyph",
    [
        "Иван",  # Cyrillic
        "日本語",  # CJK (BMP)
        "🌐🚀",  # non-BMP (surrogate pairs)
        "café",  # Latin-1 supplement
    ],
)
def test_emit_renders_non_ascii_as_utf8_not_escaped(capsys, glyph):
    # Non-ASCII must appear as readable UTF-8 in stdout, not as \uXXXX escapes.
    # Both forms are valid JSON and decode identically, but the escaped form is
    # unreadable to agents consuming the envelope. Covers scripts beyond the
    # originally-reported Cyrillic so BMP-only regressions can't sneak back in.
    emit(ok=True, command="db.query", result={"v": glyph})
    out = capsys.readouterr().out
    assert glyph in out
    assert "\\u" not in out
    assert json.loads(out)["result"]["v"] == glyph


def test_emit_with_render_typed_json_capture_keeps_unicode_end_to_end(capsys):
    # Composition (review minor #3): a json-typed Cyrillic capture rendered by
    # resolution, then embedded in the envelope, must reach stdout as readable
    # UTF-8 — no \u escapes surviving from either stage. Locks in the property
    # that the value-level and envelope-level fixes compose.
    from agctl.resolution import CaptureValue, render_typed

    rendered = render_typed("{ctx}", {"ctx": CaptureValue({"name": "Иван"}, "json")})
    emit(ok=True, command="http.call", result={"body": rendered})
    out = capsys.readouterr().out
    assert "Иван" in out
    assert "\\u" not in out
    assert json.loads(out)["result"]["body"] == '{"name": "Иван"}'
