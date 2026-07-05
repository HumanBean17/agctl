import json

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


def test_emit_renders_non_ascii_as_utf8_not_escaped(capsys):
    # Cyrillic (any non-ASCII) must appear as readable UTF-8 in stdout, not as
    # \uXXXX escapes. Both forms are valid JSON and decode identically, but the
    # escaped form is unreadable to agents consuming the envelope.
    emit(ok=True, command="db.query", result={"name": "Иван"})
    out = capsys.readouterr().out
    assert "Иван" in out
    assert "\\u" not in out
    assert json.loads(out)["result"] == {"name": "Иван"}
