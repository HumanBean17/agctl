"""Tests for the object-capture placement static check (Task 5).

``collect_capture_placement_errors`` walks a :class:`MocksConfig` and, for each
``object``-typed capture name ``N``, returns one ``{"path", "message"}`` per
placement violation:

- (a) ``{N}`` appears inline within a larger string in ``response.body`` (HTTP)
  / ``reaction.value`` (Kafka) — object captures must occupy the WHOLE field.
- (b) ``reaction.key`` is or contains ``{N}`` (Kafka only — string-only slot).
- (c) any ``reaction.headers`` value is or contains ``{N}`` (Kafka only).

``scalar``/``json`` captures are never flagged. ``mocks is None`` -> ``[]``.
Mirrors :func:`collect_jq_compile_errors` in shape so ``config validate`` and
``MockEngine.start()`` Step 0 can wire it in identically.
"""

from agctl.config.models import (
    CaptureSpec,
    HttpMockConfig,
    HttpResponse,
    HttpStub,
    KafkaMockConfig,
    KafkaReaction,
    KafkaReactor,
    MocksConfig,
)
from agctl.mock.capture_validate import collect_capture_placement_errors


def _obj_cap(from_: str) -> CaptureSpec:
    """Shorthand for an object-typed capture spec."""
    return CaptureSpec(from_=from_, type="object")


# --- None guard ---------------------------------------------------------------
def test_none_returns_empty():
    """collect_capture_placement_errors(None) -> [] (nothing to scan)."""
    assert collect_capture_placement_errors(None) == []


# --- HTTP: whole-field object capture is valid --------------------------------
def test_http_object_whole_field_is_valid():
    """An object capture occupying a whole body field ('{ctx}' alone) is the
    one valid placement — no error."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "echo": HttpStub(
                    method="POST",
                    path="/echo",
                    capture={"ctx": _obj_cap(".body.ctx")},
                    response=HttpResponse(body={"context": "{ctx}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- HTTP: inline object capture is a violation -------------------------------
def test_http_object_inline_is_flagged():
    """An object capture used inline within a larger body string
    ('pre={ctx}') -> one error whose path is the stub label."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "echo": HttpStub(
                    method="POST",
                    path="/echo",
                    capture={"ctx": _obj_cap(".body.ctx")},
                    response=HttpResponse(body={"msg": "pre={ctx}"}),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.http.stubs.echo"
    assert "{ctx}" in errors[0]["message"]
    assert set(errors[0].keys()) == {"path", "message"}


# --- Kafka: whole-field object capture in value is valid ----------------------
def test_kafka_object_whole_field_in_value_is_valid():
    """An object capture occupying a whole reaction.value field is valid."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "r": KafkaReactor(
                    topic="in",
                    capture={"ctx": _obj_cap(".value.ctx")},
                    reaction=KafkaReaction(topic="out", value={"context": "{ctx}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- Kafka: object capture in reaction.key is a violation ---------------------
def test_kafka_object_in_key_is_flagged():
    """An object capture used as reaction.key (string-only slot) -> one error.
    reaction.key cannot hold an object, so even a whole-field '{ctx}' is a
    violation."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "r": KafkaReactor(
                    topic="in",
                    capture={"ctx": _obj_cap(".value.ctx")},
                    reaction=KafkaReaction(topic="out", value={}, key="{ctx}"),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.kafka.reactors.r"
    assert "key" in errors[0]["message"].lower()


# --- Kafka: object capture in a header value is a violation -------------------
def test_kafka_object_in_header_is_flagged():
    """An object capture used inside a reaction.headers value (string-only slot)
    -> one error."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "r": KafkaReactor(
                    topic="in",
                    capture={"ctx": _obj_cap(".value.ctx")},
                    reaction=KafkaReaction(
                        topic="out",
                        value={},
                        headers={"x-trace": "{ctx}"},
                    ),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.kafka.reactors.r"
    assert "header" in errors[0]["message"].lower()


# --- Kafka: scalar capture in key is fine -------------------------------------
def test_kafka_scalar_in_key_is_fine():
    """A scalar-typed capture in reaction.key is NOT flagged — only object-typed
    captures are checked. The object capture 'ctx' is valid (whole-field in
    value); the scalar 'tid' in key is allowed."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "r": KafkaReactor(
                    topic="in",
                    capture={
                        "ctx": _obj_cap(".value.ctx"),
                        "tid": CaptureSpec(from_=".value.tid", type="scalar"),
                    },
                    reaction=KafkaReaction(
                        topic="out",
                        value={"context": "{ctx}"},
                        key="{tid}",
                    ),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- json-typed captures are never flagged ------------------------------------
def test_json_capture_inline_is_not_flagged():
    """A json-typed capture used inline is fine — only object-typed captures are
    subject to the whole-field rule (json renders as a JSON string)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "echo": HttpStub(
                    method="POST",
                    path="/echo",
                    capture={"ctx": CaptureSpec(from_=".body.ctx", type="json")},
                    response=HttpResponse(body={"msg": "pre={ctx}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- no capture -> no error ---------------------------------------------------
def test_http_stub_without_capture_is_skipped():
    """A stub with capture=None contributes no errors (nothing to check)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "bare": HttpStub(
                    method="GET",
                    path="/x",
                    response=HttpResponse(body={"msg": "{ctx}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []
