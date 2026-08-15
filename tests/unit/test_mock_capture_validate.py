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
    GrpcMockConfig,
    GrpcResponse,
    GrpcResponseMessage,
    GrpcStub,
    HttpEffect,
    HttpMockConfig,
    HttpResponse,
    HttpStub,
    KafkaEffect,
    KafkaEffectMessage,
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


# --- gRPC: whole-field object capture in response.message is valid ----------
def test_grpc_object_whole_field_in_message_is_valid():
    """A grpc object capture occupying a whole response.message field ('{ctx}'
    alone) is valid — no error (mirrors HTTP response.body whole-field rule)."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    capture={"ctx": _obj_cap(".msg.ctx")},
                    response=GrpcResponse(message={"context": "{ctx}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- gRPC: inline object capture in response.message is a violation ---------
def test_grpc_object_inline_in_message_is_flagged():
    """A grpc object capture used inline within a larger response.message string
    ('pre={ctx}') -> one error whose path is the grpc stub label."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    capture={"ctx": _obj_cap(".msg.ctx")},
                    response=GrpcResponse(message={"msg": "pre={ctx}"}),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.grpc.stubs.s"
    assert "{ctx}" in errors[0]["message"]
    assert set(errors[0].keys()) == {"path", "message"}


# --- gRPC: inline object capture in streaming messages[*].message ----------
def test_grpc_object_inline_in_streaming_message_is_flagged():
    """A grpc object capture used inline inside a streaming
    response.messages[*].message string -> one error (the streaming tree is
    walked just like the unary response.message tree)."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    capture={"ctx": _obj_cap(".msg.ctx")},
                    response=GrpcResponse(
                        messages=[
                            GrpcResponseMessage(message={"ok": "{ctx}"}),
                            GrpcResponseMessage(message={"bad": "pre={ctx}"}),
                        ],
                    ),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.grpc.stubs.s"


def test_grpc_object_whole_field_in_streaming_message_is_valid():
    """A grpc object capture occupying a whole field in EVERY streaming message
    is valid — no error."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    capture={"ctx": _obj_cap(".msg.ctx")},
                    response=GrpcResponse(
                        messages=[
                            GrpcResponseMessage(message={"context": "{ctx}"}),
                            GrpcResponseMessage(message={"ctx": "{ctx}"}),
                        ],
                    ),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- gRPC: scalar/json captures never flagged; no capture -> no error -------
def test_grpc_scalar_capture_inline_is_not_flagged():
    """A scalar-typed grpc capture used inline is fine — only object captures
    are subject to the whole-field rule."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    capture={"id": CaptureSpec(from_=".msg.id", type="scalar")},
                    response=GrpcResponse(message={"msg": "pre={id}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_grpc_stub_without_capture_is_skipped():
    """A grpc stub with capture=None contributes no errors (nothing to check)."""
    mocks = MocksConfig(
        grpc=GrpcMockConfig(
            stubs={
                "s": GrpcStub(
                    service="pkg.Svc",
                    method="Do",
                    response=GrpcResponse(message={"msg": "{ctx}"}),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


# --- http-effect object captures: placement vs SUBSEQUENT effects (Task 8) ----
def test_http_effect_object_capture_whole_field_in_later_body_is_valid():
    """An object capture on an http effect, used as the whole ``{ctx}`` field in
    a LATER effect's body, is valid — no error (mirrors the response.body rule)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "chain": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/use",
                            body={"context": "{ctx}"},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_http_effect_object_capture_whole_field_in_later_kafka_value_valid():
    """Same whole-field rule holds when the later effect is a kafka effect's
    ``value`` (values item) — valid, no error."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "tap": KafkaReactor(
                    topic="in",
                    match=".v",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        KafkaEffect(
                            type="kafka",
                            topic="out",
                            values=[
                                KafkaEffectMessage(value={"context": "{ctx}"}),
                            ],
                        ),
                    ],
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_http_effect_object_capture_unused_by_later_effects_is_allowed():
    """An object capture no later effect references is NOT an error — chained
    captures may be consumed only by events / go unused (soft posture)."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "s": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/other",
                            body={"unrelated": True},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_http_effect_object_capture_inline_in_later_body_is_flagged():
    """An object capture used INLINE within a larger string in a later
    effect's body ('pre={ctx}') -> one placement error at the capture path."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "chain": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/use",
                            body={"msg": "pre={ctx}"},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.http.stubs.chain.effects[0].capture.ctx"
    assert "{ctx}" in errors[0]["message"]
    assert set(errors[0].keys()) == {"path", "message"}


def test_http_effect_object_capture_nested_placeholder_is_flagged():
    """A nested position (e.g. "v": "{ctx.inner}") is a placement error: the
    placeholder regex does not match dotted names, so the token would render
    literally (a silent miss). One error at the capture path."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "chain": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/use",
                            body={"v": "{ctx.inner}"},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.http.stubs.chain.effects[0].capture.ctx"


def test_http_effect_object_capture_whole_in_later_kafka_key_headers_valid():
    """The whole-object placeholder may appear in a later effect's
    value/key/headers (any of the enumerated slot kinds) — whole-field is the
    valid placement everywhere in the effects walk; only non-whole occurrences
    are violations."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "tap": KafkaReactor(
                    topic="in",
                    match=".v",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        KafkaEffect(
                            type="kafka",
                            topic="out",
                            value={"context": "{ctx}"},
                            key="{ctx}",
                            headers={"x-trace": "{ctx}"},
                        ),
                    ],
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_http_effect_object_capture_inline_in_later_key_flagged():
    """An inline occurrence in a later kafka effect's key ('pre={ctx}') -> one
    placement error at the capture path (key is one of the scanned slots)."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "tap": KafkaReactor(
                    topic="in",
                    match=".v",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        KafkaEffect(
                            type="kafka",
                            topic="out",
                            value={},
                            key="pre={ctx}",
                        ),
                    ],
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(mocks)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.kafka.reactors.tap.effects[0].capture.ctx"


def test_http_effect_object_capture_in_later_headers_and_path_flagged():
    """Inline/nested occurrences in a later http effect's headers value and
    path are each violations; whole-field '{ctx}' in the same slots is not.
    Two stubs isolate the flagged case from the whole-field case."""
    flagged = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "s": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/use",
                            path="/items/{ctx.inner}",
                            headers={"x-ctx": "pre={ctx}"},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    errors = collect_capture_placement_errors(flagged)
    assert len(errors) == 1
    assert errors[0]["path"] == "mocks.http.stubs.s.effects[0].capture.ctx"

    whole = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "s": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/use",
                            path="{ctx}",
                            headers={"x-ctx": "{ctx}"},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(whole) == []


def test_http_effect_object_capture_earlier_effect_use_is_ignored():
    """Placement is checked against SUBSEQUENT effects only — a use in an
    EARLIER effect (which runs before the capture exists) is not this check's
    business (the runtime renders it literally; the jq walk catches nothing
    here). No error is reported by the placement walk."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "s": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/early",
                            body={"context": "{ctx}"},
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={"ctx": _obj_cap(".body.ctx")},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_http_effect_scalar_capture_never_flagged():
    """scalar/json captures on http effects are subject to no placement rule —
    inline use in later effects is fine."""
    mocks = MocksConfig(
        http=HttpMockConfig(
            stubs={
                "s": HttpStub(
                    method="POST",
                    path="/o",
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/fetch",
                            capture={
                                "id": CaptureSpec(from_=".body.id", type="scalar"),
                                "ctx": CaptureSpec(from_=".body.ctx", type="json"),
                            },
                        ),
                        HttpEffect(
                            type="http",
                            url="https://x/use",
                            body={"msg": "pre={id}", "raw": "{ctx}"},
                        ),
                    ],
                    response=HttpResponse(),
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []


def test_kafka_reactor_effects_only_with_capture_does_not_raise():
    """REGRESSION (T1 ledger): an effects-only reactor (reaction=None) carrying
    a capture must not crash the reactor walk — the reaction-placement checks
    are skipped when there is no reaction to misplace captures in."""
    mocks = MocksConfig(
        kafka=KafkaMockConfig(
            reactors={
                "tap": KafkaReactor(
                    topic="in",
                    match=".v",
                    capture={"ctx": _obj_cap(".value.ctx")},
                    effects=[
                        HttpEffect(
                            type="http",
                            url="https://x/notify",
                            body={"context": "{ctx}"},
                        ),
                    ],
                ),
            },
        ),
    )
    assert collect_capture_placement_errors(mocks) == []
