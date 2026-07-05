"""Acceptance fixtures for envelope-rooted capture (Task 8).

Replicates the battle-test scenarios as named regression anchors. The HTTP
cases drive a real ``MockHTTPServer`` over ``httpx``; the Kafka case drives
``KafkaReactor`` with a minimal ``FakeKafkaClient`` that exercises all three
capture sources (key, headers, value) plus object pass-through in one reaction.

The live-broker Kafka E2E (``kafka-threadhistory`` against a testcontainers
broker) is deferred pending a subagent wiring the testcontainers harness; the
``FakeKafkaClient`` case below backstops the same capture behavior, and the
unit suites cover each source individually.
"""

from __future__ import annotations

import threading
import time

import httpx
import pytest

from agctl.clients.kafka_client import ReactionResult
from agctl.config.models import (
    CaptureSpec,
    HttpResponse,
    HttpStub,
    KafkaReaction,
    KafkaReactor,
)
from agctl.mock.http_server import MockHTTPServer
from agctl.mock.kafka_reactor import KafkaReactor as Reactor


# ---------------------------------------------------------------------------
# HTTP seam (local minimal helper, mirrors tests/unit/test_mock_http_server.py)
# ---------------------------------------------------------------------------


def _start_server(stubs):
    server = MockHTTPServer(("127.0.0.1", 0), stubs=stubs, emit_event=lambda _e: None)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.01)
    return server


def test_graphql_operator_by_id_acceptance():
    """graphql-operatorById: nested .body.variables.id capture (gap 1)."""
    stubs = {
        "graphql-operatorById": HttpStub(
            method="POST",
            path="/graphql",
            capture={"op_id": CaptureSpec.model_validate({"from": ".body.variables.id"})},
            response=HttpResponse(body={"id": "{op_id}"}),
        )
    }
    server = _start_server(stubs)
    port = server.server_port
    try:
        with httpx.Client() as client:
            response = client.post(
                f"http://127.0.0.1:{port}/graphql",
                json={"query": "query { operatorById }", "variables": {"id": 7}},
            )
        assert response.status_code == 200
        assert response.json() == {"id": "7"}
    finally:
        server.shutdown()


def test_epk_chat_search_acceptance():
    """epk-chatSearch: compound body paths .body.clientInfoCriteria.firstName + .body.ucpID (gap 1)."""
    stubs = {
        "epk-chatSearch": HttpStub(
            method="POST",
            path="/chat/search",
            capture={
                "fname": CaptureSpec.model_validate(
                    {"from": ".body.clientInfoCriteria.firstName"}
                ),
                "ucp": CaptureSpec.model_validate({"from": ".body.ucpID"}),
            },
            response=HttpResponse(body={"firstName": "{fname}", "ucpID": "{ucp}"}),
        )
    }
    server = _start_server(stubs)
    port = server.server_port
    try:
        with httpx.Client() as client:
            response = client.post(
                f"http://127.0.0.1:{port}/chat/search",
                json={"clientInfoCriteria": {"firstName": "Alice"}, "ucpID": "U-123"},
            )
        assert response.status_code == 200
        assert response.json() == {"firstName": "Alice", "ucpID": "U-123"}
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# Kafka seam (local minimal FakeKafkaClient)
# ---------------------------------------------------------------------------


class _FakeKafkaClient:
    def __init__(self, messages):
        self.messages = list(messages)
        self.produce_calls = []

    def consume_loop(self, topic, *, group_id, stop_event, handle, max_retries=3, **_):
        for msg in self.messages:
            if stop_event.is_set():
                break
            for attempt in range(1, max_retries + 1):
                result = handle(msg, attempt=attempt, final=attempt >= max_retries)
                if result == ReactionResult.COMMIT:
                    break

    def probe(self, topic, *, group_id, timeout=5.0):
        return None

    def produce(self, topic, value, *, key=None, headers=None):
        self.produce_calls.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )


@pytest.fixture
def emit_event():
    events = []

    def emit(d):
        events.append(d)

    emit.events = events
    return emit


@pytest.fixture
def stop_event():
    return threading.Event()


def test_chatx_context_echo_acceptance(emit_event, stop_event):
    """chatx-it-mock: one reaction capturing key + header + object context (gaps 2 & 3).

    ``kafkaThreadHistoryFlow`` asserts the context object round-trips as a real
    object; this acceptance test reproduces that against the reactor.
    """
    config = KafkaReactor(
        topic="chatx.commands",
        match='.value.command == "SEARCH"',
        capture={
            "tid": CaptureSpec.model_validate({"from": ".key"}),
            "rqUID": CaptureSpec.model_validate({"from": ".headers.rqUID"}),
            "ctx": CaptureSpec.model_validate(
                {"from": ".value.context", "type": "object"}
            ),
        },
        reaction=KafkaReaction(
            topic="chatx.events",
            value={
                "threadId": "{tid}",
                "rs_headers": {"rqUID": "{rqUID}"},
                "context": "{ctx}",
            },
        ),
    )
    client = _FakeKafkaClient(
        messages=[
            {
                "value": {
                    "command": "SEARCH",
                    "context": {"conversationId": "abc", "eventType": "MSG"},
                },
                "key": "thread-9",
                "partition": 0,
                "offset": 1,
                "timestamp": 1719660000000,
                "headers": {"rqUID": "r-1"},
            }
        ]
    )
    reactor = Reactor(
        name="chatx",
        config=config,
        client=client,
        emit_event=emit_event,
        stop_event=stop_event,
        fail_fast=False,
        run_id="run-1",
    )
    reactor.run()
    assert len(client.produce_calls) == 1
    assert client.produce_calls[0]["value"] == {
        "threadId": "thread-9",
        "rs_headers": {"rqUID": "r-1"},
        "context": {"conversationId": "abc", "eventType": "MSG"},
    }
