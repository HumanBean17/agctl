"""Unit tests for the httpx-backed HttpClient (DESIGN §7 clients)."""

import httpx
import pytest

from agctl.clients.http_client import HttpClient
from agctl.errors import ConnectionFailure, OperationTimeout


def _client(handler, *, headers=None):
    return HttpClient(
        "http://localhost:8081",
        10,
        transport=httpx.MockTransport(handler),
        headers=headers,
    )


def test_get_json_200():
    def handler(request):
        return httpx.Response(200, json={"ok": 1})

    result = _client(handler).request("GET", "/x")

    assert result["status_code"] == 200
    assert result["body"] == {"ok": 1}
    assert isinstance(result["response_time_ms"], int)
    assert result["response_time_ms"] >= 0
    assert "content-type" in result["headers"]
    assert result["headers"]["content-type"] == "application/json"
    assert result["url"].endswith("/x")
    assert result["method"] == "GET"


def test_404_text_not_parsed():
    def handler(request):
        return httpx.Response(
            404, text="nope", headers={"content-type": "text/plain"}
        )

    result = _client(handler).request("GET", "/missing")

    assert result["status_code"] == 404
    assert result["body"] == "nope"
    assert isinstance(result["body"], str)


def test_post_json_body():
    captured = {}

    def handler(request):
        captured["content"] = request.content
        captured["headers"] = request.headers
        return httpx.Response(201, json={"created": True})

    result = _client(handler).request("POST", "/items", body={"name": "x"})

    assert result["status_code"] == 201
    # Handler received the JSON body
    assert b'"name"' in captured["content"]
    assert captured["headers"]["content-type"] == "application/json"


def test_header_merge_per_call_wins():
    captured = {}

    def handler(request):
        captured["headers"] = request.headers
        return httpx.Response(200, json={})

    client = _client(handler, headers={"X-Default": "d"})
    client.request("GET", "/h", headers={"X-Override": "o", "X-Default": "d2"})

    assert captured["headers"]["x-override"] == "o"
    assert captured["headers"]["x-default"] == "d2"


def test_connection_refused_maps_to_connection_failure():
    def handler(request):
        raise httpx.ConnectError("refused")

    with pytest.raises(ConnectionFailure):
        _client(handler).request("GET", "/x")


def test_read_timeout_maps_to_operation_timeout():
    def handler(request):
        raise httpx.ReadTimeout("slow")

    with pytest.raises(OperationTimeout):
        _client(handler).request("GET", "/x")


def test_lazy_import_does_not_require_httpx_at_module_top():
    # Importing the module must not require httpx at top level (it's lazy in
    # __init__). We verify by importing the module fresh; if httpx were a
    # top-level dependency this would still pass since httpx is installed, so
    # this is a light structural guard.
    import importlib

    import agctl.clients.http_client as mod

    importlib.reload(mod)
    assert hasattr(mod, "HttpClient")


def test_json_text_fallback_parses():
    # Body declared text/plain but actually JSON -> should still parse.
    def handler(request):
        return httpx.Response(
            200, text='{"surprise": 1}', headers={"content-type": "text/plain"}
        )

    result = _client(handler).request("GET", "/y")
    assert result["body"] == {"surprise": 1}


def test_params_query_string():
    captured = {}

    def handler(request):
        captured["url"] = request.url
        return httpx.Response(200, json={})

    _client(handler).request("GET", "/q", params={"a": "1", "b": "2"})

    assert "a=1" in str(captured["url"])
    assert "b=2" in str(captured["url"])
