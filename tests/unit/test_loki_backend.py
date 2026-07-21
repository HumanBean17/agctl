"""Unit tests for ``agctl.clients.log_backends.loki.LokiBackend`` skeleton.

Covers Task 4 of the Loki log backend plan:
  * ``validate_config`` (required url/query, scheme, auth conflict, partial basic)
  * ``_fetch_entries`` -- request construction (URL/params), success flattening
  * auth header construction (basic / bearer / org-id / none)
  * ``verify_tls`` plumbing
  * ``_normalize_loki_line`` (JSON dict vs. plain text)
  * HTTP status error mapping (400 / 401 / 503 / non-stream resultType)
  * transport-exception mapping via ``httpx.MockTransport``
    (ConnectError/ConnectTimeout -> ConnectionFailure, ReadTimeout -> OperationTimeout)
  * missing-httpx fallback -> ``ConfigError`` naming the install extra

The HTTP-status and request-construction tests inject a duck-typed fake
``http_client`` (no httpx needed). The transport tests ``importorskip`` httpx.
The missing-httpx test forces the lazy import to fail.
"""

import sys

import pytest

from agctl.clients.log_backend_protocol import CanonicalEntry
from agctl.clients.log_backends.loki import LokiBackend
from agctl.config.models import LogSource
from agctl.errors import ConfigError, ConnectionFailure, OperationTimeout


# --- helpers ----------------------------------------------------------------
class FakeResponse:
    """Minimal duck-typed stand-in for ``httpx.Response``."""

    def __init__(self, *, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data


class FakeHttpClient:
    """Records the last ``.get(...)`` call and returns a canned response.

    Raises ``exc`` (if set) instead of returning a response -- used to feed
    transport-style exceptions when we do not want to pull in httpx.
    """

    def __init__(self, response=None, *, exc=None):
        self.response = response
        self.exc = exc
        self.calls = []

    def get(
        self,
        url,
        *,
        params=None,
        headers=None,
        auth=None,
        timeout=None,
        verify=None,
    ):
        self.calls.append(
            {
                "url": url,
                "params": dict(params) if params is not None else {},
                "headers": dict(headers) if headers is not None else {},
                "auth": auth,
                "timeout": timeout,
                "verify": verify,
            }
        )
        if self.exc is not None:
            raise self.exc
        return self.response


def make_source(**kw):
    """Build a ``LogSource`` with sensible Loki defaults; ``options`` merged."""
    options = kw.pop("options", {})
    base = {"type": "loki", "url": "http://loki:3100", "query": '{app="x"}'}
    base.update(kw)
    return LogSource(options=options, **base)


def streams_body(streams):
    """Build a 200-success ``query_range`` body with the given streams.

    ``streams`` is a list of (labels_dict, values_list) tuples. ``values_list``
    is a list of ``[ts, line]`` pairs.
    """
    return {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {"stream": labels, "values": values} for labels, values in streams
            ],
        },
    }


# --- validate_config --------------------------------------------------------
def test_validate_config_missing_url_raises():
    source = LogSource(type="loki", query='{app="x"}')
    backend = LokiBackend(source)
    with pytest.raises(ConfigError) as ei:
        backend.validate_config()
    assert "url" in ei.value.message.lower()


def test_validate_config_missing_query_raises():
    source = LogSource(type="loki", url="http://loki:3100")
    backend = LokiBackend(source)
    with pytest.raises(ConfigError) as ei:
        backend.validate_config()
    assert "query" in ei.value.message.lower()


def test_validate_config_bad_scheme_raises():
    source = LogSource(type="loki", url="ftp://x", query='{app="x"}')
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_basic_and_bearer_conflict_raises():
    source = make_source(options={"username": "u", "password": "p", "token": "tok"})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_username_without_password_raises():
    source = make_source(options={"username": "u"})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_minimal_valid_does_not_raise():
    source = make_source()  # url + query only
    backend = LokiBackend(source)
    backend.validate_config()  # must not raise


# --- _fetch_entries: request construction + success flattening --------------
def test_fetch_entries_builds_query_range_url_and_params():
    body = streams_body(
        [
            ({"app": "x"}, [["T1", '{"level":"INFO","message":"a"}']]),
            ({"app": "y"}, [["T2", '{"level":"ERROR","message":"b"}']]),
        ]
    )
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    entries = backend._fetch_entries(
        start="2026-07-22T00:00:00Z",
        end="2026-07-22T01:00:00Z",
        limit=500,
        direction="forward",
    )

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["url"] == "http://loki:3100/loki/api/v1/query_range"
    # all five query_range params are present
    assert set(["query", "start", "end", "limit", "direction"]).issubset(
        call["params"].keys()
    )
    assert call["params"]["query"] == '{app="x"}'
    assert call["params"]["start"] == "2026-07-22T00:00:00Z"
    assert call["params"]["end"] == "2026-07-22T01:00:00Z"
    assert call["params"]["limit"] == 500
    assert call["params"]["direction"] == "forward"
    # flattened across both streams, preserving order
    assert [e.message for e in entries] == ["a", "b"]
    assert [e.timestamp for e in entries] == ["T1", "T2"]
    assert all(isinstance(e, CanonicalEntry) for e in entries)


# --- auth header construction ----------------------------------------------
def test_auth_basic_sends_tuple_no_bearer():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=streams_body([]))
    )
    backend = LokiBackend(
        make_source(options={"username": "u", "password": "p"}), http_client=fake
    )
    backend._fetch_entries(
        start="s", end="e", limit=10, direction="forward"
    )
    call = fake.calls[0]
    assert call["auth"] == ("u", "p")
    assert "Authorization" not in call["headers"]


def test_auth_bearer_sends_header_no_basic():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=streams_body([]))
    )
    backend = LokiBackend(make_source(options={"token": "tok"}), http_client=fake)
    backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    call = fake.calls[0]
    assert call["auth"] is None
    assert call["headers"].get("Authorization") == "Bearer tok"


def test_auth_org_id_sends_header():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=streams_body([]))
    )
    backend = LokiBackend(
        make_source(options={"org_id": "tenant-1"}), http_client=fake
    )
    backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    call = fake.calls[0]
    assert call["headers"].get("X-Scope-OrgID") == "tenant-1"


def test_auth_none_sets_neither_auth_nor_org_header():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=streams_body([]))
    )
    backend = LokiBackend(make_source(), http_client=fake)
    backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    call = fake.calls[0]
    assert call["auth"] is None
    assert "Authorization" not in call["headers"]
    assert "X-Scope-OrgID" not in call["headers"]


# --- verify_tls plumbing ----------------------------------------------------
def test_verify_tls_false_propagated_to_client():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=streams_body([]))
    )
    backend = LokiBackend(
        make_source(options={"verify_tls": False}), http_client=fake
    )
    backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert fake.calls[0]["verify"] is False


def test_verify_tls_default_is_true():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=streams_body([]))
    )
    backend = LokiBackend(make_source(), http_client=fake)
    backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert fake.calls[0]["verify"] is True


# --- _normalize_loki_line ---------------------------------------------------
def test_normalize_loki_line_json_dict_uses_normalize_dict():
    backend = LokiBackend(make_source(service="svc"))
    entry = backend._normalize_loki_line(
        '{"level":"ERROR","message":"boom","orderId":"o1"}', "T"
    )
    assert isinstance(entry, CanonicalEntry)
    assert entry.timestamp == "T"
    assert entry.level == "ERROR"
    assert entry.message == "boom"
    assert entry.fields == {"orderId": "o1"}


def test_normalize_loki_line_plain_text_becomes_message():
    backend = LokiBackend(make_source(service="svc"))
    entry = backend._normalize_loki_line("hello", "T")
    assert isinstance(entry, CanonicalEntry)
    assert entry.timestamp == "T"
    assert entry.level == ""
    assert entry.logger == ""
    assert entry.message == "hello"


# --- HTTP status error mapping ---------------------------------------------
def test_status_400_raises_config_error_with_body():
    body = {"status": "error", "error": "parse error"}
    fake = FakeHttpClient(
        response=FakeResponse(status_code=400, json_data=body, text='{"status":"error"}')
    )
    backend = LokiBackend(make_source(), http_client=fake)
    with pytest.raises(ConfigError) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    # message carries the loki error body
    assert "parse error" in ei.value.message
    assert ei.value.detail.get("status") == 400


def test_status_401_raises_connection_failure():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=401, text="unauthorized")
    )
    backend = LokiBackend(make_source(), http_client=fake)
    with pytest.raises(ConnectionFailure) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert ei.value.detail.get("status") == 401


def test_status_503_raises_connection_failure():
    fake = FakeHttpClient(
        response=FakeResponse(status_code=503, text="unavailable")
    )
    backend = LokiBackend(make_source(), http_client=fake)
    with pytest.raises(ConnectionFailure) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert ei.value.detail.get("status") == 503


def test_status_200_non_stream_resulttype_raises_config_error():
    body = {
        "status": "success",
        "data": {"resultType": "matrix", "result": []},
    }
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=body)
    )
    backend = LokiBackend(make_source(), http_client=fake)
    with pytest.raises(ConfigError) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert ei.value.detail.get("result_type") == "matrix"


# --- transport-exception mapping (httpx required) --------------------------
def test_connect_error_maps_to_connection_failure():
    httpx = pytest.importorskip("httpx")

    def handler(request):
        raise httpx.ConnectError("no route")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    backend = LokiBackend(make_source(), http_client=client)
    with pytest.raises(ConnectionFailure) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert "unreachable" in ei.value.message.lower()


def test_connect_timeout_maps_to_connection_failure():
    httpx = pytest.importorskip("httpx")

    def handler(request):
        raise httpx.ConnectTimeout("connect timed out")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    backend = LokiBackend(make_source(), http_client=client)
    with pytest.raises(ConnectionFailure) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert "unreachable" in ei.value.message.lower()


def test_read_timeout_maps_to_operation_timeout():
    httpx = pytest.importorskip("httpx")

    def handler(request):
        raise httpx.ReadTimeout("read timed out")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    backend = LokiBackend(make_source(), http_client=client)
    with pytest.raises(OperationTimeout):
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")


# --- missing httpx ----------------------------------------------------------
def test_missing_httpx_raises_config_error_naming_extra(monkeypatch):
    # Force the lazy ``import httpx`` inside _fetch_entries to fail. We do not
    # inject a client, so the code path attempts the real import.
    monkeypatch.setitem(sys.modules, "httpx", None)
    backend = LokiBackend(make_source())
    with pytest.raises(ConfigError) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert "pip install 'agctl[loki]'" in ei.value.message
    assert ei.value.detail.get("type") == "loki"
