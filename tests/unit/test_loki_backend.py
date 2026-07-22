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
import threading
from datetime import datetime, timedelta, timezone

import pytest

from agctl.clients.log_backend_protocol import (
    AwaitResult,
    CanonicalEntry,
    LogFilter,
    ScanResult,
    SchemaDescriptor,
)
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


def test_validate_config_password_without_username_raises():
    # Symmetric to username-without-password: a lone password must also fail
    # at validate time (otherwise _build_auth silently drops it -> confusing
    # runtime 401 instead of a clean ConfigError).
    source = make_source(options={"password": "p"})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_minimal_valid_does_not_raise():
    source = make_source()  # url + query only
    backend = LokiBackend(source)
    backend.validate_config()  # must not raise


# --- validate_config: fetch_limit / direction / verify_tls (DESIGN §5.3) -----
def test_validate_config_fetch_limit_zero_raises():
    source = make_source(options={"fetch_limit": 0})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_fetch_limit_negative_raises():
    source = make_source(options={"fetch_limit": -1})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_fetch_limit_non_int_raises():
    source = make_source(options={"fetch_limit": "500"})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_direction_bad_value_raises():
    source = make_source(options={"direction": "sideways"})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_verify_tls_string_raises():
    source = make_source(options={"verify_tls": "false"})
    backend = LokiBackend(source)
    with pytest.raises(ConfigError):
        backend.validate_config()


def test_validate_config_valid_fetch_limit_direction_verify_tls_pass():
    source = make_source(
        options={"fetch_limit": 1000, "direction": "backward", "verify_tls": False}
    )
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


def test_normalize_loki_line_json_not_dict_becomes_message():
    # A JSON scalar (e.g. "42" -> int 42) is valid JSON but not a dict; it is
    # stringified into ``message`` rather than routed through normalize_dict.
    backend = LokiBackend(make_source(service="svc"))
    entry = backend._normalize_loki_line("42", "T")
    assert isinstance(entry, CanonicalEntry)
    assert entry.timestamp == "T"
    assert entry.level == ""
    assert entry.logger == ""
    assert entry.message == "42"
    assert entry.service == "svc"


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


# --- _parse_streams: defensive parsing --------------------------------------
def test_parse_streams_skips_malformed_pairs():
    """Malformed [ts, line] pairs (non-list or len < 2) are skipped; valid
    pairs are still normalized. Guards against a single bad chunk aborting the
    whole response."""
    body = {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"app": "x"},
                    "values": [
                        ["T1", '{"level":"INFO","message":"keep"}'],  # valid
                        ["solo"],  # len < 2 -> skipped
                        "not-a-list",  # not a list/tuple -> skipped
                        ["T2", '{"level":"ERROR","message":"also-keep"}'],  # valid
                    ],
                }
            ],
        },
    }
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    entries = backend._fetch_entries(
        start="s", end="e", limit=10, direction="forward"
    )
    assert [e.message for e in entries] == ["keep", "also-keep"]
    assert [e.timestamp for e in entries] == ["T1", "T2"]


def test_parse_streams_non_json_body_on_200_raises_connection_failure():
    """A 200 whose body is not valid JSON is surfaced as ConnectionFailure
    (carrying the raw text) rather than crashing with a JSONDecodeError."""
    fake = FakeHttpClient(
        response=FakeResponse(status_code=200, json_data=None, text="<html>nope</html>")
    )
    backend = LokiBackend(make_source(), http_client=fake)
    with pytest.raises(ConnectionFailure) as ei:
        backend._fetch_entries(start="s", end="e", limit=10, direction="forward")
    assert "non-json" in ei.value.message.lower()
    assert ei.value.detail.get("body") == "<html>nope</html>"


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


# ============================================================================
# Task 5: scan + sample_schema
# ============================================================================

def _three_logstash_lines():
    """Three logstash-JSON lines: ERROR/WARN/INFO; ERROR carries ``orderId``."""
    return [
        ["2026-07-22T00:00:00Z", '{"level":"ERROR","message":"boom","orderId":"o1"}'],
        ["2026-07-22T00:00:01Z", '{"level":"WARN","message":"careful"}'],
        ["2026-07-22T00:00:02Z", '{"level":"INFO","message":"hi"}'],
    ]


def _win():
    """A fixed 1-minute window used by most scan tests (deterministic)."""
    return (
        datetime(2026, 7, 22, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 7, 22, 0, 1, 0, tzinfo=timezone.utc),
    )


# --- scan: happy path + filtering ------------------------------------------
def test_scan_happy_path_level_filter():
    body = streams_body([({"app": "x"}, _three_logstash_lines())])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    since, until = _win()
    result = backend.scan(
        LogFilter(level="ERROR"),
        since=since,
        until=until,
        limit=10,
        tail_lines=200,
    )

    assert isinstance(result, ScanResult)
    assert result.scanned == 3
    assert result.matched == 1
    assert result.truncated is False
    assert len(result.entries) == 1
    assert result.entries[0].level == "ERROR"
    assert result.entries[0].message == "boom"
    # request used the default fetch_limit (500) and forward direction
    call = fake.calls[0]
    assert call["params"]["direction"] == "forward"
    assert call["params"]["limit"] == 500


def test_scan_match_jq_filter_over_three_lines():
    pytest.importorskip("jq")
    body = streams_body([({"app": "x"}, _three_logstash_lines())])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    since, until = _win()
    result = backend.scan(
        LogFilter(match_jq='.fields.orderId == "o1"'),
        since=since,
        until=until,
        limit=10,
        tail_lines=0,
    )
    assert result.matched == 1
    assert result.entries[0].fields == {"orderId": "o1"}


# --- scan: truncation honesty ----------------------------------------------
def test_scan_truncated_when_matched_exceeds_limit():
    # 5 matching ERROR lines, limit=2 -> matched==5, returned==2, truncated
    lines = [
        [f"2026-07-22T00:00:0{i}Z", '{"level":"ERROR","message":"e"}']
        for i in range(5)
    ]
    body = streams_body([({"app": "x"}, lines)])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    since, until = _win()
    result = backend.scan(
        LogFilter(level="ERROR"), since=since, until=until, limit=2, tail_lines=0
    )
    assert result.scanned == 5
    assert result.matched == 5
    assert len(result.entries) == 2
    assert result.truncated is True


def test_scan_truncated_at_server_cap_when_scanned_equals_fetch_limit():
    # fetch_limit=4, fake returns exactly 4, all match, limit=10 -> truncated
    # even though matched(4) <= limit(10): the server-cap signal fires.
    lines = [
        [f"2026-07-22T00:00:0{i}Z", '{"level":"ERROR","message":"e"}']
        for i in range(4)
    ]
    body = streams_body([({"app": "x"}, lines)])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(options={"fetch_limit": 4}), http_client=fake)

    since, until = _win()
    result = backend.scan(
        LogFilter(level="ERROR"), since=since, until=until, limit=10, tail_lines=0
    )
    assert result.scanned == 4
    assert result.matched == 4
    assert result.truncated is True


def test_scan_not_truncated_when_below_both_signals():
    lines = [
        ["2026-07-22T00:00:00Z", '{"level":"ERROR","message":"e"}'],
        ["2026-07-22T00:00:01Z", '{"level":"ERROR","message":"e2"}'],
    ]
    body = streams_body([({"app": "x"}, lines)])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)  # fetch_limit=500

    since, until = _win()
    result = backend.scan(
        LogFilter(level="ERROR"), since=since, until=until, limit=10, tail_lines=0
    )
    assert result.scanned == 2
    assert result.matched == 2
    assert result.truncated is False


# --- scan: default lookback ------------------------------------------------
def test_scan_default_lookback_is_one_hour_when_since_is_none():
    body = streams_body([({"app": "x"}, [])])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    until = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)
    backend.scan(LogFilter(), since=None, until=until, limit=10, tail_lines=0)

    call = fake.calls[0]
    start = datetime.fromisoformat(call["params"]["start"])
    end = datetime.fromisoformat(call["params"]["end"])
    # end is the provided `until`; start is ~1h before end
    assert end == until
    delta = end - start
    assert timedelta(seconds=3599) <= delta <= timedelta(seconds=3601)


# --- scan: tail_lines ignored ----------------------------------------------
def test_scan_tail_lines_is_ignored():
    body = streams_body([({"app": "x"}, _three_logstash_lines())])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    since, until = _win()
    r1 = backend.scan(LogFilter(), since=since, until=until, limit=10, tail_lines=0)
    r2 = backend.scan(
        LogFilter(), since=since, until=until, limit=10, tail_lines=999
    )
    # Same scanned/matched (result invariant) ...
    assert r1.scanned == r2.scanned
    assert r1.matched == r2.matched
    # ... and the request params are identical (tail_lines nowhere on the wire).
    p1, p2 = fake.calls[0]["params"], fake.calls[1]["params"]
    for k in ("query", "start", "end", "limit", "direction"):
        assert p1[k] == p2[k]


# --- scan: custom direction + per-request timeout plumbing -----------------
def test_scan_custom_direction_backward_flows_through():
    """options.direction='backward' is plumbed into the query_range request."""
    body = streams_body([({"app": "x"}, _three_logstash_lines())])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(
        make_source(options={"direction": "backward"}), http_client=fake
    )

    since, until = _win()
    backend.scan(
        LogFilter(), since=since, until=until, limit=10, tail_lines=0
    )
    assert fake.calls[0]["params"]["direction"] == "backward"


def test_scan_plumbs_default_per_request_timeout():
    """The recorded fake call carries the per-request HTTP timeout kwarg so a
    regression dropping it fails this assertion."""
    body = streams_body([({"app": "x"}, _three_logstash_lines())])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    since, until = _win()
    backend.scan(
        LogFilter(), since=since, until=until, limit=10, tail_lines=0
    )
    # scan does not override the per-request timeout, so the default applies.
    from agctl.clients.log_backends.loki import _DEFAULT_FETCH_TIMEOUT_S

    assert fake.calls[0]["timeout"] == _DEFAULT_FETCH_TIMEOUT_S


# --- sample_schema ---------------------------------------------------------
def test_sample_schema_infers_standard_conditional_observed():
    lines = [
        [
            "2026-07-22T00:00:00Z",
            '{"level":"ERROR","message":"m","logger_name":"App",'
            '"stack_trace":"...","tags":["t1"],"requestId":"r1"}',
        ],
    ]
    body = streams_body([({"app": "x"}, lines)])
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake)

    schema = backend.sample_schema(sample_lines=50)

    assert isinstance(schema, SchemaDescriptor)
    for slot in ("timestamp", "level", "logger", "message"):
        assert slot in schema.standard
    assert "stack_trace" in schema.conditional
    assert "tags" in schema.conditional
    assert "requestId" in schema.observed

    # sample_schema uses backward direction + sample_lines as the request limit
    call = fake.calls[0]
    assert call["params"]["direction"] == "backward"
    assert call["params"]["limit"] == 50


# ============================================================================
# Task 6: await_one (two-phase poll with timestamp high-water)
# ============================================================================

class FakeClock:
    """Controllable monotonic clock for deterministic await_one polling.

    ``monotonic`` reads ``t``; ``sleep`` advances ``t`` by the slept seconds.
    Tests can override ``sleep`` to jump the clock by a different amount (e.g.
    to blow past a deadline in one step).
    """

    def __init__(self, start: float = 0.0):
        self.t = start

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


class SequentialFakeHttpClient:
    """Returns canned responses in sequence, recording every ``.get(...)`` call.

    Repeats the last response once the sequence is exhausted (so a poll loop
    that reads more times than expected still gets a canned answer rather than
    an IndexError). Mirrors :class:`FakeHttpClient`'s recorded-call shape.

    A script item may be either a ``FakeResponse`` (returned) or an
    ``Exception`` instance (raised) -- used by the follow tests to inject
    transient :class:`ConnectionFailure` / :class:`OperationTimeout` errors
    mid-stream without pulling in httpx.
    """

    def __init__(self, responses):
        self._responses = list(responses)
        self._idx = 0
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
        if not self._responses:
            raise AssertionError("SequentialFakeHttpClient has no canned responses")
        item = (
            self._responses[self._idx]
            if self._idx < len(self._responses)
            else self._responses[-1]
        )
        self._idx += 1
        if isinstance(item, Exception):
            raise item
        return item


def _ts(s):
    """Shorthand RFC3339 timestamp used by the await_one fixtures."""
    return f"2026-07-22T00:00:{s}Z"


# --- one-shot mode (timeout_s <= 0) ----------------------------------------
def test_await_one_oneshot_match_returns_first_match_no_sleep():
    """timeout_s<=0: one backward read; first matching entry returned; no sleep."""
    body = streams_body(
        [
            (
                {"app": "x"},
                [
                    [_ts("00"), '{"level":"INFO","message":"noise"}'],
                    [_ts("01"), '{"level":"ERROR","message":"boom"}'],
                ],
            )
        ]
    )
    clock = FakeClock()
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(
        make_source(), http_client=fake, monotonic=clock.monotonic, sleep=clock.sleep
    )

    result = backend.await_one(
        LogFilter(level="ERROR"),
        since=datetime(2026, 7, 22, 0, 0, 0, tzinfo=timezone.utc),
        timeout_s=0,
        poll_interval_ms=100,
        tail_lines=0,
    )

    assert isinstance(result, AwaitResult)
    assert result.entry is not None
    assert result.entry.level == "ERROR"
    assert result.entry.message == "boom"
    assert result.scanned == 2  # both entries fetched from the backward read
    # Only the single backward read; the sleep-driven forward path never ran.
    assert len(fake.calls) == 1
    assert fake.calls[0]["params"]["direction"] == "backward"


def test_await_one_oneshot_no_match_returns_none():
    """timeout_s<=0: backward read with only non-matching entries -> None."""
    body = streams_body(
        [
            (
                {"app": "x"},
                [
                    [_ts("00"), '{"level":"INFO","message":"a"}'],
                    [_ts("01"), '{"level":"WARN","message":"b"}'],
                ],
            )
        ]
    )
    clock = FakeClock()
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(
        make_source(), http_client=fake, monotonic=clock.monotonic, sleep=clock.sleep
    )

    result = backend.await_one(
        LogFilter(level="ERROR"),
        since=datetime(2026, 7, 22, 0, 0, 0, tzinfo=timezone.utc),
        timeout_s=0,
        poll_interval_ms=100,
        tail_lines=0,
    )

    assert result.entry is None
    assert result.scanned == 2  # fetched count, even though nothing matched
    assert len(fake.calls) == 1


# --- poll mode (timeout_s > 0) ---------------------------------------------
def test_await_one_poll_finds_match_after_one_sleep():
    """Phase 1 backward (no match); forward fetch 1 no match; one sleep; forward fetch 2 matches.

    Fetch-first ordering: the FIRST forward fetch returns only non-matching
    entries, then after exactly one sleep the SECOND forward fetch returns
    the match. Asserts the three reads use the correct directions, exactly
    one sleep occurred before the find, and scanned accumulates all reads
    (backward=0 + forward1=1 + forward2=1 == 2).
    """
    clock = FakeClock()
    sleeps = []

    def fake_sleep(seconds):
        clock.t += seconds
        sleeps.append(seconds)

    backward_body = streams_body([({"app": "x"}, [])])  # Phase 1: nothing
    forward_no_match = streams_body(
        [({"app": "x"}, [[_ts("10"), '{"level":"INFO","message":"A"}']])]
    )
    forward_match = streams_body(
        [
            (
                {"app": "x"},
                [[_ts("20"), '{"level":"ERROR","message":"found"}']],
            )
        ]
    )
    fake = SequentialFakeHttpClient(
        [
            FakeResponse(status_code=200, json_data=backward_body),
            FakeResponse(status_code=200, json_data=forward_no_match),
            FakeResponse(status_code=200, json_data=forward_match),
        ]
    )
    backend = LokiBackend(
        make_source(), http_client=fake, monotonic=clock.monotonic, sleep=fake_sleep
    )

    result = backend.await_one(
        LogFilter(level="ERROR"),
        since=datetime(2026, 7, 22, 0, 0, 0, tzinfo=timezone.utc),
        timeout_s=5.0,
        poll_interval_ms=100,
        tail_lines=0,
    )

    assert result.entry is not None
    assert result.entry.message == "found"
    assert result.scanned == 2  # backward(0) + forward1(1) + forward2(1)
    assert len(sleeps) == 1  # exactly one sleep before the find
    assert len(fake.calls) == 3  # one backward + two forward
    assert fake.calls[0]["params"]["direction"] == "backward"
    assert fake.calls[1]["params"]["direction"] == "forward"
    assert fake.calls[2]["params"]["direction"] == "forward"


def test_await_one_poll_no_double_count_same_timestamp():
    """An entry re-included across two forward polls is counted/matched once.

    Forward poll 1 returns [A(ts=T1)] (no match). Forward poll 2 re-includes
    A at the same ts=T1 and adds B(ts=T2, match). The high-water start
    advances past T1 so A is deduped in poll 2: scanned counts A once and B
    once (==2), and A is not re-matched (result is B, not A).
    """
    clock = FakeClock()

    def fake_sleep(seconds):
        clock.t += seconds

    backward_body = streams_body([({"app": "x"}, [])])
    forward_poll1 = streams_body(
        [({"app": "x"}, [[_ts("10"), '{"level":"INFO","message":"A"}']])]
    )
    # Poll 2 re-includes A at the SAME ts (T1) and adds the matching B at T2.
    forward_poll2 = streams_body(
        [
            (
                {"app": "x"},
                [
                    [_ts("10"), '{"level":"INFO","message":"A"}'],  # re-included
                    [_ts("20"), '{"level":"ERROR","message":"B"}'],  # new + match
                ],
            )
        ]
    )
    fake = SequentialFakeHttpClient(
        [
            FakeResponse(status_code=200, json_data=backward_body),
            FakeResponse(status_code=200, json_data=forward_poll1),
            FakeResponse(status_code=200, json_data=forward_poll2),
        ]
    )
    backend = LokiBackend(
        make_source(), http_client=fake, monotonic=clock.monotonic, sleep=fake_sleep
    )

    result = backend.await_one(
        LogFilter(level="ERROR"),
        since=datetime(2026, 7, 22, 0, 0, 0, tzinfo=timezone.utc),
        timeout_s=5.0,
        poll_interval_ms=100,
        tail_lines=0,
    )

    assert result.entry is not None
    assert result.entry.message == "B"  # A was NOT re-matched
    # A counted once (poll 1) + B once (poll 2) == 2; A not double-counted.
    assert result.scanned == 2


def test_await_one_poll_timeout_no_matches_terminates():
    """timeout_s>0, no matches ever: loop terminates after exactly one sleep.

    Fetch-first ordering: after Phase 1's backward read, one forward fetch
    runs (returns empty), then the sleep advances the fake clock 0.3s past
    the 0.2s deadline, so the loop exits. entry is None, elapsed_ms reflects
    the fake wall time, and scanned stays 0 (both reads returned empty).
    """
    clock = FakeClock()
    sleeps = []

    def fake_sleep(seconds):
        # Jump past the 0.2s deadline in one step regardless of `seconds`.
        clock.t += 0.3
        sleeps.append(seconds)

    body = streams_body([({"app": "x"}, [])])  # every read returns empty
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(
        make_source(), http_client=fake, monotonic=clock.monotonic, sleep=fake_sleep
    )

    result = backend.await_one(
        LogFilter(level="ERROR"),
        since=datetime(2026, 7, 22, 0, 0, 0, tzinfo=timezone.utc),
        timeout_s=0.2,
        poll_interval_ms=100,
        tail_lines=0,
    )

    assert result.entry is None
    assert len(sleeps) == 1  # loop terminated, did not spin forever
    assert result.elapsed_ms == 300  # 0.3s fake wall-clock elapsed
    assert result.scanned == 0  # Phase 1 empty + forward fetch empty


# ============================================================================
# Task 7: follow (poll generator with dedup + stop + transient tolerance)
# ============================================================================

def _make_stop_after_n_sleep(stop_event, n):
    """Build a fake ``sleep`` that sets ``stop_event`` on the n-th call.

    Bounds the otherwise-infinite ``follow`` loop deterministically: each
    "no new entries" cycle calls sleep once (poll_interval_ms <= chunk size),
    so the n-th sleep call corresponds to the n-th no-new cycle.
    """
    counter = [0]

    def _sleep(seconds):
        counter[0] += 1
        if counter[0] >= n:
            stop_event.set()

    return _sleep


# --- follow: yields new matches across polls -------------------------------
def test_follow_yields_new_matches_across_polls_and_skips_repeats():
    """[] then [A] then [A,B] across three polls yields exactly [A, B].

    A is not re-yielded on poll 3 (dedup). The loop is bounded by a fake
    sleep that sets stop_event on the 3rd no-new cycle.
    """
    stop_event = threading.Event()
    sleep = _make_stop_after_n_sleep(stop_event, n=3)

    body_empty = streams_body([({"app": "x"}, [])])
    body_a = streams_body(
        [({"app": "x"}, [[_ts("00"), '{"level":"ERROR","message":"A"}']])]
    )
    body_ab = streams_body(
        [
            (
                {"app": "x"},
                [
                    [_ts("00"), '{"level":"ERROR","message":"A"}'],
                    [_ts("01"), '{"level":"ERROR","message":"B"}'],
                ],
            )
        ]
    )
    fake = SequentialFakeHttpClient(
        [
            FakeResponse(status_code=200, json_data=body_empty),
            FakeResponse(status_code=200, json_data=body_a),
            FakeResponse(status_code=200, json_data=body_ab),
        ]
    )
    backend = LokiBackend(make_source(), http_client=fake, sleep=sleep)

    yielded = list(
        backend.follow(
            LogFilter(level="ERROR"),
            stop_event=stop_event,
            poll_interval_ms=100,
        )
    )

    assert [e.message for e in yielded] == ["A", "B"]


# --- follow: dedup ---------------------------------------------------------
def test_follow_dedups_repeated_timestamp_message_pair():
    """The same (timestamp, message) returned every poll is yielded once."""
    stop_event = threading.Event()
    sleep = _make_stop_after_n_sleep(stop_event, n=2)

    body = streams_body(
        [({"app": "x"}, [[_ts("00"), '{"level":"ERROR","message":"A"}']])]
    )
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake, sleep=sleep)

    yielded = list(
        backend.follow(
            LogFilter(level="ERROR"),
            stop_event=stop_event,
            poll_interval_ms=100,
        )
    )

    assert [e.message for e in yielded] == ["A"]


# --- follow: filter respected ----------------------------------------------
def test_follow_filter_respected_only_matching_yielded():
    """LogFilter(level=ERROR) drops non-matching entries among fetched ones."""
    stop_event = threading.Event()
    sleep = _make_stop_after_n_sleep(stop_event, n=2)

    body = streams_body(
        [
            (
                {"app": "x"},
                [
                    [_ts("00"), '{"level":"INFO","message":"noise"}'],
                    [_ts("01"), '{"level":"ERROR","message":"boom"}'],
                ],
            )
        ]
    )
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake, sleep=sleep)

    yielded = list(
        backend.follow(
            LogFilter(level="ERROR"),
            stop_event=stop_event,
            poll_interval_ms=100,
        )
    )

    assert [e.message for e in yielded] == ["boom"]


# --- follow: stop_event honored between yields -----------------------------
def test_follow_stop_event_set_between_yields_terminates_without_more_fetches():
    """Setting stop_event after a yield terminates the generator before the
    next ``_fetch_entries`` call (prompt termination)."""
    stop_event = threading.Event()
    body = streams_body(
        [({"app": "x"}, [[_ts("00"), '{"level":"ERROR","message":"A"}']])]
    )
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake, sleep=lambda s: None)

    gen = backend.follow(
        LogFilter(level="ERROR"),
        stop_event=stop_event,
        poll_interval_ms=100,
    )
    first = next(gen)
    assert first.message == "A"

    stop_event.set()
    fetch_count = len(fake.calls)
    with pytest.raises(StopIteration):
        next(gen)
    # No further fetch happened after stop_event was set.
    assert len(fake.calls) == fetch_count


# --- follow: stop_event honored during sleep -------------------------------
def test_follow_stop_event_set_during_sleep_terminates():
    """stop_event set by the injected sleep (no-new cycle) terminates the loop."""
    stop_event = threading.Event()
    sleep = _make_stop_after_n_sleep(stop_event, n=1)

    body = streams_body([({"app": "x"}, [])])  # always empty -> always sleeps
    fake = FakeHttpClient(response=FakeResponse(status_code=200, json_data=body))
    backend = LokiBackend(make_source(), http_client=fake, sleep=sleep)

    yielded = list(
        backend.follow(
            LogFilter(level="ERROR"),
            stop_event=stop_event,
            poll_interval_ms=100,
        )
    )

    assert yielded == []  # never matched; terminated via sleep-set-stop


# --- follow: transient error tolerated (mid-stream) ------------------------
def test_follow_transient_error_tolerated_with_stderr_warning(capsys):
    """One mid-stream ConnectionFailure is swallowed with a stderr warning;
    entries are yielded on the next poll; loop terminates via sleep-set-stop.

    Sequence: poll 1 = empty (startup ok), poll 2 = ConnectionFailure
    (transient, swallowed + warned), poll 3 = [A] (yielded), poll 4 = [A]
    (deduped, no-new -> sleep 3 -> stop). The first-fetch-propagates rule
    does NOT fire here because poll 1 succeeded.
    """
    stop_event = threading.Event()
    sleep = _make_stop_after_n_sleep(stop_event, n=3)

    body_empty = streams_body([({"app": "x"}, [])])
    body_entries = streams_body(
        [({"app": "x"}, [[_ts("00"), '{"level":"ERROR","message":"A"}']])]
    )
    fake = SequentialFakeHttpClient(
        [
            FakeResponse(status_code=200, json_data=body_empty),
            ConnectionFailure("transient blip", {}),
            FakeResponse(status_code=200, json_data=body_entries),
        ]
    )
    backend = LokiBackend(make_source(), http_client=fake, sleep=sleep)

    yielded = list(
        backend.follow(
            LogFilter(level="ERROR"),
            stop_event=stop_event,
            poll_interval_ms=100,
        )
    )

    assert [e.message for e in yielded] == ["A"]
    err = capsys.readouterr().err
    assert "agctl: loki follow transient error" in err
    assert "transient blip" in err


# --- follow: first-fetch errors propagate (startup-error path) -------------
def test_follow_first_fetch_transient_error_propagates():
    """A transient error on the very first fetch propagates (startup path).

    Mid-stream transients are swallowed, but the FIRST fetch letting errors
    through is what surfaces bad-auth/connect failures at generator start
    (ARCH §6 streaming startup-error path).
    """
    stop_event = threading.Event()
    fake = SequentialFakeHttpClient(
        [ConnectionFailure("startup connect failed", {})]
    )
    backend = LokiBackend(make_source(), http_client=fake, sleep=lambda s: None)

    gen = backend.follow(
        LogFilter(),
        stop_event=stop_event,
        poll_interval_ms=100,
    )
    with pytest.raises(ConnectionFailure) as ei:
        next(gen)
    assert "startup connect failed" in ei.value.message


# --- follow: ConfigError always propagates ---------------------------------
def test_follow_config_error_on_subsequent_fetch_propagates():
    """ConfigError (bad query / non-stream result) is permanent -- propagates
    even on a subsequent fetch, NOT swallowed like a transient blip."""
    stop_event = threading.Event()
    body_empty = streams_body([({"app": "x"}, [])])
    bad_query_body = {"status": "error", "error": "parse error"}
    fake = SequentialFakeHttpClient(
        [
            FakeResponse(status_code=200, json_data=body_empty),  # poll 1: ok
            FakeResponse(
                status_code=400, json_data=bad_query_body, text='{"status":"error"}'
            ),  # poll 2: ConfigError
        ]
    )
    backend = LokiBackend(make_source(), http_client=fake, sleep=lambda s: None)

    gen = backend.follow(
        LogFilter(level="ERROR"),
        stop_event=stop_event,
        poll_interval_ms=100,
    )
    with pytest.raises(ConfigError) as ei:
        list(gen)
    assert "parse error" in ei.value.message
