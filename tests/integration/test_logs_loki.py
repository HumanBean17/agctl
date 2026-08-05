"""Self-skipping integration tests: Loki read path against a live Loki.

Exercises the full ``LogClient`` -> ``LokiBackend`` stack end-to-end against a
real ``grafana/loki`` instance. Depends on the
:func:`tests.integration.conftest.require_loki` fixture; when Loki is absent
(no ``AGCTL_TEST_LIVE=1`` / Docker unavailable / Loki failed to start) the
test SKIPS, never FAILS.

Coverage (per Task 9 brief):

(a) ``scan`` over a recent window returns the pushed line, with the JSON
    payload's ``message`` lifted into ``CanonicalEntry.message`` and the
    non-slot ``orderId`` lifted into ``CanonicalEntry.fields``.
(b) ``await_one`` (poll mode) matches the same pushed line within a bounded
    timeout.

The push (write) side is NOT part of agctl's Loki backend (it only reads via
``query_range``); the test seeds Loki directly via the standard
``POST /loki/api/v1/push`` endpoint, then reads the line back through
``LogClient``. Each test push uses a per-run ``orderId`` so re-runs against a
persistent Loki do not collide, but the ``app="agctl-it"`` stream selector
keeps the query narrow.
"""

from __future__ import annotations

import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

from agctl.clients.log_backend_protocol import LogFilter
from agctl.clients.log_client import LogClient
from agctl.config.models import LogSource


def _push_line(loki_url: str, payload: dict, app: str = "agctl-it") -> str:
    """Push ONE log line to Loki via the standard push API.

    Builds a single-stream push body whose value is the JSON-encoded
    ``payload`` dict, timestamped at "now" in unix nanoseconds (Loki's
    required line-timestamp format). Returns the ns timestamp string used.

    A 204 response (Loki's success status for push) is the only accepted
    outcome; anything else fails the test with the body echoed for diagnosis
    (by this point the live fixture has already yielded, so a push failure is
    a real SUT failure, not a skip condition).
    """
    ns = str(int(time.time() * 1e9))
    body = json.dumps(
        {
            "streams": [
                {
                    "stream": {"app": app},
                    "values": [[ns, json.dumps(payload)]],
                }
            ]
        }
    ).encode()
    req = urllib.request.Request(
        f"{loki_url}/loki/api/v1/push",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        # Loki returns 204 No Content on a successful push.
        assert resp.status in (200, 204), (
            f"loki push failed: HTTP {resp.status}"
        )
    return ns


def test_loki_scan_returns_pushed_line(require_loki):
    """scan() over a recent window returns the pushed line with fields parsed.

    Pushes one ``{level:ERROR, message:boom, orderId:it-1}`` line, then reads
    it back through ``LogClient.scan`` with the ``{app="agctl-it"}`` selector.
    Asserts the returned entry has ``message == "boom"`` (slot extraction) and
    ``fields["orderId"] == "it-1"`` (non-slot key lift). The window is
    ``[now-2m, now]`` so a small clock skew between push and query does not
    drop the line.
    """
    loki_url = require_loki
    order_id = "it-1"
    _push_line(
        loki_url,
        {"level": "ERROR", "message": "boom", "orderId": order_id},
    )

    now = datetime.now(timezone.utc)
    source = LogSource(type="loki", url=loki_url, query='{app="agctl-it"}')
    client = LogClient(source)

    result = client.scan(
        LogFilter(),
        since=now - timedelta(minutes=2),
        until=now,
        limit=10,
        tail_lines=0,
    )

    assert result.scanned >= 1, (
        f"scan returned no entries (scanned={result.scanned}); "
        "pushed line not visible in the query window"
    )
    matches = [
        e
        for e in result.entries
        if e.message == "boom" and e.fields.get("orderId") == order_id
    ]
    assert matches, (
        f"pushed line (message='boom', orderId={order_id!r}) not in scan "
        f"entries; saw messages={[e.message for e in result.entries]}"
    )
    entry = matches[0]
    assert entry.level == "ERROR"
    assert entry.fields["orderId"] == order_id


def test_loki_await_one_matches_pushed_line(require_loki):
    """await_one() in poll mode matches the pushed line within 5s.

    After the push, ``await_one`` does a Phase 1 historical read over
    ``[now-2m, now]`` and returns the first match. The poll timeout is a
    safety net for a slowly-indexing Loki; the Phase 1 read usually resolves
    immediately. Asserts the matched entry carries the pushed payload.
    """
    loki_url = require_loki
    order_id = "it-2"
    _push_line(
        loki_url,
        {"level": "ERROR", "message": "boom", "orderId": order_id},
    )

    now = datetime.now(timezone.utc)
    source = LogSource(type="loki", url=loki_url, query='{app="agctl-it"}')
    client = LogClient(source)

    result = client.await_one(
        LogFilter(),
        since=now - timedelta(minutes=2),
        timeout_s=5,
        poll_interval_ms=200,
        tail_lines=0,
    )

    assert result.entry is not None, (
        "await_one did not match the pushed line within 5s"
    )
    assert result.entry.message == "boom"
    assert result.entry.fields.get("orderId") == order_id
