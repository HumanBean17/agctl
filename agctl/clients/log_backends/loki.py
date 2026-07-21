"""Loki log backend skeleton (DESIGN §9.2, Task 4 of the Loki plan).

This module implements the foundation the higher-level operations
(``scan``/``await_one``/``follow``/``sample_schema``) build on:

  * :class:`LokiBackend` constructor with injectable ``http_client`` /
    ``monotonic`` / ``sleep`` test seams,
  * :meth:`LokiBackend.validate_config` (§5.3 rules),
  * :meth:`LokiBackend._fetch_entries` -- the single HTTP read path issuing
    Loki's ``GET /loki/api/v1/query_range`` and mapping every documented
    failure to the typed :mod:`agctl.errors` hierarchy,
  * :meth:`LokiBackend._normalize_loki_line` -- JSON-dict vs. plain-text lines.

Discipline (task brief): ``httpx`` is **lazy-imported inside methods**, never
at module top. Importing this module must not require ``httpx``. When no
client is injected and ``httpx`` is missing, :meth:`_fetch_entries` raises
:class:`ConfigError` naming ``pip install 'agctl[loki]'``.

The high-level operations :meth:`scan`, :meth:`sample_schema`,
:meth:`await_one`, and :meth:`follow` are all implemented here.
``LokiBackend`` is registered as the built-in ``"loki"`` type in
:data:`agctl.clients.log_client.BUILTIN_BACKENDS` (alongside ``"file"``).
"""

import inspect
import json
import sys
import threading
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from agctl.clients import log_common
from agctl.clients.log_backend_protocol import (
    AwaitResult,
    CanonicalEntry,
    LogFilter,
    ScanResult,
    SchemaDescriptor,
)
from agctl.config.models import LogSource
from agctl.errors import ConfigError, ConnectionFailure, OperationTimeout

#: Default per-request httpx timeout (seconds) when callers do not pass one.
_DEFAULT_FETCH_TIMEOUT_S: float = 10.0

#: Default ``fetch_limit`` (max lines per ``query_range`` response).
_DEFAULT_FETCH_LIMIT: int = 500

#: Default ``direction`` for ``query_range`` ("forward" == oldest-first).
_DEFAULT_DIRECTION: str = "forward"

#: Chunk size (ms) for the interruptible sleep in :meth:`LokiBackend.follow`.
#: A production ``time.sleep`` blocks at most this long past a stop signal.
_SLEEP_CHUNK_MS: int = 500


class LokiBackend:
    """Log backend for a remote Loki HTTP endpoint.

    Selected by ``type: loki`` in ``agctl.yaml``. The :class:`LogSource`
    carries ``url`` (e.g. ``"http://loki:3100"``), ``query`` (a LogQL log
    selector like ``'{app="x"}'``), optional ``service`` override, and an
    ``options`` map with backend-specific auth/transport knobs:

      * ``username`` / ``password`` -- HTTP Basic auth (mutually exclusive
        with ``token``; both-or-neither for username/password).
      * ``token`` -- bearer token (``Authorization: Bearer <token>``).
      * ``org_id`` -- multi-tenant Loki scope (``X-Scope-OrgID`` header).
      * ``verify_tls`` -- default ``True``; set ``False`` to skip TLS verify.
      * ``fetch_limit`` -- default ``500``; max lines per response.
      * ``direction`` -- default ``"forward"``; ``query_range`` direction.
    """

    def __init__(
        self,
        source: LogSource,
        *,
        http_client=None,
        monotonic=time.monotonic,
        sleep=time.sleep,
    ):
        """Store resolved config and injectable clock/sleep/HTTP seam.

        Args:
            source: :class:`LogSource` with ``url``/``query``/``options``.
            http_client: Duck-typed HTTP client (object exposing
                ``.get(url, params=, headers=, auth=, timeout=, verify=)``
                returning a response with ``.status_code``/``.json()``/``.text``).
                When ``None``, :meth:`_fetch_entries` lazy-imports ``httpx``.
                Injected for unit tests; in production a real ``httpx.Client``
                is constructed on first use.
            monotonic: Injectable monotonic clock (default :func:`time.monotonic`).
            sleep: Injectable sleep function (default :func:`time.sleep`).
        """
        self._source = source
        self._http_client = http_client
        self._monotonic = monotonic
        self._sleep = sleep
        # Lazy-built httpx.Client cache (real path only; injected clients are
        # returned as-is). None == "not built yet"; populated on first read so
        # repeated ``_fetch_entries`` calls reuse one connection pool.
        self._lazy_http_client = None

        # Resolve config once -- the read surface reads these attributes
        # rather than re-walking source.options on each call.
        self._url = source.url
        self._query = source.query
        self._service = source.service  # None if unset (see reconciliation note)
        opts = source.options or {}
        self._username = opts.get("username")
        self._password = opts.get("password")
        self._token = opts.get("token")
        self._org_id = opts.get("org_id")
        self._verify_tls = opts.get("verify_tls", True)
        self._fetch_limit = opts.get("fetch_limit", _DEFAULT_FETCH_LIMIT)
        self._direction = opts.get("direction", _DEFAULT_DIRECTION)

    # -- config validation ---------------------------------------------------
    def validate_config(self) -> None:
        """Raise :class:`ConfigError` (exit 2) on a malformed Loki source.

        Rules (DESIGN §5.3):
          * ``url`` is required and must be ``http://`` or ``https://``;
          * ``query`` is required (non-empty);
          * basic (``username``+``password``) and bearer (``token``) auth
            are mutually exclusive -- setting both is an error;
          * ``username`` without ``password`` is an error.

        A minimal valid config (``url`` + ``query`` only) passes cleanly.
        """
        if not self._url:
            raise ConfigError(
                "loki backend requires 'url'", {"type": "loki"}
            )
        if not (self._url.startswith("http://") or self._url.startswith("https://")):
            raise ConfigError(
                f"loki backend 'url' must be http(s): {self._url!r}",
                {"type": "loki", "url": self._url},
            )
        if not self._query:
            raise ConfigError(
                "loki backend requires 'query'", {"type": "loki"}
            )
        # Basic / bearer conflict.
        has_basic = self._username is not None or self._password is not None
        has_bearer = self._token is not None
        if has_basic and has_bearer:
            raise ConfigError(
                "loki backend: basic auth (username/password) and bearer token "
                "are mutually exclusive",
                {"type": "loki"},
            )
        # Username without password (passwordless basic is not supported).
        if self._username is not None and self._password is None:
            raise ConfigError(
                "loki backend: 'username' requires 'password'",
                {"type": "loki"},
            )

    # -- HTTP read path ------------------------------------------------------
    def _fetch_entries(
        self,
        *,
        start: str,
        end: str,
        limit: int,
        direction: str,
        timeout: float = _DEFAULT_FETCH_TIMEOUT_S,
    ) -> list[CanonicalEntry]:
        """Issue a single ``query_range`` read; return normalized entries.

        Builds the ``GET {url}/loki/api/v1/query_range`` request with the
        five documented params (``query``/``start``/``end``/``limit``/
        ``direction``), attaches auth/headers/verify per the resolved config,
        sends it via the injected client or a lazy-imported ``httpx.Client``,
        and maps every documented failure to the typed error hierarchy
        (see the task-4 brief's error table).

        On success, asserts ``resultType == "streams"`` and flattens every
        ``[ts, line]`` pair across all ``data.result`` streams (preserving
        order) into normalized :class:`CanonicalEntry` objects. **No
        filtering is applied here** -- callers (scan/await_one/follow/
        sample_schema) layer filters on top.

        Args:
            start: Window start as an RFC3339Nano UTC string.
            end: Window end as an RFC3339Nano UTC string.
            limit: Max lines to request (``query_range`` ``limit`` param).
            direction: ``"forward"`` (oldest-first) or ``"backward"``.
            timeout: Per-request HTTP timeout in seconds.

        Returns:
            One :class:`CanonicalEntry` per ``[ts, line]`` pair, in order.
        """
        client = self._resolve_http_client()

        url = f"{self._url}/loki/api/v1/query_range"
        params = {
            "query": self._query,
            "start": start,
            "end": end,
            "limit": limit,
            "direction": direction,
        }
        headers, auth = self._build_auth()

        get_kwargs: dict = {
            "params": params,
            "headers": headers,
            "auth": auth,
            "timeout": timeout,
        }
        # The injected fake contract accepts ``verify=`` per request; a real
        # ``httpx.Client.get`` does NOT (verify is a Client-construction arg
        # in httpx 0.28). Feature-detect the kwarg so the same call path
        # drives both without try/except masking real TypeErrors. For the
        # lazy-imported httpx path verify is additionally set on the Client
        # in :meth:`_resolve_http_client`.
        if self._client_accepts_verify_kwarg(client):
            get_kwargs["verify"] = self._verify_tls

        try:
            response = client.get(url, **get_kwargs)
        except Exception as exc:
            # Map httpx transport exceptions to typed errors. Lazy-import so
            # a module-load-time httpx requirement is not introduced; if the
            # exception did not come from httpx (or httpx is missing), fall
            # through to re-raise -- callers see the original error.
            self._raise_for_transport_error(exc)

        return self._handle_response(response)

    @staticmethod
    def _client_accepts_verify_kwarg(client) -> bool:
        """True iff ``client.get`` accepts a ``verify=`` kwarg.

        The injected duck-typed fake's ``.get`` does (per the test-seam
        contract); a real ``httpx.Client.get`` does not (verify lives on the
        Client constructor). Used so one call path serves both without
        try/except masking genuine ``TypeError`` bugs.
        """
        get = getattr(client, "get", None)
        if get is None:
            return False
        try:
            sig = inspect.signature(get)
        except (TypeError, ValueError):
            # Builtins/C-implemented callables have no Python signature.
            return False
        return "verify" in sig.parameters

    def _resolve_http_client(self):
        """Return the injected client, or lazy-import ``httpx`` and build one.

        Raises :class:`ConfigError` (exit 2) if no client was injected and
        ``httpx`` is not importable -- the message names the install extra.

        The lazy (real) path caches one :class:`httpx.Client` on the instance
        so repeated reads reuse a connection pool; the injected-client path
        (used by tests) is returned unchanged every time.
        """
        # Injected client path -- unchanged (tests inject fakes here).
        if self._http_client is not None:
            return self._http_client
        # Lazy real path -- build once, cache on the instance for pooling.
        if self._lazy_http_client is None:
            try:
                import httpx  # noqa: F401  (lazy: off the module top-level)
            except ImportError as exc:
                raise ConfigError(
                    "loki backend requires httpx: pip install 'agctl[loki]'",
                    {"type": "loki"},
                ) from exc
            # ``verify`` is a Client-construction arg in httpx 0.28 (.get()
            # does not accept it), so the resolved TLS policy is applied here,
            # not at the call site.
            self._lazy_http_client = httpx.Client(verify=self._verify_tls)
        return self._lazy_http_client

    def _build_auth(self) -> tuple[dict, tuple | None]:
        """Construct per-request headers/auth from the resolved config.

        Returns ``(headers, auth)``:
          * basic (``username``+``password``) -> ``auth=(u, p)``, no
            ``Authorization`` header;
          * bearer (``token``) -> ``Authorization: Bearer <token>``,
            ``auth=None``;
          * ``org_id`` (either auth mode or none) -> ``X-Scope-OrgID``;
          * no auth -> empty headers, ``auth=None``.
        """
        headers: dict[str, str] = {}
        auth: tuple | None = None
        if self._username is not None and self._password is not None:
            auth = (self._username, self._password)
        if self._token is not None:
            headers["Authorization"] = f"Bearer {self._token}"
        if self._org_id is not None:
            headers["X-Scope-OrgID"] = self._org_id
        return headers, auth

    def _raise_for_transport_error(self, exc: Exception) -> None:
        """Map an httpx-style transport exception to a typed agctl error.

        Lazy-imports httpx to access its exception hierarchy; if httpx is
        not importable, or the exception is not an :class:`httpx.HTTPError`,
        re-raises the original so non-httpx errors surface unchanged.

        Mapping (task-4 brief error table, httpx 0.28 hierarchy):
          * ``ConnectError`` / ``ConnectTimeout`` -> :class:`ConnectionFailure`
            (``"Loki unreachable: <exc>"``);
          * other :class:`httpx.TimeoutException` (ReadTimeout, WriteTimeout,
            PoolTimeout -- ConnectTimeout was caught above) ->
            :class:`OperationTimeout`;
          * any other :class:`httpx.HTTPError` -> :class:`ConnectionFailure`.
        """
        try:
            import httpx
        except ImportError:
            raise exc

        # ConnectTimeout is NOT a subclass of ConnectError in httpx 0.28, so
        # both must be checked explicitly for the "unreachable" bucket.
        if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
            raise ConnectionFailure(f"Loki unreachable: {exc}", {}) from exc
        # Remaining timeouts (ReadTimeout etc.) -> OperationTimeout.
        if isinstance(exc, httpx.TimeoutException):
            raise OperationTimeout("Loki read timed out", {}) from exc
        if isinstance(exc, httpx.HTTPError):
            raise ConnectionFailure(f"Loki request error: {exc}", {}) from exc
        # Not an httpx error -- surface unchanged.
        raise exc

    def _handle_response(self, response) -> list[CanonicalEntry]:
        """Map an HTTP response to entries or raise the matching typed error.

        Status mapping (task-4 brief):
          * 200 with ``resultType == "streams"`` -> flatten + normalize;
          * 200 with any other ``resultType`` -> :class:`ConfigError`
            (metric query instead of log selector);
          * 400 -> :class:`ConfigError` carrying the Loki error body;
          * 401 / 403 -> :class:`ConnectionFailure` (auth failed);
          * any other non-200 (5xx etc.) -> :class:`ConnectionFailure`.
        """
        status = response.status_code
        if status == 200:
            return self._parse_streams(response)
        if status == 400:
            raise ConfigError(
                self._loki_error_message(response),
                {"status": 400, "body": response.text},
            )
        if status in (401, 403):
            raise ConnectionFailure(
                f"Loki auth failed (HTTP {status})", {"status": status}
            )
        # Any other non-200 (5xx, etc.).
        raise ConnectionFailure(
            f"Loki request failed (HTTP {status})",
            {"status": status, "body": response.text},
        )

    def _parse_streams(self, response) -> list[CanonicalEntry]:
        """Flatten a 200 ``query_range`` body into normalized entries.

        Asserts ``data.resultType == "streams"`` (raising :class:`ConfigError`
        otherwise -- e.g. a metric query returning ``matrix``), then walks
        every ``[ts, line]`` pair in every stream of ``data.result`` and
        normalizes each via :meth:`_normalize_loki_line`. Order across
        streams is preserved (caller applies filters on top).
        """
        try:
            body = response.json()
        except Exception:
            # Malformed JSON on a 200 is unexpected; surface as a connection
            # error so the caller sees the raw text.
            raise ConnectionFailure(
                f"Loki returned non-JSON body: {response.text[:200]!r}",
                {"body": response.text},
            )

        data = body.get("data", {}) if isinstance(body, dict) else {}
        result_type = data.get("resultType")
        if result_type != "streams":
            raise ConfigError(
                f"Loki returned non-stream result ({result_type!r}); "
                "use a log selector query, not a metric query",
                {"result_type": result_type},
            )

        entries: list[CanonicalEntry] = []
        for stream in data.get("result", []):
            for pair in stream.get("values", []):
                # Each pair is [ts, line]; be defensive against malformed shapes.
                if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                    continue
                ts, line = pair[0], pair[1]
                entries.append(self._normalize_loki_line(line, ts))
        return entries

    @staticmethod
    def _loki_error_message(response) -> str:
        """Extract a human-readable message from a Loki error response.

        Loki error bodies are JSON with an ``error`` (and sometimes
        ``message``) field; if the body is not JSON, fall back to raw text.
        """
        try:
            body = response.json()
        except Exception:
            return response.text or "loki returned an error"
        if isinstance(body, dict):
            return (
                body.get("error")
                or body.get("message")
                or response.text
                or "loki returned an error"
            )
        return response.text or "loki returned an error"

    # -- line normalization --------------------------------------------------
    def _normalize_loki_line(self, line: str, ts: str) -> CanonicalEntry:
        """Normalize one Loki line into a :class:`CanonicalEntry`.

        JSON-dict lines are delegated to :func:`log_common.normalize_dict`
        with ``service=self._service`` and ``ts_override=ts`` (the streaming
        chunk timestamp wins over any ``@timestamp`` in the payload). A
        non-JSON line becomes a plaintext entry with ``message=line`` --
        Loki-specific behavior (the file backend skips non-JSON instead).

        Args:
            line: Raw Loki line (JSON object or plain text).
            ts: RFC3339Nano timestamp from the streaming chunk.

        Returns:
            Normalized :class:`CanonicalEntry`.
        """
        try:
            parsed = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            return CanonicalEntry(
                timestamp=ts,
                level="",
                logger="",
                message=line,
                service=self._service,
            )
        if isinstance(parsed, dict):
            return log_common.normalize_dict(
                parsed, service=self._service, ts_override=ts
            )
        # JSON but not a dict (e.g. a bare string/number) -- treat as message.
        return CanonicalEntry(
            timestamp=ts,
            level="",
            logger="",
            message=str(parsed),
            service=self._service,
        )

    # -- high-level operations (scan/sample_schema: Task 5; await_one/follow: Task 6) ---
    def scan(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        tail_lines: int,
    ) -> ScanResult:
        """Scan a time window with client-side filtering and honest truncation.

        Issues a single :meth:`_fetch_entries` call over the window
        ``[since, until]`` (defaults: ``until = now``, ``since = now - 1h``)
        with ``limit = fetch_limit`` and ``direction = options.direction``
        (default ``"forward"``). Normalization already happened in
        :meth:`_fetch_entries`; this method layers :func:`log_common.entry_matches`
        on top client-side.

        Truncation is reported honestly via two independent signals:
          * ``matched > limit`` -- the client-side cap dropped matches, OR
          * ``scanned == fetch_limit`` -- the server cap was hit (Loki may
            have more lines beyond the ``query_range`` ``limit``).

        ``tail_lines`` is accepted for protocol compatibility but ignored --
        it is a file-backend hint with no Loki meaning (Loki orders by time).

        Args:
            filt: :class:`LogFilter` applied to every fetched entry.
            since: Window start (default: ``until - 1h``).
            until: Window end (default: ``now`` UTC).
            limit: Max matches to return; ``matched`` may exceed this.
            tail_lines: Ignored (file-backend hint).

        Returns:
            :class:`ScanResult` with up to ``limit`` matched entries.
        """
        end_dt = until if until is not None else datetime.now(timezone.utc)
        start_dt = since if since is not None else end_dt - timedelta(hours=1)

        entries = self._fetch_entries(
            start=self._to_rfc3339_nano(start_dt),
            end=self._to_rfc3339_nano(end_dt),
            limit=self._fetch_limit,
            direction=self._direction,
        )

        scanned = len(entries)
        matched_entries = [e for e in entries if log_common.entry_matches(e, filt)]
        matched = len(matched_entries)

        truncated = (matched > limit) or (scanned == self._fetch_limit)

        return ScanResult(
            entries=matched_entries[:limit],
            matched=matched,
            scanned=scanned,
            truncated=truncated,
        )

    def await_one(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        timeout_s: float,
        poll_interval_ms: int,
        tail_lines: int,
    ) -> AwaitResult:
        """Block for the next matching entry, or time out and return ``None``.

        Two modes share a Phase 1 historical read:

          * **One-shot** (``timeout_s <= 0``): a single ``backward``
            ``query_range`` over ``[since or now-1h, now]`` (limit =
            ``fetch_limit``). The first entry satisfying ``filt`` (via
            :func:`log_common.entry_matches`) is returned; otherwise
            ``entry=None``. ``scanned`` is the count fetched.

          * **Poll** (``timeout_s > 0``): Phase 1 is the same backward read
            (count its ``scanned``); if it matches, return immediately.
            Phase 2 loops until the deadline (injectable ``monotonic``):
            each iteration does a ``forward`` ``query_range`` whose ``start``
            is the **last-seen Loki timestamp** high-water, filters for new
            matches, and returns on the first one; otherwise it sleeps
            ``poll_interval_ms/1000`` and re-checks the deadline. Fetch-first
            (not sleep-first) so the first poll fires immediately after Phase
            1. An entry re-included at a timestamp already covered is deduped
            client-side (counted/matched once), so ``scanned`` never
            double-counts. On deadline, return ``entry=None`` with cumulative
            ``scanned``.

        The high-water is the max entry ``timestamp`` string seen so far
        (RFC3339Nano UTC strings sort lexically, so a string ``max`` suffices
        for same-format values). It is seeded from Phase 1's window start so a
        Phase 1 with no entries still anchors the first forward query.

        ``tail_lines`` is accepted for protocol compatibility but ignored
        (file-backend hint; Loki orders by time).

        Args:
            filt: :class:`LogFilter` applied to every fetched entry.
            since: Phase 1 window start (default: ``now - 1h``).
            timeout_s: ``<= 0`` for one-shot; ``> 0`` polls until this many
                seconds of wall clock (per injected ``monotonic``) elapse.
            poll_interval_ms: Sleep between forward polls (poll mode only).
            tail_lines: Ignored (file-backend hint).

        Returns:
            :class:`AwaitResult` with the first match (or ``None``),
            cumulative ``scanned``, and ``elapsed_ms`` from the injected
            monotonic clock.
        """
        start_wall = self._monotonic()

        # Resolve the Phase 1 window once. `end`/`start` are real wall-clock
        # values; tests inject a canned HTTP client that ignores them. The
        # deadline below uses the injected `monotonic` so polling is
        # deterministic without real waiting.
        now_dt = datetime.now(timezone.utc)
        start_dt = since if since is not None else now_dt - timedelta(hours=1)
        # Z-suffixed so the high-water seed matches Loki's entry-timestamp
        # format (Loki emits RFC3339Nano with ``Z``; ``_to_rfc3339_nano``
        # emits ``+00:00``). The seed is lexically compared against entry
        # timestamps below, so the formats must agree.
        window_start_ts = self._to_rfc3339_nano(start_dt).replace("+00:00", "Z")

        # -- Phase 1: one backward historical read (both modes) --------------
        entries = self._fetch_entries(
            start=window_start_ts,
            end=self._to_rfc3339_nano(now_dt),
            limit=self._fetch_limit,
            direction="backward",
        )
        scanned = len(entries)

        # High-water: max entry ts seen, or the window start if nothing came
        # back (so the first forward query has a stable anchor).
        last_ts = (
            max(e.timestamp for e in entries) if entries else window_start_ts
        )

        for entry in entries:
            if log_common.entry_matches(entry, filt):
                return AwaitResult(
                    entry=entry,
                    scanned=scanned,
                    elapsed_ms=self._elapsed_ms_since(start_wall),
                )

        # One-shot mode: no Phase 2 polling.
        if timeout_s <= 0:
            return AwaitResult(
                entry=None,
                scanned=scanned,
                elapsed_ms=self._elapsed_ms_since(start_wall),
            )

        # -- Phase 2: fetch-first forward poll loop until the deadline -------
        # Fetch-first: the FIRST action is a forward fetch (responsive --
        # checks for new entries immediately after Phase 1, then sleeps
        # between polls). On no match, sleep and re-check the deadline at
        # the top of the loop.
        deadline = start_wall + timeout_s
        while self._monotonic() < deadline:
            now_dt = datetime.now(timezone.utc)
            entries = self._fetch_entries(
                start=last_ts,
                end=self._to_rfc3339_nano(now_dt),
                limit=self._fetch_limit,
                direction="forward",
            )

            # Dedup: a forward response with start=last_ts can re-include
            # entries at exactly `last_ts`. Count/match only the strictly-new
            # ones (timestamp > last_ts) so each entry is seen exactly once.
            new_entries = [e for e in entries if e.timestamp > last_ts]
            scanned += len(new_entries)

            # Advance the high-water past the newest timestamp in this batch.
            if entries:
                newest = max(e.timestamp for e in entries)
                if newest > last_ts:
                    last_ts = newest

            for entry in new_entries:
                if log_common.entry_matches(entry, filt):
                    return AwaitResult(
                        entry=entry,
                        scanned=scanned,
                        elapsed_ms=self._elapsed_ms_since(start_wall),
                    )

            # No match this poll; sleep before re-checking the deadline.
            # Guard against poll_interval_ms == 0 (would otherwise busy-wait).
            self._sleep(max(poll_interval_ms, 1) / 1000)

        # Deadline exhausted with no match.
        return AwaitResult(
            entry=None,
            scanned=scanned,
            elapsed_ms=self._elapsed_ms_since(start_wall),
        )

    def _elapsed_ms_since(self, start_wall: float) -> int:
        """Wall-clock ms elapsed since ``start_wall`` per the injected clock."""
        return int((self._monotonic() - start_wall) * 1000)

    def follow(
        self,
        filt: LogFilter,
        *,
        stop_event: threading.Event,
        poll_interval_ms: int,
    ) -> Iterator[CanonicalEntry]:
        """Stream matching entries as they arrive, until ``stop_event`` is set.

        A poll generator (DESIGN §9.2). Each cycle:

          1. If ``stop_event`` is set, return.
          2. Issue a ``forward`` ``query_range`` over ``[start, now]`` (limit
             = ``fetch_limit``), where ``start`` is seeded to now on the first
             cycle and advanced to the newest emitted timestamp thereafter.
          3. Apply :func:`log_common.entry_matches`; skip any
             ``(timestamp, message)`` pair already emitted (dedup set);
             yield new matches.
          4. After each yield, re-check ``stop_event`` (prompt termination).
          5. If nothing new was emitted, sleep ``poll_interval_ms/1000`` in
             small chunks (interruptible -- a set ``stop_event`` terminates
             within one chunk of being set).

        Error tolerance:

          * The **first** fetch lets ALL exceptions propagate so a startup
            auth/connect/bad-query failure surfaces via the streaming
            startup-error path (ARCH §6).
          * On **subsequent** fetches, :class:`ConnectionFailure` and
            :class:`OperationTimeout` are swallowed with a one-line stderr
            warning (``agctl: loki follow transient error: ...``) and
            retried on the next cycle (transient network blip).
            :class:`ConfigError` (bad query / non-stream result) ALWAYS
            propagates -- it is permanent.

        Known limitation: a mid-stream auth change (401 appearing after a
        successful start) is swallowed and retried rather than surfaced;
        distinguishing it from a transient blip is deferred to a future
        revision.

        Args:
            filt: :class:`LogFilter` applied to every fetched entry.
            stop_event: When set, the generator returns promptly (checked
                before each fetch, after each yield, and between sleep
                chunks).
            poll_interval_ms: Sleep between polls when no new entries were
                emitted.

        Yields:
            :class:`CanonicalEntry` instances matching ``filt``, each
            emitted at most once.
        """
        # Seed the high-water ``start`` to "now". Subsequent cycles advance
        # it to the newest emitted entry's timestamp; a cycle that emits
        # nothing leaves ``start`` unchanged (re-fetches the same window).
        start = self._to_rfc3339_nano(datetime.now(timezone.utc))
        emitted: set[tuple[str, str]] = set()
        first_fetch = True

        while not stop_event.is_set():
            end = self._to_rfc3339_nano(datetime.now(timezone.utc))
            try:
                entries = self._fetch_entries(
                    start=start,
                    end=end,
                    limit=self._fetch_limit,
                    direction="forward",
                )
            except (ConnectionFailure, OperationTimeout) as exc:
                # First-fetch errors ALWAYS propagate (startup-error path).
                # ConfigError is not caught here and likewise propagates on
                # any fetch -- it is permanent, not a transient blip.
                if first_fetch:
                    raise
                print(
                    f"agctl: loki follow transient error: {exc}",
                    file=sys.stderr,
                )
                self._interruptible_sleep(stop_event, poll_interval_ms)
                continue

            first_fetch = False
            newest_ts: str | None = None
            yielded_any = False

            for entry in entries:
                if stop_event.is_set():
                    return
                if not log_common.entry_matches(entry, filt):
                    continue
                key = (entry.timestamp, entry.message)
                if key in emitted:
                    continue
                emitted.add(key)
                if newest_ts is None or entry.timestamp > newest_ts:
                    newest_ts = entry.timestamp
                yielded_any = True
                yield entry
                if stop_event.is_set():
                    return

            if yielded_any and newest_ts is not None:
                # Advance the high-water to the newest emitted timestamp.
                start = newest_ts
            else:
                # Nothing new this cycle; sleep interruptibly before polling.
                self._interruptible_sleep(stop_event, poll_interval_ms)

    def _interruptible_sleep(
        self, stop_event: threading.Event, total_ms: int
    ) -> None:
        """Sleep for ``total_ms`` in chunks, exiting early if ``stop_event`` sets.

        Chunked so a production :func:`time.sleep` does not block longer
        than ``_SLEEP_CHUNK_MS`` past a stop signal. The injected test seam
        :attr:`_sleep` is called once per chunk (so a test sleep that counts
        calls sees one call per chunk, not one per logical sleep).
        """
        remaining_ms = max(int(total_ms), 1)
        while remaining_ms > 0 and not stop_event.is_set():
            step = min(_SLEEP_CHUNK_MS, remaining_ms)
            self._sleep(step / 1000)
            remaining_ms -= step

    def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor:
        """Infer field-presence patterns from a recent sample of entries.

        Fetches up to ``sample_lines`` entries from the last hour
        (``direction="backward"`` so the newest lines are sampled first) and
        delegates to :func:`log_common.infer_schema` for the union of present
        standard slots, conditional slots, and observed ``fields`` keys.

        Args:
            sample_lines: Max lines to sample (``query_range`` ``limit``).

        Returns:
            :class:`SchemaDescriptor` summarizing the sample.
        """
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=1)
        entries = self._fetch_entries(
            start=self._to_rfc3339_nano(start_dt),
            end=self._to_rfc3339_nano(end_dt),
            limit=sample_lines,
            direction="backward",
        )
        return log_common.infer_schema(entries)

    @staticmethod
    def _to_rfc3339_nano(dt: datetime) -> str:
        """Convert a :class:`datetime` to an RFC3339Nano UTC string.

        Naive datetimes are treated as UTC (not local time) so the default
        lookback math stays host-independent. Output is ``isoformat()`` in
        UTC (microsecond precision; accepted by Loki's ``query_range``).
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
