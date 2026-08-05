# Loki Log Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a built-in `type: loki` `LogBackend` so `agctl logs query/assert/tail` reach a Loki HTTP endpoint, plus the shared foundation (an `options` escape hatch on `LogSource` and a `log_common` normalize/filter module) every later backend inherits.

**Architecture:** Vertical slice. The LogQL `query` is user-authored and runs server-side via Loki's `query_range`; agctl's filters apply client-side over fetched entries (identical to the file backend); `follow()` polls `query_range` with an advancing `start` (no websocket). httpx stays lazy-imported behind a new `loki` extra. No `LogBackend` protocol refactor and no shared remote-HTTP base class — both deferred until a second remote backend proves the need.

**Tech Stack:** Python ≥3.11, Pydantic v2, Click, httpx (lazy), jq (lazy via `agctl.assertions.jq_bool`), pytest; testcontainers + Docker for opt-in integration.

## Global Constraints

- Python ≥3.11; `from __future__ import annotations`; modern typing (`dict[str, X]`, `X | None`, `list[...]`).
- **httpx is lazy-imported inside methods, never at module top.** Importing `agctl.clients.log_backends.loki` must not require httpx. A missing httpx → `ConfigError(message, {"type": "loki"})`, exit 2, message naming `pip install 'agctl[loki]'`.
- **jq stays lazy** via `agctl.assertions.jq_bool` / `compile_jq` (already lazy in `assertions.py`). Missing jq with `--match` → `ConfigError` exit 2 pointing at `agctl[loki]` (the existing `_build_log_filter` rewrite in `logs_commands.py` already handles the message; do not duplicate).
- **One-JSON-envelope contract is preserved.** The command layer `agctl/commands/logs_commands.py` is **unchanged** by this plan — Loki is transparent behind `LogClient`. Do not edit it.
- **Error → exit-code mapping is fixed:** `ConfigError`→2, `ConnectionFailure`→2, `OperationTimeout`→1, `AssertionFailure`→1. All constructed as `Cls(message, detail_dict)`.
- **Behavior-preserving refactor:** the existing `tests/unit/test_logs_client.py` suite (file backend) stays green after the `log_common` extraction.
- **${ENV} interpolation** of `url`/`query`/`options` values is handled by the existing config loader (`config/loader.py`); no backend work.
- **Secret masking** of `options.password`/`options.token` already works (`config_commands._mask` recurses into nested dicts; `_is_secret` matches `password`/`token`). No masking change.
- **Unit tests must run without httpx/Docker.** Inject a duck-typed `http_client` for the happy path and HTTP-status mapping; `pytest.importorskip("httpx")` only for transport-exception tests; skip integration without `AGCTL_TEST_LIVE`/`AGCTL_TEST_LOKI_URL`.
- Fast gate: `pytest tests/unit` passes. DESIGN.md / ARCHITECTURE.md / `skills/` sync is **out of scope** here — handled by the `docs-watcher` subagent after implementation.

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `agctl/clients/log_common.py` | Shared slot-mapping normalization, per-entry filtering, schema inference | **Create** |
| `agctl/clients/log_backends/ndjson_file.py` | File backend (thinner: delegates normalize/filter/schema to `log_common`) | **Modify** (behavior-preserving) |
| `agctl/clients/log_backends/loki.py` | Loki `LogBackend` (5 protocol methods) | **Create** |
| `agctl/clients/log_client.py` | Backend dispatch; register `loki` built-in | **Modify** |
| `agctl/config/models.py` | `LogSource` gains `url`/`query`/`options` | **Modify** |
| `pyproject.toml` | Add `loki` optional extra | **Modify** |
| `tests/unit/test_log_common.py` | Shared-helper unit tests | **Create** |
| `tests/unit/test_loki_backend.py` | Loki backend unit tests | **Create** |
| `tests/unit/test_logs_client.py` | Regression guard for file-backend refactor (extend only if a gap appears) | **Modify** (minimal) |
| `tests/unit/test_config_models.py` (or existing config test file) | `LogSource` new-field parse tests | **Modify** |
| `tests/integration/conftest.py` | `require_loki` fixture + `_start_loki` | **Modify** |
| `tests/integration/test_logs_loki.py` | Self-skipping live Loki test | **Create** |

**Task dependency graph (subagent-driven-development runs sequentially; order below respects all deps):**

```
1 (log_common) ──► 2 (refactor file backend) ──┐
1 ──► 4 (loki skeleton) ──► 5 (scan) ──► 6 (await_one) ──► 7 (follow)
3 (LogSource model) ──► 4
4 + 3 ──► 8 (register + extra) ──► 9 (integration)
```
Parallelizable (if desired): Task 3 is independent of 1–2. Tasks 5/6/7 are mutually independent but all edit `loki.py`, so run sequentially to avoid file conflicts.

---

### Task 1: Extract `log_common.py` (shared normalize + filter + schema)

**Files:**
- Create: `agctl/clients/log_common.py`
- Test: `tests/unit/test_log_common.py`

**Interfaces:**
- Consumes: `CanonicalEntry`, `LogFilter`, `SchemaDescriptor` from `agctl.clients.log_backend_protocol`; `jq_bool` from `agctl.assertions` (lazy-imported inside `entry_matches` only when `filt.match_jq` is set); `dataclasses.asdict`; `fnmatch.fnmatch`.
- Produces (exact public surface, used by Tasks 2, 4, 5, 7):
  - `SLOT_SOURCE_SET: frozenset[str]` — the logstash source keys `{"@timestamp","level","logger_name","thread_name","message","service","stack_trace","tags"}` (moved verbatim from `ndjson_file._SLOT_SOURCE_SET`).
  - `normalize_dict(raw: dict, *, service: str | None = None, ts_override: str | None = None) -> CanonicalEntry` — maps a parsed logstash JSON dict to `CanonicalEntry`: `timestamp = ts_override if ts_override is not None else raw.get("@timestamp")`; `level = str(raw.get("level","")).upper()`; `logger = raw.get("logger_name")`; `message = raw.get("message")`; `thread = raw.get("thread_name")`; `service = service if service is not None else raw.get("service")`; `stack_trace = raw.get("stack_trace")`; `tags = raw.get("tags")`; `fields = {k:v for k,v in raw.items() if k not in SLOT_SOURCE_SET}`. (Generalizes `NdjsonFileBackend._normalize`: overrides are optional; with none set, byte-for-byte the current mapping.)
  - `entry_matches(entry: CanonicalEntry, filt: LogFilter) -> bool` — AND of: (a) `filt.level is None or entry.level == filt.level.upper()`; (b) `filt.logger_glob is None or fnmatch.fnmatch(entry.logger or "", filt.logger_glob)`; (c) `filt.message_substring is None or filt.message_substring in (entry.message or "")`; (d) `filt.match_jq is None or jq_bool(dataclasses.asdict(entry), filt.match_jq)`. jq is lazy-imported only in branch (d). (Extracts the filter block currently triplicated in `_read_window`/`_read_increment`/`follow`.)
  - `infer_schema(entries: list[CanonicalEntry]) -> SchemaDescriptor` — over the given (already-normalized) entries: `standard` = sorted union of present non-empty standard slots (`timestamp`,`level`,`logger`,`message`,`thread`,`service`); `conditional` = sorted union of present `stack_trace`/`tags`; `observed` = sorted union of all `entry.fields` keys excluding `{"@version","level_value"}`. (Extracts the presence-tracking currently inside `NdjsonFileBackend.sample_schema`.)

- [ ] **Step 1: Write the failing tests** (`tests/unit/test_log_common.py`)

  Each test constructs inputs directly and asserts the exact result:
  - `normalize_dict({"@timestamp":"2026-07-22T12:00:00Z","level":"error","logger_name":"c.F","message":"hi","extra":1})` → `CanonicalEntry` with `timestamp=="2026-07-22T12:00:00Z"`, `level=="ERROR"` (upper-cased), `logger=="c.F"`, `message=="hi"`, `thread is None`, `service is None`, `stack_trace is None`, `tags is None`, `fields=={"extra":1}`.
  - `normalize_dict(raw, service="svc", ts_override="2026-07-22T12:00:00.123Z")` on a raw with its own `@timestamp=="X"` and no `service` → `timestamp=="2026-07-22T12:00:00.123Z"` (override wins), `service=="svc"` (param wins), and `@timestamp`/`service` excluded from `fields`.
  - `entry_matches` with `LogFilter(level="error")` against an entry whose `level=="ERROR"` → `True`; against `level=="INFO"` → `False` (case-insensitive on the filter).
  - `entry_matches` with `LogFilter(logger_glob="com.example.*")` → True for `logger=="com.example.OrderService"`, False for `"org.other.X"`; with `message_substring="failed"` → True when substring present, False otherwise.
  - `entry_matches` with `LogFilter(match_jq=".level == \"ERROR\"")` → True for an ERROR entry, False for INFO. (Guard this assertion's jq branch with `pytest.importorskip("jq")` so it skips if jq isn't installed.)
  - `entry_matches` with an empty `LogFilter()` (all None) → `True` for any entry (no filtering).
  - `infer_schema([entry_with_stack_trace_and_tags, entry_with_fields_only])` → `standard` includes the present slots, `conditional==["stack_trace","tags"]` (when present), `observed` lists the custom field names minus `@version`/`level_value`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_log_common.py -v`
  Expected: FAIL — `ModuleNotFoundError: No module named 'agctl.clients.log_common'`.

- [ ] **Step 3: Implement `log_common.py`**

  Create the module with exactly the four public symbols above. Module top imports only stdlib (`dataclasses`, `fnmatch`) plus the DTOs from `log_backend_protocol`; `jq_bool` is imported lazily inside `entry_matches` branch (d) so a missing jq is only hit when a `match_jq` filter is actually used.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_log_common.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/log_common.py tests/unit/test_log_common.py`
  Run: `git commit -m "feat(logs): extract shared normalize/filter/schema into log_common"`

---

### Task 2: Refactor `NdjsonFileBackend` to delegate to `log_common` (behavior-preserving)

**Files:**
- Modify: `agctl/clients/log_backends/ndjson_file.py`
- Test (regression): `tests/unit/test_logs_client.py` (existing — must stay green; no new tests unless a gap appears)

**Interfaces:**
- Consumes: `log_common.normalize_dict`, `log_common.entry_matches`, `log_common.infer_schema`, `log_common.SLOT_SOURCE_SET` (from Task 1). Keeps its own file-specific concerns: `_tail_lines`, `_drain_complete_lines`, `_seed_offset`, `_read_increment` byte-offset mechanics, and the **time-window check** (`since`/`until` on `entry.timestamp` via `_parse_iso_datetime`/`_to_utc`) which stays inline (Loki does windowing server-side, so windowing is NOT part of the shared `entry_matches`).
- Produces: unchanged external behavior — `NdjsonFileBackend.scan`/`await_one`/`follow`/`sample_schema` return identical results for identical inputs (pinned by the existing suite).

- [ ] **Step 1: Capture the regression baseline**

  Run: `pytest tests/unit/test_logs_client.py -v`
  Expected: PASS (current green baseline). If any test already fails, stop and report — do not refactor on a red baseline.

- [ ] **Step 2: Refactor — replace inline logic with `log_common` calls**

  Behavior-preserving substitutions (no semantic change):
  - Delete `_SLOT_SOURCE_SET` (now imported from `log_common`) and `_normalize`; call `log_common.normalize_dict(raw)` (no overrides — file lines carry their own `@timestamp`/`service`).
  - In `_read_window`: keep the window check (`since`/`until`), keep `scanned += 1`, but replace the four-filter block (level/logger/message/jq) with `if not log_common.entry_matches(entry, filt): continue`.
  - In `_read_increment` and `follow`: same replacement of the inline four-filter block with `log_common.entry_matches(entry, filt)`.
  - In `sample_schema`: replace the per-line presence-tracking body with: read lines → `normalize_dict` each → collect into a list → `return log_common.infer_schema(entries)`. Keep the file-reading (`_tail_lines`) and the empty-result short circuits (missing path / missing file → `SchemaDescriptor([], [], [])`).
  - Remove now-unused imports (`fnmatch` if no longer used directly; keep `jq_bool` import only if still referenced — it should no longer be, since `entry_matches` owns jq).

- [ ] **Step 3: Run the full file-backend suite — must stay green**

  Run: `pytest tests/unit/test_logs_client.py -v`
  Expected: PASS (identical count to baseline). Any new failure means the refactor changed behavior — fix before proceeding.

- [ ] **Step 4: Run the shared-helper suite too (cross-check)**

  Run: `pytest tests/unit/test_log_common.py tests/unit/test_logs_client.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/log_backends/ndjson_file.py`
  Run: `git commit -m "refactor(logs): NdjsonFileBackend delegates normalize/filter/schema to log_common"`

---

### Task 3: `LogSource` model — add `url`, `query`, `options`

**Files:**
- Modify: `agctl/config/models.py` (the `LogSource` class, currently lines 394–400)
- Test: the existing config-models test file — locate it via `grep -rl "LogSource" tests/unit/` and extend it (create `tests/unit/test_log_source.py` if no focused file exists)

**Interfaces:**
- Consumes: `from typing import Any` (already imported in `models.py`); `pydantic.Field`.
- Produces: `LogSource` gains three fields (defaults keep existing `file` sources valid):
  - `url: str | None = None`
  - `query: str | None = None`
  - `options: dict[str, Any] = Field(default_factory=dict)`
  The model stays strict (no `extra="allow"`); unknown keys still error. `options` is the intentional escape hatch for backend-specific YAML.

- [ ] **Step 1: Write the failing tests**

  - Parsing a `logs.sources.loki-svc` block with `type: loki`, `url: "http://loki:3100"`, `query: '{app="x"}'`, `service: loki-svc`, and `options: {username: "u", password: "p", token: "t", org_id: "o", verify_tls: false, fetch_limit: 10, direction: "backward"}` yields a `LogSource` with each field populated and `options` preserving all keys/types (`options["verify_tls"] is False`, `options["fetch_limit"] == 10`).
  - A minimal `file` source (`type: file`, `path: /x`) still parses with `url is None`, `query is None`, `options == {}` (backward compatible).
  - An unknown top-level key on a source (e.g. `bogus: 1`) still raises a Pydantic `ValidationError` (strict model preserved — only `options` accepts arbitrary keys).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_log_source.py -v` (or the located file)
  Expected: FAIL — `url`/`query`/`options` not accepted (validation error on the loki block).

- [ ] **Step 3: Add the three fields to `LogSource`**

  Add the fields with the defaults above. Update the class docstring to note `url`/`query` are remote-backend fields and `options` is the backend-specific escape hatch (mirroring `DatabaseConnection.options`). Keep `type`/`path`/`format`/`service` unchanged.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_log_source.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/config/models.py tests/unit/test_log_source.py`
  Run: `git commit -m "feat(config): LogSource gains url/query/options for remote backends"`

---

### Task 4: `LokiBackend` skeleton — constructor, `validate_config`, httpx request helper, error mapping, line normalization

**Files:**
- Create: `agctl/clients/log_backends/loki.py`
- Test: `tests/unit/test_loki_backend.py`

**Interfaces:**
- Consumes: `CanonicalEntry`, `LogFilter`, `ScanResult`, `AwaitResult`, `SchemaDescriptor` from `log_backend_protocol`; `log_common.normalize_dict`; `ConfigError`, `ConnectionFailure`, `OperationTimeout` from `agctl.errors`; the `LogSource` fields from Task 3 (`url`, `query`, `options`, `service`); `time.monotonic`/`time.sleep` injectable.
- Produces (the skeleton later tasks build on):
  - `class LokiBackend` with `__init__(self, source, *, http_client=None, monotonic=time.monotonic, sleep=time.sleep)`. Stores `source`; exposes injectable `http_client`/`monotonic`/`sleep` for tests.
  - `validate_config(self) -> None` — §5.3 rules (see Task 4 Step 3 for the exact cases).
  - `_fetch_entries(self, *, start: str, end: str, limit: int, direction: str, timeout: float = 10.0) -> list[CanonicalEntry]` — the single HTTP read path used by `scan`/`await_one`/`follow`/`sample_schema`. Builds `query_range` params, sends the request via the (real lazy-imported httpx or injected) client, maps errors (table below), asserts `resultType == "streams"`, and returns normalized entries (one per `[ts, line]`, unfiltered). Timestamps: `start`/`end` are RFC3339Nano UTC strings; `timeout` is the per-request httpx timeout (callers pass `defaults.timeout_seconds`/`--timeout`).
  - `_normalize_loki_line(self, line: str, ts: str) -> CanonicalEntry` — try `json.loads(line)`; if it yields a `dict`, return `log_common.normalize_dict(d, service=self._service, ts_override=ts)`; otherwise return `CanonicalEntry(timestamp=ts, level="", logger="", message=line, service=self._service)`. (Non-JSON lines become `message` — Loki's plaintext case. This is Loki-specific; the file backend skips non-JSON, so this policy lives here, not in `log_common`.)
  - **Resolved config (read once, in `__init__` or a helper):** `self._url = source.url`, `self._query = source.query`, `self._service = source.service` (None if unset — see reconciliation note), and from `source.options`: `username`, `password`, `token`, `org_id`, `verify_tls` (default `True`), `fetch_limit` (default `500`), `direction` (default `"forward"`).

  **Reconciliation note (spec §5.2 said "service defaults to the source name"):** the backend receives the `LogSource` object, not its config key, so it cannot default to the source name. `service` is `source.service` or `None` — matching the file backend. Defaulting to the source name would require threading the key through `LogClient`; deferred.

  **Loki `query_range` contract (baked in so the implementer need not look it up):**
  - `GET {url}/loki/api/v1/query_range` with params `query`, `start`, `end`, `limit`, `direction`.
  - Success body: `{"status":"success","data":{"resultType":"streams","result":[{"stream":{<labels>},"values":[["<RFC3339Nano ts>","<line>"], ...]}]}}`. Flatten `values` across all `result` entries into `(ts, line)` pairs, preserving order.
  - Error body: `{"status":"error", ...}` (HTTP 4xx/5xx).

  **Auth/TLS construction (per request):** basic → httpx `auth=(username, password)`; bearer → header `Authorization: Bearer {token}`; org-id → header `X-Scope-OrgID: {org_id}`; TLS → `verify=verify_tls`. Per-request `timeout` comes from `defaults.timeout_seconds`/`--timeout` (passed by the command layer as needed; for the skeleton, accept a `timeout` param on `_fetch_entries`, default e.g. 10.0).

  **Error mapping (exact):**

  | Condition | Raise |
  |---|---|
  | httpx import fails (no injected client) | `ConfigError("loki backend requires httpx: pip install 'agctl[loki]'", {"type":"loki"})` |
  | status 200 but `resultType != "streams"` | `ConfigError("Loki returned non-stream result ... use a log selector query, not a metric query", {"result_type": <actual>})` |
  | status 400 | `ConfigError(<loki error body or text>, {"status":400, "body": <text>})` |
  | status 401 / 403 | `ConnectionFailure("Loki auth failed (HTTP <code>)", {"status": <code>})` |
  | status 5xx / other non-200 | `ConnectionFailure("Loki request failed (HTTP <code>)", {"status": <code>, "body": <text>})` |
  | `httpx.ConnectError` / `httpx.ConnectTimeout` | `ConnectionFailure("Loki unreachable: <exc>", {})` |
  | `httpx.ReadTimeout` / other `httpx.TimeoutException` | `OperationTimeout("Loki read timed out", {})` |
  | other `httpx.HTTPError` | `ConnectionFailure("Loki request error: <exc>", {})` |

- [ ] **Step 1: Write the failing tests** (`tests/unit/test_loki_backend.py`)

  Construct `LokiBackend` from a `LogSource`-like object (a small dataclass/fake exposing `url`/`query`/`options`/`service`, or a real `LogSource` from `config.models`). Inject a **duck-typed fake `http_client`** whose `.get(url, params=, headers=, auth=, timeout=, verify=)` returns a fake response with `.status_code`, `.json()`, `.text`. Each test asserts exact behavior:
  - **`validate_config`** — (a) missing `url` → `ConfigError`; (b) missing `query` → `ConfigError`; (c) `url` not `http://`/`https://` (e.g. `"ftp://x"`) → `ConfigError`; (d) both `username` and `token` set → `ConfigError` (basic/bearer conflict); (e) `username` set without `password` → `ConfigError`; (f) valid minimal config (url + query only) → no raise.
  - **`_fetch_entries` request construction** — fake records the call; assert the URL is `{url}/loki/api/v1/query_range`, `params` includes `query`, `start`, `end`, `limit`, `direction`, and returns the flattened entries from a 2-stream success body in order.
  - **Auth headers** — with basic (`username`+`password`) the fake sees `auth==(u,p)` and no bearer header; with `token` it sees header `Authorization: Bearer <token>` and `auth is None`; with `org_id` it sees `X-Scope-OrgID`; with none, neither auth nor org header.
  - **`verify_tls`** — `options.verify_tls=False` → fake sees `verify is False`; default (unset) → `verify is True`.
  - **`_normalize_loki_line`** — a JSON line `{"level":"ERROR","message":"boom","orderId":"o1"}` at ts `T` → `CanonicalEntry(timestamp="T", level="ERROR", message="boom", fields={"orderId":"o1"})`; a plain-text line `"hello"` at ts `T` → `CanonicalEntry(timestamp="T", level="", logger="", message="hello")`.
  - **Error mapping** — fake returns 400 with body `{"status":"error","error":"parse error"}` → `_fetch_entries` raises `ConfigError` (exit 2) whose message/detail carries the body; 401 → `ConnectionFailure`; 503 → `ConnectionFailure`; 200 with `resultType=="matrix"` → `ConfigError`. (These need no httpx — duck-typed fake.)
  - **Transport-exception mapping** — `pytest.importorskip("httpx")`; inject an `httpx.Client(transport=httpx.MockTransport(...))` whose handler raises `httpx.ConnectError` → `ConnectionFailure`; raises `httpx.ReadTimeout` → `OperationTimeout`.
  - **Missing httpx** — do NOT inject a client; `monkeypatch.setitem(sys.modules, "httpx", None)` (or raise on import) so the lazy import fails → `_fetch_entries` raises `ConfigError` naming `pip install 'agctl[loki]'`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_loki_backend.py -v`
  Expected: FAIL — `ModuleNotFoundError: No module named 'agctl.clients.log_backends.loki'`.

- [ ] **Step 3: Implement the skeleton**

  Create `loki.py`: the class, constructor, config resolution, `validate_config` (§5.3 rules), `_fetch_entries` (lazy httpx import; build params/headers/auth/verify; send; map errors per the table; assert streams; flatten+normalize), and `_normalize_loki_line`. Module top stays httpx-free. Do NOT yet implement `scan`/`await_one`/`follow`/`sample_schema` beyond stubs that raise `NotImplementedError` (later tasks fill them) — but the stubs are not tested yet, so they won't block.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_loki_backend.py -v`
  Expected: PASS (all skeleton tests).

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/log_backends/loki.py tests/unit/test_loki_backend.py`
  Run: `git commit -m "feat(logs): LokiBackend skeleton — config, query_range fetch, error mapping"`

---

### Task 5: `LokiBackend.scan` + truncation honesty + `sample_schema`

**Files:**
- Modify: `agctl/clients/log_backends/loki.py`
- Test: `tests/unit/test_loki_backend.py` (extend)

**Interfaces:**
- Consumes: `_fetch_entries` and `_normalize_loki_line` (Task 4); `log_common.entry_matches`/`log_common.infer_schema` (Task 1); `datetime` → RFC3339Nano conversion for `start`/`end`.
- Produces:
  - `scan(self, filt, *, since, until, limit, tail_lines) -> ScanResult` — one `_fetch_entries` call: `start` = `since` or `now - 1h` (default lookback); `end` = `until` or `now`; `limit` = `options.fetch_limit`; `direction` = `options.direction` (default `"forward"`). Normalize already done in `_fetch_entries`; apply `entry_matches` client-side; `scanned` = entries fetched; `matched` = count passing the filter; return up to `limit` matches; `truncated = (matched > limit) or (scanned == fetch_limit)`. `tail_lines` is ignored (documented file-backend hint).
  - `sample_schema(self, *, sample_lines=100) -> SchemaDescriptor` — `_fetch_entries(start=now-1h, end=now, limit=sample_lines, direction="backward")`, then `log_common.infer_schema(entries)`.

- [ ] **Step 1: Write the failing tests**

  - **Happy scan** — fake returns 3 logstash-JSON lines (levels ERROR/WARN/INFO, one with `orderId:"o1"`); `scan(filt=LogFilter(level="ERROR"), since=<T-1m>, until=<T>, limit=10, tail_lines=200)` → `ScanResult` with `matched==1`, `scanned==3`, `truncated is False`, `entries` length 1 (the ERROR entry).
  - **`--match` filter** — `LogFilter(match_jq='.fields.orderId == "o1"')` over the 3 lines → `matched==1` (guard jq branch with `pytest.importorskip("jq")`).
  - **Truncation (a)** — `matched > limit`: fake returns 5 matching lines, `limit=2` → `matched==5`, `len(entries)==2`, `truncated is True`.
  - **Truncation (b)** — server cap: fake returns exactly `fetch_limit` entries (set `options.fetch_limit=4`, fake returns 4), `limit=10`, all match → `truncated is True` (server-cap signal) even though `matched(4) <= limit(10)`.
  - **No truncation** — fake returns 2 entries, all match, `limit=10`, `fetch_limit=500` → `truncated is False`.
  - **Default lookback** — `since is None` → assert the request's `start` param is ~1h before `end` (fake captures params; assert `start` parses to a timestamp within a few seconds of `now-1h`).
  - **`tail_lines` ignored** — passing any `tail_lines` value does not change the request or result.
  - **`sample_schema`** — fake returns lines with `stack_trace`/`tags` and a custom `requestId` field → `SchemaDescriptor.standard` includes present slots, `conditional` includes `stack_trace`/`tags`, `observed` includes `requestId`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_loki_backend.py -k "scan or sample_schema" -v`
  Expected: FAIL (scan/sample_schema still `NotImplementedError` stubs or absent).

- [ ] **Step 3: Implement `scan` and `sample_schema`**

  Per the Produces contracts. Convert `datetime` → RFC3339Nano UTC string for `start`/`end` (e.g. `dt.astimezone(timezone.utc).isoformat()` or the `.strftime`/nanosecond form — pick one and keep it consistent). Apply `entry_matches`; collect up to `limit`; compute both truncation signals.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_loki_backend.py -k "scan or sample_schema" -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/log_backends/loki.py tests/unit/test_loki_backend.py`
  Run: `git commit -m "feat(logs): LokiBackend.scan + sample_schema with honest truncation"`

---

### Task 6: `LokiBackend.await_one` (two-phase, timestamp high-water)

**Files:**
- Modify: `agctl/clients/log_backends/loki.py`
- Test: `tests/unit/test_loki_backend.py` (extend)

**Interfaces:**
- Consumes: `_fetch_entries` (Task 4); `log_common.entry_matches` (Task 1); injectable `monotonic`/`sleep`.
- Produces: `await_one(self, filt, *, since, timeout_s, poll_interval_ms, tail_lines) -> AwaitResult`:
  - **One-shot mode** (`timeout_s <= 0`): one `_fetch_entries(direction="backward", limit=fetch_limit, start=since or now-1h, end=now)`; apply `entry_matches`; return the first match (or `None`) with `scanned` = entries fetched and `elapsed_ms`.
  - **Poll mode** (`timeout_s > 0`): Phase 1 = the same backward historical read once (count its `scanned`). If a match is found, return immediately. Phase 2 = loop until deadline: track a **last-seen Loki timestamp** high-water (`start` = the max entry timestamp seen so far, advancing each cycle so each entry is counted exactly once); `_fetch_entries(direction="forward", start=last_ts, end=now, limit=fetch_limit)`; filter; if a match appears, return it; else `self._sleep(poll_interval_ms/1000)` and re-check the deadline. On timeout, return `AwaitResult(entry=None, scanned=<cumulative>, elapsed_ms=<wall>)`.

- [ ] **Step 1: Write the failing tests**

  Use injectable `monotonic`/`sleep` (a controllable fake clock) so polls are deterministic without real waiting.
  - **One-shot match** — `timeout_s=0`; fake returns one matching entry → `AwaitResult.entry` is that entry, `scanned>=1`, no sleep calls.
  - **One-shot no-match** — `timeout_s=0`; fake returns only non-matching entries → `entry is None`, `scanned` reflects fetched count.
  - **Poll finds after delay** — `timeout_s=5`, `poll_interval_ms=100`; first backward read returns nothing matching; the forward poll returns a matching entry → result entry is the match; assert exactly one `sleep` occurred before the find and `scanned` accumulates both reads.
  - **No double-count** — across two forward polls, an entry present in both responses (same `(ts, line)`) is counted/yielded only once (the high-water `start` advances past it). Construct the fake so poll 2 re-includes poll 1's newest entry; assert `scanned` counts it once and it is not re-matched.
  - **Timeout** — `timeout_s=0.2`, no matches ever; the fake clock advances past the deadline on the first sleep → `entry is None`, `elapsed_ms` reflects the elapsed fake time, the loop terminated by deadline (not infinite).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_loki_backend.py -k "await_one" -v`
  Expected: FAIL.

- [ ] **Step 3: Implement `await_one`**

  Per the Produces contract. The high-water mark is the entry timestamp string (RFC3339Nano sorts lexically for same-format UTC strings, so a string max suffices; if needed, parse to compare). Honor the deadline via the injected `monotonic`.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_loki_backend.py -k "await_one" -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/log_backends/loki.py tests/unit/test_loki_backend.py`
  Run: `git commit -m "feat(logs): LokiBackend.await_one — two-phase timestamp high-water poll"`

---

### Task 7: `LokiBackend.follow` (poll generator + dedup + stop)

**Files:**
- Modify: `agctl/clients/log_backends/loki.py`
- Test: `tests/unit/test_loki_backend.py` (extend)

**Interfaces:**
- Consumes: `_fetch_entries` (Task 4); `log_common.entry_matches` (Task 1); injectable `monotonic`/`sleep`.
- Produces: `follow(self, filt, *, stop_event, poll_interval_ms) -> Iterator[CanonicalEntry]` — a generator: seed `start` to now; each iteration, if `stop_event.is_set()` return; `_fetch_entries(direction="forward", start=last_emitted_ts or now, end=now, limit=fetch_limit)`; for each fetched entry, apply `entry_matches`; skip any whose `(timestamp, message)` pair was already emitted (dedup set); yield new matches, updating the high-water `start` to the newest emitted timestamp; check `stop_event` after each yield; if nothing new, `self._sleep(poll_interval_ms/1000)` (interruptible). Transient HTTP errors are swallowed with a stderr warning and retried next cycle (bounded — permanent errors like 400/401 propagate as exceptions, surfacing via the streaming startup-error path).

- [ ] **Step 1: Write the failing tests**

  Drive the generator with a `threading.Event` stop_event and an injectable `sleep` (make `sleep` set the stop_event on the Nth call to bound the loop deterministically).
  - **Yields new matches across polls** — fake returns `[]` then `[entryA]` then `[entryA, entryB]` across three polls; collecting yields until stop gives `[entryA, entryB]` (A not re-yielded on poll 3).
  - **Dedup** — same `(timestamp, message)` returned on consecutive polls is yielded once.
  - **Filter respected** — `LogFilter(level="ERROR")`; only ERROR entries among fetched ones are yielded.
  - **Stop honored** — setting `stop_event` between yields terminates the generator promptly (no further `_fetch_entries` calls).
  - **Transient error tolerated** — fake raises `ConnectionFailure` once then returns entries on the next poll; the generator does not crash, writes a stderr warning, and eventually yields the entries (assert via `capsys` for the warning and the yielded set). (Use a duck-typed fake raising an `AgctlError`-subclass instance for the transient case.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_loki_backend.py -k "follow" -v`
  Expected: FAIL.

- [ ] **Step 3: Implement `follow`**

  Per the Produces contract. Keep a `set` of emitted `(timestamp, message)` keys; advance `start`; swallow transient `ConnectionFailure`/`OperationTimeout` with a stderr warning and continue; let `ConfigError` (bad query) propagate.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_loki_backend.py -k "follow" -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/clients/log_backends/loki.py tests/unit/test_loki_backend.py`
  Run: `git commit -m "feat(logs): LokiBackend.follow — poll generator with dedup + stop"`

---

### Task 8: Register `loki` built-in + add the `loki` extra

**Files:**
- Modify: `agctl/clients/log_client.py` (the `BUILTIN_BACKENDS` dict and the import)
- Modify: `pyproject.toml` (the `[project.optional-dependencies]` table)
- Test: `tests/unit/test_logs_client.py` (extend) — dispatch/registration; `tests/unit/test_loki_backend.py` or a config test for `config validate` surfacing a bad loki source

**Interfaces:**
- Consumes: `LokiBackend` (Task 4–7); `LogSource` new fields (Task 3).
- Produces:
  - `log_client.py`: `from .log_backends.loki import LokiBackend` added to the existing `ndjson_file` import neighborhood; `BUILTIN_BACKENDS = {"file": NdjsonFileBackend, "loki": LokiBackend}`.
  - `pyproject.toml`: add `loki = ["httpx>=0.27", "jq>=1.6"]` to `[project.optional-dependencies]` (alphabetical-ish placement near `logs`).

- [ ] **Step 1: Write the failing tests**

  - **Dispatch selects loki** — construct `LogClient(LogSource(type="loki", url="http://x", query='{a="b"}'))` (inject `backend` or rely on `BUILTIN_BACKENDS`); assert the selected backend is a `LokiBackend` and `validate_config()` does not raise. (Inject a fake backend that records calls OR assert `isinstance(client._backend, LokiBackend)`.)
  - **Unknown type still errors** — `LogClient(LogSource(type="bogus"))` → `ConfigError` (regression — unchanged behavior).
  - **Built-in wins** — `load_backends()` returns a dict containing both `"file"` and `"loki"`; a fake entry-point registration of `"loki"` does NOT override the built-in (built-ins win — mirror the existing file-backend entry-point test in `test_logs_client.py`).
  - **config validate surfaces a bad loki source** — a config with `logs.sources.x: {type: loki}` (missing `query`) → `agctl config validate` (or the validator unit path) reports an error at path `logs.sources.x` (exit 2). Drive via the validator directly or `CliRunner` on `config validate`.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_logs_client.py -k "loki" -v`
  Expected: FAIL (loki not registered / `LogClient` rejects it as unknown type).

- [ ] **Step 3: Wire registration + extra**

  Add the import and `BUILTIN_BACKENDS` entry in `log_client.py`; add the `loki` extra in `pyproject.toml`. No entry-point registration (the `agctl.logs_backends` group stays third-party-only, matching how `file` is registered today).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_logs_client.py tests/unit/test_loki_backend.py -v`
  Expected: PASS.

- [ ] **Step 5: Full unit gate**

  Run: `pytest tests/unit -q`
  Expected: PASS (whole suite green — confirms the file-backend refactor + Loki additions coexist).

- [ ] **Step 6: Commit**

  Run: `git add agctl/clients/log_client.py pyproject.toml tests/unit/test_logs_client.py`
  Run: `git commit -m "feat(logs): register loki built-in backend + add loki extra"`

---

### Task 9: Self-skipping live integration test

**Files:**
- Modify: `tests/integration/conftest.py` (add `_start_loki` + `require_loki`, mirroring the `require_schema_registry`/`_start_schema_registry` pattern)
- Create: `tests/integration/test_logs_loki.py`

**Interfaces:**
- Consumes: the `_LiveStack`/`_LIVE_SKIP_REASONS`/`LIVE` machinery in `conftest.py`; `testcontainers.core.container.DockerContainer` + `LogMessageWaitStrategy` (same as SR); `LokiBackend` via `LogClient` (or the CLI via `CliRunner`).
- Produces:
  - `require_loki` fixture: skip if `_live_skip_reason("loki")` set; else read `AGCTL_TEST_LOKI_URL`; if unset and not `LIVE`, `pytest.skip`; probe `<url>/ready` (2s GET) and skip on non-200/connection failure; yield the URL.
  - `_start_loki(stack)`: under `AGCTL_TEST_LIVE=1`, start `DockerContainer("grafana/loki:3.0.0").with_exposed_ports(3100).waiting_for(LogMessageWaitStrategy(re.compile(r".*Loki started.*")).with_startup_timeout(60))`; on success set `os.environ["AGCTL_TEST_LOKI_URL"] = f"http://localhost:{port}"`; on failure record `_LIVE_SKIP_REASONS["loki"]`. Wire it into `_live_services` (after the others) and add a `loki` attribute + teardown to `_LiveStack`.
  - `test_logs_loki.py`: `require_loki` fixture; push one log line via `POST {url}/loki/api/v1/push` (JSON `{"streams":[{"stream":{"app":"agctl-it"},"values":[["<unix_ns>","{\"level\":\"ERROR\",\"message\":\"boom\",\"orderId\":\"it-1\"}")]}]}`); then exercise `LogClient` (or `agctl logs query/assert`) with `query='{app="agctl-it"}'`, asserting the pushed line is returned by `scan` and matched by `await_one`/`follow` (bounded). Asserts only run when the fixture yields; otherwise the test skips.

- [ ] **Step 1: Write the failing test** (`tests/integration/test_logs_loki.py`)

  - The test imports `require_loki`, pushes a line, builds a `LogSource(type="loki", url=<yielded>, query='{app="agctl-it"}')`, constructs `LogClient`, and asserts `scan(...)` returns ≥1 entry whose `message=="boom"` and `fields["orderId"]=="it-1"`. (A second assertion: `await_one(..., since=now-1m, timeout_s=5)` matches.)

- [ ] **Step 2: Run it — expect a clean SKIP locally**

  Run: `pytest tests/integration/test_logs_loki.py -v`
  Expected: SKIP (`AGCTL_TEST_LOKI_URL not set; ... set AGCTL_TEST_LIVE=1`). It must NOT fail on a machine without Docker.

- [ ] **Step 3: Implement `require_loki` + `_start_loki` in conftest**

  Mirror the SR pattern exactly (manual URL via env, or `AGCTL_TEST_LIVE=1` spins `grafana/loki` on 3100). Add to `_LiveStack` + `_live_services` + `stop_all`.

- [ ] **Step 4: (If Docker available) run live; otherwise confirm skip path**

  Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_logs_loki.py -v`
  Expected: PASS if Docker is available and the image pulls; otherwise SKIP with a clear reason (never a hard failure).
  Run (no flag): `pytest tests/integration/test_logs_loki.py -v` → SKIP.

- [ ] **Step 5: Commit**

  Run: `git add tests/integration/conftest.py tests/integration/test_logs_loki.py`
  Run: `git commit -m "test(logs): self-skipping live Loki integration test"`

---

## Definition of Done

- `pytest tests/unit -q` is fully green (file-backend refactor preserved + all Loki unit tests pass).
- `agctl logs query/assert/tail --source <loki-source>` work against a real Loki (integration, when live).
- `agctl config validate` surfaces Loki source misconfig at `logs.sources.<name>` (exit 2).
- `pip install 'agctl[loki]'` brings httpx+jq; a missing extra surfaces as `ConfigError` exit 2.
- The command layer (`logs_commands.py`) is untouched; the one-JSON-envelope contract is unchanged.
- DESIGN.md / ARCHITECTURE.md / `skills/` sync is handled separately by `docs-watcher` (out of scope here).
