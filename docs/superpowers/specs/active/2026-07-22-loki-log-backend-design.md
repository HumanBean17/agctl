---

# Design: Log Source Breadth — Loki built-in (vertical slice)

**Status:** Approved (design) — pre-approved for implementation; spec self-reviewed, user review waived
**Date:** 2026-07-22
**Author:** brainstorming session
**Affects:** `agctl logs` command group; `agctl/clients/log_backends/loki.py` (new); `agctl/clients/log_common.py` (new, extracted); `agctl/clients/log_backends/ndjson_file.py` (refactored to consume `log_common`); `agctl/clients/log_client.py`; `agctl/config/models.py`; `pyproject.toml`; DESIGN.md §2.1 (`logs.sources`) / §3.4 / §9.2; ARCHITECTURE.md §3 / §8 / §10 / §11 / §14
**Relation to docs:** Adds a built-in remote log backend + a shared normalize/filter module; on implementation, DESIGN.md `logs.sources.<name>.type` enumerated values + the remote-source config shape, and ARCHITECTURE.md §3 module map + §8 LogClient layer + §10 extension points + §11 extras are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl logs *` ships with exactly **one** built-in backend: `NdjsonFileBackend`
(`type: file`), reading local NDJSON files in logstash format. The extension
surface for more sources already exists and is documented (DESIGN §9.2,
ARCHITECTURE §10): the `LogBackend` Protocol (`clients/log_backend_protocol.py`),
`LogClient` type-dispatch, and the `agctl.logs_backends` entry-point group. But
no other backend ships, so every real-world log that isn't a local NDJSON file is
out of reach — an agent testing against a containerized, k8s, or cloud-hosted SUT
cannot query/assert/tail its logs through the one-JSON-envelope contract.

The user-facing goal is **log source breadth**: reach beyond local files to
docker/container logs, journald, syslog, and remote aggregators (Loki/ELK/
CloudWatch). This spec delivers the **first non-file backend** as a deliberate
**vertical slice**: one backend, end-to-end, that exercises and hardens the
patterns the remaining backends inherit.

Two concrete blockers the slice must remove:

1. **Strict `LogSource` model.** `config/models.py` `LogSource` carries only
   `type` / `path` / `format` / `service`. Backend-specific YAML keys (endpoint,
   query, credentials, org-id) are rejected by Pydantic at load. There is no
   escape hatch. (`DatabaseConnection.options` is the precedent to copy.)
2. **Inline normalization & filtering.** The file backend's per-line
   normalization (logstash slot mapping) and per-entry filtering (level /
   logger-glob / message-substring / jq) live inline in `NdjsonFileBackend`. A
   Loki log line that carries the same JSON payload should normalize identically;
   today that logic cannot be reused.

## 2. Goals

- **Ship Loki as a built-in log backend** (`type: loki`): `logs query`,
  `logs assert`, and `logs tail` all work against a Loki HTTP endpoint.
- **Establish the shared foundation** the other five backends inherit: an
  `options` escape hatch on `LogSource`, and a `log_common` module holding
  normalization + filtering shared by every backend.
- **Preserve the canonical-entry contract** users see: a Loki source and a file
  source emitting the same log line produce the same `CanonicalEntry`.
- **Honor agctl's dependency discipline**: httpx stays lazy-imported behind a new
  `loki` extra; missing-library surfaces as `ConfigError` (exit 2) pointing at
  `pip install 'agctl[loki]'`, never a crash.
- **Don't break the file backend.** Its existing test suite stays green after the
  `log_common` extraction (behavior-preserving refactor).

## 3. Non-Goals

- **No LogQL generation / server-side filter push-down.** agctl does not
  translate `--level` / `--logger` / `--message` into LogQL. The user authors the
  LogQL `query`; agctl applies the standard flags client-side (consistent with
  the file backend). Push-down is a future optimization (§14).
- **No websocket `/tail`.** `follow()` polls `query_range` with an advancing
  `start`; true streaming via Loki's `/loki/api/v1/tail` websocket is deferred
  (§14).
- **No `LogBackend` protocol refactor, no shared remote-HTTP base class.** The
  protocol is used as-is. A second remote backend (ELK/CloudWatch) will reveal
  what is genuinely shared; refactor then, with evidence (avoid speculative
  abstraction — YAGNI).
- **No other backends in this slice.** docker, journald, syslog, ELK, CloudWatch
  follow in later specs, reusing the patterns established here.
- **No mTLS / client-cert auth for Loki in v1.** Basic, bearer, org-id, and
  `verify_tls` only (§9). Client certs deferred (§14).

---

## 4. Approach

**Vertical slice over batch.** Six backends were scoped; one (Loki) is built
first because it hardest-tests the `LogBackend` protocol for non-file sources —
remote transport, auth, a query language to carry (not generate), time-window
translation, and a streaming `follow`. Building it first surfaces protocol gaps
once, so the remaining five inherit a proven pattern rather than each re-discovering them.

Loki is the cleanest first remote backend: plain HTTP (httpx is already an
optional extra — no heavy new SDK like boto3), a well-documented read API, and
ubiquity in k8s/modern stacks.

**Three design recommendations confirmed during brainstorming:**

1. **Thin backend over the current protocol** — no protocol changes beyond
   documenting `tail_lines` as an ignorable file-backend hint.
2. **Client-side filtering** — the LogQL `query` does the server-side work;
   agctl's flags filter fetched entries (identical semantics everywhere).
3. **Poll-based `follow`** — `query_range` polled with an advancing `start`; no
   new websocket dependency.

---

## 5. Configuration

### 5.1 `LogSource` model changes (`config/models.py`)

Three fields added. Mirrors `DatabaseConnection` (common typed fields + an
`options` escape hatch). Existing `type` / `path` / `format` / `service`
unchanged.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `url` | `str \| None` | `None` | Remote endpoint (Loki/ELK). CloudWatch uses region → `options`. |
| `query` | `str \| None` | `None` | Backend-native query string (LogQL; ELK DSL later). |
| `options` | `dict[str, Any]` | `{}` | Backend-specific extras (auth, org-id, TLS, fetch tuning). Survives strict Pydantic. Solves the blocker for **all** future backends in one edit. |

The `options` escape hatch is preferred over per-backend typed fields here:
which fields are truly shared across Loki/ELK/CloudWatch is unknown until a
second remote backend exists. Frequently-common fields (e.g. `url`) may graduate
to typed later.

### 5.2 Loki source YAML shape

```yaml
logs:
  sources:
    order-service:
      type: loki                              # required
      url: "${LOKI_URL}"                      # required; e.g. http://loki:3100 or https://...
      query: '{app="order-service"}'          # required; LogQL log-stream selector + optional pipelines
      service: order-service                  # optional; defaults to the source name
      options:
        # --- auth (mutually exclusive: basic OR bearer, not both) ---
        username: "${LOKI_USER:-}"            #   basic-auth username (Grafana Cloud: instance id)
        password: "${LOKI_PASSWORD:-}"        #   basic-auth password (Grafana Cloud: API key)
        token: "${LOKI_TOKEN:-}"              #   bearer token (Authorization: Bearer)
        org_id: "${LOKI_ORG_ID:-}"            #   X-Scope-OrgID (multi-tenant Loki)
        # --- TLS ---
        verify_tls: true                      #   default true; false skips cert verification (self-signed/dev)
        # --- fetch tuning ---
        fetch_limit: 500                      #   Loki per-request `limit` (default 500; > agctl --limit for filtering headroom)
        direction: forward                    #   forward (chronological, default) | backward (newest-first)
```

All values are `${ENV}`-interpolatable (YAML values, resolved at load — §2.2).
`password` and `token` are auto-masked by the existing `config show`
secret-masking rule (keys containing `password`/`token`/`secret`).

### 5.3 `options` contract for the Loki backend

The Loki backend reads `options` with the keys below; `validate_config()`
enforces the required-field and mutual-exclusivity rules and raises
`ConfigError` (exit 2) — `agctl config validate` attributes each error to
`logs.sources.<name>`.

| Key | Required | Rule |
|---|---|---|
| `username` | no | If set, `password` must also be set (basic auth). |
| `password` | no | If set, `username` must also be set. |
| `token` | no | Mutually exclusive with `username`/`password` (basic). Exactly one auth mode or none. |
| `org_id` | no | Sent as `X-Scope-OrgID` header when non-empty. |
| `verify_tls` | no | Bool; default `true`. |
| `fetch_limit` | no | Int `> 0`; default `500`. |
| `direction` | no | `"forward"` (default) \| `"backward"`. Applies to `scan` only — `await_one` always queries backward (most-recent match), `follow` always forward (advancing `start`). See §6.3. |

`validate_config()` additionally requires `url` (must be `http://` or `https://`)
and `query` (non-empty). Unknown `options` keys are ignored (forward-compatible).

---

## 6. Component — `LokiBackend`

New module `agctl/clients/log_backends/loki.py`, class `LokiBackend`
satisfying the `LogBackend` Protocol (`validate_config` / `scan` / `await_one` /
`follow` / `sample_schema`). Registered in `BUILTIN_BACKENDS`
(`clients/log_client.py`):

```
BUILTIN_BACKENDS = {"file": NdjsonFileBackend, "loki": LokiBackend}
```

This matches the `file` convention (built-in via `BUILTIN_BACKENDS`; **not** added
to the `agctl.logs_backends` entry-point group, which remains third-party-only).
`LogClient.load_backends()` already merges entry points over built-ins, so no
dispatch change is needed.

### 6.1 Constructor & test seam

```
LokiBackend(source, *, http_client=None, monotonic=time.monotonic, sleep=time.sleep)
```

- `http_client` — DI seam. When `None`, methods lazy-import `httpx` and construct
  a client per request; when provided, used directly (tests inject
  `httpx.Client(transport=MockTransport(...))` or an httpx-compatible fake). The
  seam accepts an object exposing `httpx.Client.get(...)`'s surface
  (`get(url, params=, headers=, auth=, timeout=, verify=)` → response with
  `.status_code`, `.json()`, `.text`).
- `monotonic` / `sleep` — injectable for deterministic polling/tail tests
  (mirrors `NdjsonFileBackend`).

httpx is lazy-imported **inside methods**, never at module top — importing
`agctl.clients.log_backends.loki` must not require httpx (ARCH §8 lazy-import
convention).

### 6.2 Loki HTTP read API (contract)

All requests target `{url}/loki/api/v1/...`. The backend uses one endpoint:

- **`GET /loki/api/v1/query_range`** — params: `query` (LogQL), `start`, `end`
  (RFC3339Nano UTC), `limit` (int), `direction` (`forward` | `backward`).
  Successful log-selector response:
  ```
  {"status":"success","data":{"resultType":"streams","result":[
    {"stream":{<label>...},"values":[["<RFC3339Nano>","<line>"], ...]}
  ]}}
  ```
  Each `[ts, line]` pair → one `CanonicalEntry` via `log_common.normalize_line`
  (§7), with `ts_override = ts`.

Contract rules:
- **`resultType` must be `"streams"`.** A metric/matrix result (from a query like
  `rate(...)`) → `ConfigError` (exit 2) with a message telling the user to write a
  log selector query, not a metric query. Fail loud.
- **Error responses** carry `{"status":"error","error":"..."}` with an HTTP 4xx/5xx;
  mapped per §10.

### 6.3 Protocol methods (contract-level)

- **`validate_config()`** — §5.3 rules. Called by `LogClient.__init__` and by
  `agctl config validate` (the `validator.py` hook that constructs a `LogClient`
  per `logs.sources.*`), so misconfig surfaces at validate time, exit 2.
- **`scan(filt, *, since, until, limit, tail_lines)`** — one `query_range` call:
  `start`/`end` from `since`/`until`; `limit` = `options.fetch_limit`;
  `direction` = `options.direction` (default `forward`). If `since` is `None`, a
  1-hour lookback window applies (Loki requires a `start`); `until=None` means
  now. Normalizes returned
  lines, applies `entry_matches` (client-side), returns up to `limit` matches.
  `tail_lines` is **ignored** (a file-backend hint; documented, not enforced).
- **`await_one(filt, *, since, timeout_s, poll_interval_ms, tail_lines)`** —
  two-phase, mirroring the file backend but tracking a **last-seen Loki timestamp**
  high-water mark instead of a byte offset: read the historical window once
  (`direction=backward`, `limit=fetch_limit`); if `timeout_s <= 0` that single
  read is the whole op (one-shot mode). In poll mode (`timeout_s > 0`), re-query
  with `start = last_seen_ts` each cycle so each entry is counted exactly once.
  Returns the first matching entry or `(None, scanned)` at timeout.
- **`follow(filt, *, stop_event, poll_interval_ms)`** — generator: seed `start`
  to now, then poll `query_range` (`direction=forward`) with `start` advancing
  past the last emitted timestamp; dedup by `(timestamp, line)`; yield each
  matching entry not yet emitted; stop when `stop_event` is set. Transient HTTP
  errors are retried (bounded) with a stderr warning; permanent errors (401/400)
  raise (surfaced via the streaming startup-error path — ARCH §6).
- **`sample_schema(*, sample_lines=100)`** — fetch a small recent sample
  (`query_range`, `limit=sample_lines`) and infer field presence via
  `log_common`'s schema helper, so `--match` jq authoring works against
  discovered fields.

---

## 7. Shared module — `agctl/clients/log_common.py` (extracted)

A Loki log line and a file-backend log line must normalize identically. The
file backend's per-line normalization and per-entry filtering are extracted into
a new module, consumed by both `NdjsonFileBackend` and `LokiBackend`. Behavior is
preserved (the file backend's existing tests pin it).

Public contract (functions, not classes):

| Function | Responsibility | Origin |
|---|---|---|
| `normalize_line(line, *, service, ts_override=None) -> CanonicalEntry` | Logstash slot mapping (`@timestamp`→`timestamp`, `level`, `logger_name`→`logger`, `thread_name`→`thread`, `message`, `service`, `stack_trace`, `tags`); non-slot keys spill to `fields`. If the line is not a JSON object, the whole line becomes `message` and `ts_override` (or a sentinel) supplies `timestamp`. Level is upper-cased. | Extracted from `NdjsonFileBackend._normalize`. |
| `entry_matches(entry, filt) -> bool` | AND of: `level` (case-insensitive), `logger` glob (`fnmatch`), `message` substring, `match_jq` (`jq_bool` over the canonical entry, supporting `{placeholder}` fill from `filt.params`). | Extracted from the file backend's `_read_window` filter logic. |
| `infer_schema(entries, *, sample_lines) -> SchemaDescriptor` | Standard slots present (union) + conditional slots (`stack_trace`/`tags`) + observed custom `fields` keys. | Extracted from `NdjsonFileBackend._sample_schema`. |

`NdjsonFileBackend` becomes thinner: it keeps its file-specific concerns
(`_tail_lines`, byte-offset tailing, rollover/truncation handling, `_read_window`
orchestration) and delegates normalization + filtering + schema inference to
`log_common`. The jq import stays lazy inside `entry_matches` (a missing `jq` →
`ConfigError` pointing at the right extra — `agctl[loki]` for Loki, `agctl[logs]`
for file).

---

## 8. Data Flow

```
agctl logs query/assert/tail --source <name>
   │
   ▼
new_logs_client(source)                          # command-layer test seam
   │
   ▼
LogClient(source) → dispatch on source.type=="loki" → LokiBackend(source)
   │                                               └─ validate_config()  (config-validate hook)
   ▼
scan / await_one / follow
   │
   ▼  (lazy-import httpx inside the method)
httpx.GET {url}/loki/api/v1/query_range?query=&start=&end=&limit=&direction=
   │   headers: Authorization (bearer) | basic-auth | X-Scope-OrgID ; verify=options.verify_tls
   ▼
parse data.result[].values[] → [ts, line]
   │
   ▼
log_common.normalize_line(line, service=, ts_override=ts) → CanonicalEntry
   │
   ▼
log_common.entry_matches(entry, filt)            # client-side: level/logger/message/--match
   │
   ▼
ScanResult / AwaitResult / yielded entry → command emits (one envelope, or NDJSON for tail)
```

The command layer (`commands/logs_commands.py`) is **unchanged** — it already
calls `client.scan` / `await_one` / `follow` and emits per the existing contract.
Loki is transparent behind `LogClient`.

---

## 9. Auth & TLS

All optional, `${}`-interpolatable. Constructed per request on the httpx call:

| Mode | Source | httpx mapping |
|---|---|---|
| Basic | `options.username` + `options.password` | `auth=(username, password)` |
| Bearer | `options.token` | header `Authorization: Bearer <token>` |
| Org ID (multi-tenant) | `options.org_id` | header `X-Scope-OrgID: <org_id>` |
| TLS verification | `options.verify_tls` (default `true`) | `verify=<bool>` |

- Basic and bearer are **mutually exclusive** (§5.3). Org-id is orthogonal
  (commonly combined with bearer for Grafana Cloud).
- Per-request timeout comes from `defaults.timeout_seconds` / `--timeout`
  (mapped to httpx `timeout=`).
- mTLS / client-cert (`ca`/`cert`/`key` paths) is **deferred** (§14); the slice
  ships the `verify_tls` bool only.

---

## 10. Error Mapping & Exit Codes

Lazy-httpx exception mapping mirrors `HttpClient` (ARCH §8). All paths emit
exactly one structured envelope before exit (the `@envelope` guarantee; `logs
tail` emits a startup-error envelope before any NDJSON line).

| Condition | Exception | `error.type` | Exit |
|---|---|---|---|
| httpx not installed | `ConfigError` | `ConfigError` | 2 (msg: `pip install 'agctl[loki]'`) |
| `jq` not installed and `--match` used | `ConfigError` | `ConfigError` | 2 (msg: `pip install 'agctl[loki]'`) |
| Bad config (missing `url`/`query`, basic+bearer conflict, bad URL scheme) | `ConfigError` | `ConfigError` | 2 |
| Loki returns `resultType != "streams"` | `ConfigError` | `ConfigError` | 2 ("write a log selector query, not a metric query") |
| Loki **400** (bad LogQL) | `ConfigError` | `ConfigError` | 2 (carries Loki `error` body) |
| httpx `ConnectError` / `ConnectTimeout` | `ConnectionFailure` | `ConnectionError` | 2 |
| Loki **401** / **403** (auth) | `ConnectionFailure` | `ConnectionError` | 2 |
| Loki **5xx** / other non-200 | `ConnectionFailure` | `ConnectionError` | 2 |
| httpx `ReadTimeout` / `TimeoutException` | `OperationTimeout` | `TimeoutError` | 1 |

(`assert`-mode match failures continue to raise `AssertionFailure`, exit 1, as
today — unchanged.)

---

## 11. Filter & Limit Semantics (consequential contract)

- **Server-side filtering** = the LogQL `query` the user authors (stream selector
  + `| json | …` pipelines). agctl does not modify or extend it.
- **Client-side filtering** = agctl's `--level` / `--logger` / `--message` /
  `--match`, applied over fetched entries via `entry_matches` — identical to the
  file backend, identical semantics everywhere.
- **`--limit`** = max entries agctl **returns** after client-side filtering.
- **Loki request `limit`** = `options.fetch_limit` (default 500), strictly
  greater than `--limit` to give filtering headroom (so a fetch isn't truncated
  below the user's requested match count when many fetched lines are filtered
  out).
- **`ScanResult` fields** (honest, no silent truncation):
  - `scanned` — entries fetched from Loki.
  - `matched` — count passing the client-side filter (may exceed `len(entries)`).
  - `truncated` — **`true` if either** signal fires: (a) `matched > limit`
    (agctl truncated the returned set), or (b) Loki returned exactly
    `fetch_limit` entries (more matches likely exist server-side that were not
    fetched).

---

## 12. Packaging

- New optional extra in `pyproject.toml`:
  ```toml
  loki = ["httpx>=0.27", "jq>=1.6"]
  ```
  httpx already lives under the `http` extra; a dedicated `loki` extra makes the
  install intent explicit and keeps logs users off the `http` extra name. `jq` is
  bundled (needed for `--match`).
- Registration: add `"loki": LokiBackend` to `BUILTIN_BACKENDS` (in-tree built-in,
  matching `file`). No entry-point entry (the `agctl.logs_backends` group stays
  third-party-only, consistent with how `file` is registered today).
- `agctl config validate` surfaces Loki source errors at `logs.sources.<name>` via
  the existing validator hook (no validator change beyond what the `options`
  model permits).

---

## 13. Testing Strategy

### 13.1 Unit (`tests/unit/test_loki_backend.py`; extend `test_logs_client.py`)

Inject `http_client` (httpx `MockTransport` or a fake recording requests); inject
`monotonic` / `sleep` for deterministic poll/follow.

- **`validate_config`** — missing `url`/`query`; non-http(s) URL; basic+bearer
  conflict; basic-without-password; defaults applied.
- **query_range construction** — correct `query`/`start`/`end`/`limit`/
  `direction`; auth headers (basic → `auth=`; bearer → `Authorization`; org-id →
  `X-Scope-OrgID`); `verify_tls` passed through.
- **Normalization** — JSON logstash line → canonical slots + `fields` spill;
  plain-text line → `message`; level upper-cased; `ts_override` honored.
- **Client-side filtering** — `level`/`logger`-glob/`message`/`--match` (incl.
  `{placeholder}` fill) over fetched entries.
- **`scan` truncation honesty** — `matched > limit` → truncated; Loki returns
  exactly `fetch_limit` → truncated; both clear → not truncated.
- **`await_one`** — one-shot mode (`timeout_s <= 0`); poll mode with advancing
  `start` (each entry counted once); timeout → `(None, scanned)`.
- **`follow`** — yields new matches across polls; `(timestamp, line)` dedup;
  stops on `stop_event`.
- **`sample_schema`** — infers standard/conditional/observed from a sample.
- **Error mapping** — 400 → `ConfigError`; 401/403/5xx → `ConnectionFailure`;
  `ConnectError` → `ConnectionFailure`; `ReadTimeout` → `OperationTimeout`;
  missing httpx → `ConfigError`; `resultType != "streams"` → `ConfigError`.
- **`log_common` shared helpers** — tested directly once (file + Loki reuse).
- **Dispatch** — `LogClient` selects `loki` from `BUILTIN_BACKENDS`; unknown
  type still `ConfigError`.

### 13.2 Refactor safety

The existing `test_logs_client.py` suite (file backend normalization, scan,
await_one, follow, sample_schema) must stay green after the `log_common`
extraction — it pins the behavior-preserving refactor.

### 13.3 Integration (`tests/integration/test_logs_loki.py`, self-skipping)

Mirrors the existing integration pattern (`tests/integration/conftest.py`):
- **Manual/CI** — `AGCTL_TEST_LOKI_URL` points at a running Loki.
- **Local Docker, opt-in** — `AGCTL_TEST_LIVE=1` spins a `testcontainers` Loki
  (grafana/loki image); push a line via the Loki push API, then exercise
  `agctl logs query` / `assert` / `tail` against it.
- Flag unset → `pytest.skip()` (a plain `pytest` run stays fast).

---

## 14. Open Questions / Future Work

- **Filter push-down** — translate `--message` → LogQL `|=`, `--level`/`--logger`
  → `| json | …` when the query parses JSON and field names are known. Deferred;
  the client-side path is correct and consistent first.
- **True streaming `follow`** — Loki `/loki/api/v1/tail` websocket (needs a ws
  client extra) for sub-`poll_interval_ms` latency and fewer requests. Deferred;
  polling is adequate for an agent harness.
- **mTLS / client certs** — `options.ca` / `cert` / `key` paths → httpx. Deferred.
- **`tail_lines` protocol semantics** — observe whether a second backend also
  ignores it; if so, promote it to an explicit ignorable hint (or drop from the
  remote-backend call signature) in a protocol-hardening pass. Not changed now.
- **Shared remote-HTTP base** — extract after ELK/CloudWatch exist, when the real
  common surface (transport, auth, pagination, time-window, poll-follow) is
  visible. Not speculative now.
- **Remaining backends** — docker, journald, syslog, ELK, CloudWatch, each its
  own spec, reusing `LogSource.options` + `log_common`.

---

## TL;DR

Ship **Loki as the first non-file log backend** (`type: loki`) — a vertical
slice that also lands the shared foundation every later backend inherits: an
`options` escape hatch on `LogSource` (fixing the strict-model blocker for all
future backends at once) and a `log_common` module (shared line normalization +
entry filtering, extracted from the file backend). The LogQL `query` is
user-authored and runs server-side; agctl's flags filter client-side (consistent
with the file backend); `follow()` polls `query_range` (no websocket dep); httpx
stays lazy behind a new `loki` extra. No protocol refactor, no speculative base
class — those wait for a second remote backend to reveal real shared needs.
