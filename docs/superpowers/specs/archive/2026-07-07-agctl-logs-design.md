# Design: `agctl logs` — Service Log Access via a Pluggable Backend

**Status:** Draft v1 — design approved in brainstorming session; pending spec review
**Date:** 2026-07-07
**Author:** brainstorming session
**Affects:** new `agctl/clients/log_backend_protocol.py`, new `agctl/clients/log_client.py`, new `agctl/clients/log_backends/ndjson_file.py`, new `agctl/commands/logs_commands.py`, `agctl/config/models.py` (new `LogsConfig` / `LogSource` / `LogsDefaults`), `agctl/commands/discover_commands.py` (new `log-sources` category), `agctl/cli.py` (new `logs` group), `agctl/assertions.py` (reuse only); a small `_parse_since_until` duration/ISO-8601 helper added in `agctl/commands/logs_commands.py` (promotable to a shared util if later commands want relative durations), `pyproject.toml` (new `logs` extra + `agctl.logs_backends` entry-point group), `agctl-config` skill, DESIGN.md, ARCHITECTURE.md
**Relation to docs:** Additive — no existing flag, field, exit code, or output shape changes. On implementation, DESIGN.md (§2 config, §3 commands, extension-points) and ARCHITECTURE.md (clients layout, backend protocol, streaming) are synced via `docs-watcher`. No `version` bump (additive under major `2`).

---

## 1. Background & Problem

`agctl` can assert on HTTP responses, Kafka messages, and database rows, but it
cannot observe what happened *inside* a service during a scenario. This blocks two
primary use cases (both flagged in the originating proposal):

1. **Negative assertion** — "assert no `ERROR` was logged during this request."
2. **Correlation assertion** — "assert the service logged a specific business event for
   this exact `orderId`" (relying on MDC / `StructuredArguments` for request-scoped
   correlation).

The originating proposal (`agctl logs`) solves this for one transport: NDJSON files
written by `logstash-logback-encoder`, chosen first because the author's enterprise
machine permits no other logging software. That constraint is real, but it must not
shape the architecture: VictoriaLogs (HTTP query API), Loki, and Elasticsearch are
plausible future sources, and they are **not file-tailed** — they are queried over HTTP
with server-side filtering. A feature built around file-tailing does not *extend* to
them; it is rewritten.

The decisive design move is therefore to abstract the **transport** (the backend), not
merely the **format** (`logstash` / `ecs` / `gelf`, which the proposal already
abstracts). `agctl` already has the exact pattern for this: `DbClient` selects a
`DBDriver` by `connection.type` from built-ins plus the `agctl.db_drivers` entry-point
group, and it shipped with a single `postgresql` driver. This spec mirrors that pattern
for logs: a `LogBackend` Protocol, a `LogClient` selector, and an
`agctl.logs_backends` entry-point group — with one built-in `file` backend (NDJSON) in
v1 and Victoria as a future external package that registers with **zero core edits**.

> **Scope honesty (load-bearing).** v1 ships exactly one backend (`file` / NDJSON
> `logstash`). The pluggable seam is **justified, not speculative**: it is proven by the
> `db_drivers` precedent, and it forces the **canonical entry contract** (§5) that makes
> `--level` / `--match` / `discover` behave identically across backends. Establishing
> the contract now prevents a costly retrofit when Victoria arrives. The format adapter
> (`logstash` parser) is an internal concern of the file backend; `ecs` / `gelf` parsers
> are deferred (§13).

## 2. Goals

- Let an agent **read, assert, and tail** service logs with the same exit-code discipline
  as `kafka assert` / `db assert`, closing the "cannot fail loudly" gap for in-service
  behavior.
- Establish a **pluggable backend seam** (`LogBackend` Protocol + `agctl.logs_backends`
  entry-point) so a future Victoria backend is an external package, not a core change —
  mirroring `db_drivers` exactly.
- Define a **canonical log entry** as the cross-backend contract so `--level`,
  `--logger`, `--message`, `--match`, and `discover` are transport- and format-agnostic.
- **Reuse** the existing jq engine (`jq_bool`, `compile_jq`), the `@envelope` discipline,
  and the `http ping` streaming precedent. Add no new assertion engine.
- **Fail loud on authoring bugs, soft on data variance** — a malformed `--match` is a
  `ConfigError` (exit 2) via up-front `compile_jq`; a data-dependent jq eval error is a
  soft non-match via `jq_bool`. (Same split the http-jq spec codifies.)
- Preserve every existing behavior: no command, flag, field, exit code, or output shape
  changes elsewhere.

## 3. Scope & Design Constraints

- **Additive only.** A new `logs` command group, a new `logs:` config section, a new
  discover category, a new extra, and a new entry-point group. Nothing existing changes.
- **Transport abstraction over format abstraction.** The `LogBackend` Protocol is the
  primary seam; the `format` field is a parser hint *internal to the file backend*. This
  is the structural difference from the originating proposal, which treated `format` as
  the only discriminator and could not select a transport.
- **Canonical entry is the contract.** Every backend emits the same canonical entry
  (§5); flags and jq operate on it. A backend may **push down** structural flags
  (`--level`/`--logger`/`--since`/`--until`) to its native query mechanism for efficiency
  but must honor them semantically; `--match` (jq) is always evaluated client-side on the
  canonical entry so semantics are identical everywhere.
- **`assert` is one-shot + poll, not poll-only.** Logs persist in a file and are
  re-scannable, unlike Kafka messages consumed once. `--timeout` is **optional** on
  `assert`: omitted/`0` = one-shot scan of the window; `N > 0` = poll until first match
  or `N` elapses. This is a deliberate, documented deviation from `kafka assert` (where
  `--timeout` is required and `≤ 0` is rejected).
- **Reuse the lazy jq import.** jq is loaded only when `--match` is used; a missing
  library surfaces as `ConfigError` (exit 2) with an install hint. Structural-flag-only
  usage (`--level`/`--logger`/`--message`) imports no jq.
- **`@envelope` discipline preserved** for `query` and `assert`. `tail` is the streaming
  exception: it emits one canonical entry per line with no envelope wrapper, mirroring
  `http ping` and `mock run`.
- **Streaming mirrors `http ping`.** Signal-driven stop via a `threading.Event`, a final
  summary line, and manual startup-error envelopes for pre-stream failures.

## 4. Non-Goals

- **A Victoria (or Loki/ES) backend in v1.** Future external package(s) registering via
  `agctl.logs_backends`. The seam is built; the backend is not.
- **`ecs` / `gelf` format parsers in v1.** Only `logstash` ships; the parser seam
  exists so these are later normalizers.
- **Named log patterns** (`logs.patterns` / a `--pattern` analog). `--match` is
  sufficient for v1; named patterns are YAGNI.
- **Response/field extraction (`--capture`).** Agents still hand-roll `jq -r` to pull a
  value for the next command; deferred (same call as the http-jq spec).
- **Reading rolled-over / archived log files.** The file backend reads only the
  *current active* file (see §14 — known limitation).
- **Log shipping, aggregation, or writing logs.** `agctl logs` is read-only
  observation.
- **Behavioral coupling via the `service:` field.** It is informational (discover
  display) only in v1.
- **`--match-all`, range/regex level matching, header-style capture.** Deferred.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Full backend abstraction now: `LogBackend` Protocol + `LogClient` + `agctl.logs_backends` entry-point**, mirroring `DbClient` / `DBDriver` / `agctl.db_drivers` exactly. Built-in `file` registered in `BUILTIN_BACKENDS`; third-party (Victoria) via entry-point. | File-tail and HTTP-query are different *transports*; the proposal abstracted only the *format*, which cannot select a transport and would force a rewrite for Victoria. The `db_drivers` precedent proves a one-driver seam is worth paying for, and forces the canonical-entry contract that makes flags/discover backend-agnostic. |
| D2 | **A canonical log entry is the cross-backend contract.** Structural flags and `--match` operate on it. | Identical `--level` / `--match` / `discover` behavior whether the source is a logstash file, an ECS file, or Victoria. The contract is what makes D1 pay off. |
| D3 | **Custom / MDC / `StructuredArguments` / non-standard values live under `.fields.*`**, not as raw top-level keys. No `.raw` blob. | Top-level flat fields (the proposal's `.orderId`) are a logstash artifact; they break transport-agnosticism. `.fields.*` is uniform across backends. The file backend's normalizer maps every original top-level key to either a canonical slot or a `.fields.*` key, so nothing is lost and no `.raw` escape hatch is needed. |
| D4 | **`source.type` selects the backend (defaults to `"file"`); `format` is a file-backend parser hint.** | Transport axis (`type`) and format axis (`format`) are independent; conflating them (as the proposal did) cannot express "Victoria over HTTP." Defaulting `type` to `file` keeps the proposal's config shape backward-friendly. |
| D5 | **`assert` is one-shot + poll via an optional `--timeout`.** Omitted or `0` = one-shot (scan the `--since`/`--until` window once, exit immediately); `N > 0` = poll until first match or `N` elapses. `--since` stays required (bounds the window in both modes). | Logs persist and are re-scannable; the line is often already present when the HTTP response returns, so one-shot is natural and fast. Polling still serves async/pipelined logging. Deviates from `kafka assert` (required-positive `--timeout`), but logs and Kafka genuinely differ. |
| D6 | **A `--not` flag inverts `assert` semantics** for negative assertions. `--not` → exit `0` if *no* match in the window, exit `1` (`AssertionFailure`) if a match *is* found; the offending entry rides in `error.detail`. | Negative assertion is a primary use case (§1). The proposal's shell-level exit-code inversion (`if agctl logs assert …; then echo FAIL; fi`) is error-prone for agents. A first-class flag is scriptable and matches how agents actually write negative checks. |
| D7 | **`--match` is `{placeholder}`-filled via `--param`, then `compile_jq`-validated up front.** | Logs have no named-pattern layer (unlike `kafka --pattern`), so `--match` itself is the parameterized surface; filling it matches the proposal's UX. Up-front `compile_jq` (not `jq_bool`) makes a typo a loud `ConfigError`, not a silent no-match (the http-jq D5 precedent). This is a documented divergence from `kafka assert`, where `--match` is *not* filled. |
| D8 | **Structural flags (`--level`/`--logger`/`--message`/`--since`/`--until`) are push-down hints a backend MAY translate; `--match` (jq) is always client-side on the canonical entry.** | The file backend applies all filters client-side. A future Victoria backend can push `--level`/`--logger`/`--since` into LogsQL for efficiency, but cannot run arbitrary jq server-side — so `--match` stays client-side to keep semantics identical across backends. (See §14 for the bandwidth implication.) |
| D9 | **`tail` streams NDJSON without an envelope wrapper**, mirroring `http ping`. | Established streaming precedent (`http ping`, `mock run`): one JSON object per line, signal-driven stop, final summary line, manual startup-error envelopes. |
| D10 | **A missing log file is an empty result, not an error.** | A service that has not started (or has not yet logged) legitimately has no file. Treating absence as `0 entries` makes negative assertions and pre-warm queries work without spurious `ConnectionFailure`s. A misconfigured path that can never resolve is indistinguishable at runtime; `config validate` (§11) catches structurally-bad paths where feasible. |
| D11 | **New `logs` extra bundles `jq>=1.6`; jq is lazy-imported only when `--match` is used.** | Structural-flag-only usage needs no jq, so a zero-jq install works for `--level`/`--logger`/`--message` queries. Mirrors the lazy-import convention; the missing-library `ConfigError` hint points at `pip install 'agctl[logs]'`. |
| D12 | **New `agctl.logs_backends` entry-point group** for third-party backends; the built-in `file` backend is registered in `BUILTIN_BACKENDS` (not via entry-point), exactly as `postgresql` sits in `BUILTIN_DRIVERS`. | Symmetric with `agctl.db_drivers`. Lets Victoria ship as a separate package with no core edits. |

## 6. Command Contract — `logs query` / `assert` / `tail`

A new `logs` Click group is wired in `cli.py` (group + `add_command` per the existing
two-step registration). Each subcommand follows the core/envelope split
(`_x_core` → `_x_envelope = envelope("logs.x")(...)`) except `tail`, which is the
streaming exception. Commands obtain their backend via the `new_logs_client(source)`
factory (the test seam).

### 6.1 Shared flags

| Flag | Applies to | Description |
|---|---|---|
| `--source <name>` | all (required) | matches a key under `logs.sources` |
| `--level <LEVEL>` | all | case-insensitive; compared against canonical `level` (UPPER-normalized) |
| `--logger <glob>` | all | glob match on canonical `logger` |
| `--message <substr>` | `query`, `assert` | substring match on canonical `message` |
| `--match <jq-expr>` | all | `{placeholder}`-filled (D7); `jq_bool` predicate against the canonical entry (ANY truthy output) |
| `--param key=value` | all (repeatable) | fills `{placeholder}` in `--match` (D7) |
| `--since <value>` | `query`, `assert` | ISO-8601 **or** relative (`30s`/`5m`/`1h`); window lower bound on canonical `timestamp`. Optional for `query` (no bound ⇒ scan `tail_lines`); **required for `assert`** (bounds the window in both modes) |
| `--until <value>` | `query` only | window upper bound (default: now) |
| `--config <path>` | all | inherited global flag |

All active filters AND. `--level`/`--logger`/`--message` are pure-Python predicates on
the canonical entry (no jq); only `--match` costs a jq eval.

### 6.2 `agctl logs query` — scan and filter (`logs.query`)

```
agctl logs query
    --source <name>            (required)
    [--level <LEVEL>] [--logger <glob>] [--message <substr>] [--match <jq>]
    [--since <value>] [--until <value>]
    [--param key=value] ...    (repeatable)
    [--limit <n>]              (default: logs.defaults.limit)
    [--tail <n>]               (default: logs.defaults.tail_lines; file-backend scan hint)
```

Result (`ok:true`, exit `0`):

```json
{ "source": "order-service", "matched": 2, "scanned": 200,
  "truncated": false, "entries": [ <CanonicalEntry>, ... ] }
```

`truncated: true` when matches exceed `--limit` (the array holds the first `--limit`).
Output entries are the canonical entry **without** a `.raw` blob (D3 — none exists).

### 6.3 `agctl logs assert` — assert a log entry appeared (`logs.assert`)

```
agctl logs assert
    --source <name>            (required)
    --since <value>            (required; bounds the window in both modes)
    [--level <LEVEL>] [--logger <glob>] [--message <substr>] [--match <jq>]
    [--param key=value] ...    (repeatable)
    [--not]                    (invert: assert NO match — D6)
    [--timeout <seconds>]      (optional — D5; omitted/0 = one-shot, N>0 = poll)
```

- **Positive (default):** match found → `ok:true`, exit `0`; no match →
  `AssertionFailure` (exit `1`).
- **`--not` (D6):** no match in window → `ok:true`, exit `0`; a match found →
  `AssertionFailure` (exit `1`) with the offending entry in `error.detail`.
- **One-shot** (`--timeout` omitted/`0`): scan the window once, exit immediately.
- **Poll** (`--timeout N`, `N>0`): re-scan until a match lands (positive) or the window
  stays clean until `N` elapses (`--not`), at `logs.defaults.poll_interval_ms`.

Success result:

```json
{ "source": "order-service", "matched": true,
  "matching_entry": <CanonicalEntry>, "entries_scanned": 43, "elapsed_ms": 280 }
```

Failure (`AssertionFailure`, exit `1`):

```json
{ "type": "AssertionError",
  "message": "No matching log entry within 10s",
  "detail": { "source": "order-service", "not": false,
              "filter": { "level": "INFO", "match": ".fields.orderId == \"ord-789\"" },
              "since": "2026-07-07T20:59:50Z", "entries_scanned": 312, "elapsed_ms": 10012 } }
```

For `--not` failures, `detail.matching_entry` carries the entry that should not have
appeared.

### 6.4 `agctl logs tail` — stream live entries (`logs.tail`, streaming)

```
agctl logs tail
    --source <name>            (required)
    [--level <LEVEL>] [--logger <glob>] [--match <jq>]
    [--param key=value] ...    (repeatable)
    [--duration <seconds>]     (mutually exclusive with --until-stopped)
    [--until-stopped]          (default if neither given)
```

- Emits one canonical entry per line (NDJSON), no envelope wrapper (D9). `--message` is
  omitted from `tail` (substring-over-a-stream is rarely wanted and adds cost); use
  `--match` (`.message | contains(...)`) if needed.
- `--duration` / `--until-stopped` mutex → `ConfigError` (exit `2`) at startup.
- A SIGTERM/SIGINT sets a `threading.Event`; the loop wakes promptly and emits a final
  summary line, then exits (code `0`):

```json
{"summary": true, "total_emitted": 5, "duration_ms": 8321}
```

- Pre-stream failures (unknown source, bad `--match`, mutex violation, missing `jq`)
  emit a manual structured error envelope (the `http ping` precedent), since `tail` is
  not `@envelope`-wrapped.

## 7. Config Schema — `logs`

New pydantic models in `agctl/config/models.py`; a new `logs` field on `Config`.
`${ENV}` interpolation and `AGCTL_LOGS__*` env overrides apply automatically via the
existing pipeline (no new loader code).

### 7.1 Config contract (YAML)

```yaml
logs:
  sources:
    order-service:
      type: file                       # backend selector; defaults to "file" if omitted (D4)
      path: "logs/order-service.log"   # file-backend-specific (relative to agctl.yaml or absolute)
      format: logstash                 # file-backend parser hint; "logstash" in v1 (D4)
      service: order-service           # optional, informational only (discover display)
    payment-service:
      path: "logs/payment-service.log" # type omitted -> defaults to "file"
      format: logstash
      service: payment-service
  defaults:
    tail_lines: 200                    # file-backend scan hint for query/tail lookback
    limit: 50                          # query --limit default
    timeout_seconds: 10                # reserved default for assert poll (explicit --timeout wins)
    poll_interval_ms: 100              # assert-poll and tail follow interval
```

### 7.2 Pydantic model shape

| Model | Fields |
|---|---|
| `LogSource` | `type: str = "file"` (D4), `path: str \| None` (file-backend), `format: str = "logstash"` (file-backend), `service: str \| None` (informational) |
| `LogsDefaults` | `tail_lines: int = 200`, `limit: int = 50`, `timeout_seconds: int = 10`, `poll_interval_ms: int = 100` |
| `LogsConfig` | `sources: dict[str, LogSource] = {}`, `defaults: LogsDefaults = LogsDefaults()` |
| `Config` | gains `logs: LogsConfig = Field(default_factory=LogsConfig)` |

Backend-specific fields (e.g. `path`/`format` for file; a future `url`/`query_endpoint`
for Victoria) live under `LogSource` and are validated by the selected backend's
`validate_config` (§8) — the same shape `db.connections.<name>` uses for driver-specific
fields.

## 8. Backend Contract — `LogBackend` Protocol + DTOs

The Protocol is the contract; backends implement it. No method bodies here — signatures
and DTOs only.

### 8.1 `LogBackend` Protocol (`agctl/clients/log_backend_protocol.py`)

| Method | Purpose |
|---|---|
| `validate_config(source: LogSource) -> None` | raise `ConfigError` on a structurally-bad source (e.g. file backend missing `path`); called from `config validate` and at command entry |
| `scan(filt: LogFilter, *, since, until, limit) -> ScanResult` | one pass over the window; backs `query` and one-shot `assert` |
| `await_one(filt: LogFilter, *, since, timeout_s, poll_interval_ms, invert) -> AwaitResult` | one-shot if `timeout_s <= 0` else poll until first match (or clean window for `invert`); backs `assert` (positive and `--not`) |
| `follow(filt: LogFilter, *, stop_event: threading.Event) -> Iterator[CanonicalEntry]` | yield new matching entries until `stop_event` is set; backs `tail` |
| `sample_schema(*, sample_lines: int) -> SchemaDescriptor` | enumerate canonical slots present + `.fields.*` keys observed; backs discover item detail (§12) |

### 8.2 DTOs (contracts)

**`CanonicalEntry`** (D2/D3) — what every backend emits and what flags/`--match` see:

| Field | Type | logstash source |
|---|---|---|
| `timestamp` | str (ISO-8601) | `@timestamp` |
| `level` | str, **UPPER-normalized** | `level` |
| `logger` | str | `logger_name` |
| `message` | str | `message` |
| `thread` | str \| None | `thread_name` |
| `service` | str \| None | `service` (customFields) |
| `stack_trace` | str \| None | `stack_trace` |
| `tags` | list[str] \| None | `tags` |
| `fields` | dict[str, Any] | **all other top-level keys** (MDC + `StructuredArguments` + `@version`/`level_value`/etc.), per D3 |

**`LogFilter`** — `{level, logger_glob, message_substring, match_jq, params}` (any
optional). Passed to every backend method; backends translate structural members to their
native mechanism (D8) and evaluate `match_jq` client-side via `jq_bool`.

**`ScanResult`** — `{entries: list[CanonicalEntry], matched: int, scanned: int,
truncated: bool}`.

**`AwaitResult`** — `{entry: CanonicalEntry \| None, scanned: int, elapsed_ms: int}`.

**`SchemaDescriptor`** — `{standard: list[str], conditional: list[str],
observed: list[str]}` (see §12).

### 8.3 `LogClient` (`agctl/clients/log_client.py`)

Selects a backend by `source.type`: consults `BUILTIN_BACKENDS` (`{"file":
NdjsonFileBackend}`) first, then the `agctl.logs_backends` entry-point group. Unknown
`type` → `ConfigError` (exit `2`). Constructed via `new_logs_client(source)` in
`logs_commands.py` (the test seam commands call, monkeypatched in tests). This is the
`DbClient` analog; `type` is the `connection.type` analog.

## 9. Behavior & Semantics — `NdjsonFileBackend` (the v1 backend)

- **Tail read (`scan`/`await_one`):** backward-scan to the last `tail_lines` newlines
  (no full-file load). `since`/`until` are compared against the canonical `timestamp`,
  not file mtime.
- **Non-JSON lines:** skip; emit a `warn` to `agctl`'s stderr log; never fail (third-party
  plain-text appenders may interleave).
- **Format adapter:** a per-`format` normalizer `raw_json_obj → CanonicalEntry`. v1 ships
  `logstash` only. The normalizer maps known logstash keys to canonical slots and every
  remaining top-level key into `fields` (D3).
- **`follow` (`tail`):** size-poll loop — `stat` every `poll_interval_ms`, read new bytes
  when size grows. No `inotify` (cross-platform). On `stop_event`, stop and return.
- **`await_one` one-shot vs poll:** `timeout_s <= 0` → one `scan` of the window. `> 0` →
  loop: scan, return on first match (positive) / keep going, return cleanly on timeout
  (`--not`), sleeping `poll_interval_ms` between scans.
- **`sample_schema`:** read the last `sample_lines` (default `100`) lines, normalize to
  canonical, and report `standard` = canonical slots observed present, `conditional` =
  `["stack_trace"]`/`["tags"]` when seen, `observed` = the union of `fields.*` keys seen.
  Well-known logstash noise (`@version`, `level_value`) MAY be filtered from `observed`
  to reduce clutter (implementation detail).

## 10. Error & Exit-Code Model

No new error types; the existing `AgctlError` hierarchy covers every path.

| Failure | Type | Exit | When |
|---|---|---|---|
| Unknown `--source` (no key under `logs.sources`) | `ConfigError` | 2 | command entry |
| Unknown backend `type` | `ConfigError` | 2 | `LogClient` construction (D12) |
| File backend missing/invalid `path` | `ConfigError` | 2 | `validate_config` (§11) |
| Malformed `--match` (compile error, post-placeholder-fill) | `ConfigError` | 2 | up-front `compile_jq` (D7) |
| `--match` used and `jq` extra missing | `ConfigError` | 2 | lazy `_jq` → `pip install 'agctl[logs]'` (D11) |
| `--duration` + `--until-stopped` both given (`tail`) | `ConfigError` | 2 | startup (streaming manual envelope) |
| Positive `assert` finds no match | `AssertionFailure` | 1 | after scan/poll (D5) |
| `--not` `assert` finds a match | `AssertionFailure` | 1 | after scan/poll (D6); offending entry in `detail` |
| `--match` jq eval error at runtime | (soft non-match) | — | `jq_bool` swallows to `False` |
| Log file absent at runtime | (empty result) | — | `0 entries`, not an error (D10) |
| Unparseable `--since`/`--until` value | `ConfigError` | 2 | command entry (duration/ISO-8601 parse) |

Exit-code contract unchanged: `0` ok / `1` assertion / `2` config-or-env.

## 11. Validation Rules (`agctl/config/validator.py`)

- **Per-source `validate_config`.** During `agctl config validate`, each `logs.sources`
  entry is validated by its selected backend's `validate_config` (unknown `type`, file
  backend missing `path`, etc. → exit `2`). Mirrors how `mocks.kafka` cross-refs are
  validated today.
- **`--match` expressions are NOT pre-compiled at config time** (they are call-time CLI
  args, not config). Up-front `compile_jq` runs at command entry when `--match` is
  present (D7).
- All existing validation (unresolved `${ENV}`, dangling refs, missing-`description`
  warnings) is unchanged.

## 12. Discovery Changes — `log-sources` category

Additive, following the existing three-level protocol (no runtime registry; reads off the
pydantic `Config`):

- Add `"log-sources"` to `_VALID_CATEGORIES`; update `_SUMMARY_HINT`; add
  `log_sources = len(cfg.logs.sources)` to `_summary_core`.
- Add `elif category == "log-sources":` branches in `_category_core` (name +
  description: `"<type> logs for <name> (<path>)"`) and `_item_core`, and a loop in
  `_search_core`.
- **Item detail** calls `backend.sample_schema(sample_lines=100)` (§9), returning:

```json
{ "category": "log-sources", "name": "order-service",
  "description": "file logs for order-service",
  "path": "logs/order-service.log", "type": "file", "format": "logstash",
  "schema_fields": {
    "standard": ["timestamp","level","logger","message","thread","service"],
    "conditional": ["stack_trace"],
    "observed": ["traceId","spanId","requestId","orderId","customerId","status"]
  },
  "example": "agctl logs query --source order-service --level ERROR --since 5m" }
```

`observed` reflects what is actually in the file (sampled), not a fixed schema list.

## 13. Packaging

- **New `logs` extra** in `pyproject.toml`: `logs = ["jq>=1.6"]` under
  `[project.optional-dependencies]`. (`kafka`/`db` already bundle `jq`; `logs` stands
  alone for read/assert without a broker or DB.)
- **New entry-point group** `agctl.logs_backends` under `[project.entry-points]`, left
  empty initially (like `agctl.plugins` / `agctl.assertions`). The built-in `file`
  backend is registered in `BUILTIN_BACKENDS`, not via entry-point (D12, symmetric with
  `postgresql` in `BUILTIN_DRIVERS`).
- **New `logs` command group** wired in `cli.py`. No `version` bump (additive under
  major `2`).

## 14. Testing Strategy

Mirrors the existing test architecture and seams (CliRunner + `new_logs_client`
monkeypatch; integration tests self-skip). File logs need **no external service**, so
integration tests can be always-on (real temp files), not gated behind
`AGCTL_TEST_LIVE`.

**Unit (no I/O beyond tmp files):**
- `NdjsonFileBackend`: `logstash → CanonicalEntry` normalization (slots + `.fields.*`);
  non-JSON line skipped (warn, no fail); `scan` honors `--level`/`--logger`/`--message`
  client-side; `--match` via `jq_bool`; `since`/`until` windowing on `timestamp`;
  `await_one` one-shot vs poll (poll returns first match; `--not` returns clean on
  timeout); `follow` emits new lines and stops on `stop_event`; `sample_schema`
  enumerates slots + observed `fields` keys.
- `logs_commands`: `query` truncation + `--limit`; `assert` positive-hit (exit 0) /
  positive-miss (exit 1, `AssertionError`) / `--not` both directions; one-shot vs poll;
  `--match` placeholder fill + up-front `compile_jq` loud-fail (exit 2); missing `jq`
  => `ConfigError`; zero-flag `query` returns entries unchanged.
- `logs tail`: streaming line shape (no envelope); `--duration`/`--until-stopped` mutex
  => `ConfigError` envelope at startup; signal wakeup (stop_event set during sleep ends
  the loop promptly); summary line + exit 0.
- `discover`: `log-sources` in summary count + hint; category listing; item detail with
  `schema_fields`; search hit.
- `config/validator`: per-source `validate_config` (unknown `type`, missing `path`).

**Integration (always-on, no containers):** real temp `.log` files written by the test,
exercising `query`/`assert`/`tail` end-to-end through the Click CLI and a real
`NdjsonFileBackend` (no monkeypatch), including the §15 negative + correlation scenario.

New files: `tests/unit/test_logs_client.py`, `tests/unit/test_logs_commands.py`,
`tests/unit/test_logs_discover.py` (or extension of `test_discover_command.py`),
`tests/integration/test_logs_commands.py`.

## 15. Deferred & Not-Covered

- **Victoria / Loki / ES backend** — future external package(s) via `agctl.logs_backends`.
- **`ecs` / `gelf` format parsers** — seam exists; parsers deferred.
- **Named log patterns** (`logs.patterns` / `--pattern`) — YAGNI for v1.
- **Field extraction (`--capture path=name`)** — deferred (same call as http-jq).
- **Rolled-over / archived log files** — file backend reads only the current active file
  (see known-limitation note below).
- **`--match-all`, range/regex level matching.**
- **Caching compiled jq programs** at runtime — the up-front `compile_jq` is
  validation-only (same as http-jq D5).

### Known-limitations note (honest)

- **Rolled-over history is invisible.** The file backend reads only the *current active*
  log file (e.g. `logs/order-service.log`), not its `TimeBasedRollingPolicy` archives
  (`logs/order-service.2026-07-06.log`). A `--since 7d` query therefore misses entries in
  archived files. This is bounded by the service's own rolling policy and is acceptable
  for test-time observation (scenarios run in the present). Reading archives is a
  follow-up, not a v1 gap to hide.
- **`sample_schema` is a sample.** `observed` reflects the last `sample_lines` (100)
  lines; rarely-logged `.fields.*` keys may be absent from discover output. This is
  documented behavior, not a bug.
- **Client-side `--match` over HTTP backends (future).** Because `--match` is always
  client-side (D8), a future Victoria backend transfers all entries matching the
  *structural* filters before applying jq. A `--match`-only query with a loose
  `--since`/`--logger` could pull a large payload. Mitigation belongs to the Victoria
  backend (e.g. require/warn on a tight structural filter when `--match` is used) — not a
  v1 concern (no Victoria backend exists), but recorded so the contract is honest.
- **`--not` + poll is inherently slower than positive poll.** "Assert no `ERROR` in the
  next 10s" must wait the full window before succeeding, whereas positive assertions
  early-stop on first match. Documented, unavoidable.

## 16. Rejected Alternatives (ADR-style)

- **File-tail only (the proposal as-written), defer all transport abstraction.**
  Rejected (D1): Victoria is HTTP-query; it does not extend file-tailing, it rewrites it.
  Cheapest v1, but contradicts the stated extensibility goal.
- **Backend interface but no entry-points (hardcode file inside `LogClient`).** Rejected
  (D1/D12): adding Victoria later would need a core edit; the entry-point group is
  symmetric with `db_drivers` and costs nothing.
- **Custom/MDC values as raw top-level fields (the proposal's `.orderId`).** Rejected
  (D3): a logstash artifact; breaks transport-agnosticism. `.fields.*` is uniform.
- **A `.raw` escape-hatch blob on the canonical entry.** Rejected (D3): the normalizer
  maps every key to a slot or `.fields.*`, so nothing is lost; `.raw` would double
  payload for no gain.
- **`format` as the only config discriminator (the proposal).** Rejected (D4): cannot
  select a transport. `type` (transport) and `format` (parser) are independent axes.
- **`assert` poll-only with required-positive `--timeout` (the `kafka assert` rule).**
  Rejected (D5): logs persist and are re-scannable; one-shot is natural and faster, and
  makes negative assertions clean.
- **Shell-level exit-code inversion for negative assertions (the proposal's §6.2/§10).**
  Rejected (D6): error-prone for agents; `--not` is first-class and scriptable.
- **`--match` not placeholder-filled (the `kafka assert` rule).** Rejected (D7): logs
  have no named-pattern layer; `--match` is the parameterized surface, and the proposal's
  UX relies on filling it.
- **jq server-side for a future Victoria backend.** Rejected (D8): Victoria cannot run
  arbitrary jq server-side, and diverging semantics per backend is worse than uniform
  client-side evaluation.
- **Treating a missing log file as an error.** Rejected (D10): a not-yet-started service
  legitimately has no file; empty-result keeps negative/pre-warm assertions working.
- **Bundling jq into core / always importing it.** Rejected (D11): structural-flag-only
  usage needs no jq; lazy import preserves a zero-jq path (the http-jq D7 precedent).

## 17. Docs & Skill Impact

- **DESIGN.md** — §2 add the `logs:` config section (`sources` with `type`/`path`/
  `format`/`service`, `defaults`); §3 add the `logs query`/`assert`/`tail` commands and
  flags (note `--not`, optional `--timeout`, one-shot+poll, `--match` placeholder-fill);
  add a new extension-point entry for `agctl.logs_backends` alongside `agctl.db_drivers`.
- **ARCHITECTURE.md** — clients layout: `log_backend_protocol.py`, `log_client.py`,
  `log_backends/ndjson_file.py`; the `LogBackend` Protocol + `CanonicalEntry` contract +
  `LogClient` selection; note `logs tail` as a third streaming exception (`http ping`,
  `mock run`); note the `logs` extra and lazy-jq rule.
- **`skills/agctl-config`** — add a `logs.sources` authoring reference (`type` default
  `file`, `path`, `format`, the canonical-entry / `.fields.*` model, `--not` and
  one-shot+poll semantics, the missing-file-is-empty rule, the rolled-over-history
  limitation).
- **`README.md`** — surface the `logs` group and `pip install 'agctl[logs]'`.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").
