# `agctl` — Architecture Document

**Status:** Source-of-truth (as-built).
**Last updated:** 2026-07-02

> `ARCHITECTURE.md` is the **source of truth for how the system works today** —
> the as-built runtime, module boundaries, data flows, and extension model. Its
> companion [`DESIGN.md`](./DESIGN.md) captures *intent and specification*
> (goals, schema, CLI surface, output contract). Where the two disagree, this
> file describes reality and DESIGN.md the target; §14 catalogs the divergences.

---

## Table of Contents

1. [Purpose & Relation to DESIGN.md](#1-purpose--relation-to-designmd)
2. [System at a Glance](#2-system-at-a-glance)
3. [Module & Layer Map](#3-module--layer-map)
4. [Request Lifecycle](#4-request-lifecycle)
5. [Configuration Pipeline](#5-configuration-pipeline)
6. [Output Contract & The Envelope](#6-output-contract--the-envelope)
7. [Error & Exit-Code Model](#7-error--exit-code-model)
8. [Transport / Client Layer](#8-transport--client-layer)
9. [Assertion Engine](#9-assertion-engine)
10. [Extension Points](#10-extension-points)
11. [Dependency & Packaging Model](#11-dependency--packaging-model)
12. [Testing Architecture](#12-testing-architecture)
13. [Glossary](#13-glossary)
14. [Design-vs-Implementation Deltas](#14-design-vs-implementation-deltas)
15. [Known Limitations](#15-known-limitations)

---

## 1. Purpose & Relation to DESIGN.md

`agctl` is an agent-facing CLI harness for testing distributed systems: every
invocation emits one JSON object on stdout, exits with a deterministic code, and
is stateless and composable. This document records **how the code works**, not
what it aspires to.

| Document | Answers | Changes when |
|---|---|---|
| [`DESIGN.md`](./DESIGN.md) | *What* — goals, schema, CLI surface, output contract, roadmap | The intended design shifts |
| This file | *How* — module layout, runtime flow, extension wiring | The code changes |

**Audience:** `agctl` contributors and plugin/driver authors (§10). **Reading
order:** skim DESIGN §1–4 for the "what," then this file before touching code.

**Spec → implementation map:**

| DESIGN.md | This doc |
|---|---|
| §2 Configuration Schema | §5, §3 |
| §3 CLI Commands | §4, §6, §8 |
| §4 Output Schema | §6 |
| §5 Config Resolution Order | §5 |
| §7 Project Structure | §3 (+ §14) |
| §8 Design Principles | §4, §6, §7 |
| §9 Extension Points | §10 |

---

## 2. System at a Glance

`agctl` is a thin CLI shell around three protocol clients (HTTP, Kafka, DB). It
loads one `agctl.yaml` per invocation, resolves a named template or free-form
request, talks to the system under test, and prints exactly one JSON envelope.
No server, no daemon, no session state, no on-disk DB.

```
agent/shell ─▶ Click CLI (cli.py)            groups: config http db kafka check
                  │                                  + top-level discover
                  │                          global: --config
                  ▼
        @envelope("cmd.name") (command.py)    timer + try/except → one emit + SystemExit
                  │
        ┌─────────┼──────────────┐
        ▼         ▼              ▼
   load_config  *_core logic   output.emit() ─▶ stdout: ONE JSON object
   (config/)    (commands/)      clients/* ─▶ SUT   (except http ping → NDJSON)
```

**Invariant contract** (every command except the `http ping` streaming exception):

- Exactly one JSON object on stdout.
- Deterministic exit: `0` success, `1` failed assertion, `2` tool/config/env error (§7).
- No color, banners, or progress output; stderr is diagnostics only.
- No disk state read or written between invocations.

Step-by-step execution: §4.

---

## 3. Module & Layer Map

One responsibility per module. The directory tree as it exists today:

```
agctl/
├── cli.py                      # Click entry point; registers groups; loads plugins; secret masking
├── command.py                  # @envelope decorator + load_config_or_raise
├── output.py                   # emit() — the single permitted stdout write path
├── errors.py                   # typed AgctlError hierarchy
├── params.py                   # --param k=v  →  dict[str,str]
├── resolution.py               # {placeholder} fill, body deep_merge, :name→%(name)s
├── assertions.py               # jq / subset / equals / coercion primitives
├── assertion_registry.py       # pluggable assertion-mode registry + entry-point discovery
├── plugin_protocol.py          # Protocol contract for protocol plugins
├── config/
│   ├── loader.py               # discover → parse → interpolate → override → version → validate
│   ├── resolver.py             # AGCTL_<SECTION>__<KEY> override layer
│   ├── validator.py            # cross-reference checks + description warnings
│   └── models.py               # Pydantic v2 typed config models
├── commands/                   # one module per command group
│   ├── http_commands.py        # http call / request / ping
│   ├── kafka_commands.py       # kafka produce / consume / assert
│   ├── db_commands.py          # db query / assert / execute / schema
│   ├── check_commands.py       # check ready
│   ├── config_commands.py      # config validate / show / init
│   ├── discover_commands.py    # discover summary / category / item / search
│   └── mock_commands.py        # mock run (HTTP mock + Kafka reactors)
├── mock/                       # mock server implementation (HTTP + Kafka)
│   ├── routing.py              # path-template matching (pure functions)
│   ├── http_server.py          # stdlib ThreadingHTTPServer + handler
│   ├── kafka_reactor.py        # Kafka consumer loop + jq match + reaction
│   ├── jq_precompile.py        # walks mocks → (label, expr) pairs; compile-only validate
│   ├── capture.py              # envelope capture resolver: jq_value(envelope, from) → typed CaptureValue
│   ├── capture_validate.py     # walks mocks → object-capture placement errors; pure Python (no jq)
│   └── engine.py               # MockEngine lifecycle (start/run/shutdown; Step 0 pre-compiles jq)
├── data/
│   └── sample-config.yaml      # packaged starter config (read via importlib.resources)
└── clients/
    ├── http_client.py          # httpx wrapper (lazy import)
    ├── kafka_client.py         # confluent-kafka wrapper (lazy import)
    ├── db_client.py            # driver dispatch via agctl.db_drivers entry points
    ├── db_driver_protocol.py   # DBDriver Protocol
    └── db_drivers/postgresql.py  # built-in psycopg driver (lazy import)
```

> DESIGN §7's structure sketch predates several modules; §14 lists the deltas.

> **Module location note (mock):** The Pydantic models for `mocks` (`MocksConfig`, `HttpMockConfig`, `KafkaMockConfig`, etc.) live in `config/models.py` alongside every other section model — *not* in `agctl/mock/`. This preserves config's dependency isolation (`config/* → {errors}` only) and keeps the one-place convention. The `mock/` subpackage holds only runtime engine/server/reactor code.

**Dependency direction (inward-only, no cycles):** `cli → commands → {config,
clients, resolution, assertions, params, errors, command, output}`; `clients →
{errors, assertions, resolution}` (+ the lazy-imported heavy lib); `config/* →
{errors}`. Nothing imports `cli` or `commands` except the CLI entry point, which
keeps clients and config independently testable.

---

## 4. Request Lifecycle

Every command callback splits into a thin **Click command** and a **`_core`
function**, and the `_core` is wrapped by `@envelope(command_name)`. This split
is what makes the one-emit contract enforceable. From `db_commands.py`:

```python
@click.command("query")
@click.option(...)
def db_query(ctx, ...):                      # Click layer: unwrap ctx + delegate
    config_path = ctx.obj.get("config_path") if ctx.obj else None
    _db_query_envelope(config_path, ...)

_db_query_envelope = envelope("db.query")(_db_query_core)   # wrapped once at import
```

Trace of `agctl db query --template find-order --param orderId=42`:

1. **Click parses** argv; the root group stored `--config` in
   `ctx.obj["config_path"]`. Click invokes `db_query`, which delegates to the
   wrapped `_db_query_envelope`.
2. **`@envelope`** starts a monotonic timer and calls `_db_query_core` in a `try`.
3. **Config load** — `load_config_or_raise` runs the full pipeline (§5). Bad
   config → `ConfigError`, caught by the envelope.
4. **Resolution** — `resolve_db_request` checks exactly one of
   `--template`/`--sql` is given, looks up the template (missing →
   `TemplateNotFound`), parses `--param`, resolves the connection (explicit arg
   → template's `connection` → `defaults.database_connection`; unresolved/unknown
   → `ConfigError`).
5. **Execute** — `DbClient` `connect`/`execute`/`close` in `try/finally`. The
   driver rewrites `:name`→`%(name)s` and returns coerced dict rows.
6. **Return** — `{"rows": [...], "row_count": N, "connection": name}`.
7. **Emit + exit** — `@envelope`'s `else` branch emits `ok:true` and exits 0.

**Failure paths** all funnel through `@envelope`:

| `_core` behavior | Envelope reaction | Exit |
|---|---|---|
| returns a dict | `emit(ok=True, result=...)` | 0 |
| raises an `AgctlError` subclass | `emit(ok=False, error=err.to_dict())` + `SystemExit(err.exit_code)` | §7 |
| raises bare builtin `AssertionError` | `emit(ok=False, error={type:"AssertionError",...})` | 1 |
| raises any other `Exception` | `emit(ok=False, error={type:"InternalError",...})` | 2 |

**Commands that bypass `@envelope`** (different output model, but they
hand-reimplement the same try/except → emit + exit shape so the contract holds):

- **`http ping`** streams NDJSON (§6). Startup errors get a structured envelope
  *before* any ping line; bad `--body` JSON → `InternalError`.
- **`mock run`** streams NDJSON (the second streaming exception, §6). Startup errors emit one structured envelope *before* any event line; the command installs `SIGTERM`/`SIGINT` handlers that emit a final `summary` line and exit `0` (clean) or `1` (runtime errors occurred).
- **`discover`** has four `_core`s, each wrapped in its *own* envelope with a
  distinct tag (`discover.summary`/`.category`/`.item`/`.search`); the Click
  command selects one from the flags. Argument errors emit under `discover.summary` (exit 2).
- **`config validate`/`show`** (in `config_commands.py`) do their own load + emit; `config init` skips load (it bootstraps the file) and emits on its own.

Config is loaded **per invocation** — no in-process cache; each command
discovers, parses, and validates `agctl.yaml` from scratch. This is what makes
`agctl` stateless.

---

## 5. Configuration Pipeline

`config/loader.py::load_config`, fixed order; any stage may fail the load with a
`ConfigError`:

```
discover_config_path → yaml.safe_load → interpolate → apply_env_overrides
                                                → _check_version (major == "1")
                                                → Config.model_validate (Pydantic v2)
   (caller then runs validate_config → cross-refs)
```

**Path discovery** (`discover_config_path`, DESIGN §5), highest precedence first:

1. `--config <path>` — if given, *only* this file; no walk-up.
2. `AGCTL_CONFIG` env var (ignored if `--config` present).
3. **Walk-up** — `cwd` then each parent for `agctl.yaml`; stop at the first
   `.git` or the filesystem root; first match wins.
4. None found → `ConfigError`.

**Env interpolation** (`interpolate`, DESIGN §2.2), in every string scalar:

- `${VAR}` — required; missing → collected and the load raises one `ConfigError`
  listing **all** unresolved vars (never a silent empty substitution).
- `${VAR:-default}` — missing → the literal `default`.
- `${VAR:-}` — missing → empty string.

`${...}` is honored in string *values* only, never keys.

**Env overrides** (`apply_env_overrides`, DESIGN §5/§8) — `AGCTL_<SECTION>__<KEY>`
deep-merged with highest precedence. Two refinements beyond the spec:

- **≥2 segments required**, so `AGCTL_CONFIG` (a path) and `AGCTL_TEST_*` flags
  are not mistaken for overrides.
- **Case- and hyphen-insensitive key matching** — `AGCTL_SERVICES__ORDER_SERVICE__BASE_URL`
  overrides the real `services.order-service.base_url`, not a phantom
  `order_service` sibling. No existing-key match → new key under the lowercased
  segment (write-oriented; hyphen reconstruction not guaranteed).

**Resolver denylist** — certain config fields are excluded from env override to preserve safety-critical semantics. The following fields are denylisted and cannot be overridden via `AGCTL_*` env vars:

- `database.connections.*.writable` — write safety gate must be set explicitly in config
- `database.templates.*.mode` — read/write mode is a template authoring intent, not runtime config

**Version guard** — major only, currently `"1"`; mismatch → `ConfigError`. The `mocks` section is additive under major `1` — no version bump.

**Typed validation** (`Config.model_validate`, `models.py`) — Pydantic v2 tree
(`Config` → `ServiceConfig`, `KafkaConfig`/`KafkaSSL`, `DatabaseConfig`/…,
`HttpTemplate`, `Defaults`). Notable: `KafkaSSL.security_protocol` is
upper-cased and restricted to `PLAINTEXT|SSL|SASL_SSL|SASL_PLAINTEXT` at load,
so an invalid protocol fails fast rather than surfacing as an opaque broker error.

**Cross-reference validation** (`validator.py`) — returns `(errors, warnings)`,
catching what Pydantic cannot:

- HTTP template → unknown `service` → **error**.
- DB template → unknown `connection` → **error**.
- `defaults.database_connection` → unknown connection → **error**.
- Any template/pattern/stub/reactor missing `description` → **warning**
  (discovery degrades).
- `mocks.kafka` with reactors but no top-level `kafka.brokers` → **error**.
- Path-template shadowing: a `{name}` segment ahead of a literal at the same
  position (`/orders/{id}` before `/orders/bulk`) → **warning** (first-match-wins;
  the literal stub silently never fires). Method-agnostic — known limitation.
- **jq-shadowing**: two stubs sharing method + path, both with `match.jq` →
  **warning** (first-match-wins; a wrong predicate can fire the wrong branch
  silently — the wrong-branch false-green surface). Method-gated so a
  `GET /api/{id}` vs `DELETE /api/users` pair does not false-warn.

`validate_config` is the reference for "valid config" and is also folded into
`agctl config validate` (plus plugin validation, §10, and mock jq compile checks
via `collect_jq_compile_errors` in `mock/jq_precompile.py`, invoked from the
command layer — not `validator.py` — so `config/*` stays free of an `assertions`
dependency).

---

## 6. Output Contract & The Envelope

Every command writes one JSON object to stdout via `output.emit()`:

```json
{ "ok": true, "command": "db.query", "result": { ... }, "error": null, "duration_ms": 18 }
```

`emit()` is the **only** permitted stdout write path — `json.dumps(...,
default=str)` + newline + flush (`default=str` stringifies non-JSON-native
values rather than crashing).

**The `@envelope` guarantee.** Because every `_core` is wrapped, *every* code
path — success, `AgctlError`, builtin `AssertionError`, unexpected `Exception` —
passes through exactly one `emit()` before exit. A command cannot emit twice,
emit nothing, or leak a raw traceback to stdout.

**The streaming exception — `http ping`.** A background keepalive must stream
results as they happen, so it violates "one object per invocation":

- Not wrapped by `@envelope`.
- Emits one JSON object **per ping** (`{ping, ok, status_code, duration_ms,
  timestamp}`) directly as the loop runs.
- Installs `SIGTERM`/`SIGINT` handlers that flip a stop event; the loop emits a
  final `{summary:true, total_pings, failed_pings, duration_ms}` and exits `0`
  (all ok) or `1` (any failed).
- Startup errors emit a single structured envelope **before** any ping line.

**The second streaming exception — `mock run`.** Like `http ping`, the mock server must stream events as they happen:

- Not wrapped by `@envelope`.
- Emits one JSON object **per event** (`started`, `http.hit`, `http.unmatched`, `http.body_parse_skipped`, `capture.missing`, `kafka.reacted`, `kafka.skipped`, `kafka.error`, `summary`) directly as they occur.
- All emission goes through a single-writer path (`threading.Lock` in `MockEngine.emit_event` or a dedicated writer thread) — concurrent HTTP handler threads and Kafka reactor threads emit safely without interleaved lines.
- Installs `SIGTERM`/`SIGINT` handlers that set a stop event; the loop emits a final `{summary, http_hits, http_unmatched, http_body_parse_skipped, kafka_reactions, kafka_skipped, kafka_errors, duration_ms}` and exits `0` (clean, no runtime errors) or `1` (runtime errors occurred, or `--fail-fast` triggered).
- Startup errors emit a single structured envelope **before** any event line.

**stdout vs stderr** — all machine-readable output on stdout; stderr carries
only diagnostics an agent must never parse (plugin-load failures, entry-point
skips, stack traces). The structured `InternalError` envelope reaches stdout
*before* the process exits non-zero.

---

## 7. Error & Exit-Code Model

All command errors derive from `AgctlError` (`errors.py`). Each subclass fixes a
`type_name` (the envelope's `error.type`) and an `exit_code`; `to_dict()`
produces the `error` object.

| Exception class | `type_name` | Exit | Raised when |
|---|---|---|---|
| `AgctlError` (base) | `InternalError` | 2 | Catch-all; unexpected failure in `agctl`. |
| `AssertionFailure` | `AssertionError` | 1 | An assertion was evaluated and failed. |
| `ConfigError` | `ConfigError` | 2 | Bad/missing config, unresolved required env var, version mismatch, bad invocation. |
| `ConnectionFailure` | `ConnectionError` | 2 | Service/broker/database unreachable. |
| `OperationTimeout` | `TimeoutError` | 1 | A non-assertion op exceeded its budget (slow HTTP, hung query). |
| `TemplateNotFound` | `TemplateNotFound` | 2 | Named template/pattern/connection missing. |

**Naming nuance:** the *Python class* is `AssertionFailure` but its `type_name`
is `"AssertionError"` — the envelope string matches DESIGN's table, not the
Python identifier. The builtin `assert` statement also yields `"AssertionError"`
via `@envelope`'s dedicated branch.

**Fail loudly** is not one switch but a set of deliberate early raises:
unresolved required `${VAR}` (one error listing all), version mismatch, invalid
`security.protocol` (Pydantic), an undelivered Kafka message within flush
timeout (`ConnectionFailure`, never a silent `null` success), zero or >1
assertion mode on `db`/`kafka assert` (`ConfigError` before any network call),
`--jq-path`/`--equals` pairing misuse or non-JSON `--contains` on `http call`/
`http request` (`ConfigError` before the request is sent, via
`validate_http_assertion_args` — gating pre-request so a bad invocation never
triggers a wasted side-effect),
a malformed `match.jq`/reactor `match` in `mock run` (`ConfigError` at engine
startup before probe/bind — `MockEngine.start()` Step 0 walks mocks via
`iter_mock_jq_expressions` and `compile_jq`s each expression; a body-only
config imports nothing), and a match-miss in `kafka assert` / `db assert` /
`http call` / `http request` (`AssertionFailure`, exit 1).

**`db execute` write-safety failures** — the command rejects writes at multiple
gates, each surfacing as `ConfigError` (exit 2): missing `--write` flag, omitted
explicit target (both `--template` and `--connection` absent), non-writable
connection, or read-mode template. All fail before any database operation is
attempted. Driver-level write errors (execute, rollback, commit failures) surface
as `ConnectionFailure`.

**`db schema` failures** — all three failure modes surface as `ConfigError`
(exit 2): the selected driver lacks the optional `describe_schema` capability
(raised pre-connect by the `supports_describe_schema()` probe, so no connection
is opened), Level-2 not-found (0 matches), and Level-2 ambiguity (>1 match
across schemas, with `error.detail.candidates=[{schema, kind}]` — disambiguate
with `--schema`). The capability check fires pre-`SELECT`; the not-found and
ambiguity checks fire post-`describe_schema`. DB/catalog errors during
introspection surface as `ConnectionFailure`.

---

## 8. Transport / Client Layer

All three clients share one convention that makes optional-extras packaging
(§11) work: **the heavy library is lazy-imported inside the constructor or
methods.** Importing `agctl.clients.*` never requires `httpx`,
`confluent_kafka`, or `psycopg`; only constructing/executing a client triggers
the import, and a missing extra raises `ConfigError` pointing at the right
`pip install 'agctl[...]'`. Clients are constructed fresh per invocation and
own no shared state.

### HttpClient (`clients/http_client.py`)

httpx wrapper. `request()` returns the §4.2 result dict (`status_code`,
`response_time_ms`, lowercased `headers`, `body`, `url`, `method`). Header merge
is case-insensitive (per-call wins). Body parses as JSON when content-type says
so, else text.

Exception mapping: `ConnectError`/`ConnectTimeout` → `ConnectionFailure`;
`ReadTimeout`/other `TimeoutException` → `OperationTimeout`; other `HTTPError`
→ `ConnectionFailure`.

> A 4xx/5xx response is **not** an error by default. `http call`/`http request`
> return `ok:true` with the status in `result` — HTTP status is a result, not an
> assertion. The response-assertion flags (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`)
> flip this: ≥1 flag enters assertion mode, `evaluate_http_assertions` runs
> after the request, and a failing mode raises `AssertionFailure` (exit 1) with
> `error.detail = {response, failures}`. Zero flags leaves the default
> "result, not assertion" path byte-for-byte unchanged. (Contrast `check ready`,
> which treats 2xx as "ready" but still returns `ok:true` for the command itself.)

### KafkaClient (`clients/kafka_client.py`)

confluent-kafka wrapper, transport-agnostic — it takes an `extra_conf` dict
(`security.protocol`/`ssl.*`); the typed→librdkafka translation is owned by the
command layer (`_kafka_ssl_conf`).

- **`produce`** — JSON-encodes the value, registers a delivery-report callback,
  `flush(timeout=30)`, returns the `kafka.produce` shape. A delivery error **or**
  non-zero remaining-flush count (undelivered) → `ConnectionFailure`.
- **`consume_window`** — workhorse for `kafka consume` and custom assertions.
  Seeks each partition to `now - lookback` (or offset 0 with `from_beginning`),
  polls until the wall-clock deadline **or** `expect_count` matches ("whichever
  comes first"). A predicate that raises → non-match (silently skipped).
- **`find_in_window`** — early-stop path for `kafka assert`. Same seek mechanics,
  returns the **first** matching message + a scanned-count; no match →
  `(None, scanned)` → the command raises `AssertionFailure`.
- **`offsets_for_times` −1 edge case** — when a partition's newest message is
  older than the window, the result is `-1`; the client seeks such partitions to
  `OFFSET_END`, else `auto.offset.reset=earliest` would re-read every stale
  message and violate the window.
- **`consume_loop`** — committed consume loop for mock reactors. The reactor owns its consumer lifecycle (D13); each message invokes a `handle(message, attempt, final)` callback and returns a `ReactionResult` (`COMMIT` → store_offset + commit; `RETRY` → re-handle the same in-memory message; `STOP` → exit loop). Supports `max_retries` (must be >= 1), `stop_event`, and optional rebalance callbacks (`on_assign`/`on_revoke`). The consumer is closed in `finally` after the loop exits.
- **`probe`** — one-shot broker connectivity check. Builds a consumer, calls `list_topics(topic, timeout)`, and closes the consumer. Raises `ConnectionFailure` on any Kafka/broker error (the engine calls this at startup before binding HTTP to satisfy the spec §11 "broker unreachable at startup → exit 2" guarantee).
- **Test seams** — `producer_factory`/`consumer_factory` inject fakes sharing
  the real Producer/Consumer contract.

### DbClient + driver (`clients/db_client.py`, `clients/db_drivers/postgresql.py`)

`DbClient` dispatches to a `DBDriver` selected by the connection's `type`:
discovery merges entry points (`agctl.db_drivers`, §10) over the always-present
built-in `{"postgresql": PostgreSQLDriver}`; broken third-party drivers are
skipped. The client delegates `connect`/`execute`/`close` and exposes DI seams
(`driver`/`drivers`).

`PostgreSQLDriver` lazy-imports `psycopg` in `connect` (missing → `ConfigError`,
`db` extra). A `connectable` can be injected for tests, in which case the driver
does **not** own the connection and `close()` is a no-op. `execute` rewrites
`:name`→`%(name)s` (protecting `::` casts), runs read-only (no commit), and runs
each cell through `coerce_db_value` (§9).

**Connection mechanism** — `connect()` accepts an optional `url` field (a
PostgreSQL connection URI, e.g. `postgresql://user:pass@host:port/dbname`). When
`url` is present, it is passed to `psycopg.connect()` as the positional conninfo
string; any discrete fields (`host`/`port`/`dbname`/`user`/`password`) are
forwarded as kwargs and **override** the corresponding URI parameters (psycopg's
merge semantics — kwargs win). When `url` is absent, the driver behaves as before,
using only discrete fields.

**Optional `execute_write` capability** — write support is an optional driver
capability, not required by the `DBDriver` protocol. `DbClient.execute_write`
probes the driver for a callable `execute_write` attribute and raises
`ConfigError` if absent. `PostgreSQLDriver` implements this method; third-party
drivers may omit it (read-only drivers remain valid).

**Optional `describe_schema` capability** — live schema discovery is a second
optional driver capability following the same probe pattern.
`DbClient.supports_describe_schema()` is a **pre-connect, side-effect-free**
probe (it inspects the driver for a callable `describe_schema` attribute without
opening a connection); `agctl db schema` calls it to fail fast with
`ConfigError` (exit 2) when the driver cannot introspect. `DbClient.describe_schema`
delegates to the driver and returns its dict unchanged.
`PostgreSQLDriver.describe_schema` reads `pg_catalog` (relations from `pg_class`/
`pg_namespace`/`pg_attribute`; columns from `pg_attribute`/`pg_type`; defaults,
enum values, comments, and constraints from `pg_attrdef`/`pg_enum`/
`pg_description`/`pg_constraint`) and excludes schemas whose name starts with
`pg_` or equals `information_schema`. Third-party drivers may omit it
(non-introspection drivers remain valid for `db query`/`assert`/`execute`).

**`db schema` lifecycle** — the Click `db_schema` command dispatches on
`--table`: absent → `_db_schema_tables_core` (Level 1, command tag
`db.schema.tables`); present → `_db_schema_table_core` (Level 2, command tag
`db.schema.table`). Both `_core`s are wrapped by `@envelope` and share an
`_probe_and_describe` helper that probes → `connect` → `describe_schema` →
`close` in `try/finally`, so the close runs even on Level-2 not-found/ambiguity
raises. Level 2 flattens the single driver match into the top-level result
shape; `error.detail.candidates` carries the >1-match case.

**Commit-after-materialize ordering** — `PostgreSQLDriver.execute_write`
materializes `rows_affected` and `RETURNING` data *before* committing. The
transaction commits only after all result data is successfully fetched and
coerced. Any exception during execute, fetch, or coercion triggers a rollback
and surfaces as `ConnectionFailure`. This guarantees that a reported success
(`ok:true` with `rows_affected`) reflects a durable write, while a failure never
leaves the transaction ambiguous.

---

## 9. Assertion Engine

Primitives in `assertions.py`, composed by the command layer. Five families:

- **`jq_bool` / `jq_value`** — jq predicate / path evaluation. `jq` is
  lazy-imported; a *missing library* → `ConfigError` (exit 2), while a jq
  *expression* error → silently treated as no-match/`None` (partial matching
  must never crash on a weird message).
- **`compile_jq(expr, *, label)`** — compile-only guard, distinct from
  `jq_bool`/`jq_value`: it does **not** feed input or swallow errors. A
  malformed expression raises `ConfigError` (exit 2) with `label` context, so
  an authoring typo is caught loudly at startup / `config validate` rather than
  silently mis-matching every request (the load-bearing loud-on-typo guard).
  Drives both transports' pre-compile (HTTP `match.jq` and Kafka reactor
  `match`) via `mock/jq_precompile.py::iter_mock_jq_expressions` and
  `collect_jq_compile_errors` — covering both `MockEngine.start()` Step 0 and
  `config validate`.
- **`json_subset(needle, haystack)`** — recursive subset match for `--contains`:
  dicts key-by-key; lists order-independently (each needle element subset of
  *some* haystack element); scalars `==`.
- **`parse_equals` + `coerce_db_value` + `type_aware_equal`** — the `--equals`
  pipeline for `db assert --expect-value`: `parse_equals` JSON-parses the arg
  when valid (`"0"`→`0`, `"true"`→`True`, `"null"`→`None`) else treats it as a
  string; `coerce_db_value` normalizes a DB cell (`bool` before `int`,
  `Decimal`→int/float, datetime→ISO 8601, `UUID`→str); `type_aware_equal`
  compares strictly — a number never equals a string (`0` ≠ `"0"`).

**Where used:**

- **`db assert --expect-value`** — `coerce_db_value(jq_value(first_row, path))`
  vs `parse_equals(equals)` via `type_aware_equal`.
- **`db assert --expect-rows`** — row-count comparison.
- **`kafka assert`** — `_build_assert_predicate` combines `--contains`
  (`json_subset`, optionally narrowed by `--path`), `--match` (`jq_bool`), and
  `--pattern` (named config pattern, `{param}`-filled, then `jq_bool`). **All**
  active modes must pass.
- **`kafka consume --match`** — a `jq_bool` predicate, with short-circuit on
  `--expect-count`.
- **`http call` / `http request`** — `evaluate_http_assertions` composes
  `jq_bool` (`--match`), `json_subset` (`--contains`), and `jq_value` +
  `parse_equals` + `type_aware_equal` (`--jq-path` + `--equals`) against the
  response; `validate_http_assertion_args` gates `--jq-path`/`--equals` pairing
  and `--contains` JSON shape pre-request (`ConfigError` exit 2, before the
  request is sent). `coerce_db_value` is intentionally not reused — HTTP
  response bodies are already JSON-native.
- **`mock run` capture extraction** — `mock/capture.py::resolve_captures` reuses
  `jq_value(envelope, spec.from_)` to read each explicit `capture.<name>.from`
  off the live message envelope (HTTP request or Kafka message), producing a
  typed `CaptureValue` map consumed by `resolution.render_typed`. So `jq_value`
  now powers mock capture in addition to HTTP response assertions
  (`--jq-path`); a `from` resolving to `null`/missing is the soft-miss path
  (emits `capture.missing`, substitutes empty string), distinct from a missing
  `jq` library which re-raises as `ConfigError` (exit 2).

### Custom assertion modes (`assertion_registry.py`)

`AssertionRegistry` resolves named modes reached via `db/kafka assert --assertion
<name>` (DESIGN §9.3). Built-in names (`expect_rows`, `expect_value`,
`contains`, `match`, `pattern`) are registered **by name** for discoverability
and clean unknown-mode rejection, but their logic lives in the command layer —
`evaluate` raises `NotImplementedError`, which `evaluate_custom` turns into a
`ConfigError` ("has dedicated flags; do not invoke via `--assertion`").
Third-party modes (from the `agctl.assertions` entry point, §10) **are**
invoked: `evaluate(context)` returns `{passed, ...}`; `passed=False` →
`AssertionFailure`. Context differs by command:

- `db assert --assertion` → `{rows, row_count, sql, params, connection}`.
- `kafka assert --assertion` → `{topic, messages, count, params}` (the full
  consumed window).

`evaluate_custom` is the single bridge owning the error mapping
(`TemplateNotFound` unknown name, `ConfigError` built-in-via-`--assertion`,
`AssertionFailure` failing/raising mode).

---

## 10. Extension Points

Three independent entry-point groups, all declared in `pyproject.toml` and
discovered via `importlib.metadata` (3.11+/older-Python shim). Each load+register
is individually `try/except`-wrapped: a broken entry point is logged to stderr
and **skipped** — it never bricks the CLI, the registry, or driver discovery.

| Group | Contract (file) | Loaded by | Adds |
|---|---|---|---|
| `agctl.db_drivers` | `DBDriver` Protocol (`clients/db_driver_protocol.py`) | `DbClient.load_drivers` | a new DB `type` |
| `agctl.plugins` | `Plugin` Protocol (`plugin_protocol.py`) | `cli._load_plugins` (at import) | a new top-level command group |
| `agctl.assertions` | `Assertion` base (`assertion_registry.py`) | `AssertionRegistry.load_entry_points` (lazy, cached) | a new `--assertion` mode |

- **DB drivers** — `DbClient` selects by `connection["type"]`; unknown →
  `ConfigError`. Built-in `postgresql` always wins over a registration gap.
  The `DBDriver` protocol requires `connect`/`execute`/`close`; both
  `execute_write` and `describe_schema` are optional capabilities (probed
  without opening a connection). Built-in `postgresql` implements both;
  third-party drivers may omit either — `DbClient.execute_write`/
  `supports_describe_schema` probe for the method and raise `ConfigError`
  if absent. Register in another package's `pyproject.toml`:
  ```toml
  [project.entry-points."agctl.db_drivers"]
  mysql = "agctl_mysql:MySQLDriver"
  ```
- **Protocol plugins** — `cli._load_plugins` runs at CLI import, mounting each
  plugin's `command_group` onto the root `cli` group (name from `.name` →
  `command_group.name` → entry-point name). If a plugin exposes
  `validate_config(config_dict)`, it runs during `agctl config validate` and its
  error strings fold into the result under `plugin.<name>` (exit 2). No plugins
  ship today; the group is a clean no-op.
- **Assertion modes** — loaded lazily on first `get_default_registry()` and
  cached. See §9.

---

## 11. Dependency & Packaging Model

The as-built dependency split is the biggest divergence from DESIGN §7 (which
proposed one flat list). `pyproject.toml` splits heavy libraries into optional
extras, so a user installs only what they need and the package imports fast:

| Group | Dependencies | Needed for |
|---|---|---|
| core (always) | `click`, `pyyaml`, `pydantic` | CLI, config loading, schema |
| `http` | `httpx` | `http *`, `check ready` |
| `jq` | `jq` | HTTP response assertions (`--match`/`--jq-path` on `http call`/`request`), mock HTTP `match.jq` (and mock startup pre-compile of stub `match.jq` / reactor `match`) |
| `kafka` | `confluent-kafka`, `jq` | `kafka *` |
| `db` | `psycopg[binary]`, `jq` | `db *` |
| `dev` | `pytest` | unit tests |
| `integration` | `testcontainers`, `agctl[db,kafka,http]`, `pytest` | live integration tests |

`jq` is bundled under `kafka`/`db` (which always needed it) **and** exposed as a
dedicated `jq` extra for HTTP-only users (response assertions) and HTTP-only-mock
users (`match.jq`) — `pip install 'agctl[jq]'`. A mock with no `match.jq` and no
reactor `match` imports nothing, preserving the zero-dep HTTP-only mock. At
runtime the lazy-import convention (§8) keeps the error category correct: a
missing library → `ConfigError` (exit 2) pointing at `agctl[jq]`, not an opaque
`ModuleNotFoundError`.

**Build & entry points:** hatchling backend, wheel target `agctl`; console
scripts `agctl`/`agt` → `agctl.cli:cli`; entry-point groups `agctl.db_drivers`
(registers built-in `postgresql`), `agctl.plugins`, `agctl.assertions` (§10);
requires Python `>=3.11`.

---

## 12. Testing Architecture

```
tests/
├── unit/          # fast, no network/Docker; clients tested via injected fakes
└── integration/   # real services; self-skipping when unavailable
```

**Unit tests** mirror the module layout and use the seams the code exposes on
purpose:

- `http_commands.set_default_transport`/`_default_transport` — inject an
  `httpx.MockTransport`.
- `kafka_commands.new_kafka_client` / `KafkaClient(consumer_factory=,
  producer_factory=)` — inject fakes.
- `db_commands.new_db_client` / `DbClient(driver=)` /
  `PostgreSQLDriver(connectable=)` — inject a fake driver or connection.
- `ping_loop(...)` takes injectable `sleep_fn`, `monotonic`, `emit_line`,
  `stop_event`.
- `mock_commands.new_mock_engine` — inject a fake `MockEngine`.
- `KafkaClient.consume_loop` / `probe` — `consumer_factory` injects fakes.
- `MockEngine` — `emit_fn` injects a fake writer; `run_id` is configurable.
- `mock/routing.py` — pure functions, tested directly.
- `sys.setswitchinterval` single-writer test technique — forces thread switching to verify NDJSON emission doesn't interleave (tests the `threading.Lock` in `MockEngine.emit_event`).
- `cli._entry_points` and `assertion_registry._entry_points` are
  monkeypatchable.

Because clients lazy-import their libs, unit tests run **without** `httpx`,
`confluent_kafka`, or `psycopg` installed.

**Integration tests** are **self-skipping** — they never fail because a service
is absent; they `pytest.skip()`. The machinery (`tests/integration/conftest.py`):

- Per-service fixtures `require_http_service`/`require_postgres`/`require_kafka`
  probe reachability and skip if absent, else yield the handle.
- Two ways to supply a live service:
  1. **Manual/CI** — point at a running service via `AGCTL_TEST_*` env vars
     (`AGCTL_TEST_HTTP_URL`, `AGCTL_TEST_PG_DSN`, `AGCTL_TEST_KAFKA_BROKER`).
  2. **Local Docker, opt-in** — `AGCTL_TEST_LIVE=1` spins throwaway
     **testcontainers** (Postgres 16, Kafka+KRaft) plus a local threaded HTTP
     mock, wiring addresses into the same env vars. Flag unset → nothing starts
     and every integration test skips, so a plain `pytest` run stays fast.

**Integration tests** are **self-skipping** — they never fail because a service
is absent; they `pytest.skip()`. The machinery (`tests/integration/conftest.py`):

- Per-service fixtures `require_http_service`/`require_postgres`/`require_kafka`
  probe reachability and skip if absent, else yield the handle.
- Two ways to supply a live service:
  1. **Manual/CI** — point at a running service via `AGCTL_TEST_*` env vars
     (`AGCTL_TEST_HTTP_URL`, `AGCTL_TEST_PG_DSN`, `AGCTL_TEST_KAFKA_BROKER`).
  2. **Local Docker, opt-in** — `AGCTL_TEST_LIVE=1` spins throwaway
     **testcontainers** (Postgres 16, Kafka+KRaft) plus a local threaded HTTP
     mock, wiring addresses into the same env vars. Flag unset → nothing starts
     and every integration test skips, so a plain `pytest` run stays fast.

Run: `pytest tests/unit`; live: `AGCTL_TEST_LIVE=1 pytest tests/integration`.

**New mock-specific integration tests:**

- `tests/integration/test_mock_commands.py` — tests `mock run` end-to-end with real HTTP server and Kafka reactors (self-skips under `AGCTL_TEST_LIVE=1`).
- `FakeKafkaClient` / `consumer_factory` seams for unit tests (avoid real broker).

---

## 13. Glossary

Terms used throughout, not already defined inline:

- **Template vs free-form** — a *named template* (`http call <name>`,
  `db --template`, `kafka assert --pattern`) resolves service/path/SQL/headers
  from config; a *free-form* request (`http request`, `db --sql`) supplies them
  on the command line. Templates are preferred.
- **Windowed / lookback assertion** — `kafka consume`/`assert` seek partitions to
  `now - lookback` and read forward (not "subscribe at latest"), so an event
  published just before the command starts still falls in the window.
  `--lookback` defaults to the resolved `--timeout`; `--from-beginning`
  overrides to offset 0.
- **Send-then-assert** — the reliability property that follows: `produce` (or an
  HTTP call) then `kafka assert` works without subscribe-before-produce gymnastics.
- **Three substitution syntaxes** (do not conflate): `${VAR}` — env, resolved at
  config load (§5); `{name}` — HTTP path/body & Kafka-pattern placeholders,
  filled at call time from `--param`; `:name` — JDBC-style SQL params, rewritten
  to `%(name)s` at execute time (chosen over `{...}` in SQL to avoid colliding
  with JSON literals).
- **Secret masking** — `config show` masks values whose key contains
  `password`/`token`/`secret`, or is a bare `key`/`*_key` suffix, to `"***"`
  unless `--unmask`.
- **Test seam** — a module-level function/attribute or factory kwarg tests
  monkeypatch to inject fakes without touching the network.

---

## 14. Design-vs-Implementation Deltas

Tracks where DESIGN.md (spec) and the as-built code disagree, so neither doc
silently misleads. As of the last sync, all previously-known deltas are resolved:
DESIGN.md now matches the real dependency/packaging model, module tree,
env-override rules, and `@envelope` enforcement, and `config validate`/`show`
were extracted into their own module. Re-populate this table whenever a new
divergence is introduced.

| Area | Resolution |
|---|---|
| **Mocking non-goal reversed** | DESIGN §1 stated "No built-in mock server." The `agctl mock` command (HTTP stub server + Kafka reactors) reverses this non-goal — local testing is now self-contained. DESIGN §1 and §3.5 document the new command. ||
| Dependencies | DESIGN §7 updated to the optional-extras model (`http`/`kafka`/`db`/…). |
| Project structure | DESIGN §7 tree updated to the real layout (incl. `config/models.py`, top-level `command.py`/`errors.py`/…, `clients/db_drivers/`). |
| One-emit enforcement | DESIGN §8 documents the `@envelope` mechanism. |
| Env-override matching | DESIGN §5/§8 document case-/hyphen-insensitive matching + the ≥2-segment rule. |
| `commands/config_commands.py` | Extracted from `cli.py` — every command group now has its own module. |
| Assertion base | Already consistent; DESIGN §9.3 clarifies that built-in modes are registered by name but implemented in the command layer. |

---

## 15. Known Limitations

What the system does **not** do today (as-built; see DESIGN §10 for the roadmap):

- **No Schema Registry / Avro/Protobuf decoding** — Kafka values are raw JSON;
  `schema_registry_url` is parsed but unused.
- **No retry/polling DSL** — eventually-consistent assertions need a caller-side
  loop (e.g. shell around `db assert`).
- **No multi-step scenario primitive** — an agent chains commands in a shell.
- **SQL param rewriting doesn't parse string literals** — a `:name` inside a
  literal may be rewritten; `::` casts are protected.
- **Unsupplied `{placeholder}` values stay literal** — no call-time validation
  (deferred).
- **No MCP wrapper, no OpenTelemetry propagation, no parallel runner, no secret
  backends** — deferred per DESIGN §10.

**Mock server MVP limitations** (see DESIGN §10 "Known-wrong-result / Not Covered" for the full list with failure-mode analysis):

- **Stateful flows** (OAuth/token exchange, create-then-GET, idempotency-key replay, pagination) — static engine returns same canned response → false green.
- **TLS / HTTPS-pinned or `https://`-hardcoded SUT clients** — cannot intercept HTTPS → integration untested → false green.
- **Cross-transport sagas** (Kafka trigger → HTTP callback) — no causal linkage → false green.
- **Non-JSON Kafka values** (Avro/Protobuf/schema-registry) — emitted as `kafka.skipped` → false green.
- **`match` / `capture` `.`-root divergence (#22)** — explicit `capture.<name>.from` roots at the whole message envelope (so nested fields, the Kafka `.key`, and headers are all reachable, and `type: object` passes the live value through for true JSON-type fields), but `match.jq` (HTTP) / reactor `match` (Kafka) remain payload-rooted (body/value only). The same `.amount` means different things on either side of that fence. Unifying `match` onto the envelope root is breaking and tracked as #22; until then, author `match` payload-rooted and `capture.from` envelope-rooted.
- **Containerized SUT topology** — operator must target `host.docker.internal` / host LAN IP and avoid SUT that swallows connection errors → false green.
- **Shared broker + pinned `consumer_group` reused across runs/devs** — partition split or resume-past-messages → silently missing/old reactions → false green (mitigated by unique-per-run default).
