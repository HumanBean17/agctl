# `agctl` — Architecture Document

**Status:** Source-of-truth (as-built).
**Last updated:** 2026-07-17

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
├── __main__.py                 # python -m agctl entry point (enables `mock start` daemon spawn)
├── cli.py                      # Click entry point; registers groups; loads plugins; secret masking
├── command.py                  # @envelope decorator + load_config_or_raise
├── daemon.py                   # generic daemon primitives (spawn_daemon/terminate/require_posix_daemon/is_alive/pidfile ops) shared by mock + listen; imports only errors
├── output.py                   # emit() (one-shot envelope) + emit_ndjson_line() (streaming event sink) — the only permitted stdout write paths
├── errors.py                   # typed AgctlError hierarchy
├── params.py                   # --param k=v  →  dict[str,str]
├── resolution.py               # {placeholder} fill, body deep_merge, :name→%(name)s
├── assertions.py               # jq / subset / equals / coercion primitives
├── assertion_registry.py       # pluggable assertion-mode registry + entry-point discovery
├── plugin_protocol.py          # Protocol contract for protocol plugins
├── config/
│   ├── loader.py               # discover → merge .env → parse → interpolate → version → overlay-merge → override → validate
│   ├── env_file.py             # .env auto-load + precedence (--env-file > AGCTL_ENV_FILE > sibling)
│   ├── resolver.py             # AGCTL_<SECTION>__<KEY> override layer
│   ├── validator.py            # cross-reference checks + description warnings
│   └── models.py               # Pydantic v2 typed config models
├── commands/                   # one module per command group
│   ├── http_commands.py        # http call / request / ping
│   ├── kafka_commands.py       # kafka produce / consume / assert
│   ├── kafka_listen_commands.py # kafka listen run / start / status / stop / assert / results / messages (long-lived capture daemon)
│   ├── db_commands.py          # db query / assert / execute / schema
│   ├── logs_commands.py        # logs query / assert / tail
│   ├── check_commands.py       # check ready
│   ├── config_commands.py      # config validate / show / init / migrate
│   ├── discover_commands.py    # discover summary / category / item / search
│   ├── grpc_commands.py        # grpc call / healthcheck
│   └── mock_commands.py        # mock run / start / stop / status (HTTP mock + Kafka reactors)
├── mock/                       # mock server implementation (HTTP + Kafka + gRPC)
│   ├── routing.py              # path-template matching (pure functions)
│   ├── http_server.py          # stdlib ThreadingHTTPServer + handler
│   ├── kafka_reactor.py        # Kafka consumer loop + jq match + reaction
│   ├── grpc_server.py          # MockGrpcServer: grpc.Server + generic servicer dispatching `/<svc>/<mtd>` via the descriptor pool; pure dispatch core (build_envelope, dispatch_grpc, GrpcDispatchOutcome) reused from here; Health + Reflection registered on the same server; lifecycle start/serve_forever/shutdown/actual_listen
│   ├── jq_precompile.py        # walks mocks (http/kafka/grpc) → (label, expr) pairs; compile-only validate
│   ├── capture.py              # envelope capture resolver: jq_value(envelope, from) → typed CaptureValue
│   ├── capture_validate.py     # walks mocks (http/kafka/grpc response.message/messages) → object-capture placement errors; pure Python (no jq)
│   ├── daemon.py               # mock-specific daemon layer: port-keyed pidfile/log paths (mock-<port>|mock-kafka|mock-grpc-<port>), RunningMock (http_listen/grpc_listen), resolve_target (matches --listen against any of listen/http_listen/grpc_listen), NDJSON log parser, failure taxonomy incl. grpc.unmatched/grpc.error (generic primitives live in agctl/daemon.py)
│   └── engine.py               # MockEngine lifecycle (start/run/shutdown; Step 0 pre-compiles jq; Step 2b constructs+binds the gRPC server via grpc_server_factory seam)
├── listen/                     # kafka listen capture daemon (long-lived, capture-to-disk)
│   ├── daemon.py               # run_id keying, state paths, RunningListener, meta/asserts.jsonl helpers, events.log parser
│   ├── capture_file.py         # per-topic capture reader (filter/count/first/paginate) + build_predicate
│   ├── assert_eval.py          # evaluate_expectations: reuses kafka assert predicate machinery over capture files (no deadline)
│   ├── capture.py              # CaptureLoop: per-topic consume_loop wrapper (seek-to-end-on-assign + jq capture-match + overflow valve)
│   └── engine.py               # ListenEngine lifecycle (start/run/shutdown; per-topic threads; single-writer NDJSON emit; summary)
├── serialization/              # Confluent Schema Registry codecs (Avro / Protobuf); module top stays stdlib-only
│   ├── api.py                  # Format enum + decode_payload/encode_payload/resolve_subject/decode_message (composes wire + registry + codecs)
│   ├── wire.py                 # pure-stdlib Confluent wire-frame kernel (parse_wire / build_wire / is_confluent_frame)
│   ├── registry.py             # lazy SchemaRegistryClient (by-id / by-subject / by-latest caches; build_schema_registry_conf; check_reachable)
│   ├── avro_codec.py           # lazy fastavro decode/encode (parsed-schema cache; strict=True on encode)
│   └── protobuf_codec.py       # lazy DynamicMessage codec (compiles .proto via grpc_descriptors kernel; single-file v1)
├── data/
│   └── sample-config.yaml      # packaged starter config (read via importlib.resources)
└── clients/
    ├── http_client.py          # httpx wrapper (lazy import)
    ├── kafka_client.py         # confluent-kafka wrapper (lazy import)
    ├── db_client.py            # driver dispatch via agctl.db_drivers entry points
    ├── db_driver_protocol.py   # DBDriver Protocol
    ├── db_drivers/postgresql.py  # built-in psycopg driver (lazy import)
    ├── grpc_client.py          # grpcio wrapper (lazy import); reflection-resolution + JSON↔protobuf via the shared kernel
    ├── grpc_descriptors.py     # shared gRPC proto kernel: build_descriptor_pool, find_method, call_type_of, message_class, serialize, deserialize (grpcio-free at module top; lazy imports)
    ├── log_client.py           # log backend dispatch via agctl.logs_backends entry points
    ├── log_backend_protocol.py # LogBackend Protocol + CanonicalEntry DTOs
    └── log_backends/ndjson_file.py  # built-in NDJSON file backend (lazy import)
```

> DESIGN §7's structure sketch predates several modules; §14 lists the deltas.

> **Module location note (mock):** The Pydantic models for `mocks` (`MocksConfig`, `HttpMockConfig`, `KafkaMockConfig`, `GrpcMockConfig`/`GrpcStub`/`GrpcMatch`/`GrpcResponse`/`GrpcResponseMessage`, etc.) live in `config/models.py` alongside every other section model — *not* in `agctl/mock/`. This preserves config's dependency isolation (`config/* → {errors}` only) and keeps the one-place convention. The `mock/` subpackage holds only runtime engine/server/reactor code.

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

Trace of `agctl grpc call --target echo-server --service echo.EchoService --method Unary --message '{"msg":"hello"}'`:

1. **Click parses** argv; the root group stored `--config` in
   `ctx.obj["config_path"]`. Click invokes `grpc_call`, which routes based on
   call type detection.
2. **Call type detection** — `_detect_call_type` loads config, resolves the target
   (`echo-server`), constructs `GrpcClient`, and calls `find_method` to get the
   method descriptor and determine call type (`unary` for this example).
3. **Unary routing** — for `unary`/`client_stream`, delegates to the wrapped
   `_grpc_call_envelope` (which wraps `_grpc_call_core`).
4. **`@envelope`** starts a monotonic timer and calls `_grpc_call_core` in a `try`.
5. **Config load** — already loaded; `load_config_or_raise` re-validates.
6. **Message serialization** — JSON request is parsed, placeholders are filled,
   and `json_format.ParseDict` serializes to protobuf bytes.
7. **gRPC call** — `GrpcClient.call_unary` invokes the channel's `unary_unary`
   method with request/response serializers. Success → JSON response deserialized
   via `json_format.MessageToDict`; RpcError (non-OK status) → caught and
   returned as `GrpcUnaryResult` with status field.
8. **Post-call assertions** — if assertion flags were set, `evaluate_grpc_assertions`
   runs (reusing jq/subset/equals primitives); failure → `AssertionFailure`.
9. **Return** — `{"target": "localhost:50051", "service": "echo.EchoService",
   "method": "Unary", "call_type": "unary", "status": {"code": 0, "name": "OK",
   "message": ""}, "message": {...}, "initial_metadata": {...}, "trailers": {...}}`.
10. **Emit + exit** — `@envelope`'s `else` branch emits `ok:true` and exits 0.

**Server-streaming/bidi routing** — for `server_stream`/`bidi` call types, the
command bypasses `@envelope` and emits NDJSON directly (§6, streaming exceptions).

Trace of an inbound unary RPC hitting a gRPC mock stub (the SUT calls
`echo.EchoService/Unary` against `mocks.grpc.listen`):

1. **grpc.Server dispatch** — `MockGrpcServer._build_server` registered one
   `method_handlers_generic_handler` per service at construction, with the
   right `RpcMethodHandler` shape (unary_unary/unary_stream/stream_unary/
   stream_stream) per `(service, method)` keyed from `method_meta`. grpcio
   dispatches `/echo.EchoService/Unary` to the matching handler, deserializing
   the request bytes via the kernel's `deserialize(input_desc)`.
2. **Behavior callable** (`_make_behavior("unary")`) closes over the dispatch
   glue. `_handle_unary` reads `context.invocation_metadata()` (lowercased
   into a dict by `_metadata_to_dict`) and builds the §8.1 envelope via
   `build_envelope(service, method, metadata, message=<deserialized dict>)`.
3. **Dispatch** (`dispatch_grpc`) — first-match-wins over
   `stubs_by_method[(svc, mtd)]` (insertion-ordered dict-of-dicts). Each
   candidate is filtered by `_match_body` (`json_subset` over the request
   `message`, skipped for `client_stream`) AND `_match_jq` (`jq_bool` over
   the envelope). The first match → `resolve_captures` + `render_typed` (the
   exact HTTP/Kafka capture/render pipeline reuses `mock/capture.py` and
   `resolution.render_typed`). No match → `GrpcDispatchOutcome(matched=False)`
   and the behavior aborts with `UNIMPLEMENTED` after emitting `grpc.unmatched`
   (a fatal event).
4. **Status resolution** — `outcome.status` is `(code, name)` from
   `parse_grpc_status(response.status)` (the public helper in
   `assertions.py`, single source of truth for name/code coercion). A non-OK
   status makes the behavior `context.abort(code, name)` after emitting the
   `grpc.hit` line; an OK status returns the rendered message dict, which the
   response serializer (kernel `serialize(output_desc)`) encodes to bytes.
5. **Emission** — `emit_event` is the engine's single-writer sink
   (`threading.Lock`); `grpc.hit` increments `_grpc_hits`, `grpc.unmatched` /
   `grpc.error` increment their counters AND set `_runtime_error` (so the run
   exits `1` at shutdown). `capture.missing` is emitted by the same
   `emit_capture_missing` helper HTTP/Kafka use.
6. **Server lifecycle** — `MockEngine.start` Step 2b constructs the server via
   the `grpc_server_factory` seam (production lazy-imports `MockGrpcServer`
   inside the closure; tests inject a fake) and binds via `add_insecure_port`
   (EADDRINUSE → `ConfigError`). `serve_forever(stop_event)` blocks on a
   dedicated engine thread; `shutdown()` calls `server.stop(grace=2)`.

The other three call types share the dispatch core: `server_stream` reuses
the unary envelope and emits N `grpc.hit`s (one per `response.messages`
entry); `client_stream` aggregates the request stream into a `{messages,
count}` envelope at close (matched once, `match.body` skipped); `bidi` is
request/response pairing (one envelope per incoming request, one rendered
response per match, no stateful conversation).

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

`config/loader.py::load_config` → `compose_config`, fixed order; any stage may fail the load with a `ConfigError`:

```
discover_config_path → merge .env defaults into env (real env wins) → yaml.safe_load → interpolate → _check_version (base must be v3)
                                                ↓
                              for each overlay (in flag order):
                                discover_config_path (explicit only) → yaml.safe_load → interpolate
                                → PartialConfig.model_validate (attributes type errors to the overlay)
                                → version-major-match check (if overlay has version)
                                → deep_merge (sidecar-wins, record overrides at leaves)
                                                ↓
                              apply_env_overrides (on MERGED dict; AGCTL_* wins over overlays)
                                                ↓
                              Config.model_validate (Pydantic v2, final)
   (caller then runs validate_config → cross-refs)
```

**Path discovery** (`discover_config_path`, DESIGN §5), highest precedence first:

1. `--config <path>` — if given, *only* this file; no walk-up.
2. `AGCTL_CONFIG` env var (ignored if `--config` present).
3. **Walk-up** — `cwd` then each parent for `agctl.yaml`; stop at the first
   `.git` or the filesystem root; first match wins.
4. None found → `ConfigError`.

**`.env` auto-load** (`config/env_file.py`, new) — after config discovery and
*before* interpolation, a `.env` file is merged into the config env dict as
**defaults**: real env wins (`env = {**dotenv, **env}` — a fresh dict, `os.environ`
is never mutated). Source precedence: `--env-file` (explicit, `required=True`) >
`AGCTL_ENV_FILE` (read from the *real* env only — a value set inside the `.env`
itself would be circular and is ignored) > `.env` sibling of the resolved
`agctl.yaml` (`required=False`: a missing sibling is a no-op, not an error). An
explicit source that is missing raises `ConfigError` (mirrors `--config`).
`load_env_file` parses with `dotenv_values(path, interpolate=False)` and drops
bare-key `None` values, so raw `${...}` flows into agctl's single `interpolate()`
— one engine owns all `${...}` resolution (chains, nested). Placement is
deliberately post-discovery (the sibling sits next to the *resolved* config) and
pre-interpolation (so `${VAR}` can resolve from `.env`); a consequence is that
`AGCTL_CONFIG`/`AGCTL_ENV_FILE` set *inside* the `.env` cannot steer their own
resolution — only real-env values do. `compose_config`, `load_config`, and
`load_config_or_raise` gained an `env_file` param threaded from the CLI's global
`--env-file` flag (DESIGN §3).

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

**Version guard** — major only; `_check_version` (`loader.py`) rejects `major != TOOL_MAJOR_VERSION` (`"3"`) with a `ConfigError` whose message points at `agctl config migrate`. The version is the **config schema version**: `"3"` restructured `Config.kafka` from a single flat object into a named map `kafka.clusters.<name>` (mirroring `database.connections`) plus `default_cluster`. The `match` envelope-rooting introduced under `"2"` (`.body.x` for HTTP, `.value.x` for Kafka) is unchanged in v3; `"1"` and `"2"` (the legacy flat-`kafka:` schemas, `"1"` additionally payload-rooted) are rejected, not silently re-interpreted. `agctl config migrate` performs a STRUCTURAL lift (v1/v2 → v3: a flat `kafka:` block moves into `kafka.clusters.default` + `default_cluster: default` — see `config/migrate.py::migrate_config`/`_lift_kafka_clusters`); v1 sources additionally get the `.body | ` / `.value | ` prefix on the three `match`-site families (v2 exprs are already envelope-rooted and are not re-prefixed). CLI `--match` flags in scripts/prompts are out of its scope and must be prefixed manually (v1 inputs only).

**Typed validation** (`Config.model_validate`, `models.py`) — Pydantic v2 tree
(`Config` → `ServiceConfig`, `KafkaConfig`/`KafkaCluster`/`KafkaSSL`,
`DatabaseConfig`/…, `HttpTemplate`, `Defaults`). Notable: `KafkaSSL.security_protocol` is
upper-cased and restricted to `PLAINTEXT|SSL|SASL_SSL|SASL_PLAINTEXT` at load,
so an invalid protocol fails fast rather than surfacing as an opaque broker error.
Under v3 `KafkaConfig` is a named map (`clusters: dict[str, KafkaCluster]`,
`default_cluster`, `patterns`), mirroring `DatabaseConfig.connections`;
`KafkaCluster` owns the per-cluster knobs formerly on `KafkaConfig` (brokers / ssl /
timeout / consumer group / schema registry).

**Overlay types** — `PartialConfig` (a `Config` subclass with `version: str | None = None`) represents a fragment; version is inherited from the base at merge time. `ComposedConfig` is a `NamedTuple(config: Config, overrides: list[dict])`, where each override record is `{"path": "<dotted>", "overlay": "<file>"}` surfacing which overlay won which leaf.

**Deep merge** (`deep_merge`, `loader.py`) — sidecar-wins dict merge: for each key in the overlay, if the key is absent in the base, add it; if both base and overlay values are dicts, recurse; otherwise, record an override and replace with the overlay value. Scalar/list leaves are replaced wholesale, not merged. Overrides are recorded only at leaf level (e.g., `templates.create-order.path`), not at intermediate dicts.

**Cross-reference validation** (`validator.py`) — returns `(errors, warnings)`,
catching what Pydantic cannot:

- HTTP template → unknown `service` → **error**.
- DB template → unknown `connection` → **error**.
- `defaults.database_connection` → unknown connection → **error**.
- Any template/pattern/stub/reactor missing `description` → **warning**
  (discovery degrades).
- Each `mocks.kafka.reactors.<name>` must resolve a cluster
  (`reactor.cluster` → `kafka.default_cluster` → single-cluster auto-default,
  mirroring `resolve_cluster_name`); a dangling `reactor.cluster` → **error** at
  `…reactors.<name>.cluster`, and a resolved cluster with empty `brokers` →
  **error** at `…reactors.<name>`. (Inlined in the validator so `config/` stays
  free of a `commands/` import.)
- Each `kafka.topics.<t>` resolves its cluster with the same precedence
  (`topic.cluster` → `kafka.default_cluster` → single-cluster auto-default);
  a dangling `topic.cluster` → **error** at `…topics.<t>.cluster`. A topic
  resolving to `avro`/`protobuf` (override or cluster default) whose cluster
  has no `schema_registry_url` → **error** at `…topics.<t>` (when an override
  drove the need) or `…clusters.<c>` (when a cluster default did). A
  `subject_strategy` set on a topic whose resolved `value_format` is `json` →
  **warning** at `…topics.<t>.subject_strategy`. (Same inline-resolution
  rationale as the reactor check.)
- `kafka.clusters.<c>.schema_registry.auth` shape: `basic` without `basic_auth`
  → **error**, and `mtls` without `ssl` → **error** (both at
  `…clusters.<c>.schema_registry.auth`). Out-of-enum `auth` is a defensive
  guard — Pydantic's `Literal` rejects it at parse time first.
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
default=str, ensure_ascii=False)` + newline + flush (`default=str` stringifies
non-JSON-native values rather than crashing; `ensure_ascii=False` emits UTF-8
characters directly rather than `\uXXXX` escapes). stdout/stderr are forced to UTF-8
at CLI bootstrap via `sys.stdout.reconfigure(encoding="utf-8")` so raw UTF-8
emission doesn't raise `UnicodeEncodeError` on non-UTF-8 terminals.

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
- Emits one JSON object **per event** (`started`, `http.hit`, `http.unmatched`, `http.body_parse_skipped`, `capture.missing`, `kafka.reacted`, `kafka.skipped`, `kafka.error`, `grpc.hit`, `grpc.unmatched`, `grpc.error`, `summary`) directly as they occur. The three engines share one event stream and one `started`/`summary` line; engines not running report `null` in `started` and zero counters in `summary`. `kafka.skipped` doubles as the per-message decode-failure signal under a non-JSON reactor codec (reason `"decode failed: …"`, non-fatal, COMMIT — the trigger client's codec seam invokes `on_decode_error` before `_handle`, which emits the event and clears the per-message flag).
- All emission goes through a single-writer path (`threading.Lock` in `MockEngine.emit_event`) — concurrent HTTP handler threads, Kafka reactor threads, and gRPC per-RPC handler threads (served by the gRPC server's own `ThreadPoolExecutor(max_workers=concurrency_cap)`) emit safely without interleaved lines.
- Installs `SIGTERM`/`SIGINT` handlers that set a stop event; the loop emits a final `{summary, http_hits, http_unmatched, http_body_parse_skipped, kafka_reactions, kafka_skipped, kafka_errors, grpc_hits, grpc_unmatched, grpc_errors, duration_ms}` and exits `0` (clean, no runtime errors) or `1` (runtime errors occurred — any `kafka.error`, `grpc.unmatched`, `grpc.error`, or `--fail-fast` triggered).
- Startup errors emit a single structured envelope **before** any event line.

**The third streaming exception — `logs tail`.** Like `http ping`, the log tail command must stream entries as they appear:

- Not wrapped by `@envelope`.
- Emits one JSON object **per matching entry** (canonical entry model) directly as the loop runs.
- Installs `SIGTERM`/`SIGINT` handlers that set a stop event; the loop emits a final `{summary:true, total_emitted, duration_ms}` and exits `0`.
- Supports `--duration` (daemon timer) and `--until-stopped` (signal-only) modes, mutually exclusive.
- Startup errors emit a single structured envelope **before** any entry line.

**The fourth and fifth streaming exceptions — `grpc call` (server-streaming and bidi).** Like `http ping`, server-streaming and bidirectional gRPC calls emit NDJSON:

- Not wrapped by `@envelope`.
- Emits one JSON object **per response message** (NDJSON) directly as the stream runs.
- Each line includes `event: "message"`, the `message` dict, and optional `trailers`.
- Installs `SIGTERM`/`SIGINT` handlers that set a stop event; the loop emits a final `{summary:true, messages, matched, status, duration_ms}` and exits `0` (or `1` if `--expect-count` is not met).
- Startup errors emit a single structured envelope **before** any message line.

**The sixth streaming exception — `kafka listen run`.** Like `mock run`, the Kafka capture listener streams lifecycle events as they happen:

- Not wrapped by `@envelope`.
- Emits one JSON object **per event** (`started`, per-topic `capture.overflow`, `decode.error`, `kafka.error`, `summary`) directly as they occur (the per-message capture is written to disk, not stdout). `decode.error` fires once per failed SIDE under a non-JSON topic codec (the codec seam's `on_decode_error` callback emits it, increments the topic's `decode_errors` counter, and arms a per-message skip flag so `_handle` COMMITs without writing a partial envelope — downstream `listen assert` predicates never see a None value misread as data); `fatal: false`, so it does NOT inflate the engine's `errors` tally.
- All emission goes through a single-writer path (`threading.Lock` in `ListenEngine.emit_event`) so concurrent per-topic `CaptureLoop` threads emit safely without interleaved lines.
- Installs `SIGTERM`/`SIGINT` handlers that set a stop event; the loop emits a final `{event: "summary", topics:[{topic, captured, overflowed, decode_errors}], errors, duration_ms}` and exits `0` (clean) or `1` (any `kafka.error` occurred).
- Startup errors emit a single structured envelope **before** any event line.

**The managed daemon commands — `mock start`/`stop`/`status`** are NOT streaming exceptions. Each is a normal `@envelope`-wrapped command that emits exactly one JSON object and exits 0/1/2:

- `mock start` blocks until the daemon's `started` line appears in the log (or a startup error or timeout), then returns the `mock.start` envelope (`ok:true` with pid/listen/log_path/stubs/reactors/started_at).
- `mock stop` signals the daemon, waits for shutdown, parses the log for summary + failure events, and returns the `mock.stop` verdict (`stopped`/`pid`/`signal`/`summary`/`failures`). When fatal failures are found, it raises `AssertionFailure` (exit 1) with the verdict in `error.detail`.
- `mock status` reads the live log and returns the `mock.status` snapshot (`running`/`pid`/`listen`/`uptime_ms`/`summary_so_far`/`failures_so_far`).
- All three commands are wrapped by `@envelope` and follow the one-emit contract; they do NOT stream NDJSON like `mock run`.

**The managed daemon commands — `kafka listen start`/`stop`/`status`** mirror the mock trio (POSIX/WSL-gated via `require_posix_daemon()`, state-keyed by `run_id` under `<state-dir>/listen-<run_id>/`). Each is a normal `@envelope`-wrapped command that emits one JSON object:

- `kafka listen start` spawns a detached `kafka listen run` daemon (unique per-run group `agctl-listen-<run_id>`), writes the pidfile + `meta.json`, and readiness-polls `events.log` for the `started` line. The daemon seeks every assigned partition to `OFFSET_END` via the `consume_loop` `on_assign` callback BEFORE the first poll delivers data, so only messages produced AFTER `start` are captured.
- `kafka listen stop` SIGTERMs the daemon (SIGKILL after `--timeout`), parses `events.log` for `summary` + fatal events, then deletes the run dir + pidfile. Fatal events (`kafka.error`, or `capture.overflow` on a topic with an attached expectation) raise `AssertionFailure` (exit 1); cleanup runs on every path. `stop` does NOT auto-run `results` — an uncollected expectation is silently dropped.
- `kafka listen status` is read-only (live per-topic `captured`/`bytes`/`overflowed` snapshot; never signals the daemon, never removes the pidfile).
- `kafka listen assert`/`results`/`messages` are `@envelope`-wrapped client-side file readers (no daemon IPC, no wall-clock deadline — bounded by capture-file size).

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
| `SerializationError` | `SerializationError` | 2 | Codec / schema-conformance failure on a Kafka value or key (truncated Confluent frame, schema-violating record, Protobuf compile failure). Non-fatal per-message on the decode path (counted in `decode_errors` / emitted as `decode.error` or `kafka.skipped reason="decode failed: …"`); fatal on the encode path (a schema-violating record is a contract/config bug). |
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
a malformed `match.jq`/reactor `match`/gRPC stub `match.jq` in `mock run`
(`ConfigError` at engine startup before probe/bind — `MockEngine.start()`
Step 0 walks mocks via `iter_mock_jq_expressions` and `compile_jq`s each
expression; the walker now covers `mocks.grpc.stubs` alongside HTTP/Kafka;
a body-only config imports nothing), an unresolved gRPC `service`/`method`,
response-shape-vs-call-type mismatch, invalid `response.status`, or missing
descriptor files at `MockGrpcServer` construction (`ConfigError` at
`mocks.grpc.stubs.<name>.<field>`), and a match-miss in `kafka assert` /
`db assert` / `http call` / `http request` (`AssertionFailure`, exit 1).
At runtime, `grpc.unmatched` (no stub matches `service/method`) and
`grpc.error` (handler failure) are fatal — both set the runtime-error flag
so `mock run` exits `1` at shutdown and `mock stop` raises `AssertionFailure`
(they are in `FATAL_FAILURE_EVENTS` alongside `http.unmatched`/`kafka.error`).

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

Both `http request` and `http ping` accept a `--url <full-url>` mode (mutually
exclusive with `--service`/`--path`); `http_commands._split_url` derives the
`(base_url, path)` pair fed to this constructor, so URL mode needs no
client-side change.

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
command layer (`_kafka_ssl_conf`, which now takes the resolved `KafkaCluster`).

**Cluster resolution + client seam (v3).** Multi-cluster selection is decoupled
from client construction: `kafka_commands.resolve_cluster_name(cfg.kafka,
explicit, binding_cluster)` returns the cluster **name** to use (precedence:
`--cluster` flag > a pattern/reactor `.cluster` binding > `kafka.default_cluster`
> the single defined cluster when exactly one exists), mirroring
`db_commands.resolve_connection_name`. It raises `ConfigError` (exit 2) when no
name resolves (>1 cluster, no default) or the resolved name is absent from
`kafka.clusters`. Callers then index `cfg.kafka.clusters[name]` and build the
client via `new_kafka_client(cluster, group_id=None)` (test seam — tests
monkeypatch it). `_kafka_ssl_conf(cluster)` and `_resolve_timeout`/`_resolve_group`
read the resolved cluster's knobs. The mock engine receives one pre-built
`KafkaClient` per reactor (`kafka_clients: dict[reactor_name → KafkaClient]`;
reactors sharing a cluster reuse a single client built via `clients_by_cluster`).

- **`produce`** — JSON-encodes the value, registers a delivery-report callback,
  `flush(timeout=30)`, returns the `kafka.produce` shape. A delivery error **or**
  non-zero remaining-flush count (undelivered) → `ConnectionFailure`.
- **`consume_window`** — workhorse for `kafka consume` and custom assertions.
  Seeks each partition to `now - lookback` (or the librdkafka logical
  `OFFSET_BEGINNING` with `from_beginning`, so the broker resolves each
  partition's actual log-start offset), polls until the wall-clock deadline
  **or** `expect_count` matches ("whichever comes first"). A predicate that
  raises → non-match (silently skipped).
- **`find_in_window`** — early-stop path for `kafka assert`. Same seek mechanics,
  returns the **first** matching message + a scanned-count; no match →
  `(None, scanned)` → the command raises `AssertionFailure`.
- **`offsets_for_times` −1 edge case** — when a partition's newest message is
  older than the window, the result is `-1`; the client seeks such partitions to
  `OFFSET_END`, else `auto.offset.reset=earliest` would re-read every stale
  message and violate the window.
- **`consume_loop`** — committed consume loop for mock reactors AND the `kafka listen` `CaptureLoop`. The reactor/listener owns its consumer lifecycle (D13); each message invokes a `handle(message, attempt, final)` callback and returns a `ReactionResult` (`COMMIT` → store_offsets + commit; `RETRY` → re-handle the same in-memory message; `STOP` → exit loop). Supports `max_retries` (must be >= 1), `stop_event`, and optional rebalance callbacks (`on_assign`/`on_revoke`). `kafka listen`'s `CaptureLoop` forwards an `on_assign` that seeks every partition to `OFFSET_END` BEFORE the first poll delivers data (overriding the client's hardcoded `auto.offset.reset: earliest`), so the listener begins at the head — immune to scan-window misses, volume truncation, and broker retention cleanup. The consumer is closed in `finally` after the loop exits.
- **`probe`** — one-shot broker connectivity check. Builds a consumer, calls `list_topics(topic, timeout)`, and closes the consumer. Raises `ConnectionFailure` on any Kafka/broker error (the engine calls this at startup before binding HTTP to satisfy the spec §11 "broker unreachable at startup → exit 2" guarantee).
- **Codec seam (Avro/Protobuf via Confluent SR)** — `__init__(…, codec=None)` accepts the T8 codec shape `{"value": {"fmt": Format, "subject_strategy": str | None} | None, "key": …, "sr": SchemaRegistryClient | None}`. When `None` (default), every method is byte-for-byte the legacy JSON/string path. When set, `produce` encodes the value (and non-string key) via `encode_payload` against the codec's SR + resolved subject BEFORE publish (encode failures surface as `SerializationError`, fatal on the write path); `consume_window` / `find_in_window` / `consume_loop` accept an `on_decode_error` callback the codec seam invokes once per failed SIDE — the failed side becomes `None`, the message is still collected/scanned/handled, and the callback accounts the failure (consume/assert surface `decode_errors` in the result; the reactor emits `kafka.skipped reason="decode failed: …"`; the CaptureLoop emits `decode.error` and drops the whole message so downstream `listen assert` predicates never see a partial envelope). **Spec §8 corrupt-message handling:** `consume_window` and `find_in_window` wrap `on_decode_error` in a per-message tracker (`_make_decode_failure_tracker`) so they can tell whether a message had any decode failure — corrupt messages are kept in `messages[]` (debug visibility, failed side `None`) but EXCLUDED from the `--expect-count` tally (consume) and CANNOT satisfy the assert match (find_in_window); `kafka listen` drops them entirely. Tombstones (`value=None`) decode to `None` and are NOT counted as decode errors. The module-level `_encode_payload_with_codec(codec, topic, value, key)` is the single source of truth for the produce-side encode, shared by `KafkaClient._encode_payload` and `KafkaReactor._encode_reaction` (the reactor's reaction codec is independent of the trigger client's codec — a JSON trigger may yield an Avro reaction). The codec dict is built by `_resolve_codec(cfg, topic, cluster, cli_value_fmt, cli_key_fmt, probe=True)` in the command layer, which also runs the SR startup probe at most once per cluster.
- **Test seams** — `producer_factory`/`consumer_factory` inject fakes sharing
  the real Producer/Consumer contract, including confluent-kafka 2.15.0's
  argument validation (e.g. `subscribe` rejects an explicit `None` for
  `on_assign`/`on_revoke`; `store_offsets` — plural — is the only offset-store
  method). Keep the fakes honest against the real binding or regressions hide.

### LogClient + backend (`clients/log_client.py`, `clients/log_backends/ndjson_file.py`)

`LogClient` dispatches to a `LogBackend` selected by the source's `type`: discovery merges entry points (`agctl.logs_backends`, §10) over the always-present built-in `{"file": NdjsonFileBackend}`; broken third-party backends are skipped. The client delegates `scan`/`await_one`/`follow`/`sample_schema` and exposes DI seams (`backend`/`backends`).

`NdjsonFileBackend` lazy-imports `jq` in `scan`/`await_one`/`follow` (missing → `ConfigError`, `logs` extra). The backend reads NDJSON files in logstash format (one JSON object per line), normalizes to the canonical entry model, and applies client-side filters (level, logger glob, message substring, jq predicate). It supports three operations:

- **`scan`** — reads the last `tail_lines` from the file, applies time bounds and filters, returns up to `limit` matches with truncation flag.
- **`await_one`** — blocks until a matching entry appears or timeout; supports one-shot mode (timeout ≤ 0, single read of the last `tail_lines`) and poll mode (timeout > 0, two-phase: the historical window is read ONCE then a high-water byte offset tracks only NEW growth so each physical line is counted exactly once — no re-count across polls).
- **`follow`** — streams matching entries indefinitely until stop event; on the first successful stat of an existing file the read offset is seeded to that file's current size (EOF), so only NEW growth after connect is streamed (historical entries are not replayed); handles file truncation/rollover by resetting offset to 0.
- **`sample_schema`** — infers field presence patterns from a sample of entries (standard slots like `timestamp`/`level`/`logger`, conditional slots like `stack_trace`/`tags`, and observed custom `fields` keys).

**File-tail implementation** (`_tail_lines`) — reads the last N lines without loading the entire file, using a loop-growing read window that handles long lines robustly. Starts with an estimate and doubles the window until either N lines are captured or the start of the file is reached. Discards partial leading fragment when seeking from a non-zero offset.

**Lazy-jq rule** — `jq` is imported only when `--match` is used; a missing library surfaces as `ConfigError` (exit 2) pointing at `pip install 'agctl[logs]'`, not a crash. This keeps the zero-dep log query path working without `jq`.

---

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
`pg_description`/`pg_constraint`; and standalone unique indexes from `pg_index`,
appending each to `unique_constraints` in the same shape) and excludes schemas
whose name starts with `pg_` or equals `information_schema`. Third-party drivers
may omit it (non-introspection drivers remain valid for `db query`/`assert`/`execute`).

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

### GrpcClient (`clients/grpc_client.py`)

grpcio wrapper with descriptor resolution (reflection + descriptor fallback),
JSON↔protobuf translation, and four call types. Lazy-import pattern: importing
`agctl.clients.grpc_client` never requires `grpcio`; the constructor imports
on-demand and raises `ConfigError` pointing at `pip install 'agctl[grpc]'`.

**Shared proto kernel** (`clients/grpc_descriptors.py`) — descriptor
resolution, JSON↔protobuf translation, and call-type derivation live in a
standalone module so the gRPC client and the gRPC mock server agree on how
service/method descriptors are resolved and how protobuf messages are
(de)serialized. Public surface: `build_descriptor_pool(sources,
context_label=)`, `find_method(pool, service, method)`, `call_type_of(method_desc)`,
`message_class(message_desc)`, `serialize(message_desc)`, `deserialize(message_desc)`,
plus `add_file_protos_order_tolerant` (multi-pass add that defers a file whose
import is not yet loaded — protoc's FileDescriptorSet order is not
dependency-first). The kernel is **pure Python with no `grpc` import at module
top**; every `google.protobuf.*` / `grpc_tools.protoc` import lives inside the
function that needs it via `_require(module_name)`, which raises `ConfigError`
pointing at `pip install 'agctl[grpc]'` on a missing library. `GrpcClient`
keeps its reflection-resolution path (it talks to a live server) and delegates
descriptor-pool building, JSON↔protobuf, and call-type derivation to the kernel.

**Descriptor resolution** (reflection-first, descriptor fallback):

- **Path 1: Injected pool** — test seam; return directly.
- **Path 2: Reflection** — query `grpc.reflection.v1alpha.ServerReflection` for
  `list_services` → `file_containing_symbol` responses, deserialize
  `FileDescriptorProto` messages into a `DescriptorPool`. Reflection mode is
  controlled per-target: `"auto"` (try reflection, fall back to descriptors),
  `"on"` (require reflection, error if UNIMPLEMENTED), `"off"` (skip reflection).
- **Path 3: Descriptor fallback** — load proto files (compile via `grpc_tools.protoc`)
  or precompiled `.pb` descriptor sets into a `DescriptorPool` (kernel
  `build_descriptor_pool`).

**JSON↔protobuf translation** — kernel `serialize`/`deserialize` wrap
`json_format.ParseDict` / `json_format.MessageToDict`. Request serialization
fails with `ConfigError` on unknown fields (`ignore_unknown_fields=False`);
response deserialization skips unknown fields.

**Call types** — kernel `call_type_of(method_desc)` returns one of four strings:

- `"unary"` — `unary_unary` → single request, single response.
- `"client_stream"` — `stream_unary` → request iterator, single response.
- `"server_stream"` — `unary_stream` → single request, response iterator.
- `"bidi"` — `stream_stream` → request iterator, response iterator.

**Status-as-result semantics** — gRPC status is a result field, not an assertion.
Non-OK `RpcError` (e.g. `NOT_FOUND`, `PERMISSION_DENIED`) is caught and
returned as `GrpcUnaryResult` with `status` (code/name/message) and `message=None`;
the envelope remains `ok:true`. Assertion flags (`--status`, `--contains`,
`--match`, `--jq-path`, `--equals`) are evaluated separately via
`evaluate_grpc_assertions` (reusing `jq_bool`/`json_subset`/`jq_value`/`parse_equals`
primitives) and raise `AssertionFailure` on mismatch. Name/code resolution
everywhere routes through the public `parse_grpc_status(status)` helper in
`assertions.py` — single source of truth for the gRPC status enum (case-sensitive
name lookup, digit-string→int coercion, 0–16 range; no `grpc` import).

**Exception mapping:** `RpcError.DEADLINE_EXCEEDED` → `OperationTimeout`;
other `RpcError` → status-in-result (connection succeeded, call failed); bare
`Exception` → `ConnectionFailure`.

**Healthcheck** — calls `grpc.health.v1.Health/Check` via `HealthStub`.
Returns `GrpcHealthResult` with `status` enum name (`SERVING`/`NOT_SERVING`/`UNKNOWN`)
and optional `note`. `UNIMPLEMENTED` status returns `UNKNOWN` (not an error).

**Lazy-grpcio rule** — grpcio, grpcio-tools, grpcio-health-checking,
grpcio-reflection, and protobuf are imported only when `GrpcClient` is constructed
(or, for the mock server, inside `MockGrpcServer` methods); missing library
surfaces as `ConfigError` (exit 2) pointing at the `grpc` extra.

### MockGrpcServer (`mock/grpc_server.py`)

Validated, lazily-bound gRPC mock server. Same descriptor-driven model as the
gRPC client, but inverted: it *serves* stub responses for `/<service>/<method>`
instead of calling them. Two layers live in one module:

- **Pure dispatch core (grpcio-free at module top):** `build_envelope(service,
  method, metadata, *, message=, messages=)` builds the per-call-type match
  envelope (`{service, method, metadata (lowercased), message}` for unary /
  server_stream / bidi; `{service, method, metadata, messages:[…], count}` for
  client_stream, built at stream close). `dispatch_grpc(stubs, envelope,
  call_type, emit_capture_missing=)` does first-match-wins over the
  insertion-ordered `dict[str, GrpcStub]` (config key → stub), running
  `_match_body` (`json_subset`, skipped for client_stream) AND `_match_jq`
  (`jq_bool` over the envelope), then `resolve_captures` + `render_typed` (the
  exact HTTP/Kafka pipeline reuses `mock/capture.py` and `resolution.render_typed`).
  `GrpcDispatchOutcome` carries `matched`, `stub_name`, `messages` (rendered),
  `status` (`(code, name)` from `parse_grpc_status`), and capture-missing
  emissions. The dispatch core is importable and unit-testable without the
  grpcio extra (the `tests/unit/test_mock_grpc_dispatch.py` AST test pins this).
- **`MockGrpcServer` (lifecycle + grpc.Server):** construction resolves the
  `DescriptorPool` ONCE (via the kernel or an injected pool — the test seam),
  validates every stub's `(service, method)` against the pool (unknown →
  `ConfigError` at `mocks.grpc.stubs.<name>`), validates response-shape-vs-call-type
  (`server_stream` requires `response.messages`; others require `response.message`),
  and precomputes `stubs_by_method` (dict-of-dicts keyed by `(service, method)`)
  and `method_meta` (parallel `{(service, method): (input_desc, output_desc,
  call_type)}`). Construction does NOT bind a port. `_build_server` (lazy
  `import grpc` inside the method) registers one `method_handlers_generic_handler`
  per service with the right `RpcMethodHandler` shape, each wired with the
  kernel's `serialize`/`deserialize` for that method's input/output descriptors
  and a per-call-type `_handle_*` behavior. Health (`grpc_health.v1.health`,
  every configured service + the overall `""` key set to `SERVING`) and
  Reflection (`grpc_reflection.v1alpha.reflection`, passing the resolved pool
  so reflection answers symbol lookups against the fresh — not Default —
  `DescriptorPool`) are auto-served when `health`/`reflection` are `true`.
  `start()` calls `add_insecure_port` (EADDRINUSE → `ConfigError`),
  `serve_forever(stop_event)` polls `wait_for_termination(timeout=0.2)` until
  the engine's stop event fires, `shutdown()` calls `server.stop(grace=2)`.
  `actual_listen()` reports the post-bind address (an ephemeral `:0` request
  surfaces the actually-assigned port, mirroring HTTP bound-address reporting).

**Engine wiring** — `MockEngine` gained a third engine: `run_grpc`, `grpc_listen`,
`grpc_server_factory` (keyword-only, defaulted → backward-compat), and
`top_level_descriptors` (the command layer threads `Config.grpc.descriptors`
through so the server can fall back to it when `mocks.grpc.descriptors` is
`None`). Step 2b constructs + binds the server after the HTTP bind so a grpc
failure tears down the bound HTTP server via the outer try → `shutdown()`. The
default factory lazy-imports `MockGrpcServer` inside the closure body so the
engine module stays grpcio-free at import time. `started` gains a `grpc`
block (`{listen, stubs, services, reflection, health}`); `summary` gains
`grpc_hits`/`grpc_unmatched`/`grpc_errors`. `grpc.unmatched` and `grpc.error`
set the runtime-error flag (fatal: the run exits `1` at shutdown).

**Daemon taxonomy** (`mock/daemon.py`) — `pidfile_path`/`log_path` gained a
keyword-only `engine=None` arg: `"grpc"` keys a gRPC-only daemon under
`mock-grpc-<port>.{pid,log}` so it does not collide with an HTTP daemon on a
different port. `RunningMock` gained `http_listen` and `grpc_listen` fields
(recorded in the pidfile so a multi-engine daemon is addressable by any of its
listen addresses). `resolve_target` matches `--listen` against any of
`listen` / `http_listen` / `grpc_listen`. `EVENT_TO_COUNTER` and
`FATAL_FAILURE_EVENTS` gained `grpc.hit`/`grpc.unmatched`/`grpc.error` (the
last two are fatal at `mock stop`).

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
  compares strictly — a number never equals a string (`0` ≠ `"0"`). For two
  strings that both parse as ISO 8601 datetimes, `type_aware_equal` normalizes
  via UTC so `'...Z'` and `'...+00:00'` for the same instant compare equal.

**Where used:**

- **`db assert --expect-value`** — `coerce_db_value(jq_value(first_row, path))`
  vs `parse_equals(equals)` via `type_aware_equal`.
- **`db assert --expect-rows`** — row-count comparison.
- **`kafka assert`** — `_build_assert_predicate` combines `--contains`
  (`json_subset`, optionally narrowed by `--path`), `--match` (`jq_bool` over
  the message envelope), and `--pattern` (named config pattern, `{param}`-filled,
  then `jq_bool` over the message envelope). **All** active modes must pass.
- **`kafka consume --match`** — a `jq_bool` predicate over the message envelope,
  with short-circuit on `--expect-count`.
- **`http call` / `http request`** — `evaluate_http_assertions` composes
  `jq_bool` (`--match`, over the response envelope), `json_subset` (`--contains`,
  body-rooted), and `jq_value` + `parse_equals` + `type_aware_equal`
  (`--jq-path` + `--equals`, body-rooted); `validate_http_assertion_args` gates
  `--jq-path`/`--equals` pairing and `--contains` JSON shape pre-request
  (`ConfigError` exit 2, before the request is sent). `coerce_db_value` is
  intentionally not reused — HTTP response bodies are already JSON-native.
- **`mock run` capture extraction** — `mock/capture.py::resolve_captures` reuses
  `jq_value(envelope, spec.from_)` to read each explicit `capture.<name>.from`
  off the live message envelope (HTTP request or Kafka message), producing a
  typed `CaptureValue` map consumed by `resolution.render_typed`. So `jq_value`
  now powers mock capture in addition to HTTP response assertions
  (`--jq-path`); a `from` resolving to `null`/missing is the soft-miss path
  (emits `capture.missing`, substitutes empty string), distinct from a missing
  `jq` library which re-raises as `ConfigError` (exit 2).
- **`kafka listen assert`/`results`** — `listen/assert_eval.py::evaluate_expectations` reuses the `kafka assert` predicate composition (`_build_assert_predicate` mode merge: `contains`/`match`/`pattern`/`path`) via `listen/capture_file.py::build_predicate`, scanning the per-topic `<topic>.ndjson` capture to exhaustion with `count_matching`. There is deliberately **no wall-clock deadline** anywhere — the scan is bounded by file size only, so a listener's capture (a finite on-disk artifact) cannot hit a timeout-truncation false negative. Each `ExpectationResult` carries the same self-debugging `messages_scanned` + per-mode `root` payload as `kafka assert`'s no-match detail.

**Dialect `"2"` — five `match` eval sites are envelope-rooted.** Under the v2
dialect (gated by `_check_version`), `jq_bool` feeds the whole envelope — not
the payload — at each of five sites: HTTP stub `match.jq` (request envelope
`{method, path, headers (lowercased), body}`), Kafka reactor `match` and
`kafka assert/consume --match` and `kafka.patterns[].match` (message envelope
`{key, value, partition, offset, timestamp, headers (case-sensitive)}`), and
`http call`/`http request --match` (response envelope `{status_code,
response_time_ms, headers (lowercased), body, url, method}`). So `.body.amount` /
`.value.eventType` / `.status_code` / `.headers.x` reach the right field, and
mock `match.jq` / reactor `match` share their root with `capture.*.from`
(unifying the divergence tracked as #22). Body-rooted `match.body` (json_subset),
`--contains`, `--path`, `--jq-path`/`--equals`, and `--status` are unchanged.

**Self-debugging failure entries** — assertion failures now carry a `root` label
and a payload snapshot so an agent can correct a mis-rooted jq expression without
dropping the flag and re-running raw. The `root` field names the evaluation root
(e.g. `"response envelope"` vs `"response body"`) and differs per mode; the
payload field (`"body"`, `"row"`, `"rows"`, or `"modes"`) carries the actual
data evaluated against. Root-per-mode mapping:

- **HTTP** (`assertions.py::evaluate_http_assertions`):
  - `--match` failures → `"root": "response envelope"` + `"body": <response body snapshot>`
  - `--contains` / `--jq-path` failures → `"root": "response body"` + `"body": <response body snapshot>`
  - The `body` snapshot is size-capped via `_response_body_snapshot` (~4 KB; the
    full body always remains at `detail.response.body`) so a multi-mode failure
    can't duplicate a large body once per entry.
  - `--status` unchanged (no root label)

- **DB** (`db_commands.py::_db_assert_core`):
  - `--expect-value` mismatches → `"root": "first row"` + `"row": <first_row>`
  - `--expect-rows` mismatches → `"rows": <rows[:5]>` (sample; `actual` holds the true count)
  - No-rows case → `"rows": []`

- **Kafka** (`kafka_commands.py::_kafka_assert_core`):
  - No-match failure → `"messages_scanned": <n>` + `"modes": [{"mode","root",...}, ...]`
    listing each active mode with its root (`--match`/`--pattern` → `"message envelope"`;
    `--contains`/`--path` → `"message value"`)

This implements the self-debugging contract (DESIGN §3.1) so agents read the root
and payload from `error.detail.failures[]` (or `error.detail` for db/kafka) and
correct the expression in one shot instead of falling back to a raw inspection
call.

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
| `agctl.logs_backends` | `LogBackend` Protocol (`clients/log_backend_protocol.py`) | `LogClient.load_backends` | a new log source `type` |
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
- **Log backends** — `LogClient` selects by `source["type"]`; unknown →
  `ConfigError`. Built-in `file` always wins over a registration gap.
  The `LogBackend` protocol requires `validate_config`/`scan`/`await_one`/
  `follow`/`sample_schema`. All methods are mandatory. Built-in `file`
  implements the protocol for NDJSON files in logstash format. Third-party
  backends can add journald, syslog, ELasticsearch, etc. Register in another
  package's `pyproject.toml`:
  ```toml
  [project.entry-points."agctl.logs_backends"]
  journald = "agctl_logs_journald:JournaldBackend"
  ```
- **Protocol plugins** — `cli._load_plugins` runs at CLI import, mounting each
  plugin's `command_group` onto the root `cli` group (name from `.name` →
  `command_group.name` → entry-point name). If a plugin exposes
  `validate_config(config_dict)`, it runs during `agctl config validate` and its
  error strings fold into the result under `plugin.<name>` (exit 2). No plugins
  ship today; the group is a clean no-op.
- **Assertion modes** — loaded lazily on first `get_default_registry()` and
  cached. See §9.

> **In-tree gRPC support:** The `grpc` command group is implemented in-tree
> (`commands/grpc_commands.py`, `clients/grpc_client.py`, and the shared
> `clients/grpc_descriptors.py` kernel) and does **not** use the
> `agctl.plugins` entry point. Unlike protocol plugins (which add new
> top-level groups via entry points), gRPC is a core transport and lives
> alongside HTTP/Kafka/DB in the main module tree. The gRPC mock server
> (`mock/grpc_server.py`) is the third mock engine alongside HTTP and Kafka;
> it shares the same descriptor/JSON↔protobuf kernel as the client so the two
> paths agree on service/method resolution. The `grpc` extra
> (`grpcio`, `grpcio-tools`, `grpcio-health-checking`, `grpcio-reflection`,
> `protobuf`, `jq`) follows the same lazy-import pattern.

---

## 11. Dependency & Packaging Model

The as-built dependency split is the biggest divergence from DESIGN §7 (which
proposed one flat list). `pyproject.toml` splits heavy libraries into optional
extras, so a user installs only what they need and the package imports fast:

| Group | Dependencies | Needed for |
|---|---|---|
| core (always) | `click`, `pyyaml`, `pydantic`, `python-dotenv` | CLI, config loading, schema, `.env` parsing |
| `http` | `httpx` | `http *`, `check ready` |
| `jq` | `jq` | HTTP response assertions (`--match`/`--jq-path` on `http call`/`request`), mock HTTP `match.jq` (and mock startup pre-compile of stub `match.jq` / reactor `match`) |
| `kafka` | `confluent-kafka[schemaregistry]`, `jq` | `kafka *` (the `[schemaregistry]` sub-extra pulls `authlib`/`cachetools`/`attrs`/`certifi`/`httpx` that `confluent_kafka.schema_registry` imports at module load; without it `pip install 'agctl[kafka]'` leaves SR import-broken) |
| `db` | `psycopg[binary]`, `jq` | `db *` |
| `logs` | `jq` | `logs *` (`--match` on logs query/assert/tail) |
| `grpc` | `grpcio`, `grpcio-tools`, `grpcio-health-checking`, `grpcio-reflection`, `protobuf`, `jq` | `grpc *`, `mock run --only grpc` / `mocks.grpc` engine |
| `avro` | `fastavro` | Avro decode/encode on `kafka.clusters.<c>.value_format: avro` topics (`agctl/serialization/avro_codec.py`) |
| `protobuf` | `protobuf` | Protobuf decode/encode on `kafka.clusters.<c>.value_format: protobuf` topics (`agctl/serialization/protobuf_codec.py`); deliberately standalone — does NOT pull `grpcio` |
| `schema-registry` | meta-extra `agctl[avro,protobuf]` | convenience: one pip install for both SR codecs |
| `dev` | `pytest` | unit tests |
| `integration` | `testcontainers`, `agctl[db,kafka,http,grpc]`, `agctl[schema-registry]`, `pytest` | live integration tests (the SR codecs are needed for the Avro/Protobuf round-trip suite) |

`jq` is bundled under `kafka`/`db`/`logs` (which always needed it) **and** exposed as a
dedicated `jq` extra for HTTP-only users (response assertions) and HTTP-only-mock
users (`match.jq`) — `pip install 'agctl[jq]'`. A mock with no `match.jq` and no
reactor `match` imports nothing, preserving the zero-dep HTTP-only mock. The
gRPC mock needs the `grpc` extra regardless of whether `match.jq` is set
(descriptor-driven encoding requires `grpcio`/`protobuf`); HTTP-only and Kafka-only
mocks import no server-side gRPC code. At runtime the lazy-import convention (§8)
keeps the error category correct: a missing library → `ConfigError` (exit 2)
pointing at the right extra (`agctl[jq]` for http/mock, `agctl[grpc]` for the
gRPC mock and `grpc *`, `agctl[logs]` for logs commands with `--match`), not an
opaque `ModuleNotFoundError`. The Avro/Protobuf codecs (§3 `agctl/serialization/`)
add three more: a missing `fastavro` → `ConfigError` pointing at `agctl[avro]`;
a missing `protobuf` → `ConfigError` pointing at `agctl[protobuf]`; and a missing
`confluent_kafka.schema_registry` (or one of its transitive deps, e.g. `authlib`)
→ `ConfigError` pointing at `agctl[kafka]` whose message echoes the underlying
import error text (the `[schemaregistry]` sub-extra pins those transitive deps
so the common case is fixed by `pip install 'agctl[kafka]'`). The codecs and the
SR client lazy-import inside the function that needs them, so importing
`agctl.serialization.*` is extra-free.

**Build & entry points:** hatchling backend, wheel target `agctl`; console
scripts `agctl`/`agt` → `agctl.cli:cli`; entry-point groups `agctl.db_drivers`
(registers built-in `postgresql`), `agctl.logs_backends` (registers built-in `file`),
`agctl.plugins`, `agctl.assertions` (§10); requires Python `>=3.11`.

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
- `MockEngine` — `emit_fn` injects a fake writer; `run_id` is configurable;
  `grpc_server_factory` injects a fake gRPC server (so the engine test suite
  covers `run_grpc` lifecycle without grpcio installed).
- `MockGrpcServer` — `descriptor_pool` constructor arg injects a prebuilt pool
  (skips the slow protoc compile and isolates construction from the kernel's
  source-resolution paths). The pure dispatch core
  (`build_envelope`/`dispatch_grpc`/`GrpcDispatchOutcome`) is tested directly
  without grpcio; an AST-parse test pins "no `import grpc` at module top" so
  the dispatch brain stays grpcio-free.
- `clients/grpc_descriptors.py` — pure functions over `DescriptorPool`; unit
  tests cover `build_descriptor_pool`/`find_method`/`call_type_of`/
  `serialize`/`deserialize` in isolation.
- `mock/routing.py` — pure functions, tested directly.
- `sys.setswitchinterval` single-writer test technique — forces thread switching to verify NDJSON emission doesn't interleave (tests the `threading.Lock` in `MockEngine.emit_event`).
- `cli._entry_points` and `assertion_registry._entry_points` are
  monkeypatchable.

Because clients lazy-import their libs, unit tests run **without** `httpx`,
`confluent_kafka`, or `psycopg` installed; the gRPC mock dispatch suite runs
without `grpcio` installed (only the integration test that actually serves a
real `grpc.Server` needs the `grpc` extra).

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
- `tests/integration/test_mock_grpc.py` — exercises the gRPC mock end-to-end across all four call types (unary, server-stream, client-stream, bidi) plus Health and Reflection against a real `grpc.Server`. Self-skips when the `grpc` extra is absent.
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
  seeks to the earliest offset.
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
| **Mocking non-goal reversed** | DESIGN §1 stated "No built-in mock server." The `agctl mock` command (HTTP stub server + Kafka reactors + gRPC mock server) reverses this non-goal — local testing is now self-contained. DESIGN §1 and §3.6 document the command; the gRPC engine is the third alongside HTTP and Kafka. ||
| Dependencies | DESIGN §7 updated to the optional-extras model (`http`/`kafka`/`db`/…). |
| Project structure | DESIGN §7 tree updated to the real layout (incl. `config/models.py`, top-level `command.py`/`errors.py`/…, `clients/db_drivers/`). |
| One-emit enforcement | DESIGN §8 documents the `@envelope` mechanism. |
| Env-override matching | DESIGN §5/§8 document case-/hyphen-insensitive matching + the ≥2-segment rule. |
| `commands/config_commands.py` | Extracted from `cli.py` — every command group now has its own module. |
| Assertion base | Already consistent; DESIGN §9.3 clarifies that built-in modes are registered by name but implemented in the command layer. |
| **gRPC proto kernel extraction** | `clients/grpc_descriptors.py` is the single source of truth for descriptor resolution + JSON↔protobuf shared by `GrpcClient` (caller path) and `MockGrpcServer` (server path). `assertions.parse_grpc_status` is the single source of truth for name/code coercion, reused by the client, the config model (`GrpcResponse.status` validation), the dispatch core, and the CLI flag pre-check. The module map (§3) records both; DESIGN does not mention the kernel (its altitude is the user-facing contract). |

---

## 15. Known Limitations

What the system does **not** do today (as-built; see DESIGN §10 for the roadmap):

- **Bounded statelessness carve-out — mock daemon state.** The managed daemon commands (`mock start`/`stop`/`status`) introduce on-disk state in the system: a pidfile (`mock-<port>.pid`) and NDJSON log (`mock-<port>.log`) under `<state-dir>/` (default `./.agctl/`). This is a deliberate, scoped exception to the stateless-invocation principle, confined to the daemon lifecycle. No other commands read or write cross-invocation state.
- **Bounded statelessness carve-out — listen daemon state (second carve-out).** The `kafka listen` managed daemon (`start`/`stop`/`status`/`assert`/`results`/`messages`) is the second on-disk-state surface: a run-id-keyed pidfile (`listen-<run_id>.pid`) plus a run dir (`listen-<run_id>/`) holding `meta.json`, `asserts.jsonl` (attached expectations), per-topic `<topic>.ndjson` capture files, and `events.log`, all under `<state-dir>/`. Same scope discipline as mock — confined to the daemon lifecycle; the generic primitives (`spawn_daemon`/`terminate`/`require_posix_daemon`/`is_alive`/pidfile ops) are shared with mock via `agctl/daemon.py` (no `listen → mock` coupling). `stop` deletes the run dir + pidfile on every path (fatal or clean), so an uncollected expectation (`results` not run first) is silently dropped.
- **No Schema Registry decoding for non-Confluent / JSON-Schema registries** —
  the codec pipeline targets Confluent Schema Registry wire framing (magic
  byte + 4-byte schema id) and supports `AVRO` and `PROTOBUF` schema types.
  Non-Confluent registries and Confluent `JSON` schemas are deferred (T15+).
- **No retry/polling DSL** — eventually-consistent assertions need a caller-side
  loop (e.g. shell around `db assert`).
- **No multi-step scenario primitive** — an agent chains commands in a shell.
- **SQL param rewriting doesn't parse string literals** — a `:name` inside a
  literal may be rewritten; `::` casts are protected.
- **Unsupplied `{placeholder}` values stay literal** — no call-time validation
  (deferred).
- **No MCP wrapper, no OpenTelemetry propagation, no parallel runner, no secret
  backends** — deferred per DESIGN §10.
- **Native-Windows managed-daemon gate** — the managed-daemon `_core`s
  (`_mock_start_core`/`_mock_stop_core`/`_mock_status_core` in
  `commands/mock_commands.py`, `_kafka_listen_start_core`/`_stop_core`/
  `_status_core` in `commands/kafka_listen_commands.py`) call
  `require_posix_daemon()` (from `agctl/daemon.py`), which raises
  `ConfigError` (exit 2) when `os.name == "nt"`; WSL reports `"posix"`, so it
  passes through ungated. `mock run` and `kafka listen run` (foreground
  streaming) and every other command group run natively on Windows. Streaming
  graceful-stop contract: backgrounded streamers (`http ping`, `mock run`,
  `logs tail`, `grpc` server-stream/bidi, `kafka listen run`) install
  `SIGTERM`/`SIGINT` handlers; on native Windows only the `SIGINT`/Ctrl+C path
  reaches the handler (a `SIGTERM` via `os.kill` hard-terminates). The
  `SIGTERM`-driven graceful-stop (and the daemon's `SIGTERM`-based shutdown
  that `mock stop`/`kafka listen stop` drive) is POSIX/WSL — the reason both
  daemons are gated there.

**Mock server MVP limitations** (see DESIGN §10 "Known-wrong-result / Not Covered" for the full list with failure-mode analysis):

- **Stateful flows** (OAuth/token exchange, create-then-GET, idempotency-key replay, pagination) — static engine returns same canned response → false green.
- **TLS / HTTPS-pinned or `https://`-hardcoded SUT clients** — cannot intercept HTTPS → integration untested → false green. The gRPC mock (`mocks.grpc`) is plaintext-only v1 — TLS-pinned gRPC SUT clients cannot connect either.
- **Cross-transport sagas** (Kafka trigger → HTTP callback) — no causal linkage → false green. gRPC stub → Kafka reaction (and vice versa) is deferred alongside this.
- **Single-file Protobuf schemas only (Confluent SR)** — the Protobuf codec (`agctl/serialization/protobuf_codec.py`) compiles the registry-returned `.proto` source string with `grpc_tools.protoc --include_imports` and resolves the LAST top-level message. Multi-file schemas with `import` statements are best-effort via the kernel's order-tolerant loader; if resolution fails, the codec raises `SerializationError` with a truncated `schema_snippet` (fail-loud, not silent). Avro decode/encode is unconstrained. JSON-Schema values are not yet decoded (deferred alongside non-Confluent registries; emitted as `decode.error`).
- **`match` is envelope-rooted under dialect `"2"`+ (was: payload-rooted under `"1"`; unchanged by the v3 named-cluster schema lift). The five `match` eval sites (HTTP stub `match.jq`, Kafka reactor `match`, `kafka.patterns[].match`, `kafka assert/consume --match`, `http call`/`request --match`) feed the whole envelope; `capture.*.from` shares the same root. The gRPC mock adds a sixth site (gRPC stub `match.jq`) that feeds the per-call-type envelope from §8.1 — same rooting rule, same `capture.from` root. `match.body` / `--contains` / `--path` / `--jq-path`+`--equals` / `--status` remain payload-rooted. A v1/v2 config is rejected by `_check_version`; rewrite with `agctl config migrate`.
- **Containerized SUT topology** — operator must target `host.docker.internal` / host LAN IP and avoid SUT that swallows connection errors → false green.
- **Shared broker + pinned `consumer_group` reused across runs/devs** — partition split or resume-past-messages → silently missing/old reactions → false green (mitigated by unique-per-run default).

**gRPC mock MVP limitations** (v1 boundaries of the third mock engine):

- **Plaintext only** — `mocks.grpc` serves over h2c; no TLS on the mock listener. TLS-pinned SUT clients cannot connect.
- **Bidi is request/response pairing** — one rendered response per matched incoming request, no conversation state and no server-push.
- **Client-stream aggregates at close** — the `messages:[…]` envelope is matched once at request-stream close; per-message responding is not modeled.
- **No mid-stream abort** — server-stream stubs stream their authored `messages` to completion or a terminal `status`; `RSTSTREAM` mid-stream is not modeled.
- **Descriptors required** — `mocks.grpc` needs `proto`/`descriptor_set` sources to resolve service/method and encode responses. Server reflection is *served* (when `reflection: true`) but cannot *bootstrap* the mock itself; a config that expects reflection-only bootstrapping fails loud at server construction (exit 2).
- **gRPC `SO_REUSEPORT` port-collision blind spot** — grpcio enables `SO_REUSEPORT`, so two gRPC servers can silently bind the same port. The port-in-use guard fires only when a non-grpc process holds the port. Pick unique ports across runs to avoid the SUT silently reaching a stale mock.
