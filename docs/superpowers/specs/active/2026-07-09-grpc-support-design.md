# Design: `agctl grpc` — gRPC Support as a First-Class Transport

**Status:** Draft v1 — design approved in brainstorming session; pending spec review
**Date:** 2026-07-09
**Author:** brainstorming session
**Affects:** new `agctl/clients/grpc_client.py`, new `agctl/commands/grpc_commands.py`, `agctl/config/models.py` (new `GrpcConfig` / `GrpcTarget` / `GrpcDescriptorSource` / `GrpcTls` / `GrpcTemplate`), `agctl/commands/discover_commands.py` (new `grpc-services` / `grpc-methods` categories), `agctl/cli.py` (new `grpc` group), `agctl/assertions.py` (reuse only), `pyproject.toml` (new `grpc` extra; `grpc` added to the `integration` extra), `skills/agctl*`, DESIGN.md, ARCHITECTURE.md
**Relation to docs:** Additive — no existing flag, field, exit code, or output shape changes. On implementation, DESIGN.md gains a `grpc` command group + `grpc:` config section + `grpc` extra, the gRPC row is **struck from §10 future work**, and ARCHITECTURE.md gains the `grpc` client/command module, descriptor-resolution + JSON↔protobuf flow, and the streaming-input exception. Synced via `docs-watcher`. No `version` bump (additive under major `2`).

---

## 1. Background & Problem

`agctl` drives four transports today — HTTP, Kafka, DB, Logs — each a thin client behind a
one-JSON-envelope command group, with heavy libraries as optional extras. gRPC is the
conspicuous gap: it is listed explicitly in **DESIGN §10** (*"First-class `agctl grpc call`
command via a plugin. The plugin protocol is designed; implementation is deferred."*), and
**§9.3's `Plugin` Protocol** names gRPC/GraphQL/WebSocket as its targets.

gRPC differs from the existing transports in three load-bearing ways, each of which shapes
this design:

1. **It needs a schema to serialize.** Protobuf is binary; an agent speaks JSON. `agctl`
   must learn the service/message/method definitions — from **server reflection** (zero
   config, dynamic) or from **supplied descriptors** (`.proto` / compiled
   `FileDescriptorSet`) — and convert JSON↔protobuf on every call.
2. **It has four call types**, not one: unary, server-streaming, client-streaming,
   bidi-streaming. The one-envelope-per-invocation model has only three streaming
   exceptions today (`http ping`, `mock run`, `logs tail`) — all *output* streams.
   Client-streaming/bidi additionally need a **request-stream input**, which a stateless
   CLI can only take from stdin.
3. **Its status model is not HTTP's.** gRPC has its own status codes (0–16) and
   metadata/trailers. A non-OK status must be a *result* (`ok:true`, exit 0) unless an
   assertion says otherwise — the exact principle already established for HTTP 4xx/5xx.

The design delivers gRPC as a **fifth in-tree transport**, structurally identical to
http/kafka/db/logs: a lazy-imported client under a new `grpc` extra, a `grpc:` config
section, a `grpc` command group, assertion reuse, and discovery integration. gRPC is
**not** delivered as a third-party plugin package (D1) — the `Plugin` Protocol of §9.3
remains the seam for *future* protocols; gRPC ships in-tree because every existing
transport does and the feature branch lives in this repository.

> **Scope honesty (load-bearing).** v1 covers **all four call types** and
> **reflection-first with descriptor fallback** (both chosen in brainstorming). The
> descriptor strategy (§9) is the gRPC-specific core: it is the only thing that makes
> "an agent writes JSON, protobuf goes on the wire" possible without per-service codegen.

## 2. Goals

- Let an agent **call any gRPC method** (template or free-form) and assert on the response
  with the same exit-code discipline as `http call` / `db assert`, including non-OK status
  as a result, not an error.
- Support **all four call types**, using **NDJSON on stdout** for server-streaming/bidi
  output (the established streaming-exception model) and **NDJSON on stdin** for
  client-streaming/bidi request input — closing a bidirectional symmetry.
- Resolve proto definitions **reflection-first, descriptor-fallback** so a dev server with
  reflection needs zero config, while a locked-down server with reflection disabled works
  from supplied `.proto` files or a compiled descriptor set.
- Serialize **JSON↔protobuf** transparently: the agent writes/reads JSON; `agctl` builds
  message classes from descriptors (no generated `_pb2` modules required).
- **Reuse** the existing jq assertion engine (`jq_bool` / `compile_jq` / `json_subset` /
  `parse_equals` / `type_aware_equal`), the `@envelope` discipline, and the streaming
  precedents. Add no new assertion engine and no new error types.
- **Fail loud on authoring bugs, soft on data variance** — a malformed `--match` is a
  `ConfigError` (exit 2) via up-front `compile_jq`; a reflection miss with no fallback
  descriptors is a `ConfigError`; a data-dependent jq eval error is a soft non-match.
- Preserve every existing behavior: no command, flag, field, exit code, or output shape
  changes elsewhere.

## 3. Scope & Design Constraints

- **Additive only.** A new `grpc` command group, a new `grpc:` config section, two new
  discover categories, and a new extra. Nothing existing changes.
- **In-tree transport, not a plugin.** Ships in the core package under the `grpc` extra,
  like http/kafka/db/logs. No new entry-point group and no new `Protocol` (D1, D13).
- **Reflection-first.** When `reflection: auto` (default) or `on`, `GrpcClient` queries the
  server's reflection service for services + file descriptor protos and builds a
  `descriptor_pool`. When `off`, or when reflection is unavailable (`UNIMPLEMENTED`), it
  falls back to config-supplied descriptors. A reflection failure with **no** fallback
  configured is a `ConfigError` (exit 2).
- **JSON in, JSON out for messages.** Request messages are authored as JSON (`--message`
  or template `message`); `ParseDict` serializes them to protobuf via the resolved
  descriptor. Responses are decoded via `MessageToDict`. The agent never touches bytes.
- **Call type is auto-detected from the descriptor.** `MethodDescriptor` carries
  `client_streaming` / `server_streaming` flags; unary = neither, server-stream =
  server-streaming, client-stream = client-streaming, bidi = both. No `--streaming` flag.
- **Streaming mirrors the established exceptions.** Server-streaming/bidi emit one JSON
  object per response message (NDJSON) + a final `summary`; signal-driven stop via a
  `threading.Event`; manual startup-error envelopes for pre-stream failures. Client-stream
  is the one-result case (a single trailing response → a normal envelope).
- **Request-stream input is stdin NDJSON** for client-streaming/bidi (D4): one JSON object
  per line, EOF ends the stream. `--message`/template `message` is only valid for the
  single-request types (unary, server-stream); providing the wrong input for a call type is
  a `ConfigError` (exit 2).
- **Reuse the lazy jq import.** jq is loaded only when `--match` / `--jq-path` is used; a
  missing library surfaces as `ConfigError` (exit 2) with an install hint. The `grpc` extra
  bundles `jq` (like `db` / `kafka`).
- **`@envelope` discipline preserved** for `grpc call` unary + client-stream + `grpc
  healthcheck`. Server-streaming/bidi are the streaming exceptions.

## 4. Non-Goals

- **A gRPC mock server** (a `mocks.grpc` analog to `mocks.http`/`mocks.kafka`). The mock
  engine is HTTP+Kafka today; a gRPC mock is a separate, larger effort and is deferred
  (§15).
- **A third-party plugin package (`agctl-grpc`).** gRPC ships in-tree (D1). The `Plugin`
  Protocol (§9.3) is the seam if a *different* protocol later wants out-of-tree delivery.
- **Schema Registry / Protobuf-on-Kafka decoding.** Unrelated to the gRPC transport; that
  is the separate Kafka §10 item.
- **Cross-message ordering assertions** on streams ("messages arrive in this order") in v1.
  Per-message `--match` + `--expect-count` cover v1; ordering is deferred.
- **gRPC compression, retry policy / service-config, interceptors, OpenTelemetry
  propagation, OAuth call-credentials beyond static metadata.** Deferred (§15).
- **Reflection protocol v1 vs v1alpha selection as a config knob.** v1 prefers the modern
  reflection API and falls back as needed; the selection is not user-facing in v1.
- **Named gRPC "patterns"** (a `grpc.patterns` analog to `kafka.patterns`). Templates
  (`grpc.templates`) cover the named-call case; a patterns layer is YAGNI.
- **`--capture` response extraction.** Agents still hand-roll `jq -r` for the next
  command; deferred (same call as the http-jq / logs specs).
- **`--match-all` / regex status matching.** Deferred.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **In-tree first-class `grpc` group under a new `grpc` extra**, mirroring http/kafka/db/logs. Not a plugin package. | The feature branch lives in this repo and every sibling transport is in-tree. DESIGN §10's "via a plugin" describes the *extension mechanism* (§9.3 `Plugin` Protocol), not a delivery mandate; that Protocol remains the seam for *future* protocols. In-tree keeps gCTL's transport symmetry and lets discovery/config/assertions integrate like the others. |
| D2 | **Reflection-first descriptor resolution with descriptor fallback** (`reflection: auto` default). | Zero config for dev servers that enable reflection (the common case); descriptors cover locked-down prod servers that disable it. User-chosen in brainstorming. A reflection miss with no fallback → loud `ConfigError`, never a silent failure. |
| D3 | **JSON in / JSON out for messages; `agctl` serializes via descriptors.** Request = `ParseDict`, response = `MessageToDict`, message classes from `message_factory.GetMessageClass` over the resolved pool. | The agent speaks JSON; protobuf is binary. Forcing the agent to emit bytes is a non-starter. Descriptors are sufficient — no generated `_pb2` modules required, so any service in the descriptors is callable without codegen. |
| D4 | **All four call types; client-stream/bidi consume NDJSON on stdin, server-stream/bidi emit NDJSON on stdout.** Call type is auto-detected from the method descriptor. | User-chosen scope. The established streaming exceptions are all *output* NDJSON; stdin NDJSON closes the symmetry and is the only unbounded, quote-free way for a stateless CLI to supply a request stream. Client-stream is the one-result case (single trailing response → normal envelope). |
| D5 | **Method addressing is `package.Service/Method`** (the `grpcurl` convention), with template mode (`grpc call <name>`) and free-form mode (`--target/--service/--method`), plus `--address host:port` for no-config calls (the gRPC analog of `http request --url`). | Mirrors the `http call`/`http request` split exactly. `--address` removes the need to register a target for one-off endpoints (external APIs, ephemeral ports). |
| D6 | **A non-OK gRPC status is a result, not an error.** `ok:true`, exit `0`, status captured in `result.status` (`{code, name, message}`) + trailers — unless an assertion flag is active. | Direct mirror of the HTTP principle "4xx/5xx is a result, not an assertion" (ARCHITECTURE §8). gRPC status is an outcome the agent asserts on, not a tool failure. Only `--status`/`--contains`/`--match`/`--jq-path` mismatch → `AssertionFailure` (exit 1). |
| D7 | **Unary assertions reuse the existing suite unchanged.** `--status <gRPC code name or number>`, `--contains` (`json_subset`, message-rooted), `--match` (jq over the **response envelope** `{status, message, initial_metadata, trailers}`, dialect-"2"-style), `--jq-path`+`--equals` (message-rooted, `parse_equals`+`type_aware_equal`). Streaming uses `--match` (per-message, message-rooted) + `--expect-count`, like `kafka consume`. | No new assertion engine. `--match` envelope-rooted matches HTTP's dialect-"2" root choice, so `.message.x` / `.status.name` read naturally. Up-front `compile_jq` makes a typo a loud `ConfigError` (the http-jq D5 / logs D7 precedent). |
| D8 | **gRPC metadata via `--metadata key=value` (repeatable, caller wins) and template `metadata`; initial metadata sent, **trailers** captured in the result.** | The gRPC analog of `--header`. Per-call merge mirrors `http` header merge (case rules are gRPC's: metadata keys are lowercase on the wire). |
| D9 | **TLS/plaintext per target: `use_tls` (default `false` → plaintext h2c) + optional `tls` block** (`ca_location`/`certificate_location`/`key_location`/`override_authority`) mirroring `kafka.ssl` semantics, including the empty-string-counts-as-unset rule. | Plaintext is the dev norm for gRPC; TLS must be opt-in and explicit (never a silent downgrade). Mirroring `kafka.ssl` reuses a reviewed, unit-tested contract. |
| D10 | **Descriptor-driven dynamic stubs; no generated `_pb2`.** Reflection stubs ship with `grpcio`; `.proto` fallback compiles via `grpc_tools.protoc` into a `FileDescriptorSet`; a pre-compiled `descriptor_set` blob is also accepted. All feed one `descriptor_pool`. | One code path for reflection and descriptors. Codegen coupling (generated modules on the import path) is rejected (§16) — it pins `agctl` to a specific `protoc` run and breaks the "any service in the descriptors" property. |
| D11 | **New `grpc` extra bundles `grpcio`, `grpcio-tools`, `protobuf`, `jq`.** `grpcio`/`grpc_tools`/`google.protobuf` lazy-imported inside `GrpcClient`; missing → `ConfigError` pointing at `pip install 'agctl[grpc]'`. jq lazy-imported only for `--match`/`--jq-path` (like db/kafka). | Mirrors the optional-extras model (ARCHITECTURE §11). `grpcio-tools` is needed only for the `.proto` fallback compile, but bundling it keeps the extra self-contained; the lazy import means a reflection-only call still loads fast. |
| D12 | **Optional `grpc healthcheck` via `grpc.health.v1`.** `--target`/`--service`/`--all`; the gRPC analog of `check ready`. | Standard gRPC health-checking is ubiquitous and gives agents a readiness primitive for gRPC services that `check ready` (HTTP-based) cannot reach. Small, additive, easy to cut. |
| D13 | **No new entry-point group and no new `Protocol`.** gRPC is a single transport, not a backend family. | Unlike `db_drivers`/`logs_backends` (which select among backends by `type`), gRPC has one wire protocol. A `grpc_backends` seam would be speculative; if a *different* protocol wants out-of-tree delivery later, §9.3 `Plugin` is the seam, not a new group. (See §16.) |

## 6. Command Contract — `grpc call` / `grpc healthcheck`

A new `grpc` Click group is wired in `cli.py` (group + `add_command` per the existing
two-step registration). Subcommands follow the core/envelope split
(`_x_core` → `_x_envelope = envelope("grpc.x")(...)`) except server-streaming/bidi
`call`, which are streaming exceptions. Commands obtain their client via the
`new_grpc_client(target)` factory (the test seam).

### 6.1 `agctl grpc call` — template and free-form

Two mutually-exclusive invocation modes, mirroring `http call` / `http request`:

```
agctl grpc call <template-name>            # template mode
    [--param key=value]      # repeatable; fills {placeholders} in message + metadata
    [--message '{...}']      # deep-merged into the template message (analog of http --body)
    [--metadata key=value]   # repeatable; merged with template metadata, caller wins
    [--timeout <seconds>]

agctl grpc call                            # free-form mode
    --target <name>         # a grpc.targets key; OR --address for no-config
    [--address host:port]   # full target; no registration needed (analog of http --url)
    --service <fq-service>  # e.g. order.v1.OrderService
    --method <method>       # e.g. CreateOrder
    [--message '{...}']     # required for single-request types unless the method has an empty request
    [--metadata key=value]  # repeatable
    [--timeout <seconds>]

# unary response assertions (single-request types only; ≥1 flag ⇒ assertion mode; AND together):
    [--status <code>]       # gRPC status: name (OK|NOT_FOUND|PERMISSION_DENIED|...) or number (0..16)
    [--contains '{...}']    # JSON subset of the response message
    [--match <jq>]          # jq predicate over the response ENVELOPE
                             #   {status:{code,name,message}, message, initial_metadata, trailers}
                             #   → .message.x, .status.name, .trailers.x
    [--jq-path <jq>]        # jq path into the response message (paired with --equals)
    [--equals <value>]      # expected value (JSON-parsed when valid; strict, type-aware compare)

# streaming (server-stream / bidi) per-message controls:
    [--match <jq>]           # per-message jq predicate over the response MESSAGE (message-rooted)
    [--expect-count <n>]     # exit 1 (AssertionFailure) if fewer than n messages in the stream
```

**Mode/exclusivity rules (all `ConfigError`, exit 2, pre-call):**

- `<template-name>` is mutually exclusive with `--target`/`--address`/`--service`/`--method`.
- `--target` and `--address` are mutually exclusive; exactly one target source is required
  in free-form mode.
- `--service` and `--method` are required in free-form mode (inferred from the template in
  template mode).
- Unary assertion flags (`--status`/`--contains`/`--jq-path`/`--equals`) are rejected on
  streaming call types (server-stream/bidi) — those use `--match`/`--expect-count`. `--match`
  is valid on both, but its root differs (envelope on unary, message on stream).

### 6.2 Call-type behavior (auto-detected from the descriptor)

| Call type | Request input | Output | Command tag |
|---|---|---|---|
| **unary** | `--message` / template `message` | one envelope (`grpc.call`) | `grpc.call` |
| **server-streaming** | `--message` / template `message` | NDJSON: one `event:"message"` per response + `summary` | `grpc.call` (streaming) |
| **client-streaming** | **stdin NDJSON** (one request message per line; EOF ends) | one envelope carrying the single trailing response | `grpc.call` |
| **bidi-streaming** | **stdin NDJSON** | NDJSON: one `event:"message"` per response + `summary` | `grpc.call` (streaming) |

- `--message`/template `message` is **invalid** for client-streaming/bidi (requests come
  from stdin); stdin NDJSON is **invalid** for unary/server-streaming (single request from
  `--message`). Mismatch → `ConfigError` (exit 2).
- For bidi, stdin-EOF closes the request half; the call drains remaining responses, then
  emits the `summary` and exits.
- `--timeout` sets the gRPC call **deadline** (covers the whole call, including stream
  drain). Deadline exceeded → `OperationTimeout` (exit 1).

### 6.3 `agctl grpc healthcheck` — readiness via `grpc.health.v1`

```
agctl grpc healthcheck
    [--target <name>]        # a grpc.targets key (omit with --all)
    [--service <fq-service>] # health is per-service in v1; omit → overall ("" service)
    [--all]                  # check every configured target (default when neither --target nor --all)
    [--timeout <seconds>]
```

Result (`grpc.healthcheck`): per-target `{target, address, status}` where `status` is one of
`SERVING` / `NOT_SERVING` / `UNKNOWN` (the health-v1 enum), plus `all_serving: bool`. A
target whose health service is `UNIMPLEMENTED` reports `status: "UNKNOWN"` and a note, not a
hard error (mirrors `check ready` not-erroring on a missing health path).

## 7. Config Schema — `grpc`

New pydantic models in `agctl/config/models.py`; a new `grpc` field on `Config`. `${ENV}`
interpolation, overlays, and `AGCTL_GRPC__*` env overrides apply automatically via the
existing pipeline (no new loader code). The resolver denylist (§5 of DESIGN) is unaffected.

### 7.1 Config contract (YAML)

```yaml
grpc:
  targets:                            # named gRPC endpoints (analog of `services`)
    order-service:
      address: "${ORDER_GRPC_ADDR}"   # host:port, e.g. localhost:50051
      use_tls: false                  # plaintext h2c (default); true → TLS
      tls:                            # optional; mirrors kafka.ssl (empty string = unset)
        ca_location: "${GRPC_CA:-}"
        certificate_location: "${GRPC_CERT:-}"
        key_location: "${GRPC_KEY:-}"
        override_authority: "${GRPC_AUTHORITY:-}"
      reflection: auto                # auto (default) | on | off
    payment-service:
      address: "${PAYMENT_GRPC_ADDR}"
      use_tls: true
      reflection: auto

  descriptors:                        # FALLBACK when reflection is off / unavailable
    - proto: "protos/order/v1/*.proto"        # glob; compiled at load via grpc_tools.protoc
      include_paths: ["protos"]
    # OR a pre-compiled descriptor set (no protoc at load):
    # - descriptor_set: "protos/compiled/order.pb"

  templates:                          # named gRPC calls (analog of `templates`)
    create-order:
      description: "Create an order via gRPC"
      target: order-service           # must match a grpc.targets key
      service: order.v1.OrderService  # fully-qualified proto service
      method: CreateOrder
      metadata:                       # initial metadata (analog of `headers`)
        authorization: "Bearer ${ORDER_TOKEN}"
      message:                        # request message as JSON (proto-typed via descriptor)
        customer_id: "{customer_id}"
        items:
          - sku: "{sku}"
            quantity: 1
```

### 7.2 Pydantic model shape

| Model | Fields |
|---|---|
| `GrpcTls` | `ca_location: str \| None = None`, `certificate_location: str \| None = None`, `key_location: str \| None = None`, `override_authority: str \| None = None` |
| `GrpcTarget` | `address: str`, `use_tls: bool = False`, `tls: GrpcTls \| None = None`, `reflection: Literal["auto","on","off"] = "auto"` |
| `GrpcDescriptorSource` | exactly one of `proto: str \| None` (+ `include_paths: list[str] = []`) **or** `descriptor_set: str \| None` |
| `GrpcTemplate` | `description: str \| None`, `target: str`, `service: str`, `method: str`, `metadata: dict[str,str] = {}`, `message: dict \| None = None` |
| `GrpcConfig` | `targets: dict[str, GrpcTarget] = {}`, `descriptors: list[GrpcDescriptorSource] = []`, `templates: dict[str, GrpcTemplate] = {}` |
| `Config` | gains `grpc: GrpcConfig = Field(default_factory=GrpcConfig)` |

`GrpcDescriptorSource` uses a pydantic **model_validator** to enforce "exactly one of
`proto` / `descriptor_set`" (a structurally-bad source is a `ConfigError` at load).

## 8. Client Contract — `GrpcClient`

The client owns descriptor resolution + JSON↔protobuf + the four call types. Method
**signatures and DTOs only** here — no bodies (implementation is the plan's job).

### 8.1 `GrpcClient` (`agctl/clients/grpc_client.py`)

| Method / member | Purpose |
|---|---|
| `__init__(self, target: GrpcTarget, *, channel=None, descriptor_pool=None)` | lazy-imports `grpcio`; constructs a channel (or accepts an injected `channel` test seam). Holds the resolved `descriptor_pool`. |
| `resolve_descriptors(self) -> DescriptorPool` | reflection (if enabled/available) → pool; else config descriptors → pool. Reflection `UNIMPLEMENTED` + no descriptors → `ConfigError`. (D2, D10) |
| `find_method(self, service: str, method: str) -> MethodDescriptor` | locate the method; unknown service/method → `TemplateNotFound`. Exposes `client_streaming`/`server_streaming` for call-type detection. |
| `call_unary(self, service, method, message_json, *, metadata, timeout) -> GrpcUnaryResult` | single request → single response; status/trailers captured. (D3, D6) |
| `call_server_stream(self, service, method, message_json, *, metadata, timeout) -> Iterator[GrpcStreamMessage]` | single request → stream of responses. |
| `call_client_stream(self, request_json_iter, *, service, method, metadata, timeout) -> GrpcUnaryResult` | stream of requests (from stdin) → single trailing response. |
| `call_bidi(self, request_json_iter, *, service, method, metadata, timeout) -> Iterator[GrpcStreamMessage]` | stream of requests → stream of responses. |
| `healthcheck(self, service_name: str = "") -> GrpcHealthResult` | `grpc.health.v1.Health/Check`. (D12) |

The heavy libraries (`grpc`, `grpc_tools`, `google.protobuf`) are imported inside
`__init__`/methods — importing the module never requires the `grpc` extra (the lazy-import
convention, ARCHITECTURE §8).

### 8.2 DTOs (contracts)

**`GrpcStatus`** — `{code: int, name: str, message: str}`. `name` is the canonical gRPC
status string (`OK`, `NOT_FOUND`, …); `code` is the numeric status. Populated for **every**
call outcome, OK or not (D6).

**`GrpcUnaryResult`** — `{target, service, method, call_type, status: GrpcStatus,
message: dict | null, initial_metadata: dict, trailers: dict}`. `message` is the decoded
response (`null` for an empty response). Backs the `grpc.call` envelope result (§10.1) and
the client-streaming single response. `call_type` is a stable token — one of
`"unary" | "server_stream" | "client_stream" | "bidi"` (derived from the method
descriptor's `client_streaming`/`server_streaming` flags) — used consistently in the
envelope result, the streaming `summary`, and discovery (§13).

**`GrpcStreamMessage`** — `{message: dict, trailers: dict | null}`. One per streamed
response; `trailers` is present only on the final message of a stream (gRPC delivers
trailers at stream end). Backs each NDJSON `event:"message"` line.

**`GrpcHealthResult`** — `{target, address, status: str}` (`SERVING`/`NOT_SERVING`/`UNKNOWN`).

### 8.3 Request serialization

For single-request types, the resolved `message_json` (`--message` deep-merged into the
template `message`, `{placeholder}`-filled from `--param`) is passed to
`ParseDict(json, message_instance, ignore_unknown_fields=False)` over the method's input
message class. **Unknown fields / type mismatches raise `ConfigError` (exit 2)** pre-call —
an agent-authored request that does not match the proto schema fails loudly, never produces
a malformed message on the wire. For client-stream/bidi, each stdin NDJSON line is parsed
the same way.

## 9. Descriptor Strategy & JSON↔protobuf (the gRPC-specific core)

1. **Reflection** (`reflection: auto`/`on`): open the channel, call the server reflection
   service, enumerate services, fetch file descriptor protos, build a
   `descriptor_pool.DescriptorPool`. Reflection stubs ship with `grpcio`.
2. **Descriptor fallback** (`reflection: off`, or reflection returns `UNIMPLEMENTED` with
   `reflection: auto`): load from config — either compile `proto` globs via
   `grpc_tools.protoc` into a `FileDescriptorSet`, or load a pre-compiled `descriptor_set`
   blob. Either way the result feeds the same `descriptor_pool`.
3. **Message classes** from the pool via `message_factory.GetMessageClass(message_desc)`
   — no generated `_pb2` modules. **Stubs are built dynamically** from the channel + the
   resolved service/method descriptors, so any service/method in the pool is callable.
4. **JSON→protobuf** via `google.protobuf.json_format.ParseDict`; **protobuf→JSON** via
   `MessageToDict` (preserving field names as authored in the proto, camelCase-default per
   `json_format`, configurable).

Descriptor resolution is **per-invocation and stateless** (like config load) — no on-disk
cache, no shared pool across invocations. Reflection/descriptor load failure surfaces as the
error model in §11.

## 10. Output Contract

### 10.1 `grpc.call` — one-result cases (unary + client-streaming)

```json
{
  "ok": true,
  "command": "grpc.call",
  "result": {
    "target": "order-service",
    "service": "order.v1.OrderService",
    "method": "CreateOrder",
    "call_type": "unary",
    "status": { "code": 0, "name": "OK", "message": "" },
    "message": { "order_id": "ord-789", "status": "PENDING" },
    "initial_metadata": { "content-type": "application/grpc" },
    "trailers": {}
  },
  "duration_ms": 87
}
```

A non-OK status still yields `ok:true` / exit `0` (D6); an assertion mismatch yields
`ok:false` with `error.detail = {response, failures}` (the `http call` shape — full response
in `detail.response`, each failing mode listed with a `root` label + payload snapshot).

### 10.2 `grpc.call` — streaming cases (server-stream / bidi): NDJSON

One JSON object per response message:

```json
{"event":"message","message":{"orderId":"ord-789"},"trailers":null}
{"event":"message","message":{"orderId":"ord-790"},"trailers":null}
{"summary":true,"messages":2,"matched":2,"status":{"code":0,"name":"OK","message":""},"duration_ms":1204}
```

- The `summary` line carries `messages` (total), `matched` (after `--match`), and the final
  `status`. `--expect-count` failure (exit 1) is reflected in a `summary` with `matched <
  expect_count` plus an `AssertionFailure` exit code (the streaming-exception pattern: the
  verdict rides on the summary + exit code).
- Pre-stream startup failures (unknown target/service/method, reflection miss + no
  descriptors, bad `--match`, mutex violations, wrong input for call type, missing extra)
  emit a single structured error envelope **before** any message line (the `http ping` /
  `logs tail` precedent), then exit `2`.

### 10.3 `grpc.healthcheck`

```json
{
  "ok": true,
  "command": "grpc.healthcheck",
  "result": {
    "targets": {
      "order-service": { "address": "localhost:50051", "status": "SERVING" },
      "payment-service": { "address": "localhost:50052", "status": "UNKNOWN",
                           "note": "health service UNIMPLEMENTED" }
    },
    "all_serving": false
  },
  "duration_ms": 41
}
```

## 11. Error & Exit-Code Model

No new error types; the existing `AgctlError` hierarchy covers every path.

| Failure | Type | Exit | When |
|---|---|---|---|
| Missing `grpc` extra (`grpcio`/`grpc_tools`/`protobuf`) | `ConfigError` | 2 | `GrpcClient` construction → `pip install 'agctl[grpc]'` |
| Unknown `--target` (no key under `grpc.targets`) in free-form mode | `ConfigError` | 2 | command entry |
| Unknown `--service` / `--method` after descriptor resolution | `TemplateNotFound` | 2 | `find_method` |
| Reflection miss (`UNIMPLEMENTED`/`off`) with no `grpc.descriptors` | `ConfigError` | 2 | `resolve_descriptors` (points at `grpc.descriptors`) |
| Bad/missing `.proto` or `descriptor_set` path; protoc compile error | `ConfigError` | 2 | descriptor load |
| Structurally-bad `grpc.descriptors` entry (neither/both of `proto`/`descriptor_set`) | `ConfigError` | 2 | pydantic `model_validator` at config load |
| Request JSON fails `ParseDict` (unknown field / type mismatch) | `ConfigError` | 2 | pre-call serialization |
| Wrong input for call type (`--message` on client-stream/bidi; stdin on unary/server-stream) | `ConfigError` | 2 | command entry |
| Mode/exclusivity violation (template + free-form flags; `--target`+`--address`; unary-assertion flags on a stream) | `ConfigError` | 2 | command entry |
| Malformed `--match` (compile error, post-placeholder-fill) | `ConfigError` | 2 | up-front `compile_jq` |
| `--match` / `--jq-path` used and `jq` extra missing | `ConfigError` | 2 | lazy `_jq` → `pip install 'agctl[grpc]'` (jq bundled in the extra) |
| `--jq-path` without `--equals` (or vice versa) | `ConfigError` | 2 | pre-call arg check |
| Channel connect failure / transport error | `ConnectionFailure` | 2 | call |
| Deadline exceeded (`--timeout`) | `OperationTimeout` | 1 | call |
| Unary assertion mismatch (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`) | `AssertionFailure` | 1 | post-call |
| Stream `--expect-count` not met | `AssertionFailure` | 1 | stream end (summary + exit 1) |
| `--match` jq eval error at runtime | (soft non-match) | — | `jq_bool` swallows to `False` |
| Non-OK gRPC status with **no** assertion flags | *(not an error)* | 0 | `ok:true`, status in `result.status` (D6) |

Exit-code contract unchanged: `0` ok / `1` assertion / `2` config-or-env.

## 12. Validation Rules (`agctl/config/validator.py`)

Additive cross-reference checks during `agctl config validate` (mirroring how `mocks` and
`database` refs are validated today):

- A `grpc.templates.<name>.target` that is not a key under `grpc.targets` → **error**.
- A `grpc.descriptors` entry that is structurally invalid (neither/both of
  `proto`/`descriptor_set`) → **error** (also enforced by the pydantic validator at load).
- A `grpc.templates.<name>` / `grpc.targets.<name>` missing `description` → **warning**
  (discovery degrades; the existing missing-description rule).
- `grpc.descriptors[].proto` / `descriptor_set` paths are **not** existence-checked at
  config-validate time (a config may be validated in CI without the proto tree present) —
  a missing path surfaces as `ConfigError` at descriptor load (command time), consistent
  with how `logs.sources.<name>.path` is handled.

Service/method existence and `ParseDict`-ability are **not** validated at config time
(they require the live server / descriptor resolution); they surface at command time per §11.

## 13. Discovery Changes — `grpc-services` / `grpc-methods` categories

Additive, following the existing three-level + `--search` protocol. Categories are populated
from the **resolved descriptors** (reflection or config), so discovery may require a live
reflection call for a `reflection: auto/on` target (a target with `reflection: off` and no
descriptors contributes nothing and is skipped with a warning).

- Add `"grpc-services"` and `"grpc-methods"` to `_VALID_CATEGORIES`; update `_SUMMARY_HINT`;
  add `grpc_targets` / `grpc_services` / `grpc_methods` counts to `_summary_core`.
- **Level 1 `--category grpc-services`** → `{name, target, address, methods_count}`.
- **Level 1 `--category grpc-methods`** → `{service, method, call_type}` (`unary` /
  `server_stream` / `client_stream` / `bidi`).
- **Level 2 `--category grpc-methods --name "<service>/<method>"`** → request message field
  schema (`{name, type, repeated}`, derived from the input message descriptor), `call_type`,
  required params, and an `example` command (`agctl grpc call --target <t> --service <s>
  --method <m> --message '{...}'`).
- `--search` covers gRPC service/method names + template descriptions.

Discovery failure isolation: a single target whose reflection is unreachable does not fail
the whole `discover` call — it is reported as `unavailable` in the listing (the
self-skipping ethos of the integration suite, applied to discovery).

## 14. Packaging

- **New `grpc` extra** in `pyproject.toml`:
  `grpc = ["grpcio>=1.62", "grpcio-tools>=1.62", "protobuf>=4.25", "jq>=1.6"]` under
  `[project.optional-dependencies]`. (jq bundled because `--match`/`--jq-path` need it; the
  db/kafka precedent.)
- **`integration` extra** gains `grpc`: `integration = ["testcontainers",
  "agctl[db,kafka,http,grpc]", "pytest>=8.0"]`.
- **No new entry-point group** (D13). No new console script.
- **New `grpc` command group** wired in `cli.py`. No `version` bump (additive under major
  `2`).

## 15. Testing Strategy

Mirrors the existing test architecture and seams (CliRunner + `new_grpc_client`
monkeypatch / injected `channel` / `descriptor_pool`; integration tests self-skip). A
bundled tiny `.proto` (e.g. an `Echo` service with unary/server-stream/client-stream/bidi
methods) ships under `tests/fixtures/` and is compiled into a descriptor set at collection
time for unit tests — no live server needed for the descriptor/serialization/streaming logic.

**Unit (no network, no `grpcio` required due to lazy import):**

- `GrpcClient` descriptor resolution: reflection path against an injected fake reflection
  stub; descriptor fallback against the bundled descriptor set; reflection `UNIMPLEMENTED`
  + no descriptors → `ConfigError`.
- JSON↔protobuf: `ParseDict` happy path; unknown field / type mismatch → `ConfigError`;
  `MessageToDict` round-trip; call-type auto-detection from a method descriptor.
- `grpc call` unary: template + free-form modes; mode/exclusivity violations → `ConfigError`;
  wrong-input-for-call-type → `ConfigError`; assertion suite (`--status` name+number,
  `--contains`, envelope-rooted `--match`, `--jq-path`+`--equals`) pass/fail; non-OK status
  as result (exit 0) vs assertion mismatch (exit 1); missing extra / missing jq →
  `ConfigError`.
- `grpc call` streaming: server-stream NDJSON shape + `summary`; bidi NDJSON in (stdin)
  → out; client-stream stdin → single envelope; `--match` per-message + `--expect-count`
  pass/fail; call-type mismatch → `ConfigError`; startup-error envelope before any message.
- `grpc healthcheck`: per-target `SERVING`/`NOT_SERVING`/`UNKNOWN`; `--all`; `UNIMPLEMENTED`
  → `UNKNOWN` (not an error).
- `discover`: `grpc-services`/`grpc-methods` in summary + hint; category listing; method
  detail with field schema + `example`; search hit; unavailable-target isolation.
- `config/validator`: template→target ref errors; missing-description warnings.

**Integration (self-skipping, gated like the others):** an in-process `grpcio` server
fixture implementing the bundled `Echo` service + reflection + health, started under
`AGCTL_TEST_LIVE=1` or pointed at via `AGCTL_TEST_GRPC_ADDR`. Exercises all four call types,
TLS (self-signed), healthcheck, and discovery end-to-end through the Click CLI (no
monkeypatch).

New files: `tests/fixtures/echo.proto` (+ compiled descriptor), `tests/unit/test_grpc_client.py`,
`tests/unit/test_grpc_commands.py`, `tests/unit/test_grpc_discover.py`, and
`tests/integration/test_grpc_commands.py`.

## 16. Rejected Alternatives (ADR-style)

- **Separate plugin package (`agctl-grpc`) via §9.3 `Plugin`** (the letter of DESIGN §10).
  Rejected (D1): the branch is in-tree and every sibling transport is in-tree; in-tree
  preserves transport symmetry and integrates discovery/config/assertions natively. The
  `Plugin` Protocol remains the seam for genuinely *different* future protocols.
- **Reflection-only** (require the SUT to expose reflection). Rejected (D2): hardened prod
  servers commonly disable reflection; descriptor fallback is a small, principled addition
  that removes a hard blocker.
- **Descriptors-only / pre-compiled stubs only.** Rejected (D2/D10): reflection gives
  zero-config dev ergonomics; generated `_pb2` stubs couple `agctl` to a specific `protoc`
  run and break "any service in the descriptors."
- **Request-stream input via `--messages '[...]'` array or `--messages-file`.** Rejected
  (D4): bounded / quoting-hell / temp-file lifecycle. stdin NDJSON is unbounded, mirrors
  the output NDJSON convention, and is trivial for an agent to produce.
- **Unary-only / defer client-stream+bidi.** Rejected (D4): user chose all four call types;
  the stdin-NDJSON model makes the two input-stream types tractable without a new contract.
- **A `grpc_backends` entry-point + `Protocol`** (analog of `db_drivers`/`logs_backends`).
  Rejected (D13): gRPC is one wire protocol, not a backend family; the seam would be
  speculative. §9.3 `Plugin` is the correct out-of-tree seam if a *different* protocol
  arrives.
- **Mapping gRPC status codes to HTTP status / reusing `--status <http-code>`.** Rejected
  (D6/D7): gRPC has its own status model; `--status` takes a gRPC code (name or number).
  Translating to HTTP would lose information and surprise gRPC users.
- **`--streaming` flag to select call type.** Rejected (D4): the descriptor already declares
  the call type; auto-detection removes a foot-gun (a `--streaming` flag that disagrees with
  the proto is a silent bug).
- **Cross-message ordering assertions in v1.** Rejected (Non-Goals): per-message
  `--match` + `--expect-count` cover v1; ordering adds offset/sequence tracking that is
  deferred.
- **gRPC mock server in v1.** Rejected (Non-Goals): the mock engine is HTTP+Kafka; a gRPC
  mock (stub server + the trigger→reaction model over gRPC) is a separate effort.

## 17. Deferred & Not-Covered

- **gRPC mock server** (`mocks.grpc`) — separate effort.
- **Cross-message ordering assertions** on streams.
- **Compression, retry/service-config, interceptors, OpenTelemetry propagation.**
- **OAuth / call-credentials beyond static `metadata`** (e.g. per-call token acquisition).
- **Reflection v1 vs v1alpha as a user-facing knob.**
- **Named gRPC patterns** (`grpc.patterns`) — templates cover the named-call case.
- **Response extraction (`--capture`)** — deferred (same call as http-jq / logs).
- **`--match-all` / regex status matching.**

### Known-limitations note (honest)

- **Zero-config discovery requires reflection.** A `reflection: off` target with no
  descriptors contributes nothing to `grpc-*` discovery (skipped + warning); the operator
  must supply descriptors for offline/locked-down servers. Documented, not hidden.
- **Client-stream / bidi require an NDJSON-producing agent.** The two input-stream call
  types are only usable when the caller can write request messages to stdin (a shell pipe
  or agent subprocess). Unary/server-stream — the common cases — are unaffected.
- **`.proto` fallback compile needs `grpcio-tools` at load.** The `grpc` extra bundles it,
  but a reflection-only install that later adds a `.proto` descriptor must have the extra
  (it does — the extra is monolithic). The lazy import keeps reflection-only loads fast.
- **Server-streaming assertions are per-message.** v1 asserts on each message (`--match`)
  and the count (`--expect-count`); there is no cross-message predicate. Documented; see
  Deferred.

## 18. Docs & Skill Impact

- **DESIGN.md** — strike the gRPC row from §10; add a `grpc` subsection to §3 (command
  contract: `grpc call` template/free-form, call-type table, `grpc healthcheck`, the
  stdin/stdout NDJSON model); add `grpc:` to §2.1 (`targets`/`descriptors`/`templates`) and
  the `grpc` extra to §7/§11; add `grpc.call` / `grpc.healthcheck` shapes to §4.2; add
  `grpc-services`/`grpc-methods` to the `discover` categories (§3.8, §4.2).
- **ARCHITECTURE.md** — add `grpc_client.py` / `grpc_commands.py` to §3 module map; add a
  `grpc` request-lifecycle trace to §4; add a gRPC subsection to §8 (transport layer:
  descriptor resolution, JSON↔protobuf, four call types, status-as-result); note server-stream
  / bidi as a streaming exception in §6; §10 — note gRPC is in-tree with **no new
  entry-point**; §15 — strike the gRPC deferred note.
- **`skills/agctl*`** — add a `grpc` authoring reference: the `grpc:` config section
  (`targets`/`descriptors`/`templates`), the reflection-first/descriptor-fallback rule,
  JSON message authoring, the four call types and the stdin/stdout NDJSON model, the
  status-as-result semantics, and the `grpc` extra.
- **`README.md`** — surface the `grpc` group and `pip install 'agctl[grpc]'`.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").
