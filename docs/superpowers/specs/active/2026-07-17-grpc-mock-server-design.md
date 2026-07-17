# Design: `agctl mock grpc` — gRPC Mock Server

**Status:** Approved (brainstormed 2026-07-17)
**Spec date:** 2026-07-17
**Branch:** `feat/grpc-mock-server`
**Precedent:** [`2026-07-09-grpc-support-design.md`](./2026-07-09-grpc-support-design.md) (the gRPC *client* this builds on)

---

## 1. Background & Problem

`agctl mock` serves two mock engines today: an HTTP stub server (`mocks.http`)
and Kafka reactors (`mocks.kafka`). The `agctl grpc` command group provides a
gRPC *client* (`grpc call` / `grpc healthcheck`) with descriptor resolution
(reflection + `.proto`/`.pb` fallback) and JSON↔protobuf translation. There is
**no gRPC server**: an agent cannot point a system-under-test's gRPC client at a
fake server the way it points an HTTP client at `mocks.http`.

This leaves a false-green surface: any runbook that depends on a downstream gRPC
service can only be exercised against the real service (or not at all). The
mock's stated goal — local testing without external dependencies — does not
extend to gRPC-dependent services.

This spec closes that gap by adding a **gRPC mock engine** as a third engine
alongside HTTP and Kafka, reusing every shared primitive (`MocksConfig`,
`CaptureSpec`, `resolve_captures`, `render_typed`, `emit_event`, the daemon +
log-parser layer) and the gRPC client's descriptor + JSON↔protobuf machinery.

## 2. Goals

- **Serve gRPC stubs** the SUT's gRPC client can target, parallel to `mocks.http`.
- **All four call types**: unary, server-streaming, client-streaming, bidi.
- **Look like a real gRPC server**: serve `grpc.health.v1.Health` (SERVING) and
  `grpc.reflection.v1alpha.ServerReflection` advertising configured services.
- **Reuse, don't fork**: the stub match → capture → render pipeline is the same
  one HTTP and Kafka use; the descriptor pool and JSON↔protobuf translation are
  the same kernel the gRPC client uses.
- **Fail loud**: unresolved service/method, bad match/capture, invalid status, or
  response-shape/call-type mismatch is a `ConfigError` before the server binds.
- **Zero cost for non-gRPC users**: HTTP/Kafka-only installs never import server-side
  grpc machinery (lazy-import + optional-extra discipline preserved).

## 3. Scope & Design Constraints

- The mock is **SUT-facing**: the real application's gRPC client is pointed at the
  mock's `listen` address (same model as `mocks.http`).
- A gRPC mock **requires protobuf descriptors** to (de)serialize messages. There is
  no live server to reflect *from*, so descriptors come from config (`.proto` files
  compiled on-the-fly, or precompiled `.pb` descriptor sets) — the same
  `grpc.descriptors` / `GrpcDescriptorSource` machinery the client uses.
- The gRPC engine is a **third engine inside the existing `agctl mock` group** —
  `mock run`/`start`/`stop`/`status` gain `--only grpc` and `--grpc-listen`. One
  daemon may serve HTTP + gRPC (+ Kafka reactors).
- **Call type is derived from the descriptor**, never configured: each stub's
  `service`+`method` resolves to a method descriptor; `call_type_of` yields
  `unary|server_stream|client_stream|bidi`. The config carries no `call_type` field.
- All server-side grpc imports are lazy (inside constructors/methods), behind the
  `grpc` extra.

## 4. Non-Goals

- **TLS on the mock server** — plaintext only in v1 (mirrors the HTTP mock MVP).
  A TLS-hardcoded SUT client cannot be intercepted; noted as a limitation.
- **Stateful / server-push bidi** — v1 bidi is request/response pairing (one
  response per matched request). Stateful streaming protocols are out of scope.
- **Per-message-responds client-streaming** — v1 aggregates the request stream and
  responds once at close. Per-message aggregation policies are out of scope.
- **Mid-stream abort** — server-streaming emits all configured messages then a
  terminal status; no RST_STREAM/abort mid-stream.
- **Compiled Python stubs** — no `*_pb2_grpc.py` codegen at startup; a single
  generic servicer dispatches all methods dynamically via the descriptor pool.
- **Auto-generating `.proto` from scratch** — descriptors must be supplied.

## 5. Decisions (recorded with rationale)

1. **Generic servicer over compiled stubs.** A single `grpc.Server` with one
   generic RPC handler dispatches every configured `service/method`, (de)serializing
   dynamically via the descriptor pool + `message_factory` + `json_format`. Avoids
   startup codegen and per-service boilerplate; maximizes reuse of the client's
   descriptor path; handles any service uniformly. *Rejected:* protoc-compile
   `*_pb2_grpc.py` at `start()` and register real servicers — heavier, slower
   startup, throws away the dynamic path.

2. **Integrate into `agctl mock`, not a new `grpc mock` group.** The mock engine is
   already compositional (HTTP + Kafka share one lifecycle, emit path, daemon
   layer). gRPC is the third engine, not a separate command surface. *Rejected:* a
   decoupled `agctl grpc mock` group — duplicates daemon/log/lifecycle code and
   fragments the mock namespace.

3. **Derive call type from the descriptor.** A `call_type` config field would
   duplicate truth already encoded in the proto and invite contradiction. The
   stub's `service`/`method` resolves to a method descriptor whose
   `client_streaming`/`server_streaming` flags determine the type.

4. **Health + Reflection auto-served.** Real gRPC servers serve Health; many serve
   reflection. Auto-serving both (toggleable, default on) makes the mock behave as
   a real server: `agctl grpc healthcheck --target <mock>` succeeds and
   `reflection: auto/on` clients discover methods without bundled descriptors.

5. **Extract a shared proto kernel.** The descriptor-pool build, method lookup,
   call-type derivation, and `serialize`/`deserialize` helpers currently live on
   `GrpcClient`. Extract the proto-only subset into `agctl/clients/grpc_descriptors.py`
   so client and server share one source of truth; reflection-resolution (client-only)
   stays in `grpc_client.py`.

6. **Two-stage fail-loud validation.** Offline checks (jq compile, capture
   placement, status validity) run at Step 0 and `config validate` — no protoc, no
   proto files required. Descriptor/method resolution and response-shape/call-type
   checks run at server construction (Step 2b) — needs the pool + protoc, so
   runtime-only. Matches the HTTP mock's split (validate is offline; bind is runtime).

7. **Pidfile keying stays port-based, with a gRPC-only prefix.** HTTP-present
   daemons keep `mock-<httpport>.pid` (recording both listens inside); gRPC-only
   daemons use `mock-grpc-<grpcport>.pid`; kafka-only unchanged. `--listen`
   selection matches either recorded listen. Backward-compatible; collision-safe.

## 6. Command Contracts

### 6.1 `agctl mock run` (extended)

```
agctl mock run
    [--config <path>] [--overlay <path>] [--env-file <path>]
    [--http-listen <host:port>]      # literal (NO ${}); overrides mocks.http.listen
    [--grpc-listen <host:port>]      # literal (NO ${}); overrides mocks.grpc.listen   [NEW]
    [--only http|kafka|grpc]         # run a single engine                                       [grpc: NEW]
    [--fail-fast] [--duration <s>] [--until-stopped]
```

- `--grpc-listen` is a literal `host:port` (CLI args are not `${}`-interpolated).
- `--only grpc` runs the gRPC engine alone (HTTP/Kafka engines off).
- `--only <engine>` with that engine absent → `ConfigError` (exit 2), as today.
- No `mocks` section, or `--only grpc` with no `mocks.grpc.stubs` → emits `started`
  + `summary` with zero gRPC counts and exits `0` (idempotent no-op), as today.
- Port-in-use on either HTTP or gRPC bind → `ConfigError` with the stale-mock hint.

### 6.2 `agctl mock start` / `stop` / `status` (extended)

`mock start` gains `--grpc-listen` and `--only grpc` (flag set is `run`'s minus the
streaming-lifecycle flags, as today). `mock stop` / `mock status` keep their current
flag set; `--listen <host:port>` matches a daemon whose recorded listens (HTTP or
gRPC) include that address.

**`mock start` result (`mock.start`) — HTTP+gRPC combo:**
```json
{ "ok": true, "command": "mock.start",
  "result": { "pid": 22345, "listen": "0.0.0.0:18080",
              "grpc": { "listen": "0.0.0.0:50051", "stubs": 2, "services": ["echo.EchoService"],
                        "reflection": true, "health": true },
              "log_path": "./.agctl/mock-18080.log", "stubs": 3, "reactors": ["rc"],
              "started_at": "2026-07-17T09:00:00Z" } }
```
A gRPC-only start omits the http-level `listen`/`stubs`; an HTTP-only start omits the
`grpc` block (mirrors how kafka-only omits http today).

`mock stop`'s exit-1 verdict extends to gRPC fatal events (`grpc.unmatched`,
`grpc.error`) with no new logic — they join `FATAL_FAILURE_EVENTS`. `capture.missing`
stays non-fatal.

### 6.3 NDJSON event stream (new gRPC events)

`mock run` emits the existing event set plus:

```json
{"event":"grpc.hit","stub":"echo-unary","service":"echo.EchoService","method":"Unary","call_type":"unary","status":"OK","duration_ms":1,"timestamp":"…"}
{"event":"grpc.unmatched","service":"echo.EchoService","method":"Missing","call_type":"unary","timestamp":"…"}
{"event":"grpc.error","stub":"echo-unary","service":"echo.EchoService","method":"Unary","error":"…","fatal":true,"timestamp":"…"}
{"event":"capture.missing","stub":"echo-unary","name":"msg","from":".message.msg","timestamp":"…"}
```

- `grpc.hit` — one per **response message** sent (unary=1; server_stream=N; client_stream=1; bidi=1 per pair).
- `grpc.unmatched` — no stub matches `service/method`, or stubs match but every predicate fails → terminal `UNIMPLEMENTED`. **Fatal.**
- `grpc.error` — handler deserialize/serialize/runtime failure. **Fatal**, sets the runtime-error flag.
- `capture.missing` — non-fatal (reused, identical semantics to HTTP/Kafka).

`started` gains `grpc: {listen, stubs, services, reflection, health}`; `summary` gains
`grpc_hits`, `grpc_unmatched`, `grpc_errors`.

## 7. Config Schema — `mocks.grpc`

New models join `config/models.py`: `GrpcMockConfig`, `GrpcStub`, `GrpcMatch`,
`GrpcResponse`, `GrpcResponseMessage`. They reuse `CaptureSpec` and `GrpcDescriptorSource`
verbatim, and `MocksConfig` gains `grpc: GrpcMockConfig | None = None`.

```yaml
mocks:
  grpc:
    listen: "0.0.0.0:${AGCTL_MOCK_GRPC_PORT:-50051}"   # host:port; ${}-interpolated like http
    descriptors:                          # OPTIONAL; falls back to top-level grpc.descriptors
      - proto: "/abs/path/echo.proto"
        include_paths: ["/abs/path/protos"]
      # - descriptor_set: "/abs/path/echo.pb"
    reflection: true                      # default true → serve ServerReflection
    health: true                          # default true → serve Health (SERVING)
    concurrency_cap: 64                   # max in-flight RPCs (ThreadPoolExecutor max_workers)
    stubs:
      echo-unary:
        description: "Mock the Echo unary RPC"
        service: "echo.EchoService"       # fully-qualified
        method: "Unary"
        match:                            # OPTIONAL; both sub-fields AND-ed; omitted = always match
          body: { "msg": "hello" }        # json_subset on the deserialized request message dict
          jq: '.message.msg == "hello"'   # jq predicate on the request envelope (root per call type)
        capture:                          # OPTIONAL; same CaptureSpec as HTTP/Kafka; shares the match root
          msg: { from: ".message.msg" }
        response:
          status: OK                      # gRPC status name OR numeric code; default OK
          message: { msg: "{msg}" }       # single response message (unary / client-stream / bidi)
          metadata:                       # OPTIONAL; initial metadata (and trailers on terminal status)
            x-mock: "true"
        delay_ms: 0

      echo-stream:                        # server-streaming method
        service: "echo.EchoService"
        method: "ServerStream"
        response:
          status: OK
          messages:                       # server-streaming: ordered list; one gRPC message per entry
            - message: { chunk: "a" }
              delay_ms: 0
            - message: { chunk: "b" }
              delay_ms: 100
          metadata: { x-mock: "true" }
```

**Field contracts:**

- `GrpcMockConfig.listen` — host:port; validated via `parse_listen` (IPv6 brackets, port 0 = ephemeral).
- `GrpcMockConfig.descriptors` — `list[GrpcDescriptorSource] | None`; `None` → fall back to top-level `grpc.descriptors`.
- `GrpcMockConfig.reflection` / `.health` — `bool`, default `true`.
- `GrpcMockConfig.concurrency_cap` — `int`, default `64` (grpcio `ThreadPoolExecutor(max_workers=…)`; the pool *is* the cap).
- `GrpcMockConfig.stubs` — `dict[str, GrpcStub]` (insertion-ordered; first-match-wins).
- `GrpcStub` — `description?`, `service: str` (fully-qualified), `method: str`, `match?: GrpcMatch`, `capture?: dict[str, CaptureSpec]`, `response: GrpcResponse`, `delay_ms: int = 0`.
- `GrpcMatch` — `body?: dict` (json_subset on request message), `jq?: str` (predicate on the §8.1 envelope). AND-ed.
- `GrpcResponse` — `status: str|int = "OK"` (valid gRPC name or code), exactly one of `message` / `messages`, `metadata?: dict[str,str]`.
- `GrpcResponseMessage` — `message: Any` (the message template), `delay_ms: int = 0`.

**Response-shape rule (validated at Step 2b):** unary / client_stream / bidi stubs
must use `response.message`; server_stream stubs must use `response.messages`. Any
other combination → `ConfigError` at `mocks.grpc.stubs.<name>.response`.

## 8. Behavior & Semantics

### 8.1 Match envelope per call type

The match envelope (input to `match.jq` and `capture.from`) differs by call type.
`metadata` is lowercased-keyed (mirrors HTTP headers); `message` is the deserialized
request dict (camelCase via `json_format.MessageToDict`, identical to what the gRPC
client sees).

| Call type | Match envelope | Match timing | Response |
|---|---|---|---|
| **unary** | `{service, method, metadata, message}` | per request | one `message` |
| **server_stream** | `{service, method, metadata, message}` | once on the single request | stream all `messages`, then terminal `status` |
| **client_stream** | `{service, method, metadata, messages:[…], count}` | once, at request-stream close | one `message` |
| **bidi** | `{service, method, metadata, message}` | per incoming request (echo-style) | one `message` per matched request |

- **First-match-wins** over `stubs` in config insertion order (identical to HTTP).
- **No stub matches `service/method`, or every predicate fails** → terminal
  `UNIMPLEMENTED` + `grpc.unmatched` event (the gRPC analog of HTTP 404 +
  `http.unmatched`). Fail-loud; never a silent default response.
- **`status` simulation:** `OK` (default) = success; any other valid name/code
  (`NOT_FOUND`, `PERMISSION_DENIED`…) = the stub returns that gRPC error as the
  terminal status (no body for unary/client-stream; stream `messages` first for
  server-stream). Validated via `assertions._GRPC_STATUS_BY_NAME/_BY_CODE`.
- **client_stream** matches the aggregated `messages` list at close (captures reach
  `.messages[-1].id`, `.messages[].x`); returns exactly one response.
- **bidi** is request/response pairing: each matched incoming request emits one
  rendered response; an unmatched request emits `grpc.unmatched` and yields no
  response for that turn (stream continues). Stateful/server-push bidi is out of scope.

### 8.2 Dispatch (generic servicer)

A single generic RPC handler registered via `server.add_generic_rpc_handlers(...)`.
For `/<service>/<method>`:

1. Look up `method_meta[(service, method)]`; missing → `UNIMPLEMENTED` + `grpc.unmatched`.
2. Deserialize request bytes → dict (kernel `deserialize`).
3. Build the §8.1 envelope for the call type.
4. First-match-wins over `stubs_by_method[(service, method)]`: `match.body`
   (`json_subset`) AND `match.jq` (`jq_bool`); first match → `resolve_captures` +
   `render_typed` (the exact HTTP/Kafka pipeline). No match → `UNIMPLEMENTED` + `grpc.unmatched`.
5. Apply `delay_ms`, serialize the rendered response dict → bytes (kernel `serialize`),
   emit `grpc.hit`.

The four call types map to the four `RpcMethodHandler` constructors
(`grpc.unary_unary_rpc_method_handler` / `unary_stream_rpc_method_handler` /
`stream_unary_rpc_method_handler` / `stream_stream_rpc_method_handler`), each wired
with `request_deserializer` / `response_serializer` from the kernel and a behavior
callable implementing §8.1.

### 8.3 Health & Reflection

- **Health:** `grpc_health.v1.health.HealthServicer` added via
  `add_HealthServicer_to_server`; every configured `service` (plus overall `""`) set
  to `SERVING`. Gated by `health: true` (default).
- **Reflection:** `grpc_reflection.v1alpha.reflection.enable_server(server, service_names)`
  advertises the configured services. Gated by `reflection: true` (default).

## 9. Output Contract

Streaming exception (same model as `mock run` today): not wrapped by `@envelope`;
one NDJSON object per event via the single-writer `emit_event` lock. SIGTERM/SIGINT
handlers flush a final `summary` and exit `0` (clean) / `1` (any fatal
`grpc.error`/`grpc.unmatched`/runtime error). Startup errors emit one structured
envelope before any event line. New events: `grpc.hit`, `grpc.unmatched`,
`grpc.error` (plus reused `capture.missing`).

## 10. Error & Exit-Code Model

No new exception classes. Reused:

- `ConfigError` (exit 2) — unresolved `service`/`method`, response-shape/call-type
  mismatch, invalid `status`, malformed `match.jq`/capture placement, port-in-use,
  missing descriptor files at runtime, missing `grpc` extra.
- `AssertionFailure` (exit 1) — driven by fatal events at `mock stop` (extends
  `FATAL_FAILURE_EVENTS` with `grpc.unmatched`, `grpc.error`).
- Missing `grpc` extra surfaces as `ConfigError` pointing at `pip install 'agctl[grpc]'`
  (lazy-import convention), never an opaque `ModuleNotFoundError`.

## 11. Module Layout

```
agctl/
├── mock/
│   ├── grpc_server.py        # NEW — MockGrpcServer: grpc.Server, generic servicer,
│   │                         #         method table, dispatch (§8.2), health+reflection
│   ├── engine.py             # EXTEND — run_grpc/grpc_listen, Step 2b, grpc block in
│   │                         #         started/summary, serve thread, grpc_server_factory seam
│   ├── daemon.py             # EXTEND — mock-grpc-<port>.pid keying, RunningMock http/grpc_listen,
│   │                         #         EVENT_TO_COUNTER grpc.*, ALL_FAILURE_EVENTS/FATAL
│   ├── jq_precompile.py      # EXTEND — iter_mock_jq_expressions walks grpc stubs
│   └── capture_validate.py   # EXTEND — collect_capture_placement_errors walks grpc stub response
├── clients/
│   ├── grpc_descriptors.py   # NEW — shared proto kernel: build_descriptor_pool, find_method,
│   │                         #         call_type_of, message_class, serialize, deserialize
│   └── grpc_client.py        # REFACTOR — thin to call grpc_descriptors; reflection stays here
├── config/
│   └── models.py             # EXTEND — GrpcMockConfig/GrpcStub/GrpcMatch/GrpcResponse(+Msg);
│                             #         MocksConfig.grpc
└── commands/
    └── mock_commands.py      # EXTEND — --grpc-listen, --only grpc, _resolve_engines→3-tuple,
                              #         new_mock_engine run_grpc/grpc_listen
```

## 12. Validation Rules

- **Offline (Step 0 + `config validate`):**
  - `match.jq` compiles (`compile_jq`, label `mocks.grpc.stubs.<name>.match.jq`).
  - `capture.<name>.from` placement valid (`collect_capture_placement_errors` over `response.message`/`messages`).
  - `response.status` is a valid gRPC name or numeric code (`_GRPC_STATUS_BY_NAME/_BY_CODE`).
  - Exactly one of `response.message` / `response.messages` is set (structural).
- **Runtime (Step 2b, server construction):**
  - Every `service`+`method` resolves via `find_method` in the resolved pool (`TemplateNotFound` → `ConfigError`).
  - Response-shape matches the derived call type (`message` for unary/client-stream/bidi; `messages` for server-stream).
  - Descriptor source files exist and protoc/descriptor-set parse succeeds.
- Each error carries a precise `mocks.grpc.stubs.<name>.<field>` path.

## 13. Discovery Changes — `mock-grpc-stubs` category

- Level 0 summary gains `grpc_mock_stubs` (count of `mocks.grpc.stubs`).
- New category `mock-grpc-stubs`: Level 1 lists `{name, description}`; Level 2 returns
  `{name, description, service, method, match, params, example}`. Added to the
  category hint list.

## 14. Packaging

No new extra. The `grpc` extra already declares `grpcio`, `grpcio-tools`,
`grpcio-health-checking`, `grpcio-reflection`, `protobuf`, `jq`. The server-side
imports (`grpc.server`, `HealthServicer`, `reflection.enable_server`) reuse them.
`grpcio-health-checking` / `grpcio-reflection` become used server-side too (previously
client-only). Lazy-import discipline preserved: HTTP/Kafka-only installs import no
server-side grpc code.

## 15. Testing Strategy

- **Unit (grpc-extra-gated where proto needed):**
  - `test_grpc_descriptors.py` — `build_descriptor_pool`, `find_method`, `call_type_of`,
    `serialize`/`deserialize` (pure proto; needs protobuf/protoc).
  - `test_mock_grpc_server.py` — match/capture/render pipeline via the injected
    `grpc_server_factory` fake (no real grpcio server); envelope construction per call
    type; first-match-wins; unmatched→`UNIMPLEMENTED`; status simulation.
  - Extend `test_mock_engine.py` (run_grpc start/run/shutdown with fake server),
    `test_mock_daemon.py` (gRPC-only/multi-engine pidfile keying, event counters,
    fatal taxonomy), `test_config_models.py` (`GrpcMockConfig` validation),
    `test_jq_precompile.py` / `test_capture_validate.py` (grpc stub walks).
- **Integration (`AGCTL_TEST_LIVE=1`, grpc extra):**
  - `test_mock_grpc.py` — spin `mock run --only grpc`, drive all four call types via
    `agctl grpc call`, assert `grpc healthcheck` SERVING, and a reflection-based client
    discovers a method. Real grpcio server + client round-trip. Self-skips without the
    extra / flag, like the existing mock integration suite.

## 16. Rejected Alternatives (ADR-style)

1. **Compiled stubs via protoc at startup** — heavier (codegen + tempdir every start),
   per-service boilerplate, slower startup, abandons the client's dynamic path.
2. **Separate `agctl grpc mock` group** — duplicates daemon/log/lifecycle code and
   fragments the mock namespace.
3. **`call_type` config field** — duplicates truth in the descriptor and invites
   contradiction; rejected in favor of descriptor derivation.
4. **Run-id pidfile keying (like `kafka listen`)** — cleaner for multi-engine but a
   larger change to `RunningMock`/`resolve_target`; port-keying with a gRPC-only prefix
   is backward-compatible and sufficient.
5. **Reflection to bootstrap the mock** — impossible: there is no live server to
   reflect from. Descriptors are config-supplied; reflection is only *served*.

## 17. Deferred & Not-Covered

- TLS on the mock server (plaintext-only v1).
- Stateful / server-push bidi.
- Per-message-responds client-streaming aggregation policies.
- Mid-stream abort (RST_STREAM) on server-streaming.
- Auto-generation of `.proto` from scratch.
- Cross-transport sagas (gRPC stub → Kafka reaction linkage).

## 18. Docs & Skill Impact

- **DESIGN.md** — §1 (mock note extends to gRPC), §2.1 (add `mocks.grpc` schema block),
  §3.6 (mock: gRPC engine, lifecycle, events, `--grpc-listen`/`--only grpc`),
  §3.9 (discover: `mock-grpc-stubs` category + count), §10 (limitations: add gRPC-mock
  MVP boundaries).
- **ARCHITECTURE.md** — §3 (module map: `mock/grpc_server.py`, `clients/grpc_descriptors.py`),
  §4 (lifecycle: gRPC stub trace), §6 (output contract: gRPC events), §8 (client: note
  shared kernel; new server subsection), §10 (extension points), §14 (deltas), §15
  (limitations: gRPC-mock MVP).
- **skills/** — any consumer skill documenting `agctl mock` extended with the gRPC engine.
- **docs-watcher** invoked at implementation finish to sync DESIGN.md/ARCHITECTURE.md.
