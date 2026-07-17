# gRPC Mock Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a gRPC mock engine (`mocks.grpc`) to `agctl mock`, serving all four call types via a descriptor-driven generic servicer with auto-served Health + Reflection, reusing the existing stub pipeline and the gRPC client's proto machinery.

**Architecture:** A third engine alongside HTTP/Kafka inside `MockEngine`. New `agctl/mock/grpc_server.py` wraps a `grpc.Server` with one generic RPC handler that dispatches any `/service/method` dynamically via a shared descriptor pool. The descriptor-resolution + JSON↔protobuf helpers are extracted from `GrpcClient` into `agctl/clients/grpc_descriptors.py` (one source of truth). The stub match→capture→render pipeline is the same one HTTP/Kafka use (`json_subset`, `jq_bool`, `resolve_captures`, `render_typed`).

**Tech Stack:** Python ≥3.11, Click, Pydantic v2, grpcio + grpcio-tools + grpcio-health-checking + grpcio-reflection + protobuf (the existing `grpc` extra), `jq` (lazy). Lazy-import discipline preserved so HTTP/Kafka-only installs import no server-side grpc code.

**Spec:** [`docs/superpowers/specs/active/2026-07-17-grpc-mock-server-design.md`](../specs/active/2026-07-17-grpc-mock-server-design.md)

## Global Constraints

- **Python ≥3.11.** No new optional extra — the `grpc` extra already declares `grpcio`, `grpcio-tools`, `grpcio-health-checking`, `grpcio-reflection`, `protobuf`, `jq`.
- **Lazy import:** server-side grpc imports live inside `MockGrpcServer` methods/constructor only. `agctl/mock/grpc_server.py` MUST NOT `import grpc` (or any grpcio-* package) at module top — module-level functions (the dispatch core) must be importable and unit-testable without grpcio installed. A missing library surfaces as `ConfigError` pointing at `pip install 'agctl[grpc]'`, never a bare `ModuleNotFoundError`.
- **One-emit / streaming contract:** `mock run` is a streaming exception (not `@envelope`-wrapped); new `grpc.*` events flow through the single-writer `MockEngine.emit_event` lock.
- **Fail loud:** unresolved service/method, response-shape/call-type mismatch, invalid status, malformed `match.jq`/capture placement, port-in-use, missing descriptor files → `ConfigError` (exit 2) before the server binds or before any network side-effect.
- **Exit codes:** `0` clean, `1` assertion/fatal-runtime, `2` config/env error — unchanged model; gRPC fatal events (`grpc.unmatched`, `grpc.error`) join `FATAL_FAILURE_EVENTS`.
- **Config schema version is unchanged** (`"3"`) — `mocks.grpc` is an additive field; no migration.
- **Naming/copy:** follow existing conventions — dotted config paths in error messages, NDJSON event names use `grpc.*`, summary counters use `grpc_*` snake_case.
- **Cross-cutting lockstep** (called out per task): (a) `_resolve_engines` 3-tuple change touches `mock run` + `mock start` + `new_mock_engine` together; (b) the `started`-line `grpc` block touches `engine.py` + `mock_commands.py` + `daemon.parse_log` together; (c) the event-taxonomy dicts in `daemon.py` are the single source of truth for counters/failures.

---

## File Structure

```
agctl/
├── clients/
│   ├── grpc_descriptors.py     # NEW — shared proto kernel (Task 1)
│   └── grpc_client.py          # REFACTOR — delegate to grpc_descriptors (Task 1)
├── config/
│   └── models.py               # EXTEND — GrpcMockConfig et al. + MocksConfig.grpc (Task 3)
├── mock/
│   ├── grpc_server.py          # NEW — MockGrpcServer + pure dispatch core (Tasks 5-7)
│   ├── engine.py               # EXTEND — run_grpc engine (Task 8)
│   ├── daemon.py               # EXTEND — grpc pidfile/taxonomy (Task 9)
│   ├── jq_precompile.py        # EXTEND — walk grpc stubs (Task 4)
│   └── capture_validate.py     # EXTEND — walk grpc stub response (Task 4)
├── commands/
│   ├── mock_commands.py        # EXTEND — --grpc-listen/--only grpc, 3-tuple, start result (Task 10)
│   └── discover_commands.py    # EXTEND — mock-grpc-stubs category + count (Task 11)
└── assertions.py               # EXTEND — public parse_grpc_status helper (Task 2)

tests/
├── unit/
│   ├── test_grpc_descriptors.py       # NEW (Task 1)
│   ├── test_grpc_status.py            # NEW/extend (Task 2)
│   ├── test_config_models.py          # EXTEND (Task 3)
│   ├── test_mock_jq_precompile.py / test_mock_capture_validate.py  # EXTEND (Task 4)
│   ├── test_mock_grpc_dispatch.py     # NEW (Task 5)
│   ├── test_mock_grpc_server.py       # NEW (Tasks 6-7)
│   ├── test_mock_engine.py            # EXTEND (Task 8)
│   ├── test_mock_daemon.py            # EXTEND (Task 9)
│   ├── test_mock_commands.py          # EXTEND (Task 10)
│   └── test_discover_commands.py      # EXTEND (Task 11)
├── fixtures/
│   └── mock_grpc/echo.proto           # NEW — test proto with all 4 call types (Task 6)
└── integration/
    └── test_mock_grpc.py              # NEW — live round-trip, self-skipping (Task 12)
```

## Shared Types & Paths (referenced by every task)

- **Test proto fixture** (`tests/fixtures/mock_grpc/echo.proto`) — defines `package echo; service EchoService { rpc Unary(EchoRequest) returns (EchoResponse); rpc ServerStream(EchoRequest) returns (stream EchoResponse); rpc ClientStream(stream EchoRequest) returns (EchoResponse); rpc Bidi(stream EchoRequest) returns (stream EchoResponse); }` with messages `EchoRequest { string msg = 1; int32 n = 2; }` and `EchoResponse { string msg = 1; }`. This is the single proto used by unit + integration tests for the gRPC mock.
- **gRPC status contract** — a stub `response.status` is a gRPC status **name** (`"OK"`, `"NOT_FOUND"`, …) or **numeric code** (`0`–`16`, as int or digit-string). Validated against the 17-code canonical map; invalid → `ConfigError`.
- **gRPC mock match envelope** (input to `match.jq` and `capture.from`):
  - unary / server_stream / bidi: `{"service": str, "method": str, "metadata": dict[str,str] (lowercased keys), "message": dict}` (the deserialized request).
  - client_stream: `{"service": str, "method": str, "metadata": dict, "messages": list[dict], "count": int}` (built at request-stream close).
- **Reused helpers (do not re-implement):** `resolve_captures(envelope, captures) -> (dict[str, CaptureValue], list[tuple[str,str]])` from `agctl/mock/capture.py`; `render_typed(value, captures)` + `CaptureValue` from `agctl/resolution.py`; `json_subset`, `jq_bool`, `jq_value`, `compile_jq` from `agctl/assertions.py`; `parse_listen(listen) -> (host, port)` from `agctl/config/models.py`.

---

## Task 1: Shared proto kernel — `agctl/clients/grpc_descriptors.py`

Extract the proto-only helpers from `GrpcClient` into a new module so the mock server shares one source of truth with the client. Pure refactor — `GrpcClient` behavior is unchanged (existing client tests are the safety net).

**Files:**
- Create: `agctl/clients/grpc_descriptors.py`
- Modify: `agctl/clients/grpc_client.py` (delegate to the new kernel)
- Test: `tests/unit/test_grpc_descriptors.py`

**Interfaces:**
- Consumes: `GrpcDescriptorSource` (`proto`/`include_paths`/`descriptor_set`) from `agctl/config/models.py`; `errors.ConfigError`, `errors.TemplateNotFound`.
- Produces (new module `agctl/clients/grpc_descriptors.py`, no `grpc` runtime import — only `grpc_tools.protoc` + `google.protobuf` lazy-imported inside functions):
  - `add_file_protos_order_tolerant(pool, file_protos) -> None` — moved verbatim from the current module-level `_add_file_protos_order_tolerant`.
  - `build_descriptor_pool(sources: list[GrpcDescriptorSource], *, context_label: str) -> DescriptorPool` — the body of the current `GrpcClient._resolve_via_descriptors` lifted to a function taking the source list + a label for error messages. Raises `ConfigError` (message includes `context_label`) when `sources` is empty or a file is missing/protoc fails. Lazy-imports `descriptor_pool` and `grpc_tools.protoc` inside.
  - `find_method(pool, service: str, method: str) -> MethodDescriptor` — the current `GrpcClient.find_method` body (minus `self`): `pool.FindServiceByName(service)` (`KeyError` → `TemplateNotFound`), `.methods_by_name.get(method)` (`None` → `TemplateNotFound`).
  - `call_type_of(method_desc) -> str` — moved verbatim (the existing `@staticmethod`); returns `"unary"|"server_stream"|"client_stream"|"bidi"`.
  - `message_class(message_desc) -> type` — lazy-import `message_factory`, return `message_factory.GetMessageClass(message_desc)`.
  - `serialize(message_desc) -> Callable[[dict | bytes], bytes]` — dict path: `json_format.ParseDict(d, msg, ignore_unknown_fields=False)` then `SerializeToString()`; bytes pass through.
  - `deserialize(message_desc) -> Callable[[bytes], dict]` — `cls.FromString(b)` then `json_format.MessageToDict(msg)`.
- `GrpcClient` refactor: `resolve_descriptors`/`find_method`/`call_type_of`/`_msg_class`/`_serialize`/`_deserialize` delegate to the kernel (e.g. `find_method` calls `grpc_descriptors.find_method(self.resolve_descriptors(), service, method)`; `_resolve_via_descriptors` calls `grpc_descriptors.build_descriptor_pool(self._descriptors, context_label=self._target.address)`). `_resolve_via_reflection` stays in `grpc_client.py` (client-only). `call_type_of` remains accessible as `GrpcClient.call_type_of` (delegating staticmethod) so existing call sites don't break.

- [ ] **Step 1: Write failing tests for the kernel** — `tests/unit/test_grpc_descriptors.py`, grpc-extra-gated (skip if `grpc_tools`/`google.protobuf` import fails, mirroring how existing grpc client unit tests gate). A shared conftest fixture compiles `tests/fixtures/mock_grpc/echo.proto` (create the file in this task) to a `DescriptorPool` via `grpc_tools.protoc` + `FileDescriptorSet` (or `build_descriptor_pool([GrpcDescriptorSource(proto=<path>)])`). Tests verify: `find_method(pool, "echo.EchoService", "Unary")` returns a descriptor whose `call_type_of` is `"unary"`; `ServerStream` → `"server_stream"`; `ClientStream` → `"client_stream"`; `Bidi` → `"bidi"`; `find_method(pool, "echo.EchoService", "Missing")` raises `TemplateNotFound`; `find_method(pool, "echo.Missing", "Unary")` raises `TemplateNotFound`; `build_descriptor_pool([], context_label="x")` raises `ConfigError` whose message mentions `x`; `serialize`/`deserialize` round-trip `{"msg": "hi"}` (bytes → dict → bytes stable); `serialize` raises on an unknown field (`{"nope": 1}`) because `ignore_unknown_fields=False`.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_grpc_descriptors.py -v` → FAIL (`ModuleNotFoundError: agctl.clients.grpc_descriptors`).
- [ ] **Step 3: Create the kernel + test fixture** — create `tests/fixtures/mock_grpc/echo.proto` per Shared Types; create `agctl/clients/grpc_descriptors.py` implementing the Produces contracts by moving the proto-only logic out of `grpc_client.py`.
- [ ] **Step 4: Refactor `grpc_client.py` to delegate** — replace the inlined proto logic with calls to `grpc_descriptors.*`; keep `call_type_of` as a delegating staticmethod; keep reflection in place. Do not change any public behavior.
- [ ] **Step 5: Run kernel tests + full client test suite** — `pytest tests/unit/test_grpc_descriptors.py tests/unit/test_grpc_client.py -v` (client tests gated on grpc extra) → all PASS. Then `pytest tests/unit -q` → no regressions (tests without the grpc extra still pass).
- [ ] **Step 6: Commit** — `git add agctl/clients/grpc_descriptors.py agctl/clients/grpc_client.py tests/unit/test_grpc_descriptors.py tests/fixtures/mock_grpc/echo.proto` && `git commit -m "refactor: extract shared gRPC proto kernel into clients/grpc_descriptors.py"`.

---

## Task 2: Public gRPC status helper — `parse_grpc_status`

Factor the duplicated name-or-code status parsing out of `assertions.py` into one public helper, so config validation (Task 3) and the server (Task 5) validate `response.status` without reaching into private maps.

**Files:**
- Modify: `agctl/assertions.py`
- Test: `tests/unit/test_grpc_status.py` (new) — or extend an existing assertions test module.

**Interfaces:**
- Consumes: the existing private `_GRPC_STATUS_BY_NAME` / `_GRPC_STATUS_BY_CODE` (17 codes, 0–16) already in `assertions.py`.
- Produces (new public function in `agctl/assertions.py`):
  - `parse_grpc_status(status: str | int) -> tuple[int, str]` — coerces a digit-string to int (`"5"` → `5`); looks up a name in `_GRPC_STATUS_BY_NAME` (`"NOT_FOUND"` → `(5, "NOT_FOUND")`) or a code in `_GRPC_STATUS_BY_CODE` (`5` → `(5, "NOT_FOUND")`); raises `ConfigError(f"status must be a gRPC code name or number 0-16, got {status!r}", {})` on anything else. Returns `(code, name)`.
  - `validate_grpc_assertion_args` and `evaluate_grpc_assertions` are refactored to call `parse_grpc_status` instead of their duplicated inline logic (behavior identical — same coercion, same error message, same lookups). This is a pure dedupe; existing grpc-assertion tests are the safety net.

- [ ] **Step 1: Write failing tests** — `parse_grpc_status("OK") == (0, "OK")`; `parse_grpc_status("not_found")` — case-insensitive UPPER → `(5, "NOT_FOUND")` (match the existing behavior: the current code does NOT uppercase, so decide and assert the exact behavior — **assert case-sensitive match only** to preserve current behavior, i.e. `"not_found"` raises; document this); `parse_grpc_status(5) == (5, "NOT_FOUND")`; `parse_grpc_status("5") == (5, "NOT_FOUND")`; `parse_grpc_status(0) == (0, "OK")`; `parse_grpc_status(16) == (16, "UNAUTHENTICATED")`; `parse_grpc_status("FOO")` raises `ConfigError`; `parse_grpc_status(17)` raises `ConfigError`; `parse_grpc_status(-1)` raises `ConfigError`.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_grpc_status.py -v` → FAIL (function not defined).
- [ ] **Step 3: Implement `parse_grpc_status`** — add the public function per Produces; refactor the two existing call sites to use it (preserve the exact current error string `"--status must be a gRPC code name or number 0-16, got {status!r}"` inside `validate_grpc_assertion_args`; the new helper's own message drops the `--status ` prefix so it is reusable).
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_grpc_status.py tests/unit -q` → PASS, no regressions (existing grpc-assertion tests still pass under the grpc extra; the helper itself needs no grpc import, so it runs without the extra).
- [ ] **Step 5: Commit** — `git add agctl/assertions.py tests/unit/test_grpc_status.py` && `git commit -m "refactor: add public parse_grpc_status, dedupe status parsing"`.

---

## Task 3: Config models — `GrpcMockConfig` / `GrpcStub` / `GrpcMatch` / `GrpcResponse`

Add the typed config models for `mocks.grpc` and wire `MocksConfig.grpc`. Offline structural validation only (exactly-one-of message/messages; status validity); method-resolution is deferred to the server (Task 6).

**Files:**
- Modify: `agctl/config/models.py`
- Test: `tests/unit/test_config_models.py` (extend)

**Interfaces:**
- Consumes: `CaptureSpec` (verbatim, alias `from`↔`from_`, `type: Literal["scalar","object","json"]`); `GrpcDescriptorSource` (verbatim); `parse_listen(listen) -> (host, port)`; `parse_grpc_status` (Task 2).
- Produces (new Pydantic v2 models in `agctl/config/models.py`):
  - `GrpcMatch(BaseModel)`: `body: dict | None = None`; `jq: str | None = None`.
  - `GrpcResponseMessage(BaseModel)`: `message: Any`; `delay_ms: int = 0` (`ge=0`).
  - `GrpcResponse(BaseModel)`: `status: str | int = "OK"`; `message: Any = None`; `messages: list[GrpcResponseMessage] | None = None`; `metadata: dict[str, str] | None = None`. `@model_validator(mode="after")` enforces exactly-one-of `message`/`messages`: both set, or neither set, → `ValidationError`. `@field_validator("status")` (or a model_validator) calls `parse_grpc_status` and re-raises as `ValidationError` on invalid (so a bad status fails at model parse, exit 2). Store the original input (name or int) — the (code,name) resolution happens at render time.
  - `GrpcStub(BaseModel)`: `description: str | None = None`; `service: str`; `method: str`; `match: GrpcMatch | None = None`; `capture: dict[str, CaptureSpec] | None = None`; `response: GrpcResponse`; `delay_ms: int = 0` (`ge=0`).
  - `GrpcMockConfig(BaseModel)`: `listen: str = "0.0.0.0:50051"`; `descriptors: list[GrpcDescriptorSource] | None = None`; `reflection: bool = True`; `health: bool = True`; `concurrency_cap: int = 64` (`ge=1`); `stubs: dict[str, GrpcStub] = Field(default_factory=dict)`. `@field_validator("listen")` calls `parse_listen` (raises `ValueError` → Pydantic turns it into `ValidationError`), mirroring `HttpMockConfig.listen`.
  - `MocksConfig` gains `grpc: GrpcMockConfig | None = None`.
- **Note:** response-shape-vs-call-type (e.g. `messages` on a unary method) is NOT validated here — it needs the descriptor pool (runtime). It is validated in Task 6. Here we only enforce the structural exactly-one-of and status validity.

- [ ] **Step 1: Write failing tests** — `GrpcMockConfig` parses a full stub (service/method/match/capture/response.message/status/metadata); `listen` validator rejects `"noport"` and accepts `"0.0.0.0:50051"` + `"[::1]:50051"`; `GrpcResponse` rejects both `message` and `messages` set; rejects neither set; accepts `message` alone and `messages` alone; `status: "NOT_FOUND"` and `status: 5` and `status: "5"` all parse; `status: "FOO"` and `status: 17` raise `ValidationError`; `concurrency_cap: 0` raises; default `listen=="0.0.0.0:50051"`, `reflection is True`, `health is True`. A `MocksConfig` with only `grpc` set parses and leaves `http`/`kafka` `None`.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_config_models.py -k grpc -v` → FAIL (models undefined).
- [ ] **Step 3: Implement the models** — add the five models + `MocksConfig.grpc` per Produces.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_config_models.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add agctl/config/models.py tests/unit/test_config_models.py` && `git commit -m "feat(config): add mocks.grpc models (GrpcMockConfig/Stub/Match/Response)"`.

---

## Task 4: jq precompile + capture-placement walkers extended for gRPC stubs

Extend the two Step-0 offline validators to walk `mocks.grpc.stubs`, so malformed `match.jq` / bad capture placement fails loud before the descriptor pool builds.

**Files:**
- Modify: `agctl/mock/jq_precompile.py`, `agctl/mock/capture_validate.py`
- Test: `tests/unit/test_mock_jq_precompile.py`, `tests/unit/test_mock_capture_validate.py` (extend; create if absent)

**Interfaces:**
- Consumes: `iter_mock_jq_expressions(mocks) -> Iterator[tuple[str,str]]` yields `(path_label, expr)`; `collect_jq_compile_errors(mocks) -> list[dict]` (each `{"path","message"}`); `collect_capture_placement_errors(mocks) -> list[dict]` (each `{"path","message"}`); the `GrpcStub` shape from Task 3.
- Produces (no signature changes — both functions already take `MocksConfig | None`):
  - `iter_mock_jq_expressions` gains a third block (after kafka): `if mocks.grpc is not None: for name, stub in mocks.grpc.stubs.items():` yield `("mocks.grpc.stubs.{name}.match.jq", stub.match.jq)` when `stub.match and stub.match.jq`; and for each `(cap, spec)` in `stub.capture or {}`: yield `("mocks.grpc.stubs.{name}.capture.{cap}.from", spec.from_)`. `collect_jq_compile_errors` needs no change (it consumes the iterator).
  - `collect_capture_placement_errors` gains a gRPC block mirroring the HTTP `response.body` branch: for each grpc stub with `stub.capture`, for each object-typed capture name, walk `stub.response.message` AND each `stub.response.messages[*].message` via the existing `_walk_tree`, appending `{"path": f"mocks.grpc.stubs.{name}", "message": ...}` on an inline-placement violation (object capture must occupy a whole field).

- [ ] **Step 1: Write failing tests** — a `MocksConfig` with a grpc stub whose `match.jq` is malformed (e.g. `".msg =="`) → `collect_jq_compile_errors` returns one entry with `path == "mocks.grpc.stubs.s.match.jq"`; a grpc stub capture `from` (valid jq path string) appears in `iter_mock_jq_expressions` with the right label; an object-typed capture placed inline inside `response.message` → `collect_capture_placement_errors` returns one entry with `path == "mocks.grpc.stubs.s"`; a whole-field object capture yields no error. (jq-dependent tests skip if `jq` import fails, matching existing precompile tests.)
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_jq_precompile.py tests/unit/test_mock_capture_validate.py -v` → FAIL (grpc stubs not walked).
- [ ] **Step 3: Extend both walkers** — add the grpc blocks per Produces.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_jq_precompile.py tests/unit/test_mock_capture_validate.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add agctl/mock/jq_precompile.py agctl/mock/capture_validate.py tests/` && `git commit -m "feat(mock): extend jq-precompile + capture-placement walkers to grpc stubs"`.

---

## Task 5: Pure dispatch core (no grpcio)

The transport-agnostic brain: given a resolved method's call type, the stub list for that method, and the deserialized request(s) + metadata, pick the stub (first-match-wins), run match/capture/render, and produce the response message dict(s) + terminal status, or signal UNIMPLEMENTED. Pure functions, no grpc import, fully unit-testable.

**Files:**
- Modify: `agctl/mock/grpc_server.py` (create; module-level functions only this task — NO `import grpc` at top)
- Test: `tests/unit/test_mock_grpc_dispatch.py`

**Interfaces:**
- Consumes: `GrpcStub`/`GrpcResponse`/`GrpcMatch` (Task 3); `json_subset`, `jq_bool` from `assertions.py`; `resolve_captures`, `render_typed`, `CaptureValue` from `mock/capture.py` + `resolution.py`; `parse_grpc_status` (Task 2).
- Produces (module-level functions in `agctl/mock/grpc_server.py`):
  - `build_envelope(service, method, metadata, *, message=None, messages=None) -> dict` — returns the §Shared-Types envelope: with `message` for unary/server_stream/bidi; with `messages`+`count` for client_stream. `metadata` keys lowercased.
  - `GrpcDispatchOutcome` — a small dataclass: `matched: bool`; `stub_name: str | None`; `messages: list[Any]` (rendered response message dicts — 1 for unary/client_stream, N for server_stream, 1 for bidi-pair); `status: tuple[int, str] | None` (terminal status; `None` means OK-default handled by caller); `missing_captures: list[tuple[str, str]]` (for `capture.missing` events). When `matched is False`, the other fields are `None`/empty.
  - `dispatch_grpc(stubs: list[GrpcStub], envelope: dict, call_type: str, *, emit_capture_missing: Callable[[str, str, str], None]) -> GrpcDispatchOutcome` — iterates stubs in order; for each, evaluates `match.body` (`json_subset(stub.match.body, <root message>)` where the root is `envelope["message"]` for non-client-stream, `envelope["messages"]` last/aggregate per the §8.1 table — for client_stream, `match.body` is not applicable and is skipped) AND `match.jq` (`jq_bool(envelope, stub.match.jq)`); first stub where all set predicates pass wins. On match: run `resolve_captures(envelope, stub.capture)` → for each `(name, from_path)` in the missing list, call `emit_capture_missing(stub.name, name, from_path)`; render each response message via `render_typed(stub.response.message or each messages[*].message, captures)`; resolve `stub.response.status` via `parse_grpc_status` (default OK). Return `GrpcDispatchOutcome(matched=True, ...)`. If no stub matches → `GrpcDispatchOutcome(matched=False)`.
  - **Per-call-type message selection:** unary/client_stream/bidi use `response.message` → `messages=[rendered]`; server_stream uses `response.messages` → `messages=[rendered per entry]`. (Response-shape correctness vs call type is validated at construction in Task 6; dispatch trusts the call_type arg.)
- **Behavioral rules (asserted in tests):** first-match-wins; `match.body` AND `match.jq` both must pass when both set; omitted `match` → always match; a `match.jq` runtime error against a specific message → treated as non-match (soft), never raises (mirrors HTTP `match.jq`); `capture.from` resolving to null → `capture.missing` emitted + empty-string substitution (via `render_typed`'s existing `None`→`""` rule).

- [ ] **Step 1: Write failing tests** — cover: (a) unary stub with `match.body` subset → matched, rendered message substitutes `{placeholder}` from a capture; (b) two stubs same method, first's predicate fails, second matches → second wins; (c) `match.jq` true → match, false → skip; (d) both body+jq → AND; (e) no match → `matched=False`; (f) server_stream stub → `messages` has N entries rendered; (g) client_stream envelope with `messages:[...]` → match.jq rooted at `.messages[-1].x` works; (h) capture `from` resolving null → `emit_capture_missing` called once with `(stub_name, cap_name, from_path)` and rendered value substitutes `""`; (i) `status: "NOT_FOUND"` → `outcome.status == (5, "NOT_FOUND")`; (j) a `match.jq` that raises on the input → non-match, no raise. Use plain Python dicts for stubs (construct `GrpcStub` instances) and a list-recording fake for `emit_capture_missing`.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_grpc_dispatch.py -v` → FAIL (module/functions undefined).
- [ ] **Step 3: Implement the dispatch core** — create `agctl/mock/grpc_server.py` with the module-level functions + `GrpcDispatchOutcome`. No `import grpc` anywhere in the file yet.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_grpc_dispatch.py -q` → PASS. (jq-dependent cases skip without the `jq` extra.)
- [ ] **Step 5: Commit** — `git add agctl/mock/grpc_server.py tests/unit/test_mock_grpc_dispatch.py` && `git commit -m "feat(mock): pure gRPC dispatch core (match/capture/render/status)"`.

---

## Task 6: `MockGrpcServer` — construction, validation, method table

Build the server object up to (but not yet wiring) the grpc runtime: resolve the descriptor pool once, validate every stub's service/method + response-shape-vs-call-type, and precompute the per-method table. Construction failures → `ConfigError`.

**Files:**
- Modify: `agctl/mock/grpc_server.py` (add `MockGrpcServer` class — grpc imports lazy inside `__init__`/methods only)
- Test: `tests/unit/test_mock_grpc_server.py` (grpc-extra-gated where it needs the pool)

**Interfaces:**
- Consumes: `GrpcMockConfig`/`GrpcStub` (Task 3); `build_descriptor_pool`, `find_method`, `call_type_of`, `message_class`, `serialize`, `deserialize` (Task 1 kernel); `parse_listen`.
- Produces — `class MockGrpcServer`:
  - `__init__(self, config: GrpcMockConfig, *, top_level_descriptors: list[GrpcDescriptorSource] | None, emit_event: Callable[[dict], None], descriptor_pool=None)`:
    - Lazy-import grpc only when actually needed for binding (deferred to Task 7's `serve_forever`). In `__init__`, resolve descriptors + build tables without importing grpc runtime (the kernel uses `grpc_tools.protoc`/`google.protobuf`, not `grpc`).
    - Resolve descriptor sources: `config.descriptors or top_level_descriptors`. Build the pool once via `build_descriptor_pool(sources, context_label=f"mocks.grpc")`.
    - For each stub: `find_method(pool, stub.service, stub.method)` (miss → `ConfigError` at `mocks.grpc.stubs.<name>` with the underlying `TemplateNotFound` detail); derive `call_type = call_type_of(method_desc)`; **response-shape check:** unary/client_stream/bidi require `response.message is not None` and `response.messages is None`; server_stream requires the opposite — violation → `ConfigError` at `mocks.grpc.stubs.<name>.response` with a message naming the call type.
    - Build `self.stubs_by_method: dict[tuple[str,str], list[GrpcStub]]` (insertion order) and `self.method_meta: dict[tuple[str,str], tuple[input_msg_desc, output_msg_desc, call_type]]` (input/output via `method_desc.input_type`/`.output_type`).
    - Store `self._listen_host, self._listen_port = parse_listen(config.listen)`; `self._config = config`; `self._emit_event = emit_event`; `self._server = None` (grpc.Server, built in Task 7).
    - Expose `self.services: list[str]` (sorted unique fully-qualified service names from stubs) and `self.listen_address -> str` (`f"{host}:{port}"`).
  - `bind_address` property → `(host, port)` for the engine's started line.
  - **DI seam:** accept an injected `descriptor_pool` (tests pass a prebuilt pool; production passes `None`).
- Produces (validation error cases, all `ConfigError` exit 2, all before any bind): unresolved service; unresolved method; response-shape/call-type mismatch.

- [ ] **Step 1: Write failing tests (grpc-extra-gated)** — using the echo.proto fixture pool (Task 1's conftest, or build via the kernel in-test): (a) construct `MockGrpcServer` with a unary stub for `echo.EchoService/Unary` + `response.message` → succeeds, `services == ["echo.EchoService"]`, `stubs_by_method[("echo.EchoService","Unary")]` has 1 entry, `method_meta[...][2] == "unary"`; (b) a stub with `service="echo.Missing"` → `ConfigError` mentioning `mocks.grpc.stubs.<name>`; (c) `method="Missing"` → `ConfigError`; (d) unary stub using `response.messages` → `ConfigError` at `...response` mentioning `unary`; (e) server_stream stub (`ServerStream`) using `response.message` → `ConfigError` mentioning `server_stream`; (f) server_stream stub using `response.messages` → succeeds with `call_type=="server_stream"`; (g) descriptor fallback: `config.descriptors=None` + `top_level_descriptors=[...]` → uses top-level; both empty → `ConfigError`.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_grpc_server.py -v` → FAIL (class undefined).
- [ ] **Step 3: Implement `MockGrpcServer.__init__` + tables + validation** per Produces. Do not add grpc-runtime calls yet.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_grpc_server.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add agctl/mock/grpc_server.py tests/unit/test_mock_grpc_server.py` && `git commit -m "feat(mock): MockGrpcServer construction, descriptor validation, method table"`.

---

## Task 7: `MockGrpcServer` — generic servicer, 4 call types, Health, Reflection, lifecycle

Wire the grpc runtime: build a `grpc.Server` with one generic RPC handler dispatching via the Task-5 core, register Health + Reflection, and provide `serve_forever`/`shutdown` for the engine. Integration-style unit tests with a real in-process grpcio server + `agctl grpc call`-style client calls.

**Files:**
- Modify: `agctl/mock/grpc_server.py`
- Test: `tests/unit/test_mock_grpc_server.py` (extend; these cases need the grpc extra — real grpcio)

**Interfaces:**
- Consumes: the dispatch core (Task 5); the method table (Task 6); grpcio server API (`grpc.server`, `ThreadPoolExecutor`, `add_generic_rpc_handlers`, `grpc.unary_unary_rpc_method_handler`/`unary_stream_rpc_method_handler`/`stream_unary_rpc_method_handler`/`stream_stream_rpc_method_handler`, `grpc.ServicerContext.abort`); `grpc_health.v1` (`HealthServicer`, `add_HealthServicer_to_server`, the `HealthCheckResponse.ServingStatus.SERVING` enum); `grpc_reflection.v1alpha.reflection` (`enable_server`); `serialize`/`deserialize` (Task 1 kernel).
- Produces — `MockGrpcServer` methods:
  - `_build_server(self) -> grpc.Server` (lazy-import grpc + grpc_health + grpc_reflection inside): creates `grpc.server(futures.ThreadPoolExecutor(max_workers=self._config.concurrency_cap))`; registers one generic handler (an object whose `service(service_name)` returns a per-service handler exposing `_method_handler(method)` → the right `*_rpc_method_handler` for the derived call type, wired with `request_deserializer=deserialize(input_msg_desc)` and `response_serializer=serialize(output_msg_desc)`); if `config.health`: `add_HealthServicer_to_server(HealthServicer)` and set every `self.services` (+ overall `""`) to `SERVING`; if `config.reflection`: `reflection.enable_server(server, self.services)`.
  - The four behavior callables (each: deserialize already done by the handler's `request_deserializer`, so the callable receives a `dict` request / iterator of dicts): build the envelope via `build_envelope`, call `dispatch_grpc(stubs_by_method[(svc,mtd)], envelope, call_type, emit_capture_missing=...)`, emit `grpc.hit` per response message (fields: `stub, service, method, call_type, status_name, duration_ms`) via `self._emit_event`, apply per-message `delay_ms`; on `matched=False` emit `grpc.unmatched` and `context.abort(grpc.StatusCode.UNIMPLEMENTED, ...)`. Non-OK terminal `status` → `context.abort(<code>, <message>)` after emitting any messages. On any unexpected exception in a handler → emit `grpc.error` (`{stub, service, method, error, fatal: True}`) and re-raise/abort `INTERNAL`.
    - unary: returns one message dict (or aborts).
    - server_stream: yields each rendered message (with its `delay_ms`), then terminal status.
    - client_stream: consumes the request iterator into `messages`, matches once at close, returns one message.
    - bidi: per incoming request, match+render; yield one response per matched request; unmatched request → `grpc.unmatched` + skip (no response that turn).
  - `start(self) -> None`: `self._server = self._build_server()`; `self._server.add_insecure_port(self.listen_address)` → returns bound port (0 → ephemeral; recompute if `self._listen_port==0`); `self._server.start()`. `OSError` EADDRINUSE → `ConfigError("grpc listen address ... already in use; kill the stale mock ...", {"listen": ...})`.
  - `serve_forever(self, stop_event: threading.Event) -> None`: loop `self._server.wait_for_termination(timeout=0.2)` until `stop_event.is_set()`.
  - `shutdown(self) -> None`: `self._server.stop(grace=<2s>)` + `wait_for_termination(<2s>)`; idempotent.
  - `actual_listen(self) -> str`: the bound `host:port` (after `start`, for ephemeral-port tests).

- [ ] **Step 1: Write failing integration-style tests (grpc-extra-gated, skip without grpc extra)** — spin a real `MockGrpcServer` (echo.proto), `start()` on port 0, then call via `agctl.clients.grpc_client.GrpcClient` (or a raw `grpc.insecure_channel`) against `actual_listen()`:
  - unary stub `Echo/Unary` `match.body {"msg":"hi"}` `response.message {"msg":"{msg}"}` capture `msg from .message.msg` → call with `{"msg":"hi"}` returns `{"msg":"hi"}`; a `grpc.hit` event recorded; call with `{"msg":"bye"}` (no match) → `UNIMPLEMENTED` status + `grpc.unmatched` event.
  - server_stream stub → call returns N messages in order.
  - client_stream stub `match.jq '.messages[-1].msg == "end"'` → send 3 requests, response matches.
  - bidi stub → send 3 requests, get 3 responses (echo-style).
  - `health=True`: `agctl grpc healthcheck`-style `HealthStub.Check("")` → `SERVING`; for a configured service → `SERVING`.
  - `reflection=True`: a `ServerReflectionStub.list_services()` returns the configured services.
  - `status: "NOT_FOUND"` unary stub → call returns `NOT_FOUND`.
  - port-in-use: bind a server on a fixed port, construct a second on the same port → `ConfigError`.
  - missing-grpc-extra: the whole module's grpc path raises `ConfigError` pointing at `pip install 'agctl[grpc]'` (simulate by monkeypatching the import to fail).
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_grpc_server.py -v` → FAIL (no server wiring).
- [ ] **Step 3: Implement `_build_server`, the four behaviors, Health, Reflection, `start`/`serve_forever`/`shutdown`/`actual_listen`** per Produces. Lazy-import grpc inside methods.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_grpc_server.py -q` → PASS (grpc extra present). Confirm a no-extra run still imports the module (dispatch core) without error.
- [ ] **Step 5: Commit** — `git add agctl/mock/grpc_server.py tests/unit/test_mock_grpc_server.py` && `git commit -m "feat(mock): gRPC generic servicer, 4 call types, Health+Reflection, lifecycle"`.

---

## Task 8: `MockEngine` — third engine (run_grpc)

Extend the engine to construct/run/shut down a `MockGrpcServer`, emit the `grpc` started/summary blocks and `grpc.*` counters, with a `grpc_server_factory` seam for grpcio-free unit tests.

**Files:**
- Modify: `agctl/mock/engine.py`
- Test: `tests/unit/test_mock_engine.py` (extend)

**Interfaces:**
- Consumes: `MockGrpcServer` (Task 7); `parse_listen`; the existing `emit_event`/`_emit_lock`/`_started` machinery.
- Produces — `MockEngine.__init__` gains keyword params (defaults preserve backward compat):
  - `run_grpc: bool = False`, `grpc_listen: str | None = None`, `grpc_server_factory: Callable[..., MockGrpcServer] | None = None` (default: lazy-import + construct the real `MockGrpcServer`).
  - New instance counters `self._grpc_hits = self._grpc_unmatched = self._grpc_errors = 0`.
- Produces — behavior:
  - `start()` Step 2b (after HTTP Step 2, before Step 3 started line), gated on `self._run_grpc`: resolve `grpc_listen` (`self._grpc_listen` or `config.grpc.listen`), build a `GrpcMockConfig` view (mutate a copy with the resolved listen if `--grpc-listen` overrode), construct the server via `self._grpc_server_factory(config=config, top_level_descriptors=<cfg.grpc.descriptors fallback handled inside>, emit_event=self.emit_event)` (or the real constructor), call `server.start()` (bind). Catch `OSError` EADDRINUSE → `ConfigError` with the stale-mock hint; on any failure, shut down what's up and re-raise (existing pattern). Set `self._grpc_server`.
  - `run()`: start a daemon thread running `self._grpc_server.serve_forever(self._stop)` alongside the HTTP/reactor threads.
  - `shutdown()`: call `self._grpc_server.shutdown()` (guarded `if self._grpc_server is not None`) in addition to HTTP/kafka teardown.
  - `emit_event`: extend the if/elif counter dispatch — `event=="grpc.hit" → self._grpc_hits += 1`; `"grpc.unmatched" → self._grpc_unmatched += 1` (and it is a fatal event → set runtime-error flag like `kafka.error`); `"grpc.error" → self._grpc_errors += 1` + runtime-error flag. (`capture.missing` needs no new counter.)
  - `_emit_started_line`: add `started_line["grpc"] = {"listen": <actual_listen>, "stubs": len(stubs), "services": [...], "reflection": bool, "health": bool}` when `self._run_grpc and self._grpc_server is not None`, else `None`.
  - `_emit_summary_line`: add `grpc_hits/grpc_unmatched/grpc_errors` to the snapshot read + summary dict.
- **Lockstep:** the `started["grpc"]` block is consumed by `mock start` (Task 10) and `daemon.parse_log` (Task 9) — change together.

- [ ] **Step 1: Write failing tests (grpcio-free via the factory seam)** — a fake `MockGrpcServer` (records `start`/`serve_forever`/`shutdown` calls, exposes `services=[]`, `listen_address`, `bind_address`, `actual_listen`, `stubs_by_method={}`) injected via `grpc_server_factory`. Tests: (a) `run_grpc=True` engine `start()` → fake server `start()` called, `started` line has `grpc` block with the fake's listen; (b) `run()` spawns a serve thread that exits when stop set; (c) `shutdown()` → fake `shutdown()` called; (d) feeding `emit_event({"event":"grpc.hit"})` 3× → summary `grpc_hits==3`; `grpc.unmatched`/`grpc.error` set the runtime-error flag (summary exit 1); (e) `run_grpc=False` → no grpc block (`None`), no server constructed; (f) factory raising `ConfigError` on `start()` propagates and triggers shutdown of already-started engines.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_engine.py -k grpc -v` → FAIL.
- [ ] **Step 3: Extend `MockEngine`** per Produces. Ensure the new `__init__` params default so the existing `new_mock_engine` 2-tuple call sites still work until Task 10 updates them.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_engine.py -q` → PASS (no grpcio needed).
- [ ] **Step 5: Commit** — `git add agctl/mock/engine.py tests/unit/test_mock_engine.py` && `git commit -m "feat(mock): MockEngine gRPC engine (run_grpc, factory seam, grpc events/summary)"`.

---

## Task 9: Daemon layer — grpc pidfile keying + event taxonomy

Extend the pure daemon functions for gRPC-only and multi-engine daemons and add `grpc.*` to the log-event taxonomy.

**Files:**
- Modify: `agctl/mock/daemon.py`
- Test: `tests/unit/test_mock_daemon.py` (extend)

**Interfaces:**
- Consumes: the current `pidfile_path(state_dir, port)` / `log_path` / `RunningMock` / `EVENT_TO_COUNTER` / `FATAL_FAILURE_EVENTS` / `ALL_FAILURE_EVENTS` / `parse_log` / `has_fatal_failure`.
- Produces (extend `agctl/mock/daemon.py`):
  - `EVENT_TO_COUNTER` gains `"grpc.hit": "grpc_hits"`, `"grpc.unmatched": "grpc_unmatched"`, `"grpc.error": "grpc_errors"`. (Auto-propagates to `summary_so_far` zero-init.)
  - `FATAL_FAILURE_EVENTS` gains `"grpc.unmatched"`, `"grpc.error"`. `ALL_FAILURE_EVENTS` derives (unchanged expression).
  - `RunningMock` gains `http_listen: str | None = None` and `grpc_listen: str | None = None` (keep existing `listen`/`port` as the primary identity). The pidfile JSON writer/reader round-trips the new fields.
  - Pidfile keying: extend `pidfile_path`/`log_path` to accept an optional engine hint so gRPC-only daemons use `mock-grpc-<port>.pid`/`.log`. Concretely: `pidfile_path(state_dir, port=None, *, engine: str | None = None)` — `engine=="grpc"` → `mock-grpc-<port>.{pid,log}`; `engine is None and port is None` → `mock-kafka.{pid,log}` (unchanged); else `mock-<port>.{pid,log}` (unchanged, HTTP-present default). HTTP+gRPC combos keep `mock-<httpport>.pid` and record `grpc_listen` inside. (Add the `engine` kwarg with a default so existing call sites are unaffected.)
  - `list_running_mocks` globs `mock-*.pid` (already covers the new prefix) and reads the new fields.
  - `resolve_target`: a `--listen` value matches a `RunningMock` when it equals `http_listen` OR `grpc_listen` OR the legacy `listen`.
- Produces — `has_fatal_failure` needs no change (it iterates `FATAL_FAILURE_EVENTS`).

- [ ] **Step 1: Write failing tests** — (a) `EVENT_TO_COUNTER["grpc.hit"]=="grpc_hits"` etc.; (b) a log with a `grpc.unmatched` event → `has_fatal_failure` True; `grpc.hit`/`capture.missing` → not fatal; (c) `pidfile_path(sd, 50051, engine="grpc")` → name `mock-grpc-50051.pid`; `pidfile_path(sd, 18080)` → `mock-18080.pid`; `pidfile_path(sd, None)` → `mock-kafka.pid`; (d) `RunningMock` round-trips `http_listen`/`grpc_listen` through pidfile JSON; (e) `resolve_target` matches a daemon by its `grpc_listen`; (f) `parse_log` accumulates `grpc_hits/grpc_unmatched/grpc_errors` into `summary_so_far` from a sample log.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_daemon.py -v` → FAIL.
- [ ] **Step 3: Extend the daemon module** per Produces. Keep existing signatures backward-compatible via defaulted kwargs.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_daemon.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add agctl/mock/daemon.py tests/unit/test_mock_daemon.py` && `git commit -m "feat(mock): daemon grpc pidfile keying + grpc.* event taxonomy"`.

---

## Task 10: Commands — `--grpc-listen`, `--only grpc`, 3-tuple engines, `mock start` result

Wire the CLI: extend `mock run`/`mock start` flags, grow `_resolve_engines` to a 3-tuple, thread `run_grpc`/`grpc_listen` through `new_mock_engine`, and add the `grpc` block to the `mock start` result + daemon argv.

**Files:**
- Modify: `agctl/commands/mock_commands.py`
- Test: `tests/unit/test_mock_commands.py` (extend)

**Interfaces:**
- Consumes: `MockEngine` (Task 8); `MockGrpcServer`; daemon layer (Task 9); `MocksConfig` (Task 3).
- Produces:
  - `_resolve_engines(only, mocks) -> tuple[bool, bool, bool]` (run_http, run_kafka, run_grpc). New branch `only=="grpc"`: guard `mocks is None or mocks.grpc is None or not mocks.grpc.stubs` → `ConfigError("--only grpc but no mocks.grpc.stubs configured", {})`; else `(False, False, True)`. Default resolution gains `run_grpc = mocks is not None and mocks.grpc is not None and bool(mocks.grpc.stubs)`. Existing `http`/`kafka` branches gain a `False` grpc element.
  - `new_mock_engine(...)` gains `run_grpc: bool = False`, `grpc_listen: str | None = None`, `grpc_server_factory=...` (forwarded), preserving existing defaults.
  - `mock run` + `mock start` Click: `--only` choice becomes `["http","kafka","grpc"]`; new `--grpc-listen <host:port>` option (literal, like `--http-listen`).
  - `mock run` engine build + `mock start` daemon argv forward `--grpc-listen` (when `run_grpc`) and `--only grpc`; the daemon argv appends `["--grpc-listen", grpc_listen]` when set.
  - `mock start` result: add a `grpc` key read from `started["grpc"]` (`{"listen", "stubs", "services", "reflection", "health"}`) when `run_grpc`, else omit; keep `listen`/`stubs` as the HTTP values (omit/None when HTTP not running, as today).
- **Lockstep:** update every `_resolve_engines` caller + the `new_mock_engine` call sites in this same task.

- [ ] **Step 1: Write failing tests (CliRunner)** — (a) `mock run --only grpc` with a grpc-only `MocksConfig` → engine built with `run_grpc=True` (use the factory seam / spy on `new_mock_engine`); (b) `--only grpc` with no `mocks.grpc` → exits 2 `ConfigError`; (c) `--grpc-listen 127.0.0.1:50051` forwarded into the engine/argv; (d) `_resolve_engines(None, <grpc+http config>)` → `(True, False, True)`; (e) `mock start` result includes a `grpc` block when the daemon's `started` line has one (feed a fake started line through the readiness-poll path or unit-test the result-shaping helper directly); (f) `--only` rejects an unknown choice; (g) a config with HTTP+gRPC starts both (argv carries both `--http-listen` and `--grpc-listen`).
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_mock_commands.py -k grpc -v` → FAIL.
- [ ] **Step 3: Extend the command module** per Produces, updating all `_resolve_engines`/`new_mock_engine` call sites in lockstep.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_mock_commands.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add agctl/commands/mock_commands.py tests/unit/test_mock_commands.py` && `git commit -m "feat(mock): --grpc-listen / --only grpc / 3-tuple engines / start result grpc block"`.

---

## Task 11: Discovery — `mock-grpc-stubs` category + Level-0 count

Surface gRPC mock stubs in `agctl discover`.

**Files:**
- Modify: `agctl/commands/discover_commands.py`
- Test: `tests/unit/test_discover_commands.py` (extend)

**Interfaces:**
- Consumes: `MocksConfig.grpc` (Task 3); the existing `_VALID_CATEGORIES` tuple, `_SUMMARY_HINT`, `_summary_core` count dict, `_category_core`/`_item_core`/`_search_core` dispatch, `_mock_http_stubs`/`_mock_kafka_reactors` accessors.
- Produces:
  - `_mock_grpc_stubs(cfg) -> dict` accessor: `cfg.mocks.grpc.stubs` if `cfg.mocks and cfg.mocks.grpc` else `{}`.
  - `_VALID_CATEGORIES` gains `"mock-grpc-stubs"`.
  - `_SUMMARY_HINT` prose updated to list `mock-grpc-stubs`.
  - `_summary_core` count dict gains `"grpc_mock_stubs": len(_mock_grpc_stubs(cfg))`.
  - `_category_core`: `elif category == "mock-grpc-stubs":` branch → items `{"name", "description"}`.
  - `_item_core`: `elif category == "mock-grpc-stubs":` branch → `{category, name, description, service, method, match (if set), params (capture names + `{placeholder}` scan), example: "agctl ..." , note}`; missing name → `TemplateNotFound`.
  - `_search_core`: a parallel loop appending `{"category":"mock-grpc-stubs","name","description"}` matches.

- [ ] **Step 1: Write failing tests** — Level 0 of a config with 2 grpc stubs → `grpc_mock_stubs == 2`; `discover --category mock-grpc-stubs` → 2 items with names/descriptions; `discover --category mock-grpc-stubs --name <n>` → detail with `service`/`method`/`example`; unknown name → `TemplateNotFound` (exit 2); `discover --search <term>` matches grpc stub descriptions; `_VALID_CATEGORIES` contains `mock-grpc-stubs`.
- [ ] **Step 2: Run tests to verify they fail** — `pytest tests/unit/test_discover_commands.py -k grpc_mock -v` → FAIL.
- [ ] **Step 3: Extend discover** per Produces.
- [ ] **Step 4: Run tests** — `pytest tests/unit/test_discover_commands.py -q` → PASS.
- [ ] **Step 5: Commit** — `git add agctl/commands/discover_commands.py tests/unit/test_discover_commands.py` && `git commit -m "feat(discover): mock-grpc-stubs category + grpc_mock_stubs count"`.

---

## Task 12: Integration tests — live round-trip (self-skipping)

End-to-end: spin `agctl mock run --only grpc`, drive all four call types + health + reflection against a real grpcio server, self-skip without the grpc extra / `AGCTL_TEST_LIVE`.

**Files:**
- Create: `tests/integration/test_mock_grpc.py`

**Interfaces:**
- Consumes: the full stack (Tasks 1–11); the existing integration conftest self-skip machinery (`AGCTL_TEST_LIVE=1`, grpc extra presence).
- Produces — `tests/integration/test_mock_grpc.py`:
  - A self-skip guard: skip unless `AGCTL_TEST_LIVE=1` AND the grpc extra imports.
  - A fixture that writes a temporary `agctl.yaml` with `mocks.grpc` (echo.proto descriptors, stubs for all four call types incl. a capture + a status stub) and runs `agctl mock run --only grpc --grpc-listen 127.0.0.1:0` as a subprocess (or in-process via `MockEngine`), polling stdout for the `started` line to read the bound port.
  - Tests: unary call returns the captured/templated response; unmatched method → `UNIMPLEMENTED`; server-stream returns N messages; client-stream returns one aggregated response; bidi returns one response per request; `agctl grpc healthcheck --target <mock>` → SERVING; a reflection-based `grpc call` (no descriptors configured client-side) resolves the method; the `status: NOT_FOUND` stub returns NOT_FOUND; on `SIGTERM` the `summary` line carries `grpc_hits/grpc_unmatched/grpc_errors`.

- [ ] **Step 1: Write the tests** (they self-skip by design — "fail" here is a skip, which is the passing baseline without live grpc).
- [ ] **Step 2: Run** — `pytest tests/integration/test_mock_grpc.py -v` (no LIVE flag) → all SKIP. Then `AGCTL_TEST_LIVE=1 pytest tests/integration/test_mock_grpc.py -v` (grpc extra installed) → all PASS.
- [ ] **Step 3: Commit** — `git add tests/integration/test_mock_grpc.py` && `git commit -m "test(integration): gRPC mock live round-trip (all call types, health, reflection)"`.

---

## Task 13: Docs sync (docs-watcher) + sample config

Sync DESIGN.md / ARCHITECTURE.md to the as-built gRPC mock and add a `mocks.grpc` example to the sample config, via the `docs-watcher` subagent. Then run the full suite once more.

**Files:**
- Modify: `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `agctl/data/sample-config.yaml`
- Test: whole suite

**Interfaces:**
- Consumes: the implemented feature (Tasks 1–12); the `docs-watcher` subagent; the spec §18 docs-impact list.
- Produces:
  - DESIGN.md: §1 mock note extends to gRPC; §2.1 `mocks.grpc` schema block; §3.6 gRPC engine lifecycle/events/flags; §3.9 `mock-grpc-stubs` category + count; §10 gRPC-mock limitations.
  - ARCHITECTURE.md: §3 module map (`mock/grpc_server.py`, `clients/grpc_descriptors.py`); §4 gRPC stub lifecycle trace; §6 grpc events; §8 shared-kernel note + server subsection; §10/§14/§15 deltas + limitations.
  - `sample-config.yaml`: a commented `mocks.grpc` example validating with no env (optional fields defaulted).
  - DESIGN↔ARCHITECTURE delta table re-populated for the new divergence entries.

- [ ] **Step 1: Invoke `docs-watcher`** with the change summary (gRPC mock engine added; new modules; new config section; new commands/flags/events/discovery category) and the spec §18 list. Let it edit DESIGN.md/ARCHITECTURE.md at the correct altitude (DESIGN = WHAT/WHY, ARCHITECTURE = HOW). A correct no-op on a section is fine.
- [ ] **Step 2: Add the `mocks.grpc` sample** to `agctl/data/sample-config.yaml` (commented, validates with no env).
- [ ] **Step 3: Full verification** — `pytest tests/unit -q` (no extras) green; `pytest tests/unit -q` (with grpc+jq extras) green; `AGCTL_TEST_LIVE=1 pytest tests/integration -q` green; `agctl config validate` on the sample config green; `agctl mock run --help` shows `--grpc-listen`/`--only grpc`; `agctl discover` (on a sample config with grpc stubs) shows `grpc_mock_stubs`.
- [ ] **Step 4: Commit** — `git add docs/ agctl/data/sample-config.yaml` && `git commit -m "docs: sync DESIGN/ARCHITECTURE for gRPC mock engine + sample config"`.
- [ ] **Step 5: Final sweep** — `pytest -q` overall green; `git log --oneline` shows the task commits.

---

## Self-Review (completed during authoring; fixes applied inline)

1. **Code scan:** No method bodies, algorithms, or copy-paste code. The `.proto` snippet in Shared Types is a contract/schema (allowed), not implementation. Behavioral descriptions and TDD expected-results only. ✔
2. **Self-containment:** Every task carries its own Consumes/Produces with exact signatures and data shapes; no "see earlier" without repeating the contract. ✔
3. **Spec coverage:** Spec §5 decisions → Task 1 (decision 5), Task 7 (decisions 1,4), Tasks 8-10 (decision 2), Task 3/6 (decision 3), Task 9 (decision 7). §6 commands → Task 10. §7 config → Task 3. §8 behavior → Tasks 5-7. §9 output → Tasks 7-9. §10 errors → Tasks 3,6,7,9. §11 modules → File Structure. §12 validation → Tasks 3,4,6. §13 discovery → Task 11. §14 packaging → Global Constraints. §15 testing → each task + Task 12. §16-17 covered by Non-Goals/Global Constraints. ✔
4. **Placeholder scan:** No TBD/TODO/vague phrases; every test step states scenario + expected result. ✔
5. **Type consistency:** `GrpcMockConfig`/`GrpcStub`/`GrpcMatch`/`GrpcResponse`/`GrpcResponseMessage` names consistent across Tasks 3,5,6,8,10,11. `stubs_by_method`/`method_meta` consistent (Tasks 6,7). `run_grpc`/`grpc_listen`/`grpc_server_factory` consistent (Tasks 8,10). Event names `grpc.hit`/`grpc.unmatched`/`grpc.error` and counters `grpc_hits`/`grpc_unmatched`/`grpc_errors` consistent (Tasks 7,8,9). `parse_grpc_status` returns `(code, name)` consistently (Tasks 2,3,5). ✔
