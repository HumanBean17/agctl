# `agctl grpc` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a first-class in-tree `agctl grpc` transport (`grpc call` + `grpc healthcheck`) under a new `grpc` extra — reflection-first/descriptor-fallback proto resolution, JSON↔protobuf serialization, all four call types (stdin/stdout NDJSON for stream input/output), unary assertion reuse, and gRPC-status-as-result semantics — plus `grpc-services`/`grpc-methods` discovery.

**Architecture:** A fifth transport structurally identical to http/kafka/db/logs. `GrpcClient` (`agctl/clients/grpc_client.py`) lazy-imports `grpcio`/`grpcio-tools`/`google.protobuf`, resolves proto definitions **reflection-first with a config-descriptor fallback** into one `descriptor_pool`, builds message classes dynamically (no generated `_pb2`), and exposes the four call types. `agctl/commands/grpc_commands.py` holds the `grpc call` / `grpc healthcheck` Click commands + the `new_grpc_client` test seam. Unary + client-streaming use the standard `@envelope` core split; server-streaming + bidi are **streaming exceptions** (NDJSON out) that also read **NDJSON in on stdin** for request streams. gRPC status is a *result* (`ok:true`, exit 0) unless an assertion fails — mirroring HTTP 4xx. A gRPC-specific assertion evaluator reuses the jq/subset/equals primitives. `grpc:` config section + pydantic models; `grpc-services`/`grpc-methods` discover categories.

**Tech Stack:** Python ≥3.11, Click, Pydantic v2, PyYAML, `grpcio`/`grpcio-tools`/`grpcio-health-checking`/`protobuf` (new `grpc` extra, lazy-imported), `jq` (bundled in the extra, lazy), pytest + `click.testing.CliRunner`.

**Spec:** [`docs/superpowers/specs/active/2026-07-09-grpc-support-design.md`](../specs/active/2026-07-09-grpc-support-design.md)

## Global Constraints

These apply to every task; each task's requirements implicitly include them. They are the spec's D1–D13 plus three plan-level refinements surfaced by reading the current code (flagged).

- **Additive only (D1).** No existing flag, field, exit code, output shape, command, or entry-point changes. New `grpc` group, new `grpc:` config section, two new discover categories, new `grpc` extra. No `version` bump (additive under major `2`).
- **In-tree, not a plugin (D1/D13).** Ships in the core package under the `grpc` extra. `GrpcClient` is a **plain DI-constructed class** — no `BUILTIN_DRIVERS`, no entry-point group, no `Protocol` (gRPC is one wire protocol, not a backend family). *(Refinement: the spec's §8/§10 framing leaned on the db/log client-dispatch pattern; the plan drops that — GrpcClient has no `type` dispatch. The §9.3 `Plugin` Protocol remains the seam for genuinely different future protocols.)*
- **Reflection-first, descriptor-fallback (D2).** `reflection: auto` (default) or `on` → query the server reflection service; `off`, or reflection `UNIMPLEMENTED` under `auto` → load config descriptors (`proto` glob compiled via `grpc_tools.protoc`, or a pre-compiled `descriptor_set` blob). Reflection miss + no descriptors → `ConfigError` (exit 2).
- **JSON in / JSON out (D3).** Request authored as JSON (`--message` / template `message`); `json_format.ParseDict(json, msg, ignore_unknown_fields=False)` serializes. Response decoded via `json_format.MessageToDict`. Message classes from `message_factory.GetMessageClass(message_desc)` over the resolved `descriptor_pool` — **no generated `_pb2` modules**. `ParseDict` unknown-field/type-mismatch → `ConfigError` (exit 2) pre-call.
- **All four call types; call-type auto-detected from the descriptor (D4).** `MethodDescriptor.client_streaming`/`server_streaming` → `unary` (neither) / `server_stream` / `client_stream` / `bidi`. The emitted `call_type` token is one of `"unary" | "server_stream" | "client_stream" | "bidi"`.
- **Bidirectional NDJSON symmetry (D4).** Server-stream/bidi emit one JSON object per response message (NDJSON) + a `summary` line (streaming exceptions, mirror `logs tail`). Client-stream/bidi **consume NDJSON on stdin** (one request message per line, EOF ends). `--message`/template `message` is valid only for unary/server-stream; stdin is valid only for client-stream/bidi; mismatch → `ConfigError` (exit 2). Unary/client-stream return one envelope (the single result).
- **gRPC status is a result, not an error (D6).** A non-OK status → `ok:true`, exit `0`, status captured in `result.status = {code, name, message}` — unless an assertion flag fails. Only `--status`/`--contains`/`--match`/`--jq-path`/`--equals` mismatch → `AssertionFailure` (exit 1).
- **Error mapping.** `grpc.RpcError` with `code() == DEADLINE_EXCEEDED` → `OperationTimeout` (exit 1, the `--timeout` budget). Any other `grpc.RpcError` → captured into `result.status` (ok path, D6). A non-`RpcError` exception during a call → `ConnectionFailure` (exit 2). Missing `grpc` extra → `ConfigError` pointing at `pip install 'agctl[grpc]'`. Unknown service/method → `TemplateNotFound` (exit 2).
- **Unary assertions reuse primitives via a NEW gRPC evaluator (D7).** *(Refinement: the spec's Affects line said `assertions.py (reuse only)`; that is inaccurate — `evaluate_http_assertions` is HTTP-shaped, reading `result["status_code"]` (int) / `result["body"]` and comparing `--status` as an int.)* The plan ADDS to `agctl/assertions.py`: `validate_grpc_assertion_args(*, status, contains, match, jq_path, equals)`, `evaluate_grpc_assertions(result, *, status, contains, match, jq_path, equals)`, and a gRPC status name↔code map. They reuse `compile_jq`/`jq_bool`/`jq_value`/`json_subset`/`parse_equals`/`type_aware_equal`/`_response_body_snapshot` unchanged. `--match` is rooted at the **whole gRPC result envelope**; `--contains`/`--jq-path` at `result["message"]`; `--status` compares against `result["status"]` (name or 0–16 number).
- **gRPC metadata (D8).** `--metadata key=value` (repeatable, caller wins) + template `metadata`; initial metadata sent, **trailers** captured in the result. Metadata keys are lowercased on the wire (gRPC rule).
- **TLS/plaintext (D9).** `use_tls` (default `false` → plaintext h2c) + optional `tls` block (`ca_location`/`certificate_location`/`key_location`/`override_authority`), mirroring `kafka.ssl` including empty-string-counts-as-unset.
- **No `@model_validator` anywhere in the codebase.** *(Refinement of spec §7.2.)* The `GrpcDescriptorSource` "exactly one of `proto`/`descriptor_set`" rule is a **structural check in `validator.py`** (Task 2), not a pydantic cross-field validator. Per-field rules (e.g. `reflection` literal, `use_tls` bool) use `@field_validator` (house style) or `Literal` types.
- **Factory seam location.** *(Refinement of spec §8.3.)* `new_grpc_client(target)` is defined in **`agctl/commands/grpc_commands.py`** (mirrors `new_logs_client`/`new_db_client`), NOT in the client module. Tests monkeypatch `grpc_commands.new_grpc_client`. The factory takes the resolved `GrpcTarget` config object and lazy-imports `GrpcClient`.
- **Exit codes unchanged:** `0` ok / `1` assertion / `2` config-or-env.
- **Reuse, do not re-implement:** `envelope`/`load_config_or_raise` from `agctl/command.py`; `emit` from `agctl/output.py`; `fill_placeholders`/`deep_merge` from `agctl/resolution.py`; `parse_params` from `agctl/params.py`; the jq/subset/equals primitives from `agctl/assertions.py`; the `http ping`/`logs tail` signal + NDJSON + manual-startup-envelope streaming pattern.
- **Unit tests run without optional extras** (ARCHITECTURE §12). The `grpc_client` module imports cleanly without `grpcio`/`protobuf` (lazy import). Command-layer tests inject a fake client via `new_grpc_client` (no grpcio/protobuf). Tests that genuinely exercise protobuf/grpcio use `pytest.importorskip("google.protobuf")` / `("grpc")` so they skip when the extra is absent and run in CI (Task 3 adds the extra to CI).
- **Branch / harness:** worktree `.claude/worktrees/feat+grpc`, branch `worktree-feat+grpc`. Run tests via the shared venv (resolves worktree `agctl`): `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest`. For protobuf/grpcio-dependent tasks, first `pip install -e ".[grpc]"` in that venv. Commit after every task. End commit messages with `Co-Authored-By: Claude <noreply@anthropic.com>`.

---

## File Structure

**Created:**
- `agctl/clients/grpc_client.py` — `GrpcClient` (lazy-import grpcio/protobuf; descriptor resolution; JSON↔protobuf; 4 call types; healthcheck) + DTO dataclasses (`GrpcStatus`, `GrpcUnaryResult`, `GrpcStreamMessage`, `GrpcHealthResult`).
- `agctl/commands/grpc_commands.py` — `grpc_call` / `grpc_healthcheck` Click commands + cores/envelopes + `new_grpc_client` factory + helpers (`_resolve_target`, `_parse_metadata`, call-type dispatch, stdin/stdout NDJSON).
- `tests/fixtures/echo.proto` — a tiny service with unary/server-stream/client-stream/bidi methods (the descriptor test fixture).
- `tests/fixtures/echo_descriptor.pb` — pre-compiled `FileDescriptorSet` for `echo.proto` (generated once in Task 5; committed; no protoc at test time).
- `tests/unit/test_grpc_client.py` — DTO, descriptor-resolution, JSON↔protobuf, call-type, and healthcheck client tests.
- `tests/unit/test_grpc_assertions.py` — `validate_grpc_assertion_args` + `evaluate_grpc_assertions` tests.
- `tests/unit/test_grpc_commands.py` — `grpc call` / `grpc healthcheck` command tests (fake-client injection).
- `tests/integration/test_grpc_commands.py` — in-process grpcio server E2E (self-skipping).

**Modified:**
- `agctl/config/models.py` — add `GrpcTls`/`GrpcTarget`/`GrpcDescriptorSource`/`GrpcTemplate`/`GrpcConfig`; add `grpc` field to `Config`.
- `agctl/config/validator.py` — add a `grpc` validation pass (template→target refs, descriptor-source exactly-one, missing-description warnings).
- `agctl/assertions.py` — add `validate_grpc_assertion_args`, `evaluate_grpc_assertions`, gRPC status name↔code map.
- `agctl/commands/discover_commands.py` — add `grpc-services` / `grpc-methods` categories.
- `agctl/cli.py` — register the `grpc` group + subcommands.
- `agctl/data/sample-config.yaml` + `tests/fixtures/agctl.yaml` — add a `grpc:` section.
- `pyproject.toml` — add `grpc` extra; add `grpc` to the `integration` aggregate.
- `.github/workflows/test.yml` — add `grpc` to the CI install line.
- `tests/unit/test_models.py` — add grpc-model tests.
- `tests/unit/test_config_validator.py` (or wherever validator tests live) — add grpc validation tests.
- `tests/unit/test_discover_command.py` — add grpc category tests.
- `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/agctl*`, `README.md` — docs sync (final task, via `docs-watcher` + targeted edits).

---

### Task 1: Config models — `GrpcTls` / `GrpcTarget` / `GrpcDescriptorSource` / `GrpcTemplate` / `GrpcConfig`

Foundation: the pydantic models every later task reads. `${ENV}` interpolation and `AGCTL_GRPC__*` env overrides apply automatically via the existing loader; no loader change.

**Files:**
- Modify: `agctl/config/models.py` (add five models after `LogsConfig`, before `Config`; add one field to `Config`)
- Test: `tests/unit/test_models.py`

**Interfaces:**
- Consumes: `BaseModel`, `Field`, `field_validator` from pydantic; `Literal` from `typing` (all already imported in `models.py`).
- Produces (exact field names + types + defaults):
  - `GrpcTls(BaseModel)`: `ca_location: str | None = None`; `certificate_location: str | None = None`; `key_location: str | None = None`; `override_authority: str | None = None`.
  - `GrpcTarget(BaseModel)`: `address: str`; `use_tls: bool = False`; `tls: GrpcTls | None = None`; `reflection: Literal["auto", "on", "off"] = "auto"`.
  - `GrpcDescriptorSource(BaseModel)`: `proto: str | None = None`; `include_paths: list[str] = Field(default_factory=list)`; `descriptor_set: str | None = None`. (The "exactly one of `proto`/`descriptor_set`" rule is enforced structurally in Task 2, NOT here — no `@model_validator` exists in the codebase.)
  - `GrpcTemplate(BaseModel)`: `description: str | None = None`; `target: str`; `service: str`; `method: str`; `metadata: dict[str, str] = Field(default_factory=dict)`; `message: dict | None = None`.
  - `GrpcConfig(BaseModel)`: `targets: dict[str, GrpcTarget] = Field(default_factory=dict)`; `descriptors: list[GrpcDescriptorSource] = Field(default_factory=list)`; `templates: dict[str, GrpcTemplate] = Field(default_factory=dict)`.
  - `Config` gains: `grpc: GrpcConfig = Field(default_factory=GrpcConfig)` (placed after `logs`).

- [ ] **Step 1: Write the failing model tests**

Add to `tests/unit/test_models.py`:
1. `test_grpc_tls_defaults`: `GrpcTls()` has all four fields `None`.
2. `test_grpc_target_defaults`: `GrpcTarget(address="localhost:50051")` has `use_tls is False`, `tls is None`, `reflection == "auto"`. `GrpcTarget(address="x", use_tls=True, reflection="off")` keeps those values.
3. `test_grpc_target_rejects_bad_reflection`: `GrpcTarget(address="x", reflection="maybe")` raises `ValidationError`.
4. `test_grpc_descriptor_source_all_none`: `GrpcDescriptorSource()` has `proto is None`, `descriptor_set is None`, `include_paths == []`. (Both-set and both-None are NOT rejected here — that is Task 2's structural rule.)
5. `test_grpc_template_defaults`: `GrpcTemplate(target="t", service="s.v1.S", method="M")` has `description is None`, `metadata == {}`, `message is None`.
6. `test_grpc_config_empty_default`: `GrpcConfig()` has `targets == {}`, `descriptors == []`, `templates == {}`.
7. `test_config_has_grpc_field`: `Config(version="2")` has `.grpc` a `GrpcConfig` (empty). `Config(version="2")` built via `model_validate` with `{"version":"2","grpc":{"targets":{"svc":{"address":"h:1"}}}}` yields `cfg.grpc.targets["svc"].address == "h:1"` and `cfg.grpc.targets["svc"].reflection == "auto"` (default applied).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_models.py -q`
Expected: FAIL — `GrpcTls`/`GrpcConfig` undefined (ImportError/AttributeError).

- [ ] **Step 3: Add the five models + the Config field**

In `agctl/config/models.py`: define `GrpcTls`, `GrpcTarget`, `GrpcDescriptorSource`, `GrpcTemplate`, `GrpcConfig` (after `LogsConfig`, before `Config`), and add `grpc: GrpcConfig = Field(default_factory=GrpcConfig)` to `Config` (after `logs`). Use `Literal["auto","on","off"]` for `reflection`; no cross-field validator.

- [ ] **Step 4: Run the model tests + full unit suite (regression)**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_models.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS — adding an optional config field with a default is backward-compatible.

- [ ] **Step 5: Commit**

Run: `git add agctl/config/models.py tests/unit/test_models.py && git commit -m "feat(config): add grpc targets/descriptors/templates models"`

---

### Task 2: Config validation — `validate_config` grpc pass

Pure structural checks (no client needed): template→target dangling refs, descriptor-source "exactly one" rule, missing-description warnings.

**Files:**
- Modify: `agctl/config/validator.py`
- Test: `tests/unit/test_config_validator.py` (create if absent; otherwise extend)

**Interfaces:**
- Consumes: `Config`, `GrpcDescriptorSource` from `agctl/config/models.py`; the existing `validate_config(cfg) -> tuple[list[dict], list[dict]]` (returns `(errors, warnings)`, each `{"path": str, "message": str}`); the existing `_missing_description(value) -> bool` helper.
- Produces — append to `validate_config(cfg)` (after the existing checks), mirroring the DB template→connection pattern verbatim:
  - **template→target (error):** for each `(name, tpl)` in `cfg.grpc.templates.items()`: if `tpl.target not in cfg.grpc.targets` → `errors.append({"path": f"grpc.templates.{name}.target", "message": f"gRPC template references unknown target '{tpl.target}'"})`.
  - **descriptor-source exactly-one (error):** for each `(i, src)` in `enumerate(cfg.grpc.descriptors)`: let `has_proto = src.proto is not None`, `has_ds = src.descriptor_set is not None`. If `has_proto == has_ds` (both or neither) → `errors.append({"path": f"grpc.descriptors[{i}]", "message": "each grpc.descriptors entry must set exactly one of 'proto' or 'descriptor_set'"})`.
  - **missing-description (warning):** for each `(name, tpl)` in `cfg.grpc.templates.items()`: if `_missing_description(tpl.description)` → `warnings.append({"path": f"grpc.templates.{name}", "message": "missing description (discovery degrades without it)"})`. (Mirror the existing templates/logs warning text.)

- [ ] **Step 1: Write the failing validator tests**

In `tests/unit/test_config_validator.py` (create if missing — mirror an existing validator test's `Config(...)` construction):
1. `test_grpc_template_unknown_target_is_error`: a `Config` with `grpc.templates.t1.target="missing"` and no matching target → `validate_config` errors include `{"path":"grpc.templates.t1.target", ...}`; a matching target contributes no such error.
2. `test_grpc_descriptor_both_set_is_error`: `grpc.descriptors=[{proto:"a.proto", descriptor_set:"b.pb"}]` → error at `grpc.descriptors[0]`.
3. `test_grpc_descriptor_neither_set_is_error`: `grpc.descriptors=[{}]` → error at `grpc.descriptors[0]`.
4. `test_grpc_descriptor_exactly_one_ok`: `grpc.descriptors=[{proto:"a.proto", include_paths:["."]}` and `[{descriptor_set:"b.pb"}]` each contribute no error.
5. `test_grpc_template_missing_description_warns`: a template with `description=None` → warning at `grpc.templates.<name>`; with a description → no warning.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_config_validator.py -k grpc -q`
Expected: FAIL — no grpc checks present (errors/warnings empty).

- [ ] **Step 3: Add the grpc validation pass to `validate_config`**

Edit `agctl/config/validator.py` per the Produces contract (mirror the existing DB template→connection block style).

- [ ] **Step 4: Run validator tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_config_validator.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS (existing fixtures have no `grpc:` section → empty `grpc` → no new errors).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/validator.py tests/unit/test_config_validator.py && git commit -m "feat(config): validate grpc.templates target refs + descriptor source shape"`

---

### Task 3: Packaging — `grpc` extra + CI

Tiny but load-bearing: makes `pip install -e ".[grpc]"` work so later protobuf/grpcio tasks can run.

**Files:**
- Modify: `pyproject.toml`
- Modify: `.github/workflows/test.yml`

**Interfaces:**
- Produces:
  - `pyproject.toml` `[project.optional-dependencies]` gains: `grpc = ["grpcio>=1.62", "grpcio-tools>=1.62", "grpcio-health-checking>=1.62", "protobuf>=4.25", "jq>=1.6"]`.
  - The `integration` aggregate changes from `"agctl[db,kafka,http]"` to `"agctl[db,kafka,http,grpc]"`.
  - `.github/workflows/test.yml` install line changes from `pip install -e ".[dev,http,jq,kafka,db,logs]"` to `pip install -e ".[dev,http,jq,kafka,db,logs,grpc]"`.
  - **No new `[project.entry-points]` group** (D13).

- [ ] **Step 1: Edit pyproject + CI per the Produces contract**

Apply the three edits above.

- [ ] **Step 2: Install the extra in the worktree venv and verify resolution**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pip install -e ".[grpc]"` (from the worktree root)
Expected: succeeds; then `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -c "import grpc, grpc_tools, grpc_health, google.protobuf; print('grpc ok')"` prints `grpc ok`.

- [ ] **Step 3: Run the unit suite (regression)**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS (no behavior change).

- [ ] **Step 4: Commit**

Run: `git add pyproject.toml .github/workflows/test.yml && git commit -m "build: add grpc extra (grpcio/grpcio-tools/grpcio-health-checking/protobuf/jq)"`

---

### Task 4: `GrpcClient` DTOs + skeleton (lazy import + DI seams)

The DTOs every layer uses + a constructable client whose heavy libs are lazy-imported. Pure-logic unit tests run without `grpcio`/`protobuf` by injecting `channel` + `descriptor_pool`.

**Files:**
- Create: `agctl/clients/grpc_client.py`
- Test: `tests/unit/test_grpc_client.py`

**Interfaces:**
- Consumes: `ConfigError` from `agctl/errors.py`; `GrpcTarget` from `agctl/config/models.py`.
- Produces (exact):
  - Dataclasses (`@dataclass`, `from __future__ import annotations`):
    - `GrpcStatus`: `code: int`, `name: str`, `message: str = ""`.
    - `GrpcUnaryResult`: `target: str`, `service: str`, `method: str`, `call_type: str`, `status: GrpcStatus`, `message: dict | None`, `initial_metadata: dict`, `trailers: dict`.
    - `GrpcStreamMessage`: `message: dict`, `trailers: dict | None`.
    - `GrpcHealthResult`: `target: str`, `address: str`, `status: str`, `note: str | None = None`.
  - `class GrpcClient`:
    - `__init__(self, target: GrpcTarget, *, channel=None, descriptor_pool=None, timeout_seconds: float | None = None)`: store `self._target = target`, `self._channel = channel`, `self._pool = descriptor_pool`, `self._timeout = timeout_seconds`. **Lazy grpcio import only when `channel is None`**: if `channel is None`, `try: import grpc except ImportError: raise ConfigError("gRPC support requires the 'grpc' extra: pip install 'agctl[grpc]'", {}) from exc`; build the channel from `target.address` + TLS/plaintext (plaintext h2c when `not target.use_tls`; TLS channel credentials from `target.tls` when `use_tls`, applying `override_authority`). Store `self._grpc = grpc`. If `channel` was injected, do NOT import grpcio (keeps DI unit tests dep-free).
    - `resolve_descriptors`, `find_method`, `call_unary`, `call_server_stream`, `call_client_stream`, `call_bidi`, `healthcheck`: raise `NotImplementedError` for now (Tasks 5/6/7 fill them).
  - The module imports cleanly with neither `grpcio` nor `protobuf` installed (all heavy imports are inside methods/`__init__`'s `channel is None` branch).

- [ ] **Step 1: Write the failing DTO + skeleton tests**

Create `tests/unit/test_grpc_client.py` (no grpcio/protobuf required for these):
1. `test_status_and_result_dataclasses`: construct `GrpcStatus(code=0, name="OK")`, `GrpcUnaryResult(target="t", service="s", method="m", call_type="unary", status=GrpcStatus(0,"OK"), message={"a":1}, initial_metadata={}, trailers={})`, `GrpcStreamMessage(message={"a":1}, trailers=None)`, `GrpcHealthResult(target="t", address="h:1", status="SERVING")`; assert every field round-trips and defaults apply (`status.message==""`, `result.message` may be `None`, `health.note is None`).
2. `test_client_uses_injected_channel_and_skips_grpcio_import`: `GrpcClient(GrpcTarget(address="h:1"), channel=object(), descriptor_pool=object())` constructs without importing grpcio (assert `getattr(client, "_grpc", None) is None` — the grpcio module attr is not set when a channel is injected) and `client._channel is the injected object` and `client._pool is the injected object`.
3. `test_client_missing_extra_when_no_channel`: monkeypatch `builtins.__import__` (or `sys.modules`) so `import grpc` raises `ImportError`; `GrpcClient(GrpcTarget(address="h:1"))` (no injected channel) → raises `ConfigError` whose message contains `agctl[grpc]`.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -q`
Expected: FAIL — module/symbols undefined (ImportError).

- [ ] **Step 3: Implement DTOs + skeleton `GrpcClient`**

Create `agctl/clients/grpc_client.py` with the four dataclasses and `GrpcClient.__init__` per the Produces contract. Method bodies raise `NotImplementedError`. Keep the module import-free of grpcio/protobuf.

- [ ] **Step 4: Run the tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/grpc_client.py tests/unit/test_grpc_client.py && git commit -m "feat(grpc): GrpcClient skeleton + DTOs (lazy import, DI seams)"`

---

### Task 5: Descriptor resolution + `find_method` + call-type detection

The gRPC-specific core: learn proto definitions (reflection-first, descriptor-fallback) and locate a method. Uses the bundled `echo_descriptor.pb`. Protobuf-dependent tests use `pytest.importorskip("google.protobuf")`.

**Files:**
- Modify: `agctl/clients/grpc_client.py` (`resolve_descriptors`, `find_method`)
- Create: `tests/fixtures/echo.proto`, `tests/fixtures/echo_descriptor.pb`
- Test: `tests/unit/test_grpc_client.py`

**Interfaces:**
- Consumes: Task 4 (`GrpcClient.__init__`, `self._channel`, `self._pool`, `self._grpc`, `self._target`); `ConfigError`/`TemplateNotFound` from `agctl/errors.py`; `grpc_tools.protoc` (only for the one-time fixture generation below), `google.protobuf.descriptor_pool`, `google.protobuf.message_factory`, grpc reflection stubs (all lazy/importorskip).
- Produces:
  - `resolve_descriptors(self) -> "DescriptorPool"`:
    - If `self._pool is not None` (injected) → return it (test seam / pre-resolved).
    - Else if `self._target.reflection in ("auto","on")`: query the server reflection service over `self._channel` for the list of services, then each file descriptor proto; build a `descriptor_pool.DescriptorPool` and add each file descriptor set. On reflection `UNIMPLEMENTED` (an RpcError with that code): if `reflection == "on"` → `ConfigError("reflection requested but the server does not implement it; set grpc.targets.<name>.reflection: off and supply grpc.descriptors", {"target": self._target.address})`; if `reflection == "auto"` → fall through to the descriptor path.
    - Descriptor fallback (when `reflection == "off"`, or auto-fell-through): requires the command to have passed config descriptors (see Task 9 wiring) — accept a `descriptors: list[GrpcDescriptorSource]` via an additional `__init__` kwarg `descriptors: list | None = None` (store `self._descriptors`). For each source: if `descriptor_set` set → read the binary blob and `pool.add(serialized_pb=<bytes>)` (merge into one pool); if `proto` set → compile via `grpc_tools.protoc` to a `FileDescriptorSet` in memory and add it. If reflection fell through and `self._descriptors` is empty → `ConfigError("no gRPC descriptors available and server reflection is unavailable; configure grpc.descriptors", {"target": self._target.address})`.
  - `find_method(self, service: str, method: str) -> "MethodDescriptor"`: `pool = self.resolve_descriptors()`; look up `service_desc = pool.FindMessageTypeByName` is NOT the path — use `pool.FindServiceByName(service)` (a `ServiceDescriptor`); if `NotFound` → `TemplateNotFound(f"Unknown gRPC service: {service}", {"service": service})`; `method_desc = service_desc.methods_by_name.get(method)`; if `None` → `TemplateNotFound(f"Unknown gRPC method: {method} on {service}", {"service": service, "method": method})`; return `method_desc`.
  - **Call-type detection (free helper or staticmethod) `call_type_of(method_desc) -> str`:** `"bidi"` if `method_desc.client_streaming and method_desc.server_streaming`; `"server_stream"` if `method_desc.server_streaming and not method_desc.client_streaming`; `"client_stream"` if `method_desc.client_streaming and not method_desc.server_streaming`; else `"unary"`. (Token set pinned by Global Constraints.)
  - `GrpcClient.__init__` gains `descriptors: list | None = None` kwarg (default `None`, stored as `self._descriptors`).
- Produces (fixture): `tests/fixtures/echo.proto` declares `package echo; service Echo { rpc Unary(Request) returns (Response); rpc ServerStream(Request) returns (stream Response); rpc ClientStream(stream Request) returns (Response); rpc Bidi(stream Request) returns (stream Response); }` with simple `Request { string msg = 1; }` and `Response { string msg = 1; int32 n = 2; }` messages. `tests/fixtures/echo_descriptor.pb` is the compiled `FileDescriptorSet` (generated once — see Step 1).

- [ ] **Step 1: Author `echo.proto` + generate `echo_descriptor.pb`**

Write `tests/fixtures/echo.proto` per the Produces contract. Then generate the descriptor set ONCE (requires the Task-3 extra):
Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m grpc_tools.protoc --include_imports --proto_path=tests/fixtures --descriptor_set_out=tests/fixtures/echo_descriptor.pb tests/fixtures/echo.proto`
Expected: `tests/fixtures/echo_descriptor.pb` exists (binary). Commit it.

- [ ] **Step 2: Write the failing descriptor/find_method tests**

Add to `tests/unit/test_grpc_client.py`, each guarded with `pytest.importorskip("google.protobuf")` at the top of the test:
1. `test_resolve_descriptors_uses_injected_pool`: `GrpcClient(GrpcTarget(address="h:1"), channel=object(), descriptor_pool=SENTINEL)` → `resolve_descriptors() is SENTINEL` (no reflection call).
2. `test_find_method_from_echo_descriptor`: build a `descriptor_pool.DescriptorPool`, add `tests/fixtures/echo_descriptor.pb` bytes, construct `GrpcClient(GrpcTarget(address="h:1", reflection="off"), channel=object(), descriptor_pool=that_pool)`; `find_method("echo.Echo", "ServerStream")` returns a method descriptor; `call_type_of(it) == "server_stream"`; `find_method("echo.Echo","Unary")` → `"unary"`; `"ClientStream"` → `"client_stream"`; `"Bidi"` → `"bidi"`.
3. `test_find_method_unknown_service_and_method`: with the echo pool, `find_method("echo.Missing","Unary")` → `TemplateNotFound` (detail `service`); `find_method("echo.Echo","Nope")` → `TemplateNotFound` (detail `method`).
4. `test_resolve_reflection_unimplemented_off_path_no_descriptors_is_configerror`: with `reflection="off"`, an injected fake reflection-less channel, and no `descriptors` → `resolve_descriptors()` raises `ConfigError` mentioning `grpc.descriptors`. (Inject `descriptor_pool=None` and `descriptors=None`; do not call a real server.)
- NOTE: a true reflection-success test lives in integration (Task 13). Unit tests cover the injected-pool + fallback + miss paths.

- [ ] **Step 3: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -k "resolve_descriptors or find_method or call_type" -q`
Expected: FAIL — `resolve_descriptors`/`find_method` raise `NotImplementedError` (or skip if protobuf absent; after Task 3 they run).

- [ ] **Step 4: Implement `resolve_descriptors`, `find_method`, `call_type_of`**

Implement per the Produces contract. Add the `descriptors` kwarg to `__init__`. Use `pool.FindServiceByName`/`service_desc.methods_by_name`. Keep grpcio/protobuf imports inside the methods (lazy).

- [ ] **Step 5: Run the client tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 6: Commit**

Run: `git add agctl/clients/grpc_client.py tests/fixtures/echo.proto tests/fixtures/echo_descriptor.pb tests/unit/test_grpc_client.py && git commit -m "feat(grpc): descriptor resolution (reflection+fallback) + find_method + call-type detection"`

---

### Task 6: JSON↔protobuf + the four call types + error mapping

The transport calls. Dynamic stubs via `channel.unary_unary`/`unary_stream`/`stream_unary`/`stream_stream` with serializer/deserializer built from message classes; JSON in/out via `json_format`; `grpc.RpcError` mapped per Global Constraints.

**Files:**
- Modify: `agctl/clients/grpc_client.py` (`call_unary`, `call_server_stream`, `call_client_stream`, `call_bidi`)
- Test: `tests/unit/test_grpc_client.py`

**Interfaces:**
- Consumes: Task 5 (`resolve_descriptors`, `find_method`, `call_type_of`); `ConfigError`/`OperationTimeout`/`ConnectionFailure` from `agctl/errors.py`; lazy `google.protobuf.json_format` (`ParseDict`/`MessageToDict`) + `message_factory.GetMessageClass` + `grpc` (`StatusCode`, `RpcError`).
- Produces — private serializer/deserializer builders (signatures only):
  - `_msg_class(self, message_desc)` → `message_factory.GetMessageClass(message_desc)` (lazy import).
  - `_serialize(message_desc)`: returns a callable `dict -> bytes` = `json_format.ParseDict(d, cls(), ignore_unknown_fields=False).SerializeToString()`. (Unknown field/type mismatch inside `ParseDict` raises — the call methods convert that to `ConfigError`.)
  - `_deserialize(message_desc)`: returns `bytes -> dict` = `json_format.MessageToDict(cls.FromString(b))`.
  - `call_unary(self, service, method, message_json: dict, *, metadata: dict | None = None, timeout: float | None = None) -> GrpcUnaryResult`:
    - `md = self.find_method(service, method)`; assert `call_type_of(md) == "unary"` (mismatch → `ConfigError`, but the command layer guards this; client trusts its caller).
    - Build request via `_serialize(md.input_type)`; the invoker `fn = self._channel.unary_unary(f"/{service}/{method}", request_serializer=ser, response_deserializer=_deserialize(md.output_type))`.
    - `try: resp = fn(request_bytes, metadata=<(metadata items) or None>, timeout=timeout or self._timeout) except grpc.RpcError as e: <map per below>`.
    - RpcError mapping: if `e.code() == grpc.StatusCode.DEADLINE_EXCEEDED` → `raise OperationTimeout(message=str(e), detail={})` from e; else build `status = GrpcStatus(code=<numeric>, name=<code name>, message=e.details() or "")`, `msg=None`, and return `GrpcUnaryResult(... status=status, message=None, initial_metadata={}, trailers={})` (non-OK status = result, D6). On any non-RpcError Exception → `raise ConnectionFailure(message=str(exc)) from exc`.
    - Success (no exception): `resp` is the deserialized dict; return `GrpcUnaryResult(target=self._target... , service, method, call_type="unary", status=GrpcStatus(0,"OK"), message=resp, initial_metadata=<captured>, trailers=<captured>)`. (Capturing initial metadata/trailers from a unary call may require the call to also return trailing metadata — use grpcio's trailing-metadata accessors; if a plain unary unary returns only the message, wrap so the invoker also returns `(message, call)` — acceptable implementation detail. The result MUST populate `initial_metadata` and `trailers` dicts, even if empty.)
  - `call_server_stream(self, service, method, message_json, *, metadata=None, timeout=None) -> Iterator[GrpcStreamMessage]`: `fn = channel.unary_stream(...)`; iterate `fn(request_bytes, metadata=..., timeout=...)`; for each response dict `yield GrpcStreamMessage(message=resp, trailers=None)`; the FINAL message (or post-loop) carries `trailers=<captured>`. A `DEADLINE_EXCEEDED` RpcError mid-stream → `OperationTimeout`; any other RpcError → end the stream (yield nothing more; the command's `summary` carries the status) — specifically: catch RpcError, set a local `status`, stop iterating. (The command converts the status into the summary line.)
  - `call_client_stream(self, request_json_iter, *, service, method, metadata=None, timeout=None) -> GrpcUnaryResult`: `fn = channel.stream_unary(...)`; `resp = fn((self._serialize(md.input_type)(d) for d in request_json_iter), metadata=..., timeout=...)`; same RpcError mapping as unary; return `GrpcUnaryResult(... call_type="client_stream", ...)`.
  - `call_bidi(self, request_json_iter, *, service, method, metadata=None, timeout=None) -> Iterator[GrpcStreamMessage]`: `fn = channel.stream_stream(...)`; iterate over `fn((ser(d) for d in request_json_iter), ...)` yielding `GrpcStreamMessage` per response (trailers on the final); RpcError mapping as server-stream.
- Produces (numeric code + name): use `grpc.StatusCode` members (each has `.value[0]` = numeric code) to build `GrpcStatus.code`/`name`; OK = `(0, "OK")`.

- [ ] **Step 1: Write the failing call-type tests (protobuf-guarded, fake channel)**

Add to `tests/unit/test_grpc_client.py`, each starting with `pytest.importorskip("google.protobuf")` and using the echo descriptor pool (Task 5) + a **fake channel** whose `unary_unary/unary_stream/stream_unary/stream_stream` return canned invokers:
1. `test_call_unary_success`: fake `channel.unary_unary("/echo.Echo/Unary", ...)` returns an invoker that returns the deserialized `{"msg":"hi","n":3}`; `call_unary("echo.Echo","Unary",{"msg":"hi"})` returns a `GrpcUnaryResult` with `status.name=="OK"`, `status.code==0`, `message=={"msg":"hi","n":3}`, `call_type=="unary"`.
2. `test_call_unary_nonok_status_is_result`: invoker raises a fake `RpcError` with `.code()==grpc.StatusCode.NOT_FOUND` and `.details()=="nope"`; `call_unary(...)` returns a result with `status.name=="NOT_FOUND"`, `message is None`, and raises NOTHING (ok path).
3. `test_call_unary_deadline_is_operation_timeout`: invoker raises `RpcError` `.code()==DEADLINE_EXCEEDED` → `OperationTimeout` is raised.
4. `test_call_unary_bad_request_json_is_configerror`: the serializer's `ParseDict` raises on an unknown field — `call_unary("echo.Echo","Unary",{"unknown_field":1})` → `ConfigError` (pre-call; the fake channel is never invoked).
5. `test_call_server_stream_yields_messages`: fake `channel.unary_stream(...)` invoker returns an iterator of 2 deserialized dicts; `list(call_server_stream("echo.Echo","ServerStream",{"msg":"x"}))` == 2 `GrpcStreamMessage` with the expected `message` dicts.
6. `test_call_client_stream_returns_single_result`: fake `channel.stream_unary(...)` invoker returns `{"msg":"done"}`; `call_client_stream(iter([{"msg":"a"},{"msg":"b"}]), service="echo.Echo", method="ClientStream")` returns one `GrpcUnaryResult` with `call_type=="client_stream"`, `message=={"msg":"done"}`.
7. `test_call_bidi_yields_messages`: fake `channel.stream_stream(...)` returns an iterator of 2 dicts; `list(call_bidi(iter([{"msg":"a"}]), service="echo.Echo", method="Bidi"))` == 2 `GrpcStreamMessage`.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -k "call_" -q`
Expected: FAIL — methods raise `NotImplementedError`.

- [ ] **Step 3: Implement the serializer/deserializer builders + four call methods**

Implement per the Produces contract. Reuse `find_method`/`call_type_of` from Task 5. Map `grpc.RpcError` exactly as specified (DEADLINE_EXCEEDED→OperationTimeout; other→status result; non-RpcError→ConnectionFailure). Keep all protobuf/grpcio imports lazy.

- [ ] **Step 4: Run the client tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/grpc_client.py tests/unit/test_grpc_client.py && git commit -m "feat(grpc): JSON<->protobuf + four call types + RpcError/status mapping"`

---

### Task 7: `healthcheck` client method + `grpc_health`

Readiness via `grpc.health.v1.Health/Check`. `grpcio-health-checking` (added in Task 3) provides `HealthStub` + the status enum.

**Files:**
- Modify: `agctl/clients/grpc_client.py` (`healthcheck`)
- Test: `tests/unit/test_grpc_client.py`

**Interfaces:**
- Consumes: Task 4 (`GrpcClient`, `self._channel`); lazy `grpc_health.v1` (`HealthStub`, `health_pb2.HealthCheckRequest`, `HealthCheckResponse.ServingStatus`). `ConfigError`/`ConnectionFailure` from `agctl/errors.py`.
- Produces:
  - `healthcheck(self, service_name: str = "") -> GrpcHealthResult`:
    - Build a `HealthStub(self._channel)`; call `Check(HealthCheckRequest(service=service_name), timeout=self._timeout)`.
    - Map the response status enum to a string: `SERVING`/`NOT_SERVING`/`UNKNOWN` (use the enum `.name`).
    - On `grpc.RpcError` with code `UNIMPLEMENTED` → return `GrpcHealthResult(target=self._target..., address=self._target.address, status="UNKNOWN", note="health service UNIMPLEMENTED")` (NOT an error — mirror `check ready` on a missing health path).
    - On `grpc.RpcError` `DEADLINE_EXCEEDED` → `OperationTimeout`; other RpcError → `ConnectionFailure(message=str(e))`; non-RpcError → `ConnectionFailure`.

- [ ] **Step 1: Write the failing healthcheck test (protobuf/grpc-guarded)**

Add to `tests/unit/test_grpc_client.py`, guarded with `pytest.importorskip("grpc_health")`:
1. `test_healthcheck_serving`: a fake channel whose unary_unary returns the serialized bytes of a `HealthCheckResponse(HealthCheckResponse.SERVING)`; `healthcheck()` returns `GrpcHealthResult` with `status=="SERVING"`, `note is None`.
2. `test_healthcheck_unimplemented_is_unknown`: the fake invoker raises `RpcError(code=UNIMPLEMENTED)`; `healthcheck()` returns `status=="UNKNOWN"`, `note` mentions `UNIMPLEMENTED` (no exception).
3. `test_healthcheck_deadline_is_timeout`: `RpcError(DEADLINE_EXCEEDED)` → `OperationTimeout`.

- [ ] **Step 2: Run to verify it fails**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -k healthcheck -q`
Expected: FAIL — `NotImplementedError`.

- [ ] **Step 3: Implement `healthcheck`**

Implement per the Produces contract; lazy-import `grpc_health.v1`.

- [ ] **Step 4: Run the client tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_client.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/grpc_client.py tests/unit/test_grpc_client.py && git commit -m "feat(grpc): healthcheck client method (grpc.health.v1)"`

---

### Task 8: gRPC assertions — `validate_grpc_assertion_args` + `evaluate_grpc_assertions`

The unary assertion suite, gRPC-shaped. Reuses jq/subset/equals primitives; lives in `agctl/assertions.py` alongside the HTTP evaluator.

**Files:**
- Modify: `agctl/assertions.py`
- Test: `tests/unit/test_grpc_assertions.py` (new file)

**Interfaces:**
- Consumes: `compile_jq`, `jq_bool`, `jq_value`, `json_subset`, `parse_equals`, `type_aware_equal`, `_response_body_snapshot` (all existing in `assertions.py`); `AssertionFailure`/`ConfigError` from `agctl/errors.py`; `json` (stdlib).
- Produces:
  - `_GRPC_STATUS_BY_NAME: dict[str,int]` and `_GRPC_STATUS_BY_CODE: dict[int,str]` built from `grpc.StatusCode` (lazy import inside a helper, OR a hardcoded `{OK:0, CANCELLED:1, UNKNOWN:2, ...}` map — prefer lazy `grpc.StatusCode` iteration so names/codes stay authoritative). The map covers codes 0–16.
  - `validate_grpc_assertion_args(*, status, contains, match, jq_path, equals) -> None` — all-keyword; raises `ConfigError` (exit 2) on:
    - `jq_path`/`equals` XOR (exactly one set) → `ConfigError("--jq-path and --equals must be used together", {})`.
    - `contains is not None` and `json.loads(contains)` fails → `ConfigError("--contains must be valid JSON", {})`.
    - `status is not None` and not a valid gRPC code (not an int 0–16 in `_GRPC_STATUS_BY_CODE`, and not a name in `_GRPC_STATUS_BY_NAME`) → `ConfigError(f"--status must be a gRPC code name or number 0-16, got {status!r}", {})`.
    - Returns `None` otherwise (including all-None).
  - `evaluate_grpc_assertions(result: dict, *, status, contains, match, jq_path, equals) -> None` — `result` is the `grpc.call` result dict (`{target, service, method, call_type, status: {code,name,message}, message, initial_metadata, trailers}`). Early-return if all kwargs None. Collect failures (no short-circuit). On failures raise `AssertionFailure(f"gRPC response failed {len(failures)} assertion(s)", {"response": result, "failures": failures})`. Failure-entry shapes (pinned):
    - status: normalize the `status` arg to a code (name→code via map; int as-is); compare to `result["status"]["code"]`; on mismatch `{"mode":"status","expected":<arg>,"actual":<result status name>}`.
    - contains: `needle = json.loads(contains)`; `matched = json_subset(needle, result.get("message"))`; on miss `{"mode":"contains","needle":needle,"matched":False,"root":"response message","body":<_response_body_snapshot(result.get("message"))>}`.
    - match: `passed = jq_bool(result, match)` (envelope-rooted — the WHOLE result dict); on miss `{"mode":"match","expr":match,"result":False,"root":"response envelope","body":<_response_body_snapshot(result.get("message"))>}`. (A missing jq library surfaces as `ConfigError` via `jq_bool`/`_jq` — re-raise with a grpc-kind hint `pip install 'agctl[grpc]'`.)
    - jq-path: `actual = jq_value(result.get("message"), jq_path)`; `expected = parse_equals(equals)`; on `not type_aware_equal(expected, actual)` → `{"mode":"jq-path","path":jq_path,"expected":expected,"actual":actual,"root":"response message","body":<_response_body_snapshot(result.get("message"))>}`.

- [ ] **Step 1: Write the failing assertion tests**

Create `tests/unit/test_grpc_assertions.py`:
1. `test_validate_pairing`: `validate_grpc_assertion_args(status=None, contains=None, match=None, jq_path=".x", equals=None)` → `ConfigError` (--jq-path/--equals together).
2. `test_validate_contains_json`: `validate_grpc_assertion_args(..., contains="{bad", ...)` → `ConfigError`.
3. `test_validate_status_name_and_number`: `validate_grpc_assertion_args(status="NOT_FOUND", ...)` and `validate_grpc_assertion_args(status=5, ...)` both OK; `validate_grpc_assertion_args(status="NOPE", ...)` and `status=99` → `ConfigError`.
4. `test_validate_all_none_ok`: all-None → returns `None`.
5. `test_eval_status_pass_and_fail`: a result with `status.code==0`; `evaluate_grpc_assertions(result, status="OK", contains=None, match=None, jq_path=None, equals=None)` → None; `status="NOT_FOUND"` → raises `AssertionFailure`, `error.detail.failures[0]["mode"]=="status"`.
6. `test_eval_contains`: result `message={"a":1,"b":{"c":2}}`; `contains='{"b":{"c":2}}'` passes; `contains='{"z":1}'` fails with a `contains` failure entry carrying `root=="response message"`.
7. `test_eval_match_envelope_rooted`: `match='.status.name == "OK"'` passes; `match='.message.a == 1'` passes (envelope root reaches `.message`); `match='.status.name == "NOPE"'` fails (entry `root=="response envelope"`).
8. `test_eval_jq_path_equals`: `jq_path=".a"`, `equals="1"` passes (type-aware); `equals='"1"'` fails (number≠string).
9. `test_eval_multiple_failures_no_shortcircuit`: status + contains both wrong → `failures` length 2.
10. `test_eval_missing_jq_is_configerror`: monkeypatch the jq import to fail; `evaluate_grpc_assertions(result, match=".x", ...)` → `ConfigError` whose message contains `agctl[grpc]`.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_assertions.py -q`
Expected: FAIL — symbols undefined.

- [ ] **Step 3: Implement the validator + evaluator + status maps**

Add to `agctl/assertions.py` per the Produces contract. Reuse the named primitives; do NOT duplicate jq/subset/equals logic.

- [ ] **Step 4: Run the assertion tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_assertions.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/assertions.py tests/unit/test_grpc_assertions.py && git commit -m "feat(grpc): validate_grpc_assertion_args + evaluate_grpc_assertions (reuse jq/subset/equals)"`

---

### Task 9: `grpc call` unary + client-streaming (command + CLI group + fixture)

The one-result command path + the `grpc` CLI group wiring + the `new_grpc_client` seam. Command tests inject a fake client (no grpcio/protobuf).

**Files:**
- Create: `agctl/commands/grpc_commands.py`
- Modify: `agctl/cli.py` (import grpc commands; add `@cli.group(name="grpc")`; `grpc_group.add_command(grpc_call)`)
- Modify: `tests/fixtures/agctl.yaml` (add a `grpc:` section)
- Test: `tests/unit/test_grpc_commands.py` (new file)

**Interfaces:**
- Consumes: `envelope`, `load_config_or_raise` from `agctl/command.py`; `validate_grpc_assertion_args`, `evaluate_grpc_assertions` from `agctl/assertions.py` (Task 8); `fill_placeholders`, `deep_merge` from `agctl/resolution.py`; `parse_params` from `agctl/params.py`; `ConfigError`, `AssertionFailure`, `TemplateNotFound` from `agctl/errors.py`; `GrpcClient` + DTOs from `agctl/clients/grpc_client.py`; `GrpcTarget`/`GrpcTemplate` from `agctl/config/models.py`.
- Produces:
  - `new_grpc_client(target: GrpcTarget, *, descriptors=None) -> GrpcClient` — test seam (tests monkeypatch `grpc_commands.new_grpc_client`); body: `from ..clients.grpc_client import GrpcClient; return GrpcClient(target, descriptors=descriptors)`. Takes the resolved `GrpcTarget` object (not a name).
  - `_resolve_target(cfg, name: str | None, address: str | None) -> tuple[GrpcTarget, str]`: if `address is not None`: reject if `name is not None` → `ConfigError("--address is mutually exclusive with --target", {})`; validate `address` is `host:port` (single colon, non-empty host/port) else `ConfigError(f"--address must be host:port: {address!r}", {"address": address})`; return `(GrpcTarget(address=address), address)`. Else if `name is not None`: if `name not in cfg.grpc.targets` → `ConfigError(f"Unknown gRPC target: {name}", {"target": name})`; return `(cfg.grpc.targets[name], name)`. Else → `ConfigError("grpc call requires --target <name> or --address host:port", {})`.
  - `_parse_metadata(metadata: tuple[str,...]) -> dict[str,str]`: reuse `parse_params` (split on first `=`); values may contain `=`.
  - `_grpc_call_core(config_path, target_name, address, service, method, message, metadata, param, timeout, status, contains, match, jq_path, equals, overlay_paths) -> dict` (the unary/client-stream envelope core):
    - `cfg = load_config_or_raise(config_path, overlay_paths)`; `target, resolved_name = _resolve_target(cfg, target_name, address)`.
    - Template mode is handled by the Click command (it looks up `cfg.grpc.templates[name]` and passes `target_name=tpl.target`, `service=tpl.service`, `method=tpl.method`, plus merged `metadata`/`message`); free-form mode passes them directly. (Decide: the Click command resolves template-vs-freeform and ALWAYS calls the core with concrete target/service/method/message/metadata — the core is mode-agnostic.)
    - `params = parse_params(param)`; `client = new_grpc_client(target, descriptors=cfg.grpc.descriptors)`.
    - `md = client.find_method(service, method)`; `call_type = client.call_type_of(md)`.
    - Input-mode guard vs call_type: build `message_json` for unary/server-stream by `fill_placeholders(deep_merge(tpl_message_or_empty, parse(--message) if given), params)`; for client-stream/bidi, `--message` is rejected (`ConfigError("--message is invalid for streaming-input call types; pipe request messages on stdin", {"call_type": call_type})`). (Streaming input is Task 10; here unary/client-stream only — but the guard must reject stdin usage too. The core accepts a `stdin_lines` arg defaulting to `None`; for unary/client-stream it must be `None`.)
    - Pre-call: `validate_grpc_assertion_args(status=status, contains=contains, match=match, jq_path=jq_path, equals=equals)`.
    - Dispatch on call_type: `unary` → `result = client.call_unary(service, method, message_json, metadata=metadata_dict, timeout=timeout)`; `client_stream` → `result = client.call_client_stream(_stdin_request_iter, service=service, method=method, metadata=metadata_dict, timeout=timeout)` where `_stdin_request_iter` reads `sys.stdin` NDJSON lines (only when `call_type=="client_stream"`; here client-stream IS one-result so the core handles it).
    - Build the `grpc.call` result dict from `result` (the `GrpcUnaryResult` → `{target, service, method, call_type, status:{code,name,message}, message, initial_metadata, trailers}`).
    - Post-call assertions (only when any assertion kwarg is set — the evaluator early-returns otherwise): `evaluate_grpc_assertions(result_dict, status=status, contains=contains, match=match, jq_path=jq_path, equals=equals)`.
    - Return `result_dict`.
  - `_grpc_call_envelope = envelope("grpc.call")(_grpc_call_core)`.
  - `grpc_call` — `@click.command("call")` accepting EITHER a positional `<template_name>` OR the free-form flags; flags: `--target`, `--address`, `--service`, `--method`, `--message`, `--metadata`(multiple), `--param`(multiple), `--timeout`(float), `--status`, `--contains`, `--match`, `--jq-path`, `--equals`, `--config`(global, via `ctx.obj`), `--overlay`(global). `@click.pass_context`. Behavior:
    - Read `config_path = ctx.obj.get("config_path") if ctx.obj else None`; `ovs = ctx.obj.get("overlay_paths") if ctx.obj else None`.
    - Mode resolution: if `template_name` is given → reject any of `--target/--address/--service/--method` → `ConfigError("grpc call <template> is mutually exclusive with --target/--address/--service/--method", {})`; load `cfg` only if needed to resolve the template (or defer into the core). Resolve template fields and pass concrete args to the core.
    - **Important for Task 10:** this same `grpc_call` command handles streaming call types too, but the streaming emission logic is added in Task 10. In THIS task, the command supports unary + client-stream only; a server-stream/bidi call_type from the core currently flows through the envelope (Task 10 replaces that with NDJSON streaming). Keep the command signature complete; gate the streaming branch on `call_type in {"server_stream","bidi"}` and `raise NotImplementedError` there until Task 10.
  - `grpc_group` is created in `cli.py` (see below).

- [ ] **Step 1: Add the `grpc:` section to the test fixture**

Append to `tests/fixtures/agctl.yaml` (after `logs:`): a `grpc:` section with `targets.echo` (`address: "${TEST_GRPC_ADDR:-localhost:50051}"`, `use_tls: false`, `reflection: auto`), `descriptors: []`, and `templates.echo-unary` (`description`, `target: echo`, `service: echo.Echo`, `method: Unary`, `message: {msg: "{m}"}`).

- [ ] **Step 2: Write the failing unary/client-stream command tests**

Create `tests/unit/test_grpc_commands.py` mirroring `test_kafka_commands.py` / `test_logs_commands.py`: `from agctl.cli import cli`, `FIXTURE = .../fixtures/agctl.yaml`, an `ENV` dict, `CliRunner`. Define a `_FakeGrpcClient` (methods `find_method`/`call_type_of`/`call_unary`/`call_client_stream`/.../`healthcheck`) returning canned `GrpcUnaryResult`/etc., and an `install_fake(monkeypatch)` that does `monkeypatch.setattr(grpc_commands, "new_grpc_client", lambda target, descriptors=None: fake)`. The fake's `find_method`/`call_type_of` return a stub method descriptor + `"unary"` (or `"client_stream"`).
1. `test_grpc_call_template_unary`: fake `call_unary` returns `GrpcUnaryResult(..., message={"msg":"hi"}, status=GrpcStatus(0,"OK"), ...)`; `cli` invoke `["grpc","call","echo-unary","--param","m=hi","--config",FIXTURE]` env `ENV` → exit 0, `command=="grpc.call"`, `result.message=={"msg":"hi"}`, `result.status.name=="OK"`, `result.call_type=="unary"`.
2. `test_grpc_call_freeform_address`: `["grpc","call","--address","localhost:50051","--service","echo.Echo","--method","Unary","--message",'{"msg":"x"}']` → exit 0; the fake saw `service=="echo.Echo"`, `method=="Unary"`.
3. `test_grpc_call_target_unknown`: `--target nope` → exit 2 `ConfigError`.
4. `test_grpc_call_address_target_mutex`: `--target echo --address h:1` → exit 2 `ConfigError`.
5. `test_grpc_call_template_mutex`: `grpc call echo-unary --service x` → exit 2 `ConfigError`.
6. `test_grpc_call_nonok_status_is_result`: fake returns `status=GrpcStatus(5,"NOT_FOUND")`, no assertion flags → exit 0, `result.status.name=="NOT_FOUND"` (D6).
7. `test_grpc_call_assertion_status_fail`: fake returns OK; `--status NOT_FOUND` → exit 1 `AssertionError`, `error.detail.failures[0].mode=="status"`.
8. `test_grpc_call_assertion_match_pass`: `--match '.status.name == "OK"'` → exit 0.
9. `test_grpc_call_client_stream_envelope`: fake `call_type_of`→`"client_stream"` and `call_client_stream` returns one `GrpcUnaryResult`; invoke `["grpc","call","--target","echo","--service","echo.Echo","--method","ClientStream"]` with `input='{"msg":"a"}\n{"msg":"b"}\n'` (CliRunner `input=`) → exit 0, `result.call_type=="client_stream"`, the fake's `call_client_stream` request-iter saw 2 messages.
10. `test_grpc_call_bad_request_json_is_configerror`: `--message '{"unknown_field":1}'` → exit 2 `ConfigError` (the fake's `call_unary` is never reached; the serializer/ParseDict guard fires — drive this by having the fake's `call_unary` raise `ConfigError` on unknown fields, OR validate before — pick one and assert exit 2).

- [ ] **Step 3: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_commands.py -q`
Expected: FAIL — `grpc_commands`/`grpc` group undefined.

- [ ] **Step 4: Implement `grpc_commands.py` + wire the `grpc` group**

Implement `new_grpc_client`, `_resolve_target`, `_parse_metadata`, `_grpc_call_core`, `_grpc_call_envelope`, and `grpc_call` per the Produces contract. Match the kafka/http command style (`@click.option`, `@click.pass_context`, config_path/overlay resolution, envelope call). In `agctl/cli.py`: `from .commands.grpc_commands import grpc_call` (and later `grpc_healthcheck`); add `@cli.group(name="grpc") def grpc_group() -> None: """gRPC call/healthcheck commands."""`; add `grpc_group.add_command(grpc_call)`.

- [ ] **Step 5: Run command tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_commands.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 6: Commit**

Run: `git add agctl/commands/grpc_commands.py agctl/cli.py tests/fixtures/agctl.yaml tests/unit/test_grpc_commands.py && git commit -m "feat(grpc): grpc call unary + client-streaming command + grpc CLI group"`

---

### Task 10: `grpc call` server-streaming + bidi (NDJSON streaming + stdin input)

The streaming exceptions: NDJSON out per response message + `summary`; NDJSON in on stdin for request streams. Manual startup-error envelopes (mirror `logs tail`).

**Files:**
- Modify: `agctl/commands/grpc_commands.py` (streaming branch in `grpc_call` + a `_grpc_stream_run` helper + `_emit_stdout_line`)
- Test: `tests/unit/test_grpc_commands.py`

**Interfaces:**
- Consumes: Task 9 (`grpc_call` command, `_resolve_target`, `_parse_metadata`, `new_grpc_client`); `emit` from `agctl/output.py`; `signal`, `threading`, `time`, `json`, `sys`; the `logs tail`/`http ping` signal-install + restore + manual-envelope + summary discipline (read `agctl/commands/logs_commands.py` `logs_tail` and mirror it).
- Produces:
  - `_emit_stdout_line(line: dict) -> None` — write one NDJSON line to stdout (mirror `logs_commands._emit_stdout_line` / `http_commands._emit_stdout_line`).
  - `_grpc_stream_run(client, *, service, method, call_type, request_input, metadata, timeout, match, expect_count, stop_event, emit_line) -> dict`: drive `client.call_server_stream(...)` (for `server_stream`, request = the single `--message`/template message) or `client.call_bidi(request_json_iter=<stdin lines>, ...)` (for `bidi`); for each yielded `GrpcStreamMessage`, apply the per-message `--match` filter (`jq_bool(message_dict, filled_match)` — message-rooted; a missing jq lib → `ConfigError` surfaced as a startup error BEFORE streaming) and emit one `{"event":"message","message":<dict>,"trailers":<trailers or null>}` line via `emit_line`; count `messages` and `matched`. Honor `expect_count`. Return a `summary` dict `{"summary": True, "messages": <n>, "matched": <m>, "status": <final status or {"code":0,"name":"OK","message":""}>, "duration_ms": <elapsed>}`. If `expect_count` is set and `matched < expect_count`, the returned summary carries the shortfall and the caller exits 1.
  - `grpc_call` streaming branch (replaces the Task-9 `NotImplementedError` for `call_type in {"server_stream","bidi"}`): NOT `@envelope`-wrapped for these call types — emit NDJSON. Behavior:
    - Resolve config + target + method + call_type (same as unary); on `AgctlError` during setup → `emit(ok=False, command="grpc.call", error=err.to_dict(), duration_ms=...)` + `raise SystemExit(err.exit_code)`; on other Exception → `InternalError` envelope + `raise SystemExit(2)`.
    - Input guard: `server_stream` requires `--message`/template message (no stdin); `bidi` requires stdin request messages (reject `--message` for bidi). Unary assertion flags (`--status`/`--contains`/`--jq-path`/`--equals`) rejected on streams → `ConfigError`; only `--match`/`--expect-count` allowed.
    - If `--match` given: `filled = fill_placeholders(match, params)`; `compile_jq(filled, label="grpc --match")` up front (loud `ConfigError` on bad expr / missing jq).
    - Install `stop_event = threading.Event()` + SIGTERM/SIGINT handlers that `stop_event.set()` (guard non-main-thread); restore in `finally`.
    - `summary = _grpc_stream_run(...)`; emit the summary via `_emit_stdout_line`; `raise SystemExit(0)` (or `1` if `expect_count` not met).
  - For `bidi`, stdin-EOF closes the request half; the call drains remaining responses. For `server_stream`, the single request is sent and responses streamed.

- [ ] **Step 1: Write the failing streaming tests**

Add to `tests/unit/test_grpc_commands.py` (fake client's `call_server_stream`/`call_bidi` yield canned `GrpcStreamMessage` lists; drive via `CliRunner` `input=` for bidi):
1. `test_grpc_call_server_stream_emits_ndjson_and_summary`: fake `call_type_of`→`"server_stream"`, `call_server_stream` yields 2 `GrpcStreamMessage(message={"msg":"a"})`/`{"msg":"b"}`; invoke `["grpc","call","--target","echo","--service","echo.Echo","--method","ServerStream","--message",'{"msg":"x"}']` → exit 0; stdout has 2 `event:"message"` lines + 1 `summary` line (`json.loads` each: messages lack `summary`; summary has `summary==True`, `messages==2`, `matched==2`).
2. `test_grpc_call_bidi_reads_stdin_and_streams`: fake `call_type_of`→`"bidi"`, `call_bidi` echoes back one response per request; invoke with `input='{"msg":"a"}\n{"msg":"b"}\n'` → exit 0; stdout has 2 message lines + summary; the fake's `call_bidi` request-iter saw 2 messages.
3. `test_grpc_call_stream_match_filters_and_expect_count`: fake `call_server_stream` yields 3 messages (2 matching `--match '.message.msg == "x"'`); `--expect-count 2` → exit 0, `summary.matched==2`. `--expect-count 3` → exit 1, `error`/summary reflects the shortfall.
4. `test_grpc_call_stream_rejects_unary_assertion_flags`: `--status OK` on a server-stream → exit 2 `ConfigError`.
5. `test_grpc_call_stream_bad_match_startup_error`: `--match '.msg =='` (malformed) → exit 2, one `ConfigError` envelope line, no message lines streamed.
6. `test_grpc_call_stream_unknown_target_startup_error`: `--target nope` → exit 2 `ConfigError` envelope (single line, no streaming).

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_commands.py -k "server_stream or bidi or stream" -q`
Expected: FAIL — streaming branch raises `NotImplementedError`.

- [ ] **Step 3: Implement the streaming branch + `_grpc_stream_run` + `_emit_stdout_line`**

Implement per the Produces contract. Read `agctl/commands/logs_commands.py` (`logs_tail`) and mirror its signal-install/restore + manual-envelope + summary + `SystemExit` discipline and its `_emit_stdout_line`.

- [ ] **Step 4: Run command tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_commands.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/grpc_commands.py tests/unit/test_grpc_commands.py && git commit -m "feat(grpc): grpc call server-streaming + bidi (NDJSON out, stdin NDJSON in)"`

---

### Task 11: `grpc healthcheck` command

The readiness command (grpc analog of `check ready`). Wires `grpc_healthcheck` onto the `grpc` group.

**Files:**
- Modify: `agctl/commands/grpc_commands.py` (`grpc_healthcheck` command + core)
- Modify: `agctl/cli.py` (`grpc_group.add_command(grpc_healthcheck)`)
- Test: `tests/unit/test_grpc_commands.py`

**Interfaces:**
- Consumes: `envelope`, `load_config_or_raise`; `new_grpc_client`; `ConfigError`.
- Produces:
  - `_grpc_healthcheck_core(config_path, target, service, all_, overlay_paths) -> dict`:
    - `cfg = load_config_or_raise(config_path, overlay_paths)`. Select targets: if `all_` or neither `target` given → all `cfg.grpc.targets`; else `[target]`. Unknown `target` → `ConfigError`.
    - For each selected target name: `client = new_grpc_client(cfg.grpc.targets[name])`; `res = client.healthcheck(service or "")`; collect `{name: {"address": res.address, "status": res.status, ("note": res.note)}}`.
    - Return `{"targets": {…}, "all_serving": <all statuses == "SERVING">}`.
  - `_grpc_healthcheck_envelope = envelope("grpc.healthcheck")(_grpc_healthcheck_core)`.
  - `grpc_healthcheck` — `@click.command("healthcheck")` with `--target`, `--service`, `--all`, `--config`, `--overlay`; `@click.pass_context`; resolves config_path/overlays and calls the envelope.

- [ ] **Step 1: Write the failing healthcheck command tests**

Add to `tests/unit/test_grpc_commands.py` (fake `healthcheck` returns canned `GrpcHealthResult`):
1. `test_grpc_healthcheck_single_target`: `--target echo --service ""` → exit 0, `command=="grpc.healthcheck"`, `result.targets.echo.status=="SERVING"`, `result.all_serving==True`.
2. `test_grpc_healthcheck_all`: `--all` → one entry per fixture target; `all_serving` reflects them.
3. `test_grpc_healthcheck_unknown_is_unknown_not_error`: fake returns `status=="UNKNOWN", note="health service UNIMPLEMENTED"` → exit 0, `all_serving==False`, `targets.echo.note` present.
4. `test_grpc_healthcheck_unknown_target`: `--target nope` → exit 2 `ConfigError`.

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_commands.py -k healthcheck -q`
Expected: FAIL — `grpc_healthcheck` undefined.

- [ ] **Step 3: Implement `grpc_healthcheck` + core; register on the group**

Implement per the Produces contract. In `cli.py`: add `grpc_healthcheck` to the import and `grpc_group.add_command(grpc_healthcheck)`.

- [ ] **Step 4: Run command tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_grpc_commands.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/grpc_commands.py agctl/cli.py tests/unit/test_grpc_commands.py && git commit -m "feat(grpc): grpc healthcheck command"`

---

### Task 12: `discover` — `grpc-services` / `grpc-methods` categories

Add the two categories across the four discover modes. `grpc-methods` item detail enumerates the request message field schema via `GrpcClient`. Tests inject a fake client (protobuf-free).

**Files:**
- Modify: `agctl/commands/discover_commands.py`
- Test: `tests/unit/test_discover_command.py`

**Interfaces:**
- Consumes: `GrpcClient` from `agctl/clients/grpc_client.py` (local import inside cores to avoid a module-load cycle); existing discover helpers; `ConfigError`/`TemplateNotFound`.
- Produces:
  - `_VALID_CATEGORIES`: append `"grpc-services"` and `"grpc-methods"`.
  - `_SUMMARY_HINT`: extend the category list in the hint string with `, grpc-services, grpc-methods`.
  - `_summary_core`: add `"grpc_targets": len(cfg.grpc.targets)` and `"grpc_methods": <total methods across targets>` (counting may require resolving descriptors — to keep summary cheap and offline-safe, count `grpc_methods` as the number of `grpc.templates` plus a best-effort 0 when reflection is needed; simplest: `grpc_methods = len(cfg.grpc.templates)` in summary, with the per-service detail in `_item_core`). **Decision: summary `grpc_methods = len(cfg.grpc.templates)`** (templates are the named-call surface; full live method enumeration is the category listing).
  - `_category_core`:
    - `elif category == "grpc-services":` for `name, tgt in cfg.grpc.targets.items()`: `items.append({"name": name, "description": f"gRPC target {name} at {tgt.address} (tls={tgt.use_tls})"})`.
    - `elif category == "grpc-methods":` for `name, tpl in cfg.grpc.templates.items()`: `items.append({"name": name, "description": (tpl.description or f"{tpl.service}/{tpl.method}")})`.
  - `_item_core`:
    - `if category == "grpc-services":` if `name not in cfg.grpc.targets` → `TemplateNotFound(f"Unknown gRPC target: {name}", {"path": f"grpc.targets.{name}"})`; `tgt = cfg.grpc.targets[name]`; return `{"category":"grpc-services","name":name,"description":...,"address":tgt.address,"use_tls":tgt.use_tls,"reflection":tgt.reflection,"example":f"agctl grpc call --target {name} --service <fq> --method <m>"}`.
    - `if category == "grpc-methods":` if `name not in cfg.grpc.templates` → `TemplateNotFound(f"Unknown gRPC method template: {name}", {"path": f"grpc.templates.{name}"})`; `tpl = cfg.grpc.templates[name]`; resolve the request message schema: build a `GrpcClient(cfg.grpc.targets[tpl.target], descriptors=cfg.grpc.descriptors)`, `md = client.find_method(tpl.service, tpl.method)`, enumerate `md.input_type.fields` → `params = [{"name": f.name, "type": f.type.name, "repeated": f.label == repeated}` ...]; `call_type = client.call_type_of(md)`; return `{"category":"grpc-methods","name":name,"description":tpl.description,"target":tpl.target,"service":tpl.service,"method":tpl.method,"call_type":call_type,"request_fields":params,"example":f"agctl grpc call {name} --param ..."}`. (A target whose reflection is unreachable is reported as `unavailable` in the listing — `_item_core` catches `ConfigError` from `find_method`/`resolve_descriptors` and returns a detail dict with `"unavailable": True, "error": str(err)` rather than failing the whole discover call.)
  - `_search_core`: loop `cfg.grpc.targets` and `cfg.grpc.templates` matching name/description (lowercased substring); append `{"category":"grpc-services"/"grpc-methods","name":name,"description":...}`.
- Produces (test seam): `_item_core` constructs `GrpcClient` directly; tests that need protobuf-free behavior monkeypatch `grpc_commands.new_grpc_client` is NOT reachable from discover — so add a module-level seam `new_grpc_client_for_discover = GrpcClient` indirection OR have discover import `from ..commands.grpc_commands import new_grpc_client`. **Decision: discover reuses `grpc_commands.new_grpc_client`** (local import inside `_item_core`); tests monkeypatch `grpc_commands.new_grpc_client` to return a fake whose `find_method`/`call_type_of` return canned descriptors.

- [ ] **Step 1: Write the failing discover tests**

Add to `tests/unit/test_discover_command.py`:
1. `test_discover_summary_includes_grpc`: `agctl discover --config FIXTURE` → `result.grpc_targets == <count>`, `result.grpc_methods == <template count>`, hint contains `grpc-services` and `grpc-methods`.
2. `test_discover_category_grpc_services`: `--category grpc-services` → items include `echo` with a description naming the address; `count` matches.
3. `test_discover_category_grpc_methods`: `--category grpc-methods` → items include `echo-unary`.
4. `test_discover_item_grpc_methods_schema`: monkeypatch `grpc_commands.new_grpc_client` to a fake whose `find_method` returns a fake method descriptor (input_type with fields `msg`/string and `n`/int) and `call_type_of`→`"unary"`; `discover --category grpc-methods --name echo-unary --config FIXTURE` → `request_fields` lists `msg`/`n` with types, `call_type=="unary"`, `example` starts with `agctl grpc call echo-unary`.
5. `test_discover_item_grpc_methods_unavailable_isolated`: fake `find_method` raises `ConfigError` (reflection unreachable, no descriptors); `--category grpc-methods --name echo-unary` → exit 0, detail has `"unavailable": True` (discover does not hard-fail).
6. `test_discover_search_grpc`: `--search echo` → matches include `grpc-services`/`grpc-methods` entries.
7. `test_discover_unknown_grpc_target_item`: `--category grpc-services --name nope` → exit 2 (`TemplateNotFound` handling).

- [ ] **Step 2: Run to verify they fail**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_discover_command.py -k grpc -q`
Expected: FAIL — `grpc-services`/`grpc-methods` not valid categories.

- [ ] **Step 3: Add the two categories across the four cores**

Edit `discover_commands.py` per the Produces contract. Local-import `new_grpc_client` inside `_item_core`'s grpc-methods branch.

- [ ] **Step 4: Run discover tests + full unit suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit/test_discover_command.py -q && /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/discover_commands.py tests/unit/test_discover_command.py && git commit -m "feat(discover): add grpc-services and grpc-methods categories"`

---

### Task 13: Integration tests (in-process grpcio server, self-skipping)

End-to-end confidence against a real grpcio server implementing `echo.proto` + reflection + health. Self-skipping when `grpcio`/server unavailable (mirror the existing `AGCTL_TEST_LIVE` / `AGCTL_TEST_GRPC_ADDR` gating).

**Files:**
- Create: `tests/integration/test_grpc_commands.py`

**Interfaces:**
- Consumes: Tasks 4–12 (real client, real commands, real discover). `grpc`, `grpc_reflection`, `grpc_health`, the `echo.proto` service implemented in-process.
- Produces: an in-process `grpcio` server fixture (start `Echo` servicer + reflection + health under `AGCTL_TEST_LIVE=1`, or point at `AGCTL_TEST_GRPC_ADDR`); E2E through the real `cli`. No fakes.

- [ ] **Step 1: Write the integration tests**

Create `tests/integration/test_grpc_commands.py`. Guard with `pytest.importorskip("grpc")` and a `require_grpc_server` fixture (skip unless `AGCTL_TEST_LIVE=1` or `AGCTL_TEST_GRPC_ADDR` set; mirror `tests/integration/conftest.py`'s `require_*` fixtures). Implement a tiny `Echo` servicer (unary echoes `msg`; server-stream emits N; client-stream concatenates; bidi echoes each) + reflection + health. Each test writes a temp `agctl.yaml` whose `grpc.targets.echo.address` is the server address + `grpc.descriptors` pointing at `tests/fixtures/echo_descriptor.pb` (or relies on reflection):
1. `test_grpc_unary_call_and_assert`: `grpc call --target echo --service echo.Echo --method Unary --message '{"msg":"hi"}'` → exit 0, `result.message.msg=="hi"`; `--status OK` passes; `--status NOT_FOUND` → exit 1.
2. `test_grpc_server_stream`: `grpc call --method ServerStream --message '{"msg":"x"}'` → NDJSON with N message lines + summary, exit 0.
3. `test_grpc_client_stream`: pipe `{"msg":"a"}\n{"msg":"b"}\n` to stdin; `--method ClientStream` → one envelope result.
4. `test_grpc_bidi`: pipe 2 requests; `--method Bidi` → 2 message lines + summary.
5. `test_grpc_nonok_status_result`: a servicer method that returns `NOT_FOUND`; call with no assertion flags → exit 0, `result.status.name=="NOT_FOUND"`.
6. `test_grpc_healthcheck`: `grpc healthcheck --target echo` → `SERVING`.
7. `test_grpc_discover_methods`: `discover --category grpc-methods --name echo-unary` → `request_fields` populated from the live descriptor.
8. `test_grpc_reflection_then_descriptor_fallback`: with `reflection: off` + `grpc.descriptors` set to the `.pb`, unary call still works (fallback path).

- [ ] **Step 2: Run the integration tests (live)**

Run: `AGCTL_TEST_LIVE=1 /Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/integration/test_grpc_commands.py -q`
Expected: PASS (server starts in-process). Without the flag → SKIP.

- [ ] **Step 3: Run the entire suite**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest -q`
Expected: PASS (integration grpc tests skip without the flag; everything else passes).

- [ ] **Step 4: Commit**

Run: `git add tests/integration/test_grpc_commands.py && git commit -m "test(grpc): in-process grpcio integration tests (unary/stream/health/discover)"`

---

### Task 14: Documentation sync

Sync DESIGN.md, ARCHITECTURE.md, the consumer skill, the sample config, and README to the as-built feature. Per CLAUDE.md, invoke the `docs-watcher` subagent for DESIGN/ARCHITECTURE altitude; make targeted concrete edits (sample config, skill reference, README) directly.

**Files:**
- Modify: `agctl/data/sample-config.yaml` (add a `grpc:` section mirroring the fixture)
- Modify: `docs/DESIGN.md`, `docs/ARCHITECTURE.md` (via `docs-watcher` + targeted edits)
- Modify: `skills/agctl*` (add a `grpc` authoring reference)
- Modify: `README.md` (surface the `grpc` group + `pip install 'agctl[grpc]'`)

**Interfaces:**
- Consumes: Tasks 1–13 (as-built behavior).
- Produces: docs matching as-built.

- [ ] **Step 1: Add a `grpc:` section to the packaged sample config**

In `agctl/data/sample-config.yaml`: add a commented `grpc:` section (one plaintext target with `reflection: auto`, a `descriptors` example, and one template), mirroring the test fixture. This is the `agctl config init` starter.

- [ ] **Step 2: Invoke `docs-watcher` for DESIGN/ARCHITECTURE altitude**

Dispatch the `docs-watcher` subagent (CLAUDE.md "Docs Sync") to reconcile:
- DESIGN.md: **strike the gRPC row from §10**; add a `grpc` subsection to §3 (`grpc call` template/free-form, the call-type table, `grpc healthcheck`, the stdin/stdout NDJSON model, status-as-result); add `grpc:` to §2.1 (`targets`/`descriptors`/`templates`) and the `grpc` extra to §7/§11; add `grpc.call` / `grpc.healthcheck` shapes to §4.2; add `grpc-services`/`grpc-methods` to the discover categories (§3.8, §4.2).
- ARCHITECTURE.md: add `grpc_client.py` / `grpc_commands.py` to §3; a grpc request-lifecycle trace to §4; a grpc subsection to §8 (descriptor resolution, JSON↔protobuf, four call types, status-as-result, RpcError mapping); note server-stream/bidi as streaming exceptions in §6 (the 4th/5th after http ping/mock run/logs tail); §10 — note grpc is in-tree with **no new entry-point**; §15 — strike the gRPC deferred note.

- [ ] **Step 3: Update the consumer skill + README**

In the relevant `skills/agctl*` reference: add a `grpc` authoring reference (`grpc:` config section, reflection-first/descriptor-fallback, JSON message authoring, the four call types and the stdin/stdout NDJSON model, status-as-result semantics, the `grpc` extra). In `README.md`: surface the `grpc` group and `pip install 'agctl[grpc]'`.

- [ ] **Step 4: Verify the suite still passes + commit**

Run: `/Users/dmitry/Desktop/CursorProjects/agenttest/.venv/bin/python -m pytest tests/unit -q`
Expected: PASS (drift-guards, if any, satisfied by the Step-1 sample-config edit).

Run: `git add -A && git commit -m "docs: sync grpc feature — DESIGN/ARCHITECTURE, sample config, skill, README"`

---

## Self-Review (completed)

**1. Code scan:** No method bodies, algorithms, or test code appear. Each step states behavior, exact expected results, signatures, and data shapes (e.g. the `GrpcUnaryResult`/`GrpcStreamMessage` fields, the `RpcError` mapping rules, the `evaluate_grpc_assertions` failure-entry shapes). Dict-literal snippets are data contracts, not implementation.

**2. Self-containment:** Every task lists exact files, Consumes/Produces contracts (signatures, fields, defaults, error cases), and per-test scenarios with expected results. A zero-context implementer can do any task alone — e.g. Task 6's call-method contracts fully define behavior using only Task 5's named `find_method`/`call_type_of` + the DTOs from Task 4.

**3. Spec coverage:** D1 (in-tree) → all tasks (no plugin/entry-point); D2 (reflection+fallback) → Task 5; D3 (JSON↔protobuf) → Task 6; D4 (four call types + NDJSON symmetry) → Tasks 6/9/10; D5 (addressing + `--address`) → Task 9 (`_resolve_target`); D6 (status-as-result) → Tasks 6/8/9; D7 (assertions) → Task 8 (refined: grpc-specific evaluator); D8 (metadata/trailers) → Tasks 6/9; D9 (TLS/plaintext) → Task 4 (`__init__`); D10 (descriptor-driven dynamic stubs) → Tasks 5/6; D11 (packaging) → Task 3; D12 (healthcheck) → Tasks 7/11; D13 (no new entry-point) → Task 3. Spec §6 commands → Tasks 9/10/11; §7 config → Task 1 + §12 validation → Task 2; §8 client contract → Tasks 4–7; §9 descriptor strategy → Task 5; §10 output → Tasks 6/9/10/11; §11 error model → Tasks 4/5/6/7/8/9/10; §13 discovery → Task 12; §14 packaging → Task 3 (+CI); §15 testing → Tasks 4–13; §16/17 docs → Task 14.

**4. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". Each test step names scenario + expected result; each impl step names behavior. Named cross-task reuse (e.g. Task 10 reuses Task 9's `new_grpc_client`/`_resolve_target`/`_parse_metadata`) restates the relevant contract, not a bare pointer.

**5. Type consistency:** DTO field names (`GrpcStatus.code/name/message`, `GrpcUnaryResult.{target,service,method,call_type,status,message,initial_metadata,trailers}`, `GrpcStreamMessage.{message,trailers}`, `GrpcHealthResult.{target,address,status,note}`) identical across Tasks 4 (definition), 6/7 (client), 9/10/11 (command), 13 (integration). `call_type` token set `"unary"|"server_stream"|"client_stream"|"bidi"` consistent across Tasks 5 (`call_type_of`), 6 (`call_type=` in results), 9/10 (dispatch). `new_grpc_client(target, *, descriptors=None)` consistent across Tasks 9 (definition), 12 (discover reuse + monkeypatch target), 13. `validate_grpc_assertion_args`/`evaluate_grpc_assertions` kwargs (`status, contains, match, jq_path, equals`) consistent across Task 8 (definition) and Task 9 (call). `_resolve_target`/`_parse_metadata` consistent across Tasks 9 (definition) and 10/11 (reuse).
