# Mock Capture (envelope-rooted `capture:`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three battle-test gaps — HTTP nested-path capture (①), Kafka key/header capture (②), and JSON object pass-through (③) — by adding one optional, envelope-rooted `capture:` map to HTTP stubs and Kafka reactors.

**Architecture:** Add an optional `capture: dict[str, CaptureSpec]` field to `HttpStub` and `KafkaReactor` (peer of `match`). At react time, build the incoming-message envelope (HTTP request / Kafka normalized message), resolve each capture via the existing `jq_value`, merge with today's implicit captures (explicit overrides name **and** type), and render templates through a new typed renderer in `resolution.py` that supports `scalar`/`object`/`json` substitution. Startup validates every `capture.*.from` (jq pre-compile) and the placement of `object`-typed captures (static scan); runtime emits a non-fatal `capture.missing` event when a `from` resolves to nothing.

**Tech Stack:** Python 3.11+, Pydantic v2, `jq` (compiled python binding, optional extra), `pytest`, `httpx` (HTTP tests), `confluent-kafka` (Kafka reactor).

## Global Constraints

Copied from the spec (`docs/superpowers/specs/active/2026-07-04-agctl-mock-capture-design.md`) and refined by implementation discovery; every task's requirements include these:

- **Additive under major version `1`**; no `version` bump. Existing stubs/reactors behave identically.
- **`match` is unchanged** — `match.jq` (HTTP) and reactor `match` (Kafka) stay payload-rooted (body / value). Only `capture.from` is envelope-rooted. Unifying `match` onto the envelope root is the breaking follow-up **#22**, explicitly out of scope.
- **`from` is a Python keyword.** The YAML key is `from`; the Pydantic attribute is `from_`, declared `Field(alias="from")` with `model_config = ConfigDict(populate_by_name=True)`. This is the **first** aliased field in `agctl/config/` — there is no prior precedent (verified: no `alias`/`populate_by_name`/`ConfigDict` anywhere in `agctl/config/`).
- **`fill_placeholders(value, params: dict[str, str])` is NOT modified.** It has 9 production callers across 4 files; 5 of them (`agctl/commands/http_commands.py` ×4, `agctl/commands/kafka_commands.py` ×1) do plain `${ENV}`/param string templating unrelated to mocks. Mocks switch to a **new typed renderer** added alongside it (Task 2). *This refines spec §7.2, which says "fill_placeholders is extended" — same behavior, isolated blast radius.*
- **Header casing asymmetry.** HTTP envelope `headers` keys are **lowercased** (HTTP headers are case-insensitive). Kafka envelope `headers` keys are **case-sensitive, as-produced** (Kafka header keys are bytes; preserve the producer's exact name, e.g. `.headers.rqUID`).
- **jq packaging unchanged.** A stub/reactor with `capture` requires the `jq` extra (it reuses `jq_value`), exactly like `match.jq` / reactor `match` today. Missing library → `ConfigError` (exit 2) via the existing `_jq()` lazy import.
- **Fail loudly.** Malformed `capture.*.from` and `object`-typed capture misplacement are `ConfigError` (exit 2) at startup (both `config validate` and `mock run` Step 0). A runtime `from` resolving to nothing is **non-fatal**: emit `capture.missing` + degenerate substitution, mock continues.
- **TDD + frequent commits.** Each task: failing test → verify it fails → minimal implementation → verify pass → commit. Conventional-commit messages (repo style: `feat(mock):`, `test:`, `docs:`).

## Spec Refinements (plan-level decisions)

1. **New typed renderer instead of mutating `fill_placeholders`** (see Global Constraints). `fill_placeholders` and `tests/unit/test_resolution.py`'s existing cases stay green untouched.
2. **Missing-capture contract.** `jq_value` returns `None` both for a missing path and for a legitimately-null value (it swallows errors and empty outputs to `None`); this conflation is inherited. Contract chosen: a capture whose `jq_value` result is `None` → recorded as missing → `capture.missing` event emitted, and the renderer substitutes empty string `""` for that name **regardless of type** (so a missing `object` capture degrades to `""`, not a crash). A present non-null value renders per its type.
3. **Object-placement check** lives in a new `agctl/mock/capture_validate.py` (not `config/validator.py`), with dual entry points mirroring `collect_jq_compile_errors` — because `validate_config` is deliberately not called per-command and `config/validator.py` stays free of mock-specific logic.

---

## File Structure

**New files:**
- `agctl/mock/capture.py` — `resolve_captures(envelope, captures)`: jq extraction into typed `CaptureValue`s + missing list. Depends on `jq_value`, `CaptureSpec`, `CaptureValue`.
- `agctl/mock/capture_validate.py` — `collect_capture_placement_errors(mocks)`: static startup check that `object`-typed captures are used only in whole-field, non-string-slot positions. Depends only on `..config.models`.

**Modified files:**
- `agctl/config/models.py` — new `CaptureSpec`; new optional `capture` field on `HttpStub` and `KafkaReactor`.
- `agctl/resolution.py` — new `CaptureValue` dataclass + new `render_typed(value, captures)`; `fill_placeholders` untouched.
- `agctl/mock/jq_precompile.py` — `iter_mock_jq_expressions` also yields `capture.*.from`.
- `agctl/mock/http_server.py` — build request envelope; resolve explicit captures; merge; render via `render_typed`; emit `capture.missing`.
- `agctl/mock/kafka_reactor.py` — use normalized `msg` as envelope; resolve explicit captures; merge; render via `render_typed`; emit `capture.missing`.
- `agctl/mock/engine.py` — Step 0 also runs `collect_capture_placement_errors` (fail-fast).
- `agctl/commands/config_commands.py` — `config validate` also merges `collect_capture_placement_errors` into errors (alongside `collect_jq_compile_errors`).

**New test files:**
- `tests/unit/test_mock_capture.py` — `resolve_captures` unit tests.
- `tests/unit/test_mock_capture_validate.py` — placement-check unit tests.
- `tests/integration/test_mock_capture_e2e.py` — the four battle-test scenarios as acceptance fixtures.

**Extended test files:**
- `tests/unit/test_mock_models.py`, `tests/unit/test_resolution.py`, `tests/unit/test_jq_precompile.py`, `tests/unit/test_mock_http_server.py`, `tests/unit/test_mock_kafka_reactor.py`, `tests/unit/test_mock_engine.py`, `tests/integration/test_mock_commands.py`.

---

### Task 1: `CaptureSpec` config model

**Files:**
- Modify: `agctl/config/models.py` (add `CaptureSpec` near the other mock models, ~line 160; add `capture` field to `HttpStub` ~line 181 and `KafkaReactor` ~line 237).
- Test: `tests/unit/test_mock_models.py`.

**Interfaces:**
- Consumes: `BaseModel`, `Field`, `field_validator` (already imported, `models.py:5`); `typing.Literal` (import if not present).
- Produces:
  - `CaptureSpec(BaseModel)` with `model_config = ConfigDict(populate_by_name=True)`; field `from_: str = Field(alias="from")`; field `type: Literal["scalar", "object", "json"] = "scalar"`.
  - `HttpStub.capture: dict[str, CaptureSpec] | None = None` (new optional field, peer of `match`).
  - `KafkaReactor.capture: dict[str, CaptureSpec] | None = None` (new optional field, peer of `match`, on the **consumer** side — reads the incoming message).
  - YAML key `from` parses to attribute `from_`; constructing via Python keyword `from_=...` also works (`populate_by_name`). Default `type` is `"scalar"`; `capture` defaults to `None`.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_mock_models.py`, add cases (no helpers; construct inline per file convention):
- `CaptureSpec.model_validate({"from": ".body.variables.id"})` → object with `from_ == ".body.variables.id"` and `type == "scalar"` (default).
- `CaptureSpec.model_validate({"from": ".value.context", "type": "object"})` → `type == "object"`.
- `CaptureSpec(**{"from": ".x"})` (Python-keyword construction via `populate_by_name`) → `from_ == ".x"`.
- `CaptureSpec.model_validate({"from": ".x", "type": "bogus"})` → raises `ValidationError`.
- `HttpStub(method="POST", path="/x", response=HttpResponse())` → `.capture is None`.
- `HttpStub(method="POST", path="/x", response=HttpResponse(), capture={"op_id": CaptureSpec.model_validate({"from": ".body.variables.id"})})` → `.capture["op_id"].from_ == ".body.variables.id"`.
- `KafkaReactor(topic="t", reaction=KafkaReaction(topic="r", value={}))` → `.capture is None`; and with `capture={...}` parses analogously.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_models.py -v`
Expected: FAIL — `CaptureSpec` not defined / `HttpStub` has no `capture`.

- [ ] **Step 3: Write minimal implementation**

Add `CaptureSpec` to `agctl/config/models.py` with the alias + `populate_by_name` config and the `type` Literal defaulting to `"scalar"`. Add the optional `capture` field to `HttpStub` and to `KafkaReactor`. Do not add validators yet (none needed). Do not touch runtime code.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_models.py -v`
Expected: PASS. Also run `pytest tests/unit/test_mock_models.py tests/unit/test_validator.py -q` to confirm no existing model/validation test regressed.

- [ ] **Step 5: Commit**

Run: `git add agctl/config/models.py tests/unit/test_mock_models.py`
Run: `git commit -m "feat(mock): add CaptureSpec config model for stub/reactor capture"`

---

### Task 2: Typed renderer (`CaptureValue` + `render_typed`)

**Files:**
- Modify: `agctl/resolution.py` (add `CaptureValue` + `render_typed` after `fill_placeholders`, reusing `_PLACEHOLDER_RE`).
- Test: `tests/unit/test_resolution.py`.

**Interfaces:**
- Consumes: `_PLACEHOLDER_RE` (`resolution.py:18`); `json` (import if not present).
- Produces (exact signatures):
  - `CaptureValue` — a dataclass with `value: Any` and `type: str` (one of `"scalar"`, `"object"`, `"json"`).
  - `render_typed(value: Any, captures: dict[str, CaptureValue]) -> Any` — substitutes `{name}` placeholders. Recurses into `dict` (returns new dict) and `list` (returns new list); passes through non-string scalars unchanged.
- Substitution semantics (the contract later tasks rely on):
  - For a name not in `captures`: leave the literal `{name}` (matches `fill_placeholders`).
  - `scalar`: substitute `str(capture.value)`.
  - `json`: substitute `json.dumps(capture.value)`.
  - `object`: substitute the live `capture.value` **only when the field string is exactly `"{name}"`** (whole-field). Used anywhere else → raise `ValueError` (defensive; unreachable for configs that pass Task 5's startup check, but keeps the renderer honest).
  - **Missing rule:** when `capture.value is None` (set by the resolver for a missing path), substitute `""` (empty string) **regardless of type** — do not emit `"None"`/`"null"`/`{}`.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_resolution.py`, add cases constructing `CaptureValue` directly and calling `render_typed`. Expected results:
- `render_typed("{op_id}", {"op_id": CaptureValue(42, "scalar")})` → `"42"`.
- `render_typed({"id": "{op_id}", "n": "{n}"}, {"op_id": CaptureValue(7,"scalar"), "n": CaptureValue(True,"scalar")})` → `{"id": "7", "n": "True"}`.
- `render_typed("{ctx}", {"ctx": CaptureValue({"a":1}, "json")})` → `"{\"a\": 1}"` (a `json.dumps` string).
- `render_typed({"context": "{ctx}"}, {"ctx": CaptureValue({"a":1}, "object")})` → `{"context": {"a": 1}}` (real object, not a string).
- `render_typed({"context": "pre={ctx}"}, {"ctx": CaptureValue({"a":1}, "object")})` → raises `ValueError` (object not whole-field).
- `render_typed(["{a}", "{b}"], {"a": CaptureValue("x","scalar"), "b": CaptureValue([1,2],"json")})` → `["x", "[1, 2]"]`.
- Missing: `render_typed("{op_id}", {"op_id": CaptureValue(None, "scalar")})` → `""`; same for `type="object"` → `""`.
- Absent name: `render_typed("{missing}", {})` → `"{missing}"` (literal preserved).
- Non-mutation: the input container is not mutated (assert original dict unchanged).
- Also assert `fill_placeholders(...)` still behaves exactly as before for `dict[str,str]` (one regression sanity case).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_resolution.py -v`
Expected: FAIL — `CaptureValue`/`render_typed` not defined.

- [ ] **Step 3: Write minimal implementation**

Add `CaptureValue` (dataclass) and `render_typed` to `resolution.py`. Reuse `_PLACEHOLDER_RE` for name discovery. Implement the recursion and the per-type substitution semantics above, including the None→`""` rule and the whole-field `object` check. Do **not** modify `fill_placeholders` or its callers.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_resolution.py -v`
Expected: PASS (new cases + existing `fill_placeholders` cases).

- [ ] **Step 5: Commit**

Run: `git add agctl/resolution.py tests/unit/test_resolution.py`
Run: `git commit -m "feat(resolution): add typed CaptureValue renderer (scalar/object/json)"`

---

### Task 3: Capture resolver (`agctl/mock/capture.py`)

**Files:**
- Create: `agctl/mock/capture.py`.
- Test: `tests/unit/test_mock_capture.py`.

**Interfaces:**
- Consumes: `jq_value(value, expr)` (`agctl/assertions.py:60-72`, returns first output or `None`, raises `ConfigError` on missing lib); `CaptureSpec` (`agctl/config/models.py`); `CaptureValue` (`agctl/resolution.py`).
- Produces (exact signature):
  - `resolve_captures(envelope: dict, captures: dict[str, CaptureSpec] | None) -> tuple[dict[str, CaptureValue], list[tuple[str, str]]]`
  - Behavior: when `captures` is `None` or empty → return `({}, [])`. Otherwise, for each `(name, spec)` in `captures` (insertion order): `raw = jq_value(envelope, spec.from_)`; build `CaptureValue(raw, spec.type)`; if `raw is None` → append `(name, spec.from_)` to the missing list. Return `(typed_map, missing_list)`.
  - `jq_value` raises `ConfigError` only on a missing jq library (propagates); all jq expression/runtime errors are swallowed to `None` inside `jq_value`, so they surface as missing — consistent with the soft-miss contract.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_mock_capture.py`, build envelopes + `CaptureSpec` dicts inline and assert on the returned tuple:
- Envelope `{"body": {"variables": {"id": 7}}}`, captures `{"op_id": CaptureSpec.model_validate({"from": ".body.variables.id"})}` → typed map `{"op_id": CaptureValue(7, "scalar")}`, missing `[]`.
- Envelope `{"headers": {"authorization": "Bearer x"}}`, `{"auth": CaptureSpec.model_validate({"from": ".headers.authorization"})}` → `CaptureValue("Bearer x", "scalar")`, missing `[]`.
- Envelope `{"key": "k-1"}`, `{"tid": CaptureSpec.model_validate({"from": ".key"})}` → `CaptureValue("k-1", "scalar")`, missing `[]`.
- Object type: envelope `{"value": {"context": {"conv": "abc"}}}`, `{"ctx": CaptureSpec.model_validate({"from": ".value.context", "type": "object"})}` → `CaptureValue({"conv":"abc"}, "object")`, missing `[]`.
- Missing path: envelope `{"body": {}}`, `{"x": CaptureSpec.model_validate({"from": ".body.nope"})}` → typed map `{"x": CaptureValue(None, "scalar")}`, missing `[("x", ".body.nope")]`.
- Multiple captures, one missing: assert both map entries and the single missing pair.
- `captures=None` → `({}, [])`; `captures={}` → `({}, [])`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_capture.py -v`
Expected: FAIL — module/function not defined.

- [ ] **Step 3: Write minimal implementation**

Create `agctl/mock/capture.py` with `resolve_captures` per the contract above. Import `jq_value` from `..assertions`, `CaptureSpec` from `..config.models`, `CaptureValue` from `..resolution`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_capture.py -v`
Expected: PASS. (If the `jq` extra is unavailable in CI, these tests need jq — gate them with `pytest.importorskip("jq")` at module top so they skip rather than fail where jq is not installed, mirroring how existing jq tests behave.)

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/capture.py tests/unit/test_mock_capture.py`
Run: `git commit -m "feat(mock): add envelope capture resolver (jq_value-based)"`

---

### Task 4: jq pre-compile walks `capture.*.from`

**Files:**
- Modify: `agctl/mock/jq_precompile.py` (extend `iter_mock_jq_expressions`, lines 26-51).
- Test: `tests/unit/test_jq_precompile.py`.

**Interfaces:**
- Consumes: `iter_mock_jq_expressions`, `compile_jq`, `collect_jq_compile_errors` (same file). `HttpStub.capture`, `KafkaReactor.capture` (Task 1).
- Produces: `iter_mock_jq_expressions` additionally yields, after today's `match.jq` / `match` yields, for each stub with a non-`None` `capture`: `("mocks.http.stubs.{name}.capture.{cap}.from", spec.from_)` for each `(cap, spec)`; and for each reactor with a non-`None` `capture`: `("mocks.kafka.reactors.{name}.capture.{cap}.from", spec.from_)`. Because `collect_jq_compile_errors` iterates this walker, `config validate` automatically compiles capture `from`s; and because `engine.py` Step 0 iterates it with `compile_jq`, `mock run` fails fast on a malformed `from`.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_jq_precompile.py`, build a `MocksConfig` with HTTP stub(s) and Kafka reactor(s) carrying `capture`, then assert `list(iter_mock_jq_expressions(mocks))`:
- Contains `("mocks.http.stubs.<name>.capture.<cap>.from", ".body.variables.id")` for an HTTP stub capture.
- Contains `("mocks.kafka.reactors.<name>.capture.<cap>.from", ".key")` for a Kafka reactor capture.
- Still contains the pre-existing `match.jq` / `match` labels (regression).
- Stubs/reactors without `capture` contribute no capture labels.
- `collect_jq_compile_errors(mocks)` returns an error whose `"path"` is a capture label when a `from` is malformed (e.g. `".body["`); returns `[]` for valid `from`s.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_jq_precompile.py -v`
Expected: FAIL — capture labels not yielded.

- [ ] **Step 3: Write minimal implementation**

Extend `iter_mock_jq_expressions`: in the HTTP loop, after the `match.jq` yield, if `stub.capture` is not None, yield each capture's `from_` with the label `f"mocks.http.stubs.{name}.capture.{cap}.from"`. In the Kafka loop, after the `match` yield, if `reactor.capture` is not None, yield each with `f"mocks.kafka.reactors.{name}.capture.{cap}.from"`. No change to `collect_jq_compile_errors` (it already iterates the walker).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_jq_precompile.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/jq_precompile.py tests/unit/test_jq_precompile.py`
Run: `git commit -m "feat(mock): pre-compile capture.from jq expressions at startup"`

---

### Task 5: `object`-placement static check (`agctl/mock/capture_validate.py`)

**Files:**
- Create: `agctl/mock/capture_validate.py`.
- Modify: `agctl/commands/config_commands.py` (merge errors near line 142, alongside `collect_jq_compile_errors`).
- Modify: `agctl/mock/engine.py` (Step 0, after the `compile_jq` loop ~line 181).
- Test: `tests/unit/test_mock_capture_validate.py`; extend `tests/unit/test_mock_engine.py`; extend `tests/integration/test_mock_commands.py`.

**Interfaces:**
- Consumes: `MocksConfig`, `CaptureSpec` (`..config.models`); `ConfigError` (`..errors`). No `assertions` dependency (pure-Python scan).
- Produces:
  - `collect_capture_placement_errors(mocks: MocksConfig | None) -> list[dict]` — returns `[]` when `mocks is None`. Otherwise, for each stub/reactor that has a `capture` containing one or more `type == "object"` entries, scan the template tree and return one `{"path": str, "message": str}` per violation. `path` is `f"mocks.http.stubs.{name}"` / `f"mocks.kafka.reactors.{name}"`.
  - Placement rule for an `object`-typed name `N`: it is **valid** only when some field in `response.body` (HTTP) / `reaction.value` (Kafka) is a string whose value is exactly `"{N}"`. It is a **violation** if `"{N}"` appears: (a) inline within a larger string anywhere in the tree; (b) as `reaction.key` (string-only slot); (c) inside `reaction.headers` values (string-only slot). `scalar`/`json` names are never flagged. (HTTP has no `key`/`headers` output slot to check; only `reaction` does.)

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_mock_capture_validate.py`, construct `MocksConfig` inline and assert the returned list:
- HTTP stub, `capture={"ctx": CaptureSpec.model_validate({"from":".body.ctx","type":"object"})}`, `response.body={"context": "{ctx}"}` → `[]` (whole-field, valid).
- HTTP stub, same capture, `response.body={"msg": "pre={ctx}"}` → one error with `path == "mocks.http.stubs.<name>"` (inline object).
- Kafka reactor, `capture={"ctx": CaptureSpec.model_validate({"from":".value.ctx","type":"object"})}`, `reaction.value={"context": "{ctx}"}` → `[]`.
- Kafka reactor, same capture, `reaction.key="{ctx}"` → one error (object in `key`).
- Kafka reactor, same capture, `reaction.headers={"x": "{ctx}"}` → one error (object in header value).
- Kafka reactor with `capture={"ctx": object}`, `reaction.value={"context": "{ctx}"}, key="{tid}"` where `tid` is `scalar` → `[]` (scalar in key is fine).
- `mocks=None` → `[]`.

Extend `tests/unit/test_mock_engine.py`: a `MocksConfig` with an inline-object violation → `MockEngine.start()` raises `ConfigError` during Step 0 (fail-fast). (Use the existing engine test fixtures/seams.)

Extend `tests/integration/test_mock_commands.py`: `agctl config validate` on a config with an object-placement violation reports the error (merge into the errors list, alongside jq-compile errors).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_capture_validate.py tests/unit/test_mock_engine.py tests/integration/test_mock_commands.py -v`
Expected: FAIL — `collect_capture_placement_errors` not defined; engine/config validate don't surface the violation.

- [ ] **Step 3: Write minimal implementation**

Create `agctl/mock/capture_validate.py` with `collect_capture_placement_errors`. Implementation behavior: walk each stub/reactor; for each object-typed capture name, scan the relevant template tree(s) for occurrences of `"{name}"` and classify each by position (whole-field dict/list value vs inline-in-string vs key vs header value), appending one error per violation with a clear message.

In `agctl/commands/config_commands.py`, after the existing `errors = errors + collect_jq_compile_errors(cfg.mocks)` (~line 142), add `errors = errors + collect_capture_placement_errors(cfg.mocks)` (import from `agctl.mock.capture_validate`).

In `agctl/mock/engine.py` Step 0, after the `compile_jq` loop, add a fail-fast: compute `errs = collect_capture_placement_errors(self._mocks)`; if non-empty, raise `ConfigError(errs[0]["message"], {"path": errs[0]["path"]})` so it propagates through the outer `try` → shutdown → envelope (exit 2), matching how `compile_jq` errors propagate.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_capture_validate.py tests/unit/test_mock_engine.py tests/integration/test_mock_commands.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/capture_validate.py agctl/commands/config_commands.py agctl/mock/engine.py tests/unit/test_mock_capture_validate.py tests/unit/test_mock_engine.py tests/integration/test_mock_commands.py`
Run: `git commit -m "feat(mock): static object-placement check for typed capture at startup"`

---

### Task 6: HTTP wiring — envelope + resolve + typed render

**Files:**
- Modify: `agctl/mock/http_server.py` (`_handle_request`, lines ~211-281; reuse `self.headers`, `self.command`, `self.path`, `parsed_body`).
- Test: `tests/unit/test_mock_http_server.py`.

**Interfaces:**
- Consumes: `render_typed`, `CaptureValue` (`..resolution`); `resolve_captures` (`.capture`); `CaptureSpec` (via `stub.capture`); `urlsplit` (from `urllib.parse`, already used by `routing`/match). `emit_event` is the closure captured by `make_handler` — call as `emit_event({...})` with no `timestamp` (engine stamps it).
- Produces: HTTP request envelope `{method, path, headers, body}` (documented shape for capture `from` paths). Emits `capture.missing` events into the HTTP event stream.
- Behavior changes in `_handle_request`:
  - Build `envelope` after `parsed_body` is known: `method = self.command`; `path = urlsplit(self.path).path` (query stripped); `headers = {k.lower(): v for k, v in self.headers.items()}`; `body = parsed_body`.
  - Build the implicit typed capture context the same way as today, but as `dict[str, CaptureValue]`: path params → `CaptureValue(param_str, "scalar")`; top-level body keys (when `parsed_body` is a dict) → `CaptureValue(raw_value, "scalar")`.
  - If `stub.capture` is not None: `(explicit, missing) = resolve_captures(envelope, stub.capture)`; merge `explicit` into the context (explicit overrides — name and type); for each `(name, from_path)` in `missing`, call `emit_event({"event": "capture.missing", "stub": stub_name, "name": name, "from": from_path})`.
  - Replace the two `fill_placeholders(...)` calls (lines 276, 278) with `render_typed(stub.response.body, context)` and `render_typed(stub.response.headers or {}, context)`.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_mock_http_server.py`, use the existing `start_server(stubs, emit_event)` + `httpx.Client()` seam and the `event_sink` fixture. New cases:
- Stub `POST /graphql` with `capture={"op_id": CaptureSpec.model_validate({"from": ".body.variables.id"})}` and `response.body={"id": "{op_id}"}`. POST body `{"query": "...", "variables": {"id": 7}}` → response JSON `{"id": "7"}`; one `http.hit` event.
- Header capture: stub with `capture={"auth": CaptureSpec.model_validate({"from": ".headers.authorization"})}`, `response.body={"a": "{auth}"}`; request header `Authorization: Bearer z` → response `{"a": "Bearer z"}`.
- Object pass-through: stub with `capture={"ctx": CaptureSpec.model_validate({"from": ".body.ctx", "type": "object"})}`, `response.body={"context": "{ctx}"}`; POST `{"ctx": {"conv": "abc"}}` → response `{"context": {"conv": "abc"}}` (real object, verified by JSON parse of the response body).
- Override: stub with both a top-level body key `ctx` and an explicit `capture={"ctx": object}` → the explicit object wins (response field is an object, not a stringified dict).
- Missing: stub with `capture={"x": CaptureSpec.model_validate({"from": ".body.nope"})}`; POST `{}` → response field renders `""`; the `event_sink` contains a `capture.missing` event with `name=="x"` and `from==".body.nope"`.
- Regression: the existing numeric-capture / path-param behavior still holds (a stub with no `capture` stringifies body keys as before).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_http_server.py -v`
Expected: FAIL — `capture` not honored; `capture.missing` not emitted.

- [ ] **Step 3: Write minimal implementation**

Modify `_handle_request` per the behavior above: construct the envelope, build the typed context (implicit as `scalar` `CaptureValue`s), resolve+merge explicit captures, emit `capture.missing`, and render via `render_typed`. Leave the no-match (404) path unchanged.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_http_server.py -v`
Expected: PASS (new + existing cases).

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/http_server.py tests/unit/test_mock_http_server.py`
Run: `git commit -m "feat(mock): HTTP envelope capture + typed render + capture.missing"`

---

### Task 7: Kafka wiring — envelope + resolve + typed render

**Files:**
- Modify: `agctl/mock/kafka_reactor.py` (`_handle`, lines ~126-186).
- Test: `tests/unit/test_mock_kafka_reactor.py`.

**Interfaces:**
- Consumes: `render_typed`, `CaptureValue` (`..resolution`); `resolve_captures` (`.capture`); the normalized `msg` dict (`{key, value, partition, offset, timestamp, headers}`) which **is** the envelope. `self._emit_event({...})` (bound callable; no timestamp — engine stamps it).
- Produces: emits `capture.missing` events into the Kafka reactor event stream.
- Behavior changes in `_handle` (after the value-is-dict check and the `jq_bool` match):
  - The envelope is `msg` itself (already normalized by `_normalize_message`).
  - Build the implicit typed context as `dict[str, CaptureValue]`: top-level `value` keys → `CaptureValue(raw_value, "scalar")` (replaces `{k: str(v) ...}` at line 148).
  - If `self._config.capture` is not None: `(explicit, missing) = resolve_captures(msg, self._config.capture)`; merge `explicit` (override); for each `(name, from_path)` in `missing`, `self._emit_event({"event": "capture.missing", "reactor": self._name, "name": name, "from": from_path})`.
  - Replace the three `fill_placeholders(...)` calls (lines 152, 157, 162) with `render_typed(...)`.

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_mock_kafka_reactor.py`, follow the `test_reactor_numeric_capture_coercion` pattern (`FakeKafkaClient(messages=[...])`, assert `client.produce_calls[0]`). Provide message `headers` as a **dict** (e.g. `{"rqUID": "r-1"}`), not a list, for header-capture cases. New cases:
- Key capture: reactor with `capture={"tid": CaptureSpec.model_validate({"from": ".key"})}`, `reaction.value={"threadId": "{tid}"}`; message `value={"command":"SEARCH"}, key="k-9"` → `produce_calls[0]["value"] == {"threadId": "k-9"}`.
- Header capture (case-sensitive): `capture={"rqUID": CaptureSpec.model_validate({"from": ".headers.rqUID"})}`, `reaction.value={"rs_headers": {"rqUID": "{rqUID}"}}`; message `headers={"rqUID": "r-1"}` → produced value's `rs_headers.rqUID == "r-1"`. Also assert `.headers.rquid` (lowercase) would miss (separate case or note).
- Object pass-through (`contextEcho`): `capture={"ctx": CaptureSpec.model_validate({"from": ".value.context", "type": "object"})}`, `reaction.value={"context": "{ctx}"}`; message `value={"context": {"conversationId":"abc","eventType":"X"}}` → produced value `{"context": {"conversationId":"abc","eventType":"X"}}` (real object, asserts the E2E `kafkaThreadHistoryFlow` expectation).
- Override: a `context` top-level value key plus explicit `object` capture → object wins (not `str(dict)`).
- Missing: `capture={"x": CaptureSpec.model_validate({"from": ".value.nope"})}`, message `value={}` → produced value field `""`; the event sink contains `capture.missing` with `name=="x"`.
- Regression: `test_reactor_numeric_capture_coercion` still passes (`orderId` 42 → `"42"`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_kafka_reactor.py -v`
Expected: FAIL — `capture` not honored.

- [ ] **Step 3: Write minimal implementation**

Modify `_handle` per the behavior above: typed implicit context, resolve+merge explicit captures against the `msg` envelope, emit `capture.missing`, render via `render_typed`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_kafka_reactor.py -v`
Expected: PASS (new + existing).

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/kafka_reactor.py tests/unit/test_mock_kafka_reactor.py`
Run: `git commit -m "feat(mock): Kafka envelope capture (key/headers) + object pass-through"`

---

### Task 8: Acceptance fixtures — the four battle-test scenarios

**Files:**
- Create: `tests/integration/test_mock_capture_e2e.py`.

**Interfaces:**
- Consumes: the HTTP `start_server` + `httpx` seam (port from `tests/unit/test_mock_http_server.py` — duplicate the small helper or import it); `FakeKafkaClient` (duplicate or import from `tests/unit/test_mock_kafka_reactor.py`); the live Kafka testcontainers harness under `AGCTL_TEST_LIVE=1` for the broker-backed case.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_mock_capture_e2e.py`, four named acceptance tests replicating the battle-test report:
- `test_graphql_operatorById`: HTTP stub matching `.query | test("operatorById")` (body-root, unchanged) with `capture={"op_id": {.body.variables.id}}` → response `{id: "{op_id}"}` returns the nested id.
- `test_epk_chatSearch`: HTTP stub with `capture={fname: {.body.clientInfoCriteria.firstName}, ucp: {.body.ucpID}}` → response echoes both.
- `test_chatx_it_mock_contextEcho` (Kafka, `FakeKafkaClient`): reactor `capture={tid: {.key}, rqUID: {.headers.rqUID}, ctx: {.value.context, object}}` → produced value carries `threadId` from key, `rs_headers.rqUID` from header, and `context` as a real object.
- `test_kafka_threadhistory_live` (Kafka, gated `pytest.mark.skipif(not AGCTL_TEST_LIVE)`): end-to-end through a real broker — produce a command, assert the reacted event using the existing testcontainers harness.

Each asserts the exact rendered output matching the battle-test expectation (real object for `context`, correct key/header echo).

- [ ] **Step 2: Run test to verify it fails (or skip for live)**

Run: `pytest tests/integration/test_mock_capture_e2e.py -v`
Expected: the three non-live tests PASS (Tasks 6+7 already implemented the behavior) — this step verifies the acceptance fixtures pass against the finished implementation. The live test SKIPS unless `AGCTL_TEST_LIVE=1`. (If any non-live case fails, return to Task 6/7.)

- [ ] **Step 3: No implementation step**

Acceptance fixtures only — no production code. If a case fails, the bug is in Tasks 6/7, fixed there.

- [ ] **Step 4: Run full mock suite to verify no regressions**

Run: `pytest tests/unit/test_mock_*.py tests/unit/test_resolution.py tests/unit/test_jq_precompile.py tests/unit/test_mock_capture*.py tests/integration/test_mock_capture_e2e.py tests/integration/test_mock_commands.py -v`
Expected: PASS (live Kafka case skips).

- [ ] **Step 5: Commit**

Run: `git add tests/integration/test_mock_capture_e2e.py`
Run: `git commit -m "test(mock): add battle-test acceptance fixtures for capture (gaps 1-3)"`

---

### Task 9: Docs sync via `docs-watcher`

**Files:**
- Modified by subagent: `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/agctl-config/reference/mocks.md` (per spec §13).

**Interfaces:**
- Consumes: the approved spec §13 (Docs & Skill Impact). The `docs-watcher` subagent (CLAUDE.md "Docs Sync").

- [ ] **Step 1: Dispatch the docs-watcher subagent**

Invoke the `docs-watcher` subagent with the change summary: "Implemented envelope-rooted `capture:` for HTTP stubs and Kafka reactors (CaptureSpec: from/type scalar|object|json). Closes gaps ①②③ from battle testing. `match` stays payload-rooted (#22 tracks unification). New `capture.missing` event." Direct it to DESIGN.md (§2.1 schema ref add `capture`; §10 move nested-field/header capture + JSON-type pass-through from deferred to implemented; add the `.` divergence note referencing #22), ARCHITECTURE.md (§9 note `jq_value` now used for mock capture; §15 drop the two now-covered bullets, add the `.` divergence limitation), and `skills/agctl-config/reference/mocks.md` (capture authoring, envelope roots incl. header-casing asymmetry, type semantics, object whole-field rule, override-on-collision, `capture.missing` in the failure-stream grep set).

- [ ] **Step 2: Verify doc changes**

Run: `git diff --stat docs/ skills/`
Expected: non-empty diff touching the three files above; DESIGN.md preserves its WHAT/WHY altitude, ARCHITECTURE.md its HOW altitude (the docs-watcher enforces this).

- [ ] **Step 3: No test step**

Docs only.

- [ ] **Step 4: Final full-suite verification**

Run: `pytest -q`
Expected: full suite PASS (live tests skip without their env flags).

- [ ] **Step 5: Commit**

Run: `git add docs/ skills/`
Run: `git commit -m "docs(mock): sync DESIGN/ARCHITECTURE/skills for envelope-rooted capture"`

---

## Self-Review (run after writing — recorded outcomes)

1. **Code scan:** No method bodies, algorithms, or test/impl code in the plan — only signatures, data shapes, behavior descriptions, and expected results. ✅
2. **Self-containment:** Every task states exact signatures (`CaptureSpec`, `CaptureValue`, `render_typed`, `resolve_captures`, `collect_capture_placement_errors`), data shapes (envelope, event dicts), and the consumes/produces contracts a zero-context implementer needs. ✅
3. **Spec coverage:** spec §5 C1→T1; C2 (envelope roots)→T6/T7 + Global Constraints; C3 (types)→T2; C4 (override)→T6/T7; C5 (reuse jq_value)→T3; C6 (startup fail-loud)→T4 (jq) + T5 (object placement); C7 (capture.missing)→T3/T6/T7. §6 config contract→T1; §7 resolution rules→T2/T3/T6/T7; §8 error model→T4/T5/T6/T7; §9 validation→T4/T5; §10 testing→T6/T7/T8; §13 docs→T9. All spec sections mapped. ✅
4. **Placeholder scan:** No TBD/TODO/"add error handling"/"similar to" — each step names exact scenario + expected result. ✅
5. **Type consistency:** `CaptureSpec.from_` (aliased `from`) used consistently T1→T3→T4→T5→T6→T7. `CaptureValue(value, type)` consistent T2→T3→T6→T7. `resolve_captures(...) -> tuple[dict, list[tuple[str,str]]]` consistent T3→T6→T7. `render_typed(value, captures)` consistent T2→T6→T7. `collect_capture_placement_errors(mocks) -> list[dict]` consistent T5. ✅

Two conscious spec refinements are called out in the "Spec Refinements" section (new renderer vs mutating `fill_placeholders`; None→`""` missing-capture rule) — behavior matches the spec; only the mechanism differs.
