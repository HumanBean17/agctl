# HTTP jq (Assertions + Mock Stub Matching) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add HTTP response assertion flags to `http call`/`http request` and a `match.jq` predicate to mock HTTP stubs, reusing the existing jq/subset engine, with a compile-only `compile_jq` startup guard covering both HTTP stubs and the existing Kafka reactor `match`.

**Architecture:** Two additive features over the existing `agctl/assertions.py` engine. Feature (2) adds two helpers: a pre-request `validate_http_assertion_args` (pairing + `--contains` JSON parse → `ConfigError` before the request is sent) and a post-request `evaluate_http_assertions(result, …)` (response evaluation → `AssertionFailure` with `error.detail = {response, failures:[…]}`), both invoked from the `http call`/`request` `_core` functions (already wrapped by `@envelope`). Feature (1) adds an optional `jq: str | None` to `HttpMatch`, evaluated in `mock/http_server.py::_handle_request` right after the existing `json_subset(match.body, …)` check. A new `compile_jq(expr)` helper (compile-only, no `.input().all()`) is pre-run at `MockEngine.start()` and in `config validate` over every `match.jq` and every Kafka reactor `match`, turning authoring typos into a loud `ConfigError` (exit 2) instead of a silent mis-match / inert reactor.

**Tech Stack:** Python ≥3.11, Pydantic v2, Click 8, stdlib `http.server`, PyPI `jq` (Cython binding over C libjq) — already bundled in the `kafka`/`db` extras; this plan adds a dedicated `jq` extra.

**Spec:** `docs/superpowers/specs/active/2026-07-04-agctl-http-jq-design.md` (commit f99da37) — the source of truth. Read it fully before starting.

## Global Constraints

Copy verbatim into every task's mental model:

- **No `version` bump** (additive under major `"1"`); no new entry-point group; no new command group. Both features wire into existing commands/config.
- **Lazy-import convention:** never `import jq` at module top-level — always via `_jq()` in `agctl/assertions.py`. A missing library surfaces as `ConfigError` (exit 2), never `ModuleNotFoundError`. This preserves the HTTP-only zero-dep mock (a stub without `match.jq` never imports jq).
- **Naming:** the assertion flag is `--jq-path` (NOT `--path` — `--path` collides with the URL `--path` on `http request`/`http ping`; Click cannot register both). `--jq-path` pairs with `--equals`.
- **Raise `AssertionFailure(message, detail)` — the `AgctlError` subclass — NOT a builtin `assert`/`AssertionError`.** `@envelope` has two branches that both emit `error.type:"AssertionError"`: the `AgctlError` branch (`command.py:31-38`, preserves `err.detail` via `to_dict()`) and the builtin-`AssertionError` branch (`command.py:39-47`, hardcodes `detail:{}`). Only the former preserves `detail.response`/`failures`; a builtin `assert` discards the payload.
- **Assertion-failure envelope (pinned):** on failure, raise `AssertionFailure(message, detail)` where `detail = {"response": <full http result dict>, "failures": [<per-mode entries>]}`. `@envelope` then emits `ok:false`, `error.type:"AssertionError"`, `result:null`, exit 1. The full response always rides in `error.detail.response`.
- **Per-mode failure entries (pinned, agents parse this):**
  - `status` → `{"mode":"status", "expected":<int>, "actual":<status_code int>}`
  - `contains` → `{"mode":"contains", "needle":<parsed --contains JSON>, "matched":false}`
  - `match` → `{"mode":"match", "expr":<--match string>, "result":false}`
  - `jq-path` → `{"mode":"jq-path", "path":<--jq-path expr>, "expected":<parse_equals(--equals)>, "actual":<jq_value result or null>}`
- **`--match` ANY-semantic:** `jq_bool` is true on ANY truthy output. The "all items" idiom is the **semicolon** form `all(.items[]; .amount > 100)` — the comma form `all(.items[].amount > 100)` is invalid jq; never use it.
- **D5 pre-compile covers both transports:** every HTTP `match.jq` AND every Kafka reactor `match` is compiled at `MockEngine.start()` and in `config validate`. This is a **deliberate loudness change** for malformed reactor `match` configs (previously started silently inert; now exit 2) — the spec's §3 "byte-for-byte unchanged" claim has a documented carve-out for this case.
- **Layering (ARCHITECTURE §3):** `config/*` modules must NOT import `agctl.assertions` (config depends on errors only). The jq pre-compile inside `config validate` therefore lives in the **command layer** (`agctl/commands/config_commands.py`), not in `agctl/config/validator.py`. The shared walker lives in a new `agctl/mock/jq_precompile.py` (may import `config.models` + `assertions.compile_jq`).
- **Tests:** run `pytest tests/unit -q` after every task. Integration tests self-skip unless `AGCTL_TEST_LIVE=1` (Task 11(a) is the documented exception — always-runs, no Docker).
- **Commits:** one conventional-commit per task (`feat:`, `test:`, `refactor:`, `docs:` as appropriate). End commit messages with `Co-Authored-By: Claude <noreply@anthropic.com>`.

---

## File Structure

**New files:**
- `agctl/mock/jq_precompile.py` — shared walker + validate-collection helper (iterates `MocksConfig`, calls `compile_jq`). Depends on `config.models` + `assertions.compile_jq`.
- `tests/unit/test_jq_precompile.py` — unit tests for the walker.
- `tests/unit/test_packaging.py` — unit test for the new `jq` extra.
- `tests/unit/test_config_commands.py` — CliRunner test for jq-compile errors surfaced by `config validate`.

**Modified files:**
- `pyproject.toml` — add `jq = ["jq>=1.6"]` extra.
- `agctl/config/models.py` — `HttpMatch` gains `jq: str | None = None`.
- `agctl/assertions.py` — add `compile_jq(expr, *, label=None)`, `validate_http_assertion_args(...)`, and `evaluate_http_assertions(...)`.
- `agctl/mock/http_server.py` — `_handle_request` evaluates `match.jq` after the `match.body` check; import `jq_bool`.
- `agctl/mock/engine.py` — `start()` runs the pre-compile as Step 0 (before the Kafka probe).
- `agctl/config/validator.py` — add Check 4 (jq-shadowing warning, method-gated, both-stubs-have-jq).
- `agctl/commands/http_commands.py` — `http_call`/`http_request` gain `--status`/`--contains`/`--match`/`--jq-path`/`--equals`; the `_core` functions call `validate_http_assertion_args` (pre-request) then `evaluate_http_assertions` (post-request).
- `agctl/commands/config_commands.py` — `config_validate` merges jq-compile errors.

**Extended test files:** `tests/unit/test_assertions.py`, `tests/unit/test_mock_models.py`, `tests/unit/test_mock_engine.py`, `tests/unit/test_mock_http_server.py`, `tests/unit/test_validator.py`, `tests/unit/test_http_commands.py`, `tests/integration/test_mock_commands.py`, `tests/integration/test_http_commands.py`.

---

### Task 1: Add the dedicated `jq` extra to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml:33-42` (`[project.optional-dependencies]`)
- Test: `tests/unit/test_packaging.py` (new file)

**Interfaces:**
- Produces: a `jq` optional-dependency group whose value is exactly `["jq>=1.6"]`, so `pip install 'agctl[jq]'` installs the jq library. Existing `kafka`/`db`/`http`/`dev`/`integration` groups are unchanged.

- [ ] **Step 1: Write the failing test**

A new `tests/unit/test_packaging.py` that loads `pyproject.toml` with `tomllib`, reads `[project.optional-dependencies]`, and asserts a `jq` key exists with value `["jq>=1.6"]`, and that `kafka`/`db`/`http` are unchanged. Expected: the `jq` key is present and equal to `["jq>=1.6"]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_packaging.py -q`
Expected: FAIL — `jq` key absent in optional-dependencies.

- [ ] **Step 3: Write minimal implementation**

Add `jq = ["jq>=1.6"]` as a new line under `[project.optional-dependencies]`. Do not modify any other extra.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_packaging.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add pyproject.toml tests/unit/test_packaging.py`
Run: `git commit -m "feat(packaging): add dedicated 'jq' extra for HTTP/mock jq features"` + `Co-Authored-By` trailer.

---

### Task 2: Add `jq: str | None` field to `HttpMatch`

**Files:**
- Modify: `agctl/config/models.py:162-165` (`class HttpMatch`)
- Test: `tests/unit/test_mock_models.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `HttpMatch` now has two optional fields — `body: dict | None = None` (unchanged) and `jq: str | None = None` (new). A stub matches iff method ∧ path ∧ (`body` absent or `json_subset` passes) ∧ (`jq` absent or `jq_bool` passes). Existing configs without `jq` load unchanged (backward-compatible — default `None`).

- [ ] **Step 1: Write the failing test**

Tests constructing `HttpMatch` three ways: (a) no args → `body is None` and `jq is None`; (b) `HttpMatch(jq='.amount > 1000')` → `body is None`, `jq == '.amount > 1000'`; (c) `HttpMatch(body={"priority": "high"}, jq='.amount > 1000')` → both set (coexist). Expected: all three parse and expose the fields as stated.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_models.py -q`
Expected: FAIL — `HttpMatch` has no `jq` attribute.

- [ ] **Step 3: Write minimal implementation**

Add `jq: str | None = None` to `HttpMatch` (after the existing `body` field). No validator needed — syntax is checked later by `compile_jq`. Update the class docstring to note both fields and that `jq` is a jq predicate coexisting with `body`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_models.py tests/unit/test_loader.py -q`
Expected: PASS (no model/load regression).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/models.py tests/unit/test_mock_models.py`
Run: `git commit -m "feat(mock): add optional jq predicate to HttpMatch (coexists with body)"` + trailer.

---

### Task 3: Add `compile_jq(expr, *, label=None)` to `assertions.py`

**Files:**
- Modify: `agctl/assertions.py` (add after `jq_value`, ~line 57)
- Test: `tests/unit/test_assertions.py`

**Interfaces:**
- Consumes: `_jq()` (lazy import, raises `ConfigError` on missing library).
- Produces: `compile_jq(expr: str, *, label: str | None = None) -> None` — compiles `expr` via `_jq().compile(expr)` **without** `.input(value).all()` (compile-only). On success returns `None`. On a missing jq library, re-raises a `ConfigError` whose message points at `pip install 'agctl[jq]'` (the base `_jq()` message names only db/kafka — it MUST be rewritten for the HTTP/mock context; include `label`). On any compile-time exception (e.g. `ValueError` from a malformed expression), raises `ConfigError` whose message includes `label`, the expression, and the underlying error — so the surfaced type is `ConfigError` (exit 2), NOT `InternalError` from the envelope catch-all. **Distinct from `jq_bool`**, which wraps compile+eval in `except Exception: return False` (correct for runtime matching, wrong for the startup guard).

- [ ] **Step 1: Write the failing test**

Tests in `test_assertions.py`: (a) `compile_jq('.a == 1')` returns `None`; (b) `compile_jq(')(')` raises `ConfigError`; (c) `compile_jq('.amount >')` raises `ConfigError` (truncated expression — the case `jq_bool` would silently swallow); (d) the raised `ConfigError.message` includes the `label` when one is passed; (e) **contrast:** `jq_bool({}, ')(')` still returns `False` (the two helpers differ on the same input); (f) missing-jq — `monkeypatch.setitem(sys.modules, "jq", None)`, then `compile_jq('.a')` raises `ConfigError` whose message mentions `agctl[jq]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: FAIL — `compile_jq` not defined.

- [ ] **Step 3: Write minimal implementation**

Add `compile_jq(expr, *, label=None) -> None` per the Produces contract: call `_jq().compile(expr)` inside a try; on the missing-library `ConfigError` from `_jq()` re-raise a `ConfigError` whose message points at `pip install 'agctl[jq]'` (and includes label) — the base `_jq()` message names only db/kafka and MUST be replaced here; on any other `Exception` (compile error) raise `ConfigError` with a message combining label, expr, and the underlying error. The function must NOT call `.input().all()`. Add a docstring stating it is compile-only and distinct from `jq_bool`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: PASS (new cases + existing jq_bool/jq_value cases pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/assertions.py tests/unit/test_assertions.py`
Run: `git commit -m "feat(assertions): add compile_jq compile-only helper (loud on authoring typos)"` + trailer.

---

### Task 4: Create `agctl/mock/jq_precompile.py` (walker + validate collector)

**Files:**
- Create: `agctl/mock/jq_precompile.py`
- Test: `tests/unit/test_jq_precompile.py`

**Interfaces:**
- Consumes: `MocksConfig` (from `agctl.config.models`), `HttpMatch.jq` (Task 2 — the walker reads `stub.match.jq`), `compile_jq` (Task 3), `ConfigError` (from `agctl.errors`).
- Produces:
  - `iter_mock_jq_expressions(mocks: MocksConfig | None) -> Iterator[tuple[str, str]]` — yields `(path_label, expr)` for every HTTP stub with a non-None `match.jq` (label `f"mocks.http.stubs.{name}.match.jq"`) and every Kafka reactor with a non-None `match` (label `f"mocks.kafka.reactors.{name}.match"`), in stable order (stubs first in dict order, then reactors in dict order). `mocks is None` → yields nothing.
  - `collect_jq_compile_errors(mocks: MocksConfig | None) -> list[dict]` — iterates the walker, calls `compile_jq(expr, label=label)` inside try/except; on `ConfigError` appends `{"path": label, "message": err.message}` to the result; continues (collects ALL errors, does not raise). Returns the list (empty if all valid / mocks None).

- [ ] **Step 1: Write the failing test**

Tests in `test_jq_precompile.py`: (a) `iter_mock_jq_expressions(None)` yields nothing; (b) a `MocksConfig` with one HTTP stub whose `match.jq='.a>1'` and one Kafka reactor whose `match='.b==2'` → `list(iter_mock_jq_expressions(mocks))` returns exactly two `(label, expr)` pairs with correct labels/exprs; (c) a stub with `match=HttpMatch(body={"x":1})` (jq None) is skipped; a reactor with `match=None` is skipped; (d) `collect_jq_compile_errors` on a config with a malformed stub `match.jq')('` returns a one-element list `[{path, message}]` whose `path == "mocks.http.stubs.<name>.match.jq"`; (e) on a fully-valid config returns `[]`; (f) collects TWO errors when both a stub and a reactor are malformed (does not stop at the first).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_jq_precompile.py -q`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `agctl/mock/jq_precompile.py` with the two functions per the Produces contract. `iter_mock_jq_expressions` guards `mocks is None`, then iterates `mocks.http.stubs.items()` (yielding when `stub.match and stub.match.jq is not None`), then `mocks.kafka.reactors.items()` (yielding when `reactor.match is not None`). `collect_jq_compile_errors` loops the walker, calls `compile_jq(expr, label=label)`, catches `ConfigError`, appends `{path, message}`. Import only `config.models` + `assertions.compile_jq` + `errors`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_jq_precompile.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/jq_precompile.py tests/unit/test_jq_precompile.py`
Run: `git commit -m "feat(mock): add jq_precompile walker + validate-error collector"` + trailer.

---

### Task 5: Pre-compile jq expressions in `MockEngine.start()` (Step 0)

**Files:**
- Modify: `agctl/mock/engine.py:156-245` (`MockEngine.start`)
- Test: `tests/unit/test_mock_engine.py`

**Interfaces:**
- Consumes: `iter_mock_jq_expressions` + `compile_jq` (Task 4/3); the existing startup try/except → `shutdown()` → re-raise (engine.py:242-245) and `mock_run`'s `except AgctlError` → envelope (mock_commands.py:176-184).
- Produces: `start()` runs a new "Step 0" before the Kafka probe (Step 1): `for label, expr in iter_mock_jq_expressions(self._mocks): compile_jq(expr, label=label)`. A failure raises `ConfigError` → existing outer try catches → `shutdown()` → re-raise → `mock_run` emits the startup envelope (exit 2) before any event line. A mock with no jq expressions executes Step 0 as a no-op (imports nothing — zero-dep preserved for HTTP-only). This realizes D5 (loud-on-typo) + D6 (jq imported at startup, not first request) for the mock.

- [ ] **Step 1: Write the failing test**

Tests in `test_mock_engine.py` (use `run_http=True`/`run_kafka=False`, `http_listen="127.0.0.1:0"`): (a) an HTTP stub whose `match.jq` is malformed (e.g. `')('`) → `engine.start()` raises `ConfigError` (and does NOT emit `started`); (b) a Kafka reactor whose `match` is malformed → raises `ConfigError`; (c) an HTTP stub with a valid `match.jq` but jq "missing" (`monkeypatch.setitem(sys.modules, "jq", None)`) → raises `ConfigError` (missing extra surfaces at startup); (d) a config with only body-only stubs (no `match.jq`, no reactor `match`) → jq is NOT imported: `monkeypatch.setitem(sys.modules, "jq", None)` (so any `import jq` raises `ModuleNotFoundError`), build body-only stubs, assert `start()` reaches the started line without raising — proving the walker never entered a jq-import path.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_engine.py -q`
Expected: FAIL — start() does not pre-compile.

- [ ] **Step 3: Write minimal implementation**

In `MockEngine.start()`, inside the existing `try:` block, **before** `if self._run_kafka:` (engine.py:169), add Step 0: `for label, expr in iter_mock_jq_expressions(self._mocks): compile_jq(expr, label=label)`. Add imports `from ..assertions import compile_jq` and `from .jq_precompile import iter_mock_jq_expressions`. The existing `except Exception: self.shutdown(); raise` (engine.py:242-245) handles `ConfigError` → re-raise; `mock_run`'s `except AgctlError` (mock_commands.py:176) emits the envelope. No change to shutdown/started logic.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_engine.py tests/unit/test_mock_commands.py -q`
Expected: PASS (new + existing startup/probe/bind tests pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/engine.py tests/unit/test_mock_engine.py`
Run: `git commit -m "feat(mock): pre-compile jq expressions at engine startup (loud on typos)"` + trailer.

---

### Task 6: Evaluate `match.jq` in `mock/http_server.py::_handle_request`

**Files:**
- Modify: `agctl/mock/http_server.py:13` (import) and `:220-238` (stub-match loop)
- Test: `tests/unit/test_mock_http_server.py`

**Interfaces:**
- Consumes: `HttpMatch.jq` (Task 2), `jq_bool` (existing), `parsed_body` (already computed at http_server.py:214).
- Produces: inside the per-stub match loop, immediately after the existing `match.body` check (http_server.py:231-233), if `stub.match and stub.match.jq is not None`, evaluate `jq_bool(parsed_body, stub.match.jq)`; if `False`, `continue`. A predicate that raises is swallowed to `False` by `jq_bool` (soft non-match). A stub with both `body` and `jq` requires both to pass (AND). First stub whose full predicate passes wins (insertion order).

- [ ] **Step 1: Write the failing test**

Tests in `test_mock_http_server.py` using the existing `start_server` + httpx pattern: (a) one stub with `match.jq='.amount > 1000'`, 201 response — POST `amount: 1500` → 201 + `http.hit`; (b) same stub, POST `amount: 500` → 404 + `http.unmatched` (predicate false → fall through); (c) two stubs same method+path distinguished by `jq` (high-value→201/APPROVED; low-value→202/QUEUED) — POST 1500 hits the first, POST 500 hits the second; (d) coexist: a stub with `match.body={"priority":"high"}` AND `match.jq='.amount>1000'` — a request matching only one → 404, matching both → hit; (e) predicate-raises-soft: a stub with `match.jq='.a.b.c'` on a body without `.a` → 404 + `http.unmatched` (no 500); (f) non-JSON body: a stub with `match.jq='.amount>1000'`, POST with a plain-text body (no JSON content-type) → 404 + `http.unmatched` (`parsed_body` is `None`; `jq_bool(None, expr)` returns `False` → soft non-match → fall through).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_http_server.py -q`
Expected: FAIL — `match.jq` ignored.

- [ ] **Step 3: Write minimal implementation**

Add `jq_bool` to the existing `from agctl.assertions import …` import (http_server.py:13). In `_handle_request`, after the `if stub.match and stub.match.body is not None:` block (http_server.py:231-233), add: `if stub.match and stub.match.jq is not None: if not jq_bool(parsed_body, stub.match.jq): continue`. Do not change capture/reaction/emission.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_http_server.py -q`
Expected: PASS (new + existing body-match/capture/template tests pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/http_server.py tests/unit/test_mock_http_server.py`
Run: `git commit -m "feat(mock): evaluate match.jq predicate in HTTP stub matching"` + trailer.

---

### Task 7: Mandatory jq-shadowing warning in `validator.py` (method-gated)

**Files:**
- Modify: `agctl/config/validator.py:135-153` (add Check 4 after Check 3)
- Test: `tests/unit/test_validator.py`

**Interfaces:**
- Consumes: `cfg.mocks.http.stubs` (Task 2's `HttpMatch.jq`); the existing warning-dict shape `{"path": str, "message": str}`.
- Produces: a new validation warning when two stubs share the **same method (case-insensitive) AND the same path template AND both have a non-None `match.jq`** — the spec §10 "distinguished only by jq" case (two predicate-based stubs on the same route = the branching scenario whose wrong-branch risk §8.1 calls out). The check MUST gate on method equality — the existing path-template-shadowing Check 3 (validator.py:135-151) compares path segments only and is method-agnostic (a known limitation); do NOT copy that pattern blindly, else `GET /api/{id}` and `DELETE /api/users` would false-warn. Warning shape: `{"path": f"mocks.http.stubs.{later_name}", "message": f"Stub '{later_name}' is shadowed by '{earlier_name}' — same method+path and both use match.jq (first match wins; a wrong predicate can fire the wrong branch silently)."}`.

- [ ] **Step 1: Write the failing test**

Tests in `test_validator.py` building a `Config` and calling `validate_config(cfg)`: (a) two POST stubs same path, **both** with `match.jq` → exactly one warning referencing the later stub; (b) two stubs same path but DIFFERENT methods (POST vs DELETE), both with `match.jq` → NO warning (method gate); (c) two stubs same method+path, one with `match.jq` and one with `match.body` (no jq) → NO warning (not "distinguished only by jq"; jq-vs-body is out of scope for Check 4); (d) two stubs different paths → no warning; (e) a single stub → no warning.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_validator.py -q`
Expected: FAIL — no jq-shadowing warning emitted.

- [ ] **Step 3: Write minimal implementation**

Add "Check 4: jq-shadowing" after Check 3 (validator.py:135-151), within the `if cfg.mocks is not None and cfg.mocks.http is not None:` block. Iterate pairs `(earlier, later)` in insertion order; for each pair where `earlier.method.upper() == later.method.upper()` AND `earlier.path == later.path` AND `earlier.match and earlier.match.jq is not None` AND `later.match and later.match.jq is not None`, append one warning for the later stub and `break` (one warning per later stub). Pure structural — no `assertions` import (keeps `config/* → {errors}` layering intact).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_validator.py -q`
Expected: PASS (new + existing Check 1-3 / description-warning tests pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/validator.py tests/unit/test_validator.py`
Run: `git commit -m "feat(validator): mandatory method-gated jq-shadowing warning"` + trailer.

---

### Task 8: Add `validate_http_assertion_args` + `evaluate_http_assertions` to `assertions.py`

**Files:**
- Modify: `agctl/assertions.py` (add after `compile_jq` / `type_aware_equal`)
- Test: `tests/unit/test_assertions.py`

**Interfaces:**
- Consumes: `jq_bool`, `json_subset`, `jq_value`, `parse_equals`, `type_aware_equal` (all existing in this module); `AssertionFailure`, `ConfigError` (from `agctl.errors`); the http result dict shape `{status_code, response_time_ms, headers, body, url, method}`.
- Produces TWO functions (the split is load-bearing — Task 9 test (e) requires arg-misuse to fail BEFORE the request is sent, so the request side-effect is not triggered):
  - `validate_http_assertion_args(*, status: int|None, contains: str|None, match: str|None, jq_path: str|None, equals: str|None) -> None` — pre-request gate. Raises `ConfigError` (exit 2) on: (i) **pairing (D8)** — exactly one of `jq_path`/`equals` set → `ConfigError("--jq-path and --equals must be used together", {})`; (ii) `--contains` present but not valid JSON → `ConfigError("--contains must be valid JSON", {})`. No-op (returns `None`) when all args are None OR there is no pairing/JSON problem.
  - `evaluate_http_assertions(result: dict, *, status: int|None, contains: str|None, match: str|None, jq_path: str|None, equals: str|None) -> None` — post-request evaluation (assumes `validate_http_assertion_args` already ran on the same args). Behavior:
    1. If ALL of `status`, `contains`, `match`, `jq_path`, `equals` are `None` → return immediately.
    2. Evaluate each active mode; collect a failure entry per failing mode (NO short-circuit). Per-mode pass/fail and entry shape:
       - `status`: pass iff `result["status_code"] == status`; failure → `{"mode":"status","expected":status,"actual":result["status_code"]}`.
       - `contains`: pass iff `json_subset(json.loads(contains), result["body"])` (`contains` is pre-validated, the parse is safe); failure → `{"mode":"contains","needle":<parsed contains>,"matched":False}`.
       - `match`: pass iff `jq_bool(result["body"], match)` (ANY-truthy); failure → `{"mode":"match","expr":match,"result":False}`.
       - `jq_path`/`equals`: pass iff `type_aware_equal(jq_value(result["body"], jq_path), parse_equals(equals))`; failure → `{"mode":"jq-path","path":jq_path,"expected":parse_equals(equals),"actual":<jq_value result or None>}`.
    3. If `failures` non-empty → raise `AssertionFailure("HTTP response failed N assertion(s)", {"response": result, "failures": failures})`.
    4. Missing jq library (when `--match`/`--jq-path` used): `_jq()` raises `ConfigError`; **re-raise** a `ConfigError` whose message points at `pip install 'agctl[jq]'` (the base `_jq()` message names only db/kafka — MANDATORY rewrite, per spec D7).

- [ ] **Step 1: Write the failing test**

Tests in `test_assertions.py` with a fixture `result = {"status_code":201,"body":{"status":"PENDING","items":[{"amount":1500}]},"headers":{},"url":"u","method":"POST","response_time_ms":5}`.

`validate_http_assertion_args`: (v1) `--jq-path` only (no equals) → `ConfigError`; (v2) `--equals` only (no jq_path) → `ConfigError`; (v3) `--contains 'not json'` → `ConfigError("--contains must be valid JSON")`; (v4) all-None → returns None, no raise; (v5) valid `--contains '{"x":1}'` + paired `--jq-path`/`--equals` → returns None (valid args don't raise).

`evaluate_http_assertions`: (e1) all-None → returns None; (e2) `--status 201` → no raise; `--status 200` → `AssertionFailure` whose `detail["failures"]==[{"mode":"status","expected":200,"actual":201}]`; (e3) `--contains '{"status":"PENDING"}'` → no raise; `--contains '{"status":"PAID"}'` → failure `{"mode":"contains","needle":{"status":"PAID"},"matched":False}`; (e4) `--match '.status=="PENDING"'` → no raise; `--match '.status=="PAID"'` → `{"mode":"match","expr":'.status=="PAID"',"result":False}`; (e5) `--match '.items[].amount > 1000'` → no raise (ANY-truthy); `--match '.items[].amount > 9999'` → failure; (e6) `--jq-path '.status' --equals 'PENDING'` → no raise; `--jq-path '.status' --equals '"PAID"'` → `{"mode":"jq-path","path":".status","expected":"PAID","actual":"PENDING"}`; (e7) two failing modes → `failures` has TWO entries (no short-circuit) and `detail["response"] == result`; (e8) missing jq (`monkeypatch.setitem(sys.modules,"jq",None)`) with `--match '.x'` → `ConfigError` mentioning `agctl[jq]`; (e9) **non-JSON body:** `result["body"]="not-json"` with `--contains '{"x":1}'` → failure `{"mode":"contains",...,"matched":False}` (json_subset on a scalar haystack is False), and with `--jq-path '.status' --equals 'whatever'` → `actual: null` (jq_value on a string yields nothing).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: FAIL — neither function defined.

- [ ] **Step 3: Write minimal implementation**

Add both functions per the Produces contract. `validate_http_assertion_args` enforces only pairing + contains-JSON. `evaluate_http_assertions` evaluates active modes, builds `failures`, raises `AssertionFailure(detail={"response":result,"failures":failures})`. For `--match`/`--jq-path`, let `_jq()`'s `ConfigError` propagate and **re-raise** with the `agctl[jq]` hint (mandatory, not optional). No `coerce_db_value` (HTTP bodies are JSON-native — spec D3).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: PASS (new + existing primitive tests pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/assertions.py tests/unit/test_assertions.py`
Run: `git commit -m "feat(assertions): add validate_http_assertion_args + evaluate_http_assertions"` + trailer.

---

### Task 9: Wire assertion flags into `http call` / `http request`

**Files:**
- Modify: `agctl/commands/http_commands.py:80-148` (`_http_call_core` + `http_call`) and `:159-216` (`_http_request_core` + `http_request`)
- Test: `tests/unit/test_http_commands.py`

**Interfaces:**
- Consumes: `validate_http_assertion_args` + `evaluate_http_assertions` (Task 8); `@envelope`'s `AgctlError` branch (command.py:31-38) which preserves `err.detail`; the existing `_default_transport` / `set_default_transport` test seam (http_commands.py:42-48) for injecting an `httpx.MockTransport`.
- Produces: both commands accept five new optional Click options — `--status <int>`, `--contains <str>`, `--match <str>`, `--jq-path <str>`, `--equals <str>` (all default `None`). The `_http_call_core`/`_http_request_core` signatures gain matching parameters and the call sequence is: **(1)** `validate_http_assertion_args(status=…, contains=…, match=…, jq_path=…, equals=…)` **before** the request (raises `ConfigError` on pairing/bad-JSON misuse WITHOUT sending the request); **(2)** `result = client.request(...)`; **(3)** `evaluate_http_assertions(result, status=…, contains=…, match=…, jq_path=…, equals=…)`; **(4)** `return result`. A response-eval failure raises `AssertionFailure` → `@envelope` emits `ok:false`,`error.type:"AssertionError"`,`result:null`,`error.detail={"response":…,"failures":[…]}`, exit 1. Zero flags → both helpers no-op → unchanged behavior (`ok:true` even on 4xx/5xx).

- [ ] **Step 1: Write the failing test**

Tests in `test_http_commands.py`. IMPORTANT — the existing file uses a **static fixture** (`FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"`, ~line 13), NOT `tmp_path`; reuse that fixture (add a template/service to `tests/fixtures/agctl.yaml` only if the existing ones don't suffice). Drive commands with `click.testing.CliRunner` and assert on `result.exit_code` + parsed `result.output` JSON. Use `set_default_transport(httpx.MockTransport(…))` (the existing seam at test_http_commands.py:30-46) as the request spy. Cases: (a) zero assertion flags on a 200 → `ok:true`, exit 0, full result dict (regression guard); (b) `--status 201` on a 201 → ok:true, exit 0; (c) `--status 200` on a 201 → `ok:false`, `error.type=="AssertionError"`, `error.detail.failures==[{"mode":"status","expected":200,"actual":201}]`, `error.detail.response.status_code==201`, exit 1; (d) `--match '.status=="PENDING"'` on `{"status":"PENDING"}` → ok:true; on `{"status":"PAID"}` → ok:false, exit 1; (e) `--jq-path` without `--equals` → `ConfigError` exit 2 **and the mock transport was NOT called** (the pre-request `validate_http_assertion_args` raised before `client.request` — assert `len(captured requests)==0`); (f) `--match` with jq missing (`monkeypatch.setitem(sys.modules,"jq",None)`) → `ConfigError` exit 2.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_http_commands.py -q`
Expected: FAIL — commands reject the new options (`UsageError: no such option: --status`).

- [ ] **Step 3: Write minimal implementation**

Add five `@click.option(...)` declarations to `http_call` and `http_request` (`--status` `type=int`; rest `str`; all `default=None`). Thread the new params through the commands → envelopes → `_core` functions. In each `_core`: insert `validate_http_assertion_args(status=…, contains=…, match=…, jq_path=…, equals=…)` BEFORE `client.request(...)`; change `return client.request(...)` to `result = client.request(...)`; then `evaluate_http_assertions(result, status=…, contains=…, match=…, jq_path=…, equals=…)`; then `return result`. Add `from ..assertions import validate_http_assertion_args, evaluate_http_assertions`. Do NOT add the flags to `http ping` (deferred per spec §4).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_http_commands.py tests/unit/test_http_ping.py -q`
Expected: PASS (new + existing call/request/ping tests pass — ping unchanged).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/http_commands.py tests/unit/test_http_commands.py`
Run: `git commit -m "feat(http): add response assertion flags to http call/request"` + trailer.

---

### Task 10: Surface jq-compile errors in `config validate`

**Files:**
- Modify: `agctl/commands/config_commands.py:135-137` (the `config_validate` merge point)
- Test: `tests/unit/test_config_commands.py` (new file)

**Interfaces:**
- Consumes: `collect_jq_compile_errors` (Task 4); the existing `validate_config(cfg)` + `errors = errors + _plugin_validation_errors(...)` merge at config_commands.py:135-137.
- Produces: `config_validate` adds jq-compile errors to the `errors` list before the `if errors:` check: `errors = errors + collect_jq_compile_errors(cfg.mocks)`. A malformed `match.jq` (HTTP stub) or reactor `match` reports as a validation error `{path, message}` (exit 2). This is the `config validate` half of D5 (the `mock run` half is Task 5).

- [ ] **Step 1: Write the failing test**

Tests in `test_config_commands.py` using `click.testing.CliRunner` against the `validate` command with a temp `agctl.yaml` written via `tmp_path` (follow the temp-config pattern from `tests/unit/test_loader.py`): (a) a config with a stub whose `match.jq` is `')('` → exit 2 and `result.errors` contains an entry whose `path == "mocks.http.stubs.<name>.match.jq"`; (b) a malformed Kafka reactor `match` → exit 2, error `path == "mocks.kafka.reactors.<name>.match"`; (c) a fully-valid config → exit 0 (`valid:true`); (d) a config with no `mocks` section → exit 0 (collector returns `[]`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config_commands.py -q`
Expected: FAIL — jq-compile errors not surfaced.

- [ ] **Step 3: Write minimal implementation**

In `config_validate` (config_commands.py), after `errors, warnings = validate_config(cfg)` and the plugin merge (line 137), add `errors = errors + collect_jq_compile_errors(cfg.mocks)` before the `if errors:` check. Add `from ..mock.jq_precompile import collect_jq_compile_errors`. The existing emit/exit logic (lines 138-157) handles merged errors unchanged. Do not add jq logic to `validator.py` (layering).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_config_commands.py tests/unit/test_validator.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/config_commands.py tests/unit/test_config_commands.py`
Run: `git commit -m "feat(config): surface jq-compile errors in config validate"` + trailer.

---

### Task 11: Integration tests — (a) always-run in-process mock; (b) self-skipping live

**Files:**
- Modify: `tests/integration/test_mock_commands.py`, `tests/integration/test_http_commands.py`
- Test: these ARE the tests.

**Interfaces:**
- Consumes: all production code from Tasks 1-10; the existing `tests/integration/test_mock_commands.py::TestMockRunHTTP` pattern (subprocess `mock run --duration` + httpx — ALWAYS RUN, no Docker, no `require_*` fixture) for (a); the `require_http_service` fixture (`tests/integration/conftest.py`) for (b).
- Produces: (a) an always-run integration test of the `mock run` command + NDJSON stream for the branching-jq case; (b) a self-skipping test of `http call/request` assertion flags against a live HTTP service.

- [ ] **Step 1: Write the tests**

(a) In `test_mock_commands.py` (model on the existing `TestMockRunHTTP` class — subprocess `agctl mock run --duration N` + httpx, always-runs): start the mock with two same-method+path stubs distinguished by `match.jq` (high-value→201/APPROVED, low-value→202/QUEUED); POST high-value → assert 201; POST low-value → assert 202; parse the NDJSON stdout and assert the two `http.hit` events name the correct stubs. (b) In `test_http_commands.py`: gated by `require_http_service`, run `agctl http request … --status 201 --jq-path '.status' --equals '"APPROVED"'` against the live service → exit 0; same with `--equals '"QUEUED"'` → exit 1 (wrong-branch detection per spec §8.1/§14). Test (b) calls `pytest.skip()` when the service is absent.

- [ ] **Step 2: Run tests**

Run: `pytest tests/integration/test_mock_commands.py tests/integration/test_http_commands.py -q`
Expected: test (a) RUNS and PASSES (always-run, no Docker); test (b) reports `SKIPPED` (no live env).

- [ ] **Step 3: (Optional, if Docker available) run live**

Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_http_commands.py -q`
Expected: (b) PASS (if testcontainers available); otherwise skip is correct.

- [ ] **Step 4: Commit**

Run: `git add tests/integration/test_mock_commands.py tests/integration/test_http_commands.py`
Run: `git commit -m "test(integration): http-jq assertion + match.jq branching flow"` + trailer.

---

### Task 12: Sync docs (DESIGN.md, ARCHITECTURE.md, skills/, README) via `docs-watcher`

**Files:**
- Modify (via subagent): `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/agctl-config/**`, `README.md`
- Test: none (doc sync) — verify by inspection.

**Interfaces:**
- Consumes: the implemented code (Tasks 1-11) + spec §16.
- Produces: user-facing docs reflect the new surface, preserving each doc's altitude (DESIGN = WHAT/WHY; ARCHITECTURE = HOW; skills = operational reference).

- [ ] **Step 1: Dispatch the `docs-watcher` subagent**

Invoke the `docs-watcher` agent with the change summary: "Added HTTP response assertion flags (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`) to `http call`/`request`; added `match.jq` to mock HTTP stubs; added `compile_jq` startup pre-compile (covers HTTP stubs AND Kafka reactor `match`); added a `jq` extra. Spec: docs/superpowers/specs/active/2026-07-04-agctl-http-jq-design.md §16 lists the exact doc impacts." Let it propose edits; a correct no-op on a section is fine.

- [ ] **Step 2: Verify the expected edits landed**

Confirm by inspection: DESIGN.md §3.1 lists the assertion flags (`--jq-path`, not `--path`); §3.5/§4 note `match.jq` and that reactor `match` now pre-compiles; §10 lists deferred extraction/`--match-all`. ARCHITECTURE.md §8 notes the assertion flags; §9 references `validate_http_assertion_args` + `evaluate_http_assertions` + `compile_jq` and pre-compile covering both transports; §11 adds the `jq` extra. `skills/agctl-config/reference/mocks.md` documents `match.jq` (coexists with `body`; compile errors loud; eval errors soft; wrong-branch caveat + response-assertion mitigation). `skills/agctl-config` http-template reference documents the assertion flags + the `--match` ANY-semantic. `README.md` surfaces the flags + `pip install 'agctl[jq]'`.

- [ ] **Step 3: Commit the doc sync**

Run: `git add docs/ skills/ README.md`
Run: `git commit -m "docs: sync HTTP-jq assertions + mock match.jq (DESIGN/ARCH/skills/README)"` + trailer.

---

## Self-Review (run before handing off)

1. **Code scan:** No task contains method bodies, algorithms, or test code — only behavior descriptions, exact signatures, data shapes, and expected test results.
2. **Self-containment:** Each task's Produces block states the exact signature/shape later tasks consume. Task 8 now produces BOTH `validate_http_assertion_args` and `evaluate_http_assertions`; Task 9 consumes both with the explicit call sequence (validate → request → evaluate). No task says "see the spec" for a contract.
3. **Spec coverage:** §2 goals → Tasks 8+9 (assertions), 5+6 (mock match.jq), 3+5 (loud-on-typo). §3 constraints → Global Constraints. §5 D1-D10 → D1(Task 9), D2(Task 8 AND), D3(Task 8 no coerce), D4(Task 2 coexist), D5(Task 3+5+10), D6(Task 5), D7(Task 1+3+8 hint), D8(Task 8 validate pairing), D9(Task 8 detail shape), D10(Task 8 any-semantic). §6 → Task 9. §7 → Task 2. §8.1 → Task 6. §8.2 → Tasks 3+5. §8.3 → Task 8. §9 → encoded across Tasks. §10 → Tasks 5+10 (pre-compile) + Task 7 (shadowing). §11 → no structural change. §12 → Task 1. §13 → Tasks' test files match. §14 → deferred items respected. §16 → Task 12.
4. **Placeholder scan:** No TBD/TODO/"add error handling"/"similar to" — every test step names scenario + expected result; every implementation step names behavior.
5. **Type consistency:** `compile_jq(expr, *, label=None)` in Tasks 3/4/5/10. `validate_http_assertion_args(*, status, contains, match, jq_path, equals)` + `evaluate_http_assertions(result, *, status, contains, match, jq_path, equals)` in Tasks 8/9. `HttpMatch.jq: str | None` in Tasks 2/4/6/7. `iter_mock_jq_expressions` / `collect_jq_compile_errors` in Tasks 4/5/10. Per-mode failure-entry field names match between Task 8's Produces and the Global Constraints table. The validate/evaluate split resolves the test-(e) ordering blocker (pairing fires pre-request).
