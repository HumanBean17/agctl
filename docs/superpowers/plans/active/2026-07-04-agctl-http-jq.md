# HTTP jq (Assertions + Mock Stub Matching) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add HTTP response assertion flags to `http call`/`http request` and a `match.jq` predicate to mock HTTP stubs, reusing the existing jq/subset engine, with a compile-only `compile_jq` startup guard covering both HTTP stubs and the existing Kafka reactor `match`.

**Architecture:** Two additive features over the existing `agctl/assertions.py` engine. Feature (2) adds a new `evaluate_http_assertions(result, …)` helper invoked from the `http call`/`request` `_core` functions (already wrapped by `@envelope`); a failing assertion raises `AssertionFailure` → exit 1 with `error.detail = {response, failures:[…]}`. Feature (1) adds an optional `jq: str | None` to `HttpMatch`, evaluated in `mock/http_server.py::_handle_request` right after the existing `json_subset(match.body, …)` check. A new `compile_jq(expr)` helper (compile-only, no `.input().all()`) is pre-run at `MockEngine.start()` and in `config validate` over every `match.jq` and every Kafka reactor `match`, turning authoring typos into a loud `ConfigError` (exit 2) instead of a silent mis-match / inert reactor.

**Tech Stack:** Python ≥3.11, Pydantic v2, Click 8, stdlib `http.server`, PyPI `jq` (Cython binding over C libjq) — already bundled in the `kafka`/`db` extras; this plan adds a dedicated `jq` extra.

**Spec:** `docs/superpowers/specs/active/2026-07-04-agctl-http-jq-design.md` (commit f99da37) — the source of truth. Read it fully before starting.

## Global Constraints

Copy verbatim into every task's mental model:

- **No `version` bump** (additive under major `"1"`); no new entry-point group; no new command group. Both features wire into existing commands/config.
- **Lazy-import convention:** never `import jq` at module top-level — always via `_jq()` in `agctl/assertions.py`. A missing library surfaces as `ConfigError` (exit 2), never `ModuleNotFoundError`. This preserves the HTTP-only zero-dep mock (a stub without `match.jq` never imports jq).
- **Naming:** the assertion flag is `--jq-path` (NOT `--path` — `--path` collides with the URL `--path` on `http request`/`http ping`; Click cannot register both). `--jq-path` pairs with `--equals`.
- **Assertion-failure envelope (pinned):** on failure, raise `AssertionFailure(message, detail)` where `detail = {"response": <full http result dict>, "failures": [<per-mode entries>]}`. `@envelope` then emits `ok:false`, `error.type:"AssertionError"`, `result:null`, exit 1. The full response always rides in `error.detail.response`.
- **Per-mode failure entries (pinned, agents parse this):**
  - `status` → `{"mode":"status", "expected":<int>, "actual":<status_code int>}`
  - `contains` → `{"mode":"contains", "needle":<parsed --contains JSON>, "matched":false}`
  - `match` → `{"mode":"match", "expr":<--match string>, "result":false}`
  - `jq-path` → `{"mode":"jq-path", "path":<--jq-path expr>, "expected":<parse_equals(--equals)>, "actual":<jq_value result or null>}`
- **`--match` ANY-semantic:** `jq_bool` is true on ANY truthy output. The "all items" idiom is the **semicolon** form `all(.items[]; .amount > 100)` — the comma form `all(.items[].amount > 100)` is invalid jq; never use it.
- **D5 pre-compile covers both transports:** every HTTP `match.jq` AND every Kafka reactor `match` is compiled at `MockEngine.start()` and in `config validate`. This is a **deliberate loudness change** for malformed reactor `match` configs (previously started silently inert; now exit 2) — the spec's §3 "byte-for-byte unchanged" claim has a documented carve-out for this case.
- **Layering (ARCHITECTURE §3):** `config/*` modules must NOT import `agctl.assertions` (config depends on errors only). The jq pre-compile inside `config validate` therefore lives in the **command layer** (`agctl/commands/config_commands.py`), not in `agctl/config/validator.py`. The shared walker lives in a new `agctl/mock/jq_precompile.py` (may import `config.models` + `assertions.compile_jq`).
- **Tests:** run `pytest tests/unit -q` after every task. Integration tests self-skip unless `AGCTL_TEST_LIVE=1`.
- **Commits:** one conventional-commit per task (`feat:`, `test:`, `refactor:`, `docs:` as appropriate). End commit messages with `Co-Authored-By: Claude <noreply@anthropic.com>`.

---

## File Structure

**New files:**
- `agctl/mock/jq_precompile.py` — shared walker + validate-collection helper (iterates `MocksConfig`, calls `compile_jq`). Depends on `config.models` + `assertions.compile_jq`.
- `tests/unit/test_jq_precompile.py` — unit tests for the walker.
- `tests/unit/test_config_commands.py` — CliRunner test for jq-compile errors surfaced by `config validate`.

**Modified files:**
- `pyproject.toml` — add `jq = ["jq>=1.6"]` extra.
- `agctl/config/models.py` — `HttpMatch` gains `jq: str | None = None`.
- `agctl/assertions.py` — add `compile_jq(expr, *, label=None)` and `evaluate_http_assertions(result, *, status, contains, match, jq_path, equals)`.
- `agctl/mock/http_server.py` — `_handle_request` evaluates `match.jq` after the `match.body` check; import `jq_bool`.
- `agctl/mock/engine.py` — `start()` runs the pre-compile as Step 0 (before the Kafka probe).
- `agctl/config/validator.py` — add Check 4 (jq-shadowing warning, method-gated).
- `agctl/commands/http_commands.py` — `http_call`/`http_request` gain `--status`/`--contains`/`--match`/`--jq-path`/`--equals`; the `_core` functions call `evaluate_http_assertions`.
- `agctl/commands/config_commands.py` — `config_validate` merges jq-compile errors.

**Extended test files:** `tests/unit/test_assertions.py`, `tests/unit/test_mock_models.py`, `tests/unit/test_mock_engine.py`, `tests/unit/test_mock_http_server.py`, `tests/unit/test_validator.py`, `tests/unit/test_http_commands.py`, `tests/integration/test_mock_commands.py`, `tests/integration/test_http_commands.py`.

---

### Task 1: Add the dedicated `jq` extra to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml:33-42` (`[project.optional-dependencies]`)
- Test: `tests/unit/test_loader.py` (or a new `tests/unit/test_packaging.py`)

**Interfaces:**
- Produces: a `jq` optional-dependency group whose value is exactly `["jq>=1.6"]`, so `pip install 'agctl[jq]'` installs the jq library. Existing `kafka`/`db`/`http`/`dev`/`integration` groups are unchanged.

- [ ] **Step 1: Write the failing test**

A test that loads `pyproject.toml` with `tomllib`, reads `[project.optional-dependencies]`, and asserts a `jq` key exists with value `["jq>=1.6"]`, and that `kafka`/`db`/`http` are unchanged (still contain their current dependencies). Expected: the `jq` key is present and equal to `["jq>=1.6"]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_loader.py -q` (or the new packaging test)
Expected: FAIL — `jq` key absent in optional-dependencies.

- [ ] **Step 3: Write minimal implementation**

Add `jq = ["jq>=1.6"]` as a new line under `[project.optional-dependencies]`, alphabetically/logically grouped (e.g., after `http` and before `kafka`, or after `db`). Do not modify any other extra.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_loader.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add pyproject.toml tests/unit/test_loader.py`
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
Expected: FAIL — `HttpMatch` has no `jq` attribute (`TypeError`/`ValidationError`).

- [ ] **Step 3: Write minimal implementation**

Add `jq: str | None = None` to `HttpMatch` (after the existing `body` field). No validator needed — it is a free-form jq predicate string; syntax is checked later by `compile_jq` (Task 3/5). Update the class docstring to note both fields and that `jq` is a jq predicate coexisting with `body`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_models.py -q`
Expected: PASS. Also run `pytest tests/unit/test_mock_models.py tests/unit/test_loader.py -q` to confirm no existing model/load test regressed.

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
- Produces: `compile_jq(expr: str, *, label: str | None = None) -> None` — compiles `expr` via `_jq().compile(expr)` **without** `.input(value).all()` (compile-only). On success returns `None`. On a missing jq library, re-raises `_jq()`'s `ConfigError` with a message pointing at `pip install 'agctl[jq]'` (HTTP/mock context) and including `label`. On any compile-time exception (e.g. `ValueError` from a malformed expression), raises `ConfigError` whose message includes `label`, the expression, and the underlying error — so the surfaced type is `ConfigError` (exit 2), NOT `InternalError` from the envelope catch-all. **Distinct from `jq_bool`**, which wraps compile+eval in `except Exception: return False` (correct for runtime matching, wrong for the startup guard).

- [ ] **Step 1: Write the failing test**

Tests (all in `test_assertions.py`): (a) `compile_jq('.a == 1')` returns `None` (valid expr, no raise); (b) `compile_jq(')(')` raises `ConfigError` (malformed); (c) `compile_jq('.amount >')` raises `ConfigError` (truncated expression — the case `jq_bool` would silently swallow); (d) the raised `ConfigError.message` includes the `label` when one is passed (e.g. `label="mocks.http.stubs.x.match.jq"`); (e) **contrast:** `jq_bool({}, ')(')` still returns `False` (proving the two helpers differ on the same input); (f) missing-jq path — `monkeypatch.setitem(sys.modules, "jq", None)`, then `compile_jq('.a')` raises `ConfigError` whose message mentions `agctl[jq]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: FAIL — `compile_jq` not defined (`ImportError`/`AttributeError`).

- [ ] **Step 3: Write minimal implementation**

Add `compile_jq(expr, *, label=None) -> None` per the Produces contract: call `_jq().compile(expr)` inside a try; on `ConfigError` re-raise as-is (or re-raise with the `agctl[jq]` hint if the base message lacks it); on any other `Exception` raise `ConfigError` with a message combining label, expr, and the underlying error. The function must NOT call `.input().all()` (that would require a value and would re-introduce runtime semantics). Add a docstring stating it is compile-only and distinct from `jq_bool`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: PASS (all new cases + existing jq_bool/jq_value cases still pass — no regression).

- [ ] **Step 5: Commit**

Run: `git add agctl/assertions.py tests/unit/test_assertions.py`
Run: `git commit -m "feat(assertions): add compile_jq compile-only helper (loud on authoring typos)"` + trailer.

---

### Task 4: Create `agctl/mock/jq_precompile.py` (walker + validate collector)

**Files:**
- Create: `agctl/mock/jq_precompile.py`
- Test: `tests/unit/test_jq_precompile.py`

**Interfaces:**
- Consumes: `MocksConfig` (from `agctl.config.models`), `compile_jq` (Task 3), `ConfigError` (from `agctl.errors`).
- Produces:
  - `iter_mock_jq_expressions(mocks: MocksConfig | None) -> Iterator[tuple[str, str]]` — yields `(path_label, expr)` for every HTTP stub with a non-None `match.jq` (label `f"mocks.http.stubs.{name}.match.jq"`) and every Kafka reactor with a non-None `match` (label `f"mocks.kafka.reactors.{name}.match"`), in stable order (stubs first in dict order, then reactors in dict order). `mocks is None` → yields nothing.
  - `collect_jq_compile_errors(mocks: MocksConfig | None) -> list[dict]` — iterates the above, calls `compile_jq(expr, label=label)` inside try/except; on `ConfigError` appends `{"path": label, "message": err.message}` to the result; continues (collects ALL errors, does not raise). Returns the list (empty if all valid / mocks None).

- [ ] **Step 1: Write the failing test**

Tests in `test_jq_precompile.py`: (a) `iter_mock_jq_expressions(None)` yields nothing; (b) a `MocksConfig` with one HTTP stub whose `match.jq='.a>1'` and one Kafka reactor whose `match='.b==2'` → `list(iter_mock_jq_expressions(mocks))` returns exactly two `(label, expr)` pairs with the correct labels and exprs; (c) a stub with `match=HttpMatch(body={"x":1})` (jq None) is skipped; a reactor with `match=None` is skipped; (d) `collect_jq_compile_errors` on a config with a malformed stub `match.jq')('` returns a one-element list `[{path, message}]` whose `path == "mocks.http.stubs.<name>.match.jq"`; (e) `collect_jq_compile_errors` on a fully-valid config returns `[]`; (f) `collect_jq_compile_errors` collects TWO errors when both a stub and a reactor are malformed (does not stop at the first).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_jq_precompile.py -q`
Expected: FAIL — module does not exist (`ImportError`).

- [ ] **Step 3: Write minimal implementation**

Create `agctl/mock/jq_precompile.py` with the two functions per the Produces contract. `iter_mock_jq_expressions` guards `mocks is None` (yield nothing), then iterates `mocks.http.stubs.items()` if `mocks.http` is not None, yielding when `stub.match and stub.match.jq is not None`; then `mocks.kafka.reactors.items()` if `mocks.kafka` is not None, yielding when `reactor.match is not None`. `collect_jq_compile_errors` loops the walker, calls `compile_jq(expr, label=label)`, catches `ConfigError`, appends `{path, message}`. Keep the module dependency-direction clean: import only `config.models` + `assertions.compile_jq` + `errors`.

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
- Produces: `start()` runs a new "Step 0" before the Kafka probe (Step 1): it calls `compile_jq(expr, label=label)` for every `(label, expr)` from `iter_mock_jq_expressions(self._mocks)`. A failure raises `ConfigError` which the existing outer try catches → `shutdown()` → re-raise → `mock_run` emits the startup envelope (exit 2) before any event line. A mock with no jq expressions executes Step 0 as a no-op (and imports nothing — zero-dep preserved for HTTP-only). This realizes D5 (loud-on-typo) + D6 (jq imported at startup, not first request) for the mock.

- [ ] **Step 1: Write the failing test**

Tests in `test_mock_engine.py` constructing a `MockEngine` with: (a) an HTTP stub whose `match.jq` is malformed (e.g. `')('`) → `engine.start()` raises `ConfigError` (and does NOT emit `started`); (b) a Kafka reactor whose `match` is malformed → raises `ConfigError`; (c) an HTTP stub with a valid `match.jq` but jq is "missing" (`monkeypatch.setitem(sys.modules, "jq", None)`) → raises `ConfigError` (missing extra surfaces at startup, not first request); (d) a config with only body-only stubs (no `match.jq`, no reactor `match`) → Step 0 is a no-op and `start()` proceeds normally (does not import jq — assert no `ConfigError` raised for missing jq in this case). Use `run_http=True`/`run_kafka=False` with `http_listen="127.0.0.1:0"` to avoid real binds where possible; for (d) assert the engine reaches the started-line path without a jq-related error.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_engine.py -q`
Expected: FAIL — start() does not pre-compile (malformed expr does not raise).

- [ ] **Step 3: Write minimal implementation**

In `MockEngine.start()`, inside the existing `try:` block, **before** the `if self._run_kafka:` probe (engine.py:169), add Step 0: `for label, expr in iter_mock_jq_expressions(self._mocks): compile_jq(expr, label=label)`. Add the imports at module top (`from ..assertions import compile_jq` and `from .jq_precompile import iter_mock_jq_expressions`). The existing `except Exception: self.shutdown(); raise` (engine.py:242-245) handles the `ConfigError` → re-raise; `mock_run`'s `except AgctlError` (mock_commands.py:176) emits the envelope. No change to shutdown or the started-line logic.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_engine.py tests/unit/test_mock_commands.py -q`
Expected: PASS (new cases + existing startup/probe/bind tests still pass).

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
- Produces: inside the per-stub match loop, immediately after the existing `match.body` check (http_server.py:231-233), if `stub.match and stub.match.jq is not None`, evaluate `jq_bool(parsed_body, stub.match.jq)`; if `False`, `continue` to the next stub (non-match). A predicate that raises is already swallowed to `False` by `jq_bool` (soft non-match — falls through). A stub with both `body` and `jq` requires both to pass (AND). First stub whose full predicate passes still wins (insertion order).

- [ ] **Step 1: Write the failing test**

Tests in `test_mock_http_server.py` using the existing `start_server` + httpx pattern: (a) one stub with `match.jq='.amount > 1000'` and a 201 response — POST a body with `amount: 1500` → 201 + `http.hit` event; (b) same stub, POST `amount: 500` → 404 + `http.unmatched` event (predicate false → fall through); (c) two stubs same method+path distinguished by `jq` (high-value → 201/APPROVED; low-value → 202/QUEUED) — POST `amount: 1500` hits the first, POST `amount: 500` hits the second (branch routing); (d) coexist: a stub with `match.body={"priority":"high"}` AND `match.jq='.amount>1000'` — a request matching only one fails to match (404), a request matching both hits; (e) predicate-raises-soft: a stub with `match.jq='.a.b.c'` (navigates into a missing nested field on a body without `.a`) → 404 + `http.unmatched` (no 500, no crash).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_mock_http_server.py -q`
Expected: FAIL — `match.jq` is ignored (the high-value stub matches regardless of amount, etc.).

- [ ] **Step 3: Write minimal implementation**

Add `jq_bool` to the existing `from agctl.assertions import …` import (http_server.py:13). In `_handle_request`, after the `if stub.match and stub.match.body is not None:` block (http_server.py:231-233), add: `if stub.match and stub.match.jq is not None: if not jq_bool(parsed_body, stub.match.jq): continue`. Do not change capture, reaction, or event emission. `parsed_body` may be `None` (non-JSON body) — `jq_bool(None, expr)` returns `False` or swallows → non-match (correct).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_mock_http_server.py -q`
Expected: PASS (new cases + existing body-match/capture/template tests still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/mock/http_server.py tests/unit/test_mock_http_server.py`
Run: `git commit -m "feat(mock): evaluate match.jq predicate in HTTP stub matching"` + trailer.

---

### Task 7: Mandatory jq-shadowing warning in `validator.py` (method-gated)

**Files:**
- Modify: `agctl/config/validator.py:135-153` (add Check 4 after Check 3)
- Test: `tests/unit/test_validator.py`

**Interfaces:**
- Consumes: `cfg.mocks.http.stubs` (Task 2's `HttpMatch.jq`), the existing `_missing_description` helper style.
- Produces: a new validation warning when two stubs share the **same method (case-insensitive) AND the same path template** and are distinguished only by `jq` (i.e. both have a non-None `match.jq`, or one has `jq` and the other has neither `jq` nor `body`). The check MUST gate on method equality — the existing path-template-shadowing Check 3 (validator.py:135-151) compares path segments only and is method-agnostic (a known limitation); do NOT copy that pattern blindly, else `GET /api/{id}` and `DELETE /api/users` would false-warn. Warning shape: `{"path": f"mocks.http.stubs.{later_name}", "message": f"Stub '{later_name}' is shadowed by '{earlier_name}' — same method+path distinguished only by jq (first match wins; a wrong predicate can fire the wrong branch silently)."}`.

- [ ] **Step 1: Write the failing test**

Tests in `test_validator.py` building a `Config` via the existing pattern and calling `validate_config(cfg)`: (a) two POST stubs same path both with `match.jq` → exactly one warning referencing the later stub; (b) two stubs same path but DIFFERENT methods (POST vs DELETE) both with `match.jq` → NO warning (method gate); (c) two stubs same method+path, one with `match.jq` and one with `match.body` only → warning (distinguished only by jq-vs-body is still fragile first-match); (d) two stubs different paths → no warning; (e) a single stub → no warning.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_validator.py -q`
Expected: FAIL — no jq-shadowing warning emitted.

- [ ] **Step 3: Write minimal implementation**

Add "Check 4: jq-shadowing" after Check 3 (validator.py:135-151), within the `if cfg.mocks is not None and cfg.mocks.http is not None:` block. Iterate pairs `(earlier, later)` in insertion order; for each pair where `earlier.method.upper() == later.method.upper()` AND `earlier.path == later.path` AND both stubs' matchers are "jq-only-or-absent" (i.e. neither has `match.body`), append one warning for the later stub and `break` (one warning per later stub). Pure structural — no `assertions` import (keeps `config/* → {errors}` layering intact).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_validator.py -q`
Expected: PASS (new cases + existing Check 1-3 / description-warning tests still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/validator.py tests/unit/test_validator.py`
Run: `git commit -m "feat(validator): mandatory method-gated jq-shadowing warning"` + trailer.

---

### Task 8: Add `evaluate_http_assertions(...)` to `assertions.py`

**Files:**
- Modify: `agctl/assertions.py` (add after `compile_jq` / `type_aware_equal`)
- Test: `tests/unit/test_assertions.py`

**Interfaces:**
- Consumes: `jq_bool`, `json_subset`, `jq_value`, `parse_equals`, `type_aware_equal` (all existing in this module); `AssertionFailure`, `ConfigError` (from `agctl.errors`); the http result dict shape `{status_code, response_time_ms, headers, body, url, method}`.
- Produces: `evaluate_http_assertions(result: dict, *, status: int | None, contains: str | None, match: str | None, jq_path: str | None, equals: str | None) -> None`. Behavior:
  1. If ALL of `status`, `contains`, `match`, `jq_path`, `equals` are `None` → return immediately (no-op; preserves zero-flag behavior).
  2. **Pairing (D8):** if exactly one of `jq_path`/`equals` is set → raise `ConfigError("--jq-path and --equals must be used together", {})`.
  3. Parse `contains` (a raw JSON string) via `json.loads`; on `JSONDecodeError` raise `ConfigError("--contains must be valid JSON", {})`.
  4. Evaluate each active mode; collect a failure entry per failing mode (NO short-circuit — evaluate all active modes). Per-mode pass/fail and entry shape:
     - `status`: pass iff `result["status_code"] == status`; failure → `{"mode":"status","expected":status,"actual":result["status_code"]}`.
     - `contains`: pass iff `json_subset(contains_parsed, result["body"])`; failure → `{"mode":"contains","needle":contains_parsed,"matched":False}`.
     - `match`: pass iff `jq_bool(result["body"], match)`; failure → `{"mode":"match","expr":match,"result":False}`.
     - `jq_path`/`equals`: pass iff `type_aware_equal(jq_value(result["body"], jq_path), parse_equals(equals))`; failure → `{"mode":"jq-path","path":jq_path,"expected":parse_equals(equals),"actual":<jq_value result or None>}`.
  5. If `failures` is non-empty → raise `AssertionFailure("HTTP response failed N assertion(s)", {"response": result, "failures": failures})`.
  6. Missing jq library (when `--match`/`--jq-path` used): `_jq()` raises `ConfigError`; the helper re-raises it (the http-client context message points at `agctl[jq]`).

- [ ] **Step 1: Write the failing test**

Tests in `test_assertions.py` with a fixture `result = {"status_code":201,"body":{"status":"PENDING","items":[{"amount":1500}]},"headers":{},"url":"u","method":"POST","response_time_ms":5}`: (a) all-None → returns `None`, no raise; (b) `--status 201` → no raise; `--status 200` → raises `AssertionFailure` whose `detail["failures"]` has one entry `{"mode":"status","expected":200,"actual":201}`; (c) `--contains '{"status":"PENDING"}'` → no raise; `--contains '{"status":"PAID"}'` → one failure entry `{"mode":"contains","needle":{"status":"PAID"},"matched":False}`; (d) `--match '.status=="PENDING"'` → no raise; `--match '.status=="PAID"'` → failure `{"mode":"match","expr":'.status=="PAID"',"result":False}`; (e) `--match '.items[].amount > 1000'` → no raise (ANY-truthy: one item qualifies); `--match '.items[].amount > 9999'` → failure (no item qualifies); (f) `--jq-path '.status' --equals 'PENDING'` → no raise; `--equals` only (no jq_path) → `ConfigError` (pairing); `--jq-path` only → `ConfigError`; (g) `--jq-path '.status' --equals '"PAID"'` → failure entry `{"mode":"jq-path","path":".status","expected":"PAID","actual":"PENDING"}`; (h) two failing modes at once → `failures` has TWO entries (no short-circuit) and `detail["response"] == result` (full response preserved); (i) missing jq (`monkeypatch.setitem(sys.modules,"jq",None)`) with `--match '.x'` → `ConfigError` mentioning `agctl[jq]`; (j) `--contains 'not json'` → `ConfigError("--contains must be valid JSON")`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: FAIL — `evaluate_http_assertions` not defined.

- [ ] **Step 3: Write minimal implementation**

Add `evaluate_http_assertions(result, *, status, contains, match, jq_path, equals) -> None` per the Produces contract. Use the existing primitives directly; build the `failures` list; raise `AssertionFailure` with `detail={"response":result,"failures":failures}` when non-empty. For `--match`/`--jq-path`, let `_jq()`'s `ConfigError` propagate (optionally re-raising with the `agctl[jq]` hint). No coercion of `result["body"]` (HTTP bodies are JSON-native — `coerce_db_value` is NOT used, per spec D3).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_assertions.py -q`
Expected: PASS (all new cases + existing primitive tests pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/assertions.py tests/unit/test_assertions.py`
Run: `git commit -m "feat(assertions): add evaluate_http_assertions (per-mode failures + full response)"` + trailer.

---

### Task 9: Wire assertion flags into `http call` / `http request`

**Files:**
- Modify: `agctl/commands/http_commands.py:80-148` (`_http_call_core` + `http_call`) and `:159-216` (`_http_request_core` + `http_request`)
- Test: `tests/unit/test_http_commands.py`

**Interfaces:**
- Consumes: `evaluate_http_assertions` (Task 8); `@envelope`'s `AgctlError` branch (command.py:31-38) which preserves `err.detail` via `to_dict()`; the existing `_default_transport` test seam (injectable `httpx.MockTransport`).
- Produces: both commands accept five new optional Click options — `--status <int>`, `--contains <str>`, `--match <str>`, `--jq-path <str>`, `--equals <str>` (all default `None`). The `_http_call_core`/`_http_request_core` signatures gain matching parameters; after obtaining the `result` dict from `client.request(...)`, each calls `evaluate_http_assertions(result, status=…, contains=…, match=…, jq_path=…, equals=…)` and then returns `result`. On failure the helper raises `AssertionFailure` → `@envelope` emits `ok:false`,`error.type:"AssertionError"`,`result:null`,`error.detail={"response":…,"failures":[…]}`, exit 1. Zero flags → helper no-ops → unchanged behavior (`ok:true` even on 4xx/5xx).

- [ ] **Step 1: Write the failing test**

Tests in `test_http_commands.py` using the existing `set_default_transport(httpx.MockTransport(…))` seam (inspect existing tests for the exact builder pattern): (a) zero assertion flags on a 200 response → `ok:true`, exit 0, `result` is the full http dict (regression guard); (b) `--status 201` on a 201 → `ok:true`, exit 0; (c) `--status 200` on a 201 → `ok:false`, `error.type=="AssertionError"`, `error.detail.failures==[{"mode":"status","expected":200,"actual":201}]`, `error.detail.response.status_code==201`, exit 1; (d) `--match '.status=="PENDING"'` on a body `{"status":"PENDING"}` → ok:true; on `{"status":"PAID"}` → ok:false, exit 1; (e) `--jq-path` without `--equals` → `ConfigError` (exit 2) BEFORE the request is sent (assert the mock transport was NOT called); (f) `--match` with jq missing (`monkeypatch`) → `ConfigError` exit 2. Use `click.testing.CliRunner` to drive the commands and read `result.exit_code` + parse `result.output` JSON. Mirror the existing `http call`/`http request` test construction (template + service in a temp `agctl.yaml` via the existing conftest pattern).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_http_commands.py -q`
Expected: FAIL — commands reject the new options (`UsageError: no such option: --status`).

- [ ] **Step 3: Write minimal implementation**

Add five `@click.option(...)` declarations to `http_call` and `http_request` (types: `--status` `type=int`; the rest `str`, all `default=None`). Thread the new params through `http_call`/`http_request` → `_http_call_envelope(...)` / `_http_request_envelope(...)`; add matching parameters to `_http_call_core`/`_http_request_core`. In each `_core`, capture `result = client.request(...)` (already the last statement — change `return client.request(...)` to assign then return after the assertion call). Call `evaluate_http_assertions(result, status=status, contains=contains, match=match, jq_path=jq_path, equals=equals)` before `return result`. Add `from ..assertions import evaluate_http_assertions`. Do NOT add the flags to `http ping` (deferred per spec §4).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_http_commands.py tests/unit/test_http_ping.py -q`
Expected: PASS (new cases + existing call/request/ping tests pass — ping unchanged).

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
- Produces: `config_validate` adds jq-compile errors to the `errors` list before the `if errors:` check, i.e. `errors = errors + collect_jq_compile_errors(cfg.mocks)`. A malformed `match.jq` (HTTP stub) or reactor `match` now reports as a validation error `{path, message}` (exit 2), alongside structural cross-refs and plugin errors. This is the `config validate` half of D5 (the `mock run` half is Task 5).

- [ ] **Step 1: Write the failing test**

Tests in `test_config_commands.py` using `click.testing.CliRunner` against the `config` group's `validate` command with a temp `agctl.yaml` (follow the existing temp-config pattern from other test files): (a) a config with a stub whose `match.jq` is `')('` → `config validate` exits 2 and the JSON `result.errors` contains an entry whose `path == "mocks.http.stubs.<name>.match.jq"`; (b) a config with a malformed Kafka reactor `match` → exits 2 with an error `path == "mocks.kafka.reactors.<name>.match"`; (c) a fully-valid config (well-formed `match.jq` and reactor `match`) → exits 0 (`valid:true`), no jq errors; (d) a config with no `mocks` section → exits 0, no jq errors (collector returns `[]`).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config_commands.py -q`
Expected: FAIL — jq-compile errors not surfaced (malformed expr still validates clean, or the file/tests don't exercise the merge).

- [ ] **Step 3: Write minimal implementation**

In `config_validate` (config_commands.py, after `errors, warnings = validate_config(cfg)` and the plugin merge at line 137), add `errors = errors + collect_jq_compile_errors(cfg.mocks)` before the `if errors:` check. Add `from ..mock.jq_precompile import collect_jq_compile_errors`. The existing emit/exit logic (lines 138-157) handles the merged errors unchanged. Do not add jq logic to `validator.py` (layering — see Global Constraints).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_config_commands.py tests/unit/test_validator.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/config_commands.py tests/unit/test_config_commands.py`
Run: `git commit -m "feat(config): surface jq-compile errors in config validate"` + trailer.

---

### Task 11: Integration tests (self-skipping) for the combined flow

**Files:**
- Modify: `tests/integration/test_mock_commands.py`, `tests/integration/test_http_commands.py`
- Test: these ARE the tests.

**Interfaces:**
- Consumes: the `AGCTL_TEST_LIVE=1` testcontainers harness + local threaded HTTP mock (ARCHITECTURE §12); all production code from Tasks 1-10.
- Produces: two self-skipping integration tests that exercise the full stack end-to-end when a live environment is available, and `pytest.skip()` otherwise (matching the existing integration-test convention in `tests/integration/conftest.py`).

- [ ] **Step 1: Write the tests**

(a) In `test_mock_commands.py`: start the mock with two same-method+path stubs distinguished by `match.jq` (high-value→201/APPROVED, low-value→202/QUEUED); POST a high-value body → assert the mock served 201; POST a low-value body → assert 202; assert the NDJSON stream shows the correct `http.hit` events. (b) In `test_http_commands.py`: against the running threaded mock, run `agctl http request --service … --method POST --path … --body '{"amount":1500}' --status 201 --jq-path '.status' --equals '"APPROVED"'` → exit 0; then the same with `--equals '"QUEUED"'` → exit 1 (wrong-branch detection via the response assertion, per spec §8.1/§14 mitigation). Both tests skip when `AGCTL_TEST_LIVE` is unset.

- [ ] **Step 2: Run tests to verify they skip cleanly**

Run: `pytest tests/integration/test_mock_commands.py tests/integration/test_http_commands.py -q`
Expected: each new test reports `SKIPPED` (no live env), not FAILED or ERRORED.

- [ ] **Step 3: (Optional, if Docker available) run live**

Run: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_mock_commands.py tests/integration/test_http_commands.py -q`
Expected: PASS (if Docker/testcontainers available); otherwise skip is the correct outcome.

- [ ] **Step 4: Commit**

Run: `git add tests/integration/test_mock_commands.py tests/integration/test_http_commands.py`
Run: `git commit -m "test(integration): http-jq assertion + match.jq branching flow (self-skipping)"` + trailer.

---

### Task 12: Sync docs (DESIGN.md, ARCHITECTURE.md, skills/, README) via `docs-watcher`

**Files:**
- Modify (via subagent): `docs/DESIGN.md`, `docs/ARCHITECTURE.md`, `skills/agctl-config/**`, `README.md`
- Test: none (doc sync) — verify by inspection.

**Interfaces:**
- Consumes: the implemented code (Tasks 1-11) + spec §16 (the source-of-truth doc-impact list).
- Produces: user-facing docs reflect the new surface, preserving each doc's altitude (DESIGN = WHAT/WHY; ARCHITECTURE = HOW; skills = operational reference).

- [ ] **Step 1: Dispatch the `docs-watcher` subagent**

Invoke the `docs-watcher` agent with the change summary: "Added HTTP response assertion flags (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`) to `http call`/`request`; added `match.jq` to mock HTTP stubs; added `compile_jq` startup pre-compile (covers HTTP stubs AND Kafka reactor `match`); added a `jq` extra. Spec: docs/superpowers/specs/active/2026-07-04-agctl-http-jq-design.md §16 lists the exact doc impacts." Let it propose edits; a correct no-op on a section is fine.

- [ ] **Step 2: Verify the expected edits landed**

Confirm by inspection: DESIGN.md §3.1 lists the assertion flags (`--jq-path`, not `--path`); §3.5/§4 note `match.jq` and that reactor `match` now pre-compiles; §10 lists deferred extraction/`--match-all`. ARCHITECTURE.md §8 notes the assertion flags; §9 references `evaluate_http_assertions` + `compile_jq` and pre-compile covering both transports; §11 adds the `jq` extra. `skills/agctl-config/reference/mocks.md` documents `match.jq` (coexists with `body`; compile errors loud; eval errors soft; wrong-branch caveat + response-assertion mitigation). `skills/agctl-config` http-template reference documents the assertion flags + the `--match` ANY-semantic. `README.md` surfaces the flags + `pip install 'agctl[jq]'`.

- [ ] **Step 3: Commit the doc sync**

Run: `git add docs/ skills/ README.md`
Run: `git commit -m "docs: sync HTTP-jq assertions + mock match.jq (DESIGN/ARCH/skills/README)"` + trailer.

---

## Self-Review (run before handing off)

1. **Code scan:** No task contains method bodies, algorithms, or test code — only behavior descriptions, exact signatures, data shapes, and expected test results. (Verified: steps describe what to build, not how to code it.)
2. **Self-containment:** Each task's Produces block states the exact signature/shape later tasks consume (e.g. Task 3's `compile_jq(expr, *, label=None)`, Task 8's `evaluate_http_assertions(result, *, status, contains, match, jq_path, equals)` + per-mode failure shapes, Task 4's `iter_mock_jq_expressions` / `collect_jq_compile_errors`). No task says "see the spec" for a contract.
3. **Spec coverage:** §2 goals → Tasks 8+9 (assertions), 5+6 (mock match.jq), 3+5 (loud-on-typo). §3 constraints → Global Constraints. §5 D1-D10 → D1(Task 9), D2(Task 8 AND), D3(Task 8 no coerce), D4(Task 2 coexist), D5(Task 3+5+10), D6(Task 5), D7(Task 1+3 hint), D8(Task 8 pairing), D9(Task 8 detail shape), D10(Task 8 any-semantic). §6 → Task 9. §7 → Task 2. §8.1 → Task 6. §8.2 → Tasks 3+5. §8.3 → Task 8. §9 → encoded across Tasks (exit codes). §10 → Tasks 5+10 (pre-compile) + Task 7 (shadowing). §11 → no structural change (covered: discovery unchanged). §12 → Task 1. §13 → Tasks' test files match. §14 → deferred items respected (no extraction, no `--match-all`, no ping assertions, no status ranges). §16 → Task 12.
4. **Placeholder scan:** No TBD/TODO/"add error handling"/"similar to" — every test step names its scenario + expected result; every implementation step names the behavior.
5. **Type consistency:** `compile_jq(expr, *, label=None)` consistent in Tasks 3/4/5/10. `evaluate_http_assertions(result, *, status, contains, match, jq_path, equals)` consistent in Tasks 8/9. `HttpMatch.jq: str | None` consistent in Tasks 2/4/6/7. `iter_mock_jq_expressions` / `collect_jq_compile_errors` consistent in Tasks 4/5/10. Per-mode failure entry field names (`mode`/`expected`/`actual`/`needle`/`matched`/`expr`/`result`/`path`) match between Task 8's Produces contract and the Global Constraints table.
