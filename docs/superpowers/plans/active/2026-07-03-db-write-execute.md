# DB Write Support (`agctl db execute`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deliberately-gated `agctl db execute` command that lets agents seed/mutate DB state (INSERT/UPDATE/DELETE) for the reproduce-a-bug workflow, with two runtime safety gates, type-distinct read/write paths, and write templates that are safe-by-construction at config-validate time.

**Architecture:** New `db execute` command in `db_commands.py` reuses existing `resolve_db_request` for sql/params/connection but adds two runtime gates (`--write` flag + `writable: true` connection), an explicit-target rule, and a mode check. The write primitive is a new **optional** driver capability `execute_write` (the `DBDriver` Protocol is unchanged) probed by `DbClient`. Writes commit as the last step after result materialization so a reported failure guarantees no commit landed. Read paths (`db query`, `db assert`, `DBDriver.execute`) are untouched. Config gains `DatabaseConnection.writable` and `DatabaseTemplate.mode`; both are denylisted from `AGCTL_*` env overrides so writability stays a file-only, reviewable property.

**Tech Stack:** Python ≥3.11, Click ≥8.1, Pydantic v2, psycopg3 (the `db` extra, lazy-imported), pytest. One-JSON-envelope-per-invocation via `@envelope` + `output.emit`.

**Spec:** `docs/superpowers/specs/active/2026-07-03-db-write-execute-design.md` (source of truth).

## Global Constraints

- `requires-python = ">=3.11"`; core deps `click>=8.1`, `pyyaml>=6.0`, `pydantic>=2.0`; the `db` extra is `psycopg[binary]>=3.1` + `jq>=1.6`.
- Every command writes exactly one JSON envelope via `output.emit()`; `_core` functions are wrapped by `@envelope(command)`. `emit()` is the only permitted stdout path.
- Exit codes: `0` success, `1` AssertionError, `2` ConfigError / ConnectionError / InternalError / TemplateNotFound.
- psycopg is **lazy-imported inside driver methods** (never at module import); unit tests run without psycopg installed.
- The **read path is unchanged**: `db query`, `db assert`, `DBDriver.execute`, and `PostgreSQLDriver.execute` stay byte-for-byte identical. No regression to existing tests.
- Gate/validation failures raise `ConfigError` (exit 2); write/commit failures raise `ConnectionFailure` (exit 2). No new error types.
- Conventional-commit messages (e.g. `feat(db): ...`); one commit per task unless a task says otherwise.

---

### Task 1: Config schema fields (`writable`, `mode`)

**Files:**
- Modify: `agctl/config/models.py` (`DatabaseConnection`, `DatabaseTemplate`)
- Test: `tests/unit/test_models.py`

**Interfaces:**
- Consumes: Pydantic v2 `BaseModel` / `Field` (already imported).
- Produces:
  - `DatabaseConnection.writable: bool = False` — additive field; existing configs (which omit it) load as read-only with no behavior change.
  - `DatabaseTemplate.mode` with type `Literal["read", "write"]` and default `"read"`. Requires importing `Literal` from `typing`. Any other value (or a typo) raised by Pydantic at `model_validate` as a `ValidationError` (the loader maps this to `ConfigError`, exit 2).
  - Both fields must accept being absent (defaults apply) and must round-trip through `model_dump()`.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_models.py`, add (mirroring the file's existing style of constructing models directly):

1. `DatabaseConnection(type="postgresql")` produces an instance whose `.writable is False` (default).
2. `DatabaseConnection(type="postgresql", writable=True)` produces `.writable is True`.
3. `DatabaseTemplate(sql="SELECT 1")` produces `.mode == "read"` (default).
4. `DatabaseTemplate(sql="...", mode="write")` produces `.mode == "write"`.
5. `DatabaseTemplate(sql="...", mode="bogus")` raises `pydantic.ValidationError` (invalid Literal).
6. Round-trip: `DatabaseConnection(type="postgresql", writable=True).model_dump()["writable"] is True`, and `DatabaseTemplate(sql="x", mode="write").model_dump()["mode"] == "write"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_models.py -v`
Expected: FAIL — `DatabaseConnection` has no `writable`, `DatabaseTemplate` has no `mode` (AttributeError / ValidationError on construction).

- [ ] **Step 3: Write minimal implementation**

Add the two fields with the exact types and defaults in the Produces block. Import `Literal` from `typing`. Do not change any other field.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_models.py -v`
Expected: PASS. Also run `pytest tests/unit -q` to confirm no regression (the shared fixture still loads — unknown fields were already ignored, now they are parsed).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/models.py tests/unit/test_models.py`
Run: `git commit -m "feat(db): add writable/mode config fields for db writes"`

---

### Task 2: Resolver denylist for `writable` / `mode`

**Files:**
- Modify: `agctl/config/resolver.py` (`apply_env_overrides`, `_deep_set`)
- Test: `tests/unit/test_resolver.py`

**Interfaces:**
- Consumes: `ConfigError` from `..errors`.
- Produces:
  - `apply_env_overrides(data, env)` now **raises `ConfigError`** when an `AGCTL_*` override resolves to a leaf segment whose lowercased name is `"writable"` or `"mode"`. Error message names the offending env-var-shaped field and states it must be set in `agctl.yaml`, not via `AGCTL_*`. Example detail: `{"field": "writable"}`.
  - The denylist matches the **leaf** path segment only (the last element of the parsed path), case-insensitively (paths are already lowercased by `_parse_path`). Non-leaf uses of the words are unaffected.
  - All other override behavior (≥2-segment rule, case/hyphen-insensitive matching, write-new-key, deep-copy) is unchanged — existing `test_resolver.py` cases must still pass.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_resolver.py` (the file tests `apply_env_overrides` with inline dicts):

1. `apply_env_overrides({"database": {"connections": {"main-db": {"writable": False}}}}, {"AGCTL_DATABASE__CONNECTIONS__MAIN_DB__WRITABLE": "true"})` raises `ConfigError`.
2. `apply_env_overrides({"database": {"templates": {"t": {"mode": "read"}}}}, {"AGCTL_DATABASE__TEMPLATES__T__MODE": "write"})` raises `ConfigError`.
3. Positive control: a non-denylisted override on the same connection (e.g. `AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD`) still applies and does NOT raise (assert the returned dict has the new password).
4. Case-insensitive: `AGCTL_DATABASE__CONNECTIONS__MAIN_DB__WRITABLE` (already upper) and a lower/mixed variant both raise.
5. Non-leaf: an override path where `writable`/`mode` appears only as an intermediate segment is not denied (construct a contrived nested path; assert no raise — the denylist checks only the leaf).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_resolver.py -v`
Expected: FAIL — overrides currently apply silently; no `ConfigError` is raised.

- [ ] **Step 3: Write minimal implementation**

Introduce a module-level frozenset of denied leaf names `{"writable", "mode"}`. In the override loop (or in `_deep_set` at the point the leaf is about to be written), when the final path segment is in that set, raise `ConfigError` with the field name. Keep the change localized; do not alter matching/merging logic.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_resolver.py -v`
Expected: PASS (new cases pass; all pre-existing cases still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/resolver.py tests/unit/test_resolver.py`
Run: `git commit -m "feat(config): denylist writable/mode from AGCTL_* env overrides"`

---

### Task 3: Validator cross-ref rule (write template → writable connection)

**Files:**
- Modify: `agctl/config/validator.py` (`validate_config`)
- Test: `tests/unit/test_validator.py`

**Interfaces:**
- Consumes: `DatabaseTemplate.mode` and `DatabaseConnection.writable` from Task 1; the existing per-template loop structure in `validate_config`.
- Produces:
  - `validate_config(cfg)` gains one new rule appended to the existing DB-template loop: for each `database.templates[name]` whose `.mode == "write"`, resolve its connection as `tpl.connection` if set, else `cfg.defaults.database_connection`. If that resolved connection name is `None`, OR is not in `cfg.database.connections`, OR the resolved `DatabaseConnection.writable` is not `True`, append an **error** dict `{"path": f"database.templates.{name}", "message": <human text naming the template and the problem>}`.
  - This rule fires only for `mode == "write"` templates; read templates are unaffected. It runs wherever `validate_config` runs (i.e. `agctl config validate`) — see spec §8.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_validator.py` (uses the file's `_cfg(**overrides)` helper that builds a `Config` via `model_validate`):

1. A `write`-mode template whose `connection` points at a `writable: True` connection → `validate_config` returns **no errors** for it (and no new warnings).
2. A `write`-mode template whose `connection` points at a connection with `writable` omitted (False) → exactly one error whose `path == f"database.templates.{name}"`.
3. A `write`-mode template with no `connection` and no `defaults.database_connection` → one error (same path shape).
4. A `write`-mode template whose `connection` is an unknown name → one error (the existing dangling-connection rule may also fire; assert at least one error names the writable problem or that the write rule contributes — keep the assertion robust by checking `any(e["path"].startswith("database.templates.") for e in errors)` and that errors is non-empty).
5. A `read`-mode template (default) against a read-only connection → no error from this rule (regression guard).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_validator.py -v`
Expected: FAIL — the new write-template cases currently produce no errors.

- [ ] **Step 3: Write minimal implementation**

In the existing `for name, tpl in cfg.database.templates.items()` loop, after the current dangling-connection check, add the write-mode check described in Produces. Resolve the connection name and look it up in `cfg.database.connections`; append an error when the resolved connection is missing, unknown, or not writable. No new warnings.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_validator.py -v`
Expected: PASS (new cases pass; all existing dangling-ref/warning cases still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/config/validator.py tests/unit/test_validator.py`
Run: `git commit -m "feat(config): validate write templates target a writable connection"`

---

### Task 4: `PostgreSQLDriver.execute_write` (commit-after-materialize)

**Files:**
- Modify: `agctl/clients/db_drivers/postgresql.py` (`PostgreSQLDriver`)
- Test: `tests/unit/test_postgresql_driver.py`

**Interfaces:**
- Consumes: `convert_sql_params` (from `...resolution`), `coerce_db_value` (from `...assertions`), `ConfigError`/`ConnectionFailure` (from `...errors`), and the existing `_owned`/`_conn`/`connectable` fields. The read `execute()` method is **not** modified.
- Produces:
  - `PostgreSQLDriver.execute_write(self, sql: str, params: dict) -> dict` returning `{"rows_affected": int | None, "returning": list[dict]}`.
  - Behavior contract (NOT code — the implementer writes it):
    - Rewrite `:name` → `%(name)s` via `convert_sql_params` (same as read `execute`).
    - Open a cursor; execute the rewritten SQL with `params`.
    - `rows_affected` = the cursor's reported row count. When the driver reports no count (psycopg `cursor.rowcount == -1`, e.g. DDL), set `rows_affected = None`.
    - `returning`: if `cursor.description` is not None, fetch all rows and coerce each cell via `coerce_db_value` keyed by column name (identical to read `execute`); else `[]`.
    - **Commit is the LAST step**, after `rows_affected` and `returning` are fully materialized.
    - On any `psycopg.Error` (execute, fetch, coerce, or commit), roll the connection back and raise `ConnectionFailure` (message from the driver error). The contract guarantee: a raised `ConnectionFailure` means **no commit landed**.
    - psycopg is lazy-imported inside the method (mirrors read `execute`).
  - `close()` and `_owned` semantics are unchanged; an injected `connectable` is still not closed by this driver.

- [ ] **Step 1: Write the failing tests**

Extend `tests/unit/test_postgresql_driver.py`. The existing `FakeCursor`/`FakeConn` doubles must be extended to support writes: add a `rowcount` attribute to `FakeCursor`, a `RETURNING`-style row set (reuse `description` + `fetchall`), and `commit()`/`rollback()` methods on `FakeConn` that record calls. (These are test doubles the implementer writes from this description.)

Test cases (each describes scenario + expected result):
1. INSERT with `RETURNING id, status`: `execute_write` returns `rows_affected` equal to the fake cursor's `rowcount`, and `returning` is the list of coerced dict rows; the fake connection's `commit` was called exactly once and `rollback` was not called. Assert the cursor received the `%(name)s`-rewritten SQL and the params dict.
2. DDL / no-count statement (`rowcount == -1`): `rows_affected is None`; `returning == []`; commit called once.
3. Ordering guarantee: when `fetchall`/coercion raises (simulate by making the fake cursor's `fetchall` raise a `psycopg.Error` subclass), `execute_write` raises `ConnectionFailure`, the fake connection's `rollback` was called, and its `commit` was **never** called (this is the load-bearing correctness assertion).
4. Execute-error path: cursor `execute` raises `psycopg.Error` → `ConnectionFailure`; commit not called.
5. Injected-`connectable` still not closed after a write (regression guard for `_owned`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_postgresql_driver.py -v`
Expected: FAIL — `PostgreSQLDriver` has no `execute_write`.

- [ ] **Step 3: Write minimal implementation**

Add `execute_write` to `PostgreSQLDriver` per the Produces contract. Lazy-import psycopg. Reuse `convert_sql_params` and `coerce_db_value`. Do not modify `execute`, `connect`, or `close`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_postgresql_driver.py -v`
Expected: PASS (new cases pass; all existing read/coerce/close/protocol cases still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_drivers/postgresql.py tests/unit/test_postgresql_driver.py`
Run: `git commit -m "feat(db): add PostgreSQLDriver.execute_write with commit-after-materialize"`

---

### Task 5: `DbClient.execute_write` + optional-capability probe

**Files:**
- Modify: `agctl/clients/db_client.py` (`DbClient`)
- Test: `tests/unit/test_db_client.py`

**Interfaces:**
- Consumes: `ConfigError` (from `..errors`); the optional `execute_write` method produced by Task 4.
- Produces:
  - `DbClient.execute_write(self, sql: str, params: dict) -> dict` — if the selected driver does not expose a callable `execute_write` (probed via `getattr(self._driver, "execute_write", None)`), raise `ConfigError("connection's driver (<type>) does not support writes", {"driver": <self._conn_dict['type']>})`; otherwise delegate to `self._driver.execute_write(sql, params)` and return its dict.
  - The `DBDriver` Protocol is **not** modified (this is why out-of-tree read-only drivers keep working).

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_db_client.py` (the file already constructs `DbClient` with injected `driver=` fakes; reuse that seam):

1. A `DbClient` whose injected driver **has** an `execute_write` that returns `{"rows_affected": 2, "returning": [{"id": "x"}]}` → `DbClient.execute_write("...", {...})` returns that dict unchanged.
2. A `DbClient` whose injected driver **lacks** `execute_write` (a read-only fake with only `connect`/`execute`/`close`) → `execute_write(...)` raises `ConfigError` whose `detail["driver"]` equals the connection's `type`.
3. The probe is attribute-based and case-exact: a driver with a non-callable `execute_write` attribute (e.g. set to a string) also raises `ConfigError`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_client.py -v`
Expected: FAIL — `DbClient` has no `execute_write`.

- [ ] **Step 3: Write minimal implementation**

Add `execute_write` to `DbClient` per the Produces contract (probe, then delegate). Do not change `execute`, `connect`, `close`, `load_drivers`, or the constructor.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_client.py -v`
Expected: PASS (new cases pass; existing dispatch/selection/seam cases still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_client.py tests/unit/test_db_client.py`
Run: `git commit -m "feat(db): add DbClient.execute_write with optional-capability probe"`

---

### Task 6: `db execute` command (gates, explicit-target, mode check) + fixture + discovery-count bumps

**Files:**
- Modify: `agctl/commands/db_commands.py` (new `_db_execute_core`, `db_execute`, helper, export)
- Modify: `agctl/cli.py` (import + register `db_execute`)
- Modify: `tests/fixtures/agctl.yaml` (add `main-db-writable` connection + `seed-order` write template)
- Modify: `tests/unit/test_discover_command.py` (bump two count assertions `3 → 4`)
- Test: `tests/unit/test_db_commands.py`

**Interfaces:**
- Consumes: `resolve_db_request`, `new_db_client`, `envelope("db.execute")`, `load_config_or_raise`, `ConfigError`, the `DatabaseTemplate.mode`/`DatabaseConnection.writable` fields (Task 1), and `DbClient.execute_write` (Task 5).
- Produces:
  - A helper `def _check_template_mode(cfg, template_name: str | None, forbidden: str) -> None` — if `template_name` is not None and `cfg.database.templates[template_name].mode == forbidden`, raise `ConfigError(message, {})`. (Shared with Task 7.)
  - `def _db_execute_core(config_path, template, sql, param, connection, write) -> dict` returning `{"rows_affected": int | None, "returning": list[dict], "connection": str, "sql": str}`.
  - A Click command `db_execute` named `"execute"` with options `--template`, `--sql`, `--param` (multiple), `--connection`, and `--write` (a `click.option(..., is_flag=True)`. The Click command delegates to `_db_execute_envelope = envelope("db.execute")(_db_execute_core)`, exactly mirroring `db_query`/`db_assert`.
  - Processing order inside `_db_execute_core` (all failures `ConfigError` exit 2, before any DB connection is opened):
    1. `cfg = load_config_or_raise(config_path)`.
    2. `sql_text, params, conn_name = resolve_db_request(cfg, template=..., sql=..., param_tuple=param, connection_name=connection)` — this enforces template-XOR-sql, neither-given, unknown template (`TemplateNotFound`), unknown connection (`ConfigError`).
    3. **Explicit-target rule:** if `template is None and connection is None` → `ConfigError` ("db execute requires --template or --connection to name the write target; refusing to write to the default connection implicitly").
    4. **Invocation gate:** if `not write` → `ConfigError`.
    5. **Mode check (execute's own):** `_check_template_mode(cfg, template, forbidden="read")` (a read-mode template on `execute` is rejected).
    6. **Connection gate:** if `not cfg.database.connections[conn_name].writable` → `ConfigError`.
    7. Open `new_db_client(cfg.database.connections[conn_name])`; `connect()`; `result = client.execute_write(sql_text, params)` in a `try/finally` that `close()`s. Return `{**result, "connection": conn_name, "sql": sql_text}`. (`execute_write` already includes `rows_affected`/`returning`.)
- Fixture additions (`tests/fixtures/agctl.yaml`): a `main-db-writable` entry under `database.connections` with `type: postgresql`, the same `${DB_HOST}`/`${DB_PORT:-5432}`/`${DB_NAME}`/`${DB_USER}`/`${DB_PASSWORD}` interpolation as `main-db`, and `writable: true` (no `default`). A `seed-order` entry under `database.templates` with `description`, `mode: write`, `connection: main-db-writable`, and a SQL body that is an idempotent `INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id, status` using `:orderId` and `:status` params (author chooses exact columns; keep it self-contained against a throwaway table).
- Discovery-count bumps: `tests/unit/test_discover_command.py` line ~59 (`counts["db_templates"] == 3`) and line ~100 (`res["count"] == 3` for `--category db-templates`) both become `== 4`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_db_commands.py`. Reuse the file's `_run`/`_payload`/`install_fake` helpers and `ENV`. Extend `FakeDriver` (or add a `FakeWriteDriver`) with an `execute_write(self, sql, params)` that records `(sql, params)` and returns a configurable `{"rows_affected": ..., "returning": [...]}`; the `install_fake` factory must wire this driver so command tests avoid a real DB. Write:

1. Happy path (template): `db execute --template seed-order --param orderId=o1 --param status=PENDING --write` → exit 0, `command == "db.execute"`, `result.rows_affected`/`result.returning` echo the fake, `result.connection == "main-db-writable"`, `result.sql` is the seed-order SQL with `:orderId`/`:status` still present (placeholders intact), and the fake received `%(orderId)s`/`%(status)s` rewritten SQL + the params dict.
2. Happy path (free-form with explicit connection): `db execute --connection main-db-writable --sql "DELETE FROM t WHERE id = :i" --param i=9 --write` → exit 0; `result.connection == "main-db-writable"`.
3. Missing `--write`: same template invocation **without** `--write` → exit 2, `ConfigError`, and the fake's `execute_write` was **never** called (fail-fast guard, mirror the existing `fake.executed == []` pattern).
4. Connection gate: `db execute --connection main-db --sql "DELETE FROM t" --write` (main-db is read-only) → exit 2 `ConfigError`; fake untouched.
5. Explicit-target refusal: `db execute --sql "DELETE FROM t" --write` (no template, no connection) → exit 2 `ConfigError` with a message mentioning an explicit target; fake untouched.
6. Execute's mode check: `db execute --template find-order --write` (find-order is read-mode) → exit 2 `ConfigError`; fake untouched.
7. Mutually-exclusive `--template`/`--sql` and neither-given already raise via `resolve_db_request` → assert exit 2 `ConfigError` for `--template seed-order --sql "x" --write`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_commands.py -v`
Expected: FAIL — `db execute` is not registered (Click "No such command" / exit 2 with a non-config error).

- [ ] **Step 3: Write minimal implementation**

Add `_check_template_mode`, `_db_execute_core`, `db_execute`, and `_db_execute_envelope` to `db_commands.py` per Produces; add `db_execute` to the module's `__all__`. In `cli.py`, import `db_execute` from `.commands.db_commands` and add `db_group.add_command(db_execute)` next to the existing `db_query`/`db_assert` registrations. Add the fixture entries. Bump the two discovery-count assertions.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_commands.py tests/unit/test_discover_command.py -v`
Expected: PASS. Then run `pytest tests/unit -q` to confirm no regression anywhere.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/db_commands.py agctl/cli.py tests/fixtures/agctl.yaml tests/unit/test_db_commands.py tests/unit/test_discover_command.py`
Run: `git commit -m "feat(db): add agctl db execute command with safety gates"`

---

### Task 7: Read-side mode checks (`db query` / `db assert` refuse write-mode templates)

**Files:**
- Modify: `agctl/commands/db_commands.py` (`_db_query_core`, `_db_assert_core`)
- Test: `tests/unit/test_db_commands.py`

**Interfaces:**
- Consumes: `_check_template_mode(cfg, template_name, forbidden)` produced by Task 6; `resolve_db_request` already called inside both cores.
- Produces:
  - `_db_query_core` and `_db_assert_core` each gain one call after their `resolve_db_request(...)` step: `_check_template_mode(cfg, template, forbidden="write")`. A `write`-mode template run through either read command raises `ConfigError` (exit 2) before the DB is touched. Read-mode/default templates are unaffected. Free-form `--sql` (no template) is unaffected (no mode to check).
  - `_check_template_mode` is unchanged from Task 6 (this task consumes it).

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_db_commands.py`:

1. `db query --template seed-order --param orderId=o1` → exit 2 `ConfigError` (seed-order is write-mode); fake `execute` never called.
2. `db assert --template seed-order --param orderId=o1 --expect-rows 1` → exit 2 `ConfigError`; fake never called.
3. Regression guards: `db query --template find-order --param orderId=o1` (read-mode) still works (exit 0) and `db query --sql "SELECT 1"` (free-form) still works (exit 0). These two must remain green.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_commands.py -v`
Expected: FAIL — write-mode templates currently run through the read path (the query case likely proceeds to the fake and may exit 0; the assert case runs then fails the assertion rather than failing fast as `ConfigError`).

- [ ] **Step 3: Write minimal implementation**

Add the single `_check_template_mode(cfg, template, forbidden="write")` call inside `_db_query_core` and inside `_db_assert_core`, immediately after each `resolve_db_request(...)` call. No other changes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_commands.py -v`
Expected: PASS (new cases pass; all existing query/assert cases still pass).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/db_commands.py tests/unit/test_db_commands.py`
Run: `git commit -m "feat(db): reject write-mode templates on db query and db assert"`

---

### Task 8: Discovery `mode` marker (additive)

**Files:**
- Modify: `agctl/commands/discover_commands.py` (`_category_core`, `_item_core`, `_search_core`, `_db_example`)
- Test: `tests/unit/test_discover_command.py`

**Interfaces:**
- Consumes: `DatabaseTemplate.mode` from Task 1; the existing per-template item construction.
- Produces:
  - `_category_core` (db-templates branch): each item gains `"mode": tpl.mode`.
  - `_item_core` (db-templates branch): the item gains `"mode": tpl.mode`, and the `example` is produced by a mode-aware `_db_example`.
  - `_db_example(name, params, mode)` → when `mode == "write"`: `f"agctl db execute --template {name} --write"` plus `--param X=Y` tokens when params exist; when `mode == "read"` (or default): the existing `agctl db query --template {name}` form. (Adjust the existing `_db_example(name, params)` signature; update its callers to pass `tpl.mode`.)
  - `_search_core` (db-templates branch): each match gains `"mode": tpl.mode`.
  - The change is **purely additive** to the db-templates item dicts (existing keys unchanged). Services / http-templates / kafka-patterns outputs are untouched.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_discover_command.py` (uses the file's `_run([...], monkeypatch)` helper and shared fixture):

1. `discover --category db-templates` Level-1: every item has a `"mode"` key; `find-order`/`orders-by-status`/`count-failed-payments` report `"read"` and `seed-order` reports `"write"`; `count == 4`.
2. `discover --category db-templates --name seed-order` Level-2: item has `"mode": "write"` and `example.startswith("agctl db execute --template seed-order --write")`; the `sql` is present.
3. `discover --category db-templates --name find-order` Level-2: `"mode": "read"` and `example.startswith("agctl db query --template find-order")` (unchanged behavior).
4. `discover --search order` (or a term matching `seed-order`): the `seed-order` match carries `"mode": "write"`; `find-order` carries `"mode": "read"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_discover_command.py -v`
Expected: FAIL — db-templates items have no `mode`; write-template example is the read form.

- [ ] **Step 3: Write minimal implementation**

Thread `tpl.mode` into the three db-templates branches and make `_db_example` mode-aware per Produces. Run the full discovery test file; any pre-existing assertion that compared a db-templates item dict **exactly** (and now lacks the new `mode` key) must be updated to include the expected `"mode"` value — update those assertions in the same edit.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_discover_command.py -v`
Expected: PASS. Then `pytest tests/unit -q` for no regression.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/discover_commands.py tests/unit/test_discover_command.py`
Run: `git commit -m "feat(discover): surface db template mode in discovery output"`

---

### Task 9: Integration round-trip (live `db execute` → `db query`)

**Files:**
- Modify: `tests/integration/test_db_commands.py`
- Test: `tests/integration/test_db_commands.py` (self-skipping via `require_postgres`)

**Interfaces:**
- Consumes: the `require_postgres` fixture and `_env()` helper already in `tests/integration/conftest.py` and `tests/integration/test_db_commands.py`; the shared `FIXTURE` (now containing `main-db-writable` + `seed-order` from Task 6); `CliRunner`.
- Produces: a self-skipping integration test `test_db_execute_then_query_visible` that proves a committed write is visible to a subsequent read in a separate invocation, plus the commit-on-error rollback path.

- [ ] **Step 1: Write the failing test**

The live testcontainer Postgres is empty, so the test must create its own throwaway table via `db execute` (DDL is a write; it commits). Scenario + expected results:
1. Create table: `db execute --connection main-db-writable --sql "CREATE TABLE IF NOT EXISTS agctl_seed (id text PRIMARY KEY, status text)" --write` → exit 0.
2. Seed (idempotent): `db execute --template seed-order --param orderId=<unique> --param status=PENDING --write`. (If `seed-order`'s columns do not match `agctl_seed`, use a free-form `--sql` `INSERT INTO agctl_seed (id,status) VALUES (:i,:s) ON CONFLICT (id) DO NOTHING RETURNING id,status` with `--connection main-db-writable --write` instead.) → exit 0; `result.rows_affected in (0, 1)`; `result.returning` contains the row when inserted.
3. Visibility: `db query --connection main-db-writable --sql "SELECT status FROM agctl_seed WHERE id = :i" --param i=<unique>` → exit 0; the row is present with `status == "PENDING"` (proves the prior commit is visible to a fresh connection).
4. Rollback-on-error: `db execute --connection main-db-writable --sql "INSERT INTO agctl_seed (id) VALUES (:i); <syntax error>" --write` → exit 2 (`ConnectionFailure`); a follow-up `db query` confirms no partial row was committed. (If multi-statement behavior makes this awkward, instead assert rollback on a single malformed statement that the DB rejects, e.g. inserting into a non-existent table.)

Use a unique id per run (e.g. derived from a fixed prefix; `Date.now()`/random are not available in some harness contexts — use a hard-coded unique string or a counter from `os.getpid()`). The whole test takes `require_postgres` as an argument so it skips when Postgres is unreachable.

- [ ] **Step 2: Run test to verify it fails (or skips)**

Run: `pytest tests/integration/test_db_commands.py -v`
Expected: without `AGCTL_TEST_LIVE=1`/`AGCTL_TEST_PG_DSN`, the test **skips** ("AGCTL_TEST_PG_DSN not set"). With a live DB (`AGCTL_TEST_LIVE=1`), it should PASS once the feature is implemented; before Tasks 1–8 it would fail.

- [ ] **Step 3: (No new implementation — this task is verification-only)**

If the live run surfaces a real defect in Tasks 1–8, fix it there; otherwise this task adds only the test. Confirm the test passes against a live DB (`AGCTL_TEST_LIVE=1 pytest tests/integration/test_db_commands.py -v`) when available, and skips cleanly otherwise.

- [ ] **Step 4: Run the full suite**

Run: `pytest tests/unit tests/integration -q`
Expected: all unit tests PASS; integration tests PASS or SKIP (never fail for a missing service).

- [ ] **Step 5: Commit**

Run: `git add tests/integration/test_db_commands.py`
Run: `git commit -m "test(db): integration round-trip for db execute write-then-query"`

---

### Task 10: Skill artifacts + doc-sync handoff

**Files:**
- Create: `skills/agctl-config/reference/db-write-template.md`
- Modify: `skills/agctl-config/SKILL.md` (link the new reference; add write-template authoring guidance)
- Modify: `skills/agctl/SKILL.md` (add `db execute` usage + the seed/`--write` pattern)
- (DESIGN.md / ARCHITECTURE.md sync is performed by the `docs-watcher` subagent after this task — see Step 4.)

**Interfaces:**
- Consumes: the final behavior of Tasks 1–9; the existing `skills/agctl-config/reference/db-template.md` as the house-style template for a new reference file.
- Produces:
  - `skills/agctl-config/reference/db-write-template.md` — a reference in the same format as `db-template.md`, documenting: the `mode: write` field, the requirement that the template's `connection` be `writable: true` (caught by `agctl config validate`), the `:paramName::cast` param-typing note for numeric/timestamp columns, and an idempotent-insert example (`ON CONFLICT ... DO NOTHING`).
  - `skills/agctl-config/SKILL.md` — a short section / pointer for authoring write templates.
  - `skills/agctl/SKILL.md` — usage for `agctl db execute` (the two gates `writable` connection + `--write`, the explicit-target rule, the `db.execute` result shape with `rows_affected`/`returning`).

- [ ] **Step 1: Read the existing references for house style**

Read `skills/agctl-config/reference/db-template.md` and `skills/agctl-config/SKILL.md` to match structure/voice. Read `skills/agctl/SKILL.md` to find where `db query`/`db assert` usage lives and add `db execute` alongside.

- [ ] **Step 2: Write the new reference + update the two SKILL.md files**

Author `db-write-template.md` and edit both `SKILL.md` files per Produces. Keep guidance consistent with the spec (§6–§10): two gates, explicit target, `mode` field, `writable` connection, idempotency is the author's job, params are strings (use `::cast`).

- [ ] **Step 3: Verify the skill files are well-formed**

Re-read the three files end-to-end; confirm every command example would actually work against the implemented CLI (flags match `db execute` exactly: `--template`/`--sql`/`--param`/`--connection`/`--write`; `writable` connection; `mode: write`).

- [ ] **Step 4: Hand off to docs-watcher**

Invoke the `docs-watcher` subagent (per CLAUDE.md "Docs Sync") to sync DESIGN.md §2.1 (`writable`/`mode`), §3.3 (`db execute`), §4.2 (`db.execute` result) and ARCHITECTURE.md §5 (resolver denylist), §8 (optional `execute_write` capability + commit ordering), §7 (new error rows), §10 (Protocol unchanged). Preserve each doc's altitude (DESIGN = WHAT/WHY, ARCHITECTURE = HOW). A correct no-op for a section that already matches is fine.

- [ ] **Step 5: Commit**

Run: `git add skills/agctl-config/reference/db-write-template.md skills/agctl-config/SKILL.md skills/agctl/SKILL.md docs/DESIGN.md docs/ARCHITECTURE.md`
Run: `git commit -m "docs(skills): document db execute + write-template authoring; sync design/architecture"`

---

## Self-Review (completed during planning)

1. **Code scan:** No method bodies, algorithms, or test/implementation code appear — only signatures, data shapes, behavior descriptions, and expected test results.
2. **Self-containment:** Each task's Consumes/Produces block carries the exact signatures, types, validation rules, error cases, and processing order needed to implement it without reading the spec.
3. **Spec coverage:** §3 Threat Model (no code task — framing only), §5 decisions → Tasks 1–8; §6 command/gates/target → Task 6; §6.2 mode checks (both read commands) → Tasks 6 + 7; §7.2 env denylist → Task 2; §8 validator rule → Task 3; §9 driver capability + ordering → Tasks 4 + 5; §10 result contract → Task 6 (result dict) + Task 4 (rows_affected/returning); §11 discovery → Task 8; §12 error model → Tasks 2/3/6/7; §13 transaction semantics → Task 4 (ordering) + Task 9 (round-trip); §14 testing → all tasks; §15 docs/skill → Task 10. The one deliberately deferred spec item is the Threat Model subsection itself (prose, lives in DESIGN.md via docs-watcher in Task 10).
4. **Placeholder scan:** Each step states a concrete scenario + expected result or a concrete behavior; no TBD/TODO/"add error handling".
5. **Type consistency:** `_check_template_mode(cfg, template_name, forbidden)` is named identically in Tasks 6 and 7; `execute_write(sql, params) -> {"rows_affected": int|None, "returning": list[dict]}` is consistent across Tasks 4, 5, 6; `db.execute` result fields (`rows_affected`, `returning`, `connection`, `sql`) match across Task 6 Produces and Task 8/Task 9 expectations.
