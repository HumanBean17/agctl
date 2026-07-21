# DB Driver Breadth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship built-in MySQL (PyMySQL) + SQLite (stdlib) drivers for `agctl db *`, and harden the DBDriver protocol with typed DTOs + a `BaseDBDriver` mixin so contributors can add more drivers as one-file drop-ins.

**Architecture:** DTOs (`WriteResult`, `ColumnInfo`, `ForeignKey`, `UniqueConstraint`, `SchemaItem`, `SchemaMatch`) formalize the implicit dict contracts returned by `execute_write` / `describe_schema`. `BaseDBDriver` is a non-abstract mixin with shared helpers (`_redact_config`, `_lazy_import_or_raise`). PostgreSQL is refactored (behavior-preserving) to inherit the mixin and return DTOs; `DbClient` serializes DTOs to dicts via `dataclasses.asdict()` at the boundary so the JSON envelope users see is unchanged. Two new drivers (`mysql.py`, `sqlite.py`) follow the same shape. Per-engine extras: `[mysql]` adds PyMySQL; SQLite ships in core (stdlib).

**Tech Stack:** Python ≥3.11, Pydantic v2, Click, PyMySQL (new optional dep), stdlib `sqlite3`, pytest, testcontainers (integration).

**Spec:** [`docs/superpowers/specs/active/2026-07-21-db-driver-breadth-design.md`](../specs/active/2026-07-21-db-driver-breadth-design.md)

## Global Constraints

- **Python ≥ 3.11** (pyproject.toml floor).
- **PyMySQL ≥ 1.1** under a new `[mysql]` extra; never fold into `[db]`.
- **stdlib `sqlite3`** imported at module top in `sqlite.py` — no extra, ships in core.
- **Lazy-import invariant:** `mysql.py` and `postgresql.py` must NOT trigger PyMySQL / psycopg imports at module load — only inside `connect()`. `db_client.py`'s `BUILTIN_DRIVERS` population must not import either heavy dep.
- **PRAGMA identifier regex** (sqlite only): `^[a-zA-Z_][a-zA-Z0-9_]*$` — reject non-matching identifiers rather than escaping them.
- **`autocommit=False` on connect** for MySQL (PyMySQL) — explicit commit/rollback semantics matching PostgreSQL.
- **Native `:name` params** for SQLite (no `convert_sql_params` call); PostgreSQL + MySQL use `convert_sql_params` to rewrite `:name` → `%(name)s`.
- **DTOs return from drivers; `dataclasses.asdict()` serialization happens in `DbClient`** — never in the command layer.
- **No ABCs.** `BaseDBDriver` is a mixin; the `DBDriver` Protocol remains the structural contract.
- **No Pydantic enum on `DatabaseConnection.type`** — stays free-form string; "unknown type" surfaces at `DbClient.__init__` time, not config-load time.
- **Commit message convention:** `type: subject` (e.g., `feat:`, `refactor:`, `docs:`, `test:`). No Co-authored-by trailers.
- **Test discipline:** unit tests for MySQL use FakeCursor/FakeConn seams (mirrors PostgreSQL pattern); SQLite tests use real in-memory connections (stdlib, zero infra).

---

## Task 1: DTOs in `db_driver_protocol.py`

**Files:**
- Modify: `agctl/clients/db_driver_protocol.py`
- Test: `tests/unit/test_db_driver_protocol.py` (NEW)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: six dataclasses available at `agctl.clients.db_driver_protocol`. Later tasks import these by name.
  - `WriteResult` — fields: `rows_affected: int | None`, `returning: list[dict] = field(default_factory=list)`.
  - `ColumnInfo` — fields: `name: str`, `data_type: str`, `nullable: bool`, `default: str | None = None`, `generated: str | None = None` (values: `"always_identity"` | `"by_default_identity"` | `"stored"` | `None`), `enum_values: list[str] | None = None`, `comment: str | None = None`.
  - `ForeignKey` — fields: `name: str`, `columns: list[str]`, `references_schema: str | None`, `references_table: str`, `references_columns: list[str]`.
  - `UniqueConstraint` — fields: `name: str`, `columns: list[str]`.
  - `SchemaItem` — fields: `schema: str`, `name: str`, `kind: str` (`"table"` | `"view"`), `column_count: int`.
  - `SchemaMatch` — fields: `schema: str`, `table: str`, `kind: str`, `comment: str | None`, `columns: list[ColumnInfo]`, `primary_key: list[str]`, `foreign_keys: list[ForeignKey]`, `unique_constraints: list[UniqueConstraint]`.
  - All use `@dataclass` from `dataclasses`. Mutable defaults (`list`, `dict`) use `field(default_factory=...)`.
  - `dataclasses.asdict(instance)` on any of these returns a JSON-serializable nested dict (lists of dataclasses recurse).

- [ ] **Step 1: Write failing tests for DTO construction + asdict round-trip**

Test file: `tests/unit/test_db_driver_protocol.py`. Verify:
1. `WriteResult(rows_affected=5)` has `returning == []` (default factory).
2. `WriteResult(rows_affected=None, returning=[{"id": 1}])` round-trips via `dataclasses.asdict` to `{"rows_affected": None, "returning": [{"id": 1}]}`.
3. `ColumnInfo(name="id", data_type="integer", nullable=False)` has `default=None, generated=None, enum_values=None, comment=None`.
4. `SchemaMatch(schema="public", table="orders", kind="table", comment=None, columns=[ColumnInfo(...)], primary_key=["id"], foreign_keys=[], unique_constraints=[])` round-trips via `asdict` to a nested dict with `columns` as a list of dicts (each dict has all 7 `ColumnInfo` fields).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_driver_protocol.py -v`
Expected: FAIL with `ImportError: cannot import name 'WriteResult'` (or equivalent).

- [ ] **Step 3: Add the six dataclasses**

Append to `agctl/clients/db_driver_protocol.py` (after the existing `DBDriver` Protocol). Add `from dataclasses import dataclass, field` to imports. Each dataclass is a top-level class with the fields and defaults listed in Produces above. No methods. No docstrings beyond a one-liner per class.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_driver_protocol.py -v`
Expected: PASS (all tests green).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_driver_protocol.py tests/unit/test_db_driver_protocol.py`
Run: `git commit -m "feat(db): add DTOs for WriteResult + schema description"`

---

## Task 2: `BaseDBDriver` mixin with shared helpers

**Files:**
- Modify: `agctl/clients/db_driver_protocol.py`
- Test: `tests/unit/test_db_driver_protocol.py` (extend)

**Interfaces:**
- Consumes: `ConfigError` from `agctl.errors`.
- Produces: `BaseDBDriver` class available at `agctl.clients.db_driver_protocol`.
  - Class attributes (not instance): `_SECRET_KEY_PATTERN = re.compile(r"password|secret|token|key", re.IGNORECASE)`, `_REDACTED_SENTINEL = "***"`, `_URL_USERINFO_PATTERN = re.compile(r"^([a-zA-Z][a-zA-Z0-9+]*://)[^@/]+@")` (scheme-agnostic — any `scheme://user:pass@host/...`).
  - `@classmethod _redact_config(cls, config: dict) -> dict` — returns a shallow copy of `config` with: (a) any key whose name matches `_SECRET_KEY_PATTERN` replaced by `_REDACTED_SENTINEL`; (b) a string `url` value with leading `scheme://user:pass@` userinfo replaced by `scheme://***@` via `_URL_USERINFO_PATTERN.sub`; (c) all other keys passed through unchanged. Original `config` is NOT mutated.
  - `@classmethod _lazy_import_or_raise(cls, module: str, extra: str)` — calls `importlib.import_module(module)`; on `ImportError` raises `ConfigError(f"Database support requires the '{extra}' extra: pip install 'agctl[{extra}]'")` chained from the original `ImportError`. Returns the imported module on success.
  - Not abstract. No required overrides. Drivers inherit only if they want the helpers.

- [ ] **Step 1: Write failing tests for `_redact_config` and `_lazy_import_or_raise`**

Extend `tests/unit/test_db_driver_protocol.py`. Verify:
1. `_redact_config({"user": "u", "password": "p", "host": "h"})` returns `{"user": "u", "password": "***", "host": "h"}`.
2. `_redact_config({"api_token": "x", "ssl_key": "y", "port": 5432})` returns `{"api_token": "***", "ssl_key": "***", "port": 5432}`.
3. `_redact_config({"url": "postgresql://u:p4ss@h:5432/db"})` returns `{"url": "postgresql://***@h:5432/db"}`.
4. `_redact_config({"url": "mysql://root:secret@h:3306/db"})` returns `{"url": "mysql://***@h:3306/db"}`.
5. `_redact_config({"url": "/path/to/db.sqlite"})` returns `{"url": "/path/to/db.sqlite"}` unchanged (no scheme).
6. `_redact_config({"url": "file:/path?mode=ro"})` returns unchanged (`file:` scheme but no `@` userinfo).
7. Original input dict is not mutated (assert after call).
8. `_lazy_import_or_raise("sqlite3", "db")` returns the `sqlite3` module (stdlib, always present).
9. `_lazy_import_or_raise("nonexistent_module_xyz", "db")` raises `ConfigError` with message containing `"pip install 'agctl[db]'"` and the original `ImportError` chained (`__cause__`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_driver_protocol.py -v`
Expected: FAIL with `ImportError: cannot import name 'BaseDBDriver'`.

- [ ] **Step 3: Implement `BaseDBDriver`**

Append `BaseDBDriver` class to `agctl/clients/db_driver_protocol.py`. Add `import importlib`, `import re` to imports. Implement per Produces contract above. The `_URL_USERINFO_PATTERN` uses a capture group for the scheme prefix so the substitution can re-insert it: `pattern.sub(lambda m: f"{m.group(1)}{cls._REDACTED_SENTINEL}@", value)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_driver_protocol.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_driver_protocol.py tests/unit/test_db_driver_protocol.py`
Run: `git commit -m "feat(db): add BaseDBDriver mixin with redact_config + lazy_import_or_raise"`

---

## Task 3: Refactor PostgreSQL + DbClient DTO serialization

**Files:**
- Modify: `agctl/clients/db_drivers/postgresql.py`
- Modify: `agctl/clients/db_client.py`
- Modify: `tests/unit/test_postgresql_driver.py` (minor)
- Modify: `tests/unit/test_db_client.py` (extend)

**Interfaces:**
- Consumes: `BaseDBDriver`, `WriteResult`, `SchemaItem`, `SchemaMatch`, `ColumnInfo`, `ForeignKey`, `UniqueConstraint` from Task 1+2.
- Produces:
  - `PostgreSQLDriver(BaseDBDriver)` — same constructor signature (`__init__(self, *, connectable=None)`), same observable behavior. `execute_write` now returns a `WriteResult` instance. `describe_schema` now returns a dict `{"items": list[SchemaItem], "matches": list[SchemaMatch]}` (DTOs inside, not raw dicts). The `_redact_config` module-level function and `_URL_USERINFO_PATTERN` constant are removed; callers use `self._redact_config(config)` inherited from `BaseDBDriver`.
  - `DbClient.execute_write(sql, params) -> dict` — calls `self._driver.execute_write(...)`, then if the result is a dataclass instance, runs `dataclasses.asdict(result)`; if it's already a dict, passes through. Returns the dict.
  - `DbClient.describe_schema(table, schema) -> dict` — calls `self._driver.describe_schema(...)`, then recursively converts any dataclass instances inside the returned dict (top-level values AND nested in lists) via `dataclasses.asdict`. Returns a fully dict-ified structure.
  - JSON shape seen by users is **unchanged** from today (same field names, same nesting).

- [ ] **Step 1: Write failing test for DbClient DTO serialization**

Extend `tests/unit/test_db_client.py`. Verify:
1. A `FakeDriver` whose `execute_write` returns a `WriteResult(rows_affected=3, returning=[{"id": "x"}])` — `DbClient.execute_write(...)` returns `{"rows_affected": 3, "returning": [{"id": "x"}]}` (plain dict, not the dataclass instance).
2. A `FakeDriver` whose `describe_schema` returns `{"items": [SchemaItem(schema="public", name="orders", kind="table", column_count=3)]}` — `DbClient.describe_schema(...)` returns `{"items": [{"schema": "public", "name": "orders", "kind": "table", "column_count": 3}]}`.
3. A `FakeDriver` whose `describe_schema` returns `{"matches": [SchemaMatch(schema="public", table="t", kind="table", comment=None, columns=[ColumnInfo(name="id", data_type="int", nullable=False)], primary_key=[], foreign_keys=[], unique_constraints=[])]}` — `DbClient.describe_schema(...)` returns the fully-nested dict equivalent.
4. Backward-compat: a `FakeDriver` whose `execute_write` returns a plain dict `{"rows_affected": 1, "returning": []}` — `DbClient.execute_write(...)` returns that dict unchanged (no asdict crash on non-dataclass).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_client.py -v -k "dto or serializ"`
Expected: FAIL (asdict not applied yet).

- [ ] **Step 3: Update `DbClient` to serialize DTO returns**

Modify `agctl/clients/db_client.py`:
- `execute_write`: after calling `self._driver.execute_write(sql, params)`, if the result has `__dataclass_fields__` (i.e., is a dataclass instance), return `dataclasses.asdict(result)`; else return the result unchanged.
- `describe_schema`: after calling `self._driver.describe_schema(table, schema)`, walk the top-level dict; for each value that is a list of dataclass instances, replace with list of `dataclasses.asdict(x)`; for each value that is itself a dataclass, asdict it. Top-level string/None keys pass through. Add `import dataclasses` at module top.
- No changes to `connect`, `execute`, `close`, `supports_describe_schema`, `load_drivers`.

- [ ] **Step 4: Run DbClient tests to verify serialization passes**

Run: `pytest tests/unit/test_db_client.py -v`
Expected: PASS (all existing + new serialization tests).

- [ ] **Step 5: Update PostgreSQL driver to inherit `BaseDBDriver` + return DTOs**

Modify `agctl/clients/db_drivers/postgresql.py`:
- Class declaration: `class PostgreSQLDriver(BaseDBDriver):` (import `BaseDBDriver` from `..db_driver_protocol`).
- Delete the module-level `_SECRET_KEY_PATTERN`, `_URL_USERINFO_PATTERN`, `_REDACTED_SENTINEL`, and `_redact_config` function. All call sites that referenced `_redact_config(config)` become `self._redact_config(config)`.
- In `connect()`: replace the `try: import psycopg / except ImportError as exc: raise ConfigError(...)` block with `psycopg = self._lazy_import_or_raise("psycopg", "db")`.
- In `execute()`: replace the inline `import psycopg` with using the module already imported in `connect()` (or re-call `self._lazy_import_or_raise` — the cache makes it free).
- In `execute_write()`: change the return statement from `return {"rows_affected": rows_affected, "returning": returning}` to `return WriteResult(rows_affected=rows_affected, returning=returning)` (import `WriteResult`).
- In `describe_schema()`: the Level-1 branch's `items.append({...})` becomes `items.append(SchemaItem(schema=schema_name, name=..., kind=kind, column_count=...))`. The Level-2 branch's `matches.append(self._describe_one_relation(...))` — `_describe_one_relation` now returns a `SchemaMatch` instance with `columns=[ColumnInfo(...)]`, `foreign_keys=[ForeignKey(...)]`, `unique_constraints=[UniqueConstraint(...)]`. The `return {"items": [], "matches": matches}` shape is unchanged (still a dict); only the contents of `matches` change from dicts to DTO instances.
- In `_describe_one_relation`: build DTOs instead of dicts. The column-building loop constructs `ColumnInfo(name=..., data_type=..., nullable=..., default=..., generated=..., enum_values=..., comment=...)`. The FK loop constructs `ForeignKey(name=..., columns=..., references_schema=..., references_table=..., references_columns=...)`. The unique loop constructs `UniqueConstraint(name=..., columns=...)`. The return is `SchemaMatch(schema=..., table=..., kind=..., comment=..., columns=columns, primary_key=primary_key, foreign_keys=foreign_keys, unique_constraints=unique_constraints)`.

- [ ] **Step 6: Run PostgreSQL driver tests**

Run: `pytest tests/unit/test_postgresql_driver.py -v`
Expected: most tests PASS unchanged. A small number may FAIL if they asserted `driver.execute_write(...) == {"rows_affected": ..., "returning": ...}` (the equality now fails because the return is a `WriteResult` instance, not a dict). Fix those by changing the assertion to either compare against the `WriteResult(...)` instance, or to test through `DbClient.execute_write(...)` (which serializes). Apply the latter where possible (move the dict-equality assertion to the DbClient boundary). Same for any `describe_schema` dict-shape assertions at the driver level.

- [ ] **Step 7: Run full test suite to verify no regressions**

Run: `pytest tests/unit -v`
Expected: PASS (all green). Any remaining failures must be DTO-vs-dict assertion mismatches; fix per Step 6 guidance.

- [ ] **Step 8: Commit**

Run: `git add agctl/clients/db_drivers/postgresql.py agctl/clients/db_client.py tests/unit/test_postgresql_driver.py tests/unit/test_db_client.py`
Run: `git commit -m "refactor(db): PostgreSQLDriver inherits BaseDBDriver, returns DTOs; DbClient serializes at boundary"`

---

## Task 4: SQLite driver skeleton (`connect`/`execute`/`execute_write`/`close`)

**Files:**
- Create: `agctl/clients/db_drivers/sqlite.py`
- Test: `tests/unit/test_sqlite_driver.py` (NEW)

**Interfaces:**
- Consumes: `BaseDBDriver`, `WriteResult` from Task 1+2; `convert_sql_params` is NOT used (SQLite accepts `:name` natively). `coerce_db_value` from `agctl.assertions`. `ConnectionFailure`, `ConfigError` from `agctl.errors`.
- Produces: `SQLiteDriver` class.
  - `__init__(self, *, connectable=None)` — optional pre-built `sqlite3.Connection` for test injection. Sets `self._conn = connectable`, `self._owned = (connectable is None)`.
  - `connect(self, config: dict) -> None` — if `self._conn is not None`: return (injected). Else: read `url = config.get("url") or ":memory:"`. Detect URI mode: `uri_mode = isinstance(url, str) and url.startswith("file:")`. Call `sqlite3.connect(url, uri=uri_mode, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)`. Wrap `sqlite3.Error` → `ConnectionFailure(message=str(exc), detail={"driver": "sqlite", "config": self._redact_config(config)})`.
  - `execute(self, sql: str, params: dict) -> list[dict]` — open cursor, `cur.execute(sql, params)` (NO rewrite — pass `sql` unchanged), build dict rows from `cur.description` + `cur.fetchall()` via `coerce_db_value`, close cursor, return list. Wrap `sqlite3.Error` → `ConnectionFailure`. No commit.
  - `execute_write(self, sql: str, params: dict) -> WriteResult` — open cursor, `cur.execute(sql, params)` (NO rewrite), materialize `cur.rowcount` (None if -1), materialize `cur.fetchall()` if `cur.description` is not None (SQLite ≥3.35 `RETURNING` support) into dict rows via `coerce_db_value`, `self._conn.commit()` LAST, rollback on ANY exception (re-raise as `ConnectionFailure`), close cursor in `finally`. Return `WriteResult(rows_affected=rows_affected, returning=returning)`.
  - `close(self) -> None` — if `self._owned and self._conn is not None`: `self._conn.close()`.

- [ ] **Step 1: Write failing tests for SQLite skeleton**

Test file: `tests/unit/test_sqlite_driver.py`. Use REAL in-memory `sqlite3` connections (no fakes — stdlib is trivially instantiable). Verify:
1. `SQLiteDriver(connectable=sqlite3.connect(":memory:"))` — calling `.execute("CREATE TABLE t (id INTEGER)", {})` then `.execute("INSERT INTO t VALUES (1)", {})` then `.execute("SELECT id FROM t", {})` returns `[{"id": 1}]` (integer preserved, dict-shaped).
2. `SQLiteDriver(connectable=...)` against a table with `CREATE TABLE orders (id TEXT, status TEXT, total_cents INTEGER)` and one row `("o9", "CONFIRMED", 1500)` — `.execute("SELECT * FROM orders WHERE id = :orderId", {"orderId": "o9"})` returns `[{"id": "o9", "status": "CONFIRMED", "total_cents": 1500}]`. The `:orderId` placeholder is passed through unchanged (no `convert_sql_params` rewrite).
3. `execute_write` against `CREATE TABLE t (id INTEGER)` — `.execute_write("INSERT INTO t VALUES (:v)", {"v": 42})` returns `WriteResult(rows_affected=1, returning=[])`.
4. `execute_write` with SQLite ≥3.35 `RETURNING` — create table, `.execute_write("INSERT INTO t VALUES (1) RETURNING *", {})` returns `WriteResult(rows_affected=1, returning=[{"id": 1}])`. (Guard with `pytest.mark.skipif(sqlite3.sqlite_version_info < (3, 35, 0))`.)
5. `execute_write` rollback on error — create table with `NOT NULL` column, `.execute_write("INSERT INTO t (id) VALUES (NULL)", {})` raises `ConnectionFailure`; subsequent `.execute("SELECT COUNT(*) AS c FROM t", {})` returns `[{"c": 0}]` (rollback worked, no partial insert). Verify `conn.in_transaction` is truthy before the failed write and the rollback restored a clean state.
6. `connect({"url": ":memory:"})` on a fresh `SQLiteDriver()` (no connectable) — succeeds; `.execute("SELECT 1 AS x", {})` returns `[{"x": 1}]`.
7. `connect({"url": "file::memory:?cache=shared", ...})` — URI mode; `.execute(...)` works.
8. `connect({"url": "/nonexistent/path/db.sqlite"})` — raises `ConnectionFailure` with `detail["driver"] == "sqlite"` and the path NOT redacted (no secret keys in SQLite config).
9. `close()` on an owned connection (`SQLiteDriver()` then `.connect({"url": ":memory:"})`) — subsequent `.execute(...)` raises (connection closed). `close()` on an injected connection does NOT close it (verify by executing on the original `connectable` after).
10. `SQLiteDriver()` satisfies the `DBDriver` Protocol — `isinstance(SQLiteDriver(), DBDriver)` returns `True` (runtime-checkable Protocol).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sqlite_driver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agctl.clients.db_drivers.sqlite'`.

- [ ] **Step 3: Implement `SQLiteDriver`**

Create `agctl/clients/db_drivers/sqlite.py`. Import `sqlite3` at module top (stdlib). Import `BaseDBDriver`, `WriteResult` from `..db_driver_protocol`; `coerce_db_value` from `...assertions`; `ConnectionFailure`, `ConfigError` from `...errors`. Class `SQLiteDriver(BaseDBDriver)` per the Produces contract. No `convert_sql_params` import or call. No lazy-import helper (sqlite3 is stdlib).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sqlite_driver.py -v`
Expected: PASS (all 10 tests green, modulo the SQLite-version skip).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_drivers/sqlite.py tests/unit/test_sqlite_driver.py`
Run: `git commit -m "feat(db): SQLiteDriver skeleton (connect/execute/execute_write/close)"`

---

## Task 5: SQLite `describe_schema` (Level-1 + Level-2)

**Files:**
- Modify: `agctl/clients/db_drivers/sqlite.py`
- Test: `tests/unit/test_sqlite_driver.py` (extend)

**Interfaces:**
- Consumes: `SchemaItem`, `SchemaMatch`, `ColumnInfo`, `ForeignKey`, `UniqueConstraint` from Task 1.
- Produces: `SQLiteDriver.describe_schema(self, table: str | None, schema: str | None) -> dict` method.
  - Returns `{"items": [SchemaItem], "matches": []}` when `table is None` (Level-1).
  - Returns `{"items": [], "matches": [SchemaMatch]}` when `table is not None` (Level-2).
  - `schema` parameter is accepted but ignored for v1 (always `"main"`); a non-None `schema` value other than `"main"` is silently accepted and the result still uses `"main"` (v1 limitation; documented in docstring).
  - Identifier validation: a module-level helper `_validate_identifier(name: str) -> None` raises `ConnectionFailure` if `name` doesn't match `^[a-zA-Z_][a-zA-Z0-9_]*$`. Called before interpolating `table` into any PRAGMA.

- [ ] **Step 1: Write failing tests for `describe_schema`**

Extend `tests/unit/test_sqlite_driver.py`. Use in-memory DBs seeded with `CREATE TABLE` / `CREATE VIEW` / `CREATE TABLE ... FOREIGN KEY` / `CREATE UNIQUE INDEX`. Verify:
1. **Level-1 happy path:** create `users` (table, 2 cols) + `orders` (table, 3 cols) + `active_users` (view). `driver.describe_schema(table=None, schema=None)` returns `{"items": [SchemaItem(schema="main", name="active_users", kind="view", column_count=<n>), SchemaItem(schema="main", name="orders", kind="table", column_count=3), SchemaItem(schema="main", name="users", kind="table", column_count=2)], "matches": []}`. Items sorted by name ascending. Internal `sqlite_*` tables (auto-created indexes) do NOT appear.
2. **Level-1 empty:** fresh in-memory DB, no user tables — returns `{"items": [], "matches": []}`.
3. **Level-2 happy path:** create `CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL DEFAULT 5, status TEXT, FOREIGN KEY(customer_id) REFERENCES users(id))` plus `CREATE TABLE users(id INTEGER PRIMARY KEY)` plus `CREATE UNIQUE INDEX idx_orders_status ON orders(status)`. `driver.describe_schema(table="orders", schema=None)` returns `{"items": [], "matches": [SchemaMatch(schema="main", table="orders", kind="table", comment=None, columns=[ColumnInfo(name="id", data_type="INTEGER", nullable=True, default=None, generated=None, enum_values=None, comment=None), ColumnInfo(name="customer_id", data_type="INTEGER", nullable=False, default="5", ...), ColumnInfo(name="status", data_type="TEXT", nullable=True, default=None, ...)], primary_key=["id"], foreign_keys=[ForeignKey(name=None, columns=["customer_id"], references_schema=None, references_table="users", references_columns=["id"])], unique_constraints=[UniqueConstraint(name="idx_orders_status", columns=["status"])])]}`. (The `name=None` on FK is correct — SQLite FKs are anonymous in `foreign_key_list` output.)
4. **Level-2 not found:** `driver.describe_schema(table="nonexistent", schema=None)` returns `{"items": [], "matches": []}`.
5. **Level-2 view:** create a view; `describe_schema(table="my_view", schema=None)` returns a `SchemaMatch` with `kind="view"`, `primary_key=[]`, `foreign_keys=[]`, `unique_constraints=[]`.
6. **Identifier injection rejection:** `driver.describe_schema(table="t; DROP TABLE users; --", schema=None)` raises `ConnectionFailure` (regex validation refuses the identifier).
7. **Identifier with quote rejection:** `driver.describe_schema(table='t"', schema=None)` raises `ConnectionFailure`.
8. **Enum values always None:** create any table; `describe_schema` columns all have `enum_values=None`.
9. **Comment always None:** same — `comment=None` on columns and on the SchemaMatch.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_sqlite_driver.py -v -k "describe_schema or identifier"`
Expected: FAIL with `AttributeError: 'SQLiteDriver' object has no attribute 'describe_schema'`.

- [ ] **Step 3: Implement `describe_schema` + `_validate_identifier`**

Add to `agctl/clients/db_drivers/sqlite.py`:
- Module-level `_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")` and `_validate_identifier(name)` helper that raises `ConnectionFailure(message=f"Invalid identifier: {name!r}", detail={"identifier": name})` on non-match.
- `describe_schema(self, table, schema)` method per the Produces contract. **Level-1** (table is None): `cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name")`. For each row, count columns via `PRAGMA table_info(<name>)` (validate identifier first). Build `SchemaItem(schema="main", name=..., kind=("table" if type=="table" else "view"), column_count=count)`. **Level-2** (table not None): validate identifier; `cur.execute("SELECT name, type FROM sqlite_master WHERE name = ? AND type IN ('table', 'view')", (table,))`. For each matching row, read columns via `PRAGMA table_xinfo(<name>)` (validate identifier first), PK via `PRAGMA table_info`, FKs via `PRAGMA foreign_key_list`, unique constraints via `PRAGMA index_list` (filter `origin=="u"`) + `PRAGMA index_info(<idx>)` per index. Map `hidden` value from `table_xinfo`: `hidden in (2, 3)` → `generated="stored"`; else `None`. Build `SchemaMatch` with the gathered DTOs. Wrap `sqlite3.Error` → `ConnectionFailure`.
- A private helper `_describe_one_relation(self, cur, table_name, kind)` to keep the Level-2 loop readable; returns a `SchemaMatch`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_sqlite_driver.py -v`
Expected: PASS (all skeleton + describe_schema tests green).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_drivers/sqlite.py tests/unit/test_sqlite_driver.py`
Run: `git commit -m "feat(db): SQLiteDriver.describe_schema via sqlite_master + PRAGMAs"`

---

## Task 6: MySQL driver skeleton + `DatabaseConnection.options` field

**Files:**
- Create: `agctl/clients/db_drivers/mysql.py`
- Modify: `agctl/config/models.py` (add `options` field)
- Test: `tests/unit/test_mysql_driver.py` (NEW)
- Test: `tests/unit/test_config_models.py` (extend)

**Interfaces:**
- Consumes: `BaseDBDriver`, `WriteResult` from Task 1+2; `convert_sql_params` from `agctl.resolution`; `coerce_db_value` from `agctl.assertions`; `ConfigError`, `ConnectionFailure` from `agctl.errors`.
- Produces:
  - `DatabaseConnection` model gains: `options: dict[str, Any] = Field(default_factory=dict)`. Existing fields unchanged. The model docstring is updated to document the three built-in driver types (`postgresql`, `mysql`, `sqlite`) and the `options` field's purpose.
  - `MySQLDriver(BaseDBDriver)` class.
    - `__init__(self, *, connectable=None)` — optional pre-built PyMySQL connection for test injection. Sets `self._conn = connectable`, `self._owned = (connectable is None)`.
    - `connect(self, config: dict) -> None` — if injected: return. Else: `pymysql = self._lazy_import_or_raise("pymysql", "mysql")`. Build kwargs: `host`, `port`, `database=config.get("dbname")`, `user`, `password` from the corresponding config keys (skip Nones). If `url` present (non-empty string): parse via `urllib.parse.urlparse`, extract `hostname`/`port`/`username`/`password`/`path.lstrip("/")` and merge into kwargs (discrete fields still win when both present). Merge `config.get("options", {})` into kwargs (PyMySQL accepts `charset`, `collation`, `connect_timeout`, etc.). Call `pymysql.connect(autocommit=False, **kwargs)`. Wrap `pymysql.Error` → `ConnectionFailure(message=str(exc), detail={"driver": "mysql", "config": self._redact_config(config)})`.
    - `execute(self, sql, params) -> list[dict]` — `rewrite = convert_sql_params(sql)`, open cursor, `cur.execute(rewrite, params)`, build dict rows from `cur.description` + `cur.fetchall()` via `coerce_db_value`, close cursor, return. Wrap `pymysql.Error` → `ConnectionFailure`. No commit.
    - `execute_write(self, sql, params) -> WriteResult` — `rewrite = convert_sql_params(sql)`, open cursor, `cur.execute(rewrite, params)`, materialize `cur.rowcount` (None if -1), `cur.description` is None for non-SELECT so `returning=[]` always (MySQL has no `RETURNING`), `self._conn.commit()` LAST, rollback on ANY exception, close cursor in finally. Return `WriteResult(rows_affected=rows_affected, returning=[])`.
    - `close(self) -> None` — if `self._owned and self._conn is not None`: `self._conn.close()`.

- [ ] **Step 1: Write failing tests for `DatabaseConnection.options`**

Extend `tests/unit/test_config_models.py`. Verify:
1. `DatabaseConnection(type="mysql", host="h")` has `options == {}` (default factory).
2. `DatabaseConnection(type="mysql", host="h", options={"charset": "utf8mb4", "connect_timeout": 10})` round-trips via `model_dump()` to include `"options": {"charset": "utf8mb4", "connect_timeout": 10}`.
3. `DatabaseConnection(type="sqlite", url=":memory:")` has `options == {}` (default still applies).
4. Pydantic rejects a non-dict `options` value (e.g., `options="not a dict"`) with a validation error.

- [ ] **Step 2: Run config model tests to verify they fail**

Run: `pytest tests/unit/test_config_models.py -v -k "options"`
Expected: FAIL (`options` field doesn't exist yet).

- [ ] **Step 3: Add `options` field to `DatabaseConnection`**

Modify `agctl/config/models.py`. Add `from typing import Any` if not already imported (check existing imports first). Add `options: dict[str, Any] = Field(default_factory=dict)` as the last field of `DatabaseConnection`. Update the class docstring to document built-in driver types (`postgresql`, `mysql`, `sqlite`) and the `options` field's role (driver-specific extras; default empty; recognized keys vary by driver).

- [ ] **Step 4: Run config model tests to verify they pass**

Run: `pytest tests/unit/test_config_models.py -v`
Expected: PASS.

- [ ] **Step 5: Write failing tests for MySQL driver skeleton**

Test file: `tests/unit/test_mysql_driver.py`. Mirror the FakeCursor/FakeConn pattern from `tests/unit/test_postgresql_driver.py`. Verify:
1. **Param translation:** `FakeCursor` records the SQL passed to `execute`. `MySQLDriver(connectable=FakeConn(cur)).execute("SELECT id FROM t WHERE id = :orderId", {"orderId": "o9"})` — the FakeCursor's last_sql equals `"SELECT id FROM t WHERE id = %(orderId)s"` (convert_sql_params rewrote it); last_params equals `{"orderId": "o9"}`.
2. **Read returns dict rows:** FakeCursor returns `description=[col("id"), col("status")]` and `rows=[("o9", "CONFIRMED")]`. Driver's `execute(...)` returns `[{"id": "o9", "status": "CONFIRMED"}]`.
3. **`execute_write` returns WriteResult:** FakeCursor with `rowcount=2`, `description=None`. Driver's `execute_write(...)` returns `WriteResult(rows_affected=2, returning=[])`. (Assert on the dataclass instance, not a dict.)
4. **`execute_write` commits:** FakeConn records `commit_called`. After `execute_write(...)`, `commit_called == True`.
5. **`execute_write` rollback on error:** FakeCursor raises `pymysql.Error` (use a stub Exception subclass) inside execute. `execute_write(...)` raises `ConnectionFailure`. FakeConn's `rollback_called == True`. `commit_called == False`.
6. **Connect kwargs construction (discrete fields):** inject a FakePyMySQL module (or monkeypatch `pymysql.connect`) that records kwargs. `MySQLDriver().connect({"type": "mysql", "host": "h", "port": 3307, "dbname": "testdb", "user": "u", "password": "p"})` — recorded kwargs include `host="h"`, `port=3307`, `database="testdb"`, `user="u"`, `password="p"`, `autocommit=False`.
7. **Connect URL parsing:** `MySQLDriver().connect({"type": "mysql", "url": "mysql://u:pass@h:3306/dbname?charset=utf8mb4"})` — recorded kwargs include `host="h"`, `port=3306`, `user="u"`, `password="pass"`, `database="dbname"`. The query-string `charset=utf8mb4` is NOT auto-extracted (out of scope; documented).
8. **Connect options merge:** `MySQLDriver().connect({"type": "mysql", "host": "h", "options": {"charset": "utf8mb4", "connect_timeout": 5}})` — recorded kwargs include `charset="utf8mb4"`, `connect_timeout=5`.
9. **Discrete fields override URL:** `MySQLDriver().connect({"type": "mysql", "url": "mysql://u:p@h:3306/db", "port": 3307})` — recorded kwargs have `port=3307` (discrete wins).
10. **Connect lazy-import error:** monkeypatch `sys.modules` to make `pymysql` unimportable. `MySQLDriver().connect({"type": "mysql", "host": "h"})` raises `ConfigError` with message containing `"pip install 'agctl[mysql]'"`.
11. **Connect failure includes redacted config:** FakePyMySQL.connect raises `pymysql.Error("access denied")`. `MySQLDriver().connect({"type": "mysql", "host": "h", "user": "u", "password": "secret"})` raises `ConnectionFailure` whose `detail["config"]["password"] == "***"` and `detail["driver"] == "mysql"`.
12. **DBDriver Protocol conformance:** `isinstance(MySQLDriver(), DBDriver) == True`.

- [ ] **Step 6: Run MySQL tests to verify they fail**

Run: `pytest tests/unit/test_mysql_driver.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agctl.clients.db_drivers.mysql'`.

- [ ] **Step 7: Implement `MySQLDriver`**

Create `agctl/clients/db_drivers/mysql.py`. Do NOT import `pymysql` at module top — only inside `connect()` via `self._lazy_import_or_raise`. Import `urllib.parse` at top. Import `BaseDBDriver`, `WriteResult` from `..db_driver_protocol`; `convert_sql_params` from `...resolution`; `coerce_db_value` from `...assertions`; `ConfigError`, `ConnectionFailure` from `...errors`. Class `MySQLDriver(BaseDBDriver)` per the Produces contract. For the URL-parsing path, use `urllib.parse.urlparse(url)`; extract components, treating empty/None as absent. Merge order: URL values first, then discrete fields override, then `options` merged last (so users can force-override anything via options).

- [ ] **Step 8: Run MySQL tests to verify they pass**

Run: `pytest tests/unit/test_mysql_driver.py -v`
Expected: PASS (all 12 tests green).

- [ ] **Step 9: Commit**

Run: `git add agctl/clients/db_drivers/mysql.py agctl/config/models.py tests/unit/test_mysql_driver.py tests/unit/test_config_models.py`
Run: `git commit -m "feat(db): MySQLDriver skeleton (PyMySQL) + DatabaseConnection.options field"`

---

## Task 7: MySQL `describe_schema` (Level-1 + Level-2)

**Files:**
- Modify: `agctl/clients/db_drivers/mysql.py`
- Test: `tests/unit/test_mysql_driver.py` (extend)

**Interfaces:**
- Consumes: `SchemaItem`, `SchemaMatch`, `ColumnInfo`, `ForeignKey`, `UniqueConstraint` from Task 1.
- Produces: `MySQLDriver.describe_schema(self, table: str | None, schema: str | None) -> dict` method.
  - Returns `{"items": [SchemaItem], "matches": []}` (Level-1) or `{"items": [], "matches": [SchemaMatch]}` (Level-2). Same shape contract as SQLite/PostgreSQL.
  - Level-1 excludes system schemas: `mysql`, `performance_schema`, `information_schema`, `sys`.
  - Level-2 returns one match per relation named `table` (exact stored case) in any non-system schema; restricted to `schema` when given. Does NOT disambiguate (command layer raises on ambiguity, same as PostgreSQL).
  - Enum values parsed from MySQL `column_type` string when `data_type == "enum"`. Format: `enum('a','b','c')`. Parser handles the common cases; on parse failure returns `None` (does not crash).
  - FK rows preserve positional `columns` ↔ `references_columns` pairing via `information_schema.key_column_usage.ordinal_position`.

- [ ] **Step 1: Write failing tests for MySQL `describe_schema`**

Extend `tests/unit/test_mysql_driver.py`. Use FakeCursor/FakeConn seams that return canned `information_schema` rows (lists of tuples matching the column order of each query). Verify:
1. **Level-1 happy path:** FakeCursor returns rows for `information_schema.tables` query: `[("public", "users", "BASE TABLE"), ("public", "active_users", "VIEW"), ("mysql", "user", "BASE TABLE"), ("public", "orders", "BASE TABLE")]`. Plus a column-count subquery returning `[(2,), (3,), (1,)]` aligned by table. `driver.describe_schema(table=None, schema=None)` returns `{"items": [SchemaItem(schema="public", name="active_users", kind="view", column_count=1), SchemaItem(schema="public", name="orders", kind="table", column_count=3), SchemaItem(schema="public", name="users", kind="table", column_count=2)], "matches": []}`. The `mysql.user` system-schema row is excluded. Items sorted by `(schema, name)`.
2. **Level-1 schema filter:** same setup, `driver.describe_schema(table=None, schema="public")` returns the same 3 items; calling with `schema="mysql"` returns `{"items": [], "matches": []}`.
3. **Level-2 happy path:** FakeCursor returns one table match, columns (one `id INT NOT NULL AUTO_INCREMENT PRIMARY KEY`, one `status ENUM('new','old')`), one FK, one unique constraint. Assert the returned `SchemaMatch` has: `columns[0] == ColumnInfo(name="id", data_type="int", nullable=False, default=None, generated=None, enum_values=None, comment=...)` (auto_increment handled per the mapping below); `columns[1] == ColumnInfo(name="status", data_type="enum", nullable=True, ..., enum_values=["new", "old"], ...)`; `primary_key == ["id"]`; `foreign_keys == [ForeignKey(name="fk_orders_cust", columns=["customer_id"], references_schema="public", references_table="users", references_columns=["id"])]`; `unique_constraints == [UniqueConstraint(name="uniq_status", columns=["status"])]`.
4. **Enum literal parsing:** assert `_parse_mysql_enum("enum('new','old','paid')")` returns `["new", "old", "paid"]`; `_parse_mysql_enum("enum('a,b','c')")` returns `["a,b", "c"]` (embedded comma handled); `_parse_mysql_enum("int")` returns `None` (not an enum).
5. **Auto_increment mapping:** `information_schema.columns.extra == "auto_increment"` for a column maps to `generated="by_default_identity"` (MySQL's `auto_increment` is closest to PostgreSQL's `by_default_identity` — user can supply a value or let it auto-generate).
6. **Level-2 not found:** FakeCursor returns no table matches — `describe_schema(table="nonexistent", schema=None)` returns `{"matches": []}`.
7. **Level-2 schema-filtered:** same setup, `schema="other"` — returns `{"matches": []}` (no match in that schema).
8. **`pymysql.Error` surfaces as ConnectionFailure:** FakeCursor raises during the catalog SELECT — `describe_schema(...)` raises `ConnectionFailure`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_mysql_driver.py -v -k "describe_schema or enum"`
Expected: FAIL with `AttributeError: 'MySQLDriver' object has no attribute 'describe_schema'`.

- [ ] **Step 3: Implement `describe_schema` + enum parser**

Add to `agctl/clients/db_drivers/mysql.py`:
- Module-level `_parse_mysql_enum(column_type: str) -> list[str] | None` — returns the list of enum literals parsed from a string like `enum('a','b','c')`, or `None` if the input doesn't start with `enum(`. Handles embedded commas by respecting single-quote boundaries. On any parse anomaly returns `None` (never raises).
- `describe_schema(self, table, schema)` method per the Produces contract. **Level-1**: query `information_schema.tables` selecting `table_schema, table_name, table_type`; filter out system schemas (`mysql`, `performance_schema`, `information_schema`, `sys`) either in the WHERE clause or in Python post-filter. Build a column count per table via a separate `information_schema.columns` count query (or a GROUP BY). Build `SchemaItem(schema=..., name=..., kind=("table" if table_type=="BASE TABLE" else "view"), column_count=count)`. Sort by `(schema, name)`. **Level-2**: query `information_schema.tables` restricted by `table_name = %s` (and `table_schema = %s` when given); for each match, query `information_schema.columns` ordered by `ordinal_position` → `ColumnInfo` instances (parse enum via `_parse_mysql_enum`, map `extra="auto_increment"` → `generated="by_default_identity"`); query `information_schema.key_column_usage` + `table_constraints` for PK and unique columns; query `information_schema.referential_constraints` + `key_column_usage` for FKs with positional pairing preserved. Build `SchemaMatch`. Wrap `pymysql.Error` → `ConnectionFailure`. Use `cur.execute(sql, params)` with `%s` placeholders (PyMySQL's native paramstyle for the catalog queries — NOT `convert_sql_params`, which is only for user SQL).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_mysql_driver.py -v`
Expected: PASS (all skeleton + describe_schema tests green).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_drivers/mysql.py tests/unit/test_mysql_driver.py`
Run: `git commit -m "feat(db): MySQLDriver.describe_schema via information_schema"`

---

## Task 8: Packaging — `pyproject.toml` + `BUILTIN_DRIVERS` + import-order test

**Files:**
- Modify: `pyproject.toml`
- Modify: `agctl/clients/db_client.py`
- Modify: `tests/unit/test_packaging.py` (extend)

**Interfaces:**
- Consumes: `MySQLDriver` from Task 6, `SQLiteDriver` from Task 4.
- Produces:
  - `pyproject.toml` gains a `[mysql]` optional-dependency extra equal to `["PyMySQL>=1.1", "jq>=1.6"]`. No `[sqlite]` extra (stdlib). The `[project.entry-points."agctl.db_drivers"]` group gains `mysql = "agctl.clients.db_drivers.mysql:MySQLDriver"` and `sqlite = "agctl.clients.db_drivers.sqlite:SQLiteDriver"` (postgresql line unchanged).
  - `db_client.py`'s `BUILTIN_DRIVERS` dict gains `"mysql": MySQLDriver` and `"sqlite": SQLiteDriver`. The `from .db_drivers.postgresql import PostgreSQLDriver` import line gains sibling imports for the two new drivers.

- [ ] **Step 1: Write failing tests for packaging changes**

Extend `tests/unit/test_packaging.py`. Verify:
1. `pyproject.toml`'s `[project.optional-dependencies]` has `"mysql"` equal to `["PyMySQL>=1.1", "jq>=1.6"]`.
2. `pyproject.toml` does NOT have a `"sqlite"` key in optional-dependencies (stdlib — no extra needed).
3. `[project.entry-points."agctl.db_drivers"]` contains exactly three entries: `postgresql`, `mysql`, `sqlite` with the correct module paths.
4. **Import-order invariant:** `import agctl.clients.db_client` succeeds even when `pymysql` and `psycopg` are both absent. Verify by inspecting `sys.modules` after import — neither `pymysql` nor `psycopg` should appear as keys. (Use `monkeypatch.setitem(sys.modules, "pymysql", None)` and same for `psycopg` to simulate absence; if import crashes, the test fails.)
5. `DbClient.load_drivers()` returns a dict with at least the three keys `"postgresql"`, `"mysql"`, `"sqlite"`, each mapping to the corresponding driver class.
6. (Update the existing `test_jq_extra_exists` assertion if it hard-codes the full `[db]` extra contents — only touch if it asserts something now-false. The `[db]` extra itself is unchanged.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_packaging.py -v`
Expected: FAIL on the new tests (`mysql` extra missing, entry-points missing, etc.).

- [ ] **Step 3: Update `pyproject.toml`**

Add `mysql = ["PyMySQL>=1.1", "jq>=1.6"]` to `[project.optional-dependencies]` (place it after the existing `db = ...` line for readability). Add the two new entry-point lines under `[project.entry-points."agctl.db_drivers"]`. Do NOT add a `sqlite` extra.

- [ ] **Step 4: Update `BUILTIN_DRIVERS` in `db_client.py`**

Modify `agctl/clients/db_client.py`:
- Add imports: `from .db_drivers.mysql import MySQLDriver` and `from .db_drivers.sqlite import SQLiteDriver` next to the existing `PostgreSQLDriver` import.
- Extend `BUILTIN_DRIVERS` dict literal with `"mysql": MySQLDriver, "sqlite": SQLiteDriver`.
- Verify the lazy-import invariant holds: `mysql.py` and `postgresql.py` must not import their heavy dep at module top (already enforced by Tasks 3, 6); `sqlite.py` imports `sqlite3` at top (stdlib, zero cost, safe).

- [ ] **Step 5: Run packaging tests to verify they pass**

Run: `pytest tests/unit/test_packaging.py -v`
Expected: PASS (all existing + new tests green).

- [ ] **Step 6: Run full unit suite to verify nothing regressed**

Run: `pytest tests/unit -v`
Expected: PASS. Spot-check that `test_db_client.py` dispatch tests still pass for all three driver types.

- [ ] **Step 7: Commit**

Run: `git add pyproject.toml agctl/clients/db_client.py tests/unit/test_packaging.py`
Run: `git commit -m "feat(db): package [mysql] extra + register sqlite/mysql entry points"`

---

## Task 9: Integration tests (testcontainers MySQL, in-memory SQLite)

**Files:**
- Modify: `tests/integration/test_db_commands.py` (extend)
- Modify: `tests/integration/conftest.py` (if fixtures need adding)

**Interfaces:**
- Consumes: all three drivers reachable via `DbClient` (Task 8); the full `db query` / `db assert` / `db execute` / `db schema` command surface.
- Produces: integration test coverage exercising each driver end-to-end through the CLI command layer (not just the driver in isolation). Catches dialect-specific surprises the unit tests with fakes can't (e.g., real MySQL enum column literal parsing, real SQLite PRAGMA behavior across sqlite versions).

- [ ] **Step 1: Write integration tests for SQLite (in-memory, no container)**

Extend `tests/integration/test_db_commands.py`. Use a tempfile-backed or `:memory:` SQLite database (file-backed is preferable for cross-process state if the CLI is invoked via Click's test runner; if using CliRunner, `:memory:` per-invocation works). Verify:
1. **Full cycle on SQLite:** config with a `sqlite` connection pointing at a tempfile DB; seed it via `db execute --write`; `db query` returns the seeded row; `db assert --expect-rows 1` passes; `db schema` (Level-1) lists the table; `db schema --table <name>` (Level-2) returns the column metadata.
2. **`db assert --expect-value` on SQLite:** seed a row, assert a cell value via `--path .status --equals CONFIRMED`.
3. **`db execute --write` write gate on SQLite:** calling `db execute` without `--write` on a writable connection fails with the existing `ConfigError`. Calling with `--write` on a `writable: false` connection fails. Both unchanged behavior; verifies the gates still work for the new driver.

- [ ] **Step 2: Write integration tests for MySQL (testcontainers)**

Extend `tests/integration/test_db_commands.py`. Use `testcontainers-python` (`from testcontainers.mysql import DbContainer` or the generic `DockerContainer` with the `mysql:8` image). Tests are guarded with `pytest.mark.integration` (or the existing project convention) and skip gracefully if Docker is unavailable. Verify:
1. **Full cycle on MySQL:** spin up MySQL container; config with a `mysql` connection pointing at the container; create a table with an `ENUM('new','old')` column + a FK; `db query` returns rows; `db assert --expect-rows` passes; `db schema` (Level-1) lists the table (no system schemas); `db schema --table <name>` returns columns including the parsed `enum_values=["new","old"]`.
2. **`db execute --write` on MySQL:** insert a row, verify `rows_affected == 1`, verify the row appears in a subsequent `db query`.
3. **`db execute` returning=[] on MySQL:** verify `execute_write` result has `returning == []` even when the SQL ends with a pseudo-RETURNING syntax (document the MySQL limitation; the SQL itself errors at MySQL, surfacing as `ConnectionFailure`).
4. **Auto_increment → generated mapping:** `db schema --table <auto_increment_table>` returns a `ColumnInfo` with `generated="by_default_identity"` for the auto_increment column (or `None` if the implementation chose that mapping — assert per Task 7's actual choice).

- [ ] **Step 3: Run integration tests**

Run: `pytest tests/integration/test_db_commands.py -v -m "integration or not integration"` (use the project's existing marker convention).
Expected: SQLite tests PASS unconditionally; MySQL tests PASS if Docker is available, SKIP cleanly otherwise. No failures when Docker IS available.

- [ ] **Step 4: Commit**

Run: `git add tests/integration/test_db_commands.py tests/integration/conftest.py`
Run: `git commit -m "test(db): integration coverage for MySQL (testcontainers) + SQLite (in-memory)"`

---

## Task 10: Docs sync via `docs-watcher` subagent

**Files:**
- Modify: `docs/DESIGN.md`
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: the completed implementation (Tasks 1-9). The spec's §10 documentation checklist.
- Produces: DESIGN.md and ARCHITECTURE.md sections updated to reflect the new drivers + the hardened protocol. No code changes.

- [ ] **Step 1: Dispatch `docs-watcher` subagent**

Launch a `docs-watcher` agent with the spec's §10 checklist as its prompt:
- **DESIGN.md §2.1** — add `mysql` and `sqlite` connection examples alongside the existing PostgreSQL examples; document the new `options` field.
- **DESIGN.md §9.1** — document the DTOs, the `BaseDBDriver` mixin, the optional-capability duck-typing contract.
- **ARCHITECTURE.md §3** — module map: add `mysql.py`, `sqlite.py`; note DTO additions to `db_driver_protocol.py`.
- **ARCHITECTURE.md §8** — DB client layer: per-driver SQL param handling (PG/MySQL translate `:name` → `%(name)s`; SQLite passes through native `:name`).
- **ARCHITECTURE.md §10** — extension points: contributor story (copy `sqlite.py` as the template).
- **ARCHITECTURE.md §14** — design-vs-implementation deltas: note the MySQL + SQLite drivers now exist (resolves the PostgreSQL-only gap previously listed here).

Each doc preserves its altitude: DESIGN.md = WHAT/WHY (user-facing contract), ARCHITECTURE.md = HOW (as-built implementation).

- [ ] **Step 2: Review the docs-watcher's changes**

Read the diffs to DESIGN.md and ARCHITECTURE.md. Verify:
1. No speculative additions (YAGNI).
2. DESIGN.md stays at intent/contract altitude (no implementation logic).
3. ARCHITECTURE.md stays at as-built altitude (module map, runtime flow).
4. The PostgreSQL-only gap language in ARCHITECTURE.md §14 (if present) is updated to reflect the new state.

- [ ] **Step 3: Commit the docs changes**

Run: `git add docs/DESIGN.md docs/ARCHITECTURE.md`
Run: `git commit -m "docs: sync DESIGN.md + ARCHITECTURE.md for MySQL + SQLite drivers"`

- [ ] **Step 4: Move spec + plan to archive (optional, defer to finishing-a-development-branch)**

Per the spec lifecycle, the spec and plan move to `specs/archive/` and `plans/archive/` on release (merge into the base branch), handled by `superpowers:finishing-a-development-branch`. Do NOT move them here unless the user explicitly asks.

---

## Self-Review (run after writing the plan)

**1. Code scan:** No implementation logic, method bodies, algorithms, or copy-paste-ready code. Each step describes behavior + expected results, not the code itself. DTO field listings and `DatabaseConnection` model additions are config contracts (allowed). PRAGMA/SQL query references describe what's queried, not the SQL string.

**2. Self-containment scan:** Every task's Interfaces block spells out Consumes (with exact signatures/types from earlier tasks) and Produces (with exact signatures, types, data shapes, validation rules, error cases). A zero-context implementer can write each task from its own content + the Produces blocks of tasks it Consumes.

**3. Spec coverage:**
- §5.1 file structure → Tasks 4 (sqlite.py), 6 (mysql.py); §5.1 notes db_driver_protocol.py + db_client.py changes via Tasks 1-3, 8.
- §5.2 DTOs + BaseDBDriver → Tasks 1, 2.
- §5.3 PostgreSQL refactor → Task 3.
- §5.4 contributor story → Task 10 (docs).
- §6 MySQL driver → Tasks 6, 7.
- §7 SQLite driver → Tasks 4, 5.
- §8.1 DatabaseConnection.options → Task 6.
- §8.2 validator (no changes) → no task needed (covered by "no-op" assertion).
- §8.3 pyproject.toml → Task 8.
- §8.4 BUILTIN_DRIVERS → Task 8.
- §9 testing strategy → Tasks 1-8 (unit), Task 9 (integration).
- §10 documentation → Task 10.
- §11 open questions → no tasks (deferred by design).
- §12 risks → mitigations folded into the relevant tasks (identifier regex in Task 5; enum parsing in Task 7; autocommit=False in Task 6; import-order invariant in Task 8).

No spec gaps.

**4. Placeholder scan:** No "TBD", "TODO", "implement later", "add appropriate error handling" patterns. Each test step names the scenario + expected result; each implementation step spells out the behavior.

**5. Type consistency:**
- `WriteResult` field names consistent across Tasks 1, 3, 4, 6.
- `SchemaItem` / `SchemaMatch` / `ColumnInfo` / `ForeignKey` / `UniqueConstraint` consistent across Tasks 1, 3, 5, 7.
- `BaseDBDriver._redact_config` / `_lazy_import_or_raise` consistent across Tasks 2, 3, 6.
- `SQLiteDriver` / `MySQLDriver` constructor signature `(self, *, connectable=None)` consistent across Tasks 4, 6, and reused in Task 8's `BUILTIN_DRIVERS`.
- `_validate_identifier` (Task 5) and `_parse_mysql_enum` (Task 7) are module-level helpers; both prefixed `_` to signal internal.
- `DatabaseConnection.options` field name consistent across Tasks 6, 8, 9.

No type drift detected.
