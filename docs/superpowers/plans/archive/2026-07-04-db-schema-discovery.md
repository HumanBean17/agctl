# Live DB Schema Discovery (`agctl db schema`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `agctl db schema`, a read-only, progressive, live schema-discovery command that lets an autonomous agent learn a configured connection's actual tables/columns/types/keys (and the column metadata that decides SQL validity: identity/generated, enum values, comments) so it can author valid SQL without shelling out to a DB client.

**Architecture:** One new `agctl db schema` command with flag-driven levels — no `--table` → Level 1 (list relations, tag `db.schema.tables`); `--table X` → Level 2 (one relation's columns+keys, tag `db.schema.table`). Each level is its own `@envelope`-wrapped `_core` (the same mechanism `discover` uses for its four tags), dispatched by a thin Click command. Connection resolution is shared with the SQL commands via a new helper extracted from `resolve_db_request` (spec D8, isolated behavior-preserving commit first). Introspection is an **optional** driver capability `describe_schema` (the `DBDriver` Protocol is unchanged — the same probe pattern as `execute_write`), probed **before** `connect` via a new `DbClient.supports_describe_schema` so a non-introspection driver fails fast without opening a connection. `PostgreSQLDriver.describe_schema` reads `pg_catalog` and normalizes into a documented, dialect-agnostic dict. The read/write SQL paths (`db query` / `assert` / `execute`, `DBDriver.execute` / `execute_write`) are untouched.

**Tech Stack:** Python ≥3.11, Click ≥8.1, Pydantic v2, psycopg3 (the `db` extra, lazy-imported), pytest. One-JSON-envelope-per-invocation via `@envelope` + `output.emit`.

**Spec:** `docs/superpowers/specs/active/2026-07-04-db-schema-discovery-design.md` (source of truth).

## Global Constraints

- `requires-python = ">=3.11"`; core deps `click>=8.1`, `pyyaml>=6.0`, `pydantic>=2.0`; the `db` extra is `psycopg[binary]>=3.1` + `jq>=1.6`.
- Every command writes exactly one JSON envelope via `output.emit()`; `_core` functions are wrapped by `@envelope(command)`. `emit()` is the only permitted stdout path.
- Exit codes: `0` success, `1` AssertionError, `2` ConfigError / ConnectionError / InternalError / TemplateNotFound.
- psycopg is **lazy-imported inside driver methods** (never at module import); unit tests run without psycopg installed.
- The **read/write SQL paths are unchanged**: `db query`, `db assert`, `db execute`, `DBDriver.execute`, `DBDriver.execute_write`, `PostgreSQLDriver.execute`, `PostgreSQLDriver.execute_write` stay byte-for-byte identical. No regression to existing tests.
- The `DBDriver` Protocol (`connect`/`execute`/`close`) is **not modified** — `describe_schema` is an optional capability, so out-of-tree read-only drivers keep working.
- `db schema` is **read-only and ungated**: it has no `--write`, no `--template`, no `--sql`, no `--param`, and ignores `writable`/`mode`. Any configured connection (read-only or writable) is eligible.
- No new error types. Existing types cover every path: `ConfigError` (exit 2) for resolution/capability/not-found/ambiguity; `ConnectionFailure` (exit 2) for DB/catalog errors.
- Schema/table filter values are **bind parameters** in the catalog queries, never string-interpolated (defense against the `--schema`/`--table` values, which originate from CLI flags).
- Conventional-commit messages (e.g. `feat(db): ...`); one commit per task.

---

### Task 1: Extract shared connection-resolution helper (spec D8)

**Files:**
- Modify: `agctl/commands/db_commands.py` (`resolve_db_request`)
- Test: `tests/unit/test_db_commands.py` (unchanged — existing suite is the regression guard)

**Interfaces:**
- Consumes: `ConfigError` (from `..errors`); the existing connection-resolution block at the tail of `resolve_db_request` (the lines computing `resolved_name` from `connection_name` → `template_connection` → `cfg.defaults.database_connection`, with the two `ConfigError` raises).
- Produces:
  - A module-level helper `resolve_connection_name(cfg, *, connection_name: str | None, template_connection: str | None = None) -> str` — precedence **explicit `connection_name` > `template_connection` > `cfg.defaults.database_connection`**; returns the resolved name; raises `ConfigError("No database connection specified", {})` when none of the three is set; raises `ConfigError(f"Unknown database connection: {resolved_name}", {"connection": resolved_name})` when the resolved name is absent from `cfg.database.connections`. Identical messages/detail to the current inline block so existing assertions still match.
  - `resolve_db_request` is rewritten to **call** `resolve_connection_name(cfg, connection_name=connection_name, template_connection=template_connection)` (after `template_connection` is computed) and otherwise keep returning `(sql_text, params, resolved_name)`. No behavior change.
- This is a **behavior-preserving refactor**. Per spec D8 it lands as an isolated commit with `tests/unit/test_db_commands.py` **unchanged and green**, before any feature code. No new tests are added in this task by design — the existing suite already covers no-connection / unknown-connection / template-connection / default-fallback resolution paths, and those assertions are the regression guard.

- [ ] **Step 1: Baseline — confirm the suite is green before touching anything**

Run: `pytest tests/unit/test_db_commands.py -q`
Expected: PASS (all existing query/assert/execute cases green — this is the baseline the extraction must not break).

- [ ] **Step 2: Extract the helper**

Add `resolve_connection_name` per Produces, with the exact precedence, return, and two error raises. Rewrite the tail of `resolve_db_request` to delegate to it (passing `template_connection` only when a template was resolved). Do not change any other function, any message text, or the `(sql_text, params, resolved_name)` return tuple.

- [ ] **Step 3: Re-run the suite to confirm behavior is preserved**

Run: `pytest tests/unit/test_db_commands.py -q && pytest tests/unit -q`
Expected: PASS — identical to baseline (the existing no-connection and unknown-connection assertions still hit the same `ConfigError` messages; the read/write paths still resolve the same connection names). No new tests, no changed tests.

- [ ] **Step 4: Commit**

Run: `git add agctl/commands/db_commands.py`
Run: `git commit -m "refactor(db): extract shared connection-resolution helper"`

---

### Task 2: `PostgreSQLDriver.describe_schema` — Level 1 (relations list)

**Files:**
- Modify: `agctl/clients/db_drivers/postgresql.py` (`PostgreSQLDriver`)
- Test: `tests/unit/test_postgresql_driver.py`

**Interfaces:**
- Consumes: the existing `connectable`/`_conn`/`_owned` fields and `close()` semantics; `psycopg` (lazy-imported). The read `execute()` and `execute_write()` are **not** modified.
- Produces:
  - `PostgreSQLDriver.describe_schema(self, table: str | None, schema: str | None) -> dict` — the portable, normalized contract. This task implements the **`table is None` (Level 1) branch** and returns:
    ```
    { "items": [ { schema: str, name: str, kind: "table" | "view", column_count: int } … ] }
    ```
    (Task 3 adds the `table is not None` → `matches` branch; the method signature and the `items` key are fixed here.)
  - Level-1 behavior contract (NOT query code — the implementer writes the SQL from these sources + rules):
    - **Sources:** relations from `pg_class` joined to `pg_namespace` (for the schema name); `relkind` (relation kind); `relispartition` (partition flag); a count of non-dropped attributes per relation from `pg_attribute` (where `attisdropped = false`) for `column_count`.
    - **Scope filters (spec D6):** exclude relations whose schema name starts with `pg_` or equals `information_schema`; exclude **partition leaf** relations (`relispartition = true`) — only partitioned parents and plain tables appear; when `schema` is not None, restrict to that one namespace. The `schema` value is a **bind parameter**, not interpolated.
    - **Kind mapping (spec D5/D6):** `relkind` ordinary/partitioned table (`'r'`, `'p'`) → `"table"`; view (`'v'`) → `"view"`; every other `relkind` (materialized view `'m'`, foreign table `'f'`, sequence `'S'`, index, TOAST) is **excluded** from v1.
    - **Ordering:** deterministic — sort by `schema` then `name` (ascending) so output is stable.
    - **Lazy import:** `import psycopg` inside the method (mirrors `execute`/`execute_write`); wrap execute/fetch so any `psycopg.Error` surfaces as `ConnectionFailure` (same error mapping as the read path).
    - No `commit()` is issued (read-only catalog `SELECT`s).
  - `close()` / `_owned` semantics are unchanged; an injected `connectable` is still not closed.
- **Test-infra change (load-bearing, spec §10):** the existing single-rowset `FakeCursor`/`FakeConn` doubles are **insufficient** — `describe_schema` issues distinct catalog `SELECT`s. Extend the test doubles with a cursor that **dispatches by SQL text**: a `CatalogFakeCursor` whose `execute(sql, params)` inspects `sql` and stages the canned `(description, rows)` for the matching query, and whose `fetchall()`/`description` return the currently staged values. `FakeConn.cursor()` already returns the same cursor object each call, so a driver that opens one-or-many cursors against it is covered by a single staged cursor. (This double is reused and extended in Task 3.)

- [ ] **Step 1: Write the failing tests**

Extend `tests/unit/test_postgresql_driver.py`. Add the `CatalogFakeCursor` double (dispatch-by-SQL, staging `description` as a list of `_col(name)` entries and `rows` as a list of tuples) and a `CatalogFakeConn` returning it. Build the Level-1 canned relation rowset as tuples keyed to the aliases the implementer selects (the test asserts on the **normalized output**, not the SQL, so alias names are the implementer's choice — the double matches on SQL substring, e.g. presence of `pg_class`, to stage the relation rowset). Test cases (each = scenario + expected normalized result):

1. Mixed relation set: stage one rowset representing two tables (`public.orders`, `public.order_items`) and one view (`public.order_view`), each with a `column_count` and `relkind` mapping to table/table/view. Assert `describe_schema(table=None, schema=None)["items"]` equals the three dicts with `kind` mapped correctly, `column_count` carried through, and items sorted by `(schema, name)`.
2. System-schema exclusion (D6): include rows for `public.orders`, `pg_catalog.pg_class`, `information_schema.columns`. Assert only `public.orders` appears in `items`.
3. Partition-leaf exclusion (D6): include a partitioned parent (`relispartition=false`, `relkind='p'`) and a leaf (`relispartition=true`, `relkind='r'`). Assert only the parent appears (as `kind="table"`).
4. Matview/sequence exclusion: include rows with `relkind='m'` and `relkind='S'`. Assert neither appears.
5. `--schema` filter: stage relations across `public` and `analytics`; call `describe_schema(table=None, schema="analytics")`; assert only the `analytics` rows are returned (and that the `schema` value reached the query as a bind param — assert the staged `params` carries the schema value).
6. Empty/unknown schema: stage only `public` rows; call `describe_schema(table=None, schema="nope")`; assert `items == []` (empty result, not an error — errors are the command's concern, not the driver's).
7. Injected-`connectable` not closed after `describe_schema` (regression guard for `_owned`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_postgresql_driver.py -v`
Expected: FAIL — `PostgreSQLDriver` has no `describe_schema`.

- [ ] **Step 3: Write minimal implementation**

Add `describe_schema(self, table, schema)` to `PostgreSQLDriver`. Implement only the `table is None` (Level 1) branch returning `{"items": [...]}` per the sources, scope filters, kind mapping, ordering, lazy import, and error mapping in Produces. Bind-parameterize the `schema` filter. Do not modify `execute`, `execute_write`, `connect`, or `close`. (Leave the `table is not None` branch raising `NotImplementedError` or returning an empty `matches` for now — Task 3 fills it; the Level-1 tests never set `table`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_postgresql_driver.py -v && pytest tests/unit -q`
Expected: PASS (new Level-1 cases pass; all existing read/write/coerce/close/protocol cases still pass; full unit suite green — read path unchanged).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_drivers/postgresql.py tests/unit/test_postgresql_driver.py`
Run: `git commit -m "feat(db): PostgreSQLDriver.describe_schema Level 1 (relations list)"`

---

### Task 3: `PostgreSQLDriver.describe_schema` — Level 2 (one relation's columns + keys)

**Files:**
- Modify: `agctl/clients/db_drivers/postgresql.py` (`PostgreSQLDriver.describe_schema`)
- Test: `tests/unit/test_postgresql_driver.py`

**Interfaces:**
- Consumes: the `describe_schema` method and the `CatalogFakeCursor`/`CatalogFakeConn` doubles from Task 2; `coerce_db_value` (from `...assertions`) is NOT used here — catalog cells are emitted verbatim as the spec dictates (e.g. `default` is already-text from `pg_get_expr`); `psycopg` (lazy-imported).
- Produces:
  - Completes the **`table is not None` (Level 2) branch** of `describe_schema`. The full return shape is now:
    ```
    {
      "items": [ … ],                       # Task 2; empty list when table is set
      "matches": [ {                        # one entry per relation named `table`
        schema, table, kind,
        columns: [ { name, data_type, nullable, default, generated, enum_values, comment } … ],
        primary_key: [str …],
        foreign_keys: [ { name, columns:[str …], references_schema, references_table, references_columns:[str …] } … ],
        unique_constraints: [ { name, columns:[str …] } … ],
        comment: str | None
      } … ]
    }
    ```
  - Level-2 behavior contract (sources + normalization rules — NOT query code):
    - **Relation scope:** relations named `table` (exact stored case, spec D7), restricted to `schema` when given; **views are accepted** (spec D5). When `schema` is set, at most one match. When unset, matches may span schemas (the command disambiguates — the driver returns **all** matches; it does not raise on ambiguity).
    - **Column sources:** `pg_attribute` (name `attname`, nullability `attnotnull`, identity `attidentity`, stored-generated `attgenerated`, dropped flag `attisdropped`) joined to the type via `pg_type`/`format_type(atttypid, atttypmod)` for `data_type`; defaults from `pg_attrdef` rendered through `pg_get_expr(adbin, adrelid)`; column comments from `pg_description`. Exclude dropped columns (`attisdropped = true`). Order columns by `attnum` ascending.
    - **`data_type`:** the verbatim `format_type` text (e.g. `integer`, `text`, `uuid`, `jsonb`, `timestamp with time zone`, `character varying(255)`, or an enum type name) — spec D4.
    - **`nullable`:** boolean, the **inverse** of `attnotnull` (`not attnotnull`).
    - **`default`:** the `pg_get_expr` default-expression text, or `null`. **Redaction rule (load-bearing, spec §8.2/D9):** `default` is forced to `null` whenever `generated` is non-null (identity and stored-generated columns have no literal default the agent may supply), regardless of what `pg_attrdef` returned.
    - **`generated` mapping (spec D9/§8.2):** `attidentity == 'a'` → `"always_identity"` (agent MUST omit from INSERT); `attidentity == 'd'` → `"by_default_identity"`; `attgenerated == 's'` → `"stored"`; otherwise `null`. A `serial` column reports `generated == null` with a `nextval(...)` `default` (its `attidentity`/`attgenerated` are both empty).
    - **`enum_values` (spec D9):** for a column whose type is an enum (`pg_type.typtype == 'e'`), the enum's allowed values in declared order from `pg_enum` (ordered by `enumsortorder`); `null` for every non-enum column.
    - **`comment`:** the column's `COMMENT ON` text from `pg_description`, or `null`.
    - **`primary_key`:** from `pg_constraint` `contype == 'p'`; resolve `conkey` (an int array of attnums) to column names via the relation's attnum→name map; empty list if the relation has no PK.
    - **`foreign_keys`:** one entry per `contype == 'f'` constraint. `columns` = `conkey` attnums resolved to names on **this** table; `references_schema`/`references_table` from the constraint's `confrelid` → `pg_class`/`pg_namespace`; `references_columns` = `confkey` attnums resolved to names on the **referenced** table. **Positional pairing (load-bearing, spec §8.2):** `columns[i]` corresponds to `references_columns[i]` — preserve the array order from `conkey`/`confkey` so multi-column and self-referencing FKs pair correctly.
    - **`unique_constraints`:** one entry per `contype == 'u'` **constraint** (unique *indexes* that are not constraints are deferred — spec §12); `columns` from `conkey` resolved to names. Constraint columns ordered by their position in the constraint.
    - **table `comment`:** the relation-level comment from `pg_description` (objoid = the relation, classoid = `pg_class`), or `null`.
    - **Error mapping:** any `psycopg.Error` during the catalog reads surfaces as `ConnectionFailure`; no `commit()`.
  - The `items` key remains present (empty list when `table` is set) so the return shape is uniform across both branches.

- [ ] **Step 1: Write the failing tests**

Extend `tests/unit/test_postgresql_driver.py`, reusing and extending `CatalogFakeCursor` to also stage the Level-2 rowsets (the double dispatches each distinct catalog `SELECT` — relations, columns, defaults, constraints, enum values, comments — to its canned rows; the implementer decides the SQL substrings to match). Each test stages the canned catalog rows for one table and asserts the normalized `matches[0]` dict. Scenarios + expected results:

1. **Plain columns + PK:** a table with `id uuid NOT NULL` (PK), `name text NULL`. Assert `columns` length 2 ordered by `attnum`; `id` has `nullable == false`, `data_type == "uuid"`, `default == null`, `generated == null`, `enum_values == null`, `comment == null`; `name` has `nullable == true`; `primary_key == ["id"]`; `foreign_keys == []`; `unique_constraints == []`; table `comment == null`.
2. **Default expressions:** a column whose `pg_attrdef`/`pg_get_expr` row is `'PENDING'::order_status` and another whose default is `now()`. Assert both `default` strings pass through verbatim.
3. **`generated` mapping + redaction (load-bearing):**
   - identity-always: `attidentity == 'a'` → `generated == "always_identity"` AND `default == null` (even if a default row is staged).
   - identity-by-default: `attidentity == 'd'` → `generated == "by_default_identity"`, `default == null`.
   - stored-generated: `attgenerated == 's'` → `generated == "stored"`, `default == null`.
   - serial-like: `attidentity == ''`, `attgenerated == ''`, default text containing `nextval` → `generated == null`, `default` is the `nextval(...)` string.
4. **`enum_values` (load-bearing):** a column whose type resolves to an enum with values `PENDING, PAID, CANCELLED` (staged from `pg_enum` in that order) → `enum_values == ["PENDING","PAID","CANCELLED"]`; a non-enum column (e.g. `integer`) → `enum_values == null`.
5. **`nullable` inversion:** stage `attnotnull == true` → `nullable == false`; `attnotnull == false` → `nullable == true`.
6. **Multi-column FK with positional pairing (load-bearing):** an FK with `conkey == [1, 2]` and `confkey == [10, 11]`, referencing another table; the local attnum→name map resolves `[1,2]`→`["a","b"]` and the referenced map resolves `[10,11]`→`["x","y"]`. Assert the FK entry has `columns == ["a","b"]`, `references_schema`/`references_table` carried from `confrelid`, and `references_columns == ["x","y"]` (positional pairing preserved).
7. **Self-referencing FK:** an FK whose `confrelid` is the table itself (`parent_id → self(id)`); assert `references_schema`/`references_table` equal the table's own schema/name.
8. **`unique_constraints`:** a `contype == 'u'` constraint over `["status","customer_id"]` → one entry `{"name": …, "columns": ["status","customer_id"]}`.
9. **View target (spec D5):** a relation with `relkind == 'v'` named by `table` is returned with `kind == "view"` and its columns (views have columns in `pg_attribute` too).
10. **Table + column comments:** a column comment `"cents, not dollars"` and a table comment `"Customer orders"` → both surface in the normalized dict.
11. **Multi-schema match (no disambiguation at driver layer):** stage two relations named `orders` in schemas `public` and `legacy`. Assert `matches` has **two** entries (the command, not the driver, raises on ambiguity — the driver returns all).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_postgresql_driver.py -v`
Expected: FAIL — the `table is not None` branch is not implemented (returns empty/`NotImplementedError` from Task 2).

- [ ] **Step 3: Write minimal implementation**

Implement the `table is not None` branch of `describe_schema` per the sources and normalization rules in Produces (column/data_type/nullable/default/generated/enum_values/comment, primary_key, foreign_keys with positional pairing and self-reference, unique_constraints, table comment, view acceptance, multi-match return). Keep the `items` key in the return. Bind-parameterize the `schema` and `table` filters. Do not modify `execute`, `execute_write`, `connect`, or `close`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_postgresql_driver.py -v && pytest tests/unit -q`
Expected: PASS (new Level-2 cases pass; Task 2's Level-1 cases still pass; all pre-existing read/write/coerce/close/protocol cases still pass; full unit suite green).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_drivers/postgresql.py tests/unit/test_postgresql_driver.py`
Run: `git commit -m "feat(db): PostgreSQLDriver.describe_schema Level 2 (columns + keys)"`

---

### Task 4: `DbClient.supports_describe_schema` + `DbClient.describe_schema` (pre-connect probe)

**Files:**
- Modify: `agctl/clients/db_client.py` (`DbClient`)
- Test: `tests/unit/test_db_client.py`

**Interfaces:**
- Consumes: `ConfigError` (from `..errors`); the optional `describe_schema` method produced by Tasks 2–3.
- Produces:
  - `DbClient.supports_describe_schema(self) -> bool` — probes the selected driver for a **callable** `describe_schema` attribute; returns `True` if present-and-callable, `False` otherwise. **Side-effect-free:** it does not call the method and does not require a connection.
  - `DbClient.describe_schema(self, table: str | None, schema: str | None) -> dict` — if `supports_describe_schema()` is `False`, raise `ConfigError("connection's driver (<type>) does not support schema discovery", {"driver": <self._conn_dict['type']>})`; otherwise delegate to `self._driver.describe_schema(table, schema)` and return its dict unchanged.
  - This is the **pre-connect probe**: callers (`db schema`) invoke `supports_describe_schema()` **before** `connect()` so a non-introspection driver fails fast without opening a connection. (This differs deliberately from `execute_write`, whose probe fires after connect; the pre-connect probe is the documented exception — spec §7.)
  - The `DBDriver` Protocol is **not** modified (this is why out-of-tree read-only drivers keep working).

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_db_client.py`, mirroring the existing `TestExecuteWrite` class's fake-driver seam (`FakeDriver`, `FakeDriverWithExecuteWrite`, `FakeDriverReadOnly`, `FakeDriverWithNonCallableExecuteWrite`). Add fakes for the schema capability:

1. A driver **with** a `describe_schema(self, table, schema)` method returning a canned dict → `supports_describe_schema()` is `True` and `describe_schema(None, None)` returns that dict unchanged.
2. A read-only fake **without** `describe_schema` (only `connect`/`execute`/`close`) → `supports_describe_schema()` is `False` and `describe_schema(...)` raises `ConfigError` whose message contains "does not support schema discovery" and whose `detail["driver"]` equals the connection's `type`.
3. A driver with a **non-callable** `describe_schema` attribute (e.g. set to a string) → `supports_describe_schema()` is `False` and `describe_schema(...)` raises the same `ConfigError` (the probe requires callable, not merely present).
4. `supports_describe_schema()` does **not** open a connection — assert the fake's `connected_with` is `None` after the probe (fail-fast guarantee).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_client.py -v`
Expected: FAIL — `DbClient` has no `supports_describe_schema` / `describe_schema`.

- [ ] **Step 3: Write minimal implementation**

Add `supports_describe_schema` and `describe_schema` to `DbClient` per Produces (probe via `getattr(self._driver, "describe_schema", None)` + `callable(...)`; raise `ConfigError` with the driver type on absence; delegate otherwise). Do not change `execute`, `execute_write`, `connect`, `close`, `load_drivers`, or the constructor.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_client.py -v && pytest tests/unit -q`
Expected: PASS (new cases pass; existing dispatch/selection/execute_write/seam cases still pass; full unit suite green).

- [ ] **Step 5: Commit**

Run: `git add agctl/clients/db_client.py tests/unit/test_db_client.py`
Run: `git commit -m "feat(db): add DbClient.supports_describe_schema + describe_schema probe"`

---

### Task 5: `db schema` command (two levels, pre-connect probe, disambiguation)

**Files:**
- Modify: `agctl/commands/db_commands.py` (new `_resolve_schema_request` helper, `_db_schema_tables_core`, `_db_schema_table_core`, `db_schema`, envelopes; export)
- Modify: `agctl/cli.py` (import + register `db_schema`)
- Test: `tests/unit/test_db_commands.py`

**Interfaces:**
- Consumes: `resolve_connection_name(cfg, *, connection_name, template_connection=None) -> str` (Task 1 — restated: precedence explicit `connection_name` > `template_connection` > `cfg.defaults.database_connection`; `ConfigError("No database connection specified", {})` when none set; `ConfigError("Unknown database connection: …", {"connection": …})` when absent from `cfg.database.connections`); `new_db_client`, `envelope`, `load_config_or_raise`, `ConfigError` (existing); `DbClient.supports_describe_schema` + `DbClient.describe_schema` (Task 4). Does **not** consume `resolve_db_request` (schema has no template/sql/params).
- Produces:
  - `def _db_schema_tables_core(config_path, connection, schema) -> dict` — Level 1. Resolves `conn_name = resolve_connection_name(cfg, connection_name=connection)` (template_connection omitted → explicit `--connection` > default, else `ConfigError`), builds the client, probes `supports_describe_schema()` **before** `connect()` (raise `ConfigError("connection's driver (<type>) does not support schema discovery", {"driver": <type>})` if unsupported), then `connect()` → `describe_schema(table=None, schema=schema)` → `close()` in `try/finally`. Returns:
    ```
    {
      "connection": <conn_name>,
      "schema_filter": <schema or null>,
      "count": <len(items)>,
      "items": <items list from the driver>,
      "hint": "Run 'agctl db schema --table <name> [--schema <name>] [--connection <name>]' for columns and keys"
    }
    ```
  - `def _db_schema_table_core(config_path, connection, schema, table) -> dict` — Level 2. Resolves `conn_name` via `resolve_connection_name(cfg, connection_name=connection)` (same as Level 1), then the same pre-connect probe + connect/describe/close lifecycle, calling `describe_schema(table=table, schema=schema)`. Then **disambiguates** the driver's `matches` list:
    - `len(matches) == 0` → `ConfigError` whose message tells the agent the table was not found and to run Level 1 to list tables, with `detail = {"table": table}`.
    - `len(matches) > 1` → `ConfigError` whose message states the name is ambiguous across schemas and to pass `--schema`, with `detail = {"table": table, "candidates": [{"schema": m["schema"], "kind": m["kind"]} for m in matches]}` (spec §9).
    - `len(matches) == 1` → emit the single match **flattened** into the top-level result:
      ```
      {
        "connection": <conn_name>,
        "schema": <match["schema"]>, "table": <match["table"]>, "kind": <match["kind"]>,
        "comment": <match["comment"]>,
        "columns": <match["columns"]>,
        "primary_key": <match["primary_key"]>,
        "foreign_keys": <match["foreign_keys"]>,
        "unique_constraints": <match["unique_constraints"]>,
        "hint": "Use these columns in 'agctl db query' / 'db assert --sql' with :paramName bind params."
      }
      ```
  - A Click command `db_schema` named `"schema"` with options `--connection`, `--schema`, `--table` (all `default=None`; **no** `--template`/`--sql`/`--param`/`--write`). It resolves `config_path` from `ctx.obj`, then dispatches: `table is None` → `_db_schema_tables_envelope(...)`; else `_db_schema_table_envelope(...)`. (Mirrors `discover`'s flag-dispatch; there is no mutual-exclusion validation — `--schema` is valid at both levels.)
  - In `cli.py`: import `db_schema` from `.commands.db_commands` and add `db_group.add_command(db_schema)` next to the existing `db_query`/`db_assert`/`db_execute` registrations. Add `db_schema` to `db_commands.__all__`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_db_commands.py`, reusing the file's `_run`/`_payload`/`install_fake` helpers and `ENV`. The existing `FakeDriver` needs a `describe_schema(self, table, schema)` method recording its call args and returning a configurable canned dict (extend `FakeDriver.__init__` to accept a `schema_result=` and record `self.described = []`; mirror how `execute`/`execute_write` are recorded). The `install_fake` factory wires this driver so command tests avoid a real DB. Write:

1. **Level 1 happy path:** `db schema` (no flags, resolves to default `main-db`) → exit 0, `command == "db.schema.tables"`, `result.connection == "main-db"`, `result.schema_filter is None`, `result.count == len(canned items)`, `result.items` equals the canned list, and the `hint` is the Level-1 string. The fake received `describe_schema(table=None, schema=None)`.
2. **Level 1 with `--schema`:** `db schema --connection main-db --schema public` → exit 0; `result.schema_filter == "public"`; the fake recorded `schema == "public"`.
3. **Level 1 empty schema:** canned `items == []` for a given `--schema` → exit 0, `ok: true`, `count == 0`, `items == []` (NOT an error).
4. **Level 2 happy path:** `db schema --connection main-db --table orders` with canned `matches == [ {schema:"public", table:"orders", kind:"table", comment:null, columns:[…], primary_key:["id"], foreign_keys:[…], unique_constraints:[…] } ]` → exit 0, `command == "db.schema.table"`, and `result` is the flattened single-match dict (`schema`/`table`/`kind`/`comment`/`columns`/`primary_key`/`foreign_keys`/`unique_constraints`/`connection`/`hint`). The fake received `describe_schema(table="orders", schema=None)`.
5. **Level 2 with `--schema`:** `db schema --connection main-db --schema public --table orders` → the fake recorded `table="orders", schema="public"`; flattened result present.
6. **Pre-connect probe refusal (load-bearing):** a fake driver **without** `describe_schema` (use the existing read-only shape, or a flag on `install_fake`) → `db schema --connection main-db` → exit 2, `ConfigError` whose message contains "does not support schema discovery" and whose `detail["driver"] == "postgresql"`; **`connect` was never called** (assert `fake.connected is False`, mirroring the file's fail-fast assertion style).
7. **Level 2 not-found:** canned `matches == []` → `db schema --connection main-db --table nope` → exit 2 `ConfigError` whose message tells the agent to list tables and whose `detail["table"] == "nope"`.
8. **Level 2 ambiguity:** canned `matches == [ {schema:"public",…}, {schema:"legacy",…} ]`, no `--schema` → `db schema --connection main-db --table orders` → exit 2 `ConfigError` whose message mentions ambiguity and `--schema`, and whose `detail["candidates"]` contains `{"schema":"public","kind":"table"}` and `{"schema":"legacy",…}`.
9. **Connection resolution:** no resolvable connection (`db schema` against a config with no `defaults.database_connection` and no `--connection`) → exit 2 `ConfigError` ("No database connection specified"); unknown `--connection foo` → exit 2 `ConfigError` ("Unknown database connection: foo"). (Use the existing fixture for the positive cases; for the no-default case construct an in-memory config or rely on the helper's behavior — the `resolve_connection_name` unit behavior is already covered; here assert the command surfaces it as exit-2 `ConfigError`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_db_commands.py -v`
Expected: FAIL — `db schema` is not registered (Click "No such command" / exit 2 non-config).

- [ ] **Step 3: Write minimal implementation**

Add `_db_schema_tables_core`, `_db_schema_table_core`, `db_schema`, and the two `_envelope = envelope("db.schema.tables"/"db.schema.table")(...)` wrappers to `db_commands.py` per Produces (both cores resolve the connection via `resolve_connection_name`; pre-connect probe in BOTH cores; disambiguation in the Level-2 core; the exact hint strings; `--schema`/`--connection`/`--table` Click options only). Add `db_schema` to `__all__`. In `cli.py`, import and register `db_schema` on `db_group`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db_commands.py -v && pytest tests/unit -q`
Expected: PASS (new schema cases pass; all existing query/assert/execute cases still pass; full unit suite green — no regression).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/db_commands.py agctl/cli.py tests/unit/test_db_commands.py`
Run: `git commit -m "feat(db): add agctl db schema live discovery command"`

---

### Task 6: Integration test — live `db schema` introspection (self-skipping)

**Files:**
- Modify: `tests/integration/test_db_commands.py`
- Test: `tests/integration/test_db_commands.py` (self-skipping via `require_postgres`)

**Interfaces:**
- Consumes: the `require_postgres` fixture and `_env()` helper already in `tests/integration/conftest.py` and `tests/integration/test_db_commands.py`; the shared `FIXTURE` (uses `main-db-writable` for setup writes and `main-db-writable`/`main-db` for introspection — schema discovery is ungated, so either works); `CliRunner`.
- Produces: a self-skipping integration test `test_db_schema_live_introspection` that builds a rich schema via `db execute` DDL, then asserts `db schema` (both levels) returns the real introspected shape. The setup uses a **dedicated throwaway schema** (`agctl_schema_test`) so Level-1 assertions are deterministic and isolated from other tests' objects (`agctl_seed`, etc.).

- [ ] **Step 1: Write the failing test**

The live testcontainer Postgres is empty, so the test creates its own throwaway schema + objects via `db execute` DDL (DDL is a write; it commits). Scenario + expected results:

1. **Setup (DDL via `db execute --connection main-db-writable --write`):**
   - `DROP SCHEMA IF EXISTS agctl_schema_test CASCADE` then `CREATE SCHEMA agctl_schema_test` (clean reruns).
   - `CREATE TYPE agctl_schema_test.order_state AS ENUM ('PENDING','PAID','CANCELLED')`.
   - `CREATE TABLE agctl_schema_test.customers (id uuid PRIMARY KEY, email text NOT NULL, created_at timestamptz)`.
   - `CREATE TABLE agctl_schema_test.orders (id uuid PRIMARY KEY, customer_id uuid NOT NULL REFERENCES agctl_schema_test.customers(id), status agctl_schema_test.order_state NOT NULL DEFAULT 'PENDING', payload jsonb, audit_id integer GENERATED ALWAYS AS IDENTITY, row_total integer GENERATED ALWAYS AS (0) STORED, UNIQUE (status, customer_id))`.
   - `CREATE VIEW agctl_schema_test.order_view AS SELECT id, status FROM agctl_schema_test.orders`.
   - Column comment `COMMENT ON COLUMN agctl_schema_test.orders.status IS 'lifecycle state'` and table comment `COMMENT ON COLUMN agctl_schema_test.orders.total_cents IS 'cents'` style; add `COMMENT ON TABLE agctl_schema_test.orders IS 'Customer orders'`.
   - Each DDL statement is its own `db execute` invocation (one statement per call, so a failure is unambiguous); all expect exit 0.
2. **Level 1 (`db schema --connection main-db-writable --schema agctl_schema_test`):** exit 0, `command == "db.schema.tables"`, `result.schema_filter == "agctl_schema_test"`, and `result.items` contains exactly the three relations `customers` (table), `orders` (table), `order_view` (view) — each with the right `kind` — and `count == 3` (the enum **type** is not a relation and must not appear; partition leaves/matviews absent). Assert membership of the three expected `{schema,name,kind}` dicts rather than whole-list equality if ordering details vary, but the set must be exactly those three.
3. **Level 2 (`db schema --connection main-db-writable --schema agctl_schema_test --table orders`):** exit 0, `command == "db.schema.table"`, `result.table == "orders"`, `result.kind == "table"`, and assert the introspected richness:
   - `result.primary_key == ["id"]`.
   - `result.foreign_keys` has one entry with `columns == ["customer_id"]`, `references_schema == "agctl_schema_test"`, `references_table == "customers"`, `references_columns == ["id"]`.
   - `result.unique_constraints` has one entry with `columns == ["status","customer_id"]`.
   - The `status` column: `data_type` is the enum type name (`order_state`, possibly schema-prefixed by `format_type` — assert it ends with `order_state`), `enum_values == ["PENDING","PAID","CANCELLED"]`, `nullable == false`, `default` is the `'PENDING'::order_state` expression (assert it contains `'PENDING'`), `comment == 'lifecycle state'`.
   - The `audit_id` column: `generated == "always_identity"` and `default == null`.
   - The `row_total` column: `generated == "stored"` and `default == null`.
   - The `payload` column: `data_type == "jsonb"`, `enum_values is None`.
   - `result.comment == "Customer orders"`.
4. **Privilege/cluster caveat is informational only** — no assertion (the testcontainer superuser sees everything; the spec §3 caveat is documented, not testable here).

`require_postgres` is a test argument so the whole test skips when Postgres is unreachable.

- [ ] **Step 2: Verify the test is wired and skips cleanly (no live DB)**

Run: `pytest tests/integration/test_db_commands.py -v`
Expected: without `AGCTL_TEST_LIVE=1`/`AGCTL_TEST_PG_DSN`, the test **skips** ("AGCTL_TEST_PG_DSN not set"), exit 0 — NOT a red failure. (A self-skipping integration test cannot go red-first by design: its pre-implementation state is "skipped," and a wiring bug would surface as an *error*, not a skip. Asserting that it *skips* — rather than errors — is the red signal available at this layer.)

- [ ] **Step 3: Verify against a live DB when available (verification-only)**

This task adds no production code. If a live DB is available, confirm the test PASSES: `AGCTL_TEST_LIVE=1 pytest tests/integration/test_db_commands.py -v`. If the live run surfaces a real defect in Tasks 2–5 (e.g. a `format_type` rendering or `attidentity` mapping the fake didn't catch), fix it in the relevant task. If no live DB is available, the Step 2 skip is the accepted terminal state for this task.

- [ ] **Step 4: Run the full suite**

Run: `pytest tests/unit tests/integration -q`
Expected: all unit tests PASS; integration tests PASS or SKIP (never fail for a missing service).

- [ ] **Step 5: Commit**

Run: `git add tests/integration/test_db_commands.py`
Run: `git commit -m "test(db): integration test for db schema live introspection"`

---

### Task 7: Skill artifacts + doc-sync handoff

**Files:**
- Modify: `skills/agctl/SKILL.md` (add `db schema` to the command-forms table + gotchas; a short "discover live schema before authoring SQL" section)
- (DESIGN.md / ARCHITECTURE.md sync is performed by the `docs-watcher` subagent after this task — see Step 4.)

**Interfaces:**
- Consumes: the final behavior of Tasks 1–6; the existing `skills/agctl/SKILL.md` structure (the "Which command for which intent" table, the "Command forms" block, the "Gotchas" list, and the `db execute` section as the house-style template for a dedicated command section).
- Produces:
  - `skills/agctl/SKILL.md` — add a row to the intent table: "Discover live DB schema" → `agctl db schema [--connection C] [--schema S] [--table T]`.
  - Add the command form `agctl db schema [--connection C] [--schema S] [--table T]` to the "Command forms" block (no `--write`/`--template`/`--sql`/`--param` — read-only and ungated).
  - Add a short **"Discover live schema before authoring SQL"** subsection (near the `db execute` section) covering: the two-level progressive model (list relations, then drill into one), that `--table` accepts views, the fail-loud not-found/ambiguity behavior, and the two load-bearing authoring notes from the spec: (a) **identifier quoting** — identifiers with uppercase/non-`[a-z0-9_]` chars or matching a Postgres reserved word MUST be double-quoted (`"OrderItems"`, `"user"`) or Postgres silently folds them; (b) **omit generated columns from INSERT** — `generated == "always_identity"` or `"stored"` columns must be omitted from `INSERT` (and `"stored"` from `UPDATE` too); `"by_default_identity"` and `serial` (default `nextval(...)`) may be supplied or omitted.
  - Add a gotcha bullet: `db schema` uses `pg_catalog` (cluster-wide, not privilege-filtered — spec §3); on a shared cluster it can show tables this connection cannot `SELECT` from.

- [ ] **Step 1: Read the existing skill for house style**

Read `skills/agctl/SKILL.md` to locate the intent table, the command-forms block, the gotchas list, and the `db execute` section (the template for a dedicated command subsection). Match its voice/structure.

- [ ] **Step 2: Update the skill**

Edit `skills/agctl/SKILL.md` per Produces: the table row, the command form, the new subsection (two-level model + views + fail-loud + identifier quoting + omit-generated-from-INSERT), and the `pg_catalog` gotcha bullet. Keep guidance consistent with the spec (§6–§9): read-only/ungated, no `--write`, exact-case `--table` matching, ambiguous → `--schema`.

- [ ] **Step 3: Verify the skill is well-formed**

Re-read the file end-to-end; confirm every command example matches the implemented CLI exactly (flags: `--connection`/`--schema`/`--table` only; the `db.schema.tables` and `db.schema.table` result shapes; the two `hint` strings; the ambiguity `detail.candidates`). Confirm the `pg_catalog` privilege caveat and the identifier-quoting / omit-generated guidance are present and accurate.

- [ ] **Step 4: Hand off to docs-watcher**

Invoke the `docs-watcher` subagent (per CLAUDE.md "Docs Sync") to sync **DESIGN.md** §3.3 (the new `db schema` command: flags, two levels, read-only/ungated), §4.2 (the `db.schema.tables` and `db.schema.table` result shapes), §9.1 (`describe_schema` as an optional driver capability, Protocol unchanged) and **ARCHITECTURE.md** §3 (new command in `commands/db_commands.py`), §7 (new error rows: capability-refusal / not-found / ambiguity, all `ConfigError` exit 2), §8 (optional `describe_schema` capability, the pre-connect `supports_describe_schema` probe, the `pg_catalog` introspection source, the `db schema` two-core lifecycle), §10 (`describe_schema` as an optional capability — Protocol unchanged). Preserve each doc's altitude (DESIGN = WHAT/WHY, ARCHITECTURE = HOW); note the `db` group becomes mixed (`schema.*` carries a `hint` like `discover`, unlike `query`/`assert`/`execute` — intentional). A correct no-op for a section that already matches is fine.

- [ ] **Step 5: Commit**

Run: `git add skills/agctl/SKILL.md docs/DESIGN.md docs/ARCHITECTURE.md`
Run: `git commit -m "docs(skills): document agctl db schema + sync design/architecture"`

---

## Self-Review (completed during planning)

1. **Code scan:** No method bodies, SQL strings, algorithms, or test/implementation code appear — only signatures, data shapes, catalog sources + normalization rules, behavior descriptions, and expected test results. (The integration-test DDL in Task 6 is named at the statement level so the assertions are concrete; it is test fixture, not production logic, and the implementer writes it.)
2. **Self-containment:** Each task's Consumes/Produces carries the exact signatures, types, data shapes, normalization rules, error cases, and processing order needed to implement it without reading the spec. Task 1 restates the helper's full contract; Task 5 restates the probe and disambiguation contracts; the hint strings are inline.
3. **Spec coverage:** §1–§2 goals → Global Constraints + Architecture; §3 safety/threat (no write gates; `pg_catalog` privilege caveat) → Global Constraints (read-only/ungated), Task 6 (informational), Task 7 (gotcha); §4 non-goals → Global Constraints (no read/write path changes, no new error types); §5 D1 command surface → Task 5; D2 discover untouched (no task — correct); D3 optional capability → Tasks 2–4; D4 pg_catalog source + normalization → Tasks 2–3; D5 views → Tasks 2–3 (kind mapping, view target); D6 scope/partition exclusion → Tasks 2–3 + Task 6; D7 fail-loud `--table` + ambiguity candidates → Task 5; D8 shared connection helper → Task 1 (isolated commit) + Task 5; D9 column richness (generated/identity/enum/comments) → Task 3 + Task 6; §6 command contract → Task 5; §7 capability contract + pre-connect probe → Tasks 2–4 + Task 5; §7.1 rejected alternatives (no code — design only); §8 result contracts → Task 5 Produces; §9 error model → Task 5 Produces (all rows `ConfigError` exit 2 except DB errors `ConnectionFailure` via Tasks 2–3); §10 testing → all tasks (FakeDriver extend, multi-query CatalogFakeCursor, pre-connect assertion, integration self-skip); §11 docs/skill → Task 7; §12 deferred items are deliberately out of scope (no task).
4. **Placeholder scan:** Each step states a concrete scenario + expected result or a concrete behavior/catalog rule; no TBD/TODO/"add error handling" — the load-bearing cases (generated redaction, enum values, positional FK pairing, self-reference, pre-connect probe, ambiguity candidates, empty-schema-not-error) are each pinned to a named test.
5. **Type consistency:** `describe_schema(table, schema) -> {items, matches}` is consistent across Tasks 2, 3, 4, 5; `supports_describe_schema() -> bool` across Tasks 4 and 5; `resolve_connection_name(cfg, *, connection_name, template_connection=None) -> str` across Tasks 1 and 5; the Level-1/Level-2 result field names (`schema_filter`/`count`/`items`/`hint` vs `schema`/`table`/`kind`/`comment`/`columns`/`primary_key`/`foreign_keys`/`unique_constraints`/`hint`) match between Task 5 Produces and Task 6 assertions; the `generated` value set (`"always_identity"`/`"by_default_identity"`/`"stored"`/`null`) and `default`-redaction rule are identical in Task 3, Task 6, and Task 7.
