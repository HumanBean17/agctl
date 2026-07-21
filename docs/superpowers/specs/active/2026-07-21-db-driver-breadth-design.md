---

# Design: DB Driver Breadth — MySQL + SQLite built-ins, hardened protocol

**Status:** Approved (design) — ready for spec review, then implementation plan
**Date:** 2026-07-21
**Author:** brainstorming session
**Affects:** `agctl db` command group; `agctl/clients/db_driver_protocol.py`, `db_client.py`, `db_drivers/`; `agctl/config/models.py`; `pyproject.toml`; DESIGN.md §2.1 / §3.3 / §9.1; ARCHITECTURE.md §3 / §8 / §10 / §14
**Relation to docs:** Adds built-in drivers + a shared base; on implementation, DESIGN.md `database.connections.type` enumerated values and §9.1 DBDriver extension contract, and ARCHITECTURE.md §3 module map + §8 DB client layer + §10 extension points are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl db *` ships with exactly **one** built-in driver: `PostgreSQLDriver`
(wrapping `psycopg`). The `DBDriver` Protocol is intentionally minimal
(`connect` / `execute` / `close`), and the two optional capabilities every real
driver needs — `execute_write` (powers `db execute`) and `describe_schema`
(powers `db schema`) — are duck-typed via scattered `hasattr` / `callable`
checks in `DbClient` and the command layer. There are no DTOs formalizing the
implicit dict shapes those methods return.

Three concrete pain points follow from this:

1. **PostgreSQL-only gap.** A consumer testing against MySQL or SQLite has no
   built-in path. They must either write a third-party driver from scratch
   (against an undocumented contract) or shell out to a DB client outside the
   one-JSON-envelope contract — breaking composability for agents.
2. **Implicit contracts.** The shape returned by `execute_write`
   (`{"rows_affected": int | None, "returning": list[dict]}`) and by
   `describe_schema` (`{"items": [...]}` / `{"matches": [...]}` with nested
   column / FK / constraint dicts) lives only inside `postgresql.py`. A
   contributor writing a new driver has to read 639 lines to extract the
   contract.
3. **Duplicated boilerplate.** Every driver independently solves: secret
   redaction in error details, lazy-import-then-`ConfigError` for the optional
   dependency, SQL param translation. Today only PostgreSQL pays this cost; a
   naive second driver would copy-paste ~80 lines.

## 2. Goals

- **Ship MySQL + SQLite as built-in drivers**, both first-class (query, assert,
  execute, schema all work). Directly closes the PostgreSQL-only gap.
- **Harden the contributor extension surface** so adding a driver is a
  one-file drop-in with a clear template to copy — without speculative
  machinery.
- **Formalize the implicit dict contracts** via typed DTOs (`WriteResult`,
  `ColumnInfo`, `ForeignKey`, `UniqueConstraint`, `SchemaItem`, `SchemaMatch`).
- **Preserve the existing JSON envelope shape** users see: DTOs are an internal
  boundary; the emitted JSON is unchanged.
- **Don't break PostgreSQL.** The 1321-line `test_postgresql_driver.py` test
  file stays green throughout the refactor.

## 3. Non-Goals

- **SQL Server / Oracle built-ins.** Deferred to third-party extension
  examples / follow-up. The hardened protocol is designed so adding them
  later is a one-file drop-in.
- **SQL dialect translation.** Templates are dialect-specific (a Postgres
  `ON CONFLICT` clause will not run against MySQL). `agctl` does not translate
  SQL; an agent/consumer authors per-dialect SQL or uses `:paramName`-only
  portable fragments.
- **Formal capability registry.** Only two optional capabilities exist today
  (`execute_write`, `describe_schema`). A `capabilities = {Capability.READ,
  ...}` class-attr registry is speculative and rejected.
- **ABC base class.** The codebase has zero ABCs; the Protocol is already the
  structural contract. `BaseDBDriver` is a mixin with shared helpers, not an
  ABC with abstract methods.
- **Per-driver connection-config submodels.** `DatabaseConnection` stays
  generic; driver-specific extras (charset, collation, SSL) flow through a
  generic `options: dict` field rather than a typed per-driver submodel.
- **SQLite attached-database support.** v1 returns `schema = "main"` only;
  cross-attached-DB FKs and schemas are deferred.
- **MySQL SSL/charset as first-class fields.** Reachable via `options` or via
  query params on the `url`; a dedicated typed submodel is v2.

## 4. Design Decisions (locked via brainstorming 2026-07-21)

| Decision | Choice | Rejected alternatives |
|---|---|---|
| Driver scope | MySQL + SQLite built-ins | + SQL Server; + SQL Server/Oracle |
| MySQL driver lib | PyMySQL (pure-Python, MIT) | mysql-connector-python; mysqlclient (C); pluggable-later |
| Packaging | Per-engine extras (`[mysql]`); SQLite in core | extend `[db]`; `[databases]` meta-extra |
| Protocol hardening | DTOs + `BaseDBDriver` mixin (medium) | DTOs only; + capability registry (max); defer |
| `describe_schema` scope | Both new drivers implement it (full Level-1 + Level-2) | SQLite-only; neither; MySQL Level-1 only |

These are constraints on the implementation. Do not re-litigate without new
information (e.g., a third optional capability emerging).

## 5. Architecture & Module Layout

### 5.1 File structure

```
agctl/clients/
├── db_driver_protocol.py     # DBDriver Protocol + DTOs + BaseDBDriver mixin
├── db_client.py              # unchanged dispatch logic (BUILTIN_DRIVERS grows)
├── db_drivers/
│   ├── __init__.py
│   ├── postgresql.py         # refactored: inherits BaseDBDriver, returns DTOs
│   ├── mysql.py              # NEW — PyMySQL-backed
│   └── sqlite.py             # NEW — stdlib sqlite3-backed
```

### 5.2 `db_driver_protocol.py` additions

Three categories of addition; the `DBDriver` Protocol itself is unchanged in
shape (`connect` / `execute` / `close`).

**DTOs (dataclasses, mirroring `log_backend_protocol.py`'s pattern):**

- `WriteResult` — `{rows_affected: int | None, returning: list[dict]}`. Replaces
  the ad-hoc dict returned by `execute_write`.
- `ColumnInfo` — `{name, data_type, nullable, default, generated, enum_values,
  comment}`. One per column in a `SchemaMatch`.
- `ForeignKey` — `{name, columns, references_schema, references_table,
  references_columns}`.
- `UniqueConstraint` — `{name, columns}`.
- `SchemaItem` — Level-1 list entry: `{schema, name, kind, column_count}`.
- `SchemaMatch` — Level-2 detail entry: `{schema, table, kind, comment, columns,
  primary_key, foreign_keys, unique_constraints}`.

Drivers return DTOs; `DbClient.describe_schema()` runs `dataclasses.asdict()`
before returning to the command layer — JSON shape seen by users is unchanged
from PostgreSQL's today. The same applies to `execute_write`'s result.

**`BaseDBDriver` mixin** — no required overrides, no abstract methods. Three
protected helpers:

- `_redact_config(config: dict) -> dict` — copy of config safe for error
  detail. Generalizes PostgreSQL's existing pattern to redact any key whose
  name matches `password|secret|token|key` (case-insensitive) and any `url`
  with leading `scheme://user:pass@` userinfo (any scheme, not just
  `postgres(ql)://`).
- `_lazy_import_or_raise(module: str, extra: str)` — `importlib.import_module`
  with `ImportError` → `ConfigError` pointing at the named extra. Replaces the
  inline `try/except ImportError` pattern in each driver's `connect()`.
- Documented optional-capability contract: drivers MAY implement
  `execute_write(self, sql, params) -> WriteResult` and
  `describe_schema(self, table, schema) -> dict` (returning a dict whose
  `"items"` / `"matches"` values are lists of the corresponding DTOs). The
  Protocol module's docstring spells out the contract.

### 5.3 PostgreSQL refactor (behavior-preserving)

`PostgreSQLDriver(BaseDBDriver)`:

- `_redact_config` moves to `BaseDBDriver`; PostgreSQL's local
  `_URL_USERINFO_PATTERN` (Postgres-specific scheme) is removed in favor of the
  base's scheme-agnostic pattern.
- `connect()`'s inline `try/except ImportError` becomes
  `psycopg = self._lazy_import_or_raise("psycopg", "db")`.
- `execute_write()` returns a `WriteResult` DTO internally; `DbClient`'s write
  path serializes via `dataclasses.asdict()`.
- `describe_schema()` returns DTOs internally (Level-1 list of `SchemaItem`,
  Level-2 list of `SchemaMatch` with nested DTOs); `DbClient.describe_schema()`
  serializes.

The 1321-line `test_postgresql_driver.py` stays green: observable behavior
(JSON emitted, errors raised, connection lifecycle) is unchanged. Test churn
is limited to anything that asserted on the specific return type of the
driver method rather than the serialized dict — those few assertions move to
the `DbClient` boundary.

### 5.4 Contributor story

The simplest driver (`sqlite.py`) becomes the canonical template: copy it,
rename, swap the stdlib import or the `_lazy_import_or_raise` target, swap the
catalog-introspection queries, return the same DTOs. No core files touched.
Entry-point registration in the contributor's `pyproject.toml` is the only
out-of-tree step.

## 6. MySQL Driver Design

### 6.1 Connection config

Reuses existing `DatabaseConnection` fields:

| Config field | PyMySQL kwarg |
|---|---|
| `host` | `host` |
| `port` | `port` (default 3306 if unset) |
| `dbname` | `database` (driver renames) |
| `user` | `user` |
| `password` | `password` |
| `url` | parsed via stdlib `urllib.parse`; discrete fields override |
| `options` | merged into PyMySQL `connect(**kwargs)` (charset, collation, etc.) |

**URL scheme:** `mysql://user:pass@host:port/db?charset=utf8mb4`. The
SQLAlchemy-style `mysql+pymysql://` dialect suffix is NOT accepted (single
driver, no dialect negotiation).

### 6.2 SQL param translation

`convert_sql_params` (existing) rewrites `:name` → `%(name)s`. PyMySQL natively
accepts `%(name)s` named params. No translation-layer changes.

### 6.3 Lifecycle

- `connect()`: `autocommit=False` on connect. Matches PostgreSQL's explicit
  commit/rollback semantics. Lazy-import PyMySQL via
  `_lazy_import_or_raise("pymysql", "mysql")`. Connection failure →
  `ConnectionFailure` with redacted config in `detail`.
- `execute()`: rewrite → execute → build dict rows via `coerce_db_value` → no
  commit (read-only).
- `execute_write()`: rewrite → execute → materialize `rowcount` + any
  `RETURNING`-equivalent rows (MySQL has no `RETURNING`; `cursor.description`
  is `None` for non-SELECT, so `returning=[]` always) → `conn.commit()` last →
  rollback on any exception.
- `close()`: release if owned.

### 6.4 `describe_schema()`

- **Level 1**: `information_schema.tables`, excluding system schemas (`mysql`,
  `performance_schema`, `information_schema`, `sys`). `table_type`:
  `'BASE TABLE'` → `"table"`, `'VIEW'` → `"view"`, everything else excluded.
  Column count via subquery on `information_schema.columns`. Sorted by
  `(schema, name)`.
- **Level 2** (one `SchemaMatch` per relation named `table`, restricted to
  `schema` when given):
  - Columns: `information_schema.columns` (`column_name`, `data_type`,
    `is_nullable`, `column_default`, `extra`). `extra = 'auto_increment'` maps
    to `generated` semantics consistent with PostgreSQL's identity mapping.
  - PK / FK / unique: derived from `information_schema.table_constraints` +
    `key_column_usage` + `referential_constraints`. FK rows preserve
    positional column pairing.
  - Enum values: MySQL exposes them via `column_type` string (e.g.
    `enum('a','b')`); driver parses the literal list when
    `data_type = 'enum'`.
  - Comments: MySQL `column_comment` / `table_comment`.

### 6.5 MySQL-specific limitations (documented, not enforced)

- No `RETURNING` clause — `execute_write` against MySQL always returns
  `returning=[]`. Templates using `RETURNING` will fail at SQL time (MySQL
  syntax error), surfacing as `ConnectionFailure`.
- Generated-column semantics are weaker than PostgreSQL's identity taxonomy;
  `generated` may be `None` even for `auto_increment` columns (documented in
  driver docstring).

## 7. SQLite Driver Design

### 7.1 Connection config

| Config field | sqlite3 connect usage |
|---|---|
| `url` | the database path (bare path, `:memory:`, OR `file:` URI) |
| `host` / `port` / `user` / `password` / `dbname` | ignored |

**Two URL forms, auto-detected:**
- Bare path: `/path/to/db.sqlite` or `:memory:` →
  `sqlite3.connect(path, uri=False)`.
- URI form: `file:/path/to/db?mode=ro&immutable=1` →
  `sqlite3.connect(uri_str, uri=True)`. Enables read-only test connections.

Detection: `url.startswith("file:")` → `uri=True`, else `uri=False`.

**Driver type name:** `sqlite` (engine name, consistent with `postgresql`).

### 7.2 Lifecycle

- **No extras, no lazy import.** `sqlite3` is stdlib; imported at module top.
  Ships in core. `BUILTIN_DRIVERS` includes `"sqlite": SQLiteDriver`
  unconditionally.
- `connect()`: `sqlite3.connect(url, uri=..., detect_types=PARSE_DECLTYPES |
  PARSE_COLNAMES)` so declared `TIMESTAMP`/`DATETIME` types auto-convert
  instead of returning strings.
- `execute()` / `execute_write()`: same shape as the other drivers. SQLite
  supports `RETURNING` since 3.35; surfaced via `cursor.description` when
  present.

### 7.3 SQL param translation — none

Python's `sqlite3` natively accepts `:name` named params. Driver passes SQL
through **unchanged** and passes the params dict directly. This is the
cleanest demonstration of why `convert_sql_params` lives in the driver layer,
not as a global preprocessing step. Templates written with `:paramName`-only
placeholders are portable across all three drivers at the param level (SQL
dialect differences in the body remain the user's responsibility).

### 7.4 `describe_schema()`

- **Level 1**: `sqlite_master`, excluding internal tables (`name NOT LIKE
  'sqlite_%'`). `type`: `'table'` → `"table"`, `'view'` → `"view"`. Column
  count via `PRAGMA table_info(name)`. `schema` is always `"main"` for v1.
- **Level 2** (one `SchemaMatch` per relation named `table`):
  - Columns: `PRAGMA table_xinfo` (SQLite 3.26+) → includes `hidden` marker
    for generated columns (`hidden=2|3` → `generated="stored"`).
  - Primary key: `PRAGMA table_info`'s `pk > 0` columns, ordered by `pk` value
    (composite-PK ordering preserved).
  - Foreign keys: `PRAGMA foreign_key_list` → grouped by FK `id`, `from`
    columns in `seq` order. `references_schema` is always `None` (v1).
  - Unique constraints: `PRAGMA index_list` filtered by `origin = 'u'`, then
    `PRAGMA index_info(idx)` per index → columns in order.
  - Enum values: always `None` (SQLite has no enum type).
  - Comment: always `None` (SQLite has no native comments).
  - `data_type`: verbatim from `PRAGMA table_xinfo`'s `type` column
    (e.g. `"INTEGER"`, `"VARCHAR(255)"`).

**PRAGMA identifier interpolation caveat:** PRAGMAs do not accept bind
parameters in `sqlite3`; identifiers are interpolated with strict validation
(regex `^[a-zA-Z_][a-zA-Z0-9_]*$`) to prevent injection. This is the only
place in any driver where SQL identifier interpolation happens; documented
in a method-level comment in `sqlite.py`.

**SQLite quirks handled:**
- Views: PRAGMAs return empty for views — PKs / FKs / uniques all `[]`.
  `kind = "view"` surfaces it.
- Schema is always `"main"` for v1 (attached DBs deferred).

## 8. Config Schema & Packaging Changes

### 8.1 `DatabaseConnection` model (`agctl/config/models.py`)

One field added:

```python
class DatabaseConnection(BaseModel):
    type: str                     # "postgresql" | "mysql" | "sqlite" | <plugin>
    # ... existing fields unchanged ...
    options: dict[str, Any] = Field(default_factory=dict)  # NEW
```

- **`type` stays a free-form string.** No Pydantic enum — drivers are
  extensible via entry points, and an enum would reject third-party types at
  config-load time before `DbClient` can surface the proper "Unknown database
  type" error. The contract is documented in the model docstring (built-in
  values: `postgresql`, `mysql`, `sqlite`) rather than enforced.
- **`options` semantics:** default empty dict. Forwarded verbatim through
  `DbClient._conn_dict` to the driver (already happens via `model_dump()`).
  Each driver reads only the keys it recognizes; unrecognized keys ignored
  (forward-compat). ENV override path:
  `AGCTL_DATABASE__CONNECTIONS__<NAME>__OPTIONS__CHARSET=utf8mb4` (nested dict
  override, already supported by the resolver). **Not denylisted** —
  charset/timeout tuning is legitimate runtime config, unlike `writable` /
  `mode` which are safety gates.

### 8.2 Validator (`agctl/config/validator.py`)

**No new validation rules.** Specifically, `validate_config` does NOT check
whether `type` is a known built-in value — that would break the entry-point
extension model. The existing "unknown connection name" cross-ref checks
stay; the "unknown driver type" check happens at `DbClient.__init__` time
with the proper `ConfigError` + `{"type": db_type}` detail (exit 2,
fast-fail, before any connection attempt).

### 8.3 `pyproject.toml`

```toml
[project.optional-dependencies]
mysql = ["PyMySQL>=1.1", "jq>=1.6"]
# 'sqlite' deliberately absent — sqlite3 is stdlib, ships in core.

[project.entry-points."agctl.db_drivers"]
postgresql = "agctl.clients.db_drivers.postgresql:PostgreSQLDriver"
mysql = "agctl.clients.db_drivers.mysql:MySQLDriver"
sqlite = "agctl.clients.db_drivers.sqlite:SQLiteDriver"
```

Entry-point registration is belt-and-suspenders: all three are also in
`BUILTIN_DRIVERS`. Registration makes them visible to
`importlib.metadata.entry_points()` introspection (future tooling, and
contributors reading pyproject.toml as the registry of built-ins).

### 8.4 `BUILTIN_DRIVERS` (`db_client.py`)

Gains `mysql` and `sqlite`. **Critical import-order invariant:** the `mysql`
and `postgresql` modules must lazy-import their heavy deps inside `connect()`
(existing pattern). `sqlite.py` imports `sqlite3` at module top (stdlib, zero
cost). `BUILTIN_DRIVERS` populating at `db_client.py` import time must NOT
trigger a `PyMySQL` / `psycopg` import — verified by extending the existing
`test_packaging.py` pattern.

## 9. Testing Strategy

Mirrors the existing PostgreSQL driver's two-layer pattern.

### 9.1 Unit tests (`tests/unit/`)

- **`test_db_driver_protocol.py`** (NEW) — DTO round-trip tests
  (`dataclasses.asdict` shape equality), `BaseDBDriver._redact_config`
  coverage (secret keys, URL userinfo across schemes, no-op for non-secret
  configs), `_lazy_import_or_raise` ImportError → ConfigError mapping.
- **`test_mysql_driver.py`** (NEW) — `FakeCursor` / `FakeConn` seams mirroring
  `test_postgresql_driver.py`. Covers: param translation, `connect` kwargs
  construction (discrete fields, URL parsing, `options` merge),
  `execute_write` commit/rollback lifecycle, `describe_schema` Level-1 +
  Level-2 (parses information_schema rows → DTOs, enum parsing,
  auto_increment → generated mapping, FK pairing).
- **`test_sqlite_driver.py`** (NEW) — in-memory `sqlite3` connections (real,
  not faked — SQLite is stdlib and trivially instantiable). Covers: bare path
  + URI mode, `:name` native params (no translation), `RETURNING` since 3.35,
  `describe_schema` via PRAGMAs, identifier-validation regex rejects
  injection attempts, `generated="stored"` mapping from `table_xinfo`.
- **`test_postgresql_driver.py`** — stays green. Minor updates only if any
  assertion depended on the driver method returning a raw dict rather than a
  DTO (those move to the `DbClient` boundary).
- **`test_db_client.py`** — gains cases for `mysql` / `sqlite` type dispatch,
  and serialization of DTO returns via `dataclasses.asdict`.
- **`test_config_models.py`** — `options` field default, ENV override path.
- **`test_packaging.py`** — `[mysql]` extra contents; SQLite absence from
  extras; entry-point group contains all three.

### 9.2 Integration tests (`tests/integration/`)

- **`test_db_commands.py`** — extends with MySQL + SQLite real-connection
  scenarios (testcontainers for MySQL; in-memory or tempfile SQLite). Covers:
  `db query` / `db assert` / `db execute` / `db schema` against each driver.
- **`test_db_drivers_e2e.py`** (NEW or folded into above) — one full
  query/assert/execute/schema cycle per driver to catch dialect-specific
  surprises the unit tests with fakes can't (e.g., MySQL enum literal parsing
  against a real `enum('a','b')` column).

### 9.3 Test discipline

- Unit tests use fakes for MySQL (to match the PostgreSQL pattern and keep
  tests fast/CI-friendly); SQLite tests use real in-memory connections (zero
  infra cost, more realistic than faking a stdlib module).
- Every new driver gets at least one integration test exercising the full
  command surface, not just the driver in isolation.

## 10. Documentation Updates

Triggered via the `docs-watcher` subagent after implementation lands; the
spec lists the target sections so the watcher has a checklist.

- **DESIGN.md §2.1** — `database.connections` schema reference: add `mysql`
  and `sqlite` examples alongside the existing PostgreSQL example; document
  the new `options` field.
- **DESIGN.md §9.1** — DBDriver extension contract: document the DTOs, the
  `BaseDBDriver` mixin, the optional-capability duck-typing contract.
- **ARCHITECTURE.md §3** — module map: add `mysql.py`, `sqlite.py`; note the
  DTO additions to `db_driver_protocol.py`.
- **ARCHITECTURE.md §8** — DB client layer: brief note on per-driver SQL
  param handling (Postgres / MySQL translate `:name` → `%(name)s`; SQLite
  passes through native `:name`).
- **ARCHITECTURE.md §10** — extension points: update the driver-author
  contributor story (copy `sqlite.py` as the template).
- **ARCHITECTURE.md §14** — design-vs-implementation deltas: note that the
  MySQL + SQLite drivers now exist (resolving the PostgreSQL-only gap that
  previously lived here as a known limitation).

## 11. Open Questions / Future Work

- **MySQL typed SSL submodel.** v2 could add a dedicated `ssl:` block on
  `DatabaseConnection` for typed mTLS config, mirroring the Kafka cluster
  SSL block. v1 uses `options` / URL query params.
- **SQLite attached-database support.** v1 returns `schema = "main"`; v2
  could expose attached DBs as separate schemas in Level-1 output.
- **Third built-in optional capability.** If a third optional capability
  emerges (e.g., `explain_plan`, `list_databases`), revisit whether the
  duck-typing probe should become a formal capability registry. Two
  capabilities is below that threshold today.
- **`agctl db drivers` introspection command.** A future user-facing command
  listing available drivers + their capabilities (powered by entry-point
  discovery + the optional-method probe). Not in scope here.
- **SQL dialect-aware templates.** A template could declare per-dialect SQL
  variants (`sql.postgresql`, `sql.mysql`). Explicitly deferred — complex,
  and the agent can already branch on `connection.type` itself.

## 12. Risks

- **PostgreSQL test churn under refactor.** Mitigation: refactor is strictly
  behavior-preserving; the 1321-line test file is the safety net; any test
  that breaks moves the assertion to the `DbClient` boundary rather than
  weakening it.
- **MySQL enum literal parsing edge cases.** Enum values with embedded
  commas or quotes (e.g. `enum('a,b','c''d')`) need careful parsing.
  Mitigation: integration test against a real MySQL enum column; fall back to
  a conservative regex that handles the common cases and surfaces a
  driver-level warning for unparseable literals.
- **SQLite PRAGMA identifier injection.** Mitigation: strict regex
  validation (`^[a-zA-Z_][a-zA-Z0-9_]*$`); reject any identifier that
  doesn't match rather than attempting to escape it.
- **PyMySQL `autocommit=False` default.** PyMySQL's default `autocommit` value
  has historically been `False` but the contract should be set explicitly
  in `connect()` to avoid surprises across versions.
- **Entry-point pollution.** Third-party packages registering `mysql` or
  `sqlite` entry points could shadow built-ins. Mitigation:
  `BUILTIN_DRIVERS.update()` runs last in `load_drivers()`, so built-ins
  always win (existing behavior, unchanged).
