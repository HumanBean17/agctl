# Design: Live DB Schema Discovery — `agctl db schema`

**Status:** Approved (design, spec-reviewed) — ready for implementation plan
**Date:** 2026-07-04
**Revised:** 2026-07-04 — applied four-subagent spec-review findings (generated/identity + enum-values + comments added to v1 for SQL-authoring correctness; probe moved before connect; pg_catalog privilege disclosure; D4 rationale corrected; partition leaves excluded; §7 trimmed to design altitude; candidate-enriched ambiguity error; test plan expanded; N+1 round-trip cost acknowledged).
**Author:** brainstorming session
**Affects:** `agctl db` command group, `PostgreSQLDriver` (new optional driver capability), `DbClient`, `skills/agctl` runner skill
**Relation to docs:** Adds a capability described here; on implementation, DESIGN.md §3.3 / §4.2 / §9.1 and ARCHITECTURE.md §3 / §7 / §8 / §10 are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl discover` is **config-static**: it reflects only what an operator
hand-encoded into `agctl.yaml` (HTTP templates, Kafka patterns, DB templates,
services). It never touches a live database. The `db` group can run SQL
(`query` / `assert` / `execute`) but only against SQL the operator or agent
already wrote.

**Use case this blocks:** an autonomous agent that *generates or reproduces* test
cases must sometimes author SQL against tables it has never seen. To write a valid
`SELECT`/`JOIN`/`INSERT` it needs the **actual** schema of a configured connection
— table names, columns, data types, nullability, the primary/foreign/unique keys
that make joins and constraints legible, and enough column metadata (identity vs
generated vs enum) to avoid writing SQL the database will reject. Today there is
no `agctl` command that returns this; the agent would have to read
`agctl config show` (templates only, not the live schema) or shell out to a DB
client outside the one-JSON-envelope contract.

This spec adds a read-only, navigable, live schema-discovery command.

## 2. Goals

- Let an agent learn the **live** structure of a configured DB connection — enough
  to author valid SQL — through the same one-JSON-envelope contract as every other
  command.
- Keep it **progressive** ("a map, not a dump"): the agent lists tables, then
  drills into the one it cares about, mirroring the `discover` philosophy so token
  cost scales with what the task needs, not with database size.
- Include the **column metadata that decides SQL validity** — identity/generated
  columns, enum allowed-values, and comments — so an agent can author autonomous
  SQL rather than falling back to `psql` for half of real schemas (§9, D9).
- Make it **read-only and ungated**: works against any connection, including
  read-only ones; never requires `writable: true`, never accepts `--write`.
- Make it **extensible**: schema introspection is an optional driver capability
  (the same probe pattern as `execute_write`), so the built-in `PostgreSQLDriver`
  implements it now and a future MySQL/SQLite driver can add it without touching
  core.

> **Round-trip cost, acknowledged:** because `agctl` is stateless, each level is a
> fresh process + config load + DB connect. Authoring a multi-table `JOIN` costs
> one Level-1 call plus one Level-2 call per relation involved (N+1). This is
> accepted for v1; an inline mode that returns a table plus its FK-referenced
> tables in one envelope is the immediate next slice (§12).

## 3. Safety & Threat Note

Schema discovery is strictly read-only catalog introspection. It executes no
user-supplied SQL and writes nothing. Accordingly:

- It carries **no write-safety gates** — `writable`/`mode`/`--write` are
  irrelevant. Any configured connection is eligible.
- It does **not** bypass DB privileges: the connection's DB account must be
  permitted to read the catalog.

> **Privilege visibility (load-bearing caveat):** the chosen postgres source,
> `pg_catalog`, returns metadata for **all objects in the cluster regardless of the
> connection role's table-level privileges** — unlike `information_schema`, which
> filters by the current role's privileges. An operator pointing `agctl` at a
> shared cluster will see the structure of tables this connection cannot `SELECT`
> from. If the agent should not see unrelated schemas, point schema discovery at a
> dedicated least-privilege connection (or a specific database, not a shared
> cluster). `pg_catalog` is chosen anyway because it gives accurate type names and
> verbatim default/constraint expressions (D4), matching what `psql \d` shows.

(No multi-layer threat model is needed here, unlike `db execute`; there is no
mutation surface to guard.)

## 4. Non-Goals

- **No SQL generation.** `agctl` returns structure; the agent (or operator) writes
  the SQL. Consistent with the existing "agctl does not auto-generate tests"
  non-goal (DESIGN §1).
- **No deep metadata.** Indexes, check constraints, sequences, view definitions,
  materialized views, foreign tables, partition internals, and collations are
  **deferred** (§12). The first cut covers columns (incl. identity/generated/enum/
  comment) + PK/FK/unique — the relational minimum for authoring valid queries.
- **No row counts or sampling.** Row-count estimates (`pg_class.reltuples`) are
  stale and misleading; live `COUNT(*)` is expensive. Out of scope.
- **No inline / multi-table modes in v1.** `--with-columns` (Level 1) and
  `--with-fk-targets` / `--depth` (Level 2) are deferred to the next slice (§2, §12).
- **No cross-connection schema listing.** One connection per invocation.
- **No new error types.** Existing types cover every path (§9).
- **No change to the read/write SQL paths.** `db query` / `assert` / `execute`,
  `DBDriver.execute`, and `DBDriver.execute_write` are untouched.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Command surface = one new `agctl db schema` command with flag-driven levels** (no `--table` → table list; `--table X` → one table's detail). | Mirrors `discover`'s progressive, flag-driven shape (Level 1/2) rather than nested subcommands. Keeps the `db` group flat and the navigation model one the agent already knows. |
| D2 | **`discover` is untouched.** Schema discovery is a **live**, per-connection operation; `discover` is cheap and config-only (no network). Mixing a live category into `discover` would muddy its "config-only, no connection" contract. | Preserves the discover contract (ARCHITECTURE §4, DESIGN §3.7) and keeps concerns separated. |
| D3 | **`describe_schema` is an optional driver capability, not a required `DBDriver` Protocol method** — probed at runtime like `execute_write`. The Protocol is unchanged; only `PostgreSQLDriver` gains a method. | The `DBDriver` Protocol is a published extension surface (DESIGN §9.1, ARCHITECTURE §10). A new *required* method would break out-of-tree read-only drivers. Probe + clean `ConfigError` if absent. |
| D4 | **The capability contract is portable; the catalog queries are per-driver.** The built-in `PostgreSQLDriver` reads `pg_catalog` (what `psql \d` uses), not `information_schema`. The accurate type name, default expression, identity/generated flags, enum values, and comments all come from `pg_catalog`/`pg_attribute`/`pg_enum`/`pg_description`. | `information_schema.columns.udt_name` does carry the underlying type name (e.g. `jsonb`), so `information_schema` is a *usable fallback* — but it splits type modifiers across columns and gives neither default/constraint expressions nor identity/generated flags. `pg_catalog`'s `format_type` returns type+modifiers in one shot (`character varying(255)`, `timestamp(6) with time zone`); `pg_get_expr` returns default/constraint expressions verbatim; `pg_attribute.attidentity`/`attgenerated`, `pg_enum`, and `pg_description` supply the v1 column richness (D9). The **normalized dict** the driver returns is the portable part a future MySQL/SQLite driver reproduces from its own accurate catalog. (Trade-off: `pg_catalog` is cluster-wide and not privilege-filtered — §3.) |
| D5 | **Level 1 includes views**, marked by `kind`. `--table` accepts any relation (table or view). | Agents frequently `SELECT` from views; excluding them hides queryable relations. `pg_class.relkind` distinguishes them at no extra cost. |
| D6 | **Default scope = non-system schemas, partition leaves excluded.** Relations in schemas whose name starts with `pg_` or equals `information_schema` are excluded; partition *leaf* relations are excluded (only partitioned parents appear). `--schema <name>` narrows to one namespace at both levels. | Matches the agent's intent ("what tables exist in *this* DB") without catalog noise — and a 100-partition table surfaces as one row, not 101. |
| D7 | **`--table` resolution is fail-loud and actionable.** A table that does not exist → `ConfigError` (exit 2) with a hint to run Level 1. A name matching multiple schemas with no `--schema` → `ConfigError` whose `detail` lists the candidate `{schema, kind}` pairs so the agent can re-issue `--schema <x>` without an extra round trip. `--table` matches the catalog's stored case exactly. | Agctl-idiomatic fail-loudly (DESIGN §1); a typo must not masquerade as an empty table, and an ambiguity must not cost a wasted call. |
| D8 | **Connection resolution is reused, not duplicated.** `db schema` needs only the connection-resolution half of `resolve_db_request` (explicit `--connection` → `defaults.database_connection`, else `ConfigError`); the implementation extracts that half into a shared helper used by the SQL commands and `db schema`, then layers the feature on top. | Avoids drift between two resolution paths; small, behavior-preserving change to code this feature touches. **Process guard:** the extraction lands as an isolated, behavior-preserving commit (full `tests/unit/test_db_commands.py` unchanged) *before* the feature commit, so `query`/`assert`/`execute` cannot silently regress. (The helper's exact name/signature is the plan's call.) |
| D9 | **v1 surfaces the column metadata that decides SQL validity** — `generated` (identity / stored-generated), `enum_values`, and per-column/per-table `comment`. | Without these an agent authors SQL the DB rejects (`cannot insert into generated column "…"`) or guesses enum values (`invalid input value for enum`) or misreads units/state semantics. All three are cheap catalog reads and convert the feature from "shell out for half of real schemas" to "author autonomous SQL." |

## 6. Command Contract — `agctl db schema`

```
agctl db schema
    [--connection <name>]     # resolves like db query: explicit --connection → defaults.database_connection
    [--schema <name>]         # restrict to one namespace at both levels; default: non-system schemas (D6)
    [--table <name>]          # drill into one relation's columns + keys (Level 2); accepts table or view; absent → Level 1
```

- **Level 1** — `agctl db schema [--connection X] [--schema S]`: list relations
  (tables and views) visible in the connection. Envelope tag `db.schema.tables`.
- **Level 2** — `agctl db schema --table <name> [--schema S] [--connection X]`:
  columns + primary key + foreign keys + unique constraints for one relation.
  Envelope tag `db.schema.table`.

The command follows the established pattern (ARCHITECTURE §4): a thin Click
command selects between **two** `_core` functions, each wrapped in its own
`@envelope(tag)` at import time — the same mechanism `discover` uses for its four
tags. Connection resolution uses the shared helper from D8; no template, no sql,
no params, no `--write`.

**`--table` matching (D7):** the value is matched against the catalog's stored case
exactly (Postgres folds unquoted identifiers to lowercase; an agent should copy the
case from Level 1). With `--schema`, the match is restricted to that namespace and
ambiguity cannot arise.

## 7. Driver Capability — optional `describe_schema`

The **`DBDriver` Protocol is unchanged** (`connect` / `execute` / `close`).
`describe_schema` is an **optional capability method** that introspection-capable
drivers implement; this adds nothing to the published extension contract, so
out-of-tree read-only drivers need no changes.

```
describe_schema(table: str | None, schema: str | None) -> {
    # table is None → Level 1
    items: [ { schema: str, name: str, kind: "table" | "view", column_count: int }… ],
    # table is set  → Level 2 (one entry per matching relation; command disambiguates)
    matches: [ {
        schema, table, kind,
        columns: [ { name, data_type, nullable, default, generated, enum_values, comment }… ],
        primary_key: [str…],
        foreign_keys: [ { name, columns:[str…], references_schema, references_table, references_columns:[str…] }… ],
        unique_constraints: [ { name, columns:[str…] }… ],
        comment: str | None
    }… ]
}
```

- `table=None` → Level 1: `items` restricted to `schema` when given.
- `table="X"` → Level 2: `matches` restricted to `schema` when given (so ambiguity
  cannot arise when `--schema` is set). The command disambiguates the list
  (0 → not-found `ConfigError`; >1 without `--schema` → ambiguity `ConfigError`
  carrying the candidates; otherwise emits the single object).

The driver owns the catalog queries (D4); `PostgreSQLDriver.describe_schema`:

- Lazy-imports `psycopg` exactly like `execute`/`execute_write` (covered by the
  existing `db` extra; no new packaging) and runs **only catalog `SELECT`s** — no
  user SQL, no writes.
- Reads its data from `pg_catalog`: relations + kinds + partition exclusion +
  column counts (`pg_class`/`pg_namespace`, `relkind`, `relispartition`); columns
  with type via `format_type`, nullability, default via `pg_get_expr`, identity via
  `attidentity`, stored-generated via `attgenerated` (`pg_attribute`/`pg_attrdef`);
  enum allowed-values (`pg_type`/`pg_enum`); comments (`pg_description`); and
  primary/foreign/unique constraints with column-number → name resolution
  (`pg_constraint` `conkey`/`confkey`). The exact joins, filters, and function
  calls are the implementation plan's concern, not this spec's.
- Is unit-testable without a real DB via the existing `connectable` injection seam
  (a fake connection/cursor returning canned catalog rows).

**Capability probe — before connect (fail-fast).** `DbClient` exposes a
`supports_describe_schema` check (probes the selected driver for a *callable*
`describe_schema`). The command runs this check **before** `client.connect()`, so a
driver lacking the capability raises `ConfigError` **without opening a connection**
— matching DESIGN §1 "Fail loudly" and avoiding a wasted TCP+auth round trip. From
`connect` onward the lifecycle mirrors `_execute`: `connect → describe_schema →
close` in `try/finally`. (This differs deliberately from `execute_write`, whose
probe fires after connect; the pre-connect probe is the documented exception.)

### 7.1 Rejected alternatives (ADR-style)

- **Fold schema into `discover` as a live category.** Rejected (D2): breaks the
  config-only/cheap contract of `discover` and forces a DB connection into a
  command that today needs none.
- **`describe_schema` as a required `DBDriver` Protocol method.** Rejected (D3):
  breaks the published extension surface for out-of-tree drivers.
- **One-shot `--dump` of the entire schema in one envelope.** Rejected: violates
  "map, not a dump"; output scales with DB size and blows the agent's context.
- **`information_schema` as the postgres source.** Rejected (D4): it splits type
  modifiers across columns and gives neither default/constraint expressions nor
  identity/generated flags. (Its `udt_name` does carry the type name, so it remains
  a usable fallback for a driver whose native catalog is inaccessible.)
- **Reusing `db query` with built-in introspection SQL.** Rejected — and the real
  reason is the one §1 identifies: the agent does **not know** the `pg_catalog`
  introspection SQL, so it cannot compose its way out via `db query --sql`. The
  knowledge gap is the problem a dedicated command solves; surfacing it as a
  discover snippet would just move the burden onto the agent.
- **Returning catalog rows verbatim.** Rejected: the driver must normalize into the
  documented dict so the contract is stable and dialect-agnostic at the seam.

## 8. Result Contract

All output is the standard envelope (DESIGN §4.1). `result` is command-specific.

> **Identifier quoting (usage note for the consuming agent):** identifiers
> containing uppercase letters, characters outside `[a-z0-9_]`, or matching a
> Postgres reserved word MUST be double-quoted in authored SQL (e.g. `"OrderItems"`,
> `"user"`), or Postgres silently folds them and reports "relation does not exist."

### 8.1 `db.schema.tables` (Level 1)

```json
{
  "ok": true,
  "command": "db.schema.tables",
  "result": {
    "connection": "main-db",
    "schema_filter": null,
    "count": 4,
    "items": [
      { "schema": "public", "name": "customers",   "kind": "table", "column_count": 3 },
      { "schema": "public", "name": "orders",      "kind": "table", "column_count": 6 },
      { "schema": "public", "name": "order_items", "kind": "table", "column_count": 4 },
      { "schema": "public", "name": "order_view",  "kind": "view",  "column_count": 3 }
    ],
    "hint": "Run 'agctl db schema --table <name> [--schema <name>] [--connection <name>]' for columns and keys"
  },
  "error": null,
  "duration_ms": 18
}
```

- `schema_filter`: the `--schema` value, or `null` when unset.
- `kind`: `"table"` or `"view"` (D5). **v1 includes only these** (`relkind`
  base/partitioned table and view); materialized views, foreign tables, sequences,
  and indexes are excluded (deferred, §12).
- `column_count`: number of columns (cheap catalog count, not a row count).
- An unknown/empty `--schema` yields `count: 0`, `items: []`, `ok: true` (not an
  error — the agent learns the namespace is empty/absent).

### 8.2 `db.schema.table` (Level 2)

```json
{
  "ok": true,
  "command": "db.schema.table",
  "result": {
    "connection": "main-db",
    "schema": "public",
    "table": "orders",
    "kind": "table",
    "comment": "Customer orders; status is an enum state machine; total_cents is in cents.",
    "columns": [
      { "name": "id",          "data_type": "uuid",                     "nullable": false, "default": null,                 "generated": null,               "enum_values": null,   "comment": null },
      { "name": "status",      "data_type": "order_status",             "nullable": false, "default": "'PENDING'::order_status", "generated": null,          "enum_values": ["PENDING","PAID","CANCELLED"], "comment": "lifecycle state" },
      { "name": "total_cents", "data_type": "integer",                  "nullable": false, "default": "0",                  "generated": null,               "enum_values": null,   "comment": "cents, not dollars" },
      { "name": "customer_id", "data_type": "uuid",                     "nullable": false, "default": null,                 "generated": null,               "enum_values": null,   "comment": null },
      { "name": "payload",     "data_type": "jsonb",                    "nullable": true,  "default": null,                 "generated": null,               "enum_values": null,   "comment": null },
      { "name": "row_total",   "data_type": "integer",                  "nullable": false, "default": null,                 "generated": "stored",           "enum_values": null,   "comment": "sum(item totals)" },
      { "name": "audit_id",    "data_type": "integer",                  "nullable": false, "default": null,                 "generated": "always_identity",  "enum_values": null,   "comment": null },
      { "name": "created_at",  "data_type": "timestamp with time zone", "nullable": true,  "default": "now()",              "generated": null,               "enum_values": null,   "comment": null }
    ],
    "primary_key": ["id"],
    "foreign_keys": [
      { "name": "orders_customer_id_fkey", "columns": ["customer_id"],
        "references_schema": "public", "references_table": "customers", "references_columns": ["id"] }
    ],
    "unique_constraints": [
      { "name": "orders_status_customer_key", "columns": ["status", "customer_id"] }
    ],
    "hint": "Use these columns in 'agctl db query' / 'db assert --sql' with :paramName bind params."
  },
  "error": null,
  "duration_ms": 12
}
```

- The `result` is the **single disambiguated match** from the driver's `matches`
  list (§7); 0 or >1 matches surface as `ConfigError` before emit (§9).
- `data_type`: accurate type+modifiers from `format_type` (e.g. `integer`, `text`,
  `uuid`, `jsonb`, `timestamp with time zone`, `character varying(255)`, or an enum
  type name) — D4.
- `nullable`: boolean.
- `default`: the column's default expression as text (e.g. `'PENDING'::order_status`,
  `nextval('orders_id_seq'::regclass)`, `now()`), or `null`. **`null` when
  `generated` is non-null** — identity/stored-generated columns have no literal
  default the agent may supply.
- `generated` (D9): `"always_identity"` (`GENERATED ALWAYS AS IDENTITY` — the agent
  MUST omit this column from `INSERT`), `"by_default_identity"` (`GENERATED BY
  DEFAULT AS IDENTITY` — may omit or supply), `"stored"` (`GENERATED ALWAYS AS (…)
  STORED` — cannot be inserted or updated), or `null`. A `serial` column reports
  `generated: null` with a `nextval(...)` `default` (the agent may supply or omit).
- `enum_values` (D9): the enum's allowed values in declared order, **only** for
  enum-typed columns; `null` otherwise.
- `comment` (D9): the column's `COMMENT ON` text, or `null`. A table-level
  `comment` is also surfaced.
- `primary_key`: column names (empty list if none).
- `foreign_keys`: one entry per FK; **`columns[i]` corresponds positionally to
  `references_columns[i]`**, preserving the declared FK pairing. Self-referencing
  and multi-column FKs are represented by the same shape.
- `unique_constraints`: one entry per `UNIQUE` **constraint** (unique *indexes*
  that are not constraints are deferred with other index metadata — §12).
- Column lists are ordered by catalog attribute order; columns within a constraint
  are ordered by their position in that constraint.

## 9. Error & Exit-Code Model

No new error types. Existing types (`agctl/errors.py`) cover every path. The
capability probe runs **before** `connect` (§7); everything else runs before/around
the connection as applicable (fail-loudly, fail fast).

| Failure | Type | Exit |
|---|---|---|
| No connection resolvable (no `--connection`, no `defaults.database_connection`) | `ConfigError` | 2 |
| Unknown connection name | `ConfigError` | 2 |
| Connection's driver lacks `describe_schema` (probed before connect) | `ConfigError` | 2 |
| `--table` matches no relation (Level 2) | `ConfigError` (+ hint to run Level 1) | 2 |
| `--table` matches multiple schemas and no `--schema` given (Level 2) | `ConfigError`, `detail.candidates = [{schema, kind}…]` | 2 |
| DB unreachable / auth failure / catalog read error | `ConnectionFailure` | 2 |

`AssertionFailure` (exit 1) does not apply — `db schema` makes no assertion. An
unknown/empty `--schema` at Level 1 is `ok: true`, `count: 0` (not an error).

## 10. Testing Strategy

Mirrors the existing architecture (ARCHITECTURE §12). D8's extraction lands first
as an isolated, behavior-preserving commit (existing `tests/unit/test_db_commands.py`
unchanged).

**Unit (no DB):** inject a fake driver through the existing seams
(`db_commands.new_db_client`, `DbClient(driver=…)`). Extend the `FakeDriver` test
double with a `describe_schema` method returning canned data. Cover:
- Level 1 shape: correct items / count / hint; `kind` for tables and views;
  partition-leaf exclusion; `--schema` filter applied; empty schema → `count: 0`,
  `ok: true`.
- Level 2 shape: columns ordered, with `data_type`/`nullable`/`default`/`generated`/
  `enum_values`/`comment`; `primary_key`; `foreign_keys` (with references, positional
  pairing, self-reference, multi-column); `unique_constraints`; table `comment`.
- Optional-capability refusal: a driver without `describe_schema` → `ConfigError`,
  and **`connect` is never called** (probe is pre-connect — assert it).
- Connection resolution: no resolvable connection → `ConfigError`; unknown
  connection → `ConfigError`.
- `--table` not found → `ConfigError`; ambiguous across schemas without `--schema`
  → `ConfigError` **with candidate detail**; same name resolved when `--schema`
  supplied; case-mismatched `--table` (e.g. `Orders` vs `orders`) → not-found.

**Driver unit (`tests/unit/test_postgresql_driver.py`):** inject a fake
connection/cursor (`connectable=`). **Test-infra note:** `describe_schema` issues
several distinct catalog `SELECT`s, so the fake cursor must dispatch by SQL text or
queue result sets (the existing single-rowset `FakeCursor` is insufficient). Assert
the driver assembles the normalized dict correctly: `relkind` → `kind`; partition
leaves excluded; matview/foreign-table rows excluded; `format_type`/`pg_get_expr`
pass-through; `attnotnull` → `nullable`; `attidentity`/`attgenerated` → `generated`
(incl. `stored` redacting `default` to null); enum columns populate `enum_values`;
comments populated; `conkey`/`confkey` number→name resolution; constraint/column
ordering.

**Integration (self-skipping):** via the existing `require_postgres` fixture
(`tests/integration/conftest.py`). Create tables with a PK, a multi-column FK to a
second table, a `UNIQUE` constraint, a `jsonb` column, an enum column, a
`GENERATED ALWAYS AS IDENTITY` column, a `GENERATED ALWAYS AS (…) STORED` column,
and column/table comments; include a view; assert the real introspected shape.
Self-skips without `AGCTL_TEST_LIVE=1` / `AGCTL_TEST_PG_DSN`.

## 11. Docs & Skill Impact

- **DESIGN.md** — §3.3 gains the `db schema` command (flags, two levels, read-only);
  §4.2 gains the `db.schema.tables` and `db.schema.table` result shapes; §9.1 notes
  `describe_schema` as an optional driver capability.
- **ARCHITECTURE.md** — §3 module map notes the new command in
  `commands/db_commands.py`; §7 adds the new error rows; §8 documents the optional
  `describe_schema` capability, the pre-connect `supports_describe_schema` probe,
  and the `pg_catalog` introspection source; §10 notes `describe_schema` as an
  optional capability (Protocol unchanged). (Note: the `db` group becomes mixed —
  `schema.*` carries a `hint` like `discover`, unlike `query`/`assert`/`execute`;
  this is intentional, not a drift to "fix.")
- **`skills/agctl/SKILL.md`** (the runner skill) — add `db schema` to the
  command-forms table and a short "discover live schema before authoring SQL"
  section (including the identifier-quoting note and that identity/`stored` columns
  must be omitted from `INSERT`).

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").

## 12. Open Questions / Out of Scope (deferred)

- **Inline / multi-table modes** — the **immediate next slice**. `--with-columns`
  (Level 1 embeds columns) and `--with-fk-targets` / `--depth 1` (Level 2 also
  returns the column/key shape of FK-referenced tables) cut the N+1 cold-start cost
  of authoring multi-table `JOIN`s (§2). Deferred only to keep v1 minimal.
- **Indexes, check constraints, sequences, view definitions, materialized views,
  foreign tables, partition internals, collations, RLS, triggers** — deferred. v1
  is the relational minimum plus the validity-deciding column metadata (D9).
- **Row-count estimates / sampling** — out of scope (§4); stale estimates mislead.
- **Non-Postgres drivers** — the capability is designed for them (D3/D4); actual
  MySQL/SQLite drivers are separate efforts.
- **Schema caching across invocations** — out of scope; `agctl` is stateless
  (ARCHITECTURE §2), and the agent re-fetches only what the current task needs.
