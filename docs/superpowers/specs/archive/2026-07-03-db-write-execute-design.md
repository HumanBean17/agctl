# Design: DB Write Support — `agctl db execute`

**Status:** Approved (design, revised) — ready for implementation plan
**Date:** 2026-07-03
**Revised:** 2026-07-03 — applied spec-review findings (threat-model framing; `writable`/`mode` made non-env-overridable; `execute_write` made an optional driver capability; explicit write-target requirement; commit-after-materialize ordering; contract clarifications).
**Author:** brainstorming session
**Affects:** `agctl db` command group, `database` config schema, config resolver, DB driver layer, discovery, `agctl-config` skill
**Relation to docs:** Adds a capability described here; on implementation, DESIGN.md §3.3 / §2.1 and ARCHITECTURE.md §8 / §3 are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl` can *observe* every system state but *create* none of its own database
state. The two existing `db` commands (`db query`, `db assert`) are read-only,
and that read-only-ness is enforced at the driver layer — `PostgreSQLDriver.execute()`
runs a statement, returns dict rows, and **never commits**; under psycopg3's
default `autocommit=False` the statement lives in a transaction that rolls back
when the connection closes, so an `INSERT`/`UPDATE`/`DELETE` passed to `db query`
is discarded (full mechanism in ARCHITECTURE §8). Additionally the read result
contract assumes a row set, which does not fit write statements, and nothing in
config distinguishes a read from a write.

**Use case this blocks:** seeding test data to reproduce a test case or bug
(insert a row in a known state, flip a row to a broken state, delete rows to
reset between runs). This spec adds a first-class, deliberately-gated write path.

## 2. Goals

- Let an agent seed and mutate DB state (INSERT / UPDATE / DELETE) for the
  reproduce-a-bug workflow.
- Provide a **multi-layer safety model — two runtime gates plus a CI-time lint**
  — that targets misconfiguration and makes every write explicit, with DB user
  privileges as the backstop against hostile environments (see §3).
- Keep read and write on **type-distinct** code paths so an agent cannot run a
  write through the read path (or vice versa).
- Leave the existing read path byte-for-byte unchanged — no regression.
- Make write templates **reviewable**: a write template targets a writable
  connection, caught by `agctl config validate` before deploy.

## 3. Threat Model

`agctl` is invoked by an agent that has **shell access**. It is a convenience /
orchestration layer, **not a security boundary**. A determined actor with shell
access can bypass `agctl` entirely (`psql`, direct config edits, arbitrary env
vars). This design does **not** attempt to stop such an actor — that is the job
of **DB user privileges** (point `agctl` at a read-only account for
prod/staging). `agctl`'s gates are complementary to, not a replacement for,
least-privilege DB accounts.

What the model **does** target:

1. **Misconfiguration** — a write reaching a connection that should be read-only
   due to a typo, a wrong default, or a misrouted `--connection`.
2. **Footgun-reduction** — every write is explicit (`--write`) and names an
   explicit target, self-evident in transcripts.
3. **Review-time catching** — write templates are provably routed to writable
   connections, enforced by `agctl config validate` (§8).

**Consequence for the gate model:** the two runtime gates (§6.1) are
defense-in-depth against *mistakes*, not independent cryptographic factors
against an *adversarial principal*. They are made **honest** by two rules:
`writable`/`mode` are file-only properties not settable via `AGCTL_*` overrides
(§7.2), so config-file review is truthful; and `db execute` must name its target
explicitly (§6.3), so a write can never silently land on a default connection.

## 4. Non-Goals

- **Adversarial-agent containment.** Out of scope (§3); handled by DB privileges.
- **Multi-statement batching as a feature.** One invocation commits one unit of
  work — the SQL string as given (§13). No statement splitting/counting.
- **Idempotency magic.** `agctl` stays dumb; idempotency is the SQL author's job.
- **SQL parsing / DDL blocking.** A statement parser is a fragile guardrail;
  safety comes from connection opt-in, not parsing.
- **Typed per-verb commands** (`db insert`/`update`/`delete`).
- **Changing the read path.** `db query`/`db assert` and `DBDriver.execute` are
  untouched.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Safety = two runtime gates + a CI-time lint, targeting misconfiguration (§3).** Runtime gate 1: connection is `writable: true`. Runtime gate 2: invocation carries `--write`. CI lint: write templates route to writable connections (§8). | Honest defense-in-depth given the threat model. The connection flag is the primary guard against misrouting; `--write` is intentional friction + an audit signal; DB privileges are the adversarial backstop. |
| D2 | **Command surface = one new `agctl db execute` command.** | Read/write stay on separate paths; the type-distinctness is itself part of the safety model. Honors DESIGN §8 (a write cannot be composed from reads). |
| D3 | **Config = optional `mode` field on `database.templates`, not a second section.** | One list; read/write is a property of a template. |
| D4 | **`execute_write` is an optional driver capability, not a required `DBDriver` Protocol method.** | The `DBDriver` Protocol is a published extension surface (DESIGN §9.1, ARCHITECTURE §10). A new *required* method would break out-of-tree drivers. Instead `DbClient` probes for the method and raises a clean error if absent (§9). |
| D5 | **`db execute` must name an explicit target** (`--template` or `--connection`); a bare write to the default connection is refused (§6.3). | Closes the highest-blast-radius footgun: a destructive free-form write silently routing to `defaults.database_connection`. Asymmetric with `http request`; writes earn it. |
| D6 | **`writable`/`mode` are non-overridable via `AGCTL_*` env** (resolver denylist, §7.2). | Writability is a deployment-time, file-only property; allowing env override makes config-file review misleading and collapses the connection gate (the same principal controls both). |

## 6. Command Contract — `agctl db execute`

```
agctl db execute
    [--template <name>]         # write-mode template from database.templates
    [--sql "..."]               # free-form write SQL (escape hatch); mutually exclusive with --template
    --write                     # REQUIRED confirmation flag
    [--param key=value]         # repeatable; fills :paramName named params
    [--connection <name>]       # names the write target (REQUIRED with --sql; optional override with --template)
```

Input resolution reuses `resolve_db_request` for sql/params/connection
precedence (explicit `--connection` → template `connection` →
`defaults.database_connection`). The mode gate (§6.2) reads the template's
`mode` directly from the config object, not from the resolver (which does not
surface it).

### 6.1 Two runtime gates

Checked **before** opening a connection, in this order, after argument
validation (template XOR sql; target-naming per §6.3):

1. **Invocation gate** — `--write` must be present, else `ConfigError` (exit 2).
2. **Connection gate** — the resolved connection must have `writable: true`,
   else `ConfigError` (exit 2).

(The mode check, §6.2, runs between the two gates.) A gate failure never opens a
database connection (fail loudly, fail fast, ARCHITECTURE §7).

### 6.2 Mode check (both read commands)

The `mode` field is **author-declared** and not verified against the SQL text;
authors are expected to put DML in `write`-mode templates (an expectation, not
enforced).

- `db execute` with a `read`-mode template → `ConfigError`.
- `db query` **or** `db assert` with a `write`-mode template → `ConfigError`.

Both read commands are covered, so a write template cannot be run through either
read path.

### 6.3 Explicit target required

`db execute` must resolve to an explicit write target:

- `--template` names the target via the template's `connection` (optionally
  overridden by `--connection`).
- `--connection` names the target directly (required when using `--sql`).

A bare `db execute --sql "..." --write` with **neither** `--template` nor
`--connection` is a `ConfigError` ("db execute requires --template or
--connection to name the write target; refusing to write to the default
connection implicitly"). Free-form writes remain available — the operator must
simply say *where*.

## 7. Config Schema Changes (`agctl/config/models.py`)

Two additive, backward-compatible fields.

**`DatabaseConnection`** gains:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `writable` | `bool` | `False` | If `true`, this connection is eligible as a write target. Existing configs default to read-only. |

**`DatabaseTemplate`** gains:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `mode` | `Literal["read", "write"]` | `"read"` | Whether the template's SQL mutates state. Determines which command may run it. Pydantic rejects any other value at load. |

### 7.1 Config contract (YAML)

```yaml
database:
  connections:
    main-db:                       # existing; read-only by default (writable omitted → false)
      type: postgresql
      host: "${DB_HOST}"
      dbname: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      default: true

    main-db-writable:              # explicit write target (writable is file-only — not env-settable, §7.2)
      type: postgresql
      host: "${DB_HOST}"
      dbname: "${DB_NAME}"
      user: "${DB_WRITE_USER}"     # typically a separate, write-capable account
      password: "${DB_WRITE_PASSWORD}"
      writable: true

  templates:
    find-order:                    # read (mode omitted → "read")
      sql: "SELECT id, status FROM orders WHERE id = :orderId"

    seed-pending-order:            # write
      description: "Idempotently seed a PENDING order for bug reproduction"
      mode: write
      connection: main-db-writable
      sql: >
        INSERT INTO orders (id, status, total_cents)
        VALUES (:orderId, 'PENDING', :totalCents::int)
        ON CONFLICT (id) DO NOTHING
```

> **Param typing note:** `--param` values are bound as **strings** (`parse_params`
> yields `dict[str, str]`). Numeric/timestamp columns require an explicit
> `::cast` in the SQL (e.g. `:totalCents::int`), an inherited read-path
> constraint that applies equally to writes.

### 7.2 Env-override denylist (`agctl/config/resolver.py::apply_env_overrides`)

`writable` and `mode` are **denylisted** from `AGCTL_*` overrides. An override
resolving to either leaf field raises `ConfigError` at load
("writable/mode cannot be set via AGCTL_* override; set it in agctl.yaml").
This keeps writability a truthful, reviewable, file-only property and prevents
the connection gate from being collapsed by the same principal that supplies
`--write` (§3). All other override behavior is unchanged.

## 8. Validation Rules — CI-time lint (`agctl/config/validator.py`)

One new cross-reference rule, alongside the existing DB-template→connection check:

> For every `database.templates[*]` with `mode == "write"`, the resolved
> connection (the template's own `connection`, else `defaults.database_connection`)
> must exist **and** have `writable == true`. Otherwise: config validation error
> (exit 2).

**Scope — honest about when this runs:** this rule is evaluated by
`agctl config validate` (the review/CI-time check). It is **not** wired into the
per-command load path (`load_config` does not run `validate_config`). Therefore
the **runtime connection gate (§6.1) is the real per-invocation guard**; this
lint is a complementary review-time catch that surfaces misrouted write
templates *before* deploy. (The static check is intentionally narrower than the
runtime resolution: it cannot see `--connection`, which is a runtime flag — that
case is covered by the runtime gate.)

## 9. Driver Capability — optional `execute_write`

The **`DBDriver` Protocol is unchanged** (`connect` / `execute` / `close`).
`execute_write` is an **optional capability method** that write-capable drivers
implement; this adds nothing to the published extension contract, so out-of-tree
read-only drivers need no changes.

> **Read-path contract (inherited assumption):** the safety of free-form
> `db query --sql "<write>"` resting inert relies on the driver honoring the
> read-only `execute` contract (no autocommit). The built-in `PostgreSQLDriver`
> guarantees this via psycopg3's default; **third-party drivers must honor it
> too** — a driver that autocommits would turn `db query` into an ungated write
> path. This is a documented driver obligation, not enforced by `agctl`.

**`DbClient.execute_write`** probes for the method: if the selected driver lacks
it, raise `ConfigError` ("connection's driver (<type>) does not support writes").
The built-in `PostgreSQLDriver` implements it. `execute_write` lazy-imports
psycopg exactly like the read `execute` (ARCHITECTURE §8) and is covered by the
existing `db` optional-extra — no new packaging.

**Contract:**

```
execute_write(sql: str, params: dict) -> {
    "rows_affected": int | None,             # driver-reported count; None when the driver reports no count (e.g. DDL)
    "returning": list[dict]                  # coerced rows from a RETURNING clause (may be multi-row); [] if absent
}
```

Semantics (contract only — implementation is the plan's job):

- Named params use JDBC-style `:name`, rewritten to psycopg's `%(name)s` via the
  existing `convert_sql_params`. **Inherited limitation** (ARCHITECTURE §15):
  `:name` inside a SQL string literal may be mis-rewritten; on the write path
  this can corrupt data rather than mis-read. `::` casts are protected.
- **Runs whatever SQL is given and commits** — including DDL. We deliberately do
  not parse or block statement types (a parser is a fragile guardrail, §4). For
  statements with no affected count, `rows_affected` is `None`.
- `RETURNING` rows are coerced cell-by-cell via the existing `coerce_db_value`,
  identical to read rows.
- **Ordering (correctness contract):** materialize `rows_affected` and
  `returning` (execute, fetch `RETURNING`, coerce) **before** committing; `commit()`
  is the **last** step. Any error after `execute` but before `commit` triggers
  `rollback()`, and the driver error maps to `ConnectionFailure`. This guarantees
  **a reported failure (ok:false) means no commit landed** — so retries driven by
  the envelope's failure signal are safe.
- The injected-`connectable` test rule mirrors the read path: the driver
  commits/rolls back but only `close()`s connections it owns.

### 9.1 Rejected alternatives (ADR-style)

- **`execute_write` as a required `DBDriver` Protocol method.** Rejected (D4):
  breaks the published extension surface for out-of-tree drivers. Optional
  capability + probe avoids this entirely.
- **`commit=True` boolean on a single `execute`.** Rejected: one method serving
  two contracts risks the read path and is the smell DESIGN §8 warns against.
- **Typed `db insert|update|delete` primitives.** Rejected: reinvents the SQL
  surface, largest command surface.
- **Extend `db query` with `--write`.** Rejected: read/write would share one
  path, erasing the type-distinctness the safety model relies on.
- **Separate `database.write_templates` section.** Rejected: doubles config and
  discovery surface; `mode` keeps one list.
- **DDL/DML parser, privileges-only, flag-only safety models.** Rejected (D1/§3).

## 10. Result Contract

`db.execute` envelope (`command: "db.execute"`):

```json
{
  "ok": true,
  "command": "db.execute",
  "result": {
    "rows_affected": 3,
    "returning": [
      { "id": "ord-901", "status": "PENDING" }
    ],
    "connection": "main-db-writable",
    "sql": "INSERT INTO orders (id, status) VALUES (:orderId, 'PENDING') ON CONFLICT (id) DO NOTHING RETURNING id, status"
  },
  "error": null,
  "duration_ms": 41
}
```

- **`rows_affected`** is the count reported by the driver (`cursor.rowcount`),
  **informational, not authoritative**: for `RETURNING` statements it may reflect
  *returned* rows rather than affected rows (driver-dependent). `null` when the
  driver reports no count (e.g. DDL).
- **`returning`** is `list[dict]` and may contain **multiple rows** for
  multi-row `UPDATE...RETURNING` / multi-row `INSERT...RETURNING`; `[]` when the
  statement has no `RETURNING`. Non-JSON-native cells that survive
  `coerce_db_value` are stringified by the envelope's `json.dumps(default=str)`
  (ARCHITECTURE §6), so emission does not fail on exotic types.
- **`sql`** is the SQL text **as resolved** (the template's `sql` or the `--sql`
  arg), with `:paramName` placeholders **intact** (not substituted) — matching
  how `db.assert`'s error detail already echoes SQL. Bound params travel in
  `--param`, not in the echoed `sql`.
- **Terminology:** the read path uses `row_count` (rows returned); the write
  path uses `rows_affected` (rows mutated). Different concepts, deliberately
  different names.

Success is always `ok: true`, exit 0 — including the **0-rows-affected** case
(e.g. an `UPDATE` matching nothing, or `ON CONFLICT DO NOTHING` on an existing
row). 0-affected is a successful no-op write, not a failure.

## 11. Discovery Changes (`agctl/commands/discover_commands.py`)

No new category. The `db-templates` category gains a `mode` marker — a
**purely additive** key (`{name, description}` → `{name, description, mode}`);
existing keys are unchanged, so consumers ignoring unknown keys are unaffected.

- **Level 1 (category listing):** each item adds `"mode": "read" | "write"`.
- **Level 2 (item detail):** a write template reports `mode: "write"` with an
  example of the form `agctl db execute --template <name> --write`; a read
  template keeps `agctl db query --template <name>`.
- **Search:** matches carry `mode`.

## 12. Error & Exit-Code Model

No new error types. Existing types (`agctl/errors.py`) cover every path. Gate
checks run in the order: **argument validation → invocation gate (`--write`) →
mode check → connection gate**, all before connecting.

| Failure | Type | Exit |
|---|---|---|
| `--template` and `--sql` both given (or neither) | `ConfigError` | 2 |
| `db execute` with neither `--template` nor `--connection` (no explicit target) | `ConfigError` | 2 |
| Missing `--write` | `ConfigError` | 2 |
| Template `mode` mismatched to the command (read↔execute, on `query`/`assert`/`execute`) | `ConfigError` | 2 |
| Resolved connection not `writable: true` | `ConfigError` | 2 |
| Connection's driver lacks `execute_write` | `ConfigError` | 2 |
| `AGCTL_*` override targets `writable`/`mode` | `ConfigError` | 2 |
| Unknown template | `TemplateNotFound` | 2 |
| Unknown connection | `ConfigError` | 2 |
| Write/commit failure, driver error | `ConnectionFailure` | 2 |

`AssertionFailure` (exit 1) does not apply — `db execute` does not assert.

## 13. Transaction Semantics & Idempotency

- **One invocation = one committed unit = the SQL string as given.** The driver
  executes the SQL as a single unit and commits atomically. `agctl` does **not**
  split, count, or reject multiple semicolon-joined statements — if the driver
  runs them, they commit together as one unit. Multi-statement authoring is not a
  *feature* and is not validated; the contract is simply "the SQL string commits
  as one unit." This is consistent with the stateless, one-envelope-per-call model.
- **Idempotency is the author's responsibility**, expressed in SQL (`ON
  CONFLICT … DO NOTHING`, `WHERE NOT EXISTS`, `MERGE`). The `agctl-config`
  authoring skill carries an idempotent-insert example so re-runnable repros are
  the path of least resistance.
- **State scope:** a committed write mutates **SUT** (database) state, not
  `agctl`-local state — `agctl` remains stateless and writes nothing to disk
  locally (ARCHITECTURE §2 invariant holds; the invariant concerns `agctl`-local
  state, not the system under test).

## 14. Testing Strategy

Mirrors the existing test architecture (ARCHITECTURE §12).

**Unit (no DB):** inject a fake driver via the existing seams
(`db_commands.new_db_client`, `DbClient(driver=…)`). Cover:
- Both runtime gate failures: missing `--write`; non-writable connection.
- Explicit-target refusal: bare `db execute --sql … --write` with no target.
- Mode mismatches in **all three** directions (read template → `execute`;
  write template → `query`; write template → `assert`).
- Optional-capability refusal: a driver without `execute_write` → `ConfigError`.
- Env-override denylist: `AGCTL_*__WRITABLE` / `__MODE` → `ConfigError` at load.
- Commit-after-materialize ordering: a write whose result coercion raises is
  rolled back and reported `ok:false` with **no commit**.
- Success path: `rows_affected` + `returning` shape; absent `RETURNING` → `[]`;
  no-count statement → `rows_affected: null`; 0-affected → exit 0.
- Config validation: new `mode`/`writable` fields; the write-template→
  writable-connection cross-ref rule (pass and fail).
- Discovery: `mode` present in Level-1 listing, Level-2 detail, and search.

**Integration (self-skipping):** via the existing `require_postgres` fixture
(`tests/integration/conftest.py`) — a real `db execute` INSERT followed by a
`db query`/`db assert` round-trip confirming the row landed and is visible, plus
a rollback-on-error case. Skips automatically when no Postgres is reachable.

## 15. Docs & Skill Impact

- **DESIGN.md** — §2.1 gains the `writable` connection field and `mode` template
  field; §3.3 gains the `db execute` command (including the explicit-target and
  `--write` rules); §4.2 gains the `db.execute` result shape.
- **ARCHITECTURE.md** — §3 module map notes the new command; §5 documents the
  resolver denylist (`writable`/`mode`); §8 documents the optional
  `execute_write` capability + `DbClient` probe and the commit-after-materialize
  ordering; §7 adds the new error rows; §10 notes `execute_write` is an optional
  capability (Protocol unchanged).
- **`skills/agctl-config`** — add `reference/db-write-template.md` and surface
  write-template authoring (incl. idempotent patterns and `::cast` param typing)
  in `SKILL.md`.
- **`skills/agctl/SKILL.md`** — add `db execute` usage and the seed/`--write`
  pattern.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").

## 16. Open Questions / Out of Scope (deferred)

- **Batch / multi-statement seeding as a feature** — deferred; the agent-chains-
  commands model covers most cases, and the SQL-string-as-one-unit contract (§13)
  handles the rest without new surface.
- **Auto-rollback "seed + cleanup" primitive** (insert-then-delete pairing) —
  deferred; idempotent SQL covers reproducibility.
- **`--dry-run` / explain** — not in scope; a read-only `db query` answers most
  "what will this touch" questions.
- **Env-overridable writability** — explicitly decided **against** (D6/§7.2); if
  a future deployment genuinely needs env-controlled writability, revisit then.
