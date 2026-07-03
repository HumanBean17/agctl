# Design: DB Write Support — `agctl db execute`

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-03
**Author:** brainstorming session
**Affects:** `agctl db` command group, `database` config schema, DB driver layer, discovery, `agctl-config` skill
**Relation to docs:** Adds a capability described here; on implementation, DESIGN.md §3.3 / §2.1 and ARCHITECTURE.md §8 / §3 are synced via `docs-watcher`.

---

## 1. Background & Problem

`agctl` can *observe* every system state but *create* none of its own database
state. The two existing `db` commands (`db query`, `db assert`) are read-only,
and that read-only-ness is not a missing flag — it is an active architectural
property enforced at the driver layer:

- `PostgreSQLDriver.execute()` (`agctl/clients/db_drivers/postgresql.py`) runs a
  statement, calls `fetchall()`, and returns dict rows. It never commits. Under
  psycopg3's default `autocommit=False`, the statement lives in an open
  transaction that rolls back when the connection closes — so an
  `INSERT`/`UPDATE`/`DELETE` passed to `db query` is silently discarded.
- The result contract assumes a row set (`DBDriver.execute → list[dict]`,
  `db query → {rows, row_count}`), which does not fit write statements that
  produce an affected-row count rather than rows.
- `database.templates[*].sql` is free-form text with nothing distinguishing a
  read from a write, so discovery surfaces them identically and nothing in
  config records intent.

**Use case this blocks:** seeding test data to reproduce a test case or bug
(e.g. insert a row in a known state, flip a row to a broken state, delete rows
to reset between runs). This is a routine testing/debugging need that the
current read-only stance makes impossible.

This spec adds a first-class, deliberately-gated write path.

## 2. Goals

- Let an agent seed and mutate DB state (INSERT / UPDATE / DELETE) for the
  reproduce-a-bug workflow.
- Provide two **independent** safety gates so a misconfigured, shared, or
  production connection can never be written to by accident.
- Keep read and write on **type-distinct** code paths so an agent cannot run a
  write through the read path (or vice versa).
- Leave the existing read path byte-for-byte unchanged — no regression to
  anything that works today.
- Make write templates **safe by construction**: a write template provably
  targets a writable connection, enforced at config-load time.

## 3. Non-Goals

- **Multi-statement batches or cross-invocation transactions.** Seeding is
  naturally one statement per template; multi-step setup is the agent chaining
  commands (agctl's existing model, DESIGN §11). Out of scope for v1.
- **Idempotency magic.** agctl stays dumb; idempotency is the SQL author's job
  (`ON CONFLICT`, `WHERE NOT EXISTS`, `MERGE`). The authoring skill carries the
  pattern.
- **SQL parsing / DDL blocking.** A statement parser is a fragile guardrail
  (rejected alternative, §8). Safety comes from connection opt-in, not parsing.
- **Typed per-verb commands** (`db insert`/`update`/`delete`). Rejected
  alternative (§8); SQL already has those verbs.
- **Changing the read path.** `db query`/`db assert` and `DBDriver.execute`
  are untouched.

## 4. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Safety = opt-in per connection + per-invocation `--write` flag.** A connection must declare `writable: true`, and every invocation must carry `--write`. | Defense-in-depth independent of DB user privileges; fits agctl's fail-loudly ethos (DESIGN §1/§8). The connection flag is the seatbelt; `--write` is intentional friction + an audit signal in transcripts. Alternatives (privileges-only, flag-only, DML-parser) rejected in §8. |
| D2 | **Command surface = one new `agctl db execute` command** (sibling to `query`/`assert`), not typed primitives, not an extension of `db query`. | Read/write stay on separate paths; the type-distinctness is itself part of the safety model. Honors DESIGN §8 ("ten focused commands"; a write cannot be composed from reads). |
| D3 | **Config = optional `mode` field on `database.templates`, not a second section.** | One list, not two; read/write is a property of a template. |
| D4 | **Driver = new `execute_write` method on the `DBDriver` protocol**, not a `commit=True` boolean on `execute`. | One method doing two things is the smell DESIGN §8 warns against; read `execute()` stays unchanged. |

## 5. Command Contract — `agctl db execute`

```
agctl db execute
    [--template <name>]         # write-mode template from database.templates
    [--sql "..."]               # free-form write SQL (escape hatch); mutually exclusive with --template
    --write                     # REQUIRED confirmation flag
    [--param key=value]         # repeatable; fills :paramName named params
    [--connection <name>]       # overrides template connection / defaults.database_connection
```

Input resolution reuses the existing `resolve_db_request` machinery (template XOR
free-form SQL, `--param` parsing, connection precedence: explicit arg → template
`connection` → `defaults.database_connection`).

### 5.1 Two safety gates (both required; checked before connecting)

1. **Invocation gate** — `--write` must be present, else `ConfigError` (exit 2).
2. **Connection gate** — the resolved connection must have `writable: true`, else
   `ConfigError` (exit 2).

A gate failure never opens a database connection (fail loudly, fail fast,
ARCHITECTURE §7).

### 5.2 Mode check (templated path only)

- `db execute` with a `read`-mode template → `ConfigError`.
- `db query` with a `write`-mode template → `ConfigError`.

Free-form `--sql` carries no template, so it is gated by the two safety gates
alone — consistent with `http request` being the less-guarded escape hatch
relative to `http call`. (Read-only free-form writes via `db query --sql "INSERT..."`
remain a no-op under today's transaction semantics; writes go through `db execute`.)

## 6. Config Schema Changes (`agctl/config/models.py`)

Two additive, backward-compatible fields.

**`DatabaseConnection`** gains:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `writable` | `bool` | `False` | If `true`, this connection is eligible as a write target. Existing configs default to read-only — zero behavior change. |

**`DatabaseTemplate`** gains:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `mode` | `Literal["read", "write"]` | `"read"` | Whether the template's SQL mutates state. Determines which command may run it. |

### 6.1 Config contract (YAML)

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

    main-db-writable:              # explicit write target
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
        VALUES (:orderId, 'PENDING', :totalCents)
        ON CONFLICT (id) DO NOTHING
```

## 7. Validation Rules (`agctl/config/validator.py`)

One new cross-reference rule, alongside the existing DB-template→connection check:

> For every `database.templates[*]` with `mode == "write"`, the resolved
> connection (the template's own `connection`, else `defaults.database_connection`)
> must exist **and** have `writable == true`. Otherwise: config validation error
> (exit 2).

This is the "safe by construction" property: a write template cannot be defined
against a read-only connection — load fails before any command runs. The existing
read templates require no change. This rule is **complementary, not redundant**,
to the runtime connection gate (§5.1 gate #2): the config rule statically
guarantees declared write templates target a writable connection; the runtime
gate additionally covers free-form `--sql` (which has no template to validate at
load time) and re-confirms the connection at execution.

## 8. Driver Protocol Change (`agctl/clients/db_driver_protocol.py`)

The `DBDriver` Protocol gains a second method. The read method is unchanged.

**Contract:**

```
execute(sql, params) -> list[dict]            # UNCHANGED — read-only, no commit
execute_write(sql, params) -> {
    "rows_affected": int,
    "returning": list[dict]                   # coerced dict rows from a RETURNING clause; [] if absent
}
```

`execute_write` semantics (contract only — implementation is the plan's job):
- Named params use JDBC-style `:name`, rewritten to the driver's native bind
  syntax via the existing `convert_sql_params` (`agctl/resolution.py`).
- On success: the statement is committed.
- On error: the transaction is rolled back, and the driver error maps to
  `ConnectionFailure` (exit 2).
- `RETURNING` rows are coerced cell-by-cell via the existing `coerce_db_value`
  (`agctl/assertions.py`), identical to read rows.

`PostgreSQLDriver` (`agctl/clients/db_drivers/postgresql.py`) implements
`execute_write` against psycopg3; the injected-`connectable` test rule mirrors
the read path (the driver commits/rolls back but only `close()`s connections it
owns). `DbClient` (`agctl/clients/db_client.py`) exposes a thin `execute_write`
delegator alongside the existing `execute`.

### 8.1 Rejected alternatives (ADR-style)

- **`commit=True` boolean on a single `execute`.** Rejected (D4): one method
  serving two contracts is the smell DESIGN §8 warns against, and it risks the
  read path regressing.
- **Typed `db insert|update|delete` primitives.** Rejected: reinvents the SQL
  surface (bulk, WHERE, upsert/MERGE), largest command surface, fights §8.
- **Extend `db query` with `--write`.** Rejected: read/write would share one
  path, erasing the type-distinctness the safety model relies on.
- **Separate `database.write_templates` section.** Rejected: doubles config and
  discovery surface for no gain; `mode` keeps one list.
- **DB-privileges-only / flag-only / DDL-parser safety models.** Rejected (D1):
  privileges-only and flag-only leave real prod-connection footguns; a DDL parser
  is fragile and bypassable.

## 9. Result Contract

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

When the statement has no `RETURNING` clause, `returning` is `[]`. Success is
always `ok: true`, exit 0.

## 10. Discovery Changes (`agctl/commands/discover_commands.py`)

No new category. The `db-templates` category gains a `mode` marker:

- **Level 1 (category listing):** each item adds `"mode": "read" | "write"` so an
  agent scanning the list sees writes at a glance.
- **Level 2 (item detail):** a write template reports `mode: "write"` and an
  example of the form `agctl db execute --template <name> --write`. A read
  template keeps `agctl db query --template <name>`.
- **Search:** matches carry `mode` as well.

This ensures an agent cannot discover a write template and run it through `db
query` by mistake (the mode check in §5.2 refuses it regardless, but discovery
makes intent visible first).

## 11. Error & Exit-Code Model

No new error types. Existing types (`agctl/errors.py`) cover every path:

| Failure | Type | Exit |
|---|---|---|
| Missing `--write` | `ConfigError` | 2 |
| Resolved connection not `writable: true` | `ConfigError` | 2 |
| Template `mode` mismatched to the command (read↔execute) | `ConfigError` | 2 |
| Write template resolves to a non-writable connection (config load) | `ConfigError` | 2 |
| Unknown template/connection | `TemplateNotFound` | 2 |
| Write/commit failure, driver error | `ConnectionFailure` | 2 |

`AssertionFailure` (exit 1) does not apply — `db execute` does not assert; a
write succeeding is always exit 0.

## 12. Transaction Semantics & Idempotency

- **One statement per invocation.** Commit on success, rollback on error. This
  is consistent with agctl's stateless, one-envelope-per-call model and the
  `@envelope` contract (ARCHITECTURE §4/§6).
- **Idempotency is the author's responsibility**, expressed in SQL. The
  `agctl-config` authoring skill carries an idempotent-insert example (e.g.
  `ON CONFLICT … DO NOTHING`) so re-runnable repros are the path of least
  resistance.

## 13. Testing Strategy

Mirrors the existing test architecture (ARCHITECTURE §12).

**Unit (no DB):** inject a fake driver implementing `execute_write` via the
existing seams (`db_commands.new_db_client`, `DbClient(driver=…)`). Cover:
- Both gate failures: missing `--write`; non-writable connection.
- Mode mismatches in both directions (read template → `execute`; write template → `query`).
- Success path: `rows_affected` + `returning` shape; absent `RETURNING` → `[]`.
- Error mapping: write/commit failure → `ConnectionFailure`.
- Config validation: new `mode`/`writable` fields; the write-template→
  writable-connection cross-ref rule (both pass and fail).
- Discovery: `mode` present in Level-1 listing, Level-2 detail, and search.

**Integration (self-skipping):** via the existing `require_postgres` fixture
(`tests/integration/conftest.py`) — a real `db execute` INSERT followed by a `db
query`/`db assert` round-trip confirming the row landed, plus a rollback-on-error
case. Skips automatically when no Postgres is reachable.

## 14. Docs & Skill Impact

- **DESIGN.md** — §2.1 gains the `writable` connection field and `mode` template
  field; §3.3 gains the `db execute` command; §4.2 gains the `db.execute` result
  shape.
- **ARCHITECTURE.md** — §3 module map notes the new command; §8 documents the
  `execute_write` driver method and `DbClient` delegator; §7 error rows.
- **`skills/agctl-config`** — add `reference/db-write-template.md` and surface
  write-template authoring in `SKILL.md`.
- **`skills/agctl/SKILL.md`** — add `db execute` usage and the seed/`--write`
  pattern.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").

## 15. Open Questions / Out of Scope (deferred)

- **Batch / multi-statement seeding** — deferred to v2 if real usage demands it;
  the agent-chains-commands model covers most cases today.
- **Auto-rollback "seed + cleanup" primitive** (insert-then-delete pairing) —
  deferred; idempotent SQL covers reproducibility without new surface.
- **`--dry-run` / explain** — not in scope; a read-only `db query` against the
  same data answers most "what will this touch" questions.
