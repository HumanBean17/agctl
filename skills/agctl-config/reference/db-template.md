# `db` mode — extract a DB template

Point at a SQL query / repo method; produce a `database.templates.<name>` block. The
non-negotiable contract (placeholder syntaxes, cross-refs, naming, verify-after) lives in
`SKILL.md` — this file is the extraction detail.

## Inputs

- The config path.
- The artifact: a SQL string, a `@Query` / mapper method, or a repo call.

## Extraction

1. **sql** — the query, with bind params normalized to **`:name`** (JDBC-style).
2. **connection** — match to an existing `database.connections:` key, or omit it to fall back to
   `defaults.database_connection`; if neither fits, create a connection (ask for type/host/dbname;
   `${ENV}` for secrets).
3. **description** — one line.
4. **name** — kebab-case from the query (`SELECT … FROM orders WHERE id = :orderId` → `find-order`).

## Normalize bind params → `:name`

| Source style | Example | agctl form |
|---|---|---|
| JDBC named | `:orderId` | `:orderId` (unchanged) |
| Postgres `$1` / Python `%s` / `?` | `WHERE id = $1` | rename to a meaningful `:orderId` |
| SQLAlchemy named | `:status` | `:status` (unchanged) |
| Some ORMs `.param` / `@name` | `@status` | `:status` |

Positional params (`?` / `$1` / `%s`) carry no name — **invent a clear one** from the
column/meaning (`:orderId`, `:status`).

## Stack snippets

### Spring (JVM)

- `@Query("SELECT … WHERE id = :id")` → sql is already in `:name` form.
- `JdbcTemplate.queryForObject(sql, id)` → take the `sql` string; map positional `?` to `:name`.

### Python

- SQLAlchemy `text("… :status …")`, `psycopg` `cur.execute("… %s …")` → normalize to `:name`.

### Node

- `pg` `client.query("… $1 …")`, `knex` `.where({ id })` → normalize to `:name`.

## What to clarify

- Which connection to bind to (if several exist, or none).
- type / host / dbname + env-var names if you must create a connection.
- Meaningful param names when the source used positional placeholders.

## Where it writes

Under `database.templates:` (nested under `database:`). Omit `connection:` to fall back to
`defaults.database_connection`.

## Gotchas

- SQL uses **`:name`** only — never `{}` (HTTP) or `${}` (env).
- Don't put a `:` bind inside a string literal (`'FAILED:foo'`) — agctl rewrites `:name`→`%(name)s`
  and may mis-rewrite it. `::` casts (e.g. `::jsonb`) are protected and safe.
- agctl runs queries **read-only (no commit)**. An `INSERT`/`UPDATE`/`DELETE` template executes
  but won't persist — these are read templates. If the user wants a write, flag that agctl isn't
  the right tool for it.
- If this template will later feed `db assert --expect-value`, the comparison is **type-aware**
  (`0` ≠ `"0"`); pick `--path` / `--equals` accordingly when you run it.
