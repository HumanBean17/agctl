# agctl — config-db: extract a read DB template

Produce a `database.templates.<name>` block from a SQL query / repo method.

## Extraction

1. **sql** — the query, bind params normalized to **`:name`** (JDBC-style).
2. **connection** — match an existing `database.connections:` key, or omit to
   fall back to `defaults.database_connection`; neither fits → create one
   (ask type/url/host/dbname; `${ENV}` for secrets).
3. **description** — one line. **name** — kebab-case from the query
   (`SELECT … FROM orders WHERE id = :orderId` → `find-order`).

## Normalize bind params → `:name`

| Source | Example | agctl form |
|---|---|---|
| JDBC named | `:orderId` | unchanged |
| Postgres `$1` / Python `%s` / `?` | `WHERE id = $1` | meaningful `:orderId` |
| SQLAlchemy named | `:status` | unchanged |
| Some ORMs `.param` / `@name` | `@status` | `:status` |

Positional params carry no name — invent a clear one from the
column/meaning.

## Stack snippets

- **Spring**: `@Query("… :id …")` already `:name`;
  `JdbcTemplate.queryForObject(sql, id)` → map positional `?` to `:name`.
- **Python**: SQLAlchemy `text("… :status …")`, psycopg `%s` → normalize.
- **Node**: `pg` `$1`, knex `.where({id})` → normalize.

## Gotchas

- SQL uses **`:name`** only — never `{}` (HTTP) or `${}` (env).
- Don't put a `:` bind inside a string literal (`'FAILED:foo'`) — agctl
  rewrites `:name`→`%(name)s` and may mis-rewrite it. `::` casts
  (`::jsonb`) are protected and safe.
- **Read templates execute read-only (no commit)** — an INSERT/UPDATE/DELETE
  in a read template runs but won't persist. Writes need `mode: write` (see
  `--topic config-db-write`).
- Feeding `db assert --expect-value`? Comparison is type-aware (`0` ≠ `"0"`)
  — pick `--path`/`--equals` accordingly.

## Clarify (genuine gaps only)

Which connection to bind (if several/none); connection details + env-var
names for a new one; meaningful names for positional params.
