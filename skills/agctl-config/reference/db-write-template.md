# `db` mode — author a write DB template

Point at an INSERT/UPDATE/DELETE SQL statement or repo method; produce a
`database.templates.<name>` block with `mode: write`. The non-negotiable contract
(placeholder syntaxes, cross-refs, naming, verify-after) lives in `SKILL.md` —
this file is the extraction detail for write operations.

## Inputs

- The config path.
- The artifact: a SQL write statement, a `@Insert`/`@Update`/`@Delete` method,
  or a repo call that mutates state.

## Extraction

1. **sql** — the write statement, with bind params normalized to **`:name`**
   (JDBC-style).
2. **connection** — must match a connection with `writable: true` (caught by
   `agctl config validate`). If omitted, `defaults.database_connection` must
   also be `writable: true`.
3. **description** — one line.
4. **name** — kebab-case from the query
   (`INSERT INTO orders …` → `insert-order` or `create-order`).
5. **mode** — `write` (required for `agctl db execute`).

## Normalize bind params → `:name`

Same rules as read templates (see `db-template.md`):

| Source style | Example | agctl form |
|---|---|---|
| JDBC named | `:orderId` | `:orderId` (unchanged) |
| Postgres `$1` / Python `%s` / `?` | `WHERE id = $1` | rename to a meaningful `:orderId` |
| SQLAlchemy named | `:status` | `:status` (unchanged) |
| Some ORMs `.param` / `@name` | `@status` | `:status` |

Positional params carry no name — **invent a clear one** from the column/meaning.

## Type casting for numeric/timestamp columns

**Params are bound as strings** by `agctl`. For numeric or timestamp columns,
cast the param in SQL using PostgreSQL `::` syntax:

```sql
INSERT INTO orders (id, amount_cents, created_at)
VALUES (:orderId, :amountCents::int, :createdAt::timestamp)
```

The `::cast` is **not** rewritten by agctl (only `:name` → `%(name)s`), so it's
safe and idiomatic. Without the cast, PostgreSQL may reject the type mismatch or
apply implicit conversions that are not what you intend.

## Stack snippets

### Spring (JVM)

- `@Query("INSERT …")` → sql is already in `:name` form.
- `JdbcTemplate.update(sql, id)` → take the `sql` string; map positional `?`
  to `:name`.

### Python

- SQLAlchemy `text("… :status …")`, `psycopg` `cur.execute("… %s …")` →
  normalize to `:name` and add `::` casts where needed.

### Node

- `pg` `client.query("… $1 …")`, `knex` `.insert({…})` → normalize to `:name`
  and add `::` casts.

## Idempotency is the author's job

`agctl db execute` does NOT enforce idempotency. If you need an idempotent write,
encode it in the SQL using `ON CONFLICT` / `ON DUPLICATE KEY`:

```sql
INSERT INTO orders (id, customer_id, status)
VALUES (:orderId, :customerId, 'PENDING')
ON CONFLICT (id) DO NOTHING
RETURNING id, status, created_at
```

The `RETURNING` clause is optional but strongly recommended — it gives you the
inserted/updated row back in `result.returning`.

## What to clarify

- Which connection to bind to (must have `writable: true`).
- Meaningful param names when the source used positional placeholders.
- Whether idempotency is needed (e.g., for test repeatability).

## Where it writes

Under `database.templates:` (nested under `database:`). Omit `connection:` to
fall back to `defaults.database_connection` — **but only if that connection is
writable** (`agctl config validate` will reject otherwise).

## Gotchas

- SQL uses **`:name`** only — never `{}` (HTTP) or `${}` (env).
- Don't put a `:` bind inside a string literal (`'FAILED:foo'`) — agctl
  rewrites `:name`→`%(name)s` and may mis-rewrite it. `::` casts (e.g.
  `::jsonb`, `::int`, `::timestamp`) are protected and safe.
- **Params are strings** — use `::cast` for numeric/timestamp columns.
- **Idempotency is your job** — use `ON CONFLICT` / `ON DUPLICATE KEY` if the
  test may retry the same write.
- `mode: write` is required — templates default to `mode: read`, and
  `agctl db execute` rejects read-mode templates.
