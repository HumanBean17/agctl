# agctl — config-db-write: author a write DB template

Produce a `database.templates.<name>` block with `mode: write` from an
INSERT/UPDATE/DELETE statement or mutating repo method. Read-template rules
(bind-param normalization, `:name` only, `::`-cast safety) carry over — see
`--topic config-db`.

## Extraction

1. **sql** — the write statement, binds normalized to `:name`.
2. **connection** — must have `writable: true` (validate enforces). Omitted →
   `defaults.database_connection` must also be writable.
3. **description** — one line. **name** — kebab-case (`INSERT INTO orders …`
   → `insert-order`).
4. **mode: write** — required; `db execute` rejects read-mode templates.

## Params are strings — cast in SQL

agctl binds params as strings; numeric/timestamp columns need a PostgreSQL
`::` cast (not rewritten by the `:name`→`%(name)s` pass, so safe):

```sql
INSERT INTO orders (id, amount_cents, created_at)
VALUES (:orderId, :amountCents::int, :createdAt::timestamp)
```

## Idempotency is the author's job

`db execute` does NOT enforce it. Encode `ON CONFLICT` / `ON DUPLICATE KEY`
when the test may retry the same write; `RETURNING` is optional but strongly
recommended (gives the row back in `result.returning`):

```sql
INSERT INTO orders (id, customer_id, status)
VALUES (:orderId, :customerId, 'PENDING')
ON CONFLICT (id) DO NOTHING
RETURNING id, status, created_at
```

## Stack snippets

- **Spring**: `@Query("INSERT …")` already `:name`; `JdbcTemplate.update(sql,
  id)` → map `?` to `:name`.
- **Python**: SQLAlchemy `text`, psycopg `%s` → normalize + add `::` casts.
- **Node**: `pg` `$1`, knex `.insert({…})` → normalize + cast.

## Clarify (genuine gaps only)

Which writable connection to bind; meaningful positional-param names;
whether idempotency is needed (test repeatability).

## Gotchas

- `:name` only in SQL — never `{}` or `${}`; no `:` bind inside a string
  literal; `::jsonb`/`::int`/`::timestamp` casts are safe.
- Running it takes **two gates** (`writable: true` connection + `--write`
  flag) and an **explicit target** (`--template` or `--connection`) — see
  `--topic gotchas`.
