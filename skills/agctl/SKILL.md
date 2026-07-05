---
name: agctl
description: Reference for driving the agctl/agt CLI test harness (HTTP/Kafka/DB). Invoke only when running agctl commands or parsing its output — not proactively.
---

# Using `agctl`

`agctl` (alias `agt`) tests a **running** system over HTTP, Kafka, and DB. Every
invocation prints **one JSON object** on stdout and exits deterministically:

```json
{"ok": true, "command": "http.call", "result": {}, "error": null, "duration_ms": 87}
```

**Exit codes — the one thing to get right:**
- `0` — success; assertions passed.
- `1` — an assertion **failed**: the *system under test* is wrong. Investigate it.
- `2` — tool/config/env error: *your command* is wrong (bad flag, missing extra,
  unresolved `${ENV}`, bad config). Never a test result — fix the invocation.

Read `ok` first; on `false` read `error.type`. Parse **stdout only** — stderr is
diagnostics. For any command's exact flags run `agctl <cmd> --help`.

## Orient first: `agctl discover`

`discover` is a map, not a dump — it tells you what's configured. Don't guess
template/topic names; look them up.

```
agctl discover                                        # counts + category names
agctl discover --category http-templates              # names + one-line descriptions
agctl discover --category http-templates --name NAME  # params + a copy-paste example
agctl discover --search payment                       # cross-category keyword search
```
Categories: `services`, `http-templates`, `kafka-patterns`, `db-templates`, `mock-http-stubs`, `mock-kafka-reactors`.

## Which command for which intent

| Intent | Command |
|---|---|
| What can I do here? | `agctl discover` |
| Send a known request | `agctl http call <tpl> [--param k=v]…` |
| Ad-hoc request | `agctl http request --service S --method M --path P` |
| Assert an HTTP response | `agctl http call <tpl> --status N [--contains '{…}'] [--match '<jq>'] [--jq-path .x --equals v]` |
| Verify an event was published | `agctl kafka assert --topic T <mode> --timeout N` |
| See what was published | `agctl kafka consume --topic T [--match <jq>]` |
| Publish a message | `agctl kafka produce --topic T --message '{…}'` |
| Execute a DB write | `agctl db execute (--template\|--sql) --write [--connection C]` |
| Assert a DB row count | `agctl db assert (--template\|--sql) --expect-rows N` |
| Assert a DB field value | `agctl db assert (…) --expect-value --path .x --equals v` |
| Inspect raw DB state | `agctl db query (--template\|--sql)` |
| Discover live DB schema | `agctl db schema [--connection C] [--schema S] [--table T]` |
| Impersonate a dependency (HTTP stub / Kafka reaction) | `agctl mock run` |
| Are services up? | `agctl check ready --all` |
| Validate / debug config | `agctl config validate` / `config show` |
| Migrate v1 config to dialect `"2"` | `agctl config migrate [--dry-run]` |

## Command forms

Only `--config <path>` is global. `--timeout` is **not** global (see gotchas).
`[brackets]` = optional; trailing `…` = repeatable; `<mode>` = one of
`--contains '{…}' | --match '<jq>' | --pattern <name>`.

```
agctl http call   <tpl> [--param k=v]… [--body '{…}'] [--header k=v]… [--timeout N]
                    [--status N] [--contains '{…}'] [--match '<jq>'] [--jq-path <jq> --equals <v>]
agctl http request --service S --method GET|POST|PUT|PATCH|DELETE --path P [--body '{…}'] [--header k=v]…
                    [--status N] [--contains '{…}'] [--match '<jq>'] [--jq-path <jq> --equals <v>]
agctl http ping   [<tpl> | --service S --path P] --interval N [--duration N | --until-stopped]   # streams NDJSON; background it

agctl kafka assert [--topic T] <mode> [--param k=v]… [--path <jq>] --timeout N [--from-beginning]
agctl kafka consume --topic T [--timeout N] [--match '<jq>'] [--expect-count N] [--from-beginning]
agctl kafka produce --topic T --message '{…}' [--key K] [--header k=v]…

agctl db query    (--template T | --sql "…") [--param k=v]… [--connection C]
agctl db assert   (--template T | --sql "…") (--expect-rows N | --expect-value --path <jq> --equals V)
agctl db execute  (--template T | --sql "…") [--param k=v]… [--connection C] --write
agctl db schema   [--connection C] [--schema S] [--table T]                # read-only; NO --write/--template/--sql/--param

agctl mock run    [--only http|kafka] [--http-listen H:P] [--fail-fast] [--duration N | --until-stopped]   # streams NDJSON; background it

agctl check ready [--service S | --all]
agctl config validate | config show [--unmask]
agctl config migrate [--config <path>] [--dry-run]
```

- `--body` on `http call` is **deep-merged** over the template body (adds/overrides).
- `--header` merges with template headers; caller wins.
- `db`/`kafka produce` have **no** `--timeout`. `kafka assert --timeout` is **required**.
- **HTTP response assertions** (`http call`/`http request`): `--status` / `--contains '{…}'`
  / `--match '<jq>'` / `--jq-path <jq> --equals <v>`. ≥1 flag => assertion mode (all AND;
  fail => exit 1, `AssertionError`). `--jq-path` needs `--equals` (else exit 2). `--match`
  is "any truthy output"; for "all items" use `all(.body.items[]; .pred)`. `--match`/`--jq-path`
  need `pip install 'agctl[jq]'`.

## Gotchas (what `--help` won't tell you)

1. **Two streaming commands** — `http ping` (one JSON object **per ping**) and
   `mock run` (one NDJSON event line as things happen, plus a final `summary`).
   Both are meant to run backgrounded with `&` and `kill`ed when done. Exits `0`
   (all ok) / `1` (any failed). Everything else emits exactly one object.
2. **A 4xx/5xx HTTP response is `ok:true` — unless you assert.** Status is a
   *result*, not an error. Add `--status` / `--contains` / `--match` / `--jq-path` /
   `--equals` to `http call` / `http request` to flip a wrong response into
   `AssertionError` (exit 1); zero assertion flags leaves the default
   "result, not assertion" path unchanged.
3. **`--match` is envelope-rooted under dialect `"2"` (not payload-rooted).**
   - **HTTP** `http call`/`http request --match` evaluates against the **response
     envelope** `{status_code, response_time_ms, headers (lowercased), body, url,
     method}` — so `.body.order_id`, `.status_code`, `.headers.x`. Prefix legacy
     body-form exprs with `.body | ` (e.g. `.status == "X"` → `.body | .status == "X"`).
   - **Kafka** `kafka assert`/`kafka consume --match` and `kafka.patterns[].match`
     evaluate against the **message envelope**
     `{key, value, partition, offset, timestamp, headers}` — so `.value.eventType`,
     `.key`, `.headers.rqUID` (header keys are **case-sensitive**). Prefix legacy
     value-form exprs with `.value | `.
   - **Unchanged:** `match.body` (json_subset), `--contains`, `--path`,
     `--jq-path`/`--equals` (still body-rooted), `--status`.
   - A v1 `agctl.yaml` is rejected (exit 2) with a pointer at `agctl config migrate`,
     which rewrites the three `match`-site families in the file (`mocks.http.stubs.*.match.jq`,
     `mocks.kafka.reactors.*.match`, `kafka.patterns.*.match`). **CLI `--match` flags in
     scripts/prompts are NOT rewritten** — prefix them by hand.
4. **Three placeholder syntaxes — don't mix them:**
   - `${VAR}` — env var, resolved at **config load** (`${VAR}` required → exit 2 if
     unset; `${VAR:-default}` optional; `${VAR:-}` optional/empty).
   - `{name}` — HTTP path/body & Kafka patterns, filled at **call time** by `--param`.
   - `:name` — SQL params (templates and `--sql`), filled by `--param`.
5. **Kafka reads are windowed, not "latest".** `consume`/`assert` seek to
   `now - --lookback` (default = `--timeout`) and read forward — so an event
   published just before you started is still matched (send-then-assert is
   reliable by default). `--from-beginning` → earliest offset. Narrow busy topics
   with `--match`/`--contains` so you don't match stale events.
6. **`kafka assert`** modes are **combinable** — when several are given, **all**
   must pass. `--pattern` infers the topic from config (omit `--topic`). On no
   match within the window it exits `1` with `error.detail = {topic, timeout}`
   (distinct from a `ConnectionError`, which is exit `2`).
7. **`db assert`** takes exactly one mode; `--expect-value` needs **both**
   `--path` and `--equals`. `--equals` is JSON-parsed if valid (`"0"`→0,
   `"true"`→bool, `"null"`→null) else a plain string; compared **strictly**
   (`0` ≠ `"0"`). Match a timestamp column with `--equals "2026-…Z"`.
8. **`ConnectionError` is exit `2`.** The service/broker/DB is unreachable — run
   `agctl check ready --all` and confirm it's up before retrying; don't blame the
   assertion.
9. **No built-in "event did NOT arrive" assert.** `kafka consume --expect-count 0`
   is **not** it (it always exits 0). To check absence, run `kafka consume --topic
   T --timeout N [--match …]` and inspect `result.count` (0 = no match in window).
10. **`db execute` requires two gates** — a `writable: true` connection AND the
    `--write` flag. It also requires an explicit target (`--template` or
    `--connection`), refusing to write to the default connection implicitly.
11. **`db schema` reads `pg_catalog`, which is cluster-wide and NOT
    privilege-filtered.** On a shared cluster it can list relations this
    connection cannot actually `SELECT` from — discovering a name is not a
    grant. Treat the listing as "visible," not "accessible"; let the
    subsequent `SELECT` fail loudly if privileges are missing.

## Discover live schema before authoring SQL

`agctl db schema` is **read-only and ungated** — no `--write`, no `--template`,
no `--sql`, no `--param`; it ignores `writable`/`mode`. Any configured connection
(read-only or writable) is eligible. Use it before authoring `db execute` /
`db query` SQL: `pg_catalog` is the source of truth, not your memory of the schema.

**Two levels (progressive):**

1. **List relations** — `agctl db schema [--connection C] [--schema S]`
   (tag `db.schema.tables`). Returns `{connection, schema_filter, count,
   items:[{schema, name, kind, column_count}], hint}`. `kind` is `"table"` or
   `"view"`. Call this first to find the exact relation name.
2. **Drill into one** — `agctl db schema --table T [--schema S] [--connection C]`
   (tag `db.schema.table`). Returns `{connection, schema, table, kind, comment,
   columns:[{name, data_type, nullable, default, generated, enum_values,
   comment}], primary_key:[col], foreign_keys:[{name, columns, references_schema,
   references_table, references_columns}], unique_constraints:[{name, columns}],
   hint}`. `--table` accepts views. Match is **exact-case** on the stored name.

**Fail-loud `--table` matching:** 0 matches → `ConfigError` (exit 2) telling you
to run Level 1; >1 match across schemas → `ConfigError` with
`error.detail.candidates=[{schema, kind}]` → disambiguate with `--schema`.

**Authoring rules the schema tells you (load-bearing):**

1. **Quote mixed-case / reserved identifiers.** PostgreSQL folds unquoted
   identifiers to lowercase. If a column or table name contains uppercase or
   non-`[a-z0-9_]` characters, or matches a Postgres reserved word, you MUST
   double-quote it in your SQL: `"OrderItems"`, `"user"`. The `name` field is
   the exact stored case — copy it verbatim, quoting as needed.
2. **Omit generated columns from INSERT.** A column with
   `generated == "always_identity"` or `generated == "stored"` MUST be omitted
   from `INSERT` (and `"stored"` from `UPDATE` too). `generated ==
   "by_default_identity"` and `serial` columns (default `nextval(...)`,
   `generated == null`) may be supplied or omitted.

## `db execute` — write operations

`agctl db execute` runs INSERT/UPDATE/DELETE statements. It has **two safety gates**
to prevent accidental writes:

1. **Connection gate** — the connection must have `writable: true` in config
   (caught by `agctl config validate`).
2. **Invocation gate** — the `--write` flag is required (no-op flag is not enough).

**Explicit-target rule:** `db execute` refuses to write to the default connection
implicitly. You must pass `--template <name>` (which specifies a connection) or
`--connection <name>` to name the write target. This prevents accidental writes
when you forget which connection is default.

**Result shape:**

```json
{
  "ok": true,
  "command": "db.execute",
  "result": {
    "rows_affected": 1,
    "returning": [
      {"id": "ord-789", "status": "PENDING", "created_at": "2026-07-03T12:34:56Z"}
    ],
    "connection": "main-db",
    "sql": "INSERT INTO orders (id, customer_id, status) VALUES (:orderId, :customerId, 'PENDING') ON CONFLICT (id) DO NOTHING RETURNING *"
  },
  "duration_ms": 45
}
```

- `rows_affected` — int or `null` (null for statements like DDL that don't report
  a row count).
- `returning` — list of dict rows when the SQL includes a `RETURNING` clause, else
  `[]`.
- `connection` — the connection name used (echoed for clarity).
- `sql` — the SQL that was executed (echoed for debugging).

**Mode checking:** A template with `mode: read` is rejected by `db execute`
(ConfigError, exit 2). Write templates must set `mode: write` — see the
`agctl-config` skill for authoring write templates.

**Example:**

```bash
# Using a write template (recommended)
agctl db execute --template insert-order \
  --param orderId=ord-789 \
  --param customerId=cust-42 \
  --write

# Free-form SQL with explicit connection
agctl db execute \
  --sql "UPDATE orders SET status = 'CANCELLED' WHERE id = :orderId" \
  --param orderId=ord-789 \
  --connection main-db-writable \
  --write
```

**Idempotency:** `db execute` does NOT enforce idempotency. If you need repeatable
writes (e.g., for flaky-test resilience), encode idempotency in the SQL using
`ON CONFLICT` (PostgreSQL) or `ON DUPLICATE KEY UPDATE` (MySQL). The template
author's job — see `agctl-config/reference/db-write-template.md`.

## `agctl mock run` — impersonate a dependency

`agctl mock run` stands in for the system's **external** dependencies so a local test is
self-contained — an HTTP API the SUT calls, or the downstream Kafka consumer the SUT
expects to react to its events. It is **SUT-facing**: the real application's HTTP client
points at the mock's `listen`, and Kafka reactors join the SUT's *real* broker as consumers
(it is not a broker itself). Stubs/reactors are authored in the `mocks:` config section —
see the `agctl-config` skill (`reference/mocks.md`).

```
agctl mock run [--only http|kafka] [--http-listen H:P] [--fail-fast] [--duration N | --until-stopped]
```

- `--only http` runs just the HTTP server (no `kafka` extra / `kafka.brokers` needed);
  `--only kafka` just the reactors.
- `--http-listen` is a **literal** `host:port` (CLI args are not `${}`-interpolated); it
  overrides `mocks.http.listen`.
- `--duration N` (foreground, stops after N s) and `--until-stopped` (run until SIGTERM/SIGINT; default behavior if --duration is omitted) are mutually
  exclusive. `--fail-fast` exits `1` on the **first** runtime error instead of continuing.

It streams one NDJSON event per line: `started`, `http.hit`, `http.unmatched`,
`http.body_parse_skipped`, `capture.missing`, `kafka.reacted`, `kafka.skipped`, `kafka.error`,
and a final `summary`. A clean run exits `0`; a clean run in which any `kafka.error` occurred
exits `1`. Startup failures (bad `mocks:`, port in use, broker unreachable) emit **one**
structured envelope then exit `2`.

### Background lifecycle — load-bearing

The failure signals (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`,
`kafka.error`, `capture.missing`) live **only** on stdout, and the exit-1 escalation arrives
only on a clean `SIGTERM`. The plain `&` / `kill` pattern loses both and silently produces a
**false green**. Always follow this protocol:

1. **Redirect stdout to a log:** `agctl mock run > mock.log 2>&1 &` (and capture the PID).
2. **Poll** `mock.log` for the `started` line **before** running the SUT — don't sleep a
   fixed delay.
3. **Stop with `SIGTERM` and `wait`** — never `SIGKILL` (it skips the shutdown handler, the
   `summary` line, and the exit code).
4. **Grep the log for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` /
   `kafka.error` / `capture.missing` regardless of the test result** — any hit is a failure,
   even if the assertions passed. `capture.missing` is non-fatal at runtime (the mock
   substitutes empty string and continues), but it marks a `capture.from` that resolved to
   nothing — usually a misconfigured path silently yielding a plausible-but-wrong field.
   `--fail-fast` is the synchronous alternative for `--duration` runs.

```bash
nohup agctl mock run > mock.log 2>&1 &
MOCK_PID=$!
until grep -q '"event":"started"' mock.log; do sleep 0.1; done    # poll, don't guess
# … run the SUT / assertions, pointing the SUT at the mock's listen address …
kill -TERM "$MOCK_PID"; wait "$MOCK_PID"                            # SIGTERM + wait, never SIGKILL
grep -E 'http.unmatched|http.body_parse_skipped|kafka.skipped|kafka.error|capture.missing' mock.log && exit 1
```

## Recipes

```bash
# Send → assert the downstream Kafka event (reliable by default)
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001
agctl kafka assert --topic orders.created --contains '{"customer_id":"cust-42"}' --timeout 10

# Assert the HTTP response itself in one call (no shell jq; exit 1 if it fails)
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001 \
  --status 201 --match '.body.order_id != null' --contains '{"status": "PENDING"}'
# Type-aware value equality via jq path (0 ≠ "0"; pairing needs both flags)
agctl http call get-order --param order_id=ord-789 --jq-path '.status' --equals '"CONFIRMED"'

# E2E: thread an ID through HTTP → Kafka → DB
OID=$(agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001 | jq -r '.result.body.order_id')
agctl kafka assert --topic orders.created --contains "{\"order_id\":\"$OID\"}" --timeout 10
agctl db assert --sql "SELECT 1 FROM orders WHERE id = :order_id AND status = 'PENDING'" --param order_id="$OID" --expect-rows 1

# Seed DB state before a test (idempotent write)
agctl db execute --template upsert-customer --param customerId=cust-42 --param email=test@example.com --write

# Keep a session alive during a long test (background it, capture PID, kill when done)
agctl http ping heartbeat --interval 5 --until-stopped &
PID=$!
# … run the scenario …
kill "$PID"
```

Prefer **templates** over free-form. Explore with **`discover`**, never `config show`.
