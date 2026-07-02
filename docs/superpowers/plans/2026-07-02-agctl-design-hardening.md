# agctl Design Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the approved change-set (decisions D1–D10 + mechanical M1–M5) to `docs/DESIGN.md`, producing a tightened, implementation-ready v0.6 design.

**Architecture:** Single-file documentation edit. Every change is anchored to an exact current string in `docs/DESIGN.md` and replaced with the hardened text. No source code exists yet; verification is grep-based consistency checks plus an optional parse of the embedded YAML/TOML code blocks.

**Tech Stack:** Markdown (`docs/DESIGN.md`), embedded YAML (config schema) and TOML (pyproject skeleton). Verification via `grep`/`python3`.

**Spec:** [`docs/superpowers/specs/2026-07-02-agctl-design-hardening.md`](../specs/2026-07-02-agctl-design-hardening.md)

---

## Verification strategy (read first)

There is no test runner for this work. Each task verifies its edit with a targeted `grep` (the old pattern must be gone; the new pattern present) and/or a parse of the affected code block. The final task (Task 13) runs a whole-doc consistency sweep and bumps the version. The spec's §5 code tests (config-load, kafka timing, `--equals` coercion, error typing) are **deferred** to the future code-implementation plan — they cannot run until the `agctl` package exists.

**Optional YAML/TOML parse helper.** If `python3` with PyYAML and a TOML lib is available, embedded blocks can be validated. This is optional; grep is the required check.

**Commit cadence:** one commit per task. The repo currently has zero commits; Task 1 establishes the baseline.

---

## Task 1: Baseline commit

**Files:**
- Stage: `docs/DESIGN.md`, `docs/superpowers/specs/2026-07-02-agctl-design-hardening.md`, `CLAUDE.md`

This task only commits the current state so every later task is a clean, reviewable diff. No content change.

- [ ] **Step 1: Confirm current state**

Run: `git status --short && git log --oneline -1 2>/dev/null || echo "(no commits yet)"`
Expected: untracked `docs/`, `CLAUDE.md`, `.claude/`; "(no commits yet)".

- [ ] **Step 2: Stage the design artifacts (exclude `.claude/` local config)**

```bash
git add CLAUDE.md docs/DESIGN.md docs/superpowers/specs/2026-07-02-agctl-design-hardening.md
```

- [ ] **Step 3: Commit baseline**

```bash
git commit -m "docs: add agctl design (v0.5-draft) and design-hardening spec

Baseline commit. DESIGN.md is the pre-hardening v0.5-draft; the spec at
docs/superpowers/specs/2026-07-02-agctl-design-hardening.md captures the
approved change-set that the following commits apply.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

- [ ] **Step 4: Verify**

Run: `git log --oneline -1`
Expected: one commit shown.

---

## Task 2: D1 — Resolve duplicate `database:` key (§2.1)

**Files:**
- Modify: `docs/DESIGN.md` (§2.1, the two `database:` blocks)

🔴 Broken as written: two top-level `database:` keys; the second silently overwrites the first in YAML, dropping all connection profiles. Collapse into one block with `connections:` + `templates:` sub-sections.

- [ ] **Step 1: Replace the first `database:` block (connections) with the combined block**

Edit `docs/DESIGN.md`. Replace this exact text —

```
# ---------------------------------------------------------------------------
# database — named connection profiles.
# All fields support ${ENV_VAR} interpolation.
# Currently supported types: postgresql (extensible via plugins, see §9).
# ---------------------------------------------------------------------------
database:
  main-db:
    type: postgresql
    host: "${DB_HOST}"
    port: 5432
    dbname: "${DB_NAME}"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    default: true                               # used when --connection is omitted

  analytics-db:
    type: postgresql
    host: "${ANALYTICS_DB_HOST}"
    port: 5432
    dbname: "analytics"
    user: "${ANALYTICS_DB_USER}"
    password: "${ANALYTICS_DB_PASSWORD}"
```

— with this —

```
# ---------------------------------------------------------------------------
# database — named connection profiles and SQL query templates.
# All fields support ${ENV_VAR} interpolation (see §2.2).
# Connection types: postgresql (extensible via plugins, see §9).
# ---------------------------------------------------------------------------
database:
  connections:
    main-db:
      type: postgresql
      host: "${DB_HOST}"
      port: 5432
      dbname: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      default: true                               # used when --connection is omitted

    analytics-db:
      type: postgresql
      host: "${ANALYTICS_DB_HOST}"
      port: 5432
      dbname: "analytics"
      user: "${ANALYTICS_DB_USER}"
      password: "${ANALYTICS_DB_PASSWORD}"

  # templates — named SQL queries. `connection` is optional (falls back to
  # defaults.database_connection). `sql` uses :paramName named params (JDBC-style).
  templates:
    find-order:
      description: "Fetch a single order by ID"
      connection: main-db
      sql: "SELECT id, status, total_cents, created_at FROM orders WHERE id = :orderId"

    orders-by-status:
      description: "List orders in a given status, optionally filtered by customer"
      connection: main-db
      sql: "SELECT id, status FROM orders WHERE status = :status AND customer_id = :customerId"

    count-failed-payments:
      description: "Count failed payments after a given timestamp"
      connection: main-db
      sql: "SELECT COUNT(*) AS cnt FROM payments WHERE status = 'FAILED' AND created_at > :since"
```

- [ ] **Step 2: Delete the now-duplicate second `database:` block**

Edit `docs/DESIGN.md`. Replace this exact text (the entire second block, including its header comment and the trailing blank line) —

```
# ---------------------------------------------------------------------------
# database.templates — named SQL query templates.
# connection: optional; falls back to defaults.database_connection
# sql:        query string using :paramName for named parameters (JDBC-style)
# ---------------------------------------------------------------------------
database:
  templates:
    find-order:
      description: "Fetch a single order by ID"
      connection: main-db
      sql: "SELECT id, status, total_cents, created_at FROM orders WHERE id = :orderId"

    orders-by-status:
      description: "List orders in a given status, optionally filtered by customer"
      connection: main-db
      sql: "SELECT id, status FROM orders WHERE status = :status AND customer_id = :customerId"

    count-failed-payments:
      description: "Count failed payments after a given timestamp"
      connection: main-db
      sql: "SELECT COUNT(*) AS cnt FROM payments WHERE status = 'FAILED' AND created_at > :since"

```

— with an empty string (delete it). Leave the surrounding `---` separators and the `defaults:` block intact.

- [ ] **Step 3: Verify only one `database:` key remains, and both sub-sections exist**

Run: `grep -nE '^database:' docs/DESIGN.md`
Expected: exactly **one** match.

Run: `grep -nE '^  connections:|^  templates:' docs/DESIGN.md`
Expected: at least one `connections:` and one `templates:` indented under `database:`.

Optional (if `python3` + PyYAML available), parse the §2.1 block and assert both `database.connections.main-db` and `database.templates.find-order` resolve.

- [ ] **Step 4: Commit**

```bash
git add docs/DESIGN.md
git commit -m "fix(design): merge duplicate database: key into connections+templates (D1)

Two top-level database: keys silently collapsed in YAML, dropping all DB
connection profiles. Now one database: block with connections: and templates:
sub-sections.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: D2 — Unify SQL parameter syntax to `:name` (all sections except §6)

**Files:**
- Modify: `docs/DESIGN.md` (§2.3, §3.3, §4.1, §11 UC-5)

Free-form SQL used `$paramName` (not native to psycopg). Unify all SQL on JDBC-style `:paramName`. (§6 AGENTS.md SQL is handled in Task 10 to keep that region's edits together.)

- [ ] **Step 1: Update §3.3 prose**

Replace:
```
Both modes accept `--param key=value`. In templates, named parameters use `:paramName` syntax (JDBC-style). In free-form SQL, use `$paramName`.
```
with:
```
Both modes accept `--param key=value`. Named parameters use `:paramName` (JDBC-style) in both templates and free-form SQL; agctl translates `:paramName` to the driver's native bind syntax at execution time. (`{placeholder}` is reserved for HTTP path/body params — see §2.3 — and is not used in SQL to avoid colliding with JSON literals like `'{"a":1}'::jsonb`.)
```

- [ ] **Step 2: Update the three §3.3 free-form SQL examples**

Replace `--sql "SELECT status FROM orders WHERE id = \$orderId" \` → `--sql "SELECT status FROM orders WHERE id = :orderId" \`

Replace `--sql "SELECT 1 FROM orders WHERE id = \$orderId AND status = 'CONFIRMED'" \` → `--sql "SELECT 1 FROM orders WHERE id = :orderId AND status = 'CONFIRMED'" \`

Replace `--sql "SELECT status FROM orders WHERE id = \$orderId" \` (second occurrence, inside `--expect-value` example) → `--sql "SELECT status FROM orders WHERE id = :orderId" \`

> Note: these two `--sql "...id = \$orderId"` strings differ only by the `\` line continuation vs. later flags. Match each within its full surrounding line using the Edit tool. If a string is not unique, include the following line in the match to disambiguate.

- [ ] **Step 3: Update the §4.1 error-detail SQL**

Replace:
```
    "sql": "SELECT 1 FROM orders WHERE id = $order_id AND status = 'CONFIRMED'",
```
with:
```
    "sql": "SELECT 1 FROM orders WHERE id = :order_id AND status = 'CONFIRMED'",
```

- [ ] **Step 4: Update the §11 UC-5 debugging query**

Replace:
```
agctl db query   --sql "SELECT id, status, created_at, updated_at FROM orders WHERE customer_id = \$cid ORDER BY created_at DESC LIMIT 5"   --param cid=cust-flaky
```
with:
```
agctl db query   --sql "SELECT id, status, created_at, updated_at FROM orders WHERE customer_id = :cid ORDER BY created_at DESC LIMIT 5"   --param cid=cust-flaky
```

- [ ] **Step 5: Add the SQL param syntax to §2.3**

Replace:
```
Template `path` and `body` values use `{placeholder}` (single braces) for runtime substitution via `--param key=value`. This is distinct from env var interpolation (`${...}`), which is resolved at config load time.
```
with:
```
HTTP template `path` and `body` values use `{placeholder}` (single braces) for runtime substitution via `--param key=value`. SQL (templates and free-form) uses `:paramName` (JDBC-style) instead — `{...}` is avoided in SQL to prevent collisions with JSON literals. Both are distinct from env var interpolation (`${...}`), which is resolved at config load time.
```

- [ ] **Step 6: Verify no `$paramName` remains outside §6**

Run: `grep -nE '\$(orderId|order_id|cid|paramName)\b' docs/DESIGN.md`
Expected: matches **only** in the §6 AGENTS.md template region (handled by Task 10). Confirm any matches shown are within §6 (between the `## Testing with` heading and its closing). No matches in §2/§3/§4/§11.

- [ ] **Step 7: Commit**

```bash
git add docs/DESIGN.md
git commit -m "fix(design): unify SQL param syntax on :name, drop \$name (D2)

Free-form SQL used \$paramName (not native to psycopg). All SQL now uses
JDBC-style :paramName; HTTP keeps {placeholder}.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: D3 optional interpolation + M4 version enforcement (§2.2, §2.1, §3.5)

**Files:**
- Modify: `docs/DESIGN.md` (§2.1 version comment, §2.2, §3.5)

- [ ] **Step 1: Add optional `${VAR:-default}` syntax to §2.2**

Replace:
```
### 2.2 Environment Variable Interpolation

Any YAML string value containing `${VAR_NAME}` is resolved at load time:

1. Look up `VAR_NAME` in the process environment.
2. If missing, emit a config error (exit 2) listing all unresolved variables — do not silently substitute empty string.

The `${VAR_NAME}` syntax is only supported in string scalar values, not in keys.
```
with:
```
### 2.2 Environment Variable Interpolation

Any YAML string value containing `${...}` is resolved at load time. Three forms are supported:

1. `${VAR_NAME}` — **required**. Look up `VAR_NAME` in the process environment. If missing, emit a config error (exit 2) listing all unresolved variables. Never silently substitute an empty string.
2. `${VAR_NAME:-default}` — **optional with default**. If `VAR_NAME` is missing, substitute the literal `default`.
3. `${VAR_NAME:-}` — **optional, empty**. If `VAR_NAME` is missing, substitute an empty string (no error). Use this for fields that are not always set, e.g. `schema_registry_url: "${SCHEMA_REGISTRY_URL:-}"`.

The `${...}` syntax is only supported in string scalar values, not in keys.
```

- [ ] **Step 2: Clarify the version field in §2.1**

Replace:
```
# Version must match the agctl major version that loaded this file.
```
with:
```
# Version tracks the agctl MAJOR version only (currently "1"). A major-version
# mismatch is a ConfigError (exit 2); minor/patch are not tracked.
```

- [ ] **Step 3: Add version enforcement to §3.5 `config validate`**

Replace:
```
Parse and validate `agctl.yaml`. Reports schema errors, unresolvable env vars, and dangling service references in templates. Exits 2 on any error.
```
with:
```
Parse and validate `agctl.yaml`. Reports schema errors, unresolvable required env vars, dangling service/connection references in templates, and major-version mismatches. Exits 2 on any error.
```

- [ ] **Step 4: Verify**

Run: `grep -n 'VAR_NAME:-' docs/DESIGN.md`
Expected: at least two matches (the `:-default` and `:-` rules, plus the schema_registry example).

Run: `grep -n 'MAJOR version only' docs/DESIGN.md`
Expected: one match.

- [ ] **Step 5: Commit**

```bash
git add docs/DESIGN.md
git commit -m "feat(design): optional \${VAR:-default} interpolation + version check (D3, M4)

Bare \${VAR} stays a hard error (fail-loud); \${VAR:-x} and \${VAR:-} opt in
to optional/default. version is major-only; mismatch is a ConfigError.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: D5 — Document `http call --body` merge semantics (§3.1)

**Files:**
- Modify: `docs/DESIGN.md` (§3.1, after the `http call` Examples)

- [ ] **Step 1: Add a Body merge paragraph after the `http call` examples**

Insert immediately **after** the line `# Override a body field` example block — i.e., after this exact closing —

```
agctl http call create-order \
  --param customer_id=cust-42 \
  --param sku=WIDGET-001 \
  --body '{"priority": "high"}'
```

— append (after a blank line):

```

**Body merge:** when `--body '{...}'` is supplied, it is deep-merged into the template's body — nested objects merge recursively, arrays are replaced wholesale, and scalar leaves from `--body` override the template. The template body is the base; `--body` only adds or overrides fields.
```

- [ ] **Step 2: Verify**

Run: `grep -n '\*\*Body merge:\*\*' docs/DESIGN.md`
Expected: one match in §3.1.

- [ ] **Step 3: Commit**

```bash
git add docs/DESIGN.md
git commit -m "docs(design): specify http call --body deep-merge semantics (D5)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: D6 — Kafka offset/timing model (§3.2)

**Files:**
- Modify: `docs/DESIGN.md` (§3.2 consume + assert usage blocks + a shared note)

🟠 The default "latest" offset races the producer. Replace with a timestamp-lookback window.

- [ ] **Step 1: Update the `kafka consume` usage block**

Replace:
```
agctl kafka consume
    --topic <name>
    [--timeout <seconds>]       # default: kafka.timeout_seconds from config
    [--match <jq-expr>]         # jq boolean predicate; only messages where the expression
                                #   is true are counted/returned
    [--filter-key <jq-expr>]    # DEPRECATED alias for --match; prefer --match
    [--expect-count <n>]        # if set, exit 1 if fewer than n matching messages received
    [--from-beginning]          # consume from earliest offset (default: latest)
    [--consumer-group <name>]   # override default_consumer_group
```
with:
```
agctl kafka consume
    --topic <name>
    [--timeout <seconds>]       # default: kafka.timeout_seconds from config
    [--lookback <seconds>]      # seek to now - lookback before reading; default: = --timeout
    [--match <jq-expr>]         # jq boolean predicate; only messages where the expression
                                #   is true are counted/returned
    [--filter-key <jq-expr>]    # DEPRECATED alias for --match; prefer --match
    [--expect-count <n>]        # if set, exit 1 (AssertionError) if fewer than n matching
                                #   messages are received within the window
    [--from-beginning]          # seek to earliest offset (default: seek to now - lookback)
    [--consumer-group <name>]   # override default consumer group (default: agctl-consumer)
```

- [ ] **Step 2: Update the `kafka assert` usage block**

Replace:
```
agctl kafka assert
    [--topic <name>]            # required unless --pattern is used (topic inferred from pattern)
    [--contains '{...}']        # JSON subset that must be present in the message value
    [--match <jq-expr>]         # jq boolean predicate; more flexible than --contains
    [--pattern <name>]          # use a named pattern from config
    [--param key=value]         # repeatable; fills {placeholder} in pattern match expression
    [--path <jq-path>]          # narrow --contains match to a sub-path, e.g. ".event_type"
    --timeout <seconds>
    [--from-beginning]
    [--consumer-group <name>]
```
with:
```
agctl kafka assert
    [--topic <name>]            # required unless --pattern is used (topic inferred from pattern)
    [--contains '{...}']        # JSON subset that must be present in the message value
    [--match <jq-expr>]         # jq boolean predicate; more flexible than --contains
    [--pattern <name>]          # use a named pattern from config
    [--param key=value]         # repeatable; fills {placeholder} in pattern match expression
    [--path <jq-path>]          # narrow --contains match to a sub-path, e.g. ".event_type"
    [--lookback <seconds>]      # seek to now - lookback; default: = --timeout
    --timeout <seconds>         # assert fails (AssertionError) if no match within this window
    [--from-beginning]          # seek to earliest offset (default: seek to now - lookback)
    [--consumer-group <name>]   # override default consumer group (default: agctl-consumer)
```

- [ ] **Step 3: Update the assert intro line for error typing**

Replace:
```
Assert that a message matching a predicate or pattern appears on a topic within a timeout. Fails (exit 1) if no matching message arrives in time.
```
with:
```
Assert that a message matching a predicate or pattern appears on a topic within a timeout. Fails with `AssertionError` (exit 1) if no matching message arrives within the window.
```

- [ ] **Step 4: Add the shared "Offset & timing model" note at the end of §3.2**

Insert immediately **after** the final `kafka assert` example block (the `--pattern payment-failed` example) — after this exact text:

```
agctl kafka assert \
  --pattern payment-failed \
  --timeout 15
```

— append (after a blank line):

```

**Offset & timing model (consume and assert):** By default the consumer seeks each partition to the timestamp `now - --lookback` (via `offsets_for_times`) and reads forward, rather than subscribing at "latest". This makes the common send-then-assert pattern reliable: an event published a moment before the command starts still falls inside the window. `--lookback` defaults to the resolved `--timeout` (look back as far as you wait forward); `--from-beginning` overrides to the earliest offset. For `assert`, committed offsets are ignored — each invocation re-seeks by time, so repeated asserts are independent and deterministic. On high-volume topics, narrow with `--match`/`--contains` to avoid matching stale events from prior runs.
```

- [ ] **Step 5: Verify**

Run: `grep -n '\-\-lookback' docs/DESIGN.md`
Expected: matches in both the consume and assert usage blocks plus the timing-model note (≥4).

Run: `grep -n 'default: latest' docs/DESIGN.md`
Expected: **no** matches (the old default is gone).

- [ ] **Step 6: Commit**

```bash
git add docs/DESIGN.md
git commit -m "fix(design): kafka timestamp-lookback offset model (D6)

consume/assert seek to now-lookback (offsets_for_times) instead of latest,
so send-then-assert no longer races the producer. New --lookback (default
= --timeout); --from-beginning overrides to earliest.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: D7 check-ready default + D9 discover SQL (§3.4, §3.6)

**Files:**
- Modify: `docs/DESIGN.md` (§3.4, §3.6 db-template discover example, §4.2 discover.item note)

- [ ] **Step 1: Specify the `check ready` default in §3.4**

Replace:
```
```
agctl check ready
    [--service <name>]          # check a single service
    [--all]                     # check all services defined in config
    [--timeout <seconds>]
```

A service is considered ready if its `health_path` returns HTTP 2xx. If `health_path` is not configured for a service, a `GET /` is attempted.
```
with:
```
```
agctl check ready
    [--service <name>]          # check a single service
    [--all]                     # check all services defined in config (default when neither flag is given)
    [--timeout <seconds>]
```

With neither `--service` nor `--all`, every configured service is checked (equivalent to `--all`). A service is considered ready if its `health_path` returns HTTP 2xx. If `health_path` is not configured for a service, a `GET /` is attempted.
```

- [ ] **Step 2: Add `sql` to the §3.6 db-template discover.item example**

Replace:
```
**Example — `agctl discover --category db-templates --name find-order`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "db-templates",
    "name": "find-order",
    "description": "Fetch a single order by ID",
    "connection": "main-db",
    "params": ["orderId"],
    "example": "agctl db query --template find-order --param orderId=X"
  },
  "duration_ms": 1
}
```
```
with:
```
**Example — `agctl discover --category db-templates --name find-order`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "db-templates",
    "name": "find-order",
    "description": "Fetch a single order by ID",
    "connection": "main-db",
    "sql": "SELECT id, status, total_cents, created_at FROM orders WHERE id = :orderId",
    "params": ["orderId"],
    "example": "agctl db query --template find-order --param orderId=X"
  },
  "duration_ms": 1
}
```
```

- [ ] **Step 3: Update the §4.2 discover.item note that said SQL is omitted**

Replace:
```
Shape varies by category. All items share `name`, `description`, `params[]`, and `example`. HTTP templates add `method`, `service`, `path`; Kafka patterns add `topic` and `match`; DB templates add `connection` (SQL is omitted from discovery output to keep responses small).
```
with:
```
Shape varies by category. All items share `name`, `description`, `params[]`, and `example`. HTTP templates add `method`, `service`, `path`; Kafka patterns add `topic` and `match`; DB templates add `connection` and `sql` (so an agent can read a query's result columns before writing a `--path` value assertion).
```

- [ ] **Step 4: Verify**

Run: `grep -n 'default when neither flag' docs/DESIGN.md`
Expected: one match.

Run: `grep -n '"sql":' docs/DESIGN.md`
Expected: the discover.item example now contains a `"sql"` field (at least one match in §3.6).

Run: `grep -n 'SQL is omitted' docs/DESIGN.md`
Expected: **no** matches.

- [ ] **Step 5: Commit**

```bash
git add docs/DESIGN.md
git commit -m "feat(design): check ready defaults to all; discover.item shows SQL (D7, D9)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 8: D8 `--equals` coercion + D10 error typing + M5 envelope exception (§3.3, §4.1)

**Files:**
- Modify: `docs/DESIGN.md` (§3.3 value-assert, §4.1 envelope intro + error-type table)

- [ ] **Step 1: Add `--equals` coercion paragraph to §3.3**

Insert immediately **after** the final `--expect-value` example (the free-form SQL one) — after this exact text:

```
agctl db assert \
  --sql "SELECT status FROM orders WHERE id = :orderId" \
  --param orderId=ord-789 \
  --expect-value \
  --path ".status" \
  --equals "CONFIRMED"
```

— append (after a blank line):

```

**Value coercion (`--equals`):** The argument is parsed as JSON when it is valid JSON (`"0"` → number 0, `"true"` → bool, `"null"` → null, `"[1,2]"` → array); otherwise it is treated as a plain string (e.g. `CONFIRMED`). The DB result value is coerced to a JSON-native type before comparison — numbers → number, booleans → bool, timestamps/dates → ISO 8601 string, null → null. Comparison is strict and type-aware: a number never equals a string (`0` ≠ `"0"`). To match a timestamp column, write `--equals "2026-06-29T14:22:00Z"`.
```

> Note: Task 3 already converted the `$orderId` in that example to `:orderId`. This anchor uses the post-Task-3 form. Run Task 3 before this anchor will match.

- [ ] **Step 2: Note the `http ping` envelope exception in §4.1**

Replace:
```
### 4.1 Envelope

Every invocation writes exactly one JSON object to stdout:
```
with:
```
### 4.1 Envelope

Every invocation writes exactly one JSON object to stdout (the sole exception is `http ping`, which streams one JSON object per ping plus a final summary — see §3.1):
```

- [ ] **Step 3: Rewrite the §4.1 error-type table**

Replace:
```
| Type | Exit code | Description |
|---|---|---|
| `AssertionError` | 1 | An assertion was evaluated and failed |
| `ConfigError` | 2 | Config file missing, invalid, or env vars unresolved |
| `ConnectionError` | 2 | Could not reach a service, broker, or database |
| `TimeoutError` | 1 | Operation exceeded the configured timeout |
| `TemplateNotFound` | 2 | Named template does not exist in config |
| `InternalError` | 2 | Unexpected error in `agctl` itself |
```
with:
```
| Type | Exit code | Applies when |
|---|---|---|
| `AssertionError` | 1 | An assertion was evaluated and failed — including `kafka assert` timing out (no matching message within the window) and `kafka consume --expect-count` receiving fewer than expected |
| `ConfigError` | 2 | Config missing/invalid, an unresolvable **required** env var, or a major-version mismatch |
| `ConnectionError` | 2 | Could not reach a service, broker, or database |
| `TimeoutError` | 1 | A non-assertion operation exceeded its time budget (e.g. a slow HTTP request or a hung DB query) |
| `TemplateNotFound` | 2 | Named template/pattern/connection does not exist in config |
| `InternalError` | 2 | Unexpected error in `agctl` itself |
```

- [ ] **Step 4: Verify**

Run: `grep -n 'Value coercion' docs/DESIGN.md`
Expected: one match in §3.3.

Run: `grep -n 'Applies when' docs/DESIGN.md`
Expected: one match (new table header).

Run: `grep -n 'sole exception is .http ping' docs/DESIGN.md`
Expected: one match in §4.1.

- [ ] **Step 5: Commit**

```bash
git add docs/DESIGN.md
git commit -m "feat(design): --equals JSON coercion; AssertionError for assert-timeout; ping envelope note (D8, D10, M5)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 9: D4 — `__` override delimiter (§5)

**Files:**
- Modify: `docs/DESIGN.md` (§5 env-override convention)

- [ ] **Step 1: Rewrite the §5 override convention block**

Replace:
```
**Env var override convention** (`AGCTL_<SECTION>_<KEY>`):

```
AGCTL_DEFAULTS_TIMEOUT_SECONDS=30
AGCTL_KAFKA_DEFAULT_CONSUMER_GROUP=my-group
AGCTL_DATABASE_MAIN_DB_PASSWORD=s3cr3t
```

Nested keys are separated by `_`. Connection/service names with hyphens are uppercased and hyphens are converted to `_`.
```
with:
```
**Env var override convention** (`AGCTL_<SECTION>__<KEY>` — double-underscore `__` separates path segments; a single `_` stays within a key segment):

```
AGCTL_DEFAULTS__TIMEOUT_SECONDS=30
AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP=my-group
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD=s3cr3t
AGCTL_SERVICES__ORDER_SERVICE__BASE_URL=http://order-svc:8080
```

To map a config path to an env var: uppercase each path segment, convert hyphens to `_` within a segment, and join segments with `__`. Example: `services.order-service.base_url` → `AGCTL_SERVICES__ORDER_SERVICE__BASE_URL`. Parsing back is not guaranteed to reconstruct hyphens (a `_` could be a hyphen or a literal underscore); overrides are write-oriented, so this is acceptable. The `__` delimiter mirrors Pydantic Settings / Spring Boot and keeps keys containing single underscores unambiguous.
```

- [ ] **Step 2: Verify**

Run: `grep -nE 'AGCTL_[A-Z]+__[A-Z]' docs/DESIGN.md`
Expected: matches in §5 (and §8 after Task 12).

Run: `grep -n 'AGCTL_DATABASE_MAIN_DB_PASSWORD=s3cr3t' docs/DESIGN.md`
Expected: **no** matches in §5 (old flat form gone).

- [ ] **Step 3: Commit**

```bash
git add docs/DESIGN.md
git commit -m "fix(design): use __ delimiter for AGCTL_* overrides (D4)

Single-_ names were unparseable (underscore was both separator and key char).
Now __ separates path segments; single _ stays within a key.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 10: §6 AGENTS.md template — `--param`, SQL syntax, lookback note

**Files:**
- Modify: `docs/DESIGN.md` (§6 AGENTS.md template)

- [ ] **Step 1: Fix Pattern 3 — `--params` → `--param` and `$order_id` → `:order_id`**

Replace:
```
# 3. Assert DB row exists and has correct status
agctl db assert \
  --sql "SELECT 1 FROM orders WHERE id = \$order_id AND status = 'PENDING'" \
  --params order_id=$ORDER_ID \
  --expect-rows 1
```
with:
```
# 3. Assert DB row exists and has correct status
agctl db assert \
  --sql "SELECT 1 FROM orders WHERE id = :order_id AND status = 'PENDING'" \
  --param order_id=$ORDER_ID \
  --expect-rows 1
```

- [ ] **Step 2: Fix Pattern 4 — `--params` → `--param` and `$order_id` → `:order_id`**

Replace:
```
agctl db query \
  --sql "SELECT id, status, total_cents FROM orders WHERE id = \$order_id" \
  --params order_id=ord-789
```
with:
```
agctl db query \
  --sql "SELECT id, status, total_cents FROM orders WHERE id = :order_id" \
  --param order_id=ord-789
```

- [ ] **Step 3: Add a lookback-reliability note under "Common Testing Patterns"**

Insert immediately **after** the Pattern 2 block (the send-then-assert example) — after this exact text:

```
agctl kafka assert \
  --topic orders.created \
  --contains '{"customer_id": "cust-42"}' \
  --timeout 5
```

— append (after a blank line), before `#### Pattern 3`:

```

> Send-then-assert is reliable by default: `kafka assert` seeks back by `--lookback` (default = `--timeout`), so an event published just before the assert starts is still matched. See §3.2.
```

- [ ] **Step 4: Verify**

Run: `grep -n '\-\-params ' docs/DESIGN.md`
Expected: **no** matches anywhere.

Run: `grep -nE '\$(orderId|order_id|cid)\b' docs/DESIGN.md`
Expected: **no** matches anywhere (§6 was the last holdout).

Run: `grep -n 'reliable by default' docs/DESIGN.md`
Expected: one match in §6.

- [ ] **Step 5: Commit**

```bash
git add docs/DESIGN.md
git commit -m "docs(design): AGENTS.md uses --param and :param SQL; note lookback (M2, D2, D6)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 11: §7 pyproject — single `agctl` script + `agt` alias (M1, M3)

**Files:**
- Modify: `docs/DESIGN.md` (§7 pyproject skeleton)

- [ ] **Step 1: Fix the duplicated scripts block**

Replace:
```
[project.scripts]
agctl = "agctl.cli:cli"
agctl = "agctl.cli:cli"
```
with:
```
[project.scripts]
agctl = "agctl.cli:cli"
agt = "agctl.cli:cli"
```

- [ ] **Step 2: Optional — validate the TOML block parses**

If `python3` (3.11+) is available:

```bash
python3 -c "import tomllib,sys,re; \
src=open('docs/DESIGN.md').read(); \
m=re.search(r'\[build-system\].*?\[project\.entry-points\.\"agctl\.plugins\"\][^\n]*', src, re.S); \
tomllib.loads(m.group(0).split('```')[0]) and print('toml ok')"
```

Expected: `toml ok`. (If the regex is fiddly in your environment, skip — the grep check below is the required gate.)

- [ ] **Step 3: Verify**

Run: `grep -n 'agt = "agctl.cli:cli"' docs/DESIGN.md`
Expected: one match.

Run: `awk '/\[project\.scripts\]/{f=1} f&&/agctl = "agctl.cli:cli"/{c++} /^\[/{if($0!~/scripts/)f=0} END{print c}' docs/DESIGN.md`
Expected: `1` (the `agctl` script appears exactly once).

- [ ] **Step 4: Commit**

```bash
git add docs/DESIGN.md
git commit -m "fix(design): single agctl script + agt alias in pyproject (M1, M3)

Removes the duplicated [project.scripts] agctl line; adds the agt alias
referenced elsewhere in the doc.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 12: §8 principles — ping exception, windowed-assertion principle, override convention (M5, D6, D4)

**Files:**
- Modify: `docs/DESIGN.md` (§8)

- [ ] **Step 1: Add the `http ping` exception to "One JSON object per invocation"**

Replace:
```
### One JSON object per invocation

`output.emit()` is the **only** permitted stdout write path. Every command calls it exactly once before exiting. Any intermediate logging goes to stderr.
```
with:
```
### One JSON object per invocation

`output.emit()` is the **only** permitted stdout write path. Every command calls it exactly once before exiting. Any intermediate logging goes to stderr. The sole exception is `http ping`, which streams newline-delimited objects (one per ping plus a final summary) for background/keepalive use — see §3.1.
```

- [ ] **Step 2: Add a "Windowed assertions" principle**

Insert immediately **after** the "Stateless invocations" subsection — after this exact text:

```
No session files, no lock files, no local state databases. Each invocation is fully self-contained. Kafka consumer groups are used for offset tracking when needed; that state lives in Kafka, not on disk.
```

— append (after a blank line):

```

### Windowed assertions (reliable send-then-assert)

`kafka assert`/`consume` seek to `now - --lookback` and read forward, rather than subscribing at "latest". This makes the send-then-assert pattern reliable without subscribe-before-produce gymnastics: the lookback window catches events published just before the command started. See §3.2.
```

- [ ] **Step 3: Update the §8 env-override convention to `__`**

Replace:
```
### Env var override convention

```
AGCTL_<SECTION>_<KEY>=<value>
```

Mapping rules:
- Section and key are uppercased.
- Hyphens in YAML keys become underscores: `main-db` → `MAIN_DB`.
- Nested keys are flattened with `_`: `database.main-db.password` → `AGCTL_DATABASE_MAIN_DB_PASSWORD`.

Full examples:

```bash
AGCTL_DEFAULTS_TIMEOUT_SECONDS=30
AGCTL_KAFKA_DEFAULT_CONSUMER_GROUP=ci-consumer
AGCTL_DATABASE_MAIN_DB_HOST=localhost
AGCTL_DATABASE_MAIN_DB_PASSWORD=supersecret
AGCTL_SERVICES_ORDER_SERVICE_BASE_URL=http://order-svc:8080
```

These overrides are applied after `${}` interpolation and have the highest precedence.
```
with:
```
### Env var override convention

```
AGCTL_<SECTION>__<KEY>=<value>      # double-underscore __ separates path segments
```

Mapping rules:
- Each path segment is uppercased.
- Hyphens within a segment become underscores: `main-db` → `MAIN_DB`.
- Path segments are joined with `__` (double underscore), so a single `_` unambiguously belongs to a key: `database.connections.main-db.password` → `AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD`.

Full examples:

```bash
AGCTL_DEFAULTS__TIMEOUT_SECONDS=30
AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP=ci-consumer
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__HOST=localhost
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD=supersecret
AGCTL_SERVICES__ORDER_SERVICE__BASE_URL=http://order-svc:8080
```

These overrides are applied after `${}` interpolation and have the highest precedence. (The `__` delimiter mirrors Pydantic Settings / Spring Boot and keeps keys containing single underscores unambiguous.)
```

- [ ] **Step 4: Verify**

Run: `grep -n 'sole exception is .http ping' docs/DESIGN.md`
Expected: matches in §4.1 (Task 8) and now §8 (≥2 total).

Run: `grep -n 'Windowed assertions' docs/DESIGN.md`
Expected: one match in §8.

Run: `grep -nE 'AGCTL_DEFAULTS_TIMEOUT_SECONDS|AGCTL_DATABASE_MAIN_DB_HOST' docs/DESIGN.md`
Expected: **no** matches (old single-`_` examples gone).

- [ ] **Step 5: Commit**

```bash
git add docs/DESIGN.md
git commit -m "docs(design): §8 principles — ping exception, windowed assertions, __ overrides (M5, D6, D4)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 13: Version bump + whole-doc consistency sweep

**Files:**
- Modify: `docs/DESIGN.md` (header)

- [ ] **Step 1: Bump the header to v0.6 and link the spec**

Replace:
```
**Version:** 0.5-draft  
**Last updated:** 2026-07-02  
**Status:** Foundation design — ready for implementation
```
with:
```
**Version:** 0.6-draft  
**Last updated:** 2026-07-02  
**Status:** Foundation design — hardened; ready for implementation  
**Change spec:** [`docs/superpowers/specs/2026-07-02-agctl-design-hardening.md`](./superpowers/specs/2026-07-02-agctl-design-hardening.md)
```

- [ ] **Step 2: Run the whole-doc consistency sweep**

All of these must pass:

```bash
# One database: key only
test "$(grep -cE '^database:' docs/DESIGN.md)" -eq 1 && echo "D1 ok"

# No --params anywhere (must be --param)
! grep -nq -- '--params ' docs/DESIGN.md && echo "M2 ok"

# No \$paramName SQL leftovers anywhere
! grep -nqE '\$(orderId|order_id|cid|paramName)\b' docs/DESIGN.md && echo "D2 ok"

# No "default: latest" / "SQL is omitted" leftovers
! grep -nq 'default: latest' docs/DESIGN.md && ! grep -nq 'SQL is omitted' docs/DESIGN.md && echo "D6/D9 ok"

# agt alias present, agctl script singular
grep -q 'agt = "agctl.cli:cli"' docs/DESIGN.md && echo "M3 ok"

# __ overrides present in both §5 and §8
test "$(grep -cE 'AGCTL_[A-Z]+__[A-Z]' docs/DESIGN.md)" -ge 2 && echo "D4 ok"
```

Expected: every line prints `… ok`. If any check fails, revisit the corresponding task.

- [ ] **Step 3: Read-through for coherence**

Read `docs/DESIGN.md` §2–§8 end to end. Confirm: the config example is valid-looking YAML (one `database:` key); the kafka timing note appears; the error table reads correctly; no section still references the old `$param` / `--params` / flat `AGCTL_DATABASE_MAIN_DB_*` forms.

- [ ] **Step 4: Commit**

```bash
git add docs/DESIGN.md
git commit -m "docs(design): bump to v0.6-draft after hardening pass

All D1–D10 + M1–M5 changes applied per the hardening spec.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

- [ ] **Step 5: Final verify**

Run: `git log --oneline`
Expected: ~13 commits, baseline first, version-bump last.

---

## Self-review (completed during authoring)

**1. Spec coverage** — every spec decision maps to a task:
- D1 → Task 2 · D2 → Task 3 + Task 10 (§6) · D3 → Task 4 · D4 → Task 9 + Task 12 · D5 → Task 5 · D6 → Task 6 + Task 10/12 notes · D7 → Task 7 · D8 → Task 8 · D9 → Task 7 · D10 → Task 8 · M1 → Task 11 · M2 → Task 10 · M3 → Task 11 · M4 → Task 4 · M5 → Task 8 + Task 12. No spec section is uncovered.
**2. Placeholder scan** — no TBD/TODO/"add validation"; every edit shows exact old→new strings; verification uses concrete grep/expected-output. Anchors that depend on a prior task (Task 8 Step 1 uses the post-Task-3 `:orderId` form) are flagged with an ordering note.
**3. Consistency** — `database.connections.*` path is used identically in Task 2 (schema), Task 9/12 (override examples). `:paramName` is used uniformly after Task 3. Error-type names match between Task 8's table and Task 6's assert intro. `--lookback` appears in consume, assert, and the §3.2/§8 notes.
