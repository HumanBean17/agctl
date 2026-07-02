# `agctl` Design Hardening — Change Specification

**Version:** 0.6-draft
**Date:** 2026-07-02
**Status:** Approved change-set — ready for implementation planning
**Relates to:** [`docs/DESIGN.md`](../../DESIGN.md) (v0.5-draft)
**Scope:** Hardening pass — fix bugs-as-written, resolve semantic gaps, tighten consistency. No new features; no changes to §9 Extension Points or §10 Open Questions.

---

## 1. Context

`DESIGN.md` v0.5 is detailed and marked "ready for implementation," but an audit found one config-loading bug (a duplicate YAML key that silently drops data), several correctness gaps that would cause silent wrong results at runtime, and a set of internal inconsistencies. This spec captures every change needed to make the design implementation-ready, with rationale and exact edits.

Audit method: full read of `DESIGN.md`; findings grouped by severity — 🔴 broken as written, 🟠 correctness (silent wrong results), 🟡 inconsistency, 🔵 usability/spec gap. Each finding maps to a single decision; all decisions were reviewed and approved with the author.

This document is the **brainstorming deliverable**. Applying these changes to `DESIGN.md` (and the `AGENTS.md` / `pyproject.toml` skeletons) is the subsequent implementation step, driven by a writing-plans plan.

---

## 2. Decision summary

| Ref | Finding (severity) | Decision |
|---|---|---|
| D1 | Duplicate `database:` key (🔴) | One `database:` block: `connections:` + `templates:` |
| D2 | Three parameter syntaxes (🟡) | HTTP `{n}`; all SQL `:n`; drop `$n` |
| D3 | Optional `${VAR}` vs fail-loud (🟠) | Add `${VAR:-default}`; bare `${VAR}` stays a hard error |
| D4 | `AGCTL_*` names unparseable (🔵) | `__` double-underscore nesting delimiter |
| D5 | `--body` merge undefined (🔵) | Deep merge, arrays replaced wholesale, scalar wins |
| D6 | `kafka assert` races the producer (🟠) | Timestamp-lookback window (`now - lookback`) |
| D7 | `check ready` no-flag behavior undefined (🟠) | Default to all |
| D8 | `--equals` type coercion undefined (🟠) | JSON-typed-when-valid else string; strict compare |
| D9 | `discover.item` hides SQL (🔵) | Include `sql` in db-template detail |
| D10 | assert-timeout error type fuzzy (🟠) | `AssertionError`; `TimeoutError` reserved for op timeouts |
| M1 | Duplicate `[project.scripts]` line (🔴) | Single `agctl` line + `agt` alias |
| M2 | `--params` vs `--param` (🟡) | `--param` everywhere |
| M3 | `agt` alias referenced but not installed (🟡) | Install `agt` as an alias |
| M4 | `version` mismatch undefined (🟠) | Major-only; mismatch → ConfigError (exit 2) |
| M5 | `http ping` breaks one-envelope rule (🟡) | Documented as the sole exception |

---

## 3. Detailed changes

### 3.1 §2 Configuration Schema

#### D1 — Resolve the duplicate `database:` key (🔴)

Replace the two separate top-level `database:` blocks with one. Under the new shape, connections live under `database.connections:` and query templates under `database.templates:`:

```yaml
database:
  connections:
    main-db:
      type: postgresql
      host: "${DB_HOST}"
      port: 5432
      dbname: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      default: true
    analytics-db:
      type: postgresql
      host: "${ANALYTICS_DB_HOST}"
      port: 5432
      dbname: "analytics"
      user: "${ANALYTICS_DB_USER}"
      password: "${ANALYTICS_DB_PASSWORD}"

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

`defaults.database_connection: main-db` still references a connection name (unchanged). HTTP templates remain under top-level `templates:`; Kafka patterns remain under `kafka.patterns:`. That asymmetry is now a documented choice (HTTP is the primary surface; the protocol-specific blocks stay nested under their protocol), not an accident.

**Rationale:** a duplicate top-level YAML key is silently resolved last-wins by standard parsers (PyYAML included), so the first `database:` block — every connection profile — would be dropped at load time. The tool would then have templates referencing connections that no longer exist.

#### D2 — Unify parameter syntax (🟡)

- HTTP templates (path and body): `{name}`, filled via `--param name=value`. *(unchanged)*
- SQL templates **and** free-form SQL: `:name` (JDBC-style), filled via `--param name=value`. *(free-form SQL changes from `$name` to `:name`)*

`agctl` translates `:name` to the driver's native bind syntax at execution time (psycopg → `%(name)s`). `{name}` is intentionally avoided inside SQL because it collides with JSON literals (e.g. `'{"a":1}'::jsonb`).

Examples to rewrite — every `$paramName` becomes `:paramName`:
- §3.3 free-form SQL examples (`$orderId` → `:orderId`)
- §4.1 error-detail SQL string (`$order_id` → `:order_id`)
- §6 AGENTS.md Patterns 3 & 4, and UC-5 (`$order_id` / `$cid` → `:order_id` / `:cid`)

#### D3 — Optional environment interpolation (🟠)

`${VAR}` resolution rules:
- `${VAR}` — **required**. Missing → ConfigError (exit 2); never an empty substitution. *(unchanged)*
- `${VAR:-default}` — **optional with default**. Missing → the literal `default`.
- `${VAR:-}` — **optional, empty**. Missing → empty string, no error.

This lets optional fields like `schema_registry_url: "${SCHEMA_REGISTRY_URL:-}"` coexist with the fail-loud principle: bare `${VAR}` remains the default for required values, and the `:-` form is an explicit opt-in for optional ones. The `${VAR}` syntax is still only supported in string scalar values, not keys.

#### M4 — Config version (🟠)

`version` encodes the major version only (currently `"1"`). A major-version mismatch between file and tool is a ConfigError (exit 2); minor/patch differences are not tracked. `agctl config validate` enforces this.

### 3.2 §3 CLI Commands

#### D5 — `http call --body` merge semantics (🔵)

`--body '{...}'` deep-merges into the template body:
- Objects merge recursively.
- Arrays are replaced wholesale (not element-merged).
- Scalar leaves from `--body` override the template.

#### D6 — Kafka offset/timing model (🟠)

`kafka assert` and `kafka consume` no longer rely on "latest offset" semantics. At start, the consumer seeks each assigned partition to the timestamp `now - lookback` via `offsets_for_times`, then reads forward — until a match is found (`assert`) or the window / expected count is satisfied (`consume`).

New and changed flags:
- `--lookback <seconds>` — how far before "now" to seek. Default = the resolved operation `--timeout` (symmetric window: look back as far as you wait forward).
- `--from-beginning` — overrides to the earliest offset (seek to start).
- `--consumer-group <name>` — retained for callers that want offset continuity; default group is `agctl-consumer`. For `assert`, committed offsets are ignored (the time-seek determines the start each invocation), so repeated asserts are independent and deterministic.

**Effect:** the send-then-assert pattern is reliable — an event published a moment before the assert starts still falls inside the lookback window and is matched. This must be documented in §3.2 and the AGENTS.md patterns as *the reason* the pattern works.

**Caveat (to document):** messages older than the lookback window are not seen unless `--from-beginning` is used; on high-volume topics, agents should narrow with `--match`/`--contains` to avoid matching stale events from prior runs.

#### D7 — `check ready` default (🟠)

`agctl check ready` with neither `--service` nor `--all` checks every configured service (equivalent to `--all`).

#### D9 — `discover.item` includes SQL for db-templates (🔵)

db-template detail adds the `sql` field. **Rationale:** an agent needs a query's result columns to write `--path ".status"` value assertions; hiding SQL forces a speculative `db query` round-trip first. Detail views are fetched one-at-a-time (lazy discovery), so including SQL does not bloat category listings.

### 3.3 §4 Output Schema

#### D8 — `--expect-value --equals` type handling (🟠)

`--equals <value>` resolution and comparison:
1. Parse `<value>` as JSON. If valid, use the typed value (`"0"` → number 0, `"true"` → bool, `"[1,2]"` → array, `"null"` → null).
2. If not valid JSON, treat as a plain string (e.g. `CONFIRMED`).
3. Coerce the DB result value to a JSON-native type before comparing: numbers → number, booleans → bool, timestamps/dates → ISO 8601 string, null → null.
4. Compare with strict type-aware equality (number ≠ string; `0` ≠ `"0"`).

The timestamp → ISO 8601 coercion is documented explicitly so callers know to write `--equals "2026-06-29T14:22:00Z"`.

#### D10 — Error typing (🟠)

Update the error-type table so intent is unambiguous:

| Type | Exit | Applies when |
|---|---|---|
| `AssertionError` | 1 | An assertion was evaluated and failed — **including `kafka assert` timing out** (expected message not seen within the window) and **`kafka consume --expect-count` receiving fewer than expected** |
| `TimeoutError` | 1 | A non-assertion operation exceeded its time budget — e.g. a slow HTTP request (`http call` / `request` per-request timeout), a hung DB query |
| `ConfigError` | 2 | Config missing/invalid, unresolvable **required** env var, major-version mismatch |
| `ConnectionError` | 2 | Service / broker / database unreachable |
| `TemplateNotFound` | 2 | Named template / pattern / connection does not exist |
| `InternalError` | 2 | Unexpected error in `agctl` itself |

Key change: assert-timeout and consume-count-miss move from `TimeoutError` to `AssertionError` (exit code stays 1). The test semantics — "the expected state did not materialize" — are what an agent reading `error.type` cares about.

#### M5 — `http ping` envelope exception (🟡)

§4.1 and §8 state "exactly one JSON object per invocation / `emit()` called once." Add an explicit, named exception: `http ping` emits newline-delimited JSON (one object per ping plus a final summary), and ping lines are **not** the standard envelope. This is the only command with streaming output; it is required for background/keepalive use.

### 3.4 §5 Configuration Resolution Order

#### D4 — Override delimiter (🔵)

`AGCTL_*` overrides use `__` (double underscore) to separate path segments; a single `_` stays within a key segment. Hyphens in YAML keys map to `_` within a segment (then the segment is uppercased). This matches the Pydantic Settings / Spring convention and makes the common case — keys that contain single underscores — unambiguous, which the single-`_` scheme was not.

Mapping (config path → env var):
- `kafka.default_consumer_group` → `AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP`
- `database.connections.main-db.password` → `AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD`
- `services.order-service.base_url` → `AGCTL_SERVICES__ORDER_SERVICE__BASE_URL`
- `defaults.timeout_seconds` → `AGCTL_DEFAULTS__TIMEOUT_SECONDS`

To parse back: split on `__`; each segment is lowercased and its internal `_` preserved as a literal key character. (Hyphen reconstruction from `_` is not guaranteed and not required — overrides are write-oriented.)

### 3.5 §6 AGENTS.md Template (mechanical)

- Every `--params key=value` → `--param key=value` (Patterns 3 & 4).
- `agt` alias references are kept — it is now installed (see §3.6).
- Add a one-line note that send-then-assert is reliable by default thanks to the lookback window.

### 3.6 §7 Project Structure / pyproject (mechanical)

- `[project.scripts]`: a single `agctl = "agctl.cli:cli"` plus `agt = "agctl.cli:cli"` (replaces the duplicated `agctl` line).
- If the `DBDriver` / config-structure prose is touched, refresh it to reflect `database.connections`.

### 3.7 §8 Key Design Principles

- Add: `http ping` is the documented sole exception to one-object-per-invocation (ties to M5).
- Add: assertions are window-based (timestamp lookback), which makes send-then-assert reliable without subscribe-before-produce gymnastics (ties to D6).
- Update: the env-override convention uses `__` (ties to D4).

---

## 4. Out of scope

- **§9 Extension Points** — unchanged.
- **§10 Open Questions / Future Work** — all items remain deferred: schema-registry integration, message-ordering assertions, multi-step scenario chaining, MCP server wrapper, retry/polling DSL, template-variable validation, gRPC support, secret backends, parallel command execution, OpenTelemetry trace propagation. Notably, template-variable validation (warn/error on unsupplied `{placeholder}` / `:param`) stays deferred — the D2/D3 changes do not enable it.
- **§1 Non-Goals** — unchanged (human-facing formatting, mocking, infra lifecycle, auto-test generation remain out of scope).
- Result-set pagination and row-count caps for `db query` were not surfaced by the audit and are deferred.

---

## 5. Verification notes (for the implementation plan)

These are the regressions and edge cases the implementation must cover; they belong in the plan, not in this design spec, but are listed so nothing is lost:

- **D1:** config-load test — load a full `agctl.yaml` and assert *both* connections and templates are present (the exact bug being fixed).
- **D2:** a free-form SQL query with a `:param` binds correctly; a JSON-literal SQL string containing `{}` is not mis-interpreted as a placeholder.
- **D3:** required `${VAR}` missing → exit 2; `${VAR:-x}` missing → `x`; `${VAR:-}` missing → empty, exit 0.
- **D6:** integration test — produce then assert within the window passes; a message older than the lookback is not matched (without `--from-beginning`).
- **D8:** unit tests for each JSON type, the timestamp → ISO 8601 path, and the not-valid-JSON → string fallback.
- **D10:** one test per error-type cell (assert-timeout → `AssertionError` exit 1; slow request → `TimeoutError` exit 1; missing template → `TemplateNotFound` exit 2; etc.).
