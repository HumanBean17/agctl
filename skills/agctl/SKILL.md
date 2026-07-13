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
- `2` — tool/config/env error: *your command* is wrong (bad flag, unresolved
  `${ENV}`, bad config). Never a test result — fix the invocation.

Read `ok` first; on `false` read `error.type`. Parse **stdout only** — stderr is
diagnostics. **`agctl <cmd> --help` is the authoritative flag spec** (flags, types,
required args, even `--lookback`/`--consumer-group`/`--assertion`); this skill
keeps only what `--help` won't tell you — semantics, roots, traps.

**Pin the version you target.** `--match`/`--jq-path` roots and failure shapes
moved in agctl ≥1.0 (dialect v2), and the config schema moved to v3 (named
`kafka.clusters`). A stale global install (e.g. `0.1.0`, where `--match` was
body-rooted) silently contradicts this skill. After upgrading, reinstall in every
env that runs it (`pip install -U 'agctl[jq]'`) so `--help` and behavior match.

## Orient first: `agctl discover`

`discover` is a map, not a dump — look up template/topic names, don't guess.
Categories: `services`, `http-templates`, `kafka-patterns`, `db-templates`,
`mock-http-stubs`, `mock-kafka-reactors`. (`--category` / `--name` / `--search`
flags: see `--help`.)

## Intent → command

| Intent | Command |
|---|---|
| What can I do here? | `discover` |
| Send a known / ad-hoc request | `http call <tpl> [--param…]` / `http request (--service S --path P \| --url …)` |
| Assert an HTTP response | `http call/request … --status N [--contains '{…}'] [--match '<jq>'] [--jq-path .x --equals v]` |
| Verify an event was published | `kafka assert [--topic T] <mode> --timeout N` |
| See what was published | `kafka consume --topic T [--match …]` |
| Publish a message | `kafka produce --topic T --message '{…}'` |
| DB write / rows / value / inspect / schema | `db execute --write` / `db assert --expect-rows N` / `db assert --expect-value --path .x --equals v` / `db query` / `db schema` |
| Call gRPC services / healthcheck | `grpc call <tpl> [--param…]` / `grpc call --target T --address host:port` / `grpc healthcheck` |
| Impersonate a dependency | `mock run` (foreground) / `mock start\|stop\|status` (daemon) |
| Are services up? / validate config / migrate v1/v2→v3 | `check ready --all` / `config validate` / `config migrate` |

`--config <path>`, `--overlay <path>` (repeatable), and `--env-file <path>` are global;
**`--timeout` is not global**. A `.env` next to the resolved `agctl.yaml` is auto-loaded as
env defaults (real env wins) — reach for `--env-file`/`AGCTL_ENV_FILE` only to point at a
different one. `<mode>` for kafka = `--contains '{…}' | --match '<jq>' | --pattern <name>`.
`kafka produce|consume|assert` take `--cluster <name>` (default: the pattern's bound
cluster for `assert --pattern`, else `kafka.default_cluster`, else the single defined
cluster; `--cluster` always wins) — set it only when a command must target a non-default
cluster.

## Flag semantics (`--help` shows flags; these are how they behave)

- `http call --body` is **deep-merged** over the template body (add/override);
  `--header` merges with template headers, caller wins.
- `db` and `kafka produce` have **no** `--timeout`; `kafka assert --timeout` is **required**.
- **HTTP assertions** (`http call`/`request`): `--status` / `--contains '{…}'` /
  `--match '<jq>'` / `--jq-path <jq> --equals <v>`. ≥1 flag ⇒ assertion mode, all AND,
  fail ⇒ exit 1 (`AssertionError`). `--jq-path` needs `--equals` (else exit 2).
  `--match` = "any truthy output"; for "all items" use `all(.body.items[]; .pred)`.
  `--match` / `--jq-path` need `pip install 'agctl[jq]'`.

## Gotchas (what `--help` won't tell you)

1. **Two streaming commands** — `http ping` (one JSON object **per ping**) and
   `mock run` (one NDJSON event per line + a final `summary`). Background with `&`,
   `kill` when done; exit 0 (all ok) / 1 (any failed). The managed `mock start`/
   `stop`/`status` are **not** streaming — each emits one object. Every other
   command emits exactly one object.
2. **A 4xx/5xx HTTP response is `ok:true` — unless you assert.** Status is a
   *result*, not an error. Add `--status`/`--contains`/`--match`/`--jq-path`/
   `--equals` to `http call`/`request` to flip a wrong response into
   `AssertionError` (exit 1); zero assertion flags leaves the result path unchanged.
3. **`--match` is envelope-rooted (dialect `"2"`+; not payload-rooted).** `--help`
   names the envelope; the trap is the migration. (v3 only changed the kafka config
   shape to named clusters — the `match` rooting is unchanged.)
   - **HTTP** `--match` → response envelope `{status_code, response_time_ms, headers
     (lowercased), body, url, method}` ⇒ `.body.order_id`, `.status_code`, `.headers.x`.
     Prefix legacy body-form exprs with `.body | ` (`.status == "X"` → `.body | .status == "X"`).
   - **Kafka** `assert`/`consume --match` and `kafka.patterns[].match` → message
     envelope `{key, value, partition, offset, timestamp, headers}` ⇒ `.value.eventType`,
     `.key`, `.headers.rqUID` (header keys **case-sensitive**). Prefix legacy
     value-form with `.value | `.
   - **Unchanged:** `match.body` (json_subset), `--contains`, `--path`,
     `--jq-path`/`--equals` (still body-rooted), `--status`.
   - A v1 or v2 `agctl.yaml` is rejected (exit 2) → `config migrate` lifts it to v3
     (structural `kafka.clusters` lift for v1/v2; the three `match`-site families are
     `.body | ` / `.value | `-prefixed for **v1 only**). **CLI `--match` flags in
     scripts/prompts are NOT rewritten** — prefix them by hand, and only for v1 inputs.
4. **Three placeholder syntaxes — don't mix:** `${VAR}` env, resolved at config
   load (required → exit 2 if unset; `${VAR:-default}` optional; `${VAR:-}`
   optional/empty); `{name}` HTTP path/body & Kafka patterns, filled at call time
   by `--param`; `:name` SQL params (templates & `--sql`), filled by `--param`.
5. **Kafka reads are windowed, not "latest".** `consume`/`assert` seek to
   `now - --lookback` (default = `--timeout`) and read forward — an event published
   just before you started is still matched (send-then-assert reliable by default).
   `--from-beginning` → earliest. Narrow busy topics with `--match`/`--contains`
   so you don't match stale events.
6. **`kafka assert` modes are combinable** — several given ⇒ **all** must pass.
   `--pattern` infers topic from config (omit `--topic`). No match in window ⇒
   exit 1 with `error.detail = {topic, timeout}` (distinct from `ConnectionError`,
   exit 2).
7. **`db assert`** takes exactly one mode; `--expect-value` needs **both** `--path`
   and `--equals`. `--equals` is JSON-parsed if valid (`"0"`→0, `"true"`→bool,
   `"null"`→null) else plain string; compared **strictly** (`0` ≠ `"0"`). Match a
   timestamp column with `--equals "2026-…Z"`.
8. **`ConnectionError` is exit 2.** Service/broker/DB unreachable — run
   `check ready --all`, confirm it's up before retrying; don't blame the assertion.
9. **No built-in "event did NOT arrive" assert.** `kafka consume --expect-count 0`
   is **not** it (always exits 0). Check absence via `kafka consume --topic T
   --timeout N [--match …]` and inspect `result.count` (0 = no match in window).
10. **`db execute` needs two gates** — a `writable: true` connection **and** the
    `--write` flag — plus an **explicit target** (`--template` or `--connection`;
    refuses implicit default-connection writes). A `mode: read` template is
    rejected (exit 2). Result echoes `rows_affected` (int or null), `returning`
    (rows if `RETURNING`, else `[]`), `connection`, `sql`. **No idempotency** —
    encode `ON CONFLICT` (Postgres) / `ON DUPLICATE KEY UPDATE` (MySQL) in SQL.
11. **`db schema` reads `pg_catalog`, cluster-wide and NOT privilege-filtered.**
    On a shared cluster it can list relations this connection cannot `SELECT` from
    — discovering a name is not a grant. Treat the listing as "visible," not
    "accessible"; let the subsequent `SELECT` fail loudly.
12. **Assertion failures self-document their root + payload.** When
    `--match`/`--jq-path`/`--contains`/`--path`/`--equals` fail, read
    `error.detail.failures[].root` (HTTP) / `error.detail.root` (DB) /
    `error.detail.modes[].root` (Kafka) to see what the expression was evaluated
    against: `"response envelope"` vs `"response body"` (HTTP), `"message
    envelope"` vs `"message value"` (Kafka), `"first row"` (DB `--path`). The
    payload snapshot (`"body"`/`"row"`/`"rows"`/`"modes"`) shows the actual data —
    correct a mis-rooted jq path (e.g. `.data.operator` → `.body.data.operator`)
    without dropping the flag and re-running raw.
13. **`mock stop` uses the strict failure rule.** Unlike `mock run` (exit 1 only
    when `kafka_errors > 0`), `mock stop` treats **any** of `http.unmatched`,
    `http.body_parse_skipped`, `kafka.skipped`, or `kafka.error` as fatal ⇒ exit 1
    (verdict in `error.detail`); `capture.missing` is non-fatal but surfaced. On a
    clean stop the verdict travels in `result`.
14. **`mock stop --all` returns an array of verdicts.** `--all` iterates every
    running mock in `--state-dir`; `result.stopped` is an **array** (one entry per
    mock). If any mock was fatal, exit 1 carries the array in `error.detail.stopped`.
    A single selector (`--listen`/`--pid`/no-arg) returns a boolean.
15. **`mock start` is the readiness gate.** Blocks until the daemon's `started`
    line appears (or a startup error/timeout), then returns — no separate polling.
    The old four-step protocol is now `mock start` → `mock stop`.
16. **Daemon state under `.agctl/` is the only on-disk state.** Managed daemons
    write a pidfile (`mock-<port>.pid`) and log (`mock-<port>.log`) under
    `<state-dir>/` (default `./.agctl/`). Sole exception to the stateless-invocation
    principle, scoped to the daemon lifecycle. Clean up with `rm -rf .agctl`.

## Discover live schema before authoring SQL

`db schema` is **read-only and ungated** — no `--write`/`--template`/`--sql`/`--param`;
ignores `writable`/`mode`; any configured connection is eligible. Use it before
authoring `db execute`/`db query` SQL: `pg_catalog` is the source of truth, not
your memory.

Two levels: (1) **list relations** `db schema [--connection C] [--schema S]` (tag
`db.schema.tables`) → `{count, items:[{schema, name, kind, column_count}], hint}`;
`kind` is `"table"`/`"view"`. (2) **drill one** `db schema --table T [--schema S]
[--connection C]` (tag `db.schema.table`) → `{columns:[{name, data_type, nullable,
default, generated, …}], primary_key, foreign_keys, unique_constraints, hint}`.
`--table` accepts views; match is **exact-case** on the stored name. 0 matches ⇒
`ConfigError` (exit 2, "run Level 1"); >1 across schemas ⇒ `ConfigError` with
`error.detail.candidates=[{schema, kind}]` → disambiguate with `--schema`.

**Authoring rules the schema tells you (load-bearing):**
1. **Quote mixed-case / reserved identifiers.** Postgres folds unquoted ids to
   lowercase. If a name has uppercase / non-`[a-z0-9_]` chars or matches a reserved
   word, double-quote it: `"OrderItems"`, `"user"`. The `name` field is the exact
   stored case — copy it verbatim, quoting as needed.
2. **Omit generated columns from INSERT.** `generated == "always_identity"` or
   `"stored"` MUST be omitted from INSERT (and `"stored"` from UPDATE too).
   `"by_default_identity"` and serial (`default nextval(...)`, `generated == null`)
   may be supplied or omitted.

## `agctl mock` — impersonate a dependency

Stands in for the SUT's **external** deps — an HTTP API the SUT calls, or the
downstream Kafka consumer expected to react to its events. **SUT-facing:** the
app's HTTP client points at the mock's `listen`; Kafka reactors join the SUT's
*real* broker as consumers (the mock is not a broker). Stubs/reactors are authored
in the `mocks:` config section (see the `agctl-config` skill).

Two modes: **foreground streaming** (`mock run`) and **managed daemon**
(`mock start`/`stop`/`status`). **Prefer the daemon mode for new tests** — it
collapses the four-step background protocol into `mock start` → `mock stop` and
surfaces failures cleanly (see gotchas 13–15).

**Windows:** the managed daemon is unavailable on native Windows (exits `2`
with a `ConfigError` pointing at `mock run`/WSL) — use `agctl mock run`
(foreground) there or run inside WSL.

### Using `mock run` directly — background lifecycle (load-bearing)

The failure signals (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`,
`kafka.error`, `capture.missing`) live **only on stdout**, and the exit-1
escalation arrives only on a clean `SIGTERM`. The plain `&`/`kill` pattern loses
both and silently produces a **false green**. Always:

1. Redirect stdout to a log: `agctl mock run > mock.log 2>&1 &` (capture the PID).
2. Poll `mock.log` for the `started` line **before** running the SUT — don't sleep
   a fixed delay.
3. Stop with `SIGTERM` + `wait` — **never `SIGKILL`** (skips the shutdown handler,
   the `summary` line, and the exit code).
4. Grep the log for `http.unmatched|http.body_parse_skipped|kafka.skipped|kafka.error|capture.missing`
   **regardless of the test result** — any hit is a failure, even if assertions
   passed. `capture.missing` is non-fatal at runtime (the mock substitutes empty
   string and continues), but it marks a `capture.from` that resolved to nothing —
   usually a misconfigured path silently yielding a plausible-but-wrong field.
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

# Assert the HTTP response in one call (no shell jq; exit 1 if it fails)
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001 \
  --status 201 --match '.body.order_id != null' --contains '{"status": "PENDING"}'
# Type-aware value equality via jq path (0 ≠ "0"; needs both flags)
agctl http call get-order --param order_id=ord-789 --jq-path '.status' --equals '"CONFIRMED"'

# E2E: thread an ID through HTTP → Kafka → DB
OID=$(agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001 | jq -r '.result.body.order_id')
agctl kafka assert --topic orders.created --contains "{\"order_id\":\"$OID\"}" --timeout 10
agctl db assert --sql "SELECT 1 FROM orders WHERE id = :order_id AND status = 'PENDING'" --param order_id="$OID" --expect-rows 1

# Seed DB state before a test (idempotent write)
agctl db execute --template upsert-customer --param customerId=cust-42 --param email=test@example.com --write

# Keep a session alive during a long test (background, capture PID, kill when done)
agctl http ping heartbeat --interval 5 --until-stopped &
PID=$!; … run the scenario …; kill "$PID"
```

## `agctl grpc` — gRPC service calls

gRPC support requires the `grpc` extra: `pip install 'agctl[grpc]'` (includes grpcio, grpcio-tools, grpcio-health-checking, grpcio-reflection, protobuf, jq).

**Two invocation modes:**
- **Template mode** `grpc call <template>` — resolves `grpc.templates[<name>]` (service, method, request body with `{placeholder}` support)
- **Free-form mode** `grpc call --target <name>` or `--address host:port` — ad-hoc calls without config templates

**Four call types** (auto-detected from method descriptor):
- **Unary** — single request, single response (returns `grpc.call` envelope with `call_type: "unary"`)
- **Client-stream** — NDJSON stdin requests → single response (same envelope shape)
- **Server-stream** — single request → NDJSON stdout responses (streaming exception; one JSON object per response + final `summary`)
- **Bidi** — NDJSON stdin requests ↔ NDJSON stdout responses (streaming exception; bidirectional stream)

**stdin/stdout NDJSON model:** For client-stream and bidi calls, pipe NDJSON request objects via stdin. Each line is a complete JSON request object (placeholders filled per line). Server-stream and bidi emit one NDJSON line per response message plus a final `{"summary": true, ...}`.

**Status-as-result semantics:** gRPC status codes are result fields, not assertion failures. A non-OK status (e.g. `StatusCode.NOT_FOUND`) still returns `ok: true` with `result.status.code`/`name`/`message`. Assertions (`--status`, `--match`, etc.) evaluate separately and raise `AssertionFailure` (exit 1) on mismatch.

**Config structure:**
```yaml
grpc:
  targets:
    my-service:
      address: "host:port"
      use_tls: false
      # tls: { override_authority: "" }  # optional for TLS
  descriptors:                    # fallback when reflection is off / unavailable
    - proto: "protos/my-service/v1/*.proto"   # compile .proto globs at load
      include_paths: ["protos"]
    # OR a pre-compiled descriptor set:
    # - descriptor_set: "protos/compiled/my-service.pb"
  templates:
    my-method:
      target: my-service
      service: "ServiceName"
      method: "MethodName"
      request: { field: "{value}" }
```

**Discovery:** `agctl discover --category grpc-services` / `--category grpc-methods` lists discoverable services and methods (reflection-based or from descriptors).

**Gotchas:**
- **`--address` format** must be `host:port` (single colon, both non-empty). Mutually exclusive with `--target`.
- **Reflection-first fallback:** Agctl tries server reflection first; if unavailable, falls back to `descriptors[]` proto files. Provide descriptors for air-gapped environments.
- **Plaintext by default:** Set `use_tls: true` (and optionally `tls.override_authority`) for TLS services. Plaintext h2c is the default.
- **Streaming exceptions:** Server-stream and bidi are the 4th and 5th streaming exceptions (after `http ping`, `mock run`, `logs tail`). Background with `&`, capture PID, kill when done.

Prefer **templates** over free-form. Explore with **`discover`**, never `config show`.
