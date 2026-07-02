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
Categories: `services`, `http-templates`, `kafka-patterns`, `db-templates`.

## Which command for which intent

| Intent | Command |
|---|---|
| What can I do here? | `agctl discover` |
| Send a known request | `agctl http call <tpl> [--param k=v]…` |
| Ad-hoc request | `agctl http request --service S --method M --path P` |
| Verify an event was published | `agctl kafka assert --topic T <mode> --timeout N` |
| See what was published | `agctl kafka consume --topic T [--match <jq>]` |
| Publish a message | `agctl kafka produce --topic T --message '{…}'` |
| Assert a DB row count | `agctl db assert (--template\|--sql) --expect-rows N` |
| Assert a DB field value | `agctl db assert (…) --expect-value --path .x --equals v` |
| Inspect raw DB state | `agctl db query (--template\|--sql)` |
| Are services up? | `agctl check ready --all` |
| Validate / debug config | `agctl config validate` / `config show` |

## Command forms

Only `--config <path>` is global. `--timeout` is **not** global (see gotchas).
`[brackets]` = optional; trailing `…` = repeatable; `<mode>` = one of
`--contains '{…}' | --match '<jq>' | --pattern <name>`.

```
agctl http call   <tpl> [--param k=v]… [--body '{…}'] [--header k=v]… [--timeout N]
agctl http request --service S --method GET|POST|PUT|PATCH|DELETE --path P [--body '{…}'] [--header k=v]…
agctl http ping   [<tpl> | --service S --path P] --interval N [--duration N | --until-stopped]   # streams NDJSON; background it

agctl kafka assert [--topic T] <mode> [--param k=v]… [--path <jq>] --timeout N [--from-beginning]
agctl kafka consume --topic T [--timeout N] [--match '<jq>'] [--expect-count N] [--from-beginning]
agctl kafka produce --topic T --message '{…}' [--key K] [--header k=v]…

agctl db query   (--template T | --sql "…") [--param k=v]… [--connection C]
agctl db assert  (--template T | --sql "…") (--expect-rows N | --expect-value --path <jq> --equals V)

agctl check ready [--service S | --all]
agctl config validate | config show [--unmask]
```

- `--body` on `http call` is **deep-merged** over the template body (adds/overrides).
- `--header` merges with template headers; caller wins.
- `db`/`kafka produce` have **no** `--timeout`. `kafka assert --timeout` is **required**.

## Gotchas (what `--help` won't tell you)

1. **`http ping` is the only streaming command** — one JSON object **per ping**
   (NDJSON), meant to run backgrounded with `&`; `kill` it when done. Exits `0`
   (all ok) / `1` (any failed). Everything else emits exactly one object.
2. **A 4xx/5xx HTTP response is `ok:true`.** Status is a *result*, not an error.
3. **Three placeholder syntaxes — don't mix them:**
   - `${VAR}` — env var, resolved at **config load** (`${VAR}` required → exit 2 if
     unset; `${VAR:-default}` optional; `${VAR:-}` optional/empty).
   - `{name}` — HTTP path/body & Kafka patterns, filled at **call time** by `--param`.
   - `:name` — SQL params (templates and `--sql`), filled by `--param`.
4. **Kafka reads are windowed, not "latest".** `consume`/`assert` seek to
   `now - --lookback` (default = `--timeout`) and read forward — so an event
   published just before you started is still matched (send-then-assert is
   reliable by default). `--from-beginning` → earliest offset. Narrow busy topics
   with `--match`/`--contains` so you don't match stale events.
5. **`kafka assert`** modes are **combinable** — when several are given, **all**
   must pass. `--pattern` infers the topic from config (omit `--topic`). On no
   match within the window it exits `1` with `error.detail = {topic, timeout}`
   (distinct from a `ConnectionError`, which is exit `2`).
6. **`db assert`** takes exactly one mode; `--expect-value` needs **both**
   `--path` and `--equals`. `--equals` is JSON-parsed if valid (`"0"`→0,
   `"true"`→bool, `"null"`→null) else a plain string; compared **strictly**
   (`0` ≠ `"0"`). Match a timestamp column with `--equals "2026-…Z"`.
7. **`ConnectionError` is exit `2`.** The service/broker/DB is unreachable — run
   `agctl check ready --all` and confirm it's up before retrying; don't blame the
   assertion.
8. **No built-in "event did NOT arrive" assert.** `kafka consume --expect-count 0`
   is **not** it (it always exits 0). To check absence, run `kafka consume --topic
   T --timeout N [--match …]` and inspect `result.count` (0 = no match in window).

## Recipes

```bash
# Send → assert the downstream Kafka event (reliable by default)
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001
agctl kafka assert --topic orders.created --contains '{"customer_id":"cust-42"}' --timeout 10

# E2E: thread an ID through HTTP → Kafka → DB
OID=$(agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001 | jq -r '.result.body.order_id')
agctl kafka assert --topic orders.created --contains "{\"order_id\":\"$OID\"}" --timeout 10
agctl db assert --sql "SELECT 1 FROM orders WHERE id = :order_id AND status = 'PENDING'" --param order_id="$OID" --expect-rows 1

# Keep a session alive during a long test (background it, capture PID, kill when done)
agctl http ping heartbeat --interval 5 --until-stopped &
PID=$!
# … run the scenario …
kill "$PID"
```

Prefer **templates** over free-form. Explore with **`discover`**, never `config show`.
