# agctl — gotchas (the full list)

1. **Six streaming commands** — `http ping`, `mock run`, `logs tail`, `grpc
   call` server-stream/bidi, `kafka listen run` — one JSON object **per
   line** + a final `summary`; background with `&`, `kill` when done. The
   managed daemons (`mock start`/`stop`/`status`, `kafka listen
   start`/`stop`/`status`) are **not** streaming — one object each. Every
   other command emits exactly one object.
2. **A 4xx/5xx HTTP response is `ok:true` — unless you assert.** Status is a
   *result*, not an error. Assertion flags flip a wrong response into
   `AssertionError` (exit 1); zero flags leaves the result path unchanged.
3. **`--match` is envelope-rooted.** HTTP → response envelope
   `{status_code, response_time_ms, headers (lowercased), body, url, method}`
   ⇒ `.body.order_id`. Kafka (`assert`/`consume --match`,
   `kafka.patterns[].match`) → message envelope
   `{key, value, partition, offset, timestamp, headers}` ⇒ `.value.eventType`;
   header keys **case-sensitive**. Unchanged: `match.body` (json_subset),
   `--contains`, `--path`, `--jq-path`/`--equals` (still body-rooted),
   `--status`. `--match`/`--jq-path` need `pip install 'agctl[jq]'`. A v1/v2
   `agctl.yaml` → `config migrate` lifts to v3; CLI `--match` flags in
   scripts are **NOT rewritten** — prefix v1-era body-form exprs with
   `.body | ` / `.value | ` by hand.
4. **Three placeholder syntaxes — don't mix:** `${VAR}` env (config load;
   required → exit 2 if unset), `{name}` HTTP path/body & Kafka patterns
   (call time, `--param`), `:name` SQL params (`--param`). Detail:
   `--topic config`.
5. **Kafka reads are windowed, not "latest".** `consume`/`assert` seek to
   `now - --lookback` (default = `--timeout`) and read forward — an event
   published just before you started still matches. `--from-beginning` →
   earliest. Narrow busy topics with `--match`/`--contains`.
6. **`kafka assert` modes are combinable** — several given ⇒ **all** must
   pass. `--pattern` infers topic from config. No match in window ⇒ exit 1
   with `error.detail = {topic, timeout}` (distinct from `ConnectionError`).
7. **`db assert`** takes exactly one mode; `--expect-value` needs **both**
   `--path` and `--equals`. `--equals` is JSON-parsed if valid (`"0"`→0)
   else plain string; compared **strictly** (`0` ≠ `"0"`).
8. **`ConnectionError` is exit 2.** Service/broker/DB unreachable — run
   `check ready --all`, confirm it's up before retrying; don't blame the
   assertion.
9. **No built-in "event did NOT arrive" assert.** `kafka consume
   --expect-count 0` always exits 0 — check absence via `kafka consume
   --topic T --timeout N [--match …]` and inspect `result.count`.
10. **`db execute` needs two gates** — a `writable: true` connection **and**
    `--write` — plus an **explicit target** (`--template` or `--connection`;
    refuses implicit default-connection writes; `mode: read` template →
    exit 2). **No idempotency** — encode `ON CONFLICT` / `ON DUPLICATE KEY
    UPDATE` in SQL.
11. **`db schema` reads `pg_catalog`, cluster-wide, NOT privilege-filtered.**
    Discovering a name is not a grant; let the `SELECT` fail loudly.
12. **Assertion failures self-document their root + payload.** Read
    `error.detail.failures[].root` (HTTP) / `.root` (DB) / `.modes[].root`
    (Kafka). The payload snapshot shows the actual data — correct a mis-rooted
    path (`.data.x` → `.body.data.x`) without dropping the flag.
13. **`mock stop` uses the strict failure rule.** Any of `http.unmatched`,
    `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`,
    `grpc.unmatched`, `grpc.error` ⇒ exit 1 (verdict in `error.detail`);
    `capture.missing` non-fatal but surfaced.
14. **`mock stop --all` returns an array of verdicts** (`result.stopped`);
    any fatal mock ⇒ exit 1 with the array in `error.detail.stopped`.
15. **`mock start` is the readiness gate.** Blocks until the daemon's
    `started` line (or startup error/timeout); the line carries
    `http`/`kafka`/`grpc` blocks (null when not running).
16. **Daemon state under `.agctl/` is the only on-disk state.** Per-run
    pidfiles/logs keyed by engine (`mock-<port>.*`, `mock-kafka.pid`,
    `mock-grpc-<port>.*`, `listen-<run_id>/`). Clean up with
    `rm -rf .agctl`. `kafka listen stop` deletes its run dir on every path —
    `results` not run first ⇒ expectations silently dropped.

## Authoring SQL: `db schema` first

`db schema` is read-only and ungated. Level 1: `db schema [--connection C]
[--schema S]` lists relations (`schema, name, kind, column_count`). Level 2:
`db schema --table T [--schema S]` → columns (`name, data_type, nullable,
default, generated`), `primary_key`, `foreign_keys`, `unique_constraints`.
Exact-case match; 0 hits ⇒ exit 2 ("run Level 1"); >1 across schemas ⇒
candidates in `error.detail` → disambiguate with `--schema`. Rules:

1. **Quote mixed-case / reserved identifiers.** Postgres folds unquoted ids
   to lowercase; `name` is the exact stored case — copy verbatim, quoting as
   needed (`"OrderItems"`).
2. **Omit generated columns from INSERT.** `generated` ∈
   {`always_identity`, `stored`} MUST be omitted (stored from UPDATE too);
   `by_default_identity`/serial may be supplied or omitted.

## Recipes

```bash
# Type-aware equality via jq path (0 ≠ "0"); response asserting in one call
agctl http call create-order --param customer_id=cust-42 \
  --status 201 --match '.body.order_id != null' --contains '{"status":"PENDING"}'
agctl http call get-order --param order_id=ord-789 --jq-path '.status' --equals '"CONFIRMED"'

# E2E: thread an ID through HTTP → Kafka → DB (send-then-assert is reliable)
OID=$(agctl http call create-order --param customer_id=cust-42 | jq -r '.result.body.order_id')
agctl kafka assert --topic orders.created --contains "{\"order_id\":\"$OID\"}" --timeout 10
agctl db assert --sql "SELECT 1 FROM orders WHERE id = :order_id AND status = 'PENDING'" \
  --param order_id="$OID" --expect-rows 1
```
