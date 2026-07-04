# Runbook: <name>

**Source:** <spec link | "one-line request: …">   **Date:** YYYY-MM-DD

> An agctl test runbook. Author it with `agctl-write-test-runbook`; execute it
> with `agctl-run-test-runbook`, which annotates each step with its verbatim
> command + exit code + raw result and writes `runbook.results.md`.
>
> **Fields:**
> - **Command** — the verbatim `agctl` invocation. May reference `$VAR` captured by an earlier step.
> - **Capture** *(optional)* — `VAR=<envelope-path>` (dotted path into the result envelope). Runbook-scoped, string-default; substituted as `$VAR` in later Commands.
> - **Expected** — a list of `<envelope-path>: <literal>` pairs (ANDed; compared type-aware, like agctl `--equals`). Assertion steps use the shorthand `Expected: exit 0`.

## Goal

<1-2 sentences: what this runbook verifies.>

## Preconditions

- `agctl check ready --all` → all services ready
- <env assumptions, e.g. `DB_WRITE_USER` set>

## Fixtures

*Include the subsections you need; drop the rest. Background commands
(`mock run`, `http ping`) live here, not under Steps — they stream NDJSON
rather than returning a single result envelope.*

### Seed data

- `agctl db execute --template seed-… --write`   (the template has `mode: write`; keep writes idempotent via `ON CONFLICT`)

### Mocks

- `agctl mock run > mock.log 2>&1 &`   → poll `mock.log` for `started` before continuing. See `fixtures-mock.md`.

### Heartbeat

- `agctl http ping heartbeat --interval 5 --until-stopped &`. See `fixtures-heartbeat.md`.

## Steps

### 1. <step name>

- **Command:** `agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001`
- **Capture:** `ORDER_ID=result.body.order_id`   *(optional; runbook-scoped; available as `$ORDER_ID` in later steps)*
- **Expected:** `ok: true`, `result.status_code: 201`

### 2. <step name>

- **Command:** `agctl kafka assert --pattern order-created --param orderId=$ORDER_ID --timeout 10`
- **Expected:** exit 0

### 3. <step name>

- **Command:** `agctl db assert --template find-order --param orderId=$ORDER_ID --expect-value --path .status --equals PENDING`
- **Expected:** exit 0

## Cleanup

*Reverse of fixtures.*

- kill heartbeat PID
- SIGTERM mock PID + `wait`
- (optional) reset seed data
