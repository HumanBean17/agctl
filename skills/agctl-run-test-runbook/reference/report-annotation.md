# Report format — `runbook.results.md`

> The run skill writes the annotated runbook alongside the source: `runbook.md`
> → `runbook.results.md`. Each step gains an **Actual** block beneath its
> **Expected**; a verdict summary sits at the top; Setup/Teardown appear as
> their own blocks. Source: spec §8.

The file is a copy of the runbook with results filled in — expected and actual
sit side by side, so a reviewer reads one file and never has to dig through
agent logs.

## Shape

```
# Runbook results: <name>
**Verdict:** <PASS | FAIL at step N> · <P> passed · <F> failed · <S> skipped (not exercised) · <ISO-8601 Z timestamp>

## Setup
<fixture startups: check ready, mock started + pid, heartbeat started + pid>

### 1. <step name> — <PASS | FAIL | SKIPPED>
- **Run:** <verbatim command as executed>
- **Exit:** <code> · **ok:** <true|false>   [· **error.type:** <type>   on FAIL]
- **Actual:** <curated excerpt>
- **Expected:** <echoed expected>   [✓ | ✗]

## Teardown
<fixture stops + mock log grep result>
```

## Rules

- **Mandatory spine.** Every executed step's Actual block carries the verbatim
  **Run** command, the **Exit** code, and the raw **ok** / **error.type**.
  Curated excerpts are *additional* — never a substitute. An agent can still
  fabricate these; the point is that a reviewer can re-run any **Run** command
  and compare (see "For reviewers" below).
- **PASS / FAIL / SKIPPED.** PASS and FAIL show the spine + Actual + Expected
  (with ✓ / ✗). **SKIPPED** steps echo **Expected** for context but show **no**
  Run / Exit / Actual, and are marked *not exercised* — they were blocked by an
  earlier failure and did not run.
- **Excerpt truncation.** Keep curated excerpts to roughly the first 150
  characters; append `… (truncated)` past ~200 characters, or render the full
  value in a fenced code block when a reviewer needs it whole.
- **Timestamps.** ISO-8601 Z (e.g. `2026-07-04T15:37:00Z`), matching agctl house
  style (`http ping`, `mock run`).
- **Fixtures live in Setup/Teardown**, not the step tally. The verdict counts
  Steps only (pass / fail / skipped). A fixture or teardown failure — e.g. mock
  error events found in the log — flips the overall verdict to FAIL even if every
  Step passed.

## For reviewers — how to verify a report

The spine makes a report **auditable, not trusted**. To verify: re-run any
step's verbatim **Run** command against the same environment and compare its
**Exit** / **ok** / excerpt to what the report claims. Fabrication is
falsifiable, not prevented.

## Worked example

```
# Runbook results: create-order flow
**Verdict:** FAIL at step 2 · 1 passed · 1 failed · 1 skipped (not exercised) · 2026-07-04T15:37:00Z

## Setup
- `check ready --all` → ok
- mock started (pid 4123)
- heartbeat started (pid 4124)

### 1. create order — PASS
- **Run:** `agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001`
- **Exit:** 0 · **ok:** true
- **Actual:** status_code 201, body.order_id=ord-xyz   (captured ORDER_ID=ord-xyz)
- **Expected:** ok:true, result.status_code:201 ✓

### 2. assert ORDER_CREATED — FAIL
- **Run:** `agctl kafka assert --pattern order-created --param orderId=ord-xyz --timeout 10`
- **Exit:** 1 · **ok:** false · **error.type:** AssertionError
- **Actual:** no matching message within 10s (scanned 3)
- **Expected:** exit 0 ✗

### 3. assert DB status — SKIPPED (not exercised — blocked by step 2)
- **Expected:** exit 0   *(not run)*

## Teardown
- heartbeat killed (pid 4124)
- mock SIGTERM+wait (pid 4123, exit 0)
- mock.log grep: no error events
```
