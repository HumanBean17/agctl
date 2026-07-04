---
name: agctl-run-test-runbook
description: Execute an agctl test runbook step-by-step and produce an annotated, auditable results report (runbook.results.md) with the verbatim command, exit code, and raw result per step. Invoke when asked to run or execute a test runbook authored with agctl-write-test-runbook.
---

# Running an agctl test runbook

Execute a runbook and produce an evidence report a human can review without
reading agent logs. For each step, the report carries the **verbatim command**,
its **exit code**, and the raw **ok** / **error.type** from the envelope — an
auditable spine, not a prose "all passed."

This skill is self-contained: you do **not** need `agctl-write-test-runbook`
installed to run a runbook.

## Runbook anatomy (what you parse)

A runbook is markdown with these sections, in order:

- **Goal** — what the runbook verifies (informational).
- **Preconditions** — a checklist run in Setup (typically `agctl check ready --all` plus env assumptions).
- **Fixtures** — background setup, each optional: **Seed data** (`agctl db execute --write`), **Mocks** (`agctl mock run`), **Heartbeat** (`agctl http ping`). Started in Setup, stopped in Teardown.
- **Steps** — the ordered assertions/actions. Each step has:
  - **Command** — the verbatim `agctl` invocation (may use `$VAR` from a prior Capture).
  - **Capture** *(optional)* — `VAR=<envelope-path>`; stored for `$VAR` substitution in later steps.
  - **Expected** — `<envelope-path>: <literal>` pairs (ANDed; compared type-aware), or `exit 0` for an assertion step.
- **Cleanup** — the reverse of fixtures; run in Teardown.

Background commands (`mock run`, `http ping`) live under **Fixtures**, never
**Steps** — they stream NDJSON, not a single envelope.

## Procedure

### 1. Validate (pre-execution)

Before running anything, check the runbook:

- Every step has a **Command** and a non-empty **Expected**.
- Every `$VAR` used in a Command resolves to a prior **Capture**.
- Assertion steps use `Expected: exit 0`.

On any violation: stop with a validation error. Do not execute — no partial runs.

### 2. Setup

- Run **Preconditions**: `agctl check ready --all` (and any env checks).
- Start **Fixtures**, capturing PIDs:
  - Seed data: `agctl db execute --template … --write`.
  - Mock: `agctl mock run > mock.log 2>&1 &`, then **poll `mock.log` for `started`** before continuing.
  - Heartbeat: `agctl http ping … --until-stopped &`.

Record each in the report's `## Setup` block.

### 3. Execute

For each step, in order:

- Substitute `$VAR` from prior Captures into the Command.
- Run the verbatim command.
- From the JSON envelope, capture the exit code, `ok`, `error.type` (if any), and a curated excerpt (e.g. `result.status_code`, a DB row, the matched Kafka message).

### 4. Annotate

Write the step's **Actual** block beneath its **Expected**, per
`reference/report-annotation.md`: Run / Exit / ok / error.type / Actual /
Expected (✓ or ✗).

### 5. On failure

A step fails when exit≠0, `ok:false`, or any declared Expected field mismatches.
On the first failure: **stop**, and mark every remaining step
**SKIPPED (not exercised — blocked by step N)**. Do not continue — a downstream
step that depends on a failed one produces cascade noise, not signal.

### 6. Teardown (always — even on failure)

- Kill heartbeat PIDs.
- `SIGTERM` the mock PID, then `wait`.
- **Grep `mock.log` for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error`** — any hit is a failure that flips the overall verdict to FAIL.
- Optional: reset seed data.

Record each in the report's `## Teardown` block.

### 7. Emit

Write the verdict line at the top and save `runbook.results.md` next to the
runbook, per `reference/report-annotation.md`.

## Verdict tally

Counts **Steps** only: passed / failed / skipped. Fixtures appear in the
Setup/Teardown blocks, not the tally. A fixture or teardown failure (e.g. mock
error events in the log) flips the overall verdict to FAIL even if every Step
passed.

## Reference

- `reference/report-annotation.md` — the annotated-report format contract (spine, SKIPPED rendering, truncation, worked example).
- `agctl-write-test-runbook` — the companion skill that authors runbooks.
