# agctl — runbook-run: execute a runbook, write the evidence report

Execute a runbook (see `--topic runbook-write` for the format) step-by-step
and produce `runbook.results.md` — evidence a human can review without
reading agent logs: per step the **verbatim command**, **exit code**, raw
**ok** / **error.type**, and a curated excerpt.

## 1. Validate (pre-execution — no partial runs)

- Every step has a Command and non-empty Expected; assertion steps use
  `Expected: exit 0`.
- Every `$VAR` resolves to a prior Capture.
- Every `{{…}}` token is a known generator (`{{uuid}}`, `{{ts}}[:ms|:iso]`,
  `{{rand}}[:N]`) — unknown tokens are validation errors.
- **Sidecar:** a sibling `<runbook-base>.agctl.yaml` next to the runbook →
  `agctl config validate --overlay <sidecar>` first; ok → active for the
  whole run (surface `overridden by overlay` warnings into the report);
  failure → validation error, do not execute.

On any violation: stop, no execution.

## 2. Setup

1. Start fixtures **a `check ready` precondition will hit** first (a mock
   the readiness check probes), else `check ready` sees it down.
2. Run Preconditions: `agctl check ready --all` (+ env checks). Non-zero →
   overall **FAIL**, go straight to Teardown.
3. Start remaining fixtures, capturing PIDs: seed `db execute --write`;
   mock `agctl mock run > mock.log 2>&1 &` then poll for `started`;
   heartbeat `agctl http ping … --until-stopped &`.

**Overlay injection:** sidecar active → prefix EVERY invocation
`agctl --overlay <sidecar> <group> <cmd> …` (setup, fixtures, steps).

## 3. Execute → 4. Annotate

Per step, in order: substitute `$VAR`s from prior Captures; run the
verbatim command (with `--overlay` when active); record exit code, `ok`,
`error.type`, curated excerpt (status code + key body field / matched Kafka
message / DB row / matched log entry). Write the **Actual** block beneath
the step's Expected: Run / Exit / ok / error.type / Actual / Expected ✓|✗.

## 5. On failure

A step fails on exit≠0, `ok:false`, or any Expected mismatch. First failure
→ **stop**; mark every remaining step **SKIPPED (not exercised — blocked by
step N)**. No cascades.

## 6. Teardown (always — even on failure)

Kill heartbeat PIDs; `SIGTERM` the mock PID + `wait`; **grep `mock.log` for
`http.unmatched | http.body_parse_skipped | kafka.skipped | kafka.error |
grpc.unmatched | grpc.error | capture.missing`** — any hit flips the overall
verdict to FAIL (assertions passing doesn't excuse it); optional seed reset.

## 7. Emit — `runbook.results.md` (next to the runbook)

```
# Runbook results: <name>
**Verdict:** <PASS | FAIL at step N> · P passed · F failed · S skipped (not exercised) · <ISO-8601 Z>

## Setup
<check ready result; mock/heartbeat PIDs>
### 1. <step> — <PASS | FAIL | SKIPPED>
- **Run:** <verbatim command>
- **Exit:** N · **ok:** true|false   [· **error.type:** T on FAIL]
- **Actual:** <curated excerpt>
- **Expected:** <echoed>   ✓ | ✗
## Teardown
<fixture stops + mock.log grep result>
```

Rules: the Run/Exit/ok spine is mandatory — excerpts never substitute for
it (a reviewer can re-run any Run command and compare; the report is
auditable, not trusted). SKIPPED steps echo Expected but show no Run/Exit.
Excerpts ≈ first 150 chars (`… (truncated)` past ~200; fenced block when a
reviewer needs it whole). ISO-8601 Z timestamps.

**Verdict tally counts Steps only** (passed/failed/skipped); fixtures live
in Setup/Teardown — but a fixture or teardown failure (mock error events in
the log) flips the overall verdict to FAIL even with every Step passing.
