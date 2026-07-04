---
name: agctl-write-test-runbook
description: Author an agctl test runbook from a spec or a one-line testing request — an ordered sequence of agctl commands with Expected outcomes, Capture variables, fixtures (seed data / mocks / heartbeat), and cleanup. Invoke when asked to plan or write a test runbook for an agctl-driven system.
---

# Writing an agctl test runbook

A **runbook** is a markdown test plan: ordered `agctl` steps, each with the
result it expects, plus the fixtures and cleanup around them. The companion
`agctl-run-test-runbook` skill executes a runbook and produces an auditable
results report.

This skill writes runbooks; it does not run them. Point at a spec (or a one-line
testing request) plus the repo's `agctl.yaml`, and produce a well-formed
`runbook.md` grounded in the system's real templates — not invented commands.

## Procedure

### 1. Ingest

Accept either input and scale planning depth to it:

- **One-line testing request** → produce a **Steps-only** runbook (no Fixtures/Cleanup sections). Keep it to the assertions that answer the request.
- **Spec link / design doc** → produce the full structure: Goal + Preconditions + Fixtures + Steps + Cleanup.

Record the source in the runbook's `**Source:**` line.

### 2. Discover

Run `agctl discover` to ground steps in real templates:

```
agctl discover                                   # summary first
agctl discover --category http-templates
agctl discover --category kafka-patterns
agctl discover --category db-templates
agctl discover --category services
```

Then `agctl discover --category <X> --name <Y>` for any template you intend to
use, to read its params and example before invoking it.

**`discover` has no `mocks` category** (DESIGN §3.7 lists only `services`,
`http-templates`, `kafka-patterns`, `db-templates`). To ground a mock fixture,
read the `mocks:` section of `agctl.yaml` directly — the same way the
`agctl-config` skill reads config. Do not invent stubs.

### 3. Design

Sequence the steps. For each step decide:

- **Command** — prefer a named template (`agctl http call <name>`,
  `agctl db assert --template <name>`, `agctl kafka assert --pattern <name>`).
  Use free-form (`http request`, `db --sql`) only when no template exists.
- **Capture** *(optional)* — `VAR=<envelope-path>` when a later step needs a
  value from this step's result (e.g. `ORDER_ID=result.body.order_id`).
- **Expected** — `<envelope-path>: <literal>` pairs (ANDed; compared type-aware,
  like `--equals`), or `exit 0` for an assertion step.

Identify the fixtures (seed data, mocks, heartbeat) and the cleanup that
reverses them. Background commands (`mock run`, `http ping`) go under Fixtures,
never as Steps — they stream NDJSON, not a single envelope.

### 4. Emit

Instantiate `reference/runbook-template.md`, prune the fixture sections you
don't need, and fill in goal, source, preconditions, steps, and cleanup. Write
the file as `runbook.md` (committable — it is a test plan; the `*.results.md`
report produced at execution is gitignored).

## Reference

- `reference/runbook-template.md` — the adaptive skeleton (the format contract).
- `reference/fixtures-mock.md` — the `agctl mock run` lifecycle and failure-stream protocol.
- `reference/fixtures-heartbeat.md` — the `agctl http ping` background pattern.

## See also

- `agctl-config` — the sibling authoring skill (writes `agctl.yaml`); same authoring-pattern precedent.
- `agctl-run-test-runbook` — executes a runbook and writes the results report.
