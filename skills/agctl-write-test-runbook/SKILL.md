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

- **One-line testing request** → produce a **Steps-only** runbook (omit the Fixtures and Cleanup sections entirely). Keep it to the assertions that answer the request.
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
agctl discover --category mock-http-stubs
agctl discover --category mock-kafka-reactors
```

Then `agctl discover --category <X> --name <Y>` for any template you intend to
use, to read its params and example before invoking it.

**Mocks have their own categories** — `mock-http-stubs` and `mock-kafka-reactors`.
To ground a mock fixture, run `agctl discover --category mock-http-stubs` /
`mock-kafka-reactors` (and `--name <Y>` for full detail) — the same way you ground
templates. Do not invent stubs.

### 3. Design

Sequence the steps. For each step decide:

- **Command** — prefer a named template (`agctl http call <name>`,
  `agctl db assert --template <name>`, `agctl kafka assert --pattern <name>`).
  Use free-form (`http request`, `db --sql`) only when no template exists.
- **Capture** *(optional)* — `VAR=<envelope-path>` when a later step needs a
  value from this step's result (e.g. `ORDER_ID=result.body.order_id`). Captured
  values are stringified (a numeric id `42` becomes `"42"`).
- **Expected** — `<envelope-path>: <literal>` pairs (ANDed; compared type-aware,
  like `--equals`), or `exit 0` for an assertion step.

Identify the fixtures and the cleanup that reverses them:

- **Seed data** when the test needs specific DB state (`agctl db execute --write`).
- **Mocks** when a downstream dependency should not be hit for real (`agctl mock run`).
- **Heartbeat** when the SUT enforces a session timeout a long run would trip (`agctl http ping`).

If none apply, omit the Fixtures section entirely. Background commands
(`mock run`, `http ping`) go under Fixtures, never as Steps — they stream NDJSON,
not a single envelope.

**Config placement rule:** Ground every template, mock, seed-template, and pattern via `agctl discover` against the main config. When a needed definition is **not** present, place it in a sidecar `<runbook-base>.agctl.yaml` (sibling to the runbook) rather than editing the main `agctl.yaml`. Shared infrastructure stays in the main config; runbook-specific fixtures (one-off seed templates, ad-hoc mocks, scratch HTTP templates) and per-runbook overrides belong in the sidecar.

### 4. Emit

Instantiate `reference/runbook-template.md`, prune the fixture subsections you
don't need, and fill in goal, source, preconditions, steps, and cleanup. For a
**Steps-only** runbook (one-line request), omit the Fixtures and Cleanup
sections entirely. Write the file as `runbook.md` wherever you prefer (a
`runbooks/` directory at the repo root is common). It is committable — a test
plan; the `*.results.md` report produced at execution is gitignored.

**Sidecar emission:** When any template, mock, seed-template, or pattern definition was placed in a sidecar (per the Design rule), also write `<runbook-base>.agctl.yaml` next to the runbook, and add a `Preconditions` line to the runbook: `Requires overlay: <runbook-base>.agctl.yaml`. The runbook stays pure markdown — no YAML front-matter, no embedded config block. The companion `agctl-run-test-runbook` skill looks for this sibling sidecar and activates the overlay at run-time.

## Reference

- `reference/runbook-template.md` — the adaptive skeleton (the format contract).
- `reference/fixtures-mock.md` — the `agctl mock run` lifecycle and failure-stream protocol.
- `reference/fixtures-heartbeat.md` — the `agctl http ping` background pattern.

## See also

- `agctl-config` — the sibling authoring skill (writes `agctl.yaml`); same authoring-pattern precedent.
- `agctl-run-test-runbook` — executes a runbook and writes the results report.
