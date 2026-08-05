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

**Stop after emitting the runbook.** Never invoke `agctl-run-test-runbook` (or
run any step) on your own — execution is a separate, user-initiated action.

## Procedure

### 1. Ingest

Accept either input and scale planning depth to it:

- **One-line testing request** → produce a **Steps-only** runbook (omit the Fixtures and Cleanup sections entirely). Keep it to the assertions that answer the request.
- **Spec link / design doc** → produce the full structure: Goal + Preconditions + Fixtures + Steps + Cleanup.

**Scope:** one runbook per independent scenario. If the spec spans multiple
flows, flag it and author one runbook each — don't merge them into one.

Record the source in the runbook's `**Source:**` line.

### 2. Discover

Run `agctl discover` to ground steps in real templates:

```
agctl discover                                   # summary first
agctl discover --category http-templates
agctl discover --category kafka-patterns
agctl discover --category db-templates
agctl discover --category log-sources
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

### 3. Clarify (only when ambiguous)

If the input already pins down the target scenario and its expected outcomes,
skip this step and go straight to Design. Only when something is genuinely
ambiguous ask the user — one question at a time, multiple-choice where possible.
Scope each question to what would change the runbook: which scenario or branch
(happy-path vs. error), what to assert, or which downstream to mock vs. let
through. Don't interrogate a clear spec.

### 4. Design

Sequence the steps. For each step decide:

- **Command** — prefer a named template (`agctl http call <name>`,
  `agctl db assert --template <name>`, `agctl kafka assert --pattern <name>`).
  Use free-form (`http request`, `db --sql`) only when no template exists. Logs
  are a co-equal assertion channel for effects with no HTTP/Kafka/DB signal:
  `agctl logs assert <source> --match '<jq>'` (see **Log assertions** below).
- **Capture** *(optional)* — `VAR=<envelope-path>` when a later step needs a
  value from this step's result (e.g. `ORDER_ID=result.body.order_id`). Captured
  values are stringified (a numeric id `42` becomes `"42"`).
- **Expected** — `<envelope-path>: <literal>` pairs (ANDed; compared type-aware,
  like `--equals`), or `exit 0` for an assertion step.
- **Log assertions** — reach for `agctl logs assert` when the effect is visible
  only in the SUT's own log (audit lines, retry attempts, background work with no
  HTTP/Kafka/DB signal). `--match` is **entry-rooted** over the canonical entry
  (`.level`, `.message`, `.fields.orderId`), with `{placeholder}` filled via
  `--param` (needs `pip install 'agctl[logs]'`). Use **poll** (`--timeout N>0`)
  for "this line appears after the trigger" — logs already persist to a file, so
  no background-capture fixture is needed (unlike Kafka `listen`). Add `--not` to
  assert absence ("no ERROR logs in the window"). The source must resolve in
  `discover --category log-sources`.

**Template variables (`{{…}}` and `agctl gen`):** agctl expands inline tokens in
any string argument, so prefer these over the `bash -c '…$(uuidgen)…'`
anti-pattern when a step needs a unique value:

| Token | Produces |
|---|---|
| `{{uuid}}` | RFC-4122 v4 UUID (lowercase) |
| `{{ts}}`, `{{ts:ms}}`, `{{ts:iso}}` | Unix seconds / milliseconds / ISO-8601 UTC |
| `{{rand}}`, `{{rand:N}}` | random lowercase hex (default 16 chars; `N` = char count) |

The **same token text resolves to one value within a step** (memoized per
invocation): `{{uuid}}` placed in both `--key` and the message body yields a
single UUID echoed across both fields. (`{{uuid}}` and `{{uuid:2}}` are
different tokens → independent values.) Two patterns cover every case:

- **(A) Inline, single-step, same value in several fields** — drop the token
  straight into each field that needs it:
  ```
  agctl kafka produce --topic t --key {{uuid}} --message '{"conversation_id":"{{uuid}}","case_id":"CASE-{{ts}}"}'
  ```
- **(B) Cross-step via `gen` + Capture** — generate up front, capture, then
  reuse through the existing `$VAR` mechanism:
  ```
  agctl gen uuid --count 2          # config-free; result.values = ["<id>", "<id>"]
  # Capture: CID=result.values[0], MID=result.values[1]
  # later step: agctl kafka produce --key $CID --message "{\"id\":\"$MID\"}"
  ```

`agctl gen uuid|ts|rand` is **config-free** (no `agctl.yaml` needed) and emits
`result.value` (single) or `result.values[]` (with `--count N`); it ignores
`--no-template-vars` since it *is* generation. For a **literal** `{{…}}` payload
(the string must reach the SUT untouched — e.g. testing a mustache/Jinja body),
add the global `--no-template-vars` flag to leave tokens unsubstituted.

Identify the fixtures and the cleanup that reverses them:

- **Seed data** when the test needs specific DB state (`agctl db execute --write`).
- **Mocks** when a downstream dependency should not be hit for real (`agctl mock run`).
- **Heartbeat** when the SUT enforces a session timeout a long run would trip (`agctl http ping`).
- **Long-lived Kafka capture** when verifying events on a long saga / high-volume topic where a windowed `kafka assert` could miss (`agctl kafka listen start` BEFORE the trigger → `assert` → `results` → `stop`). See the `agctl` skill for the lifecycle; `stop` does NOT auto-run `results`.

If none apply, omit the Fixtures section entirely. Background commands
(`mock run`, `http ping`, `kafka listen run`) go under Fixtures, never as Steps —
they stream NDJSON, not a single envelope.

**Config placement rule:** Ground every template, mock, seed-template, pattern, and log-source via `agctl discover` against the main config. When a needed definition is **not** present, place it in a sidecar `<runbook-base>.agctl.yaml` (sibling to the runbook) rather than editing the main `agctl.yaml`. Shared infrastructure stays in the main config; runbook-specific fixtures (one-off seed templates, ad-hoc mocks, scratch HTTP templates, scratch log sources) and per-runbook overrides belong in the sidecar.

### 5. Emit

Instantiate `reference/runbook-template.md`, prune the fixture subsections you
don't need, and fill in goal, source, preconditions, steps, and cleanup. For a
**Steps-only** runbook (one-line request), omit the Fixtures and Cleanup
sections entirely. Write the file as `runbook.md` wherever you prefer (a
`runbooks/` directory at the repo root is common). It is committable — a test
plan; the `*.results.md` report produced at execution is gitignored.

**Sidecar emission:** When any template, mock, seed-template, pattern, or log-source definition was placed in a sidecar (per the Design rule), also write `<runbook-base>.agctl.yaml` next to the runbook, and add a `Preconditions` line to the runbook: `Requires overlay: <runbook-base>.agctl.yaml`. The runbook stays pure markdown — no YAML front-matter, no embedded config block. The companion `agctl-run-test-runbook` skill looks for this sibling sidecar and activates the overlay at run-time.

### 6. Self-review

Before declaring the runbook done, scan it and fix inline:

- No leftover `<...>` / TBD placeholders.
- Every `$CAPTURE` is defined by an earlier step before any later step uses it.
- Cleanup reverses every fixture — one teardown line per fixture (mock PID,
  heartbeat PID, seed reset).
- Each `Expected` asserts the field that actually answers the Goal.
- Every template, mock, seed-template, pattern, and log-source resolves in
  `agctl discover` against the main config or the sidecar — nothing invented.

## Reference

- `reference/runbook-template.md` — the adaptive skeleton (the format contract).
- `reference/fixtures-mock.md` — the `agctl mock run` lifecycle and failure-stream protocol.
- `reference/fixtures-heartbeat.md` — the `agctl http ping` background pattern.

## See also

- `agctl-config` — the sibling authoring skill (writes `agctl.yaml`); same authoring-pattern precedent.
- `agctl-run-test-runbook` — executes a runbook and writes the results report.
