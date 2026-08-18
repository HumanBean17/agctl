# agctl — runbook-write: author a test runbook

A **runbook** is a markdown test plan: ordered `agctl` steps, each with the
result it expects, plus fixtures and cleanup. Ground it in the system's real
templates — never invented commands. **Stop after emitting the runbook** —
never execute it; execution is `--topic runbook-run`, a separate
user-initiated action.

Procedure: **Ingest → Discover → Clarify → Design → Emit → Self-review.**

## 1. Ingest

- **One-line testing request** → **Steps-only** runbook (omit Fixtures and
  Cleanup); keep it to the assertions that answer the request.
- **Spec link / design doc** → full structure: Goal + Preconditions +
  Fixtures + Steps + Cleanup.
- One runbook per independent scenario — a multi-flow spec gets one each.
  Record the source in the runbook's `**Source:**` line.

## 2. Discover (ground everything)

`agctl discover` (summary) → per category (`http-templates`,
`kafka-patterns`, `db-templates`, `log-sources`, `services`,
`mock-http-stubs`, `mock-kafka-reactors`) → `--category <X> --name <Y>` for
any template you intend to use (params + example). Do not invent stubs.

## 3. Clarify — only when genuinely ambiguous

One question at a time, multiple-choice where possible: which scenario or
branch (happy-path vs error), what to assert, which downstream to mock vs
let through. Don't interrogate a clear spec.

## 4. Design

Per step: **Command** — prefer named templates (`http call <name>`, `db
assert --template`, `kafka assert --pattern`); free-form only when none
exists. **Capture** *(opt)* — `VAR=<envelope-path>` when a later step needs
a value (stringified: `42` → `"42"`). **Expected** —
`<envelope-path>: <literal>` pairs (ANDed, type-aware) or `exit 0`.

**Log assertions** — `agctl logs assert <source> --match '<jq>'` when the
effect is visible only in the SUT's log (audit lines, retries, background
work). `--match` is entry-rooted (`.level`, `.message`, `.fields.orderId`),
`{placeholder}` via `--param`; **poll** (`--timeout N>0`) for "appears after
the trigger" (logs persist — no capture fixture needed); `--not` asserts
absence. Source must resolve in `discover --category log-sources`.

**Template variables** — prefer inline `{{…}}` tokens over `$(uuidgen)`
shell anti-patterns: `{{uuid}}`, `{{ts}}`/`{{ts:ms}}`/`{{ts:iso}}`,
`{{rand}}`/`{{rand:N}}`. The same token text = one value per step
(`--key {{uuid}}` + `"{{uuid}}"` in the body echo one UUID). Cross-step
sharing: `agctl gen uuid --count 2` (config-free; `result.values[]`) +
Capture. Literal `{{…}}` payload (testing a mustache body)? Add global
`--no-template-vars`.

**Fixtures** (background streamers — never Steps): **seed data**
(`db execute --write`, keep idempotent), **mocks** (`mock run` — see
`--topic mock`), **heartbeat** (`http ping <tpl> --interval 5
--until-stopped &` — only when the SUT enforces a session timeout a long
run would trip), **kafka listen** for long sagas (`--topic listen`; start
BEFORE the trigger, `results` before `stop`). None apply → omit Fixtures.

**Config placement:** ground against the main config; a missing definition
goes in a sidecar `<runbook-base>.agctl.yaml` (sibling file), never edits
to the main config for runbook-only fixtures. Then add a Preconditions
line: `Requires overlay: <runbook-base>.agctl.yaml`.

## 5. Emit

Skeleton (prune unneeded fixture subsections; Steps-only omits Fixtures +
Cleanup):

```markdown
# Runbook: <name>
**Source:** <spec | one-line request>   **Date:** YYYY-MM-DD
## Goal            — 1-2 sentences
## Preconditions   — `agctl check ready --all` → all ready; env assumptions;
                     [Requires overlay: <base>.agctl.yaml]
## Fixtures        — Seed data / Mocks / Heartbeat subsections as needed
## Steps
### 1. <name>
- **Command:** `agctl http call create-order --param customer_id=cust-42`
- **Capture:** `ORDER_ID=result.body.order_id`   *(optional)*
- **Expected:** `ok: true`, `result.status_code: 201`   *(or `exit 0`)*
## Cleanup          — reverse of fixtures (kill heartbeat PID; SIGTERM mock
                      + wait; optional seed reset)
```

Write `runbook.md` (a `runbooks/` dir at repo root is common). It is
committable — a test plan; `*.results.md` is gitignored.

## 6. Self-review

No `<...>`/TBD placeholders; every `$CAPTURE` defined before use; Cleanup
reverses every fixture (one teardown line each); each Expected asserts the
field that answers the Goal; every template/mock/pattern/log-source resolves
in `discover` (main config or sidecar) — nothing invented.
