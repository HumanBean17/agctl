# Design: Test Runbook Skills — `agctl-write-test-runbook` + `agctl-run-test-runbook`

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-04
**Author:** brainstorming session
**Affects:** new `skills/agctl-write-test-runbook/`, new `skills/agctl-run-test-runbook/` (portable consumer skills)
**Relation to docs:** No change to the `agctl` CLI contract. `DESIGN.md` / `ARCHITECTURE.md` are expected no-ops (these are consumer skill artifacts, not CLI code/config); the `skills/` tree grows. Confirmed by `docs-watcher` after implementation per CLAUDE.md "Docs Sync".

---

## 1. Background & Problem

The contributor's established workflow with `agctl` is:

1. Generate a plan/spec and implement it.
2. Hand the plan to an agent that has `agctl`, and ask for a testing plan.
3. The agent produces a testing runbook (a sequence of `agctl` invocations with assertions) and works through it.

Today this is done ad hoc: every session reinvents the runbook's structure, the fixture/cleanup discipline, and — critically — the way test *results* are reported. The last gap is the load-bearing one. An agent that "ran the tests" can summarize "all passed" in prose, leaving the human to trust the agent or to dig through agent logs guessing whether each step was actually run. There is no structured, reviewable artifact that ties each `agctl` command to its verbatim execution and raw result.

This spec formalizes the workflow as **two portable skills** that ship in `skills/` alongside `agctl` and `agctl-config`. It deliberately adds **no CLI code** — `agctl`'s primitives already cover everything a runbook needs (`http`, `kafka`, `db`, `check`, `mock`, `discover`); the gap is procedural discipline and an auditable report, not missing commands.

> **Scope honesty (load-bearing).** A skill is a procedure an agent *follows*, not a runtime that *enforces*. These skills cannot force an agent to be honest — an agent can still fabricate an exit code. What they do is (a) make review trivial (a structured, persisted report), (b) raise the cost of fabrication (every step carries the verbatim command + exit code + raw `ok`/`error.type`), and (c) make omission visible (a missing step is a red flag). This is "verify, don't trust," not trusted execution. Stated explicitly so a user is never misled into believing the report is tamper-proof.

## 2. Goals

- Formalize the spec-or-request → runbook → execute workflow into a repeatable procedure, so it is not reinvented each session.
- Produce a **runbook** artifact (the plan) grounded in the consuming repo's real `agctl` templates/patterns via `discover`, not invented commands.
- Produce an **evidence report** (annotated runbook) that a human can review without reading agent logs, with an un-fakeable spine per step (command + exit code + raw `ok`).
- Stay consistent with `agctl`'s principles: composable primitives, fail loudly, system-agnostic, narrow CLI surface (no new commands).
- Match the existing portable-skill pattern (`agctl`, `agctl-config`): shipped in `skills/`, consumed by copying into a user's `.claude/skills/`, **not** wired into `agctl`'s runtime.

## 3. Scope & Design Constraints

- **Two skills, split by deliverable.** `agctl-write-test-runbook` produces a runbook (a plan artifact). `agctl-run-test-runbook` produces an evidence report. They have different trust contracts and different success criteria, so they are separate skills.
- **Portable, not wired.** Like `agctl-config`, these live in `skills/` for users to copy; `agctl`'s runtime is unchanged.
- **Agent-executed, no CLI runner.** Execution is driven by the agent following the run skill. A declarative CLI `agctl run` is explicitly a deferred non-goal (§4, §9) — it is redundant while the agent is in the loop.
- **Markdown runbook format.** Agent-native, human-readable, diff-able, committable. A YAML runbook is rejected (§9) — it only earns its keep if a non-agent runtime consumes it, which is out of scope.
- **Input-adaptive.** The write skill accepts a rich spec *or* a one-line testing request; planning depth scales to input.
- **`agctl`-native.** Steps are grounded in `agctl` primitives via `discover`. Free-form (`http request`, `db --sql`) is the escape hatch only when no template exists — mirroring `agctl`'s own philosophy and AGENTS.md.
- **Layers on the operational `agctl` skill.** Both skills assume the agent can already drive `agctl` primitives (call commands, interpret the JSON envelope, read `ok`/exit codes). They add orchestration + reporting discipline, not primitive knowledge.

## 4. Non-Goals

- **A CLI scenario executor** (`agctl run <runbook>`). Deferred (DESIGN §10 "multi-step scenario chaining"); redundant while the agent drives execution (§9).
- **A parallel test framework** (a pytest replacement). The runbook is a *procedure an agent follows*, not a *program a runtime executes*.
- **Trusted execution / tamper-proofing.** The report is auditable, not enforced (§1 scope honesty).
- **Runbooks that orchestrate non-`agctl` steps.** The skills are `agctl`-native; they do not aim to drive arbitrary shell. (A step may still be a free-form `agctl` command.)
- **Automatic runbook generation without agent judgment.** `agctl` itself does not inspect a codebase and write tests (DESIGN §1 non-goal). The write skill is an agent procedure that uses judgment, not a generator.
- **A runbook registry / persistence backend.** Runbooks are files; where they live is a convention, not a system.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Skill, not CLI code.** Formalize as portable skills in `skills/`; add no `agctl` command. | `agctl`'s primitives already cover runbook needs; the gap is procedural discipline + an auditable report. A skill is the right layer for an agent-reasoning procedure; a CLI cannot "read a spec and design a runbook." Honors DESIGN §1 non-goals (no auto-generating tests; no first-class scenario orchestration). |
| D2 | **Split into two skills by deliverable.** `agctl-write-test-runbook` (→ runbook) and `agctl-run-test-runbook` (→ evidence report). | Different deliverables, different trust contracts. The run skill's entire reason to exist is the auditable report; making it a headline (not a buried section) is what delivers the "verify don't trust" value. Mirrors the global `writing-plans` / `executing-plans` precedent, but for a stronger, domain-specific reason. |
| D3 | **Names: `agctl-write-test-runbook` / `agctl-run-test-runbook`.** Keep the `agctl-` family prefix; use the verb `write` (not `build`). | `agctl-config` is already an authoring/procedure skill and kept the `agctl-` prefix — these are its cousins. `agent-` was rejected (§9) as it breaks family consistency and obscures the hard `agctl` coupling. `write` > `build` (unambiguous; parallels `writing-plans`). |
| D4 | **Built-in adaptive template.** One `reference/runbook-template.md` with the full structure; unused fixture sections are pruned. | Consistency *is* the value of formalizing; without a concrete template the skill is prose and agents drift. Direct precedent: `skills/agctl-config/reference/` ships snippet templates. One adaptive template beats two (no sync burden) or a section library (over-engineered for a linear runbook). |
| D5 | **Annotated runbook report.** The run skill writes `runbook.md` → `runbook.results.md`: each step gains an **Actual** block beneath its **Expected**, plus a verdict summary. | Expected-vs-actual inline in one persisted, diff-able file is the most direct fix for "reviewing logs and guessing." Avoids the cross-referencing of a standalone report and the ephemerality of an in-chat report (both rejected, §9). |
| D6 | **Un-fakeable spine per step.** Every step records the verbatim command, exit code, and raw `ok` / `error.type`. Curated excerpts (HTTP body, DB rows, matched Kafka message) sit alongside, never instead of. | The spine is what makes fabrication costlier and review trivial; curated excerpts alone could hide a failure. |
| D7 | **Stop on first failure; mark the rest "skipped (blocked by step N)."** | Avoids cascade noise: a failed `create-order` would make the downstream `kafka assert` falsely "fail" too, eroding trust in the report — the one thing the run skill exists to build. Matches `agctl`'s AGENTS.md "stop and diagnose" rule. Continuing is rejected (§9). |
| D8 | **Discovery-grounded steps.** Write runs `agctl discover` and prefers named templates/patterns; free-form is the escape hatch only when no template exists. | Mirrors `agctl`'s own philosophy and AGENTS.md. Grounds runbooks in the consuming repo's real surface; avoids invented commands that drift from config. |
| D9 | **Pass/fail semantics.** Assertion steps (`db/kafka assert`, `check ready`) pass on exit 0. Non-assertion steps (`http call`, `db query/execute`) have no built-in verdict — the runbook's **Expected** block declares what counts; **all** declared Expected fields must match. | `agctl` treats HTTP status as a result, not an assertion; the runbook must declare expected outcomes for non-assertion steps so the run skill can compare. |
| D10 | **Write owns the format; run references it.** The runbook template + format live in the write skill's `reference/`; the run skill consumes and references it. | One definition of "runbook," not two. The run skill owns the *annotation* contract (`report-annotation.md`). |
| D11 | **File locations: runbooks committable, results gitignored.** Runbooks are test plans (committable like code); `*.results.md` are ephemeral per-run (`.gitignore`). The skills suggest a `runbooks/` dir but enforce no path. | Runbooks are durable artifacts worth versioning; results are transient outputs that would create noise if committed. |

## 6. Skill Structures

### 6.1 `skills/agctl-write-test-runbook/`

```
skills/agctl-write-test-runbook/
├── SKILL.md                       # authoring procedure
└── reference/
    ├── runbook-template.md        # the adaptive runbook skeleton (central format, §7)
    ├── fixtures-mock.md           # mock lifecycle protocol (tricky → own page)
    └── fixtures-heartbeat.md      # http ping background pattern
```

**SKILL.md procedure (design-level):**

1. **Ingest** — accept a spec (link/doc) *or* a one-line testing request. Scale planning depth to input: a one-line request yields a minimal runbook (steps + assertions); a spec yields the full structure with fixtures/cleanup.
2. **Discover** — run `agctl discover` (summary → category → item) to identify the real templates, patterns, and connections relevant to the goal.
3. **Design** — sequence steps grounded in discovered templates; declare each step's **Expected** (D9); identify fixtures (seed data, mocks, heartbeat) and cleanup.
4. **Emit** — instantiate `reference/runbook-template.md`, pruning unused fixture sections; fill in goal, source, preconditions, steps, cleanup.

Seed-data fixtures (`db execute`, idempotent via `ON CONFLICT`) are simple enough to cover inline in the template. Mocks and heartbeat have failure-prone lifecycle protocols and get dedicated reference pages (the `agctl-config` precedent of per-topic reference docs).

### 6.2 `skills/agctl-run-test-runbook/`

```
skills/agctl-run-test-runbook/
├── SKILL.md                       # execution + report procedure
└── reference/
    └── report-annotation.md       # the Actual-block + verdict format contract (§8)
```

**SKILL.md procedure (design-level):**

1. **Load** the target `runbook.md`.
2. **Execute** steps in order. For each step: run the verbatim command; capture the exit code and the raw `ok` / `error.type` (plus a curated excerpt) from the envelope.
3. **Annotate** an **Actual** block beneath each step's **Expected** (D5, D6).
4. **On failure** (exit ≠ 0 for assertion steps, or any declared Expected field ≠ actual for non-assertion steps): stop; mark all remaining steps *skipped (blocked by step N)* (D7).
5. **Emit** the verdict summary at the top; write `runbook.results.md` alongside the runbook.

## 7. Runbook Format Contract (`reference/runbook-template.md`)

The adaptive skeleton. Sections marked *(optional)* are pruned when unused.

````markdown
# Runbook: <name>
**Source:** <spec link | "one-line request: …">   **Date:** YYYY-MM-DD

## Goal
<1-2 sentences: what this verifies>

## Preconditions
- `agctl check ready --all` → all services ready
- <env assumptions, e.g. DB_WRITE_USER set>

## Fixtures  *(include the subsections you need; drop the rest)*
### Seed data *(optional)*
- `agctl db execute --template seed-… --write`   (idempotent via ON CONFLICT)
### Mocks *(optional — see fixtures-mock.md)*
- `agctl mock run > mock.log 2>&1 &`   → poll mock.log for "started" before continuing
### Heartbeat *(optional — see fixtures-heartbeat.md)*
- `agctl http ping heartbeat --interval 5 --until-stopped &`

## Steps
### 1. <step name>
- **Command:** `agctl http call create-order --param customer_id=cust-42 …`
- **Expected:** `ok: true`, `result.status_code: 201`

### 2. <step name>
- **Command:** `agctl kafka assert --pattern order-created --param orderId=$ORDER_ID --timeout 10`
- **Expected:** exit 0

### 3. <step name>
- **Command:** `agctl db assert --template find-order --param orderId=$ORDER_ID --expect-value --path .status --equals PENDING`
- **Expected:** exit 0

## Cleanup  *(reverse of fixtures)*
- kill heartbeat PID; SIGTERM mock PID + wait; (optional) reset seed data
````

**Step semantics:** each step is a numbered block with a **Command** (the verbatim `agctl` invocation, possibly referencing a shell variable captured from an earlier step's result, e.g. `$ORDER_ID`) and an **Expected** (the pass criterion per D9). A step may also *export* a value for later steps (e.g. `ORDER_ID` from a `create-order` response body) — the run skill captures these during execution.

## 8. Report Format Contract (`reference/report-annotation.md`)

The annotated runbook (`runbook.results.md`). Each step gains an **Actual** block; a verdict summary sits at the top.

````markdown
# Runbook results: <name>
**Verdict:** FAIL at step 2 · 1 passed · 1 failed · 1 skipped (blocked by step 2) · <timestamp>

### 1. <step name> — PASS
- **Run:** `agctl http call create-order --param customer_id=cust-42 …`
- **Exit:** 0 · **ok:** true
- **Actual:** status_code 201, body.order_id=ord-xyz
- **Expected:** ok:true, result.status_code:201 ✓

### 2. <step name> — FAIL
- **Run:** `agctl kafka assert --pattern order-created --param orderId=ord-xyz --timeout 10`
- **Exit:** 1 · **ok:** false · **error.type:** AssertionError
- **Actual:** no matching message within 10s (scanned 3)
- **Expected:** exit 0 ✗

### 3. <step name> — SKIPPED (blocked by step 2)
````

**Mandatory spine (D6):** every executed step's Actual block carries the verbatim **Run** command, **Exit** code, and raw **ok** / **error.type**. Curated excerpts (status/body/rows/matched message) are *additional*, never a substitute. A **PASS** / **FAIL** / **SKIPPED** marker per step rolls up into the verdict line.

## 9. Rejected Alternatives (ADR-style)

- **A CLI `agctl run <runbook>` executor.** Rejected (D1): reverses DESIGN §1's scenario-orchestration non-goal; forces a YAML runbook (killing the markdown format); duplicates the in-context aggregation the agent already does; grows the command surface against `agctl`'s "ten focused commands" rule (DESIGN §8). The agent is in the loop in the stated workflow, making a runner redundant. Deferred until an agentless-CI need is concrete.
- **One skill with build + execute phases.** Rejected (D2): buries the evidence report as a section, undercutting the "verify don't trust" emphasis that justifies the skill.
- **`agent-` prefix.** Rejected (D3): breaks the `agctl-*` family consistency (the skills are hard-coupled to `agctl`); `agctl-config` already proves procedure skills keep the `agctl-` prefix.
- **YAML runbook format.** Rejected: only earns its keep if a non-agent runtime consumes it (out of scope); markdown is agent-native, human-readable, diff-able.
- **Two templates (full + minimal) or a section library.** Rejected (D4): two templates create a sync burden and an upfront "which one" choice; a section library is over-engineered for a linear runbook. One adaptive template with pruning covers the input spectrum.
- **Standalone report file.** Rejected (D5): forces the reviewer to cross-reference runbook + report to compare expected vs actual; annotating the runbook keeps them inline.
- **In-chat-only report.** Rejected (D5): not persisted, diff-able, or reviewable after the fact — undercuts the "stop guessing from logs" goal.
- **Continue-and-record-all on failure.** Rejected (D7): cascade noise erodes the report's credibility.

## 10. Validation Strategy

These are markdown procedures, not code, so validation is end-to-end behavior + contract checks:

- **Dry-run (write skill):** run the write skill against the repo's own example `agctl.yaml` (the order/payment system in DESIGN §2.1) targeting a small goal (e.g. "verify create-order → ORDER_CREATED → PENDING row"); confirm it produces a well-formed `runbook.md` that uses real discovered templates and prunes unused fixture sections.
- **Dry-run (run skill):** execute that runbook; confirm `runbook.results.md` is well-formed, every executed step carries the D6 spine, and a deliberately-broken step produces the correct FAIL + skipped-cascade + verdict.
- **Command cross-check:** confirm every `agctl` command referenced by both skills (`http call/request/ping`, `kafka produce/consume/assert`, `db query/assert/execute`, `check ready`, `mock run`, `discover`) exists in `agctl` today (DESIGN §3), so the skills never reference a non-existent surface.

After implementation, `docs-watcher` runs (CLAUDE.md "Docs Sync") — expected no-op for DESIGN.md/ARCHITECTURE.md (CLI contract unchanged), confirming the `skills/` tree grew correctly.

## 11. Docs & Skill Impact

- **`skills/` tree** — two new portable skill directories (`agctl-write-test-runbook/`, `agctl-run-test-runbook/`), consumed by copying into a user's `.claude/skills/`. Not wired into `agctl`'s runtime.
- **`DESIGN.md` / `ARCHITECTURE.md`** — expected no-op; these docs describe the `agctl` CLI, whose contract is unchanged. `docs-watcher` confirms.
- **No code, config, packaging, or entry-point changes.** No `version` bump. No new `agctl` command.
