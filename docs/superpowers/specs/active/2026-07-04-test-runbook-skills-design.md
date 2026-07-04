# Design: Test Runbook Skills — `agctl-write-test-runbook` + `agctl-run-test-runbook`

**Status:** Approved (design, revised) — ready for implementation plan
**Date:** 2026-07-04
**Revised:** 2026-07-04 — applied fan-out spec-review findings: variable-capture syntax (D12); background commands as fixture-lifecycle, not verdict-bearing Steps (D13); run-procedure Setup/Teardown phases + pre-execution runbook validation (§6.2); Expected-block grammar + type-aware coercion + "exit≠0/ok:false is always FAIL" (D9); D10 split into authoring-template (write) vs self-contained parse-anatomy (run) for independent portability; honest D2/D6 framing; input-adaptive specificity (§6.1); validation retargeted at the repo's own mock server (§10); `discover` has no `mocks` category — mocks read from config directly (§6.1); SKIPPED rendering, excerpt-truncation rule, ISO-8601 verdict timestamp (§8); softened `*.results.md` gitignore (D11).
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

> **Scope honesty (load-bearing).** A skill is a procedure an agent *follows*, not a runtime that *enforces*. These skills cannot force an agent to be honest — an agent can still fabricate an exit code. What they do is (a) make review trivial (a structured, persisted report), (b) make the report *auditable* — a reviewer can re-run any step's verbatim command and compare, and (c) make omission visible (a missing step is a red flag). This is "verify, don't trust," not trusted execution. Stated explicitly so a user is never misled into believing the report is tamper-proof.

## 2. Goals

- Formalize the spec-or-request → runbook → execute workflow into a repeatable procedure, so it is not reinvented each session.
- Produce a **runbook** artifact (the plan) grounded in the consuming repo's real `agctl` templates/patterns via `discover`, not invented commands.
- Produce an **evidence report** (annotated runbook) that a human can review without reading agent logs, with an auditable spine per step (command + exit code + raw `ok`).
- Stay consistent with `agctl`'s principles: composable primitives, fail loudly, system-agnostic, narrow CLI surface (no new commands).
- Match the existing portable-skill pattern (`agctl`, `agctl-config`): shipped in `skills/`, consumed by copying into a user's `.claude/skills/`, **not** wired into `agctl`'s runtime.

## 3. Scope & Design Constraints

- **Two skills, split by deliverable.** `agctl-write-test-runbook` produces a runbook (a plan artifact). `agctl-run-test-runbook` produces an evidence report. Authoring and execution are separate workflow steps; splitting them also makes the evidence report a first-class deliverable.
- **Portable, not wired.** Like `agctl-config`, these live in `skills/` for users to copy; `agctl`'s runtime is unchanged. Each skill is file-level independent (D10): a run-only install can parse a runbook without the write skill.
- **Agent-executed, no CLI runner.** Execution is driven by the agent following the run skill. A declarative CLI `agctl run` is explicitly a deferred non-goal (§4, §9) — it is redundant while the agent is in the loop.
- **Markdown runbook format.** Agent-native, human-readable, diff-able, committable. A YAML runbook is rejected (§9) — it only earns its keep if a non-agent runtime consumes it, which is out of scope.
- **Input-adaptive.** The write skill accepts a rich spec *or* a one-line testing request; planning depth scales to input (§6.1 specifies how).
- **`agctl`-native.** Steps are grounded in `agctl` primitives via `discover`. Free-form (`http request`, `db --sql`) is the escape hatch only when no template exists — mirroring `agctl`'s own philosophy and AGENTS.md.
- **Layers on the operational `agctl` skill.** Both skills assume the agent can already drive `agctl` primitives (call commands, interpret the JSON envelope, read `ok`/exit codes). They add orchestration + reporting discipline, not primitive knowledge.

## 4. Non-Goals

- **A CLI scenario executor** (`agctl run <runbook>`). Deferred (DESIGN §10 "multi-step scenario chaining"); redundant while the agent drives execution (§9).
- **A parallel test framework** (a pytest replacement). The runbook is a *procedure an agent follows*, not a *program a runtime executes*.
- **Trusted execution / tamper-proofing.** The report is auditable, not enforced (§1 scope honesty).
- **Runbooks that orchestrate non-`agctl` steps.** The skills are `agctl`-native; they do not aim to drive arbitrary shell. (A step may still be a free-form `agctl` command.)
- **Automatic runbook generation without agent judgment.** `agctl` itself does not inspect a codebase and write tests (DESIGN §1 non-goal). The write skill is an agent procedure that uses judgment, not a generator.
- **A runbook registry / persistence backend.** Runbooks are files; where they live is a convention, not a system.
- **Background commands (`mock run`, `http ping`) as verdict-bearing Steps.** They emit NDJSON, not a single envelope; they are fixture-lifecycle (D13), not Steps.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Skill, not CLI code.** Formalize as portable skills in `skills/`; add no `agctl` command. | `agctl`'s primitives already cover runbook needs; the gap is procedural discipline + an auditable report. A skill is the right layer for an agent-reasoning procedure; a CLI cannot "read a spec and design a runbook." Stays within DESIGN §1's scope: it does not add a CLI command that auto-generates tests or orchestrates scenarios. |
| D2 | **Split into two skills by deliverable.** `agctl-write-test-runbook` (→ runbook) and `agctl-run-test-runbook` (→ evidence report). | *(Revised for honesty.)* The split is **workflow separation**, not a technical "different trust contract" — both skills are agent-followed and equally trust the agent. The value is: (a) authoring and execution are separate workflow steps (write now, run later or by another session); (b) it mirrors the author/run separation seen in plan workflows; (c) it makes the auditable report a *headline deliverable* with its own skill, not a buried section. |
| D3 | **Names: `agctl-write-test-runbook` / `agctl-run-test-runbook`.** Keep the `agctl-` family prefix; use the verb `write` (not `build`). | `agctl-config` is already an authoring/procedure skill and kept the `agctl-` prefix — these are its cousins. `agent-` was rejected (§9) as it breaks family consistency and obscures the hard `agctl` coupling. `write` > `build` (unambiguous). |
| D4 | **Built-in adaptive template.** One `reference/runbook-template.md` with the full structure; unused fixture sections are pruned. | Consistency *is* the value of formalizing; without a concrete template the skill is prose and agents drift. Direct precedent: `skills/agctl-config/reference/` ships snippet templates. One adaptive template beats two (no sync burden) or a section library (over-engineered for a linear runbook). |
| D5 | **Annotated runbook report.** The run skill writes `runbook.md` → `runbook.results.md`: each step gains an **Actual** block beneath its **Expected**, plus a verdict summary. | Expected-vs-actual inline in one persisted, diff-able file is the most direct fix for "reviewing logs and guessing." Avoids the cross-referencing of a standalone report and the ephemerality of an in-chat report (both rejected, §9). |
| D6 | **Auditable spine per step.** Every step records the verbatim command, exit code, and raw `ok` / `error.type`. Curated excerpts (HTTP body, DB rows, matched Kafka message) sit alongside, never instead of. | *(Revised for honesty.)* The spine does **not** deter fabrication — an agent can still invent an exit code. What it does is make the report *auditable*: a reviewer can re-run any step's verbatim command and compare (see §8 "For reviewers"). "Verify, don't trust," not tamper-proofing (§1). Curated excerpts alone could hide a failure, so the spine is mandatory. |
| D7 | **Stop on first failure; mark the rest "skipped — not exercised (blocked by step N)."** | Avoids cascade noise: a failed `create-order` would make the downstream `kafka assert` falsely "fail" too, eroding trust in the report. Matches `agctl`'s AGENTS.md "stop and diagnose" rule. *(Acknowledged blind spot:)* a skipped step might have failed for an *independent* reason a regression sweep would catch — the report marks these explicitly as **not exercised** so a reviewer knows to re-run after fixing. Continue-and-record-all remains rejected (§9); report credibility outweighs one-pass completeness. |
| D8 | **Discovery-grounded steps.** Write runs `agctl discover` and prefers named templates/patterns; free-form is the escape hatch only when no template exists. | Mirrors `agctl`'s own philosophy and AGENTS.md. Grounds runbooks in the consuming repo's real surface; avoids invented commands that drift from config. (`discover` has no `mocks` category — §6.1.) |
| D9 | **Expected grammar + pass/fail semantics.** A step's **Expected** is a list of `<envelope-path>: <literal>` pairs (dotted path into the JSON envelope per DESIGN §4.1/§4.2); all entries are ANDed. Coercion inherits `agctl`'s `--equals` type-aware semantics (DESIGN §3.3: literals are JSON-parsed and compared strictly — `0` ≠ `"0"`). Assertion steps use the shorthand `Expected: exit 0`. **Any** step with exit≠0 or `ok:false` is always FAIL, regardless of Expected. | `agctl` treats HTTP status as a result, not an assertion; the runbook declares expected outcomes so the run skill can compare. Type-awareness matches `--equals` to avoid `201` vs `"201"` ambiguity. Without a grammar, two implementers diverge (freeform prose vs structured pairs). |
| D10 | **Authoring template vs parse anatomy, split across skills.** The *authoring template* lives only in the write skill's `reference/runbook-template.md`. The run skill carries a *self-contained runbook anatomy* in its SKILL.md (the fields it parses: Goal, Preconditions, Fixtures, Steps [Command/Capture/Expected], Cleanup) so a run-only install can execute a runbook without the write skill. | *(Revised.)* The prior "run references write's template" broke independent portability — a run-only install had no format definition. Splitting authoring (write) from parsing (run) keeps each skill file-level independent, like `agctl` / `agctl-config`. |
| D11 | **File locations: runbooks committable, results typically gitignored.** Runbooks are test plans (committable like code); `*.results.md` are ephemeral per-run and typically `.gitignore`d — but a user may `git add` a specific results file as evidence. The skills suggest a `runbooks/` dir but enforce no path. | Runbooks are durable artifacts worth versioning; results are transient, but locking them entirely out of git would prevent using a failure report as reviewable evidence. |
| D12 | **Variable capture across steps.** A step may declare `Capture: VAR=<envelope-path>` (e.g. `Capture: ORDER_ID=result.body.order_id`); the run skill exports it **runbook-scoped** (string-default) and substitutes `$VAR` in later steps' Commands. Unresolved `$VAR` at execution is a validation error (§6.2). | The run-then-assert pattern needs a created ID in later steps. Without an explicit capture syntax, agents guess (jq? regex? typing? scoping?) and runbooks lose portability — the top review finding. String-default matches shell substitution; agctl's own AGENTS.md pattern-3 uses `jq -r '.result.body.order_id'`. |
| D13 | **Background commands (`mock run`, `http ping`) are fixture-lifecycle, not verdict-bearing Steps.** They emit NDJSON, not a single envelope, so they cannot have a per-step exit/`ok`. They live under **Fixtures**: started in Setup (poll the mock log for the `started` line), stopped in Teardown (SIGTERM + wait, never SIGKILL). A mock is assessed by grepping its log for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error` — any hit is a failure that flips the overall verdict to FAIL. | D9's semantics assumed one envelope per step; streaming commands break that. Treating them as lifecycle keeps the Steps loop uniform (one envelope each) and inherits the mock's documented failure-stream protocol (mock spec §10.1). |

## 6. Skill Structures

### 6.1 `skills/agctl-write-test-runbook/`

```
skills/agctl-write-test-runbook/
├── SKILL.md                       # authoring procedure
└── reference/
    ├── runbook-template.md        # the adaptive runbook skeleton (central format, §7)
    ├── fixtures-mock.md           # mock lifecycle + failure-stream protocol
    └── fixtures-heartbeat.md      # http ping background pattern
```

**Reference-file scope** (sources cited so an implementer can author them without guessing):
- `fixtures-mock.md` — start `mock run > mock.log 2>&1 &`; **poll `mock.log` for the `started` line** before proceeding; terminate with **SIGTERM + `wait`** (never SIGKILL, which skips the summary/exit code); after the run, **grep the log for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error`** and treat any hit as a failure. Sources: DESIGN §3.5, mock spec §10.1.
- `fixtures-heartbeat.md` — start `agctl http ping <template> --interval N --until-stopped &`; capture the PID; `kill` it in cleanup. Sources: DESIGN §3.1, AGENTS.md pattern 5.

**SKILL.md procedure (design-level):**

1. **Ingest** — accept a spec (link/doc) *or* a one-line testing request. **One-line request → Steps-only runbook** (no Fixtures/Cleanup sections). **Spec link → full structure** (Goal + Preconditions + Fixtures + Steps + Cleanup).
2. **Discover** — run `agctl discover` for `services` / `http-templates` / `kafka-patterns` / `db-templates` to ground steps in real templates. **`discover` has no `mocks` category** (DESIGN §3.7 lists only those four) — read the `mocks:` config section directly to ground mock fixtures (precedent: `agctl-config`).
3. **Design** — sequence steps grounded in discovered templates; declare each step's **Expected** (D9) and any **Capture** (D12); identify fixtures (seed data, mocks, heartbeat) and cleanup.
4. **Emit** — instantiate `reference/runbook-template.md`, pruning unused fixture sections; fill in goal, source, preconditions, steps, cleanup.

Seed-data fixtures (`agctl db execute`, `mode: write`, idempotent via `ON CONFLICT`) are simple enough to cover inline in the template. Mocks and heartbeat have failure-prone lifecycle protocols and get the dedicated reference pages above.

### 6.2 `skills/agctl-run-test-runbook/`

```
skills/agctl-run-test-runbook/
├── SKILL.md                       # execution + report procedure (includes self-contained runbook anatomy, D10)
└── reference/
    └── report-annotation.md       # the Actual-block + verdict format contract (§8)
```

**SKILL.md procedure (design-level):**

1. **Validate** *(pre-execution)* — every step has a `Command` and a non-empty `Expected`; every `$VAR` resolves to a prior `Capture`; assertion steps use `Expected: exit 0`. On any violation, stop with a validation error **before executing anything** (no partial runs).
2. **Setup** — run Preconditions (`agctl check ready`); start fixtures: seed data (`db execute`), mock (`mock run > mock.log 2>&1 &`, then **poll `mock.log` for `started`**), heartbeat (`http ping … &`). Capture fixture PIDs.
3. **Execute** steps in order. For each: substitute `$VAR` from prior Captures; run the verbatim command; capture exit code + raw `ok` / `error.type` + a curated excerpt from the envelope.
4. **Annotate** an **Actual** block beneath each step's **Expected** (D5, D6).
5. **On failure** (exit≠0, `ok:false`, or any Expected mismatch per D9): stop; mark all remaining steps **skipped — not exercised (blocked by step N)** (D7).
6. **Teardown** *(always, even on failure)* — kill heartbeat PIDs; SIGTERM mock PID + `wait`; **grep `mock.log` for error events** (any → overall verdict FAIL); optional seed reset.
7. **Emit** — verdict summary at the top; write `runbook.results.md` alongside the runbook.

**Verdict tallies** count Steps only (pass / fail / skipped). Fixtures appear in a Setup/Teardown block, not the step tally; a fixture or teardown failure (e.g. mock error events) flips the overall verdict to FAIL.

## 7. Runbook Format Contract (`reference/runbook-template.md`)

The adaptive skeleton. Sections marked *(optional)* are pruned when unused. The run skill's self-contained anatomy (D10) parses the same fields.

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
- `agctl db execute --template seed-… --write`   (template has `mode: write`; idempotent via ON CONFLICT)
### Mocks *(optional — see fixtures-mock.md)*
- `agctl mock run > mock.log 2>&1 &`   → poll mock.log for "started" before continuing
### Heartbeat *(optional — see fixtures-heartbeat.md)*
- `agctl http ping heartbeat --interval 5 --until-stopped &`

## Steps
### 1. <step name>
- **Command:** `agctl http call create-order --param customer_id=cust-42 …`
- **Capture:** `ORDER_ID=result.body.order_id`   *(optional; runbook-scoped; $ORDER_ID in later steps)*
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

**Step semantics:**
- **Command** — the verbatim `agctl` invocation (may reference `$VAR` from a prior Capture).
- **Capture** *(optional)* — `VAR=<envelope-path>`; exported runbook-scoped (string-default) for `$VAR` substitution in later Commands (D12).
- **Expected** — per D9: a list of `<envelope-path>: <literal>` pairs (ANDed; type-aware); or `exit 0` for assertion steps.

Background commands (`mock run`, `http ping`) appear under **Fixtures**, not Steps, because they emit NDJSON rather than a single envelope (D13).

## 8. Report Format Contract (`reference/report-annotation.md`)

The annotated runbook (`runbook.results.md`). Each step gains an **Actual** block; a verdict summary sits at the top; Setup/Teardown appear as their own blocks.

````markdown
# Runbook results: <name>
**Verdict:** FAIL at step 2 · 1 passed · 1 failed · 1 skipped (not exercised) · 2026-07-04T15:37:00Z

## Setup
- `check ready --all` → ok   · mock started (pid 4123)   · heartbeat started (pid 4124)

### 1. <step name> — PASS
- **Run:** `agctl http call create-order --param customer_id=cust-42 …`
- **Exit:** 0 · **ok:** true
- **Actual:** status_code 201, body.order_id=ord-xyz   (captured ORDER_ID=ord-xyz)
- **Expected:** ok:true, result.status_code:201 ✓

### 2. <step name> — FAIL
- **Run:** `agctl kafka assert --pattern order-created --param orderId=ord-xyz --timeout 10`
- **Exit:** 1 · **ok:** false · **error.type:** AssertionError
- **Actual:** no matching message within 10s (scanned 3)
- **Expected:** exit 0 ✗

### 3. <step name> — SKIPPED (not exercised — blocked by step 2)
- **Expected:** exit 0   *(not run)*

## Teardown
- heartbeat killed · mock SIGTERM+wait (exit 0) · mock.log grep: no error events
````

**Rules:**
- **Mandatory spine (D6):** every executed step's Actual block carries the verbatim **Run** command, **Exit** code, and raw **ok** / **error.type**. Curated excerpts are *additional*, never a substitute.
- **SKIPPED steps** echo **Expected** for context but show no Run/Exit/Actual, and are marked *not exercised*.
- **Excerpt truncation:** curated excerpts keep the first ~150 chars + `… (truncated)` when a value exceeds ~200 chars, or render the full value in a fenced code block if needed for review.
- **Timestamps:** ISO-8601 Z (matching `agctl` house style — `http ping`, `mock run`).
- **Fixture results** appear in the Setup/Teardown blocks, not the step tally; a teardown-detected failure (e.g. mock error events) flips the verdict to FAIL even if all Steps passed.

**For reviewers — how to verify a report (D6):** re-run any step's verbatim `Run` command against the same environment and compare `Exit` / `ok` / the excerpt. The spine exists so this check is trivial; it does not prevent fabrication, it makes it falsifiable.

## 9. Rejected Alternatives (ADR-style)

- **A CLI `agctl run <runbook>` executor.** Rejected (D1): reverses DESIGN §1's scenario-orchestration non-goal; forces a YAML runbook (killing the markdown format); duplicates the in-context aggregation the agent already does; grows the command surface against `agctl`'s "ten focused commands" rule (DESIGN §8). The agent is in the loop in the stated workflow, making a runner redundant. Deferred until an agentless-CI need is concrete.
- **One skill with build + execute phases.** Rejected (D2): buries the evidence report as a section, undercutting its first-class-deliverable status.
- **`agent-` prefix.** Rejected (D3): breaks the `agctl-*` family consistency (the skills are hard-coupled to `agctl`); `agctl-config` already proves procedure skills keep the `agctl-` prefix.
- **YAML runbook format.** Rejected: only earns its keep if a non-agent runtime consumes it (out of scope); markdown is agent-native, human-readable, diff-able.
- **Two templates (full + minimal) or a section library.** Rejected (D4): two templates create a sync burden and an upfront "which one" choice; a section library is over-engineered for a linear runbook. One adaptive template with pruning covers the input spectrum.
- **Standalone report file.** Rejected (D5): forces the reviewer to cross-reference runbook + report; annotating the runbook keeps expected vs actual inline.
- **In-chat-only report.** Rejected (D5): not persisted, diff-able, or reviewable after the fact — undercuts the "stop guessing from logs" goal.
- **Continue-and-record-all on failure.** Rejected (D7): cascade noise erodes the report's credibility. The blind spot (an independently-failing downstream step goes unseen) is acknowledged and mitigated by marking skipped steps *not exercised*; a reviewer wanting a full sweep re-runs after fixing. A `--continue-on-failure` flag is YAGNI for now.
- **Background commands as Steps with a per-step verdict.** Rejected (D13): they emit NDJSON, not a single envelope; forcing them into the per-step exit/`ok` model would misrepresent their result. Fixture-lifecycle is the honest model.

## 10. Validation Strategy

These are markdown procedures, not code, so validation is end-to-end behavior + contract checks. The repo cannot run the sample `agctl.yaml`'s order-service SUT, so validation targets the repo's **own mock server** (which it *can* stand up), gated behind the existing live-test flag:

- **Dry-run (write skill):** run the write skill against the repo's `agctl.yaml` targeting a goal the mock satisfies — e.g. *`kafka produce` to a mock reactor's input topic → the reactor fires its reaction → `kafka assert` matches the reaction* (and/or *`http call` against a mock HTTP stub → assert the stubbed response*). The mock is within-transport only (DESIGN §3.5), so these are independent exercises, not an HTTP→Kafka cross-trigger. Confirm it produces a well-formed `runbook.md` that uses real discovered templates, reads `mocks:` from config (not discover), and prunes unused fixture sections.
- **Dry-run (run skill):** execute that runbook under `AGCTL_TEST_LIVE=1` (testcontainers Postgres + Kafka + the local mock, per ARCHITECTURE §12); confirm `runbook.results.md` is well-formed, every executed step carries the D6 spine, `$VAR` Capture flows between steps, Setup/Teardown run, and a deliberately-broken step produces the correct FAIL + skipped-not-exercised cascade + verdict.
- **Command cross-check:** confirm every `agctl` command referenced by both skills (`http call/request/ping`, `kafka produce/consume/assert`, `db query/assert/execute`, `check ready`, `mock run`, `discover`) exists in `agctl` today (DESIGN §3), so the skills never reference a non-existent surface.

After implementation, `docs-watcher` runs (CLAUDE.md "Docs Sync") — expected no-op for DESIGN.md/ARCHITECTURE.md (CLI contract unchanged), confirming the `skills/` tree grew correctly.

## 11. Docs & Skill Impact

- **`skills/` tree** — two new portable skill directories (`agctl-write-test-runbook/`, `agctl-run-test-runbook/`), consumed by copying into a user's `.claude/skills/`. Not wired into `agctl`'s runtime. Each is file-level independent (D10): the run skill embeds its own runbook anatomy and does not require the write skill to be installed.
- **`DESIGN.md` / `ARCHITECTURE.md`** — expected no-op; these docs describe the `agctl` CLI, whose contract is unchanged. `docs-watcher` confirms.
- **No code, config, packaging, or entry-point changes.** No `version` bump. No new `agctl` command.
