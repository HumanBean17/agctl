# Test Runbook Skills Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement two portable Claude skills (`agctl-write-test-runbook`, `agctl-run-test-runbook`) that formalize the agctl spec/request → runbook → execute → auditable-report workflow.

**Architecture:** Six markdown files across two `skills/` directories — no agctl CLI, config, packaging, or entry-point changes. The write skill owns the runbook format (adaptive template + Capture/Expected grammar); the run skill embeds a self-contained runbook anatomy (D10) so it works without the write skill installed, executes steps with a Setup/Teardown lifecycle, and writes an annotated evidence report.

**Tech Stack:** GitHub-flavored Markdown; portable Claude skill format (`SKILL.md` + `reference/`). Zero runtime dependencies.

## Global Constraints

Copied verbatim from the approved spec (`docs/superpowers/specs/active/2026-07-04-test-runbook-skills-design.md`, commit `1eef4f4`); every task's requirements implicitly include these:

- **No agctl CLI/config/packaging/entry-point changes.** No `version` bump, no new command, no code. Deliverables are markdown only.
- **Portable, not wired.** Skills ship in `skills/` for users to copy into their own `.claude/skills/`; they are NOT imported by `agctl`'s runtime.
- **File-level independence (D10).** The run skill must parse a runbook without the write skill installed (it carries its own anatomy section).
- **Every referenced agctl command/flag must exist** in DESIGN §3: `http call|request|ping`, `kafka produce|consume|assert`, `db query|assert|execute`, `check ready`, `mock run`, `discover`. The `discover` categories are `services`, `http-templates`, `kafka-patterns`, `db-templates` only — **no `mocks` category** (read `mocks:` from config directly).
- **agctl output model (DESIGN §4):** one JSON envelope per invocation (`{ok, command, result, error, duration_ms}`) except `http ping` and `mock run`, which stream NDJSON. Exit codes: `0` success, `1` assertion failure, `2` tool/config/env error.
- **Grammar rules (spec D9/D12):** Capture is `VAR=<envelope-path>` (dotted path into the envelope), runbook-scoped, string-default, substituted as `$VAR`. Expected is a list of `<envelope-path>: <literal>` pairs (ANDed; type-aware per agctl `--equals`, DESIGN §3.3). Assertion steps use `Expected: exit 0`. exit≠0 or `ok:false` is always FAIL.
- **Background commands (D13):** `mock run` and `http ping` are fixture-lifecycle (Setup/Teardown), never verdict-bearing Steps.
- **Timestamps** ISO-8601 Z (house style).
- **Verification model:** because deliverables are markdown, each task's "test" is a structural-conformance check (exact `grep` assertions against the contracts below) plus the §10 end-to-end dry-run in Task 6. There are no unit tests.

---

## File Structure

```
skills/agctl-write-test-runbook/
├── SKILL.md                       # Task 3 — authoring procedure (Ingest/Discover/Design/Emit)
└── reference/
    ├── runbook-template.md        # Task 1 — adaptive runbook skeleton (central format)
    ├── fixtures-mock.md           # Task 2 — mock lifecycle + failure-stream protocol
    └── fixtures-heartbeat.md      # Task 2 — http ping background pattern

skills/agctl-run-test-runbook/
├── SKILL.md                       # Task 5 — execution procedure + self-contained runbook anatomy (D10)
└── reference/
    └── report-annotation.md       # Task 4 — annotated-runbook report format contract
```

**Ordering rationale:** Template (Task 1) first because both SKILL.md files and the report contract reference its grammar. Fixtures (Task 2) before the write SKILL.md (Task 3) that points to them. Report contract (Task 4) before the run SKILL.md (Task 5) that points to it. Validation (Task 6) last — it exercises everything.

---

## Task 1: Runbook template

**Files:**
- Create: `skills/agctl-write-test-runbook/reference/runbook-template.md`

**Interfaces:**
- **Consumes:** spec §7 (runbook format contract), D9 (Expected grammar), D12 (Capture syntax), D13 (background commands as fixtures).
- **Produces:** the adaptive runbook skeleton. The write SKILL.md (Task 3) instantiates it; the run SKILL.md anatomy (Task 5) mirrors its field names exactly. **Field names are the contract** — later tasks use these literals verbatim: section headings `Goal`, `Preconditions`, `Fixtures`, `Steps`, `Cleanup`; fixture subsection headings `Seed data`, `Mocks`, `Heartbeat`; per-step fields `**Command:**`, `**Capture:**` (optional), `**Expected:**`.

**Required content (the implementer authors prose around this exact structure):**
- Top: `# Runbook: <name>` then a metadata line `**Source:** <spec link | "one-line request: …">   **Date:** YYYY-MM-DD`.
- `## Goal` — placeholder for 1-2 sentences on what the runbook verifies.
- `## Preconditions` — a checklist entry `agctl check ready --all` → all services ready, plus an env-assumption placeholder.
- `## Fixtures` with the note "include the subsections you need; drop the rest", containing three optional subsections:
  - `### Seed data` — `agctl db execute --template seed-… --write` with the note: template has `mode: write`; idempotent via `ON CONFLICT`.
  - `### Mocks` — `agctl mock run > mock.log 2>&1 &` with the note: poll `mock.log` for `started` before continuing; see `fixtures-mock.md`.
  - `### Heartbeat` — `agctl http ping heartbeat --interval 5 --until-stopped &`; see `fixtures-heartbeat.md`.
- `## Steps` with three example step blocks, each `### N. <step name>`:
  1. `**Command:** agctl http call create-order --param customer_id=cust-42 …`; `**Capture:** ORDER_ID=result.body.order_id` (annotate: optional, runbook-scoped, `$ORDER_ID` in later steps); `**Expected:** ok: true, result.status_code: 201`.
  2. `**Command:** agctl kafka assert --pattern order-created --param orderId=$ORDER_ID --timeout 10`; `**Expected:** exit 0`.
  3. `**Command:** agctl db assert --template find-order --param orderId=$ORDER_ID --expect-value --path .status --equals PENDING`; `**Expected:** exit 0`.
- `## Cleanup` — "reverse of fixtures": kill heartbeat PID; SIGTERM mock PID + wait; (optional) reset seed data.
- A short "Step semantics" note restating: Command (verbatim, may use `$VAR`), Capture (`VAR=<envelope-path>`, runbook-scoped, string-default), Expected (`<envelope-path>: <literal>` pairs ANDed, or `exit 0` for assertions). And: background commands belong under Fixtures, not Steps (they emit NDJSON).

- [ ] **Step 1: Define conformance criteria (the "test")**

The file must contain, exactly: 5 top-level sections (`## Goal`, `## Preconditions`, `## Fixtures`, `## Steps`, `## Cleanup`); 3 fixture subsections (`### Seed data`, `### Mocks`, `### Heartbeat`); at least one occurrence each of `**Command:**`, `**Capture:**`, `**Expected:**`; the literals `mode: write`, `ON CONFLICT`, `$ORDER_ID`, `result.body.order_id`, `exit 0`; and the poll-`started` instruction.

- [ ] **Step 2: Author the template**

Write `skills/agctl-write-test-runbook/reference/runbook-template.md` containing the structure above with light instructional prose (prune markers, the "include what you need" note, the semantics note). Use the literal field names and example commands verbatim — they are the contract other tasks depend on.

- [ ] **Step 3: Verify conformance**

Run each; every line must print `1` (or the stated minimum):
```
grep -c '^## Goal$'          skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^## Preconditions$' skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^## Fixtures$'      skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^## Steps$'         skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^## Cleanup$'       skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^### Seed data$'    skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^### Mocks$'        skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '^### Heartbeat$'    skills/agctl-write-test-runbook/reference/runbook-template.md   # → 1
grep -c '\*\*Capture:\*\*'   skills/agctl-write-test-runbook/reference/runbook-template.md   # → ≥1
grep -c '\*\*Expected:\*\*'  skills/agctl-write-test-runbook/reference/runbook-template.md   # → ≥1
grep -c 'mode: write'        skills/agctl-write-test-runbook/reference/runbook-template.md   # → ≥1
grep -c 'ON CONFLICT'        skills/agctl-write-test-runbook/reference/runbook-template.md   # → ≥1
grep -c 'ORDER_ID=result.body.order_id' skills/agctl-write-test-runbook/reference/runbook-template.md  # → 1
grep -c '\$ORDER_ID'         skills/agctl-write-test-runbook/reference/runbook-template.md   # → ≥1
grep -c 'started'            skills/agctl-write-test-runbook/reference/runbook-template.md   # → ≥1 (mock poll-for-started note)
```
If any differs, fix the file. Do not proceed until all match.

- [ ] **Step 4: Commit**

```
git add skills/agctl-write-test-runbook/reference/runbook-template.md
git commit -m "feat(skills): add runbook template for agctl-write-test-runbook"
```

---

## Task 2: Fixture reference docs

**Files:**
- Create: `skills/agctl-write-test-runbook/reference/fixtures-mock.md`
- Create: `skills/agctl-write-test-runbook/reference/fixtures-heartbeat.md`

**Interfaces:**
- **Consumes:** spec §6.1 (reference-file scope + sources), D13. Sources to draw from: DESIGN §3.5 (`agctl mock run` lifecycle, NDJSON events, agent failure-stream protocol), mock spec §10.1 (the 4-step log protocol), DESIGN §3.1 (`http ping`), AGENTS.md pattern 5 (heartbeat background+kill).
- **Produces:** the two lifecycle protocols. The write SKILL.md (Task 3) points to them by name; the run SKILL.md Setup/Teardown (Task 5) relies on the mock's log-grep contract: a run is a mock failure iff `http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, or `kafka.error` appears in `mock.log`.

**`fixtures-mock.md` required content:**
- How to start: `agctl mock run > mock.log 2>&1 &` (redirect stdout to a log; capture the PID).
- Startup gate: **poll `mock.log` for the `started` line** before running any Step — do not sleep a fixed delay.
- Termination: `SIGTERM` the PID then `wait` — **never `SIGKILL`** (it skips the shutdown handler, the summary line, and the exit code).
- Failure detection (the load-bearing contract): after the run, grep `mock.log` for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error`; any hit is a failure that must flip the run verdict to FAIL.
- `--fail-fast` as the synchronous foreground alternative for `--duration` runs.

**`fixtures-heartbeat.md` required content:**
- How to start: `agctl http ping <template> --interval N --until-stopped &`; capture the PID; note it emits one JSON line per ping plus a final summary on SIGTERM/SIGINT.
- Cleanup: `kill $PID` (SIGTERM) in Teardown so the summary line lands; do not SIGKILL.
- Typical use: session-keepalive during a long multi-step run (AGENTS.md pattern 5).

- [ ] **Step 1: Define conformance criteria**

`fixtures-mock.md` must contain the literals: `mock.log`, `started`, `SIGTERM`, `SIGKILL` (as the thing to avoid), and all four failure event names (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`), plus `--fail-fast`. `fixtures-heartbeat.md` must contain: `http ping`, `--interval`, `--until-stopped`, `kill`, and a reference to the summary line.

- [ ] **Step 2: Author both files**

Write the two reference docs per the content above, citing the source sections (DESIGN §3.5 / §3.1, AGENTS.md pattern 5) inline so a reader can verify.

- [ ] **Step 3: Verify conformance**

```
grep -c 'http.unmatched'        skills/agctl-write-test-runbook/reference/fixtures-mock.md   # → ≥1
grep -c 'http.body_parse_skipped' skills/agctl-write-test-runbook/reference/fixtures-mock.md # → ≥1
grep -c 'kafka.skipped'          skills/agctl-write-test-runbook/reference/fixtures-mock.md  # → ≥1
grep -c 'kafka.error'            skills/agctl-write-test-runbook/reference/fixtures-mock.md  # → ≥1
grep -c 'SIGTERM'                skills/agctl-write-test-runbook/reference/fixtures-mock.md  # → ≥1
grep -c 'SIGKILL'                skills/agctl-write-test-runbook/reference/fixtures-mock.md  # → ≥1
grep -c 'poll mock.log'          skills/agctl-write-test-runbook/reference/fixtures-mock.md  # → ≥1
grep -c '\-\-until-stopped'      skills/agctl-write-test-runbook/reference/fixtures-heartbeat.md # → ≥1
grep -c 'http ping'              skills/agctl-write-test-runbook/reference/fixtures-heartbeat.md # → ≥1
grep -c 'kill'                   skills/agctl-write-test-runbook/reference/fixtures-heartbeat.md # → ≥1
```

- [ ] **Step 4: Commit**

```
git add skills/agctl-write-test-runbook/reference/fixtures-mock.md skills/agctl-write-test-runbook/reference/fixtures-heartbeat.md
git commit -m "feat(skills): add mock + heartbeat fixture reference docs"
```

---

## Task 3: Write skill `SKILL.md`

**Files:**
- Create: `skills/agctl-write-test-runbook/SKILL.md`

**Interfaces:**
- **Consumes:** Task 1 (`runbook-template.md`), Task 2 (`fixtures-mock.md`, `fixtures-heartbeat.md`); spec §6.1, D4, D8, D12.
- **Produces:** the authoring procedure. It owns the runbook format (Task 1's template is its artifact). The run SKILL.md (Task 5) does NOT depend on this file (D10 independence), but the grammar it parses must match Task 1 exactly.

**Required content:**
- **Frontmatter:** mirror the shape used by `skills/agctl-config/SKILL.md` (a `name:` + `description:` YAML block) — follow that established pattern. `name: agctl-write-test-runbook`; `description:` a one-liner: "Author an agctl test runbook (a sequence of agctl commands + assertions, with fixtures and cleanup) from a spec or a one-line testing request."
- **A 4-step procedure** with these exact headings (the implementer fills prose):
  1. **Ingest** — accept a spec (link/doc) or a one-line testing request; **state the input-adaptive rule explicitly**: a one-line request yields a Steps-only runbook (no Fixtures/Cleanup sections); a spec link yields the full structure (Goal + Preconditions + Fixtures + Steps + Cleanup).
  2. **Discover** — run `agctl discover` for `services` / `http-templates` / `kafka-patterns` / `db-templates` to ground steps in real templates. **State the no-`mocks`-category rule:** `discover` has no `mocks` category (DESIGN §3.7), so read the `mocks:` config section directly to ground mock fixtures (precedent: `agctl-config`).
  3. **Design** — sequence steps grounded in discovered templates; declare each step's `**Expected:**` (envelope-path:literal pairs, ANDed; `exit 0` for assertions) and any `**Capture:**` (`VAR=<envelope-path>`); prefer named templates, allow free-form only when no template exists; identify fixtures (seed / mock / heartbeat) and cleanup.
  4. **Emit** — instantiate `reference/runbook-template.md`, pruning unused fixture sections; fill goal, source, preconditions, steps, cleanup; write `runbook.md` (committable; results are gitignored — D11).
- **Pointers** to the three reference files: `reference/runbook-template.md`, `reference/fixtures-mock.md`, `reference/fixtures-heartbeat.md`.
- A short "see also" line pointing to the sibling `agctl-config` skill as the authoring-pattern precedent.

- [ ] **Step 1: Define conformance criteria**

The file must contain frontmatter with `name: agctl-write-test-runbook`; the four procedure steps (Ingest, Discover, Design, Emit); the input-adaptive rule (mention of "Steps-only"); the no-`mocks`-category rule; pointers to all three reference files; and a pointer to `agctl-config`.

- [ ] **Step 2: Author the SKILL.md**

Write `skills/agctl-write-test-runbook/SKILL.md` per the content above. Keep setup/discovery/install meta out of SKILL.md (it lives in the spec/memory). Lead with `description`, then the procedure.

- [ ] **Step 3: Verify conformance**

```
grep -c '^name: agctl-write-test-runbook' skills/agctl-write-test-runbook/SKILL.md   # → 1
grep -c 'Ingest'    skills/agctl-write-test-runbook/SKILL.md   # → ≥1
grep -c 'Discover'  skills/agctl-write-test-runbook/SKILL.md   # → ≥1
grep -c 'Design'    skills/agctl-write-test-runbook/SKILL.md   # → ≥1
grep -c 'Emit'      skills/agctl-write-test-runbook/SKILL.md   # → ≥1
grep -c 'Steps-only' skills/agctl-write-test-runbook/SKILL.md  # → ≥1
grep -ci 'no .mocks. category' skills/agctl-write-test-runbook/SKILL.md  # → ≥1
grep -c 'runbook-template.md'   skills/agctl-write-test-runbook/SKILL.md # → ≥1
grep -c 'fixtures-mock.md'      skills/agctl-write-test-runbook/SKILL.md # → ≥1
grep -c 'fixtures-heartbeat.md' skills/agctl-write-test-runbook/SKILL.md # → ≥1
grep -c 'agctl-config'          skills/agctl-write-test-runbook/SKILL.md # → ≥1
```

- [ ] **Step 4: Commit**

```
git add skills/agctl-write-test-runbook/SKILL.md
git commit -m "feat(skills): add agctl-write-test-runbook SKILL.md"
```

---

## Task 4: Report annotation contract

**Files:**
- Create: `skills/agctl-run-test-runbook/reference/report-annotation.md`

**Interfaces:**
- **Consumes:** spec §8, D5, D6, D7, D9. The grammar from Task 1 (field names `**Command:**`/`**Run:**`, `**Expected:**`/`**Actual:**`, `**Capture:**`).
- **Produces:** the annotated-runbook format the run SKILL.md (Task 5) writes to `runbook.results.md`.

**Required content:**
- The report skeleton (`runbook.results.md`): a `# Runbook results: <name>` title; a `**Verdict:**` line of the form `<PASS|FAIL at step N> · <P> passed · <F> failed · <S> skipped (not exercised) · <ISO-8601 Z timestamp>`.
- A `## Setup` block listing fixture startups (`check ready`, mock started + pid, heartbeat started + pid).
- Per-step rendering: `### N. <step name> — <PASS|FAIL|SKIPPED>`. PASS/FAIL show the spine (`**Run:**` verbatim command, `**Exit:** <code> · **ok:** <bool>` and on FAIL `· **error.type:** <type>`), a curated `**Actual:**` excerpt, and `**Expected:**` echoed with ✓/✗. SKIPPED shows only `**Expected:**` (echoed for context) marked *not exercised* — no Run/Exit/Actual.
- A `## Teardown` block (heartbeat killed; mock SIGTERM+wait + exit code; mock.log grep result: "no error events" or the events found).
- **Rules** section: mandatory spine (D6 — curated excerpts never substitute for command+exit+ok); SKIPPED renders Expected-only; excerpt truncation (first ~150 chars + `… (truncated)` over ~200 chars, or a fenced block); timestamps ISO-8601 Z; fixture results live in Setup/Teardown, not the step tally, and a teardown-detected failure flips the verdict to FAIL.
- **"For reviewers — how to verify a report"** note (D6): re-run any step's verbatim `Run` command and compare Exit/ok/excerpt; the spine makes fabrication falsifiable, not impossible.

- [ ] **Step 1: Define conformance criteria**

Must contain the literals: `**Verdict:**`, `## Setup`, `## Teardown`, `**Run:**`, `**Exit:**`, `**ok:**`, `**Actual:**`, `**Expected:**`, `not exercised`, `error.type`, `… (truncated)`, an ISO-8601 example timestamp (e.g. `2026-07-04T15:37:00Z`), and the "For reviewers" heading.

- [ ] **Step 2: Author the contract doc**

Write `skills/agctl-run-test-runbook/reference/report-annotation.md` per the content above. Include a concrete fully-worked example report (3 steps: PASS, FAIL, SKIPPED) mirroring spec §8.

- [ ] **Step 3: Verify conformance**

```
grep -c '\*\*Verdict:\*\*'   skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c '## Setup'            skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c '## Teardown'         skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c '\*\*Run:\*\*'        skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c '\*\*Exit:\*\*'       skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c 'not exercised'       skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c 'error.type'          skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -c '2026-07-04T15:37:00Z' skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
grep -ci 'For reviewers'      skills/agctl-run-test-runbook/reference/report-annotation.md  # → ≥1
```

- [ ] **Step 4: Commit**

```
git add skills/agctl-run-test-runbook/reference/report-annotation.md
git commit -m "feat(skills): add annotated-report format contract for agctl-run-test-runbook"
```

---

## Task 5: Run skill `SKILL.md`

**Files:**
- Create: `skills/agctl-run-test-runbook/SKILL.md`

**Interfaces:**
- **Consumes:** Task 4 (`report-annotation.md`); spec §6.2, D5, D6, D7, D9, D10, D13; the grammar from Task 1. Per D10 it does NOT import the write skill's files — it embeds its own anatomy.
- **Produces:** the execution + report procedure. Self-contained: a run-only install can execute a runbook.

**Required content:**
- **Frontmatter:** mirror `skills/agctl-config/SKILL.md`'s shape. `name: agctl-run-test-runbook`; `description:` one-liner: "Execute an agctl test runbook step-by-step and produce an annotated, auditable results report (`runbook.results.md`) with the verbatim command, exit code, and raw result per step."
- **Self-contained runbook anatomy (D10):** a section listing exactly the fields the run skill parses — `Goal`, `Preconditions`, `Fixtures` (Seed data / Mocks / Heartbeat), `Steps` (each `Command`, optional `Capture`, `Expected`), `Cleanup`. State the grammar: `Capture: VAR=<envelope-path>` (runbook-scoped, string-default, `$VAR`); `Expected:` envelope-path:literal pairs ANDed, or `exit 0`. This must match Task 1 verbatim.
- **A 7-step procedure** with these exact names (implementer fills prose):
  1. **Validate (pre-execution)** — every step has `Command` + non-empty `Expected`; every `$VAR` resolves to a prior `Capture`; assertion steps use `Expected: exit 0`. On any violation, stop with a validation error before executing anything (no partial runs).
  2. **Setup** — run Preconditions (`agctl check ready`); start fixtures (seed via `db execute`; mock via `mock run > mock.log 2>&1 &` then poll `mock.log` for `started`; heartbeat via `http ping … &`); capture PIDs.
  3. **Execute** — for each step: substitute `$VAR` from prior Captures; run the verbatim command; capture exit code + raw `ok`/`error.type` + a curated excerpt from the envelope.
  4. **Annotate** — write the Actual block beneath each Expected (per Task 4).
  5. **On failure** — exit≠0, `ok:false`, or any Expected mismatch (D9): stop; mark remaining steps **skipped — not exercised (blocked by step N)**.
  6. **Teardown (always, even on failure)** — kill heartbeat PIDs; SIGTERM mock + `wait`; grep `mock.log` for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error` (any → overall verdict FAIL); optional seed reset.
  7. **Emit** — verdict summary at top; write `runbook.results.md` per Task 4.
- **Verdict-tally rule:** tallies count Steps only (pass/fail/skipped); fixtures appear in Setup/Teardown; a fixture/teardown failure flips the overall verdict to FAIL even if all Steps passed.
- **Pointer** to `reference/report-annotation.md`.

- [ ] **Step 1: Define conformance criteria**

Must contain: `name: agctl-run-test-runbook`; an anatomy section mentioning all five parsed field groups (Goal, Preconditions, Fixtures, Steps, Cleanup); all 7 procedure steps (Validate, Setup, Execute, Annotate, On failure, Teardown, Emit); the validation checks (`$VAR`, non-empty Expected); the teardown grep events (all four); `not exercised`; `ok:false`; the pointer to `report-annotation.md`; and the tally rule.

- [ ] **Step 2: Author the SKILL.md**

Write `skills/agctl-run-test-runbook/SKILL.md` per the content above. The anatomy section must repeat the grammar from Task 1 verbatim (D10 independence + consistency).

- [ ] **Step 3: Verify conformance**

```
grep -c '^name: agctl-run-test-runbook' skills/agctl-run-test-runbook/SKILL.md  # → 1
grep -c 'Validate'   skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'Setup'      skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'Execute'    skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'Annotate'   skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'Teardown'   skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'Emit'       skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c '\$VAR'      skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'http.unmatched' skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'kafka.error'    skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'not exercised'  skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'ok:false'       skills/agctl-run-test-runbook/SKILL.md  # → ≥1
grep -c 'report-annotation.md' skills/agctl-run-test-runbook/SKILL.md  # → ≥1
```

- [ ] **Step 4: Commit**

```
git add skills/agctl-run-test-runbook/SKILL.md
git commit -m "feat(skills): add agctl-run-test-runbook SKILL.md with self-contained anatomy"
```

---

## Task 6: End-to-end validation + housekeeping

**Files:**
- Modify: `.gitignore` (add `*.results.md`)
- Transient (not committed): a sample runbook + its results, authored under `/tmp` (or a gitignored path) during validation.

**Interfaces:**
- **Consumes:** both complete skills (Tasks 1–5); the repo's `agctl.yaml` + `mocks:` config + the mock server; spec §10.
- **Produces:** validation evidence (not committed); the `.gitignore` entry.

- [ ] **Step 1: Housekeeping — gitignore results**

Add `*.results.md` to `.gitignore` (so dry-run reports and any consumer results are not committed accidentally). Run `git check-ignore -v runbook.results.md` (create an empty file of that name first if needed) → it must print a matching `.gitignore:N:*.results.md` line.

- [ ] **Step 2: Command cross-check (automatable)**

Extract every agctl command reference across both skills and confirm each is a real DESIGN §3 surface with valid flags:
```
grep -rhoE 'agctl (http|kafka|db|check|mock|config|discover)[a-z ]*( --[a-z-]+)*' skills/agctl-write-test-runbook skills/agctl-run-test-runbook | sort -u
```
Manually verify each printed `agctl <group> <sub>` against DESIGN §3, and that every `--flag` is valid for that command. **Valid subcommands:** `http call|request|ping`, `kafka produce|consume|assert`, `db query|assert|execute`, `check ready`, `mock run`, `discover`. Any reference outside this set is a defect → fix the skill file. Expected: zero invalid references.

- [ ] **Step 3: Write-skill dry-run — author a sample runbook**

Using the authored write skill, produce a sample runbook targeting a mock-driven goal the repo can satisfy (DESIGN §3.5 mock is within-transport only — no HTTP→Kafka cross-trigger). Example goal: `kafka produce` to a mock reactor's input topic → reactor fires its reaction → `kafka assert` matches the reaction; and/or `http call` against a mock HTTP stub → assert the stubbed response. Save to `/tmp/sample-runbook.md`.

- [ ] **Step 4: Verify the sample runbook structurally**

The sample must contain all 5 sections, at least one `**Capture:**`, `**Expected:**` in envelope-path:literal form (or `exit 0` for asserts), and at least one step using a `$VAR` from a prior Capture. Re-run the Task 1 greps against `/tmp/sample-runbook.md` (sections + fields) — all must match.

- [ ] **Step 5: Run-skill dry-run — execute the sample**

Under `AGCTL_TEST_LIVE=1` (testcontainers Postgres + Kafka + the local mock, ARCHITECTURE §12), use the run skill to execute `/tmp/sample-runbook.md` and produce `/tmp/sample-runbook.results.md`. Run: `AGCTL_TEST_LIVE=1 <invoke run skill against /tmp/sample-runbook.md>`.

- [ ] **Step 6: Verify the results report**

`/tmp/sample-runbook.results.md` must contain: a `**Verdict:**` line; `## Setup` and `## Teardown` blocks; every executed step showing the spine (`**Run:**`, `**Exit:**`, `**ok:**`); the captured `$VAR` resolved in a later step's Run; ISO-8601 Z timestamp. Then introduce a deliberate failure (e.g. wrong `--equals` value in one step) and re-run: the failing step must render `— FAIL` with `error.type`, and all subsequent steps `— SKIPPED (not exercised)`, with the verdict showing `1 failed` + the skip count.
```
grep -c '\*\*Verdict:\*\*' /tmp/sample-runbook.results.md   # → 1
grep -c '## Setup'        /tmp/sample-runbook.results.md    # → 1
grep -c '## Teardown'     /tmp/sample-runbook.results.md    # → 1
grep -c '\*\*Exit:\*\*'   /tmp/sample-runbook.results.md    # → ≥1
grep -c '\*\*ok:\*\*'     /tmp/sample-runbook.results.md    # → ≥1
grep -c 'not exercised'   /tmp/sample-runbook.results.md    # → ≥1 (in the deliberate-failure run)
```

- [ ] **Step 7: Commit housekeeping + run docs-watcher**

```
git add .gitignore
git commit -m "chore: gitignore *.results.md runbook reports"
```
Then invoke the `docs-watcher` subagent per CLAUDE.md "Docs Sync" to confirm DESIGN.md / ARCHITECTURE.md are no-ops (CLI contract unchanged) and the `skills/` tree growth is noted. Expected: docs-watcher reports no DESIGN/ARCHITECTURE change needed.

---

## Self-Review (run after writing, before handoff)

- **Code scan:** no implementation logic / method bodies / test code — only structure, contracts, field names, and conformance greps. (Holds: this is a markdown plan.)
- **Self-containment:** each task restates the exact contract it builds (field names, grammar, required literals) — a zero-context implementer can author the file from the task alone; pointers to the spec are for cross-check, not required reading.
- **Spec coverage:** D1 (skill not CLI) ✓ Global Constraints; D2/D3 (split, names) ✓ filenames; D4 (adaptive template) ✓ Task 1; D5 (annotated report) ✓ Task 4; D6 (spine + reviewer note) ✓ Tasks 4/5; D7 (stop + not-exercised) ✓ Tasks 4/5; D8 (discover-grounded) ✓ Task 3; D9 (Expected grammar) ✓ Tasks 1/3/5; D10 (anatomy split) ✓ Task 5; D11 (gitignore) ✓ Task 6; D12 (Capture) ✓ Tasks 1/3/5; D13 (background lifecycle) ✓ Tasks 2/5. §10 validation ✓ Task 6. No spec section untasked.
- **Placeholder scan:** each step states exact content + expected grep result; no "TBD"/"add error handling".
- **Type/Name consistency:** field names (`**Command:**`, `**Capture:**`, `**Expected:**`, `**Run:**`, `**Actual:**`, `**Exit:**`) and section headings (`Goal`/`Preconditions`/`Fixtures`/`Steps`/`Cleanup`) are identical across Tasks 1, 4, 5. The four mock failure events (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`) are identical across Tasks 2 and 5.
