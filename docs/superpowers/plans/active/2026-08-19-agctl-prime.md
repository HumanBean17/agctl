# `agctl prime` — Agent Knowledge Channel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the four out-of-tree skills with a thin router skill stub plus `agctl prime` — wheel-hosted, budget-tested markdown the installed binary emits on demand — and install the stub via `agctl config init`.

**Architecture:** All agent-facing content becomes data files under `agctl/data/prime/` (plus the stub at `agctl/data/skills/agctl/SKILL.md`), read through a new content-access module `agctl/prime_content.py` (importlib.resources, CRLF-normalized — the `_load_sample` precedent). A new config-free top-level command `agctl prime` renders core/topics/hook output; `agctl config init` gains stub installation with refuse-to-clobber idempotence. The repo's `skills/` directory is deleted at the end; its text is the *source material* the topics compress.

**Tech Stack:** Python ≥3.11, Click 8 (existing), pytest (existing), importlib.resources (existing pattern), no new dependencies.

**Spec:** `docs/superpowers/specs/active/2026-08-19-agctl-prime-skills-replacement-design.md`

## Global Constraints

- **Additive only:** no behavioral change to existing commands except `config init`, whose existing result keys (`path`, `created`, `bytes`) and refusal semantics stay intact; new keys are added alongside.
- **Budgets, enforced by tests:** rendered default `prime` output ≤ 5,000 chars; hook pointer (`additionalContext` text) ≤ 800 chars; every topic file ≤ 6,000 chars and > 200 chars.
- **prime never emits the JSON envelope** — raw markdown (or the hook JSON) on stdout, exit 0 on success, exit 2 (Click `UsageError`) on bad usage only. prime never exits 1.
- **Topic registry (final, 14 entries, this order):** `gotchas, mock, listen, grpc, config, config-http, config-kafka, config-db, config-db-write, config-mocks, config-logs, config-init, runbook-write, runbook-run`.
- **Version token:** content files use the literal token `{version}`; rendering replaces it with `agctl.__version__` via `.replace("{version}", ...)` (NOT `.format()` — JSON braces elsewhere in the text would break it).
- **LF normalization:** every resource read applies the same CRLF→LF collapse as `agctl/commands/config_commands.py::_load_sample` (Windows-checkout safety).
- **Hook envelope (verified against Claude Code docs):** `{"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": "<pointer>"}}`, printed as one JSON line, exit 0.
- **Stub contract:** `agctl/data/skills/agctl/SKILL.md` ≤ 20 lines; frontmatter `name: agctl` + one-line description; body carries zero domain content beyond the envelope/exit-code one-liner and the pointer to `agctl prime`.
- **Claude Code only**; no MCP detection, no override files, no other agents.
- Commits: conventional-commit style (`feat:`, `test:`, `docs:`), one per task.

## File Structure (who owns what)

- `agctl/prime_content.py` (new) — content access only: resource loading, `TOPIC_ORDER`, rendering, stub text, `HOOK_SETTINGS_SNIPPET`. No Click.
- `agctl/commands/prime_commands.py` (new) — the Click command; thin presentation over `prime_content`.
- `agctl/cli.py` — register `prime` top-level (beside `discover` / `gen_group`).
- `agctl/commands/config_commands.py` — extend `config_init` (stub install).
- `agctl/data/prime/` — `core.md`, `hook.md`, 14 topic files.
- `agctl/data/skills/agctl/SKILL.md` — the packaged stub.
- `tests/unit/test_prime_content.py`, `tests/unit/test_prime_commands.py` (new); `tests/unit/test_cli.py` (init tests live here already).
- README.md, CLAUDE.md, `skills/` (deleted) — final task.

---

### Task 1: `prime_content` module + `core.md`

**Files:**
- Create: `agctl/prime_content.py`
- Create: `agctl/data/prime/core.md`
- Test: `tests/unit/test_prime_content.py`

**Interfaces:**
- Consumes: `agctl.__version__` (str, e.g. `"3.0.1.dev5"`, fallback `"0.0.0+unknown"`); the `_load_sample` CRLF-normalization + `importlib.resources.files("agctl")` precedent in `agctl/commands/config_commands.py:380-401`.
- Produces (used by Tasks 2-9):
  - `TOPIC_ORDER: tuple[str, ...]` — starts `()` in this task; content tasks extend it. Order fixed by the Global Constraints registry.
  - `read_resource(*parts: str) -> str` — reads a text resource under the `agctl` package (e.g. `read_resource("data", "prime", "core.md")`), applies CRLF→LF collapse, raises `agctl.errors.ConfigError(f"...not found in the agctl package...", detail={"resource": "/".join(parts)})` on `FileNotFoundError`/`OSError` (mirror `_load_sample`).
  - `_render(text: str) -> str` — replaces the `{version}` token with `agctl.__version__`.
  - `render_core() -> str` — `_render(read_resource("data", "prime", "core.md"))`.
  - `topic_text(name: str) -> str | None` — returns the raw (unrendered — topics contain no `{version}` today, but rendering is harmless; choose raw) topic text for a name in `TOPIC_ORDER`, else `None`. Reads `data/prime/<name>.md`.
  - `render_all() -> str` — `render_core()` + `"\n\n"` + each topic in `TOPIC_ORDER` joined by `"\n\n"`.
  - `hook_pointer() -> str` — `_render(read_resource("data", "prime", "hook.md"))` (file created in Task 8; this task does NOT define it — leave it for Task 8).
  - `stub_text() -> str` — `read_resource("data", "skills", "agctl", "SKILL.md")` (file created in Task 9; function defined there).
  - Registry invariant helpers the tests use: none needed — tests walk the directory via `importlib.resources` themselves.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_prime_content.py`, following the house import style of `tests/unit/test_cli.py`. Tests:
1. `test_core_renders_with_version` — `render_core()` contains the exact substring `f"agctl v{agctl.__version__}"` and starts with a `# ` heading line.
2. `test_core_budget` — `len(render_core()) <= 5000`.
3. `test_registry_no_dangling` — every name in `TOPIC_ORDER` resolves: `topic_text(name)` is not `None` for each (vacuously true while `TOPIC_ORDER == ()`).
4. `test_registry_no_orphans` — listing `data/prime/` via `importlib.resources.files("agctl").joinpath("data", "prime").iterdir()` (files only): the set of stems minus `{"core", "hook"}` equals `set(TOPIC_ORDER)` (vacuous now).
5. `test_registry_indexed_in_core` — `render_core()` contains every name in `TOPIC_ORDER` (vacuous now).
6. `test_read_resource_missing_raises_config_error` — `read_resource("data", "prime", "nope.md")` raises `ConfigError` whose message contains "not found in the agctl package".

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'agctl.prime_content'` (tests 1-2 fail on import; the file-read tests fail on missing core.md once the import exists).

- [ ] **Step 3: Write the module and `core.md`**

Module per the Produces contract: docstring explaining it is the prime content channel (config-free, wheel-hosted), `__all__`, `read_resource`, `_render`, `render_core`, `topic_text`, `render_all` (works with empty registry), `TOPIC_ORDER: tuple[str, ...] = ()`. `stub_text`/`hook_pointer` are added by later tasks — do not create them yet.

Author `agctl/data/prime/core.md` to final content **except** the topic-index entries (three later tasks append their entries under the index heading). Structure (≈4,000 chars target):
- First line: `# agctl v{version} — agent manual` then one line: *this output matches the installed binary; `agctl <cmd> --help` is the authoritative flag spec.*
- **What agctl is** (2-3 lines): tests a *running* system over HTTP, Kafka, DB, gRPC, and logs; alias `agt`; every invocation prints one JSON object on stdout (`{ok, command, result, error, duration_ms}`) and exits deterministically; parse stdout only — stderr is diagnostics.
- **Exit codes** (3 bullets): `0` success; `1` assertion failed — the *system under test* is wrong; `2` tool/config/env error — *your command* is wrong. Read `ok` first; on `false` read `error.type`.
- **Intent → command** table, condensed from `skills/agctl/SKILL.md:39-63` (≈15 rows: discover / http call / http request / kafka assert / kafka consume / kafka produce / kafka listen / db execute / db assert / db query / db schema / grpc call / mock run+start / logs query / check ready / config validate+migrate). Trim the prose footnotes to the two load-bearing lines: global `--config`/`--overlay`/`--env-file`; `--timeout` is NOT global and `kafka assert --timeout` is required.
- **Top-5 gotchas** (numbered, one line each; full list lives in `--topic gotchas`): ① a 4xx/5xx response is `ok:true` unless you assert (`--status`/`--contains`/`--match`/`--jq-path` flip it to exit 1); ② `--match` is envelope-rooted — HTTP `.body.x`, Kafka `.value.x` (not payload-rooted); ③ Kafka reads are windowed (`now - --lookback`), so send-then-assert is reliable; narrow busy topics with `--match`; ④ `ConnectionError` is exit 2 — run `agctl check ready --all` before blaming an assertion; ⑤ streaming commands (`http ping`, `mock run`, `logs tail`, `grpc` server-stream/bidi, `kafka listen run`) emit one object *per line* + `summary` — background with `&`, `kill` when done.
- **Depth on demand** index section: heading `## Depth: agctl prime --topic <name>` with a one-line purpose per topic; entries are appended by Tasks 3, 5, 6 (runtime: gotchas/mock/listen/grpc; config: config + config-http/kafka/db/db-write/mocks/logs/init; runbooks: runbook-write/runbook-run).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: PASS (all 6).

- [ ] **Step 5: Commit**

Run: `git add agctl/prime_content.py agctl/data/prime/core.md tests/unit/test_prime_content.py`
Run: `git commit -m "feat(prime): content module + core manual (wheel-hosted, budget-tested)"`

---

### Task 2: `agctl prime` command — default output

**Files:**
- Create: `agctl/commands/prime_commands.py`
- Modify: `agctl/cli.py` (import + `cli.add_command(prime)` beside the `gen_group` registration, ~line 236)
- Test: `tests/unit/test_prime_commands.py`

**Interfaces:**
- Consumes: `prime_content.render_core()` (Task 1).
- Produces: Click command object `prime` (name `prime`, no `--config` interaction — config-free like `gen`; ignores the root group's context options). In this task it takes NO flags yet; `--topic` (Task 4), `--all` (Task 7), `--hook-json` ( Task 8) arrive later. Invocation `agctl prime` prints `render_core()` to stdout via `click.echo` and exits 0.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_prime_commands.py` using `click.testing.CliRunner` (house pattern — see existing command tests). Invoke through the root `agctl.cli` group so registration is proven:
1. `test_prime_default_outputs_core` — `cli` invoked with `["prime"]` in an isolated empty dir (runner `isolated_filesystem()` — proves config-free: no `agctl.yaml` present): `result.exit_code == 0`, `result.output == render_core() + "\n"` (or `.strip()` comparison — pin one), `result.stderr == ""`.
2. `test_prime_registered_on_root_help` — `["--help"]` output lists `prime`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_commands.py -v`
Expected: FAIL — `No such command 'prime'` (exit 2) from the runner.

- [ ] **Step 3: Write the command + registration**

`prime_commands.py`: module docstring (config-free documentation command; exempt from the JSON envelope like `--help`; never exits 1), `__all__ = ["prime"]`, `@click.command("prime")` whose body echoes `render_core()`. Register in `cli.py` with a comment mirroring the `gen_group` one.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_commands.py tests/unit/test_cli.py -v`
Expected: PASS (new tests + no regression in existing CLI tests).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/prime_commands.py agctl/cli.py tests/unit/test_prime_commands.py`
Run: `git commit -m "feat(prime): top-level command emitting the core manual"`

---

### Task 3: Runtime topics — `gotchas`, `mock`, `listen`, `grpc`

**Files:**
- Create: `agctl/data/prime/gotchas.md`, `mock.md`, `listen.md`, `grpc.md`
- Modify: `agctl/prime_content.py` (`TOPIC_ORDER` += the 4 names, in registry order)
- Modify: `agctl/data/prime/core.md` (append the 4 index entries under the Depth section)
- Test: `tests/unit/test_prime_content.py` (extend)

**Interfaces:**
- Consumes: Task 1's `read_resource`/`TOPIC_ORDER` machinery; Task 1 tests 3-5 (vacuous until now) become live.
- Produces: four topic files, each: first line `# agctl — <topic>`, 201-6,000 chars, LF. Content mapping (source text lives in `skills/agctl/SKILL.md` — compress, don't copy verbatim; drop anything `--help` already says):
  - `gotchas.md` ← `skills/agctl/SKILL.md:76-176` (all 16 gotchas, keep the numbering and one-line essence of each; collapse multi-line code samples to inline fragments) **plus** `:178-202` (db-schema authoring: quote mixed-case/reserved identifiers; omit generated columns from INSERT) and `:319-343` recipes — keep the 3 highest-value recipes (send→assert, E2E HTTP→Kafka→DB threading, idempotent seed) as compact code blocks.
  - `mock.md` ← `:204-251`: three engines + SUT-facing wiring, daemon mode preferred (`mock start` → `mock stop`), strict `mock stop` failure rule, `--all` array verdicts, state under `.agctl/`, the four-step `mock run` background lifecycle (redirect log, poll `started`, SIGTERM+wait never SIGKILL, grep failure events), Windows daemon caveat.
  - `listen.md` ← `:253-317`: the three false-negative cases, start-before-trigger/OFFSET_END, disk capture + byte valve, assert-then-results-then-stop protocol (results BEFORE stop — expectations silently dropped otherwise), selectors, `--pattern` vs `--topic`, fatality rules, Windows fallback.
  - `grpc.md` ← `:345-392`: `pip install 'agctl[grpc]'`, template vs free-form, four call types, NDJSON stdin/stdout model, status-as-result semantics, `grpc:` config block, reflection-first fallback, plaintext-by-default, streaming exceptions.
- `TOPIC_ORDER` after this task: `("gotchas", "mock", "listen", "grpc")`.

- [ ] **Step 1: Write the failing tests**

Extend `tests/unit/test_prime_content.py`:
1. `test_topic_budgets_runtime` — for each of the 4 names: `t = topic_text(name)`; assert `t is not None`, `200 < len(t) <= 6000`, and `t.startswith("# agctl — ")`.
2. Tasks 1's tests 3-5 now assert real values automatically — run them to confirm they fail *before* the files exist (that is this step's red run): dangling/orphan/index tests fail because `TOPIC_ORDER` names files that don't exist yet.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: FAIL — registry dangling + budget tests (files missing).

- [ ] **Step 3: Author the four topics + registry/index updates**

Write the four files per the mapping above; update `TOPIC_ORDER` and `core.md`'s index (one line each, e.g. `mock — impersonate a dependency: engines, daemon vs foreground, failure rules`). Keep `render_core()` ≤ 5,000 chars after the additions (test 1 Task 1 enforces).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: PASS (all, including the now-live registry tests).

- [ ] **Step 5: Commit**

Run: `git add agctl/data/prime/ agctl/prime_content.py tests/unit/test_prime_content.py`
Run: `git commit -m "feat(prime): runtime topics — gotchas, mock, listen, grpc"`

---

### Task 4: `--topic` flag

**Files:**
- Modify: `agctl/commands/prime_commands.py`
- Test: `tests/unit/test_prime_commands.py` (extend)

**Interfaces:**
- Consumes: `prime_content.topic_text(name) -> str | None`, `prime_content.TOPIC_ORDER` (Tasks 1, 3).
- Produces: `--topic <name>` (Click `multiple=True`, so repeatable) on `prime`. Behavior contract:
  - `agctl prime --topic mock` → prints `topic_text("mock")` verbatim, exit 0.
  - `--topic a --topic b` → topics concatenated in **argument order**, joined by a blank line.
  - Unknown name (anything not in `TOPIC_ORDER`, including empty string) → `raise click.UsageError(f"Unknown topic '{name}'. Valid topics: {', '.join(TOPIC_ORDER)}")` → exit 2, message on stderr, stdout empty.
  - `--topic` without `--hook-json`/`--all` interplay: those flags don't exist yet; exclusivity is added in Task 8 alongside `--hook-json`.

- [ ] **Step 1: Write the failing tests**

1. `test_prime_topic_single` — `["prime", "--topic", "mock"]`: exit 0, output == `topic_text("mock")` + newline, stdout only.
2. `test_prime_topic_repeatable_argument_order` — `["prime", "--topic", "grpc", "--topic", "mock"]`: output is grpc-then-mock (argument order, NOT registry order).
3. `test_prime_topic_unknown_exit2` — `["prime", "--topic", "bogus"]`: exit 2, stderr contains `Unknown topic 'bogus'` and lists every `TOPIC_ORDER` member, stdout empty.
4. `test_prime_no_args_still_core` — default invocation unchanged (regression guard).

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_commands.py -v`
Expected: FAIL — `No such option: --topic` (exit 2, but with different stderr than the test expects).

- [ ] **Step 3: Implement the flag**

Add the `--topic` option (`multiple=True`); on empty tuple fall through to core output. Resolve each name via `topic_text`; `None` → `UsageError` as pinned. Output via `click.echo("\n\n".join(texts))`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_commands.py -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/prime_commands.py tests/unit/test_prime_commands.py`
Run: `git commit -m "feat(prime): --topic — on-demand depth, repeatable, verbatim"`

---

### Task 5: Config topics — 7 files

**Files:**
- Create: `agctl/data/prime/config.md`, `config-http.md`, `config-kafka.md`, `config-db.md`, `config-db-write.md`, `config-mocks.md`, `config-logs.md`, `config-init.md`
- Modify: `agctl/prime_content.py` (`TOPIC_ORDER` += `config, config-http, config-kafka, config-db, config-db-write, config-mocks, config-logs, config-init` — appended after `grpc`, registry order)
- Modify: `agctl/data/prime/core.md` (8 index entries)
- Test: `tests/unit/test_prime_content.py` (extend)

**Interfaces:**
- Consumes: Task 1 machinery; Task 3's pattern (files + registry + index in lockstep).
- Produces: 8 topic files (same header/char rules). Mapping from `skills/agctl-config/`:
  - `config.md` ← `skills/agctl-config/SKILL.md:1-158` — mode table (artifact → section), config discovery order (explicit path → `AGCTL_CONFIG` → walk up), the 7-point authoring contract (three placeholder syntaxes + the mocks `{name}`-means-capture exception, cross-references must resolve, kebab-case keys vs snake_case params, `description` effectively required, idempotent updates, secrets→env + `.env.example`, clarify-don't-guess), mandatory close-out (`config validate` + `discover --name`), sidecar/overlay verification variant, and the structural checklist (fallback when agctl absent) — keep as a compact bullet checklist.
  - `config-http.md` ← `reference/http-template.md` (86 lines): extraction steps from route/controller/OpenAPI + stack snippets, compressed.
  - `config-kafka.md` ← `reference/kafka-pattern.md` (96 lines).
  - `config-db.md` ← `reference/db-template.md` (68 lines).
  - `config-db-write.md` ← `reference/db-write-template.md` (108 lines): two-gate rule, `writable: true`, explicit target, no idempotency → `ON CONFLICT`.
  - `config-mocks.md` ← `reference/mocks.md` (451 lines — heaviest compression; ≤6,000 chars hard): stub/reactor authoring, `{name}` capture-from-trigger, cross-transport effects pointer, failure events.
  - `config-logs.md` ← `reference/logs-sources.md` (127 lines): `logs.sources.<name>` blocks, file + loki types.
  - `config-init.md` ← `reference/init-config.md` (78 lines): repo scan order, env-var reuse strategy, `.env.example` authoring.
- `TOPIC_ORDER` after this task: 12 entries.

- [ ] **Step 1: Write the failing tests**

`test_topic_budgets_config` — same assertions as Task 3's budget test over the 8 new names (each `> 200`, `≤ 6000`, `# agctl — ` header). Registry tests (dangling/orphan/index) go red automatically until files land.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: FAIL — new budget test + registry tests (files missing).

- [ ] **Step 3: Author the eight topics + registry/index updates**

Per the mapping; update `TOPIC_ORDER` and `core.md` index (keep `render_core()` ≤ 5,000 — one line per entry).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/data/prime/ agctl/prime_content.py tests/unit/test_prime_content.py`
Run: `git commit -m "feat(prime): config authoring topics (contract + 7 per-mode references)"`

---

### Task 6: Runbook topics — 2 files

**Files:**
- Create: `agctl/data/prime/runbook-write.md`, `runbook-run.md`
- Modify: `agctl/prime_content.py` (`TOPIC_ORDER` += the 2, registry complete at 14)
- Modify: `agctl/data/prime/core.md` (2 index entries)
- Test: `tests/unit/test_prime_content.py` (extend)

**Interfaces:**
- Consumes: Task 1 machinery; Task 3 pattern.
- Produces:
  - `runbook-write.md` ← `skills/agctl-write-test-runbook/SKILL.md` (169 lines: ingest scaling one-line-vs-spec, one-runbook-per-scenario, runbook anatomy, grounding in real templates via `discover`, clarify/decompose/self-review steps, **stop after emitting — never execute**) + its 3 references (`runbook-template.md` 71 ln — the markdown skeleton; `fixtures-heartbeat.md` 37 ln; `fixtures-mock.md` 67 ln) folded in as compact sections.
  - `runbook-run.md` ← `skills/agctl-run-test-runbook/SKILL.md` (118 lines: runbook anatomy to parse, setup/preconditions, per-step execution — verbatim command, exit code, envelope capture, `$VAR` substitution, Expected comparison type-aware, teardown/cleanup, never declare done on failed step) + `reference/report-annotation.md` (93 ln — the `runbook.results.md` evidence format: verbatim command + exit code + raw result per step).
- `TOPIC_ORDER` final: all 14 names, Global-Constraints order.

- [ ] **Step 1: Write the failing tests**

`test_topic_budgets_runbook` — same assertions over the 2 new names. Also `test_registry_complete` — `len(TOPIC_ORDER) == 14` and equals the Global-Constraints list exactly (order-sensitive tuple comparison) — guards against future accidental reordering/drops.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_content.py -v`
Expected: FAIL — budget + completeness tests.

- [ ] **Step 3: Author the two topics + final registry/index updates**

Per the mapping; `core.md` index now lists all 14.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_content.py tests/unit/test_prime_commands.py -v`
Expected: PASS (registry tests over the full 14; `--topic` still green).

- [ ] **Step 5: Commit**

Run: `git add agctl/data/prime/ agctl/prime_content.py tests/unit/test_prime_content.py`
Run: `git commit -m "feat(prime): runbook write/run topics — registry complete at 14"`

---

### Task 7: `--all` flag

**Files:**
- Modify: `agctl/commands/prime_commands.py`
- Test: `tests/unit/test_prime_commands.py` (extend)

**Interfaces:**
- Consumes: `prime_content.render_all() -> str` (Task 1; live now that all 14 topics exist) — core + topics in `TOPIC_ORDER`, blank-line joined.
- Produces: `--all` flag on `prime`: `agctl prime --all` prints `render_all()`, exit 0. `--all` + `--topic` → `click.UsageError("--all and --topic are mutually exclusive")` (exit 2; redundant + order-ambiguous, so rejected).

- [ ] **Step 1: Write the failing tests**

1. `test_prime_all_equals_concatenation` — `["prime", "--all"]`: exit 0; output == `render_all()` + newline; and `render_all()` == `render_core()` + blank-line join of all 14 topics in `TOPIC_ORDER` order (assert the relation, not a fixture blob).
2. `test_prime_all_with_topic_rejected` — `["prime", "--all", "--topic", "mock"]`: exit 2, stderr contains "mutually exclusive".

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_commands.py -v`
Expected: FAIL — `No such option: --all`.

- [ ] **Step 3: Implement the flag**

Add `--all` (flag); exclusivity check raises `UsageError` before any output.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_commands.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/commands/prime_commands.py tests/unit/test_prime_commands.py`
Run: `git commit -m "feat(prime): --all — one-shot full manual"`

---

### Task 8: `--hook-json` + pointer content + settings snippet

**Files:**
- Create: `agctl/data/prime/hook.md`
- Modify: `agctl/prime_content.py` (add `hook_pointer()`, `HOOK_SETTINGS_SNIPPET`)
- Modify: `agctl/commands/prime_commands.py` (add `--hook-json`)
- Test: `tests/unit/test_prime_content.py`, `tests/unit/test_prime_commands.py` (extend both)

**Interfaces:**
- Consumes: `_render` (Task 1); the verified envelope shape from Global Constraints.
- Produces:
  - `hook.md` — the pointer, `{version}` token included: names installed version, instructs "run `agctl prime` before driving agctl", lists the topic groups compactly (e.g. `--topic gotchas|mock|listen|grpc|config|runbook-write|runbook-run` + "config-* per-mode"), and the exit-code one-liner. Rendered ≤ 800 chars.
  - `hook_pointer() -> str` — `_render(read_resource("data", "prime", "hook.md"))`.
  - `HOOK_SETTINGS_SNIPPET: str` — module `Final` constant, compact one-line JSON: `{"hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": "agctl prime --hook-json"}]}]}}`.
  - `--hook-json` on `prime`: prints `json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": hook_pointer()}}, separators=(",", ":"))` as a single line, exit 0. Mutually exclusive with `--topic` and `--all` → `UsageError("--hook-json is mutually exclusive with --topic/--all")` (exit 2).

- [ ] **Step 1: Write the failing tests**

`test_prime_content.py`:
1. `test_hook_pointer_budget_and_content` — `hook_pointer()` ≤ 800 chars; contains `f"agctl v{agctl.__version__}"`, `"agctl prime"`, and `"--topic"`.
2. `test_hook_snippet_shape` — `json.loads(HOOK_SETTINGS_SNIPPET)["hooks"]["SessionStart"][0]["hooks"][0]["command"] == "agctl prime --hook-json"`.

`test_prime_commands.py`:
3. `test_prime_hook_json_valid_envelope` — `["prime", "--hook-json"]`: exit 0; `json.loads(result.output)` has `hookSpecificOutput.hookEventName == "SessionStart"`; its `additionalContext == hook_pointer()`; `len(additionalContext) <= 800`.
4. `test_prime_hook_json_exclusive` — `["prime", "--hook-json", "--topic", "mock"]` and `["prime", "--hook-json", "--all"]`: exit 2, stderr contains "mutually exclusive".

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_prime_content.py tests/unit/test_prime_commands.py -v`
Expected: FAIL — `HOOK_SETTINGS_SNIPPET`/`hook_pointer` missing (import error); `--hook-json` unknown option.

- [ ] **Step 3: Implement pointer, snippet, flag**

Per the Produces contract. Note `hook.md` joins `core.md` in the orphan-test's exclusion set `{"core", "hook"}` — already pinned in Task 1's test 4.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_prime_content.py tests/unit/test_prime_commands.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

Run: `git add agctl/data/prime/hook.md agctl/prime_content.py agctl/commands/prime_commands.py tests/`
Run: `git commit -m "feat(prime): --hook-json SessionStart pointer + settings snippet"`

---

### Task 9: Stub + `config init` integration

**Files:**
- Create: `agctl/data/skills/agctl/SKILL.md`
- Modify: `agctl/prime_content.py` (add `stub_text()`)
- Modify: `agctl/commands/config_commands.py` (`config_init`, lines ~404-448)
- Test: `tests/unit/test_cli.py` (extend, beside the existing init tests at ~147-205)

**Interfaces:**
- Consumes: `read_resource` (Task 1); existing `config_init` (`--output/-o`, `--force`, `emit(...)`, `_emit_config_error`, refusal envelope); `HOOK_SETTINGS_SNIPPET` (Task 8).
- Produces:
  - `stub_text() -> str` — `read_resource("data", "skills", "agctl", "SKILL.md")`.
  - The stub file itself (≤ 20 lines, LF): YAML frontmatter `name: agctl` + `description:` one line — "Drive the agctl/agt CLI test harness — run agctl commands against a running system, author or validate agctl.yaml, write or execute agctl test runbooks. Invoke whenever agctl, agt, or agctl.yaml is involved." Body (~6 lines): "Run `agctl prime` before driving agctl — the manual (output envelope, exit codes 0/1/2, intent→command map, gotchas). Depth on demand: `agctl prime --topic <name>` (topics listed in prime output; e.g. mock, listen, gotchas, config, runbook-write, runbook-run). Every agctl command prints one JSON object on stdout and exits 0 ok / 1 assertion failed / 2 bad invocation. `agctl <cmd> --help` is the authoritative flag spec."
  - Extended `config_init(ctx, output, force, skills_only, no_skills)` with new options `--skills-only` (flag) and `--no-skills` (flag). Behavior contract:
    - **Targets:** config dest = existing logic (`--output` or `./agctl.yaml`); stub dest = `Path.cwd() / ".claude" / "skills" / "agctl" / "SKILL.md"` (independent of `--output`; parents created on write).
    - **Default mode (no new flags):** existing config-refusal behavior first (existing dest + no `--force` → today's exit-2 envelope, stub NOT touched, nothing written). Then stub pre-flight: stub exists AND its content != `stub_text()` AND no `--force` → exit-2 envelope (`ok: false`, `error.type "ConfigError"`, message `Refusing to overwrite modified <path> (pass --force to overwrite).`, `result {"path": …, "created": False, "skills_path": str, "skills_status": "refused"}`) **with nothing written** (config not written either — pre-flight precedes all writes). Then write config (as today), then stub if absent-or-identical-or-`--force`.
    - **`--skills-only`:** config write skipped entirely (no refusal on existing config — that's the point of the flag); stub logic as above; `path: None, created: False, bytes: 0`.
    - **`--no-skills`:** stub skipped; default config behavior unchanged.
    - **Idempotence:** existing stub byte-equal to `stub_text()` → no write, `skills_status "unchanged"`. Fresh write → `"created"`. `--force` over a modified stub → `"overwritten"`. Skipped → `"skipped"` with `skills_path: None`.
    - **Result (additive; existing keys unchanged in default mode):** adds `skills_path: str | None`, `skills_status: "created" | "unchanged" | "overwritten" | "skipped" | "refused"`, and `hook_snippet: str` (= `HOOK_SETTINGS_SNIPPET`, always present on `ok: true`).

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_cli.py` (house `CliRunner` + `tmp_path`/monkeypatch `Path.cwd` pattern used by the existing init tests):
1. `test_config_init_installs_stub_by_default` — fresh dir: init exits 0; `./agctl.yaml` == `_load_sample()` (existing behavior); `.claude/skills/agctl/SKILL.md` exists and `== stub_text()`; result JSON `skills_status == "created"`, `hook_snippet == HOOK_SETTINGS_SNIPPET`.
2. `test_config_init_idempotent_identical_stub` — run init `--force` twice (or second run `--skills-only`): second run `ok: true`, `skills_status == "unchanged"`, stub content still `stub_text()`.
3. `test_config_init_refuses_modified_stub` — init once, then edit the stub file; run `--skills-only`: exit 2, `error.type == "ConfigError"`, message contains "pass --force", `skills_status == "refused"`, stub content unchanged (consumer edits preserved).
4. `test_config_init_force_overwrites_modified_stub` — same setup + `--force`: exit 0, `skills_status == "overwritten"`, stub == `stub_text()`.
5. `test_config_init_no_skills` — fresh dir + `--no-skills`: exit 0; no `.claude/` tree created; `skills_status == "skipped"`, `skills_path is None`.
6. `test_config_init_skills_only_skips_config` — dir with a pre-existing sentinel `agctl.yaml`: `--skills-only` exits 0; sentinel file byte-unchanged; stub == `stub_text()`; result `path is None`, `created is False`.
7. `test_config_init_existing_config_refusal_untouched` — pre-existing `agctl.yaml`, plain init: today's refusal (exit 2, `created: false`) and stub NOT written (no `.claude/` tree).
8. `test_stub_packaged_shape` — `stub_text()` ≤ 20 lines (`len(text.splitlines()) <= 20`); starts with `---`; contains `name: agctl` and a `description:` line; body contains `agctl prime` and does NOT contain `--match` or `kafka listen` (zero-domain-content guard).

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_cli.py -v -k init`
Expected: FAIL — `--skills-only`/`--no-skills` unknown options; `stub_text` import error; new keys absent.

- [ ] **Step 3: Implement stub file, `stub_text`, and the init extension**

Per the Produces contract. Keep the refusal envelopes in the house `emit(...)` style with `duration_ms=_ms(start)`. Pre-flight order: config refusal → stub refusal → writes (config, then stub).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_cli.py tests/unit/test_prime_content.py -v`
Expected: PASS (new + existing init tests unbroken).

- [ ] **Step 5: Commit**

Run: `git add agctl/data/skills/ agctl/prime_content.py agctl/commands/config_commands.py tests/unit/test_cli.py`
Run: `git commit -m "feat(init): install the agctl skill stub (default-on, idempotent, refuse-to-clobber)"`

---

### Task 10: Migration — delete `skills/`, README, CLAUDE.md, docs sync

**Files:**
- Delete: `skills/` (all 4 skill dirs, 16 files)
- Modify: `README.md` (new "Agent setup" section)
- Modify: `CLAUDE.md` (project structure: drop the `skills/` entry)
- Modify: `docs/DESIGN.md`, `docs/ARCHITECTURE.md` (via `docs-watcher` subagent — project convention)
- Test: `tests/unit/test_prime_content.py` (README drift test)

**Interfaces:**
- Consumes: `HOOK_SETTINGS_SNIPPET`, `stub_text()` (Tasks 8-9); everything before it is merged and green.
- Produces: the repo's end state — no `skills/` dir; README documents the consumer journey; DESIGN.md records the prime channel + envelope exception; ARCHITECTURE.md records `prime_content` / `prime_commands` / data layout / init's extended contract.

- [ ] **Step 1: Write the failing test**

`test_hook_snippet_matches_readme` — drift guard mirroring `test_sample_matches_readme_block` (`tests/unit/test_cli.py:204`): extract the settings-JSON code block from README's "Agent setup" section and assert `json.loads(readme_block) == json.loads(HOOK_SETTINGS_SNIPPET)`. Expected now: FAIL (no such section).

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_prime_content.py::test_hook_snippet_matches_readme -v`
Expected: FAIL — section/block absent.

- [ ] **Step 3: Delete `skills/`, write README section, update CLAUDE.md**

- `git rm -r skills/`.
- README "Agent setup" section (placed near install/usage): `pip install 'agctl[kafka,db,…]'` → `agctl config init` (writes `agctl.yaml` + installs `.claude/skills/agctl/SKILL.md`) → the optional SessionStart hook block (pretty-printed settings.json containing exactly `HOOK_SETTINGS_SNIPPET`'s object) → "migrating from the old skills: delete the four copied skills under `.claude/skills/` (`agctl`, `agctl-config`, `agctl-write-test-runbook`, `agctl-run-test-runbook`), then `agctl config init --skills-only`" → one line on `agctl prime` / `--topic` / `--all`.
- CLAUDE.md: remove the `skills/` bullet from Project Structure.

- [ ] **Step 4: Run the full unit suite**

Run: `python -m pytest tests/unit -v`
Expected: PASS (no test references `skills/` — verified during planning; the drift test now green).

- [ ] **Step 5: Docs sync via `docs-watcher`**

Dispatch the `docs-watcher` subagent (project CLAUDE.md convention) to sync DESIGN.md (prime channel replaces skills; prime/`--help` envelope exception beside the §4 output contract) and ARCHITECTURE.md (`prime_content` + `prime_commands` modules, `data/prime/` + `data/skills/` layout, `config init` extended contract). Review its diff; a correct no-op on either doc is acceptable.

- [ ] **Step 6: Commit**

Run: `git add -A skills/ README.md CLAUDE.md docs/DESIGN.md docs/ARCHITECTURE.md tests/unit/test_prime_content.py`
Run: `git commit -m "feat(prime): retire skills/ — README agent setup, docs sync (skills → prime)"`

---

## Self-Review (recorded after writing)

1. **Code scan:** No method bodies, algorithms, or test code; the click `UsageError` strings, result-key shapes, and JSON envelopes are contracts, not implementations. The `{version}` `.replace` note is a contract rule (why not `.format()`), kept.
2. **Self-containment:** Every task restates its files, exact signatures (`read_resource(*parts)`, `topic_text -> str | None`, `HOOK_SETTINGS_SNIPPET`, result keys + status enum), and expected test outcomes. Task 9's init contract is complete without the spec.
3. **Spec coverage:** §6 command contract → Tasks 1-4, 7, 8; §7 data model → Tasks 1, 3, 5, 6, 8, 9 (stub); §8 init → Task 9; §10 testing → budget/registry/hook/idempotence tests distributed per task incl. `test_registry_complete`, `test_stub_packaged_shape`, README drift; §11 docs/migration → Task 10 (+ docs-watcher). Spec's `.env.example` fix and pinned hook envelope were corrected in the spec before planning.
4. **Placeholders:** None — content tasks carry per-topic source mappings (file:lines) and editorial directives rather than "write content".
5. **Type consistency:** `TOPIC_ORDER` tuple ordering identical across Tasks 1/3/5/6 and Global Constraints; `skills_status` enum identical in Task 9's contract and tests; `hook_pointer()` named consistently in Tasks 8-9.
