# Design: `agctl prime` — the agent knowledge channel (skills → wheel-hosted prime)

**Status:** implemented
**Date:** 2026-08-19
**Branch:** `feat/agctl-prime`
**Affects:** `agctl/commands/prime_commands.py` (new); `agctl/commands/config_commands.py` (`config init`); `agctl/data/prime/` (new); `agctl/data/skills/agctl/SKILL.md` (new); `skills/` (deleted); README.md; DESIGN.md §4 (output contract exception) + agent-channel sections; ARCHITECTURE.md §3 (module map) + data layout; CLAUDE.md (project structure); `tests/` (prime + init coverage)
**Precedent:** `bd prime` (beads) — CLI-emitted agent context with a `--hook-json` SessionStart envelope; `agctl/data/sample-config.yaml` drift-guard test (packaged-data-is-contract pattern this design generalizes)
**Relation to docs:** Replaces the out-of-tree `skills/` artifacts as the agent-facing knowledge channel. On implementation, DESIGN.md gains the prime channel description and the prime/`--help` envelope exception; ARCHITECTURE.md gains `prime_commands` and the `data/prime/` / `data/skills/` layout; README gains the consumer "Agent setup" section. Synced via `docs-watcher`.

---

## 1. Background & Problem

agctl ships its agent-facing knowledge as four skills in a repo directory (`skills/`), which consumers copy into `.claude/skills/`:

| Skill | Body | References | Role |
|---|---|---|---|
| `agctl` | 392 ln | — | runtime reference (loaded every test session) |
| `agctl-config` | 195 ln | 7 files | config authoring |
| `agctl-write-test-runbook` | 169 ln | 3 files | runbook authoring |
| `agctl-run-test-runbook` | 118 ln | 1 file | runbook execution |

This has three structural costs:

1. **Constant token tax.** Every installed skill's name + description sits in the consumer agent's context for *every* session — including sessions that never touch agctl (~400+ tokens across four descriptions).
2. **Burst tax.** The `agctl` skill body alone is ~5–6k tokens, injected whole whenever agctl is driven, regardless of how much of it the task needs.
3. **Drift.** Skills are not packaged; consumers copy them by hand. A copied skill can silently contradict the installed binary — the skill text itself currently spends tokens warning about exactly this.

The `bd prime` command (beads) demonstrates the alternative: the binary emits its own agent context as markdown on demand, sized to a token budget, wrappable in a SessionStart hook envelope. The knowledge ships *inside* the tool it describes.

## 2. Goals

- **Cut the constant tax:** one thin skill stub (~15 lines, one-line description) replaces four skills; steady-state cost is one short description per session.
- **Cut the burst tax:** default prime output is a lean core (~1.2k tokens); depth arrives per topic on demand.
- **Kill drift:** all agent-facing content ships in the wheel and is emitted by the installed binary; content cannot contradict the binary it came from, and the core output carries the binary's version.
- **One-command setup:** `agctl config init` installs the stub (plus its existing config bootstrap) — no manual copying.
- **Cheap ambient discoverability (optional):** a `--hook-json` pointer (~100–200 tokens) consumers may wire as a SessionStart hook.

## 3. Scope & Design Constraints

- **Claude Code only.** SKILL.md format, `.claude/skills/`, and SessionStart hook envelopes may be assumed.
- **Additive only.** One new command (`prime`), one extended command (`config init`). No behavioral change to existing commands.
- **prime is static.** It reads packaged data files; it needs no config, no extras, no network. It runs anywhere the binary runs.
- **Content is the deliverable.** The four skills' 2,156 lines are re-compressed into `data/prime/` under test-enforced budgets — this is an editorial pass, not a file move.

## 4. Non-Goals

- **`PRIME.md`-style consumer overrides / `--export`** (bd has both) — nothing asks for them here.
- **MCP-mode detection or adaptive output.**
- **Non–Claude-Code agents.**
- **Skill-triggered workflows beyond the stub.** The runbook write/run and config-authoring workflows live on as prime topics, not as skills.

---

## 5. Decisions (recorded with rationale)

1. **One thin router skill over zero skills or four slimmed skills.** Zero skills lose skill-matching discoverability ("when the user asks to test the system…"), relying on a CLAUDE.md line or hook alone. Slimmed skills keep the description tax ×4 and the drift problem. A single stub whose body only points at `prime` keeps discoverability at ~40 tokens and moves all content into the binary. *Rejected:* zero skills; hybrid (prime + workflow skills as-is).
2. **Lean core + `--topic` depth over a full-manual dump.** A ~5k-token default output reproduces today's burst tax under a new name. The core carries what every agctl session needs (envelope, exit codes, intent→command map, the five most failure-prone gotchas); domain depth (mock lifecycle, listen protocol, grpc, authoring, runbooks) is fetched only when the task touches it. `--all` preserves the one-shot path for consumers who want it.
3. **Wheel-hosted data files (`agctl/data/prime/*.md`) as the single source of truth.** The repo `skills/` directory is deleted; markdown topics are authored directly under `data/prime/`, read via `importlib.resources`. One location, no build-time copying, testable like `sample-config.yaml`. *Rejected:* Python-embedded strings (unwritable markdown); build-time `force-include` from a surviving `skills/` dir (two names for one thing, drift between layout and wheel).
4. **Install via `agctl config init`, not a dedicated skills command.** `config init` is already the "bootstrap this repo for agctl" command (config + `.env.example`); the stub belongs to bootstrapping. A dedicated command adds surface for one file copy. `--skills-only` covers existing configs (init otherwise refuses them).
5. **Hook payload is a pointer, not the core.** A SessionStart hook fires every session in the repo — injecting the 1.2k-token core would recreate the constant tax the redesign removes. The pointer (~100–200 tokens) says agctl is installed and names `agctl prime`; the stub skill already provides the same nudge, so the hook is an *alternative*, not a requirement.
6. **init prints the hook snippet, never writes `settings.json`.** A hook alters every future session in that repo; that is the consumer's deliberate choice. The stub, by contrast, is inert until skill-matched, so installing it by default is safe.
7. **Idempotence = refuse to clobber a modified stub.** Identical stub → no-op success. Different stub → refuse with the diff summary; `--force` overwrites. Consumers who edited their stub keep their edits until they explicitly surrender them.
8. **prime is exempt from the JSON envelope, `--help`-class.** Its consumer is the agent reading markdown, not a program parsing results. Success is always exit 0 with raw markdown on stdout; the only failure is usage error (unknown topic, mutually exclusive flags) → exit 2 on stderr listing valid topics. prime never exits 1 (it asserts nothing).
9. **Budgets are enforced in characters, not tokens.** Chars are deterministic in tests; the token figures (~1.2k core, ~200 pointer) are the design intent the char ceilings approximate.
10. **The stub carries zero domain content.** If a fact appears in prime output, it does not go in the stub — the stub is a trigger and a pointer, nothing else. This is what keeps it ~15 lines forever.

## 6. Command Contract — `agctl prime`

Top-level, config-free command (registered beside `discover` / `gen`), module `agctl/commands/prime_commands.py`.

```
agctl prime [--topic <name>]… [--all] [--hook-json]
```

| Invocation | Output | Budget |
|---|---|---|
| `agctl prime` | lean core markdown | ≤ 5,000 chars (~1.2k tok) |
| `--topic <name>` (repeatable) | the named topic(s), concatenated in argument order | each ≤ 6,000 chars |
| `--all` | core + every topic, registry order | — (sum of parts) |
| `--hook-json` | pointer wrapped in the SessionStart hook envelope | ≤ 800 chars (~100–200 tok) |

- **Flag exclusivity:** `--hook-json` is mutually exclusive with `--topic` / `--all` (usage error, exit 2).
- **Unknown topic:** usage error — exit 2, stderr lists the valid topic names.
- **Core content:** version line (`agctl v{version} — this output matches the installed binary`), the one-JSON-envelope-per-invocation contract, exit codes 0/1/2, intent→command map, the five most failure-prone gotchas, the topic index, and the standing rule that `agctl <cmd> --help` is the authoritative flag spec.
- **Hook envelope:** Claude Code SessionStart shape — `{"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": "<pointer>"}}`, exit 0. The pointer states the installed version, instructs the agent to run `agctl prime` before driving agctl, and names `--topic` for depth.

## 7. Data Model — `agctl/data/`

Plain markdown files shipped in the wheel. Each topic's header is `# agctl — <topic>`; the core ends with the topic index (the registry).

| File | Content (compressed from) |
|---|---|
| `prime/core.md` | `agctl` skill §§ intro, exit codes, intent→command map, flag semantics, top-5 gotchas, topic index |
| `prime/gotchas.md` | `agctl` skill — the full 16-gotcha list + `db schema` authoring rules |
| `prime/mock.md` | `agctl` skill § mock (engines, daemon vs foreground, strict failure rule, background lifecycle) |
| `prime/listen.md` | `agctl` skill § kafka listen (protocol, byte valve, selectors, fatality rules) |
| `prime/grpc.md` | `agctl` skill § grpc (call types, NDJSON model, config, gotchas) |
| `prime/config.md` | `agctl-config` skill (authoring contract, placeholder table, cross-ref rules, close-out, structural checklist) |
| `prime/config-http.md`, `config-kafka.md`, `config-db.md`, `config-db-write.md`, `config-mocks.md`, `config-logs.md`, `config-init.md` (7 files) | `agctl-config/reference/*.md` — per-mode extraction rules, 1:1 |
| `prime/runbook-write.md` | `agctl-write-test-runbook` skill + its 3 references |
| `prime/runbook-run.md` | `agctl-run-test-runbook` skill + its 1 reference |
| `skills/agctl/SKILL.md` | the packaged stub (installed by `config init`) |

**Stub contract:** frontmatter `name: agctl` + a one-line description that triggers on testing with agctl, running agctl commands, or authoring `agctl.yaml`/runbooks; body = run `agctl prime` before driving agctl, `--topic` for depth, the envelope one-liner. Nothing else.

## 8. `config init` — Extended Contract

`agctl config init` keeps its current behavior (writes a sample `agctl.yaml`, refuses an existing config) and adds:

- **Default on:** writes `.claude/skills/agctl/SKILL.md` as a byte-identical copy of the packaged stub. `--no-skills` opts out.
- **`--skills-only`:** installs/refreshes only the stub; permitted when `agctl.yaml` already exists (the upgrade path for existing consumers).
- **Idempotence:** existing identical stub → no-op success; existing modified stub → refuse with a message pointing at `--force`; `--force` overwrites.
- **Result envelope unchanged:** one JSON object; `result` lists the paths written and carries the ready-to-paste hook snippet for `.claude/settings.json`:

```json
{"hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": "agctl prime --hook-json"}]}]}}
```

## 9. Consumer Journey

```
pip install 'agctl[kafka,db,…]'
agctl config init     # agctl.yaml + .claude/skills/agctl/SKILL.md (~15 lines)
                      # result carries the optional SessionStart hook snippet
```

Steady state: one short skill description (~40 tokens) per session; ~1.2k tokens in sessions that drive agctl; topic depth only when a mock/listen/grpc/config/runbook task demands it.

**Migration for existing consumers** (release note): delete the four copied skills; run `agctl config init --skills-only`.

## 10. Testing

- **Registry integrity:** every file in `data/prime/` is indexed in `core.md`, and every indexed topic exists on disk — no orphans, no dangling pointers.
- **Budgets:** core ≤ 5,000 chars; hook pointer ≤ 800; every topic ≤ 6,000; every topic > 200 chars (an empty stub-of-a-topic cannot slip in).
- **`--hook-json`:** output parses as JSON, carries the additionalContext field, within budget.
- **Unknown topic:** exit 2, stderr lists valid topics.
- **`--all`:** output equals core + every topic in registry order; core's rendered version line matches the package version.
- **Flag exclusivity:** `--hook-json` with `--topic`/`--all` → exit 2.
- **`config init`:** stub written byte-identical; re-init over identical stub → no-op success; modified stub → refused; `--force` overwrites; `--skills-only` works alongside an existing config; `--no-skills` skips the write; result lists written paths + hook snippet.

## 11. Repository & Docs Impact

- **Delete `skills/`** — its content is re-compressed into `data/prime/` (editorial pass under the §10 budgets).
- **README.md:** new "Agent setup" section — `pip install` → `agctl config init` → optional hook snippet. (Today the README never mentions the skills at all; this becomes the first real consumer entry point.)
- **DESIGN.md:** prime replaces skills as the agent knowledge channel; the prime/`--help` envelope exception is recorded beside the output contract.
- **ARCHITECTURE.md:** `prime_commands` module; `data/prime/` + `data/skills/` layout; `config init`'s extended contract.
- **CLAUDE.md (this repo):** `skills/` removed from the project-structure map.
- **Release note:** the migration line from §9.
