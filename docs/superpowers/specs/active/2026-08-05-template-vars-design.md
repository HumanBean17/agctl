# Design: Template Variables — runtime value generators (uuid / ts / rand)

**Status:** in_progress
**Date:** 2026-08-05
**Author:** brainstorming session (GitHub issue #55)
**Affects:** `agctl/template_vars.py` (new); `agctl/resolution.py`; `agctl/cli.py`; `agctl/commands/gen_commands.py` (new); `agctl/commands/config_commands.py`; `agctl/commands/{http,kafka,db,grpc,logs,kafka_listen}_commands.py` (free-form arg sites); `skills/agctl-write-test-runbook/`; `skills/agctl-run-test-runbook/`; DESIGN.md §2 / §3 / §10; ARCHITECTURE.md §3 / §5 / §9
**Relation to docs:** Adds runtime value generation to the fill pipeline + a config-free `agctl gen` command; on implementation, DESIGN.md §2 (generators in template bodies), §3 (`agctl gen`), §10 (template-variable validation moves from deferred → done; new deferred items), and ARCHITECTURE.md §3 (module map), §5 (fill pipeline), §9 are synced via `docs-watcher`.

---

## 1. Background & Problem

Issue #55 asks for template variables in runbook commands. The issue is filed
against a premise that **does not match this codebase**: it describes an
in-`agctl` "runbook executor" that runs commands through a shell wrapper blocking
`$(...)`, an "already-existing Capture mechanism" with a `--capture-as` flag, and
a proposed `agctl uuid` command. None of that exists:

- `agctl` has **no shell execution at all** (no `os.system`, no `shell=True`, no
  `subprocess` with a shell) — so there is no `$()` to block and no shell surface
  to inject into.
- The only Capture mechanism is **mock-only** (`mock/capture.py`): it reads a value
  off an *incoming* mock request and substitutes it into the *outgoing* response,
  per match, never persisted. There is no `--capture-as` flag.
- The "runbook executor" is the **`agctl-run-test-runbook` skill** (agent-driven
  prose) operating on markdown runbooks; its cross-step Capture is `VAR=<envelope
  path>` substituted as `$VAR`.

The **real, unmet need** (stripped of the wrong framing): authors need *freshly
generated* values — UUID, timestamp, random token — injected into agctl command
arguments, with the **same** value reusable across several fields of one command,
without `/tmp` env-file hacks and without any command-injection risk. Today the
runbook skill can capture a value that *comes back* from agctl, but it cannot
*generate* one, and no single agctl invocation can reuse a generated value across
its own arguments.

This spec adds runtime value generation as a first-class, injection-safe-by-
construction capability, plus a standalone `agctl gen` command for cross-step
reuse via the runbook's existing `$VAR` Capture.

## 2. Goals

- **Generate fresh values inline** in any agctl string argument or template body
  via `{{uuid}}` / `{{ts}}` / `{{rand}}`, with values that **cannot carry shell
  metacharacters** (the generators' charset invariant).
- **Same token → same value within one invocation**, covering the "one
  `conversation_id` in `--key` and in the message body" case in a single step.
- **Cross-step reuse** of a generated value through a config-free `agctl gen`
  command whose output the runbook skill captures into `$VAR`.
- **Fail loudly** on unknown/typo tokens (`{{uuids}}`) at validate time and at run
  time — the opposite of today's silent `$()` passthrough.
- **One engine, all surfaces**: CLI args, config-defined template bodies, and
  runbook command strings share a single substitution pass.
- **Zero new dependencies** (generators use only the stdlib: `uuid`, `time`,
  `secrets`, `re`).

## 3. Non-Goals

- **No shell execution, no `$()`/backtick handling.** agctl stays shell-free; the
  issue's "shell wrapper" is moot here.
- **No `{{$NAME}}` reference syntax, no `--capture-as`, no `--var`.** Cross-step
  sharing reuses the runbook skill's existing `$VAR` Capture; a second reference
  syntax would duplicate it.
- **No new run-scoped variable store inside agctl.** agctl remains stateless
  (DESIGN §8); the memo that makes a token consistent lives only for one process.
- **No generated-values audit trail, no replay/seed mode, no pluggable custom
  generators, no relative timestamps (`{{now -1h}}`), no `{{env:NAME}}`.** Each is
  filed as a follow-up issue (§16).
- **No change to `{name}` (`--param`) or `${ENV}` (load-time) semantics.** The new
  `{{...}}` syntax is grammatically distinct and does not collide.

---

## 4. Approach

Three decisions confirmed during brainstorming:

1. **CLI-native generators over skill-only.** Generators live in `agctl`
   (`template_vars.py`) and run inside the fill pipeline, so the injection-safety
   guarantee is enforced in code (not prose) and every caller benefits — runbooks,
   ad-hoc agent commands, and humans.
2. **One umbrella command, not scattered top-level groups.** Generators are
   exposed standalone as a single `agctl gen` group (`uuid` / `ts` / `rand`) so
   `agctl --help` gains exactly one line and the "ten focused commands" principle
   (DESIGN §8) is preserved.
3. **Cross-step via existing `$VAR`.** The runbook skill captures `agctl gen`
   output into `$VAR`; no new reference syntax or CLI var flag.

`{{...}}` is applied uniformly at every string-fill site and on free-form string
arguments. Unknown tokens error (fail loud); a global `--no-template-vars` flag is
the escape hatch for payloads that must contain a literal `{{...}}`.

---

## 5. The Generator Language

New double-brace tokens, parsed and produced by `agctl/template_vars.py`.
Grammatically distinct from `{name}` (single brace, `--param`) and `${ENV}`
(load-time): the token regex matches `{{name[:opts]}}` where `name` is
`[A-Za-z_][A-Za-z0-9_]*` and `opts` is `:...\}`. **No dots, no spaces** inside the
braces — so common templating tokens like `{{user.name}}` or `{{ title }}` do not
match and are left literal.

| Token | Produces | Charset (security invariant) |
|---|---|---|
| `{{uuid}}` | RFC 4122 v4, lowercase | `[0-9a-f-]` |
| `{{uuid:N}}` | N **distinct** UUIDs, space-joined | `[0-9a-f- ]` |
| `{{ts}}` | Unix seconds | `[0-9]` |
| `{{ts:ms}}` | Unix milliseconds | `[0-9]` |
| `{{ts:iso}}` | ISO-8601 UTC, seconds precision | `[0-9T:-Z]` |
| `{{rand:N}}` | N lowercase hex chars | `[0-9a-f]` |

**Semantics:**

1. Generators run **before** `{name}` fill; the two never collide (different
   braces).
2. The **same token text** resolves to **one value within one process** (memo keyed
   by token string). So `{{uuid}}` twice in one command → one UUID; `{{uuid}}` and
   `{{uuid:2}}` are different tokens → independent values.
3. A token is recognized only in the exact `{{name[:opts]}}` shape above. Anything
   else (dots, spaces, unbalanced braces) is left untouched as a literal string.

The charset invariant is the load-bearing security property: a generated value can
never contain `;`, `&`, `|`, `$`, backtick, quotes, or whitespace (except the
single space joining `{{uuid:N}}`), so even if such a value reached a shell it
could not change the command. agctl has no shell regardless; the invariant is
defense in depth and a contract for any future consumer.

---

## 6. Substitution Pipeline & `--no-template-vars`

The `substitute_generators(value, memo)` recursive walker (strings / dicts /
lists) lives in `agctl/template_vars.py` (§7) and is invoked from the fill
pipeline in `agctl/resolution.py`. It is called:

- at every existing `{name}` / `:name` fill site (HTTP path/body, gRPC
  message/metadata, Kafka pattern bodies, logs `--match`, listen-assert
  predicates), and
- on free-form string arguments that are not `{name}` fill sites (`--key`,
  `--message`, `--body`, `--sql`, `--metadata`).

Net effect: `{{...}}` works the same in a CLI arg, a config-defined template body,
and a runbook command string — one engine, all surfaces.

**Ordering:** generator pass first (produces safe strings), then the existing
`{name}` fill. A generated value substituted into a position later read by
`{name}` fill is still treated as a literal (no recursive expansion) — generators
never emit braces.

**`--no-template-vars`** (new global flag, sibling of `--config` / `--overlay` /
`--env-file`): when set, every substitution site leaves `{{...}}` literal and
performs **no** generation or unknown-token checking. It is the escape hatch for
payloads that must carry a literal `{{uuid}}` (e.g. testing a mustache/Jinja
endpoint). The `agctl gen` command ignores the flag — it *is* generation.

---

## 7. Component — `agctl/template_vars.py` (new)

Stdlib-only module. Owns the generator registry, the substitution walker, and the
validation helper. Imported by `resolution.py`, `gen_commands.py`, and the config
validator path. Public contract:

| Name | Responsibility |
|---|---|
| Generator registry | A name → generator mapping. Built-ins: `uuid`, `ts`, `rand`. Each generator is a callable from parsed options to a `str` (single value) or `list[str]` (the `uuid:N` multi case), emitting only the §5 charset. A registry (not bare functions) so the deferred pluggable-generator entry point (§16) slots in without rework. |
| `parse_token(token) -> (name, opts)` | Splits a matched `{{...}}` interior into `(name, opts)`; raises `ConfigError` on a malformed option (e.g. `{{uuid:abc}}`). |
| `substitute_generators(value, memo) -> value'` | Recurses strings/dicts/lists; for each `{{...}}` match, resolves via the registry, memoizing by token text in `memo`; returns the substituted structure. Unknown name → `ConfigError`. Non-matching text is unchanged. |
| `find_unknown_templates(value) -> list[str]` | Walks a config value tree, returning any `{{...}}` token whose name is not in the registry (for `config validate`, §11). Pure scan — no generation. |

The per-invocation `memo` (a `dict[str, str]`) is constructed once per command
execution and threaded into `substitute_generators`; it is what makes the same
token consistent within one process. It is deliberately **not** persisted — agctl
stays stateless.

## 8. Component — `agctl gen` command group (new)

New `agctl/commands/gen_commands.py`. One Click group `gen` with three
`@envelope`-wrapped subcommands, each calling the **same** generator functions as
the inline pass. **Config-free**: `gen` does not call `load_config_or_raise` (it
works with no `agctl.yaml`, like `config init`) and ignores `--no-template-vars`.

```
agctl gen uuid [--count N] [--upper]   →  command "gen.uuid"
agctl gen ts   [--ms | --iso]          →  command "gen.ts"
agctl gen rand [--length N]            →  command "gen.rand"
```

Result shapes (`@envelope` `result` payload):

| Invocation | `result` |
|---|---|
| `gen uuid` | `{"value": "<uuid>"}` |
| `gen uuid --count N` (N ≥ 1) | `{"values": ["<uuid>", …]}` (N distinct) |
| `gen ts` | `{"value": "<unix-seconds>"}` |
| `gen ts --ms` / `gen ts --iso` | `{"value": "<ms>"}` / `{"value": "<ISO-8601 Z>"}` |
| `gen rand --length N` (default 16) | `{"value": "<hex>"}` |

`--upper` upper-cases UUIDs (command-only convenience; the inline `{{uuid}}` is
lowercase to match the issue's contract). Defaults: `--count 1`, `--length 16`.
Bad integers (`--count 0`, `--length abc`) → `ConfigError` (exit 2).

`agctl --help` gains one line (`gen`); `agctl gen --help` lists the three
subcommands. No top-level regression.

## 9. Cross-step Flow (runbook `$VAR` + Capture)

agctl invocations are stateless, so a value generated in step 1 cannot be
remembered for step 3 inside agctl. Cross-step reuse flows through the runbook
skill's existing Capture, fed by `agctl gen`:

```
### 1. Generate two IDs
- **Command:** `agctl gen uuid --count 2`
- **Capture:** `CID=result.values[0]`, `MID=result.values[1]`
- **Expected:** `ok: true`

### 2. Use them
- **Command:** `agctl kafka produce --topic orders --key $CID --message '{"cid":"$CID","mid":"$MID"}'`
- **Expected:** exit 0
```

The skill substitutes `$CID` / `$MID` into the command string before running it
(existing mechanism, unchanged). No `{{$NAME}}`, no `--capture-as`, no `--var`.

For the **single-step, same-value-multiple-fields** case (the common one), no
`gen` step is needed — inline tokens cover it:

```
agctl kafka produce --topic orders \
  --key {{uuid}} \
  --message '{"conversation_id":"{{uuid}}","case_id":"CASE-{{ts}}"}'
```

Both `{{uuid}}` resolve to one UUID within this invocation.

## 10. Runbook Skill Changes

- **`agctl-write-test-runbook`**: document the `{{...}}` inline tokens and the
  `agctl gen` command in the runbook template + authoring guidance; show the
  two patterns above (inline single-step; `gen` + `$VAR` cross-step) as the
  supported ways to get a fresh value — replacing the `bash -c '…$(uuidgen)…'`
  anti-pattern.
- **`agctl-run-test-runbook`**: extend the Validate step's existing
  `$VAR`-defined-before-used check to also reject unknown `{{...}}` tokens in
  command strings (fail before execution, no partial runs).

These are documentation/prose edits to the skills, consistent with how the skills
already describe `$VAR` Capture and sidecar overlays.

---

## 11. Validation

- **`config validate`**: invoke `find_unknown_templates` over the resolved config
  tree (template bodies, SQL, gRPC messages, etc.); any unknown `{{...}}` token is
  an error attributed at its config path (exit 2), alongside the existing
  `collect_*_errors` checks. Catches typos like `{{uuids}}` in a config-defined
  template body at load time. (`{{...}}` in config values is intentional
  generality — a template author may bake a fresh UUID into a reusable body.)
- **Runbook load**: the run skill's Validate step (§10) checks command strings.
- **Runtime**: any unknown token that reaches `substitute_generators` (e.g. a
  free-form CLI arg, not visible to `config validate`) raises `ConfigError` at
  execution time. Defense in depth — fail loud at the earliest layer that sees it.

## 12. Error Mapping & Exit Codes

All paths emit exactly one structured envelope before exit (the `@envelope`
guarantee).

| Condition | Exception | `error.type` | Exit |
|---|---|---|---|
| Unknown `{{...}}` generator (validate or runtime) | `ConfigError` | `ConfigError` | 2 (names the token + valid generators) |
| Malformed option (`{{uuid:abc}}`, `--count 0`) | `ConfigError` | `ConfigError` | 2 |
| `--no-template-vars` set | — (no error; `{{...}}` left literal) | — | n/a |
| `gen` bad integer (`--length abc`) | `ConfigError` | `ConfigError` | 2 |
| `gen` success | — | — | 0 |

---

## 13. Data Flow

```
agctl <cmd> … --key {{uuid}} --message '{"cid":"{{uuid}}"}' [--no-template-vars]
   │
   ▼
ctx.obj["no_template_vars"]                       # global flag
   │
   ▼  (per command execution, one memo constructed)
substitute_generators(arg/memo)  ──►  template_vars registry
   │     • {{uuid}} → memo["{{uuid}}"] = "<uuid>"   (same both times)
   │     • unknown name → ConfigError
   │     • --no-template-vars → skip, leave literal
   ▼
existing {name} / :name fill  ──►  resolution.fill_placeholders / convert_sql_params
   │
   ▼
protocol client (httpx / confluent-kafka / psycopg / grpcio / …)   — no shell
```

For the standalone command:

```
agctl gen uuid --count 2   (no config load; ignores --no-template-vars)
   │
   ▼
gen.uuid _core → registry["uuid"](count=2) → ["<uuid>","<uuid>"]
   │
   ▼
@envelope → {"ok":true,"command":"gen.uuid","result":{"values":[…]}, …}
   │
   ▼  (runbook skill)
Capture: CID=result.values[0], MID=result.values[1]   →   $CID / $MID in later steps
```

## 14. Testing Strategy

### 14.1 Unit — `tests/unit/test_template_vars.py` (new)

- **Generator charsets/formats** — each of `uuid` / `ts` / `ts:ms` / `ts:iso` /
  `rand:N` / `uuid:N` matches its §5 regex (the security property).
- **`substitute_generators`** — recursion into dict/list; multiple distinct tokens
  in one string; non-matching text (`{{user.name}}`, `{{ x }}`, `{{{x}}}`) left
  literal; unknown name → `ConfigError`; `--no-template-vars` passthrough.
- **Memoization** — same token twice in one call → identical value; different
  tokens (`{{uuid}}` vs `{{uuid:2}}`) → independent; fresh memo per invocation.
- **`find_unknown_templates`** — surfaces `{{uuids}}` in a config tree; ignores
  non-matching text.
- **`parse_token`** — name/opts split; malformed option → `ConfigError`.

### 14.2 Unit — `tests/unit/test_gen_commands.py` (new)

- Output shapes per §8 (`value` vs `values`); `--count`/`--upper`/`--length`/`--ms`/
  `--iso`; `@envelope` wrapping and `command` tags; `gen` runs with no config
  (no `agctl.yaml`); `--no-template-vars` ignored; bad integers → `ConfigError`.

### 14.3 Extend `tests/unit/test_resolution.py`

- Generators-then-`{name}` ordering; no collision between `{{uuid}}` and
  `{orderId}`; a generated value is not re-expanded by `{name}` fill.

### 14.4 Integration of substitution into command paths

- One test per affected command group that a `{{...}}` in the relevant free-form
  arg is substituted before the call (e.g. `kafka produce --key {{uuid}}` uses the
  generated value; `http request --body '{"id":"{{uuid}}"}'` substitutes; `db
  query --sql` with `{{ts}}`). These can be unit-level with a mocked client.

### 14.5 The security pin

A dedicated property test asserting **every** built-in generator output matches
`^[0-9a-fA-FT:Z -]+$`. This is the "no shell metacharacter can ever appear"
invariant; it must not regress, and it is the evidence that the feature is
injection-safe by construction.

---

## 15. Backward Compatibility & Docs Sync

- **Backward compatible.** `{name}` (`--param`) and `${ENV}` are unchanged.
  `{{...}}` is opt-in; existing runbooks/configs keep working. The only behavioral
  risk is a payload that legitimately contains a bare-identifier `{{name}}`
  (mustache-style) — now an unknown-generator error; `--no-template-vars` is the
  documented escape. Tokens with dots/spaces (Jinja/mustache paths) do not match
  the regex and are unaffected.
- **No new dependency** — stdlib only.
- **Docs sync (via `docs-watcher`):**
  - DESIGN §2 — generators recognized in template bodies; the `{{...}}` token
    table.
  - DESIGN §3 — the new `agctl gen` group; the `--no-template-vars` global flag.
  - DESIGN §10 — move "Template variable validation" from deferred → done; add
    the new deferred items (§16).
  - ARCHITECTURE §3 — `agctl/template_vars.py` + `agctl/commands/gen_commands.py`
    in the module map.
  - ARCHITECTURE §5 — the generator pass in the fill pipeline.
  - ARCHITECTURE §9 — generators as part of the resolution/substitution layer.

---

## 16. Open Questions / Future Work

Each is filed as a follow-up GitHub issue (per the brainstorming decision to defer
the "maximal" expansions):

- **Generated-values audit trail** — record each `{{...}}` → actual value used in
  `runbook.results.md`, so a non-deterministic run is still auditable.
- **Replay / seed mode** — regenerate deterministically from recorded values for
  reproducible flaky-test diagnosis (DESIGN UC-5).
- **Pluggable generators** — an `agctl.template_vars` entry point for domain
  generators (`{{ulid}}`, `{{phone}}`, `{{iban}}`, …); the registry in §7 is shaped
  for this.
- **Relative timestamps** — `{{now -1h}}`-style for SLA/window tests.
- **`{{env:NAME}}`** — argued for **dropping** as redundant with shell expansion /
  `--param` / `${ENV}`; filed so the decision is recorded and revisitable.

---

## TL;DR

Add **runtime value generators** to agctl: `{{uuid}}` / `{{ts}}` / `{{rand}}`
inline tokens substituted by a new stdlib-only `agctl/template_vars.py` module at
every string-fill site (CLI args, config template bodies, runbook commands),
gated by a global `--no-template-vars` escape hatch. Values are injection-safe by
construction (generators emit only hex/digits/dashes), and the same token resolves
to one value within an invocation. Cross-step reuse flows through a config-free
`agctl gen uuid|ts|rand` command whose output the runbook skill captures into the
existing `$VAR` — no `{{$NAME}}`, no `--capture-as`, no new var flag. Unknown
tokens fail loud at `config validate`, runbook load, and runtime. The audit trail,
replay/seed mode, pluggable generators, relative timestamps, and `{{env:NAME}}`
are filed as follow-up issues.
