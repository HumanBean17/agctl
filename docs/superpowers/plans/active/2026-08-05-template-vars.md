# Template Variables (runtime value generators) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let any agctl command generate fresh, injection-safe values (`{{uuid}}` / `{{ts}}` / `{{rand}}`) inline at every string-fill surface, reuse the same value within one invocation, and generate standalone via a config-free `agctl gen uuid|ts|rand` command for cross-step capture — fail-loud on unknown tokens, gated by a global `--no-template-vars` flag.

**Architecture:** A new stdlib-only `agctl/template_vars.py` owns a generator registry, a recursive `substitute_generators(value, memo)` walker (per-invocation memo → same token = same value), and `find_unknown_templates` for validation. `resolution.py`'s `fill_placeholders` gains an optional generator pre-pass (generators-before-`{name}`). Each command `_core` creates one memo per invocation and threads a `template_vars_enabled` flag from the new `--no-template-vars` global. A new `agctl/commands/gen_commands.py` exposes the generators as a `gen` group (config-free, ignores the flag). `config validate` rejects unknown tokens in config templates. Runbook skills document the tokens + command and validate at load.

**Tech Stack:** Python ≥3.11, stdlib only (`uuid`, `time`, `secrets`, `re`) — no new dependencies. Click (CLI), Pydantic v2 (config, unchanged). pytest for tests.

## Global Constraints

(Copied from the spec; every task implicitly includes these.)

- **Stdlib-only generators.** `uuid` (RFC 4122 v4), `time`, `secrets`, `re`. No new dependency, no new pyproject extra.
- **Charset invariant (load-bearing).** Every generator output matches `^[0-9a-fA-FT:Z -]+$` — no `; & | $ \` " ' < >` and no whitespace except the single space joining `{{uuid:N}}`. Pinned by a property test in Task 1.
- **Exit codes.** `0` success, `1` assertion failure, `2` tool/config/env error (`ConfigError`). Unknown/malformed template token → `ConfigError` exit 2.
- **One-emit contract.** Every command path emits exactly one JSON envelope via `@envelope` (`agctl/command.py`). New `gen.*` commands are `@envelope`-wrapped.
- **agctl style.** Full-word, hyphenated flags (`--no-template-vars`, `--count`, `--length`, `--upper`, `--ms`, `--iso`). No top-level command bloat — generators exposed under one `gen` group (one new `agctl --help` line).
- **Stateless.** The per-invocation memo is never persisted. Cross-step sharing is the runbook skill's existing `$VAR` Capture, fed by `agctl gen` output — no `{{$NAME}}`, no `--capture-as`, no `--var`.
- **Backward compatible.** `{name}` (`--param`) and `${ENV}` (load-time) unchanged. `--no-template-vars` is the escape hatch for literal `{{...}}` payloads.
- **Tests.** New unit tests mirror `tests/unit/test_resolution.py` patterns. Use existing DI seams (e.g. kafka `producer_factory`/`consumer_factory`, http/grpc injectable clients) — do not hit real services in unit tests.

**Spec:** `docs/superpowers/specs/active/2026-08-05-template-vars-design.md` (source of truth for design/contracts).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `agctl/template_vars.py` (new) | Generator registry, built-in generators, `parse_token`, `substitute_generators`, `find_unknown_templates` | 1, 2 |
| `agctl/resolution.py` | `fill_placeholders` gains optional generator pre-pass (memo + enable) | 5 |
| `agctl/commands/gen_commands.py` (new) | `agctl gen uuid\|ts\|rand` — `@envelope`-wrapped, config-free | 3 |
| `agctl/cli.py` | Register `gen` group; add `--no-template-vars` global flag → `ctx.obj["no_template_vars"]` | 3, 4 |
| `agctl/commands/kafka_commands.py` | Thread memo + enable; substitute `--key`/`--message`/`--header` + pattern fill | 6 |
| `agctl/commands/http_commands.py` | Thread memo + enable; substitute template body + `--body` + path; ping | 7 |
| `agctl/commands/grpc_commands.py` | Thread memo + enable; substitute `--message`/`--metadata` + template + assert fills | 8 |
| `agctl/commands/db_commands.py`, `agctl/commands/logs_commands.py`, `agctl/listen/assert_eval.py`, `agctl/commands/kafka_listen_commands.py` | Thread memo + enable; substitute `--sql`, `--match`, listen-assert pattern fill | 9 |
| `agctl/commands/config_commands.py` | `config validate` calls `find_unknown_templates` over the config tree | 10 |
| `skills/agctl-write-test-runbook/SKILL.md`, `.../reference/runbook-template.md` | Document `{{...}}` + `agctl gen`; show inline + cross-step patterns | 11 |
| `skills/agctl-run-test-runbook/SKILL.md` | Validate step rejects unknown `{{...}}` in command strings | 11 |
| `docs/DESIGN.md`, `docs/ARCHITECTURE.md` | Sync via `docs-watcher` (§2/§3/§10 DESIGN; §3/§5/§9 ARCH) | 12 |
| `tests/unit/test_template_vars.py` (new) | Generators, parse_token, substitute_generators, find_unknown_templates, charset pin | 1, 2 |
| `tests/unit/test_gen_commands.py` (new) | gen uuid/ts/rand shapes, flags, config-free, exit codes | 3 |
| `tests/unit/test_resolution.py` (extend) | generator-then-`{name}` ordering; memo sharing | 5 |
| `tests/unit/test_<proto>_template_vars.py` (new, per protocol) | end-to-end substitution + `--no-template-vars` passthrough per command group | 6–9 |
| `tests/unit/test_config_validate.py` (extend or new) | unknown `{{...}}` in a config template → error | 10 |

---

## Task 1: Generator registry + built-in generators + `parse_token`

**Files:**
- Create: `agctl/template_vars.py`
- Test: `tests/unit/test_template_vars.py`

**Interfaces:**
- Consumes: `agctl/errors.py::ConfigError` (raise on malformed option).
- Produces (used by Tasks 2, 3, and 10):
  - `BUILTIN_GENERATORS: dict[str, Callable[[str | None], list[str]]]` — keys `"uuid"`, `"ts"`, `"rand"`. Each generator takes an `opts: str | None` and returns a `list[str]` (always a list; single-value generators return a 1-element list).
  - **`uuid` generator**: `opts=None` → `[one lowercase RFC-4122 v4 UUID]`. `opts="<n>"` (integer ≥1) → `n` distinct lowercase UUIDs. `opts` non-integer / `<1` → `ConfigError`.
  - **`ts` generator**: `opts=None` → `[unix seconds, 10 digits]`. `opts="ms"` → `[unix milliseconds, 13 digits]`. `opts="iso"` → `[ISO-8601 UTC seconds, e.g. 2026-08-05T12:00:00Z]`. Any other `opts` → `ConfigError`.
  - **`rand` generator**: `opts=None` → `[16 lowercase hex chars]`. `opts="<n>"` (integer ≥1) → `[n lowercase hex chars]`. Non-integer/`<1` → `ConfigError`. (Always a 1-element list — `opts` is char length, not count.)
  - `parse_token(interior: str) -> tuple[str, str | None]` — splits a matched token's interior (the text between `{{` and `}}`) on the FIRST `:` only. `"uuid"` → `("uuid", None)`; `"uuid:2"` → `("uuid", "2")`; `"ts:ms"` → `("ts", "ms")`. Everything after the first colon is `opts` verbatim (the generator validates it).
  - Every generator's output strings MUST match the Global Constraints charset regex.

- [ ] **Step 1: Write failing tests for the three generators' output format + charset**

  In `tests/unit/test_template_vars.py`:
  - `uuid(None)` returns a 1-element list whose sole element matches `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`.
  - `uuid("3")` returns a 3-element list of distinct such UUIDs.
  - `uuid("0")`, `uuid("-1")`, `uuid("abc")` each raise `ConfigError`.
  - `ts(None)` → 1-element list, sole element matches `^\d{10}$`.
  - `ts("ms")` → matches `^\d{13}$`; `ts("iso")` → matches `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`; `ts("bogus")` → `ConfigError`.
  - `rand(None)` → 1-element list of 16 hex chars matching `^[0-9a-f]{16}$`; `rand("8")` → 8 hex chars; `rand("0")`/`rand("x")` → `ConfigError`.
  - **Charset property test (load-bearing pin):** for each generator, for each of `opts in {None, "1", "2", "ms", "iso", "16", "32"}` valid for that generator, every string in the returned list matches `^[0-9a-fA-FT:Z -]+$`. (Use `pytest.mark.parametrize`.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_template_vars.py -v`
  Expected: FAIL — module/functions not defined.

- [ ] **Step 3: Write minimal implementation**

  Create `agctl/template_vars.py`: implement the three generators (use `uuid.uuid4()`, `time.time()`, `secrets.token_hex`), the `BUILTIN_GENERATORS` dict mapping names → generators, and `parse_token` (split on first `:`). Raise `ConfigError` (from `agctl.errors`) on invalid `opts`. No substitution logic yet (Task 2).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_template_vars.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/template_vars.py tests/unit/test_template_vars.py`
  Run: `git commit -m "feat(template-vars): generator registry (uuid/ts/rand) + parse_token"`

---

## Task 2: `substitute_generators` walker + `find_unknown_templates`

**Files:**
- Modify: `agctl/template_vars.py`
- Test: `tests/unit/test_template_vars.py`

**Interfaces:**
- Consumes: Task 1 (`BUILTIN_GENERATORS`, `parse_token`, `ConfigError`).
- Produces (used by Tasks 5, 6–9, 10):
  - `TEMPLATE_RE` — a compiled regex matching `{{name[:opts]}}` where the interior is `{{` + `[A-Za-z_][A-Za-z0-9_]*` + optionally `:[^}]*` + `}}`. Tokens containing dots, spaces, or unbalanced braces (e.g. `{{user.name}}`, `{{ x }}`, `{{{x}}}`) do NOT match and are left literal.
  - `substitute_generators(value, memo: dict[str, str], *, enabled: bool = True) -> value'`:
    - `value` may be `str`, `dict`, or `list` — recurse into containers, returning the same container type with substituted strings.
    - For a `str`: find every `TEMPLATE_RE` match. For each match, the full matched text is the **memo key**. If the key is in `memo`, reuse its value; otherwise call `parse_token` on the interior, look up `BUILTIN_GENERATORS[name]`, call it with `opts`, **join** the returned list to a single string (1-element → that element; n-element → joined by a single space), store under the memo key, then replace the match. A name absent from `BUILTIN_GENERATORS` → `ConfigError` (message names the token and lists valid generators). Non-matching text is unchanged.
    - `enabled=False` → return `value` unchanged (no regex scan, no errors). This is the `--no-template-vars` path.
  - `find_unknown_templates(value) -> list[str]`: recurse the value tree (str/dict/list); return the full token texts (deduped, insertion-ordered) whose `name` is absent from `BUILTIN_GENERATORS`. **No generation, no raise** — pure scan for the validator.

- [ ] **Step 1: Write failing tests for `substitute_generators`**

  - `substitute_generators("no tokens here", {})` returns `"no tokens here"` unchanged.
  - `substitute_generators("{{uuid}}", {})` returns a 36-char lowercase UUID.
  - `substitute_generators("id={{uuid}} again {{uuid}}", {})` — both occurrences are the **same** UUID (assert the two halves are equal). (This is the memo rule.)
  - `substitute_generators("{{uuid}} vs {{uuid:2}}", {})` — the two tokens are **different** values (different memo keys); the `{{uuid:2}}` half is two space-separated UUIDs.
  - `substitute_generators("a={{ts}}", {})` → `"a=<10 digits>"`.
  - `substitute_generators("{{rand:8}}", {})` → 8 hex chars.
  - `substitute_generators("{{user.name}} and {{ x }}", {})` returns the input **unchanged** (non-matching).
  - `substitute_generators("{{nope}}", {})` raises `ConfigError` whose message contains `"nope"` and the valid generator names.
  - `substitute_generators("{{uuid}}", {}, enabled=False)` returns `"{{uuid}}"` unchanged.
  - `substitute_generators({"k": "{{uuid}}", "list": ["{{ts}}", "plain"]}, {})` recurses: dict value substituted, list[0] substituted, list[1] unchanged. Same `{{uuid}}` reused if a second field references it (add `{"k2": "{{uuid}}"}` to the dict → identical to `k`).

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_template_vars.py -v`
  Expected: FAIL — `substitute_generators` not defined.

- [ ] **Step 3: Write minimal implementation**

  Add `TEMPLATE_RE`, `substitute_generators` (recursive over str/dict/list; memo by full match text; list-join rule; unknown → `ConfigError`; `enabled=False` early-return), and `find_unknown_templates` (recursive scan, no raise).

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_template_vars.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/template_vars.py tests/unit/test_template_vars.py`
  Run: `git commit -m "feat(template-vars): substitute_generators walker + find_unknown_templates"`

---

## Task 3: `agctl gen uuid|ts|rand` command group

**Files:**
- Create: `agctl/commands/gen_commands.py`
- Modify: `agctl/cli.py` (register the `gen` group)
- Test: `tests/unit/test_gen_commands.py`

**Interfaces:**
- Consumes: Task 1 (`BUILTIN_GENERATORS` — call the generators directly; do NOT go through `substitute_generators`). `agctl/command.py::envelope`, `agctl/output.py::emit` (via envelope), `agctl/errors.py::ConfigError`.
- Produces: a `click.Group` named `gen` with three `@envelope`-wrapped subcommands. `command` envelope tags: `gen.uuid`, `gen.ts`, `gen.rand`. **Config-free**: these commands do NOT call `load_config_or_raise` and must succeed with no `agctl.yaml` present. They **ignore** `--no-template-vars` (they ARE generation).
  - Result shapes:
    - `gen uuid` → `{"value": "<uuid>"}`; `gen uuid --count N` (N≥2) → `{"values": ["<uuid>", …]}` (N distinct). (`--count 1` → `{"value": "<uuid>"}`.)
    - `gen uuid --upper` → uppercase UUID(s) in the same single/multi shape.
    - `gen ts` → `{"value": "<10 digits>"}`; `--ms` → `{"value": "<13 digits>"}`; `--iso` → `{"value": "<ISO-8601 Z>"}`.
    - `gen rand --length N` (default 16) → `{"value": "<N hex chars>"}`.
  - Flags: `--count INT` (uuid, default 1, must be ≥1), `--upper` (uuid, flag), `--ms`/`--iso` (ts, mutually exclusive, default seconds), `--length INT` (rand, default 16, ≥1). Bad integers / `<1` / `--ms`+`--iso` together → `ConfigError` exit 2.

- [ ] **Step 1: Write failing tests for the three subcommands**

  Use Click's `CliRunner` invoking the `gen` group. For each, assert `ok == True`, the `command` tag, and the result shape:
  - `gen uuid` → `command == "gen.uuid"`, `result.value` matches UUID regex.
  - `gen uuid --count 3` → `result.values` is a list of 3 distinct valid UUIDs.
  - `gen uuid --upper` → `result.value` is an uppercase UUID.
  - `gen uuid --count 0` → `ok == False`, `error.type == "ConfigError"`, exit 2.
  - `gen ts` → `result.value` matches `^\d{10}$`; `--ms` → `^\d{13}$`; `--iso` → ISO regex.
  - `gen ts --ms --iso` → `ConfigError`, exit 2.
  - `gen rand --length 8` → `result.value` matches `^[0-9a-f]{8}$`; default (no `--length`) → 16 chars.
  - `gen rand --length abc` → `ConfigError`, exit 2.
  - **Config-free test:** run from a temp dir with no `agctl.yaml` and no `AGCTL_CONFIG`; `gen uuid` still succeeds (does not raise a config-missing error). (Run the command's `_core` directly or via CliRunner with `env={"AGCTL_CONFIG": ""}` from an empty cwd — assert it does NOT emit a ConfigError about config.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_gen_commands.py -v`
  Expected: FAIL — `gen` group not defined.

- [ ] **Step 3: Write minimal implementation**

  Create `agctl/commands/gen_commands.py`: a `click.Group("gen")` and three subcommands, each delegating to a thin `_core` wrapped by `envelope(...)`. Each `_core` reads its flags, calls the Task-1 generator with the right `opts` (uuid: opts = `str(count)`; ts: opts = `"ms"`/`"iso"`/`None`; rand: opts = `str(length)`), validates flags (raise `ConfigError` on bad values / `--ms`+`--iso`), and returns the result dict (`{"value": ...}` or `{"values": ...}`). Do NOT load config. Register the group in `agctl/cli.py` alongside the other command groups.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_gen_commands.py -v`
  Expected: PASS. Also run `agctl gen --help` and `agctl --help` manually to confirm one new top-level line and the three subcommands.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/gen_commands.py agctl/cli.py tests/unit/test_gen_commands.py`
  Run: `git commit -m "feat(template-vars): agctl gen uuid|ts|rand command group"`

---

## Task 4: `--no-template-vars` global flag

**Files:**
- Modify: `agctl/cli.py`
- Test: `tests/unit/test_cli_template_vars_flag.py` (new)

**Interfaces:**
- Consumes: `agctl/cli.py` root group + `ctx.obj` pattern (mirrors how `--config` populates `ctx.obj["config_path"]`).
- Produces: a root-level `--no-template-vars` flag (boolean, default False). When set, `ctx.obj["no_template_vars"] = True`. Each command Click wrapper reads `template_vars_enabled = not ctx.obj.get("no_template_vars", False)` and passes it into its `_core` (the `_core` threading is done in Tasks 6–9; this task only adds the flag + `ctx.obj` population). `agctl gen` does NOT honor the flag.

- [ ] **Step 1: Write failing test for the flag**

  With `CliRunner` invoking any existing command (e.g. `agctl config show --no-template-vars` or `agctl --no-template-vars config show`), assert the command still runs (exit 0 or 2 from config, NOT a Click error) and — by inspecting `ctx.obj` via a tiny test-only command or by confirming the flag is accepted without "no such option" — that `--no-template-vars` is a recognized global option. (A direct assertion: invoke the root group with `--no-template-vars --help` and assert exit 0; invoke a command with the flag and assert no Click `UsageError`.)

- [ ] **Step 2: Run test to verify it fails**

  Run: `pytest tests/unit/test_cli_template_vars_flag.py -v`
  Expected: FAIL — `--no-template-vars` unrecognized (`no such option`).

- [ ] **Step 3: Write minimal implementation**

  In `agctl/cli.py`, add the `@click.option("--no-template-vars", is_flag=True, default=False)` to the root group's `ctx.obj` setup (same place `--config` is handled). Store `ctx.obj["no_template_vars"] = no_template_vars`.

- [ ] **Step 4: Run test to verify it passes**

  Run: `pytest tests/unit/test_cli_template_vars_flag.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/cli.py tests/unit/test_cli_template_vars_flag.py`
  Run: `git commit -m "feat(template-vars): --no-template-vars global flag"`

---

## Task 5: Generators in the `resolution.py` fill pipeline

**Files:**
- Modify: `agctl/resolution.py` (`fill_placeholders` ≈ lines 38–58; `_PLACEHOLDER_RE` ≈ line 30)
- Test: `tests/unit/test_resolution.py` (extend)

**Interfaces:**
- Consumes: Task 2 `substitute_generators`, `TEMPLATE_RE`. Existing `fill_placeholders(value, params)`.
- Produces: `fill_placeholders(value, params, *, memo: dict[str, str] | None = None, template_vars_enabled: bool = True)`:
  - When `memo is None`: behaves exactly as today (no generator pass) — backward compatible for any caller not yet passing a memo.
  - When `memo` is provided: for each leaf string, run `substitute_generators(leaf, memo, enabled=template_vars_enabled)` **before** the existing `{name}` substitution. The generator pass and the `{name}` pass never collide (different braces); a generated value is treated as literal by the `{name}` pass (no recursive expansion).
  - The memo is shared across every call within one command execution (the caller — each `_core` in Tasks 6–9 — creates one memo and passes it to all its `fill_placeholders` and `substitute_generators` calls), which is what makes the same token resolve to one value across `--key` + `--message` etc.

- [ ] **Step 1: Write failing tests extending `test_resolution.py`**

  - `fill_placeholders("{{uuid}}", {}, memo={})` → a 36-char UUID (generator pass ran).
  - `fill_placeholders("{name}", {"name": "x"}, memo={})` → `"x"` (existing `{name}` still works with a memo).
  - `fill_placeholders("id={{uuid}} name={name}", {"name": "n"}, memo=m)` (shared `m={}`) → `"id=<uuid> name=n"`; a second call `fill_placeholders("{{uuid}}", {}, memo=m)` returns the SAME uuid (memo shared across calls).
  - `fill_placeholders("{{uuid}}", {}, memo={}, template_vars_enabled=False)` → `"{{uuid}}"` (passthrough).
  - `fill_placeholders("{{uuid}}", {})` (no memo) → `"{{uuid}}"` unchanged (backward-compat default: no generator pass when memo is None).
  - `fill_placeholders({"k": "{{ts}}"}, {}, memo={})` recurses into dict.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_resolution.py -v`
  Expected: FAIL — new memo/enabled behavior absent.

- [ ] **Step 3: Write minimal implementation**

  Extend `fill_placeholders` with the `memo`/`template_vars_enabled` keyword params. In the string leaf path, when `memo is not None`, call `template_vars.substitute_generators(s, memo, enabled=template_vars_enabled)` before applying `_PLACEHOLDER_RE`. Leave the no-memo path byte-for-byte unchanged.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_resolution.py -v`
  Expected: PASS (new tests pass; existing `fill_placeholders` tests still pass — they call without a memo).

- [ ] **Step 5: Commit**

  Run: `git add agctl/resolution.py tests/unit/test_resolution.py`
  Run: `git commit -m "feat(template-vars): generator pre-pass in fill_placeholders"`

---

## Task 6: Kafka commands — inline substitution + memo threading

**Files:**
- Modify: `agctl/commands/kafka_commands.py` (produce `--key`/`--message`/`--header`; assert `--pattern` fill ≈ line 818)
- Test: `tests/unit/test_kafka_template_vars.py` (new)

**Interfaces:**
- Consumes: Task 2 `substitute_generators`, Task 5 `fill_placeholders(..., memo=, template_vars_enabled=)`, Task 4 flag (`template_vars_enabled` from `ctx.obj`). Existing kafka test seams: inject a fake producer via `producer_factory` / `consumer_factory` (per ARCH §8 KafkaClient test seams).
- Produces: `kafka produce` and `kafka assert` create one `memo = {}` per invocation, read `template_vars_enabled`, and substitute generators on `--key`, each `--message` (parsed-or-raw JSON string), each `--header` value, and (for `assert --pattern`) the pattern's `{placeholder}`-filled match expression (via `fill_placeholders(..., memo=memo, ...)`). The published key and the message body referencing the same token receive the same value.

- [ ] **Step 1: Write failing tests**

  Inject a fake producer that records the published `(topic, key, value)`:
  - `kafka produce --topic t --key {{uuid}} --message '{"cid":"{{uuid}}"}'` → the recorded **key** equals the recorded **value's `cid` field** (same UUID), and both are valid UUIDs.
  - `kafka produce --topic t --key {{uuid}} --header "trace={{uuid}}" --message '{{ts}}'` → key, header `trace`, and message are all substituted; key == header trace (same `{{uuid}}`).
  - Same command with `template_vars_enabled=False` (simulating `--no-template-vars`) → the recorded key is the literal string `"{{uuid}}"`, value's `cid` is the literal `"{{uuid}}"`, no error.
  - `kafka produce --topic t --message '{{nope}}'` → `ConfigError` exit 2 (unknown generator), raised before any produce.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_kafka_template_vars.py -v`
  Expected: FAIL — generators not applied to kafka args.

- [ ] **Step 3: Write minimal implementation**

  In the `produce` and `assert` Click wrappers, read `template_vars_enabled` from `ctx.obj` and pass to the `_core`. In each `_core`, create `memo = {}`, run `substitute_generators(...)` on `--key`/`--message`/each `--header` value (and the message after JSON-parsing is NOT required — substitute on the raw string before the client parses it), and pass `memo=memo, template_vars_enabled=enabled` to the pattern-match `fill_placeholders` call. Ensure the unknown-generator error surfaces as `ConfigError` (exit 2) before any network call.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_kafka_template_vars.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/kafka_commands.py tests/unit/test_kafka_template_vars.py`
  Run: `git commit -m "feat(template-vars): kafka produce/assert inline generators + memo"`

---

## Task 7: HTTP commands — inline substitution + memo threading

**Files:**
- Modify: `agctl/commands/http_commands.py` (call/request fill ≈ lines 138–152; ping ≈ 544–555)
- Test: `tests/unit/test_http_template_vars.py` (new)

**Interfaces:**
- Consumes: Tasks 2, 4, 5. Existing http DI: an injectable `http_client`/request seam (or mock the client at the `_core` boundary).
- Produces: `http call`, `http request`, and `http ping` create one memo per invocation; substitute generators on the resolved template body (via `fill_placeholders(..., memo=, ...)`), on `--body`, on `--header` values, and on the `--url`/path. Same token → same value across body + headers within one call.

- [ ] **Step 1: Write failing tests**

  Mock the HTTP client to record the sent `(method, path, headers, body)`:
  - `http request --service s --path /o --method POST --body '{"id":"{{uuid}}","ref":"{{uuid}}"}'` → sent body's `id` equals `ref` (same UUID); valid UUIDs.
  - `http request --service s --path "/o/{{ts}}"` → path contains a 10-digit timestamp (or substitute applies to a `--url`).
  - With `template_vars_enabled=False` → body is literal `{{uuid}}`, no error.
  - `--body '{{nope}}'` → `ConfigError` exit 2 before the request is sent.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_http_template_vars.py -v`
  Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

  Thread `template_vars_enabled` from `ctx.obj` into the `call`/`request`/`ping` `_core` calls. In each `_core`, create `memo = {}`; pass `memo=memo, template_vars_enabled=enabled` to the template-body `fill_placeholders`; run `substitute_generators` on `--body`, `--header` values, and `--url`/path strings. Unknown generator → `ConfigError` (exit 2) before any request.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_http_template_vars.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/http_commands.py tests/unit/test_http_template_vars.py`
  Run: `git commit -m "feat(template-vars): http call/request/ping inline generators + memo"`

---

## Task 8: gRPC commands — inline substitution + memo threading

**Files:**
- Modify: `agctl/commands/grpc_commands.py` (call fill ≈ 212–229; streaming 122; assert fills ≈ 312, 501, 702, 720)
- Test: `tests/unit/test_grpc_template_vars.py` (new)

**Interfaces:**
- Consumes: Tasks 2, 4, 5. Existing gRPC DI: injectable channel/client at the `_core` boundary.
- Produces: `grpc call` (template + free-form) creates one memo per invocation; substitutes on `--message`, each `--metadata` value, and the template message/metadata (via `fill_placeholders(..., memo=, ...)`). Assert-side `--match`/`--jq-path` fills also use the memo. Same token → same value across message + metadata.

- [ ] **Step 1: Write failing tests**

  Mock the gRPC client to record the serialized request message + metadata:
  - `grpc call --target t --service S --method M --message '{"id":"{{uuid}}","ref":"{{uuid}}"}' --metadata "trace={{uuid}}"` → message's `id` == `ref` == metadata `trace` (one UUID).
  - With `template_vars_enabled=False` → message is literal, no error.
  - `--message '{{nope}}'` → `ConfigError` exit 2 before the call.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_grpc_template_vars.py -v`
  Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

  Thread `template_vars_enabled` into the `call`/streaming `_core` calls; create `memo = {}`; `substitute_generators` on `--message`/`--metadata`/metadata values; pass `memo`/`enabled` to template `fill_placeholders` and assert-side fills. Unknown generator → `ConfigError` (exit 2) before any RPC.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_grpc_template_vars.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/grpc_commands.py tests/unit/test_grpc_template_vars.py`
  Run: `git commit -m "feat(template-vars): grpc call inline generators + memo"`

---

## Task 9: db / logs / kafka-listen — inline substitution + memo threading

**Files:**
- Modify: `agctl/commands/db_commands.py` (`--sql` free-form), `agctl/commands/logs_commands.py` (`--match` fill ≈ line 111), `agctl/listen/assert_eval.py` (≈ line 83), `agctl/commands/kafka_listen_commands.py` (≈ line 1050)
- Test: `tests/unit/test_db_template_vars.py`, `tests/unit/test_logs_template_vars.py` (new)

**Interfaces:**
- Consumes: Tasks 2, 4, 5. Existing DI: injectable DB driver, log backend, and (for listen) the capture-file reader.
- Produces:
  - `db query`/`assert`/`execute`: one memo per invocation; `substitute_generators` on `--sql` (free-form) before `:paramName` processing; template `sql` is config-defined (generators there are validated by Task 10, and substituted when the SQL string is handled). Same token → same value within the call.
  - `logs query`/`assert`/`tail` and `kafka listen assert`/`results`: one memo; `--match`/pattern `fill_placeholders` calls pass `memo`/`enabled`.

- [ ] **Step 1: Write failing tests**

  - db: inject a fake driver recording the executed SQL; `db query --sql "SELECT {{ts}} AS t" --connection c` (or free-form against a writable skip) → recorded SQL contains a 10-digit timestamp; `--no-template-vars` → literal `{{ts}}`; `--sql "{{nope}}"` → `ConfigError` exit 2 before execute. (If a live connection is awkward in unit tests, assert at the `_core` boundary by mocking the client to capture the SQL string.)
  - logs: `logs query --source s --match '.ts == "{{ts}}"'` → the match expression handed to the backend contains a 10-digit timestamp (assert by injecting a fake backend that records the filter); `--no-template-vars` → literal.
  - listen: `kafka listen assert --topic t --match '{{uuid}}'` (or via `--contains`) → the resolved predicate carries a substituted value.

- [ ] **Step 2: Run tests to verify they fail**

  Run: `pytest tests/unit/test_db_template_vars.py tests/unit/test_logs_template_vars.py -v`
  Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

  Thread `template_vars_enabled` into the db/logs/listen `_core` calls; create `memo = {}`; `substitute_generators` on `--sql`; pass `memo`/`enabled` to logs/listen `--match` and pattern `fill_placeholders`. Unknown generator → `ConfigError` (exit 2) before any DB/log/listen operation.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `pytest tests/unit/test_db_template_vars.py tests/unit/test_logs_template_vars.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/db_commands.py agctl/commands/logs_commands.py agctl/listen/assert_eval.py agctl/commands/kafka_listen_commands.py tests/unit/test_db_template_vars.py tests/unit/test_logs_template_vars.py`
  Run: `git commit -m "feat(template-vars): db/logs/listen inline generators + memo"`

---

## Task 10: `config validate` rejects unknown `{{...}}` tokens

**Files:**
- Modify: `agctl/commands/config_commands.py` (`config_validate` ≈ lines 129–182, where `collect_capture_placement_errors` is invoked ≈ line 162)
- Test: `tests/unit/test_config_validate.py` (extend or create)

**Interfaces:**
- Consumes: Task 2 `find_unknown_templates`. The resolved `Config` object (Pydantic model) from `compose_config`.
- Produces: `config validate` walks the resolved config's string-bearing fields (HTTP/DB/gRPC template bodies, SQL, kafka patterns, etc.), calls `find_unknown_templates` on each, and appends each unknown token as an error attributed at its config path (exit 2 on any). This runs alongside the existing `collect_jq_compile_errors` / `collect_capture_placement_errors` checks.

- [ ] **Step 1: Write failing test**

  Build a minimal in-memory `Config` (or a temp `agctl.yaml`) containing an HTTP template whose `body` includes `"id": "{{uuids}}"` (typo) and another with `"id": "{{uuid}}"` (valid). Run `config validate`:
  - The `{{uuids}}` case → `valid == False`, an error whose `path` is the template body path and whose `message` contains `"{{uuids}}"` and the valid generator names; exit 2.
  - The `{{uuid}}` case alone → `valid == True` (no error).
  - A body with `"{{user.name}}"` (non-matching) → `valid == True` (not flagged).

- [ ] **Step 2: Run test to verify it fails**

  Run: `pytest tests/unit/test_config_validate.py -v`
  Expected: FAIL — unknown tokens not detected.

- [ ] **Step 3: Write minimal implementation**

  In `config_validate` (or a small helper it calls), iterate the resolved config's known string-bearing fields (template bodies/paths/SQL/messages/pattern match expressions), call `template_vars.find_unknown_templates` on each, and append `{path, message}` errors for any non-empty result (message names the token + valid generators). Reuse the existing error-accumulation + exit-2 flow.

- [ ] **Step 4: Run test to verify it passes**

  Run: `pytest tests/unit/test_config_validate.py -v`
  Expected: PASS.

- [ ] **Step 5: Commit**

  Run: `git add agctl/commands/config_commands.py tests/unit/test_config_validate.py`
  Run: `git commit -m "feat(template-vars): config validate rejects unknown {{...}} tokens"`

---

## Task 11: Runbook skills — document tokens + `gen`, validate at load

**Files:**
- Modify: `skills/agctl-write-test-runbook/SKILL.md`, `skills/agctl-write-test-runbook/reference/runbook-template.md`
- Modify: `skills/agctl-run-test-runbook/SKILL.md`
- (No unit tests — these are prose/contract edits; verify by re-reading.)

**Interfaces:**
- Consumes: Tasks 1–10 (the feature surface to document).
- Produces:
  - **Write skill**: a new short subsection documenting the `{{uuid}}`/`{{ts}}`/`{{rand}}` inline tokens (one value per token within a step), the `agctl gen uuid|ts|rand` command, and the two supported patterns — (A) inline single-step same-value-multiple-fields, (B) `gen` + `Capture` (`CID=result.values[0]`) for cross-step — as the replacement for the `bash -c '…$(uuidgen)…'` anti-pattern. The runbook template gains a worked inline-token example step.
  - **Run skill**: the Validate step (§1 of `agctl-run-test-runbook/SKILL.md`) gains a rule: every `{{...}}` token in a Command must be a known generator (uuid/ts/rand); an unknown token stops the run with a validation error before execution (no partial runs), mirroring the existing `$VAR`-defined-before-used check.

- [ ] **Step 1: Write the acceptance check (manual prose review)**

  Define the exact edits: (a) write-skill subsection text + a template example step using `{{uuid}}` inline and a `gen`+`Capture` cross-step example; (b) run-skill Validate bullet rejecting unknown `{{...}}`. State the acceptance: both skills read cleanly and the two patterns are exemplified.

- [ ] **Step 2: Verify current skills lack this content**

  Run: `grep -n "uuid\|{{" skills/agctl-write-test-runbook/SKILL.md skills/agctl-run-test-runbook/SKILL.md`
  Expected: no `{{uuid}}`/`gen` references yet (confirms the gap).

- [ ] **Step 3: Apply the edits**

  Add the subsection + template example to the write skill; add the Validate rule to the run skill. Keep each skill's altitude (authoring guidance vs execution procedure) unchanged.

- [ ] **Step 4: Verify the edits read correctly**

  Re-read both edited files end-to-end; confirm the two patterns are exemplified and the Validate rule is unambiguous. (No automated test for prose.)

- [ ] **Step 5: Commit**

  Run: `git add skills/agctl-write-test-runbook skills/agctl-run-test-runbook`
  Run: `git commit -m "docs(skills): document {{...}} template vars + agctl gen in runbook skills"`

---

## Task 12: Sync DESIGN.md + ARCHITECTURE.md via `docs-watcher`

**Files:**
- Modify (via `docs-watcher` subagent): `docs/DESIGN.md`, `docs/ARCHITECTURE.md`
- (Verification: the docs-watcher's own review; plus `grep` checks below.)

**Interfaces:**
- Consumes: the implemented feature (Tasks 1–11). Project rule (CLAUDE.md): after code/config changes, invoke `docs-watcher` to check whether DESIGN.md / ARCHITECTURE.md need syncing. DESIGN = WHAT/WHY; ARCHITECTURE = HOW (as-built).
- Produces: DESIGN §2 (generators recognized in template bodies; the token table), §3 (the `agctl gen` group + `--no-template-vars` flag), §10 (move "Template variable validation" from deferred → done; add the new deferred items from spec §16); ARCHITECTURE §3 (`agctl/template_vars.py` + `agctl/commands/gen_commands.py` in the module map), §5 (the generator pass in the fill pipeline), §9 (generators as part of the resolution layer).

- [ ] **Step 1: Define the sync acceptance**

  List the exact sections to update (above). A correct no-op on a section is acceptable if the watcher judges it covered; the watcher must not make speculative edits.

- [ ] **Step 2: Capture pre-sync state**

  Run: `grep -n "template.var\|gen uuid\|no-template-vars\|{{uuid}}" docs/DESIGN.md docs/ARCHITECTURE.md`
  Expected: few/no hits (confirms the docs predate the feature).

- [ ] **Step 3: Invoke the `docs-watcher` subagent**

  Dispatch `docs-watcher` to sync DESIGN.md (§2/§3/§10) and ARCHITECTURE.md (§3/§5/§9) against the implemented feature, preserving each doc's altitude. The subagent makes the edits.

- [ ] **Step 4: Verify the sync**

  Run: `grep -n "agctl gen\|no-template-vars\|{{uuid}}\|template_vars.py" docs/DESIGN.md docs/ARCHITECTURE.md`
  Expected: hits in the intended sections; DESIGN §10 no longer lists "Template variable validation" as deferred (it is now done) and lists the new deferred items.

- [ ] **Step 5: Commit**

  Run: `git add docs/DESIGN.md docs/ARCHITECTURE.md`
  Run: `git commit -m "docs: sync DESIGN/ARCHITECTURE for template variables + agctl gen"`

---

## Notes for the implementer

- **TDD strictly.** Each task writes the failing test first, then the minimal implementation. Do not write implementation code in advance.
- **One memo per `_core`.** The same-token-same-value invariant depends on a single `memo = {}` created at the top of each command `_core` and passed to every `fill_placeholders`/`substitute_generators` call within it. Do not create a fresh memo per argument.
- **Fail loud, fail early.** Unknown/malformed tokens raise `ConfigError` (exit 2) at the earliest layer that sees them — `config validate` for config values, the run-skill Validate for runbook commands, and the `_core` for free-form CLI args — always before any network/DB side effect.
- **Do not touch** `agctl/mock/capture.py` (mock capture is unrelated), `${ENV}` interpolation in `config/loader.py`, or the `--param` `{name}` semantics.
- **`agctl gen` is config-free** and ignores `--no-template-vars`.
- After the final task, run the full unit suite (`pytest tests/unit -q`) to confirm no regressions before opening the PR.
