# Design: Unify `match` onto the envelope root (#22) — dialect v2

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-05
**Author:** brainstorming session
**Affects:** `agctl/config/loader.py` (`TOOL_MAJOR_VERSION`, `_check_version`),
`agctl/mock/http_server.py` (stub `match.jq` eval), `agctl/mock/kafka_reactor.py`
(reactor `match` eval), `agctl/commands/kafka_commands.py` (`_build_assert_predicate`,
`--match`), `agctl/commands/http_commands.py` (`evaluate_http_assertions`, `--match`),
`agctl/commands/config_commands.py` (new `config migrate`), `pyproject.toml` (semver),
`agctl-config` skill, DESIGN §2.1/§3/§4/§6/§10/§11, ARCHITECTURE §5/§9/§15.
**Relation to docs:** Closes **#22** (DESIGN §10 roadmap; ARCHITECTURE §15 known
limitation). Supersedes the `match`/`capture` `.`-root divergence accepted by the
sibling capture spec ([`2026-07-04-agctl-mock-capture-design.md`](../archive/2026-07-04-agctl-mock-capture-design.md)),
which deliberately left `match` payload-rooted as the cost of additive capture.

---

## 1. Background & Problem

The capture spec (shipped in #23) introduced an envelope-rooted `capture:` map on
mock stubs and reactors: `capture.<name>.from` is a jq path evaluated against the
**whole incoming message envelope** (HTTP request / Kafka message), so nested fields,
the Kafka `.key`, and headers are all reachable. To stay additive under config major
`"1"`, `match` was **not** migrated. The result is a `.`-root divergence on the same
stub/reactor:

```yaml
match: { jq: ".amount > 1000" }      # payload-rooted (HTTP body / Kafka value)
capture:
  amount: { from: ".body.amount" }    # envelope-rooted (whole request / message)
```

Same field, two different `.` roots — a usability wart and a copy-paste trap. This is
documented as a deferred breaking change in DESIGN §10 and ARCHITECTURE §15.

Beyond mocks, the same payload-rooted `match` exists on `kafka.patterns[].match`
(config) and the free-form CLI flags `http call/request --match` and
`kafka assert/consume --match`. The acceptance criterion for #22 is that `match` and
`capture` share **one** `.` root; this design extends that to **every** jq `match` site
so `.` means a consistent thing tool-wide.

## 2. Goals

- **One `.` root per transport.** Every jq `match` roots at the same envelope its
  sibling `capture` (mocks) or natural "whole message" (CLI) already uses.
- **A clean, atomic cutover.** A single version-gated dialect switch flips every site
  at once; no long-lived per-stub knobs.
- **A mechanical, automatable migration.** Existing configs carry a documented
  one-line-per-expression rewrite, automated by a new `agctl config migrate` command.
- **Preserve fail-loudly.** A forgotten migration surfaces as an exit-2 config error
  (config-defined match) or an exit-1 assertion failure (CLI flags), never a silent
  false-green.

## 3. Scope & Design Constraints

- **In scope — all jq `match` sites:**
  - Mock HTTP stub `match.jq`.
  - Mock Kafka reactor `match`.
  - `kafka.patterns[].match` (config).
  - `kafka assert --match` / `kafka consume --match` (and the deprecated `--filter-key`
    alias).
  - `http call --match` / `http request --match`.
- **Dialect switch.** The config `version` major is the jq-dialect switch: `"1"` =
  payload-rooted `match` (frozen legacy); `"2"` = envelope-rooted `match` (new).
  Config is loaded before any `--match` is evaluated (every command resolves
  services/templates first), so the switch governs config-defined **and** CLI-supplied
  `match` uniformly.
- **Roots reuse existing envelopes.** Mock `match` adopts the request/message envelope
  already constructed for `capture` (parent spec §7.1). CLI `match` adopts the response
  result / normalized message already produced by the transport clients.
- **`capture` is unchanged.** It is already envelope-rooted; this design only lifts
  `match` to meet it.

## 4. Non-Goals

- Changing `--contains`, `--path`, `--jq-path`/`--equals`, `--status`, or stub
  `match.body` (json_subset). These are not jq-`.`-rooted predicates; their targets are
  unchanged.
- Decoding non-JSON HTTP bodies or Kafka values (inherited limitation).
- Auto-migrating CLI `--match` expressions embedded in shell scripts / agent prompts —
  these are out of the config's reach; covered by a docs rule and fail-loud at runtime.
- A general jq-expression parser. The migration is a fixed-string prepend, not parsing
  (§7).
- Backward-compatible runtime evaluation of v1 configs under a v2 tool. v1 is rejected
  with a migrate pointer; `config migrate` is the bridge.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| **D1** | **Config `version` major = jq dialect switch** (`"1"`→`"2"` flips all `match` to envelope-rooted). | The version gate already exists (`_check_version`) and already rejects major mismatch with exit 2. Reusing it gives one atomic lever across config-defined and CLI `match`. No new per-stub knob. |
| **D2** | **Migration = fixed-string prepend** of `.body \| ` (HTTP) / `.value \| ` (Kafka) to each `match` expression. | jq's `\|` rebinds `.`, so `<root> \| <predicate>` runs the predicate with `.` rebound to the payload — byte-for-byte equivalent to the v1 predicate against the payload. No jq parsing; obviously semantics-preserving and idempotent. |
| **D3** | **New `agctl config migrate` subcommand** applies D2 + bumps `version`, in place, with a backup. | The user base is small and known (the four battle-tested mock servers); a one-shot automatable rewrite makes the break cheap. Mirrors the project's safety convention (`config init --force`, `db execute --write`) via a backup + `--dry-run`. |
| **D4** | **Version-gate error becomes a migration pointer**, not a bare mismatch. | Same `ConfigError` exit 2; the message tells the user exactly what to run. Loud, actionable. |
| **D5** | **CLI `--match` migrations are docs-only.** `config migrate` cannot reach shell/prompt expressions; the guide carries the prepend rule, and a stale predicate fails loud at runtime. | No way to auto-rewrite ephemeral expressions; fail-loud (predicate → `null` → false → assertion timeout/failure, exit 1) is the acceptable backstop. |
| **D6** | **Envelope shapes are reused, not reinvented.** Mock `match` uses the capture envelope; CLI `match` uses the transport result envelope. Header-casing follows each transport's own convention (lowercased HTTP; case-sensitive Kafka). | Closes the divergence by construction (mock `match` and `capture` literally share an envelope). No new envelope code. |
| **D7** | **Package semver bumped to `1.0.0`** alongside the config-major bump. | Signals the dialect boundary in both version tracks; config major and package major finally coincide at a real contract break. |

## 6. The v2 Envelope-Root Contract

Under dialect `"2"`, each jq `match` site roots `.` at one of three envelopes. Mock
`match` and its sibling `capture` share a root by construction.

| `match` site | v1 root (`.` = ) | **v2 root (`.` = )** |
|---|---|---|
| Mock HTTP stub `match.jq` | request **body** | **request envelope** `{method, path, headers‡, body}` |
| Mock Kafka reactor `match` | message **value** | **message envelope** `{key, value, partition, offset, timestamp, headers†}` |
| `kafka.patterns[].match` (config) | message **value** | **message envelope** (same as reactor) |
| `kafka assert --match` / `consume --match` | message **value** | **message envelope** (same) |
| `http call` / `request --match` | response **body** | **response envelope** `{status_code, response_time_ms, headers‡, body, url, method}` |

‡ HTTP headers **lowercased** (transport convention; matches `HttpClient` and the mock
request envelope from the parent spec §7.1). † Kafka headers **case-sensitive
as-produced** (matches reactor `capture` and the `kafka_client` normalized message — do
not lowercase).

**Reach gained under v2 (features, not regressions):**
- Mocks: `.body.amount` / `.value.command` reach the payload; `.headers.authorization`,
  `.key`, `.headers.rqUID` become matchable in `match`, exactly as they already are in
  `capture`. The intra-mock `.` divergence is closed.
- `kafka assert/consume --match`: `.key` and `.headers.rqUID` become matchable for the
  first time.
- `http --match`: `.status_code`, `.headers.x`, `.response_time_ms` reachable
  (orthogonal to the existing `--status` / `--jq-path` flags, which are unchanged).

**Fail-loud for forgotten CLI-flag migrations:** a stale `kafka assert --match '.amount
== "X"'` under v2 resolves `.amount` on the message envelope → `null` → predicate false
→ assert times out → `AssertionError` exit 1. Same for `http --match`. Never a silent
false-green.

## 7. Migration Mechanism

### 7.1 The prepend transform (D2)

Per transport, the v1 predicate and its v2-equivalent form:

| Transport | v1 (payload-rooted) | v2 (envelope-rooted), equivalent form |
|---|---|---|
| HTTP (`match.jq`, `http --match`) | `.amount > 1000` | `.body \| .amount > 1000` |
| Kafka (reactor `match`, `kafka.patterns`, `kafka --match`) | `.command == "X"` | `.value \| .command == "X"` |

The transform is a **prefix insertion**. It is semantics-preserving for any predicate
written against the payload, because `<root> \| <predicate>` runs `<predicate>` with `.`
rebound to the extracted payload. It is **idempotent**: an expression already beginning
with `.body \| ` / `.value \| ` is skipped, so re-running `config migrate` (or running
it on a hand-migrated expression) double-nests nothing.

### 7.2 `agctl config migrate` (new subcommand, D3)

```
agctl config migrate
    [--config <path>]            # auto-discovered by default
    [--dry-run]                  # preview the rewrite set without writing
```

Behavior:
- Walks every jq `match` site — mock stub `match.jq`, reactor `match`,
  `kafka.patterns[].match` — and applies the per-transport prepend from §7.1.
- Bumps `version: "1"` → `"2"`.
- **Scope is strictly `match`.** `capture.*.from` (already envelope-rooted), stub
  `match.body` (json_subset, not jq), and all non-jq assertion flags are **not**
  touched.
- **Idempotent** — expressions already prefixed are skipped; running on a config already
  at `"2"` rewrites 0 and reports "already dialect 2".
- **Safety** — writes a backup (`agctl.yaml.bak`) alongside the rewrite; `--dry-run`
  previews the `rewritten` set without writing. Mirrors the project's safety convention.
- **Output** — the standard one-JSON envelope:
  `result: {path, from_version, to_version, rewritten: [{path, before, after}],
  skipped: [...], cli_flags_note}`. The `cli_flags_note` reminds the operator that CLI
  `--match` expressions in scripts/prompts must be prefixed manually (D5).
- Implemented as a pure helper (`migrate_match_exprs(config_dict)`) over the parsed
  config dict, wrapped by the Click command — keeping the transform unit-testable
  independent of file I/O.

### 7.3 Version-gate pointer (D4)

`_check_version` continues to raise `ConfigError` (exit 2) when a config's major ≠
`TOOL_MAJOR_VERSION`. The message/detail is reshaped into a migration pointer:

> Config dialect `"1"` is payload-rooted; this tool speaks dialect `"2"`
> (envelope-rooted `match`). Run `agctl config migrate`, or manually bump `version` to
> `"2"` and prefix each `match` with `.body \| ` (HTTP) / `.value \| ` (Kafka).

Same exit code and error type as today; additive hint only.

### 7.4 CLI-flag migration (docs-only, D5)

`--match` flags on `http call/request` and `kafka assert/consume` live in shell scripts
and agent prompts; `config migrate` cannot reach them. The migration guide (DESIGN
§3.1/§3.2 + skill) carries the one-line prepend rule. A forgotten migration fails loud
at runtime (§6).

## 8. Component Surface

| Module | Change (semantics; no new model shape) |
|---|---|
| `config/loader.py` | `TOOL_MAJOR_VERSION` `"1"`→`"2"`; `_check_version` keeps exit-2 `ConfigError`, reshapes message to the §7.3 pointer. |
| `mock/http_server.py` | Stub-match eval roots at the request envelope `{method, path, headers, body}` — the same envelope built for `capture`. |
| `mock/kafka_reactor.py` | Reactor-match eval roots at the message envelope `{key, value, partition, offset, timestamp, headers}` — already normalized for `capture`. |
| `commands/kafka_commands.py` | `_build_assert_predicate` / consume `--match`: the jq `--match`/`--pattern` branch roots at the whole message. `--contains`/`--path` stay value-rooted. Covers the deprecated `--filter-key` alias. |
| `commands/http_commands.py` | `evaluate_http_assertions` `--match` branch roots at the response result envelope `{status_code, response_time_ms, headers, body, url, method}`. `--status`/`--contains`/`--jq-path`/`--equals` unchanged. |
| `commands/config_commands.py` | New `config migrate` subcommand (§7.2) wrapping `migrate_match_exprs`; standard envelope output. |
| `mock/jq_precompile.py` | **No change.** `compile_jq` is root-agnostic; `iter_mock_jq_expressions` already covers stub/reactor match. The new `kafka.patterns[].match` walk lives in the `config migrate` helper (§7.2), not here — migrate-only, not the validate walker. |
| `config/models.py` | No model/field changes. `KafkaPattern.match`, `HttpStub.match.jq`, `KafkaReactor.match` keep their types; root semantics are now version-gated. |
| `pyproject.toml` | Package version bumped to `1.0.0` (D7). |

## 9. Error & Exit-Code Model

| Failure | Type | Exit | When |
|---|---|---|---|
| Config dialect `"1"` under a `"2"` tool | `ConfigError` | 2 | load — `_check_version` (§7.3), message points at `config migrate`. |
| Malformed jq in any (migrated) `match` | `ConfigError` | 2 | startup — existing `compile_jq` pre-compile (`mock run` Step 0 / `config validate`). |
| `config migrate` write/backup failure | `ConfigError`/`InternalError` | 2 | `config migrate`. |
| `config migrate` on an already-`"2"` config | ok (0 rewritten) + "already dialect 2" note | 0 | `config migrate`. |
| Forgotten CLI `--match` migration | `AssertionError` | 1 | runtime — predicate resolves `null` → false → assert timeout/failure (§6). |

No new error types; all additions reuse the existing hierarchy.

## 10. Testing Strategy

Mirrors the existing mock/command test seams (ARCHITECTURE §12).

**Unit (no network):**
- **Per-site root shift (7 sites):** a predicate true under v1 at the payload path holds
  under v2 at the envelope-equivalent path; new reach (`.key`, `.headers.rqUID`,
  `.status_code`) asserted where applicable.
- **`migrate_match_exprs`:**
  - prepend correctness per transport (HTTP `.body \| `, Kafka `.value \| `);
  - **idempotency** — re-run yields 0 rewritten, no double-nesting;
  - `capture.*.from` and `match.body` are **not** touched;
  - `version` bumped `"1"`→`"2"`;
  - `kafka.patterns[].match` included in the rewrite set.
- **Version gate:** v1 config under v2 tool → `ConfigError` exit 2 with the migrate-hint
  substring; v2 config accepted; missing-`version` still errors.
- **Regression:** `kafka --contains`/`--path` and `http --status`/`--contains`/
  `--jq-path`/`--equals` are unaffected by the dialect flip (still value-/body-targeted).
- **#22 closure case:** a stub/reactor carrying **both** `match` and `capture` over the
  same `.body.x` / `.value.x` — both resolve against the shared envelope root.
- **`config migrate` command:** `--dry-run` writes nothing; default writes `agctl.yaml`
  + `agctl.yaml.bak`; envelope shape and `cli_flags_note` present.
- **compile_jq root-agnosticism:** a deliberately bad jq expression still raises
  `ConfigError` at startup under v2.

**Integration / E2E (acceptance):** the four capture-spec battle-test scenarios
extended so `match` also exercises the envelope root (e.g. a reactor matching on
`.value.command` **and** `.headers.rqUID`; an HTTP stub matching on `.body.x` alongside
a `capture` of the same field). Kafka E2E self-skips without `AGCTL_TEST_LIVE=1`
(existing harness).

## 11. Deferred & Out-of-Scope

- Auto-migrating CLI `--match` expressions in scripts/prompts (D5).
- A jq parser for "smart" rewriting beyond the prepend (D2 is deliberately dumb and
  safe).
- Compile-checking `kafka.patterns[].match` in `config validate` (a pre-existing gap;
  `config migrate` reports the patterns it touches but formal validate-coverage of
  patterns is a separate item).
- HTTP `query` envelope field / query-string parsing (parent spec §4).
- Decoding non-JSON bodies/values (inherited).

## 12. Rejected Alternatives (ADR-style)

- **`match_root: envelope` compat shim** (per-stub/reactor opt-in, default stays
  payload-rooted, flip at a future major). Rejected (D1/D3): the shim does not map onto
  the free-form CLI `--match` flags without a per-invocation `--match-root` clutter
  flag; the `.` divergence *persists* through the transition (the goal becomes opt-in,
  not achieved); and the eventual flip is *still* a major bump — same destination, more
  surface carried longer.
- **Lint / auto-detection only.** Rejected as a standalone solution: `.amount > 1000` is
  valid jq against both a body and an envelope, so an author's intent is undecidable.
  Detection is valuable only as the migration aid folded into `config migrate` (D3),
  which emits exact rewrites rather than warnings.
- **Runtime v1-on-v2 compatibility** (auto-prefixing match at load when dialect is 1,
  so v1 configs keep working under a v2 tool). Rejected (D1): it defeats the one-root
  cleanup, leaves the dialect switch half-meaningful, and hides the break from the
  author. The version gate is the contract; `config migrate` is the bridge.
- **Per-site root knobs** (e.g. a separate envelope switch per command). Rejected (D1):
  one dialect switch is simpler to reason about, test, and document than N independent
  flips.

## 13. Docs & Skill Impact

Doc sync is performed via the `docs-watcher` subagent after implementation (CLAUDE.md
"Docs Sync").

- **DESIGN.md §2.1** — `version` note: `"2"` = envelope-rooted `match` dialect; `match.jq`
  / reactor `match` root at the request/message envelope (peer of `capture`). Drop the
  "match stays payload-rooted" comment from the capture block.
- **DESIGN.md §3.1 / §3.2** — `http --match` / `kafka --match` semantics: root = response
  / message envelope; migration note (D5).
- **DESIGN.md §3.6** — add `agctl config migrate`.
- **DESIGN.md §4 / §10** — error table note (dialect mismatch pointer); remove the #22
  deferred row.
- **DESIGN.md §2.1 patterns + §11 use-cases + §6 AGENTS.md template** — update concrete
  `--match` / `.match` examples to v2 form (e.g. `.eventType` → `.value.eventType`,
  `.payload.orderId` → `.value.payload.orderId`).
- **ARCHITECTURE.md §5** — version guard now gates dialect; `_check_version` hint.
- **ARCHITECTURE.md §9** — `match` eval sites are envelope-rooted under v2; mock `match`
  and `capture` share a root.
- **ARCHITECTURE.md §15** — remove the `match`/`capture` `.`-divergence bullet; note the
  dialect switch.
- **`skills/agctl-config/reference/`** — authoring guidance: the three envelope roots,
  the HTTP/Kafka header-casing asymmetry, the `.body \| `/`.value \| ` legacy form,
  `config migrate`, and the CLI-flag migration rule (D5).
