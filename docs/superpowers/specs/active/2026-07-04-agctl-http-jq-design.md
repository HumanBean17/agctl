# Design: jq Support for HTTP — Assertions + Mock Stub Matching

**Status:** Draft v3 — re-amended after a second (delta) multi-agent review; the v2 amendments were verified sound, this pass folded in 3 residual majors + minors
**Date:** 2026-07-04
**Author:** brainstorming session
**Affects:** `commands/http_commands.py`, `agctl/assertions.py` (two helpers: `evaluate_http_assertions`, `compile_jq`), `config/models.py` (`HttpMatch`), `mock/http_server.py`, `mock/engine.py` (startup), `mock/kafka_reactor.py` (startup pre-compile, shared seam), `config/validator.py`, `pyproject.toml` (new `jq` extra), `agctl-config` skill, DESIGN.md, ARCHITECTURE.md
**Relation to docs:** Additive — no non-goal reversed. On implementation, DESIGN.md §2 / §3.1 / §3.5 / §4 / §10 and ARCHITECTURE.md §8 / §9 / §11 are synced via `docs-watcher`. No `version` bump (additive under major `1`).

---

## 1. Background & Problem

`agctl`'s jq engine is transport-agnostic and already shared: `agctl/assertions.py`
exposes `jq_bool`, `jq_value`, `json_subset`, and a lazy `_jq()` that turns a missing
library into a `ConfigError` (exit 2). Today jq is exercised by the Kafka side
(`kafka.patterns` `match`, mock Kafka reactor `match`, `kafka assert/consume --match`,
`db assert --path`) and by `json_subset` (shared with `kafka assert --contains` and the
mock HTTP stub `match.body`). The HTTP *client* surface, however, has no jq at all.

This produces two concrete gaps:

1. **HTTP responses cannot be asserted inside `agctl`.** `http call` returns the raw
   response and exits `0` even on a `5xx` body (ARCHITECTURE §8: "HTTP status is a
   result, not an assertion"). To *verify* a response, an agent must leave the envelope
   and hand-roll shell `jq` — re-implementing assertion logic in a string with quoting
   that agents routinely get wrong (the DESIGN.md cookbook itself ships a malformed
   example, §11 UC-1). This breaks agctl's central agent contract — *"treat exit code `1`
   as a test failure"* (AGENTS.md template) — for the one transport that can't fail loudly.
2. **Mock HTTP stubs cannot match on a predicate.** `match.body` is `json_subset` — pure
   structural containment. It cannot express `.amount > 100`, `has("priority") and
   .priority != "low"`, or regex/membership. A stub for a *branching* SUT ("high-value →
   fraud-check, low-value → auto-approve") returns one canned response regardless of input,
   so the SUT's branch handling is exercised against a lie — the plausible-but-wrong
   (false-green) mode agctl's own §15 limitations table warns about.

Both gaps close by reusing machinery that already exists. This spec adds **(2) HTTP
response assertions** and **(1) a jq predicate on mock HTTP stubs**, and deliberately does
**not** add jq to outgoing request-body construction (YAGNI — `{placeholder}` + `--body`
deep-merge already covers building requests).

> **Scope honesty (load-bearing).** Feature (2) covers **assertion**, not **extraction**.
> It replaces the shell-jq an agent writes to *verify* a response; it does NOT yet replace
> the shell `jq -r` an agent writes to *extract* a field for the next command (e.g.
> `ORDER_ID=$(... | jq -r '.result.body.order_id')`). Extraction (`--capture path=name`)
> is explicitly deferred (§14). Claiming otherwise would oversell the feature.
>
> (2) is an unambiguous improvement: it restores exit-code consistency across all three
> transports. (1) pays off only when the mocked SUT has conditional branches; for
> single-response stubs it is dormant. It is cheap and symmetric, so the bar is low — but
> it is not "always better."

## 2. Goals

- Let an agent **assert** on an HTTP **response** (status + body) with the same exit-code
  discipline as `kafka assert` / `db assert`, so HTTP stops being the one transport that
  cannot fail loudly. (Extraction is out of scope — see §1, §14.)
- Let a mock HTTP **stub** match an incoming request with a jq predicate, mirroring the
  Kafka reactor's `match`, so conditional mocks stop false-greening.
- **Reuse** the existing jq/subset/coercion primitives and the lazy-import convention; add
  no new assertion engine and no new dependency beyond the already-bundled `jq`.
- **Close, not widen, the fail-loudly surface**: a jq typo in config must fail loud at
  startup (not silently mis-match every request); this guarantee is extended to the
  existing Kafka reactor `match` at the same seam.
- Preserve every existing behavior: no-flag `http call`, `match.body` stubs, the HTTP-only
  zero-extra mock, and the output/error contracts.

## 3. Scope & Design Constraints

- **Additive only.** No existing flag, field, exit code, or output shape changes. Both
  features are opt-in. The default path is byte-for-byte unchanged **with one deliberate
  exception**: D5's startup pre-compile extends to *existing* Kafka reactor `match`
  expressions, so a config with a malformed reactor `match` that previously started and ran
  silently inert now fails loud at startup (exit 2). This is a justified loudness fix for an
  already-broken config, not a behavior change for valid configs.
- **Coexist, don't replace.** Assertion modes coexist and AND together — the `kafka
  assert` pattern (`db assert` modes are **mutually exclusive**, not a coexistence
  precedent). Mock `match.body` (subset) and `match.jq` (predicate) coexist and AND
  together. jq never *replaces* `body`.
- **Reuse the lazy import.** jq is loaded only when a jq flag/expression is actually used;
  a missing library surfaces as `ConfigError` (exit 2) with an install hint, never a bare
  `ModuleNotFoundError`. This is what preserves the HTTP-only zero-dep mock (a stub without
  `match.jq` never imports jq).
- **Fail loud on authoring bugs, soft on data variance — via two distinct helpers.**
  `jq_bool(value, expr)` *conflates* compile-time and run-time errors into a single `False`
  (correct for runtime matching, where partial matching must never crash). A *malformed
  expression* is an authoring bug and must fail loud, so D5 uses a **compile-only** path
  (`compile_jq(expr)`, no `.input().all()`) that lets `ValueError` propagate as
  `ConfigError`. The two helpers are not interchangeable.
- **`@envelope` discipline preserved.** HTTP assertion failures raise `AssertionFailure`
  and ride the existing envelope path (`ok:false`, `error.type:"AssertionError"`,
  exit 1). No new streaming exception, no envelope signature change.

## 4. Non-Goals

- **Request-body construction via jq.** `{placeholder}` + `--body` deep-merge already
  covers building outgoing bodies; jq construction is YAGNI.
- **Response extraction (`--capture path=name`).** Agents still hand-roll `jq -r` to pull
  a field for the next command; a built-in capture/extraction flag is deferred (§14) to
  keep v1 focused on the fail-loudly goal.
- **A separate `http assert` command.** See D1 — assertion composes onto `http call`'s
  existing result; a separate command would duplicate the request machinery.
- **Replacing `match.body` with jq.** Breaking change to a freshly-shipped feature
  (v0.2.0); loses the declarative subset semantic; breaks the coexist precedent.
- **Nested-field / header capture** in mock responses (deferred — affects both transports
  symmetrically; not a one-sided HTTP addition).
- **`http ping` assertions.** Ping streams NDJSON per ping; wiring assertion results into
  the streaming line shape is a separate extension (deferred).
- **`--status` range matching** (`2xx`, `4xx`) in v1 — exact code only.
- **A dedicated `--match-all` flag.** The "all items" case is covered by a jq idiom (§6);
  a sibling flag is deferred.

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Assertion flags on `http call` / `http request`, not a new command.** | `http call`'s success-path result *is* the HTTP response; bolting on assertion flags adds only a fail-path, leaving the success result shape unchanged. A separate `http assert` would duplicate the request machinery for no output-contract benefit — rejected per DESIGN §8 ("ten focused commands over fifty overlapping ones"). The operative differentiator: http assertion flags are lightweight *additives* (any-flag-triggers; success result unchanged), whereas db's `--expect-*` modes are heavyweight mutually-exclusive selectors that earned a separate command. (`kafka assert`'s separation comes from its consume-window; db assert's is conventional — HTTP shares neither shape.) |
| D2 | **Flags coexist and AND together**, mirroring `kafka assert` (`--contains` + `--match` + `--pattern`, "all active modes must pass"). | One consistent rule across transports; the agent picks modes per call based on what it checks. |
| D3 | **Reuse `assertions.py` primitives** for runtime evaluation — `jq_bool` (predicate), `json_subset` (subset), `jq_value` (path extraction), `parse_equals` + `type_aware_equal` (strict value equality) — via one new helper `evaluate_http_assertions(result, …)`. A **second new helper `compile_jq(expr)`** is added for startup validation (D5). | No new assertion logic; the engine is already transport-agnostic. `coerce_db_value` is **not** reused — HTTP response bodies are JSON-native (`json.loads`), so DB-cell coercion (Decimal/datetime/UUID) is a silent no-op there; it stays DB-only. |
| D4 | **Mock `match.jq` coexists with `match.body` (AND).** `HttpMatch` gains `jq: str \| None`. | `body` = declarative subset; `jq` = predicate. Same split `kafka assert` makes between `--contains` and `--match`. Replacing `body` is a breaking change that drops the subset semantic and the shared `json_subset` reuse. |
| D5 | **All config jq match expressions are pre-compiled at startup and in `config validate` via `compile_jq(expr)`; a compile error → `ConfigError` (exit 2). Runtime predicate errors → soft non-match (via `jq_bool`).** Covers **both** HTTP `match.jq` and the existing Kafka reactor `match`. | A typo is an authoring bug; `jq_bool` would swallow it to `False` (silent mis-match / inert reactor — the exact false-green to prevent). `compile_jq` runs `_jq().compile(expr)` with no `.input().all()`, letting `ValueError` propagate as `ConfigError`. Extending to the Kafka reactor removes an asymmetry *and* fixes a latent bug (a typo in a reactor `match` today silently makes the reactor inert). |
| D6 | **jq is imported at mock engine startup when any stub uses `match.jq`**, so a missing `jq` extra fails cleanly (one envelope, exit 2) before any request is served. | A lazy import that first triggers inside an HTTP handler thread cannot emit a clean envelope mid-request. Startup-time import mirrors the existing "broker unreachable at startup → exit 2" guarantee. |
| D7 | **New dedicated `jq` extra** (`jq = ["jq>=1.6"]`); the missing-library hint points HTTP/mock users at `pip install 'agctl[jq]'`. **Mechanism:** the shared `_jq()` stays transport-agnostic (no signature change); `compile_jq` and `evaluate_http_assertions` catch its `ConfigError` and re-raise with the `agctl[jq]` hint. | Additive; zero change to `kafka`/`db`/`http`/core installs (kafka/db still bundle jq). Bundling jq into `http` penalizes every httpx user and does not help bare mock-only users; pointing the hint at an existing extra ("install kafka for an HTTP feature") is confusing. |
| D8 | **`--jq-path` and `--equals` are a pair** (both or neither); one without the other → `ConfigError` (exit 2). | Renamed from `--path` (which collides with the URL `--path` on `http request`/`http ping` — Click cannot register both; verified `http_commands.py:197,376`). `--jq-path` follows the **db** `--path`+`--equals` convention (a value-equality pair), not the **kafka** `--path` convention (which narrows `--contains` and takes no `--equals`); this divergence is documented. Unlike db (which gates value-assertion behind a `--expect-value` selector), http enters assertion mode on flag-presence, so no `--expect-*` selector is added. |
| D9 | **HTTP assertion failure keeps the envelope contract and preserves debuggability.** `result: null` (the envelope sets `null` on any raised exception); `error.type:"AssertionError"`; `error.detail` carries **both** the full HTTP response and a structured failures list. exit 1. | `@envelope` sets `result:null` on exceptions and does not offer a "partial-result-on-failure" path; changing that signature is out of scope (shared infra). To avoid discarding the response (a debuggability regression flagged in review), the full response rides in `error.detail.response`; the per-mode failures ride in `error.detail.failures`. This is HTTP-specific (db/kafka do not embed their subject) — it is *not* claimed to mirror db/kafka. |
| D10 | **`--match` semantics are "any truthy output" (jq_bool), and this is documented.** | `jq_bool` returns true if the expression yields ANY truthy output, so `--match '.items[].amount > 100'` means "at least one item," not "all." Documented explicitly (§6) with the correct "all" jq form `all(.items[]; .amount > 100)` (semicolon generator;condition — the comma form `all(.items[].amount > 100)` is invalid jq and inverts through `jq_bool`); a `--match-all` sibling is deferred. |

## 6. Command Contract — HTTP assertion flags (Feature 2)

Additive optional flags on `agctl http call` and `agctl http request`:

```
agctl http call <template-name>
    [--param key=value]
    [--body '{...}']
    [--header key=value]
    [--timeout <seconds>]
    # ── new assertion flags (all optional; >=1 => assertion mode) ──
    [--status <code>]            # exact int; asserts result.status_code
    [--contains '{...}']         # json_subset against result.body
    [--match <jq-expr>]          # jq_bool predicate against result.body (ANY truthy output)
    [--jq-path <jq-expr>]        # jq path into result.body (requires --equals)
    [--equals <value>]           # expected value; JSON-parsed when valid (requires --jq-path)
```

- **Zero assertion flags => unchanged behavior** (`ok:true`, exit `0`, even on 4xx/5xx; the
  response is a result, not an assertion).
- **>=1 flag => assertion mode.** All active flags must pass (AND, like `kafka assert`).
  - Pass => normal `http.call` / `http.request` result, `ok:true`, exit `0`.
  - Fail => raise `AssertionFailure` => `ok:false`, `error.type:"AssertionError"`,
    `result:null`, exit `1`. `error.detail` is:
    `{ "response": {status_code, body, headers, url, method, response_time_ms},
       "failures": [ {"mode","expected","actual"}, ... ] }`
    listing **every** failing mode (no short-circuit). The full response is always present
    under `response` regardless of which mode failed, so shell-piping agents can read
    `.error.detail.response.body` to diagnose.
- **`--jq-path` / `--equals` pairing (D8):** one without the other => `ConfigError` (exit 2).
- **`--match` ANY semantic (D10):** `jq_bool` is true on any truthy output.
  `--match '.items[].amount > 100'` means ">=1 item qualifies." To assert "all items,"
  use the SEMICOLON form `--match 'all(.items[]; .amount > 100)'` (or
  `[.items[]|select(.amount <= 100)] | length == 0`). The comma form
  `all(.items[].amount > 100)` is invalid jq — do not use it. Note `all(...)` is vacuously
  true over an empty collection; require non-empty with
  `(.items | length > 0) and all(.items[]; .amount > 100)` when emptiness should fail.
- **Non-JSON body:** `--contains` (`json_subset`) cannot structurally match a scalar body
  and fails loudly (`AssertionFailure`) when the body is not a JSON object/array.
  `--jq-path` and `--match` evaluate jq against whatever `result.body` is — a JSON scalar
  (or even a raw text string) is a valid jq input, so they fail normally (path yields no
  value / predicate false) rather than via a body-shape guard (`jq_value` does not
  self-guard on scalars, and none is added). `--status` is always independent of body shape.
- **Missing `jq` extra** when `--match`/`--jq-path` is used => `ConfigError` (exit 2) via
  the lazy `_jq()`, pointing at `pip install 'agctl[jq]'`.

The command tag remains `http.call` / `http.request`; assertion mode changes only the exit
code and the `error` object, not the `command` field.

## 7. Config Schema — `HttpMatch.jq` (Feature 1)

`HttpMatch` (`agctl/config/models.py`) gains one optional field; `match.body` is unchanged.
Backward-compatible — existing stub configs are unaffected.

### 7.1 Config contract (YAML)

```yaml
mocks:
  http:
    stubs:
      create-order-high-value:
        method: POST
        path: "/api/v1/orders"
        match:
          body: { "priority": "high" }   # existing: json_subset containment (optional)
          jq: '.amount > 1000'           # new: jq_bool predicate (optional)
        response:
          status: 201
          body: { order_id: "{customer_id}-mock", status: "APPROVED" }

      create-order-low-value:
        method: POST
        path: "/api/v1/orders"
        match:
          jq: '.amount <= 1000'
        response:
          status: 202
          body: { order_id: "{customer_id}-mock", status: "QUEUED" }
```

### 7.2 Pydantic model shape

| Model | Fields |
|---|---|
| `HttpMatch` | `body: dict \| None` (subset filter, unchanged), **`jq: str \| None`** (jq predicate, new) |

Both optional; a stub matches iff method ∧ path ∧ (`body` absent or `json_subset` passes) ∧ (`jq` absent or `jq_bool` passes).

## 8. Behavior & Semantics

### 8.1 HTTP stub matching with `match.jq`

In `mock/http_server.py::_handle_request`, evaluate `jq_bool(parsed_body, match.jq)`
immediately after the existing `json_subset(match.body, …)` check:

- **Match** — proceed to the reaction (existing path: capture top-level body keys,
  `{placeholder}`-render `response.body`/`headers`, apply `delay_ms`, emit `http.hit`).
- **Non-match (predicate returns false)** — this stub does not match; fall through to the
  next stub in mapping order, else `404` + `http.unmatched`.
- **Predicate raises (data-dependent eval error)** — treated as **non-match** (soft), via
  `jq_bool`'s existing swallow-on-error. The request falls through; `http.unmatched`
  surfaces a 404 if no stub matches.
- **Stub ordering** — first stub whose full predicate (method + path + body + jq) passes
  wins (`dict` insertion order, already relied upon). `config validate` **warns** when two
  stubs share method + path and are distinguished only by `jq` (mandatory, mirroring the
  existing path-template-shadowing check — §10).

> **Wrong-branch false-green (load-bearing, see §14).** When two stubs share method+path
> and differ only by predicate (the branching use case), a subtly-wrong `jq` routes the
> request to the *other* branch's stub — which returns 2xx and emits `http.hit`, **not**
> `http.unmatched`. The §3.5 log-grep protocol does not catch this, and D5 catches only
> syntax errors, not logic errors. Mitigation: pair branching stubs with a Feature-2
> response assertion that distinguishes branches (e.g.
> `http call create-order --jq-path '.status' --equals '"APPROVED"'`); a wrong-branch fire
> then fails loudly (exit 1).

### 8.2 Startup pre-compile (D5) and import (D6)

- **Pre-compile via `compile_jq` (not `jq_bool`).** At `MockEngine` startup (and in
  `config validate`), every stub's `match.jq` **and** every Kafka reactor's `match` is
  compiled once via the new `compile_jq(expr)` helper, which runs `_jq().compile(expr)`
  with no `.input().all()` and **converts** any compile-time exception (e.g. `ValueError`)
  to `ConfigError` explicitly — so the surfaced error type is `ConfigError` (exit 2), not
  `InternalError` from the envelope's catch-all. This is distinct from `jq_bool`, which
  wraps compile+eval in `except Exception: return False` and would silently swallow a typo.
  A compile error => `ConfigError` envelope, exit 2 — *before* any request is served. This
  is the load-bearing guard against a typo silently mis-matching every request / making a
  reactor inert.
- **Validation-only, not a runtime cache.** The startup compile is a *guard*, not a
  cached compiled program: runtime matching still re-compiles per call via `jq_bool`
  (cheap; caching compiled programs is an optional future optimization, not assumed here).
- **Startup import:** if any stub defines `match.jq` (or any reactor uses `match`), the jq
  library is imported at engine startup (not on first request). A missing `jq` extra =>
  `ConfigError` (exit 2) with the install hint, emitted as the single startup envelope.
- A mock with **no** `match.jq`/reactor-`match` triggers neither — HTTP-only mock stays
  zero-dep (stdlib only), unchanged. (Kafka reactors already require the `kafka` extra,
  which bundles `jq`.)

### 8.3 HTTP response assertions (Feature 2)

A new helper `evaluate_http_assertions(result, status, contains, match, jq_path, equals)`
in `agctl/assertions.py` composes the runtime primitives and is called from the
`http call` / `http request` `_core` (wrapped by `@envelope`) when any assertion flag is
present:

| Flag | Evaluation |
|---|---|
| `--status <code>` | `result.status_code == code` |
| `--contains '{...}'` | `json_subset(contains, result.body)` |
| `--match <jq>` | `jq_bool(result.body, match)` — true on ANY truthy output (D10) |
| `--jq-path <jq> --equals <v>` | `type_aware_equal(jq_value(result.body, jq_path), parse_equals(equals))` |

All active flags AND; on failure, `error.detail.failures` lists **every** failing mode (no
short-circuit), each a pinned shape agents can parse:

| `mode` | failure-entry fields |
|---|---|
| `status` | `{mode:"status", expected:<int>, actual:<status_code int>}` |
| `contains` | `{mode:"contains", needle:<--contains JSON>, matched:false}` (no `actual` — the full body is in `response`) |
| `match` | `{mode:"match", expr:<--match string>, result:false}` (predicate; no extracted `actual`) |
| `jq-path` | `{mode:"jq-path", path:<--jq-path expr>, expected:<parse_equals(--equals)>, actual:<jq_value result or null>}` |

On success the command returns the unchanged HTTP result. On failure the full response is
preserved in `error.detail.response` (D9).

## 9. Error & Exit-Code Model

No new error types; the existing `AgctlError` hierarchy covers every path.

| Failure | Type | Exit | When |
|---|---|---|---|
| `--jq-path` without `--equals` or vice versa | `ConfigError` | 2 | call time (D8) |
| `--match`/`--jq-path` used and `jq` extra missing | `ConfigError` | 2 | call time (lazy `_jq` -> install hint) |
| Any assertion flag evaluates false | `AssertionFailure` | 1 | after the request (D9); full response + per-mode failures in `error.detail` |
| `--contains`/`--jq-path` against a non-JSON body | `AssertionFailure` | 1 | after the request (loud; cannot structurally match) |
| Malformed `match.jq` or reactor `match` (compile error) | `ConfigError` | 2 | mock startup / `config validate` (D5) |
| Stub uses `match.jq` and `jq` extra missing | `ConfigError` | 2 | mock startup (D6) |
| `match.jq` predicate raises at request time | (non-match, soft) | — | falls through to next stub / 404 |
| Wrong-branch match (predicate logic error) | (no direct signal — stub returns 2xx + `http.hit`) | — | mitigated by pairing with a Feature-2 response assertion (§8.1, §14) |

## 10. Validation Rules (`agctl/config/validator.py`)

- **Pre-compile all match expressions.** During `agctl config validate`, compile each
  stub's `match.jq` **and** each Kafka reactor's `match` via `compile_jq`; a compile error
  is a validation error (exit 2). (Like the existing `mocks.kafka` cross-ref, this runs in
  `config validate`; `mock run` performs its own equivalent startup guard per ARCHITECTURE §5.)
- **jq-shadowing warning (mandatory).** Warn when two stubs share method + path and are
  distinguished only by `jq` (fragile first-match-wins; the wrong-branch surface in §8.1).
  **Must gate on method equality** — the existing path-template-shadowing check
  (`validator.py`) compares path segments only and is method-agnostic (a known limitation);
  copying it blindly would false-warn across methods (e.g. `GET /api/{id}` vs
  `DELETE /api/users`).
- All existing validation (unresolved `${ENV}`, dangling refs, missing-`description`
  warnings, path-template shadowing) is unchanged.

## 11. Discovery Changes

Minimal and additive:

- **Feature 2:** no structural change. Assertion flags are call-time; the `http-templates`
  Level-2 detail already reports `method`/`path`/`params`/`example`. Example commands *may*
  illustrate `--status`/`--match`, but the schema is unchanged.
- **Feature 1:** the mock-stub Level-2 detail already reports `match`; the serializer
  includes `match.jq` when present. No new category or field beyond surfacing it.

## 12. Packaging

- **New `jq` extra** in `pyproject.toml`: `jq = ["jq>=1.6"]` under
  `[project.optional-dependencies]`.
- `kafka` and `db` extras unchanged (they already bundle `jq`) — so the hint for users on
  those transports keeps the bundled-extras form; the new `agctl[jq]` form targets
  HTTP-client-assertion and HTTP-only-mock users.
- The lazy `_jq()` missing-library `ConfigError` hint is **context-aware** (points HTTP/mock
  users at `pip install 'agctl[jq]'`).
- **No `version` bump**, no new entry-point group, no new command group. Both features are
  wired into existing commands/config.

## 13. Testing Strategy

Mirrors the existing test architecture and seams (ARCHITECTURE §12).

**Unit (no network):**
- `evaluate_http_assertions`: each flag in isolation; AND combinations; `--match` any/truthy
  semantic; failure `error.detail` carries full `response` + per-mode `failures` (no
  short-circuit); `--jq-path`/`--equals` pairing => `ConfigError`; missing `jq`
  (monkeypatch the lazy import) => `ConfigError`.
- `compile_jq`: a malformed expression raises/propagates as `ConfigError` (loud); a valid
  expression compiles (does not feed input). Verify `jq_bool` still swallows the same
  expression to `False` (proving the two helpers differ).
- `http_commands`: assertion flags wired through `@envelope`; pass => exit 0 with full
  result; fail => exit 1 with `AssertionError` and `error.detail.response` populated; zero
  flags => unchanged (regression guard).
- `mock/http_server` (injectable `handler_factory`): `match.jq` pass => reaction;
  non-match => fall-through to next stub / 404 + `http.unmatched`; coexist with `match.body`
  (both must pass); predicate eval error => soft non-match.
- `MockEngine` startup + `kafka_reactor`: pre-compile catches a malformed `match.jq` and a
  malformed reactor `match` => `ConfigError` before serving; missing `jq` extra at startup
  => `ConfigError`; stubs/reactors with no jq expression import nothing (zero-dep preserved
  for HTTP-only).
- `config/validator`: pre-compiles `match.jq` and reactor `match`; **mandatory**
  jq-shadowing warning (two stubs, same method+path, jq-only distinction).

**Integration (self-skipping):** under `AGCTL_TEST_LIVE=1`, drive the local threaded HTTP
mock: `http call --status 201 --jq-path '.status' --equals '"APPROVED"'` against a stub
with `match.jq`, asserting conditional responses and exit codes; verify a wrong-branch fire
is caught by the response assertion. Self-skips otherwise.

New files: extend `tests/unit/test_assertions.py`, `tests/unit/test_http_commands.py`,
`tests/unit/test_mock_http_server.py` (or equivalent), `tests/unit/test_config_validator.py`,
`tests/unit/test_mock_kafka_reactor.py`; extend `tests/integration/test_http_commands.py` /
`test_mock_commands.py`.

## 14. Deferred & Not-Covered

- **Response extraction (`--capture path=name`).** Agents still hand-roll `jq -r` to pull a
  field; a built-in capture flag (surfacing values in the result envelope) is a follow-up.
- **`--match-all` flag** (the "every item" case; today covered by a jq idiom).
- **Nested-field / header capture** in mock responses (affects Kafka reactors too;
  symmetric enhancement, not one-sided).
- **`http ping` assertions** (streaming line shape extension).
- **`--status` range matching** (`2xx`/`4xx`).
- **Caching compiled jq programs** at runtime (startup compile is validation-only).
- **A dedicated mock/stub-jq example in the `agctl-config` skill** beyond the standard
  authoring reference.

### Known-wrong-result note (honest)

`match.jq` **does** introduce a new false-green surface that `match.body` did not have:
when two stubs share method+path and differ only by predicate (the branching use case), a
subtly-wrong `jq` routes a request to the *other* branch's stub, which returns 2xx and
emits `http.hit` — **not** `http.unmatched` — so the §3.5 log-grep protocol does not catch
it. D5 catches only syntax errors, not logic errors. This is mitigated, not eliminated, by:
(a) the mandatory jq-shadowing warning (§10), and (b) pairing branching stubs with a
Feature-2 response assertion (§8.1) that distinguishes branches, turning a wrong-branch
fire into a loud exit 1. A direct `http.branch_ambiguous` signal was considered and
deferred (over-engineering for v1). The pre-compile guard (D5) does remove the *other*
new footgun — a typo silently mis-matching every request — which would otherwise be the
worst-case false-green.

## 15. Rejected Alternatives (ADR-style)

- **A separate `http assert` command.** Rejected (D1): assertion composes onto `http call`'s
  existing result; a separate command duplicates the request machinery. DESIGN §8 forbids
  overlapping commands. (`kafka assert` earns separation via its consume-window; db assert's
  separation is conventional — neither forces HTTP to follow.)
- **`--path` as the assertion flag name.** Rejected (D8): collides with the URL `--path` on
  `http request`/`http ping`. Renamed to `--jq-path`.
- **jq replaces `match.body`.** Rejected (D4): breaking change to v0.2.0; drops the
  declarative subset semantic and the shared `json_subset` reuse; breaks the coexist
  precedent every other transport follows.
- **Request-body construction via jq.** Rejected: YAGNI; `{placeholder}` + `--body`
  deep-merge covers building outgoing bodies.
- **Response extraction (`--capture`) in v1.** Rejected for v1: keeps scope on the
  fail-loudly goal; deferred (§14).
- **Bundle `jq` into the `http` extra.** Rejected (D7): penalizes every httpx user even
  when they never assert, and does not help bare mock-only users.
- **Lazy-only packaging, hint at an existing extra.** Rejected (D7): "install
  `agctl[kafka]` to use an HTTP feature" is confusing.
- **Reuse `jq_bool` for the D5 startup compile.** Rejected (D3/D5): `jq_bool` swallows
  compile errors to `False`; it would silently defeat the load-bearing loud-on-typo guard.
  A compile-only `compile_jq` is required.
- **Returning the response in `result` on failure.** Rejected (D9): would require changing
  the shared `@envelope` signature (which sets `result:null` on exceptions) for one
  transport. The response rides in `error.detail.response` instead.
- **`--status` range matching in v1.** Rejected: exact code keeps v1 simple; ranges deferred.

## 16. Docs & Skill Impact

- **DESIGN.md** — §2 note that HTTP templates gain no jq (request building stays
  `{placeholder}`); §3.1 add the assertion flags to `http call`/`request` (note
  `--jq-path`, not `--path`); §3.5/§4 note `match.jq` on mock stubs (coexists with
  `match.body`) and that reactor `match` now pre-compiles; §10 add the deferred items
  (extraction, `--match-all`).
- **ARCHITECTURE.md** — §8 note the new assertion flags on the HTTP client path;
  §9 reference `evaluate_http_assertions` + `compile_jq` and the pre-compile guard covering
  HTTP stubs and Kafka reactors; §11 add the `jq` extra and the "imported at mock startup
  when a stub/reactor uses jq" rule.
- **`skills/agctl-config`** — extend the mock authoring reference with `match.jq`
  (coexists with `body`; compile errors fail loud; eval errors soft; wrong-branch caveat +
  response-assertion mitigation), and the http-template reference with the assertion flags
  (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`, the `--match` any-semantic).
- **`README.md`** — surface the assertion flags on `http call`/`request`, and
  `pip install 'agctl[jq]'`.

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").
