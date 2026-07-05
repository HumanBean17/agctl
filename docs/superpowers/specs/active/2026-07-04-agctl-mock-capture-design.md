# Design: Mock Capture — envelope-rooted `capture:` for HTTP stubs & Kafka reactors

**Status:** Approved (design) — ready for implementation plan
**Date:** 2026-07-04
**Author:** brainstorming session
**Affects:** config models (`HttpStub`, `KafkaReactor`, new `CaptureSpec`), `agctl/resolution.py` (`fill_placeholders`), `agctl/mock/http_server.py`, `agctl/mock/kafka_reactor.py`, `agctl/mock/jq_precompile.py`, `agctl/config/validator.py`, `agctl-config` skill
**Relation to docs:** Closes two DESIGN §10 deferred items — *nested-field / header capture* and *JSON-type pass-through*. On implementation, DESIGN.md (§2.1, §10) and ARCHITECTURE.md (§9, §15) are synced via `docs-watcher`. The residual `match`/`capture` `.`-root divergence is tracked separately in **#22** (breaking change, deferred).

---

## 1. Background & Problem

`agctl mock` (shipped per the parent spec, `2026-07-03-agctl-mock-server-design.md`)
captures values from an incoming HTTP request / Kafka message and interpolates them
into the response / reaction via `{placeholder}` substitution. Today's capture is
**implicit, top-level-keys-only, and stringified**:

- **HTTP** (`agctl/mock/http_server.py:266-272`) — capture context = path params
  (`routing.py`) ∪ top-level keys of the parsed JSON body; every value coerced via
  `str()`.
- **Kafka** (`agctl/mock/kafka_reactor.py:148`) — capture context = top-level keys
  of the parsed message value; every value coerced via `str()`.

Battle-testing agctl as a drop-in replacement for four real mock servers surfaced
three gaps that this stringification + flat-key limitation makes unmockable:

- **① HTTP compound-path capture.** `graphql-operatorById` needs
  `.variables.id` (nested object in the GraphQL body); `epk-chatSearch` needs
  `.clientInfoCriteria.firstName` and `.ucpID`. Top-level-only capture cannot
  reach them.
- **② Kafka key / header capture.** `chatx-it-mock` must echo the message **key**
  and a **header** (`rqUID`) into the reaction. The reactor reads only
  `msg["value"]`; the decoded `msg["key"]` / `msg["headers"]`
  (`agctl/clients/kafka_client.py:542-568`) are discarded by capture.
- **③ Object pass-through.** `chatx-it-mock`'s `contextEcho` copies the `context`
  object from request value to reaction value (the E2E `kafkaThreadHistoryFlow`
  asserts exactly this). Today `str(dict)` produces Python repr (single quotes,
  `True`/`None`) — not JSON — so the downstream strict parse rejects it.

All three are **already documented as deferred**: DESIGN §10 ("nested-field /
header capture"; "JSON-type pass-through") and ARCHITECTURE §15 ("Header-borne
correlation IDs … reactor cannot capture from headers"; "captured values are
stringified → strict downstream may reject → false failure"). Battle-testing
promotes them from deferred to **now-blocking**.

## 2. Goals

- Close gaps ①, ②, ③ with **one** mechanism, on both transports.
- **Reuse** the existing jq extractor (`agctl/assertions.py:60-72`, `jq_value`)
  and the existing `{placeholder}` engine (`agctl/resolution.py:26-46`) — no new
  primitive, no new templating language.
- Preserve **fail loudly**: capture misconfiguration is caught at startup (exit 2),
  not at react time.
- **Backward compatible**: no existing stub/reactor config changes; no `version`
  bump (additive under major `1`).

## 3. Scope & Design Constraints

- **Additive only.** A new optional `capture` field on `HttpStub` and
  `KafkaReactor`. Implicit capture and `match` are untouched.
- **Envelope-rooted capture.** `capture.<name>.from` is a jq path evaluated
  against the whole incoming message envelope (HTTP request / Kafka message), so
  nested fields, key, and headers are all reachable through one mechanism.
- **`match` stays payload-rooted.** Today's `match.jq` (HTTP) and reactor `match`
  (Kafka) remain body/value-rooted. Migrating them to the envelope root is a
  breaking change tracked in **#22**, deliberately out of scope here. The
  resulting `.`-root divergence (match = payload, capture = envelope) is the
  accepted cost; §11 documents it.
- **Stateless.** Capture extracts from the single triggering message; it carries
  no cross-call state (inherits D5 from the parent spec).
- **JSON bodies only** (inherited): HTTP `.body` is the parsed JSON body (or
  `null`); Kafka `.value` is the parsed JSON value. Non-JSON capture is out of
  scope (§9).

## 4. Non-Goals

- Migrating `match`/`match.jq` to the envelope root (**#22**).
- HTTP query-string parsing (no `.query` envelope field yet).
- Capture from non-JSON HTTP bodies or non-object Kafka values (jq over `null`
  yields `null` → a `capture.missing` event; not a decode feature).
- Schema Registry / Avro / Protobuf decoding (inherited limitation,
  ARCHITECTURE §15).
- A capture *merge* directive (e.g. `merge_from_value`). Object pass-through is
  handled by `type: object`, not by a separate merge operation (§12).

## 5. Decisions (recorded with rationale)

| # | Decision | Rationale |
|---|---|---|
| **C1** | **One unified envelope-rooted `capture:` map** on both `HttpStub` and `KafkaReactor`, each entry `{ from, type }`. | One mechanism covers all three gaps; reuses `jq_value`; avoids three transport-specific syntaxes. |
| **C2** | **`from` roots at the message envelope** (HTTP request / Kafka message); **`match` stays payload-rooted.** | Envelope rooting reaches nested fields, key, and headers uniformly. `match` is not migrated (breaking — #22); the `.` divergence is accepted and documented (§11). |
| **C3** | **Three substitution types** — `scalar` (default; `str()` inline), `object` (live-object pass-through, whole-field placeholder only), `json` (`json.dumps()` as a string). | `scalar` preserves today's behavior (backward compat). `object` is the only way to satisfy ③'s true-object field. `json` covers fields that must carry a JSON *string*. |
| **C4** | **Keep implicit capture; explicit `capture:` overrides on name collision** — value **and** type. | No config migration; existing mocks unchanged. Gap ③ is closed by adding an explicit `object`-typed entry that overrides the implicit stringified one. |
| **C5** | **Reuse `jq_value(envelope, from)`** for every capture; no new extractor. | Inherits the parent spec's "no reinvention" (D9). `jq_value` already swallows compile/runtime errors to `None` (ARCHITECTURE §9), which feeds the runtime `capture.missing` policy (C7). |
| **C6** | **Fail loud at startup** — extend the jq pre-compile walker (`agctl/mock/jq_precompile.py`) to compile every `capture.*.from`, and statically reject an `object`-typed name used anywhere except a whole-field placeholder. | Matches `match.jq` treatment (compile loud at startup, exit 2). Catches typos and the one ill-defined `object` usage before any request/message is processed. |
| **C7** | **Runtime missing-path is non-fatal** — a `from` resolving to `null`/missing emits a `capture.missing` NDJSON event and substitutes empty string; the mock continues. | A mock must not die mid-test on one bad message (inherits D8). The event is visible in the agent failure-stream protocol (parent §10.1), so false greens still surface. |

## 6. Config Contract

### 6.1 YAML

```yaml
mocks:
  http:
    stubs:
      graphql-operatorById:
        method: POST
        path: /graphql
        match: { jq: '.query | test("operatorById")' }        # body-rooted (unchanged)
        capture:                                                # NEW (envelope-rooted)
          op_id: { from: ".body.variables.id" }                # ① nested path
        response:
          body: { id: "{op_id}" }                               # {op_id} stays a simple name

      epk-chatSearch:
        method: POST
        path: /chat/search
        capture:
          fname: { from: ".body.clientInfoCriteria.firstName" }# ①
          ucp:   { from: ".body.ucpID" }                       # ①
        response:
          body: { firstName: "{fname}", ucpID: "{ucp}" }

  kafka:
    reactors:
      chatx-it-mock:
        topic: chatx.commands
        match: '.command == "SEARCH"'                          # unchanged (value-rooted)
        capture:                                                # NEW
          tid:   { from: ".key" }                              # ② message key
          rqUID: { from: ".headers.rqUID" }                    # ② header (case-sensitive)
          ctx:   { from: ".value.context", type: object }      # ③ pass-through
        reaction:
          topic: chatx.events
          value:
            threadId: "{tid}"
            rs_headers: { rqUID: "{rqUID}" }
            context: "{ctx}"                                    # {ctx} is the whole field → object
```

### 6.2 Pydantic models (`agctl/config/models.py`)

A single shared spec, reused on both transports (the `.` root differs by transport
context — §7.1):

| Model | Fields |
|---|---|
| `CaptureSpec` (new) | `from_: str` (YAML key `from`, aliased — `from` is a Python keyword; `populate_by_name=True`); `type: Literal["scalar","object","json"] = "scalar"` |
| `HttpStub` (extended) | adds `capture: dict[str, CaptureSpec] \| None = None` (peer of `match`) |
| `KafkaReactor` (extended) | adds `capture: dict[str, CaptureSpec] \| None = None` (peer of `match`, on the consumer side — capture reads the *incoming* message) |

Notes:
- `capture` is **optional** on both; absence = today's implicit-only behavior.
- `CaptureSpec` lives in `config/models.py` alongside the other mock models
  (preserves config's dependency isolation — parent spec §7.2 / ARCHITECTURE §3).

## 7. Capture Resolution Rules

### 7.1 Envelope roots

`from` is evaluated by `jq_value(envelope, from)`, where the envelope is:

- **HTTP request envelope** (built in `http_server.py::_handle_request`):
  `{ method, path, headers, body }`, where
  - `method` — the request method, as received;
  - `path` — the request path **without query string** (query parsing is #9/§4);
  - `headers` — request headers as a dict with **lowercased keys** (HTTP headers
    are case-insensitive; lowercasing removes the `.Authorization` vs
    `.authorization` footgun);
  - `body` — the parsed JSON body (`dict`/`list`/scalar) or `null`.
- **Kafka message envelope** — the already-normalized message
  (`agctl/clients/kafka_client.py:542-568`):
  `{ key, value, partition, offset, timestamp, headers }`, where
  - `key` — the decoded message key (`str | None`);
  - `value` — the parsed JSON value (`dict`/`list`/scalar) or `null`;
  - `headers` — header dict **with names as-produced** (Kafka header keys are
    case-sensitive bytes; do **not** lowercase — the producer's exact name is the
    lookup key, e.g. `.headers.rqUID`).

The HTTP/Kafka header-casing asymmetry reflects the transports' own conventions and
is documented in the skill (§13).

### 7.2 Type semantics on substitution (the gap-③ fix)

The capture context becomes a mapping of `name → (value, type)`. Implicit captures
(path params, top-level body/value keys) enter the context as `type = scalar`.
The substitution engine (`agctl/resolution.py::fill_placeholders`) consults the
type per name:

- **`scalar`** (default): substitute `str(value)` inline — today's behavior,
  backward-compatible. Valid inline or as a whole field.
- **`json`**: substitute `json.dumps(value)` as a string. Valid inline or as a
  whole field (a field that must carry a JSON *string* value).
- **`object`**: substitute the **live Python object**. Legal **only when the
  placeholder token is the sole content of its field** (the field string is
  exactly `"{name}"`). This is the only mode that yields a real object field —
  what `contextEcho` requires.

The `{name}` reference syntax is unchanged: simple names matching
`[A-Za-z_][A-Za-z0-9_]*` (`resolution.py:18`). Compound paths live exclusively in
`from` (jq), never inside the braces — so `{op_id}` references the captured name
regardless of how deeply `from` reached into the envelope.

### 7.3 Conflict with implicit capture (C4)

When a name appears in both the implicit context and an explicit `capture:` entry,
the **explicit entry wins**, supplying both its value (re-extracted from the
envelope via `from`) and its type. This is precisely how gap ③ is closed without
disabling implicit capture: `ctx: { from: ".value.context", type: object }`
overrides the implicit stringified `ctx`, so `context: "{ctx}"` renders as a real
object.

## 8. Error & Exit-Code Model

| Failure | Type | Exit | When |
|---|---|---|---|
| Malformed jq in any `capture.*.from` | `ConfigError` | 2 | startup — `mock run` Step 0 / `config validate` (C6) |
| `object`-typed name used in a non-whole-field position, or in a string-only slot (`reaction.key`, a header value) | `ConfigError` | 2 | startup — static check over `response.body` / `reaction.{value,key,headers}` (C6) |
| `from` resolves to `null`/missing at runtime | `capture.missing` event (non-fatal) + empty-string substitution | — | runtime (C7); visible in the failure-stream protocol |

The new **`capture.missing`** NDJSON event (one line, single-writer — parent §10):

```json
{"event":"capture.missing","stub":"epk-chatSearch","name":"ucp","from":".body.ucpID","timestamp":"…"}
{"event":"capture.missing","reactor":"chatx-it-mock","name":"rqUID","from":".headers.rqUID","timestamp":"…"}
```

It joins the existing failure-surface events (`http.unmatched`,
`http.body_parse_skipped`, `kafka.skipped`, `kafka.error`) and is added to the
agent failure-stream grep set (parent §10.1).

## 9. Validation Rules (`agctl/config/validator.py` + `agctl/mock/jq_precompile.py`)

1. **Extend the jq pre-compile walker** (`jq_precompile.py:26-51`) to yield
   `(label, expr)` for every `capture.*.from` on every HTTP stub and Kafka
   reactor, in addition to today's `match.jq` / reactor `match` yields. A malformed
   `from` raises `ConfigError` at startup via `compile_jq` (`assertions.py:75-106`)
   and appears in `collect_jq_compile_errors` for `config validate`. Labels follow
   `mocks.http.stubs.<name>.capture.<cap>.from` / `mocks.kafka.reactors.<name>.capture.<cap>.from`.
2. **Static `object`-placement check.** For each `object`-typed capture name, scan
   the stub/reactor's template tree (`response.body`; `reaction.value`, `reaction.key`,
   `reaction.headers`). The name may appear only as a field whose string value is
   exactly `"{name}"`, and only within a structurally-typed (non-string) position —
   i.e. inside `response.body` / `reaction.value`. Referencing it inline within a
   larger string, or in `reaction.key` / a header value (string-only slots), is a
   `ConfigError` at startup.
3. **Name-collision is allowed** (C4 — explicit overrides implicit); not a warning.

## 10. Testing Strategy

Mirrors the existing mock test seams (parent §15 / ARCHITECTURE §12).

**Unit (no network):**
- CaptureSpec model: `from` alias round-trip; `type` default `scalar`; invalid
  `type` rejected.
- HTTP capture matrix, per type × source:
  - nested body path (`.body.variables.id`) — gap ①;
  - header (`.headers.authorization`, lowercased) — bonus reach;
  - `object` pass-through renders a real object field (gap ③ analog);
  - `json` renders a JSON string; `scalar` renders `str()`.
- Kafka capture matrix, per type × source:
  - `.key` (gap ②); `.headers.rqUID` case-sensitive (gap ②);
  - `.value.context` with `type: object` → real object (gap ③, `contextEcho`);
  - scalar/json as above.
- **Conflict (C4):** an explicit `object` entry overrides the implicit stringified
  same-name capture (type promotion).
- **Fail loud (C6):** bad `from` jq → `ConfigError` at startup (both `config
  validate` and `mock run` Step 0); `object` name used inline / in `reaction.key`
  → startup `ConfigError`.
- **Runtime miss (C7):** `from` → `null` emits `capture.missing` and substitutes
  empty string; mock continues.
- jq pre-compile walker yields `capture.*.from` labels (extend the existing
  walker unit test).

**Integration / E2E (acceptance):** the four battle-test scenarios as fixtures —
`graphql-operatorById`, `epk-chatSearch`, `chatx-it-mock` (incl. `contextEcho` /
`kafkaThreadHistoryFlow`), `kafka-threadhistory`. Kafka E2E self-skips without
`AGCTL_TEST_LIVE=1` (existing harness).

The dict/list capture case flagged as untested during review is covered by the
`object`-type unit tests.

## 11. Deferred & Out-of-Scope

- **#22 — unify `match` onto the envelope root.** Removes the `.` divergence
  (`match.jq: ".amount > 1000"` vs `capture.x.from: ".body.amount"`). Breaking
  change; needs a major-version bump or a `match_root` compat shim. Tracked as a
  standalone issue.
- HTTP `query` envelope field (query-string parsing).
- Capture from non-JSON HTTP bodies / non-object Kafka values (`.body`/`.value` →
  `null` → `capture.missing`).
- List-index navigation beyond what jq already provides (free today: `.body.items[0].id`).

## 12. Rejected Alternatives (ADR-style)

- **Payload-rooted `from` + `from_key` / `from_header` fields** (capture-root
  option A). Rejected (C1/C2): two field shapes; cannot reach HTTP headers without
  yet more fields; envelope-rooting is the strict superset and is uniform.
- **Per-transport capture syntax** (HTTP body-rooted; Kafka `{__key__}` /
  `{__headers__.x}` special vars). Rejected (C1): three mechanisms for one
  concern; `{__headers__.rqUID}` also breaks the placeholder regex (dots not in
  the name charset).
- **A separate `merge_from_value` / `merge_from` directive** for object
  pass-through. Rejected (C3): `type: object` unifies pass-through under the same
  capture map, avoiding a second template-evaluation mechanism.
- **Switching `scalar` from `str()` to `json.dumps()` (scalar-only).** Rejected
  (C3): inline-spliced JSON text is still a *string* field, so ③'s object-field
  assertion stays broken; only `object` type solves it.
- **Disabling implicit capture when `capture:` is present.** Rejected (C4):
  breaks backward compatibility for no benefit; override semantics suffice.

## 13. Docs & Skill Impact

- **DESIGN.md §2.1 (schema ref)** — add `capture` to `HttpStub` and `KafkaReactor`
  with the `CaptureSpec` shape and the envelope-root note.
- **DESIGN.md §10** — move *nested-field / header capture* and *JSON-type
  pass-through* from "deferred" to "implemented"; add the `.` divergence note
  referencing #22.
- **ARCHITECTURE.md §9 (assertion engine)** — note `jq_value` is now also used for
  mock capture extraction (not only HTTP response assertions).
- **ARCHITECTURE.md §15 (limitations)** — drop the "header-borne correlation IDs
  cannot capture" and "JSON-type pass-through stringified" bullets (now covered);
  add the `match`/`capture` `.` divergence as a known limitation referencing #22.
- **`skills/agctl-config/reference/mocks.md`** — add `capture` authoring: `from` /
  `type`, the HTTP vs Kafka envelope roots (incl. the header-casing asymmetry),
  type semantics, the `object` whole-field-only rule, and override-on-collision.
  Update the "Capture value coercion" section (`object` / `json` now exist). Add
  `capture.missing` to the agent failure-stream grep set (parent §10.1).

Doc sync is performed via the `docs-watcher` subagent after implementation
(CLAUDE.md "Docs Sync").
