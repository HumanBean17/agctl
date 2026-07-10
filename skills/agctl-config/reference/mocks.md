# `mock` mode — author HTTP stubs and Kafka reactors

Add a `mocks:` block so `agctl` can *impersonate* the system's external dependencies
during local testing — an HTTP API the SUT calls, or the downstream Kafka consumer the
SUT expects to react to its events. The non-negotiable contract (placeholder syntaxes,
naming, verify-after) lives in `SKILL.md`; this file is the extraction detail.

Two sub-shapes live under one `mocks:` section. Point at the **dependency the SUT talks to**
(not the SUT itself): an OpenAPI doc / route of the downstream HTTP service, or the
`@KafkaListener` / event schema of the consumer you're standing in for.

```yaml
mocks:
  http:
    listen: "${AGCTL_MOCK_HTTP_HOST:-0.0.0.0}:${AGCTL_MOCK_HTTP_PORT:-18080}"
    stubs:
      create-order: { ... }     # one server, many stubs (path-routed)
  kafka:
    reactors:
      order-command-handler: { ... }   # joins the SUT's real broker as a consumer
```

## ⚠️ The one thing that trips everyone: `{name}` here is *capture*, not `--param`

In an ordinary HTTP template, `{customer_id}` is filled at **call time** by `--param
customer_id=…`. In a mock it is **not**. A mock has no caller passing params — the SUT's
own request *is* the input. So `{name}` means **"capture this from the trigger"**:

- **HTTP** `{name}` in a stub `path` (`/orders/{order_id}`) captures that path segment from
  the incoming request; `{name}` in `response.body`/`response.headers` is then filled from
  the capture context. Implicit capture = path params ∪ top-level keys of the JSON request
  body; an explicit `capture:` block (below) reaches nested fields and headers too.
- **Kafka** `{name}` in `reaction.value`/`key`/`headers` is filled from the capture context.
  Implicit capture = top-level keys of the matched message's JSON value; an explicit
  `capture:` block (below) also reaches the message `.key`, `.headers.*`, and nested
  `.value.*`.

`match.body` (HTTP) and `match` (Kafka jq) are **filters** that narrow *when* the stub/
reactor fires — they are **not** capture sources. `${ENV}` still resolves at config **load**
(`listen`, `consumer_group`, topics) exactly like everywhere else. There is **no** `:name`
(SQL) and **no** `--param` in mock land.

## Inputs

- The config path (resolved in `SKILL.md` Step 0).
- For an HTTP stub: the downstream API the SUT calls — an **OpenAPI/Swagger doc** of the
  real dependency is best; else a controller/route or a plain description
  (`POST /api/v1/orders`, body `{…}`, returns `201 {order_id, status}`).
- For a Kafka reactor: the downstream consumer the SUT expects — an `@KafkaListener` /
  consumer config (the **command** topic it reads) plus the **event** it should emit back
  (an event class / schema for the reaction shape).

## Extraction — HTTP stub (`mocks.http.stubs.<name>`)

1. **listen** — `host:port`; `${ENV}` at load; default `0.0.0.0:18080`. `0.0.0.0` is
   deliberate so a containerized SUT reaches it via `host.docker.internal` / host LAN IP.
2. **method** — any verb; upper-cased at load (write it lowercase or uppercase — same result).
3. **path** — copy the route; each variable segment is `{name}`. **Trailing slash is
   significant** (`/orders` ≠ `/orders/`); the query string is stripped before matching.
4. **match.body** *(optional)* — a JSON subset the request body must contain for this stub
   to fire (reuses `json_subset`, **body-rooted**). Omit = match on method+path alone.
5. **match.jq** *(optional)* — a jq boolean predicate over the **request envelope**
   (`{method, path, headers (lowercased), body}`), so `.body.amount > 1000`,
   `.headers.authorization != null`, `.path == "/x"`. AND-ed with `match.body` when both
   are present. `body` is declarative subset containment (body-rooted); `jq` is an
   envelope-rooted predicate — same split as `kafka assert --contains` vs `--match`.
   Needs `pip install 'agctl[jq]'`.
6. **response** — `status` (100–599), `headers` (optional; `Content-Type` defaults to
   `application/json` for a dict/list body, `text/plain` otherwise), `body` (any). Render
   `{name}` against the capture context.
7. **delay_ms** *(optional)* — simulated latency; capped at 64 concurrent requests (overflow
   → `429`).
8. **name** — kebab-case from the route (`POST /api/v1/orders` → `create-order`).
9. **description** — one line (effectively required per contract #4).

**First-match-wins.** Stubs are matched in YAML mapping order; put a literal segment ahead
of a `{name}` segment at the same position (`/orders/bulk` before `/orders/{order_id}`) or
the param stub silently shadows the literal one (config validate warns on this). Two stubs
sharing **method + path** and distinguished **only by `match.jq`** earn a separate
jq-shadowing warning (see Gotchas).

## Extraction — Kafka reactor (`mocks.kafka.reactors.<name>`)

1. **topic** — the **command** topic to consume from (the SUT publishes here).
2. **consumer_group** *(optional)* — **omit it** unless you have a reason. Omitted → a
   unique per-run group (`agctl-mock-<name>-<runid>`) so a restart never silently resumes
   past messages produced between runs. Pinning a stable group is opt-in and carries a
   resume hazard (see Not covered).
3. **match** *(optional)* — a jq boolean predicate over the **message envelope**
   (`{key, value, partition, offset, timestamp, headers}`), so `.value.command == "CREATE_ORDER"`,
   `.key`, `.headers.rqUID` (header keys are **case-sensitive**). Omit = match all. Non-JSON /
   non-object values never match — they're visibly skipped, not silently dropped.
4. **cluster** *(optional)* — the named cluster this reactor binds to (a key under
   `kafka.clusters`). Omit to fall back to `kafka.default_cluster` or the single defined
   cluster. Set it when the reactor consumes from a non-default cluster; a dangling name is
   a `config validate` error. Each reactor gets its own `KafkaClient` built from the
   resolved cluster (reactors sharing a cluster reuse one client).
5. **reaction** — what to **produce** back: `topic` (the **event** topic), `key` (optional),
   `value` (JSON-serializable), `headers` (optional; **string values only** — a non-string
   value is a config error). Render `{name}` against the message-value context.
6. **name** — kebab-case from the consumer/event.
7. **description** — one line.

**Requires a resolved cluster with brokers.** `mocks.kafka` joins the SUT's *real* broker — it is
not a broker itself. Each reactor resolves a cluster (`reactor.cluster` → `kafka.default_cluster`
→ single-cluster auto-default); the resolved cluster must have non-empty `brokers`, or
`mock run` fails fast at startup (exit 2). `mock run` also needs the `kafka` extra installed
(`pip install 'agctl[kafka]'`). HTTP-only mocks need neither brokers nor the extra.

## Capture value coercion (load-bearing)

Captured values are frequently non-string (numeric IDs, bools, nested objects). How a
value lands in the rendered response/reaction depends on its **type**, and `type` lives
on the capture name (set in `capture:`, below). Implicit captures (path params, top-level
body/value keys) enter as `scalar` and stay that way unless an explicit `capture:` entry
overrides the name (which also promotes the type).

- **`scalar`** (default) — `str(value)` inline. `orderId` `42` → `"42"`, `true` → `"True"`,
  `null` → `"None"`. Valid inline or as a whole field; always yields a string.
- **`object`** — passes the **live Python value** through (a real JSON object/array field,
  not a stringified one). Legal **only as a whole-field placeholder** — a field whose
  string value is exactly `"{name}"`. Used inline within a larger string, or in a
  string-only slot (`reaction.key`, a `reaction.headers` value), it is a startup
  `ConfigError` (exit 2) caught by a static placement check. This is the one way to
  satisfy a strict-typed downstream consumer that needs the actual object.
- **`json`** — emits `json.dumps(value)` as a string. Use when a field must carry a JSON
  *string* (a serialized document), distinct from `object` which yields the live value.

`null`/missing resolves to the empty string `""` regardless of declared type (and emits
`capture.missing`, below).

## Explicit `capture:` — envelope-rooted extraction (HTTP stubs & Kafka reactors)

Implicit capture (path params + top-level body/value keys, always `scalar`) cannot reach
nested fields, the Kafka message key, or headers. An optional **`capture:`** block on a
stub or reactor fixes that with one mechanism: each entry reads a jq path **off the whole
incoming message envelope** into a named slot, then `{name}` substitutes it (same
placeholder syntax, same name charset — the path lives in `from`, never inside `{}`).

```yaml
# YAML shape — both stubs and reactors
capture:
  <name>: { from: "<jq path>", type: scalar|object|json }   # type defaults to scalar
```

- **`from`** — a jq path evaluated against the **envelope** (not the payload). Under
  dialect `"2"`+ `match` shares this root (envelope-rooted); `capture.from` and `match`
  reach the same fields. (Under dialect `"1"` `match` was payload-rooted — a `.`
  divergence unified by #22 and the v2 dialect switch; the v3 schema lift left this
  rooting unchanged.)
- **`type`** — `scalar` (default) / `object` / `json`. See "Capture value coercion" above
  for what each renders. `object` is the only way to produce a real JSON object/array
  field; it requires the placeholder to occupy the **whole field** (`ctx: "{ctx}"`, not
  `ctx: "prefix-{ctx}"`) — anything else is a startup `ConfigError`.
- **Envelope roots** (where `from` starts):
  - **HTTP request envelope** — `{ method, path, headers, body }`. `headers` keys are
    **lowercased** (HTTP headers are case-insensitive; write `.headers.authorization`,
    never `.headers.Authorization`). `body` is the parsed JSON body (or `null`).
  - **Kafka message envelope** — `{ key, value, partition, offset, timestamp, headers }`.
    `headers` keys are **case-sensitive as-produced** (Kafka header keys are bytes; do
    **not** lowercase — use the producer's exact name, e.g. `.headers.rqUID`). `value` is
    the parsed JSON value (or `null`); `key` is the decoded message key (`str | None`).
- **Explicit overrides implicit** — when a name appears in both the implicit context and
  `capture:`, the explicit entry wins, supplying both its value (re-extracted from the
  envelope via `from`) **and** its type. This is how a top-level key that implicit
  capture would stringify becomes a true object: add an explicit
  `ctx: { from: ".value.context", type: object }` and `context: "{ctx}"` renders the live
  object.
- **Compile loud, evaluate soft** — a malformed `from` (jq typo) fails loud at startup
  (`config validate` AND `mock run` Step 0 pre-compile, exit 2), same as `match.jq`. A
  `from` that *compiles* but resolves to `null`/missing against a particular message is a
  **soft miss**: it emits a `capture.missing` NDJSON event and substitutes `""`; the mock
  continues.
- **`capture.missing` joins the failure-stream grep set** — alongside `http.unmatched`,
  `kafka.skipped`, `kafka.error`. It is **non-fatal** (the mock substitutes empty string
  and continues), but it marks a `from` that produced nothing — usually a misconfigured
  path silently yielding a plausible-but-wrong (empty) field. An agent grepping the mock
  log for failure events must include `capture.missing` (see the `agctl` skill's mock
  lifecycle protocol for the grep command). A missing `jq` library is a different, fatal
  failure: `ConfigError` at startup (exit 2), pointing at `pip install 'agctl[jq]'`.

```yaml
# HTTP — envelope-rooted match.jq + envelope-rooted capture (same root)
graphql-operatorById:
  method: POST
  path: /graphql
  match: { jq: '.body.query | test("operatorById")' }   # envelope-rooted under "2"
  capture: { op_id: { from: ".body.variables.id" } }     # same envelope root
  response: { body: { id: "{op_id}" } }

# Kafka — envelope-rooted match + key + case-sensitive header + object pass-through
chatx-it-mock:
  topic: chatx.commands
  match: '.value.command == "SEARCH"'                  # envelope-rooted under "2"
  capture:
    tid:   { from: ".key" }
    rqUID: { from: ".headers.rqUID" }                  # exact producer casing
    ctx:   { from: ".value.context", type: object }    # whole-field "{ctx}" only
  reaction:
    topic: chatx.events
    value: { threadId: "{tid}", rs_headers: { rqUID: "{rqUID}" }, context: "{ctx}" }
```

## Idempotent reactions (at-least-once delivery)

Delivery is **at-least-once**: a crash/restart can re-deliver a message after the reaction
fired but before the commit landed. Reactions must therefore be **idempotent** — derive the
reaction `key` from the message key so a duplicate produce is a no-op for an idempotent
downstream consumer, or embed a stable idempotency id in `value`. The offset commits **only
on success**; a failing reaction is retried (bounded), then surfaced as `kafka.error`
(visible in the stream, never silently dropped).

## jq match semantics — compile loud, evaluate soft (load-bearing)

Both `match.jq` (HTTP stub) and `match` (Kafka reactor) use the jq engine, but the engine
treats **authoring typos** and **data-variance errors** differently. Conflating them is the
#1 jq-match trap:

- **Compile-time error (a typo)** — caught **loudly**. `agctl config validate` AND
  `mock run` startup pre-compile every expression (via `compile_jq`); a malformed `match.jq`
  or reactor `match` → `ConfigError` (exit 2) before any request/message is served. This
  extends to the *existing* Kafka reactor `match` — a config that previously started and ran
  silently inert now fails loud.
- **Runtime eval error (data-dependent)** — treated as a **soft non-match**. If the
  predicate raises against a particular envelope (e.g. `.body.amount > 1000` against a body
  missing `amount`), `jq_bool` swallows it to `false` — the stub/reactor falls through, the
  request goes to the next stub (or `404` + `http.unmatched`), the message is skipped. This
  is deliberate: partial matching must never crash the server.
- **Missing `jq` extra** → `ConfigError` (exit 2) at startup / `config validate`, pointing
  at `pip install 'agctl[jq]'`. A stub/reactor with no jq expression imports nothing — the
  HTTP-only zero-dep mock stays zero-dep.

**Wrong-branch false-green (the branching-mock trap).** When two stubs share method+path
and are distinguished **only by `match.jq`** (the high-value vs low-value use case), a
subtly-wrong predicate routes the request to the *other* branch's stub — which returns 2xx
and emits `http.hit`, **not** `http.unmatched`. The §3.5 log-grep protocol does not catch
this, and the compile guard catches only syntax errors, not logic errors. `config validate`
emits a jq-shadowing warning for this shape. **Mitigation**: pair branching stubs with a
response assertion that distinguishes branches — e.g.
`agctl http call create-order --jq-path '.status' --equals '"APPROVED"'` — so a wrong-branch
fire fails loudly (exit 1). See the `agctl` skill for the assertion flags.

## Stack snippets

### HTTP — the downstream service's contract
- **OpenAPI** (preferred): `paths["/api/v1/orders"].post` → method + path;
  `responses["201"]` → `response`; `parameters[in=path]` → `{name}` path captures.
- **Spring/FastAPI/NestJS** of the *dependency*: read its controller to copy method, path,
  and the response body shape you want to return.

### Kafka — the consumer you're impersonating
- **Spring** `@KafkaListener(topics = "orders.commands")` → consume `topic`; the handler's
  branch on `.command` → your `match` jq; the `kafkaTemplate.send("orders.events", …)` in
  that branch → `reaction`.
- **Python** (`confluent_kafka`/`aiokafka`/`faust`) consumer subscription → `topic`; the
  event it emits → `reaction.value`.
- **Node** `kafkajs` consumer `subscribe({ topic })` → `topic`; `producer.send({ topic,
  messages })` in the handler → `reaction`.

## What to clarify (only genuine gaps)

- The concrete consume/produce topic strings if the code computes them.
- Whether to pin `consumer_group` (default: **don't** — explain the resume hazard first).
- The env-var names for `listen` / `consumer_group` if they should be overridable
  (`${AGCTL_MOCK_HTTP_PORT:-18080}`).
- Whether the message value is always JSON (Avro/Protobuf topics are not mockable today).

## Where it writes

Under top-level `mocks:`. Idempotent: if the stub/reactor key
exists, diff and confirm (contract #5).

## Not covered — don't trust a false green

The MVP mocks **stateless, single-consumer, value-keyed, plaintext** flows. These patterns
are **not** mocked and tend toward a *plausible-but-wrong* result rather than a clear
failure — surface them to the user rather than shipping a config that looks like it covers
them (full list + failure modes in DESIGN §10 "Known-wrong-result / Not Covered"):

- **Stateful flows** — OAuth/token exchange, create-then-GET (201→200), idempotency-key
  replay, pagination, 429-then-retry. The static engine returns the same canned response
  regardless of prior calls.
- **TLS / HTTPS-pinned or `https://`-hardcoded SUT clients** — cannot connect to a
  plaintext mock at all (payments/auth/healthcare are disproportionately affected).
- **Cross-transport sagas** (Kafka trigger → HTTP callback) — no causal linkage.
- **Non-JSON Kafka values** (Avro/Protobuf/schema-registry) — emitted as `kafka.skipped`
  (visible), but effectively un-mockable.
- **Pinned `consumer_group` reused across runs/devs** — partition split or resume-past-
  messages → silently missing/old reactions. Mitigated by the unique-per-run default.
- **Wrong-branch match (predicate logic error)** — two stubs sharing method+path and
  distinguished only by `match.jq`: a subtly-wrong predicate silently fires the wrong branch
  (returns 2xx + `http.hit`, **not** `http.unmatched`). The compile guard catches only
  syntax errors; mitigate by pairing with a response assertion (see "jq match semantics"
  above).

## Gotchas

- `{name}` = capture-from-trigger here — **never** `${}` in a path/reaction template, and
  there is no `--param` for mocks. `${ENV}` is fine in `listen`/`consumer_group`/topics.
- `mocks.kafka` requires each reactor's resolved cluster to have non-empty
  `kafka.clusters.<name>.brokers` (and the `kafka` extra); HTTP-only mocks don't.
- `reaction.headers` values must be strings — a non-string is a config error.
- `description` is optional but effectively required (contract #4); its absence degrades
  `discover` and earns a validate warning.
- Mocks **are** surfaced by `agctl discover`: `--category mock-http-stubs` /
  `mock-kafka-reactors` (and `--name <key>` for full detail). After editing, confirm the item
  lists, then verify with `agctl config validate` and a `mock run --duration` smoke (see the
  `agctl` skill).
- A jq **typo** in `match.jq` / reactor `match` / `capture.*.from` fails loud at startup
  (exit 2); a jq **eval error** (or a `from` resolving to `null`) against a particular
  request/message is a soft non-match / `capture.missing` (falls through, empty string).
  Two different guards for two different error classes — see "jq match semantics" above.
- Under dialect `"2"`+, **`match` and `capture.from` share an envelope root** — `.body.amount`
  (HTTP) / `.value.command` (Kafka) on both sides. Under dialect `"1"` `match` was
  payload-rooted; `agctl config migrate` lifts a v1/v2 config to v3 (structural
  `kafka.clusters` lift for both; `.body | ` / `.value | ` prefix on the three match-site
  families for v1 only) but does **not** touch CLI `--match` flags in shell scripts /
  prompts.
- HTTP `headers` in the capture envelope are **lowercased** (`.headers.authorization`);
  Kafka `headers` are **case-sensitive as-produced** (`.headers.rqUID`, exact producer
  casing). Don't lowercase Kafka header names.
- `type: object` must occupy the **whole field** (`key: "{name}"` exactly) — inline use
  or a string-only slot (`reaction.key`, a header value) is a startup `ConfigError`.
- `match.jq` / reactor `match` / `capture.*.from` need `pip install 'agctl[jq]'` (bundled
  in `agctl[kafka]` and `agctl[db]`). A stub/reactor with none of these imports nothing.
