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
  the capture context (path params ∪ top-level keys of the JSON request body).
- **Kafka** `{name}` in `reaction.value`/`key`/`headers` is filled from the top-level keys
  of the matched message's JSON value.

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
   to fire (reuses `json_subset`). Omit = match on method+path alone.
5. **response** — `status` (100–599), `headers` (optional; `Content-Type` defaults to
   `application/json` for a dict/list body, `text/plain` otherwise), `body` (any). Render
   `{name}` against the capture context.
6. **delay_ms** *(optional)* — simulated latency; capped at 64 concurrent requests (overflow
   → `429`).
7. **name** — kebab-case from the route (`POST /api/v1/orders` → `create-order`).
8. **description** — one line (effectively required per contract #4).

**First-match-wins.** Stubs are matched in YAML mapping order; put a literal segment ahead
of a `{name}` segment at the same position (`/orders/bulk` before `/orders/{order_id}`) or
the param stub silently shadows the literal one (config validate warns on this).

## Extraction — Kafka reactor (`mocks.kafka.reactors.<name>`)

1. **topic** — the **command** topic to consume from (the SUT publishes here).
2. **consumer_group** *(optional)* — **omit it** unless you have a reason. Omitted → a
   unique per-run group (`agctl-mock-<name>-<runid>`) so a restart never silently resumes
   past messages produced between runs. Pinning a stable group is opt-in and carries a
   resume hazard (see Not covered).
3. **match** *(optional)* — a jq boolean predicate over the message **value**
   (`.command == "CREATE_ORDER"`). Omit = match all. Non-JSON / non-object values never
   match — they're visibly skipped, not silently dropped.
4. **reaction** — what to **produce** back: `topic` (the **event** topic), `key` (optional),
   `value` (JSON-serializable), `headers` (optional; **string values only** — a non-string
   value is a config error). Render `{name}` against the message-value context.
5. **name** — kebab-case from the consumer/event.
6. **description** — one line.

**Requires top-level `kafka.brokers`.** `mocks.kafka` joins the SUT's *real* broker — it is
not a broker itself. `mock run` enforces this at startup (exit 2 if `kafka.brokers` is
absent), and needs the `kafka` extra installed (`pip install 'agctl[kafka]'`). HTTP-only
mocks need neither.

## Capture value coercion (load-bearing)

Captured values are frequently non-string (numeric IDs, bools). Every captured value is
stringified via `str()` before `{name}` substitution — so `orderId` `42` becomes `"42"`,
`true` → `"True"`, `null` → `"None"`. **JSON-type pass-through is not supported**: a
reaction field always receives a string. If a downstream consumer strictly types a field,
this can bite (see Not covered) — design the reaction to be type-tolerant, or echo a
literal.

## Idempotent reactions (at-least-once delivery)

Delivery is **at-least-once**: a crash/restart can re-deliver a message after the reaction
fired but before the commit landed. Reactions must therefore be **idempotent** — derive the
reaction `key` from the message key so a duplicate produce is a no-op for an idempotent
downstream consumer, or embed a stable idempotency id in `value`. The offset commits **only
on success**; a failing reaction is retried (bounded), then surfaced as `kafka.error`
(visible in the stream, never silently dropped).

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

Under top-level `mocks:` (additive — no `version` bump). Idempotent: if the stub/reactor key
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
- **Header-borne correlation IDs** (`traceparent`, CloudEvents `ce-*`) — headers aren't
  capturable, so a correlatable reply can't be produced.
- **Non-JSON Kafka values** (Avro/Protobuf/schema-registry) — emitted as `kafka.skipped`
  (visible), but effectively un-mockable.
- **JSON-type pass-through** — captured values are stringified; a strict-typed downstream
  may reject the reaction.
- **Pinned `consumer_group` reused across runs/devs** — partition split or resume-past-
  messages → silently missing/old reactions. Mitigated by the unique-per-run default.

## Gotchas

- `{name}` = capture-from-trigger here — **never** `${}` in a path/reaction template, and
  there is no `--param` for mocks. `${ENV}` is fine in `listen`/`consumer_group`/topics.
- `mocks.kafka` requires top-level `kafka.brokers` (and the `kafka` extra); HTTP-only mocks
  don't.
- `reaction.headers` values must be strings — a non-string is a config error.
- `description` is optional but effectively required (contract #4); its absence degrades
  `discover` and earns a validate warning.
- Mocks are **not** surfaced by `agctl discover` (no `mocks` category) — navigate the
  `mocks:` section directly. Verify with `agctl config validate` and a `mock run --duration`
  smoke (see the `agctl` skill).
