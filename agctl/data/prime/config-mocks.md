# agctl — config-mocks: HTTP stubs, Kafka reactors, gRPC stubs

A `mocks:` block impersonates the SUT's **external dependencies**: the
downstream HTTP API, the Kafka consumer expected to react, or the gRPC
service its client targets.

```yaml
mocks:
  http:
    listen: "${AGCTL_MOCK_HTTP_HOST:-0.0.0.0}:${AGCTL_MOCK_HTTP_PORT:-18080}"
    stubs: { create-order: {…} }              # path-routed
  kafka:
    reactors: { order-command-handler: {…} }  # joins the SUT's real broker
  grpc:
    listen: "${AGCTL_MOCK_GRPC_HOST:-0.0.0.0}:${AGCTL_MOCK_GRPC_PORT:-50051}"
    descriptors: [{ proto: "protos/echo/v1/echo.proto" }]
    stubs: { echo-unary: {…} }                # (service, method)-routed
```

## The #1 trap: `{name}` here is *capture*, not `--param`

A mock has no caller — the SUT's own request/message **is** the input, so
`{name}` means **capture-from-trigger**: in a stub `path`
(`/orders/{order_id}`) it captures that segment; in `response.body` /
`reaction.value`/`key`/`headers` it is filled from the capture context.
Implicit capture = path params ∪ top-level JSON body/value keys; explicit
`capture:` reaches nested fields, `.key`, `.headers.*`. `match.body`/`match`
are **filters**, never capture sources. No `--param`/`:name`; `${ENV}`
resolves at load (listen, topics).

## HTTP stub (`mocks.http.stubs.<name>`)

`method` (any verb) · `path` (route with `{name}` segments; **trailing slash
significant**, query stripped) · `match.body` *(opt)* JSON subset
(body-rooted) · `match.jq` *(opt)* predicate over the request envelope
`{method, path, headers (lowercased), body}`, AND-ed with `match.body` ·
`response` (`status` 100-599, `headers`,
`body` — `{name}` rendered from capture) · `delay_ms` *(opt; >64 concurrent
→ 429)* · kebab-case name · description. **First-match-wins in YAML order** —
put `/orders/bulk` before `/orders/{order_id}` or the param stub shadows it
(validate warns).

## Kafka reactor (`mocks.kafka.reactors.<name>`)

`topic` (the **command** topic the SUT publishes) · `consumer_group`
(**omit** — default is unique-per-run; pinning carries a resume hazard) ·
`match` *(opt)* jq over
the message envelope (`.value.command == "CREATE_ORDER"`; headers
case-sensitive; non-JSON never matches — visibly skipped) · `cluster`
*(opt;* resolved cluster needs non-empty `brokers` + the `kafka` extra) ·
`reaction` — `topic` (**event** topic), `key`, `value`
(JSON-serializable, `{name}`-rendered), `headers` (**string values only**).
At-least-once delivery — make reactions idempotent (derive `key` from the
message key or embed an idempotency id).

## gRPC stub (`mocks.grpc.stubs.<name>`)

Needs the `grpc` extra. Plaintext h2c listener (no TLS on the mock);
health + reflection auto-served (default on). **Descriptors are required to
resolve `service`/`method`** — reflection cannot bootstrap the mock;
omitting both descriptor sources fails at server construction (exit 2).
`service` (fully-qualified) + `method`; **call type is derived from the
descriptor** (unary /
server_stream / client_stream / bidi) — never configured. `match.body`
(message-rooted subset; skipped for client_stream) / `match.jq` over the
per-call envelope (`{service, method, metadata (lowercased), message}`;
client-stream: `{…, messages, count}` at stream close). `response`:
`status` (gRPC name, case-sensitive, or 0-16; non-OK = terminal status) ·
`message` (unary/client-stream/bidi) XOR `messages` (server-stream,
`[{message, delay_ms}]`, required) · `metadata` (string values) ·
`delay_ms`. First-match-wins per `(service, method)`. `grpc.unmatched` /
`grpc.error` are **fatal** (exit 1 at stop). **Unique ports across runs** —
grpcio `SO_REUSEPORT` lets two servers silently share a port.

## Capture value coercion

`scalar` (default) — `str(value)` inline. `object` — the **live value**;
whole-field placeholder only (else startup `ConfigError`). `json` —
`json.dumps` string. `null`/missing → `""` + `capture.missing`.

## Cross-transport `effects:`

An optional ordered `effects:` list on a stub or reactor (sibling of
`response`/`reaction`; `reaction` XOR `effects`): `type: kafka` produces
(`value` XOR `values[]`; own cluster resolution), `type: http` calls
(`service`+`path` or `url`). Effects run before the HTTP response is sent;
each effect's `capture` chains into later ones (ordered, last-wins).
**Effect failure is fatal** (`effect.error` → exit 1 at stop).

## Explicit `capture:`

```yaml
capture:
  <name>: { from: "<jq path off the envelope>", type: scalar|object|json }
```

`from` shares the envelope root with `match` (dialect 2+): HTTP
`.body.variables.id`, Kafka `.key`/`.headers.rqUID`, gRPC `.message.field`.
HTTP/gRPC headers **lowercased**; Kafka **case-sensitive**. Explicit
overrides implicit (value + type).

## jq: compile loud, evaluate soft

A **typo** (compile error) in `match.jq` / reactor `match` /
`capture.*.from` fails loud at startup (exit 2; also `config validate`); an
**eval error** against a particular message is a soft non-match (falls
through, `404`+`http.unmatched` or skipped); `from`→null is
`capture.missing` + `""`. Needs `agctl[jq]` (bundled in kafka/db/grpc
extras); no-jq stubs import nothing.

**Wrong-branch false-green:** two stubs sharing method+path (or
`(service, method)`) distinguished only by `match.jq`/`match.body` — a
wrong predicate fires the *other* branch (2xx + `http.hit`). Mitigate: pair
with an assertion that distinguishes branches.

## Not covered — don't trust a false green

Stateless/static engine: no stateful flows (token exchange, 201→then-GET,
pagination, 429-retry), no TLS/https-pinned SUT clients (plaintext only),
no non-JSON Kafka values (Avro/Protobuf → `kafka.skipped`), gRPC bidi is
request/response pairing (no conversation state or server push),
client-stream aggregates at close.

Close-out: `config validate` + `discover --category mock-http-stubs|
mock-kafka-reactors|mock-grpc-stubs` + a `mock run --duration 5` smoke
(catches gRPC resolution validate can't).
