# `agctl` — CLI Design Document

**Version:** 0.6-draft  
**Last updated:** 2026-07-02  
**Status:** Foundation design — hardened; ready for implementation  
**Change spec:** [`docs/superpowers/specs/2026-07-02-agctl-design-hardening.md`](./superpowers/specs/2026-07-02-agctl-design-hardening.md)

---

## Table of Contents

1. [Goals & Non-Goals](#1-goals--non-goals)
2. [Configuration Schema](#2-configuration-schema)
3. [CLI Command Design](#3-cli-command-design) *(includes `agctl discover` §3.6)*
4. [Output Schema](#4-output-schema)
5. [Configuration Resolution Order](#5-configuration-resolution-order)
6. [AGENTS.md Template](#6-agentsmd-template)
7. [Project Structure](#7-project-structure)
8. [Key Design Principles](#8-key-design-principles)
9. [Extension Points](#9-extension-points)
10. [Open Questions / Future Work](#10-open-questions--future-work)
11. [Agentic Workflow Use Cases](#11-agentic-workflow-use-cases)

---

## 1. Goals & Non-Goals

### Goals

- **System-agnostic.** The tool ships with zero knowledge of any particular project. All system-specific configuration lives in `agctl.yaml` in the consuming repo.
- **Agent-friendly.** Every invocation produces exactly one JSON object on stdout. Exit codes are deterministic and machine-readable. No ANSI color, no progress spinners, no banner text.
- **Composable.** Commands are narrow and orthogonal. An agent chains them together — send a request, then assert a Kafka message, then assert a DB row — rather than relying on a monolithic "run scenario" command.
- **Maintainable.** The codebase has one responsibility per module. Adding a new database driver or protocol does not require touching core logic.
- **Fail loudly.** A wrong assertion always exits non-zero with a structured error. Silent false-positives are the worst possible failure mode for an agent harness.

### Non-Goals

- **Human-facing UI or formatted output.** Pretty-printing, tables, and color are explicitly out of scope for the initial design. A thin wrapper script can format JSON for humans if needed.
- **Auto-generating tests.** `agctl` runs assertions; it does not inspect a codebase and write test cases.
- **Orchestrating multi-step scenarios as a first-class primitive.** An agent can chain commands in a shell script or inline code; `agctl` does not need a built-in scenario runner to be useful. This is deferred (see §10).
- **Managing infrastructure lifecycle.** Starting, stopping, or provisioning services is out of scope.

> **Mocking support:** `agctl` now includes a built-in mock server for HTTP and Kafka (see `agctl mock` in §3.3 and the `mocks:` config section in §2.1). This supersedes the earlier non-goal — local testing no longer requires external tools like WireMock or LocalStack for common HTTP stubbing and Kafka reaction patterns.

---

## 2. Configuration Schema

### 2.1 Schema Reference

```yaml
# agctl.yaml
# ---------------
# Version is the jq-dialect switch: "2" = envelope-rooted `match` (`.body.x`
# for HTTP, `.value.x` for Kafka). A major-version mismatch is a ConfigError
# (exit 2) pointing at `agctl config migrate`; minor/patch are not tracked.
version: "2"

# ---------------------------------------------------------------------------
# services — named HTTP base URLs for services under test.
# base_url supports ${ENV_VAR} interpolation.
# ---------------------------------------------------------------------------
services:
  order-service:
    base_url: "${ORDER_SERVICE_URL}"           # e.g. http://localhost:8081
    health_path: "/actuator/health"             # used by `agctl check ready`
    timeout_seconds: 10                         # optional, overrides defaults.timeout_seconds

  payment-service:
    base_url: "${PAYMENT_SERVICE_URL}"
    health_path: "/health"
    timeout_seconds: 15

# ---------------------------------------------------------------------------
# kafka — broker configuration.
# ---------------------------------------------------------------------------
kafka:
  brokers:
    - "${KAFKA_BROKER_HOST}:9092"
  default_consumer_group: "agctl-consumer"  # optional
  schema_registry_url: "${SCHEMA_REGISTRY_URL}" # optional; omit if not used
  timeout_seconds: 30                            # default consume/assert timeout

  # kafka.ssl — optional TLS/mTLS settings for brokers that require SSL.
  #   Setting any field (to a non-empty value) enables TLS; security.protocol
  #   defaults to "SSL" unless overridden. Hostname verification stays on
  #   unless endpoint_identification_algorithm is set to "none" (self-signed/dev).
  #   ca_location is optional: when unset, librdkafka falls back to the system
  #   trust store (use it for publicly-trusted brokers like Confluent Cloud; pin
  #   a CA for private-PKI brokers). All values support ${ENV_VAR} interpolation
  #   and AGCTL_KAFKA__SSL__* overrides; an empty string counts as unset.
  ssl:
    ca_location: "${KAFKA_SSL_CA:-}"               # path to CA certificate (PEM); optional
    certificate_location: "${KAFKA_SSL_CERT:-}"    # path to client certificate (mTLS)
    key_location: "${KAFKA_SSL_KEY:-}"             # path to client private key (mTLS)
    key_password: "${KAFKA_SSL_KEY_PASSWORD:-}"    # optional private-key password
    # endpoint_identification_algorithm: "none"    # uncomment to disable hostname verification
    # security_protocol: "SSL"                     # defaults to SSL; set SASL_SSL when adding SASL later

# ---------------------------------------------------------------------------
# kafka.patterns — named Kafka filter patterns, analogous to HTTP templates.
# topic:   Kafka topic name
# match:   jq predicate expression evaluated against each message envelope
#          ({key, value, partition, offset, timestamp, headers}); so .value.eventType,
#          .value.payload.orderId, .key, .headers.<name>. Supports {placeholder}
#          substitution via --param at assert time.
# ---------------------------------------------------------------------------
  patterns:
    order-created:
      description: "An ORDER_CREATED event for a specific order"
      topic: orders.created
      match: '.value.eventType == "ORDER_CREATED" and .value.payload.orderId == "{orderId}"'

    payment-failed:
      description: "Any PAYMENT_FAILED event regardless of order"
      topic: payments.events
      match: '.value.eventType == "PAYMENT_FAILED"'

# ---------------------------------------------------------------------------
# mocks — HTTP mock server and Kafka reactors (DESIGN §3.3).
#   HTTP: agctl serves stubs; the SUT's HTTP client points at `listen`.
#   Kafka: agctl joins as a consumer on the SUT's real broker and reacts.
#   Supports ${ENV} interpolation at load, {placeholder} substitution at
#   match/react time, and jq predicates on stubs (match.jq) and reactors.
# ---------------------------------------------------------------------------
mocks:
  http:
    listen: "${AGCTL_MOCK_HTTP_HOST:-0.0.0.0}:${AGCTL_MOCK_HTTP_PORT:-18080}"
    stubs:
      create-order:
        description: "Mock the downstream order API"
        method: POST
        path: "/api/v1/orders"
        match:
          body: { "priority": "high" }    # optional: json_subset containment filter (body-rooted)
          # jq: '.body.amount > 1000'     # optional: jq predicate, AND-ed with body.
          #                                 Under dialect "2" `match.jq` is envelope-rooted
          #                                 (HTTP request {method, path, headers (lowercased),
          #                                 body}), so .body.amount / .headers.authorization.
        # capture: OPTIONAL envelope-rooted extraction (same root as `match.jq`).
        # Each entry is { from, type }:
        #   from: jq path evaluated against the whole incoming message envelope
        #         (HTTP request {method, path, headers (lowercased), body} /
        #         Kafka message {key, value, headers (case-sensitive), ...}). `match.jq`
        #         shares this root under dialect "2".
        #   type: scalar (default, str()) | object (live pass-through; whole-field
        #         placeholder "{name}" only) | json (json.dumps as a string).
        # Explicit entries override implicit top-level-body/value captures on name
        # collision (value AND type). A `from` resolving to null emits a non-fatal
        # `capture.missing` event and substitutes empty string.
        # capture:
        #   cust_id: { from: ".body.customer_id" }            # nested path / header reachable
        #   ctx:     { from: ".body.context", type: object }  # whole-object pass-through
        response:
          status: 201
          headers: { Content-Type: "application/json" }
          body:
            order_id: "{customer_id}-mock"
            status: "PENDING"
        delay_ms: 0

      get-order:
        method: GET
        path: "/api/v1/orders/{order_id}"
        response:
          status: 200
          body: { order_id: "{order_id}", status: "CONFIRMED" }

  kafka:
    reactors:
      order-command-handler:
        description: "Mock the service that consumes order commands"
        topic: orders.commands
        consumer_group: agctl-mock-order-handler   # OPTIONAL; omit → unique per-run group
        match: '.value.command == "CREATE_ORDER"'    # envelope-rooted (whole Kafka message)
        # capture: same CaptureSpec shape as HTTP stubs; `from` shares the `match`
        # root — the Kafka message envelope ({key, value, partition, offset,
        # timestamp, headers}).
        # Header keys are case-sensitive here (as-produced) — do NOT lowercase them
        # (unlike HTTP headers). Reaches `.key`, `.headers.<name>`, nested `.value.*`.
        # capture:
        #   tid:   { from: ".key" }
        #   rqUID: { from: ".headers.rqUID" }
        #   ctx:   { from: ".value.context", type: object }
        reaction:
          topic: orders.events
          key: "{orderId}"
          value:
            eventType: "ORDER_CREATED"
            orderId: "{orderId}"
            status: "PENDING"

# ---------------------------------------------------------------------------
# database — named connection profiles and SQL query templates.
# All fields support ${ENV_VAR} interpolation (see §2.2).
# Connection types: postgresql (extensible via plugins, see §9).
# ---------------------------------------------------------------------------
database:
  connections:
    main-db:
      type: postgresql
      host: "${DB_HOST}"
      port: 5432
      dbname: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      default: true                               # used when --connection is omitted

    main-db-writable:
      # In production, use a separate least-privilege write-capable account
      # (e.g., ${DB_WRITE_USER}/${DB_WRITE_PASSWORD}) per spec §3.
      type: postgresql
      host: "${DB_HOST}"
      port: 5432
      dbname: "${DB_NAME}"
      user: "${DB_WRITE_USER:-${DB_USER}}"
      password: "${DB_WRITE_PASSWORD:-${DB_PASSWORD}}"
      writable: true                              # required for `agctl db execute --write`

    analytics-db:
      type: postgresql
      host: "${ANALYTICS_DB_HOST}"
      port: 5432
      dbname: "analytics"
      user: "${ANALYTICS_DB_USER}"
      password: "${ANALYTICS_DB_PASSWORD}"

    # Connection via URL (optional, supports ${ENV} interpolation).
    # When `url` is set, discrete fields (host/port/dbname/user/password) may
    # still be provided — they override the corresponding URI params.
    url-db:
      type: postgresql
      url: "${DATABASE_URL}"                 # e.g. "postgresql://user:pass@host:port/dbname"
      port: 5432                              # overrides the port from DATABASE_URL (if present)

  # templates — named SQL queries. `connection` is optional (falls back to
  # defaults.database_connection). `sql` uses :paramName named params (JDBC-style).
  # `mode` is optional; defaults to "read". Set `mode: write` for templates used
  # with `agctl db execute --write` (write templates are rejected by `db query`/`db assert`).
  templates:
    find-order:
      description: "Fetch a single order by ID"
      connection: main-db
      sql: "SELECT id, status, total_cents, created_at FROM orders WHERE id = :orderId"

    orders-by-status:
      description: "List orders in a given status, optionally filtered by customer"
      connection: main-db
      sql: "SELECT id, status FROM orders WHERE status = :status AND customer_id = :customerId"

    count-failed-payments:
      description: "Count failed payments after a given timestamp"
      connection: main-db
      sql: "SELECT COUNT(*) AS cnt FROM payments WHERE status = 'FAILED' AND created_at > :since"

    insert-order:
      description: "Create a new order (idempotent via ON CONFLICT)"
      connection: main-db-writable
      mode: write
      sql: "INSERT INTO orders (id, customer_id, status) VALUES (:orderId, :customerId, 'PENDING') ON CONFLICT (id) DO NOTHING RETURNING *"

# ---------------------------------------------------------------------------
# templates — named HTTP request templates.
# method:   HTTP verb (GET, POST, PUT, PATCH, DELETE)
# service:  must match a key under `services`
# path:     supports {placeholder} path parameters resolved via --param
# headers:  static headers merged with any --header overrides at call time
# body:     JSON body; can reference path params (see note below)
# ---------------------------------------------------------------------------
templates:
  create-order:
    description: "Submit a new order for a customer"
    method: POST
    service: order-service
    path: "/api/v1/orders"
    headers:
      Content-Type: "application/json"
      X-Request-Source: "agctl"
    body:
      customer_id: "{customer_id}"
      items:
        - sku: "{sku}"
          quantity: 1

  get-order:
    description: "Fetch a single order by ID"
    method: GET
    service: order-service
    path: "/api/v1/orders/{order_id}"

  charge-payment:
    description: "Trigger payment charge for an order"
    method: POST
    service: payment-service
    path: "/api/v1/payments"
    headers:
      Content-Type: "application/json"
      Authorization: "Bearer ${PAYMENT_SERVICE_TOKEN}"
    body:
      order_id: "{order_id}"
      amount_cents: "{amount_cents}"

  get-payment-status:
    description: "Fetch payment status by order ID"
    method: GET
    service: payment-service
    path: "/api/v1/payments/{order_id}/status"

# ---------------------------------------------------------------------------
# defaults — project-wide fallbacks.
# ---------------------------------------------------------------------------
defaults:
  timeout_seconds: 10
  database_connection: main-db
```

### 2.2 Environment Variable Interpolation

Any YAML string value containing `${...}` is resolved at load time. Three forms are supported:

1. `${VAR_NAME}` — **required**. Look up `VAR_NAME` in the process environment. If missing, emit a config error (exit 2) listing all unresolved variables. Never silently substitute an empty string.
2. `${VAR_NAME:-default}` — **optional with default**. If `VAR_NAME` is missing, substitute the literal `default`.
3. `${VAR_NAME:-}` — **optional, empty**. If `VAR_NAME` is missing, substitute an empty string (no error). Use this for fields that are not always set, e.g. `schema_registry_url: "${SCHEMA_REGISTRY_URL:-}"`.

The `${...}` syntax is only supported in string scalar values, not in keys.

### 2.3 Path Parameter Syntax

HTTP template `path` and `body` values use `{placeholder}` (single braces) for runtime substitution via `--param key=value`. SQL (templates and free-form) uses `:paramName` (JDBC-style) instead — `{...}` is avoided in SQL to prevent collisions with JSON literals. Both are distinct from env var interpolation (`${...}`), which is resolved at config load time.

---

## 3. CLI Command Design

All commands share these global flags:

| Flag | Default | Description |
|---|---|---|
| `--config <path>` | auto-discovered | Explicit path to `agctl.yaml` |
| `--timeout <seconds>` | from config `defaults` | Override request/operation timeout |
| `--version` | — | Print version and exit |

### 3.1 `agctl http` — HTTP Requests

#### `agctl http call <template-name>`

Execute a named template from config.

```
agctl http call <template-name>
    [--param key=value]     # repeatable; fills {placeholders} in path and body
    [--body '{...}']        # override or extend the template body (merged, not replaced)
    [--header key=value]    # repeatable; merged with template headers; caller wins on conflict
    [--timeout <seconds>]
    # Response assertions (≥1 flag => assertion mode; all active flags AND together):
    [--status <code>]       # exact HTTP status code the response must return
    [--contains '{...}']    # JSON needle that must be a subset of the response body
    [--match <jq-expr>]     # jq predicate; true on ANY truthy output against the response
                            #   envelope {status_code, response_time_ms, headers (lowercased),
                            #   body, url, method} — so .body.x, .status_code, .headers.x
    [--jq-path <jq>]        # jq path into the body (must be paired with --equals)
    [--equals <value>]      # expected value for --jq-path (JSON-parsed when valid; strict compare)
```

**Examples:**

```bash
# Create an order
agctl http call create-order \
  --param customer_id=cust-42 \
  --param sku=WIDGET-001

# Fetch a specific order
agctl http call get-order --param order_id=ord-789

# Override a body field
agctl http call create-order \
  --param customer_id=cust-42 \
  --param sku=WIDGET-001 \
  --body '{"priority": "high"}'
```

**Body merge:** when `--body '{...}'` is supplied, it is deep-merged into the template's body — nested objects merge recursively, arrays are replaced wholesale, and scalar leaves from `--body` override the template. The template body is the base; `--body` only adds or overrides fields.

#### `agctl http request`

Free-form HTTP request — escape hatch for cases not covered by templates.

```
agctl http request
    --service <name>        # must match a services key in config
    --method GET|POST|PUT|PATCH|DELETE
    --path <path>           # e.g. /api/v1/orders/ord-123
    [--body '{...}']
    [--header key=value]    # repeatable
    [--timeout <seconds>]
    # Response assertions (≥1 flag => assertion mode; all active flags AND together):
    [--status <code>]       # exact HTTP status code the response must return
    [--contains '{...}']    # JSON needle that must be a subset of the response body
    [--match <jq-expr>]     # jq predicate; true on ANY truthy output against the response
                            #   envelope {status_code, response_time_ms, headers (lowercased),
                            #   body, url, method} — so .body.x, .status_code, .headers.x
    [--jq-path <jq>]        # jq path into the body (must be paired with --equals)
    [--equals <value>]      # expected value for --jq-path (JSON-parsed when valid; strict compare)
```

**Example:**

```bash
agctl http request \
  --service order-service \
  --method GET \
  --path /api/v1/orders/ord-789
```

**Response assertions (`http call` and `http request`):** Optional flags let you assert on the response in the same invocation, with the same exit-code discipline as `kafka assert` / `db assert`. Zero flags (the default) leaves behavior unchanged — a 4xx/5xx response is still `ok:true` (HTTP status is a result, not an assertion). Supplying ≥1 flag enters assertion mode; all active flags must pass (AND). A failed flag raises `AssertionError` (exit 1) with `error.detail = {response, failures}`, where `response` is the full HTTP result and `failures` lists every failing mode (no short-circuit).

- `--status <code>` — exact HTTP status code the response must return.
- `--contains '{...}'` — JSON subset match against the response body.
- `--match <jq>` — jq predicate evaluated against the **response envelope** (`{status_code, response_time_ms, headers (lowercased keys), body, url, method}`), so `.body.x`, `.status_code`, `.headers.x`. True on ANY truthy output (`.body.items[].x > 100` means "≥1 item qualifies," not "all"). To assert "all," use the jq form `all(.body.items[]; .x > 100)`.
- `--jq-path <jq>` + `--equals <v>` — extract a value via jq (rooted at the response **body**) and compare strictly (type-aware: `0` ≠ `"0"`). The two flags must be used together; one without the other → `ConfigError` (exit 2).

`--match` and `--jq-path` require the `jq` extra (`pip install 'agctl[jq]'`); a missing library surfaces as `ConfigError` (exit 2), not a crash.

> **v1 → v2 migration:** under dialect `"1"` `--match` was body-rooted (`.x` meant a body field). Under dialect `"2"` it is envelope-rooted, so `.x` resolves against the response envelope — prefix legacy expressions with `.body | ` (e.g. `.status == "PENDING"` → `.body | .status == "PENDING"`, or simply `.body.status == "PENDING"`). `agctl config migrate` does **not** rewrite CLI flags in scripts/prompts — only the config file.

#### `agctl http ping`

Send a repeated HTTP request at a fixed interval, emitting one JSON line per ping to stdout. Designed for session-keepalive scenarios (e.g. heartbeat endpoints that require periodic calls to prevent logout).

Unlike all other `agt` commands, `agctl http ping` emits one JSON object **per ping** (newline-delimited) rather than a single envelope. This allows the agent to stream results while running the command in the background.

```
agctl http ping
    <template-name> | --service <name> --path <path>
    --interval <seconds>           # delay between pings (e.g. 5)
    [--method GET|POST|...]        # default: GET (or template method)
    [--body '{...}']
    [--header key=value]
    [--duration <seconds>]         # stop after this many seconds; mutually exclusive with --until-stopped
    [--until-stopped]              # run until SIGTERM/SIGINT (default if --duration is omitted)
    [--timeout <seconds>]          # per-request timeout
```

Each emitted line:

```json
{"ping": 1, "ok": true, "status_code": 200, "duration_ms": 34, "timestamp": "2026-06-29T14:22:05Z"}
{"ping": 2, "ok": true, "status_code": 200, "duration_ms": 31, "timestamp": "2026-06-29T14:22:10Z"}
{"ping": 3, "ok": false, "status_code": 401, "duration_ms": 12, "timestamp": "2026-06-29T14:22:15Z", "error": "Unexpected status 401"}
```

On `SIGTERM`/`SIGINT`, the command emits a final summary line and exits 0 (if all pings succeeded) or 1 (if any ping failed):

```json
{"summary": true, "total_pings": 3, "failed_pings": 1, "duration_ms": 15034}
```

**Usage pattern (agent running a test while keeping session alive):**

```bash
# Start heartbeat in background
agctl http ping heartbeat --interval 5 --until-stopped &
HEARTBEAT_PID=$!

# ... run test scenario ...
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001
agctl kafka assert --topic orders.created --contains '{"customer_id": "cust-42"}' --timeout 10

# Stop heartbeat
kill $HEARTBEAT_PID
```

---

### 3.2 `agctl kafka` — Kafka Operations

#### `agctl kafka consume`

Consume messages from a topic. Returns as soon as `--expect-count` matching messages are received or `--timeout` elapses, whichever comes first.

```
agctl kafka consume
    --topic <name>
    [--timeout <seconds>]       # default: kafka.timeout_seconds from config
    [--lookback <seconds>]      # seek to now - lookback before reading; default: = --timeout
    [--match <jq-expr>]         # jq boolean predicate over the message envelope
                                #   ({key, value, partition, offset, timestamp, headers}); only
                                #   messages where the expression is true are counted/returned
    [--filter-key <jq-expr>]    # DEPRECATED alias for --match; prefer --match
    [--expect-count <n>]        # if set, exit 1 (AssertionError) if fewer than n matching
                                #   messages are received within the window
    [--from-beginning]          # seek to earliest offset (default: seek to now - lookback)
    [--consumer-group <name>]   # override default consumer group (default: agctl-consumer)
```

`--match` is a jq boolean expression evaluated against each message **envelope** (`{key, value, partition, offset, timestamp, headers}`) — so `.value.eventType`, `.key`, `.headers.rqUID`. Header keys are case-sensitive (as-produced). Messages where the expression returns `false` or raises an error are silently skipped. This enables partial matching — you do not need to know the full message structure.

**Examples:**

```bash
# Wait up to 10s for at least 1 order.created event (any content)
agctl kafka consume \
  --topic orders.created \
  --timeout 10 \
  --expect-count 1

# Only count messages where orderId matches, ignoring all others
agctl kafka consume \
  --topic orders.created \
  --match '.value.payload.orderId == "ord-789"' \
  --timeout 10 \
  --expect-count 1

# Filter by eventType without caring about other fields
agctl kafka consume \
  --topic orders.created \
  --match '.value.eventType == "ORDER_CREATED" and .value.payload.customerId != null' \
  --timeout 15
```

#### `agctl kafka produce`

Publish a single message.

```
agctl kafka produce
    --topic <name>
    --message '{...}'           # JSON string; value is published as-is
    [--key <string>]            # optional Kafka message key
    [--header key=value]        # repeatable; Kafka message headers
```

**Example:**

```bash
agctl kafka produce \
  --topic payments.commands \
  --key ord-789 \
  --message '{"order_id": "ord-789", "command": "refund"}'
```

#### `agctl kafka assert`

Assert that a message matching a predicate or pattern appears on a topic within a timeout. Fails with `AssertionError` (exit 1) if no matching message arrives within the window.

Supports three matching modes, usable together:
- `--contains` — JSON subset match against the full message value
- `--match` — jq boolean predicate over the message **envelope** (`{key, value, partition, offset, timestamp, headers}`), so `.value.eventType`, `.key`, `.headers.rqUID` (preferred for real systems)
- `--pattern` — reference a named pattern from `kafka.patterns` in config

When multiple modes are combined, all conditions must be satisfied.

```
agctl kafka assert
    [--topic <name>]            # required unless --pattern is used (topic inferred from pattern)
    [--contains '{...}']        # JSON subset that must be present in the message value
    [--match <jq-expr>]         # jq boolean predicate over the message envelope; more flexible than --contains
    [--pattern <name>]          # use a named pattern from config
    [--param key=value]         # repeatable; fills {placeholder} in pattern match expression
    [--path <jq-path>]          # narrow --contains match to a sub-path, e.g. ".event_type"
    [--lookback <seconds>]      # seek to now - lookback; default: = --timeout
    --timeout <seconds>         # assert fails (AssertionError) if no match within this window
    [--from-beginning]          # seek to earliest offset (default: seek to now - lookback)
    [--consumer-group <name>]   # override default consumer group (default: agctl-consumer)
```

**Examples:**

```bash
# Simple subset match
agctl kafka assert \
  --topic orders.created \
  --contains '{"order_id": "ord-789"}' \
  --timeout 5

# Predicate match — partial, no need to know full message structure
agctl kafka assert \
  --topic orders.created \
  --match '.value.payload.orderId == "ord-789" and .value.eventType != null' \
  --timeout 5

# Named pattern with param substitution (topic inferred from pattern)
agctl kafka assert \
  --pattern order-created \
  --param orderId=ord-789 \
  --timeout 10

# Named pattern, no params needed
agctl kafka assert \
  --pattern payment-failed \
  --timeout 15
```

**Offset & timing model (consume and assert):** By default the consumer seeks each partition to the timestamp `now - --lookback` (via `offsets_for_times`) and reads forward, rather than subscribing at "latest". This makes the common send-then-assert pattern reliable: an event published a moment before the command starts still falls inside the window. `--lookback` defaults to the resolved `--timeout` (look back as far as you wait forward); `--from-beginning` overrides to the earliest offset. For `assert`, committed offsets are ignored — each invocation re-seeks by time, so repeated asserts are independent and deterministic. On high-volume topics, narrow with `--match`/`--contains` to avoid matching stale events from prior runs.

**TLS / transport model (produce, consume, assert):** All three commands connect to brokers using the `kafka.ssl` block (see §2.1). Setting any field to a non-empty value enables TLS and defaults `security.protocol` to `SSL` (mTLS); `ca_location` is optional and falls back to the system trust store when unset. Hostname verification is **on** by default (librdkafka default) — set `endpoint_identification_algorithm: "none"` only for self-signed or dev brokers. An empty string (e.g. an unresolved `${VAR:-}`) is treated as unset, so a partially-configured `ssl:` block never silently downgrades to plaintext nor disables verification. TLS configuration is unit-tested only; the live integration suite runs a plaintext broker.

---

### 3.3 `agctl db` — Database Operations

All `db` commands resolve their connection with the following precedence: an explicit `--connection`, then the template's own `connection` field (when using `--template`), then `defaults.database_connection` in config.

Supports two input modes:
- `--template <name>` — use a named SQL template from `database.templates` in config (preferred)
- `--sql "..."` — free-form SQL (escape hatch)

Both modes accept `--param key=value`. Named parameters use `:paramName` (JDBC-style) in both templates and free-form SQL; agctl translates `:paramName` to the driver's native bind syntax at execution time. (`{placeholder}` is reserved for HTTP path/body params — see §2.3 — and is not used in SQL to avoid colliding with JSON literals like `'{"a":1}'::jsonb`.)

#### `agctl db query`

Execute a SQL query and return all rows.

```
agctl db query
    [--template <name>]         # named template from database.templates
    [--sql "SELECT ..."]        # free-form SQL; mutually exclusive with --template
    [--param key=value]         # repeatable; fills :paramName named params (templates and SQL)
    [--connection <name>]       # overrides template's connection and defaults
```

**Examples:**

```bash
# Using a named template (preferred)
agctl db query \
  --template find-order \
  --param orderId=ord-789

# Free-form SQL (escape hatch)
agctl db query \
  --sql "SELECT status FROM orders WHERE id = :orderId" \
  --param orderId=ord-789
```

#### `agctl db assert --expect-rows`

Assert that a query returns exactly N rows. Exits 1 if the count does not match.

```
agctl db assert
    [--template <name>]
    [--sql "SELECT ..."]
    --expect-rows <n>
    [--param key=value]
    [--connection <name>]
```

**Examples:**

```bash
# Using a named template
agctl db assert \
  --template find-order \
  --param orderId=ord-789 \
  --expect-rows 1

# Free-form SQL
agctl db assert \
  --sql "SELECT 1 FROM orders WHERE id = :orderId AND status = 'CONFIRMED'" \
  --param orderId=ord-789 \
  --expect-rows 1
```

#### `agctl db assert --expect-value`

Assert that a specific field in the first result row equals a given value.

```
agctl db assert
    [--template <name>]
    [--sql "SELECT ..."]
    --expect-value              # flag indicating value-assertion mode
    --path <jq-path>            # jq path into the first row object, e.g. ".status"
    --equals <value>            # expected value as a JSON-compatible string
    [--param key=value]
    [--connection <name>]
```

**Examples:**

```bash
# Template + value assertion
agctl db assert \
  --template find-order \
  --param orderId=ord-789 \
  --expect-value \
  --path ".status" \
  --equals "CONFIRMED"

# Aggregate template assertion
agctl db assert \
  --template count-failed-payments \
  --param since=2026-06-01T00:00:00Z \
  --expect-value \
  --path ".cnt" \
  --equals "0"

# Free-form SQL
agctl db assert \
  --sql "SELECT status FROM orders WHERE id = :orderId" \
  --param orderId=ord-789 \
  --expect-value \
  --path ".status" \
  --equals "CONFIRMED"
```

**Value coercion (`--equals`):** The argument is parsed as JSON when it is valid JSON (`"0"` → number 0, `"true"` → bool, `"null"` → null, `"[1,2]"` → array); otherwise it is treated as a plain string (e.g. `CONFIRMED`). The DB result value is coerced to a JSON-native type before comparison — numbers → number, booleans → bool, timestamps/dates → ISO 8601 string, null → null. Comparison is strict and type-aware: a number never equals a string (`0` ≠ `"0"`). To match a timestamp column, write `--equals "2026-06-29T14:22:00Z"`.

#### `agctl db execute`

Execute a write SQL statement (INSERT/UPDATE/DELETE) and return the affected row count. This command has **two safety gates** to prevent accidental writes:

```
agctl db execute
    [--template <name>]         # named template from database.templates (must have mode: write)
    [--sql "INSERT ..."]        # free-form SQL; mutually exclusive with --template
    [--param key=value]         # repeatable; fills :paramName named params (templates and SQL)
    [--connection <name>]       # required when using --sql; when using --template, the template's
                                #   connection is used if specified, otherwise defaults.database_connection
    --write                     # required flag to confirm write intent
```

**Explicit-target rule:** `db execute` refuses to write to the default connection implicitly. You must pass either `--template <name>` (which specifies a connection) or `--connection <name>` to explicitly name the write target. This prevents accidental writes when you forget which connection is default.

**Connection gate:** The target connection must have `writable: true` in config (validated by `agctl config validate`).

**Invocation gate:** The `--write` flag is required. A no-op call without `--write` fails with `ConfigError` (exit 2).

**Mode checking:** A template with `mode: read` is rejected by `db execute` (`ConfigError`, exit 2). Write templates should set `mode: write` to document intent and prevent accidental use in `db query`/`db assert`.

**Examples:**

```bash
# Using a write template (recommended)
agctl db execute --template insert-order \
  --param orderId=ord-789 \
  --param customerId=cust-42 \
  --write

# Free-form SQL with explicit connection
agctl db execute \
  --sql "UPDATE orders SET status = 'CANCELLED' WHERE id = :orderId" \
  --param orderId=ord-789 \
  --connection main-db-writable \
  --write
```

**Idempotency:** `db execute` does NOT enforce idempotency. If you need repeatable writes (e.g., for flaky-test resilience), encode idempotency in the SQL using `ON CONFLICT` (PostgreSQL) or `ON DUPLICATE KEY UPDATE` (MySQL). Consider using `RETURNING` clauses to return inserted/updated rows for verification.

#### `agctl db schema`

Discover live database schema — read-only and ungated. No `--write`, no `--template`, no `--sql`, no `--param`; ignores `writable`/`mode` entirely, so any configured connection (read-only or writable) is eligible. Two levels, picked by `--table`:

```
agctl db schema
    [--connection <name>]       # overrides defaults.database_connection
    [--schema <name>]           # schema filter (valid at both levels)
    [--table <name>]            # relation to drill into; omit for the relations list
```

- **Level 1 (no `--table`)** — list relations (tables and views) the agent can drill into.
- **Level 2 (`--table <name>`)** — return the single matching relation's columns, primary key, foreign keys, and unique constraints (both `pg_constraint`-backed UNIQUE constraints and standalone `CREATE UNIQUE INDEX` indexes). `--table` accepts views. Match is **exact-case** on the stored name; `--schema` disambiguates when the same name exists in multiple schemas.

**Capability gate:** schema discovery requires the driver to implement the optional `describe_schema` capability (see §9.1). A driver without it is valid for `db query`/`assert`/`execute` but ineligible for `db schema`; the command fails fast with `ConfigError` (exit 2) **without opening a connection**.

---

### 3.4 `agctl check` — Service Health

#### `agctl check ready`

Hit health endpoints for one or all configured services.

```
agctl check ready
    [--service <name>]          # check a single service
    [--all]                     # check all services defined in config (default when neither flag is given)
    [--timeout <seconds>]
```

With neither `--service` nor `--all`, every configured service is checked (equivalent to `--all`). A service is considered ready if its `health_path` returns HTTP 2xx. If `health_path` is not configured for a service, a `GET /` is attempted.

**Examples:**

```bash
agctl check ready --service order-service
agctl check ready --all
```

---

### 3.5 `agctl mock` — Mock Server (HTTP & Kafka)

The `agctl mock` command group provides two ways to run mock servers: foreground streaming (`mock run`) and managed daemon lifecycle (`mock start`/`stop`/`status`). Both share the same engine and configuration; choose the mode that fits your testing workflow.

#### `agctl mock run`

Run an HTTP mock server and/or Kafka reactors, streaming NDJSON events to stdout. The mock is SUT-facing: the real application's HTTP client points at the mock's `listen` address, and Kafka reactors join the SUT's real broker as consumers. The command blocks until stopped (foreground process, designed for backgrounding via `&` like `http ping`).

```
agctl mock run
    [--config <path>]            # auto-discovered by default
    [--http-listen <host:port>]  # literal string (NO ${} interpolation); overrides mocks.http.listen
    [--only http|kafka]          # run a single engine (HTTP-only needs no kafka extra)
    [--fail-fast]                # exit non-zero on the FIRST runtime error (default: continue + summarize)
    [--duration <seconds>]       # stop after N s; mutually exclusive with --until-stopped
    [--until-stopped]            # default: run until SIGTERM/SIGINT
```

**Lifecycle:**
- `--http-listen` is a **literal** `host:port` string (CLI args are not `${}`-interpolated, only YAML values).
- `--only` restricts to one engine; `kafka`-extra / `kafka.brokers` checks are gated on engines actually started.
- **No `mocks` section** → emits `started` + `summary` with zero counts and exits `0` (idempotent no-op).
- **`--only <engine>` with that engine absent** → `ConfigError` (exit 2).
- **Startup hazard:** when backgrounding, wrap with `nohup`/`setsid` so the mock is not killed by `SIGHUP` when the launching shell exits, and capture the PID.
- **Port-in-use guard:** at startup, refuses to bind if the port is already in use (emits a `ConfigError` envelope with a hint to kill the stale mock).

**NDJSON event stream (second streaming exception after `http ping`):**

```json
{"event":"started","http":{"listen":"0.0.0.0:18080","stubs":2},"kafka":{"reactors":[{"name":"order-command-handler","topic":"orders.commands","consumer_group":"agctl-mock-order-handler-<runid>"}]},"timestamp":"…"}
{"event":"http.hit","stub":"create-order","method":"POST","path":"/api/v1/orders","status":201,"duration_ms":3,"timestamp":"…"}
{"event":"http.unmatched","method":"GET","path":"/api/v1/unknown","status":404,"timestamp":"…"}
{"event":"http.body_parse_skipped","stub":"oauth-token","method":"POST","path":"/oauth/token","reason":"non-JSON body; response has unresolved placeholders","timestamp":"…"}
{"event":"capture.missing","stub":"epk-chatSearch","name":"ucp","from":".body.ucpID","timestamp":"…"}
{"event":"kafka.reacted","reactor":"order-command-handler","topic":"orders.events","key":"ord-789","duration_ms":1,"timestamp":"…"}
{"event":"kafka.skipped","reactor":"order-command-handler","topic":"orders.commands","reason":"non-object message value","count":3,"timestamp":"…"}
{"event":"kafka.error","reactor":"order-command-handler","topic":"orders.commands","offset":1043,"partition":2,"error":"…","fatal":false,"timestamp":"…"}
{"event":"summary","http_hits":7,"http_unmatched":1,"http_body_parse_skipped":0,"kafka_reactions":3,"kafka_skipped":3,"kafka_errors":0,"duration_ms":45213}
```

**Agent failure-stream protocol (load-bearing):**

The mock's failure signals (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`, `capture.missing`) live **only** on stdout, and the exit-1-at-shutdown escalation arrives only on a clean `SIGTERM`. The background `&`/`kill` pattern loses both by default. Agents must follow this protocol:

1. Redirect the mock's stdout to a log file: `agctl mock run > mock.log 2>&1 &`.
2. **Poll** `mock.log` for the `started` line before running the SUT (do not sleep a fixed delay).
3. Terminate with `SIGTERM` and `wait` — **never `SIGKILL`** (which skips shutdown/summary/exit-code).
4. After the test, **grep the log for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error` / `capture.missing`** regardless of the test result, and treat any hit as a failure. `capture.missing` is non-fatal (the mock substitutes empty string and continues), but it marks a likely-misconfigured `from` silently producing a plausible-but-wrong response — investigate rather than ignore.

**`--fail-fast` synchronous alternative:**

For foreground runs with `--duration`, `--fail-fast` exits `1` immediately on the first runtime error (first `kafka.error` or fatal reactor failure), avoiding the log-grep step.

**Examples:**

```bash
# Start both HTTP and Kafka mocks in background
agctl mock run &
MOCK_PID=$!

# ... run tests ...

# Stop gracefully and check exit code
kill $MOCK_PID
wait $MOCK_PID
EXIT_CODE=$?

# HTTP-only mode (no kafka extra needed)
agctl mock run --only http --http-listen 127.0.0.1:18080

# Fail-fast synchronous run with duration
agctl mock run --duration 30 --fail-fast
```

#### `agctl mock start` — managed daemon (start)

Start the mock server as a detached daemon with automatic pidfile management and readiness polling. The daemon runs the same engine as `mock run` but in the background, with its stdout redirected to a log file. The command returns a single JSON envelope once the daemon is ready (the `started` line has appeared in the log).

```
agctl mock start
    [--config <path>]            # auto-discovered by default
    [--http-listen <host:port>]  # literal (NO ${} interpolation); overrides mocks.http.listen
    [--only http|kafka]          # run a single engine
    [--fail-fast]                # forwarded to the daemon
    [--duration <seconds>]       # forwarded; daemon self-stops after N s
    [--state-dir <path>]         # default ./.agctl
```

`--until-stopped` is dropped (it is the daemon's default behavior). The flag set is `run`'s minus the streaming-lifecycle flags.

**Result shape (`mock.start`):**

```json
{
  "ok": true,
  "command": "mock.start",
  "result": {
    "pid": 12345,
    "listen": "0.0.0.0:18080",
    "log_path": "./.agctl/mock-18080.log",
    "stubs": 2,
    "reactors": ["order-command-handler"],
    "started_at": "2026-07-05T09:00:00Z"
  },
  "duration_ms": 312
}
```

The daemon writes its pidfile to `<state-dir>/mock-<port>.pid` (or `mock-kafka.pid` for Kafka-only mocks) and its NDJSON log to `<state-dir>/mock-<port>.log`. If a mock is already running on the resolved port, `start` fails with `ConfigError` (exit 2).

#### `agctl mock stop` — managed daemon (stop)

Stop a running mock daemon by signaling it, waiting for graceful shutdown, parsing the log for the final summary, and returning the verdict. If any fatal failure events are found (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`), `stop` surfaces them and exits 1 (the strict rule). `capture.missing` is included in the failure list but is non-fatal.

```
agctl mock stop
    [--listen <host:port>]       # select when >1 running
    [--pid <pid>]                # explicit selector
    [--all]                      # stop every running mock in --state-dir
    [--timeout <seconds>]        # graceful-wait budget (default 10); SIGKILL after
    [--state-dir <path>]
```

Selector resolution: no-arg works when exactly one mock is running in `--state-dir`; otherwise `--listen`, `--pid`, or `--all` is required (else `ConfigError` exit 2).

**Result shape (`mock.stop`) — clean stop (no fatal failures):**

```json
{
  "ok": true,
  "command": "mock.stop",
  "result": {
    "stopped": true,
    "pid": 12345,
    "signal": "SIGTERM",
    "summary": {
      "http_hits": 7,
      "http_unmatched": 0,
      "http_body_parse_skipped": 0,
      "kafka_reactions": 3,
      "kafka_skipped": 0,
      "kafka_errors": 0,
      "duration_ms": 45213
    },
    "failures": []
  },
  "duration_ms": 8
}
```

**Failure shape (`mock.stop`) — fatal failures found (exit 1):**

When fatal failures are detected, `stop` raises `AssertionFailure` (exit 1) and carries the verdict in `error.detail`:

```json
{
  "ok": false,
  "command": "mock.stop",
  "result": null,
  "error": {
    "type": "AssertionError",
    "message": "mock run had 2 fatal failure event(s)",
    "detail": {
      "stopped": true,
      "pid": 12345,
      "signal": "SIGTERM",
      "summary": {
        "http_hits": 7,
        "http_unmatched": 2,
        "http_body_parse_skipped": 0,
        "kafka_reactions": 3,
        "kafka_skipped": 0,
        "kafka_errors": 1,
        "duration_ms": 45213
      },
      "failures": [
        {"event": "http.unmatched", "method": "GET", "path": "/x", "timestamp": "..."},
        {"event": "kafka.error", "reactor": "order-command-handler", "error": "...", "timestamp": "..."}
      ]
    }
  },
  "duration_ms": 8
}
```

**`--all` shape:** with `--all`, `stop` iterates every running mock in `--state-dir`, collecting one verdict per mock. On success the verdicts are returned in `result.stopped` as an **array**; if **any** stopped mock had a fatal failure, `stop` raises `AssertionFailure` (exit 1) and the array is carried in `error.detail.stopped`.

**SIGKILL behavior:** when a mock daemon does not exit on SIGTERM within the `--timeout` budget, `stop` sends SIGKILL. In this case, the verdict includes a `warning` field (string) explaining the timeout, and the `summary` field may be absent or incomplete (the daemon was killed before it could emit the final `summary` line).

#### `agctl mock status` — managed daemon (status)

Query whether a mock daemon is running and, if so, report live statistics by reading the log file. This command never signals the daemon and never removes the pidfile — it is read-only introspection.

```
agctl mock status
    [--listen <host:port>]
    [--state-dir <path>]
```

**Result shape (`mock.status`) — running:**

```json
{
  "ok": true,
  "command": "mock.status",
  "result": {
    "running": true,
    "pid": 12345,
    "listen": "0.0.0.0:18080",
    "uptime_ms": 12034,
    "summary_so_far": {
      "http_hits": 3,
      "http_unmatched": 0,
      "http_body_parse_skipped": 0,
      "kafka_reactions": 1,
      "kafka_skipped": 0,
      "kafka_errors": 0
    },
    "failures_so_far": []
  },
  "duration_ms": 2
}
```

A not-running mock returns `{running:false}` (and `ok:true`, exit 0). Stale pidfiles (dead pid) are detected and cleaned up automatically.

#### Simplified lifecycle with managed daemons

The managed daemon commands collapse the four-step `mock run` protocol into two commands:

```bash
# Start the daemon (blocks until ready)
agctl mock start

# ... run the SUT / assertions ...

# Stop the daemon and get the verdict (fatal failures → exit 1)
agctl mock stop
```

The old protocol (redirect log → poll `started` → SIGTERM+wait → grep log) is now handled by `start`/`stop` internally. Agents no longer need to manage the PID file, log redirection, or failure grepping manually.

---

### 3.6 `agctl config` — Config Introspection

#### `agctl config validate`

Parse and validate `agctl.yaml`. Reports schema errors, unresolvable required env vars, dangling service/connection references in templates, malformed jq expressions in `mocks` (stub `match.jq` / reactor `match`), and major-version mismatches. Exits 2 on any error.

```
agctl config validate
    [--config <path>]
```

#### `agctl config show`

Dump the fully resolved configuration as JSON. All secret-looking values (password, token, key fields) are masked to `"***"`. Intended for debugging config resolution — **not** for agent discovery (use `agctl discover` instead).

```
agctl config show
    [--config <path>]
    [--unmask]                  # disable masking (use only in secured environments)
```

#### `agctl config init`

Write a sample `agctl.yaml` to the filesystem. The sample is a clean baseline that validates with no environment variables (all optional fields have defaults). Refuses to overwrite an existing file unless `--force` is passed. Does not accept `--config` (it bootstraps the config file itself).

```
agctl config init
    [--output <path>]           # default: ./agctl.yaml
    [--force]                   # overwrite if exists
```

#### `agctl config migrate`

Rewrite a dialect-`"1"` config to dialect `"2"` (envelope-rooted `match`). Backs up the original to `<path>.bak` and writes the rewritten config back to `<path>`. A config already at `"2"` is a clean no-op (`already_v2: true`, `rewrites: []`). Refuses to clobber an existing `<path>.bak` (`ConfigError`, exit 2 — remove or rename it first); the backup is the only safety net for the reformat the rewrite performs.

```
agctl config migrate
    [--config <path>]           # auto-discovered by default
    [--dry-run]                 # preview the rewrite; do not write
```

The rewrite walks the three `match`-site families and prepends the envelope prefix:

- `mocks.http.stubs.<name>.match.jq` → prefix `.body | ` (e.g. `.amount > 1000` → `.body | .amount > 1000`).
- `mocks.kafka.reactors.<name>.match` → prefix `.value | `.
- `kafka.patterns.<name>.match` → prefix `.value | `.

Then bumps `version` to `"2"`. `capture.*.from` and `match.body` are **not** visited (out of scope). Idempotent — expressions that already start with the prefix are not double-prefixed; an already-`"2"` config is returned unchanged.

**`cli_flags_note` (load-bearing caveat):** CLI `--match` flags (and the deprecated `--filter-key` alias) passed to `agctl http` / `agctl kafka` in shell scripts, agent prompts, or runbooks are **not** rewritten by this command (it walks the config file only). Prefix them manually: `.body | ` for HTTP, `.value | ` for Kafka. (`agctl mock run` has no `--match` CLI flag — mock matchers are config-file only, and ARE rewritten.)

**`formatting_note`:** the rewritten file is emitted via `yaml.safe_dump`, which normalizes indentation/quotes and drops comments; the original is preserved verbatim in `<path>.bak`. Review the full diff before committing.

**Result shape (excerpt):**

```json
{
  "ok": true,
  "command": "config.migrate",
  "result": {
    "path": "./agctl.yaml",
    "already_v2": false,
    "from_version": "1",
    "to_version": "2",
    "rewritten": [
      {"path": "mocks.http.stubs.create-order.match.jq", "before": ".amount > 1000", "after": ".body | .amount > 1000"}
    ],
    "cli_flags_note": "CLI --match flags (and the deprecated --filter-key alias) on `agctl http` / `agctl kafka` … must be prefixed manually: `.body | ` for HTTP, `.value | ` for Kafka.",
    "formatting_note": "yaml.safe_dump reformats the file and drops comments; the original is preserved in <path>.bak …"
  },
  "duration_ms": 3
}
```

---

### 3.7 `agctl discover` — Lazy Scoped Discovery

The discovery subsystem lets an agent understand what is available in the system **without loading everything into context at once**. It is structured as three progressive levels: a summary, a category listing, and a single-item detail. The agent fetches only what the current task requires.

> **Design principle:** discovery is a map, not a dump. The agent navigates to the detail it needs rather than receiving everything upfront.

#### Level 0 — System summary

Run once at the start of a session to understand system shape. Returns only counts and category names — no templates, no params, no SQL.

```bash
agctl discover
```

```json
{
  "ok": true,
  "command": "discover.summary",
  "result": {
    "services": 4,
    "http_templates": 12,
    "kafka_patterns": 5,
    "db_templates": 8,
    "hint": "Run 'agctl discover --category <name>' to list items. Categories: services, http-templates, kafka-patterns, db-templates"
  },
  "duration_ms": 1
}
```

#### Level 1 — Category listing

Returns names and one-line descriptions for all items in a category. No params, no examples, no SQL.

```bash
agctl discover --category <name>
# <name>: services | http-templates | kafka-patterns | db-templates
```

**Example — `agctl discover --category http-templates`:**

```json
{
  "ok": true,
  "command": "discover.category",
  "result": {
    "category": "http-templates",
    "count": 3,
    "items": [
      { "name": "create-order",       "description": "Create a new order for a customer" },
      { "name": "get-payment-status", "description": "Fetch payment status by order ID" },
      { "name": "cancel-order",       "description": "Cancel an existing order" }
    ],
    "hint": "Run 'agctl discover --category http-templates --name <name>' for full detail"
  },
  "duration_ms": 1
}
```

#### Level 2 — Single item detail

Returns the full schema for one named item: method, path, required params, and a ready-to-use example command with placeholder values. The agent fetches this immediately before using a template it has not seen before.

```bash
agctl discover --category <name> --name <item-name>
```

**Example — `agctl discover --category http-templates --name create-order`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "http-templates",
    "name": "create-order",
    "description": "Create a new order for a customer",
    "method": "POST",
    "service": "order-service",
    "path": "/api/v1/orders",
    "params": ["customer_id", "sku"],
    "example": "agctl http call create-order --param customer_id=X --param sku=Y"
  },
  "duration_ms": 1
}
```

**Example — `agctl discover --category db-templates --name find-order`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "db-templates",
    "name": "find-order",
    "description": "Fetch a single order by ID",
    "connection": "main-db",
    "sql": "SELECT id, status, total_cents, created_at FROM orders WHERE id = :orderId",
    "params": ["orderId"],
    "example": "agctl db query --template find-order --param orderId=X"
  },
  "duration_ms": 1
}
```

**Example — `agctl discover --category kafka-patterns --name order-created`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "kafka-patterns",
    "name": "order-created",
    "description": "An ORDER_CREATED event for a specific order",
    "topic": "orders.created",
    "params": ["orderId"],
    "example": "agctl kafka assert --pattern order-created --param orderId=X --timeout 10"
  },
  "duration_ms": 1
}
```

#### `--search` — Cross-category keyword search

When the agent does not know which category to look in, it searches across all categories by name and description. Returns a flat list of matching items at the name+description level only (no detail). The agent follows up with a Level 2 call for the item it wants to use.

```bash
agctl discover --search <term>
```

**Example — `agctl discover --search payment`:**

```json
{
  "ok": true,
  "command": "discover.search",
  "result": {
    "query": "payment",
    "matches": [
      { "category": "http-templates",  "name": "get-payment-status",   "description": "Fetch payment status by order ID" },
      { "category": "kafka-patterns",  "name": "payment-failed",        "description": "Any PAYMENT_FAILED event regardless of order" },
      { "category": "db-templates",    "name": "count-failed-payments", "description": "Count failed payments after a given timestamp" }
    ],
    "hint": "Run 'agctl discover --category <category> --name <name>' for full detail on any match"
  },
  "duration_ms": 2
}
```

#### Discovery workflow the agent must follow

The `AGENTS.md` template (§6) prescribes this exact sequence:

1. **Start of session:** run `agctl discover` → understand system size and available categories
2. **Before starting a task:** run `agctl discover --category <X>` for the relevant category only
3. **Before using any template:** run `agctl discover --category <X> --name <Y>` to get params and example
4. **When unsure which category:** run `agctl discover --search <term>`, then do step 3
5. **Never** load categories not relevant to the current task

#### `description` field requirement

Every template and pattern in `agctl.yaml` should have a `description` field. `agctl config validate` emits a **warning** (not an error) for any item missing a description, since discovery output degrades significantly without it.

---

## 4. Output Schema

### 4.1 Envelope

Every invocation writes exactly one JSON object to stdout (the sole exception is `http ping`, which streams one JSON object per ping plus a final summary — see §3.1):

```json
{
  "ok": true,
  "command": "http.call",
  "result": {},
  "error": null,
  "duration_ms": 142
}
```

| Field | Type | Description |
|---|---|---|
| `ok` | boolean | `true` if the command succeeded and all assertions passed |
| `command` | string | Dot-separated command path, e.g. `http.call`, `kafka.assert`, `db.query` |
| `result` | object or null | Command-specific payload; null when `ok` is false and no partial result exists |
| `error` | object or null | Populated on any failure; null on success |
| `duration_ms` | integer | Wall-clock time for the operation in milliseconds |

**Error object:**

```json
{
  "type": "AssertionError",
  "message": "Expected 1 row, got 0",
  "detail": {
    "sql": "SELECT 1 FROM orders WHERE id = :order_id AND status = 'CONFIRMED'",
    "params": {"order_id": "ord-789"},
    "actual_rows": 0,
    "expected_rows": 1
  }
}
```

**Error types:**

| Type | Exit code | Applies when |
|---|---|---|
| `AssertionError` | 1 | An assertion was evaluated and failed — including `kafka assert` timing out (no matching message within the window), `kafka consume --expect-count` receiving fewer than expected, and `http call`/`http request` response assertions (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`) evaluating false |
| `ConfigError` | 2 | Config missing/invalid, an unresolvable **required** env var, or a major-version (jq-dialect) mismatch — a v1 config under the v2 tool is rejected with a pointer to `agctl config migrate` |
| `ConnectionError` | 2 | Could not reach a service, broker, or database |
| `TimeoutError` | 1 | A non-assertion operation exceeded its time budget (e.g. a slow HTTP request or a hung DB query) |
| `TemplateNotFound` | 2 | Named template/pattern/connection does not exist in config |
| `InternalError` | 2 | Unexpected error in `agctl` itself |

### 4.2 Result Shapes by Command Group

#### `http.call` / `http.request`

```json
{
  "status_code": 201,
  "response_time_ms": 87,
  "headers": {
    "content-type": "application/json",
    "x-request-id": "7f3a1c9e"
  },
  "body": {
    "order_id": "ord-789",
    "status": "PENDING",
    "total_cents": 4999
  },
  "url": "http://localhost:8081/api/v1/orders",
  "method": "POST"
}
```

#### `kafka.consume`

```json
{
  "topic": "orders.created",
  "messages": [
    {
      "key": "ord-789",
      "value": {
        "order_id": "ord-789",
        "event_type": "ORDER_CREATED",
        "customer_id": "cust-42",
        "timestamp": "2026-06-29T14:22:01Z"
      },
      "partition": 2,
      "offset": 10043,
      "timestamp": "2026-06-29T14:22:01Z",
      "headers": {}
    }
  ],
  "count": 1,
  "timed_out": false
}
```

#### `kafka.produce`

```json
{
  "topic": "payments.commands",
  "partition": 0,
  "offset": 5512,
  "key": "ord-789",
  "timestamp": "2026-06-29T14:22:05Z"
}
```

#### `kafka.assert`

```json
{
  "topic": "orders.created",
  "matched": true,
  "matching_message": {
    "key": "ord-789",
    "value": {
      "order_id": "ord-789",
      "event_type": "ORDER_CREATED"
    },
    "partition": 2,
    "offset": 10043,
    "timestamp": "2026-06-29T14:22:01Z",
    "headers": {}
  },
  "messages_scanned": 3,
  "elapsed_ms": 340
}
```

#### `db.query`

```json
{
  "rows": [
    {
      "id": "ord-789",
      "status": "CONFIRMED",
      "total_cents": 4999,
      "created_at": "2026-06-29T14:22:00Z"
    }
  ],
  "row_count": 1,
  "connection": "main-db"
}
```

#### `db.assert`

```json
{
  "assertion_type": "expect_rows",
  "expected": 1,
  "actual": 1,
  "passed": true,
  "sql": "SELECT 1 FROM orders WHERE id = :order_id AND status = 'CONFIRMED'",
  "connection": "main-db"
}
```

```json
{
  "assertion_type": "expect_value",
  "path": ".status",
  "expected": "CONFIRMED",
  "actual": "CONFIRMED",
  "passed": true,
  "connection": "main-db"
}
```

#### `db.execute`

```json
{
  "rows_affected": 1,
  "returning": [
    {"id": "ord-789", "status": "PENDING", "created_at": "2026-07-03T12:34:56Z"}
  ],
  "connection": "main-db",
  "sql": "INSERT INTO orders (id, customer_id, status) VALUES (:orderId, :customerId, 'PENDING') ON CONFLICT (id) DO NOTHING RETURNING *"
}
```

`rows_affected` is an integer count or `null` (for statements like DDL that don't report a row count). `returning` is a list of dict rows when the SQL includes a `RETURNING` clause, otherwise an empty array `[]`.

#### `db.schema.tables`

```json
{
  "connection": "main-db",
  "schema_filter": null,
  "count": 2,
  "items": [
    { "schema": "public", "name": "orders",   "kind": "table", "column_count": 4 },
    { "schema": "public", "name": "order_view", "kind": "view",  "column_count": 3 }
  ],
  "hint": "Run 'agctl db schema --table <name> [--schema <name>] [--connection <name>]' for columns and keys"
}
```

#### `db.schema.table`

```json
{
  "connection": "main-db",
  "schema": "public",
  "table": "orders",
  "kind": "table",
  "comment": null,
  "columns": [
    { "name": "id", "data_type": "integer", "nullable": false, "default": null, "generated": null, "enum_values": null, "comment": null }
  ],
  "primary_key": ["id"],
  "foreign_keys": [
    { "name": "orders_customer_fkey", "columns": ["customer_id"], "references_schema": "public", "references_table": "customers", "references_columns": ["id"] }
  ],
  "unique_constraints": [
    { "name": "orders_external_id_key", "columns": ["external_id"] }
  ],
  "hint": "Use these columns in 'agctl db query' / 'db assert --sql' with :paramName bind params."
}
```

The `db` group is now **mixed**: the `schema.*` tags carry a `hint` string (like `discover`) to chain the agent's next call, while `db.query`/`db.assert`/`db.execute` do not.

#### `check.ready`

```json
{
  "services": {
    "order-service": {
      "ready": true,
      "status_code": 200,
      "url": "http://localhost:8081/actuator/health",
      "response_time_ms": 12
    },
    "payment-service": {
      "ready": false,
      "status_code": null,
      "url": "http://localhost:8082/health",
      "response_time_ms": null,
      "error": "Connection refused"
    }
  },
  "all_ready": false
}
```

Every service entry always includes `response_time_ms` (an integer on success, `null` when the request failed or did not complete); `error` is present only when `ready` is false.

#### `discover.summary`

```json
{
  "services": 4,
  "http_templates": 12,
  "kafka_patterns": 5,
  "db_templates": 8,
  "hint": "Run 'agctl discover --category <name>' to list items. Categories: services, http-templates, kafka-patterns, db-templates"
}
```

#### `discover.category`

```json
{
  "category": "http-templates",
  "count": 3,
  "items": [
    { "name": "create-order",       "description": "Create a new order for a customer" },
    { "name": "get-payment-status", "description": "Fetch payment status by order ID" }
  ],
  "hint": "Run 'agctl discover --category http-templates --name <name>' for full detail"
}
```

#### `discover.item`

Shape varies by category. All items share `name`, `description`, `params[]`, and `example`. HTTP templates add `method`, `service`, `path`; Kafka patterns add `topic` and `match`; DB templates add `connection` and `sql` (so an agent can read a query's result columns before writing a `--path` value assertion).

#### `discover.search`

```json
{
  "query": "payment",
  "matches": [
    { "category": "http-templates", "name": "get-payment-status",   "description": "Fetch payment status by order ID" },
    { "category": "kafka-patterns", "name": "payment-failed",        "description": "Any PAYMENT_FAILED event regardless of order" },
    { "category": "db-templates",   "name": "count-failed-payments", "description": "Count failed payments after a given timestamp" }
  ],
  "hint": "Run 'agctl discover --category <category> --name <name>' for full detail on any match"
}
```

#### `config.validate`

```json
{
  "valid": false,
  "errors": [
    {
      "path": "services.order-service.base_url",
      "message": "Unresolved environment variable: ORDER_SERVICE_URL"
    }
  ]
}
```

#### `config.show`

Returns the fully resolved config as a JSON object with secrets masked. Structure mirrors the YAML schema.

#### `config.init`

Success (file created):

```json
{
  "ok": true,
  "command": "config.init",
  "result": {
    "path": "./agctl.yaml",
    "created": true,
    "bytes": 4815
  },
  "duration_ms": 1.2
}
```

Refused to overwrite (without `--force`):

```json
{
  "ok": false,
  "command": "config.init",
  "result": {
    "path": "./agctl.yaml",
    "created": false
  },
  "error": {
    "type": "ConfigError",
    "message": "Refusing to overwrite existing ./agctl.yaml (pass --force to overwrite)."
  },
  "duration_ms": 0.8
}
```

#### `mock.run` streaming output

`mock run` is the second streaming exception (after `http ping`). It emits one JSON object per line (NDJSON) as events happen, plus a final `summary` line. Each line is a complete JSON object with an `event` field.

**Event types (per-line vocabulary):**

| Event | Description |
|---|---|
| `started` | Emitted once at startup after HTTP bind and Kafka probe succeed. Includes `http.listen`/`stubs` and `kafka.reactors[]`. |
| `http.hit` | Emitted per matching HTTP stub hit. Includes `stub`, `method`, `path`, `status`, `duration_ms`. |
| `http.unmatched` | Emitted per HTTP request that matched no stub (returned 404). Includes `method`, `path`, `status`. |
| `http.body_parse_skipped` | Emitted when a stub matches but the request body doesn't parse as JSON and the response has unresolved placeholders. Includes `stub`, `method`, `path`, `reason`. |
| `kafka.reacted` | Emitted per Kafka message that matched a reactor and produced a reaction. Includes `reactor`, `topic`, `key`, `duration_ms`. |
| `kafka.skipped` | Emitted when messages are consumed but not matched (e.g., non-object value). Includes `reactor`, `topic`, `reason`, `count`. |
| `kafka.error` | Emitted on a reaction produce failure or reactor error. Includes `reactor`, `topic`, `error`, `fatal`. Under `--fail-fast`, the run exits `1` immediately after a fatal error. |
| `capture.missing` | Emitted when an explicit `capture.<name>.from` resolves to `null`/missing at runtime (HTTP stub or Kafka reactor). Includes `stub` *or* `reactor`, `name`, `from`. Non-fatal: the mock substitutes empty string and continues; investigate as a likely-misconfigured `from`. |
| `summary` | Emitted once at shutdown. Includes `http_hits`, `http_unmatched`, `http_body_parse_skipped`, `kafka_reactions`, `kafka_skipped`, `kafka_errors`, `duration_ms`. |

**Agent protocol (load-bearing):** See `agctl mock` §3.5 for the background lifecycle protocol (redirect stdout → log, poll for `started`, SIGTERM+wait, grep log for errors). Without this, "fail loudly" is aspirational — a silent false-positive is possible.

**Startup errors:** Like `http ping`, startup failures emit **one** structured envelope before any event line (with `command: "mock.run"` and `error.type`), then exit `2`.

**Exit codes:**
- `0` — clean shutdown, no runtime errors.
- `1` — runtime errors occurred (`kafka_errors > 0` or fatal reactor failure) or `--fail-fast` triggered.
- `2` — startup error (config, bind, broker probe, or missing `kafka` extra).

---

## 5. Configuration Resolution Order

`agctl` resolves its configuration through the following precedence chain (highest to lowest):

1. **`--config <path>` CLI flag** — if provided, only this file is loaded; no discovery walk is performed.
2. **`AGCTL_CONFIG` environment variable** — if set, used as the config file path. Ignored when `--config` is explicitly passed.
3. **Auto-discovery** — search for `agctl.yaml` starting in the current working directory, walking up parent directories until the filesystem root or a `.git` directory is found (whichever is first). The first `agctl.yaml` found wins.
4. **`${ENV_VAR}` interpolation within YAML values** — after the file is located and parsed, all `${VAR}` references in string values are resolved from the process environment.
5. **`AGCTL_<SECTION>_<KEY>` environment variable overrides** — after file loading and `${}` interpolation, specific values can be overridden by structured env vars (see §8 for the exact convention).

**If no config file is found and no `--config` or `AGCTL_CONFIG` is set**, the tool exits with code 2 and a `ConfigError` JSON object.

**Env var override convention** (`AGCTL_<SECTION>__<KEY>` — double-underscore `__` separates path segments; a single `_` stays within a key segment):

```
AGCTL_DEFAULTS__TIMEOUT_SECONDS=30
AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP=my-group
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD=s3cr3t
AGCTL_SERVICES__ORDER_SERVICE__BASE_URL=http://order-svc:8080
```

To map a config path to an env var: uppercase each path segment, convert hyphens to `_` within a segment, and join segments with `__`. Example: `services.order-service.base_url` → `AGCTL_SERVICES__ORDER_SERVICE__BASE_URL`.

Two matching rules keep overrides predictable:

- **At least two segments required.** An override must contain a `__` separator. This prevents single-token env vars from being misread as overrides — notably `AGCTL_CONFIG` (the config-file path) and `AGCTL_TEST_*` flags, which must not touch config values.
- **Case- and hyphen-insensitive key matching against existing keys.** `AGCTL_SERVICES__ORDER_SERVICE__BASE_URL` overrides the real `services.order-service.base_url` entry rather than creating a phantom `order_service` sibling. When no existing key matches, a new key is written under the lowercased segment name.

Parsing back is not guaranteed to reconstruct hyphens (a `_` could be a hyphen or a literal underscore); overrides are write-oriented, so this is acceptable. The `__` delimiter mirrors Pydantic Settings / Spring Boot and keeps keys containing single underscores unambiguous.

---

## 6. AGENTS.md Template

The following is a ready-to-paste `AGENTS.md` section. Teams should customize the placeholder lines (marked `# CUSTOMIZE`) and commit it to their repository root.

````markdown
## Testing with `agctl` (`agt`)

This repo uses [`agctl`](https://github.com/your-org/agenttest) — a CLI testing
harness designed for use by AI coding agents. It is your primary tool for verifying
that code changes work correctly against running services.

### Setup

`agctl` reads `agctl.yaml` at the project root. Required environment variables
are listed in `.env.example`. Ensure they are set before running any `agctl` command.

### Discovering Available Resources

Use the three-level discovery workflow to orient yourself without consuming unnecessary context.

**Step 1 — System summary (always run first):**

```bash
agctl discover
```

Returns only counts and category names. Cheap — run it at the start of every session.

**Step 2 — List a category (only the category you need):**

```bash
agctl discover --category http-templates    # available HTTP request templates
agctl discover --category kafka-patterns    # available Kafka filter patterns
agctl discover --category db-templates      # available SQL query templates
agctl discover --category services          # registered services and base URLs
```

Returns names and one-line descriptions only — no params, no SQL.

**Step 3 — Get full detail for a specific item (before using it):**

```bash
agctl discover --category http-templates --name <template-name>
agctl discover --category db-templates --name <template-name>
agctl discover --category kafka-patterns --name <pattern-name>
```

Returns method, path, required params, and a ready-to-use example command.

**When you don't know which category to look in:**

```bash
agctl discover --search <keyword>
```

Searches names and descriptions across all categories. Returns name+description only — follow up with Step 3 for the item you want.

**Rules:**
- Never use `agctl config show` for discovery — it dumps raw config and is context-expensive
- Only load the category relevant to your current task
- Always run Step 3 before using a template you have not seen in this session

### Exit Codes

| Code | Meaning |
|------|---------|
| `0`  | Command succeeded; all assertions passed |
| `1`  | An assertion was evaluated and **failed** — the system is not in the expected state |
| `2`  | Tool or configuration error — do not interpret as an assertion result |

Exit code `2` means something is wrong with the environment or command invocation, not
with the system under test. Fix the command or environment before re-running.

### Interpreting Output

Every command returns a single JSON object. Always check `"ok": true` before using
`"result"`. On failure, `"error"` contains `"type"`, `"message"`, and `"detail"`.

```json
{ "ok": false, "command": "db.assert", "result": null,
  "error": { "type": "AssertionError", "message": "Expected 1 row, got 0",
             "detail": { "actual_rows": 0, "expected_rows": 1 } },
  "duration_ms": 18 }
```

### Common Testing Patterns

#### Pattern 1: Send a request and check the HTTP response

```bash
# Use a named template whenever one exists
agctl http call create-order \
  --param customer_id=cust-42 \
  --param sku=WIDGET-001

# Check the response in your shell / code using the returned JSON
```

#### Pattern 2: Trigger an action and assert a downstream Kafka event

```bash
# 1. Send the triggering request
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001

# 2. Assert that an order.created event appears within 5 seconds
agctl kafka assert \
  --topic orders.created \
  --contains '{"customer_id": "cust-42"}' \
  --timeout 5
```

> Send-then-assert is reliable by default: `kafka assert` seeks back by `--lookback` (default = `--timeout`), so an event published just before the assert starts is still matched. See §3.2.

#### Pattern 3: Full end-to-end: HTTP → Kafka → DB

```bash
# 1. Create order
ORDER=$(agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001)
ORDER_ID=$(echo $ORDER | jq -r '.result.body.order_id')

# 2. Assert Kafka event published
agctl kafka assert \
  --topic orders.created \
  --contains "{\"order_id\": \"$ORDER_ID\"}" \
  --timeout 10

# 3. Assert DB row exists and has correct status
agctl db assert \
  --sql "SELECT 1 FROM orders WHERE id = :order_id AND status = 'PENDING'" \
  --param order_id=$ORDER_ID \
  --expect-rows 1
```

#### Pattern 4: Query DB state for debugging

```bash
agctl db query \
  --sql "SELECT id, status, total_cents FROM orders WHERE id = :order_id" \
  --param order_id=ord-789
```

#### Pattern 5: Keep session alive during a long test

```bash
# Start heartbeat in background (5-second interval, run until killed)
agctl http ping heartbeat --interval 5 --until-stopped &
HEARTBEAT_PID=$!

# Run full test scenario
ORDER=$(agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001)
ORDER_ID=$(echo $ORDER | jq -r '.result.body.order_id')
agctl kafka assert --pattern order-created --param orderId=$ORDER_ID --timeout 10
agctl db assert --template find-order --param orderId=$ORDER_ID --expect-value --path ".status" --equals "PENDING"

# Stop heartbeat
kill $HEARTBEAT_PID
```

### Rules

- **Prefer templates over free-form requests.** Use `agctl http call <template>` unless no
  suitable template exists. Templates encode the correct service, path, and headers.
- **Do not swallow non-zero exit codes.** Treat exit code `1` as a test failure. Stop
  and diagnose before proceeding.
- **Do not assume service availability.** Run `agctl check ready --all` at the start of
  any test session if services may not be running.
- **Do not parse stderr.** All machine-readable output is on stdout. Stderr is for
  `agctl` internal errors and stack traces only.
- **Do not run `agctl config show --unmask` in CI.** The `--unmask` flag exposes secrets.
````

---

## 7. Project Structure

```
agctl/                         # Repository root
│
├── agctl/                     # Main Python package
│   ├── __init__.py
│   ├── cli.py                     # Click entry point; registers command groups; loads plugins
│   ├── command.py                 # @envelope decorator + load_config_or_raise
│   ├── output.py                  # emit() — the single permitted stdout write path
│   ├── errors.py                  # typed AgctlError hierarchy
│   ├── params.py                  # --param k=v  →  dict[str,str]
│   ├── resolution.py              # {placeholder} fill, body deep_merge, :name→%(name)s
│   ├── assertions.py              # jq / subset / equals / coercion primitives
│   ├── assertion_registry.py      # pluggable assertion-mode registry + entry-point discovery
│   ├── plugin_protocol.py         # Protocol contract for protocol plugins
│   │
│   ├── commands/                  # One module per command group
│   │   ├── __init__.py
│   │   ├── http_commands.py       # `agctl http call` / `request` / `ping`
│   │   ├── kafka_commands.py      # `agctl kafka produce / consume / assert`
│   │   ├── db_commands.py         # `agctl db query` and `agctl db assert`
│   │   ├── check_commands.py      # `agctl check ready`
│   │   ├── config_commands.py     # `agctl config validate` / `show` / `init`
│   │   └── discover_commands.py   # `agctl discover` (summary / category / item / search)
│   │
│   ├── config/                    # Config loading pipeline
│   │   ├── __init__.py
│   │   ├── loader.py              # Discovery walk, interpolation, override merge, validation
│   │   ├── resolver.py            # AGCTL_* env var override layer
│   │   ├── validator.py           # Cross-reference checks + description warnings
│   │   │                          #   (Pydantic schema validation lives in models.py)
│   │   └── models.py              # Pydantic v2 typed config models
│   │
│   └── clients/                   # Protocol clients; each is independently instantiable
│       ├── __init__.py
│       ├── http_client.py         # httpx-based (lazy import); accepts a ServiceConfig
│       ├── kafka_client.py        # confluent-kafka based (lazy import)
│       ├── db_client.py           # Dispatches to registered driver plugins
│       ├── db_driver_protocol.py  # DBDriver Protocol
│       └── db_drivers/
│           └── postgresql.py      # Built-in psycopg-backed driver (lazy import)
│
├── tests/
│   ├── unit/
│   │   ├── test_config_loader.py
│   │   ├── test_config_resolver.py
│   │   ├── test_output.py
│   │   └── test_template_resolution.py
│   └── integration/               # Require running services (Docker Compose)
│       ├── conftest.py
│       ├── test_http_commands.py
│       ├── test_kafka_commands.py
│       └── test_db_commands.py
│
├── pyproject.toml
├── agctl.yaml                 # Example config for this repo's own integration tests
└── AGENTS.md                      # Drop-in template (see §6)
```

### `pyproject.toml` skeleton

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "agctl"
version = "0.1.0"
description = "Agent-facing CLI harness for testing distributed systems"
requires-python = ">=3.11"
dependencies = [
    "click>=8.1",
    "pyyaml>=6.0",
    "pydantic>=2.0",
]

# Protocol libraries are OPTIONAL EXTRAS so users install only what they need
# and the package imports fast. Each client lazy-imports its library at construct
# time; a missing extra surfaces as a ConfigError pointing at the right
# `pip install 'agctl[...]'` rather than an opaque ImportError.
[project.optional-dependencies]
http = ["httpx>=0.27"]
kafka = ["confluent-kafka>=2.4", "jq>=1.6"]
db = ["psycopg[binary]>=3.1", "jq>=1.6"]
dev = ["pytest>=8.0"]
integration = ["testcontainers", "agctl[db,kafka,http]", "pytest>=8.0"]

[project.scripts]
agctl = "agctl.cli:cli"
agt = "agctl.cli:cli"

[project.entry-points."agctl.db_drivers"]
postgresql = "agctl.clients.db_drivers.postgresql:PostgreSQLDriver"

[project.entry-points."agctl.plugins"]
# Third-party plugins register here, e.g.:
# grpc = "agctl_grpc:GRPCPlugin"
```

### `output.py` interface

```python
# agctl/output.py

import json
import sys
import time
from typing import Any

def emit(
    ok: bool,
    command: str,
    result: Any = None,
    error: dict | None = None,
    duration_ms: int = 0,
) -> None:
    """Write the JSON envelope to stdout and flush. Call exactly once per invocation."""
    payload = {
        "ok": ok,
        "command": command,
        "result": result,
        "error": error,
        "duration_ms": duration_ms,
    }
    sys.stdout.write(json.dumps(payload, default=str))
    sys.stdout.write("\n")
    sys.stdout.flush()
```

---

## 8. Key Design Principles

### One JSON object per invocation

`output.emit()` is the **only** permitted stdout write path. Every command calls it exactly once before exiting. Any intermediate logging goes to stderr. The sole exception is `http ping`, which streams newline-delimited objects (one per ping plus a final summary) for background/keepalive use — see §3.1.

This is enforced structurally: each command callback is split into a thin Click command and a `_core` function, and the `_core` is wrapped by an `@envelope(command)` decorator (`command.py`) that guarantees exactly one `emit()` — and one process exit code — on every code path (success, assertion failure, or error).

### stderr is not machine-readable

Stderr is reserved for unexpected internal errors and stack traces. An agent must never parse stderr. If a stack trace reaches stderr, the command exits with code 2 and has already written a structured `InternalError` envelope to stdout.

### Stateless invocations

No session files, no lock files, no local state databases. Each invocation is fully self-contained. Kafka consumer groups are used for offset tracking when needed; that state lives in Kafka, not on disk.

**Bounded exception — mock daemon state.** The managed daemon commands (`mock start`/`stop`/`status`) introduce a deliberate, scoped carve-out: a pidfile (`mock-<port>.pid`) and NDJSON log (`mock-<port>.log`) under `<state-dir>/` (default `./.agctl/`). This is the sole exception to "no session files" and is confined to the daemon lifecycle only. No other commands write disk state.

### Windowed assertions (reliable send-then-assert)

`kafka assert`/`consume` seek to `now - --lookback` and read forward, rather than subscribing at "latest". This makes the send-then-assert pattern reliable without subscribe-before-produce gymnastics: the lookback window catches events published just before the command started. See §3.2.

### Fail fast, fail loudly

- Unresolvable env vars at config load time → immediate exit 2, never an empty string substitution.
- A failed assertion always exits 1, never 0. There is no "soft fail" mode.
- A template reference to a non-existent service key → exit 2 at startup, not at request time.

### Ten focused commands over fifty overlapping ones

The command surface area is intentionally small. Before adding a new command, verify it cannot be composed from existing ones.

### Self-documenting flag names

All flags use full words with hyphens: `--expect-rows`, `--filter-key`, `--from-beginning`. No single-letter flags except where established convention demands it (none currently).

### Env var override convention

```
AGCTL_<SECTION>__<KEY>=<value>      # double-underscore __ separates path segments
```

Mapping rules:
- Each path segment is uppercased.
- Hyphens within a segment become underscores: `main-db` → `MAIN_DB`.
- Path segments are joined with `__` (double underscore), so a single `_` unambiguously belongs to a key: `database.connections.main-db.password` → `AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD`.

Full examples:

```bash
AGCTL_DEFAULTS__TIMEOUT_SECONDS=30
AGCTL_KAFKA__DEFAULT_CONSUMER_GROUP=ci-consumer
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__HOST=localhost
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD=supersecret
AGCTL_SERVICES__ORDER_SERVICE__BASE_URL=http://order-svc:8080
```

These overrides are applied after `${}` interpolation and have the highest precedence. Matching is case- and hyphen-insensitive against existing config keys, and requires at least two `__`-separated segments (so `AGCTL_CONFIG` is not mistaken for an override) — see §5. (The `__` delimiter mirrors Pydantic Settings / Spring Boot and keeps keys containing single underscores unambiguous.)

---

## 9. Extension Points

### 9.1 Database Driver Plugins

`agctl` discovers DB drivers via the `agctl.db_drivers` entry point group. Each driver must implement the `DBDriver` protocol:

```python
# agctl/clients/db_driver_protocol.py
from typing import Protocol, Any

class DBDriver(Protocol):
    def connect(self, config: dict) -> None: ...
    def execute(self, sql: str, params: dict) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...
```

The Protocol requires only `connect`/`execute`/`close`. Two capabilities are **optional** and follow the same probe pattern (their presence is detected without opening a connection):

- **`execute_write(sql, params) -> {rows_affected, returning}`** — enables `agctl db execute`. Absent → the driver is read-only and `db execute` raises `ConfigError` (exit 2) before any database operation.
- **`describe_schema(table, schema) -> {items, matches}`** — enables `agctl db schema` (§3.3). Absent → the driver is valid but ineligible for `db schema`; `DbClient.supports_describe_schema()` is a pre-connect, side-effect-free probe, so the command fails fast with `ConfigError` (exit 2) **without opening a connection**.

To add a MySQL driver in a separate package:

```toml
# In agctl-mysql/pyproject.toml
[project.entry-points."agctl.db_drivers"]
mysql = "agctl_mysql:MySQLDriver"
```

`agctl` loads all registered drivers at startup and selects the correct one based on the `type` field in a `database` connection config.

### 9.2 Protocol Plugins

New top-level command groups (e.g., gRPC, GraphQL, WebSocket) are registered via the `agctl.plugins` entry point group. Each plugin must implement the `Plugin` protocol:

```python
# agctl/plugin_protocol.py
import click
from typing import Protocol

class Plugin(Protocol):
    name: str                         # used as the subcommand name, e.g. "grpc"
    command_group: click.Group        # the Click group to register
    def validate_config(self, config: dict) -> list[str]: ...
    # Returns a list of error strings; empty list = valid
```

`cli.py` iterates `entry_points(group="agctl.plugins")` and registers each `command_group` onto the root `cli` group (using `.name` as the subcommand, falling back to `command_group.name`). During `agctl config validate`, each loaded plugin's `validate_config(config)` is invoked with the fully-resolved config dict; any error strings it returns are folded into the validation result (exit 2). No changes to core code are needed.

### 9.3 Custom Assertion Types

For `agctl db assert` and `agctl kafka assert`, new assertion modes are added by subclassing `Assertion` and registering via `agctl.assertions`:

```toml
[project.entry-points."agctl.assertions"]
json_schema = "agctl_jsonschema:JSONSchemaAssertion"
```

A registered mode is invoked via the `--assertion <name>` escape hatch on `agctl db assert` and `agctl kafka assert` (mutually exclusive with the built-in modes). The mode's `evaluate(context)` receives a free-form context dict and returns `{"passed": bool, ...}` (or raises `AssertionFailure`):

- `db assert --assertion <name>`: `context = {"rows": [...], "row_count": int, "sql": str, "params": {...}, "connection": str}`.
- `kafka assert --assertion <name>`: `context = {"topic": str, "messages": [...], "count": int, "params": {...}}` (the full consumed lookback window).

`passed: false` (or a raised `AssertionFailure`) yields an `AssertionError` exit 1; an unknown name yields `TemplateNotFound` exit 2. Built-in modes (`expect_rows`, `expectValue`, `contains`, `match`, `pattern`) have dedicated flags and are rejected via `--assertion` with a `ConfigError`. They are registered by name only for discoverability and clean unknown-mode rejection; their logic lives in the command layer (calling `evaluate` on one raises `NotImplementedError`), so only third-party modes are actually invoked through `evaluate(context)`.

---

## 10. Open Questions / Future Work

These items are intentionally deferred. Do not implement them until the core design is stable.

| Item | Notes |
|---|---|
| **Schema Registry integration** | `kafka.assert` should optionally decode Avro/Protobuf messages using the configured schema registry. Currently, all Kafka values are treated as raw JSON. |
| **Message ordering assertions** | `agctl kafka assert --ordered` to verify a sequence of messages in partition order. Requires careful offset tracking. |
| **Multi-step scenario chaining** | A lightweight `scenarios` section in YAML that sequences named steps. Enables atomic pass/fail over a workflow without shell scripting. The JSON output envelope would add a `steps` array. |
| **MCP server wrapper** | Expose `agctl` as an MCP (Model Context Protocol) tool server so agents that support MCP can call it without shell access. The JSON output schema maps cleanly to MCP tool results. |
| **Retry / polling DSL** | `agctl db assert --retry-until-pass --max-attempts 5 --interval-ms 500` for assertions against eventually-consistent state. Currently, callers must implement polling themselves. |
| **Template variable validation** | Warn (or error) at call time if a template defines `{placeholder}` variables that are not supplied via `--param`. Currently, unsupplied placeholders are left as literal strings. |
| **gRPC support** | First-class `agctl grpc call` command via a plugin. The plugin protocol is designed; implementation is deferred. |
| **Secret backends** | Pull secrets from Vault or AWS Secrets Manager instead of environment variables. Would be implemented as a resolver plugin hooked into the config resolution pipeline (§5). |
| **Parallel command execution** | `agctl run --parallel step1.sh step2.sh` for agents that want to fire multiple requests concurrently and assert on all results. |
| **OpenTelemetry trace propagation** | Inject `traceparent` headers automatically when a trace context is available, enabling distributed traces that span `agctl` invocations. |
| **HTTP response extraction (`--capture path=name`)** | Agents still hand-roll shell `jq -r` to pull a field for the next command. Response *assertion* (`--status`/`--contains`/`--match`/`--jq-path`/`--equals`) covers the verify-on-response case; a built-in capture/extraction flag is deferred to keep v1 focused on fail-loudly. |
| **`--match-all` flag (HTTP / Kafka)** | The "every item" case (e.g. all order items satisfy a predicate). Today covered by a jq idiom (`all(.items[]; .predicate)`); a dedicated sibling flag is deferred. |
| **Mock: cross-transport reactions** | HTTP trigger → Kafka produce; Kafka trigger → HTTP callback. The trigger→reaction model admits this later without a rewrite. |
| **Mock: stateful / scenario mocks** | Sequences, "Nth call → Y", reactor behavior change after N messages. |
| **Mock: control socket / runtime RPC** | A control socket for live runtime control (add stub at runtime, live counter queries) is deferred. The current daemon model uses signal + log-file parsing for observation only (start/stop/status). |
| **Mock: record / replay** | Record real traffic into stubs for later replay. |
| **Mock: exactly-once reactor delivery** | Reaction retry/backoff on broker errors (today: at-least-once with idempotent reactions). |
| **Mock: TLS / HTTPS mock** | Cert-pinned SUT clients cannot connect to a plaintext mock. |
| **Mock: multiple HTTP servers / ports** | One server, many stubs (path-routed) is the only model today. |

### Known-wrong-result / Not Covered (Mock MVP Limitations)

The mock MVP covers **stateless, single-consumer, value-keyed, plaintext** flows. The following patterns are **not** mocked and tend toward a **plausible-but-wrong (false-green)** result rather than a clear failure. Use an external tool (WireMock/LocalStack) or wait for the deferred features for these:

| Pattern | Why it's not covered | Failure mode |
|---|---|---|
| **Stateful flows** (OAuth/token exchange, create-then-GET lifecycle, idempotency-key replay, pagination cursors, 429-then-retry) | Static engine returns the same canned response regardless of prior calls. | State-propagation and dedupe logic go untested → false green. |
| **TLS / HTTPS-pinned or `https://`-hardcoded SUT clients** | Plaintext mock only; cannot intercept HTTPS. | Integration is untested → false green (especially for payments/auth/healthcare). |
| **Cross-transport sagas** (Kafka trigger → HTTP callback) | No causal linkage; requires manual orchestration. | End-to-end flow goes unexercised → false green. |
| **Non-JSON Kafka values** (Avro/Protobuf/schema-registry-backed topics) | Emitted as `kafka.skipped` (visible), but topic is effectively un-mockable until decoding lands. | Topic appears idle → false green if consumer expects a reaction. |
| **Containerized SUT topology** (docker-compose) | `0.0.0.0` bind works, but operator must target `host.docker.internal` / host LAN IP and avoid a SUT that swallows connection errors. | SUT may silently fail to connect → false green if it treats network errors as "fallback worked." |
| **Shared broker + pinned `consumer_group` reused across runs/devs** | Partition split or resume-past-messages. | Silently missing/old reactions → false green. (Mitigated by unique-per-run default.) |

---

## 11. Agentic Workflow Use Cases

This section shows how `agctl` fits into real agentic development workflows end-to-end. Each use case describes the agent's goal, the discovery and testing sequence it follows, and the commands it runs. These are not scripts to copy verbatim — they are reference patterns for understanding how the CLI composes into coherent agent behavior.

---

### UC-1: Implement a new feature and verify it end-to-end

**Context:** The agent has been asked to implement a new endpoint (e.g. `POST /orders/bulk`) and verify that it works correctly against the running local environment.

**Agent workflow:**

```
1. Write and compile the code
2. Orient: agctl discover
3. Find relevant templates: agctl discover --category http-templates
4. Find relevant DB templates: agctl discover --category db-templates
5. Call the new endpoint (free-form, since no template exists yet)
6. Assert the HTTP response
7. Assert that events were published to Kafka
8. Assert that the DB reflects the new state
9. Report results
```

**Commands:**

```bash
# Step 2 — orient
agctl discover

# Step 3–4 — discover what already exists
agctl discover --category http-templates
agctl discover --category db-templates

# Step 5–6 — call the new endpoint and assert the response
RESULT=$(agctl http request   --service order-service   --method POST   --path /api/v1/orders/bulk   --body '{"customer_id": "cust-42", "items": [{"sku": "WIDGET-001", "qty": 2}]}')

echo $RESULT | jq '.ok'                        # must be true
echo $RESULT | jq '.result.status_code'        # expect 201
ORDER_ID=$(echo $RESULT | jq -r '.result.body.order_id')

# Step 7 — assert Kafka event published
agctl kafka assert   --topic orders.created   --match ".value.payload.orderId == "$ORDER_ID""   --timeout 10

# Step 8 — assert DB state
agctl db assert   --template find-order   --param orderId=$ORDER_ID   --expect-rows 1

agctl db assert   --template find-order   --param orderId=$ORDER_ID   --expect-value --path ".status" --equals "PENDING"
```

**What makes this work well:** the agent discovers what templates already exist before reaching for free-form commands. Free-form is only used for the new endpoint which has no template yet — after the feature is merged, a template should be added to `agctl.yaml`.

---

### UC-2: Reproduce and verify a bug fix

**Context:** A bug report says that cancelling an order that is already in `CANCELLED` state returns HTTP 200 instead of HTTP 409. The agent must reproduce the bug, fix the code, and verify the fix.

**Agent workflow:**

```
1. Orient and find the cancel-order template
2. Create an order (to get a valid order ID)
3. Cancel it once (happy path — should succeed)
4. Cancel it again (should now return 409)
5. Fix the code
6. Repeat steps 2–4 to verify the fix
```

**Commands:**

```bash
# Step 1 — find the relevant template
agctl discover --category http-templates
agctl discover --category http-templates --name cancel-order

# Step 2 — create an order
CREATE=$(agctl http call create-order   --param customer_id=cust-bug-repro   --param sku=WIDGET-001)
ORDER_ID=$(echo $CREATE | jq -r '.result.body.order_id')

# Step 3 — first cancel (expect 200)
agctl http call cancel-order --param order_id=$ORDER_ID
# assert ok=true, status_code=200

# Step 4 — second cancel (bug: returns 200, should return 409)
SECOND=$(agctl http call cancel-order --param order_id=$ORDER_ID)
echo $SECOND | jq '.result.status_code'
# actual: 200  ← bug confirmed

# ... agent fixes the code, restarts the service ...

# Step 6 — reproduce with fixed code
CREATE2=$(agctl http call create-order   --param customer_id=cust-bug-verify   --param sku=WIDGET-001)
ORDER_ID2=$(echo $CREATE2 | jq -r '.result.body.order_id')

agctl http call cancel-order --param order_id=$ORDER_ID2
# expect 200 ✓

RETRY=$(agctl http call cancel-order --param order_id=$ORDER_ID2)
echo $RETRY | jq '.result.status_code'
# expect 409 ✓ — fix verified
```

---

### UC-3: Session-aware test with heartbeat

**Context:** The system requires a heartbeat HTTP call every 5 seconds or the session expires and subsequent requests return 401. The agent must run a multi-step test scenario without losing the session.

**Agent workflow:**

```
1. Authenticate and obtain a session token
2. Start the heartbeat in the background
3. Run the full test scenario (may take 30–120 seconds)
4. Stop the heartbeat
5. Assert final state
```

**Commands:**

```bash
# Step 1 — authenticate (free-form, result contains session token)
AUTH=$(agctl http request   --service auth-service   --method POST   --path /api/v1/auth/login   --body '{"username": "test-agent", "password": "test"}')
TOKEN=$(echo $AUTH | jq -r '.result.body.token')

# Step 2 — start heartbeat in background, inject auth header
agctl http ping heartbeat   --header "Authorization=Bearer $TOKEN"   --interval 5   --until-stopped &
HEARTBEAT_PID=$!

# Step 3 — run the scenario (agent chains multiple steps here)
agctl http call create-order   --param customer_id=cust-session-test   --param sku=WIDGET-001   --header "Authorization=Bearer $TOKEN"

# ... additional steps ...

agctl kafka assert   --pattern order-created   --param orderId=<order_id>   --timeout 15

# Step 4 — stop heartbeat
kill $HEARTBEAT_PID

# Step 5 — final DB assertion
agctl db assert   --template find-order   --param orderId=<order_id>   --expect-value --path ".status" --equals "CONFIRMED"
```

---

### UC-4: Regression sweep after a refactor

**Context:** A core service has been refactored. The agent must verify that all key flows still work correctly. It uses named Kafka patterns and DB templates to keep assertions stable and readable.

**Agent workflow:**

```
1. Discover all templates and patterns
2. Run all critical HTTP flows via templates
3. Assert expected Kafka events for each flow
4. Assert DB state after each flow
5. Report any failures
```

**Commands:**

```bash
# Step 1 — discover all categories
agctl discover --category http-templates
agctl discover --category kafka-patterns
agctl discover --category db-templates

# Flow 1: order creation
O1=$(agctl http call create-order --param customer_id=cust-r1 --param sku=WIDGET-001)
OID1=$(echo $O1 | jq -r '.result.body.order_id')
agctl kafka assert --pattern order-created --param orderId=$OID1 --timeout 10
agctl db assert --template find-order --param orderId=$OID1 --expect-rows 1

# Flow 2: payment processing
agctl http call process-payment --param order_id=$OID1 --param amount_cents=4999
agctl kafka assert --topic payments.events   --match ".value.orderId == "$OID1" and .value.status == "SUCCESS""   --timeout 10
agctl db assert --template find-order --param orderId=$OID1   --expect-value --path ".status" --equals "PAID"

# Flow 3: order cancellation
O2=$(agctl http call create-order --param customer_id=cust-r2 --param sku=WIDGET-002)
OID2=$(echo $O2 | jq -r '.result.body.order_id')
agctl http call cancel-order --param order_id=$OID2
agctl db assert --template find-order --param orderId=$OID2   --expect-value --path ".status" --equals "CANCELLED"

# Flow 4: assert no unexpected failures in DB
agctl db assert   --template count-failed-payments   --param since=$(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ)   --expect-value --path ".cnt" --equals "0"
```

---

### UC-5: Diagnosing a flaky test

**Context:** A test passes sometimes and fails sometimes. The agent suspects a race condition between the HTTP response and the Kafka event. It uses `kafka consume` to inspect what is actually being published, and `db query` to check raw state at each step.

**Agent workflow:**

```
1. Run the action
2. Consume Kafka with a wide timeout and inspect all messages
3. Query DB directly to see raw state
4. Adjust timing assumptions or find the actual bug
```

**Commands:**

```bash
# Step 1 — trigger the action
agctl http call create-order   --param customer_id=cust-flaky   --param sku=WIDGET-001

# Step 2 — consume broadly to see what actually arrived and when
agctl kafka consume   --topic orders.created   --timeout 30   --match '.value.payload.customerId == "cust-flaky"'
# Inspect: how many messages? what timestamps? what fields?

# Step 3 — query raw DB state immediately
agctl db query   --sql "SELECT id, status, created_at, updated_at FROM orders WHERE customer_id = :cid ORDER BY created_at DESC LIMIT 5"   --param cid=cust-flaky
# Inspect: is the row there? what status? any timing anomaly?

# Step 4 — re-run assertion with a longer timeout to confirm it's a timing issue
agctl kafka assert   --topic orders.created   --match '.value.payload.customerId == "cust-flaky"'   --timeout 30
# If this passes but --timeout 5 fails: confirmed race condition, increase default timeout
```

---

### Summary: which command for which agent intent

| Agent intent | Primary command |
|---|---|
| Understand what the system offers | `agctl discover` (levels 0→1→2) |
| Send a known request type | `agctl http call <template>` |
| Send an ad-hoc request | `agctl http request --service ... --path ...` |
| Keep a session alive during a long test | `agctl http ping <template> --interval N --until-stopped` |
| Verify an event was published | `agctl kafka assert --pattern / --match` |
| Inspect what was actually published | `agctl kafka consume --match` |
| Assert DB row exists / count | `agctl db assert --template --expect-rows` |
| Assert a specific DB field value | `agctl db assert --template --expect-value --path --equals` |
| Inspect raw DB state for debugging | `agctl db query --template / --sql` |
| Check services are up before testing | `agctl check ready --all` |
| Bootstrap a starter config | `agctl config init` |
| Debug config resolution | `agctl config show` |
| Validate config before committing | `agctl config validate` |
