# `agctl` — CLI Design Document

**Version:** 0.6-draft  
**Last updated:** 2026-07-06  
**Status:** Foundation design — hardened; ready for implementation  
**Change spec:** [`docs/superpowers/specs/2026-07-02-agctl-design-hardening.md`](./superpowers/specs/2026-07-02-agctl-design-hardening.md)

---

## Table of Contents

1. [Goals & Non-Goals](#1-goals--non-goals)
2. [Configuration Schema](#2-configuration-schema)
3. [CLI Command Design](#3-cli-command-design) *(includes `agctl discover` §3.9)*
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

> **Mocking support:** `agctl` now includes a built-in mock server for HTTP, Kafka, and gRPC (see `agctl mock` in §3.6 and the `mocks:` config section in §2.1). This supersedes the earlier non-goal — local testing no longer requires external tools like WireMock or LocalStack for common HTTP stubbing, Kafka reaction, and gRPC stubbing patterns.

> **Long-saga / high-volume capture:** `agctl kafka listen` runs a long-lived Kafka capture daemon that seeks to the latest offset at `start` (positioned at the head BEFORE the runbook trigger fires), records every matching message to disk, and evaluates attached expectations with no wall-clock deadline (see §3.2). This closes the three false-negative surfaces a windowed `kafka assert` can hit on long sagas or high-volume topics: scan-window misses, volume-induced timeout truncation, and broker retention cleanup.

---

## 2. Configuration Schema

### 2.1 Schema Reference

```yaml
# agctl.yaml
# ---------------
# Version is the config schema version. "3" = named `kafka.clusters` (the flat
# `kafka:` block was lifted into `kafka.clusters.<name>` + `default_cluster`).
# The `match` envelope-rooting introduced under "2" (`.body.x` for HTTP,
# `.value.x` for Kafka) is unchanged in v3. A major-version mismatch is a
# ConfigError (exit 2) pointing at `agctl config migrate`; minor/patch are not
# tracked.
version: "3"

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
# kafka — named clusters + patterns (v3). Mirrors database.connections:
#   clusters.<name>: a named broker profile (brokers, TLS, timeout, group, schema registry)
#   default_cluster: cluster used when no --cluster flag / pattern.cluster selects one
#                    (required only when >1 cluster is defined; a single cluster auto-defaults)
# Each cluster's fields support ${ENV_VAR} interpolation (see §2.2).
# ---------------------------------------------------------------------------
kafka:
  clusters:
    default:
      brokers:
        - "${KAFKA_BROKER_HOST}:9092"
      default_consumer_group: "agctl-consumer"  # optional
      schema_registry_url: "${SCHEMA_REGISTRY_URL}" # optional; omit if not used
      timeout_seconds: 30                            # default consume/assert timeout

      # kafka.clusters.<name>.ssl — optional TLS/mTLS settings, per cluster.
      #   Setting any field (to a non-empty value) enables TLS; security.protocol
      #   defaults to "SSL" unless overridden. Hostname verification stays on
      #   unless endpoint_identification_algorithm is set to "none" (self-signed/dev).
      #   ca_location is optional: when unset, librdkafka falls back to the system
      #   trust store (use it for publicly-trusted brokers like Confluent Cloud; pin
      #   a CA for private-PKI brokers). All values support ${ENV_VAR} interpolation
      #   and AGCTL_KAFKA__CLUSTERS__<NAME>__SSL__* overrides; an empty string counts as unset.
      ssl:
        ca_location: "${KAFKA_SSL_CA:-}"               # path to CA certificate (PEM); optional
        certificate_location: "${KAFKA_SSL_CERT:-}"    # path to client certificate (mTLS)
        key_location: "${KAFKA_SSL_KEY:-}"             # path to client private key (mTLS)
        key_password: "${KAFKA_SSL_KEY_PASSWORD:-}"    # optional private-key password
        # endpoint_identification_algorithm: "none"    # uncomment to disable hostname verification
        # security_protocol: "SSL"                     # defaults to SSL; set SASL_SSL when adding SASL later

    # A second named cluster (optional). Patterns and reactors bind a cluster by name;
    # the CLI `--cluster` flag overrides any binding. Omit `default_cluster` only when
    # exactly one cluster is defined (it auto-defaults); with >1 cluster, `default_cluster`
    # is required.
    # analytics:
    #   brokers:
    #     - "${ANALYTICS_KAFKA_BROKER_HOST}:9092"

  default_cluster: default

# ---------------------------------------------------------------------------
# kafka.patterns — named Kafka filter patterns, analogous to HTTP templates.
# topic:   Kafka topic name
# match:   jq predicate expression evaluated against each message envelope
#          ({key, value, partition, offset, timestamp, headers}); so .value.eventType,
#          .value.payload.orderId, .key, .headers.<name>. Supports {placeholder}
#          substitution via --param at assert time.
# cluster: optional named cluster this pattern binds to (default: default_cluster,
#          or the single defined cluster). The CLI `--cluster` flag overrides it.
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
# mocks — HTTP mock server, Kafka reactors, and gRPC mock server (DESIGN §3.6).
#   HTTP: agctl serves stubs; the SUT's HTTP client points at `listen`.
#   Kafka: agctl joins as a consumer on the SUT's real broker and reacts.
#   gRPC: agctl serves stubs for all four call types; the SUT's gRPC client
#         points at `listen`. Auto-serves Health + Reflection.
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
        # cluster: analytics                          # OPTIONAL named cluster this reactor binds to
                                                      # (default: kafka.default_cluster / single-cluster auto-default)
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
  # mocks.grpc — gRPC mock server (DESIGN §3.6). agctl serves stubs for all
  # four call types (unary / server-streaming / client-streaming / bidi) via a
  # descriptor-driven generic servicer; the SUT's gRPC client points at
  # `listen`. Auto-serves grpc.health.v1.Health (SERVING) + server reflection.
  # Supports ${ENV} interpolation at load, {placeholder} substitution at match
  # time, and jq predicates on stubs (match.jq). Requires the `grpc` extra
  # (`pip install 'agctl[grpc]'`); a missing extra → ConfigError (exit 2).
  # Call type is DERIVED from the descriptor (not configured); response-shape
  # vs call type is validated at server construction.
  # ---------------------------------------------------------------------------
  grpc:
    listen: "${AGCTL_MOCK_GRPC_HOST:-0.0.0.0}:${AGCTL_MOCK_GRPC_PORT:-50051}"
    # descriptors: optional list of GrpcDescriptorSource (proto / descriptor_set);
    #              omit to fall back to top-level grpc.descriptors.
    # descriptors:
    #   - proto: "/path/to/echo.proto"
    #     include_paths: ["/path/to/protos"]
    #   - descriptor_set: "/path/to/echo.pb"
    reflection: true                     # serve ServerReflection (default true)
    health: true                         # serve grpc.health.v1.Health (default true)
    concurrency_cap: 64                  # max in-flight RPCs (ThreadPoolExecutor)
    stubs:
      echo-unary:
        description: "Mock the Echo unary RPC"
        service: "echo.EchoService"      # fully-qualified
        method: "Unary"
        match:                           # optional; both sub-fields AND-ed; omit = always match
          body: { "msg": "hello" }       # json_subset on the deserialized request message
          # jq: '.message.msg == "hello"' # jq predicate on the per-call-type envelope
          #                                 (unary/server_stream/bidi = {service, method, metadata
          #                                 (lowercased), message}; client_stream = {service, method,
          #                                 metadata, messages:[…], count} matched once at stream close).
          #                                 `match.body` is skipped for client_stream.
        # capture: same CaptureSpec shape as HTTP/Kafka stubs; `from` shares the `match.jq`
        # root — the per-call-type gRPC envelope. `metadata` keys are lowercased.
        # capture:
        #   msg: { from: ".message.msg" }
        response:
          status: OK                     # gRPC status name OR numeric code; default OK
          message: { msg: "{msg}" }      # single response message (unary / client-stream / bidi)
          # messages:                     # server-streaming: ordered list; one gRPC message per entry
          #   - message: { chunk: "a" }
          #     delay_ms: 0
          #   - message: { chunk: "b" }
          #     delay_ms: 100
          metadata:                       # optional; initial metadata (and trailers on terminal status)
            x-mock: "true"
        delay_ms: 0

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
# logs — log source configuration for log query/assert/tail.
#   sources.<name>: named log source definitions
#     type: backend type (default "file"; extensible via agctl.logs_backends entry point)
#     path: file path (required for type "file")
#     format: log format (default "logstash"; "logstash" = NDJSON with @timestamp)
#     service: optional service name override (defaults to source name)
#   defaults: fallback values for logs commands
#     tail_lines: number of trailing lines to read (default 200)
#     limit: max entries to return from query (default 50)
#     timeout_seconds: default timeout for logs assert (default 10)
#     poll_interval_ms: poll interval for tail/assert (default 100)
# ---------------------------------------------------------------------------
logs:
  sources:
    order-service:
      type: file
      path: "/var/log/order-service/app.log"
      format: logstash
      service: order-service

    payment-service:
      type: file
      path: "/var/log/payment-service/app.log"
      format: logstash
      service: payment-service

  defaults:
    tail_lines: 200
    limit: 50
    timeout_seconds: 10
    poll_interval_ms: 100

# ---------------------------------------------------------------------------
# grpc — gRPC targets, descriptors, and call templates.
#   targets.<name>: gRPC endpoint configuration
#     address: host:port of the gRPC server
#     use_tls: enable TLS (default false)
#     tls: optional TLS/mTLS settings
#       ca_location: path to CA certificate (PEM)
#       certificate_location: path to client certificate (mTLS)
#       key_location: path to client private key (mTLS)
#       override_authority: override TLS authority name for self-signed certs
#     reflection: server reflection mode ("auto" tries reflection, falls back to descriptors; "on" requires reflection; "off" disables it)
#   descriptors: fallback proto descriptor sources (used when reflection is unavailable/off)
#     proto: path to .proto file to compile on-the-fly
#     include_paths: proto import paths (optional, for proto files)
#     descriptor_set: path to precompiled .pb descriptor set file
#   templates.<name>: gRPC call templates
#     description: human-readable description
#     target: name of target from targets
#     service: fully-qualified service name (e.g. echo.EchoService)
#     method: method name within the service
#     metadata: optional metadata dict (supports {placeholder})
#     message: request message JSON (supports {placeholder})
# ---------------------------------------------------------------------------
grpc:
  targets:
    echo-server:
      address: "localhost:50051"
      use_tls: false
      reflection: auto

    secure-server:
      address: "prod.example.com:443"
      use_tls: true
      tls:
        ca_location: "/etc/ssl/certs/ca.pem"
        certificate_location: "/etc/ssl/certs/client.pem"
        key_location: "/etc/ssl/private/client.key"
      reflection: auto

  descriptors:
    - proto: "/path/to/echo.proto"          # compile on-the-fly
      include_paths: ["/path/to/protos"]
    - descriptor_set: "/path/to/descriptor.pb"  # precompiled

  templates:
    echo-unary:
      description: "Call the Echo unary method"
      target: echo-server
      service: "echo.EchoService"
      method: "Unary"
      message:
        message: "{msg}"
      metadata:
        x-trace-id: "{trace_id}"

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

**`.env` defaults.** Env vars for `${...}` interpolation and `AGCTL_*` overrides may also come from a `.env` file, loaded as **defaults** — a value already in the real environment overrides `.env`, so CI/production can inject real secrets over committed defaults (the universal convention: docker compose, python-dotenv, rails, django):

- A `.env` next to the **resolved** `agctl.yaml` is loaded automatically — no need to `source` it in the shell first.
- Point at a different file with `--env-file <path>` (global flag) or `AGCTL_ENV_FILE`. Precedence: `--env-file` > `AGCTL_ENV_FILE` > sibling `.env`; an explicit source *replaces* the auto-load (like `--config` replaces walk-up).
- A missing sibling `.env` is a silent no-op; a missing explicit `--env-file` / `AGCTL_ENV_FILE` is a config error (exit 2), mirroring `--config`.
- Raw `.env` values flow into the same `${...}` interpolation as real env vars — agctl's single engine owns all `${...}` resolution (chained, nested), so a line like `FOO=a-${BAR}` resolves consistently. The `.env` itself does not do its own `${VAR}` expansion.

### 2.3 Path Parameter Syntax

HTTP template `path` and `body` values use `{placeholder}` (single braces) for runtime substitution via `--param key=value`. SQL (templates and free-form) uses `:paramName` (JDBC-style) instead — `{...}` is avoided in SQL to prevent collisions with JSON literals. Both are distinct from env var interpolation (`${...}`), which is resolved at config load time.

### 2.4 Overlay Fragments

An overlay is a partial config file layered on top of the base `agctl.yaml` to provide runbook-specific fixtures or test-time overrides without cluttering the shared config. Overlays are merged with the base config before validation; the final result is a complete, valid `Config` object.

**Overlay syntax:** An overlay file is any YAML file that matches the `agctl.yaml` schema but with the `version` field optional (inherited from the base). All other fields follow the same rules as the base config.

**Usage:** Pass `--overlay <path>` one or more times to any `agctl` command. Overlays are applied in flag order (later overlays win on conflict).

**Runbook sidecar convention:** A runbook can carry its own fixtures as a co-located overlay fragment `<runbook-base>.agctl.yaml` (sibling to the runbook markdown file). The runbook stays pure markdown and gains one `Preconditions` line `Requires overlay: <file>`.

**Merge behavior (sidecar-wins):**
- Nested dicts are merged key-by-key (recursive).
- Scalar values and lists are replaced by the overlay (no array merge).
- Type clashes (e.g. overlay provides a string where base expects a list) raise `ConfigError`.

**Override tracking:** `config validate --overlay` surfaces each overridden leaf as a warning: `{"path": "<dotted-path>", "message": "overridden by overlay <file>"}`. Cross-file dangling references remain hard errors (exit 2).

**Precedence:** Base config < overlays (in flag order) < `AGCTL_*` environment variable overrides. This is not a schema version change — overlays load over v2 and do not bump the dialect.

---

## 3. CLI Command Design

All commands share these global flags:

| Flag | Default | Description |
|---|---|---|
| `--config <path>` | auto-discovered | Explicit path to `agctl.yaml` |
| `--env-file <path>` | `.env` next to resolved config | Explicit path to a `.env` file; values are defaults, real env wins (precedence: `--env-file` > `AGCTL_ENV_FILE` > sibling `.env`) |
| `--overlay <path>` | — | Overlay config fragment (repeatable; later wins); layered on base config |
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

Free-form HTTP request — escape hatch for cases not covered by templates. Two mutually-exclusive invocation modes:

- **Service mode** — `--service <name>` + `--path <path>`, resolved against a configured service's base URL.
- **URL mode** — `--url <full-url>` for a complete request URL with **no service registration required** (the HTTP analog of `db query --sql`). Use this for endpoints not in config: external APIs, ngrok tunnels, ephemeral ports, URLs pulled from logs.

`--url` cannot be combined with `--service` or `--path` (`ConfigError`, exit 2); exactly one mode is required. `${ENV}` interpolation does **not** apply to `--url` — CLI args are never `${}`-interpolated (only YAML values are), so use shell `$VAR` expansion.

```
agctl http request
    # --- exactly one mode (mutually exclusive) ---
    [--service <name>]      # service mode: must match a services key in config
    [--path <path>]         # service mode: e.g. /api/v1/orders/ord-123
    [--url <full-url>]      # URL mode: full request URL, e.g. https://host/path?q=1
    # --- common ---
    [--method GET|POST|PUT|PATCH|DELETE]   # default: GET
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

**Examples:**

```bash
# Service mode (method defaults to GET)
agctl http request --service order-service --path /api/v1/orders/ord-789

# URL mode — full URL, no service registration needed
agctl http request --url https://abc123.ngrok.io/api/v1/orders/ord-789

# URL mode with a POST + body
agctl http request --url https://host/api/v1/orders --method POST --body '{"customer_id":"cust-42"}'
```

**Response assertions (`http call` and `http request`):** Optional flags let you assert on the response in the same invocation, with the same exit-code discipline as `kafka assert` / `db assert`. Zero flags (the default) leaves behavior unchanged — a 4xx/5xx response is still `ok:true` (HTTP status is a result, not an assertion). Supplying ≥1 flag enters assertion mode; all active flags must pass (AND). A failed flag raises `AssertionError` (exit 1) with `error.detail = {response, failures}`, where `response` is the full HTTP result and `failures` lists every failing mode (no short-circuit). Each failure entry includes a `root` label and a (size-capped) payload snapshot (e.g. `contains`/`jq-path` failures include `"root": "response body"` + `"body": <response body>`, while `match` failures include `"root": "response envelope"` + `"body": <response body>`) so an agent can self-correct a mis-rooted expression without dropping the flag and re-running raw. The full, untruncated body always remains at `error.detail.response.body`.

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
    <template-name> | --service <name> --path <path> | --url <full-url>
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
    [--timeout <seconds>]       # default: resolved cluster's timeout_seconds from config
    [--lookback <seconds>]      # seek to now - lookback before reading; default: = --timeout
    [--match <jq-expr>]         # jq boolean predicate over the message envelope
                                #   ({key, value, partition, offset, timestamp, headers}); only
                                #   messages where the expression is true are counted/returned
    [--filter-key <jq-expr>]    # DEPRECATED alias for --match; prefer --match
    [--expect-count <n>]        # if set, exit 1 (AssertionError) if fewer than n matching
                                #   messages are received within the window
    [--from-beginning]          # seek to earliest offset (default: seek to now - lookback)
    [--consumer-group <name>]   # override default consumer group (default: agctl-consumer)
    [--cluster <name>]          # named kafka cluster (default: kafka.default_cluster / single-cluster)
```

`--match` is a jq boolean expression evaluated against each message **envelope** (`{key, value, partition, offset, timestamp, headers}`) — so `.value.eventType`, `.key`, `.headers.rqUID`. Header keys are case-sensitive (as-produced). A **malformed expression** (syntax error) raises `ConfigError` (exit 2) before any polling; messages where the expression evaluates to `false` or raises a **runtime error** against that specific message are silently skipped. This enables partial matching — you do not need to know the full message structure.

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
    [--cluster <name>]          # named kafka cluster (default: kafka.default_cluster / single-cluster)
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
    [--cluster <name>]          # named kafka cluster; overrides the pattern's bound cluster
                                #   (default: pattern.cluster > kafka.default_cluster / single-cluster)
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

**TLS / transport model (produce, consume, assert):** All three commands connect to brokers using the resolved cluster's `ssl` block (see §2.1). Setting any field to a non-empty value enables TLS and defaults `security.protocol` to `SSL` (mTLS); `ca_location` is optional and falls back to the system trust store when unset. Hostname verification is **on** by default (librdkafka default) — set `endpoint_identification_algorithm: "none"` only for self-signed or dev brokers. An empty string (e.g. an unresolved `${VAR:-}`) is treated as unset, so a partially-configured `ssl:` block never silently downgrades to plaintext nor disables verification. TLS configuration is unit-tested only; the live integration suite runs a plaintext broker.

**Cluster resolution (produce, consume, assert):** All three commands target a single named cluster per invocation. Resolution precedence: `--cluster` (explicit) > a pattern's bound `cluster` (`assert --pattern` only) > `kafka.default_cluster` > the single defined cluster when exactly one is configured. An unresolvable name (`>1 cluster` and no `--cluster`/`default_cluster`), or a name absent from `kafka.clusters`, raises `ConfigError` (exit 2). This mirrors DB connection resolution (`--connection` > template's `connection` > `defaults.database_connection`).

#### `agctl kafka listen` — Long-lived Capture Daemon

A long-lived Kafka capture daemon for verifying events on **long sagas**, **high-volume topics**, or topics under **broker retention pressure** — the three cases where a windowed `kafka assert` can false-negative (scan-window miss, volume-induced timeout truncation, retention cleanup). It mirrors the `agctl mock` managed-daemon pattern (DESIGN §3.6).

**Listener / offset model (contrast with `kafka assert`):** where `assert`/`consume` seek each partition to `now - --lookback` and read forward against a wall-clock deadline, `kafka listen`:

- **Seeks to the latest offset at `start`** — every assigned partition is positioned at its head (`OFFSET_END`) BEFORE the first poll delivers data, so only messages produced AFTER `start` are captured. No scan-window miss, no backlog replay.
- **Captures to disk** — every matching message is appended to a per-topic NDJSON file under `<state-dir>/listen-<run_id>/`. A byte-bound overflow valve (`--max-bytes-per-topic`, default 256 MiB; `0` disables) emits `capture.overflow` once and STOPs capturing that topic rather than silently truncating.
- **Asserts with no deadline** — `kafka listen results` scans the capture file client-side, bounded by file size only. No timeout-truncation false negative.

**Lifecycle:** `start` → `assert` (attach, repeatable across topics) → `results` (evaluate all, exit 1 on any failure) → `stop` (terminate + cleanup). `stop` does **not** auto-run `results` — an uncollected expectation is silently dropped when the run dir is deleted. Always run `results` BEFORE `stop`.

##### `agctl kafka listen start` — managed daemon (start)

Spawn a detached capture daemon; block until ready (subscribed + seeked-to-end); return the start envelope.

```
agctl kafka listen start
    (--topic <name> | --pattern <name>)+   # repeatable; --pattern reuses kafka.patterns (topic/match/cluster)
    [--cluster <name>]                      # overrides the pattern's bound cluster
    [--capture-match <jq>]                  # coarse capture filter (volume guardrail; default: capture all)
    [--max-bytes-per-topic <bytes>]         # overflow valve; default 256 MiB; 0 = unlimited
    [--state-dir <path>]                    # default ./.agctl
    [--config/--env-file/--overlay ...]
```

**Result (`kafka.listen.start`):** `{pid, run_id, state_dir, topics, group, cluster, started_at}`. POSIX/WSL only — on native Windows use `kafka listen run` (foreground) or run inside WSL. Exits 2 if a listener is already running for the resolved key, the broker is unreachable at startup, or no/ambiguous cluster resolves.

##### `agctl kafka listen assert` — attach an expectation

Attach an expectation to the listener's `asserts.jsonl`. Does **not** evaluate and does **not** stop the listener. Callable repeatedly across topics; the same predicate modes and roots as `kafka assert` (`--contains` / `--match` / `--pattern` / `--path`), all active modes AND-ed.

```
agctl kafka listen assert
    --topic <name>
    [--contains '{...}'] [--match <jq>] [--pattern <name>]   # ≥1 mode required; all active modes AND together
    [--expect-count <n>]            # minimum matching count (at-least; default 1)
    [--path <jq>]                   # narrows --contains (as in kafka assert)
    [--param key=value]             # fills {placeholder} in --pattern match (resolved at results time)
    [--id <name>]                   # stable id; default exp-<n>
    [--run-id <id> | --pid <pid>]
    [--state-dir <path>]
```

**Result (`kafka.listen.assert`):** `{attached: true, id, topic, modes: [...], expect_count}`. Exit 0 / 2.

##### `agctl kafka listen results` — evaluate all

Evaluate every attached expectation against the captured files. **Exits 1 if any expectation fails.**

```
agctl kafka listen results [--run-id <id> | --pid <pid>] [--state-dir <path>]
```

**Result (`kafka.listen.results`) — all pass:**

```json
{
  "evaluated": 2, "passed": 2, "failed": 0,
  "results": [
    {"id": "ord", "topic": "orders.created", "passed": true, "matched_count": 1, "expect_count": 1, "modes": [...], "detail": {}},
    {"id": "pay", "topic": "payments.events", "passed": true, "matched_count": 1, "expect_count": 1, "modes": [...], "detail": {}}
  ]
}
```

**Failure (`AssertionFailure`, exit 1):** `error.detail.results[]` carries, per failed expectation, the same self-debugging payload as `kafka assert` (`messages_scanned`, per-mode `root`) so an agent self-corrects a mis-rooted expression in one shot.

##### `agctl kafka listen messages` — debug tap

Dump captured messages for a topic, optionally further filtered/limited. Reads the topic's capture file directly.

```
agctl kafka listen messages
    --topic <name>
    [--match <jq>] [--param key=value]      # further filter the captured set
    [--limit <n>]                            # default 50
    [--run-id <id> | --pid <pid>] [--state-dir <path>]
```

**Result (`kafka.listen.messages`):** `{topic, matched, truncated, messages: [<envelope>…]}`. Exit 0 / 2.

##### `agctl kafka listen status` — live snapshot (read-only)

Live, read-only snapshot; never signals the daemon, never removes the pidfile.

```
agctl kafka listen status [--run-id <id> | --pid <pid>] [--state-dir <path>]
```

**Result (`kafka.listen.status`):**

```json
{
  "running": true, "pid": 24001, "run_id": "9f3c1a2b", "uptime_ms": 12034,
  "topics": [
    {"topic": "orders.created", "captured": 412, "bytes": 183402, "overflowed": false},
    {"topic": "payments.events", "captured": 7, "bytes": 2104, "overflowed": false}
  ]
}
```

A not-running listener returns `{running: false}` (`ok:true`, exit 0). Stale pidfiles (dead pid) are detected and cleaned up automatically.

##### `agctl kafka listen stop` — managed daemon (stop)

SIGTERM the daemon (SIGKILL after `--timeout`), parse `events.log` for the final summary, **delete the run state dir** (capture files + asserts + meta + pidfile).

```
agctl kafka listen stop [--run-id <id> | --pid <pid>] [--all] [--timeout <seconds>] [--state-dir <path>]
```

**Result (`kafka.listen.stop`):** `{stopped: true, pid, signal, summary: {…}, cleaned: true, failures: [...]}`. Fatal capture errors found in the log (`kafka.error`, or `capture.overflow` on a topic with an attached expectation) raise `AssertionFailure` (exit 1). With `--all`, one verdict per listener; any fatal → exit 1. `--all` returns `result.stopped` as an **array** (one entry per listener); on any fatal failure the array moves to `error.detail.stopped` and the command exits 1. `capture.overflow` on a non-asserted topic is a warning (visible in `status`, non-fatal at `stop`).

##### `agctl kafka listen run` — foreground (daemon spawn target; streaming)

The capture engine in the foreground, emitting NDJSON — the daemon's spawn target (used by `start`) and the cross-platform / native-Windows fallback. Parallels `mock run`.

```
agctl kafka listen run
    (--topic <name> | --pattern <name>)+
    [--cluster <name>] [--capture-match <jq>] [--max-bytes-per-topic <bytes>]
    [--duration <seconds>] [--until-stopped]    # mutually exclusive (until-stopped is the default)
    [--state-dir <path>] [--run-id <id>]
    [--config/--env-file/--overlay ...]
```

**NDJSON stream (the sixth streaming exception after `http ping` / `mock run` / `logs tail` / `grpc` server-stream/bidi):** `started`, per-topic `capture.overflow`, `kafka.error`, `summary` (the per-message capture is written to disk, not stdout). Installs `SIGTERM`/`SIGINT` handlers that flush a final `summary` and exit `0` (clean) / `1` (any `kafka.error`). Startup errors emit one structured envelope *before* any event line. Not wrapped by `@envelope`.

**Selector resolution (start/stop/status/assert/results/messages):** `--run-id <id>` / `--pid <pid>` / implicit-singleton (no flag works when exactly one listener is running in `--state-dir`). With multiple listeners running and no selector, the command raises `ConfigError` (exit 2) listing the candidates. `--all` (stop only) iterates every running listener.

**Cluster resolution (start/run):** same precedence as `kafka produce/consume/assert` — `--cluster` > a `--pattern`'s bound `cluster` > `kafka.default_cluster` > single-cluster auto-default — with the same `ConfigError` (exit 2) on unresolvable/absent name.

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

### 3.4 `agctl logs` — Log Query, Assert, and Tail

The `agctl logs` command group provides three modes for interrogating service logs: query (scan and filter), assert (poll for a match), and tail (stream live entries). All commands operate on a **canonical log entry model** regardless of the underlying log format, enabling uniform queries across different log sources.

#### Canonical entry model

All backends normalize to this structure. Optional fields default to `null`:

| Field | Type | Required | Description |
|---|---|---|---|
| `timestamp` | string (ISO 8601) | Yes | Event timestamp (e.g. `"2026-07-08T12:34:56.789Z"`) |
| `level` | string (UPPERCASE) | Yes | Log level (`INFO`, `WARN`, `ERROR`, etc.) |
| `logger` | string | Yes | Logger name (e.g. `"com.example.OrderService"`) |
| `message` | string | Yes | Log message |
| `thread` | string | No | Thread name |
| `service` | string | No | Service name (from config source or inferred) |
| `stack_trace` | string | No | Exception stack trace (when present) |
| `tags` | array[string] | No | Log tags |
| `fields` | object | No | Custom/MDC fields (backend-specific extras) |

Flags operate on the canonical fields; custom values live under `.fields.*`.

#### `agctl logs query`

Scan logs within a time window, applying level/logger/message/jq filters, returning a paginated result set.

```
agctl logs query
    --source <name>              # required; log source name from logs.sources
    [--level <INFO|WARN|ERROR>]  # case-insensitive level match
    [--logger <glob>]             # logger name glob pattern (e.g. "com.example.*")
    [--message <substring>]       # message substring match
    [--match <jq-expr>]          # jq predicate over canonical entry; supports {placeholder}
    [--param key=value]           # repeatable; fills placeholders in --match
    [--since <spec>]              # start time: ISO-8601 or duration (30s, 5m, 1h)
    [--until <spec>]              # end time: ISO-8601 or duration (defaults to now)
    [--limit <n>]                 # max entries to return (default: logs.defaults.limit)
    [--timeout <seconds>]         # operation timeout
    [--config <path>]
```

**Result shape (`logs.query`):**

```json
{
  "ok": true,
  "command": "logs.query",
  "result": {
    "source": "order-service",
    "matched": 3,
    "scanned": 200,
    "truncated": false,
    "entries": [
      {
        "timestamp": "2026-07-08T12:34:56.789Z",
        "level": "ERROR",
        "logger": "com.example.OrderService",
        "message": "Order processing failed",
        "thread": "http-nio-8081-exec-1",
        "service": "order-service",
        "stack_trace": "java.lang.IllegalArgumentException: ...",
        "fields": { "orderId": "ord-789", "customerId": "cust-42" }
      }
    ]
  },
  "duration_ms": 45
}
```

**Time window parsing:** `--since` and `--until` accept either ISO-8601 strings (`"2026-07-08T12:00:00Z"`) or relative durations (`30s`, `5m`, `1h`). Duration forms compute `now(UTC) - duration`.

**`--match` predicate:** Evaluated against the full canonical entry (all top-level fields plus `.fields.*`). True on **any** truthy output (`.fields.orderId == "ord-789"` means "≥1 entry qualifies," not "all"). Use `{placeholder}` syntax for runtime values filled via `--param`. A malformed expression raises `ConfigError` (exit 2); missing `jq` library surfaces as `ConfigError` pointing at `pip install 'agctl[logs]'`.

#### `agctl logs assert`

Poll for a matching log entry and raise `AssertionError` (exit 1) if no match appears within the timeout. Supports `--not` for negative assertions (fail if a match **is** found).

```
agctl logs assert
    --source <name>              # required
    [--level <INFO|WARN|ERROR>]
    [--logger <glob>]
    [--message <substring>]
    [--match <jq-expr>]
    [--param key=value]
    --since <spec>               # required; start of search window
    [--not]                       # invert: fail if a match IS found (exit 1)
    [--timeout <seconds>]        # poll timeout; omit/0 for one-shot mode (single read)
    [--config <path>]
```

**Execution modes:**

- **One-shot mode** (`--timeout` omitted or `0`): Single read attempt. Returns immediately with `matched: true/false` and `scanned` count.
- **Poll mode** (`--timeout N>0`): Loop with deadline, re-reading the log source each iteration. Returns the first matching entry or timeout.

**Result shape — success (`logs.assert`):**

```json
{
  "ok": true,
  "command": "logs.assert",
  "result": {
    "source": "order-service",
    "matched": true,
    "matching_entry": { /* canonical entry */ },
    "entries_scanned": 15,
    "elapsed_ms": 234
  },
  "duration_ms": 234
}
```

**Failure shape — negative assertion (`--not`):**

When `--not` is set and a match IS found, raises `AssertionError` (exit 1):

```json
{
  "ok": false,
  "command": "logs.assert",
  "error": {
    "type": "AssertionError",
    "message": "Matching log entry found",
    "detail": {
      "source": "order-service",
      "not": true,
      "filter": { "level": "ERROR", "logger_glob": null, "message_substring": null, "match_jq": null },
      "since": "2026-07-08T12:34:00Z",
      "entries_scanned": 8,
      "elapsed_ms": 12,
      "matching_entry": { /* the offending entry */ }
    }
  },
  "duration_ms": 12
}
```

**Failure shape — timeout (positive assertion):**

When `--not` is omitted and no match appears within the timeout:

```json
{
  "ok": false,
  "command": "logs.assert",
  "error": {
    "type": "AssertionError",
    "message": "No matching log entry found within 10s",
    "detail": {
      "source": "order-service",
      "not": false,
      "filter": { "level": "ERROR", ... },
      "since": "2026-07-08T12:34:00Z",
      "entries_scanned": 200,
      "elapsed_ms": 10000
    }
  },
  "duration_ms": 10000
}
```

**`--since` requirement:** The flag is mandatory to guard against runaway scans. Explicit bounds make the search window deterministic and prevent accidental full-file reads on large logs.

#### `agctl logs tail`

Stream log entries in real-time until stopped. Emits one NDJSON line per entry (newline-delimited), followed by a final `summary` line. The third streaming exception after `http ping` and `mock run`.

```
agctl logs tail
    --source <name>              # required
    [--level <INFO|WARN|ERROR>]
    [--logger <glob>]
    [--match <jq-expr>]
    [--param key=value]
    [--duration <seconds>]        # stop after N seconds; mutually exclusive with --until-stopped
    [--until-stopped]            # run until SIGTERM/SIGINT (default if --duration omitted)
    [--config <path>]
```

**NDJSON event stream (one canonical entry per line):**

```json
{"timestamp":"2026-07-08T12:34:56.789Z","level":"INFO","logger":"com.example.OrderService","message":"Processing order","thread":"http-nio-8081-exec-1","service":"order-service","fields":{"orderId":"ord-789"}}
{"timestamp":"2026-07-08T12:34:57.123Z","level":"ERROR","logger":"com.example.OrderService","message":"Order processing failed","thread":"http-nio-8081-exec-2","service":"order-service","stack_trace":"java.lang.IllegalArgumentException: ...","fields":{"orderId":"ord-790"}}
```

**Final `summary` line (on SIGTERM/SIGINT or `--duration` expiry):**

```json
{"summary":true,"total_emitted":2,"duration_ms":1500}
```

**Signal handling:** Installs `SIGTERM`/`SIGINT` handlers that flush the summary line and exit `0`. Startup errors emit a structured envelope **before** any entry line (same pattern as `http ping` and `mock run`).

**Examples:**

```bash
# Stream all ERROR entries from order-service logs
agctl logs tail --source order-service --level ERROR

# Stream entries matching a specific order ID, stop after 30 seconds
agctl logs tail --source order-service --match '.fields.orderId == "ord-789"' --duration 30

# Stream in background for a test scenario
agctl logs tail --source payment-service --until-stopped &
TAIL_PID=$!

# ... run test scenario ...

kill $TAIL_PID
wait $TAIL_PID
```

---

### 3.5 `agctl check` — Service Health

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

### 3.6 `agctl mock` — Mock Server (HTTP, Kafka & gRPC)

The `agctl mock` command group provides two ways to run mock servers: foreground streaming (`mock run`) and managed daemon lifecycle (`mock start`/`stop`/`status`). Both share the same engine and configuration; choose the mode that fits your testing workflow.

#### `agctl mock run`

Run an HTTP mock server and/or Kafka reactors and/or a gRPC mock server, streaming NDJSON events to stdout. The mock is SUT-facing: the real application's HTTP client points at the mock's `http listen`, the gRPC client points at the mock's `grpc listen`, and Kafka reactors join the SUT's real broker as consumers. The command blocks until stopped (foreground process, designed for backgrounding via `&` like `http ping`).

```
agctl mock run
    [--config <path>]            # auto-discovered by default
    [--overlay <path>]           # repeatable; overlay config fragments to compose
    [--http-listen <host:port>]  # literal string (NO ${} interpolation); overrides mocks.http.listen
    [--grpc-listen <host:port>]  # literal string (NO ${} interpolation); overrides mocks.grpc.listen
    [--only http|kafka|grpc]     # run a single engine (HTTP-only needs no kafka/grpc extra)
    [--fail-fast]                # exit non-zero on the FIRST runtime error (default: continue + summarize)
    [--duration <seconds>]       # stop after N s; mutually exclusive with --until-stopped
    [--until-stopped]            # default: run until SIGTERM/SIGINT
```

**Lifecycle:**
- `--http-listen` and `--grpc-listen` are **literal** `host:port` strings (CLI args are not `${}`-interpolated, only YAML values).
- `--only` restricts to one engine; `kafka`-extra / per-reactor cluster `brokers` checks are gated on engines actually started, and gRPC requires the `grpc` extra.
- **No `mocks` section** → emits `started` + `summary` with zero counts and exits `0` (idempotent no-op).
- **`--only <engine>` with that engine absent** → `ConfigError` (exit 2).
- **Startup hazard:** when backgrounding, wrap with `nohup`/`setsid` so the mock is not killed by `SIGHUP` when the launching shell exits, and capture the PID.
- **Port-in-use guard:** at startup, refuses to bind if the HTTP or gRPC port is already in use (emits a `ConfigError` envelope with a hint to kill the stale mock). Note: grpcio enables `SO_REUSEPORT`, so two gRPC servers can silently bind the same port — the guard fires only when a non-grpc process holds the port.

**NDJSON event stream (second streaming exception after `http ping`):**

```json
{"event":"started","http":{"listen":"0.0.0.0:18080","stubs":2},"kafka":{"reactors":[{"name":"order-command-handler","topic":"orders.commands","consumer_group":"agctl-mock-order-handler-<runid>"}]},"grpc":{"listen":"0.0.0.0:50051","stubs":2,"services":["echo.EchoService"],"reflection":true,"health":true},"timestamp":"…"}
{"event":"http.hit","stub":"create-order","method":"POST","path":"/api/v1/orders","status":201,"duration_ms":3,"timestamp":"…"}
{"event":"http.unmatched","method":"GET","path":"/api/v1/unknown","status":404,"timestamp":"…"}
{"event":"http.body_parse_skipped","stub":"oauth-token","method":"POST","path":"/oauth/token","reason":"non-JSON body; response has unresolved placeholders","timestamp":"…"}
{"event":"capture.missing","stub":"epk-chatSearch","name":"ucp","from":".body.ucpID","timestamp":"…"}
{"event":"kafka.reacted","reactor":"order-command-handler","topic":"orders.events","key":"ord-789","duration_ms":1,"timestamp":"…"}
{"event":"kafka.skipped","reactor":"order-command-handler","topic":"orders.commands","reason":"non-object message value","count":3,"timestamp":"…"}
{"event":"kafka.error","reactor":"order-command-handler","topic":"orders.commands","offset":1043,"partition":2,"error":"…","fatal":false,"timestamp":"…"}
{"event":"grpc.hit","stub":"echo-unary","service":"echo.EchoService","method":"Unary","call_type":"unary","status":"OK","duration_ms":1,"timestamp":"…"}
{"event":"grpc.unmatched","service":"echo.EchoService","method":"Missing","call_type":"unary","timestamp":"…"}
{"event":"grpc.error","stub":"echo-unary","service":"echo.EchoService","method":"Unary","error":"…","fatal":true,"timestamp":"…"}
{"event":"summary","http_hits":7,"http_unmatched":1,"http_body_parse_skipped":0,"kafka_reactions":3,"kafka_skipped":3,"kafka_errors":0,"grpc_hits":2,"grpc_unmatched":1,"grpc_errors":0,"duration_ms":45213}
```

**Agent failure-stream protocol (load-bearing):**

The mock's failure signals (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`, `grpc.unmatched`, `grpc.error`, `capture.missing`) live **only** on stdout, and the exit-1-at-shutdown escalation arrives only on a clean `SIGTERM`. The background `&`/`kill` pattern loses both by default. Agents must follow this protocol:

1. Redirect the mock's stdout to a log file: `agctl mock run > mock.log 2>&1 &`.
2. **Poll** `mock.log` for the `started` line before running the SUT (do not sleep a fixed delay).
3. Terminate with `SIGTERM` and `wait` — **never `SIGKILL`** (which skips shutdown/summary/exit-code).
4. After the test, **grep the log for `http.unmatched` / `http.body_parse_skipped` / `kafka.skipped` / `kafka.error` / `grpc.unmatched` / `grpc.error` / `capture.missing`** regardless of the test result, and treat any hit as a failure. `capture.missing` is non-fatal (the mock substitutes empty string and continues), but it marks a likely-misconfigured `from` silently producing a plausible-but-wrong response — investigate rather than ignore.

**`--fail-fast` synchronous alternative:**

For foreground runs with `--duration`, `--fail-fast` exits `1` immediately on the first runtime error (first `kafka.error` or fatal reactor failure), avoiding the log-grep step.

**Examples:**

```bash
# Start all configured engines (HTTP + Kafka + gRPC) in background
agctl mock run &
MOCK_PID=$!

# ... run tests ...

# Stop gracefully and check exit code
kill $MOCK_PID
wait $MOCK_PID
EXIT_CODE=$?

# HTTP-only mode (no kafka/grpc extra needed)
agctl mock run --only http --http-listen 127.0.0.1:18080

# gRPC-only mode (needs the grpc extra)
agctl mock run --only grpc --grpc-listen 127.0.0.1:50051

# Fail-fast synchronous run with duration
agctl mock run --duration 30 --fail-fast
```

#### Platform support

The managed daemon (`mock start`/`stop`/`status`) is supported on **Linux, macOS, and WSL**. On **native Windows** it exits `2` with a `ConfigError` whose message points at `mock run` (foreground) or running inside WSL. `mock run` (foreground) and every other command group are cross-platform, including native Windows — the jq-powered features (`--match`, `--jq-path`, mock `match.jq`) included.

**Streaming graceful-stop.** Backgrounded streaming commands (`http ping`, `mock run`, `logs tail`, `grpc` server-stream/bidi) stop gracefully on `Ctrl+C`/`SIGINT` everywhere, including native Windows. The `SIGTERM`-driven graceful-stop pattern — which the managed daemon's shutdown (`mock stop`) relies on — is POSIX/WSL, which is why the daemon is gated to those platforms.

#### `agctl mock start` — managed daemon (start)

Start the mock server as a detached daemon with automatic pidfile management and readiness polling. The daemon runs the same engine as `mock run` but in the background, with its stdout redirected to a log file. The command returns a single JSON envelope once the daemon is ready (the `started` line has appeared in the log).

```
agctl mock start
    [--config <path>]            # auto-discovered by default
    [--overlay <path>]           # repeatable; forwarded to the daemon
    [--http-listen <host:port>]  # literal (NO ${} interpolation); overrides mocks.http.listen
    [--grpc-listen <host:port>]  # literal (NO ${} interpolation); overrides mocks.grpc.listen
    [--only http|kafka|grpc]     # run a single engine
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
    "grpc": {
      "listen": "0.0.0.0:50051",
      "stubs": 2,
      "services": ["echo.EchoService"],
      "reflection": true,
      "health": true
    },
    "started_at": "2026-07-05T09:00:00Z"
  },
  "duration_ms": 312
}
```

The `grpc` block is present only when the gRPC engine is running. `listen`/`stubs` are `null` when HTTP is not running; `reactors` is `[]` when Kafka is not running. The daemon writes its pidfile to `<state-dir>/mock-<port>.pid` (or `mock-kafka.pid` for Kafka-only mocks, or `mock-grpc-<port>.pid` for gRPC-only mocks) and its NDJSON log to the matching `.log` path. If a mock is already running on the resolved port, `start` fails with `ConfigError` (exit 2).

#### `agctl mock stop` — managed daemon (stop)

Stop a running mock daemon by signaling it, waiting for graceful shutdown, parsing the log for the final summary, and returning the verdict. If any fatal failure events are found (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`, `kafka.error`, `grpc.unmatched`, `grpc.error`), `stop` surfaces them and exits 1 (the strict rule). `capture.missing` is included in the failure list but is non-fatal.

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
      "grpc_hits": 2,
      "grpc_unmatched": 0,
      "grpc_errors": 0,
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
        "grpc_hits": 2,
        "grpc_unmatched": 1,
        "grpc_errors": 0,
        "duration_ms": 45213
      },
      "failures": [
        {"event": "http.unmatched", "method": "GET", "path": "/x", "timestamp": "..."},
        {"event": "kafka.error", "reactor": "order-command-handler", "error": "...", "timestamp": "..."},
        {"event": "grpc.unmatched", "service": "echo.EchoService", "method": "Missing", "call_type": "unary", "timestamp": "..."}
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
      "kafka_errors": 0,
      "grpc_hits": 1,
      "grpc_unmatched": 0,
      "grpc_errors": 0
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

### 3.7 `agctl config` — Config Introspection

#### `agctl config validate`

Parse and validate `agctl.yaml`. Reports schema errors, unresolvable required env vars, dangling service/connection references in templates, malformed jq expressions in `mocks` (HTTP stub `match.jq`, Kafka reactor `match`, gRPC stub `match.jq`/capture placement), invalid gRPC `response.status`, structural response-shape violations (exactly-one-of `response.message`/`response.messages`), and major-version mismatches. Exits 2 on any error. gRPC service/method resolution against the descriptor pool is deferred to `mock run`/`mock start` startup (server construction) — `config validate` cannot load the pool offline.

```
agctl config validate
    [--config <path>]
    [--overlay <path>]         # repeatable; overlay config fragments to validate
```

When `--overlay` is used, warnings include one entry per overridden leaf: `{"path": "<dotted-path>", "message": "overridden by overlay <filename>"}`.

#### `agctl config show`

Dump the fully resolved configuration as JSON. All secret-looking values (password, token, key fields) are masked to `"***"`. Intended for debugging config resolution — **not** for agent discovery (use `agctl discover` instead).

```
agctl config show
    [--config <path>]
    [--overlay <path>]         # repeatable; overlay config fragments to compose
    [--unmask]                  # disable masking (use only in secured environments)
```

When `--overlay` is used, emits `{"config": <masked dump>, "overrides": [...]}`; without overlays, returns the back-compat form (direct config dict).

#### `agctl config init`

Write a sample `agctl.yaml` to the filesystem. The sample is a clean baseline that validates with no environment variables (all optional fields have defaults). Refuses to overwrite an existing file unless `--force` is passed. Does not accept `--config` (it bootstraps the config file itself).

```
agctl config init
    [--output <path>]           # default: ./agctl.yaml
    [--force]                   # overwrite if exists
```

#### `agctl config migrate`

Rewrite a v1/v2 config to dialect `"3"` (named `kafka.clusters`). v3 restructured `Config.kafka` from a single flat object into a named map `kafka.clusters.<name>` (mirroring `database.connections`) plus `default_cluster`, and bumped the version. v1 configs additionally need the v2 jq-dialect rewrite (every `match` expression envelope-rooted: HTTP `.body`, Kafka `.value`); a v1 input is carried through BOTH the jq rewrite and the structural lift in one pass, a v2 input through the lift only. Backs up the original to `<path>.bak` and writes the rewritten config back to `<path>`. A config already at `"3"` is a clean no-op (`already_current: true`, `rewrites: []`). Refuses to clobber an existing `<path>.bak` (`ConfigError`, exit 2 — remove or rename it first); the backup is the only safety net for the reformat the rewrite performs.

```
agctl config migrate
    [--config <path>]           # auto-discovered by default
    [--dry-run]                 # preview the rewrite; do not write
```

The structural lift runs for any non-current source (v1 and v2 both may carry a flat `kafka:` block): when `kafka:` holds any of the five flat keys (`brokers`/`ssl`/`timeout_seconds`/`default_consumer_group`/`schema_registry_url`) and no `clusters` key, those keys move into `kafka.clusters.default` and `default_cluster: default` is set. A missing or already-clustered `kafka` contributes no rewrites and never raises.

Additionally, for **v1 sources only** (v2 exprs are already envelope-rooted and would be double-prefixed), the rewrite walks the three `match`-site families and prepends the envelope prefix:

- `mocks.http.stubs.<name>.match.jq` → prefix `.body | ` (e.g. `.amount > 1000` → `.body | .amount > 1000`).
- `mocks.kafka.reactors.<name>.match` → prefix `.value | `.
- `kafka.patterns.<name>.match` → prefix `.value | `.

Then bumps `version` to `"3"`. `capture.*.from` and `match.body` are **not** visited (out of scope). Idempotent — jq-prefix expressions that already start with the prefix are not double-prefixed; the structural lift never double-lifts an already-clustered `kafka`; an already-`"3"` config is returned unchanged.

**`cli_flags_note` (load-bearing caveat):** CLI `--match` flags (and the deprecated `--filter-key` alias) passed to `agctl http` / `agctl kafka` in shell scripts, agent prompts, or runbooks are **not** rewritten by this command (it walks the config file only). The `.body | ` / `.value | ` prefix guidance applies only to **v1** inputs being lifted to v3 — v2/v3 exprs are already envelope-rooted. (`agctl mock run` has no `--match` CLI flag — mock matchers are config-file only, and ARE rewritten on v1 sources.)

**`formatting_note`:** the rewritten file is emitted via `yaml.safe_dump`, which normalizes indentation/quotes and drops comments; the original is preserved verbatim in `<path>.bak`. Review the full diff before committing.

**Result shape (excerpt):**

```json
{
  "ok": true,
  "command": "config.migrate",
  "result": {
    "path": "./agctl.yaml",
    "already_current": false,
    "from_version": "2",
    "to_version": "3",
    "rewritten": [
      {"path": "kafka.clusters.default.brokers", "before": ["host:9092"], "after": ["host:9092"]},
      {"path": "kafka.default_cluster", "before": null, "after": "default"}
    ],
    "cli_flags_note": "CLI --match flags (and the deprecated --filter-key alias) on `agctl http` / `agctl kafka` … the `.body | ` / `.value | ` prefix applies only to v1 inputs; v2/v3 exprs are already envelope-rooted.",
    "formatting_note": "yaml.safe_dump reformats the file and drops comments; the original is preserved in <path>.bak …"
  },
  "duration_ms": 3
}
```

---

### 3.8 `agctl grpc` — gRPC Operations

The `agctl grpc` command group provides gRPC client operations: unary calls, streaming calls, and health checks. All commands support server reflection for descriptor resolution and fall back to configured proto descriptor files.

#### `agctl grpc call`

Make a gRPC call (unary, client-streaming, server-streaming, or bidirectional). Either invoke a template or provide free-form arguments.

```
# Template mode
agctl grpc call <template>
    [--param key=value]           # fill {placeholder} in template message/metadata

# Free-form mode (mutually exclusive with template)
agctl grpc call
    [--target <name>]            # named gRPC target from config
    [--address host:port]        # raw gRPC address (host:port format)
    --service <Service>          # fully-qualified service name (e.g. echo.EchoService)
    --method <Method>            # method name within the service
    [--message 'JSON']           # request message body (JSON)
    [--metadata k=v]             # repeatable metadata headers
    [--timeout <seconds>]

# Assertion flags (all require ≥1 to enter assertion mode)
    [--status <code>]            # expected gRPC status code (name or number)
    [--contains 'JSON']          # JSON needle in response message (subset match)
    [--match <jq>]               # jq predicate against result envelope
    [--jq-path <jq>]             # jq path (used with --equals)
    [--equals <value>]           # expected value for --jq-path
```

**Call types and I/O model:**

| Call type | Input | Output | Example |
|---|---|---|---|
| **unary** | Single `--message` JSON object | Single JSON response (envelope) | RPC-like request/response |
| **client-stream** | NDJSON on stdin (one JSON per line) | Single JSON response (envelope) | Upload streaming |
| **server-stream** | Single `--message` JSON object | NDJSON on stdout (one per line) | Download streaming |
| **bidi** | NDJSON on stdin | NDJSON on stdout | Bidirectional chat |

**Streaming calls (server-streaming, bidi):** Unlike unary/client-streaming which return a single JSON envelope, server-streaming and bidi emit one NDJSON line per response message. The command installs `SIGTERM`/`SIGINT` handlers and emits a final `summary` line with `messages`, `matched`, `status`, and `duration_ms`. Exit code is `0` on clean shutdown, or `1` if `--expect-count` is not met.

**Template mode example:**

```bash
agctl grpc call echo-unary --param msg="hello world"
```

**Free-form example:**

```bash
agctl grpc call --target echo-server --service echo.EchoService --method Unary --message '{"msg":"hello"}'
agctl grpc call --address localhost:50051 --service echo.EchoService --method Unary --message '{"msg":"hello"}'
```

**Client-streaming example (pipe NDJSON on stdin):**

```bash
echo '{"id":1}' '{"id":2}' '{"id":3}' | agctl grpc call --target echo-server --service upload.Uploader --method Upload
```

**Server-streaming example (NDJSON on stdout):**

```bash
agctl grpc call --target echo-server --service stream.Streamer --method Stream --message '{"count":10}'
# Emits one JSON line per message, then final summary line
```

**Bidirectional streaming (NDJSON in, NDJSON out):**

```bash
echo '{"request":"a"}' '{"request":"b"}' | agctl grpc call --target echo-server --service chat.Chat --method ChatStream
# Each stdin request generates one stdout response line
```

**Response assertions:** When any assertion flag (`--status`, `--contains`, `--match`, `--jq-path`, `--equals`) is set, the command evaluates all assertions post-call (AND logic). A failed assertion raises `AssertionError` (exit 1) with `error.detail` containing the full result and failure list. A non-OK gRPC status is **not** an assertion failure by default — status is a result field. Use `--status OK` (or `0`) to assert success.

**Status-as-result semantics:** gRPC status is surfaced in the result envelope under `status` (with `code`, `name`, `message`). Even non-OK statuses (e.g. `NOT_FOUND`, `PERMISSION_DENIED`) return `ok:true` with the status in `result.status` — only assertion flags flip this to `ok:false`. This mirrors HTTP: a 4xx/5xx response is `ok:true` by default, assertions are opt-in.

#### `agctl grpc healthcheck`

Check health of gRPC services via the standard gRPC health protocol (grpc.health.v1.Health).

```
agctl grpc healthcheck
    [--target <name>]            # check specific target
    [--service <name>]           # optional service name (empty = overall health)
    [--all]                      # check all targets (default when neither flag given)
```

Returns `ok:true` with `result.targets` (dict keyed by target name) and `result.all_serving` (bool). Each target entry contains `address`, `status` (`SERVING`/`NOT_SERVING`/`UNKNOWN`), and optional `note`.

**Examples:**

```bash
agctl grpc healthcheck                     # Check all targets
agctl grpc healthcheck --all              # Same
agctl grpc healthcheck --target echo       # Check specific target
agctl grpc healthcheck --target echo --service echo.EchoService  # Check specific service
```

---

### 3.9 `agctl discover` — Lazy Scoped Discovery

The discovery subsystem lets an agent understand what is available in the system **without loading everything into context at once**. It is structured as three progressive levels: a summary, a category listing, and a single-item detail. The agent fetches only what the current task requires.

All `discover` commands accept `--overlay <path>` (repeatable) to list composed entries from overlay fragments.

> **Design principle:** discovery is a map, not a dump. The agent navigates to the detail it needs rather than receiving everything upfront.

#### Level 0 — System summary

Run once at the start of a session to understand system shape. Returns only counts and category names — no templates, no params, no SQL.

```bash
agctl discover
    [--overlay <path>]         # repeatable; overlay config fragments to compose
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
    "log_sources": 2,
    "grpc_targets": 2,
    "grpc_templates": 3,
    "grpc_mock_stubs": 2,
    "hint": "Run 'agctl discover --category <name>' to list items. Categories: services, http-templates, kafka-patterns, db-templates, mock-http-stubs, mock-kafka-reactors, log-sources, grpc-services, grpc-methods, mock-grpc-stubs"
  },
  "duration_ms": 1
}
```

#### Level 1 — Category listing

Returns names and one-line descriptions for all items in a category. No params, no examples, no SQL.

```bash
agctl discover --category <name> [--overlay <path>]
# <name>: services | http-templates | kafka-patterns | db-templates | mock-http-stubs | mock-kafka-reactors | log-sources | grpc-services | grpc-methods | mock-grpc-stubs
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
agctl discover --category <name> --name <item-name> [--overlay <path>]
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
    "cluster": "default",
    "params": ["orderId"],
    "example": "agctl kafka assert --pattern order-created --param orderId=X --timeout 10"
  },
  "duration_ms": 1
}
```

**Example — `agctl discover --category grpc-methods --name echo-unary`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "grpc-methods",
    "name": "echo-unary",
    "description": "Call the Echo unary method",
    "target": "echo-server",
    "service": "echo.EchoService",
    "method": "Unary",
    "params": ["msg"],
    "example": "agctl grpc call echo-unary --param msg=X"
  },
  "duration_ms": 1
}
```

**Example — `agctl discover --category mock-grpc-stubs --name echo-unary`:**

```json
{
  "ok": true,
  "command": "discover.item",
  "result": {
    "category": "mock-grpc-stubs",
    "name": "echo-unary",
    "description": "Mock the Echo unary RPC",
    "service": "echo.EchoService",
    "method": "Unary",
    "params": ["msg"],
    "example": "grpcurl -plaintext localhost:50051 echo.EchoService/Unary",
    "note": "Active only while `agctl mock run` (grpc engine) is running."
  },
  "duration_ms": 1
}
```

The `example` for a mock stub is the **external** command to exercise the mock (`grpcurl` for gRPC, `curl` for HTTP) against the mock's `listen` address — these are MOCK stubs, not call templates, so `agctl grpc call` / `agctl http call` are the wrong hint.

#### `--search` — Cross-category keyword search

When the agent does not know which category to look in, it searches across all categories by name and description. Returns a flat list of matching items at the name+description level only (no detail). The agent follows up with a Level 2 call for the item it wants to use.

```bash
agctl discover --search <term> [--overlay <path>]
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

Every invocation writes exactly one JSON object to stdout (the streaming commands — `http ping`, `mock run`, `logs tail`, `grpc call` server-stream/bidi, `kafka listen run` — emit one JSON object per line plus a final summary; see §3):

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
| `ConfigError` | 2 | Config missing/invalid, an unresolvable **required** env var, or a major-version (config-schema) mismatch — a v1/v2 config under the v3 tool is rejected with a pointer to `agctl config migrate` |
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

#### `logs.query`

```json
{
  "source": "order-service",
  "matched": 3,
  "scanned": 200,
  "truncated": false,
  "entries": [
    {
      "timestamp": "2026-07-08T12:34:56.789Z",
      "level": "ERROR",
      "logger": "com.example.OrderService",
      "message": "Order processing failed",
      "thread": "http-nio-8081-exec-1",
      "service": "order-service",
      "stack_trace": "java.lang.IllegalArgumentException: ...",
      "fields": { "orderId": "ord-789", "customerId": "cust-42" }
    }
  ]
}
```

#### `logs.assert` (success)

```json
{
  "source": "order-service",
  "matched": true,
  "matching_entry": {
    "timestamp": "2026-07-08T12:34:56.789Z",
    "level": "ERROR",
    "logger": "com.example.OrderService",
    "message": "Order processing failed",
    "thread": "http-nio-8081-exec-1",
    "service": "order-service",
    "stack_trace": "java.lang.IllegalArgumentException: ...",
    "fields": { "orderId": "ord-789" }
  },
  "entries_scanned": 15,
  "elapsed_ms": 234
}
```

#### `logs.tail` (NDJSON stream)

The `logs tail` command is the third streaming exception (after `http ping` and `mock run`). Each line is a complete JSON object representing a canonical entry, followed by a final summary line:

```json
{"timestamp":"2026-07-08T12:34:56.789Z","level":"INFO","logger":"com.example.OrderService","message":"Processing order","thread":"http-nio-8081-exec-1","service":"order-service","fields":{"orderId":"ord-789"}}
{"timestamp":"2026-07-08T12:34:57.123Z","level":"ERROR","logger":"com.example.OrderService","message":"Order processing failed","thread":"http-nio-8081-exec-2","service":"order-service","stack_trace":"java.lang.IllegalArgumentException: ...","fields":{"orderId":"ord-790"}}
{"summary":true,"total_emitted":2,"duration_ms":1500}
```

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
  "mock_http_stubs": 2,
  "mock_kafka_reactors": 1,
  "grpc_mock_stubs": 2,
  "hint": "Run 'agctl discover --category <name>' to list items. Categories: services, http-templates, kafka-patterns, db-templates, mock-http-stubs, mock-kafka-reactors, log-sources, grpc-services, grpc-methods, mock-grpc-stubs"
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

Shape varies by category. All items share `name`, `description`, `params[]`, and `example`. HTTP templates add `method`, `service`, `path`; Kafka patterns add `topic`, `cluster` (the resolved cluster name, or `null` when no cluster resolves), and `match`; DB templates add `connection` and `sql` (so an agent can read a query's result columns before writing a `--path` value assertion).

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

#### `grpc.call` (unary / client-stream)

```json
{
  "target": "localhost:50051",
  "service": "echo.EchoService",
  "method": "Unary",
  "call_type": "unary",
  "status": {
    "code": 0,
    "name": "OK",
    "message": ""
  },
  "message": {
    "message": "hello world"
  },
  "initial_metadata": {
    "content-type": "application/grpc+proto"
  },
  "trailers": {}
}
```

#### `grpc.healthcheck`

```json
{
  "targets": {
    "echo-server": {
      "address": "localhost:50051",
      "status": "SERVING"
    },
    "secure-server": {
      "address": "prod.example.com:443",
      "status": "SERVING"
    }
  },
  "all_serving": true
}
```

#### `grpc.call` (server-stream / bidi) — NDJSON stream

Server-streaming and bidirectional calls emit one JSON object per response message (NDJSON), followed by a final `summary` line:

```json
{"event":"message","message":{"id":1,"result":"first"},"trailers":null}
{"event":"message","message":{"id":2,"result":"second"},"trailers":null}
{"event":"message","message":{"id":3,"result":"third"},"trailers":{"grpc-status":"0","grpc-message":"OK"}}
{"summary":true,"messages":3,"matched":3,"status":{"code":0,"name":"OK","message":""},"duration_ms":45}
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
  ],
  "warnings": [
    {
      "path": "templates.create-order.path",
      "message": "overridden by overlay sidecar.yaml"
    }
  ]
}
```

When `--overlay` is used, warnings include one entry per overridden leaf: `{"path": "<dotted-path>", "message": "overridden by overlay <filename>"}`. Cross-file dangling references remain hard errors (exit 2).

#### `config.show`

Returns the fully resolved config as a JSON object with secrets masked. Structure mirrors the YAML schema. When `--overlay` is used, the result shape changes to `{"config": <masked dump>, "overrides": [...]}`; without overlays, the back-compat form (direct config dict) is unchanged.

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
| `started` | Emitted once at startup after HTTP bind and Kafka probe succeed. Includes `http.listen`/`stubs`, `kafka.reactors[]`, and (when the gRPC engine runs) `grpc.{listen,stubs,services,reflection,health}`. Engines not running are emitted as `null`. |
| `http.hit` | Emitted per matching HTTP stub hit. Includes `stub`, `method`, `path`, `status`, `duration_ms`. |
| `http.unmatched` | Emitted per HTTP request that matched no stub (returned 404). Includes `method`, `path`, `status`. |
| `http.body_parse_skipped` | Emitted when a stub matches but the request body doesn't parse as JSON and the response has unresolved placeholders. Includes `stub`, `method`, `path`, `reason`. |
| `kafka.reacted` | Emitted per Kafka message that matched a reactor and produced a reaction. Includes `reactor`, `topic`, `key`, `duration_ms`. |
| `kafka.skipped` | Emitted when messages are consumed but not matched (e.g., non-object value). Includes `reactor`, `topic`, `reason`, `count`. |
| `kafka.error` | Emitted on a reaction produce failure or reactor error. Includes `reactor`, `topic`, `error`, `fatal`. Under `--fail-fast`, the run exits `1` immediately after a fatal error. |
| `grpc.hit` | Emitted per response message sent (one for unary; one per streamed message; one per matched request for client-stream / bidi). Includes `stub`, `service`, `method`, `call_type`, `status`, `duration_ms`. |
| `grpc.unmatched` | Emitted when no stub matches `service/method`, or every predicate fails (returned `UNIMPLEMENTED`). Includes `service`, `method`, `call_type`. **Fatal** — sets the runtime-error flag so the run exits `1` at shutdown. |
| `grpc.error` | Emitted on a handler deserialize/serialize/runtime failure. Includes `stub` (may be `null`), `service`, `method`, `error`, `fatal: true`. **Fatal.** |
| `capture.missing` | Emitted when an explicit `capture.<name>.from` resolves to `null`/missing at runtime (HTTP stub, Kafka reactor, or gRPC stub). Includes `stub` *or* `reactor`, `name`, `from`. Non-fatal: the mock substitutes empty string and continues; investigate as a likely-misconfigured `from`. |
| `summary` | Emitted once at shutdown. Includes `http_hits`, `http_unmatched`, `http_body_parse_skipped`, `kafka_reactions`, `kafka_skipped`, `kafka_errors`, `grpc_hits`, `grpc_unmatched`, `grpc_errors`, `duration_ms`. |

**Agent protocol (load-bearing):** See `agctl mock` §3.5 for the background lifecycle protocol (redirect stdout → log, poll for `started`, SIGTERM+wait, grep log for errors). Without this, "fail loudly" is aspirational — a silent false-positive is possible.

**Startup errors:** Like `http ping`, startup failures emit **one** structured envelope before any event line (with `command: "mock.run"` and `error.type`), then exit `2`.

**Exit codes:**
- `0` — clean shutdown, no runtime errors.
- `1` — runtime errors occurred (`kafka_errors > 0`, fatal reactor failure, any `grpc.unmatched`/`grpc.error` event, or `--fail-fast` triggered).
- `2` — startup error (config, bind, broker probe, missing `kafka` extra, or missing `grpc` extra when the gRPC engine is selected).

#### `kafka.listen.run` streaming output

`kafka listen run` is the sixth streaming exception (after `http ping` / `mock run` / `logs tail` / `grpc` server-stream/bidi). It emits one JSON object per line (NDJSON) as lifecycle events happen, plus a final `summary` line. Each line carries an `event` field; the per-message capture is written to disk (`<run_dir>/<topic>.ndjson`), not stdout.

**Event types (per-line vocabulary):**

| Event | Description |
|---|---|
| `started` | Emitted once at startup after every topic's capture loop has seeked its partitions to `OFFSET_END`. Includes `run_id`, `topics[]`, `group`, `cluster`, `started_at`. |
| `capture.overflow` | Emitted at most once per topic when its capture file reaches `--max-bytes-per-topic`; the topic's capture loop STOPs (no truncation). Includes `topic`, `bytes`. |
| `kafka.error` | Emitted on a per-topic capture-loop death (e.g. broker error). Includes `topic`, `error`, `fatal: true`. The engine exit code flips to 1. |
| `summary` | Emitted once at shutdown. Includes `topics[]` (each `{topic, captured, overflowed}`), `errors`, `duration_ms`. |

**Startup errors:** Like `mock run`, startup failures emit **one** structured envelope before any event line (with `command: "kafka.listen.run"` and `error.type`), then exit `2`.

**Exit codes:**
- `0` — clean shutdown, no `kafka.error` events.
- `1` — at least one `kafka.error` occurred (a per-topic capture loop died).
- `2` — startup error (config, broker probe, missing `kafka` extra, or `--duration` + `--until-stopped` both given).

---

## 5. Configuration Resolution Order

`agctl` resolves its configuration through the following precedence chain (highest to lowest):

1. **`--config <path>` CLI flag** — if provided, only this file is loaded; no discovery walk is performed.
2. **`AGCTL_CONFIG` environment variable** — if set, used as the config file path. Ignored when `--config` is explicitly passed.
3. **Auto-discovery** — search for `agctl.yaml` starting in the current working directory, walking up parent directories until the filesystem root or a `.git` directory is found (whichever is first). The first `agctl.yaml` found wins.
4. **`${ENV_VAR}` interpolation within YAML values** — after the file is located and parsed, all `${VAR}` references in string values are resolved from the process environment, supplemented by `.env` defaults (real env wins; see §2.2 for `.env` source precedence). `AGCTL_*` overrides (step 6) likewise read from this merged env, so an `AGCTL_*` line in `.env` applies unless overridden by the real env.
5. **`--overlay <path>` CLI flags (repeatable)** — after base interpolation, each overlay file is loaded, interpolated, and deep-merged into the base config in flag order (later overlays win on conflict). Overlay version (if present) must match the base config's major version.
6. **`AGCTL_<SECTION>_<KEY>` environment variable overrides** — after overlays are merged, specific values can be overridden by structured env vars (see §8 for the exact convention). These have the highest precedence and win over both base and overlay values.

**If no config file is found and no `--config` or `AGCTL_CONFIG` is set**, the tool exits with code 2 and a `ConfigError` JSON object.

**Env var override convention** (`AGCTL_<SECTION>__<KEY>` — double-underscore `__` separates path segments; a single `_` stays within a key segment):

```
AGCTL_DEFAULTS__TIMEOUT_SECONDS=30
AGCTL_KAFKA__CLUSTERS__DEFAULT__DEFAULT_CONSUMER_GROUP=my-group
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
are listed in `.env.example` — copy it to `.env` and fill in the values. agctl
auto-loads a `.env` next to `agctl.yaml` (real env wins), so no shell sourcing
is needed before running `agctl` commands.

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
- **For endpoints not in config (external APIs, ngrok, ephemeral ports, URLs from logs), use
  `agctl http request --url <full-url>`** rather than shelling out to `curl` — you keep the
  JSON envelope, deterministic exit codes, and response assertions. `--url` is mutually
  exclusive with `--service`/`--path`.
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
version = "1.1.1"
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

`output.emit()` is the **only** permitted stdout write path. Every command calls it exactly once before exiting. Any intermediate logging goes to stderr. The streaming commands (`http ping`, `mock run`, `logs tail`, `grpc call` server-stream/bidi, `kafka listen run`) are the deliberate exceptions — they stream newline-delimited objects (one per event plus a final summary) for background / keepalive / live-capture use; see §3.

This is enforced structurally: each command callback is split into a thin Click command and a `_core` function, and the `_core` is wrapped by an `@envelope(command)` decorator (`command.py`) that guarantees exactly one `emit()` — and one process exit code — on every code path (success, assertion failure, or error).

### stderr is not machine-readable

Stderr is reserved for unexpected internal errors and stack traces. An agent must never parse stderr. If a stack trace reaches stderr, the command exits with code 2 and has already written a structured `InternalError` envelope to stdout.

### Stateless invocations

No session files, no lock files, no local state databases. Each invocation is fully self-contained. Kafka consumer groups are used for offset tracking when needed; that state lives in Kafka, not on disk.

**Bounded exceptions — daemon state.** Two managed-daemon surfaces introduce a deliberate, scoped carve-out to "no session files", each confined to its own daemon lifecycle:

- **Mock daemon** (`mock start`/`stop`/`status`) — a pidfile (`mock-<port>.pid`) and NDJSON log (`mock-<port>.log`) under `<state-dir>/` (default `./.agctl/`).
- **Listen daemon** (`kafka listen start`/`stop`/`status`/`assert`/`results`/`messages`) — a run-id-keyed pidfile (`listen-<run_id>.pid`) plus a run dir (`listen-<run_id>/`) holding `meta.json`, `asserts.jsonl`, per-topic `<topic>.ndjson` capture files, and `events.log`, all under `<state-dir>/`. `stop` deletes the run dir on every path, so an uncollected expectation is silently dropped.

No other commands write disk state.

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
AGCTL_KAFKA__CLUSTERS__DEFAULT__DEFAULT_CONSUMER_GROUP=ci-consumer
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

### 9.2 Log Backend Plugins

`agctl` discovers log backends via the `agctl.logs_backends` entry point group. Each backend must implement the `LogBackend` protocol:

```python
# agctl/clients/log_backend_protocol.py
import threading
from typing import Protocol, Iterator
from datetime import datetime
from dataclasses import dataclass, field

@dataclass
class CanonicalEntry:
    """Canonical log entry representation."""
    timestamp: str
    level: str
    logger: str
    message: str
    thread: str | None = None
    service: str | None = None
    stack_trace: str | None = None
    tags: list[str] | None = None
    fields: dict = field(default_factory=dict)

@dataclass
class LogFilter:
    """Filter criteria for log queries."""
    level: str | None = None
    logger_glob: str | None = None
    message_substring: str | None = None
    match_jq: str | None = None
    params: dict = field(default_factory=dict)

@dataclass
class ScanResult:
    """Result of a scan operation."""
    entries: list[CanonicalEntry]
    matched: int
    scanned: int
    truncated: bool

@dataclass
class AwaitResult:
    """Result of an await_one operation."""
    entry: CanonicalEntry | None
    scanned: int
    elapsed_ms: int

@dataclass
class SchemaDescriptor:
    """Schema descriptor from sample_schema."""
    standard: list[str]
    conditional: list[str]
    observed: list[str]

class LogBackend(Protocol):
    """Structural contract for a log backend."""

    def validate_config(self) -> None: ...

    def scan(self, filt: LogFilter, *, since: datetime | None, until: datetime | None, limit: int, tail_lines: int) -> ScanResult: ...

    def await_one(self, filt: LogFilter, *, since: datetime | None, timeout_s: float, poll_interval_ms: int, tail_lines: int) -> AwaitResult: ...

    def follow(self, filt: LogFilter, *, stop_event: threading.Event, poll_interval_ms: int) -> Iterator[CanonicalEntry]: ...

    def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor: ...
```

The built-in `file` backend (type `"file"`) reads NDJSON files in logstash format. Third-party backends can add support for journald, syslog, ELasticsearch, etc. To add a journald backend:

```toml
# In agctl-logs-journald/pyproject.toml
[project.entry-points."agctl.logs_backends"]
journald = "agctl_logs_journald:JournaldBackend"
```

`agctl` loads all registered backends at startup and selects the correct one based on the `type` field in a `logs.sources` config. A broken third-party backend is skipped (logged to stderr) rather than crashing the CLI.

### 9.3 Protocol Plugins

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

### 9.4 Custom Assertion Types

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
| **Mock: TLS / HTTPS mock** | Cert-pinned SUT clients cannot connect to a plaintext mock. Applies to the HTTP and gRPC mock (`mocks.grpc` is plaintext-only v1). |
| **Mock: multiple HTTP servers / ports** | One server, many stubs (path-routed) is the only model today. |
| **Mock: stateful / server-push gRPC bidi** | The gRPC mock's bidi support is request/response pairing (one rendered response per matched incoming request). Stateful conversation, server-push, and per-message client-stream aggregation are deferred. |
| **Mock: mid-stream abort (gRPC)** | `RSTSTREAM` mid-server-stream is not modeled; a gRPC stub streams its authored `messages` to completion or a terminal `status`. |
| **Mock: reflection-bootstrapped gRPC stubs** | The gRPC mock requires `proto`/`descriptor_set` sources to resolve service/method and encode responses — reflection is *served* but cannot *bootstrap* the mock itself. |
| **Mock: cross-transport gRPC sagas** | gRPC stub → Kafka reaction (or vice versa) linkage is deferred alongside the HTTP↔Kafka cross-transport item above. |

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
| **TLS / TLS-pinned gRPC SUT clients** | `mocks.grpc` is plaintext-only v1 (no TLS on the mock listener). | TLS-pinned SUT clients cannot connect → integration untested → false green. |
| **Stateful / server-push gRPC bidi** | The gRPC mock's bidi is request/response pairing (one rendered response per matched request); conversation state and server-push are not modeled. | State-machine coverage gaps → false green. |
| **Per-message-responds client-stream gRPC** | Client-stream aggregates `messages` at stream close and emits one rendered response; per-message responding is not modeled. | Incremental-response code paths go unexercised → false green. |
| **gRPC mock from reflection alone** | `mocks.grpc` requires `proto`/`descriptor_set` sources to resolve service/method and encode responses; server reflection is *served* but cannot *bootstrap* the mock. | A config that expects reflection-only bootstrapping fails at startup (exit 2) — fail-loud, not silent. |
| **gRPC port collision via `SO_REUSEPORT`** | grpcio enables `SO_REUSEPORT`; two gRPC servers can bind the same port silently. The port-in-use guard fires only when a non-grpc process holds the port. | The SUT may reach a stale mock → silently stale responses → false green. Pick unique ports across runs. |

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
| Send an ad-hoc request | `agctl http request --service ... --path ...` (or `--url <full-url>` when no service is configured) |
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
