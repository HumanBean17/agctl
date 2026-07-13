# agctl

> Agent-facing CLI harness for testing distributed systems.

`agctl` (alias: `agt`) is a small, system-agnostic command-line tool that an AI
coding agent drives to verify a running system. It talks **HTTP**, **Kafka**, and
**databases**, and gives the agent one consistent contract for all of them: every
invocation prints exactly **one JSON object** on stdout and exits with a
deterministic code (`0` success, `1` assertion failed, `2` config/tool/env error).

```
$ agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001
{"ok": true, "command": "http.call", "result": {"status_code": 201, ...}, "error": null, "duration_ms": 87}
```

## Why it exists

Agents need **deterministic, machine-readable feedback** to know whether a change
worked. Raw `curl` output, prose logs, and non-zero exit codes from shell glue are
noisy and ambiguous — an agent can't reliably tell "the feature is broken" from
"my command had a typo."

`agctl` closes that gap. It is a **harness** built specifically to be driven by an
agent (humans and CI can use it too):

- **One object, one code.** A single JSON envelope + a strict exit code, on every
  command, across every protocol. Parse `ok`/`result`/`error` and move on.
- **Composable, narrow commands.** The agent chains them instead of relying on a
  monolithic "run scenario" command: send a request → assert a Kafka event →
  assert a DB row.
- **System-agnostic.** The tool ships with zero knowledge of your project. All
  endpoints, topics, connections, SQL, and request templates live in an
  `agctl.yaml` you commit to your repo.
- **Fail loudly.** A wrong assertion always exits `1`. There is no silent
  false-positive — the worst possible failure mode for an agent harness.
- **Discovery, not dumps.** A three-level `discover` command lets the agent learn
  what your system offers *without* loading your entire config into its context.

Design intent and the full spec live in [`docs/DESIGN.md`](./docs/DESIGN.md); the
as-built architecture (module layout, runtime flow, extension points) is the
source of truth in [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

---

## Setup

**Requirements:** Python ≥ 3.11. Runs on Linux, macOS, native Windows, and WSL.
The managed mock daemon (`mock start`/`stop`/`status`) is Linux/macOS/WSL-only —
on native Windows use `mock run` (foreground) or run inside WSL.

Install `agctl` into your project (this repo uses `uv` — `pip` works too):

```bash
# with uv (recommended — this repo ships a uv.lock)
uv pip install -e .

# or with pip
pip install -e .
```

The core install pulls only `click`, `pyyaml`, and `pydantic`. Protocol libraries
are **optional extras** — install only what your system needs. Heavy libs are
lazy-imported, so a missing extra fails with a clear `ConfigError` (exit 2) rather
than a crash:

```bash
pip install -e ".[http]"            # http call / request / ping / check ready
pip install -e ".[jq]"             # jq flags: http --match/--jq-path, mock match.jq
pip install -e ".[kafka]"           # kafka produce / consume / assert
pip install -e ".[db]"              # db query / assert
pip install -e ".[grpc]"            # grpc call / healthcheck (includes grpcio, protobuf, jq)
pip install -e ".[logs]"            # logs query / assert / tail (includes jq)
pip install -e ".[http,kafka,db]"   # everything except logs/grpc (typical — bundles jq)
pip install -e ".[http,kafka,db,grpc,logs]"   # everything
```

Verify the install — both binary names work:

```bash
agctl --help
agt --help
```

Scaffold a config file you can edit. `agctl config init` writes a sample
`agctl.yaml` at your repo root with concrete localhost values — replace them with
your own services, topics, and connections. (It refuses to overwrite an existing
file; pass `--force` to replace one.) Confirm it loads and validates:

```bash
agctl config init        # writes ./agctl.yaml (edit the values it contains)
agctl config validate
```

Then orient yourself — this is the first command an agent runs in a session:

```bash
agctl discover
```

---

## CLI abilities

`--config <path>` is a global flag on every command (otherwise `agctl.yaml` is
auto-discovered from the current directory upward).

| Group | Command | What it does |
|---|---|---|
| **`http`** | `call <template>` | Execute a named HTTP template from config |
| | `request` | Free-form request (escape hatch; `--service --method --path`) |
| | `ping <template>` | Repeat a request on an interval — stream NDJSON (session keepalive) |
| **`kafka`** | `produce` | Publish one message (`--topic --message`) |
| | `consume` | Read a topic; return up to `--expect-count` matches within `--timeout` |
| | `assert` | Fail (exit 1) unless a matching message arrives within `--timeout`. Modes: `--contains`, `--match <jq>`, `--pattern <name>` (combinable) |
| **`db`** | `query` | Run `--template` or free-form `--sql`; return all rows |
| | `assert` | Assert `--expect-rows N`, or `--expect-value --path <jq> --equals <v>` on the first row |
| **`grpc`** | `call <template>` | Execute a named gRPC template (unary, client-stream, server-stream, bidi) |
| | `healthcheck` | gRPC health check via grpc.health.v1.Health |
| **`logs`** | `query` | Scan log sources; filter by `--since`, `--match` (jq), `--level` |
| | `assert` | Assert logs match/nmatch a condition within `--timeout` (one-shot or poll) |
| | `tail` | Stream log entries as NDJSON (with `--duration` or `--until-stopped`) |
| **`check`** | `ready` | Hit `health_path` for one (`--service`) or all services; 2xx = ready |
| **`config`** | `validate` | Validate schema, env vars, cross-references, version |
| | `show` | Dump fully-resolved config as JSON (secrets masked) |
| | `init` | Write a sample `agctl.yaml` to edit (refuses to clobber; `--force`) |
| **`discover`** | *(top-level)* | Three levels: summary → `--category` → `--name`; plus `--search` |

**Composing commands** — the core pattern is *send, then assert*:

```bash
# 1. Trigger an action
agctl http call create-order --param customer_id=cust-42 --param sku=WIDGET-001

# 2. Assert the downstream Kafka event arrived (reliable by default — see notes)
agctl kafka assert --topic orders.created --contains '{"customer_id": "cust-42"}' --timeout 10

# 3. Assert the DB reflects the new state
agctl db assert --template find-order --param orderId=ord-789 --expect-value \
  --path ".status" --equals "PENDING"
```

**Exit codes** (the contract the agent relies on):

| Code | Meaning |
|---|---|
| `0` | Success; all assertions passed |
| `1` | An assertion was evaluated and **failed** — the system is not in the expected state |
| `2` | Tool/config/env error — not an assertion result; fix the invocation or environment |

> **Send-then-assert is reliable by default.** `kafka consume`/`assert` seek each
> partition to `now - --lookback` (default `= --timeout`) and read forward — they
> do **not** subscribe at "latest" — so an event published a moment before the
> command starts still falls inside the window.

See [`docs/DESIGN.md` §3](./docs/DESIGN.md) for the complete flag reference and
[`docs/DESIGN.md` §11](./docs/DESIGN.md) for end-to-end agentic workflow examples.

---

## Configuration

`agctl` loads one `agctl.yaml` per invocation. Resolution order (highest first):

1. `--config <path>` — if given, *only* this file is loaded.
2. `AGCTL_CONFIG` — env var pointing at the config file.
3. **Walk-up discovery** — searches from the current directory upward for
   `agctl.yaml`, stopping at the first `.git` or the filesystem root.
4. **`${ENV_VAR}` interpolation** in YAML string values (after parsing).
5. **`AGCTL_<SECTION>__<KEY>` overrides** — highest precedence; applied last.

If no file is found, it exits `2` with a `ConfigError`.

### Env-var interpolation (in any string value)

| Syntax | Behavior |
|---|---|
| `${VAR}` | **Required.** Missing → `ConfigError` (exit 2), never a silent empty string. |
| `${VAR:-default}` | Optional with a literal default. |
| `${VAR:-}` | Optional; missing → empty string. |

Three substitution syntaxes exist — don't conflate them:

- `${VAR}` — environment, resolved at config load.
- `{name}` — HTTP path/body & Kafka-pattern placeholders, filled at call time from `--param key=value`.
- `:name` — JDBC-style SQL params (templates and free-form `--sql`), bound at execute time.

### Env-var overrides

`AGCTL_<SECTION>__<KEY>=value` (double underscore separates path segments;
uppercase each segment; hyphens → `_` within a segment). Applied **after**
interpolation, with highest precedence:

```bash
AGCTL_DEFAULTS__TIMEOUT_SECONDS=30
AGCTL_KAFKA__CLUSTERS__DEFAULT__DEFAULT_CONSUMER_GROUP=ci-consumer
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__HOST=localhost
AGCTL_DATABASE__CONNECTIONS__MAIN_DB__PASSWORD=supersecret
AGCTL_SERVICES__ORDER_SERVICE__BASE_URL=http://order-svc:8080
```

### Complete, copy-paste-ready config

`agctl config init` writes exactly this file — shown here for reference and for
browsing on GitHub without installing. It has **concrete localhost values and no
required env vars**, so `agctl config validate` passes as-is. (The production
version is the same file with secrets/hosts moved into `${...}` and loaded from a
`.env` — see the note after it.)

```yaml
# agctl.yaml
# Version tracks the agctl MAJOR version only (currently "3").
version: "3"

# --- services: named HTTP base URLs for services under test -----------------
services:
  order-service:
    base_url: "http://localhost:8081"
    health_path: "/actuator/health"   # used by `agctl check ready`
    timeout_seconds: 10               # optional; overrides defaults.timeout_seconds

  payment-service:
    base_url: "http://localhost:8082"
    health_path: "/health"
    timeout_seconds: 15

# --- kafka: named clusters + patterns --------------------------------------
# Mirrors database.connections: a named map of clusters, a default_cluster,
# and a global patterns map. A single cluster needs no default_cluster
# (auto-defaulted), but it is set here for clarity.
kafka:
  clusters:
    default:
      brokers:
        - "localhost:9092"
      default_consumer_group: "agctl-consumer"
      schema_registry_url: ""           # optional; omit/leave empty if unused
      timeout_seconds: 30               # default consume/assert timeout

      # Optional TLS/mTLS — uncomment for brokers that require SSL. Setting ANY
      # field to a non-empty value enables TLS (security.protocol defaults to "SSL").
      # ca_location is optional: unset → librdkafka uses the system trust store
      # (fine for publicly-trusted brokers; pin a CA for private-PKI brokers).
      # Hostname verification stays ON unless endpoint_identification_algorithm: "none".
      # ssl:
      #   ca_location: ""
      #   certificate_location: ""          # path to client cert (mTLS)
      #   key_location: ""                  # path to client private key (mTLS)
      #   key_password: ""                  # optional private-key password
      #   # endpoint_identification_algorithm: "none"   # disable hostname verification
      #   # security_protocol: "SSL"                     # default; set SASL_SSL when adding SASL

  default_cluster: default

  # patterns: named Kafka filters, analogous to HTTP templates.
  #   topic: Kafka topic
  #   match: jq boolean predicate over each message envelope;
  #          supports {placeholder} substitution via --param at assert time
  #   cluster: optional named cluster this pattern binds to (default = default_cluster)
  patterns:
    order-created:
      description: "An ORDER_CREATED event for a specific order"
      topic: orders.created
      match: '.value.eventType == "ORDER_CREATED" and .value.payload.orderId == "{orderId}"'

    payment-failed:
      description: "Any PAYMENT_FAILED event regardless of order"
      topic: payments.events
      match: '.value.eventType == "PAYMENT_FAILED"'

# --- database: named connection profiles and SQL templates ------------------
database:
  connections:
    main-db:
      type: postgresql                 # extensible via plugins (entry point agctl.db_drivers)
      host: "localhost"
      port: 5432
      dbname: "app"
      user: "app"
      password: "app"
      default: true                    # used when --connection is omitted

    analytics-db:
      type: postgresql
      host: "localhost"
      port: 5432
      dbname: "analytics"
      user: "analytics"
      password: "analytics"

  # templates: named SQL queries. `connection` is optional (falls back to
  # defaults.database_connection). Use :paramName named params (JDBC-style).
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

# --- templates: named HTTP request templates --------------------------------
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
      Authorization: "Bearer ${PAYMENT_SERVICE_TOKEN:-change-me}"   # optional env var (has a default)
    body:
      order_id: "{order_id}"
      amount_cents: "{amount_cents}"

  get-payment-status:
    description: "Fetch payment status by order ID"
    method: GET
    service: payment-service
    path: "/api/v1/payments/{order_id}/status"

# --- defaults: project-wide fallbacks --------------------------------------
defaults:
  timeout_seconds: 10
  database_connection: main-db

# --- grpc: gRPC service targets and templates -------------------------------
grpc:
  # targets: named gRPC server addresses
  targets:
    order-service:
      address: "localhost:50051"
      use_tls: false               # optional; defaults to false (plaintext)
      reflection: auto             # auto (default) | on | off
      # tls:                       # optional TLS settings (when use_tls: true)
      #   override_authority: ""  # optional; override the TLS server name

  # descriptors: fallback proto descriptor sets when reflection is unavailable.
  # Each entry sets exactly one of `descriptor_set` (a compiled FileDescriptorSet
  # .pb file) or `proto` (a .proto glob compiled at load via protoc; pair with
  # `include_paths`).
  descriptors:
    - descriptor_set: "protos/order-service.desc"
    # - proto: "protos/orders/v1/*.proto"
    #   include_paths: ["protos"]

  # templates: named gRPC request templates
  templates:
    create-order:
      description: "Create a new order via gRPC"
      target: order-service
      service: "orders.OrderService"
      method: "CreateOrder"
      message:
        customer_id: "{customer_id}"
        items:
          - sku: "{sku}"
            quantity: 1

# --- logs: log file sources for tailing and searching ------------------------
logs:
  sources:
    order-service:
      path: "logs/order-service.log"
      format: logstash
      service: order-service
  defaults:
    tail_lines: 200
    limit: 50
    timeout_seconds: 10
    poll_interval_ms: 100
```

> **Note:** `charge-payment` uses the `${PAYMENT_SERVICE_TOKEN:-change-me}` form —
> an *optional* env var with a literal default — so `config validate` passes even
> with nothing exported. For production, `export PAYMENT_SERVICE_TOKEN=<real token>`
> (or move the whole value into `${...}` loaded from a `.env`); see below.

**Moving to environment-driven config** — replace the concrete values above with
`${...}` and put them in a `.env`. agctl auto-loads the `.env` next to the resolved
`agctl.yaml` at config load time (real env wins, so CI/prod can override committed
defaults); point at a different location with `--env-file <path>` or `AGCTL_ENV_FILE`:

```bash
# .env  — never commit real secrets
ORDER_SERVICE_URL=http://order-svc:8081
PAYMENT_SERVICE_URL=http://payment-svc:8082
KAFKA_BROKER=kafka:9092
DB_HOST=postgres
DB_NAME=app
DB_USER=app
DB_PASSWORD=change-me
PAYMENT_SERVICE_TOKEN=change-me
```

```yaml
# agctl.yaml (snippet) — same file, env-interpolated
services:
  order-service:
    base_url: "${ORDER_SERVICE_URL}"
database:
  connections:
    main-db:
      host: "${DB_HOST}"
      dbname: "${DB_NAME}"
      password: "${DB_PASSWORD}"
      port: "${DB_PORT:-5432}"     # optional-with-default form
```

Validate before committing, and use `config show` to inspect the resolved result
(secrets are masked; pass `--unmask` only in trusted environments):

```bash
agctl config validate
agctl config show
```
