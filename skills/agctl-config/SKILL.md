---
name: agctl-config
description: Author/maintain agctl.yaml — add an HTTP template, Kafka pattern, or DB template from source code (controller/producer/SQL), or bootstrap (init) a whole config from the repo. Invoke when asked to add or edit agctl.yaml (templates/patterns/connections) or generate a starter config.
---

# Authoring `agctl.yaml`

`agctl` (alias `agt`) tests a running system over HTTP / Kafka / DB. All of that is
driven by one file, **`agctl.yaml`**. This skill writes that file: you point at the
config **and a source artifact** — a REST controller, a Kafka producer, a SQL query —
and it produces the right block, in the right section, in valid form, then verifies.
It asks only when something is genuinely ambiguous.

This is the **authoring** counterpart to the `agctl` skill, which covers *running*
commands. Don't conflate them: `agctl` drives the CLI; `agctl-config` writes its config.

## Pick a mode (infer from the artifact; confirm if unclear)

| Mode | Artifact you point at | Section it writes |
|---|---|---|
| `http` | route / controller / OpenAPI doc | `templates:` (+ a `services:` entry if the service is new) |
| `kafka` | producer / emitter / event class | `kafka.patterns:` |
| `db` | SQL query / repo method | `database.templates:` (+ a `database.connections:` entry if new) |
| `db` (write) | INSERT/UPDATE/DELETE / mutating repo method | `database.templates:` with `mode: write` (requires `writable: true` connection) |
| `init` | the whole repo | a full `agctl.yaml` + `.env.example` |

Then read the matching `reference/<mode>.md` in this skill's directory for extraction
steps and stack snippets. For write templates, see `reference/db-write-template.md`.

## Step 0 — locate the config (never guess)

Mirror agctl's own discovery, highest precedence first:

1. An explicit path the user gave.
2. The `AGCTL_CONFIG` env var.
3. Walk up from the working dir to the first `agctl.yaml` (stop at `.git` or the filesystem root).

If none is found, **ask** — don't invent one. This skill edits the **consuming repo's
`agctl.yaml`** — never the packaged `agctl/data/sample-config.yaml` (a drift-guard test
pins it byte-identical to the README).

## The contract — every block you write obeys this

**1. Three placeholder syntaxes — never mix them.** This is the #1 source of broken config.

| Syntax | Meaning | Where it's legal | Resolved |
|---|---|---|---|
| `${VAR}` | env var | any string **value** (URLs, passwords, tokens) | config **load** time |
| `{name}` | call-time param | HTTP `path` / `body`, Kafka `match` | call, via `--param name=…` |
| `:name` | SQL bind param | `database.templates.*.sql` | execute, via `--param name=…` |

`${VAR}` forms: `${VAR}` required (missing → exit 2); `${VAR:-default}` default if unset;
`${VAR:-}` empty if unset. Never put `${}` in keys; never use `{name}` call-time params in SQL,
and never use `:name` SQL bind syntax in an HTTP path/body (a literal `:` inside a value is fine);
`::` casts like `::jsonb` are safe in SQL.

**2. Cross-references must resolve** (else `config validate` exits 2). Before you finish:

- every `templates.<t>.service` must be a key under `services:`
- every `database.templates.<t>.connection` (if set) must be a key under `database.connections:`; if omitted, `defaults.database_connection` must resolve
- `defaults.database_connection` (if set) must be a real connection

If a new template needs a service/connection that doesn't exist, **add it** (and flag it
to the user) — never leave a dangling reference.

**3. Keys are kebab-case**, derived from the route/topic/query — not the source identifier
(`OrderCreationController` → `create-order`, not `order_creation_controller`). This is for *keys*
(template / pattern / connection names). Call-time params are **snake_case**, matching the source
field (`{customer_id}`, `:orderId`) — never kebab-case inside a `{}` or `:`.

**4. `description` is effectively required.** Always write a one-liner. Missing it is only a
*warning*, but `agctl discover` output degrades without it.

**5. Idempotent updates.** If the key already exists, diff field-by-field, show the user what
changes, and ask before overwriting. Never duplicate a key or silently clobber.

**6. Secrets → env.** Header values, DB passwords, tokens → `${ENV_VAR}` (or `${ENV:-}` if
optional), and add the var to `.env.example`. Never inline a real secret.

**7. Clarify, don't guess.** Ask only about genuine gaps (which connection? replace or append?
what's the service's base URL?). Each question carries a recommended default.

## Mandatory close-out — verify before you stop

Every edit ends with these two commands (the config must stay valid):

```bash
agctl config validate                                                       # ok:true, exit 0 (warnings fine; errors are not)
agctl discover --category <http-templates|kafka-patterns|db-templates> --name <new-key>   # must list expected params
```

If `agctl` isn't installed, run the **structural checklist** below instead and tell the user
live validation was skipped. **Never** declare done on config that doesn't validate.

### Structural checklist (fallback when agctl is absent)

- [ ] YAML parses.
- [ ] `version` is present, major part `"1"`.
- [ ] Every `templates.*.service` ∈ `services`.
- [ ] Every `database.templates.*.connection` (if set) ∈ `database.connections`.
- [ ] `defaults.database_connection` (if set) ∈ `database.connections`.
- [ ] `kafka.ssl.security_protocol` (if set) ∈ {PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT}.
- [ ] Every `templates` / `database.templates` / `kafka.patterns` entry has a non-empty `description`.

## Worked example (HTTP, Spring)

Point at `OrdersController` with `@PostMapping("/api/v1/orders")`,
`@RequestBody {customerId, items[{sku,qty}]}`, `@RequestHeader Authorization`. The skill
writes. If `order-service` isn't already under `services:`, add it first — the template's
`service` must resolve (contract #2):

```yaml
services:
  order-service:
    base_url: "${ORDER_SERVICE_URL:-http://localhost:8081}"   # ${VAR:-default} keeps the file valid before .env is set
    health_path: "/actuator/health"

templates:
  create-order:
    description: "Submit a new order for a customer"
    method: POST
    service: order-service                                    # resolves to the services: entry above
    path: "/api/v1/orders"
    headers:
      Content-Type: "application/json"
      Authorization: "Bearer ${ORDER_SERVICE_TOKEN}"          # secret → bare ${ENV}; add to .env.example; set before validate
    body:
      customer_id: "{customer_id}"                            # {name} param → --param customer_id=…  (params are snake_case)
      items:
        - sku: "{sku}"
          quantity: 1
```

then runs `agctl config validate` and `agctl discover --category http-templates --name create-order`
(expect `params: ["customer_id", "sku"]`).

---

For the dense per-mode extraction rules, OpenAPI/Spring/FastAPI/Node snippets, and "what to
clarify," read the matching file in `reference/`. To **run** the templates you create, see the
`agctl` skill.
