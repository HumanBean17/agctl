# agctl — config: authoring agctl.yaml

You point at the config **and a source artifact** — a REST controller, a
Kafka producer, a SQL query — and produce the right block, in the right
section, in valid form, then verify. Ask only about genuine ambiguities.

**Infra vs. fixtures boundary:** shared infrastructure (services,
connections, core patterns) belongs in the main `agctl.yaml`;
runbook-specific fixtures live in sidecars (`<runbook>.agctl.yaml`) layered
via `--overlay`.

| Mode | Artifact | Section |
|---|---|---|
| `http` | route / controller / OpenAPI | `templates:` (+ `services:` entry if new) |
| `kafka` | producer / emitter / event class | `kafka.patterns:` |
| `db` | SQL query / repo method | `database.templates:` (+ connection if new) |
| `db` (write) | INSERT/UPDATE/DELETE | `database.templates:` with `mode: write` |
| `mock` | downstream contract to impersonate | `mocks:` (`http.stubs` / `kafka.reactors` / `grpc.stubs`) |
| `logs` | log file / Loki / logback config | `logs.sources:` |
| `init` | the whole repo | full `agctl.yaml` + `.env.example` |

Per-mode extraction detail: `--topic config-http`, `config-kafka`,
`config-db`, `config-db-write`, `config-mocks`, `config-logs`, `config-init`.

## Locate the config (never guess)

Precedence: explicit path from the user → `AGCTL_CONFIG` env → walk up from
cwd to the first `agctl.yaml` (stop at `.git`/root). None found → **ask**.

## The contract — every block obeys

1. **Three placeholder syntaxes — never mix** (the #1 breakage source):

   | Syntax | Meaning | Where | Resolved |
   |---|---|---|---|
   | `${VAR}` | env var | any string value (URLs, secrets) | config load |
   | `{name}` | call-time param | HTTP `path`/`body`, Kafka `match` | call, `--param` |
   | `:name` | SQL bind | `database.templates.*.sql` | execute, `--param` |

   `${VAR}` required (missing → exit 2); `${VAR:-default}` default;
   `${VAR:-}` empty-allowed. Never `${}` in keys; never `:name` in an HTTP
   path/body; `::` casts (`::jsonb`) are safe in SQL. **Mocks are the
   exception** — `{name}` there means capture-from-trigger (see
   `--topic config-mocks`).
2. **Cross-references must resolve** (else `config validate` exits 2): every
   `templates.<t>.service` ∈ `services:`; every
   `database.templates.<t>.connection` ∈ `database.connections:` (or omitted
   with `defaults.database_connection` resolving). Need a missing
   service/connection → **add it** and flag it; never leave a dangling ref.
3. **Keys are kebab-case** from the route/topic/query
   (`OrderCreationController` → `create-order`); call-time params are
   **snake_case** matching the source field.
4. **`description` is effectively required** — one-liner; `discover` degrades
   without it.
5. **Idempotent updates** — key exists → diff field-by-field, show changes,
   ask before overwriting. Never duplicate or silently clobber.
6. **Secrets → env** — `${ENV}` + add to `.env.example`; never inline.
   A `.env` next to the config auto-loads (real env wins).
7. **Clarify, don't guess** — only genuine gaps, each with a recommended
   default.

## Mandatory close-out

```bash
agctl config validate                                    # ok:true, exit 0
agctl discover --category <http-templates|kafka-patterns|db-templates|mock-http-stubs|...> --name <new-key>
```

Sidecar edits verify with `config validate --config <base> --overlay
<sidecar>` and `discover --overlay <sidecar> --name <key>`. Mock edits:
confirm in `discover`, then smoke `agctl mock run --duration 5` (no
`http.unmatched`/`kafka.error`/`grpc.unmatched`/`grpc.error`). If agctl
isn't installed, run the structural checklist instead and say live
validation was skipped — **never declare done on config that doesn't
validate**.

## Structural checklist (fallback when agctl is absent)

- YAML parses; `version` major = `"3"` (named `kafka.clusters`).
- All cross-refs resolve (contract #2); every entry has a non-empty
  `description`.
- `kafka.default_cluster` / `patterns.<name>.cluster` / `topics.<t>.cluster`
  ∈ `kafka.clusters`; `ssl.security_protocol` ∈ {PLAINTEXT, SSL, SASL_SSL,
  SASL_PLAINTEXT}; Avro/Protobuf topics (`value_format` override or cluster
  default) need a `schema_registry_url` on that cluster.
- Reactor resolved clusters need non-empty `brokers`.
- `mocks.http.listen` / `mocks.grpc.listen` parse as `host:port` (IPv6
  bracketed, e.g. `[::1]:18080`).
- gRPC stubs: `response` sets exactly one of `message` (unary/client-stream/
  bidi) or `messages` (server-stream, required); `status` is a valid gRPC
  name (case-sensitive) or 0–16.
- `reaction.headers` / `response.metadata` values are strings.
