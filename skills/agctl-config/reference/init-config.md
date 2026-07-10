# `init` mode — bootstrap a config from the repo

Scan the consuming repo and draft a populated `agctl.yaml` + `.env.example` from what's actually
there. The per-section extraction detail lives in the sibling reference files; this file is the
scan plan.

## Guard

- Refuse to overwrite an existing `agctl.yaml` (mirror `agctl config init`); ask before a
  `--force`-style replacement.
- You can always fall back to `agctl config init` (which dumps the packaged sample) and then fill
  it in — but scanning first is the point of this mode.

## Scan order (each source → one section)

1. **Services + base URLs** → `services:`
   - `docker-compose*.yml` (service names + mapped ports), `application*.yml` / `.properties`,
     `.env` / `.env.example`, k8s manifests.
   - Derive `base_url` (e.g. `http://localhost:8081`). `health_path` = `/actuator/health` if
     Spring (Actuator present), else ask.
   - **Reuse existing env-var names.** Before inventing `${ORDER_SERVICE_URL}`, grep the repo's
     `.env` / compose / `application*.yml` for names already in use (`ORDER_SERVICE_BASE_URL`,
     etc.) and reuse them — avoids a confusing double-set of vars.
2. **HTTP templates** → `templates:` — run the `http` extraction (see `http-template.md`) over
   controllers / OpenAPI. In a monorepo, emit one `services:` key per distinct `base_url`;
   controllers sharing a port / base URL fold into one service.
3. **Kafka** → `kafka:` — brokers from compose / props → `kafka.clusters.<name>` (a named
   map mirroring `database.connections`); set `default_cluster` to one of the cluster names
   (or omit it when only one cluster is defined — it auto-defaults). Producers / topics →
   `kafka.patterns:` (see `kafka-pattern.md`); a pattern may bind a `cluster` if its event
   lives on a non-default cluster.
4. **Database** → `database:` — datasource config (Spring `spring.datasource`, `DATABASE_URL`, a
   compose `postgres` service) → `database.connections:` (mark one `default: true`); queries /
   repos → `database.templates:` (see `db-template.md`).
5. **defaults** → `defaults:` — `timeout_seconds` (10 if unknown); `database_connection` = the
   `default: true` connection.

## Env-var strategy (keep the file valid out of the box)

The packaged sample validates with **no** env vars set — match that. For values that vary by
environment, prefer `${VAR:-default}` so the file still validates before the user sets anything:

```yaml
base_url: "${ORDER_SERVICE_URL:-http://localhost:8081}"   # validates as-is; overridable in CI
```

Use a bare required `${VAR}` only for secrets / values with no safe default (passwords, tokens,
prod URLs). Then every such var lands in `.env.example` (below) so the user knows to set it.

## `.env.example`

Collect every `${VAR}` you wrote:

- Required `${VAR}` → `VAR=` (with a placeholder / comment).
- Optional `${VAR:-default}` / `${VAR:-}` → `VAR=default` (or blank), commented as optional.

Never put real secret values in it.

## What to clarify (real gaps only)

- Which DB connection is the default.
- `health_path` when it's not Spring / Actuator.
- Env-var names for secrets if the repo doesn't already define them.
- Whether to include the optional per-cluster `kafka.clusters.<name>.ssl` block (only if brokers need TLS).

## Close-out

- Put `version: "3"` at the top.
- Write `.env.example` (above), then have the user copy & fill it: `cp .env.example .env`
  (edit the secrets).
- Source it before validating, so required `${VAR}`s resolve: `set -a; . ./.env; set +a`.
- **Then** run the mandatory verify from `SKILL.md`: `agctl config validate` (expect `ok:true`)
  followed by `agctl discover` (summary → a category → a sample item).
- A bare-required `${VAR}` makes `config validate` exit 2 *until `.env` is sourced* — that's
  expected, not a failure. Never declare done while it still exits 2.
