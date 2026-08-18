# agctl — config-init: bootstrap a config from the repo

Scan the consuming repo and draft a populated `agctl.yaml` + `.env.example`
from what's actually there. Per-section extraction detail lives in the
sibling topics (`config-http`, `config-kafka`, `config-db`, `config-mocks`,
`config-logs`); this is the scan plan. Guard: refuse to overwrite an
existing `agctl.yaml` (mirror `agctl config init`); fallback =
`agctl config init` (packaged sample) then fill in.

## Scan order (each source → one section)

1. **Services** → `services:` — `docker-compose*.yml` (names + mapped
   ports), `application*.yml`/`.properties`, `.env`/`.env.example`, k8s
   manifests. Derive `base_url` (`http://localhost:8081`); `health_path` =
   `/actuator/health` if Spring/Actuator, else ask. **Reuse existing
   env-var names** — grep the repo before inventing `${ORDER_SERVICE_URL}`.
2. **HTTP templates** → `templates:` — run the `config-http` extraction over
   controllers / OpenAPI. Monorepo: one `services:` key per distinct
   `base_url`; controllers sharing a port fold into one service.
3. **Kafka** → `kafka:` — brokers from compose/props →
   `kafka.clusters.<name>`; set `default_cluster` (or omit with exactly one
   cluster — auto-default). Producers/topics → `kafka.patterns:`
   (`config-kafka`); bind `cluster` only for non-default events.
4. **Database** → `database:` — `spring.datasource`, `DATABASE_URL`, a
   compose `postgres` service → `database.connections:` (mark one
   `default: true`); queries/repos → `database.templates:`.
5. **defaults** → `defaults:` — `timeout_seconds` (10 if unknown);
   `database_connection` = the `default: true` connection.

## Env-var strategy (valid out of the box)

The packaged sample validates with **no env vars set** — match that.
Prefer `${VAR:-default}` so the file validates before anything is set:

```yaml
base_url: "${ORDER_SERVICE_URL:-http://localhost:8081}"   # valid as-is
```

Bare required `${VAR}` only for secrets / no safe default. Collect every
`${VAR}` into `.env.example`: required → `VAR=` (placeholder + comment);
optional → `VAR=default` commented as optional. Never real secrets. Have
the user `cp .env.example .env` and fill it.

## Close-out

- `version: "3"` at the top.
- agctl auto-loads a `.env` next to the resolved `agctl.yaml` (real env
  wins) — required vars resolve at `config validate` with no shell
  sourcing; `--env-file`/`AGCTL_ENV_FILE` point elsewhere.
- Then the mandatory verify: `agctl config validate` (`ok:true`) followed
  by `agctl discover` (summary → category → sample item).
- A bare-required `${VAR}` keeps validate at exit 2 until `.env` provides
  it — expected, not a failure. Never declare done while it exits 2.

## Clarify (real gaps only)

The default DB connection; `health_path` when not Spring; env-var names for
secrets the repo doesn't define; whether per-cluster
`kafka.clusters.<n>.ssl` is needed (only for TLS brokers).
