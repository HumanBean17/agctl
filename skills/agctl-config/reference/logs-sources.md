# `logs` mode — configure log sources

Point at a service log file or a remote log backend (e.g. Loki); produce a `logs.sources.<name>` block. The
non-negotiable contract (canonical entry model, placeholder syntax, one-shot+poll semantics)
lives in `SKILL.md` — this file is the extraction detail.

## Inputs

- The config path.
- The artifact: a log file path (for `type: file`), a Loki URL + LogQL query (for `type: loki`), a logback/log4j XML config, or a description of where logs are written.

## Extraction

1. **path** — the log file path. Use an absolute path or a path relative to the project root.
2. **format** — the log format. Default: `logstash` (NDJSON with `@timestamp`, `level`, `message`, `logger`, `thread`, `stack_trace`, `tags`, `fields`). If the app uses a custom format, ask for a sample line and whether a custom backend exists.
3. **service** — optional; the service name for `discover log-sources` grouping. Omit if the log isn't tied to a single service or if the source name already makes this clear.
4. **type** — default: `file`. Also built-in: `loki` (remote Loki HTTP endpoint; needs `url` + `query`, and the `loki` extra). Other types (e.g. `syslog`, `journald`) need a custom backend plugin via `agctl.logs_backends`.
5. **name** — kebab-case from the service or log file (`order-service.log` → `order-service`).

A log source answers "where do I find logs for this service?" — the entry point for `logs query`, `logs assert`, and `logs tail`.

## Stack snippets

### Spring (JVM)

- `logback-spring.xml` `<file>` or `<fileNamePattern>` → `path` (use the literal file path; if it uses rolling, point at the current file).
- `log4j2.xml` `<File>` or `<RollingFile fileName>` → `path`.

### Python

- `logging.FileHandler` → `path`.
- `structlog` JSON output → `format: logstash` (NDJSON).

### Node

- `winston.transports.File` → `path`.

## Canonical entry model

All log entries are normalized to a **canonical shape** before filtering/asserting. Built-in backends (`file`, `loki`) produce entries with these fields:

- `timestamp` — ISO-8601 datetime string.
- `level` — uppercase log level (`DEBUG`, `INFO`, `WARN`, `ERROR`).
- `message` — the raw log message.
- `logger` — the logger name (often the class/module).
- `thread` — the thread name (for services that use threads).
- `service` — optional; the service name (from `logs.sources.<name>.service` or inferred).
- `stack_trace` — optional; the exception stack trace if present.
- `tags` — optional; array of string tags.
- `fields` — object; custom/MDC fields live under `.fields.*` (e.g., `.fields.orderId`, `.fields.userId`).

When you write `--match` predicates or `discover` schema queries, reference these fields. Custom fields (e.g., MDC in Java, extra dict keys in Python structlog) are nested under `.fields.*`.

## Writing a log source

### Minimal file source (logstash format)

```yaml
logs:
  sources:
    order-service:
      path: "logs/order-service.log"
      format: logstash
      service: order-service
```

### Multiple sources with defaults

```yaml
logs:
  sources:
    order-service:
      path: "logs/order-service.log"
      format: logstash
      service: order-service
    payment-service:
      path: "logs/payment-service.log"
      format: logstash
      service: payment-service
  defaults:
    tail_lines: 200
    limit: 50
    timeout_seconds: 10
    poll_interval_ms: 100
```

The `logs.defaults` block sets fallback values for command flags (`--limit`, `--timeout`, `--poll-interval`, `--tail-lines`). Omit it if the core defaults are fine.

### Remote Loki source

```yaml
logs:
  sources:
    order-service:
      type: loki
      url: "${LOKI_URL:-http://loki:3100}"
      query: '{app="order-service"}'        # LogQL log selector; required
      service: order-service
      options:                               # backend-specific extras
        org_id: "${LOKI_ORG_ID:-}"           # multi-tenant scope (X-Scope-OrgID)
        fetch_limit: 1000                    # max lines per query_range (default 500)
```

`type: loki` reads a remote Loki HTTP endpoint (`{url}/loki/api/v1/query_range`). `url` and `query` are required; `service` overrides the canonical entry's service (otherwise taken from the log line). Backend-specific knobs (auth, TLS, fetch tuning) live under `options` — the model rejects unknown **top-level** keys (`extra="forbid"`), so never put them next to `type`/`url`. Requires `pip install 'agctl[loki]'`.

## What to clarify

- The **exact file path** (absolute or relative to project root). If the service uses rolling files (e.g., `order-service.log.2024-01-01`), point at the current file — agctl reads only the live file, not historical rolls.
- Whether the format is `logstash` (NDJSON). If not, ask for a sample line and whether a custom backend plugin exists.
- The service name (optional but helpful for `discover log-sources` grouping).

## Where it writes

Under `logs.sources:` (nested under `logs:`). The file also needs `logs.defaults:` if you want to override core defaults.

## Gotchas

- `logs query`/`assert` read **only the current file** — they don't follow rolled-over history. If the service rotates logs nightly and you need yesterday's entries, point `path` at the archived file.
- **Missing file = empty source**. If the file doesn't exist, commands return zero entries (exit 0). This is by design — it allows config for services that may not have started yet.
- `logs tail` streams NDJSON to stdout (like `http ping` and `mock run`). Stop it with `--duration N` or `--until-stopped`.
- `logs assert` has two modes:
  - **one-shot** (no `--timeout` or `--timeout 0`): scan once, exit 1 if no matches.
  - **poll** (`--timeout N>0`): scan repeatedly until a match or timeout (default poll interval is `logs.defaults.poll_interval_ms`).
- Use `--not` with `logs assert` to invert the condition ("no error logs in the last 5 minutes").
- `--match` uses jq over the canonical entry (fill `{placeholder}` via `--param`). If jq is missing, install the extra: `pip install 'agctl[logs]'`.
- `type: loki` needs `pip install 'agctl[loki]'` (httpx); a missing extra is a `ConfigError` (exit 2). `logs tail`/`assert` poll Loki over HTTP (query_range) — it is not a websocket `/tail`, so follow latency is bounded by the poll interval. Auth/TLS/fetch knobs go under `options` (unknown top-level keys are rejected).
- Built-in types are `file` (default) and `loki`. Other types require a plugin installed via the `agctl.logs_backends` entry point — analogous to `agctl.db_drivers`.
