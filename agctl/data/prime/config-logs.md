# agctl — config-logs: configure log sources

A `logs.sources.<name>` block answers "where do I find logs for this
service?" — the entry point for `logs query` / `logs assert` / `logs tail`.

## Extraction

1. **path** — the log file path (absolute or project-root-relative).
   Point at the **current** file when the service rolls logs.
2. **format** — default `logstash` (NDJSON). Custom format → ask for a
   sample line and whether a custom backend plugin exists.
3. **service** *(opt)* — grouping for `discover log-sources`.
4. **type** — default `file`; built-in `loki` (remote, needs `url` +
   `query` + the `loki` extra); others (`syslog`, `journald`) need a plugin
   via the `agctl.logs_backends` entry point.
5. **name** — kebab-case from the service/file (`order-service.log` →
   `order-service`).

```yaml
logs:
  sources:
    order-service: { path: "logs/order-service.log", format: logstash }
    payment-logs:
      type: loki
      url: "${LOKI_URL:-http://loki:3100}"
      query: '{app="payment"}'          # LogQL selector; required
      options: { org_id: "${LOKI_ORG_ID:-}", fetch_limit: 1000 }
  defaults: { tail_lines: 200, limit: 50, timeout_seconds: 10, poll_interval_ms: 100 }
```

`logs.defaults` sets fallbacks for `--limit`/`--timeout`/
`--poll-interval`/`--tail-lines`. Backend-specific knobs (auth, TLS, fetch)
live under `options` — unknown **top-level** keys are rejected, so never
next to `type`/`url`.

## Canonical entry model

Entries are normalized before filtering/asserting: `timestamp` (ISO-8601),
`level` (uppercase), `message`, `logger`, `thread`, `service` *(opt)*,
`stack_trace` *(opt)*, `tags` *(opt)*, `fields` (object — custom/MDC fields
under `.fields.orderId`). Write `--match` predicates and `discover` queries
against these.

## Stack snippets

- **Spring**: `logback-spring.xml` `<file>`/`<fileNamePattern>` → `path`;
  `log4j2.xml` `<File>`/`<RollingFile fileName>` → `path`.
- **Python**: `logging.FileHandler` → `path`; structlog JSON → `format:
  logstash`.
- **Node**: `winston.transports.File` → `path`.

## Gotchas

- Reads **only the current file** — no rolled-over history; need
  yesterday's entries → point `path` at the archived file.
- File reads are **windowed to the last `tail_lines` lines** (default 200;
  `logs query --limit N` grows the window to N) — older entries are
  invisible. `result.truncated: true` = more matches than `--limit` **or** a
  capped read (file window / loki `fetch_limit`); widen the knob and re-query.
- **Missing file = empty source** (exit 0, zero entries) — config for
  not-yet-started services is valid.
- `logs tail` streams NDJSON (a streaming command) — stop with `--duration
  N` or `--until-stopped`.
- `logs assert`: **one-shot** (no `--timeout`) scans once, exit 1 on no
  match; **poll** (`--timeout N>0`) scans until match/timeout. `--not`
  inverts ("no error logs in the last 5 minutes").
- `--match` is jq over the canonical entry (`{placeholder}` via `--param`);
  needs `agctl[logs]`. `type: loki` needs `agctl[loki]` (httpx) and polls
  `query_range` — follow latency is bounded by the poll interval.

## Clarify (genuine gaps only)

The exact file path; whether the format is `logstash` (else a sample line);
the service name for grouping.
