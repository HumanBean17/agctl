# agctl — mock: impersonate a dependency

`agctl mock` stands in for the SUT's **external** deps — an HTTP API the SUT
calls, a downstream Kafka consumer expected to react to its events, or a gRPC
service the SUT's client targets. **SUT-facing:** the app's HTTP client points
at the mock's `http listen`; the gRPC client at the `grpc listen`; Kafka
reactors join the SUT's *real* broker as consumers (the mock is not a broker).
Stubs/reactors are authored in the `mocks:` config section (see
`--topic config-mocks`).

`--only http|kafka|grpc` restricts to one engine; `--http-listen` /
`--grpc-listen` are literal `host:port` overrides (CLI args are NOT
`${}`-interpolated).

**Two modes:** foreground streaming (`mock run`) and managed daemon
(`mock start`/`stop`/`status`). **Prefer the daemon mode** — it collapses the
background protocol into `mock start` → `mock stop` and surfaces failures
cleanly (`mock start` blocks until the `started` line; `mock stop` applies the
strict failure rule: any `http.unmatched`, `http.body_parse_skipped`,
`kafka.skipped`, `kafka.error`, `grpc.unmatched`, `grpc.error` ⇒ exit 1).

**Windows:** the managed daemon is unavailable on native Windows (exit 2,
`ConfigError` pointing at `mock run`/WSL) — use foreground `mock run` there.

## `mock run` background lifecycle (the false-green trap)

Failure signals (`http.unmatched`, `http.body_parse_skipped`, `kafka.skipped`,
`kafka.error`, `grpc.unmatched`, `grpc.error`, `capture.missing`) live **only
on stdout**, and the exit-1 escalation arrives only on a clean `SIGTERM`. The
plain `&`/`kill` pattern loses both and silently produces a **false green**:

```bash
nohup agctl mock run > mock.log 2>&1 &
MOCK_PID=$!
until grep -q '"event":"started"' mock.log; do sleep 0.1; done   # poll, don't sleep fixed
# … run the SUT / assertions, pointing the SUT at the mock's listen addresses …
kill -TERM "$MOCK_PID"; wait "$MOCK_PID"                          # SIGTERM + wait, never SIGKILL
grep -E 'http.unmatched|http.body_parse_skipped|kafka.skipped|kafka.error|grpc.unmatched|grpc.error|capture.missing' mock.log && exit 1
```

Rules: redirect stdout to a log (capture the PID); poll the `started` line
before running the SUT; stop with `SIGTERM` + `wait` — **never `SIGKILL`**
(skips the shutdown handler, `summary`, and exit code); grep the log for
failure events **regardless of the test result** — any hit is a failure even
if assertions passed. `capture.missing` is non-fatal at runtime (mock
substitutes empty string) but marks a `capture.from` that resolved to nothing
— usually a misconfigured path silently yielding a plausible-but-wrong field.
`--fail-fast` is the synchronous alternative for `--duration` runs.

Exit rule for `mock run` itself: exit 1 only when `kafka_errors > 0` or a
`grpc.unmatched`/`grpc.error` event fires (stricter at `mock stop` — see
above). Daemon state (pidfile + NDJSON log keyed by engine) lives under
`<state-dir>/` (default `./.agctl/`); clean up with `rm -rf .agctl`.

**`mock stop` selectors:** `--listen <X>` (matches any of the daemon's listen
addresses — HTTP, Kafka implicit, or gRPC), `--pid <pid>`, or no arg when
exactly one mock runs. `--all` iterates every running mock in `--state-dir`
and returns the verdict array; multiple running + no selector ⇒ exit 2.
