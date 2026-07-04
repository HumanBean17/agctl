# Mock fixture — `agctl mock run`

> The mock server is a **background, SUT-facing** process: the real app's HTTP
> client points at the mock's listen address, and Kafka reactors join the SUT's
> real broker. It streams NDJSON events to stdout. Source: DESIGN §3.5.

A mock is a **fixture**, not a Step. It emits NDJSON (one event per line), not a
single result envelope, so it has no per-step exit code or `ok`. Start it in
**Setup**, verify startup, and stop it in **Teardown**.

## Start

```bash
agctl mock run > mock.log 2>&1 &
MOCK_PID=$!
```

Redirect stdout to a log file and capture the PID.

## Startup gate

Poll mock.log for the `started` line before running any Step. The mock emits a
`started` event once it has bound HTTP and probed the broker:

```bash
grep -Eq '"event":\s*"started"' mock.log
```

Do not sleep a fixed delay — poll mock.log until the line appears. If it does
not appear within ~30 s, fail the run: the mock failed to start (inspect
`mock.log`).

## Stop

```bash
kill $MOCK_PID        # SIGTERM
wait $MOCK_PID
```

**Never `SIGKILL`** (`kill -9`): it skips the shutdown handler, the final
`summary` line, and the exit code. `SIGTERM` lets the mock emit its summary and
exit `0` (clean) or `1` (runtime errors occurred).

## Failure detection (load-bearing)

The mock's failure signals live **only** on stdout (in `mock.log`), and the
exit-1 escalation arrives only on a clean `SIGTERM`. After the run — regardless
of the Steps' verdict — grep the log for these events and treat any hit as a
failure that flips the run verdict to FAIL:

- `http.unmatched` — an HTTP request matched no stub (returned 404).
- `http.body_parse_skipped` — a stub matched but the request body didn't parse and the response had unresolved placeholders.
- `kafka.skipped` — messages consumed but not matched (e.g. non-object value).
- `kafka.error` — a reaction produce failure or reactor error.

```bash
grep -E 'http\.unmatched|http\.body_parse_skipped|kafka\.skipped|kafka\.error' mock.log
```

A non-zero match count → FAIL.

## `--fail-fast` (synchronous alternative)

For foreground `--duration` runs, `--fail-fast` exits `1` immediately on the
first `kafka.error` or fatal reactor failure, avoiding the log-grep step. Not
the default; the background-and-grep protocol above is the load-bearing path.
