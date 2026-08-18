# agctl v{version} — agent manual

This output matches the installed binary. `agctl <cmd> --help` is the
authoritative flag spec (flags, types, required args); this manual keeps only
what `--help` won't tell you — semantics, roots, traps.

`agctl` (alias `agt`) tests a **running** system over HTTP, Kafka, DB, gRPC,
and logs. Every invocation prints **one JSON object** on stdout and exits
deterministically; stderr is diagnostics — parse **stdout only**.

```json
{"ok": true, "command": "http.call", "result": {}, "error": null, "duration_ms": 87}
```

**Exit codes:** `0` success · `1` an **assertion failed** — the system under
test is wrong · `2` tool/config/env error — *your command* is wrong. Read `ok`
first; on `false` read `error.type`.

## Intent → command

| Intent | Command |
|---|---|
| What can I do here? | `discover` (a map, not a dump) |
| Send a known / ad-hoc request | `http call <tpl> [--param…]` / `http request (--service S --path P \| --url …)` |
| Assert an HTTP response | `… --status N [--contains '{…}'] [--match '<jq>'] [--jq-path .x --equals v]` |
| Verify an event was published | `kafka assert [--topic T] <mode> --timeout N` |
| See what was published | `kafka consume --topic T [--match …]` |
| Publish a message | `kafka produce --topic T --message '{…}'` |
| Capture a long saga / busy topic | `kafka listen start` → `assert` → `results` → `stop` |
| DB write / rows / value / schema | `db execute --write` / `db assert --expect-rows N` / `db assert --expect-value --path .x --equals v` / `db schema` |
| Call gRPC | `grpc call <tpl> [--param…]` / `grpc call --target T \| --address host:port` |
| Impersonate a dependency | `mock run` (foreground) / `mock start\|stop\|status` (daemon — preferred) |
| Query / assert logs | `logs query` / `logs assert` / `logs tail` |
| Ready? / validate / migrate config | `check ready --all` / `config validate` / `config migrate` |

Global flags: `--config`, `--overlay` (repeatable), `--env-file`; a `.env`
next to the resolved `agctl.yaml` auto-loads (real env wins). **`--timeout` is
not global**; `kafka assert --timeout` is **required**. Kafka `<mode>` =
`--contains '{…}' | --match '<jq>' | --pattern <name>`.

## Top-5 gotchas (full list: `--topic gotchas`)

1. **A 4xx/5xx response is `ok:true` — unless you assert.** Add
   `--status`/`--contains`/`--match`/`--jq-path` to flip a wrong response into
   an `AssertionError` (exit 1).
2. **`--match` is envelope-rooted:** HTTP `.body.x` (response envelope), Kafka
   `.value.x` (message envelope) — not payload-rooted. `--match`/`--jq-path`
   need `pip install 'agctl[jq]'`.
3. **Kafka reads are windowed** (`now - --lookback`), not "latest": an event
   published just before you started is still matched; narrow busy topics with
   `--match`/`--contains`.
4. **`ConnectionError` is exit 2** — service/broker/DB unreachable. Run
   `agctl check ready --all` before blaming the assertion.
5. **Streaming commands** (`http ping`, `mock run`, `logs tail`, `grpc`
   server-stream/bidi, `kafka listen run`) emit one object **per line** + a
   final `summary`. Background with `&`, `kill` when done.

## Depth: `agctl prime --topic <name>`

One topic per domain — fetch only what the task needs:

- `gotchas` — the full 16-gotcha list, db-schema authoring rules, recipes
- `mock` — impersonate a dependency: engines, daemon vs foreground, failure rules
- `listen` — long-lived Kafka capture: protocol, byte valve, fatality rules
- `grpc` — gRPC calls: call types, NDJSON model, config, discovery
- `config` — authoring agctl.yaml: contract, placeholder table, close-out, checklist
- `config-http` / `config-kafka` / `config-db` / `config-db-write` / `config-mocks` / `config-logs` — per-mode extraction from source artifacts
- `config-init` — bootstrap a whole config by scanning the repo
