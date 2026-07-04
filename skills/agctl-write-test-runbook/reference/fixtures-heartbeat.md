# Heartbeat fixture — `agctl http ping`

> Some systems expire the session unless an endpoint is hit periodically. A
> heartbeat keeps the session alive across a long multi-step run. Source:
> DESIGN §3.1, AGENTS.md pattern 5.

Like the mock, `http ping` is a **background fixture**, not a Step. It streams
one JSON object per ping (plus a final summary on `SIGTERM`/`SIGINT`), not a
single result envelope.

## Start

```bash
agctl http ping heartbeat --interval 5 --until-stopped &
HEARTBEAT_PID=$!
```

`--interval` is seconds between pings; `--until-stopped` runs until killed.
Inject the auth header when the heartbeat endpoint requires it:

```bash
agctl http ping heartbeat --header "Authorization=Bearer $TOKEN" --interval 5 --until-stopped &
```

## Stop

```bash
kill $HEARTBEAT_PID        # SIGTERM → emits the final summary line
```

`kill` (SIGTERM) in Teardown so the summary line lands. Do not `SIGKILL`.

## When to use

Only when the SUT enforces a session timeout that a long run (30 s+) would
trip. Otherwise it is unnecessary overhead.
