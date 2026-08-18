# agctl — kafka listen: long-lived capture

A capture daemon for verifying events on **long sagas**, **high-volume
topics**, or topics under **broker retention pressure** — the three cases
where a windowed `kafka assert` can false-negative (scan-window miss,
volume-induced timeout truncation, retention cleanup). Where `kafka assert`
scans a bounded lookback window against a wall-clock deadline:

- **Start BEFORE the trigger.** `kafka listen start` seeks every assigned
  partition to its head (`OFFSET_END`) before the first poll delivers data,
  so only messages produced AFTER `start` are captured. No scan-window miss,
  no backlog replay. (Blocks until seeked-to-end + ready.)
- **Captures to disk.** Per-topic NDJSON under
  `<state-dir>/listen-<run_id>/`. A byte valve (`--max-bytes-per-topic`,
  default 256 MiB; `0` = unlimited) emits `capture.overflow` once and STOPs
  that topic instead of silently truncating.
- **Asserts with no deadline.** `kafka listen results` scans the capture
  file client-side, bounded by file size only — no timeout-truncation false
  negative.

## Protocol (load-bearing)

```bash
# 1. Start BEFORE the trigger (repeatable --topic)
agctl kafka listen start --topic orders.created --topic payments.events
# 2. Trigger the runbook (HTTP call, DB write, …)
OID=$(agctl http call create-order --param customer_id=cust-42 | jq -r '.result.body.order_id')
# 3. Attach expectations (repeatable; modes + roots identical to kafka assert)
agctl kafka listen assert --topic orders.created --pattern order-created --param orderId="$OID"
agctl kafka listen assert --topic payments.events --contains '{"status":"SUCCESS"}' --expect-count 1
# 4. Give the saga time, then collect (exit 1 if any expectation fails)
agctl kafka listen results
# 5. Stop + cleanup (deletes the run dir)
agctl kafka listen stop
```

**`stop` does NOT auto-run `results`** — termination + cleanup only. Skipping
`results` silently drops attached expectations when the run dir is deleted.
Always run `results` BEFORE `stop`.

**Commands:** `start` (daemon, POSIX/WSL-only) · `assert` (attach; exit 0,
does NOT evaluate) · `results` (evaluate all; exit 1 on any failure) · `stop`
(SIGTERM + cleanup) · `status` (read-only peek) · `messages` (debug tap on a
topic's capture) · `run` (foreground streaming — the daemon's spawn target and
native-Windows fallback).

**Selectors:** every subcommand takes `--run-id <id>` / `--pid <pid>` /
implicit-singleton (exactly one listener in `--state-dir`). Multiple listeners
+ no selector ⇒ exit 2 listing candidates. `stop --all` iterates every
running listener.

**`--topic` vs `--pattern`:** `--pattern <name>` reuses `kafka.patterns`
(contributes the pattern's `topic` + `match` + `cluster`); `--topic` is bare.
Both repeatable, de-duped. `--capture-match` is a coarse capture filter
(volume guardrail); assertion narrowing uses `assert --match`/`--contains`/
`--path` (same modes + envelope roots as `kafka assert`).

**Fatal at `stop`** (exit 1): `kafka.error`, or `capture.overflow` on a topic
with an attached expectation. Overflow on a non-asserted topic is a warning
(visible in `status`, non-fatal).
