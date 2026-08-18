---
name: agctl
description: Drive the agctl/agt CLI test harness — run agctl commands against a running system (HTTP/Kafka/DB/gRPC/logs), author or validate agctl.yaml, or write and execute agctl test runbooks. Invoke whenever agctl, agt, or agctl.yaml is involved.
---

# agctl

Run `agctl prime` before driving agctl — it prints the manual: output
envelope, exit codes, intent→command map, top gotchas. `agctl <cmd> --help`
is the authoritative flag spec.

Depth on demand: `agctl prime --topic <name>` (topics are listed in prime
output — e.g. gotchas, mock, config, runbook-write); `--all` for everything.

Every agctl command prints one JSON object on stdout and exits 0 (ok) /
1 (assertion failed) / 2 (bad invocation, config, or env). Parse stdout
only; stderr is diagnostics.
