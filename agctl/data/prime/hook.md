agctl v{version} is installed in this repo — the CLI test harness for its
running system (HTTP / Kafka / DB / gRPC / logs).

Before driving agctl, run: `agctl prime`
(the manual: JSON envelope, exit codes 0/1/2, intent→command map, top
gotchas — matches the installed binary).

Depth on demand: `agctl prime --topic <name>` — gotchas | mock | listen |
grpc | config | runbook-write | runbook-run (+ per-mode config-* topics,
listed in prime output); `--all` for everything.
