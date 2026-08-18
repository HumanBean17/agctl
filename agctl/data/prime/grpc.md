# agctl — grpc: gRPC service calls

Requires the `grpc` extra: `pip install 'agctl[grpc]'` (grpcio, -tools,
-health-checking, -reflection, protobuf, jq).

**Two invocation modes:**
- **Template** `grpc call <template>` — resolves `grpc.templates[<name>]`
  (service, method, request with `{placeholder}` support). Prefer templates.
- **Free-form** `grpc call --target <name>` or `--address host:port` — ad-hoc
  calls without config. `--address` must be `host:port` (single colon, both
  non-empty); mutually exclusive with `--target`.

**Four call types** (auto-detected from the method descriptor): **unary**
(single→single), **client-stream** (NDJSON stdin requests → single response),
**server-stream** (single request → NDJSON stdout responses + final
`summary`), **bidi** (NDJSON stdin ↔ stdout). Streaming calls are the
streaming exceptions — background with `&`, capture PID, kill when done.

**Status-as-result semantics:** gRPC status codes are result fields, not
assertion failures. A non-OK status (e.g. `StatusCode.NOT_FOUND`) still
returns `ok: true` with `result.status.code`/`name`/`message`. Assertions
(`--status`, `--match`, …) evaluate separately and raise `AssertionFailure`
(exit 1) on mismatch.

**Healthcheck:** `agctl grpc healthcheck` (standard grpc.health.v1).

**Config:**

```yaml
grpc:
  targets:
    my-service:
      address: "host:port"
      use_tls: false            # plaintext h2c is the default; set true for TLS
      # tls: { override_authority: "" }
  descriptors:                  # fallback when reflection is off / unavailable
    - proto: "protos/my-service/v1/*.proto"
      include_paths: ["protos"]
    # OR - descriptor_set: "protos/compiled/my-service.pb"
  templates:
    my-method:
      target: my-service
      service: "ServiceName"
      method: "MethodName"
      request: { field: "{value}" }
```

**Discovery:** `agctl discover --category grpc-services` / `grpc-methods`
(reflection-based or from descriptors). Explore with `discover`, never
`config show`.

**Gotchas:** reflection-first with `descriptors[]` fallback (provide
descriptors for air-gapped environments); plaintext by default — set
`use_tls: true` for TLS services.
