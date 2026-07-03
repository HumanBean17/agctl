# `http` mode — extract an HTTP template

Point at a route / controller / OpenAPI doc; produce a `templates.<name>` block. The
non-negotiable contract (placeholder syntaxes, cross-refs, naming, verify-after) lives in
`SKILL.md` — this file is the extraction detail.

## Inputs

- The config path (resolved in `SKILL.md` Step 0).
- The artifact, most reliable first: an **OpenAPI/Swagger doc**, a **controller/handler source
  file**, or a plain description (`POST /api/v1/orders` with body `{…}`).

## Extraction order

1. **Method** — GET / POST / PUT / PATCH / DELETE.
2. **Path** — copy the route; convert each path param to `{name}` (e.g. `/orders/{id}`).
3. **Body** — mirror the request DTO; wrap each *variable* leaf in `{name}`. Default each DTO
   primitive field to `{field_name}` (caller supplies it per call); treat enum/literal-typed or
   `@DefaultValue`-annotated fields as static; recurse into arrays/objects. When a field could be
   either, mark it `{field_name}` and surface it for the user to confirm — over-parameterizing
   makes `discover` output noisy, under-parameterizing makes the template rigid. Omit `body:`
   entirely for GET/DELETE unless the endpoint really reads one.
4. **Headers** — `Content-Type` (default `application/json` when there's a body);
   `Authorization` / cookies → `${ENV}`. Emit only headers the endpoint actually needs.
5. **Service** — match the base URL / path prefix to an existing `services:` key. If none fits,
   create one (ask for `base_url` and `health_path`).
6. **Name** — kebab-case from the route (`POST /api/v1/orders` → `create-order`;
   `GET /orders/{id}` → `get-order`).
7. **Description** — one line.

## Stack snippets

### OpenAPI / Swagger (preferred — no source heuristics)

If `openapi.json` / `openapi.yaml` exists in the repo, read it directly:

- `paths["/api/v1/orders"].post` → method + path.
- `parameters[in=path]` → `{name}` path params.
- `requestBody.content["application/json"].schema` → body (resolve `$ref`).
- `securitySchemes` (apiKey/http bearer) → `${ENV}` header.

### Spring (JVM)

- `@RestController` + class-level `@RequestMapping("/api/v1")` concatenated with method-level
  `@GetMapping` / `@PostMapping` / `@PutMapping` / `@PatchMapping` / `@DeleteMapping` → path.
- `@PathVariable("id")` → `{id}` in the path.
- `@RequestBody OrderCreateRequest req` → body (read the DTO's fields).
- `@RequestHeader("Authorization")` / `@CookieValue` → `${ENV}` header.
- `@RequestParam` → a query string on the path (`?status={status}`), not a body field.

### FastAPI (Python)

- `@router.post("/orders")` / `@app.post(...)` → method + path.
- A path param in the signature (`order_id: str`) → `{order_id}`.
- A Pydantic model param (`req: OrderCreate`) → body (read the model's fields).
- `Header(...)` / `Cookie(...)` dependencies → `${ENV}` header.

### Node — NestJS / Express

- NestJS: `@Controller("api/v1")` + `@Post("orders")` → path; `@Param("id")` → `{id}`; a DTO
  class → body.
- Express: `router.post("/orders/:id", ...)` → method + path with `{id}`; the `req.body` shape
  → body.

## What to clarify (only genuine gaps)

- Which `service` if the prefix matches more than one — or none.
- `base_url` + `health_path` if you must create a service.
- Whether a field is dynamic (`{name}`) or static, when the DTO doesn't say.
- The env-var name for a secret header.

## Where it writes

Under top-level `templates:`, as a new key. Idempotent: if the key exists, diff and confirm
(`SKILL.md` contract #5).

## Gotchas

- Body and path use **`{name}`** only. Never `${VAR}` (that's env) or `:` (that's SQL).
- `service` must resolve to a real `services:` key — add one if missing (contract #2).
- A 4xx/5xx response is `ok:true` at runtime; templates don't encode "expected status" — that's
  the caller's assertion, not config.
- A body value that is itself a secret (rare) → `${ENV}`, not a literal.
