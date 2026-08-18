# agctl — config-http: extract an HTTP template

Produce a `templates.<name>` block from a route / controller / OpenAPI doc.
Best artifact first: OpenAPI/Swagger doc, controller source, or a plain
description (`POST /api/v1/orders` with body `{…}`).

## Extraction order

1. **method** — GET/POST/PUT/PATCH/DELETE.
2. **path** — copy the route; each path param → `{name}` (`/orders/{id}`).
3. **body** — mirror the request DTO; wrap each *variable* leaf in `{name}`
   (default a DTO primitive to `{field_name}`; enum/literal/`@DefaultValue`
   fields stay static; recurse into arrays/objects; ambiguous → `{field}`
   and confirm — over-parameterizing makes `discover` noisy,
   under-parameterizing makes the template rigid). Omit `body:` for
   GET/DELETE unless the endpoint really reads one.
4. **headers** — `Content-Type` (default `application/json` with a body);
   `Authorization`/cookies → `${ENV}`. Only headers the endpoint needs.
5. **service** — match base URL/prefix to a `services:` key; none fits →
   create one (ask `base_url`, `health_path`).
6. **name** — kebab-case from the route (`POST /api/v1/orders` →
   `create-order`).
7. **description** — one line.

## Stack snippets

- **OpenAPI** (preferred): `paths["/api/v1/orders"].post` → method+path;
  `parameters[in=path]` → `{name}`; `requestBody.content["application/json"]
  .schema` → body (resolve `$ref`); `securitySchemes` → `${ENV}` header.
- **Spring**: `@RequestMapping("/api/v1")` + `@PostMapping("orders")` →
  path; `@PathVariable("id")` → `{id}`; `@RequestBody OrderCreateRequest` →
  body (read the DTO); `@RequestHeader`/`@CookieValue` → `${ENV}` header;
  `@RequestParam` → query string on the path (`?status={status}`), not a
  body field.
- **FastAPI**: `@router.post("/orders")`; signature path param
  (`order_id: str`) → `{order_id}`; Pydantic model param → body;
  `Header(...)`/`Cookie(...)` deps → `${ENV}` header.
- **Node**: NestJS `@Controller("api/v1")` + `@Post("orders")`, `@Param("id")`
  → `{id}`, DTO class → body; Express `router.post("/orders/:id")`,
  `req.body` shape → body.

## Clarify (genuine gaps only)

Which `service` when the prefix matches several/none; `base_url` +
`health_path` for a new service; dynamic-vs-static when the DTO is silent;
the env-var name for a secret header.

## Gotchas

- Body and path use `{name}` **only** — never `${VAR}` (env) or `:` (SQL).
- `service` must resolve (add the entry if missing).
- Templates don't encode "expected status" — a 4xx/5xx at runtime is
  `ok:true`; asserting is the caller's job (`--status`/`--contains`/
  `--match`/`--jq-path` on `http call`/`request`).
- A body value that is itself a secret (rare) → `${ENV}`.
