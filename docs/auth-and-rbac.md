# Wire-Speed Edge RBAC, ABAC & Identity Injection

SpectraGQL features an in-engine, sub-microsecond **Role-Based Access Control (RBAC)** and declarative **Attribute-Based Access Control (ABAC)** interceptor running on Cloudflare Pingora worker threads.

It provides centralized, line-rate zero-trust enforcement at the network perimeter before traffic reaches upstream microservices or is appended to downstream event logs.

---

## 1. Edge Authorization Architecture & Capabilities

SpectraGQL operates as a standalone perimeter gatekeeper on Cloudflare Pingora. It provides centralized authentication and authorization before requests reach upstream services or downstream event streams, operating independently of any specific backend or worker framework.

Whether downstream consumer workers are written in Go, Python, Node.js, Rust, or Temporal workflows, or reads are reverse-proxied to existing GraphQL/REST monoliths, SpectraGQL enforces zero-trust security at line rate:

```
                                  Client Request / Mutation
                                              │
                                              ▼
                               ┌─────────────────────────────┐
                               │   SpectraGQL Edge Gateway   │
                               │   (Cloudflare Pingora)      │
                               │                             │
                               │  • Line-Rate JWT/OIDC Auth  │
                               │  • Bounded L1 Cache (<50µs) │
                               │  • Operation-Level RBAC     │
                               │  • Declarative ABAC (CEL)   │
                               └──────────────┬──────────────┘
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     │                                                 │
          Mode A: Query (Read Path)                         Mode B: Mutation (Write Path)
                     │                                                 │
                     ▼                                                 ▼
      ┌─────────────────────────────┐                   ┌─────────────────────────────┐
      │ Upstream Microservices      │                   │ Append-Only Event Sink      │
      │ (Go, Node, Rails, Hasura)   │                   │ (NATS, Kafka, Redis Streams)│
      │                             │                   └──────────────┬──────────────┘
      │ • Injected Identity Headers:│                                  │
      │   - x-user-id               │                                  ▼
      │   - x-tenant-id             │                   ┌─────────────────────────────┐
      │   - x-user-roles            │                   │ Bring-Your-Own-Worker (BYOW)│
      │ • Zero Cryptographic Load   │                   │ (Go, Python, Temporal, etc.)│
      │ • Protects Connection Pools │                   │                             │
      └─────────────────────────────┘                   │ • Zero Log Poisoning        │
                                                        │ • Pre-Sanitized Payloads    │
                                                        │ • Verified Caller Claims    │
                                                        └─────────────────────────────┘
```

### A. Mode B (Write Path): Broker Poisoning Prevention for Any Consumer Fleet
In an asynchronous, event-driven CQRS architecture, appending an unauthenticated or unauthorized mutation into Kafka, NATS JetStream, or Redis Streams is hazardous:
* **Log Poisoning Prevention:** Downstream consumer workers waste expensive CPU cycles and database transactions processing unauthorized jobs, and dead-letter queues (DLQs) quickly get flooded with invalid events. SpectraGQL blocks unauthorized mutations at the network perimeter in $< 50\,\mu\text{s}$ before they can ever enter the event log.
* **Sub-Millisecond Edge Dispatches:** When authorized, SpectraGQL immediately returns a deterministic HTTP 200 `ACCEPTED` Command Receipt with a monotonic UUIDv7 ID and Hybrid Logical Clock (HLC) timestamp in $< 1\text{ ms}$, then writes the command to the broker with pre-validated caller identity (`userId`, `tenantId`, `roles`, metadata).
* **Universal BYOW Support:** Downstream workers written in **Go, Python, Java, Rust, Node.js, or Temporal workflows** receive clean, pre-sanitized events with verified identity metadata. Workers no longer need to bundle repetitive JWT verification SDKs or make out-of-band JWKS network calls.
* **Edge Interceptor Rejection Audits:** When a request is rejected by an RBAC rule, CEL attribute check, or invalid signature, SpectraGQL publishes a structured audit event to the active broker under `interceptors.rejected.<operation_name>` with client IP, HLC timestamp, and rejection reason for SIEM analysis.

### B. Mode A (Read Path): Centralized Cryptographic Offloading & Backend Shielding
For reverse-proxied GraphQL queries and REST reads:
* **Cryptographic Offloading:** Upstream backend services (Node.js, Go, Rails, Hasura, etc.) do not need to fetch remote JWKS certificates, parse X.509 keys, or execute compute-heavy RSA/ECDSA signature verification. SpectraGQL handles this at line rate ($< 50\,\mu\text{s}$) in native Rust.
* **Trusted Identity Header Injection:** Upon successful token verification, SpectraGQL injects sanitized, tamper-proof HTTP headers into the upstream proxy request:
  * `x-user-id`: Subject ID from JWT (`sub`).
  * `x-tenant-id`: Multi-tenant organization identifier (`org_id` / `tenant_id`).
  * `x-user-roles`: Comma-delimited list of authorized roles.
  * `x-user-email`: Verified email claim.
  Upstream services consume these headers directly with zero cryptographic overhead.
* **Pre-Routing Attack Shielding:** Anonymous probes, credential stuffers, and unauthorized actors are terminated at the edge with HTTP `401 Unauthorized` or `403 Forbidden`. Unauthorized requests never reach internal networks or exhaust backend database connection pools.

### C. Universal External AuthN & Identity Provider (IdP) Support
SpectraGQL is designed to authenticate tokens issued by **any standard OpenID Connect (OIDC) or OAuth 2.0 provider**, eliminating vendor lock-in and avoiding custom gateway authentication plugins:
* **Turnkey Provider Compatibility:** Native support for **Auth0, Clerk, Supabase, Firebase Authentication, AWS Cognito, Okta, Keycloak, WorkOS**, or self-hosted identity servers.
* **Streaming Remote JWKS Resolution:** Points directly to the provider's standard JSON Web Key Set endpoint (`jwks_url`, e.g., `https://<tenant>.clerk.accounts.dev/.well-known/jwks.json` or `https://<tenant>.auth0.com/.well-known/jwks.json`). SpectraGQL asynchronously resolves and caches public keys (supporting RS256, RS384, RS512, and Ed25519) with automated background key rotation (`refresh_interval_seconds`).
* **Flexible Custom Claims & Role Extraction:** Identity providers structure user roles and tenant scopes differently. SpectraGQL provides configurable JSON pointer paths (`roles_claim`, `tenant_claim`, `user_id_claim`, `permissions_claim`) to extract claims regardless of provider conventions:
  * **Clerk:** `roles_claim = "metadata.roles"`, `tenant_claim = "org_id"`
  * **Auth0:** `roles_claim = "https://your-domain.com/roles"` or standard `"permissions"`
  * **Supabase:** `roles_claim = "app_metadata.roles"`, `tenant_claim = "app_metadata.tenant_id"`
  * **Keycloak:** `roles_claim = "realm_access.roles"`
* **Symmetric Secrets & Machine-to-Machine (M2M):** For internal services or server-to-server microservices using HMAC signatures, configure static shared secrets via `secret = "env:SPECTRA_JWT_SECRET"`.

---

## 2. Architecture & Hot-Path Invariants

### 1. Bounded L1 Memory Cache (LRU Eviction)
To prevent heap exhaustion under adversarial denial-of-service traffic (e.g. rotating malicious bearer tokens), verified tokens and claims are cached in a thread-safe, lock-free LRU cache (`lru::LruCache`) bounded at **10,000 entries**. Cache lookups complete in sub-microsecond time.

### 2. Zero Hot-Path Allocations
* **Borrowed Operation Names:** Operation names are evaluated via borrowed string slices (`op_name.as_deref().unwrap_or("")`) rather than cloned strings.
* **Pre-Allocated Static CEL Keys:** Inbound and outbound CEL evaluators utilize compile-time `std::sync::LazyLock` pre-allocated `cel::objects::Key` instances, eliminating runtime heap allocations on edge worker threads.

### 3. Standards Compliance (RFC 6750 & RFC 7519)
* **RFC 6750 Case-Insensitive Bearer Matching:** Resolves `Authorization` headers using case-insensitive scheme matching (`bearer ` or `Bearer `).
* **RFC 7519 Skew Tolerance & Claims:** Enforces $\pm 60\text{s}$ physical clock skew tolerance on expiration (`exp`) claims and validates Not-Before (`nbf`) claims.

---

## 3. Configuration Reference (`spectra.toml`)

Enable edge RBAC/ABAC in `spectra.toml`:

```toml
[interceptors.auth_guard]
enabled = true
policy_mode = "deny_unlisted" # "audit_only" | "deny_unlisted" | "allow_authenticated" | "pass_all"

# 1. Identity Provider / JWKS Configuration (Clerk, Auth0, Supabase, Keycloak, etc.)
[interceptors.auth_guard.oidc]
# Examples:
# Clerk:    "https://<your-tenant>.clerk.accounts.dev/.well-known/jwks.json"
# Auth0:    "https://<your-tenant>.auth0.com/.well-known/jwks.json"
# Supabase: "https://<project-ref>.supabase.co/auth/v1/.well-known/jwks.json"
jwks_url = "https://your-tenant.clerk.accounts.dev/.well-known/jwks.json"
refresh_interval_seconds = 3600

# Or static HS256 / RS256 secret (can also be supplied via SPECTRA_JWT_SECRET)
# secret = "env:SPECTRA_JWT_SECRET"

# 2. Custom Claims & Role Extraction
[interceptors.auth_guard.roles_mapping]
roles_claim = "roles"           # e.g. "roles", "permissions", "metadata.roles", "realm_access.roles"
tenant_claim = "org_id"         # e.g. "org_id", "tenant_id", or "app_metadata.tenant_id"
user_id_claim = "sub"

# 3. Declarative Operation Permissions
[interceptors.auth_guard.operations]
# Public operations
"login" = ["*"]
"registerUser" = ["*"]
"publicCatalog" = ["*"]

# Role-restricted operations
"createInterview" = ["candidate", "recruiter", "admin"]
"gradeSubmission" = ["evaluator", "admin"]
"deleteAccount"   = ["admin"]

# 4. Declarative ABAC Rules via CEL
[[interceptors.auth_guard.rules]]
name = "tenant_isolation"
expression = "request.headers['x-tenant-id'] == request.auth.claims['tenant_id']"
action = "deny"
code = "TENANT_MISMATCH"
message = "Cross-tenant access prohibited"
```

---

## 4. Policy Mode Continuum

SpectraGQL provides a progression of operational enforcement modes:

| Mode | Behavior | Use Case |
| :--- | :--- | :--- |
| **`audit_only`** *(Default)* | Evaluates tokens and permissions, emits structured warning logs and audit events, but **never blocks requests**. | Safe onboarding, production traffic analysis, and dry-run policy rollout. |
| **`deny_unlisted`** | Strictly requires a valid JWT with permitted roles for listed operations; unlisted operations are **denied by default**. | Production zero-trust microservice environments. |
| **`allow_authenticated`**| Permits any request with a valid signature and non-expired token; verifies operation roles only when explicitly mapped. | Developer platforms with mixed public/authenticated boundaries. |
| **`pass_all`** | Completely disables auth verification on this interceptor. | Local offline prototyping or mock testing. |

---

## 5. Optional Companion Appliance: SpectraFlux (The Turnkey WASM Worker)

If your organization **does not already have an existing worker fleet** or wants an ultra-lean, sandboxed WebAssembly execution environment for event handling, SpectraGQL pairs seamlessly with **[SpectraFlux](https://github.com/brainendeavor/SpectraFlux)**.

`SpectraFlux` is a purpose-built WebAssembly execution chassis designed specifically to consume Mode B events dispatched by SpectraGQL:

```
 SpectraGQL (Edge Gatekeeper)
   ├── Line-rate JWT / OIDC verification against JWKS
   ├── Coarse operation RBAC & perimeter CEL rules
   └── Dispatches verified command envelope to broker
              │
              ▼ (NATS / Kafka / Redis Streams)
 SpectraFlux (Turnkey WASM Chassis)
   ├── Zero Cryptographic Overhead (no JWKS or RSA in WASM)
   ├── Sandboxed Fluxcells enforce fine-grained domain authorization
   ├── PostgreSQL role persistence via embedded dbmate (`auth_user_roles`)
   └── Decoupled storage tiers (internal session state vs. app KV)
```

* **Coarse vs. Fine-Grained Authorization:** SpectraGQL enforces coarse perimeter permissions ("Can user with role `editor` call `updateProject`?"), while SpectraFlux guest fluxcells enforce fine-grained domain authorization ("Does user `#123` own project `#42` in PostgreSQL?").
* **Zero Cryptographic Overhead:** Because SpectraGQL has already verified signatures and sanitized inputs at the edge, WASM fluxcells execute with zero crypto overhead.
* **Detailed Companion Guide:** For full architectural details, guest SDK examples (Rust & TypeScript), and PostgreSQL schema patterns, see the **[SpectraFlux Downstream Authorization Guide](https://github.com/brainendeavor/SpectraFlux/blob/main/docs/authz.md)**.
