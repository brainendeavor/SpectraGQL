# AGENTS.md: Developer & Agent Architectural Guide for SpectraGQL

This document outlines the core architectural tenets, hot-path performance invariants, event sink compatibility rules, and development guidelines for engineers and autonomous AI agents working in the `SpectraGQL` repository.

---

## 1. Core Architectural Tenets

SpectraGQL is an ultra-high-performance **wire-speed API gateway and event-driven appliance** built on Cloudflare's Pingora engine in Rust.

### Dual-Pillar CQRS
* **Mode A (Read Path — Queries):** Reverse-proxies GraphQL queries and REST reads to upstream services with connection pooling, retries, and passive telemetry logging.
* **Mode B (Write Path — Mutations):** Terminates GraphQL mutations at the gateway edge:
  1. Issues an immediate, deterministic **Command Receipt** with a monotonic **UUIDv7** Command ID and **Hybrid Logical Clock (HLC)** timestamp.
  2. Dispatches the raw command payload into an append-only event sink.
  3. Returns HTTP 200 `ACCEPTED` in **sub-millisecond latency** ($< 1\text{ ms}$).

---

## 2. Hot-Path Performance Invariants (Zero Bloat)

Every microsecond, heap allocation, and mutex lock on the request/dispatch hot path degrades line-rate throughput. All code touching `early_request_filter`, `request_filter`, `handle_mode_b_edge`, and upstream proxy dispatch must strictly adhere to these invariants:

### Invariant 1: Zero Redundant Deserialization
* **Rule:** Never re-parse a request body that has already been parsed by the protocol decoder.
* **Bad:** Calling `gql.gql_request_body()` or `serde_json::from_str(&body_str)` a second time on the hot path.
* **Good:** Reuse `request_info.gql.as_ref().map(|g| g.json_body())` which was already parsed during the initial protocol decode step.

### Invariant 2: Zero Formatting & Pretty-Printing on Edge Worker Threads
* **Rule:** Formatting whitespace, newlines, and pretty JSON is strictly prohibited on hot-path edge worker threads.
* **Bad:** Calling `serde_json::to_string_pretty(&vars)` synchronously inside the mutation dispatch filter.
* **Good:** Store compact, unindented JSON (`vars.to_string()`) in memory. Shift all visual formatting, syntax indentation, and JSON beautification to the client browser (e.g., `JSON.stringify(JSON.parse(v), null, 2)`) when an administrator views the drawer.

### Invariant 3: Lock-Free Atomics & Non-Blocking Buffer Writes
* **Rule:** Real client traffic must **never wait on an observability lock**.
* **Bad:** Acquiring an exclusive, blocking `RwLock.write()` on rolling traffic buffers or latency accumulators on every incoming request.
* **Good:**
  * Metric counters and latency accumulators must use lock-free atomics (`AtomicU64` storing microsecond timestamps with `Ordering::Relaxed`).
  * Circular buffers must use non-blocking `.try_write()`. If an admin inspection holds a read lock, edge worker threads drop or sample the traffic entry rather than blocking user traffic.

### Invariant 4: Strict Out-of-Band Administrative APIs
* **Rule:** All administrative inspection and management endpoints (`/admin/*`) must execute out-of-band and introduce zero branching, locking, or allocation into `/graphql`, `/gql`, or `/api`.

### Invariant 5: Zero Unwraps & Fail-Safe Result Propagation
* **Rule:** Never call `.unwrap()` or `.expect()` in production code paths, runtime initialization, network parsing, or concurrency guards.
* **Bad:** `addr.to_socket_addrs().unwrap().next().unwrap()`, `HeaderValue::from_str(...).unwrap()`, `lock().unwrap()`.
* **Good:**
  * Network resolution and configuration loading must propagate typed `Result` errors with actionable diagnostics.
  * HTTP headers must use compile-time static constants (`HeaderValue::from_static(...)`) or fallible conversion (`HeaderValue::try_from(...)`).
  * Concurrency locks must use poison-safe recovery (`lock().unwrap_or_else(|e| e.into_inner())`) or lock-free atomics.

---

## 3. Event Sink Philosophy & Least-Common-Denominator Capabilities

SpectraGQL supports multiple event sinks. When designing cross-sink features (such as consumer lag inspection, worker health checks, or log telemetry), **never assume broker-specific RPC primitives**.

### The Sink Spectrum
* **Message Brokers with Ephemeral Inboxes / Native RPC:**
  * **NATS:** Native Request-Reply via dynamic inboxes (`_INBOX.xxx`).
* **Log-Oriented & Streaming Event Sinks (No Native RPC):**
  * **Apache Kafka / Redpanda:** Distributed partition offset log. (RPC is a severe anti-pattern in Kafka).
  * **Apache Iggy:** Zero-copy append-only sequential stream over TCP/QUIC.
  * **SierraDB:** Causal append-only event store.
  * **HTTP Webhooks:** Unidirectional fire-and-forget push.
* **Key-Value / Multi-Model Stores:**
  * **Redis / Valkey Streams:** Append-only stream (`XADD`), consumer groups (`XREADGROUP`), and list primitives (`LPUSH`/`LRANGE`).

### Sink Support Tiers
1. **Tier 1 (First-Class Supported Sinks):**
   * **NATS JetStream:** Primary reference implementation for streaming CQRS.
   * **Redis Streams (Valkey):** Low-latency stream buffer with consumer groups.
   * **Apache Kafka / Redpanda:** Enterprise partition streaming.
   * **Apache Iggy:** High-throughput Rust zero-copy streaming.
   * **SierraDB:** Causal monotonic event store.
2. **Under Evaluation ("Maybe" List):**
   * **RabbitMQ (AMQP):** Deferred to prevent broker sprawl and maintain focus on streaming and event-sourcing architectures.
3. **Reclassified Components:**
   * **HTTP Webhooks:** Synchronous HTTP POST on the edge write path causes head-of-line blocking when third-party endpoints lag. Webhooks are implemented as an **out-of-the-box consumer worker** (`spectra-webhook-worker`) reading from the broker with configurable exponential backoff, retries, and dead-letter queues (DLQ).

### Edge Interceptor Rejection Audit Convention
* When a request is rejected at the gateway edge by an interceptor (CEL rule, WASM plugin, or sanitization failure), SpectraGQL publishes a structured audit event to the active broker:
  * **Topic Pattern:** `interceptors.rejected.<operation_name>` (e.g., `interceptors.rejected.recordvote`, `interceptors.rejected.anonymous`).
  * **Payload:** Monotonic UUIDv7 ID, HLC timestamp, operation name/type, rejection code, status code, reason, client IP, and query/variables preview.

---

## 4. Consumer Worker Telemetry & BYOW Discovery Guidelines

SpectraGQL is **broker-first and framework-agnostic**. Downstream mutation write handlers follow the "Bring-Your-Own-Worker" (BYOW) paradigm:

### Option A: Zero-Touch Broker-Native Discovery (Active Primary Standard)
* **Zero Worker Code:** Downstream workers (written in Go, Node.js, Python, Rust, or Temporal workflows) do **not** need custom telemetry SDKs, open HTTP ports, or reverse network connectivity to the gateway's admin port.
* **Broker Ground Truth:** SpectraGQL's `EventSinkInspector` queries broker consumer groups directly out-of-band:
  * **NATS JetStream:** Inspects consumer lag (`num_pending`), unacked messages (`num_ack_pending`), and redeliveries (`num_redelivered`) via `js.consumer_info()`.
  * **Kafka / Redpanda:** Inspects high-watermarks, partition offsets, and active consumer group members.
  * **Redis / Valkey Streams:** Inspects consumer groups via `XINFO GROUPS` and pending messages via `XPENDING`.
* **Automatic Admin UI Resolution:** When no workers explicitly register via direct API, `/admin/api/v1/workers` synthesizes worker summaries from active broker consumer metrics.

### Option B: Universal Direct Telemetry API (Optional Embedded / Legacy Fallback)
* **API Endpoint:** `POST /admin/api/v1/telemetry/report`
* **Use Case:** Opt-in for embedded edge appliances (e.g. SpectraFlux instances) or local development when in-memory log rollups inside the gateway drawer are desired.
* **Bounded Registry:** `WorkerRegistry` (`src/admin/registry.rs`) caps history to 200 log entries per worker with a 15-second liveness timeout.

---

## 5. Development & Testing Conventions

* **Check Code:** `cargo check`
* **Run Unit Tests:** `cargo test --lib`
* **Admin HTML Assets:** Embedded at compile time via `include_str!("assets/admin.html")`. Verify HTML test assertions in `src/admin/mod.rs` whenever updating UI assets.
* **Sandbox Awareness:** In sandboxed test environments, ephemeral network binding may fail with `PermissionDenied`. Integration tests requiring real TCP sockets should run with unsandboxed execution if permitted.

---

## 6. Remote Fluxcell Deployment & Security Governance Standards

SpectraGQL supports remote, dynamic deployment and hot-swapping of WebAssembly **Fluxcells** into the downstream `spectra-flux` chassis. Autonomous agents and developers must strictly follow these invariants:

### Zero-Compiler Downstream Container Invariant
* **Rule:** `spectra-flux` runtime containers must NEVER bundle `rustc`, `cargo`, or `git`. Container images must remain ultra-lean ($< 25\text{ MB}$).
* **Build Pattern:** Compilation occurs in external CI/CD pipelines (e.g. GitHub Actions, Docker multi-stage builds). The gateway/chassis operates exclusively on immutable pre-compiled `.wasm` binaries published to remote HTTPS object stores (GitHub Releases, AWS S3, Cloudflare R2).

### Deployment Authentication & Token Configuration Hierarchy
All deployment operations (`deployFluxcell`, `activateFluxcell`, `removeFluxcell`) are intercepted by `DeployAuthInterceptor` using constant-time token comparison. The deployment token is resolved using the following strict hierarchy:
1. **Environment Variable (Top Precedence - 12-Factor Secret Standard):**
   `SPECTRA_DEPLOY_TOKEN` (or `SPECTRAGQL_DEPLOY_TOKEN`).
2. **Route/Interceptor-Level Token (`spectra.toml`):**
   `[interceptors.deploy_guard]` with `token = "..."`.
3. **Admin Configuration Fallback (`spectra.toml`):**
   `[admin]` with `deploy_token = "..."`.
4. **Fail-Safe Default:**
   If no token is configured in either the environment or `spectra.toml`, all deployment operations are **unconditionally rejected with HTTP 401 `DEPLOY_UNAUTHORIZED`**.

### Multi-Layer Security Invariants
1. **Disabled-by-Default Routes:** Deployment routes must default to `enabled = false` in `spectra.toml`.
2. **Downstream SSRF Shield:** Remote artifact URLs must use `https://`, match configured host allowlists, and pass pre-connect DNS inspection blocking loopback, private RFC 1918 subnets, and cloud instance metadata (`169.254.169.254`).
3. **Atomic Dual Killswitches:** `external_deploy_enabled` and `dev_upload_enabled` must use lock-free atomics and be instantly killable via `POST /admin/api/v1/security/lockdown`.
4. **Two-Phase Staging:** Deployments default to `auto_activate = false`. Cells remain in `Staged` state without mounting routes into the Radix tree until explicit administrator approval or `activateFluxcell`.

---

## 7. Public Open-Source Repository Invariant (Zero Application Pollution)

`SpectraGQL` and `SpectraFlux` are public, generic infrastructure appliances and open-source engine frameworks.

* **Strict Invariant:** Application-specific domain names (e.g., `*.coeval.bio`, `db.coeval.us`), application IDs (`coeval`, `humanbase`), application-specific GraphQL operations (`recordVote`), business schemas, or proprietary fluxcell `.wasm` binaries must **NEVER** be hardcoded or committed into `SpectraGQL` or `SpectraFlux` repository files (including `spectra.toml`, `spectra-flux.toml`, or default source code).
* **Configuration Mechanism:** All application-specific routes, apps, upstreams, domains, and tokens must be supplied dynamically at deployment runtime via:
  1. **12-Factor Environment Variables:** Configured via the hosting platform (e.g., Railway, Kubernetes ConfigMaps, Docker compose env files).
  2. **External Volume-Mounted Configuration:** Production deployments mount custom `spectra.toml` or `spectra-flux.toml` files at container launch.
  3. **Dynamic Deployer APIs:** Uploading and activating compiled application fluxcell `.wasm` binaries via `POST /_flux/deployer/upload`.


