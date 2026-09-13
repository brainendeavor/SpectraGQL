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
   * **HTTP Webhooks:** Outbound push delivery.
2. **Under Evaluation ("Maybe" List):**
   * **RabbitMQ (AMQP):** Deferred to prevent broker sprawl and maintain focus on streaming and event-sourcing architectures.

---

## 4. Consumer Worker Telemetry (SWTP) Design Guidelines

When collecting downstream worker status and logs:

### Option A: Capability-Driven Degradation (Current Default)
* The gateway's `EventSinkInspector` reports native broker capabilities in `GET /admin/api/v1/eventsink`.
* Sinks with native RPC (NATS) declare `"Worker Live Telemetry (SWTP RPC)"` and enable the interactive `[📄 Logs]` drawer.
* Streaming-only sinks (Kafka, Iggy, SierraDB) declare passive monitoring capabilities. The Admin UI disables RPC-based buttons and renders explanatory badges.

### Option B: The Universal Telemetry Stream Pattern (Universal Least-Common-Denominator)
If cross-sink worker telemetry is standardized via a shared stream (`spectra.telemetry`):
* **Mandatory Retention Bounds:** To prevent unbounded storage growth in the broker:
  * **NATS JetStream:** Configure stream with `max_msgs: 5000` or `max_age: 1h` with `discard: old`.
  * **Redis Streams:** Mandate `MAXLEN ~ 1000` on `XADD`.
  * **Kafka:** Topic retention must enforce `retention.ms=3600000` (1 hour) or `cleanup.policy=compact` keyed by `workerId`.
  * **Iggy:** Enforce max segment size / stream retention.
* **Throttled / Rollup Emission:** Workers must **never** publish individual `DEBUG`/`INFO` lines on every processed event (which doubles broker write volume). Workers must publish only on state transitions, errors, and periodic batched rollups (e.g., once every 3–5 seconds).

### Redis / Valkey Considerations
* Standardizing on Redis/Valkey for both distributed idempotency and worker telemetry lists (`LPUSH` + `LTRIM 0 199`) is technically clean, but **must remain optional**. SpectraGQL must operate as a zero-dependency self-contained appliance for teams running purely on NATS or Kafka.

---

## 5. Development & Testing Conventions

* **Check Code:** `cargo check`
* **Run Unit Tests:** `cargo test --lib`
* **Admin HTML Assets:** Embedded at compile time via `include_str!("assets/admin.html")`. Verify HTML test assertions in `src/admin/mod.rs` whenever updating UI assets.
* **Sandbox Awareness:** In sandboxed test environments, ephemeral network binding may fail with `PermissionDenied`. Integration tests requiring real TCP sockets should run with unsandboxed execution if permitted.
