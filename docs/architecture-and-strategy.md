# SpectraGQL Architecture & Strategy
**The Write-Path Gateway for GraphQL**
*Transforming GraphQL mutations into high-performance event streams*

---

## 1. Executive Summary & Core Thesis

### The Core Problem
GraphQL is widely praised for giving frontend teams expressive control over data retrieval. However, in enterprise environments, its write path (**Mutations**) has devolved into an undisciplined architectural liability:
- **Mutations as Arbitrary RPCs:** Mutations are often implemented as unstructured, unversioned RPC endpoints with weak input contracts and zero cross-boundary coordination.
- **The Synchronous Fan-Out / Dual-Write Anti-Pattern:** A single mutation resolver frequently attempts to coordinate multiple downstream microservices or databases synchronously. If an intermediate step fails, state is left corrupted and uncoordinated, with no native rollback, saga orchestration, or audit trail.
- **Federation's Blind Spot:** Existing enterprise GraphQL solutions (Apollo Federation, WunderGraph Cosmo, Hive) focus overwhelmingly on the **Read path**—distributed query planning, subgraph schema composition, and entity resolution. They treat Mutations as an afterthought, merely forwarding HTTP POST requests to subgraphs.

### The Thesis of SpectraGQL
GraphQL inherently provides the architectural boundary needed for **Command Query Responsibility Segregation (CQRS)**:
- `Query` operations are explicitly **Reads** (idempotent, cacheable, fan-out friendly).
- `Mutation` operations are explicitly **Commands** (intents to alter state, subject to domain validation and destined for an immutable, ordered event log).

> For a deep dive into the architectural debate, deconstructing the mutation anti-pattern critique, and why GraphQL mutations belong on the event-driven write path, see **[Philosophy & Core Thesis](philosophy.md)**.

```
                      ┌───────────────────────────────────────────────┐
                      │          SpectraGQL Proxy (Pingora)           │
                      │                                               │
                      │   [Parse GQL AST: Query vs Mutation]          │
                      └───────┬───────────────────────────────┬───────┘
                              │                               │
                      Query   │                      Mutation │ (Command)
                    (Read)    ▼                               ▼
               ┌──────────────────────┐             ┌───────────────────┐
               │ Legacy API / Backend │             │  Ratification     │
               │ Query Service / Read │             │  - Idempotency    │
               │ Cache / Replicas     │             │  - Schema Rules   │
               └──────────────────────┘             │  - WASM / Policy  │
                                                    └─────────┬─────────┘
                                                              │ Dispatch
                                                              ▼
                                                    ┌───────────────────┐
                                                    │ Event Backbone    │
                                                    │ NATS / Iggy /     │
                                                    │ Kafka / SierraDB  │
                                                    └─────────┬─────────┘
                                                              │
                                                              ▼
                                                    ┌───────────────────┐
                                                    │ Event Consumers   │
                                                    │ (Async Resolvers) │
                                                    └───────────────────┘
```

**SpectraGQL** is a high-performance reverse proxy built on Cloudflare's **Pingora** that sits at the network boundary, intercepting GraphQL traffic at wire speed:
1. Routing **Queries** directly to existing backends, read replicas, or caches.
2. Intercepting **Mutations**, **ratifying** them (enforcing idempotency, versioning rules, and governance policies), and **dispatching** them as immutable Commands to an event backbone (NATS JetStream, Apache Iggy, SierraDB, Kafka).
3. Empowering teams to build **mutation resolvers as event-sourced consumers** rather than synchronous HTTP handlers locked into a request/response cycle.

---

## 2. Market Analysis: The Gateway Landscape

### Does any existing gateway do this intentionally?
**Short answer: No.** There is currently no streamlined, dedicated, independent CQRS Command Gateway for GraphQL on the market.

Here is an objective breakdown of how the landscape handles this today:

| Gateway / Solution | Primary Focus | Write/Mutation Strategy | Limitations for CQRS & Event Sourcing |
| :--- | :--- | :--- | :--- |
| **Apollo Router** (Rust) | Apollo Federation v2 query planner | Subgraph HTTP routing | Mutations are treated as simple HTTP POST calls to subgraphs. Event streaming requires building custom Rust plugins or Rhai scripts from scratch with zero architectural guidance. |
| **WunderGraph Cosmo** (Go) | Open-source GraphQL Federation | Directives (`@edfs__natsPublish`, `@edfs__kafkaPublish`, `@edfs__natsRequest`) | **Closest feature match in the industry**, but tightly coupled to full Apollo Federation v2 schema composition and Cosmo's proprietary control plane. Cannot function as a standalone, transparent L7 proxy in front of legacy monolithic or non-federated GraphQL APIs. Lacks in-flight mutation ratification and schema drift diagnostics. |
| **Tailcall** (Rust) | High-performance GQL composition | HTTP / gRPC execution | Purely focused on read-side N+1 query optimization and declarative schema composition. |
| **Hasura / PostGraphile** | Database-to-GraphQL compiler | Database transactions + CDC Event Triggers | Events are emitted *after* database commit (Change Data Capture), tightly coupled to specific SQL engines. Not a gateway for distributed microservices. |
| **Envoy / Kong / Traefik** | General Layer 7 Gateways | Blind HTTP proxying | GraphQL plugins are limited to basic query cost analysis, depth limiting, and rate limiting. They lack the semantic AST awareness to separate Queries from Mutations into different backend topologies. |
| **Stellate** | GraphQL CDN | Edge caching for queries | Completely bypasses mutations. |

### The Unoccupied White Space
No tool exists that acts as a **transparent, drop-in CQRS proxy** that:
- Runs at native wire speed in Rust (via Pingora).
- Does not demand migrating to a massive, complex Federation schema.
- Provides an active **Ratification pipeline** for mutation discipline (idempotency, deprecation enforcement, WASM policy).
- Supports pluggable, modern streaming protocols (NATS JetStream, Apache Iggy, SierraDB, Kafka).

---

## 3. Core Architectural Concepts & Operating Modes

### The Paradigm Shift: Resolvers as Event Consumers
In traditional GraphQL architectures, mutation resolvers are synchronous workers that handle validation, database transactions, third-party API calls, and notification triggers in a single thread. If any step times out or fails, the system enters an ambiguous state.

In the SpectraGQL paradigm:
- The proxy ratifies and ingests the mutation as a **Command**.
- The resolver is re-architected as an **Event Consumer / Saga Worker** that subscribes to the command topic on the event backbone.
- Projections update read models asynchronously.

To bridge the gap between legacy systems and this event-driven future, SpectraGQL supports two core operational modes (for a deep-dive sequence analysis and configuration examples, see [Modes of Operation](modes-of-operation.md)):

### Mode A: The Workhorse Gateway (Pragmatic Event Choreography — Flagship)
- **Mechanism:** SpectraGQL intercepts the mutation, forwards the HTTP request to the primary backend service for synchronous local execution, and—upon receiving an HTTP 2xx response—dispatches the completed domain event (stashed request arguments + response data) to the event backbone.
- **Client Experience:** 100% transparent. The client sends a traditional GraphQL mutation, receives its expected synchronous response, and enjoys automatic Apollo/Relay cache normalization.
- **Value Proposition:** **Zero code changes required.** Allows core resolvers to do one fast write and eliminates synchronous cross-service fan-out. Downstream subsystems (loyalty, notifications, search indexing) react choreographically off NATS/Iggy/SierraDB.
- **Dispatch Policies:** Supports `response_only` (default: 1 event per success, zero phantom writes), `response_with_failure` (emits stashed request on timeout/error), and `raw_audit` (dual ingress/egress).

### Mode B: The Event-Native Gateway (Pure Asynchronous CQRS)
- **Mechanism:** SpectraGQL intercepts the mutation, ratifies it, and publishes it directly to the event backbone. It terminates the HTTP request immediately, returning a deterministic **Command Receipt**:
  ```json
  {
    "data": {
      "submitOrder": {
        "commandId": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        "status": "ACCEPTED",
        "timestamp": "2026-09-08T20:50:00Z"
      }
    }
  }
  ```
- **Client Experience:** The client is designed for eventual consistency. State updates are observed via GraphQL Subscriptions (streaming from the event bus), WebSockets, or background polling.
- **Value Proposition:** True CQRS for high-throughput ingestion (IoT, gaming, financial order streams) and long-running sagas (bulk imports, media encoding).

### Mode C: "The Mirage" (Evaluated & Retired — "Not Today")
- **Status:** Officially archived. Mode C attempted to use NATS request-reply to hold synchronous HTTP connections while event workers processed in the background. It created an uncanny valley: retaining all the timeout vulnerabilities of synchronous HTTP while incurring the full operational weight of an event broker. Mode A is strictly superior for synchronous clients.

> **Hybrid Routing Strategy:** SpectraGQL enables per-route configuration. 95% of mutations run in **Mode A**, while specialized high-volume or long-running operations run in **Mode B**.

---

## 4. The Request Pipeline & Engine Components

Built on Cloudflare's **Pingora** framework, SpectraGQL operates as a multi-stage, zero-allocation pipeline:

```
Session Ingest (Pingora)
       │
       ▼
┌────────────────────────────────────────────────────────┐
│ 1. Early Route & Filter                                │
│    - Fast path matching (matchit Router)               │
│    - Differentiate GQL vs. REST traffic                │
└──────────────────────┬─────────────────────────────────┘
                       ▼
┌────────────────────────────────────────────────────────┐
│ 2. Request Ratification Engine                         │
│    - High-speed AST parsing (Operation & Arguments)    │
│    - Idempotency verification (Deduplication)          │
│    - Schema compliance & deprecation diagnostics       │
│    - Policy evaluation (WASM / Lua plugins)            │
└──────────────────────┬─────────────────────────────────┘
                       ▼
         ┌─────────────┴─────────────┐
   [Query / Read]              [Mutation / Command]
         │                           │
         ▼                           ▼
┌───────────────────┐       ┌────────────────────────────┐
│ 3a. Upstream Peer │       │ 3b. Dispatch Layer         │
│     Direct proxy  │       │     Pluggable Adapters:    │
│     to read API,  │       │     - NATS JetStream       │
│     cache, or     │       │     - Apache Iggy          │
│     replicas      │       │     - SierraDB             │
└────────┬──────────┘       │     - Apache Kafka         │
         │                  └──────────────┬─────────────┘
         │                                 │
         │   ┌─────────────────────────────┘
         │   │ (Mode A: Also send upstream; Mode B: Return Receipt)
         ▼   ▼
┌────────────────────────────────────────────────────────┐
│ 4. Response Ratification & Capture                     │
│    - Inject correlation headers (x-spectra-request-id)  │
│    - Capture response payload & status                 │
│    - Dispatch response telemetry / audit event         │
└────────────────────────────────────────────────────────┘
```

### The Ratification Engine (Governance & Discipline)
The Ratification layer provides the discipline missing in ad-hoc GraphQL setups:
1. **Idempotency Enforcement:**
   - Evaluates incoming `Idempotency-Key` headers or hashes `(client_id, operation_name, variables)`.
   - Checks state against a cache (Redis or in-memory ring buffer) to reject duplicate executions and prevent double-writes caused by network retries.
2. **Schema Drift & Deprecation Guardrails:**
   - Detects when clients invoke mutations marked `@deprecated`.
   - Logs or blocks breaking payload changes across system boundaries.
3. **WASM & Lua Extension Hooks:**
   - Lightweight execution sandbox (via Wasmtime or Extism for WASM, mlua for Lua).
   - Allows platform teams to enforce tenant-isolation rules, inject authenticated claims into commands, or sanitize sensitive arguments (PII redaction) before dispatching to the event log.

### Pluggable Dispatch Adapters
To avoid vendor lock-in while leveraging cutting-edge Rust technology, dispatchers implement a common trait:

```rust
#[async_trait]
pub trait DispatchAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    async fn dispatch_command(&self, topic: &str, payload: &CommandPayload) -> Result<()>;
    async fn dispatch_event(&self, topic: &str, payload: &EventPayload) -> Result<()>;
}
```

#### Supported Backends:
1. **Apache Iggy:** Pure-Rust, cache-friendly streaming broker delivering sub-millisecond tail latencies and massive throughput.
2. **SierraDB:** Native event-sourcing database engineered specifically for storing immutable event streams.
3. **NATS JetStream:** Lightweight, cloud-native pub/sub, streaming, and request-reply engine.
4. **Apache Kafka / Redpanda:** Ubiquitous enterprise event streaming platform.
5. **Webhook Dispatch (`WebhookDispatch`):** Outbound HTTP POST dispatch for server-to-server integrations, notifying external partners or triggering third-party webhooks upon mutation ratification or command completion.

---

## 5. Technical Modernization & Component Refactor

The initial prototype proved the viability of Pingora and Lua ratification. The next generation of SpectraGQL requires modernizing key components:

1. **AST Parser Upgrade:**
   - *Current:* `graphql-query` (designed for client-side AST generation).
   - *Target:* **`apollo-parser`** or **`async-graphql-parser`**. These provide resilient, zero-copy, high-speed parsing that extracts operation types, operation names, variable definitions, and directives without allocations.
2. **Response Ratification Lifecycle:**
   - Fully wire `ratify_response` into Pingora's `response_body_filter` and `logging` hooks to allow downstream responses to dynamically trigger compensation events or audit logs.
3. **Pluggable Dispatch Trait:**
   - Decouple NATS-specific logic from the core pipeline into dynamic, configurable adapter modules (`NatsAdapter`, `IggyAdapter`, `KafkaAdapter`, `SierraAdapter`).
4. **Configuration Cleanliness:**
   - Consolidate per-service configurations into a unified schema supporting per-route dispatch targets, operational modes (Mode A vs Mode B), and ratification rules.

---

---

## 6. The Developer Appliance Strategy

To eliminate friction and demolish the "DevOps tax" perception often associated with event-driven architectures, SpectraGQL provides pre-packaged, single-container **Developer Appliances**. 

Rather than requiring developers to configure an external distributed messaging cluster, each appliance packages the compiled SpectraGQL Pingora reverse proxy alongside an embedded, lightweight broker.

### Appliance Matrix & The Kafka-Compatible Developer Experience (DX)

A crucial insight for developer adoption is that **the Kafka wire protocol is ubiquitous**. Every enterprise language ecosystem already has production-ready Kafka client libraries (`librdkafka`, `kafkajs`, `confluent-kafka-python`, `kafka-go`). Supporting the Kafka protocol inside an appliance allows developers to consume SpectraGQL mutation events using tools they already know:

| Appliance Image | Embedded Engine | Protocol / Focus | Best For |
| :--- | :--- | :--- | :--- |
| **`spectragql/appliance:nats`** | **NATS JetStream** | NATS protocol | **The Swiss Army Knife:** Single static Go binary (<30MB), ultra-low memory, pub/sub, KV, and multi-language client SDKs. |
| **`spectragql/appliance:nisshi`** | **Nisshi (Rust Kafka)** | Kafka wire protocol | **Lightweight Kafka DX:** Pure-Rust Kafka-compatible broker backed by SQLite or in-memory storage. Zero JVM, instant startup. |
| **`spectragql/appliance:redpanda`** | **Redpanda (C++)** | Kafka wire protocol | **Enterprise Kafka Parity:** Production-grade Kafka API compatibility in C++ Seastar dev-container mode. |
| **`spectragql/appliance:iggy`** | **Apache Iggy** | Iggy binary / TCP / QUIC | **Pure Rust Speed:** Byte-level streaming engine built for high-throughput NVMe drives, cache locality, and sub-millisecond tail latencies. |
| **`spectragql/appliance:sierradb`** | **SierraDB** | SierraDB event stream | **The Event-Store Specialist:** Immutable event sourcing, built-in aggregate reconstruction, temporal projections, and database queries. |

> **Emerging & Experimental Log Engines Evaluated:**
> - **Walrus (`nubskr/walrus`):** A high-throughput, io_uring-based streaming log written in Rust. Features segment-based sharding and metadata-only Raft (>1M writes/sec). High potential as an embedded storage core for extreme NVMe performance.
> - **Jocko (`travisjeffery/jocko`):** An early pure-Go Kafka implementation. While historically interesting as a pioneer in eliminating the JVM for Kafka, it is largely unmaintained and superseded by modern engines like Redpanda and Nisshi.

```bash
# Instant local gateway with embedded NATS JetStream
docker run -p 8000:8000 -p 4222:4222 spectragql/appliance:nats --upstream http://localhost:4000

# Instant local gateway speaking the Kafka wire protocol via Nisshi
docker run -p 8000:8000 -p 9092:9092 spectragql/appliance:nisshi --upstream http://localhost:4000
```

---

## 7. Downstream Ecosystem: Consuming SpectraGQL Events

A write-path gateway is only as valuable as the downstream systems that consume its events. Once SpectraGQL ratifies and dispatches GraphQL mutations, what does a real-world downstream consumer architecture look like?

Downstream consumers typically fall into four distinct architectural archetypes:

```
                               ┌────────────────────────────────────────────────────────┐
                               │               SpectraGQL Mode A Proxy                  │
                               └──────────────────────────┬─────────────────────────────┘
                                                          │
                                         Dispatches Completed Mutation Event
                                                          │
                                                          ▼
                                ┌──────────────────────────────────────────────────┐
                                │      Event Backbone (NATS / Kafka / Iggy)        │
                                └──┬──────────────┬───────────────┬──────────────┬─┘
                                   │              │               │              │
                   ┌───────────────┘              │               │              └───────────────┐
                   ▼                              ▼               ▼                              ▼
    ┌───────────────────────────┐  ┌───────────────────────────┐  ┌───────────────────────────┐  ┌───────────────────────────┐
    │     Stream Processing     │  │     Complex Event (CEP)   │  │   Saga & Workflow Engine  │  │   Search & Projection Sync│
    │          (Arroyo)         │  │         (ArkFlow)         │  │     (Temporal / Inngest)  │  │   (Meilisearch / Redis)   │
    │  - Real-time SQL on stream│  │  - Tokio async pipeline   │  │  - Multi-day workflows    │  │  - Zero-lag search index   │
    │  - Materialized views     │  │  - Inline AI inference    │  │  - Payment retries        │  │  - Normalized read cache   │
    │  - Rolling aggregations   │  │  - Anomaly detection      │  │  - Compensation sagas     │  │  - Cache invalidation      │
    └───────────────────────────┘  └───────────────────────────┘  └───────────────────────────┘  └───────────────────────────┘
```

### 1. Stateful Stream Processing & Real-Time Projections: Arroyo (`ArroyoSystems/arroyo`)
* **What it is:** A distributed stream processing engine written in **Rust** (often described as "Apache Flink rewritten in modern Rust").
* **How it pairs with SpectraGQL:**
  * Arroyo consumes the raw GraphQL mutation stream from Kafka or NATS.
  * It executes continuous, stateful **Streaming SQL** over tumbling/sliding windows (e.g. `SELECT tenant_id, COUNT(*), SUM(amount) FROM mutations GROUP BY TUMBLE(interval '1 minute')`).
  * **Completing the CQRS Loop:** Arroyo continuously outputs **Materialized Views** into Redis, PostgreSQL, or ClickHouse. When frontend clients send GraphQL `Query` operations, they read from Arroyo's pre-computed, sub-millisecond materialized read models!
  * Arroyo can also function as a direct dispatch target via HTTP/SSE ingestion.

### 2. Complex Event Processing (CEP) & Real-Time AI Inference: ArkFlow (`arkflow-rs/arkflow`)
* **What it is:** A high-performance stream processing and complex event processing engine written in **Rust** on Tokio (enlisted in the CNCF Cloud Native Landscape).
* **How it pairs with SpectraGQL:**
  * ArkFlow is designed for low-latency rule evaluation, anomaly detection, and AI/ML model inference.
  * When a mutation arrives (e.g., `createComment` or `submitTransaction`), ArkFlow intercepts the event stream, runs inline Python UDFs or ONNX AI models for content moderation, fraud classification, or spam filtering, and emits enriched domain events or triggers automated policy alerts without burdening the primary GraphQL backend.

### 3. Saga & Long-Running Workflow Orchestration (Temporal, Hatchet, Inngest)
* For operations that require distributed coordination across third-party APIs (Stripe charges, shipping provider label generation, multi-day KYC verification), a dedicated workflow worker consumes the mutation event from the broker and orchestrates durable, retryable sagas with automated compensation logic.

### 4. Read Projection & Search Synchronization (Meilisearch, TypeSense, Redis)
* Mutation events stream directly to search index workers. When a `updateProductCatalog` mutation succeeds, the Meilisearch or Elasticsearch index is updated within milliseconds, completely eliminating the need for periodic full-database re-indexing sweeps.

---

## 8. Objective Analysis: Skeptic Objections & Realistic Scenario Suitability

A rigorous product strategy requires acknowledging where a technology fits and where it should **not** be chosen.

### Re-evaluating the Skeptic Objections:

1. **"Isn't this overkill for a single monolith with an ACID database?"**
   * **Verdict: Yes.** If an organization has a single monolithic backend (Rails, Django, Spring Boot) writing to PostgreSQL within a single `BEGIN ... COMMIT` transaction, mutations work natively with zero distributed systems headaches. SpectraGQL is pure overhead here.
2. **"What about organizations that believe 'Mutations are an anti-pattern'?"**
   * **Verdict: Not our target audience.** Teams adopting tRPC, Next.js Server Actions, or raw REST/RPC POST endpoints have already abandoned GraphQL for writes. SpectraGQL is designed specifically to protect GraphQL's typed developer experience and client cache normalization.
3. **"What about Apollo Federation v2 / WunderGraph Cosmo users?"**
   * **Verdict: Boundary condition.** In a federated graph, mutations often return nested fields across multiple subgraphs (`order { user { loyaltyTier } }`), which require Apollo Router's distributed query planner. SpectraGQL does not replace Apollo Router's federated query planner; it can sit behind or beside it, but cannot be a full drop-in replacement for complex federated schemas today.
4. **"Public / Partner APIs (Shopify / GitHub model)?"**
   * **Verdict: Mode A works; Mode B does not.** Public API developers demand strict synchronous GraphQL spec compliance. You cannot return an async `202 Accepted` receipt to third-party developers. Mode A works transparently for internal side-effects, but Mode B is unusable for public schemas.
5. **"Why not use Database-Level CDC (Debezium / Postgres WAL) instead of a Gateway Outbox?"**
   * **Verdict: The 99% practical compromise.** While DB-level CDC guarantees atomic commits at the storage layer, setting up Debezium, Kafka Connect, and schema registries across 20 legacy microservices requires 6–18 months of platform team effort. SpectraGQL's Mode A provides a **Gateway Outbox** that achieves 99.9% of the practical decoupling in 10 minutes with zero database migrations.
6. **"The DevOps Tax / Kafka PTSD?"**
   * **Verdict: Solved by Pingora + NATS/Iggy/Nisshi appliances.** Event-driven architectures no longer require multi-gigabyte JVM heaps, ZooKeeper clusters, or dedicated SREs.
7. **"Synchronous Resolver Fan-out vs. Selection Set Resolution?"**
   * **Verdict: Solved by Event Choreography.** In Mode A, the primary resolver executes only its core write and returns the entity immediately. Downstream systems consume the event choreographically, isolating faults and eliminating p99 latency compounding.

---

### Scenario Suitability Matrix

| Architectural Scenario | SpectraGQL Fit | Key Rationale |
| :--- | :--- | :--- |
| **Microservices with GraphQL API (3+ services)** | **Flagship (High)** | Solves synchronous resolver fan-out; gives new services instant event streams via Mode A with zero frontend changes. |
| **High-Volume Ingestion / Long-Running Sagas** | **Ideal (High)** | Mode B provides true async CQRS, edge ratification, and broker queue buffering for telemetry, IoT, and heavy batch jobs. |
| **Rapid Prototyping / Local Dev** | **High** | Dev Appliances (`:nats`, `:nisshi`, `:redpanda`, `:iggy`, `:sierradb`) spin up a complete CQRS gateway in one command. |
| **Single Monolith + Single Relational DB** | **Zero (Overkill)** | ACID transactions already solve the write path natively. |
| **Full Apollo Federation v2 Subgraph Mesh** | **Low / Niche** | Requires federated entity resolution and distributed query planning across subgraphs. |
| **Public / Partner GraphQL APIs** | **Moderate (Mode A only)** | Third-party clients require synchronous spec contracts. Mode A works for internal side-effects; Mode B cannot be used. |
| **Pure Read-Heavy Content Platforms (99% Reads)** | **Low** | CDN caching (Fastly/Stellate) handles queries; writes are too infrequent to warrant write-path infrastructure. |

---

## 9. Strategic Roadmap & Milestones

```
Milestone 1: Core Engine Modernization
├── Upgrade AST parser (apollo-parser)
├── Refactor CompositeServiceProxy pipeline in Pingora
└── Centralize typed configuration

Milestone 2: Pluggable Dispatch Architecture
├── Abstract DispatchAdapter trait
├── Implement Apache Iggy native Rust adapter
├── Implement SierraDB native event-store adapter
├── Implement Arroyo stream-processing integration
└── Maintain NATS JetStream & Kafka adapters

Milestone 3: Ratification & Governance Layer
├── Implement Idempotency filter (in-flight stash & hash tracking)
├── Schema drift & deprecation detector
└── WASM plugin runtime (Extism / Wasmtime) alongside Lua

Milestone 4: Operational Modes & Dispatch Policies
├── Mode A: Workhorse Gateway with configurable dispatch policies:
│   ├── Policy 1: response_only (atomic event on 2xx, zero phantom writes)
│   ├── Policy 2: response_with_failure (stashed request emitted on timeout/drop)
│   └── Policy 3: raw_audit (ingress request + egress response stream)
├── Mode B: Pure CQRS Async Command Receipts
└── Archive Mode C ("The Mirage")

Milestone 5: Developer Appliances & Ecosystem
├── Docker appliance builds:
│   ├── spectragql/appliance:nats (NATS JetStream)
│   ├── spectragql/appliance:nisshi (Rust-native Kafka protocol + SQLite)
│   ├── spectragql/appliance:redpanda (Enterprise Kafka protocol)
│   ├── spectragql/appliance:iggy (Apache Iggy pure-Rust)
│   └── spectragql/appliance:sierradb (SierraDB event-sourcing)
├── Beast GUI (Tauri / Svelte dashboard for observing mutation streams)
└── End-to-end integration test harness with mock brokers
```

---

## 10. Conclusion

SpectraGQL does not need to compete with Apollo or Cosmo on complex federated query planning. 

Its true wedge is **solving the Write side of GraphQL**: transforming GraphQL mutations into a robust, high-performance, event-sourced CQRS command engine. By focusing on **Mode A for pragmatic microservice event choreography** and **Mode B for pure asynchronous CQRS**, paired with modern lightweight engines like NATS JetStream, Apache Iggy, and SierraDB, SpectraGQL provides the missing architectural backbone that enterprise GraphQL has needed for years.
