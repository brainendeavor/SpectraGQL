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
               │ Legacy API / Backend │             │ Edge Guards:      │
               │ Query Service / Read │             │ - Syntax & Depth  │
               │ Cache / Replicas     │             │ - Idempotency Lock│
               │                      │             │ - Type-State PII  │
               └──────────────────────┘             └─────────┬─────────┘
                                                              │
                                     ┌────────────────────────┴────────────────────────┐
                                     │                                                 │
                                     ▼                                                 ▼
                         [Mode A: Sync Forwarding]                         [Mode B: Edge Command]
                         - Commit to Primary Backend                       - Immediate Command Receipt
                         - Post-Response Event Dispatch                      {"status": "ACCEPTED"}
                                     │                                                 │
                                     └────────────────────────┬────────────────────────┘
                                                              │ Atomic EventSink
                                                              ▼
                                                    ┌───────────────────┐
                                                    │ Event Backbone    │
                                                    │ NATS / Kafka /    │
                                                    │ Redis / Iggy /    │
                                                    │ SierraDB / Rabbit │
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
| **Hive Router** (Rust) | Open-source Apollo Federation v2 router | Subgraph HTTP routing | Purpose-built for high-speed federated query planning. Treats mutations as simple HTTP forwards. Perfect complementary read-path partner. |
| **Apollo Router** (Rust) | Apollo Federation v2 query planner | Subgraph HTTP routing | Commercial ELv2 license tied to GraphOS. Forwards mutation HTTP calls without CQRS or outbox mechanics. |
| **WunderGraph Cosmo** (Go) | Open-source GraphQL Federation | Directives (`@edfs__natsPublish`, `@edfs__kafkaPublish`) | Tied to full Federation schema composition and Cosmo control plane. Cannot act as a standalone L7 proxy for non-federated setups. |
| **Hive Gateway** (JS/Rust) | Schema Stitching & multi-protocol gateway | Subservice HTTP routing | Excels at stitching independent GraphQL, REST, and gRPC services together without Federation subgraphs. Pure read/aggregation focus. |
| **Tailcall / Grafbase** | *Pivoted / Archived* | N/A | Tailcall pivoted to ForgeCode (AI coding agent); Grafbase Gateway entered maintenance mode for archival by May 2026. |
| **Envoy / Kong / Traefik** | General Layer 7 Gateways | Blind HTTP proxying | Lack GraphQL semantic AST awareness to bifurcate Queries and Mutations into different backend topologies. |
| **Stellate** | GraphQL CDN | Edge caching for queries | Completely bypasses mutations. |

### The Unoccupied White Space
No tool exists that acts as a **transparent, drop-in CQRS proxy** that:
- Runs at native wire speed in Rust (via Pingora).
- Focuses exclusively on the **Write Path**, eliminating synchronous fan-out and driving event choreography.
- Pairs seamlessly with existing query routers on the read path.

---

### Upstream Read-Path Partners: Pairing SpectraGQL with Query Engines

Because SpectraGQL bifurcates traffic at Layer 7 based on the GraphQL AST, it does not attempt to reinvent distributed query planning. Instead, it delegates GraphQL `Query` operations to specialized upstream query partners:

```
                                      Client
                                         │
                                         ▼
                             ┌───────────────────────┐
                             │   SpectraGQL Proxy    │
                             │ (Pingora L7 in Rust)  │
                             └───────────┬───────────┘
                                         │
                        ┌────────────────┴────────────────┐
                        ▼ (Query / Read)                  ▼ (Mutation / Write)
             ┌────────────────────────┐        ┌─────────────────────────────┐
             │   Upstream Query Hop   │        │   SpectraGQL Write Engine   │
             │                        │        │                             │
             │  • Hive Router (Rust)  │        │   • Mode A: Pragmatic Proxy │
             │    (if Federated)      │        │     (Choreography on 2xx)   │
             │           OR           │        │   • Mode B: Pure CQRS       │
             │  • Hive Gateway        │        │     (202 Command Receipt)   │
             │    (if Not Federated)  │        │                             │
             │           OR           │        └──────────────┬──────────────┘
             │  • Cosmo / Apollo      │                       │
             └──────────┬─────────────┘                       │
                        │                                     │
                        ▼                                     ▼
              [ Microservice Reads ]                  [ Event Broker ]
              (Direct / Stitched)                  (NATS / Iggy / Kafka)
```

#### Topology 1: The Pure-Rust Federated Stack (SpectraGQL + Hive Router)
* **Best For:** Microservices implementing Apollo Federation v2 where the organization demands an uncompromised, 100% Rust architecture.
* **Flow:**
  * `Query` $\rightarrow$ SpectraGQL proxies directly to **Hive Router** (`:4000`), which plans the query and fans out across federated subgraphs.
  * `Mutation` $\rightarrow$ SpectraGQL ratifies at the edge and dispatches to **NATS JetStream**, **Apache Iggy**, or **Nisshi** (Mode A or Mode B).
* **Configuration:**
  ```toml
  # spectra.toml
  [upstream]
  addr = "127.0.0.1:4000" # Hive Router
  name = "hive_federation_router"

  [gql]
  paths = "/graphql"
  ops_to_dispatch = "mutation" # Queries pass through to Hive Router
  ```

#### Topology 2: The Open Federation Stack (SpectraGQL + WunderGraph Cosmo / Apollo Router)
* **Best For:** Teams already invested in Apollo Federation or WunderGraph Cosmo for schema composition and entity resolution.
* **Flow:**
  * SpectraGQL sits at the ingress perimeter.
  * Read queries pass transparently to **Cosmo Router** or **Apollo Router**.
  * Write mutations are intercepted by SpectraGQL, stamped with HLC timestamps and idempotency keys, and fed into Kafka or NATS.

#### Topology 3: The Non-Federated Stitching Stack (SpectraGQL + Hive Gateway)
* **Best For:** Teams whose microservices are **plain REST APIs, gRPC services, or independent GraphQL backends** without Apollo Federation subgraphs.
* **Flow:**
  * **Hive Gateway** handles **Schema Stitching**—declaratively mapping REST and GraphQL endpoints into a unified query schema.
  * SpectraGQL routes `Query` operations to Hive Gateway, while converting `Mutation` operations into clean event streams for downstream microservices.

---

## 3. Core Architectural Concepts & Operating Modes

### The Paradigm Shift: Resolvers as Event Consumers
In traditional GraphQL architectures, mutation resolvers are synchronous workers that handle validation, database transactions, third-party API calls, and notification triggers in a single thread. If any step times out or fails, the system enters an ambiguous state.

In the SpectraGQL paradigm:
- The proxy ratifies and ingests the mutation as a **Command**.
- The resolver is re-architected as an **Event Consumer / Saga Worker** that subscribes to the command topic on the event backbone.
- Projections update read models asynchronously.

To bridge the gap between legacy systems and this event-driven future, SpectraGQL supports two core operational modes (for a deep-dive sequence analysis and configuration examples, see [Modes of Operation](modes-of-operation.md)):

### Mode A: The Workhorse Gateway (`ExecutionStrategy::SyncUpstreamExecution` — Flagship)
- **Mechanism:** SpectraGQL intercepts the mutation, forwards the HTTP request to the primary backend service for synchronous local execution, and—upon receiving an HTTP 2xx response—dispatches the completed domain event (`CompletionEvent` with stashed request arguments + response data) to the event backbone.
- **Client Experience:** 100% transparent. The client sends a traditional GraphQL mutation, receives its expected synchronous response, and enjoys automatic Apollo/Relay cache normalization.
- **Value Proposition:** **Zero code changes required.** Allows core resolvers to do one fast write and eliminates synchronous cross-service fan-out. Downstream subsystems (loyalty, notifications, search indexing) react choreographically off NATS/Iggy/SierraDB.
- **Dispatch Policies:** Supports `response_only` (default: 1 event per success, zero phantom writes), `response_with_failure` (emits stashed request on timeout/error), and `raw_audit` (dual ingress/egress).

### Mode B: The Event-Native Gateway (`ExecutionStrategy::AsyncEdgeCommand` — Pure Asynchronous CQRS)
- **Mechanism:** SpectraGQL intercepts the mutation, executes guards, and publishes it directly to the event backbone. It terminates the HTTP request immediately at the edge, returning a deterministic **Command Receipt**:
  ```json
  {
    "data": {
      "submitOrder": {
        "commandId": "0191b2c4-8840-7ac3-8a02-0e9f1a0e882a",
        "hlc": "1789151435592.000001",
        "status": "ACCEPTED"
      }
    }
  }
  ```
- **Error Handling:** If broker dispatch fails, SpectraGQL returns `"status": "DISPATCH_FAILED"`, attaches header `x-spectra-dispatch: failed`, and immediately evicts the in-flight idempotency key so the client can retry.
- **Client Experience:** The client is designed for eventual consistency. State updates are observed via GraphQL Subscriptions (streaming from the event bus), WebSockets, or background polling.
- **Value Proposition:** True CQRS for high-throughput ingestion (IoT, gaming, financial order streams) and long-running sagas (bulk imports, media encoding).

### Mode C: "The Mirage" (Evaluated & Retired — "Not Today")
- **Status:** Officially archived. Mode C attempted to use NATS request-reply to hold synchronous HTTP connections while event workers processed in the background. It created an uncanny valley: retaining all the timeout vulnerabilities of synchronous HTTP while incurring the full operational weight of an event broker. Mode A is strictly superior for synchronous clients.

> **Hybrid Routing Strategy:** SpectraGQL enables per-route configuration via `ExecutionStrategy`. 95% of mutations run in **Mode A**, while specialized high-volume or long-running operations run in **Mode B**.

---

## 4. The Request Pipeline & Engine Components

Built on Cloudflare's **Pingora** framework, SpectraGQL decomposes the Layer 7 proxy into a composable, sequential filter pipeline:

```
Session Ingest (Pingora)
       │
       ▼
[ 1. HealthFilter ]      ──(hit /healthz, /livez)──► Return 200 OK
       │ (continue)
[ 2. AdminFilter ]       ──(hit /admin)────────────► Return SPA / JSON API (CIDR Allowlist)
       │ (continue)
[ 3. WebSocketFilter ]   ──(Upgrade: websocket)────► graphql-ws Duplex Pump
       │ (continue)
[ 4. RequestGuardFilter] ──(malformed / toxic)─────► Reject 400 Bad Request
       │ (sanitized & verified)
[ 5. IdempotencyFilter ] ──(in-flight conflict)────► Reject 409 Conflict
       │                 ──(cached replay)─────────► Return Cached Replay (0 Upstream Hops)
       │ (new key)
[ 6. StrategyRouter ]
       ├── ExecutionStrategy::AsyncEdgeCommand  ──► Commit to EventSink & Return Command Receipt
       └── ExecutionStrategy::SyncUpstreamExecution:
                │
                ▼
           [ Proxy to Upstream Microservice ]
                │
                ▼
           [ 7. ResponseGuardFilter ]  ──(PII leak / token)──► Reject 500
                │
                ▼
           [ 8. TelemetryDispatcher ]  ──(logging phase)────► Publish CompletionEvent to EventSink
                │
                ▼
           [ Complete Idempotency Cache & Return to Client ]
```

### Domain Guards & Type-State Safety

1. **Inbound RequestGuards (`RequestGuard`):**
   - AST validation, depth & complexity checks, and required header validation before hitting upstreams.
2. **Outbound ResponseGuards (`ResponseGuard`):**
   - Body inspection preventing accidental PII leakage or sensitive token exposure to consumers.
3. **Pluggable Rule Evaluator (`RuleEvaluator`):**
   - Extensible policy enforcement port (`NativeRuleEvaluator` built-in; ready for WASM/Lua sandbox modules).
4. **Compile-Time Type-State Security:**
   - Tracks data safety through `RawPayload<T>` and `SanitizedPayload<T>`.
   - `CompletionEvent.request` and event sinks accept **only** `SanitizedPayload<RequestInfo>`, guaranteeing that un-redacted credentials and tokens can never leak into Kafka, NATS, or event logs.
5. **Idempotency Enforcement (`IdempotencyFilter`):**
   - Evaluates incoming `Idempotency-Key` headers or hashes `(client_id, operation_name, variables)`.
   - Rejects concurrent duplicate mutations with standard `409 Conflict` GraphQL errors, and replays completed mutations from cache with zero upstream calls.

### Decoupled EventSink & Encoder Architecture

To adhere to the Interface Segregation Principle (ISP) and prevent transport/serialization coupling, message brokers implement the atomic `EventSink` port:

```rust
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Publishes raw bytes to the destination topic / subject / stream.
    async fn publish(&self, topic: &str, payload: &[u8]) -> pingora::Result<()>;
}

pub trait EventEncoder: Send + Sync {
    fn encode_request(&self, request: &RequestInfo) -> pingora::Result<Vec<u8>>;
    fn encode_completion(&self, completion: &CompletionEvent) -> pingora::Result<Vec<u8>>;
    fn encode_sanitized_request(&self, request: &SanitizedPayload<RequestInfo>) -> pingora::Result<Vec<u8>>;
}
```

#### Supported Broker Adapters:
1. **NATS JetStream:** Cloud-native streaming and pub/sub with built-in persistence.
2. **Apache Kafka / Redpanda:** Enterprise standard event streaming platform.
3. **Redis Streams / Dragonfly / Valkey:** Lightweight in-memory streaming with persistent consumer groups.
4. **RabbitMQ:** AMQP message broker integration.
5. **Apache Iggy:** Pure-Rust, cache-friendly streaming broker delivering sub-millisecond tail latencies.
6. **SierraDB:** Native event-sourcing database engineered specifically for immutable event streams.
7. **Webhook Dispatch (`WebhookDispatch`):** Outbound HTTP POST dispatch for server-to-server integrations.

---

## 5. Completed Architectural Modernization

The codebase has undergone a complete architectural modernization:

1. **Core Library Target (`src/lib.rs`):**
   - Extracted reusable library target exporting engine components and composable filters. Converted `main.rs` to a thin binary bootstrap.
2. **God Object Decomposition (`src/proxy/filters/`):**
   - Deconstructed the 1,038-line `CompositeServiceProxy` into single-responsibility pipeline filters (`HealthFilter`, `AdminFilter`, `IdempotencyFilter`, `StrategyRouter`, `TelemetryDispatcher`).
3. **Guard Contracts & Rule Port (`src/guards/`):**
   - Implemented `RequestGuard`, `ResponseGuard`, and `RuleEvaluator` trait ports with zero-overhead native rule execution.
4. **Compile-Time Type-State Security (`src/payload/typestate.rs`):**
   - Enforced `RawPayload<T>` to `SanitizedPayload<T>` type transitions. Eliminated PII risk in Mode B edge dispatch.
5. **Strongly Typed Serde Error Envelopes (`src/payload/graphql_error.rs`):**
   - Replaced all string-interpolated JSON format strings with strongly-typed `GraphQLErrorResponse` and `CommandReceipt` models.
6. **Automated Integration Test Matrix:**
   - Expanded test suite to **111 tests** across 9 dedicated test files under `tests/` with 100% pass rate and 0 warnings.

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
