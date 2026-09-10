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

To bridge the gap between legacy systems and this event-driven future, SpectraGQL supports three distinct operational modes (for a deep-dive sequence analysis and configuration examples, see [Modes of Operation](modes-of-operation.md)):

### Mode A: Edge Outbox / Dual Dispatch (The Adoption Wedge)
- **Mechanism:** When a mutation arrives, SpectraGQL dispatches the validated command to the event stream *and* forwards the request upstream to the existing backend API. On the response path, SpectraGQL captures the backend's response and publishes a completion event.
- **Client Experience:** 100% transparent. The client sends a traditional GraphQL mutation and receives the exact synchronous response it expects.
- **Value Proposition:** **Zero code changes required.** An enterprise can place SpectraGQL in front of an existing monolithic GraphQL server tomorrow. Every mutation immediately becomes an auditable, replayable stream on NATS or Kafka, allowing new teams to build decoupled event-driven services without touching the legacy codebase.

### Mode B: Pure CQRS / Asynchronous Command (The Architectural Destination)
- **Mechanism:** SpectraGQL intercepts the mutation, validates it, and publishes it directly to the event backbone. It terminates the HTTP request immediately, returning a deterministic **Command Receipt**:
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
- **Value Proposition:** True CQRS. Backends can absorb massive spikes in write volume without crashing, and complex workflows are processed via sagas and outboxes.

### Mode C: Coordinated Request-Reply (The Synchronous Bridge)
- **Mechanism:** SpectraGQL publishes the mutation command to an event topic with a unique `reply_to` inbox subject (e.g., NATS request-reply pattern). An event-sourced consumer processes the event and publishes the computed response back to the reply subject. SpectraGQL awaits the reply within a configurable SLA timeout and returns the synthesized response to the client.
- **Client Experience:** Synchronous GraphQL response.
- **Value Proposition:** Allows backend teams to rewrite their mutation resolvers as decoupled event consumers while preserving existing synchronous frontend expectations.

> **Hybrid Routing Strategy:** SpectraGQL enables per-route or per-operation configuration. High-value, critical mutations (e.g., `checkout`, `transferFunds`) can run in Mode B or Mode C, while legacy CRUD mutations continue running in Mode A.

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

## 6. Strategic Roadmap & Milestones

```
Milestone 1: Core Engine Modernization
├── Upgrade AST parser (apollo-parser)
├── Refactor CompositeServiceProxy pipeline in Pingora
└── Centralize typed configuration

Milestone 2: Pluggable Dispatch Architecture
├── Abstract DispatchAdapter trait
├── Implement Apache Iggy native Rust adapter
├── Implement SierraDB native event-store adapter
└── Maintain NATS JetStream & Kafka adapters

Milestone 3: Ratification & Governance Layer
├── Implement Idempotency filter (key & hash tracking)
├── Schema drift & deprecation detector
└── WASM plugin runtime (Extism / Wasmtime) alongside Lua

Milestone 4: Operational Modes (Adoption Wedge)
├── Mode A: Transparent Dual Dispatch & Response Capture
├── Mode B: Pure CQRS Async Command Receipts
└── Mode C: NATS Request-Reply synchronous bridge

Milestone 5: Developer Experience & Ecosystem
├── Beast GUI (Tauri / Svelte dashboard for observing mutation streams)
├── CLI tooling for schema diffing and route verification
└── End-to-end integration test harness with mock brokers
```

---

## 7. Conclusion

SpectraGQL does not need to compete with Apollo or Cosmo on complex schema stitching or federated query planning. 

Its true wedge is **solving the Write side of GraphQL**: transforming GraphQL mutations into a robust, high-performance, event-sourced CQRS command engine. By leveraging Cloudflare Pingora and modern streaming backends like Apache Iggy, SierraDB, and NATS, SpectraGQL provides the missing architectural backbone that enterprise GraphQL has needed for years.
