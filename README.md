# SpectraGQL

<p align="center">
  <img src="assets/spectragql_logo.svg" alt="SpectraGQL Logo" width="220" />
</p>

<p align="center">
  <strong>The Write-Path Gateway for GraphQL</strong><br>
  <em>Transforming GraphQL mutations into high-performance event streams at wire speed.</em>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT" /></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-2024%20edition-orange.svg" alt="Rust Edition" /></a>
  <a href="https://github.com/cloudflare/pingora"><img src="https://img.shields.io/badge/engine-Cloudflare%20Pingora-black.svg" alt="Pingora" /></a>
  <a href="https://spectragql.dev"><img src="https://img.shields.io/badge/website-spectragql.dev-00F0FF" alt="Website" /></a>
  <img src="https://img.shields.io/badge/tests-111%20passed-brightgreen.svg" alt="Test Status" />
</p>

---

## The Core Thesis

GraphQL gives frontend teams expressive control over data retrieval. However, in enterprise microservice architectures, its write path (**Mutations**) frequently devolves into an architectural liability:

- **The Synchronous Fan-Out Anti-Pattern:** A single mutation resolver often coordinates multiple downstream microservices or databases synchronously. If an intermediate step fails, state is left corrupted with no native rollback, distributed saga coordination, or audit trail.
- **Federation's Blind Spot:** Traditional gateways (Apollo Federation, Cosmo, Hive) focus almost exclusively on distributed query planning and schema composition. They treat mutations as an afterthought, simply proxying HTTP POST requests.

**SpectraGQL** treats GraphQL as the natural boundary for **Command Query Responsibility Segregation (CQRS)**:
- `Query` operations are explicitly **Reads** (idempotent, cacheable, fan-out friendly).
- `Mutation` operations are explicitly **Commands** (intents to alter state, subject to edge validation, idempotency controls, and dispatched to an immutable event log).
- `Subscription` operations are explicitly **Realtime Streams** (reverse event queues terminated at the edge via WebSockets without backend connection storms).

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

---

## Execution Strategies

SpectraGQL bridges existing microservices and event-driven backends through two primary operational modes configured per operation via `ExecutionStrategy`:

| Strategy | Upstream Hop | Event Role | Client Requirement | Primary Role |
| :--- | :---: | :--- | :--- | :--- |
| **`SyncUpstreamExecution`**<br>*(Mode A: The Workhorse)* | **Yes**<br>(Forward to backend) | Event Choreography Stream (`CompletionEvent`) | Standard GraphQL Client (Sync)<br>Full Apollo/Relay cache normalization | **Flagship (90% of workloads)**<br>Zero frontend changes; reliable post-response audit events; decouples secondary downstream services. |
| **`AsyncEdgeCommand`**<br>*(Mode B: Edge Command)* | **No**<br>(Edge broker commit) | Primary Command Bus | Async-Aware Client<br>Expects deterministic Command Receipt (`ACCEPTED`) | **Specialized / High-Scale**<br>Pure CQRS for bulk ingestion, IoT, mobile offline mutations, and long-running saga workflows. |
| *`Mode C: The Mirage`* | *Retired* | *Coordinated Request-Reply* | *Standard Client* | *Evaluated and archived in favor of Mode A simplicity and stability.* |

---

## Architectural Highlights

### 1. Composable Filter Pipeline
The monolithic proxy architecture has been decomposed into sequential, single-responsibility pipeline filters under `src/proxy/filters/`:
- **`HealthFilter`**: Instant zero-allocation response for `/healthz` and `/livez` liveness probes.
- **`AdminFilter`**: CIDR-secured administration dashboard, dynamic metrics, and schema inspector.
- **`IdempotencyFilter`**: Prevents duplicate mutation execution via explicit `Idempotency-Key` headers or automatic SHA-256 fingerprinting. In-flight duplicates receive standard `409 Conflict` GraphQL errors, while completed operations replay from cache with zero upstream calls.
- **`StrategyRouter`**: Operation-level routing bifurcating traffic between Mode A upstream forwarding and Mode B edge command receipts.
- **`TelemetryDispatcher`**: Asynchronous logging phase telemetry publication to configured event broker sinks.

### 2. Guard Contracts & Extensible Rule Evaluation
Located in `src/guards/`, domain guards enforce validation before and after processing:
- **`RequestGuard`**: Validates inbound GraphQL syntax, operation structure, and required headers prior to upstream forwarding.
- **`ResponseGuard`**: Inspects outbound response bodies to prevent PII leakage and sensitive token exposure.
- **`RuleEvaluator`**: Pluggable evaluation port for business-aligned criteria (`NativeRuleEvaluator` built-in, designed for WASM and Lua extensions).

### 3. Compile-Time Type-State Security
SpectraGQL enforces sensitive data redaction at compile time using Rust's type-state pattern:
- **`RawPayload<T>`** represents unverified input.
- **`SanitizedPayload<T>`** represents cryptographically and pattern-sanitized data safe for event streams.
- Telemetry `CompletionEvent` and broker sinks accept **only** `SanitizedPayload<RequestInfo>`, guaranteeing that passwords, tokens, API keys, and authorization headers can never leak onto Kafka, NATS, or other brokers.

### 4. Atomic EventSink Decoupling
Network transports and message brokers implement the atomic [`EventSink`](file:///Users/bmo/code/SpectraGQL/src/dispatch/sink.rs) port:
- Supported brokers: **NATS JetStream**, **Apache Kafka / Redpanda**, **Redis Streams**, **RabbitMQ**, **Apache Iggy**, **SierraDB**, and **Webhooks**.
- Serialization is completely decoupled via [`JsonEventEncoder`](file:///Users/bmo/code/SpectraGQL/src/dispatch/sink.rs).

### 5. Deterministic Causal Ordering
Every operation is tagged with:
- **UUIDv7**: Monotonically increasing time-ordered identifiers (`x-spectra-request-id`).
- **Hybrid Logical Clocks (HLC)**: Nanosecond-precision physical clock combined with a logical counter (`x-spectra-hlc`) to provide strict causal ordering across distributed gateways without distributed coordination.

---

## Core Documentation

- **[Philosophy & Core Thesis](docs/philosophy.md)**: Deconstructing the mutation debate, CQRS, and durable event sourcing across microservices.
- **[Architecture & Strategy](docs/architecture-and-strategy.md)**: Deep dive into the Pingora pipeline, filter architecture, and design patterns.
- **[Modes of Operation](docs/modes-of-operation.md)**: Sequence diagrams and failure policies for Mode A and Mode B.
- **[Subscriptions & Realtime](docs/subscriptions-and-realtime.md)**: Reverse event queues, WebSocket (`graphql-ws`) termination, and live updates.
- **[Local Appliance Setup](docs/dev-appliances.md)**: Quick-start Docker Compose environments for local NATS, Kafka, Redis, and Iggy brokers.

---

## Quickstart

### 1. Configuration (`spectra.toml`)

Create a `spectra.toml` configuration file:

```toml
bind_addr = "0.0.0.0:8000"

[upstream]
addr = "localhost:4000"
name = "default"

[gql]
paths = "/gql,/graphql"
ops_to_dispatch = "query, mutation, subscription"
dispatch.name = "gql"
dispatch.method = "NATS"
dispatch.addr = "127.0.0.1:4222"

# Named upstreams for microservice routing
[named_upstreams]
inventory = "127.0.0.1:5001"
crm = "127.0.0.1:5002"

# Operation-level route overrides
[[routes]]
operation = "createReview"
mode = "SyncUpstreamExecution" # Mode A: forward to upstream + publish CompletionEvent

[[routes]]
operation = "importCatalog"
mode = "AsyncEdgeCommand"      # Mode B: edge-terminated command receipt
receipt_status = "ACCEPTED"
```

### 2. Run the Gateway

```bash
# Run with Cargo
RUST_LOG=info cargo run -- --config spectra.toml

# Or run the release binary
./target/release/spectragql --config spectra.toml
```

### 3. Send Requests

#### Standard Query (Mode A: Forwarded to Upstream)
```bash
curl -X POST http://127.0.0.1:8000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query GetViewer { viewer { id name } }"}'
```

#### Idempotent Mutation (Mode A: Forwarded with Deduplication & Audit)
```bash
curl -X POST http://127.0.0.1:8000/graphql \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ord-create-42" \
  -d '{"query": "mutation CreateReview { createReview(id: \"rev-100\", rating: 5) { id rating } }"}'
```
*Re-sending this identical request returns the cached result with `x-spectra-idempotent-replay: true` and makes zero upstream calls.*

#### Edge Command Mutation (Mode B: Terminated at Edge)
```bash
curl -X POST http://127.0.0.1:8000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "mutation BulkImport { importCatalog(file: \"catalog.csv\") { commandId status hlc } }"}'
```
*Returns an immediate deterministic Command Receipt:*
```json
{
  "data": {
    "importCatalog": {
      "commandId": "0191b2c4-8840-7ac3-8a02-0e9f1a0e882a",
      "hlc": "1789151435592.000001",
      "status": "ACCEPTED"
    }
  }
}
```

---

## Automated Test Verification

SpectraGQL maintains a comprehensive, isolated test suite organized into dedicated files under `tests/`:

```bash
cargo test
```

```
test result: ok. 73 passed (lib unit tests)
test result: ok.  6 passed (tests/admin_security.rs)
test result: ok.  2 passed (tests/dispatch_failure.rs)
test result: ok.  1 passed (tests/e2e_gateway.rs)
test result: ok.  4 passed (tests/graphql_parser_edge_cases.rs)
test result: ok.  7 passed (tests/guard_pipeline.rs)
test result: ok.  3 passed (tests/idempotency_concurrency.rs)
test result: ok.  4 passed (tests/sanitizer_edge_cases.rs)
test result: ok.  5 passed (tests/subscription_protocol.rs)
test result: ok.  6 passed (tests/typestate_sanitization.rs)
Total: 111 passed; 0 failed; 0 ignored; 0 warnings
```

---

## License

This project is licensed under the [MIT License](LICENSE).
