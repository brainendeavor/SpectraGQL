# SpectraGQL

<p align="center">
  <img src="assets/spectragql_logo.svg" alt="SpectraGQL Logo" width="220" />
</p>

<p align="center">
  <strong>The CQRS Command Gateway for GraphQL</strong><br>
  <em>High-performance wire-speed reverse proxy built on Cloudflare Pingora in Rust</em>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT" /></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-2024%20edition-orange.svg" alt="Rust Edition" /></a>
  <a href="https://github.com/cloudflare/pingora"><img src="https://img.shields.io/badge/engine-Cloudflare%20Pingora-black.svg" alt="Pingora" /></a>
  <a href="https://spectragql.dev"><img src="https://img.shields.io/badge/website-spectragql.dev-00F0FF" alt="Website" /></a>
</p>

---

## The Core Thesis

GraphQL gives frontend teams expressive control over data retrieval. However, in enterprise environments, its write path (**Mutations**) frequently devolves into an architectural liability:

- **The Synchronous Fan-Out Anti-Pattern:** A single mutation resolver often coordinates multiple downstream microservices or databases synchronously. If an intermediate step fails, state is left corrupted with no native rollback, saga coordination, or audit trail.
- **Federation's Blind Spot:** Existing gateways (Apollo Federation, Cosmo, Hive) focus almost exclusively on distributed query planning and schema composition. They treat mutations as an afterthought, simply proxying HTTP POST requests.

**SpectraGQL** treats GraphQL as the natural boundary for **Command Query Responsibility Segregation (CQRS)**:
- `Query` operations are explicitly **Reads** (idempotent, cacheable, fan-out friendly).
- `Mutation` operations are explicitly **Commands** (intents to alter state, subject to edge validation and dispatched to an immutable event log).
- `Subscription` operations are explicitly **Realtime Streams** (reverse event queues terminated at the edge via WebSockets or SSE).

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

---

## Operational Modes

SpectraGQL bridges existing REST/GraphQL servers and modern event-driven backends across three distinct operational modes:

| Mode | Upstream Hop | Event Role | Client Requirement | Adoption Target |
| :--- | :--- | :--- | :--- | :--- |
| **Mode A: Edge Outbox** | **Yes** (Forward to backend) | Shadow / Audit Stream | Standard GraphQL Client (Sync) | **Brownfield / Legacy**<br>Zero code changes across frontend and backend |
| **Mode B: Pure CQRS** | **No** (Broker only) | Primary Command Bus | Async-Aware Client (`202 Accepted`) | **Greenfield / High-Scale**<br>Decoupled event workers |
| **Mode C: Coordinated Request-Reply** | **No** (Broker only) | Command Bus + Reply Inbox | Standard GraphQL Client (Sync) | **Modern Backend / Legacy Client**<br>Event resolvers with sync HTTP return |

---

## Core Features

- **Built on Cloudflare Pingora:** Zero-allocation network pipeline operating at wire speed in Rust.
- **AST Routing:** Parses GraphQL operations to automatically bifurcate queries and mutations.
- **Edge Ratification:** Idempotency key tracking, argument sanitization, and correlation headers (`x-spectra-request-id`, `x-spectra-hlc`).
- **Multi-Broker Dispatch:** NATS JetStream, Apache Iggy, Kafka, SierraDB, InfluxDB, GreptimeDB, and Webhooks.
- **Realtime Termination:** Edge WebSocket (`graphql-ws`) and SSE termination completing the CQRS loop without backend connection storms.

---

## Core Documentation

- **[Architecture & Strategy](docs/architecture-and-strategy.md)**: Deep dive into the Pingora pipeline, thesis, and roadmap.
- **[Modes of Operation](docs/modes-of-operation.md)**: Detailed sequence flows for Mode A, Mode B, and Mode C.
- **[Subscriptions & Realtime](docs/subscriptions-and-realtime.md)**: The reverse event queue architecture and connection termination.

---

## Quickstart

### 1. Configuration (`spectra.toml`)

Create a `spectra.toml` file in your working directory (or use environment variables with the `SPECTRA_` prefix):

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

[rest]
paths = "/api,/api/{*path}"
```

### 2. Run the Gateway

```bash
# Run with Cargo
RUST_LOG=info cargo run -- --config spectra.toml

# Or run the release binary
./target/release/spectragql
```

### 3. Send a Query or Mutation

```bash
# Query routed to upstream read backend
curl -X POST http://127.0.0.1:8000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query GetOrder { order(id: 42) { id status } }"}'

# Mutation ratified and dispatched to event broker
curl -X POST http://127.0.0.1:8000/graphql \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ord-create-42" \
  -d '{"query": "mutation CreateOrder($item: String!) { createOrder(item: $item) { id } }", "variables": {"item": "Quantum Drive"}}'
```

---

## License

This project is licensed under the [MIT License](LICENSE).
