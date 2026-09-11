# SpectraGQL Modes of Operation: Detailed Architecture

This document defines the operational architecture supported by **SpectraGQL Proxy**:
- **Mode A (The Workhorse Gateway):** The primary, battle-tested operational mode for 90% of real-world GraphQL microservice architectures.
- **Mode B (The Event-Native Gateway):** Pure async CQRS for high-throughput, bulk ingestion, or long-running workflows.
- **Mode C (The Mirage - Evaluated & Retired):** An analysis of coordinated request-reply, why it was evaluated, and why engineering decided "not today".

---

## Quick Comparison Matrix

| Dimension | Mode A: The Workhorse Gateway (`ExecutionStrategy::SyncUpstreamExecution`) | Mode B: The Event-Native Gateway (`ExecutionStrategy::AsyncEdgeCommand`) | Mode C: The Mirage ("Not Today") |
| :--- | :--- | :--- | :--- |
| **Primary Philosophy** | **Pragmatic Event Choreography** | **Pure Asynchronous CQRS** | **Compromise / Hybrid** |
| **Status** | **Flagship (Recommended)** | **Specialized / Greenfield** | **Retired / Architectural Case Study** |
| **Client Requirement** | **Unchanged (100% Transparent)**<br>Full Apollo/Relay cache normalization | **Async-Aware**<br>Expects deterministic Command Receipt (`ACCEPTED`) | **Unchanged (Standard)**<br>Standard sync response |
| **Backend Requirement** | **Standard HTTP Service / Microservice**<br>(Executes core write, returns entity) | **Event Consumer**<br>(Subscribes to broker, no HTTP needed) | **Event Consumer with Reply Inbox**<br>(Must compute selection set for reply) |
| **Upstream HTTP Hop** | **Yes** (Fast local write in primary context) | **No** (Terminates immediately at edge) | **No** (Waits on broker inbox) |
| **Event Bus Role** | Domain event stream (`CompletionEvent`) for downstream microservices | Primary command queue & domain log | Command bus + ephemeral reply topic |
| **Timeout Risk** | Dependent on core service SLA (typically < 100ms) | **Zero timeout risk** (Instant ACK) | Subject to worker SLA + broker latency |
| **Cache Normalization** | **Fully preserved** (Standard GraphQL return) | **Bypassed** (Requires subscription glue) | **Preserved** (If worker computes selection set) |
| **Adoption Target** | **Microservices & Brownfield**<br>Zero frontend changes, instant decoupling | **Bulk ingestion, IoT, Video, Long Sagas** | **N/A (Retired)** |

---

## Mode A: The Workhorse Gateway (Pragmatic Event Choreography)

### The Concept
Mode A is the **flagship mode** of SpectraGQL. It acts as an intelligent Layer 7 reverse proxy sitting in front of your primary GraphQL service. 

It preserves synchronous client expectations (and automatic Apollo/Relay cache normalization) while using the mutation response to emit a rich, reliable domain event to your broker (NATS JetStream, Apache Iggy, SierraDB, Kafka). Downstream subsystems (loyalty, inventory, notifications, search indexing) consume this event **choreographically** rather than forcing the core resolver into a brittle, synchronous fan-out.

### Sequence Flow (Post-Response Gateway Outbox)
```
Client                     SpectraGQL Proxy                    Primary Backend                 Event Broker
  │                              │                                   │                             │
  │── 1. POST Mutation ─────────>│                                   │                             │
  │                              │── 2. Ratify & Stash in Memory     │                             │
  │                              │      (Idempotency & HLC Clock)    │                             │
  │                              │                                   │                             │
  │                              │── 3. Forward HTTP Request ───────>│                             │
  │                              │                                   │ (Fast DB Commit & Return)   │
  │                              │<── 4. Return HTTP 200 (Result) ───│                             │
  │                              │                                                                 │
  │                              │── 5. Dispatch Completed Domain Event ──────────────────────────>│
  │                              │      (Stashed Request Args + Response Data)                     ├─► [Loyalty Worker]
  │<── 6. Return GQL Response ───│                                                                 ├─► [Notification Worker]
  │   (Normal GQL Data)          │                                                                 └─► [Search Worker]
```

---

### Mode A Event Dispatch Options

Different organizations have different risk tolerances regarding failures and in-flight tracking. Mode A provides **three configurable dispatch policies**:

#### Option 1: Response-Only Events (Default / Cleanest)
* **Mechanic:** SpectraGQL intercepts the mutation, forwards it to the primary backend, and only publishes an event to the broker upon receiving an HTTP 2xx response.
* **Payload:** The published event contains both the stashed request parameters (operation name, variables, client identity, HLC timestamp) and the response data.
* **Pros:** 
  * Exactly one event per successful state change.
  * **Zero phantom writes:** If the backend rejects the mutation (400 validation error, unique constraint violation, 500 crash), no event is emitted.
  * Downstream consumers have trivial logic—no need to correlate or filter out aborted requests.
* **Cons:** If the upstream backend hangs or crashes mid-request, downstream services have no visibility into the failed attempt.

#### Option 2: Response-Only + In-Flight Stash (Failure / Timeout Emission)
* **Mechanic:** Leverages SpectraGQL's built-in **`IdempotencyEngine`** memory stash:
  1. When a mutation arrives, the key, arguments, and HLC are registered in an in-memory ring buffer.
  2. On HTTP 2xx: SpectraGQL emits `CompletionEvent` with `OperationOutcome::Success` and clears the stash.
  3. On Upstream Timeout or Connection Drop: SpectraGQL automatically emits an explicit `CompletionEvent` with `OperationOutcome::Failed` (with reason `UPSTREAM_TIMEOUT` or `BACKEND_ERROR`) to a dead-letter or failure topic before returning an error to the client.
* **Pros:** 
  * Best-in-class operational visibility without polluting the happy-path event stream.
  * Downstream recovery workers can detect abandoned or aborted mutations and trigger alerts or compensations.
* **Cons:** Requires slight in-memory bookkeeping inside the proxy (already standard for idempotency deduplication).

#### Option 3: Raw Audit Stream (Dual Ingress / Egress Dispatch)
* **Mechanic:** Dispatches an event on request ingress (`MutationInitiated`), forwards upstream, then dispatches a second event on response egress (`MutationCompleted` or `MutationFailed`).
* **Pros:** Provides a strict, real-time audit tap of all incoming network attempts, even if the backend process crashes instantly.
* **Cons:** Downstream application consumers must maintain state machines to correlate UUIDs across topics and avoid acting on uncommitted requests. Recommended primarily for compliance, security taps, and telemetry rather than business choreography.

---

## Mode B: The Event-Native Gateway (Pure CQRS)

### The Concept
Mode B (`ExecutionStrategy::AsyncEdgeCommand`) is **pure, asynchronous CQRS**. Mutations are treated strictly as **Commands**. There is no upstream HTTP backend in the write path. SpectraGQL receives the mutation, executes guards (validating syntax, depth, idempotency locks, and redacting PII via compile-time type-states), commits it to the event stream, and immediately returns a deterministic GraphQL command receipt:

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

### Dispatch Failure & Retry Resilience
If the downstream broker is unreachable or fails to persist the command:
1. SpectraGQL responds with `"status": "DISPATCH_FAILED"`.
2. Adds header `x-spectra-dispatch: failed`.
3. Evicts the in-flight idempotency lock from memory so client retries are not blocked by false conflict errors.

### Sequence Flow
```
Client                     SpectraGQL Proxy                                                 Event Broker             Async Consumer / Projection
  │                              │                                                                │                               │
  │── 1. POST Mutation ─────────>│                                                                │                               │
  │                              │── 2. RequestGuard & Type-State Sanitizer                       │                               │
  │                              │── 3. Commit Sanitized Command to Broker ──────────────────────>│                               │
  │                              │<── 4. Broker ACK ──────────────────────────────────────────────│                               │
  │<── 5. Return Command Receipt ─│                                                                │                               │
  │   { status: ACCEPTED,        │                                                                │                               │
  │     commandId: "uuid",       │                                                                │                               │
  │     hlc: "time.counter" }    │                                                                │── 6. Consume & Execute ──────>│
  │                              │                                                                │                               │ (Updates DB / Read Model)
  │                              │                                                                │<── 7. Publish Domain Event ───│
  │                              │                                                                │
  │<── 8. (Optional) Realtime Update via GQL Subscription / WebSocket / SSE ──────────────────────│
```

### When to Use Mode B:
* **High-Throughput Ingestion:** Telemetry, IoT command streams, high-frequency bidding, or gaming actions.
* **Long-Running Workflows:** Video transcoding, bulk catalog imports, report generation, or multi-step payment sagas.
* **Client Expectation:** The frontend must be designed for eventual consistency, tracking the `commandId` and listening for completion via a GraphQL Subscription or polling.

---

## Mode C: "The Mirage" (Evaluated & Retired — "Not Today")

### What was Mode C?
Mode C ("Coordinated Request-Reply") was conceived as an attempt to hedge between Mode A and Mode B:
1. Allow backend engineers to write pure event consumers (subscribing to NATS/Kafka topics with no HTTP servers).
2. While holding the client's HTTP connection open at the SpectraGQL proxy using a temporary NATS `reply_to` inbox subject.
3. Once the event worker finished, it would reply to the inbox, and SpectraGQL would synthesize the synchronous GraphQL response.

```
[Client] ──(HTTP)──► [SpectraGQL Proxy] ──(NATS Request)──► [Event Worker]
                         │                                       │
                         │◄──────(NATS Reply Inbox)──────────────┘
[Client] ◄──(HTTP 200)───┘
```

### Why We Evaluated "The Mirage" and Decided "Not Today":
Upon rigorous distributed systems review, Mode C proved to be an **uncanny valley** that combines the worst trade-offs of both paradigms:

1. **Compensating for Mode B's Awkwardness:** Mode C was designed because Mode B breaks synchronous client expectations. But rather than embracing true asynchronous CQRS, it attempts to cosmetically disguise an asynchronous worker as a synchronous RPC endpoint.
2. **Double Fragility:** It inherits the full operational overhead of an asynchronous message broker, worker pools, and correlation routing, while **retaining the exact same synchronous connection hold, timeout vulnerability, and gateway resource locking** of legacy HTTP.
3. **The Selection Set Dilemma:** In GraphQL, clients request specific nested selection sets. In Mode C, the event worker either has to become a full GraphQL execution engine itself or coordinate with other services to resolve nested fields—re-introducing the exact synchronous cross-service fan-out SpectraGQL exists to prevent.
4. **Mode A Is Strictly Superior for Synchronous Clients:** If a client requires a synchronous response and cache normalization, simply running Mode A (where the primary domain service handles the write and SpectraGQL choreographs downstream side-effects) is orders of magnitude simpler, more reliable, and battle-tested.

> **Decision:** Mode C is officially archived. SpectraGQL focuses entirely on **Mode A (The Workhorse)** for pragmatic microservice adoption and **Mode B (Pure CQRS)** for specialized asynchronous pipelines.

---

## Configuration Example (`spectra.toml`)

```toml
bind_addr = "0.0.0.0:8000"

[upstream]
addr = "127.0.0.1:4000"
name = "core_graphql_backend"

[gql]
paths = "/graphql"
ops_to_dispatch = "mutation"

# Mode A default settings
[gql.mode_a]
enabled = true
# Options: "response_only", "response_with_failure", "raw_audit"
dispatch_policy = "response_with_failure"
timeout_ms = 3000

# Named upstreams for microservice routing
[named_upstreams]
inventory = "127.0.0.1:5001"
crm = "127.0.0.1:5002"

# Operation-level route overrides (ExecutionStrategy)
[[routes]]
operation = "createReview"
mode = "SyncUpstreamExecution" # Mode A: forward upstream + publish CompletionEvent
upstream = "crm"

[[routes]]
operation = "importCatalog"
mode = "AsyncEdgeCommand"      # Mode B: edge-terminated command receipt
receipt_status = "ACCEPTED"

# Event Broker Dispatch (Atomic EventSink)
[dispatch]
method = "NATS"
addr = "127.0.0.1:4222"
topic_prefix = "spectra.events"
```

---

## Summary: The Evolutionary Path

Rather than a complex 3-step migration, teams adopt SpectraGQL with clear, low-risk steps:

1. **Day 1 (Mode A across all routes):** Drop SpectraGQL in front of your existing GraphQL server. Zero client changes. Standard cache normalization. Immediate, reliable event stream on NATS/Iggy for new microservices and data pipelines.
2. **Selective Optimization (Mode B for heavy ops):** For specific, high-scale or long-running operations (e.g., bulk uploads, reports, async sagas), mark individual mutation routes as `mode = "B"` and wire the client to GraphQL Subscriptions.
