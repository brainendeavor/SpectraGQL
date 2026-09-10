# SpectraGQL Modes of Operation: Detailed Architecture

This document breaks down the three operational modes supported by **SpectraGQL Proxy**: **Mode A**, **Mode B**, and **Mode C**.

The fundamental challenge SpectraGQL solves is bridging **two different worlds**:
1. **The Client World:** Standard GraphQL clients (Apollo Client, Relay, urql) that expect synchronous HTTP request/response semantics (`useMutation()` returning immediate data).
2. **The Backend World:** Event-driven, event-sourced architectures (CQRS, NATS JetStream, Apache Iggy, Kafka, SierraDB) where state changes are commands and domain events processed by asynchronous workers.

---

## Quick Comparison Matrix

| Dimension | Mode A: Edge Outbox (Dual Dispatch) | Mode B: Pure CQRS (Async Command Receipt) | Mode C: Coordinated Request-Reply (The Bridge) |
| :--- | :--- | :--- | :--- |
| **Client Requirement** | **Unchanged (Standard / Dumb)**<br>Expects normal GraphQL response | **Smart / Async-Aware**<br>Expects receipt (`ACCEPTED`), listens via Subscription / WebSocket / polling | **Unchanged (Standard / Dumb)**<br>Expects normal GraphQL response |
| **Backend Requirement** | **Legacy HTTP Server**<br>(e.g. Apollo Server, Rails, Spring Boot) | **Event Consumer**<br>(Subscribes to broker, no HTTP server needed) | **Event Consumer**<br>(Subscribes to broker, no HTTP server needed) |
| **Upstream HTTP Hop** | **Yes** (SpectraGQL proxies HTTP to backend) | **No** (SpectraGQL talks only to broker) | **No** (SpectraGQL talks only to broker) |
| **Event Bus Role** | Shadow log / Audit stream / CDC mirror | Primary command bus & event log | Primary command bus with reply inbox |
| **Timeout Risk** | Dependent on legacy HTTP server SLA | **Zero timeout risk** (Instant ACK) | Subject to event consumer SLA (Configurable timeout) |
| **Adoption Target** | **Brownfield / Legacy**<br>Zero code changes across frontend and backend | **Greenfield / High-Scale**<br>Modern event-driven frontend + backend | **Greenfield Backend / Legacy Frontend**<br>Rewrite backend resolvers as event workers without touching frontend |

---

## Mode A: Edge Outbox / Dual Dispatch (The Brownfield Wedge)

### The Concept
Mode A is a **smart reverse proxy** that sits transparently in front of your existing GraphQL server. It fulfills the legacy HTTP request while asynchronously "shadowing" the mutation to your event stream.

### Sequence Flow
```
Client                     SpectraGQL Proxy                   Legacy Backend                 Event Broker
  │                              │                                  │                             │
  │── 1. POST Mutation ─────────>│                                  │                             │
  │                              │── 2. Ratify & Parse              │                             │
  │                              │                                  │                             │
  │                              │── 3. Dispatch Command Event ──────────────────────────────────>│
  │                              │                                  │                             │
  │                              │── 4. Forward HTTP Request ──────>│                             │
  │                              │                                  │ (Executes DB write)         │
  │                              │<── 5. Return HTTP Response ──────│                             │
  │                              │                                                                │
  │                              │── 6. Dispatch Response/Result Event ──────────────────────────>│
  │<── 7. Return GQL Response ───│                                                                │
```

### Why Use Mode A?
- **Zero code changes:** No changes to your iOS, Android, or React web apps. No changes to your backend resolvers.
- **Instant Event-Driven Enablement:** New squads in your company can immediately start consuming mutation events from NATS or Kafka to build new microservices, notification systems, or search indexing without asking the core backend team to build webhooks or Kafka producers.
- **Edge Audit Log:** Every mutation attempt and its resulting response is cryptographically stamped with a UUID and recorded in an immutable log.

---

## Mode B: Pure CQRS / Asynchronous Command (The Architectural Ideal)

### The Concept
Mode B is **pure CQRS**. Mutations are treated strictly as **Commands**. There is **no legacy HTTP server** involved in the mutation path. SpectraGQL receives the mutation, validates/ratifies it at the edge, commits it to the event stream, and immediately returns an HTTP `202 Accepted` style receipt to the client.

### Sequence Flow
```
Client                     SpectraGQL Proxy                                                 Event Broker             Async Consumer / Projection
  │                              │                                                                │                               │
  │── 1. POST Mutation ─────────>│                                                                │                               │
  │                              │── 2. Ratify & Parse                                            │                               │
  │                              │── 3. Commit Command to Broker ────────────────────────────────>│                               │
  │                              │<── 4. Broker ACK ──────────────────────────────────────────────│                               │
  │<── 5. Return Command Receipt ─│                                                                │                               │
  │   { status: ACCEPTED,        │                                                                │                               │
  │     commandId: "uuid" }      │                                                                │── 6. Consume & Execute ──────>│
  │                              │                                                                │                               │ (Updates DB / Read Model)
  │                              │                                                                │<── 7. Publish Domain Event ───│
  │                              │                                                                │
  │<── 8. (Optional) Realtime Update via GQL Subscription / WebSocket / SSE ──────────────────────│
```

### Why Use Mode B?
- **Extreme Throughput & Resilience:** The edge gateway terminates the connection in microseconds. Backends cannot be DOS'd by write spikes; writes sit durably in the broker queue (Kafka/Iggy/NATS) and are processed at the consumer's maximum sustainable rate.
- **Client Requirements:** The client must be designed for eventual consistency:
  - It displays optimistic UI updates.
  - It tracks `commandId`.
  - It listens for completion via a GraphQL Subscription, WebSocket channel, or polling.

---

## Mode C: Coordinated Request-Reply (The Greenfield Bridge)

### The Concept
Mode C answers the question: **"What if we want to write our backend resolvers as pure event-sourced consumers (like in Mode B), but our frontend clients still expect a standard synchronous GraphQL response (like in Mode A)?"**

In Mode C:
- There is **no upstream HTTP server**.
- Your backend resolvers are **event consumers** listening to the message broker.
- SpectraGQL uses the **Request-Reply pattern** (native to NATS and supported in Kafka via correlation IDs) to wait for the consumer to finish and return the result synchronously over the original HTTP connection.

### Sequence Flow
```
Client                     SpectraGQL Proxy                                                 Event Broker             Async Consumer / Resolver
  │                              │                                                                │                               │
  │── 1. POST Mutation ─────────>│                                                                │                               │
  │                              │── 2. Ratify & Parse                                            │                               │
  │                              │── 3. Publish Command with `reply_to` inbox subject ───────────>│                               │
  │                              │                                                                │── 4. Consume Command ────────>│
  │                              │   (SpectraGQL waits on inbox with timeout, e.g. 500ms)             │                               │ (Executes logic & DB write)
  │                              │                                                                │<── 5. Publish Result to Inbox ─│
  │                              │<── 6. Receive Reply ───────────────────────────────────────────│                               │
  │<── 7. Return GQL Response ───│                                                                │                               │
  │   { data: { order: {...} } } │                                                                │                               │
  │                              │                                                                │                               │
  │   [If timeout expires]:      │                                                                │                               │
  │<── 7b. Return GQL Error ─────│                                                                │                               │
  │   "Consumer timed out"       │                                                                │                               │
```

### Mode C Delivery Flavors: Beyond the Synchronous Connection Hold

Mode C is fundamentally about **coordinating an asynchronous event consumer with a client that needs a result**. While the baseline pattern holds the original HTTP connection open, SpectraGQL can deliver that result across several lightweight delivery flavors:

### Client Compatibility Assessment: Zero Custom Client Required

A core design principle of SpectraGQL is **never requiring a proprietary "SpectraGQL Client SDK"**. Frontend teams should continue using standard, off-the-shelf GraphQL clients (Apollo Client, Relay, urql, Swift/iOS Apollo, Kotlin/Android Apollo).

Here is how the delivery mechanisms evaluate against standard GraphQL clients:

| Approach | Client Compatibility | Standard Client Mechanism | Requires Custom Client / SDK? |
| :--- | :--- | :--- | :--- |
| **1. Synchronous Hold** (Fast Ops) | **100% Universal** | Standard `useMutation()` | **NO.** Standard HTTP response. |
| **2. One-Shot Subscription** (Long Ops) | **100% Spec Compliant** | Standard `useSubscription()` | **NO.** Closes cleanly on spec `complete` frame. |
| **3. Multipart Streaming (`@defer`)** | Experimental | Draft RFC (Multipart HTTP) | **Partial.** Supported in modern Apollo Web, but breaks many mobile/legacy clients. |
| **4. Webhook Callbacks** | B2B / Server-to-Server | Standard HTTP POST receiver | **NO.** But only applicable to backend-to-backend consumers. |
| **5. Custom REST Endpoint** | Non-GraphQL | Custom `fetch()` / EventSource | **YES.** Breaks GraphQL caching and client paradigms. |

> **The Winning Formula:** To maintain 100% client transparency without custom SDKs, SpectraGQL focuses on **Approach 1 (Synchronous Hold)** for operations $< 1$ second, and **Approach 2 (Ephemeral One-Shot Subscription)** for operations $> 1$ second.

---

#### 1. Baseline: Synchronous Request-Reply (Connection Hold)
- **Mechanic:** SpectraGQL holds the open HTTP connection and subscribes to a temporary NATS `reply_to` inbox. When the consumer replies, SpectraGQL returns the HTTP response and closes.
- **Best for:** Fast operations (< 1000ms) like checkout, user updates, or balance checks.

#### 2. Ephemeral "One-Shot" Subscription (Auto-Closing Event Stream)
- **Mechanic:** Traditional GraphQL subscriptions remain open indefinitely for continuous event streams (e.g., chat feeds). An **Ephemeral One-Shot Subscription** is registered specifically for a single command completion:
  ```graphql
  subscription OnCommandDone {
    commandResult(commandId: "cmd-999") {
      status
      result
      error
    }
  }
  ```
- **Behavior:** SpectraGQL binds to the broker reply subject (`spectra.replies.cmd-999`). The instant the worker emits the completion event, SpectraGQL pushes a single `next` frame down the WebSocket/SSE followed immediately by a `complete` frame, auto-closing the subscription.
- **Best for:** Operations taking 2–30 seconds where keeping an HTTP connection open is risky due to mobile network drops or reverse proxy timeouts.

#### 3. Incremental Delivery / Chunked HTTP Streaming (`@defer`)
- **Mechanic:** Client issues the mutation over standard HTTP with multipart/mixed chunked transfer.
- **Behavior:** SpectraGQL immediately sends Chunk 1:
  ```json
  { "data": { "processVideo": { "status": "PENDING", "commandId": "cmd-88" } }, "hasNext": true }
  ```
  When the event consumer publishes the finished result to the broker, SpectraGQL emits Chunk 2 over the *same* HTTP connection:
  ```json
  { "data": { "processVideo": { "status": "COMPLETED", "videoUrl": "https://..." } }, "hasNext": false }
  ```
  Connection cleanly closes without requiring WebSockets.

#### 3. Webhook Dispatch (Outbound Server-to-Server Integration)
*Note: As an architectural distinction, Webhooks belong to the **outbound mutation dispatch engine** rather than interactive client transports, since the receiving party must host a reachable HTTP endpoint.*
- **Mechanic:** For B2B integrations, external partners, or third-party webhooks (e.g. Zapier, Slack, partner APIs), the mutation processing pipeline dispatches an HTTP POST directly to the configured endpoint upon mutation ratification or command completion.
- **Role:** Implemented via SpectraGQL's `WebhookDispatch` adapter in the dispatch layer, running alongside NATS/Kafka.
- **Mechanic:** For B2B or third-party consumers calling mutations, the client includes an `X-Callback-URL: https://partner.com/webhook` header or passes a callback argument.
- **Behavior:** SpectraGQL terminates the HTTP call with an ACK. When the async event worker finishes, SpectraGQL's dispatch engine fires an HTTP POST webhook containing the completed GraphQL payload directly to the partner's callback endpoint.

### Why Use Mode C?
- **Starting Greenfield without Legacy Baggage:** If you are building a new system or rewriting your backend, you do NOT have to stand up an Express/Apollo HTTP server just to satisfy GraphQL mutation semantics. You write pure event-driven consumers in Go, Rust, or Python that consume from NATS/Iggy/Kafka.
- **Frontend Stays Simple:** Frontend developers do not need to rewrite their apps with complex subscription glue code. They continue using standard `useMutation()` hooks.
- **Graceful Fallback:** If a consumer takes longer than the configured timeout (e.g. 1000ms), SpectraGQL can return an informative GraphQL error or automatically fall back to returning the `commandId` receipt so the client can check back later.

---

## The Evolutionary Journey: Mode A $
ightarrow$ Mode C $
ightarrow$ Mode B

SpectraGQL allows you to configure modes **per mutation route**:

```toml
# spectra.toml example configuration

[gql.mutations.legacy_user_update]
path = "updateUserProfile"
mode = "A"                     # Proxies to legacy monolith + emits event to NATS

[gql.mutations.modern_order_checkout]
path = "checkoutOrder"
mode = "C"                     # NATS Request-Reply with 800ms timeout to order-consumer
timeout_ms = 800

[gql.mutations.batch_video_upload]
path = "importCatalog"
mode = "B"                     # Pure async: returns immediate commandId receipt
```

1. **Phase 1 (Day 1):** Deploy SpectraGQL in **Mode A** across all existing mutations. Immediate observability, audit log, and shadow streaming with zero risk.
2. **Phase 2 (Modernizing Backends):** As squads rewrite brittle resolvers into event-sourced consumers, flip those specific routes to **Mode C**. Frontend apps don't notice any change.
3. **Phase 3 (True Eventual Consistency):** For high-scale operations (payments, bulk imports, order processing), flip routes to **Mode B** and wire frontend UIs to GraphQL Subscriptions.
