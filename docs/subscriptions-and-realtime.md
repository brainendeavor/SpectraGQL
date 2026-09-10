# GraphQL Subscriptions in SpectraGQL: The Reverse Event Pipeline

This document defines the architectural role of **GraphQL Subscriptions** in **SpectraGQL Proxy** and explains how they complete the CQRS loop by acting as **an event queue in the reverse direction**.

---

## 1. How GraphQL Subscriptions Work (The Protocols)

In GraphQL, while `Query` and `Mutation` follow a traditional request-response lifecycle over HTTP POST, `Subscription` is an **asynchronous event stream from server to client**.

There are two primary transport mechanisms in modern GraphQL:

### A. WebSockets (The Established Standard)
- **Protocol:** Uses the official `graphql-ws` subprotocol (or legacy `subscriptions-transport-ws`).
- **Handshake:**
  1. Client sends an HTTP GET request with headers:
     `Upgrade: websocket`
     `Sec-WebSocket-Protocol: graphql-transport-ws`
  2. Client sends a JSON message: `{"type": "connection_init"}`.
  3. Server acknowledges: `{"type": "connection_ack"}`.
  4. Client registers a subscription:
     ```json
     {
       "id": "sub-1",
       "type": "subscribe",
       "payload": {
         "query": "subscription OnOrder($id: ID!) { orderStatus(id: $id) { status } }",
         "variables": { "id": "42" }
       }
     }
     ```
  5. The server pushes data frames whenever events occur:
     ```json
     {
       "id": "sub-1",
       "type": "next",
       "payload": { "data": { "orderStatus": { "status": "COMPLETED" } } }
     }
     ```

### B. Server-Sent Events (SSE) (The Modern Lightweight Alternative)
- **Protocol:** Standard HTTP GET or POST with `Accept: text/event-stream`.
- **Advantage:** Simpler than WebSockets, natively traverses corporate firewalls/proxies, supports HTTP/2 multiplexing, and does not require a protocol upgrade handshake.

---

## 2. The Enterprise Problem: Why Subscriptions Break at Scale

In conventional enterprise architectures, GraphQL Subscriptions are notoriously painful to manage:

1. **Stateful Application Servers:** If an application server (written in Node.js, Python, Ruby, or Java) manages subscriptions, it must maintain thousands of open, stateful TCP/WebSocket connections. This rapidly exhausts thread pools, file descriptors, and memory.
2. **Thundering Herd Disconnections:** Whenever a backend microservice restarts or redeploys, thousands of WebSockets disconnect simultaneously. Clients immediately attempt to reconnect, creating massive thundering herd storms.
3. **Complex Federation Routing:** Apollo Federation and schema aggregators struggle with subscriptions because subgraphs have to orchestrate long-lived connections back to the gateway.
4. **Ad-hoc PubSub Infrastructure:** Backend teams usually end up duct-taping Redis PubSub or Kafka into application resolvers just to broadcast events to client websockets.

---

## 3. The SpectraGQL Solution: The "Reverse Event Queue"

Your intuition captures the core architecture:
> **If a Mutation is a Command pushing into an event queue, a Subscription is an Event Queue pushing down to the client.**

```
=============================================================================
WRITE PATH (Mutations):  Client ──(HTTP POST)──> SpectraGQL Proxy ──(Command)──> Message Broker
READ PATH  (Queries):    Client ──(HTTP POST)──> SpectraGQL Proxy ─────────────> Read Replicas / API
STREAM PATH(Subscribe):  Client <─(WS / SSE)──── SpectraGQL Proxy <──(Events)─── Message Broker
=============================================================================
```

```
┌─────────────────┐                                  ┌────────────────────────┐
│  Browser / App  │                                  │ Backend Microservices  │
│  (Apollo/urql)  │                                  │ (Stateless Consumers)  │
└────────┬────────┘                                  └───────────┬────────────┘
         │                                                       │
         │ 1. Connect WebSocket / SSE                            │
         ▼                                                       │
┌──────────────────────────────────────────────────┐             │
│          SPECTRAGQL PROXY (Pingora / Rust)       │             │
│                                                  │             │
│  - Terminates 100k+ idle WebSockets/SSE easily   │             │
│  - Parses Subscription AST                       │             │
│  - Extracts event topics & variable filters      │             │
│  - Subscribes to NATS / Iggy / Kafka subject     │             │
│  - Formats GQL payload & streams down to client  │             │
└────────┬─────────────────────────────────────────┘             │
         │                                                       │
         │ 2. Listen on subject:                                 │ 3. State change:
         │    "spectra.events.orders.42"                          │    Publishes event
         ▼                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    EVENT BACKBONE (NATS / Iggy / Kafka)                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Architectural Benefits:
1. **100% Stateless Backends:** Backend services **never touch a WebSocket**. When a domain worker processes an order or payment, it simply publishes a lightweight domain event to NATS or Kafka. It doesn't know or care whether 0 or 10,000 clients are currently connected.
2. **Edge Termination at Scale:** Rust and Tokio (Pingora's foundation) handle 100,000+ concurrent idle connections with minimal RAM (a few kilobytes per connection vs. tens of megabytes in Node/Python).
3. **Seamless Deployments:** Backend services can deploy, restart, or scale to zero without dropping a single client WebSocket connection, because all connections are terminated and held open by SpectraGQL at the edge.

---

## 4. How Subscriptions Complete Mode B (Pure CQRS)

Subscriptions are the missing link that makes **Mode B (Pure CQRS / Asynchronous Command Receipts)** practical for real-world frontend applications.

### The Pure CQRS Loop:
```
1. Client Subscribes:
   subscription { orderUpdates(orderId: "order-99") { status, trackingNumber } }
   └─> SpectraGQL opens WebSocket, binds to NATS topic: `events.order.order-99`

2. Client Mutates:
   mutation { submitOrder(id: "order-99", items: [...]) }
   └─> SpectraGQL returns immediate Mode B receipt in <2ms:
       { data: { submitOrder: { status: "ACCEPTED", commandId: "cmd-abc" } } }

3. UI displays: "Order Submitted (Processing...)"

4. Async Worker consumes `cmd-abc` from NATS, charges credit card, writes to DB.

5. Async Worker emits event to NATS: `events.order.order-99` { status: "CONFIRMED", trackingNumber: "TRK123" }

6. SpectraGQL receives event from NATS, wraps it in GraphQL payload, and pushes it down the open WebSocket:
   { data: { orderUpdates: { status: "CONFIRMED", trackingNumber: "TRK123" } } }

7. UI updates in real-time: "Order Confirmed!"
```

With this pattern, the frontend gets **instant perceived performance** (<2ms mutation responses) and **real-time reactive UI updates** without writing custom polling or ad-hoc websocket glue code.

---

## 5. Implementation Path in SpectraGQL Proxy

### 5.1 Protocol Decision: `graphql-transport-ws` as First-Class Citizen

While Server-Sent Events (SSE) offer simpler proxying over HTTP chunked transfer, the enterprise GraphQL ecosystem (Apollo Client, Relay, urql, iOS/Android Apollo) treats the **`graphql-transport-ws`** subprotocol as the canonical standard for subscriptions. 

To deliver a transparent edge proxy that requires **zero custom client SDKs**, SpectraGQL prioritizes **`graphql-ws` edge termination**.

---

### 5.2 Technical Deep Dive: WebSocket Edge Termination in Pingora

In standard proxy scenarios, gateways either blindly tunnel TCP connections or proxy WebSockets to a stateful upstream server. In SpectraGQL's CQRS architecture, **there is no stateful upstream subscription server**; resolvers are stateless event consumers. SpectraGQL must **terminate** the WebSocket connection at the edge, manage its lifecycle, and pump events directly from the message broker to the client.

We evaluated two architectural paths for terminating WebSockets inside Cloudflare Pingora:

```
                  ┌─────────────────────────────────────────────────────────┐
                  │                 SpectraGQL Proxy (Pingora)              │
                  │                                                         │
Client            │   1. HTTP GET /graphql (Upgrade: websocket)             │
───────────────>  │   2. Pingora validates Sec-WebSocket-Key                │
                  │   3. Respond HTTP 101 (Sec-WebSocket-Protocol)          │
                  │                                                         │
                  │             ┌─────────────────────────────┐             │
                  │             │  Pingora Connection Hijack  │             │
                  │             └──────────────┬──────────────┘             │
                  │                            │ Underlying IO Stream       │
                  │                            ▼                            │
                  │             ┌─────────────────────────────┐             │
                  │             │   tokio-tungstenite Engine  │             │
                  │             │  (RFC 6455 Frame / Masking) │             │
                  │             └──────────────┬──────────────┘             │
                  │                            │ Decoded Frames             │
                  │                            ▼                            │
                  │             ┌─────────────────────────────┐             │
                  │             │  graphql-transport-ws State │             │
                  │             │  - connection_init / ack    │             │
                  │             │  - subscribe / complete     │             │
                  │             │  - ping / pong heartbeat    │             │
                  │             └──────────────┬──────────────┘             │
                  └────────────────────────────┼────────────────────────────┘
                                               │
                                       Subscribe / Events
                                               │
                                               ▼
                                ┌─────────────────────────────┐
                                │ Event Broker (NATS / Iggy / │
                                │ Redis Streams / Kafka)      │
                                └─────────────────────────────┘
```

#### Option 1: Pingora Raw Stream Hijacking + `tokio-tungstenite` (Recommended)

* **How it Works:**
  1. **Handshake Interception:** Pingora intercepts the incoming HTTP request in `early_request_filter` / `request_filter`. It validates headers (`Upgrade: websocket`, `Connection: Upgrade`, `Sec-WebSocket-Key`, and `Sec-WebSocket-Protocol: graphql-transport-ws`).
  2. **101 Switching Protocols:** Pingora synthesizes the SHA-1 acceptance key (`Sec-WebSocket-Accept`), commits the HTTP 101 Switching Protocols response header, and flushes it to the client socket.
  3. **Connection Hijacking:** Pingora detaches and yields the raw underlying bidirectional socket stream (TCP or BoringSSL/TLS stream) from the Pingora `Session` lifecycle.
  4. **Tungstenite Wrapping:** The raw socket stream is passed to Tokio:
     ```rust
     let ws_stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
         raw_stream,
         tokio_tungstenite::tungstenite::protocol::Role::Server,
         Some(websocket_config),
     ).await;
     ```
  5. **Spawn Dedicated Connection Actor:** Tokio spawns a lightweight task managing the WebSocket framing and subprotocol state machine for that client.
* **Trade-Offs:**
  * **Pros:**
    * 100% compliant RFC 6455 implementation out of the box (binary/text framing, unmasking, control frames, close handshakes).
    * Eliminates thousands of lines of brittle custom WebSocket framing code.
    * Highly optimized zero-copy Rust performance natively integrated with Tokio.
    * Isolates long-lived WebSocket actors from Pingora's HTTP request-response worker thread pool.
  * **Cons:** Requires clean socket handoff from Pingora's session engine into an independent Tokio task.

#### Option 2: Native Pingora Custom Frame Parser & Upgrade Pipeline

* **How it Works:**
  - Build custom Layer 7 frame decoder directly within Pingora's internal filters (`request_body_filter` and raw streaming callbacks) without external libraries like `tokio-tungstenite`.
* **Trade-Offs:**
  * **Pros:** Keeps the entire connection lifecycle unified strictly within Pingora's internal abstractions.
  * **Cons:** Substantial implementation and maintenance overhead. RFC 6455 requires handling frame fragmentation, payload masking, UTF-8 validation, control frame interleaving (pings arriving between fragmented data frames), and graceful close handshakes. Not an 80/20 decision.

> [!TIP]
> **Architecture Decision:** **Option 1 (Stream Hijacking + `tokio-tungstenite`)** is the optimal strategy. Pingora excels as the wire-speed TLS terminator and routing gateway, while `tokio-tungstenite` provides a rock-solid, production-proven WebSocket framing engine.

---

### 5.3 Protocol State Machine (`graphql-transport-ws`)

SpectraGQL's WebSocket handler implements the official `graphql-transport-ws` protocol state machine:

| Client Message | Direction | Payload | Gateway Action |
| :--- | :--- | :--- | :--- |
| `connection_init` | Client $\to$ Gateway | Optional `{ "headers": { "Authorization": "..." } }` | Authenticate/ratify credentials. Return `connection_ack` on success, or close with `4401 Unauthorized`. |
| `ping` | Client $\leftrightarrow$ Gateway | Optional payload | Echo back `pong` (or send periodic heartbeat `ping` to prevent NAT timeouts). |
| `subscribe` | Client $\to$ Gateway | `{ "id": "sub_1", "payload": { "query": "...", "variables": {...} } }` | 1. Parse AST with `apollo-parser`.<br>2. Extract subscription root field and arguments.<br>3. Compute broker subject (e.g. `spectra.events.orders.42`).<br>4. Bind consumer listener.<br>5. Register in connection subscription registry. |
| `next` | Gateway $\to$ Client | `{ "id": "sub_1", "payload": { "data": { ... } } }` | Transmit event payload to client when broker emits message. |
| `complete` | Client $\to$ Gateway | `{ "id": "sub_1" }` | Unbind broker consumer and deregister subscription from registry. |
| `complete` | Gateway $\to$ Client | `{ "id": "sub_1" }` | Emitted by gateway for **Mode C ephemeral one-shot subscriptions** immediately after delivering the command result. |

---

### 5.4 Multiplexing & Connection Registry Architecture

A single client connection (e.g. a browser tab or mobile app instance) frequently runs multiple concurrent subscriptions simultaneously (e.g. `orderUpdates` and `unreadNotifications`).

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Client Connection Actor                         │
│                                                                        │
│  Active Subscriptions (Map<SubId, SubscriptionHandle>):                │
│  ├── sub-1: "orders.42"      ──> [NATS Consumer: spectra.events.42]   │
│  └── sub-2: "notifications"   ──> [NATS Consumer: user.notifications]  │
│                                                                        │
│  Outgoing MPSC Channel (Bounded):                                      │
│  [ next(sub-1) ] ──> [ next(sub-2) ] ──> Socket Sink (Wire Speed)      │
└────────────────────────────────────────────────────────────────────────┘
```

1. **Connection-Local Multiplexer:** Each connection actor maintains a thread-safe registry of its active subscriptions (`HashMap<String, BoundedSender>`).
2. **Reverse Broker Event Pump:** When a domain event is published by an async resolver to NATS, Iggy, or Redis Streams:
   - SpectraGQL's shared broker listener matches the subject against the gateway's topic routing table.
   - The event is dispatched to all matching client connection channels.
3. **Slow Client Protection & Backpressure:** Each WebSocket sink uses a bounded Tokio MPSC channel (e.g., buffer capacity 256). If a mobile client on a degraded connection stalls, incoming events do not cause unbounded gateway memory growth. When buffers fill, the gateway can selectively drop intermediate progress events or terminate the slow connection according to policy.
