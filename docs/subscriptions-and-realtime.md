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

1. **Protocol Support:**
   - Implement **Server-Sent Events (SSE)** first: Since Pingora natively handles chunked HTTP responses, SSE requires zero WebSocket protocol upgrade machinery and works out of the box over HTTP/1.1 and HTTP/2.
   - Implement **WebSocket (`graphql-ws`)** using `tokio-tungstenite` or Pingora's raw stream hijacking for full bidirectional support.
2. **Topic Mapping Engine:**
   - Map subscription operation names and arguments directly to broker subjects:
     `subscription OrderUpdates($id: ID!)` -> `spectra.events.order.<id>`
3. **Connection Lifecycle & Multiplexing:**
   - Allow a single WebSocket connection from a client to host multiple active subscriptions (`sub-1`, `sub-2`), with SpectraGQL demultiplexing events from different broker subjects onto the same socket.
