# Multi-Container Cloud Deployment Guide: 2-Container Topology

This document details the production cloud deployment architecture for a high-performance **CQRS GraphQL stack** comprising **SpectraGQL**, an embedded message broker (**NATS JetStream**), an **Upstream Application Service**, and **SpectraFlux** (downstream event chassis) connecting to managed PostgreSQL.

---

## 1. Architectural Topology Overview

Rather than deploying complex distributed clusters across multiple cloud providers (which introduces cross-cloud latency for GraphQL Mode A reads and cold database connections), the architecture is consolidated into **2 co-located container machines** in a private mesh network (such as Fly.io 6PN, Railway Private Networking, or Kubernetes VPC):

```
                                  PUBLIC INTERNET (HTTPS / 443)
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Machine 1: Infrastructure Appliance ("app-infra")                                      │
│                                                                                        │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ SpectraGQL Gateway (:8000)                                                     │   │
│   │  • Reverse-proxies / and /public/*  ──► app-backend.internal:3000              │   │
│   │  • Mode A Queries                   ──► app-backend.internal:3000 (DB reads)   │   │
│   │  • Mode B Mutations (< 1ms Receipt) ──► 127.0.0.1:4222 (Local NATS JetStream)  │   │
│   │  • Admin Control Plane & Drawer     ──► :8000/admin                            │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
│                                       │ Localhost (Sub-ms Loopback Dispatch)           │
│                                       ▼                                                │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ NATS JetStream Server (:4222)                                                  │   │
│   │  • Subject: spectra.events.* on stream SPECTRA                                 │   │
│   │  • Persistent NVMe Volume mounted at /data                                     │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
└───────────────────────────────────────┼────────────────────────────────────────────────┘
                                        │ Private Mesh Network
                                        │ (app-infra.internal)
                                        ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Machine 2: Application & Worker ("app-backend", Private, No Public IP)                 │
│                                                                                        │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Upstream Web & GraphQL Engine (:3000)                                          │   │
│   │  • GET  / and static client assets                                             │   │
│   │  • POST /graphql  ── Executes queries & Mode A writes via DB connection pool   │   │
│   │  • Dispatches POST /admin/api/v1/dns/rescan on startup                         │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
│                                       │                                                │
│   ┌───────────────────────────────────┴────────────────────────────────────────────┐   │
│   │ SpectraFlux Chassis / Downstream Worker (:8081)                                │   │
│   │  • Subscribes to spectra.events.> on stream SPECTRA                            │   │
│   │  • Monotonic HLC causal resolution & dead-letter queue routing                 │   │
│   │  • Executes sandboxed WebAssembly Fluxcells with zero compiler bloat           │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
└───────────────────────────────────────┼────────────────────────────────────────────────┘
                                        ▼ (TLS Connection Pool)
                            [ Managed PostgreSQL Database ]
```

---

## 2. Component Specifications

### Container 1: `app-infra` (SpectraGQL + NATS Appliance)
* **Image:** Built from [`docker/appliances/Dockerfile.nats`](../docker/appliances/Dockerfile.nats).
* **Supervisor:** `oxmgr` (pure-Rust supervisor managing `nats-server` and `spectragql`).
* **Sizing:** Shared CPU 1x, 512MB RAM.
* **Volume:** Persistent volume mounted at `/data` for NATS JetStream write-ahead log (WAL).
* **Networking:**
  * Public Ports: `80` (HTTP redirect) and `443` (TLS) forwarding to container port `8000`.
  * Private Mesh: Exposes `app-infra.internal:4222` (NATS) and `app-infra.internal:8000` (Gateway & Admin API).
* **Configuration (`spectra.toml`):**
  ```toml
  bind_addr = "0.0.0.0:8000"

  [upstream]
  addr = "app-backend.internal:3000"
  name = "app_upstream"

  [gql]
  paths = "/gql,/graphql"
  ops_to_dispatch = "query, mutation, subscription"

  [gql.mode_a]
  enabled = true
  timeout_ms = 5000

  [[routes]]
  operation = "submitOrder"
  mode = "AsyncEdgeCommand"
  receipt_status = "ACCEPTED"

  [dispatch]
  method = "NATS"
  addr = "nats://127.0.0.1:4222"
  name = "default"

  [admin]
  enabled = true
  bind_addr = "0.0.0.0:8000"
  path_prefix = "/admin"
  enable_ui = true
  ```

---

### Container 2: `app-backend` (Application Server + Downstream Worker)
* **Image:** Application base image (e.g. `oven/bun:1`, `node:20-slim`, `rust:1-slim`).
* **Sizing:** Shared CPU 1x, 512MB–1GB RAM.
* **Networking:** Private only (no public IP; reachable exclusively via internal private mesh network from Container 1).
* **Processes Supervised:**
  1. Primary GraphQL application server (listening on `0.0.0.0:3000` or dual-stack `::`).
  2. `spectra-flux` runtime chassis executing downstream mutation event handlers.
* **Environment Variables:**
  * `PORT=3000`
  * `DATABASE_URL=postgres://user:password@db.provider.internal:5432/app?sslmode=require`
  * `NATS_URL=nats://app-infra.internal:4222`
  * `SPECTRA_ADMIN_URL=http://app-infra.internal:8000`

> [!IMPORTANT]
> **Administrative URL Invariant:**
> `SPECTRA_ADMIN_URL` must specify **strictly the base origin and port**, and must **NEVER** include the `/admin` path suffix:
> - ✅ `SPECTRA_ADMIN_URL=http://app-infra.internal:8000`
> - ❌ `SPECTRA_ADMIN_URL=http://app-infra.internal:8000/admin` (causes broken routes like `/admin/admin/api/...`)

---

## 3. Dynamic Upstream DNS Re-Resolution Hook

When the application container redeploys, cloud platforms assign a new ephemeral private IP address. The upstream service triggers an automated non-blocking rescan hook upon boot using `spectra.ts`:

```typescript
import { logNetworkBindings, triggerGatewayDnsRescan } from "./spectra";

// Log socket interfaces to guarantee external interface binding
logNetworkBindings(process.env.HOST || "0.0.0.0", 3000, "Backend Application");

// Notify gateway to re-resolve upstream DNS and update socket pool
triggerGatewayDnsRescan(1000);
```

For full helper implementations, see [Application Integration Guide](app-integration.md).

---

## 4. Deployment Commands (Example: Fly.io)

### Step 1: Deploy Infrastructure Appliance (`app-infra`)
```bash
# Create Fly app for infrastructure
fly apps create app-infra

# Create persistent storage for NATS JetStream
fly volumes create nats_data --size 1 --region ord

# Deploy using turnkey NATS Dockerfile
fly deploy --dockerfile docker/appliances/Dockerfile.nats
```

### Step 2: Deploy Application & Worker (`app-backend`)
```bash
# Create private application app
fly apps create app-backend

# Set production secrets
fly secrets set \
  DATABASE_URL="postgres://user:pass@db.provider.internal:5432/app?sslmode=require" \
  NATS_URL="nats://app-infra.internal:4222" \
  SPECTRA_ADMIN_URL="http://app-infra.internal:8000" \
  SPECTRA_ADMIN_TOKEN="sk_admin_live_secret"

# Deploy private machine
fly deploy
```

---

## 5. Verification & Health Checks

1. **Gateway Liveness:** `curl -I https://app.example.com/healthz` returns HTTP 200 `OK`.
2. **Broker Readiness:** `curl -I https://app.example.com/livez` verifies NATS connectivity.
3. **GraphQL Mode A (Query):** Standard queries proxy directly to upstream with sub-millisecond gateway overhead.
4. **GraphQL Mode B (Mutation):** `mutation { submitOrder(id: "100") { commandId status hlc } }` returns HTTP 200 `ACCEPTED` in $< 1\text{ ms}$.
5. **SpectraHub Admin Console:** Open `https://app.example.com/admin` to inspect real-time throughput, active routes, NATS consumer lag, and worker telemetry.
