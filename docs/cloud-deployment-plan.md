# CoEval & Multi-App Cloud Deployment Plan: 2-Container Fly.io Topology

This document details the production cloud deployment architecture for **Open CoEval**, **SpectraGQL**, **NATS JetStream**, and **NeonDB PostgreSQL**.

---

## 1. Architectural Topology Overview

Rather than splitting the frontend onto Cloudflare Workers (which introduces cross-cloud latency for GraphQL Mode A reads and cold database connections), the entire stack is consolidated into **2 Fly.io machines** connecting to **NeonDB**:

```
                                  PUBLIC INTERNET (HTTPS / 443)
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Fly.io Machine 1: Infrastructure Appliance ("coeval-infra")                            │
│                                                                                        │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ SpectraGQL Gateway (:8000)                                                     │   │
│   │  • Reverse-proxies / and /public/*  ──► coeval-app.internal:3000               │   │
│   │  • Mode A Queries                   ──► coeval-app.internal:3000 (NeonDB read) │   │
│   │  • Mode B Mutations (< 1ms Receipt) ──► 127.0.0.1:4222 (Local NATS JetStream)  │   │
│   │  • SpectraHub Admin Console         ──► :8000/admin                            │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
│                                       │ Localhost (Sub-ms Dispatch)                    │
│                                       ▼                                                │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ NATS JetStream Server (:4222)                                                  │   │
│   │  • Subject: mutation.* on stream SPECTRA                                       │   │
│   │  • Persistent Volume (1GB NVMe) mounted at /data                               │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
└───────────────────────────────────────┼────────────────────────────────────────────────┘
                                        │ Fly.io 6PN Private Mesh
                                        │ (coeval-infra.internal)
                                        ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Fly.io Machine 2: Application & Worker ("coeval-app", Private, No Public IP)           │
│                                                                                        │
│   ┌────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Hono Web & GraphQL Engine (:3000)                                              │   │
│   │  • GET  / and /public/* ── Alpine.js + table.js client bundle                  │   │
│   │  • POST /graphql        ── Executes queries via postgres.js pool               │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
│                                       │                                                │
│   ┌───────────────────────────────────┴────────────────────────────────────────────┐   │
│   │ CoEval Projection Worker (scripts/projection_worker.ts)                        │   │
│   │  • Durable consumer: coeval-projection-worker                                  │   │
│   │  • Processes votes/observations, updates coeval_votes, recalculates scores     │   │
│   │  • Reports heartbeat & logs to SpectraHub Admin API                            │   │
│   └───────────────────────────────────┬────────────────────────────────────────────┘   │
└───────────────────────────────────────┼────────────────────────────────────────────────┘
                                        ▼ (TLS Connection Pool)
                              [ NeonDB PostgreSQL ]
```

---

## 2. Component Specifications

### Container 1: `coeval-infra` (SpectraGQL + NATS Appliance)
* **Image:** Built from [`SpectraGQL/docker/appliances/Dockerfile.nats`](file:///Users/bmo/code/SpectraGQL/docker/appliances/Dockerfile.nats).
* **Supervisor:** `oxmgr` (pure-Rust supervisor managing `nats-server` and `spectragql`).
* **Sizing:** Shared CPU 1x, 512MB RAM.
* **Volume:** 1GB Fly persistent volume mounted at `/data` for NATS JetStream write-ahead log (WAL).
* **Networking:**
  * Public Ports: `80` (HTTP redirect) and `443` (TLS) forwarding to container port `8000`.
  * Private Mesh (Fly 6PN): Exposes `coeval-infra.internal:4222` (NATS) and `coeval-infra.internal:8000` (Admin API).
* **Configuration (`spectra.toml`):**
  ```toml
  bind_addr = "0.0.0.0:8000"

  [upstream]
  addr = "coeval-app.internal:3000"
  name = "coeval_hono"

  [gql]
  paths = "/gql,/graphql"
  ops_to_dispatch = "query, mutation, subscription"

  [gql.mode_a]
  enabled = true
  timeout_ms = 5000

  [gql.routes.record_vote]
  operation = "recordVote"
  mode = "B"
  receipt_status = "ACCEPTED"

  [gql.routes.record_observations]
  operation = "recordObservationBatch"
  mode = "B"
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

### Container 2: `coeval-app` (Hono Server + Projection Worker)
* **Image:** `oven/bun:1` base image.
* **Sizing:** Shared CPU 1x, 512MB RAM.
* **Networking:** Private only (no public IP; reachable exclusively via Fly 6PN private network from Container 1).
* **Processes Supervised:**
  1. `bun src/index.ts` (listening on `0.0.0.0:3000`).
  2. `bun scripts/projection_worker.ts`.
* **Environment Variables:**
  * `PORT=3000`
  * `DATABASE_URL=postgres://<user>:<password>@<ep-pooler>.neon.tech/coeval?sslmode=require`
  * `NATS_URL=nats://coeval-infra.internal:4222`
  * `SPECTRA_ADMIN_URL=http://coeval-infra.internal:8000`

---

## 3. Database Layer: NeonDB PostgreSQL
* **Compute:** Serverless autoscaling PostgreSQL 16.
* **Pooler:** Use Neon's connection pooler URL for high concurrency.
* **Tables:**
  * `coeval_entities` (company metadata, descriptions, JSONB metrics).
  * `coeval_votes` (user upvotes with monotonic HLC timestamps).
  * `coeval_observations` (crowdsourced biotech attribute observations).

---

## 4. Cloudflare Role (Optional CDN Proxy)
* Do **not** deploy Cloudflare Workers.
* Set Cloudflare DNS record (`coeval.bio`) to Proxied ("Orange Cloud") pointing to Fly.io Anycast IP.
* Cloudflare provides free DDoS mitigation, SSL termination, and edge caching for static assets (`/public/table.js`, images) without executing any custom worker code.

---

## 5. Deployment Commands

### Step 1: Deploy Infrastructure Appliance (`coeval-infra`)
```bash
cd /Users/bmo/code/SpectraGQL

# Create Fly app
fly apps create coeval-infra

# Create persistent storage for NATS JetStream
fly volumes create nats_data --size 1 --region ord

# Deploy using turnkey NATS Dockerfile
fly deploy --dockerfile docker/appliances/Dockerfile.nats
```

### Step 2: Deploy Application & Worker (`coeval-app`)
```bash
cd /Users/bmo/code/CoEval/OpenCoEval

# Create private Fly app
fly apps create coeval-app

# Set production secrets
fly secrets set \
  DATABASE_URL="postgres://user:pass@ep-xyz.neon.tech/coeval?sslmode=require" \
  NATS_URL="nats://coeval-infra.internal:4222" \
  SPECTRA_ADMIN_URL="http://coeval-infra.internal:8000"

# Deploy private machine
fly deploy
```

---

## 6. Verification & Health Checks
1. **Frontend UI:** `curl -I https://coeval.bio/` returns HTTP 200 with HTML view.
2. **GraphQL Mode A (Query):** Querying `companies` returns JSON entity list in `< 50ms`.
3. **GraphQL Mode B (Mutation):** `mutation { recordVote(companyId: "...", value: 1) { status hlc } }` returns HTTP 200 `ACCEPTED` in `< 1ms`.
4. **SpectraHub Admin Console:** Open `https://coeval.bio/admin` to inspect real-time throughput, NATS consumer lag, and projection worker log telemetry.
