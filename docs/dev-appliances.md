# SpectraGQL Dev Appliances & Admin Console

SpectraGQL Dev Appliances are **single-container turnkey environments** designed to get local development, testing, and strangler-fig migration running in seconds with **zero external infrastructure dependencies**.

---

## 1. Architectural Model

Each appliance runs as a self-contained container with **`oxmgr`** (a lightweight pure-Rust process manager) as PID 1, supervising:
1. **The Broker Daemon** (NATS JetStream, Nisshi Kafka, Redis/Valkey, Apache Iggy, or Redpanda).
2. **The SpectraGQL Gateway** running Cloudflare Pingora with Dual-Pillar routing (Modes A & B), distributed idempotency, realtime subscriptions, and an administrative control plane.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 SPECTRAGQL DEV APPLIANCE (Single Container)                 │
│                                                                             │
│   PID 1: oxmgr (Pure-Rust Process Supervisor)                               │
│   ├── Broker Daemon  ──(listening on internal & exposed port)               │
│   └── SpectraGQL     ──(listening on port 8000)                             │
│                                                                             │
│   Client Ingress (:8000):                                                   │
│   • POST /graphql       ───> Mode A (Proxy to Host Monolith)                │
│                              Mode B (Terminates at Edge with 202 Receipt)   │
│   • GET  /graphql       ───> Realtime Subscriptions (graphql-transport-ws)  │
│   • GET  /healthz       ───> Instant HTTP 200 Liveness Probe                │
│   • GET  /livez         ───> Broker Connectivity Readiness Probe            │
│   • GET  /admin         ───> Single-Page Admin Dashboard                    │
│   • GET  /admin/api/*   ───> Secured Admin REST API                         │
│                                                                             │
│   Egress:                                                                   │
│   • Host Upstream Hop   ──(host.docker.internal:4000)──> Host Developer App │
│   • Broker Dispatch     ──(localhost:<broker_port>)────> Embedded Broker    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### BYOService (Bring Your Own Service)
Appliances do **not** bundle mock services or demoware. You point the appliance at your real local backend running on your host machine (default: `host.docker.internal:4000`).

---

## 2. 1-Command Startup

Choose your streaming broker and launch the appliance:

### 🌟 NATS JetStream (Swiss Army Knife)
```bash
docker run -p 8000:8000 -p 4222:4222 -p 8222:8222 spectragql/appliance:nats
```

### ⚡ Nisshi (Pure-Rust Kafka Wire Protocol)
```bash
docker run -p 8000:8000 -p 9092:9092 spectragql/appliance:nisshi
```

### 🔄 Redis / Valkey (Streams + Idempotency Cache)
```bash
docker run -p 8000:8000 -p 6379:6379 spectragql/appliance:redis
```

### 🚀 Apache Iggy (NVMe-Optimized Append-Only Streaming)
```bash
docker run -p 8000:8000 -p 8090:8090 -p 3000:3000 spectragql/appliance:iggy
```

### 🏢 Redpanda Kafka (Enterprise Kafka)
```bash
docker run -p 8000:8000 -p 9092:9092 spectragql/appliance:redpanda
```

---

## 3. Host Upstream Network Configuration (BYOService)

By default, the gateway resolves the default upstream GraphQL monolith at `host.docker.internal:4000`.

### macOS & Windows (Docker Desktop)
Works automatically out-of-the-box. Ensure your local GraphQL backend is listening on port `4000`.

### Linux (Docker Engine)
Include the `--add-host` flag so the container can resolve the host machine:
```bash
docker run --add-host=host.docker.internal:host-gateway \
  -p 8000:8000 -p 4222:4222 spectragql/appliance:nats
```

### Custom Host Address or Port
Override using the `SPECTRA_UPSTREAM_ADDR` environment variable:
```bash
docker run -e SPECTRA_UPSTREAM_ADDR="192.168.1.50:8080" \
  -p 8000:8000 -p 4222:4222 spectragql/appliance:nats
```

---

## 4. Admin Console & Security Controls

SpectraGQL includes an administrative interface accessible at `http://localhost:8000/admin`.

### Security Configuration (`spectra.toml`)
Admin routes are governed by explicit security boundaries:

```toml
[admin]
enabled = true              # Master toggle: set false to disable all /admin routes
bind_addr = "0.0.0.0:8000"  # Can bind to dedicated port (e.g., "127.0.0.1:8001")
path_prefix = "/admin"      # Base URL path
enable_ui = true            # Set false to serve REST API only without HTML/JS

# IP & CIDR Allowlist:
# Unauthorized IPs receive HTTP 403 Forbidden
allowed_ips = [
  "127.0.0.1",
  "::1",
  "10.0.0.0/8",
  "172.16.0.0/12",
  "192.168.0.0/16"
]
```

---

## 5. Admin REST API Reference (`/admin/api/v1/*`)

Administrative endpoints are designed as clean, zero-overhead REST APIs:

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/admin` | `GET` | Single-page visual admin dashboard (HTML/JS) |
| `/admin/api/v1/status` | `GET` | Gateway version, uptime, active operational modes, and broker status |
| `/admin/api/v1/routes` | `GET` | Configured routes, targets, operational modes, and upstream addresses |
| `/admin/api/v1/config` | `GET` / `POST` | Inspects or hot-reloads runtime configuration with `ArcSwap` |
| `/admin/api/v1/dns/rescan` | `POST` | Triggers dynamic upstream DNS re-resolution and socket pool refresh |
| `/admin/api/v1/workers` | `GET` | Discovers downstream consumers, broker consumer lag, and worker telemetry |
| `/admin/api/v1/schema` | `GET` | Introspected upstream GraphQL schema with **Strangler-Fig Coverage Analysis** |
| `/admin/api/v1/schema/refresh` | `POST` | Triggers an immediate re-fetch of upstream GraphQL introspection |
| `/admin/api/v1/subscriptions` | `GET` | Active WebSocket client connections and registered topic subscriptions |
| `/admin/api/v1/idempotency` | `GET` | In-flight idempotency locks, LRU capacity, and active record count |
| `/admin/api/v1/security/lockdown`| `POST` | Emergency security lockdown; flips atomic killswitches cluster-wide |
| `/healthz` | `GET` | Instant HTTP 200 liveness probe (does not touch upstream) |
| `/livez` | `GET` | Broker readiness check probe |

### Example: Status Check
```bash
curl -s http://localhost:8000/admin/api/v1/status | jq .
```
```json
{
  "version": "0.1.0",
  "uptime_seconds": 342,
  "mode_a_enabled": true,
  "mode_a_dispatch_policy": "ResponseWithFailure",
  "mode_a_timeout_ms": 3000,
  "mode_b_routes_count": 1,
  "broker_method": "NATS",
  "broker_addr": "127.0.0.1:4222",
  "broker_status": "online"
}
```

### Example: Strangler-Fig Schema Coverage
```bash
curl -s http://localhost:8000/admin/api/v1/schema | jq .
```
```json
{
  "total_mutations": 12,
  "strangled_count": 4,
  "mode_b_count": 2,
  "monolith_fallback_count": 6,
  "drift_count": 0,
  "coverage_percent": 50.0,
  "mutations": [
    {
      "field_name": "adjustInventory",
      "classification": "Strangled",
      "target_upstream": "inventory",
      "mode": "A",
      "receipt_status": null
    },
    {
      "field_name": "importCatalog",
      "classification": "EdgeTerminatedModeB",
      "target_upstream": null,
      "mode": "B",
      "receipt_status": "ACCEPTED"
    }
  ],
  "drifted_routes": []
}
```

---

## 6. Inspecting Streams Directly From Host CLIs

Because broker ports are mapped to the host, you can use your preferred CLI tools:

### NATS CLI
```bash
# Subscribe to all events dispatched by SpectraGQL
nats sub "spectra.>"

# Inspect JetStream streams
nats stream list
```

### Kafka / Nisshi / Redpanda (`kcat`)
```bash
# Tail events published to Kafka
kcat -b localhost:9092 -C -t spectra.events
```

### Redis CLI
```bash
# Read events from Redis Stream
redis-cli XREAD COUNT 10 STREAMS spectra:events 0-0

# Inspect active idempotency records
redis-cli KEYS "spectra:idempotency:*"
```
