# Application Integration Guide: Connecting Upstream Services to SpectraGQL

This guide details the recommended architectural patterns, configuration standards, and helper utilities for upstream services (written in Node.js, Bun, Go, Python, or Rust) that sit behind or interact with **SpectraGQL**.

---

## 1. Upstream Service Responsibilities in CQRS

When integrating with SpectraGQL, understand your application's role across the two primary operational modes:

### Mode A (Sync Forwarding — The Workhorse)
- **Role:** Your service is the primary GraphQL upstream execution target.
- **Ingress:** SpectraGQL forwards GraphQL `Query` and Mode A `Mutation` requests over HTTP POST.
- **Headers:** SpectraGQL injects diagnostic and causal tracking headers into every forwarded request:
  - `x-spectra-request-id`: A monotonic UUIDv7 uniquely identifying the request.
  - `x-spectra-hlc`: A Hybrid Logical Clock timestamp (`<physical_ms>.<logical_counter>`) for distributed causal ordering.
  - `x-spectra-app`: The configured client application identifier (if provided).
  - `idempotency-key`: The client-supplied idempotency key (if provided).
- **Execution & Response:** Your service executes the fast local database write and returns standard GraphQL JSON. SpectraGQL captures the response and asynchronously emits a `CompletionEvent` to the configured event sink (NATS JetStream, Apache Kafka, Redis Streams, or Apache Iggy) for downstream event choreography.

### Mode B (Async Edge Command — Pure CQRS)
- **Role:** Your upstream service is **not** on the synchronous HTTP request path.
- **Flow:** SpectraGQL terminates the mutation at the gateway edge in $< 1\text{ ms}$, issues a deterministic **Command Receipt** (`ACCEPTED`), and dispatches the raw command payload to the message broker.
- **Workers:** Downstream consumers, background workers, or Temporal sagas consume the command directly from the message broker.

---

## 2. Dynamic Upstream DNS Re-Resolution Engine (`POST /admin/api/v1/dns/rescan`)

### The Cloud Ephemeral IP Problem
In modern container platforms (Railway, Fly.io, Kubernetes, AWS ECS), private mesh networking assigns dynamic IP addresses to containers:
1. When your application service redeploys, its internal DNS name (e.g. `my-app.railway.internal`) resolves to a **new private IP address**.
2. Traditional reverse proxies cache resolved upstream IP addresses indefinitely or hold stale TCP connections, resulting in `502 Bad Gateway` or `Connection Refused` errors until the gateway is manually restarted.

### The Solution: Post-Deployment Signal Hook
SpectraGQL features an administrative endpoint:
```http
POST /admin/api/v1/dns/rescan
```
Calling this endpoint triggers SpectraGQL's background DNS resolver to immediately re-resolve all upstream hostnames and update its internal socket pools with **zero downtime**.

### The `SPECTRA_ADMIN_URL` Base Origin Standard
When configuring upstream services, deployment hooks, or CI/CD pipelines to trigger DNS rescans or report telemetry, the environment variable `SPECTRA_ADMIN_URL` must specify **strictly the base server origin and port**, and must **NEVER** include the `/admin` path suffix:

- ✅ **Correct:** `SPECTRA_ADMIN_URL=http://spectragql.railway.internal:8000` or `http://localhost:8000`
- ❌ **Incorrect:** `SPECTRA_ADMIN_URL=http://spectragql.railway.internal:8000/admin` (causes duplicate pathing: `/admin/admin/api/...`)
- ❌ **Incorrect:** `SPECTRA_ADMIN_URL=spectragql.railway.internal` (missing scheme `http://` and listening port `:8000`)

In platform-specific environments (like Railway private networks), `${{SpectraGQL.RAILWAY_PRIVATE_DOMAIN}}` resolves only to the bare hostname. Upstream services must configure:
```bash
SPECTRA_ADMIN_URL=http://${{SpectraGQL.RAILWAY_PRIVATE_DOMAIN}}:8000
```

---

## 3. Reference Implementation: `spectra.ts`

For Node.js and Bun applications (e.g., Express, Hono, Fastify), the following canonical integration utility is provided in [`docs/snippets/spectra.ts`](snippets/spectra.ts):

```typescript
import os from "node:os";

export interface SpectraContext {
  requestId?: string;
  hlc?: string;
  appId?: string;
  idempotencyKey?: string;
}

/**
 * Pedantically enumerates and logs active network interfaces and socket bindings.
 * Ensures services bind to dual-stack IPv6 & IPv4 ("::") or IPv4 ("0.0.0.0") so
 * upstream container mesh networks (Railway, Fly.io, K8s) can route traffic reliably.
 */
export function logNetworkBindings(hostname: string, port: number, appName = "Upstream Service"): void {
  console.log("\n==================================================");
  console.log(`🌐 ${appName} Initializing`);
  console.log(`   Configured HOST: "${hostname}"`);
  console.log(`   Configured PORT: ${port}`);

  if (hostname === "::") {
    console.log("   Socket Binding : Dual-Stack IPv6 & IPv4 (all interfaces)");
    console.log(`   IPv6 Wildcard  : http://[::]:${port}`);
    console.log(`   IPv4 Wildcard  : http://0.0.0.0:${port}`);
  } else if (hostname === "0.0.0.0") {
    console.log("   Socket Binding : IPv4 Only (all interfaces)");
    console.log(`   IPv4 Wildcard  : http://0.0.0.0:${port}`);
  } else {
    const display = hostname.includes(":") ? `[${hostname}]` : hostname;
    console.log(`   Socket Binding : http://${display}:${port}`);
  }

  try {
    const ifaces = os.networkInterfaces();
    console.log("   Active Network Interfaces:");
    for (const [name, addrs] of Object.entries(ifaces)) {
      if (!addrs) continue;
      for (const addr of addrs) {
        const isV6 = addr.family === "IPv6";
        const ip = isV6 ? `[${addr.address}]` : addr.address;
        const type = addr.internal ? "loopback" : "external";
        console.log(`     • ${name.padEnd(8)} [${addr.family.padEnd(4)}] -> http://${ip}:${port} (${type})`);
      }
    }
  } catch (e: any) {
    console.warn("   Could not enumerate network interfaces:", e.message);
  }
  console.log("==================================================\n");
}

/**
 * Dispatches a non-blocking post-deployment DNS rescan notification to SpectraGQL.
 */
export function triggerGatewayDnsRescan(delayMs = 1000): void {
  const rawUrl = process.env.SPECTRA_ADMIN_URL || process.env.SPECTRAGQL_ADMIN_URL;
  const token = process.env.SPECTRA_ADMIN_TOKEN || process.env.SPECTRAGQL_ADMIN_TOKEN;
  if (!rawUrl) return;

  setTimeout(async () => {
    try {
      let url = rawUrl.trim();
      if (!/^https?:\/\//i.test(url)) {
        url = `http://${url}`;
      }
      const baseUrl = url.replace(/\/+$/, "").replace(/\/admin\/?$/i, "");
      const rescanUrl = `${baseUrl}/admin/api/v1/dns/rescan`;
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      if (token) {
        headers["Authorization"] = `Bearer ${token}`;
      }
      const resp = await fetch(rescanUrl, { method: "POST", headers });
      if (resp.ok) {
        const body = await resp.json().catch(() => ({}));
        console.log("✅ SpectraGQL DNS rescan triggered successfully:", JSON.stringify(body));
      } else {
        console.warn(`⚠️ SpectraGQL DNS rescan returned HTTP ${resp.status}`);
      }
    } catch (err: any) {
      console.warn("⚠️ Failed to trigger SpectraGQL DNS rescan:", err.message);
    }
  }, delayMs);
}

/**
 * Extracts standard SpectraGQL request tracking headers.
 */
export function extractSpectraContext(headers: Headers): SpectraContext {
  return {
    requestId: headers.get("x-spectra-request-id") || undefined,
    hlc: headers.get("x-spectra-hlc") || undefined,
    appId: headers.get("x-spectra-app") || undefined,
    idempotencyKey: headers.get("idempotency-key") || undefined,
  };
}
```

---

## 4. Usage in Upstream Application Bootstrap

### Step 1: Server Startup & Network Logging
Ensure your server listens on `0.0.0.0` or `::` (avoid `127.0.0.1` / `localhost`, which makes containers unreachable to the gateway mesh):

```typescript
// server.ts (e.g. Hono or Express)
import { Hono } from "hono";
import { logNetworkBindings, triggerGatewayDnsRescan, extractSpectraContext } from "./spectra";

const app = new Hono();
const port = parseInt(process.env.PORT || "3000", 10);
const host = process.env.HOST || "0.0.0.0";

// Pedantically log network interfaces
logNetworkBindings(host, port, "My Order Microservice");

// Trigger gateway DNS rescan after server is ready
triggerGatewayDnsRescan(1500);

export default {
  port,
  hostname: host,
  fetch: app.fetch,
};
```

### Step 2: Request Context & Correlation Logging
In your GraphQL resolver or HTTP middleware, extract the tracing context:

```typescript
app.post("/graphql", async (c) => {
  const ctx = extractSpectraContext(c.req.raw.headers);

  console.log(`[SpectraGQL Request] ID=${ctx.requestId} HLC=${ctx.hlc} IdempotencyKey=${ctx.idempotencyKey}`);

  // Forward context to your database or logging span
  return c.json({
    data: {
      order: { id: "123", status: "PENDING" }
    }
  });
});
```

---

## 5. Security & Authentication

If your SpectraGQL administrative endpoints are protected with an administrative token (`[admin] token = "..."` or `SPECTRA_ADMIN_TOKEN`):

1. Export the token in your upstream service's environment:
   ```bash
   SPECTRA_ADMIN_TOKEN="sk_admin_live_secret"
   ```
2. `triggerGatewayDnsRescan()` automatically includes `Authorization: Bearer <SPECTRA_ADMIN_TOKEN>` in its POST request.
3. Requests originating without a matching token will receive `HTTP 401 Unauthorized`.
