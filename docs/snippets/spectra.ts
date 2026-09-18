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
 *
 * In modern container environments (Railway, Fly.io, AWS ECS), redeploying an upstream
 * service assigns a new private IP. Triggering this hook notifies SpectraGQL's dynamic
 * DNS re-resolution engine (POST /admin/api/v1/dns/rescan) to immediately refresh its
 * upstream socket pools with zero downtime.
 *
 * Requirements:
 * - SPECTRA_ADMIN_URL must be set to the base server origin and port (e.g. http://gateway.internal:8000).
 *   Never include the /admin path prefix in SPECTRA_ADMIN_URL.
 * - SPECTRA_ADMIN_TOKEN (optional): Bearer token if admin endpoint authentication is enabled.
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
      // Guarantee base URL without trailing slashes or duplicate /admin paths
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
 * Extracts standard SpectraGQL request tracking and causal ordering headers:
 * - x-spectra-request-id: Monotonic UUIDv7 request identifier.
 * - x-spectra-hlc: Hybrid Logical Clock timestamp ("<physical_ms>.<logical_counter>").
 * - x-spectra-app: Application identifier if configured.
 * - idempotency-key: Inbound idempotency key from client.
 */
export function extractSpectraContext(headers: Headers): SpectraContext {
  return {
    requestId: headers.get("x-spectra-request-id") || undefined,
    hlc: headers.get("x-spectra-hlc") || undefined,
    appId: headers.get("x-spectra-app") || undefined,
    idempotencyKey: headers.get("idempotency-key") || undefined,
  };
}
