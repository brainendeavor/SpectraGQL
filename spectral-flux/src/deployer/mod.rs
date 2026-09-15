pub mod guard;
pub mod registry;
pub mod ssrf_shield;

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::sync::{Arc, RwLock};

pub use guard::DeployerGuard;
pub use registry::{DeploymentAuditEvent, DeployerRegistry, FluxcellRecord, FluxcellStatus};
pub use ssrf_shield::SSRFShield;

pub struct FluxcellDeployer {
    config: crate::config::DeployerConfig,
    guard: Arc<DeployerGuard>,
    registry: Arc<DeployerRegistry>,
    ssrf_shield: Arc<SSRFShield>,
    wasm_host: Arc<crate::wasm::WasmHost>,
    router: Arc<RwLock<crate::http::FluxRouter>>,
    http_client: reqwest::Client,
}

impl FluxcellDeployer {
    pub fn new(
        config: crate::config::DeployerConfig,
        guard: Arc<DeployerGuard>,
        registry: Arc<DeployerRegistry>,
        wasm_host: Arc<crate::wasm::WasmHost>,
        router: Arc<RwLock<crate::http::FluxRouter>>,
    ) -> Self {
        let ssrf_shield = Arc::new(SSRFShield::new(
            config.allowed_artifact_hosts.clone(),
            config.require_https,
            config.block_private_networks,
        ));

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();

        Self {
            config,
            guard,
            registry,
            ssrf_shield,
            wasm_host,
            router,
            http_client,
        }
    }

    pub fn config(&self) -> &crate::config::DeployerConfig {
        &self.config
    }

    pub fn guard(&self) -> &Arc<DeployerGuard> {
        &self.guard
    }

    pub fn registry(&self) -> &Arc<DeployerRegistry> {
        &self.registry
    }

    /// Fetches a remote WASM artifact via HTTPS, verifies its SHA-256 hash, and stages it.
    pub async fn stage_remote_artifact(
        &self,
        name: &str,
        artifact_url: &str,
        expected_sha256: &str,
        mount_path: &str,
        timeout_ms: Option<u64>,
        max_memory_mb: Option<usize>,
        auth_header: Option<&str>,
    ) -> Result<FluxcellRecord> {
        if !self.guard.is_external_deploy_allowed() {
            let err_msg = "External CI/CD deployments are locked down or disabled in configuration";
            self.registry.record_audit("REJECT", name, expected_sha256, err_msg);
            return Err(anyhow!(err_msg));
        }

        // 1. SSRF and Host Whitelist Validation
        let validated_url = self.ssrf_shield.validate_url(artifact_url).await.map_err(|e| {
            self.registry.record_audit("REJECT", name, expected_sha256, &e.to_string());
            e
        })?;

        // 2. Fetch artifact stream with size ceiling
        let mut req_builder = self.http_client.get(validated_url.as_str());
        if let Some(auth) = auth_header {
            req_builder = req_builder.header("Authorization", auth);
        }

        let resp = req_builder
            .send()
            .await
            .with_context(|| format!("Failed to connect to artifact URL '{}'", artifact_url))?;

        if !resp.status().is_success() {
            let err_msg = format!("Artifact server responded with HTTP {}", resp.status());
            self.registry.record_audit("REJECT", name, expected_sha256, &err_msg);
            return Err(anyhow!(err_msg));
        }

        let body_bytes = resp
            .bytes()
            .await
            .context("Failed to read artifact body stream")?
            .to_vec();

        if body_bytes.len() > self.config.max_wasm_size_bytes {
            let err_msg = format!(
                "Artifact size {} bytes exceeds max limit {} bytes",
                body_bytes.len(),
                self.config.max_wasm_size_bytes
            );
            self.registry.record_audit("REJECT", name, expected_sha256, &err_msg);
            return Err(anyhow!(err_msg));
        }

        // 3. Cryptographic Checksum Pinning
        let mut hasher = Sha256::new();
        hasher.update(&body_bytes);
        let computed_sha256 = format!("{:x}", hasher.finalize());

        if !computed_sha256.eq_ignore_ascii_case(expected_sha256) {
            let err_msg = format!(
                "SHA-256 checksum mismatch: expected '{}', computed '{}'",
                expected_sha256, computed_sha256
            );
            self.registry.record_audit("REJECT", name, &computed_sha256, &err_msg);
            return Err(anyhow!(err_msg));
        }

        // 4. Stage and optionally activate
        self.stage_bytes(
            name,
            body_bytes,
            mount_path,
            Some(artifact_url.to_string()),
            &computed_sha256,
            timeout_ms,
            max_memory_mb,
        )
    }

    /// Stages an uploaded WASM byte buffer directly (for local dev / SpectraHub UI).
    pub fn stage_uploaded_artifact(
        &self,
        name: &str,
        bytes: Vec<u8>,
        mount_path: &str,
        timeout_ms: Option<u64>,
        max_memory_mb: Option<usize>,
    ) -> Result<FluxcellRecord> {
        if !self.guard.is_dev_upload_allowed() {
            let err_msg = "Direct dev uploads are locked down or disabled in configuration";
            self.registry.record_audit("REJECT", name, "upload", err_msg);
            return Err(anyhow!(err_msg));
        }

        if bytes.len() > self.config.max_wasm_size_bytes {
            let err_msg = format!(
                "Uploaded artifact size {} bytes exceeds max limit {} bytes",
                bytes.len(),
                self.config.max_wasm_size_bytes
            );
            self.registry.record_audit("REJECT", name, "upload", &err_msg);
            return Err(anyhow!(err_msg));
        }

        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let computed_sha256 = format!("{:x}", hasher.finalize());

        self.stage_bytes(
            name,
            bytes,
            mount_path,
            None,
            &computed_sha256,
            timeout_ms,
            max_memory_mb,
        )
    }

    fn stage_bytes(
        &self,
        name: &str,
        bytes: Vec<u8>,
        mount_path: &str,
        artifact_url: Option<String>,
        sha256: &str,
        timeout_ms: Option<u64>,
        max_memory_mb: Option<usize>,
    ) -> Result<FluxcellRecord> {
        // 1. Basic WASM Header Check
        if !bytes.starts_with(b"\0asm") {
            let err_msg = "Invalid binary: payload does not start with WASM magic header";
            self.registry.record_audit("REJECT", name, sha256, err_msg);
            return Err(anyhow!(err_msg));
        }

        // 2. Reserved Mount Path Check
        if crate::http::FluxRouter::is_reserved_mount_path(mount_path) {
            let err_msg = format!(
                "Mount path '{}' is reserved by system infrastructure",
                mount_path
            );
            self.registry.record_audit("REJECT", name, sha256, &err_msg);
            return Err(anyhow!(err_msg));
        }

        // 3. Pre-compile with Wasmtime to validate bytecode and extract metadata
        let initial_cfg = crate::wasm::FluxcellWasmConfig::default();
        let module = wasmtime::Module::new(self.wasm_host.engine(), &bytes)
            .map_err(|e| anyhow!("WASM compilation failed: {:#}", e))?;

        // Query guest exports
        let meta = self.inspect_guest_metadata(&module, &initial_cfg)?;

        // 3.5. Explicit Execution Configuration Resolution (Fail-Fast)
        if timeout_ms.is_none() && meta.profile.is_none() && meta.timeout_ms.is_none() {
            let err_msg = format!(
                "Fluxcell '{}' rejected: execution profile must be explicitly configured (e.g. profile = 'standard', 'extended', 'batch') or explicit timeout_ms provided",
                name
            );
            self.registry.record_audit("REJECT", name, sha256, &err_msg);
            return Err(anyhow!(err_msg));
        }

        let profile_name = meta.profile.as_deref().unwrap_or("standard");
        let (prof_timeout, prof_instances, prof_offload, prof_mem) = match profile_name {
            "extended" => (120_000, 4, crate::config::OffloadStrategy::BlockingPool, 32),
            "batch" => (300_000, 2, crate::config::OffloadStrategy::DedicatedWorker, 64),
            "standard" => (10_000, 16, crate::config::OffloadStrategy::BlockingPool, 16),
            other => {
                let err_msg = format!("Fluxcell '{}' specifies unknown profile '{}'", name, other);
                self.registry.record_audit("REJECT", name, sha256, &err_msg);
                return Err(anyhow!(err_msg));
            }
        };

        let timeout = timeout_ms.or(meta.timeout_ms).unwrap_or(prof_timeout);
        if timeout < crate::config::MIN_TIMEOUT_MS || timeout > crate::config::MAX_TIMEOUT_MS {
            let err_msg = format!(
                "Fluxcell '{}' timeout_ms ({}) out of bounds: must be between {}ms and {}ms",
                name, timeout, crate::config::MIN_TIMEOUT_MS, crate::config::MAX_TIMEOUT_MS
            );
            self.registry.record_audit("REJECT", name, sha256, &err_msg);
            return Err(anyhow!(err_msg));
        }

        let max_instances = prof_instances;
        let offload = prof_offload;
        let final_mem_mb = max_memory_mb.or(meta.max_memory_mb).unwrap_or(prof_mem);
        let max_memory_bytes = final_mem_mb * 1024 * 1024;

        let wasm_cfg = crate::wasm::FluxcellWasmConfig {
            profile: profile_name.to_string(),
            timeout_ms: timeout,
            max_memory_bytes,
            max_instances,
            offload,
            circuit_breaker: crate::wasm::CircuitBreakerConfig::default(),
        };

        // 4. Check for route collisions in active router
        {
            let router_lock = self.router.read().unwrap();
            let clean_mount = crate::http::clean_path_prefix(mount_path);
            for r in &meta.routes {
                let clean_rel = crate::http::clean_path_suffix(&r.relative_path);
                let full_path = if clean_mount.is_empty() && clean_rel.is_empty() {
                    "/".to_string()
                } else {
                    format!("{}{}", clean_mount, clean_rel)
                };
                let method = r.method.to_uppercase();

                // If already registered by another cell, fail-fast
                if let Ok(m) = router_lock.lookup(&method, &full_path) {
                    if m.fluxcell_name != name {
                        let err_msg = format!(
                            "Route collision: path '{} {}' is already claimed by active fluxcell '{}'",
                            method, full_path, m.fluxcell_name
                        );
                        self.registry.record_audit("REJECT", name, sha256, &err_msg);
                        return Err(anyhow!(err_msg));
                    }
                }
            }
        }

        // 5. Save .wasm file to persistent storage directory
        let short_sha = &sha256[..8.min(sha256.len())];
        let filename = format!("{}-{}.wasm", name, short_sha);
        let target_path = self.registry.storage_dir().join(&filename);
        std::fs::write(&target_path, &bytes)
            .with_context(|| format!("Failed to write WASM file '{:?}'", target_path))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut record = FluxcellRecord {
            name: name.to_string(),
            version: meta.version.unwrap_or_else(|| "0.1.0".to_string()),
            git_hash: meta.git_hash,
            build_time: meta.build_time,
            sha256: sha256.to_string(),
            artifact_url,
            mount_path: mount_path.to_string(),
            status: FluxcellStatus::Staged,
            routes: meta.routes.clone(),
            subscriptions: meta.subscriptions.clone(),
            profile: profile_name.to_string(),
            timeout_ms: timeout,
            max_memory_bytes,
            max_instances,
            offload,
            installed_at: now,
            activated_at: None,
            wasm_file: filename,
        };

        // 6. Two-phase activation decision
        if self.config.auto_activate {
            self.hot_activate_cell(&record, &bytes, wasm_cfg)?;
            record.status = FluxcellStatus::Active;
            record.activated_at = Some(now);
            self.registry.record_audit("STAGE_AND_ACTIVATE", name, sha256, "Auto-activated to production");
        } else {
            self.registry.record_audit("STAGE", name, sha256, "Staged for administrative activation");
        }

        self.registry.upsert_record(record.clone())?;
        Ok(record)
    }

    /// Activates a previously staged fluxcell into live production traffic.
    pub fn activate(&self, name: &str, expected_sha256: &str) -> Result<FluxcellRecord> {
        let mut record = self
            .registry
            .get_record(name)
            .ok_or_else(|| anyhow!("Fluxcell '{}' not found in registry", name))?;

        if !record.sha256.eq_ignore_ascii_case(expected_sha256) {
            return Err(anyhow!(
                "SHA-256 mismatch for activation: expected '{}', staged record has '{}'",
                expected_sha256,
                record.sha256
            ));
        }

        let wasm_path = self.registry.storage_dir().join(&record.wasm_file);
        let bytes = std::fs::read(&wasm_path)
            .with_context(|| format!("Failed to read WASM artifact '{:?}'", wasm_path))?;

        let wasm_cfg = crate::wasm::FluxcellWasmConfig {
            profile: record.profile.clone(),
            timeout_ms: record.timeout_ms,
            max_memory_bytes: record.max_memory_bytes,
            max_instances: record.max_instances,
            offload: record.offload,
            circuit_breaker: crate::wasm::CircuitBreakerConfig::default(),
        };

        self.hot_activate_cell(&record, &bytes, wasm_cfg)?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        record.status = FluxcellStatus::Active;
        record.activated_at = Some(now);
        self.registry.upsert_record(record.clone())?;
        self.registry.record_audit("ACTIVATE", name, &record.sha256, "Activated by operator");

        log::info!("FluxcellDeployer: Activated '{}' onto mount '{}'", name, record.mount_path);
        Ok(record)
    }

    fn hot_activate_cell(
        &self,
        record: &FluxcellRecord,
        bytes: &[u8],
        wasm_cfg: crate::wasm::FluxcellWasmConfig,
    ) -> Result<()> {
        // Register in WasmHost
        self.wasm_host.register_wasm_bytes(&record.name, bytes, wasm_cfg)?;

        // Atomically update FluxRouter
        let mut router_lock = self.router.write().unwrap();
        // Remove existing routes if updating an existing cell
        router_lock.unregister_fluxcell_routes(&record.name);
        router_lock.register_fluxcell_routes(&record.name, &record.mount_path, &record.routes)?;
        Ok(())
    }

    /// Unmounts and removes a fluxcell completely.
    pub fn remove(&self, name: &str) -> Result<()> {
        // 1. Unmount routes
        self.router.write().unwrap().unregister_fluxcell_routes(name);
        // 2. Unload from WASM host
        self.wasm_host.unregister_fluxcell(name);
        // 3. Remove record and file from disk
        if let Some(record) = self.registry.remove_record(name)? {
            let wasm_path = self.registry.storage_dir().join(&record.wasm_file);
            let _ = std::fs::remove_file(wasm_path);
            self.registry.record_audit("REMOVE", name, &record.sha256, "Removed and unmounted");
        }
        log::info!("FluxcellDeployer: Removed fluxcell '{}'", name);
        Ok(())
    }

    fn inspect_guest_metadata(
        &self,
        module: &wasmtime::Module,
        config: &crate::wasm::FluxcellWasmConfig,
    ) -> Result<GuestMetadata> {
        let mut subscriptions = Vec::new();
        let mut routes = Vec::new();
        let mut version = None;
        let mut git_hash = None;
        let mut build_time = None;
        let mut profile = None;
        let mut timeout_ms = None;
        let mut max_memory_mb = None;

        let limits = wasmtime::StoreLimitsBuilder::new()
            .memory_size(config.max_memory_bytes)
            .build();
        let mut store = wasmtime::Store::new(self.wasm_host.engine(), limits);

        // Epoch deadline for inspection
        store.set_epoch_deadline(50);

        if let Ok(instance) = wasmtime::Instance::new(&mut store, module, &[]) {
            // Query get_subscriptions
            if let Ok(func) = instance.get_typed_func::<(), u64>(&mut store, "get_subscriptions") {
                if let Ok(packed) = func.call(&mut store, ()) {
                    if let Ok(s) = read_guest_string(&mut store, &instance, packed) {
                        if let Ok(subs) = serde_json::from_str::<Vec<String>>(&s) {
                            subscriptions = subs;
                        }
                    }
                }
            }

            // Query get_routes
            if let Ok(func) = instance.get_typed_func::<(), u64>(&mut store, "get_routes") {
                if let Ok(packed) = func.call(&mut store, ()) {
                    if let Ok(s) = read_guest_string(&mut store, &instance, packed) {
                        #[derive(serde::Deserialize)]
                        struct RouteMeta {
                            method: String,
                            path: String,
                            description: String,
                            #[serde(default)]
                            timeout_ms: Option<u64>,
                        }
                        if let Ok(r_list) = serde_json::from_str::<Vec<RouteMeta>>(&s) {
                            routes = r_list
                                .into_iter()
                                .map(|r| crate::http::RouteDefinition {
                                    method: r.method,
                                    relative_path: r.path,
                                    description: r.description,
                                    timeout_ms: r.timeout_ms,
                                })
                                .collect();
                        }
                    }
                }
            }

            // Query get_config
            if let Ok(func) = instance.get_typed_func::<(), u64>(&mut store, "get_config") {
                if let Ok(packed) = func.call(&mut store, ()) {
                    if let Ok(s) = read_guest_string(&mut store, &instance, packed) {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&s) {
                            if let Some(p) = val.get("profile").and_then(|v| v.as_str()) {
                                profile = Some(p.to_string());
                            }
                            if let Some(t) = val.get("timeout_ms").and_then(|v| v.as_u64()) {
                                timeout_ms = Some(t);
                            }
                            if let Some(m) = val.get("max_memory_mb").and_then(|v| v.as_u64()) {
                                max_memory_mb = Some(m as usize);
                            }
                        }
                    }
                }
            }

            // Query get_metadata
            if let Ok(func) = instance.get_typed_func::<(), u64>(&mut store, "get_metadata") {
                if let Ok(packed) = func.call(&mut store, ()) {
                    if let Ok(s) = read_guest_string(&mut store, &instance, packed) {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&s) {
                            version = val.get("version").and_then(|v| v.as_str()).map(|s| s.to_string());
                            git_hash = val.get("git_hash").and_then(|v| v.as_str()).map(|s| s.to_string());
                            build_time = val.get("build_time").and_then(|v| v.as_str()).map(|s| s.to_string());
                            if profile.is_none() {
                                profile = val.get("profile").and_then(|v| v.as_str()).map(|s| s.to_string());
                            }
                            if timeout_ms.is_none() {
                                timeout_ms = val.get("timeout_ms").and_then(|v| v.as_u64());
                            }
                            if max_memory_mb.is_none() {
                                max_memory_mb = val.get("max_memory_mb").and_then(|v| v.as_u64()).map(|m| m as usize);
                            }
                        }
                    }
                }
            }
        }

        Ok(GuestMetadata {
            subscriptions,
            routes,
            version,
            git_hash,
            build_time,
            profile,
            timeout_ms,
            max_memory_mb,
        })
    }
}

#[derive(Debug, Clone)]
pub struct GuestMetadata {
    pub subscriptions: Vec<String>,
    pub routes: Vec<crate::http::RouteDefinition>,
    pub version: Option<String>,
    pub git_hash: Option<String>,
    pub build_time: Option<String>,
    pub profile: Option<String>,
    pub timeout_ms: Option<u64>,
    pub max_memory_mb: Option<usize>,
}

fn read_guest_string(
    store: &mut wasmtime::Store<wasmtime::StoreLimits>,
    instance: &wasmtime::Instance,
    packed: u64,
) -> Result<String> {
    let ptr = (packed >> 32) as usize;
    let len = (packed & 0xFFFF_FFFF) as usize;
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| anyhow!("Fluxcell does not export 'memory'"))?;

    let mem_size = memory.data_size(&*store);
    if ptr.saturating_add(len) > mem_size || len > 16 * 1024 * 1024 {
        return Err(anyhow!("Guest memory range out of bounds"));
    }

    let mut bytes = vec![0u8; len];
    memory.read(&*store, ptr, &mut bytes)?;
    String::from_utf8(bytes).map_err(|e| anyhow!("Invalid UTF-8 from guest: {}", e))
}
