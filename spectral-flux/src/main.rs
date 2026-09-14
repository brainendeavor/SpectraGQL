use anyhow::{Context, Result};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ServerBuilder;
use spectral_flux::broker::create_broker;
use spectral_flux::config::FluxConfig;
use spectral_flux::deployer::{DeployerGuard, DeployerRegistry, FluxcellDeployer, FluxcellStatus};
use spectral_flux::http::{handle_request, FluxRouter, RouteDefinition};
use spectral_flux::storage::create_storage;
use spectral_flux::telemetry::TelemetryClient;
use spectral_flux::wasm::{CircuitBreakerConfig, FluxcellWasmConfig, WasmHost};
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    log::info!("⚡ Initializing Spectral Flux Engine...");

    // 1. Load Configuration
    let config_path = std::env::var("FLUX_CONFIG").unwrap_or_else(|_| "spectral-flux.toml".to_string());
    let config = match FluxConfig::load_from_file(&config_path) {
        Ok(c) => {
            log::info!("Loaded configuration from '{}'", config_path);
            c
        }
        Err(e) => {
            log::warn!("Could not load '{}' ({}), using default configuration", config_path, e);
            FluxConfig::default_local()
        }
    };

    let worker_id = format!("spectral-flux-{}", uuid::Uuid::now_v7());
    log::info!("Instance Worker ID: {}", worker_id);

    // 2. Initialize Telemetry Client
    let telemetry = Arc::new(TelemetryClient::new(
        worker_id.clone(),
        config.broker.method.clone(),
        config.broker.stream.clone(),
        200,
    ));

    telemetry.record_log("INFO", "Spectral Flux engine initializing", None);

    // Start heartbeat reporter if gateway admin URL configured
    let stop_signal = Arc::new(AtomicBool::new(false));
    if let Some(admin_url) = &config.gateway_admin_url {
        log::info!("Configuring telemetry heartbeat to Gateway at '{}'...", admin_url);
        telemetry.clone().start_heartbeat_task(
            admin_url.clone(),
            Duration::from_secs(3),
            stop_signal.clone(),
        );
    }

    // 3. Initialize Unified Storage
    log::info!("Initializing storage backend '{}'...", config.storage.backend);
    let storage = create_storage(&config.storage.backend, config.storage.addr.as_deref())
        .await
        .context("Failed to initialize storage engine")?;
    telemetry.record_log("INFO", &format!("Storage backend '{}' ready", config.storage.backend), None);

    // 4. Initialize WASM Host
    log::info!("Initializing Wasmtime Host Engine with epoch interruption...");
    let wasm_host = Arc::new(
        WasmHost::new(5, Some(storage.clone()))
            .context("Failed to initialize WASM host engine")?,
    );

    // 5. Initialize Router with Collision Detection
    let mut router = FluxRouter::new();

    for (name, cell_cfg) in &config.fluxcells {
        if !cell_cfg.enabled {
            continue;
        }

        log::info!("Loading fluxcell '{}' mounted at '{}'...", name, cell_cfg.mount_path);

        // Check if wasm module file exists
        if std::path::Path::new(&cell_cfg.wasm_module).exists() {
            let bytes = std::fs::read(&cell_cfg.wasm_module)
                .with_context(|| format!("Failed to read WASM module '{}'", cell_cfg.wasm_module))?;
            wasm_host
                .register_wasm_bytes(name, &bytes, FluxcellWasmConfig::default())
                .with_context(|| format!("Failed to register WASM module '{}'", cell_cfg.wasm_module))?;

            if let Some(routes) = wasm_host.get_fluxcell_routes(name) {
                router
                    .register_fluxcell_routes(name, &cell_cfg.mount_path, &routes)
                    .with_context(|| format!("Route collision detected for fluxcell '{}'", name))?;
            }
        } else {
            // Built-in fallback routes for out-of-the-box fluxcells
            let routes = match name.as_str() {
                "magic_link" | "magic-link" => vec![
                    RouteDefinition {
                        method: "GET".to_string(),
                        relative_path: "/verify".to_string(),
                        description: "Verify magic link token".to_string(),
                    },
                    RouteDefinition {
                        method: "POST".to_string(),
                        relative_path: "/verify".to_string(),
                        description: "Redeem magic link token".to_string(),
                    },
                    RouteDefinition {
                        method: "GET".to_string(),
                        relative_path: "/status".to_string(),
                        description: "Auth service status".to_string(),
                    },
                ],
                "webhook" => vec![
                    RouteDefinition {
                        method: "GET".to_string(),
                        relative_path: "/health".to_string(),
                        description: "Webhook service health".to_string(),
                    },
                    RouteDefinition {
                        method: "GET".to_string(),
                        relative_path: "/dlq".to_string(),
                        description: "Dead-letter queue status".to_string(),
                    },
                    RouteDefinition {
                        method: "POST".to_string(),
                        relative_path: "/test".to_string(),
                        description: "Test webhook delivery".to_string(),
                    },
                ],
                _ => Vec::new(),
            };

            if !routes.is_empty() {
                router
                    .register_fluxcell_routes(name, &cell_cfg.mount_path, &routes)
                    .with_context(|| format!("Route collision detected for fluxcell '{}'", name))?;
                log::info!("Registered {} built-in routes for fluxcell '{}'", routes.len(), name);
            }
        }
    }

    let shared_router = Arc::new(RwLock::new(router));

    // 6. Initialize Deployer Subsystem (if enabled)
    let deployer: Option<Arc<FluxcellDeployer>> = if config.deployer.enabled {
        log::info!("Initializing Fluxcell Deployer subsystem (storage: {:?})...", config.deployer.storage_dir);
        let guard = Arc::new(DeployerGuard::new(
            config.deployer.external_deploy_enabled,
            config.deployer.dev_upload_enabled,
        ));
        let registry = Arc::new(DeployerRegistry::new(&config.deployer.storage_dir)?);
        let dep = Arc::new(FluxcellDeployer::new(
            config.deployer.clone(),
            guard,
            registry.clone(),
            wasm_host.clone(),
            shared_router.clone(),
        ));

        // Boot-load persisted active cells from fluxcells.json
        let persisted = registry.list_records();
        for record in persisted {
            if record.status == FluxcellStatus::Active {
                let wasm_file = registry.storage_dir().join(&record.wasm_file);
                if wasm_file.exists() {
                    match std::fs::read(&wasm_file) {
                        Ok(bytes) => {
                            let wasm_cfg = FluxcellWasmConfig {
                                timeout_ms: record.timeout_ms,
                                max_memory_bytes: record.max_memory_bytes,
                                circuit_breaker: CircuitBreakerConfig::default(),
                            };
                            if let Err(e) = wasm_host.register_wasm_bytes(&record.name, &bytes, wasm_cfg) {
                                log::error!("Failed to register persisted fluxcell '{}': {}", record.name, e);
                            } else {
                                let mut router_lock = shared_router.write().unwrap();
                                if let Err(e) = router_lock.register_fluxcell_routes(&record.name, &record.mount_path, &record.routes) {
                                    log::error!("Failed to mount routes for persisted fluxcell '{}': {}", record.name, e);
                                } else {
                                    log::info!("Restored active fluxcell '{}' (v{}) mounted at '{}'", record.name, record.version, record.mount_path);
                                }
                            }
                        }
                        Err(e) => log::error!("Failed to read persisted wasm file {:?}: {}", wasm_file, e),
                    }
                }
            }
        }

        Some(dep)
    } else {
        log::info!("Fluxcell Deployer subsystem is disabled in configuration.");
        None
    };

    // 7. Connect Broker Consumer Loop
    log::info!("Connecting to broker '{}' at '{}'...", config.broker.method, config.broker.addr);
    match create_broker(&config.broker).await {
        Ok(broker) => {
            let tele_clone = telemetry.clone();
            let storage_clone = storage.clone();
            let dep_broker = deployer.clone();
            let subjects = vec![
                "mutation.>".to_string(),
                "webhook.>".to_string(),
                "auth.>".to_string(),
                "deployer.>".to_string(),
            ];
            let group = config.broker.consumer_group.clone();

            tokio::spawn(async move {
                match broker.subscribe(&subjects, &group).await {
                    Ok(mut stream) => {
                        use futures_util::StreamExt;
                        log::info!("Broker consumer subscribed to subjects: {:?}", subjects);
                        while let Some(msg) = stream.next().await {
                            tele_clone.increment_processed(None);
                            tele_clone.record_log(
                                "INFO",
                                &format!("Processed event id={} topic={}", msg.id, msg.topic),
                                None,
                            );

                            // Handle deployment events
                            if (msg.topic.ends_with("deployfluxcell") || msg.topic == "deployer.deploy") && dep_broker.is_some() {
                                if let Some(dep) = &dep_broker {
                                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                                        let name = val.pointer("/request/gql/jsonBody/variables/name")
                                            .or_else(|| val.pointer("/variables/name"))
                                            .or_else(|| val.pointer("/name"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        let artifact_url = val.pointer("/request/gql/jsonBody/variables/artifactUrl")
                                            .or_else(|| val.pointer("/request/gql/jsonBody/variables/artifact_url"))
                                            .or_else(|| val.pointer("/variables/artifactUrl"))
                                            .or_else(|| val.pointer("/artifact_url"))
                                            .or_else(|| val.pointer("/artifactUrl"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        let sha256 = val.pointer("/request/gql/jsonBody/variables/sha256")
                                            .or_else(|| val.pointer("/variables/sha256"))
                                            .or_else(|| val.pointer("/sha256"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        let mount_path = val.pointer("/request/gql/jsonBody/variables/mountPath")
                                            .or_else(|| val.pointer("/request/gql/jsonBody/variables/mount_path"))
                                            .or_else(|| val.pointer("/variables/mountPath"))
                                            .or_else(|| val.pointer("/mount_path"))
                                            .or_else(|| val.pointer("/mountPath"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();

                                        if !name.is_empty() && !artifact_url.is_empty() && !sha256.is_empty() {
                                            log::info!("Received deployFluxcell event for '{}' from '{}'", name, artifact_url);
                                            let dep_clone = dep.clone();
                                            let name = name.to_string();
                                            let artifact_url = artifact_url.to_string();
                                            let sha256 = sha256.to_string();
                                            let mount_path = mount_path.to_string();
                                            tokio::spawn(async move {
                                                match dep_clone.stage_remote_artifact(&name, &artifact_url, &sha256, &mount_path, None, None, None).await {
                                                    Ok(rec) => log::info!("Successfully staged remote fluxcell '{}' (status: {:?})", rec.name, rec.status),
                                                    Err(e) => log::error!("Failed to stage remote fluxcell '{}': {}", name, e),
                                                }
                                            });
                                        }
                                    }
                                }
                            } else if (msg.topic.ends_with("activatefluxcell") || msg.topic == "deployer.activate") && dep_broker.is_some() {
                                if let Some(dep) = &dep_broker {
                                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                                        let name = val.pointer("/request/gql/jsonBody/variables/name")
                                            .or_else(|| val.pointer("/variables/name"))
                                            .or_else(|| val.pointer("/name"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        let sha256 = val.pointer("/request/gql/jsonBody/variables/sha256")
                                            .or_else(|| val.pointer("/variables/sha256"))
                                            .or_else(|| val.pointer("/sha256"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or_default();
                                        if !name.is_empty() && !sha256.is_empty() {
                                            match dep.activate(name, sha256) {
                                                Ok(rec) => log::info!("Activated fluxcell '{}' into production", rec.name),
                                                Err(e) => log::error!("Failed to activate fluxcell '{}': {}", name, e),
                                            }
                                        }
                                    }
                                }
                            }

                            // Handle built-in fluxcell event routing
                            if msg.topic.ends_with("requestmagiclink") || msg.topic == "auth.magic_link" {
                                let email = if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                                    val.pointer("/request/gql/jsonBody/variables/email")
                                        .or_else(|| val.pointer("/variables/email"))
                                        .or_else(|| val.pointer("/email"))
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| "user@example.com".to_string())
                                } else {
                                    "user@example.com".to_string()
                                };
                                let token = fluxcell_magic_link::mint_magic_token(&email);
                                let _ = storage_clone.set(&format!("magic_token:{}", token), &email, 900).await;
                                tele_clone.record_log(
                                    "INFO",
                                    &format!("Minted magic link token for {} in storage (token: {})", email, token),
                                    None,
                                );
                            }

                            let _ = broker.ack(&msg).await;
                        }
                    }
                    Err(e) => {
                        log::warn!("Broker subscription could not be established: {}", e);
                    }
                }
            });
        }
        Err(e) => {
            log::warn!("Broker connection failed: {}. Continuing in standalone API mode.", e);
        }
    }

    // 8. Start HTTP Server (:8081)
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    log::info!("🚀 Spectral Flux HTTP Support Server listening on http://{}", addr);

    let router_arc = shared_router.clone();
    let telemetry_arc = telemetry.clone();
    let wasm_dispatcher = wasm_host.clone();
    let deployer_arc = deployer.clone();

    tokio::select! {
        _ = async {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(res) => res,
                    Err(e) => {
                        log::error!("TCP accept error: {}", e);
                        continue;
                    }
                };

                let io = TokioIo::new(stream);
                let router_clone = router_arc.clone();
                let tele_clone = telemetry_arc.clone();
                let dispatcher_clone = wasm_dispatcher.clone();
                let deployer_clone = deployer_arc.clone();

                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| {
                        handle_request(req, router_clone.clone(), tele_clone.clone(), dispatcher_clone.clone(), deployer_clone.clone())
                    });

                    if let Err(err) = ServerBuilder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        log::debug!("HTTP connection closed: {:?}", err);
                    }
                });
            }
        } => {}
        _ = tokio::signal::ctrl_c() => {
            log::info!("Shutting down Spectral Flux Engine gracefully...");
            stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    log::info!("Spectral Flux Engine shutdown complete.");
    Ok(())
}
