use crate::core::clock::HlcClock;
use crate::core::config::{SpectraModeAConfig, SpectraRouteConfig, SpectraSubscriptionsConfig};
use crate::gateway::filters::{
    AdminFilter, FaviconFilter, HealthFilter, IdempotencyFilter, IdempotencyInterceptResult, StrategyRouter,
    TelemetryDispatcher,
};
use crate::gateway::SpectraProxyService;
use crate::gateway::{
    ExtraServiceParams, PathRouter, ProxyService, REQUEST_ID_HEADER, SpectraProxyCtx,
    new_proxy_service,
};
use crate::idempotency::IdempotencyEngine;
use crate::protocol::{ResponseBody, RequestDecoder};
use crate::telemetry::DispatchHandler;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use pingora::ErrorType::ConnectNoRoute;
use pingora::http::ResponseHeader;
use pingora::proxy::{ProxyHttp, Session};
use pingora::upstreams::peer::HttpPeer;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use super::ServiceHandle;

#[derive(Clone)]
pub struct ServiceConfig {
    pub service: Arc<ProxyService>,
    pub upstream_addr: std::net::SocketAddr,
    pub upstream_routes: String,
}

#[derive(Clone)]
pub struct DynamicGatewayState {
    pub named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
    pub upstream_targets: Arc<HashMap<String, String>>,
    pub dns_config: crate::core::config::SpectraDnsConfig,
    pub mode_a: SpectraModeAConfig,
    pub routes: Arc<HashMap<String, SpectraRouteConfig>>,
    pub apps: Arc<Vec<crate::core::SpectraAppConfig>>,
    pub default_app: Option<String>,
    pub interceptor_manager: Arc<crate::interceptors::InterceptorManager>,
    pub telemetry_config: crate::core::config::SpectraTelemetryConfig,
    pub raw_config: Arc<String>,
    pub version: u64,
    pub updated_at_epoch_ms: u64,
    pub config_hash: String,
}

impl Default for DynamicGatewayState {
    fn default() -> Self {
        Self {
            named_upstreams: Arc::new(HashMap::new()),
            upstream_targets: Arc::new(HashMap::new()),
            dns_config: crate::core::config::SpectraDnsConfig::default(),
            mode_a: SpectraModeAConfig::default(),
            routes: Arc::new(HashMap::new()),
            apps: Arc::new(vec![]),
            default_app: None,
            interceptor_manager: Arc::new(crate::interceptors::InterceptorManager::empty()),
            telemetry_config: crate::core::config::SpectraTelemetryConfig::default(),
            raw_config: Arc::new(String::new()),
            version: 1,
            updated_at_epoch_ms: 0,
            config_hash: String::new(),
        }
    }
}

impl DynamicGatewayState {
    pub fn new_from_config(
        new_cfg: &crate::core::SpectraConfig,
        raw_content: String,
        version: u64,
    ) -> Result<Self> {
        use sha2::Digest;
        let (named_upstreams, upstream_targets) = new_cfg.resolve_all_upstreams_with_targets()?;
        let interceptor_manager = crate::interceptors::InterceptorManager::from_config(new_cfg)?;
        let hash = format!("{:x}", sha2::Sha256::digest(raw_content.as_bytes()));
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Ok(Self {
            named_upstreams: Arc::new(named_upstreams),
            upstream_targets: Arc::new(upstream_targets),
            dns_config: new_cfg.dns.clone(),
            mode_a: new_cfg.gql.mode_a.clone(),
            routes: Arc::new(new_cfg.gql.routes.clone()),
            apps: Arc::new(new_cfg.apps.clone()),
            default_app: new_cfg.default_app.clone(),
            interceptor_manager: Arc::new(interceptor_manager),
            telemetry_config: new_cfg.telemetry.clone(),
            raw_config: Arc::new(raw_content),
            version,
            updated_at_epoch_ms: now_ms,
            config_hash: hash,
        })
    }

    /// Rescans DNS for all configured upstream hostnames and atomically updates dynamic state if changed.
    pub fn rescan_dns(
        dynamic_state: &Arc<arc_swap::ArcSwap<DynamicGatewayState>>,
    ) -> Result<crate::admin::api::AdminDnsRescanResponse> {
        let start = std::time::Instant::now();
        let current_state = dynamic_state.load();
        let targets = &current_state.upstream_targets;
        let prev_upstreams = &current_state.named_upstreams;

        let mut new_upstreams = (**prev_upstreams).clone();
        let mut upstream_entries = Vec::new();
        let mut changed_count = 0;

        for (name, target) in targets.iter() {
            let prev_addr = prev_upstreams.get(name).cloned();
            use std::net::ToSocketAddrs;
            match target.to_socket_addrs() {
                Ok(mut iter) => {
                    if let Some(new_addr) = iter.next() {
                        let is_changed = prev_addr.map(|a| a != new_addr).unwrap_or(true);
                        if is_changed {
                            changed_count += 1;
                            log::info!(
                                "DNS rescan: upstream '{}' ({}) updated from {:?} to {}",
                                name,
                                target,
                                prev_addr,
                                new_addr
                            );
                        }
                        new_upstreams.insert(name.clone(), new_addr);
                        upstream_entries.push(crate::admin::api::AdminDnsUpstreamEntry {
                            name: name.clone(),
                            target: target.clone(),
                            current_addr: new_addr.to_string(),
                            previous_addr: prev_addr.map(|a| a.to_string()),
                            changed: is_changed,
                        });
                    } else {
                        log::warn!(
                            "DNS rescan: no addresses resolved for upstream '{}' ({})",
                            name,
                            target
                        );
                        if let Some(pa) = prev_addr {
                            upstream_entries.push(crate::admin::api::AdminDnsUpstreamEntry {
                                name: name.clone(),
                                target: target.clone(),
                                current_addr: pa.to_string(),
                                previous_addr: Some(pa.to_string()),
                                changed: false,
                            });
                        }
                    }
                }
                Err(err) => {
                    log::warn!(
                        "DNS rescan: failed to resolve upstream '{}' ({}): {}. Retaining previous address {:?}",
                        name,
                        target,
                        err,
                        prev_addr
                    );
                    if let Some(pa) = prev_addr {
                        upstream_entries.push(crate::admin::api::AdminDnsUpstreamEntry {
                            name: name.clone(),
                            target: target.clone(),
                            current_addr: pa.to_string(),
                            previous_addr: Some(pa.to_string()),
                            changed: false,
                        });
                    }
                }
            }
        }

        if changed_count > 0 {
            let mut updated_state = (**current_state).clone();
            updated_state.named_upstreams = Arc::new(new_upstreams);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            updated_state.updated_at_epoch_ms = now_ms;
            dynamic_state.store(Arc::new(updated_state));
        }

        upstream_entries.sort_by(|a, b| a.name.cmp(&b.name));
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        Ok(crate::admin::api::AdminDnsRescanResponse {
            status: "success".to_string(),
            total_upstreams: targets.len(),
            changed_count,
            duration_ms,
            upstreams: upstream_entries,
        })
    }

    /// Returns current DNS status and upstream target mappings.
    pub fn get_dns_status(
        dynamic_state: &Arc<arc_swap::ArcSwap<DynamicGatewayState>>,
    ) -> crate::admin::api::AdminDnsStatusResponse {
        let current_state = dynamic_state.load();
        let mut upstream_entries = Vec::new();
        for (name, target) in current_state.upstream_targets.iter() {
            let current_addr = current_state
                .named_upstreams
                .get(name)
                .map(|a| a.to_string())
                .unwrap_or_else(|| "unresolved".to_string());
            upstream_entries.push(crate::admin::api::AdminDnsUpstreamEntry {
                name: name.clone(),
                target: target.clone(),
                current_addr,
                previous_addr: None,
                changed: false,
            });
        }
        upstream_entries.sort_by(|a, b| a.name.cmp(&b.name));
        crate::admin::api::AdminDnsStatusResponse {
            enabled: current_state.dns_config.enabled,
            interval_secs: current_state.dns_config.interval_secs,
            total_upstreams: current_state.upstream_targets.len(),
            upstreams: upstream_entries,
        }
    }
}

/// Spawns a background OS thread that periodically rescans DNS for all configured upstreams
/// when dynamic DNS re-resolution is enabled.
pub fn spawn_dns_refresher(
    dynamic_state: Arc<arc_swap::ArcSwap<DynamicGatewayState>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("spectra-dns-refresher".to_string())
        .spawn(move || {
            log::info!("Dynamic DNS background refresher thread started");
            loop {
                let current_state = dynamic_state.load();
                let dns_cfg = current_state.dns_config.clone();
                let interval_secs = if dns_cfg.interval_secs == 0 {
                    15
                } else {
                    dns_cfg.interval_secs
                };

                std::thread::sleep(std::time::Duration::from_secs(interval_secs));

                let current_state = dynamic_state.load();
                if current_state.dns_config.enabled {
                    if let Err(e) = DynamicGatewayState::rescan_dns(&dynamic_state) {
                        log::warn!("Dynamic DNS background rescan error: {}", e);
                    }
                }
            }
        })
        .expect("Failed to spawn spectra-dns-refresher thread")
}

#[derive(Clone)]
pub struct CompositeServiceProxy {
    upstream_services: Vec<ServiceConfig>,
    router: PathRouter,
    pub idempotency_engine: Arc<IdempotencyEngine>,
    pub named_upstreams: Arc<HashMap<String, std::net::SocketAddr>>,
    pub upstream_targets: Arc<HashMap<String, String>>,
    pub mode_a: SpectraModeAConfig,
    pub routes: Arc<HashMap<String, SpectraRouteConfig>>,
    pub subscription_hub: Arc<crate::subscriptions::SubscriptionHub>,
    pub subscriptions_config: SpectraSubscriptionsConfig,
    pub admin_engine: Option<crate::admin::AdminEngine>,
    pub interceptor_manager: Arc<crate::interceptors::InterceptorManager>,
    pub traffic_recorder: Arc<crate::admin::TrafficRecorder>,
    pub apps: Arc<Vec<crate::core::SpectraAppConfig>>,
    pub default_app: Option<String>,
    pub telemetry_config: crate::core::config::SpectraTelemetryConfig,
    pub dynamic_state: Arc<arc_swap::ArcSwap<DynamicGatewayState>>,
}

impl ServiceConfig {
    pub fn new(
        service_type: &str,
        upstream_address: &str,
        upstream_routes: &str,
        dispatch_method: &str,
        dispatch_address: &str,
        extra_params: ExtraServiceParams,
    ) -> Self {
        Self::try_new(
            service_type,
            upstream_address,
            upstream_routes,
            dispatch_method,
            dispatch_address,
            extra_params,
        )
        .expect("Failed to initialize ServiceConfig")
    }

    pub fn try_new(
        service_type: &str,
        upstream_address: &str,
        upstream_routes: &str,
        dispatch_method: &str,
        dispatch_address: &str,
        extra_params: ExtraServiceParams,
    ) -> Result<Self> {
        let upstream_addr = upstream_address
            .to_socket_addrs()
            .map_err(|e| anyhow!("Failed to resolve upstream address '{}': {}", upstream_address, e))?
            .next()
            .ok_or_else(|| anyhow!("No socket address resolved for upstream '{}'", upstream_address))?;
        let proxy_service =
            new_proxy_service(service_type, dispatch_method, dispatch_address, extra_params)?;
        let service = Arc::new(proxy_service);

        Ok(ServiceConfig {
            service,
            upstream_addr,
            upstream_routes: upstream_routes.to_string(),
        })
    }
}

impl CompositeServiceProxy {
    pub fn new() -> Self {
        let dynamic_state = Arc::new(arc_swap::ArcSwap::from_pointee(DynamicGatewayState::default()));
        CompositeServiceProxy {
            upstream_services: vec![],
            router: PathRouter::new(),
            idempotency_engine: Arc::new(IdempotencyEngine::default()),
            named_upstreams: Arc::new(HashMap::new()),
            upstream_targets: Arc::new(HashMap::new()),
            mode_a: SpectraModeAConfig::default(),
            routes: Arc::new(HashMap::new()),
            subscription_hub: Arc::new(crate::subscriptions::SubscriptionHub::new()),
            subscriptions_config: SpectraSubscriptionsConfig::default(),
            admin_engine: None,
            interceptor_manager: Arc::new(crate::interceptors::InterceptorManager::empty()),
            traffic_recorder: Arc::new(crate::admin::TrafficRecorder::new(250)),
            apps: Arc::new(vec![]),
            default_app: None,
            telemetry_config: crate::core::config::SpectraTelemetryConfig::default(),
            dynamic_state,
        }
    }

    pub fn with_dynamic_state(
        mut self,
        dynamic_state: Arc<arc_swap::ArcSwap<DynamicGatewayState>>,
    ) -> Self {
        let loaded = dynamic_state.load();
        self.named_upstreams = loaded.named_upstreams.clone();
        self.upstream_targets = loaded.upstream_targets.clone();
        self.mode_a = loaded.mode_a.clone();
        self.routes = loaded.routes.clone();
        self.apps = loaded.apps.clone();
        self.default_app = loaded.default_app.clone();
        self.interceptor_manager = loaded.interceptor_manager.clone();
        self.telemetry_config = loaded.telemetry_config.clone();
        self.dynamic_state = dynamic_state;
        self
    }

    fn sync_dynamic_state(&self) {
        let prev = self.dynamic_state.load();
        let new_state = DynamicGatewayState {
            named_upstreams: self.named_upstreams.clone(),
            upstream_targets: self.upstream_targets.clone(),
            dns_config: prev.dns_config.clone(),
            mode_a: self.mode_a.clone(),
            routes: self.routes.clone(),
            apps: self.apps.clone(),
            default_app: self.default_app.clone(),
            interceptor_manager: self.interceptor_manager.clone(),
            telemetry_config: self.telemetry_config.clone(),
            raw_config: prev.raw_config.clone(),
            version: prev.version,
            updated_at_epoch_ms: prev.updated_at_epoch_ms,
            config_hash: prev.config_hash.clone(),
        };
        self.dynamic_state.store(Arc::new(new_state));
    }

    pub fn with_telemetry(
        mut self,
        telemetry: crate::core::config::SpectraTelemetryConfig,
    ) -> Self {
        self.telemetry_config = telemetry;
        self.sync_dynamic_state();
        self
    }

    pub fn with_routing_with_targets(
        mut self,
        named_upstreams: HashMap<String, std::net::SocketAddr>,
        upstream_targets: HashMap<String, String>,
        mode_a: SpectraModeAConfig,
        routes: HashMap<String, SpectraRouteConfig>,
    ) -> Self {
        self.named_upstreams = Arc::new(named_upstreams);
        self.upstream_targets = Arc::new(upstream_targets);
        self.mode_a = mode_a;
        self.routes = Arc::new(routes);
        self.sync_dynamic_state();
        self
    }

    pub fn with_routing(
        self,
        named_upstreams: HashMap<String, std::net::SocketAddr>,
        mode_a: SpectraModeAConfig,
        routes: HashMap<String, SpectraRouteConfig>,
    ) -> Self {
        let targets: HashMap<String, String> = named_upstreams
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect();
        self.with_routing_with_targets(named_upstreams, targets, mode_a, routes)
    }

    pub fn with_dns_config(
        self,
        dns: crate::core::config::SpectraDnsConfig,
    ) -> Self {
        let prev = self.dynamic_state.load();
        let mut new_state = (**prev).clone();
        new_state.dns_config = dns;
        self.dynamic_state.store(Arc::new(new_state));
        self
    }

    pub fn with_apps(
        mut self,
        apps: Vec<crate::core::SpectraAppConfig>,
        default_app: Option<String>,
    ) -> Self {
        self.apps = Arc::new(apps);
        self.default_app = default_app;
        self.sync_dynamic_state();
        self
    }

    /// Spawns a background OS thread for dynamic DNS re-resolution.
    pub fn spawn_dns_refresher(&self) -> std::thread::JoinHandle<()> {
        spawn_dns_refresher(self.dynamic_state.clone())
    }

    pub fn reload_config(&self, new_cfg: &crate::core::SpectraConfig, raw_content: String) -> Result<Arc<DynamicGatewayState>> {
        let prev = self.dynamic_state.load();
        let new_version = prev.version + 1;
        let new_state = Arc::new(DynamicGatewayState::new_from_config(new_cfg, raw_content, new_version)?);
        self.dynamic_state.store(new_state.clone());
        log::info!("CompositeServiceProxy: Configuration successfully hot-reloaded to version #{}", new_version);
        Ok(new_state)
    }

    pub fn resolve_app_with_apps<'a>(
        apps: &'a [crate::core::SpectraAppConfig],
        default_app: Option<&'a str>,
        host: Option<&str>,
        header_app: Option<&str>,
        path: &str,
    ) -> Option<&'a crate::core::SpectraAppConfig> {
        if let Some(h_app) = header_app {
            if let Some(app) = apps.iter().find(|a| a.id.eq_ignore_ascii_case(h_app)) {
                return Some(app);
            }
        }
        if let Some(h) = host {
            let clean_host = h.split(':').next().unwrap_or(h).trim();
            if let Some(app) = apps.iter().find(|a| {
                a.domains.iter().any(|d| {
                    let clean_d = d.split(':').next().unwrap_or(d).trim();
                    clean_d.eq_ignore_ascii_case(clean_host)
                })
            }) {
                return Some(app);
            }
        }
        if let Some(app) = apps.iter().find(|a| {
            a.path_prefixes.iter().any(|prefix| {
                let clean_prefix = prefix.trim_end_matches('/');
                path == clean_prefix || path.starts_with(&format!("{}/", clean_prefix))
            })
        }) {
            return Some(app);
        }
        if let Some(def_id) = default_app {
            if let Some(app) = apps.iter().find(|a| a.id.eq_ignore_ascii_case(def_id)) {
                return Some(app);
            }
        }
        apps.first()
    }

    pub fn resolve_app<'a>(
        &'a self,
        host: Option<&str>,
        header_app: Option<&str>,
        path: &str,
    ) -> Option<&'a crate::core::SpectraAppConfig> {
        Self::resolve_app_with_apps(&self.apps, self.default_app.as_deref(), host, header_app, path)
    }

    pub fn with_idempotency_engine(
        mut self,
        idempotency_engine: Arc<IdempotencyEngine>,
    ) -> Self {
        self.idempotency_engine = idempotency_engine;
        self
    }

    pub fn with_subscriptions(
        mut self,
        subscription_hub: Arc<crate::subscriptions::SubscriptionHub>,
        subscriptions_config: SpectraSubscriptionsConfig,
    ) -> Self {
        self.subscription_hub = subscription_hub;
        self.subscriptions_config = subscriptions_config;
        self
    }

    pub fn with_admin(mut self, admin_engine: crate::admin::AdminEngine) -> Self {
        self.admin_engine = Some(admin_engine);
        self
    }

    pub fn with_interceptor_manager(
        mut self,
        interceptor_manager: Arc<crate::interceptors::InterceptorManager>,
    ) -> Self {
        self.interceptor_manager = interceptor_manager;
        self.sync_dynamic_state();
        self
    }

    pub fn with_traffic_recorder(
        mut self,
        traffic_recorder: Arc<crate::admin::TrafficRecorder>,
    ) -> Self {
        self.traffic_recorder = traffic_recorder;
        self
    }

    pub fn add_service_config(&mut self, service_config: ServiceConfig) {
        let service_handle = self.upstream_services.len();
        let routes = service_config.upstream_routes.clone();
        for route in routes.split(',') {
            self.router
                .add_service_handle(route.to_string().trim(), service_handle)
                .expect("Failed to add service at {} to composite service.")
        }
        self.upstream_services.push(service_config);
    }

    pub fn get_service_config(&self, service_handle: ServiceHandle) -> &ServiceConfig {
        &self.upstream_services[service_handle]
    }

    pub fn get_service_handle_by_path(&self, path: &str) -> Option<ServiceHandle> {
        self.router.get_service_handle(path)
    }

    pub fn get_service_config_and_handle_by_path(
        &self,
        path: &str,
    ) -> Result<(&ServiceConfig, ServiceHandle)> {
        match self.get_service_handle_by_path(path) {
            Some(service_handle) => {
                return Ok((self.get_service_config(service_handle), service_handle));
            }
            None => {
                bail!("No proxy service for {}", path);
            }
        }
    }

    #[allow(dead_code)]
    pub fn subscription_hub(&self) -> Arc<crate::subscriptions::SubscriptionHub> {
        self.subscription_hub.clone()
    }

    pub async fn handle_websocket_subscription(&self, session: &mut Session) -> pingora::Result<bool> {
        crate::subscriptions::WebSocketHandler::handle(
            session,
            self.subscription_hub.clone(),
            &self.subscriptions_config,
        )
        .await
    }

    /// Helper to record traffic events without boilerplate.
    pub fn record_traffic_event(
        &self,
        session: &Session,
        ctx: &CompositeServiceProxyCtx,
        details: TrafficEventDetails,
    ) {
        let latency_ms = ctx.proxy_context.start_time.elapsed().as_secs_f64() * 1000.0;
        let epoch_ms = crate::admin::now_epoch_ms();
        let client_ip = crate::admin::extract_client_ip(session).to_string();

        self.traffic_recorder.record(crate::admin::TrafficRecord {
            id: ctx.proxy_context.request_id.to_string(),
            hlc: ctx.proxy_context.hlc.to_compact_string(),
            timestamp_epoch_ms: epoch_ms,
            timestamp_formatted: crate::admin::format_timestamp(epoch_ms),
            client_ip,
            method: session.req_header().method.to_string(),
            path: session.req_header().uri.path().to_string(),
            app_id: ctx.proxy_context.app_id.clone(),
            operation_name: details.operation_name,
            operation_type: details.operation_type,
            mode: details.mode,
            status_code: details.status_code,
            receipt_status: details.receipt_status,
            latency_ms,
            target: details.target,
            query_preview: details.query_preview.or_else(|| ctx.proxy_context.query_preview.clone()),
            variables_preview: details.variables_preview.or_else(|| ctx.proxy_context.variables_preview.clone()),
            audit_tag: details.audit_tag.or_else(|| ctx.proxy_context.audit_tag.clone()),
            audit_rule: details.audit_rule.or_else(|| ctx.proxy_context.audit_rule.clone()),
            response_preview: details.response_preview.or_else(|| ctx.proxy_context.response_preview.clone()),
            error_preview: details.error_preview.or_else(|| ctx.proxy_context.error_preview.clone()),
        });
    }
}

/// Parameters for recording a structured gateway traffic event.
#[derive(Default)]
pub struct TrafficEventDetails {
    pub operation_name: Option<String>,
    pub operation_type: String,
    pub mode: String,
    pub status_code: u16,
    pub receipt_status: Option<String>,
    pub target: String,
    pub query_preview: Option<String>,
    pub variables_preview: Option<String>,
    pub audit_tag: Option<String>,
    pub audit_rule: Option<String>,
    pub response_preview: Option<String>,
    pub error_preview: Option<String>,
}

// TODO: absorb SpectraProxyCtx
pub struct CompositeServiceProxyCtx {
    pub proxy_context: SpectraProxyCtx,
    pub service_handle: Option<ServiceHandle>,
    pub service: Option<Arc<ProxyService>>,
}

#[async_trait]
impl ProxyHttp for CompositeServiceProxy {
    type CTX = CompositeServiceProxyCtx;
    fn new_ctx(&self) -> Self::CTX {
        let dynamic = self.dynamic_state.load();
        let (request_id, hlc) = HlcClock::global().now_uuidv7();
        let proxy_context = SpectraProxyCtx {
            request_id,
            hlc,
            start_time: std::time::Instant::now(),
            request_topic: "spectra".to_string(),
            request_info: None,
            response_parts: None,
            response_body: None,
            buffer: vec![],
            idempotency_key: None,
            is_replay: false,
            target_upstream_addr: None,
            is_mode_b_terminated: false,
            dispatch_policy: dynamic.mode_a.dispatch_policy,
            active_operation: None,
            has_response_interception: false,
            query_preview: None,
            variables_preview: None,
            app_id: dynamic.default_app.clone().unwrap_or_else(|| "default".to_string()),
            audit_tag: None,
            audit_rule: None,
            response_preview: None,
            error_preview: None,
        };
        CompositeServiceProxyCtx {
            proxy_context,
            service_handle: None,
            service: None,
        }
    }

    async fn early_request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let req = session.req_header();
        let path = req.uri.path();

        // 1. Health check probes do not need an upstream service
        if HealthFilter::is_health_probe(path) {
            return Ok(());
        }

        // 2. Favicon requests do not need an upstream service
        if FaviconFilter::is_favicon_request(path) {
            return Ok(());
        }

        // 3. Admin Engine requests do not need an upstream service
        if AdminFilter::is_admin_request(self.admin_engine.as_ref(), path) {
            return Ok(());
        }

        let dynamic = self.dynamic_state.load();

        // 3. Resolve app based on Host, X-App-ID / X-Tenant-ID header, or path
        let host = req.headers.get("host").and_then(|v| v.to_str().ok());
        let header_app = req.headers.get("x-app-id")
            .or_else(|| req.headers.get("x-tenant-id"))
            .and_then(|v| v.to_str().ok());
        if let Some(app) = Self::resolve_app_with_apps(&dynamic.apps, dynamic.default_app.as_deref(), host, header_app, path) {
            ctx.proxy_context.app_id = app.id.clone();
            if let Some(addr) = dynamic.named_upstreams.get(&app.upstream) {
                ctx.proxy_context.target_upstream_addr = Some(*addr);
            }
        } else if let Some(ref def_id) = dynamic.default_app {
            ctx.proxy_context.app_id = def_id.clone();
            if let Some(app) = dynamic.apps.iter().find(|a| a.id == *def_id) {
                if let Some(addr) = dynamic.named_upstreams.get(&app.upstream) {
                    ctx.proxy_context.target_upstream_addr = Some(*addr);
                }
            }
        } else if let Some(addr) = dynamic.named_upstreams.get("default") {
            ctx.proxy_context.target_upstream_addr = Some(*addr);
        }

        match self.get_service_config_and_handle_by_path(path) {
            Ok((service_config, service_handle)) => {
                ctx.service_handle = Some(service_handle);
                ctx.service = Some(service_config.service.clone());
            }
            Err(e) => {
                log::error!("{}", e);
                return Err(pingora::Error::explain(
                    ConnectNoRoute,
                    format!("No route configured for {}.", path),
                ));
            }
        };
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Box<HttpPeer>> {
        let dynamic = self.dynamic_state.load();
        let upstream_addr = if let Some(target) = ctx.proxy_context.target_upstream_addr {
            target
        } else if let Some(default_addr) = dynamic.named_upstreams.get("default") {
            *default_addr
        } else if let Some(handle) = ctx.service_handle {
            let service_config = self.get_service_config(handle);
            service_config.upstream_addr
        } else {
            return Err(pingora::Error::explain(
                ConnectNoRoute,
                "No upstream peer available.",
            ));
        };

        let upstream_ip_port = format!("{}:{}", upstream_addr.ip(), upstream_addr.port());
        let peer = HttpPeer::new(upstream_addr, false, upstream_ip_port);
        Ok(Box::new(peer))
    }

    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // 1. Health check probes
        if HealthFilter::handle(session).await? {
            return Ok(true);
        }

        // 2. Favicon requests
        if FaviconFilter::handle(session).await? {
            return Ok(true);
        }

        // 3. Admin Engine routing
        if AdminFilter::handle(
            session,
            self.admin_engine.as_ref(),
            &self.idempotency_engine,
            &self.subscription_hub,
        )
        .await?
        {
            return Ok(true);
        }

        // 3. Check for WebSocket Subscription Upgrade
        if self.subscriptions_config.enabled && crate::subscriptions::is_websocket_upgrade(session.req_header()) {
            let path = session.req_header().uri.path();
            if path == "/graphql" || path == "/gql" || path.starts_with("/graphql") || path.starts_with("/gql") {
                log::info!("CompositeServiceProxy: terminating WebSocket subscription upgrade on '{}'", path);
                return self.handle_websocket_subscription(session).await;
            }
        }

        let dynamic = self.dynamic_state.load();

        log::info!(
            "request_filter uuid: {} hlc: {}",
            ctx.proxy_context.request_id,
            ctx.proxy_context.hlc
        );
        let req = session.req_header_mut();
        if let Err(e) =
            req.append_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string())
        {
            log::warn!(
                "Failed to append request header for request_id: {} {}",
                ctx.proxy_context.request_id,
                e
            );
        };
        if let Err(e) =
            req.append_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string())
        {
            log::warn!("Failed to append x-spectra-hlc: {}", e);
        }
        if let Err(e) = req.append_header("x-spectra-app", &ctx.proxy_context.app_id) {
            log::warn!("Failed to append x-spectra-app: {}", e);
        }

        // 4. Check for explicit Idempotency-Key in request headers
        match IdempotencyFilter::handle_explicit_key(
            session,
            &self.idempotency_engine,
            &ctx.proxy_context.request_id,
            ctx.proxy_context.hlc,
        )
        .await?
        {
            IdempotencyInterceptResult::Conflict => return Ok(true),
            IdempotencyInterceptResult::Replay => {
                ctx.proxy_context.is_replay = true;
                self.record_traffic_event(
                    session,
                    ctx,
                    TrafficEventDetails {
                        operation_name: None,
                        operation_type: "Mutation".to_string(),
                        mode: "Replay (Idempotency)".to_string(),
                        status_code: 200,
                        receipt_status: Some("REPLAY".to_string()),
                        target: "Idempotency Cache".to_string(),
                        ..Default::default()
                    },
                );
                return Ok(true);
            }
            IdempotencyInterceptResult::NewKey(k) => {
                ctx.proxy_context.idempotency_key = Some(k);
            }
            IdempotencyInterceptResult::None => {}
        }

        // Enable retry buffering and read request body if POST (capped at 10MB to prevent OOM denial of service)
        const MAX_GATEWAY_BODY_BYTES: usize = 10 * 1024 * 1024; // 10MB

        if session.req_header().method == http::Method::POST {
            session.enable_retry_buffering();
            let mut payload_too_large = false;
            while let Some(chunk) = session.read_request_body().await? {
                if ctx.proxy_context.buffer.len() + chunk.len() > MAX_GATEWAY_BODY_BYTES {
                    payload_too_large = true;
                    break;
                }
                ctx.proxy_context.buffer.extend_from_slice(&chunk);
            }

            if payload_too_large {
                log::warn!(
                    "Request body exceeded {} bytes limit from client",
                    MAX_GATEWAY_BODY_BYTES
                );
                let err_body = r#"{"errors":[{"message":"Payload Too Large: request body exceeds 10MB limit"}]}"#;
                let mut header = pingora::http::ResponseHeader::build(413, None)?;
                let _ = header.insert_header("content-type", "application/json");
                let _ = header.insert_header("content-length", err_body.len().to_string());
                let _ = header.insert_header(REQUEST_ID_HEADER, ctx.proxy_context.request_id.to_string());
                let _ = header.insert_header("x-spectra-hlc", ctx.proxy_context.hlc.to_compact_string());
                if let Err(e) = session.write_response_header(Box::new(header), false).await {
                    log::debug!("Client disconnected before 413 header write: {}", e);
                    return Ok(true);
                }
                if let Err(e) = session.write_response_body(Some(bytes::Bytes::from(err_body)), true).await {
                    log::debug!("Client disconnected before 413 body write: {}", e);
                    return Ok(true);
                }
                if let Some(key) = ctx.proxy_context.idempotency_key.take() {
                    self.idempotency_engine.remove(&key).await;
                }
                return Ok(true);
            }
        }

        if !ctx.proxy_context.buffer.is_empty() {
            let mut body_str = match std::str::from_utf8(&ctx.proxy_context.buffer) {
                Ok(s) => s.to_string(),
                Err(_) => return Ok(false),
            };

            let (request_protocol, dispatch_method) = match ctx.service.as_ref() {
                Some(s) => (s.get_request_protocol().clone(), s.get_dispatch_method().clone()),
                None => return Ok(false),
            };

            if let Ok(request_info) = request_protocol.decode_request(
                ctx.proxy_context.request_id,
                ctx.proxy_context.hlc,
                session.req_header().as_owned_parts(),
                &body_str,
            ) {
                let base_topic = dispatch_method.get_dispatch_topic(&request_info);
                let app_prefix = dynamic.apps.iter()
                    .find(|a| a.id == ctx.proxy_context.app_id)
                    .map(|a| a.effective_subject_prefix())
                    .or_else(|| {
                        if !ctx.proxy_context.app_id.is_empty() {
                            Some(format!("mutation.{}", ctx.proxy_context.app_id))
                        } else {
                            None
                        }
                    });
                ctx.proxy_context.request_topic = if let Some(prefix) = app_prefix {
                    if let Some((kind, op)) = base_topic.split_once('.') {
                        if kind == "query" && prefix.starts_with("mutation.") {
                            format!("query.{}.{}", prefix.trim_start_matches("mutation."), op)
                        } else if kind == "subscription" && prefix.starts_with("mutation.") {
                            format!("subscription.{}.{}", prefix.trim_start_matches("mutation."), op)
                        } else {
                            format!("{}.{}", prefix, op)
                        }
                    } else {
                        format!("{}.{}", prefix, base_topic)
                    }
                } else {
                    base_topic
                };
                ctx.proxy_context.request_info = Some(request_info.clone());

                let operation_name = request_info
                    .gql
                    .as_ref()
                    .and_then(|g| g.operation_name.clone());
                ctx.proxy_context.active_operation = operation_name.clone();

                // Zero-overhead preview extraction: reuse already-parsed AST without re-parsing or formatting on hot path
                let (qp, vp) = if let Some(gql) = &request_info.gql {
                    let json = gql.json_body();
                    let q = json.get("query").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let v = json.get("variables").and_then(|vars| {
                        if vars.is_null()
                            || (vars.is_object() && vars.as_object().map_or(false, |o| o.is_empty()))
                        {
                            None
                        } else {
                            Some(vars.to_string()) // compact unformatted JSON; lazy-formatted in client browser
                        }
                    });
                    (q, v)
                } else if !body_str.is_empty() {
                    (Some(body_str.chars().take(500).collect()), None)
                } else {
                    (None, None)
                };

                ctx.proxy_context.query_preview = qp;
                ctx.proxy_context.variables_preview = vp;

                // Evaluate Request Interceptors (global + route-specific)
                let req_pipeline = dynamic
                    .interceptor_manager
                    .get_request_pipeline(operation_name.as_deref());
                if !req_pipeline.is_empty() {
                    let mut interceptor_ctx = crate::interceptors::InterceptorContext::new(
                        ctx.proxy_context.request_id,
                        ctx.proxy_context.hlc,
                    );
                    if let Some(g) = &request_info.gql {
                        interceptor_ctx.operation_name = g.operation_name.clone();
                        interceptor_ctx.operation_type = Some(g.operation_type.clone());
                        interceptor_ctx.json_body = Some(g.json_body().clone());
                    }

                    let mut req_parts = session.req_header().as_owned_parts();
                    let verdict = req_pipeline.intercept_request(
                        &mut interceptor_ctx,
                        &mut req_parts,
                        &body_str,
                    );

                    match verdict {
                        crate::interceptors::InterceptorVerdict::Pass => {}
                        crate::interceptors::InterceptorVerdict::Audit { rule_name, tag, reason } => {
                            log::info!("Request flagged by audit rule '{}': tag={:?}, reason={}", rule_name, tag, reason);
                            ctx.proxy_context.audit_tag = tag;
                            ctx.proxy_context.audit_rule = Some(rule_name);
                        }
                        crate::interceptors::InterceptorVerdict::Reject(rejection) => {
                            log::info!(
                                "Request rejected at edge by interceptor: code={}, status={}",
                                rejection.code,
                                rejection.status_code
                            );
                            if let Some(key) = ctx.proxy_context.idempotency_key.take() {
                                self.idempotency_engine.remove(&key).await;
                            }
                            let err_body = rejection.to_graphql_response();
                            let mut header = pingora::http::ResponseHeader::build(
                                rejection.status_code.as_u16(),
                                None,
                            )?;
                            let _ = header.insert_header("content-type", "application/json");
                            let _ = header.insert_header(
                                REQUEST_ID_HEADER,
                                ctx.proxy_context.request_id.to_string(),
                            );
                            let _ = header.insert_header(
                                "x-spectra-hlc",
                                ctx.proxy_context.hlc.to_compact_string(),
                            );

                            let err_bytes = bytes::Bytes::from(err_body);
                            let _ = header.insert_header("content-length", err_bytes.len().to_string());
                            if let Err(e) = session.write_response_header(Box::new(header), false).await {
                                log::debug!("Client disconnected before edge rejection header write: {}", e);
                                return Ok(true);
                            }
                            if let Err(e) = session
                                    .write_response_body(Some(err_bytes), true)
                                    .await
                            {
                                log::debug!("Client disconnected before edge rejection body write: {}", e);
                                return Ok(true);
                            }

                            let client_ip = crate::admin::extract_client_ip(session).to_string();

                            // Emit edge rejection audit event to broker
                            let op_clean = request_info
                                .gql
                                .as_ref()
                                .and_then(|g| g.operation_name.clone())
                                .unwrap_or_else(|| "anonymous".to_string())
                                .to_ascii_lowercase();
                            let rejection_topic = crate::telemetry::topic::TopicResolver::rejection_topic(&op_clean);

                            let rejection_payload = serde_json::json!({
                                "requestId": ctx.proxy_context.request_id.to_string(),
                                "hlc": ctx.proxy_context.hlc.to_compact_string(),
                                "operationName": request_info.gql.as_ref().and_then(|g| g.operation_name.clone()),
                                "operationType": request_info.gql.as_ref().map(|g| g.operation_type.to_string()).unwrap_or_else(|| "GQL".to_string()),
                                "rejectionCode": rejection.code,
                                "statusCode": rejection.status_code.as_u16(),
                                "reason": rejection.message,
                                "clientIp": client_ip,
                                "timestamp": crate::admin::now_epoch_ms(),
                                "queryPreview": ctx.proxy_context.query_preview.clone(),
                                "variablesPreview": ctx.proxy_context.variables_preview.clone(),
                            });

                            let audit_handler = dispatch_method.clone();
                            let audit_topic = rejection_topic.clone();
                            let audit_payload = rejection_payload.to_string();
                            tokio::spawn(async move {
                                let _ = audit_handler.dispatch_payload(&audit_topic, &audit_payload).await;
                            });

                            self.record_traffic_event(
                                session,
                                ctx,
                                TrafficEventDetails {
                                    operation_name: request_info.gql.as_ref().and_then(|g| g.operation_name.clone()),
                                    operation_type: request_info.gql.as_ref().map(|g| g.operation_type.to_string()).unwrap_or_else(|| "GQL".to_string()),
                                    mode: "Rejected (Edge)".to_string(),
                                    status_code: rejection.status_code.as_u16(),
                                    receipt_status: Some("REJECTED".to_string()),
                                    target: format!("Sink: {}", rejection_topic),
                                    error_preview: Some(format!("{}: {}", rejection.code, rejection.message)),
                                    ..Default::default()
                                },
                            );

                            return Ok(true);
                        }
                        crate::interceptors::InterceptorVerdict::Transform { headers, body } => {
                            if let Some(h) = headers {
                                for (k, v) in h {
                                    if let Some(k) = k {
                                        let _ = session
                                            .req_header_mut()
                                            .insert_header(k, v.to_str().unwrap_or(""));
                                    }
                                }
                            }
                            if let Some(b) = body {
                                if let Ok(s) = std::str::from_utf8(&b) {
                                    body_str = s.to_string();
                                }
                                ctx.proxy_context.buffer = b;
                            }
                        }
                    }
                }

                    // If no explicit idempotency key, register fingerprint for mutations
                    let is_mutation = request_info
                        .gql
                        .as_ref()
                        .map(|g| g.operation_type == crate::protocol::GraphQLOperationType::Mutation)
                        .unwrap_or(false);

                    if ctx.proxy_context.idempotency_key.is_none() && is_mutation {
                        let client_id = session
                            .req_header()
                            .headers
                            .get("x-forwarded-for")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("client")
                            .to_string();
                        let op_name = request_info
                            .gql
                            .as_ref()
                            .and_then(|g| g.operation_name.as_deref());
                        match IdempotencyFilter::handle_fingerprint(
                            session,
                            &self.idempotency_engine,
                            &ctx.proxy_context.request_id,
                            ctx.proxy_context.hlc,
                            &client_id,
                            op_name,
                            &body_str,
                        )
                        .await?
                        {
                            IdempotencyInterceptResult::Conflict => return Ok(true),
                            IdempotencyInterceptResult::Replay => {
                                ctx.proxy_context.is_replay = true;
                                self.record_traffic_event(
                                    session,
                                    ctx,
                                    TrafficEventDetails {
                                        operation_name: op_name.map(|s| s.to_string()),
                                        operation_type: "Mutation".to_string(),
                                        mode: "Replay (Fingerprint)".to_string(),
                                        status_code: 200,
                                        receipt_status: Some("REPLAY".to_string()),
                                        target: "Idempotency Cache".to_string(),
                                        ..Default::default()
                                    },
                                );
                                return Ok(true);
                            }
                            IdempotencyInterceptResult::NewKey(k) => {
                                ctx.proxy_context.idempotency_key = Some(k);
                            }
                            IdempotencyInterceptResult::None => {}
                        }
                    }

                    // Check route overrides using StrategyRouter
                    if let Some(route) = StrategyRouter::match_route(&dynamic.routes, &request_info) {
                        if !route.enabled {
                            let op_name = request_info.gql.as_ref().and_then(|g| g.operation_name.clone());
                            let err_msg = format!("Operation '{}' is disabled by gateway policy", route.operation);
                            let err_json = serde_json::json!({
                                "errors": [{
                                    "message": err_msg,
                                    "extensions": {
                                        "code": "ROUTE_DISABLED",
                                        "operation": route.operation,
                                    }
                                }]
                            });
                            let err_bytes = bytes::Bytes::from(err_json.to_string());
                            let mut header = pingora::http::ResponseHeader::build(403, None)?;
                            let _ = header.insert_header("content-type", "application/json");
                            let _ = header.insert_header("content-length", err_bytes.len().to_string());
                            if let Err(e) = session.write_response_header(Box::new(header), false).await {
                                log::debug!("Client disconnected before disabled route header write: {}", e);
                                return Ok(true);
                            }
                            if let Err(e) = session.write_response_body(Some(err_bytes), true).await {
                                log::debug!("Client disconnected before disabled route body write: {}", e);
                                return Ok(true);
                            }

                            self.record_traffic_event(
                                session,
                                ctx,
                                TrafficEventDetails {
                                    operation_name: op_name,
                                    operation_type: "Mutation".to_string(),
                                    mode: "Rejected (Disabled Route)".to_string(),
                                    status_code: 403,
                                    receipt_status: Some("DISABLED".to_string()),
                                    target: "Route Policy".to_string(),
                                    error_preview: Some(err_msg),
                                    ..Default::default()
                                },
                            );
                            return Ok(true);
                        }

                        if route.mode.is_async_edge_command() {
                            let res = StrategyRouter::handle_mode_b_edge(
                                session,
                                ctx,
                                route,
                                &request_info,
                                &dispatch_method,
                                &self.idempotency_engine,
                            )
                            .await;

                            self.record_traffic_event(
                                session,
                                ctx,
                                TrafficEventDetails {
                                    operation_name: request_info.gql.as_ref().and_then(|g| g.operation_name.clone()),
                                    operation_type: "Mutation".to_string(),
                                    mode: "Async (Receipt)".to_string(),
                                    status_code: 200,
                                    receipt_status: Some(route.receipt_status.clone()),
                                    target: format!("NATS: {}", ctx.proxy_context.request_topic),
                                    ..Default::default()
                                },
                            );

                            return res;
                        }

                        if let Some(addr) =
                            StrategyRouter::resolve_mode_a_upstream(route, &dynamic.named_upstreams)
                        {
                            ctx.proxy_context.target_upstream_addr = Some(addr);
                        }

                        if let Some(policy) = route.dispatch_policy {
                            ctx.proxy_context.dispatch_policy = policy;
                        }
                    }

                    StrategyRouter::handle_mode_a_dispatch_policy(
                        ctx.proxy_context.dispatch_policy,
                        &request_info,
                        &dispatch_method,
                    )
                    .await;
            }
        }

        Ok(false)
    }

    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        if ctx.proxy_context.request_info.is_some() {
            return Ok(());
        }
        if let Some(b) = body {
            ctx.proxy_context.buffer.extend(&b[..]);
        }
        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        resp: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<()> {
        ctx.proxy_context.buffer.clear();
        if let Err(e) = resp.append_header(
            REQUEST_ID_HEADER,
            ctx.proxy_context.request_id.to_string(),
        ) {
            log::warn!(
                "Failed to append response header for request_id: {} {}",
                ctx.proxy_context.request_id,
                e
            )
        }
        if let Err(e) = resp.append_header(
            "x-spectra-hlc",
            ctx.proxy_context.hlc.to_compact_string(),
        ) {
            log::warn!("Failed to append x-spectra-hlc: {}", e);
        }
        ctx.proxy_context.response_parts = Some(resp.as_owned_parts());

        // Check if response interceptors are registered for this operation
        let dynamic = self.dynamic_state.load();
        if dynamic.interceptor_manager.has_response_interceptors(ctx.proxy_context.active_operation.as_deref()) {
            ctx.proxy_context.has_response_interception = true;
            // Remove content-length so downstream HTTP framing uses chunked encoding,
            // avoiding framing corruption if the payload is resized or transformed.
            let _ = resp.remove_header("content-length");
        }

        return Ok(());
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> pingora::Result<Option<Duration>>
    where
        Self::CTX: Send + Sync,
    {
        let dynamic = self.dynamic_state.load();

        // 1. Fast path: If no response interceptors are registered, stream chunks through with zero buffering!
        if !ctx.proxy_context.has_response_interception {
            let status = ctx.proxy_context.response_parts.as_ref().map(|p| p.status);
            let is_err = status.map(|s| s.is_client_error() || s.is_server_error()).unwrap_or(false);

            if let Some(b) = body {
                let max_bytes = dynamic.telemetry_config.error_capture.max_body_bytes;
                if is_err && dynamic.telemetry_config.error_capture.enabled {
                    if ctx.proxy_context.buffer.len() < max_bytes {
                        let to_take = (max_bytes - ctx.proxy_context.buffer.len()).min(b.len());
                        ctx.proxy_context.buffer.extend_from_slice(&b[..to_take]);
                    }
                } else if ctx.proxy_context.dispatch_policy != crate::core::config::ModeADispatchPolicy::ResponseOnly
                    && ctx.proxy_context.dispatch_policy != crate::core::config::ModeADispatchPolicy::None
                {
                    const MAX_TELEMETRY_RESPONSE_BYTES: usize = 64 * 1024; // 64KB cap for telemetry logging
                    if ctx.proxy_context.buffer.len() < MAX_TELEMETRY_RESPONSE_BYTES {
                        let to_take = (MAX_TELEMETRY_RESPONSE_BYTES - ctx.proxy_context.buffer.len()).min(b.len());
                        ctx.proxy_context.buffer.extend_from_slice(&b[..to_take]);
                    }
                }
            }

            if end_of_stream {
                let body_str = std::str::from_utf8(&ctx.proxy_context.buffer).unwrap_or_default();
                if is_err && dynamic.telemetry_config.error_capture.enabled {
                    ctx.proxy_context.error_preview = Some(body_str.to_string());
                }
                let response_body = match ctx.proxy_context.response_parts.as_ref() {
                    Some(parts) => ResponseBody::new(&parts.headers, body_str),
                    None => ResponseBody::new(&http::HeaderMap::new(), body_str),
                };
                ctx.proxy_context.response_body = Some(response_body);
                ctx.proxy_context.buffer.clear();
            }

            return Ok(None);
        }

        // 2. Intercepted path: Buffer chunks until end_of_stream to allow full-body inspection/transformation
        if let Some(b) = body.take() {
            ctx.proxy_context.buffer.extend(&b[..]);
        }

        if end_of_stream {
            let resp_pipeline = dynamic
                .interceptor_manager
                .get_response_pipeline(ctx.proxy_context.active_operation.as_deref());
            let mut interceptor_ctx = crate::interceptors::InterceptorContext::new(
                ctx.proxy_context.request_id,
                ctx.proxy_context.hlc,
            );
            interceptor_ctx.duration_ms = ctx.proxy_context.start_time.elapsed().as_millis() as u64;
            interceptor_ctx.operation_name = ctx.proxy_context.active_operation.clone();

            let mut fake_parts = http::response::Response::builder()
                .body(())
                .unwrap()
                .into_parts()
                .0;
            if let Some(parts) = &ctx.proxy_context.response_parts {
                fake_parts.status = parts.status;
                fake_parts.headers = parts.headers.clone();
            }

            let verdict = resp_pipeline.intercept_response(
                &interceptor_ctx,
                &mut fake_parts,
                &ctx.proxy_context.buffer,
            );

            let max_bytes = self.telemetry_config.error_capture.max_body_bytes;
            let final_bytes = match verdict {
                crate::interceptors::InterceptorVerdict::Pass => {
                    std::mem::take(&mut ctx.proxy_context.buffer)
                }
                crate::interceptors::InterceptorVerdict::Transform { headers: _, body } => {
                    body.unwrap_or_else(|| std::mem::take(&mut ctx.proxy_context.buffer))
                }
                crate::interceptors::InterceptorVerdict::Reject(rejection) => {
                    log::info!(
                        "Response rejected by interceptor: code={}, status={}",
                        rejection.code,
                        rejection.status_code
                    );
                    ctx.proxy_context.error_preview = Some(format!("{}: {}", rejection.code, rejection.message));
                    rejection.to_graphql_response().into_bytes()
                }
                crate::interceptors::InterceptorVerdict::Audit { rule_name, tag, reason } => {
                    log::info!(
                        "Response flagged by audit rule '{}': tag={:?}, reason={}",
                        rule_name,
                        tag,
                        reason
                    );
                    ctx.proxy_context.audit_tag = tag;
                    ctx.proxy_context.audit_rule = Some(rule_name);
                    let preview_len = ctx.proxy_context.buffer.len().min(max_bytes);
                    let mut preview = String::from_utf8_lossy(&ctx.proxy_context.buffer[..preview_len]).to_string();
                    if ctx.proxy_context.buffer.len() > max_bytes {
                        preview.push_str("... [truncated]");
                    }
                    ctx.proxy_context.response_preview = Some(preview);
                    std::mem::take(&mut ctx.proxy_context.buffer)
                }
            };

            let body_str = std::str::from_utf8(&final_bytes).unwrap_or_default().to_string();
            let response_body = match ctx.proxy_context.response_parts.as_ref() {
                Some(parts) => ResponseBody::new(&parts.headers, &body_str),
                None => ResponseBody::new(&http::HeaderMap::new(), &body_str),
            };
            ctx.proxy_context.response_body = Some(response_body);
            ctx.proxy_context.buffer.clear();

            *body = Some(Bytes::from(final_bytes));
        }

        Ok(None)
    }

    async fn logging(&self, session: &mut Session, e: Option<&pingora::Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let dispatch_method = match ctx.service.as_ref() {
            Some(s) => s.get_dispatch_method().clone(),
            None => return,
        };

        if !ctx.proxy_context.is_mode_b_terminated && !ctx.proxy_context.is_replay {
            let path = session.req_header().uri.path();
            if path.starts_with("/gql") || path.starts_with("/graphql") {
                let status_code = ctx
                    .proxy_context
                    .response_parts
                    .as_ref()
                    .map(|p| p.status.as_u16())
                    .unwrap_or(if e.is_some() { 502 } else { 200 });

                let op_type = ctx
                    .proxy_context
                    .request_info
                    .as_ref()
                    .and_then(|r| r.gql.as_ref())
                    .map(|g| g.operation_type.to_string())
                    .unwrap_or_else(|| "Query".to_string());

                let op_name = ctx
                    .proxy_context
                    .active_operation
                    .clone()
                    .or_else(|| {
                        ctx.proxy_context
                            .request_info
                            .as_ref()
                            .and_then(|r| r.gql.as_ref())
                            .and_then(|g| g.operation_name.clone())
                    });

                let target_str = if let Some(t) = ctx.proxy_context.target_upstream_addr {
                    format!("Upstream: {}", t)
                } else if let Some(h) = ctx.service_handle {
                    format!("Upstream: {}", self.get_service_config(h).upstream_addr)
                } else {
                    "Upstream".to_string()
                };

                let (query_preview, variables_preview) = if ctx.proxy_context.query_preview.is_some() {
                    (
                        ctx.proxy_context.query_preview.clone(),
                        ctx.proxy_context.variables_preview.clone(),
                    )
                } else {
                    let qp = ctx.proxy_context.request_info.as_ref().and_then(|r| {
                        if !r.http.uri.query().unwrap_or("").is_empty() {
                            Some(r.http.uri.to_string())
                        } else {
                            None
                        }
                    });
                    (qp, None)
                };

                if let Some(err) = e {
                    ctx.proxy_context.error_preview = Some(err.to_string());
                }

                self.record_traffic_event(
                    session,
                    ctx,
                    TrafficEventDetails {
                        operation_name: op_name,
                        operation_type: op_type,
                        mode: "Sync (Proxy)".to_string(),
                        status_code,
                        receipt_status: None,
                        target: target_str,
                        query_preview,
                        variables_preview,
                        ..Default::default()
                    },
                );
            }
        }

        TelemetryDispatcher::dispatch_logging(
            session,
            e,
            ctx,
            &dispatch_method,
            &self.idempotency_engine,
        )
        .await;
    }
}

#[allow(unused_imports)]
pub use crate::gateway::filters::strategy::generate_command_receipt;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::clock::HlcClock;
    use crate::core::types::ExecutionStrategy;
    use crate::protocol::parse_graphql_operation;
    use std::net::SocketAddr;

    #[test]
    fn test_generate_command_receipt_schema() {
        let (command_id, hlc) = HlcClock::global().now_uuidv7();
        let receipt = generate_command_receipt("importCatalog", &command_id, &hlc, "ACCEPTED");

        assert_eq!(
            receipt["data"]["importCatalog"]["commandId"],
            command_id.to_string()
        );
        assert_eq!(
            receipt["data"]["importCatalog"]["status"],
            "ACCEPTED"
        );
        assert_eq!(
            receipt["data"]["importCatalog"]["hlc"],
            hlc.to_compact_string()
        );
    }

    #[test]
    fn test_multi_upstream_routing_resolution() {
        let mut named_upstreams = HashMap::new();
        let default_addr: SocketAddr = "127.0.0.1:4000".parse().unwrap();
        let inventory_addr: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let crm_addr: SocketAddr = "127.0.0.1:5002".parse().unwrap();

        named_upstreams.insert("default".to_string(), default_addr);
        named_upstreams.insert("inventory".to_string(), inventory_addr);
        named_upstreams.insert("crm".to_string(), crm_addr);

        let mut routes = HashMap::new();
        routes.insert(
            "inventory_update".to_string(),
            SpectraRouteConfig {
                operation: "adjustInventory".to_string(),
                mode: ExecutionStrategy::SyncUpstreamExecution,
                enabled: true,
                upstream: Some("inventory".to_string()),
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
                dispatch_policy: None,
            },
        );
        routes.insert(
            "customer_address".to_string(),
            SpectraRouteConfig {
                operation: "updateCustomerAddress".to_string(),
                mode: ExecutionStrategy::SyncUpstreamExecution,
                enabled: true,
                upstream: Some("crm".to_string()),
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
                dispatch_policy: None,
            },
        );
        routes.insert(
            "bulk_import".to_string(),
            SpectraRouteConfig {
                operation: "importCatalog".to_string(),
                mode: ExecutionStrategy::AsyncCommandReceipt,
                enabled: true,
                upstream: None,
                receipt_status: "ACCEPTED".to_string(),
                interceptors: vec![],
                dispatch_policy: None,
            },
        );

        let proxy = CompositeServiceProxy::new().with_routing(
            named_upstreams,
            SpectraModeAConfig::default(),
            routes,
        );

        // 1. Mutation matching inventory override
        let op_inv = parse_graphql_operation("mutation { adjustInventory(itemId: 1) { id } }").unwrap();
        let matched_inv = proxy.routes.values().find(|r| op_inv.matches_operation(&r.operation));
        assert!(matched_inv.is_some());
        let inv_route = matched_inv.unwrap();
        assert_eq!(inv_route.mode, ExecutionStrategy::SyncUpstreamExecution);
        let target_addr = proxy.named_upstreams.get(inv_route.upstream.as_ref().unwrap()).unwrap();
        assert_eq!(*target_addr, inventory_addr);

        // 2. Mutation matching CRM override
        let op_crm = parse_graphql_operation("mutation UpdateAddress { updateCustomerAddress(id: 1) { ok } }").unwrap();
        let matched_crm = proxy.routes.values().find(|r| op_crm.matches_operation(&r.operation));
        assert!(matched_crm.is_some());
        let crm_route = matched_crm.unwrap();
        assert_eq!(crm_route.mode, ExecutionStrategy::SyncUpstreamExecution);
        let target_addr = proxy.named_upstreams.get(crm_route.upstream.as_ref().unwrap()).unwrap();
        assert_eq!(*target_addr, crm_addr);

        // 3. Mutation matching Async Command Receipt (zero upstream hop)
        let op_mode_b = parse_graphql_operation("mutation { importCatalog(file: \"a.csv\") { status } }").unwrap();
        let matched_b = proxy.routes.values().find(|r| op_mode_b.matches_operation(&r.operation));
        assert!(matched_b.is_some());
        let b_route = matched_b.unwrap();
        assert_eq!(b_route.mode, ExecutionStrategy::AsyncCommandReceipt);
        assert_eq!(b_route.receipt_status, "ACCEPTED");

        // 4. Query or unmapped mutation -> falls back to default upstream
        let op_query = parse_graphql_operation("query { hero { name } }").unwrap();
        let matched_query = proxy.routes.values().find(|r| op_query.matches_operation(&r.operation));
        assert!(matched_query.is_none());

        let op_unmapped = parse_graphql_operation("mutation { addReview(stars: 5) { id } }").unwrap();
        let matched_unmapped = proxy.routes.values().find(|r| op_unmapped.matches_operation(&r.operation));
        assert!(matched_unmapped.is_none());
    }

    #[test]
    fn test_composite_proxy_multi_app_resolution() {
        let apps = vec![
            crate::core::SpectraAppConfig {
                id: "coeval".to_string(),
                name: "Open CoEval".to_string(),
                domains: vec!["coeval.bio".to_string()],
                path_prefixes: vec!["/coeval".to_string()],
                upstream: "coeval_core".to_string(),
                subject_prefix: Some("mutation.coeval".to_string()),
            },
            crate::core::SpectraAppConfig {
                id: "humanbase".to_string(),
                name: "HumanBase".to_string(),
                domains: vec!["humanbase.bio".to_string()],
                path_prefixes: vec!["/humanbase".to_string()],
                upstream: "humanbase_core".to_string(),
                subject_prefix: None,
            },
        ];

        let proxy = CompositeServiceProxy::new().with_apps(apps, Some("coeval".to_string()));

        // Resolve by domain
        let resolved_domain = proxy.resolve_app(Some("coeval.bio"), None, "/graphql").unwrap();
        assert_eq!(resolved_domain.id, "coeval");

        // Resolve by header override
        let resolved_header = proxy.resolve_app(Some("coeval.bio"), Some("humanbase"), "/graphql").unwrap();
        assert_eq!(resolved_header.id, "humanbase");
        assert_eq!(resolved_header.effective_subject_prefix(), "mutation.humanbase");

        // Resolve by path prefix
        let resolved_path = proxy.resolve_app(None, None, "/humanbase/api").unwrap();
        assert_eq!(resolved_path.id, "humanbase");

        // Fallback to default
        let resolved_default = proxy.resolve_app(Some("other.internal"), None, "/graphql").unwrap();
        assert_eq!(resolved_default.id, "coeval");
    }

    #[test]
    fn test_dynamic_gateway_state_and_hot_reload() {
        let initial_toml = r#"
bind_addr = "0.0.0.0:8000"

[upstream]
name = "default"
addr = "127.0.0.1:4000"

[dispatch]
method = "nats"
addr = "127.0.0.1:4222"
name = "default"

[gql.routes.initialOp]
operation = "initialOp"
mode = "AsyncCommandReceipt"
enabled = true
"#;
        let cfg1 = crate::core::SpectraConfig::from_toml_str(initial_toml).unwrap();
        let dynamic_state_obj = DynamicGatewayState::new_from_config(&cfg1, initial_toml.to_string(), 1).unwrap();
        assert_eq!(dynamic_state_obj.version, 1);
        assert!(dynamic_state_obj.routes.contains_key("initialOp"));

        let arc_swap_state = Arc::new(arc_swap::ArcSwap::from_pointee(dynamic_state_obj));
        let proxy = CompositeServiceProxy::new().with_dynamic_state(arc_swap_state.clone());

        let loaded1 = proxy.dynamic_state.load();
        assert_eq!(loaded1.version, 1);
        assert!(loaded1.routes.contains_key("initialOp"));

        let updated_toml = r#"
bind_addr = "0.0.0.0:8000"

[upstream]
name = "default"
addr = "127.0.0.1:4000"

[dispatch]
method = "nats"
addr = "127.0.0.1:4222"
name = "default"

[gql.routes.updatedOp]
operation = "updatedOp"
mode = "SyncUpstreamExecution"
enabled = true
"#;
        let cfg2 = crate::core::SpectraConfig::from_toml_str(updated_toml).unwrap();
        let new_state = proxy.reload_config(&cfg2, updated_toml.to_string()).unwrap();

        assert_eq!(new_state.version, 2);
        assert!(new_state.routes.contains_key("updatedOp"));
        assert!(!new_state.routes.contains_key("initialOp"));

        let loaded2 = arc_swap_state.load();
        assert_eq!(loaded2.version, 2);
        assert!(loaded2.routes.contains_key("updatedOp"));
    }

    #[test]
    fn test_dynamic_dns_status_and_rescan() {
        let toml_str = r#"
bind_addr = "0.0.0.0:8000"

[upstream]
name = "default"
addr = "127.0.0.1:4000"

[upstreams.inventory]
name = "inv"
addr = "127.0.0.1:4001"

[dns]
enabled = true
interval_secs = 10

[dispatch]
method = "nats"
addr = "127.0.0.1:4222"
name = "default"
"#;
        let cfg = crate::core::SpectraConfig::from_toml_str(toml_str).unwrap();
        let state = DynamicGatewayState::new_from_config(&cfg, toml_str.to_string(), 1).unwrap();
        let dynamic_state = Arc::new(arc_swap::ArcSwap::from_pointee(state));

        // 1. Test get_dns_status
        let status = DynamicGatewayState::get_dns_status(&dynamic_state);
        assert!(status.enabled);
        assert_eq!(status.interval_secs, 10);
        assert_eq!(status.total_upstreams, 3);
        assert!(status.upstreams.iter().any(|u| u.name == "default" && u.target == "127.0.0.1:4000"));
        assert!(status.upstreams.iter().any(|u| u.name == "inventory" && u.target == "127.0.0.1:4001"));
        assert!(status.upstreams.iter().any(|u| u.name == "inv" && u.target == "127.0.0.1:4001"));

        // 2. Test rescan_dns
        let rescan = DynamicGatewayState::rescan_dns(&dynamic_state).unwrap();
        assert_eq!(rescan.status, "success");
        assert_eq!(rescan.total_upstreams, 3);
        assert_eq!(rescan.changed_count, 0);
        assert!(rescan.duration_ms >= 0.0);
    }
}


