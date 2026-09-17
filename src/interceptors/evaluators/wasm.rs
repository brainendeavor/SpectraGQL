use crate::interceptors::context::{
    InterceptorContext, InterceptorRejection, InterceptorVerdict,
};
use crate::interceptors::request::RequestInterceptor;
use crate::interceptors::response::ResponseInterceptor;
use crate::interceptors::rules::RuleEvaluator;
use anyhow::{anyhow, Context, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use wasmtime::{Engine, Instance, Module, Store, StoreLimits, StoreLimitsBuilder};

/// Policy defining behavior when an interceptor circuit is open or unrecoverable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailMode {
    /// Reject requests with HTTP 503 Service Unavailable when the circuit is open.
    #[default]
    FailClosed,
    /// Pass requests through unaffected (InterceptorVerdict::Pass) when the circuit is open.
    FailOpen,
}

/// Configuration for the plugin circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub consecutive_failure_threshold: u32,
    pub cooloff_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            consecutive_failure_threshold: 5,
            cooloff_duration: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitPermission {
    Allow,
    Probe,
    Denied,
}

/// Thread-safe circuit breaker protecting the gateway from repeating plugin failures.
#[derive(Debug)]
pub struct WasmCircuitBreaker {
    config: CircuitBreakerConfig,
    failures: AtomicU32,
    state: RwLock<CircuitState>,
    last_tripped_at: RwLock<Option<Instant>>,
}

impl WasmCircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            failures: AtomicU32::new(0),
            state: RwLock::new(CircuitState::Closed),
            last_tripped_at: RwLock::new(None),
        }
    }

    pub fn can_execute(&self) -> CircuitPermission {
        let current_state = *self.state.read();
        match current_state {
            CircuitState::Closed => CircuitPermission::Allow,
            CircuitState::Open => {
                let tripped_at = *self.last_tripped_at.read();
                if let Some(t) = tripped_at
                    && t.elapsed() >= self.config.cooloff_duration
                {
                    let mut state_lock = self.state.write();
                    if *state_lock == CircuitState::Open {
                        *state_lock = CircuitState::HalfOpen;
                        return CircuitPermission::Probe;
                    }
                }
                CircuitPermission::Denied
            }
            CircuitState::HalfOpen => CircuitPermission::Probe,
        }
    }

    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        let mut state_lock = self.state.write();
        *state_lock = CircuitState::Closed;
    }

    pub fn record_failure(&self) {
        let prev = self.failures.fetch_add(1, Ordering::Relaxed);
        let failures = prev + 1;
        let mut state_lock = self.state.write();
        if *state_lock == CircuitState::HalfOpen || failures >= self.config.consecutive_failure_threshold {
            *state_lock = CircuitState::Open;
            let mut tripped_lock = self.last_tripped_at.write();
            *tripped_lock = Some(Instant::now());
        }
    }

    pub fn is_open(&self) -> bool {
        *self.state.read() == CircuitState::Open
    }
}

/// Configuration for an individual WASM plugin.
#[derive(Debug, Clone)]
pub struct WasmPluginConfig {
    pub timeout_ms: u64,
    pub max_memory_bytes: usize,
    pub fail_mode: FailMode,
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for WasmPluginConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 10,
            max_memory_bytes: 16 * 1024 * 1024, // 16 MB
            fail_mode: FailMode::FailClosed,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

/// Global configuration for the Wasmtime engine and runtime environment.
#[derive(Debug, Clone)]
pub struct WasmEngineConfig {
    /// In production mode (strict_aot = true), loading raw .wasm is rejected.
    pub strict_aot: bool,
    /// Explicitly permits Cranelift JIT compilation for local development.
    pub allow_jit: bool,
    /// Resolution of the background epoch interruption ticker (default 1ms).
    pub epoch_tick_interval_ms: u64,
}

impl Default for WasmEngineConfig {
    fn default() -> Self {
        Self {
            strict_aot: true,
            allow_jit: false,
            epoch_tick_interval_ms: 1,
        }
    }
}

struct RegisteredPlugin {
    module: Arc<Module>,
    config: WasmPluginConfig,
    circuit_breaker: Arc<WasmCircuitBreaker>,
}

struct HostState {
    limits: StoreLimits,
}

/// High-performance Wasmtime evaluation host with zero-JIT AOT execution,
/// dynamic per-plugin deadlines, and circuit breaker resilience.
pub struct WasmInterceptorEvaluator {
    config: WasmEngineConfig,
    engine: Engine,
    plugins: RwLock<HashMap<String, Arc<RegisteredPlugin>>>,
    ticker_running: Arc<AtomicBool>,
}

impl WasmInterceptorEvaluator {
    pub fn new(config: WasmEngineConfig) -> Result<Self> {
        let mut wasm_cfg = wasmtime::Config::new();
        wasm_cfg.epoch_interruption(true);
        wasm_cfg.cranelift_opt_level(wasmtime::OptLevel::Speed);
        wasm_cfg.allocation_strategy(wasmtime::InstanceAllocationStrategy::pooling());

        let engine = Engine::new(&wasm_cfg)
            .map_err(|e| anyhow!("{:#}", e))
            .context("Failed to initialize Wasmtime Engine with epoch interruption")?;

        let ticker_running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&ticker_running);
        let engine_clone = engine.clone();
        let tick_interval = Duration::from_millis(config.epoch_tick_interval_ms.max(1));

        std::thread::Builder::new()
            .name("spectragql-wasm-epoch-ticker".to_string())
            .spawn(move || {
                while running_clone.load(Ordering::Relaxed) {
                    std::thread::sleep(tick_interval);
                    engine_clone.increment_epoch();
                }
            })
            .context("Failed to spawn Wasmtime epoch ticker thread")?;

        Ok(Self {
            config,
            engine,
            plugins: RwLock::new(HashMap::new()),
            ticker_running,
        })
    }

    /// Precompiles WebAssembly bytecode into a target-specific Ahead-of-Time (.cwasm) artifact.
    pub fn precompile(&self, wasm_bytes: &[u8]) -> Result<Vec<u8>> {
        self.engine
            .precompile_module(wasm_bytes)
            .map_err(|e| anyhow!("{:#}", e))
            .context("Failed to precompile WASM bytecode into AOT artifact")
    }

    /// Loads a precompiled AOT (.cwasm) binary via zero-copy mmap.
    /// Recommended for production deployments to avoid Cranelift JIT compilation on the hot path.
    pub fn load_cwasm_file(&self, name: impl Into<String>, path: &Path, plugin_cfg: WasmPluginConfig) -> Result<()> {
        let name = name.into();
        // Module::deserialize_file is unsafe because the caller guarantees the file is not corrupted or malicious.
        let module = unsafe {
            Module::deserialize_file(&self.engine, path)
                .map_err(|e| anyhow!("{:#}", e))
                .with_context(|| format!("Failed to deserialize precompiled .cwasm file from {:?}", path))?
        };

        let plugin = RegisteredPlugin {
            module: Arc::new(module),
            circuit_breaker: Arc::new(WasmCircuitBreaker::new(plugin_cfg.circuit_breaker.clone())),
            config: plugin_cfg,
        };

        self.plugins.write().insert(name, Arc::new(plugin));
        Ok(())
    }

    /// Loads raw WebAssembly bytecode or text format (.wat).
    /// Enforces strict AOT guardrails: errors if strict_aot is active without allow_jit.
    pub fn load_wasm_bytes(&self, name: impl Into<String>, bytes: &[u8], plugin_cfg: WasmPluginConfig) -> Result<()> {
        let name = name.into();

        if self.config.strict_aot && !self.config.allow_jit {
            return Err(anyhow!(
                "Uncompiled WebAssembly binary provided for plugin '{}'. In production mode (strict_aot = true), \
                 precompiled AOT artifacts (.cwasm) are required to eliminate JIT compilation latency. \
                 Precompile using 'evaluator.precompile(&bytes)' or configure 'allow_jit = true' for local development.",
                name
            ));
        }

        if !self.config.strict_aot || self.config.allow_jit {
            log::warn!(
                "WARNING [PERFORMANCE]: Compiling raw WASM plugin '{}' via Cranelift JIT at startup. \
                 This causes cold-start latency and MUST NOT be used in production. Precompile to .cwasm.",
                name
            );
        }

        let module = Module::new(&self.engine, bytes)
            .map_err(|e| anyhow!("{:#}", e))
            .with_context(|| format!("Failed to compile WASM module for plugin '{}'", name))?;

        let plugin = RegisteredPlugin {
            module: Arc::new(module),
            circuit_breaker: Arc::new(WasmCircuitBreaker::new(plugin_cfg.circuit_breaker.clone())),
            config: plugin_cfg,
        };

        self.plugins.write().insert(name, Arc::new(plugin));
        Ok(())
    }

    /// Loads WebAssembly text format (.wat) for testing and development.
    pub fn load_wat(&self, name: impl Into<String>, wat_str: &str, plugin_cfg: WasmPluginConfig) -> Result<()> {
        let bytes = wat::parse_str(wat_str)
            .context("Failed to parse WebAssembly text format (WAT)")?;
        self.load_wasm_bytes(name, &bytes, plugin_cfg)
    }

    /// Directly checks whether a plugin circuit breaker is currently open.
    pub fn is_circuit_open(&self, plugin_name: &str) -> bool {
        self.plugins
            .read()
            .get(plugin_name)
            .map(|p| p.circuit_breaker.is_open())
            .unwrap_or(false)
    }

    /// Evaluates an inbound HTTP request through the designated WASM plugin.
    pub fn intercept_request(
        &self,
        plugin_name: &str,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> InterceptorVerdict {
        let plugin = match self.plugins.read().get(plugin_name).cloned() {
            Some(p) => p,
            None => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    "WASM_PLUGIN_NOT_FOUND",
                    format!("WASM plugin '{}' is not registered", plugin_name),
                ));
            }
        };

        // Circuit breaker check
        match plugin.circuit_breaker.can_execute() {
            CircuitPermission::Denied => {
                log::warn!("WASM plugin '{}' invocation blocked by open circuit breaker", plugin_name);
                return match plugin.config.fail_mode {
                    FailMode::FailOpen => InterceptorVerdict::Pass,
                    FailMode::FailClosed => InterceptorVerdict::Reject(InterceptorRejection::new(
                        http::StatusCode::SERVICE_UNAVAILABLE,
                        "WASM_CIRCUIT_OPEN",
                        format!("WASM plugin '{}' circuit is open due to consecutive failures", plugin_name),
                    )),
                };
            }
            CircuitPermission::Allow | CircuitPermission::Probe => {}
        }

        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|val| (k.as_str().to_string(), val.to_string())))
            .collect();

        let input_payload = serde_json::json!({
            "request_id": ctx.request_id.to_string(),
            "operation_name": ctx.operation_name,
            "operation_type": ctx.operation_type.as_ref().map(|op| format!("{:?}", op)),
            "uri": parts.uri.to_string(),
            "method": parts.method.as_str(),
            "headers": headers,
            "body": body,
        });

        match self.invoke_guest(&plugin, "spectragql_intercept_request", &input_payload) {
            Ok(verdict_val) => {
                plugin.circuit_breaker.record_success();
                Self::parse_verdict(verdict_val)
            }
            Err(e) => {
                plugin.circuit_breaker.record_failure();
                Self::handle_execution_error(plugin_name, &plugin.config, e)
            }
        }
    }

    /// Evaluates an outbound HTTP response through the designated WASM plugin.
    pub fn intercept_response(
        &self,
        plugin_name: &str,
        ctx: &InterceptorContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict {
        let plugin = match self.plugins.read().get(plugin_name).cloned() {
            Some(p) => p,
            None => {
                return InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    "WASM_PLUGIN_NOT_FOUND",
                    format!("WASM plugin '{}' is not registered", plugin_name),
                ));
            }
        };

        // Circuit breaker check
        match plugin.circuit_breaker.can_execute() {
            CircuitPermission::Denied => {
                log::warn!("WASM plugin '{}' invocation blocked by open circuit breaker", plugin_name);
                return match plugin.config.fail_mode {
                    FailMode::FailOpen => InterceptorVerdict::Pass,
                    FailMode::FailClosed => InterceptorVerdict::Reject(InterceptorRejection::new(
                        http::StatusCode::SERVICE_UNAVAILABLE,
                        "WASM_CIRCUIT_OPEN",
                        format!("WASM plugin '{}' circuit is open due to consecutive failures", plugin_name),
                    )),
                };
            }
            CircuitPermission::Allow | CircuitPermission::Probe => {}
        }

        let headers: Vec<(String, String)> = parts
            .headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|val| (k.as_str().to_string(), val.to_string())))
            .collect();

        let body_str = String::from_utf8_lossy(body);

        let input_payload = serde_json::json!({
            "request_id": ctx.request_id.to_string(),
            "status_code": parts.status.as_u16(),
            "headers": headers,
            "body": body_str,
        });

        match self.invoke_guest(&plugin, "spectragql_intercept_response", &input_payload) {
            Ok(verdict_val) => {
                plugin.circuit_breaker.record_success();
                Self::parse_verdict(verdict_val)
            }
            Err(e) => {
                plugin.circuit_breaker.record_failure();
                Self::handle_execution_error(plugin_name, &plugin.config, e)
            }
        }
    }

    fn invoke_guest(
        &self,
        plugin: &RegisteredPlugin,
        export_name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(plugin.config.max_memory_bytes)
            .build();

        let host_state = HostState { limits };
        let mut store = Store::new(&self.engine, host_state);
        store.limiter(|state| &mut state.limits);

        // Dynamic timeout calculation
        let deadline_ticks = (plugin.config.timeout_ms / self.config.epoch_tick_interval_ms.max(1)).max(1);
        store.set_epoch_deadline(deadline_ticks);

        let instance = Instance::new(&mut store, &plugin.module, &[])
            .map_err(|e| anyhow!("{:#}", e))
            .context("Failed to instantiate WASM plugin module")?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow!("WASM plugin does not export 'memory'"))?;

        let alloc_fn = instance
            .get_typed_func::<u32, u32>(&mut store, "spectragql_allocate")
            .map_err(|e| anyhow!("{:#}", e))
            .context("WASM plugin does not export 'spectragql_allocate'")?;

        let dealloc_fn = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "spectragql_deallocate")
            .map_err(|e| anyhow!("{:#}", e))
            .context("WASM plugin does not export 'spectragql_deallocate'")?;

        let handler_fn = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, export_name)
            .map_err(|e| anyhow!("{:#}", e))
            .with_context(|| format!("WASM plugin does not export '{}'", export_name))?;

        let input_bytes = serde_json::to_vec(input)?;
        let input_len = input_bytes.len() as u32;

        let input_ptr = alloc_fn.call(&mut store, input_len)
            .map_err(|e| anyhow!("{:#}", e))?;
        memory.write(&mut store, input_ptr as usize, &input_bytes)
            .map_err(|e| anyhow!("{:#}", e))?;

        // Execute guest function with epoch deadline
        let packed_res = handler_fn.call(&mut store, (input_ptr, input_len))
            .map_err(|e| anyhow!("{:#}", e))?;

        // Deallocate input
        let _ = dealloc_fn.call(&mut store, (input_ptr, input_len));

        let res_ptr = (packed_res >> 32) as u32;
        let res_len = (packed_res & 0xFFFF_FFFF) as u32;

        let mut res_bytes = vec![0u8; res_len as usize];
        memory.read(&store, res_ptr as usize, &mut res_bytes)
            .map_err(|e| anyhow!("{:#}", e))?;

        // Deallocate result
        let _ = dealloc_fn.call(&mut store, (res_ptr, res_len));

        let verdict_val: serde_json::Value = serde_json::from_slice(&res_bytes)
            .context("Failed to parse JSON verdict returned by WASM guest")?;

        Ok(verdict_val)
    }

    fn parse_verdict(val: serde_json::Value) -> InterceptorVerdict {
        let verdict_type = val.get("verdict").and_then(|v| v.as_str()).unwrap_or("pass");
        match verdict_type {
            "pass" => InterceptorVerdict::Pass,
            "reject" => {
                let status_code = val
                    .get("status_code")
                    .and_then(|s| s.as_u64())
                    .and_then(|s| http::StatusCode::from_u16(s as u16).ok())
                    .unwrap_or(http::StatusCode::BAD_REQUEST);

                let code = val
                    .get("code")
                    .and_then(|c| c.as_str())
                    .unwrap_or("WASM_REJECTED")
                    .to_string();

                let message = val
                    .get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Request rejected by WASM interceptor")
                    .to_string();

                let details = val.get("details").cloned();

                let mut rej = InterceptorRejection::new(status_code, code, message);
                if let Some(d) = details {
                    rej = rej.with_details(d);
                }
                InterceptorVerdict::Reject(rej)
            }
            "transform" => {
                let mut header_map: Option<http::HeaderMap> = None;
                if let Some(headers_obj) = val.get("headers").and_then(|h| h.as_object()) {
                    let mut map = http::HeaderMap::new();
                    for (k, v) in headers_obj {
                        if let (Ok(hn), Ok(hv)) = (
                            http::header::HeaderName::from_bytes(k.as_bytes()),
                            http::header::HeaderValue::from_str(v.as_str().unwrap_or("")),
                        ) {
                            map.insert(hn, hv);
                        }
                    }
                    header_map = Some(map);
                }

                let body = val
                    .get("body")
                    .and_then(|b| b.as_str())
                    .map(|b| b.as_bytes().to_vec());

                InterceptorVerdict::Transform {
                    headers: header_map,
                    body,
                }
            }
            _ => InterceptorVerdict::Pass,
        }
    }

    fn handle_execution_error(
        plugin_name: &str,
        config: &WasmPluginConfig,
        err: anyhow::Error,
    ) -> InterceptorVerdict {
        let err_str = err.to_string();
        let is_timeout = err_str.contains("interrupt") || err_str.contains("epoch");

        if is_timeout {
            log::error!("WASM plugin '{}' exceeded execution deadline ({}ms)", plugin_name, config.timeout_ms);
            return match config.fail_mode {
                FailMode::FailOpen => InterceptorVerdict::Pass,
                FailMode::FailClosed => InterceptorVerdict::Reject(InterceptorRejection::new(
                    http::StatusCode::GATEWAY_TIMEOUT,
                    "WASM_EXECUTION_TIMEOUT",
                    format!("WASM plugin '{}' exceeded deadline of {}ms", plugin_name, config.timeout_ms),
                )),
            };
        }

        log::error!("WASM plugin '{}' execution failed: {:#}", plugin_name, err);
        match config.fail_mode {
            FailMode::FailOpen => InterceptorVerdict::Pass,
            FailMode::FailClosed => InterceptorVerdict::Reject(InterceptorRejection::new(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "WASM_EXECUTION_ERROR",
                format!("WASM plugin '{}' failed: {}", plugin_name, err),
            )),
        }
    }
}

impl Drop for WasmInterceptorEvaluator {
    fn drop(&mut self) {
        self.ticker_running.store(false, Ordering::Relaxed);
    }
}

impl RuleEvaluator for WasmInterceptorEvaluator {
    fn evaluate(&self, rule_name: &str, input: &serde_json::Value) -> Result<bool, crate::interceptors::context::RuleEvaluationError> {
        let plugin = self.plugins.read().get(rule_name).cloned().ok_or_else(|| {
            crate::interceptors::context::RuleEvaluationError::NotFound(rule_name.to_string())
        })?;

        match self.invoke_guest(&plugin, "spectragql_evaluate_rule", input) {
            Ok(val) => {
                let passed = val.get("result").and_then(|r| r.as_bool()).unwrap_or(false);
                Ok(passed)
            }
            Err(e) => Err(crate::interceptors::context::RuleEvaluationError::EvaluationFailed(e.to_string())),
        }
    }
}

/// RequestInterceptor adapter for a registered WASM plugin.
pub struct WasmRequestInterceptor {
    evaluator: Arc<WasmInterceptorEvaluator>,
    plugin_name: String,
}

impl WasmRequestInterceptor {
    pub fn new(evaluator: Arc<WasmInterceptorEvaluator>, plugin_name: impl Into<String>) -> Self {
        Self {
            evaluator,
            plugin_name: plugin_name.into(),
        }
    }
}

impl RequestInterceptor for WasmRequestInterceptor {
    fn intercept_request(
        &self,
        ctx: &mut InterceptorContext,
        parts: &mut http::request::Parts,
        body: &str,
    ) -> InterceptorVerdict {
        self.evaluator.intercept_request(&self.plugin_name, ctx, parts, body)
    }
}

/// ResponseInterceptor adapter for a registered WASM plugin.
pub struct WasmResponseInterceptor {
    evaluator: Arc<WasmInterceptorEvaluator>,
    plugin_name: String,
}

impl WasmResponseInterceptor {
    pub fn new(evaluator: Arc<WasmInterceptorEvaluator>, plugin_name: impl Into<String>) -> Self {
        Self {
            evaluator,
            plugin_name: plugin_name.into(),
        }
    }
}

impl ResponseInterceptor for WasmResponseInterceptor {
    fn intercept_response(
        &self,
        ctx: &InterceptorContext,
        parts: &mut http::response::Parts,
        body: &[u8],
    ) -> InterceptorVerdict {
        self.evaluator.intercept_response(&self.plugin_name, ctx, parts, body)
    }
}
