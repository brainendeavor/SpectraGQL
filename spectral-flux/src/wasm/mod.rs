use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use wasmtime::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
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

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub consecutive_failure_threshold: u32,
    pub cooloff_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            consecutive_failure_threshold: 5,
            cooloff_duration: Duration::from_secs(15),
        }
    }
}

pub struct WasmCircuitBreaker {
    config: CircuitBreakerConfig,
    failures: AtomicU32,
    state: RwLock<CircuitState>,
    last_tripped_at: RwLock<Option<Instant>>,
    probing: AtomicBool,
}

impl WasmCircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            failures: AtomicU32::new(0),
            state: RwLock::new(CircuitState::Closed),
            last_tripped_at: RwLock::new(None),
            probing: AtomicBool::new(false),
        }
    }

    pub fn can_execute(&self) -> CircuitPermission {
        let current_state = *self.state.read().unwrap();
        match current_state {
            CircuitState::Closed => CircuitPermission::Allow,
            CircuitState::Open => {
                let tripped_at = *self.last_tripped_at.read().unwrap();
                if let Some(t) = tripped_at {
                    if t.elapsed() >= self.config.cooloff_duration {
                        let mut state_lock = self.state.write().unwrap();
                        if *state_lock == CircuitState::Open {
                            *state_lock = CircuitState::HalfOpen;
                            self.probing.store(true, Ordering::SeqCst);
                            return CircuitPermission::Probe;
                        }
                    }
                }
                CircuitPermission::Denied
            }
            CircuitState::HalfOpen => {
                if self
                    .probing
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    CircuitPermission::Probe
                } else {
                    CircuitPermission::Denied
                }
            }
        }
    }

    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        self.probing.store(false, Ordering::SeqCst);
        let mut state_lock = self.state.write().unwrap();
        *state_lock = CircuitState::Closed;
    }

    pub fn record_failure(&self) {
        self.probing.store(false, Ordering::SeqCst);
        let prev = self.failures.fetch_add(1, Ordering::Relaxed);
        let failures = prev + 1;
        let mut state_lock = self.state.write().unwrap();
        if *state_lock == CircuitState::HalfOpen
            || failures >= self.config.consecutive_failure_threshold
        {
            *state_lock = CircuitState::Open;
            let mut tripped_lock = self.last_tripped_at.write().unwrap();
            *tripped_lock = Some(Instant::now());
        }
    }

    pub fn is_open(&self) -> bool {
        *self.state.read().unwrap() == CircuitState::Open
    }
}

#[derive(Debug, Clone)]
pub struct FluxcellWasmConfig {
    pub timeout_ms: u64,
    pub max_memory_bytes: usize,
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for FluxcellWasmConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 25,
            max_memory_bytes: 16 * 1024 * 1024, // 16 MB
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

struct RegisteredFluxcell {
    #[allow(dead_code)]
    name: String,
    module: Arc<Module>,
    config: FluxcellWasmConfig,
    circuit_breaker: Arc<WasmCircuitBreaker>,
    subscriptions: Vec<String>,
    routes: Vec<crate::http::RouteDefinition>,
}

struct HostState {
    limits: StoreLimits,
}

pub struct WasmHost {
    engine: Engine,
    epoch_tick_interval_ms: u64,
    ticker_running: Arc<AtomicBool>,
    fluxcells: RwLock<HashMap<String, Arc<RegisteredFluxcell>>>,
    #[allow(dead_code)]
    storage: Option<Arc<dyn crate::storage::FluxStorage>>,
}

impl WasmHost {
    pub fn new(
        epoch_tick_interval_ms: u64,
        storage: Option<Arc<dyn crate::storage::FluxStorage>>,
    ) -> Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        config.cranelift_opt_level(OptLevel::Speed);

        let engine = Engine::new(&config)
            .map_err(|e| anyhow!("{:#}", e))
            .context("Failed to initialize Wasmtime Engine with epoch interruption")?;

        let ticker_running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&ticker_running);
        let engine_clone = engine.clone();
        let tick_interval = Duration::from_millis(epoch_tick_interval_ms.max(1));

        std::thread::Builder::new()
            .name("spectral-flux-epoch-ticker".to_string())
            .spawn(move || {
                while running_clone.load(Ordering::Relaxed) {
                    std::thread::sleep(tick_interval);
                    engine_clone.increment_epoch();
                }
            })
            .context("Failed to spawn epoch ticker thread")?;

        Ok(Self {
            engine,
            epoch_tick_interval_ms,
            ticker_running,
            fluxcells: RwLock::new(HashMap::new()),
            storage,
        })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn register_wat(
        &self,
        name: &str,
        wat_str: &str,
        config: FluxcellWasmConfig,
    ) -> Result<()> {
        let bytes = wat::parse_str(wat_str)
            .context("Failed to parse WebAssembly text format (WAT)")?;
        self.register_wasm_bytes(name, &bytes, config)
    }

    pub fn register_wasm_bytes(
        &self,
        name: &str,
        bytes: &[u8],
        config: FluxcellWasmConfig,
    ) -> Result<()> {
        let module = Module::new(&self.engine, bytes)
            .map_err(|e| anyhow!("{:#}", e))
            .with_context(|| format!("Failed to compile WASM module for fluxcell '{}'", name))?;

        let cb = Arc::new(WasmCircuitBreaker::new(config.circuit_breaker.clone()));

        // Query guest exports for routes & subscriptions if available
        let (subscriptions, routes) = self.query_guest_metadata(&module, &config)?;

        let reg = RegisteredFluxcell {
            name: name.to_string(),
            module: Arc::new(module),
            config,
            circuit_breaker: cb,
            subscriptions,
            routes,
        };

        self.fluxcells
            .write()
            .unwrap()
            .insert(name.to_string(), Arc::new(reg));
        Ok(())
    }

    fn query_guest_metadata(
        &self,
        module: &Module,
        config: &FluxcellWasmConfig,
    ) -> Result<(Vec<String>, Vec<crate::http::RouteDefinition>)> {
        // Query get-subscriptions and get-routes if exported by module
        let mut subscriptions = Vec::new();
        let mut routes = Vec::new();

        let limits = StoreLimitsBuilder::new()
            .memory_size(config.max_memory_bytes)
            .build();
        let mut store = Store::new(&self.engine, HostState { limits });
        store.limiter(|s| &mut s.limits);

        // Epoch deadline is mandatory when epoch_interruption is enabled on Engine
        let deadline_ticks = (config.timeout_ms / self.epoch_tick_interval_ms.max(1)).max(10);
        store.set_epoch_deadline(deadline_ticks);

        if let Ok(instance) = Instance::new(&mut store, module, &[]) {
            // Attempt to query get_subscriptions
            if let Ok(func) = instance.get_typed_func::<(), u64>(&mut store, "get_subscriptions") {
                if let Ok(packed) = func.call(&mut store, ()) {
                    if let Ok(json_str) = self.read_guest_memory_string(&mut store, &instance, packed) {
                        if let Ok(subs) = serde_json::from_str::<Vec<String>>(&json_str) {
                            subscriptions = subs;
                        }
                    }
                }
            }

            // Attempt to query get_routes
            if let Ok(func) = instance.get_typed_func::<(), u64>(&mut store, "get_routes") {
                if let Ok(packed) = func.call(&mut store, ()) {
                    if let Ok(json_str) = self.read_guest_memory_string(&mut store, &instance, packed) {
                        #[derive(serde::Deserialize)]
                        struct RouteMeta {
                            method: String,
                            path: String,
                            description: String,
                        }
                        if let Ok(r_list) = serde_json::from_str::<Vec<RouteMeta>>(&json_str) {
                            routes = r_list
                                .into_iter()
                                .map(|r| crate::http::RouteDefinition {
                                    method: r.method,
                                    relative_path: r.path,
                                    description: r.description,
                                })
                                .collect();
                        }
                    }
                }
            }
        }

        Ok((subscriptions, routes))
    }

    fn read_guest_memory_string(
        &self,
        store: &mut Store<HostState>,
        instance: &Instance,
        packed: u64,
    ) -> Result<String> {
        let ptr = (packed >> 32) as usize;
        let len = (packed & 0xFFFF_FFFF) as usize;
        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| anyhow!("Fluxcell does not export 'memory'"))?;

        let mem_size = memory.data_size(&*store);
        if ptr.saturating_add(len) > mem_size || len > 16 * 1024 * 1024 {
            return Err(anyhow!(
                "Invalid guest memory range: offset {} + len {} exceeds memory capacity {}",
                ptr,
                len,
                mem_size
            ));
        }

        let mut bytes = vec![0u8; len];
        memory.read(&*store, ptr, &mut bytes)
            .map_err(|e| anyhow!("{:#}", e))?;
        String::from_utf8(bytes).map_err(|e| anyhow!("Invalid UTF-8 from guest: {}", e))
    }

    pub fn get_fluxcell_routes(&self, name: &str) -> Option<Vec<crate::http::RouteDefinition>> {
        self.fluxcells
            .read()
            .unwrap()
            .get(name)
            .map(|f| f.routes.clone())
    }

    pub fn get_fluxcell_subscriptions(&self, name: &str) -> Option<Vec<String>> {
        self.fluxcells
            .read()
            .unwrap()
            .get(name)
            .map(|f| f.subscriptions.clone())
    }

    pub fn is_circuit_open(&self, name: &str) -> bool {
        self.fluxcells
            .read()
            .unwrap()
            .get(name)
            .map(|f| f.circuit_breaker.is_open())
            .unwrap_or(false)
    }

    pub fn unregister_fluxcell(&self, name: &str) -> bool {
        self.fluxcells.write().unwrap().remove(name).is_some()
    }

    pub fn list_registered_fluxcells(&self) -> Vec<String> {
        let mut list: Vec<String> = self.fluxcells.read().unwrap().keys().cloned().collect();
        list.sort();
        list
    }

    pub fn invoke_http(
        &self,
        fluxcell_name: &str,
        relative_path: &str,
        method: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
        let fluxcell = self
            .fluxcells
            .read()
            .unwrap()
            .get(fluxcell_name)
            .cloned()
            .ok_or_else(|| anyhow!("Fluxcell '{}' not registered", fluxcell_name))?;

        // 1. Circuit breaker gate
        match fluxcell.circuit_breaker.can_execute() {
            CircuitPermission::Denied => {
                return Err(anyhow!(
                    "Fluxcell '{}' circuit breaker is OPEN due to consecutive failures",
                    fluxcell_name
                ));
            }
            CircuitPermission::Allow | CircuitPermission::Probe => {}
        }

        // 2. Prepare payload
        let req_json = serde_json::json!({
            "path": relative_path,
            "method": method,
            "headers": headers,
            "body": String::from_utf8_lossy(&body),
        });

        // 3. Execution with epoch timeout
        match self.invoke_guest_json(&fluxcell, "handle_http", &req_json) {
            Ok(resp_json) => {
                fluxcell.circuit_breaker.record_success();
                let status = resp_json.get("status").and_then(|s| s.as_u64()).unwrap_or(200) as u16;
                let resp_headers: Vec<(String, String)> = resp_json
                    .get("headers")
                    .and_then(|h| serde_json::from_value(h.clone()).ok())
                    .unwrap_or_default();
                let resp_body = resp_json
                    .get("body")
                    .and_then(|b| b.as_str())
                    .map(|s| s.as_bytes().to_vec())
                    .unwrap_or_default();

                Ok((status, resp_headers, resp_body))
            }
            Err(e) => {
                fluxcell.circuit_breaker.record_failure();
                Err(e)
            }
        }
    }

    fn invoke_guest_json(
        &self,
        fluxcell: &RegisteredFluxcell,
        function_name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(fluxcell.config.max_memory_bytes)
            .build();
        let mut store = Store::new(&self.engine, HostState { limits });
        store.limiter(|s| &mut s.limits);

        // Epoch timeout ticks
        let deadline_ticks = (fluxcell.config.timeout_ms / self.epoch_tick_interval_ms.max(1)).max(1);
        store.set_epoch_deadline(deadline_ticks);

        let instance = Instance::new(&mut store, &fluxcell.module, &[])
            .map_err(|e| anyhow!("{:#}", e))
            .context("Failed to instantiate WASM module")?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow!("WASM module does not export 'memory'"))?;

        let alloc_fn = instance
            .get_typed_func::<u32, u32>(&mut store, "allocate")
            .map_err(|e| anyhow!("{:#}", e))
            .context("WASM module does not export 'allocate'")?;

        let dealloc_fn = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "deallocate")
            .map_err(|e| anyhow!("{:#}", e))
            .context("WASM module does not export 'deallocate'")?;

        let handler_fn = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, function_name)
            .map_err(|e| anyhow!("{:#}", e))
            .with_context(|| format!("WASM module does not export '{}'", function_name))?;

        let input_bytes = serde_json::to_vec(input)?;
        let input_len = input_bytes.len() as u32;

        let input_ptr = alloc_fn.call(&mut store, input_len).map_err(|e| anyhow!("{:#}", e))?;
        memory.write(&mut store, input_ptr as usize, &input_bytes).map_err(|e| anyhow!("{:#}", e))?;

        let packed_res = handler_fn.call(&mut store, (input_ptr, input_len)).map_err(|e| anyhow!("{:#}", e))?;
        let _ = dealloc_fn.call(&mut store, (input_ptr, input_len));

        let res_ptr = (packed_res >> 32) as usize;
        let res_len = (packed_res & 0xFFFF_FFFF) as usize;

        let mem_size = memory.data_size(&store);
        if res_ptr.saturating_add(res_len) > mem_size || res_len > fluxcell.config.max_memory_bytes {
            return Err(anyhow!(
                "Invalid guest memory range: offset {} + len {} exceeds memory capacity {} (max configured {})",
                res_ptr,
                res_len,
                mem_size,
                fluxcell.config.max_memory_bytes
            ));
        }

        let mut res_bytes = vec![0u8; res_len];
        memory.read(&store, res_ptr, &mut res_bytes).map_err(|e| anyhow!("{:#}", e))?;

        serde_json::from_slice(&res_bytes).map_err(|e| anyhow!("Failed to parse guest JSON: {}", e))
    }
}

impl Drop for WasmHost {
    fn drop(&mut self) {
        self.ticker_running.store(false, Ordering::Relaxed);
    }
}

impl WasmHost {
    /// Dispatches HTTP requests to native built-in fluxcells when no guest WASM module is loaded.
    pub async fn dispatch_builtin(
        &self,
        fluxcell_name: &str,
        relative_path: &str,
        _method: &str,
        _headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), anyhow::Error> {
        let clean_path = relative_path.split('?').next().unwrap_or(relative_path);
        let query_str = relative_path.split_once('?').map(|(_, q)| q).unwrap_or("");

        match fluxcell_name {
            "magic_link" | "magic-link" => {
                if clean_path == "/verify" {
                    let mut token_opt = None;
                    for param in query_str.split('&') {
                        if let Some((k, v)) = param.split_once('=') {
                            if k == "token" {
                                token_opt = Some(v.to_string());
                                break;
                            }
                        }
                    }
                    if token_opt.is_none() && !body.is_empty() {
                        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&body) {
                            token_opt = val.get("token").and_then(|t| t.as_str()).map(|s| s.to_string());
                        }
                    }

                    if let Some(token) = token_opt {
                        if let Some(storage) = &self.storage {
                            let key = format!("magic_token:{}", token);
                            if let Ok(Some(email)) = storage.get_del(&key).await {
                                let session_id = uuid::Uuid::now_v7().to_string();
                                let resp = serde_json::json!({
                                    "status": "VERIFIED",
                                    "email": email,
                                    "session_id": session_id,
                                });
                                let resp_bytes = serde_json::to_vec(&resp).unwrap();
                                let headers = vec![("content-type".to_string(), "application/json".to_string())];
                                return Ok((200, headers, resp_bytes));
                            }
                        }
                    }

                    let err_resp = serde_json::json!({
                        "error": "INVALID_OR_EXPIRED_TOKEN",
                        "message": "The magic link token is invalid, expired, or has already been redeemed."
                    });
                    let resp_bytes = serde_json::to_vec(&err_resp).unwrap();
                    let headers = vec![("content-type".to_string(), "application/json".to_string())];
                    return Ok((401, headers, resp_bytes));
                } else if clean_path == "/status" {
                    let status = serde_json::json!({
                        "status": "ok",
                        "fluxcell": "magic_link",
                        "routes": fluxcell_magic_link::get_routes(),
                        "subscriptions": fluxcell_magic_link::get_subscriptions(),
                    });
                    let resp_bytes = serde_json::to_vec(&status).unwrap();
                    let headers = vec![("content-type".to_string(), "application/json".to_string())];
                    return Ok((200, headers, resp_bytes));
                }
            }
            "webhook" => {
                if clean_path == "/health" {
                    let status = serde_json::json!({ "status": "ok", "fluxcell": "webhook" });
                    let resp_bytes = serde_json::to_vec(&status).unwrap();
                    let headers = vec![("content-type".to_string(), "application/json".to_string())];
                    return Ok((200, headers, resp_bytes));
                } else if clean_path == "/dlq" {
                    let dlq_entries: Vec<fluxcell_webhook::DlqEntry> = Vec::new();
                    let resp_bytes = serde_json::to_vec(&dlq_entries).unwrap();
                    let headers = vec![("content-type".to_string(), "application/json".to_string())];
                    return Ok((200, headers, resp_bytes));
                } else if clean_path == "/test" {
                    let resp = serde_json::json!({ "status": "delivered" });
                    let resp_bytes = serde_json::to_vec(&resp).unwrap();
                    let headers = vec![("content-type".to_string(), "application/json".to_string())];
                    return Ok((200, headers, resp_bytes));
                }
            }
            _ => {}
        }

        Err(anyhow!("Fluxcell '{}' not registered", fluxcell_name))
    }
}

#[async_trait::async_trait]
impl crate::http::FluxcellHttpDispatcher for WasmHost {
    async fn dispatch(
        &self,
        fluxcell_name: &str,
        relative_path: &str,
        method: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), anyhow::Error> {
        match self.invoke_http(fluxcell_name, relative_path, method, headers.clone(), body.clone()) {
            Ok(res) => Ok(res),
            Err(e) => {
                // If WASM guest not registered, fallback to native built-in fluxcells
                match self.dispatch_builtin(fluxcell_name, relative_path, method, headers, body).await {
                    Ok(builtin_res) => Ok(builtin_res),
                    Err(_) => Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_circuit_breaker_lifecycle() {
        let cb = WasmCircuitBreaker::new(CircuitBreakerConfig {
            consecutive_failure_threshold: 3,
            cooloff_duration: Duration::from_millis(50),
        });

        assert_eq!(cb.can_execute(), CircuitPermission::Allow);
        assert!(!cb.is_open());

        // 1st failure
        cb.record_failure();
        assert_eq!(cb.can_execute(), CircuitPermission::Allow);

        // 2nd failure
        cb.record_failure();
        assert_eq!(cb.can_execute(), CircuitPermission::Allow);

        // 3rd failure: trips breaker open
        cb.record_failure();
        assert_eq!(cb.can_execute(), CircuitPermission::Denied);
        assert!(cb.is_open());

        // During cooloff: denied
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(cb.can_execute(), CircuitPermission::Denied);

        // After cooloff: transitions to half-open probe
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(cb.can_execute(), CircuitPermission::Probe);

        // Success resets to closed
        cb.record_success();
        assert_eq!(cb.can_execute(), CircuitPermission::Allow);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_wasm_host_initialization() {
        let host = WasmHost::new(5, None).unwrap();
        assert!(!host.is_circuit_open("non-existent"));
    }

    #[test]
    fn test_wasm_host_wat_execution() {
        // String: {"status":200,"headers":[],"body":"pong"}
        // Length: 41 bytes (0x29 in hex)
        let wat = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 2048) "{\"status\":200,\"headers\":[],\"body\":\"pong\"}")
          (func (export "allocate") (param i32) (result i32)
            i32.const 1024
          )
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            ;; offset 2048 (0x800), len 41 (0x29) -> (0x800 << 32) | 0x29
            i64.const 0x00000800_00000029
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("test-cell", wat, FluxcellWasmConfig::default()).unwrap();

        let (status, headers, body) = host.invoke_http(
            "test-cell",
            "/ping",
            "GET",
            vec![],
            vec![],
        ).unwrap();

        assert_eq!(status, 200);
        assert!(headers.is_empty());
        assert_eq!(String::from_utf8(body).unwrap(), "pong");
    }

    #[test]
    fn test_wasm_host_delayed_execution() {
        let wat = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 2048) "{\"status\":200,\"headers\":[],\"body\":\"alive\"}")
          (func (export "allocate") (param i32) (result i32) i32.const 1024)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            i64.const 0x00000800_0000002a
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("delayed-cell", wat, FluxcellWasmConfig::default()).unwrap();

        // Let the engine epoch ticker run for 100ms
        std::thread::sleep(Duration::from_millis(100));

        // Execution should still succeed without premature epoch deadline abort
        let (status, _, body) = host.invoke_http("delayed-cell", "/test", "GET", vec![], vec![]).unwrap();
        assert_eq!(status, 200);
        assert_eq!(String::from_utf8(body).unwrap(), "alive");
    }

    #[test]
    fn test_wasm_host_epoch_timeout_on_infinite_loop() {
        let wat_infinite = r#"
        (module
          (memory (export "memory") 1)
          (func (export "allocate") (param i32) (result i32) i32.const 1024)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            (loop (br 0))
            i64.const 0
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        let cfg = FluxcellWasmConfig {
            timeout_ms: 20, // 20ms timeout
            max_memory_bytes: 16 * 1024 * 1024,
            circuit_breaker: CircuitBreakerConfig::default(),
        };
        host.register_wat("infinite-cell", wat_infinite, cfg).unwrap();

        let start = Instant::now();
        let result = host.invoke_http("infinite-cell", "/loop", "POST", vec![], vec![]);
        let elapsed = start.elapsed();

        // Must fail with epoch timeout error
        assert!(result.is_err());
        // Must complete within ~100ms (not hang indefinitely)
        assert!(elapsed < Duration::from_millis(300), "Infinite loop did not terminate in time: {:?}", elapsed);
    }

    #[test]
    fn test_wasm_host_circuit_breaker_trips_on_repeated_failures() {
        let wat_fail = r#"
        (module
          (memory (export "memory") 1)
          (func (export "allocate") (param i32) (result i32) i32.const 1024)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            (unreachable)
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        let cfg = FluxcellWasmConfig {
            timeout_ms: 50,
            max_memory_bytes: 16 * 1024 * 1024,
            circuit_breaker: CircuitBreakerConfig {
                consecutive_failure_threshold: 3,
                cooloff_duration: Duration::from_millis(100),
            },
        };
        host.register_wat("failing-cell", wat_fail, cfg).unwrap();

        assert!(!host.is_circuit_open("failing-cell"));

        // Fail 3 times to trip breaker
        for _ in 0..3 {
            let _ = host.invoke_http("failing-cell", "/fail", "GET", vec![], vec![]);
        }

        assert!(host.is_circuit_open("failing-cell"));

        // 4th call should fail immediately at circuit breaker gate
        let err = host.invoke_http("failing-cell", "/fail", "GET", vec![], vec![]).unwrap_err();
        assert!(err.to_string().contains("circuit breaker is OPEN"));
    }

    #[test]
    fn test_wasm_host_metadata_reflection() {
        // String: ["webhook.dispatch","mutation.*"]
        // Length: 33 bytes (0x21) -> (0x400 << 32) | 0x21
        // String: [{"method":"GET","path":"/health","description":"Health"}]
        // Length: 58 bytes (0x3a) -> (0x800 << 32) | 0x3a
        let wat = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 1024) "[\"webhook.dispatch\",\"mutation.*\"]")
          (data (i32.const 2048) "[{\"method\":\"GET\",\"path\":\"/health\",\"description\":\"Health\"}]")
          (func (export "get_subscriptions") (result i64)
            i64.const 0x00000400_00000021
          )
          (func (export "get_routes") (result i64)
            i64.const 0x00000800_0000003a
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("meta-cell", wat, FluxcellWasmConfig::default()).unwrap();

        let subs = host.get_fluxcell_subscriptions("meta-cell").unwrap();
        assert_eq!(subs, vec!["webhook.dispatch", "mutation.*"]);

        let routes = host.get_fluxcell_routes("meta-cell").unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].method, "GET");
        assert_eq!(routes[0].relative_path, "/health");
        assert_eq!(routes[0].description, "Health");
    }

    #[test]
    fn test_wasm_host_multi_cell_coexistence() {
        let cell1_wat = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 1024) "{\"status\":200,\"headers\":[],\"body\":\"from-cell-1\"}")
          (func (export "allocate") (param i32) (result i32) i32.const 512)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            i64.const 0x00000400_00000030
          )
        )
        "#;

        let cell2_wat = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 1024) "{\"status\":201,\"headers\":[],\"body\":\"from-cell-2\"}")
          (func (export "allocate") (param i32) (result i32) i32.const 512)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            i64.const 0x00000400_00000030
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("cell-1", cell1_wat, FluxcellWasmConfig::default()).unwrap();
        host.register_wat("cell-2", cell2_wat, FluxcellWasmConfig::default()).unwrap();

        let (status1, _, body1) = host.invoke_http("cell-1", "/", "GET", vec![], vec![]).unwrap();
        assert_eq!(status1, 200);
        assert_eq!(String::from_utf8(body1).unwrap(), "from-cell-1");

        let (status2, _, body2) = host.invoke_http("cell-2", "/", "GET", vec![], vec![]).unwrap();
        assert_eq!(status2, 201);
        assert_eq!(String::from_utf8(body2).unwrap(), "from-cell-2");
    }

    #[test]
    fn test_wasm_adversarial_huge_packed_pointer_oom_protection() {
        // Module returns packed pointer: offset 0, length 0xFFFF_FFFF (4GB)
        let wat_oom_bomb = r#"
        (module
          (memory (export "memory") 1)
          (func (export "allocate") (param i32) (result i32) i32.const 0)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            i64.const 0x00000000_FFFFFFFF
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("oom-bomb", wat_oom_bomb, FluxcellWasmConfig::default()).unwrap();

        // Must fail gracefully without host OOM panic or kernel crash
        let err = host.invoke_http("oom-bomb", "/test", "GET", vec![], vec![]).unwrap_err();
        assert!(err.to_string().contains("Invalid guest memory range"));
    }

    #[test]
    fn test_wasm_adversarial_out_of_bounds_pointer() {
        // Module returns packed pointer: offset 200_000 (beyond 64KB page 1), length 50
        let wat_oob = r#"
        (module
          (memory (export "memory") 1)
          (func (export "allocate") (param i32) (result i32) i32.const 0)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            i64.const 0x00030D40_00000032
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("oob-cell", wat_oob, FluxcellWasmConfig::default()).unwrap();

        let err = host.invoke_http("oob-cell", "/test", "GET", vec![], vec![]).unwrap_err();
        assert!(err.to_string().contains("Invalid guest memory range"));
    }

    #[test]
    fn test_wasm_adversarial_garbage_json_response() {
        // Module returns valid memory slice containing non-JSON garbage
        let wat_garbage = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 1024) "<<GARBAGE BINARY DATA NOT JSON>>")
          (func (export "allocate") (param i32) (result i32) i32.const 0)
          (func (export "deallocate") (param i32 i32))
          (func (export "handle_http") (param i32 i32) (result i64)
            i64.const 0x00000400_00000020
          )
        )
        "#;

        let host = WasmHost::new(5, None).unwrap();
        host.register_wat("garbage-cell", wat_garbage, FluxcellWasmConfig::default()).unwrap();

        let err = host.invoke_http("garbage-cell", "/test", "GET", vec![], vec![]).unwrap_err();
        assert!(err.to_string().contains("Failed to parse guest JSON"));
    }

    #[test]
    fn test_circuit_breaker_concurrent_half_open_single_probe() {
        let cb = Arc::new(WasmCircuitBreaker::new(CircuitBreakerConfig {
            consecutive_failure_threshold: 2,
            cooloff_duration: Duration::from_millis(5),
        }));

        // Trip breaker to Open
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_open());

        // Wait for cooloff
        std::thread::sleep(Duration::from_millis(15));

        // Spawn 50 threads simultaneously attempting can_execute()
        let num_threads = 50;
        let mut handles = Vec::new();
        let probe_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let denied_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let barrier = Arc::new(std::sync::Barrier::new(num_threads));

        for _ in 0..num_threads {
            let cb_clone = cb.clone();
            let barrier_clone = barrier.clone();
            let p_count = probe_count.clone();
            let d_count = denied_count.clone();

            handles.push(std::thread::spawn(move || {
                barrier_clone.wait();
                match cb_clone.can_execute() {
                    CircuitPermission::Probe => {
                        p_count.fetch_add(1, Ordering::SeqCst);
                    }
                    CircuitPermission::Denied => {
                        d_count.fetch_add(1, Ordering::SeqCst);
                    }
                    CircuitPermission::Allow => panic!("Unexpected Allow in HalfOpen"),
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // EXACTLY 1 thread must be admitted as probe, and 49 denied
        assert_eq!(probe_count.load(Ordering::SeqCst), 1, "Expected exactly 1 canary probe in HalfOpen");
        assert_eq!(denied_count.load(Ordering::SeqCst), 49, "Expected remaining threads denied");
    }

    #[test]
    fn test_circuit_breaker_concurrent_failure_burst() {
        let cb = Arc::new(WasmCircuitBreaker::new(CircuitBreakerConfig {
            consecutive_failure_threshold: 5,
            cooloff_duration: Duration::from_millis(100),
        }));

        let num_threads = 20;
        let mut handles = Vec::new();
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));

        for _ in 0..num_threads {
            let cb_clone = cb.clone();
            let barrier_clone = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier_clone.wait();
                cb_clone.record_failure();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(cb.is_open(), "Circuit breaker must be open after failure burst");
        assert_eq!(cb.can_execute(), CircuitPermission::Denied);
    }
}
