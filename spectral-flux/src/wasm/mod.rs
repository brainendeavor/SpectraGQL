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
                            return CircuitPermission::Probe;
                        }
                    }
                }
                CircuitPermission::Denied
            }
            CircuitState::HalfOpen => CircuitPermission::Probe,
        }
    }

    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        let mut state_lock = self.state.write().unwrap();
        *state_lock = CircuitState::Closed;
    }

    pub fn record_failure(&self) {
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
        self.invoke_http(fluxcell_name, relative_path, method, headers, body)
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
}
