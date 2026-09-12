use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Context, Result, anyhow, bail};

use crate::core::config::{
    InterceptorStage, InterceptorType, SpectraConfig,
};
use crate::interceptors::evaluators::cel::{CelRequestInterceptor, CelResponseInterceptor};
use crate::interceptors::evaluators::wasm::{
    CircuitBreakerConfig, WasmEngineConfig, WasmInterceptorEvaluator, WasmPluginConfig,
    WasmRequestInterceptor, WasmResponseInterceptor,
};
use crate::interceptors::request::{
    GraphQLSyntaxInterceptor, HeaderValidationInterceptor, RequestInterceptor,
    RequestInterceptorPipeline,
};
use crate::interceptors::response::{
    ResponseInterceptor, ResponseInterceptorPipeline, SensitiveDataResponseInterceptor,
};

/// Orchestrates registration, precompilation, and route-level assembly of Request and Response interceptors.
#[derive(Clone)]
pub struct InterceptorManager {
    global_request_pipeline: RequestInterceptorPipeline,
    global_response_pipeline: ResponseInterceptorPipeline,
    route_request_pipelines: HashMap<String, RequestInterceptorPipeline>,
    route_response_pipelines: HashMap<String, ResponseInterceptorPipeline>,
    pub wasm_evaluator: Option<Arc<WasmInterceptorEvaluator>>,
}

impl std::fmt::Debug for InterceptorManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InterceptorManager")
            .field("global_request_len", &self.global_request_pipeline.len())
            .field("global_response_len", &self.global_response_pipeline.len())
            .field("routes_count", &self.route_request_pipelines.len())
            .finish()
    }
}

impl InterceptorManager {
    /// Creates an empty InterceptorManager with zero interceptors configured.
    pub fn empty() -> Self {
        Self {
            global_request_pipeline: RequestInterceptorPipeline::default(),
            global_response_pipeline: ResponseInterceptorPipeline::default(),
            route_request_pipelines: HashMap::new(),
            route_response_pipelines: HashMap::new(),
            wasm_evaluator: None,
        }
    }

    pub fn set_global_request_pipeline(&mut self, pipeline: RequestInterceptorPipeline) {
        self.global_request_pipeline = pipeline;
    }

    pub fn set_global_response_pipeline(&mut self, pipeline: ResponseInterceptorPipeline) {
        self.global_response_pipeline = pipeline;
    }

    pub fn set_route_request_pipeline(
        &mut self,
        operation: impl Into<String>,
        pipeline: RequestInterceptorPipeline,
    ) {
        self.route_request_pipelines.insert(operation.into(), pipeline);
    }

    pub fn set_route_response_pipeline(
        &mut self,
        operation: impl Into<String>,
        pipeline: ResponseInterceptorPipeline,
    ) {
        self.route_response_pipelines.insert(operation.into(), pipeline);
    }

    /// Initializes and validates all interceptors from `SpectraConfig`.
    /// Fails fast if a CEL expression is invalid or if WASM plugins cannot be loaded.
    pub fn from_config(config: &SpectraConfig) -> Result<Self> {
        let has_wasm = config
            .interceptors
            .values()
            .any(|ic| ic.interceptor_type == InterceptorType::Wasm);

        let wasm_evaluator = if has_wasm {
            let wasm_engine_cfg = WasmEngineConfig {
                strict_aot: config.wasm.strict_aot,
                allow_jit: config.wasm.allow_jit,
                epoch_tick_interval_ms: config.wasm.epoch_tick_interval_ms,
            };
            let eval = WasmInterceptorEvaluator::new(wasm_engine_cfg)
                .context("Failed to initialize WasmInterceptorEvaluator")?;
            Some(Arc::new(eval))
        } else {
            None
        };

        // Registry of instantiated interceptors
        let mut request_interceptors: HashMap<String, Arc<dyn RequestInterceptor>> = HashMap::new();
        let mut response_interceptors: HashMap<String, Arc<dyn ResponseInterceptor>> = HashMap::new();

        for (name, ic) in &config.interceptors {
            match ic.interceptor_type {
                InterceptorType::Cel => {
                    let expr = ic.expression.as_deref().ok_or_else(|| {
                        anyhow!("Missing 'expression' for CEL interceptor '{}'", name)
                    })?;
                    let status_code = ic.status_code.map(|c| {
                        http::StatusCode::from_u16(c).unwrap_or(http::StatusCode::FORBIDDEN)
                    });
                    match ic.stage {
                        InterceptorStage::Request => {
                            let interceptor = CelRequestInterceptor::new(
                                expr,
                                status_code,
                                ic.code.clone(),
                                ic.message.clone(),
                            )
                            .map_err(|e| anyhow!("Invalid CEL request rule '{}': {}", name, e))?;
                            request_interceptors.insert(name.clone(), Arc::new(interceptor));
                        }
                        InterceptorStage::Response => {
                            let interceptor = CelResponseInterceptor::new(
                                expr,
                                status_code,
                                ic.code.clone(),
                                ic.message.clone(),
                            )
                            .map_err(|e| anyhow!("Invalid CEL response rule '{}': {}", name, e))?;
                            response_interceptors.insert(name.clone(), Arc::new(interceptor));
                        }
                    }
                }
                InterceptorType::Wasm => {
                    let eval = wasm_evaluator.as_ref().ok_or_else(|| {
                        anyhow!("Wasm engine was not initialized for interceptor '{}'", name)
                    })?;
                    let path = ic.path.as_deref().ok_or_else(|| {
                        anyhow!("Missing 'path' for WASM interceptor '{}'", name)
                    })?;

                    let timeout_ms = ic.timeout_ms.unwrap_or(config.wasm.default_timeout_ms);
                    let fail_mode = ic.fail_mode.unwrap_or_default();
                    let circuit_breaker = CircuitBreakerConfig {
                        consecutive_failure_threshold: ic.failure_threshold.unwrap_or(5),
                        cooloff_duration: std::time::Duration::from_secs(
                            ic.cooloff_duration_secs.unwrap_or(30),
                        ),
                    };

                    let plugin_config = WasmPluginConfig {
                        timeout_ms,
                        max_memory_bytes: 16 * 1024 * 1024,
                        fail_mode,
                        circuit_breaker,
                    };

                    if path.ends_with(".cwasm") {
                        eval.load_cwasm_file(name, std::path::Path::new(path), plugin_config)
                            .with_context(|| format!("Failed to load precompiled .cwasm from '{}'", path))?;
                    } else if path.ends_with(".wat") {
                        let wat_str = std::fs::read_to_string(path)
                            .with_context(|| format!("Failed to read WAT file from '{}'", path))?;
                        eval.load_wat(name, &wat_str, plugin_config)
                            .with_context(|| format!("Failed to load WAT from '{}'", path))?;
                    } else {
                        let bytes = std::fs::read(path)
                            .with_context(|| format!("Failed to read WASM file from '{}'", path))?;
                        eval.load_wasm_bytes(name, &bytes, plugin_config)
                            .with_context(|| format!("Failed to load WASM bytes from '{}'", path))?;
                    }

                    match ic.stage {
                        InterceptorStage::Request => {
                            let interceptor = WasmRequestInterceptor::new(eval.clone(), name.clone());
                            request_interceptors.insert(name.clone(), Arc::new(interceptor));
                        }
                        InterceptorStage::Response => {
                            let interceptor = WasmResponseInterceptor::new(eval.clone(), name.clone());
                            response_interceptors.insert(name.clone(), Arc::new(interceptor));
                        }
                    }
                }
                InterceptorType::Native => {
                    let kind = ic.kind.as_deref().unwrap_or(name.as_str());
                    match (ic.stage, kind) {
                        (InterceptorStage::Request, "graphql_syntax") => {
                            request_interceptors.insert(name.clone(), Arc::new(GraphQLSyntaxInterceptor));
                        }
                        (InterceptorStage::Request, "header_validation") => {
                            request_interceptors.insert(name.clone(), Arc::new(HeaderValidationInterceptor::default()));
                        }
                        (InterceptorStage::Response, "sensitive_data") => {
                            response_interceptors.insert(name.clone(), Arc::new(SensitiveDataResponseInterceptor::new()));
                        }
                        _ => {
                            bail!(
                                "Unknown native interceptor kind '{}' for stage '{:?}' in '{}'",
                                kind,
                                ic.stage,
                                name
                            );
                        }
                    }
                }
            }
        }

        // Build global pipelines
        let mut global_request_pipeline = RequestInterceptorPipeline::default();
        let mut global_response_pipeline = ResponseInterceptorPipeline::default();

        for interceptor_name in &config.gql.interceptors {
            if let Some(req) = request_interceptors.get(interceptor_name) {
                global_request_pipeline = global_request_pipeline.with_arc_interceptor(req.clone());
            } else if let Some(resp) = response_interceptors.get(interceptor_name) {
                global_response_pipeline = global_response_pipeline.with_arc_interceptor(resp.clone());
            } else {
                bail!(
                    "Global interceptor '{}' is not registered in [interceptors]",
                    interceptor_name
                );
            }
        }

        // Build route-specific pipelines
        let mut route_request_pipelines = HashMap::new();
        let mut route_response_pipelines = HashMap::new();

        for (route_key, route_cfg) in &config.gql.routes {
            let mut req_pipe = RequestInterceptorPipeline::default();
            let mut resp_pipe = ResponseInterceptorPipeline::default();

            for interceptor_name in &route_cfg.interceptors {
                if let Some(req) = request_interceptors.get(interceptor_name) {
                    req_pipe = req_pipe.with_arc_interceptor(req.clone());
                } else if let Some(resp) = response_interceptors.get(interceptor_name) {
                    resp_pipe = resp_pipe.with_arc_interceptor(resp.clone());
                } else {
                    bail!(
                        "Route '{}' references unknown interceptor '{}'",
                        route_key,
                        interceptor_name
                    );
                }
            }

            if !req_pipe.is_empty() {
                route_request_pipelines.insert(route_cfg.operation.clone(), req_pipe);
            }
            if !resp_pipe.is_empty() {
                route_response_pipelines.insert(route_cfg.operation.clone(), resp_pipe);
            }
        }

        Ok(Self {
            global_request_pipeline,
            global_response_pipeline,
            route_request_pipelines,
            route_response_pipelines,
            wasm_evaluator,
        })
    }

    /// Composes the request pipeline for an operation: global interceptors always run first,
    /// followed by any route-specific interceptors.
    pub fn get_request_pipeline(&self, operation: Option<&str>) -> RequestInterceptorPipeline {
        let mut pipeline = self.global_request_pipeline.clone();
        if let Some(op) = operation {
            if let Some(route_pipe) = self.route_request_pipelines.get(op) {
                pipeline.extend(route_pipe);
            }
        }
        pipeline
    }

    /// Composes the response pipeline for an operation: global interceptors always run first,
    /// followed by any route-specific interceptors.
    pub fn get_response_pipeline(&self, operation: Option<&str>) -> ResponseInterceptorPipeline {
        let mut pipeline = self.global_response_pipeline.clone();
        if let Some(op) = operation {
            if let Some(route_pipe) = self.route_response_pipelines.get(op) {
                pipeline.extend(route_pipe);
            }
        }
        pipeline
    }

    /// Fast check to determine if any response interceptors are registered for this operation.
    /// If false, response chunks stream directly through with zero buffering.
    pub fn has_response_interceptors(&self, operation: Option<&str>) -> bool {
        if !self.global_response_pipeline.is_empty() {
            return true;
        }
        if let Some(op) = operation {
            if let Some(route_pipe) = self.route_response_pipelines.get(op) {
                return !route_pipe.is_empty();
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::SpectraConfig;

    #[test]
    fn test_interceptor_manager_cel_and_native_composition() {
        let toml_str = r#"
            bind_addr = "0.0.0.0:8000"

            [upstream]
            addr = "127.0.0.1:4000"

            [dispatch]
            name = "default"
            method = "NATS"
            addr = "127.0.0.1:4222"

            [interceptors.global_syntax]
            type = "native"
            stage = "request"
            kind = "graphql_syntax"

            [interceptors.tenant_req]
            type = "cel"
            stage = "request"
            expression = 'request.headers["x-tenant-id"] != ""'
            status_code = 403
            code = "TENANT_REQUIRED"

            [interceptors.ssn_leak]
            type = "cel"
            stage = "response"
            expression = 'data.customer.ssn == "" || data.customer.ssn.startsWith("***-**-")'
            status_code = 500
            code = "DATA_LEAK_PREVENTED"

            [gql]
            paths = "/graphql"
            ops_to_dispatch = "query, mutation"
            interceptors = ["global_syntax"]

            [gql.routes.tenant_op]
            operation = "getTenantData"
            mode = "A"
            interceptors = ["tenant_req"]

            [gql.routes.customer_op]
            operation = "getCustomerData"
            mode = "A"
            interceptors = ["ssn_leak"]

            [rest]
            paths = "/api"
        "#;

        let cfg: SpectraConfig = config::Config::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        let manager = InterceptorManager::from_config(&cfg).expect("Manager should initialize");

        // 1. Check has_response_interceptors
        assert!(!manager.has_response_interceptors(None));
        assert!(!manager.has_response_interceptors(Some("getTenantData")));
        assert!(manager.has_response_interceptors(Some("getCustomerData")));

        // 2. Request pipeline for default op only has global interceptor (len 1)
        let default_pipe = manager.get_request_pipeline(None);
        assert_eq!(default_pipe.len(), 1);

        // 3. Request pipeline for getTenantData has global + route interceptor (len 2)
        let tenant_pipe = manager.get_request_pipeline(Some("getTenantData"));
        assert_eq!(tenant_pipe.len(), 2);

        // 4. Response pipeline for getCustomerData has route interceptor (len 1)
        let cust_resp_pipe = manager.get_response_pipeline(Some("getCustomerData"));
        assert_eq!(cust_resp_pipe.len(), 1);
    }

    #[test]
    fn test_interceptor_manager_invalid_cel_fails_fast() {
        let toml_str = r#"
            bind_addr = "0.0.0.0:8000"

            [upstream]
            addr = "127.0.0.1:4000"

            [dispatch]
            name = "default"
            method = "NATS"
            addr = "127.0.0.1:4222"

            [interceptors.broken_cel]
            type = "cel"
            stage = "request"
            expression = "this is not valid cel %%% syntax"

            [gql]
            paths = "/graphql"
            ops_to_dispatch = "query"
            interceptors = ["broken_cel"]

            [rest]
            paths = "/api"
        "#;

        let cfg: SpectraConfig = config::Config::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        let res = InterceptorManager::from_config(&cfg);
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid CEL request rule 'broken_cel'"));
    }
}
