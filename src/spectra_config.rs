// `BIND_ADDR`: Address to listen on (default: "0.0.0.0:8000")
// `UPSTREAM_ADDR`: Upstream server host (default: "localhost:4000")
// `GQL_PATHS`: Comma-separated list of GraphQL endpoint paths (default: "/gql,/graphql")
// `GQL_OPS_TO_DISPATCH`: Comma-separated list of GraphQL operations to dispatch (default: "query,mutation,subscription")
// `REST_PATHS`: Comma-separated list of REST endpoint paths (default: "/api,/api/{*path}")

// use std::collections::HashSet;
use std::env;

// use crate::payload::gql_request_info::GraphQLOperationType;
use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct SpectraUpstreamConfig {
    pub name: String,
    pub description: Option<String>,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SpectraDispatchConfig {
    pub name: String,
    pub description: Option<String>,
    pub method: String,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SpectraRatificationConfig {}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct SpectraGqlConfig {
    pub description: Option<String>,
    pub paths: String,
    pub dispatch_query: bool,
    pub dispatch_mutation: bool,
    pub dispatch_subscription: bool,
    pub ops_to_dispatch: String,
    pub upstream: Option<SpectraUpstreamConfig>,
    pub dispatch: Option<SpectraDispatchConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct SpectraRestConfig {
    pub paths: String,
    pub upstream: Option<SpectraUpstreamConfig>,
    pub dispatch: Option<SpectraDispatchConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct SpectraConfig {
    pub bind_addr: String,
    pub gql: SpectraGqlConfig,
    pub rest: SpectraRestConfig,
    pub upstream: SpectraUpstreamConfig,
    pub dispatch: SpectraDispatchConfig,
}

impl SpectraConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let spectra_env = env::var("SPECTRA_ENV")
            .or_else(|_| env::var("SPECTRAGQL_ENV"))
            .unwrap_or_else(|_| "development".into());

        let config_builder = Config::builder()
            .set_default("bind_addr", "0.0.0.0:8000")?
            .set_default("upstream.addr", "localhost:4000")?
            .set_default("upstream.name", "default")?
            .set_default("dispatch.method", "nats")?
            .set_default("dispatch.addr", "127.0.0.1:4222")?
            .set_default("dispatch.name", "default")?
            .set_default("gql.paths", "/gql,/graphql")?
            .set_default("gql.ops_to_dispatch", "query, mutation, subscription")?
            .set_default("rest.paths", "/api,/api/{*path}")?
            .add_source(File::with_name("spectra").required(false))
            .add_source(File::with_name(&format!("{spectra_env}-spectra")).required(false))
            .add_source(Environment::with_prefix("spectra").separator("_"))
            .add_source(Environment::with_prefix("spectragql").separator("_"))
            .build()?;

        config_builder.try_deserialize()
    }

    pub fn gql_upstream(&self) -> &SpectraUpstreamConfig {
        self.gql.upstream.as_ref().unwrap_or_else(|| &self.upstream)
    }

    pub fn gql_dispatch(&self) -> &SpectraDispatchConfig {
        self.gql.dispatch.as_ref().unwrap_or_else(|| &self.dispatch)
    }

    pub fn rest_upstream(&self) -> &SpectraUpstreamConfig {
        self.rest
            .upstream
            .as_ref()
            .unwrap_or_else(|| &self.upstream)
    }

    pub fn rest_dispatch(&self) -> &SpectraDispatchConfig {
        self.rest
            .dispatch
            .as_ref()
            .unwrap_or_else(|| &self.dispatch)
    }
}
