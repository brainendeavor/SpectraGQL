mod clock;
mod dispatch;
mod payload;
mod proxy;
mod ratify;
mod spectra_config;

use anyhow::Result;
use pingora::proxy::http_proxy_service_with_name;
use pingora::server::Server;
use pingora::server::configuration;

// use crate::gql_parse::GraphQLHandler;
use crate::proxy::{CompositeService, ExtraServiceParams, ServiceConfig}; //GraphQLService, HttpService};
use crate::spectra_config::SpectraConfig;

fn main() -> Result<()> {
    env_logger::init();

    // Create server configuration
    let opt = configuration::Opt::parse_args();
    let mut server = Server::new(Some(opt))?;
    server.bootstrap();

    let spectra_configuration = SpectraConfig::new()?;
    log::info!("SPECTRAGQL CONFIGURATION: {spectra_configuration:?}");

    // ------------------------------------------------------------------------
    // GQL
    // ------------------------------------------------------------------------

    let gql_dispatch = spectra_configuration.gql_dispatch();
    let mut extra_params = ExtraServiceParams::new();
    extra_params.insert(
        "ops_to_dispatch".to_string(),
        spectra_configuration.gql.ops_to_dispatch.clone(),
    );
    let gql_service = ServiceConfig::new(
        "gql",
        spectra_configuration.gql_upstream().addr.as_str(),
        spectra_configuration.gql.paths.as_str(),
        gql_dispatch.method.as_str(),
        gql_dispatch.addr.as_str(),
        extra_params,
        // spectra_configuration.gql.ops_to_dispatch.as_str(),
    );

    // ------------------------------------------------------------------------
    // HTTP/REST
    // ------------------------------------------------------------------------

    let rest_dispatch = spectra_configuration.rest_dispatch();
    let rest_service = ServiceConfig::new(
        "http",
        spectra_configuration.rest_upstream().addr.as_str(),
        spectra_configuration.rest.paths.as_str(),
        rest_dispatch.method.as_str(),
        rest_dispatch.addr.as_str(),
        ExtraServiceParams::new(),
    );

    // ========================================================================
    // Composite Service -> Pingora
    // ------------------------------------------------------------------------

    let named_upstreams = spectra_configuration.resolve_all_upstreams()?;
    let mut composite_service = CompositeService::new().with_routing(
        named_upstreams,
        spectra_configuration.gql.mode_a.clone(),
        spectra_configuration.gql.routes.clone(),
    );
    composite_service.add_service_config(gql_service);
    composite_service.add_service_config(rest_service);
    let mut proxy_service =
        http_proxy_service_with_name(&server.configuration, composite_service, "SpectraGQL");
    // ========================================================================

    // Add proxy service to server
    proxy_service.add_tcp(spectra_configuration.bind_addr.as_str());
    server.add_service(proxy_service);

    log::info!(
        "SpectraGQL Proxy starting on {}, forwarding to {}",
        spectra_configuration.bind_addr,
        &spectra_configuration.upstream.addr
    );
    server.run_forever();
}
