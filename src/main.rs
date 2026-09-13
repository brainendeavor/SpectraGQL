use anyhow::Result;
use pingora::proxy::http_proxy_service_with_name;
use pingora::server::Server;
use pingora::server::configuration;
use spectragql::build_composite_service;
use spectragql::SpectraConfig;

fn main() -> Result<()> {
    env_logger::init();

    // Create server configuration
    let opt = configuration::Opt::parse_args();
    let mut server = Server::new(Some(opt))?;
    server.bootstrap();

    let spectra_configuration = match SpectraConfig::new() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("CRITICAL: SpectraConfig::new failed: {e:?}");
            return Err(e.into());
        }
    };
    log::info!("SPECTRAGQL CONFIGURATION: {spectra_configuration:?}");

    let composite_service = match build_composite_service(&spectra_configuration) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("CRITICAL: build_composite_service failed: {e:?}");
            return Err(e);
        }
    };

    let mut proxy_service =
        http_proxy_service_with_name(&server.configuration, composite_service, "SpectraGQL");

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
