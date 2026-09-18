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

    // When running in an interactive terminal, register a SIGHUP guard so that closing
    // the terminal window cleanly shuts down the process instead of leaving an orphaned zombie.
    #[cfg(unix)]
    {
        let is_interactive = unsafe {
            libc::isatty(libc::STDIN_FILENO) != 0 || libc::isatty(libc::STDOUT_FILENO) != 0
        };
        if is_interactive {
            unsafe {
                extern "C" fn handle_interactive_sighup(_: libc::c_int) {
                    unsafe {
                        libc::_exit(0);
                    }
                }
                libc::signal(libc::SIGHUP, handle_interactive_sighup as *const () as libc::sighandler_t);
            }
            log::info!("Interactive terminal detected: SIGHUP exit guard registered");
        }
    }

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

    if spectra_configuration.dns.enabled {
        composite_service.spawn_dns_refresher();
    }

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
