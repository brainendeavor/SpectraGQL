use crate::dispatch::{DispatchMethod, find_dispatch_handler_by_method};
use crate::proxy::{ExtraServiceParams, SpectraProxyService};
use crate::ratify::RatificationProtocol;
use crate::ratify::http_ratifier::LuaRatifier; // HttpRequestRatifier

#[derive(Clone)]
pub struct HttpServiceProxy {
    request_handler: RatificationProtocol,
    dispatch_handler: DispatchMethod,
}

impl HttpServiceProxy {
    pub fn new(
        dispatch_method: &str,
        dispatch_endpoint: &str,
        _extra_params: ExtraServiceParams,
    ) -> Self {
        // TODO: make this configurable if lua code file exists
        // no file => HttpRequestRatifier::new().into()
        let request_handler: RatificationProtocol = LuaRatifier::new().into();
        // let request_handler: RatificationProtocol = HttpRequestRatifier::new().into();

        HttpServiceProxy {
            request_handler,
            dispatch_handler: find_dispatch_handler_by_method(dispatch_method, dispatch_endpoint)
                .unwrap(),
        }
    }
}

impl SpectraProxyService for HttpServiceProxy {
    fn get_request_protocol(&self) -> &RatificationProtocol {
        &self.request_handler
    }
    fn get_dispatch_method(&self) -> &DispatchMethod {
        &self.dispatch_handler
    }
}
