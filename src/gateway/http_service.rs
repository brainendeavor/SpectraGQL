use crate::gateway::{ExtraServiceParams, SpectraProxyService};
use crate::protocol::{HttpDecoder, ProtocolDecoder};
use crate::telemetry::{DispatchMethod, find_dispatch_handler_by_method};

#[derive(Clone)]
pub struct HttpServiceProxy {
    request_handler: ProtocolDecoder,
    dispatch_handler: DispatchMethod,
}

impl HttpServiceProxy {
    pub fn new(
        dispatch_method: &str,
        dispatch_endpoint: &str,
        _extra_params: ExtraServiceParams,
    ) -> Self {
        let request_handler: ProtocolDecoder = HttpDecoder::new().into();

        HttpServiceProxy {
            request_handler,
            dispatch_handler: find_dispatch_handler_by_method(dispatch_method, dispatch_endpoint)
                .unwrap(),
        }
    }
}

impl SpectraProxyService for HttpServiceProxy {
    fn get_request_protocol(&self) -> &ProtocolDecoder {
        &self.request_handler
    }
    fn get_dispatch_method(&self) -> &DispatchMethod {
        &self.dispatch_handler
    }
}
