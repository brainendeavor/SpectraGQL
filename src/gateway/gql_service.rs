use crate::gateway::{ExtraServiceParams, SpectraProxyService};
use crate::protocol::{GraphQLDecoder, ProtocolDecoder};
use crate::telemetry::{DispatchMethod, find_dispatch_handler_by_method};

#[derive(Clone)]
pub struct GraphQLServiceProxy {
    request_handler: ProtocolDecoder,
    dispatch_handler: DispatchMethod,
}

impl GraphQLServiceProxy {
    pub fn new(
        dispatch_method: &str,
        dispatch_endpoint: &str,
        extra_params: ExtraServiceParams,
    ) -> Self {
        let mut ops_to_dispatch = "query, mutation, subscription";
        if let Some(ops) = extra_params.get("ops_to_dispatch") {
            ops_to_dispatch = ops;
        };

        GraphQLServiceProxy {
            request_handler: GraphQLDecoder::new(ops_to_dispatch).into(),
            dispatch_handler: find_dispatch_handler_by_method(dispatch_method, dispatch_endpoint)
                .unwrap(),
        }
    }
}

impl SpectraProxyService for GraphQLServiceProxy {
    fn get_request_protocol(&self) -> &ProtocolDecoder {
        &self.request_handler
    }
    fn get_dispatch_method(&self) -> &DispatchMethod {
        &self.dispatch_handler
    }
}
