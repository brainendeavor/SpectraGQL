use crate::dispatch::{DispatchMethod, find_dispatch_handler_by_method};
use crate::proxy::{ExtraServiceParams, SpectraProxyService};
use crate::ratify::RatificationProtocol;
use crate::ratify::gql_ratifier::GraphQLRatifier;

#[derive(Clone)]
pub struct GraphQLServiceProxy {
    request_handler: RatificationProtocol,
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
            request_handler: GraphQLRatifier::new(ops_to_dispatch).into(),
            dispatch_handler: find_dispatch_handler_by_method(dispatch_method, dispatch_endpoint)
                .unwrap(),
        }
    }
}

impl SpectraProxyService for GraphQLServiceProxy {
    fn get_request_protocol(&self) -> &RatificationProtocol {
        &self.request_handler
    }
    fn get_dispatch_method(&self) -> &DispatchMethod {
        &self.dispatch_handler
    }
}
