use anyhow::Result;
use std::collections::HashSet;
use std::str::FromStr;

use crate::payload::{
    GraphQLOperationType, GraphQLRequestInfo, RequestInfo,
};
use crate::ratify::RequestRatification;

#[derive(Clone)]
pub struct GraphQLRatifier {
    #[allow(dead_code)]
    pub ops_to_dispatch: HashSet<GraphQLOperationType>,
}

impl GraphQLRatifier {
    pub fn new(ops_to_dispatch: &str) -> Self {
        let ops = HashSet::from_iter(
            ops_to_dispatch
                .split(',')
                .map(|s| GraphQLOperationType::from_str(s.trim()).unwrap()),
        );

        GraphQLRatifier {
            ops_to_dispatch: ops,
        }
    }
}

impl RequestRatification for GraphQLRatifier {
    fn ratify_request(
        &self,
        request_id: uuid::Uuid,
        hlc: crate::clock::HlcTimestamp,
        http_request_parts: http::request::Parts,
        http_request_body: &str,
    ) -> Result<RequestInfo> {
        log::info!("enum_dispatch RequestRatification -> GraphQLAbstractSyntaxTree!");

        let mut request_info = RequestInfo::new(request_id, hlc, http_request_parts);

        let mut gql_request_info = GraphQLRequestInfo::new(http_request_body);
        let gql_request_body = gql_request_info.gql_request_body()?;

        let parsed_op = crate::payload::gql_parser::parse_graphql_operation_with_name(
            &gql_request_body.query,
            gql_request_body.operation_name.as_deref(),
        )?;
        gql_request_info.operation_name = parsed_op.operation_name;
        gql_request_info.operation_type = parsed_op.operation_type;
        gql_request_info.root_fields = parsed_op.root_fields;
        request_info.gql = Some(gql_request_info);

        Ok(request_info)
    }
}
