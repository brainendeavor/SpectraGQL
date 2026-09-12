use anyhow::Result;
use enum_dispatch::enum_dispatch;
use std::collections::HashSet;
use std::str::FromStr;

use crate::core::clock::HlcTimestamp;
use crate::protocol::{GraphQLOperationType, GraphQLRequestInfo, RequestInfo};

#[enum_dispatch]
pub trait RequestDecoder {
    fn decode_request(
        &self,
        request_id: uuid::Uuid,
        hlc: HlcTimestamp,
        http_request_parts: http::request::Parts,
        http_request_body: &str,
    ) -> Result<RequestInfo>;
}

#[derive(Clone)]
pub struct GraphQLDecoder {
    pub ops_to_dispatch: HashSet<GraphQLOperationType>,
}

impl GraphQLDecoder {
    pub fn new(ops_to_dispatch: &str) -> Self {
        let ops = HashSet::from_iter(
            ops_to_dispatch
                .split(',')
                .map(|s| GraphQLOperationType::from_str(s.trim()).unwrap()),
        );
        GraphQLDecoder {
            ops_to_dispatch: ops,
        }
    }
}

impl RequestDecoder for GraphQLDecoder {
    fn decode_request(
        &self,
        request_id: uuid::Uuid,
        hlc: HlcTimestamp,
        http_request_parts: http::request::Parts,
        http_request_body: &str,
    ) -> Result<RequestInfo> {
        let mut request_info = RequestInfo::new(request_id, hlc, http_request_parts);
        let mut gql_request_info = GraphQLRequestInfo::new(http_request_body);
        let gql_request_body = gql_request_info.gql_request_body()?;

        let parsed_op = crate::protocol::parser::parse_graphql_operation_with_name(
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

#[derive(Clone, Default)]
pub struct HttpDecoder {}

impl HttpDecoder {
    pub fn new() -> Self {
        HttpDecoder {}
    }
}

impl RequestDecoder for HttpDecoder {
    fn decode_request(
        &self,
        request_id: uuid::Uuid,
        hlc: HlcTimestamp,
        http_request_parts: http::request::Parts,
        _http_request_body: &str,
    ) -> Result<RequestInfo> {
        Ok(RequestInfo::new(request_id, hlc, http_request_parts))
    }
}

#[derive(Clone)]
#[enum_dispatch(RequestDecoder)]
pub enum ProtocolDecoder {
    GraphQL(GraphQLDecoder),
    Http(HttpDecoder),
}
