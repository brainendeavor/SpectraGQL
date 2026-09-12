pub mod decoder;
pub mod error;
pub mod gql_request;
pub mod http_request;
pub mod parser;
pub mod request_info;
pub mod response_body;
pub mod response_info;
pub mod typestate;

pub use decoder::{GraphQLDecoder, HttpDecoder, ProtocolDecoder, RequestDecoder};
pub use error::{GraphQLError, GraphQLErrorLocation, GraphQLErrorResponse};
pub use gql_request::{GraphQLOperationType, GraphQLRequestInfo};
pub use http_request::HttpRequestInfo;
pub use parser::{ParsedGraphQLOperation, parse_graphql_operation, parse_graphql_operation_with_name};
pub use request_info::RequestInfo;
pub use response_body::ResponseBody;
pub use response_info::ResponseInfo;
pub use typestate::{GuardedPayload, Raw, RawPayload, Sanitized, SanitizedPayload};

use serde::Serialize;
use std::fmt::Debug;

#[derive(Clone, Debug, strum_macros::Display, Serialize)]
pub enum PayloadType {
    Request,
    Response,
}
