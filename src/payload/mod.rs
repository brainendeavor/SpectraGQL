pub mod gql_parser;
pub mod gql_request_info;
pub mod http_request_info;
pub mod request_info;
pub mod response_body;
pub mod response_info;
pub mod terminal_event;

pub use gql_parser::{parse_graphql_operation, parse_graphql_operation_with_name};

pub use gql_request_info::GraphQLRequestInfo;
pub use http_request_info::HttpRequestInfo;
pub use request_info::RequestInfo;
pub use response_body::ResponseBody;
pub use response_info::ResponseInfo;
pub use terminal_event::{CompletionEvent, EventStatus, OperationOutcome, TerminalEvent};

pub use gql_request_info::GraphQLOperationType;

use serde::Serialize;
use std::fmt::Debug;

#[derive(Clone, Debug, strum_macros::Display, Serialize)]
pub enum PayloadType {
    Request,
    Response,
}
