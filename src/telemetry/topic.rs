use crate::protocol::RequestInfo;

/// Canonical Topic Resolver for GraphQL operations and HTTP endpoints.
pub struct TopicResolver;

impl TopicResolver {
    /// Resolves the default dispatch topic from a `RequestInfo`.
    ///
    /// For GraphQL:
    /// Format is `"{operation_type}.{operation_name}"` in lowercase.
    /// If no explicit operation name is present, the first root field is used.
    /// Defaults to `"anonymous"` if neither is found.
    ///
    /// For REST / HTTP:
    /// Format is `"{METHOD}.{normalized_uri}"` where dots in URI are replaced with underscores.
    pub fn resolve_dispatch_topic(request_info: &RequestInfo) -> String {
        match request_info.gql.as_ref() {
            Some(gql_op) => {
                let op_type = gql_op.operation_type.to_string();
                let op_name = gql_op
                    .operation_name
                    .clone()
                    .or_else(|| gql_op.root_fields.first().cloned())
                    .unwrap_or_else(|| "anonymous".to_string());
                format!("{}.{}", op_type, op_name).to_lowercase()
            }
            None => {
                format!(
                    "{}.{}",
                    request_info.http.method.to_string().to_uppercase(),
                    request_info
                        .http
                        .uri
                        .to_string()
                        .replace('.', "_")
                        .to_lowercase()
                )
            }
        }
    }

    /// Derives the failed notification topic for split topic routing.
    #[inline]
    pub fn failed_topic(primary_topic: &str) -> String {
        format!("{}.failed", primary_topic)
    }

    /// Derives the audit topic for rejected interceptor operations.
    #[inline]
    pub fn rejection_topic(operation_name: &str) -> String {
        format!("interceptors.rejected.{}", operation_name.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use crate::core::clock::HlcClock;
    use crate::protocol::{GraphQLOperationType, GraphQLRequestInfo};

    fn make_req(uri: &str, method: Method, gql: Option<GraphQLRequestInfo>) -> RequestInfo {
        let (id, hlc) = HlcClock::global().now_uuidv7();
        let http_req = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(())
            .unwrap();
        let (parts, _) = http_req.into_parts();
        let mut req = RequestInfo::new(id, hlc, parts);
        req.gql = gql;
        req
    }

    #[test]
    fn test_resolve_dispatch_topic_graphql() {
        let mut gql = GraphQLRequestInfo::new(r#"{"query":"mutation { recordVote }"}"#);
        gql.operation_type = GraphQLOperationType::Mutation;
        gql.operation_name = Some("RecordVote".to_string());
        gql.root_fields = vec!["recordVote".to_string()];

        let req = make_req("/graphql", Method::POST, Some(gql));
        assert_eq!(TopicResolver::resolve_dispatch_topic(&req), "mutation.recordvote");
    }

    #[test]
    fn test_resolve_dispatch_topic_graphql_anonymous_fallback_root_field() {
        let mut gql = GraphQLRequestInfo::new(r#"{"query":"mutation { addComment }"}"#);
        gql.operation_type = GraphQLOperationType::Mutation;
        gql.operation_name = None;
        gql.root_fields = vec!["addComment".to_string()];

        let req = make_req("/graphql", Method::POST, Some(gql));
        assert_eq!(TopicResolver::resolve_dispatch_topic(&req), "mutation.addcomment");
    }

    #[test]
    fn test_resolve_dispatch_topic_graphql_anonymous_no_root_field() {
        let mut gql = GraphQLRequestInfo::new(r#"{"query":"{ anonymous }"}"#);
        gql.operation_type = GraphQLOperationType::Query;
        gql.operation_name = None;
        gql.root_fields = vec![];

        let req = make_req("/graphql", Method::POST, Some(gql));
        assert_eq!(TopicResolver::resolve_dispatch_topic(&req), "query.anonymous");
    }

    #[test]
    fn test_resolve_dispatch_topic_rest() {
        let req = make_req("/api/v1.0/users", Method::GET, None);
        assert_eq!(TopicResolver::resolve_dispatch_topic(&req), "GET./api/v1_0/users");
    }

    #[test]
    fn test_failed_and_rejection_topics() {
        assert_eq!(TopicResolver::failed_topic("mutation.vote"), "mutation.vote.failed");
        assert_eq!(TopicResolver::rejection_topic("RecordVote"), "interceptors.rejected.recordvote");
    }
}
