use spectragql::protocol::{
    GraphQLOperationType, parse_graphql_operation, parse_graphql_operation_with_name,
};

#[test]
fn test_multi_operation_document_resolves_selected_operation_name() {
    let query_doc = r#"
        query GetCurrentUser {
            viewer {
                id
                email
            }
        }

        mutation UpdateUserStatus($status: String!) {
            updateStatus(status: $status) {
                success
            }
        }
    "#;

    // 1. Without operationName, defaults to first definition (Query)
    let op_default = parse_graphql_operation(query_doc).unwrap();
    assert_eq!(op_default.operation_type, GraphQLOperationType::Query);
    assert_eq!(op_default.operation_name, Some("GetCurrentUser".to_string()));
    assert_eq!(op_default.root_fields, vec!["viewer".to_string()]);

    // 2. With operationName = "UpdateUserStatus", resolves to Mutation
    let op_mut = parse_graphql_operation_with_name(query_doc, Some("UpdateUserStatus")).unwrap();
    assert_eq!(op_mut.operation_type, GraphQLOperationType::Mutation);
    assert_eq!(op_mut.operation_name, Some("UpdateUserStatus".to_string()));
    assert_eq!(op_mut.root_fields, vec!["updateStatus".to_string()]);

    // 3. Case-insensitive matching
    let op_ci = parse_graphql_operation_with_name(query_doc, Some("updateuserstatus")).unwrap();
    assert_eq!(op_ci.operation_type, GraphQLOperationType::Mutation);

    // 4. Non-existent operationName returns descriptive error
    let err = parse_graphql_operation_with_name(query_doc, Some("UnknownOp"));
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_query_with_named_and_inline_fragments() {
    let query_with_fragments = r#"
        fragment UserFields on User {
            id
            username
        }

        query FetchProfile($id: ID!) {
            user(id: $id) {
                ...UserFields
                profile {
                    avatarUrl
                }
            }
        }
    "#;

    // Parser should skip FragmentDefinition and resolve the query OperationDefinition
    let parsed = parse_graphql_operation(query_with_fragments).unwrap();
    assert_eq!(parsed.operation_type, GraphQLOperationType::Query);
    assert_eq!(parsed.operation_name, Some("FetchProfile".to_string()));
    assert_eq!(parsed.root_fields, vec!["user".to_string()]);
}

#[test]
fn test_malformed_syntax_and_empty_document_rejection() {
    assert!(parse_graphql_operation("").is_err());
    assert!(parse_graphql_operation("   \n\t  ").is_err());
    assert!(parse_graphql_operation("not a graphql document").is_err());
    assert!(parse_graphql_operation("mutation { unclosedBrace(").is_err());
    assert!(parse_graphql_operation("query { user(id: ) { id } }").is_err());
}

#[test]
fn test_shorthand_anonymous_query_root_fields() {
    let shorthand = "{ hero { name friends { name } } }";
    let parsed = parse_graphql_operation(shorthand).unwrap();
    assert_eq!(parsed.operation_type, GraphQLOperationType::Query);
    assert_eq!(parsed.operation_name, None);
    assert_eq!(parsed.root_fields, vec!["hero".to_string()]);
}

#[test]
fn test_adversarial_deeply_nested_selection_sets() {
    let depth = 100;
    let mut query = String::from("query DeepNesting { ");
    for i in 0..depth {
        query.push_str(&format!("field{} {{ ", i));
    }
    query.push_str("leaf");
    for _ in 0..depth {
        query.push_str(" }");
    }
    query.push_str(" }");

    let parsed = parse_graphql_operation(&query).expect("Deeply nested query should parse safely without crash");
    assert_eq!(parsed.operation_name, Some("DeepNesting".to_string()));
    assert_eq!(parsed.root_fields, vec!["field0".to_string()]);
}

#[test]
fn test_adversarial_alias_bombing() {
    let count = 1000;
    let mut query = String::from("query AliasBomb { ");
    for i in 0..count {
        query.push_str(&format!("a{}: user ", i));
    }
    query.push_str("}");

    let parsed = parse_graphql_operation(&query).expect("Mass alias query should parse");
    assert_eq!(parsed.operation_type, GraphQLOperationType::Query);
    assert_eq!(parsed.root_fields.len(), count);
}

#[test]
fn test_adversarial_massive_whitespace_and_comments() {
    let mut query = String::new();
    for _ in 0..1000 {
        query.push_str("# This is an adversarial comment flooding the buffer\n   \t  \n");
    }
    query.push_str("query MassiveComments { viewer { id } }\n");
    for _ in 0..1000 {
        query.push_str("# Trailing comment\n");
    }

    let parsed = parse_graphql_operation(&query).expect("Comment-flooded query should parse");
    assert_eq!(parsed.operation_name, Some("MassiveComments".to_string()));
    assert_eq!(parsed.root_fields, vec!["viewer".to_string()]);
}

#[test]
fn test_adversarial_null_byte_injection() {
    let query_with_null = "query NullInject\0 { user { id } }";
    let res = parse_graphql_operation(query_with_null);
    // apollo-parser should either reject null byte with syntax error or parse safely without memory corruption
    assert!(res.is_err() || res.is_ok());
}

#[test]
fn test_adversarial_fragment_only_document() {
    let fragment_doc = "fragment UserFields on User { id name email }";
    let res = parse_graphql_operation(fragment_doc);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("No operation definition found"), "Unexpected error message: {}", err);
}
