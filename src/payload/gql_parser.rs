use anyhow::{Result, anyhow};
use apollo_parser::Parser;
use apollo_parser::cst::Definition;

use crate::payload::GraphQLOperationType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedGraphQLOperation {
    pub operation_type: GraphQLOperationType,
    pub operation_name: Option<String>,
    pub root_fields: Vec<String>,
}

impl ParsedGraphQLOperation {
    #[allow(dead_code)]
    pub fn matches_operation(&self, target: &str) -> bool {
        if let Some(name) = &self.operation_name {
            if name.eq_ignore_ascii_case(target) {
                return true;
            }
        }
        self.root_fields.iter().any(|f| f.eq_ignore_ascii_case(target))
    }
}

pub fn parse_graphql_operation(query_str: &str) -> Result<ParsedGraphQLOperation> {
    let parser = Parser::new(query_str);
    let ast = parser.parse();

    let errors: Vec<_> = ast.errors().collect();
    if !errors.is_empty() {
        let err_messages: Vec<String> = errors.iter().map(|e| e.message().to_string()).collect();
        return Err(anyhow!("GraphQL syntax error: {}", err_messages.join("; ")));
    }

    let doc = ast.document();
    for def in doc.definitions() {
        if let Definition::OperationDefinition(op) = def {
            let operation_type = match op.operation_type() {
                Some(op_type) => {
                    if op_type.query_token().is_some() {
                        GraphQLOperationType::Query
                    } else if op_type.mutation_token().is_some() {
                        GraphQLOperationType::Mutation
                    } else if op_type.subscription_token().is_some() {
                        GraphQLOperationType::Subscription
                    } else {
                        GraphQLOperationType::Unknown
                    }
                }
                None => GraphQLOperationType::Query,
            };

            let operation_name = op.name().map(|n| n.text().to_string());

            let mut root_fields = Vec::new();
            if let Some(selection_set) = op.selection_set() {
                for selection in selection_set.selections() {
                    if let apollo_parser::cst::Selection::Field(f) = selection {
                        if let Some(name) = f.name() {
                            root_fields.push(name.text().to_string());
                        }
                    }
                }
            }

            return Ok(ParsedGraphQLOperation {
                operation_type,
                operation_name,
                root_fields,
            });
        }
    }

    Err(anyhow!("No operation definition found in GraphQL query"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_explicit_query() {
        let q = "query HeroName { hero { name } }";
        let parsed = parse_graphql_operation(q).unwrap();
        assert_eq!(parsed.operation_type, GraphQLOperationType::Query);
        assert_eq!(parsed.operation_name, Some("HeroName".to_string()));
    }

    #[test]
    fn test_parse_shorthand_query() {
        let q = "{ hero { name } }";
        let parsed = parse_graphql_operation(q).unwrap();
        assert_eq!(parsed.operation_type, GraphQLOperationType::Query);
        assert_eq!(parsed.operation_name, None);
    }

    #[test]
    fn test_parse_mutation() {
        let q = "mutation CreateUser($name: String!) { createUser(name: $name) { id } }";
        let parsed = parse_graphql_operation(q).unwrap();
        assert_eq!(parsed.operation_type, GraphQLOperationType::Mutation);
        assert_eq!(parsed.operation_name, Some("CreateUser".to_string()));
    }

    #[test]
    fn test_parse_subscription() {
        let q = "subscription OnOrderUpdate($id: ID!) { orderUpdate(id: $id) { status } }";
        let parsed = parse_graphql_operation(q).unwrap();
        assert_eq!(parsed.operation_type, GraphQLOperationType::Subscription);
        assert_eq!(parsed.operation_name, Some("OnOrderUpdate".to_string()));
    }

    #[test]
    fn test_parse_invalid_syntax() {
        let q = "query { hero { ";
        let res = parse_graphql_operation(q);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_mutation_root_fields_and_matching() {
        let q = "mutation AdjustInventoryMutation { adjustInventory(itemId: \"42\") { id status } }";
        let parsed = parse_graphql_operation(q).unwrap();
        assert_eq!(parsed.operation_type, GraphQLOperationType::Mutation);
        assert_eq!(parsed.operation_name, Some("AdjustInventoryMutation".to_string()));
        assert_eq!(parsed.root_fields, vec!["adjustInventory".to_string()]);

        // Matches operation name
        assert!(parsed.matches_operation("AdjustInventoryMutation"));
        assert!(parsed.matches_operation("adjustinventorymutation"));
        // Matches root field
        assert!(parsed.matches_operation("adjustInventory"));
        assert!(parsed.matches_operation("ADJUSTINVENTORY"));
        // Does not match unrelated
        assert!(!parsed.matches_operation("deleteUser"));
    }

    #[test]
    fn test_parse_anonymous_mutation_with_multiple_root_fields() {
        let q = "mutation { updateCustomerAddress(id: 1) { ok } adjustInventory(id: 2) { ok } }";
        let parsed = parse_graphql_operation(q).unwrap();
        assert_eq!(parsed.operation_type, GraphQLOperationType::Mutation);
        assert_eq!(parsed.operation_name, None);
        assert_eq!(parsed.root_fields, vec!["updateCustomerAddress".to_string(), "adjustInventory".to_string()]);
        assert!(parsed.matches_operation("updateCustomerAddress"));
        assert!(parsed.matches_operation("adjustInventory"));
    }
}
