use serde::{Deserialize, Serialize};

/// Routing and execution strategy for incoming GraphQL operations.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Default)]
pub enum ExecutionStrategy {
    #[default]
    #[serde(
        rename = "sync_upstream_execution",
        alias = "SYNC_UPSTREAM_EXECUTION",
        alias = "A",
        alias = "a"
    )]
    SyncUpstreamExecution,

    #[serde(
        rename = "async_edge_command",
        alias = "ASYNC_EDGE_COMMAND",
        alias = "B",
        alias = "b"
    )]
    AsyncEdgeCommand,
}

pub type OperationMode = ExecutionStrategy;

impl ExecutionStrategy {
    pub const A: ExecutionStrategy = ExecutionStrategy::SyncUpstreamExecution;
    pub const B: ExecutionStrategy = ExecutionStrategy::AsyncEdgeCommand;

    pub fn is_async_edge_command(&self) -> bool {
        matches!(self, ExecutionStrategy::AsyncEdgeCommand)
    }

    pub fn is_sync_upstream_execution(&self) -> bool {
        matches!(self, ExecutionStrategy::SyncUpstreamExecution)
    }
}

/// Dispatch policy options for Mode A (synchronous upstream execution).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModeADispatchPolicy {
    #[default]
    ResponseWithFailure,
    ResponseOnly,
    RawAudit,
}

/// Lifecycle outcome of an executed operation.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationOutcome {
    Success,
    Failed,
    Rejected,
}

pub type EventStatus = OperationOutcome;
