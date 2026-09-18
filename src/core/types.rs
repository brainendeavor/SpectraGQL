use serde::{Deserialize, Serialize};

/// Routing and execution strategy for incoming GraphQL operations.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Default)]
pub enum ExecutionStrategy {
    #[default]
    #[serde(
        rename = "sync",
        alias = "SYNC",
        alias = "sync_upstream_execution",
        alias = "SYNC_UPSTREAM_EXECUTION",
        alias = "synchronous",
        alias = "proxy",
        alias = "upstream",
        alias = "UPSTREAM",
        alias = "SyncUpstreamExecution",
        alias = "A",
        alias = "a"
    )]
    SyncUpstreamExecution,

    #[serde(
        rename = "async",
        alias = "ASYNC",
        alias = "AsyncCommandReceipt",
        alias = "async_command_receipt",
        alias = "ASYNC_COMMAND_RECEIPT",
        alias = "async_edge_command",
        alias = "ASYNC_EDGE_COMMAND",
        alias = "asynchronous",
        alias = "receipt",
        alias = "queue",
        alias = "QUEUE",
        alias = "B",
        alias = "b"
    )]
    AsyncCommandReceipt,
}

pub type OperationMode = ExecutionStrategy;

impl ExecutionStrategy {
    pub const A: ExecutionStrategy = ExecutionStrategy::SyncUpstreamExecution;
    pub const B: ExecutionStrategy = ExecutionStrategy::AsyncCommandReceipt;
    #[allow(non_upper_case_globals)]
    pub const AsyncEdgeCommand: ExecutionStrategy = ExecutionStrategy::AsyncCommandReceipt;

    pub fn is_async_command_receipt(&self) -> bool {
        matches!(self, ExecutionStrategy::AsyncCommandReceipt)
    }

    pub fn is_async_edge_command(&self) -> bool {
        matches!(self, ExecutionStrategy::AsyncCommandReceipt)
    }

    pub fn is_sync_upstream_execution(&self) -> bool {
        matches!(self, ExecutionStrategy::SyncUpstreamExecution)
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            ExecutionStrategy::SyncUpstreamExecution => "Sync",
            ExecutionStrategy::AsyncCommandReceipt => "Async",
        }
    }
}

/// Dispatch policy options for synchronous upstream execution (Mode A).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModeADispatchPolicy {
    #[default]
    ResponseWithFailure,
    ResponseOnly,
    RawAudit,
    None,
}

impl ModeADispatchPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            ModeADispatchPolicy::ResponseWithFailure => "response_with_failure",
            ModeADispatchPolicy::ResponseOnly => "response_only",
            ModeADispatchPolicy::RawAudit => "raw_audit",
            ModeADispatchPolicy::None => "none",
        }
    }
}

pub type SyncDispatchPolicy = ModeADispatchPolicy;
pub type DispatchPolicy = ModeADispatchPolicy;

/// Lifecycle outcome of an executed operation.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationOutcome {
    Success,
    Failed,
    Rejected,
}

pub type EventStatus = OperationOutcome;
