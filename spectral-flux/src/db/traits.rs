use anyhow::Result;

#[async_trait::async_trait]
pub trait FluxDb: Send + Sync {
    /// Acquire a pooled connection and initiate an isolated transaction.
    async fn begin_tx(&self) -> Result<Box<dyn FluxTx>>;
}

#[async_trait::async_trait]
pub trait FluxTx: Send + Sync {
    /// Execute a mutating SQL statement (INSERT, UPDATE, DELETE) inside the transaction.
    async fn execute(&mut self, sql: &str, params: &[serde_json::Value]) -> Result<u64>;

    /// Execute a query (SELECT) inside the transaction returning rows as JSON.
    async fn query(&mut self, sql: &str, params: &[serde_json::Value]) -> Result<Vec<serde_json::Value>>;

    /// Commit the transaction and return the connection to the pool.
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction and return the connection to the pool.
    async fn rollback(self: Box<Self>) -> Result<()>;
}
