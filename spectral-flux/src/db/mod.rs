pub mod traits;

#[allow(unused_imports)]
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

pub use traits::{FluxDb, FluxTx};

/// Registry of configured database pools accessible by Fluxcells
pub struct DatabaseRegistry {
    databases: HashMap<String, Arc<dyn FluxDb>>,
    default_db: Option<String>,
}

impl DatabaseRegistry {
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
            default_db: None,
        }
    }

    pub fn register(&mut self, name: &str, db: Arc<dyn FluxDb>, is_default: bool) {
        self.databases.insert(name.to_string(), db);
        if is_default || self.default_db.is_none() || name == "default" {
            self.default_db = Some(name.to_string());
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn FluxDb>> {
        if name.is_empty() {
            self.default_db
                .as_ref()
                .and_then(|def| self.databases.get(def))
                .cloned()
        } else {
            self.databases.get(name).cloned()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.databases.is_empty()
    }

    pub fn list_databases(&self) -> Vec<String> {
        let mut list: Vec<String> = self.databases.keys().cloned().collect();
        list.sort();
        list
    }
}

impl Default for DatabaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "postgres")]
pub struct PostgresDb {
    pool: Arc<deadpool_postgres::Pool>,
}

#[cfg(feature = "postgres")]
impl PostgresDb {
    pub fn new(database_url: &str, max_connections: usize) -> Result<Self> {
        let pg_config: tokio_postgres::Config = database_url
            .parse()
            .with_context(|| format!("Failed to parse database connection URL: {}", database_url))?;

        let mgr_config = deadpool_postgres::ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        };

        let host = pg_config.get_hosts().first().cloned();
        let is_local = match host {
            Some(tokio_postgres::config::Host::Tcp(ref h)) => {
                h == "localhost" || h == "127.0.0.1" || h == "0.0.0.0"
            }
            _ => false,
        };

        let requires_tls = match pg_config.get_ssl_mode() {
            tokio_postgres::config::SslMode::Require => true,
            tokio_postgres::config::SslMode::Disable => false,
            _ => !is_local, // Remote databases default to TLS
        };

        let pool = if requires_tls {
            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
            let mgr = deadpool_postgres::Manager::from_config(pg_config, tls, mgr_config);
            deadpool_postgres::Pool::builder(mgr)
                .max_size(max_connections.max(1))
                .build()
                .context("Failed to build PostgreSQL pool with TLS")?
        } else {
            let mgr = deadpool_postgres::Manager::from_config(pg_config, tokio_postgres::NoTls, mgr_config);
            deadpool_postgres::Pool::builder(mgr)
                .max_size(max_connections.max(1))
                .build()
                .context("Failed to build PostgreSQL pool with NoTls")?
        };

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl FluxDb for PostgresDb {
    async fn begin_tx(&self) -> Result<Box<dyn FluxTx>> {
        let client = self
            .pool
            .get()
            .await
            .context("Failed to acquire PostgreSQL connection from pool")?;
        client
            .execute("BEGIN", &[])
            .await
            .context("Failed to execute BEGIN on PostgreSQL connection")?;
        Ok(Box::new(PostgresTx {
            client: Some(client),
            committed: false,
        }))
    }
}

#[cfg(feature = "postgres")]
pub struct PostgresTx {
    client: Option<deadpool_postgres::Object>,
    committed: bool,
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl FluxTx for PostgresTx {
    async fn execute(&mut self, sql: &str, params: &[serde_json::Value]) -> Result<u64> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Transaction already finalized"))?;
        let dyn_params: Vec<DynamicSqlParam> = params.iter().map(DynamicSqlParam::from).collect();
        let to_sql_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = dyn_params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows_affected = client
            .execute(sql, &to_sql_params[..])
            .await
            .with_context(|| format!("PostgreSQL execute failed for SQL: {}", sql))?;
        Ok(rows_affected)
    }

    async fn query(&mut self, sql: &str, params: &[serde_json::Value]) -> Result<Vec<serde_json::Value>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Transaction already finalized"))?;
        let dyn_params: Vec<DynamicSqlParam> = params.iter().map(DynamicSqlParam::from).collect();
        let to_sql_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = dyn_params
            .iter()
            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows = client
            .query(sql, &to_sql_params[..])
            .await
            .with_context(|| format!("PostgreSQL query failed for SQL: {}", sql))?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let mut obj = serde_json::Map::new();
            for (idx, col) in row.columns().iter().enumerate() {
                let val = row_cell_to_json(&row, idx, col.type_());
                obj.insert(col.name().to_string(), val);
            }
            result.push(serde_json::Value::Object(obj));
        }
        Ok(result)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        if let Some(client) = self.client.take() {
            self.committed = true;
            client
                .execute("COMMIT", &[])
                .await
                .context("Failed to execute COMMIT on PostgreSQL transaction")?;
        }
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        if let Some(client) = self.client.take() {
            self.committed = true;
            client
                .execute("ROLLBACK", &[])
                .await
                .context("Failed to execute ROLLBACK on PostgreSQL transaction")?;
        }
        Ok(())
    }
}

#[cfg(feature = "postgres")]
impl Drop for PostgresTx {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(client) = self.client.take() {
                // Connection was dropped without explicit commit: issue asynchronous rollback
                tokio::spawn(async move {
                    let _ = client.execute("ROLLBACK", &[]).await;
                });
            }
        }
    }
}

#[cfg(feature = "postgres")]
#[derive(Debug)]
pub enum DynamicSqlParam<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(&'a str),
    Json(&'a serde_json::Value),
}

#[cfg(feature = "postgres")]
impl<'a> From<&'a serde_json::Value> for DynamicSqlParam<'a> {
    fn from(val: &'a serde_json::Value) -> Self {
        match val {
            serde_json::Value::Null => DynamicSqlParam::Null,
            serde_json::Value::Bool(b) => DynamicSqlParam::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    DynamicSqlParam::Int(i)
                } else if let Some(f) = n.as_f64() {
                    DynamicSqlParam::Float(f)
                } else {
                    DynamicSqlParam::Null
                }
            }
            serde_json::Value::String(s) => DynamicSqlParam::String(s.as_str()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => DynamicSqlParam::Json(val),
        }
    }
}

#[cfg(feature = "postgres")]
impl<'a> tokio_postgres::types::ToSql for DynamicSqlParam<'a> {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            DynamicSqlParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            DynamicSqlParam::Bool(b) => b.to_sql(ty, out),
            DynamicSqlParam::Int(i) => match *ty {
                tokio_postgres::types::Type::INT2 => (*i as i16).to_sql(ty, out),
                tokio_postgres::types::Type::INT4 => (*i as i32).to_sql(ty, out),
                tokio_postgres::types::Type::INT8 => (*i).to_sql(ty, out),
                tokio_postgres::types::Type::FLOAT4 => (*i as f32).to_sql(ty, out),
                tokio_postgres::types::Type::FLOAT8 => (*i as f64).to_sql(ty, out),
                _ => i.to_sql(ty, out),
            },
            DynamicSqlParam::Float(f) => match *ty {
                tokio_postgres::types::Type::FLOAT4 => (*f as f32).to_sql(ty, out),
                _ => f.to_sql(ty, out),
            },
            DynamicSqlParam::String(s) => match *ty {
                tokio_postgres::types::Type::UUID => {
                    let parsed = uuid::Uuid::parse_str(s)?;
                    parsed.to_sql(ty, out)
                }
                tokio_postgres::types::Type::TIMESTAMP | tokio_postgres::types::Type::TIMESTAMPTZ => {
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                        let utc: chrono::DateTime<chrono::Utc> = dt.into();
                        utc.to_sql(ty, out)
                    } else {
                        s.to_sql(ty, out)
                    }
                }
                _ => s.to_sql(ty, out),
            },
            DynamicSqlParam::Json(j) => j.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

#[cfg(feature = "postgres")]
fn row_cell_to_json(
    row: &tokio_postgres::Row,
    idx: usize,
    ty: &tokio_postgres::types::Type,
) -> serde_json::Value {
    match *ty {
        tokio_postgres::types::Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number((v as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number((v as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(v as f64))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::TEXT
        | tokio_postgres::types::Type::VARCHAR
        | tokio_postgres::types::Type::BPCHAR => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::JSON | tokio_postgres::types::Type::JSONB => row
            .try_get::<_, Option<serde_json::Value>>(idx)
            .ok()
            .flatten()
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::UUID => row
            .try_get::<_, Option<uuid::Uuid>>(idx)
            .ok()
            .flatten()
            .map(|u| serde_json::Value::String(u.to_string()))
            .unwrap_or(serde_json::Value::Null),
        tokio_postgres::types::Type::TIMESTAMP | tokio_postgres::types::Type::TIMESTAMPTZ => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
            .ok()
            .flatten()
            .map(|dt| serde_json::Value::String(dt.to_rfc3339()))
            .unwrap_or_else(|| {
                row.try_get::<_, Option<String>>(idx)
                    .ok()
                    .flatten()
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null)
            }),
        _ => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_registry() {
        let reg = DatabaseRegistry::new();
        assert!(reg.is_empty());
        assert!(reg.get("").is_none());
    }

    #[test]
    fn test_dynamic_param_conversion() {
        let v_null = serde_json::Value::Null;
        let v_bool = serde_json::json!(true);
        let v_int = serde_json::json!(42);
        let v_str = serde_json::json!("hello");
        let v_obj = serde_json::json!({"foo": "bar"});

        assert!(matches!(DynamicSqlParam::from(&v_null), DynamicSqlParam::Null));
        assert!(matches!(DynamicSqlParam::from(&v_bool), DynamicSqlParam::Bool(true)));
        assert!(matches!(DynamicSqlParam::from(&v_int), DynamicSqlParam::Int(42)));
        assert!(matches!(DynamicSqlParam::from(&v_str), DynamicSqlParam::String("hello")));
        assert!(matches!(DynamicSqlParam::from(&v_obj), DynamicSqlParam::Json(_)));
    }
}
