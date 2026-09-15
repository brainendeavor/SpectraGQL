use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FluxcellStatus {
    Staged,
    Active,
    Disabled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FluxcellRecord {
    pub name: String,
    pub version: String,
    pub git_hash: Option<String>,
    pub build_time: Option<String>,
    pub sha256: String,
    pub artifact_url: Option<String>,
    pub mount_path: String,
    pub status: FluxcellStatus,
    pub routes: Vec<crate::http::RouteDefinition>,
    pub subscriptions: Vec<String>,
    #[serde(default = "default_profile_name")]
    pub profile: String,
    pub timeout_ms: u64,
    pub max_memory_bytes: usize,
    #[serde(default = "default_record_instances")]
    pub max_instances: usize,
    #[serde(default)]
    pub offload: crate::config::OffloadStrategy,
    pub installed_at: u64,
    pub activated_at: Option<u64>,
    pub wasm_file: String,
}

fn default_profile_name() -> String {
    "standard".to_string()
}

fn default_record_instances() -> usize {
    16
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentAuditEvent {
    pub event_id: String,
    pub action: String, // "STAGE", "ACTIVATE", "REMOVE", "REJECT"
    pub name: String,
    pub sha256: String,
    pub details: String,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ManifestFile {
    pub fluxcells: HashMap<String, FluxcellRecord>,
    #[serde(default)]
    pub history: Vec<DeploymentAuditEvent>,
}

#[derive(Debug)]
pub struct DeployerRegistry {
    storage_dir: PathBuf,
    manifest_path: PathBuf,
    records: RwLock<HashMap<String, FluxcellRecord>>,
    history: RwLock<Vec<DeploymentAuditEvent>>,
}

impl DeployerRegistry {
    pub fn new<P: AsRef<Path>>(storage_dir: P) -> Result<Self> {
        let dir = storage_dir.as_ref().to_path_buf();
        if !dir.exists() {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("Failed to create storage directory '{:?}'", dir))?;
        }

        let manifest_path = dir.join("fluxcells.json");
        let mut records = HashMap::new();
        let mut history = Vec::new();

        if manifest_path.exists() {
            if let Ok(data) = std::fs::read(&manifest_path) {
                if let Ok(manifest) = serde_json::from_slice::<ManifestFile>(&data) {
                    records = manifest.fluxcells;
                    history = manifest.history;
                    log::info!("DeployerRegistry: Loaded {} fluxcell records from manifest", records.len());
                }
            }
        }

        Ok(Self {
            storage_dir: dir,
            manifest_path,
            records: RwLock::new(records),
            history: RwLock::new(history),
        })
    }

    pub fn storage_dir(&self) -> &Path {
        &self.storage_dir
    }

    pub fn save_to_disk(&self) -> Result<()> {
        let manifest = ManifestFile {
            fluxcells: self.records.read().unwrap().clone(),
            history: self.history.read().unwrap().clone(),
        };

        let json_bytes = serde_json::to_vec_pretty(&manifest)
            .context("Failed to serialize manifest to JSON")?;
        std::fs::write(&self.manifest_path, json_bytes)
            .with_context(|| format!("Failed to write manifest to '{:?}'", self.manifest_path))?;
        Ok(())
    }

    pub fn upsert_record(&self, record: FluxcellRecord) -> Result<()> {
        {
            let mut lock = self.records.write().unwrap();
            lock.insert(record.name.clone(), record);
        }
        self.save_to_disk()
    }

    pub fn get_record(&self, name: &str) -> Option<FluxcellRecord> {
        self.records.read().unwrap().get(name).cloned()
    }

    pub fn remove_record(&self, name: &str) -> Result<Option<FluxcellRecord>> {
        let removed = {
            let mut lock = self.records.write().unwrap();
            lock.remove(name)
        };
        if removed.is_some() {
            self.save_to_disk()?;
        }
        Ok(removed)
    }

    pub fn list_records(&self) -> Vec<FluxcellRecord> {
        let mut list: Vec<FluxcellRecord> = self.records.read().unwrap().values().cloned().collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }

    pub fn record_audit(&self, action: &str, name: &str, sha256: &str, details: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let event = DeploymentAuditEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            action: action.to_string(),
            name: name.to_string(),
            sha256: sha256.to_string(),
            details: details.to_string(),
            timestamp: now,
        };

        {
            let mut lock = self.history.write().unwrap();
            lock.push(event);
            if lock.len() > 100 {
                let excess = lock.len() - 100;
                lock.drain(0..excess);
            }
        }
        let _ = self.save_to_disk();
    }

    pub fn get_history(&self, limit: usize) -> Vec<DeploymentAuditEvent> {
        let lock = self.history.read().unwrap();
        lock.iter().rev().take(limit).cloned().collect()
    }

    pub fn list_audit_events(&self) -> Vec<DeploymentAuditEvent> {
        self.get_history(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_lifecycle_and_persistence() {
        let temp_dir = std::env::temp_dir().join(format!("test_reg_{}", uuid::Uuid::new_v4()));
        let registry = DeployerRegistry::new(&temp_dir).unwrap();

        let rec = FluxcellRecord {
            name: "invoice-mailer".to_string(),
            version: "1.0.0".to_string(),
            git_hash: Some("abc1234".to_string()),
            build_time: None,
            sha256: "deadbeef".to_string(),
            artifact_url: Some("https://github.com/my-org/cell.wasm".to_string()),
            mount_path: "/api/invoices".to_string(),
            status: FluxcellStatus::Staged,
            routes: Vec::new(),
            subscriptions: vec!["order.completed".to_string()],
            profile: "standard".to_string(),
            timeout_ms: 10_000,
            max_memory_bytes: 16 * 1024 * 1024,
            max_instances: 16,
            offload: crate::config::OffloadStrategy::BlockingPool,
            installed_at: 1000,
            activated_at: None,
            wasm_file: "cell.wasm".to_string(),
        };

        registry.upsert_record(rec.clone()).unwrap();
        registry.record_audit("STAGE", "invoice-mailer", "deadbeef", "Staged from GitHub Release");

        assert_eq!(registry.list_records().len(), 1);
        assert_eq!(registry.get_record("invoice-mailer").unwrap().status, FluxcellStatus::Staged);
        assert_eq!(registry.get_history(10).len(), 1);

        // Reopen from disk
        let reopened = DeployerRegistry::new(&temp_dir).unwrap();
        assert_eq!(reopened.list_records().len(), 1);
        assert_eq!(reopened.get_record("invoice-mailer").unwrap().sha256, "deadbeef");

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
