use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct DeployerGuard {
    external_deploy_enabled: AtomicBool,
    dev_upload_enabled: AtomicBool,
}

impl DeployerGuard {
    pub fn new(external_deploy_enabled: bool, dev_upload_enabled: bool) -> Self {
        Self {
            external_deploy_enabled: AtomicBool::new(external_deploy_enabled),
            dev_upload_enabled: AtomicBool::new(dev_upload_enabled),
        }
    }

    pub fn is_external_deploy_allowed(&self) -> bool {
        self.external_deploy_enabled.load(Ordering::Relaxed)
    }

    pub fn is_dev_upload_allowed(&self) -> bool {
        self.dev_upload_enabled.load(Ordering::Relaxed)
    }

    pub fn set_external_deploy_enabled(&self, enabled: bool) {
        self.external_deploy_enabled.store(enabled, Ordering::SeqCst);
        log::info!("DeployerGuard: external_deploy_enabled set to {}", enabled);
    }

    pub fn set_dev_upload_enabled(&self, enabled: bool) {
        self.dev_upload_enabled.store(enabled, Ordering::SeqCst);
        log::info!("DeployerGuard: dev_upload_enabled set to {}", enabled);
    }

    /// Emergency one-click lockdown freezing both external deployments and dev uploads.
    pub fn emergency_lockdown(&self) {
        self.external_deploy_enabled.store(false, Ordering::SeqCst);
        self.dev_upload_enabled.store(false, Ordering::SeqCst);
        log::warn!("EMERGENCY LOCKDOWN: All Fluxcell deployment ingestion has been frozen!");
    }
}

impl Default for DeployerGuard {
    fn default() -> Self {
        Self::new(false, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployer_guard_lifecycle_and_lockdown() {
        let guard = DeployerGuard::new(true, true);
        assert!(guard.is_external_deploy_allowed());
        assert!(guard.is_dev_upload_allowed());

        guard.set_dev_upload_enabled(false);
        assert!(guard.is_external_deploy_allowed());
        assert!(!guard.is_dev_upload_allowed());

        guard.emergency_lockdown();
        assert!(!guard.is_external_deploy_allowed());
        assert!(!guard.is_dev_upload_allowed());
    }
}
