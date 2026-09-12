use crate::admin::AdminEngine;
use crate::idempotency::IdempotencyEngine;
use crate::subscriptions::SubscriptionHub;
use pingora::proxy::Session;
use std::sync::Arc;

/// Intercepts requests targeting administrative endpoints (/admin/*)
/// and routes them to the AdminEngine.
pub struct AdminFilter;

impl AdminFilter {
    pub fn is_admin_request(admin: Option<&AdminEngine>, path: &str) -> bool {
        if let Some(admin) = admin {
            if admin.config.enabled {
                return path == admin.config.path_prefix
                    || path.starts_with(&format!("{}/", admin.config.path_prefix));
            }
        }
        false
    }

    pub async fn handle(
        session: &mut Session,
        admin: Option<&AdminEngine>,
        idempotency: &Arc<IdempotencyEngine>,
        subscriptions: &Arc<SubscriptionHub>,
    ) -> pingora::Result<bool> {
        if let Some(admin) = admin {
            if admin.config.enabled {
                let path = session.req_header().uri.path();
                if path == admin.config.path_prefix
                    || path.starts_with(&format!("{}/", admin.config.path_prefix))
                {
                    return admin.handle_request(session, idempotency, subscriptions).await;
                }
            }
        }
        Ok(false)
    }
}
