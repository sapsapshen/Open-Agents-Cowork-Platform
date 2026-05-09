use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, anyhow};
use chrono::Utc;
use parking_lot::RwLock;
use platform_domain::{HealthStatus, RuntimeDescriptor, RuntimeHeartbeat};

#[derive(Clone, Default)]
pub struct RuntimeRegistry {
    inner: Arc<RwLock<BTreeMap<String, RuntimeDescriptor>>>,
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, runtime: RuntimeDescriptor) -> Result<RuntimeDescriptor> {
        let mut guard = self.inner.write();
        if let Some(existing) = guard.get(&runtime.runtime_id)
            && (existing.endpoint != runtime.endpoint || existing.agent_id != runtime.agent_id)
        {
            return Err(anyhow!(
                "runtime_id {} is already bound to a different agent identity",
                runtime.runtime_id
            ));
        }
        guard.insert(runtime.runtime_id.clone(), runtime.clone());
        Ok(runtime)
    }

    pub fn heartbeat(&self, heartbeat: RuntimeHeartbeat) -> Option<RuntimeDescriptor> {
        let mut guard = self.inner.write();
        let runtime = guard.get_mut(&heartbeat.runtime_id)?;
        runtime.health.last_heartbeat_at = Utc::now();
        runtime.health.availability = heartbeat.availability;
        runtime.health.status = if heartbeat.availability > 0.95 {
            HealthStatus::Healthy
        } else if heartbeat.availability > 0.8 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unavailable
        };
        runtime.load.active_tasks = heartbeat.active_tasks;
        runtime.load.queued_tasks = heartbeat.queued_tasks;
        Some(runtime.clone())
    }

    pub fn list(&self) -> Vec<RuntimeDescriptor> {
        self.inner.read().values().cloned().collect()
    }

    pub fn get(&self, runtime_id: &str) -> Option<RuntimeDescriptor> {
        self.inner.read().get(runtime_id).cloned()
    }

    pub fn remove(&self, runtime_id: &str) -> Option<RuntimeDescriptor> {
        self.inner.write().remove(runtime_id)
    }
}
