use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow};
use chrono::Utc;
use parking_lot::RwLock;
use platform_domain::{CapabilitySignal, HealthStatus, RuntimeDescriptor, RuntimeHeartbeat};

#[derive(Clone)]
pub struct RuntimeRegistry {
    inner: Arc<RwLock<BTreeMap<String, RuntimeDescriptor>>>,
    capability_history: Arc<RwLock<BTreeMap<String, Vec<CapabilitySignal>>>>,
    capability_history_path: Option<PathBuf>,
}

impl Default for RuntimeRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            capability_history: Arc::new(RwLock::new(BTreeMap::new())),
            capability_history_path: None,
        }
    }
}

impl RuntimeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_history_path(path: PathBuf) -> Self {
        Self {
            capability_history: Arc::new(RwLock::new(load_capability_history(&path))),
            capability_history_path: Some(path),
            ..Self::default()
        }
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
        let mut runtime = runtime;
        apply_capability_history(&mut runtime, &self.capability_history.read());
        guard.insert(runtime.runtime_id.clone(), runtime.clone());
        let mut history_guard = self.capability_history.write();
        history_guard.insert(runtime.runtime_id.clone(), runtime.capabilities.clone());
        persist_capability_history(&self.capability_history_path, &history_guard);
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

    pub fn record_stage_outcome(
        &self,
        runtime_id: &str,
        capability_name: &str,
        latency_ms: u32,
        success: bool,
    ) -> Option<RuntimeDescriptor> {
        let mut guard = self.inner.write();
        let runtime = guard.get_mut(runtime_id)?;
        let capability = runtime
            .capabilities
            .iter_mut()
            .find(|capability| capability.name == capability_name)?;

        capability.success_rate = blend_success_rate(capability.success_rate, success);
        if success {
            capability.median_latency_ms = blend_latency(capability.median_latency_ms, latency_ms);
        }
        let updated = runtime.clone();
        drop(guard);

        let mut history_guard = self.capability_history.write();
        history_guard.insert(runtime_id.to_string(), updated.capabilities.clone());
        persist_capability_history(&self.capability_history_path, &history_guard);

        Some(updated)
    }
}

fn apply_capability_history(
    runtime: &mut RuntimeDescriptor,
    history: &BTreeMap<String, Vec<CapabilitySignal>>,
) {
    let Some(saved_capabilities) = history.get(&runtime.runtime_id) else {
        return;
    };

    for capability in &mut runtime.capabilities {
        if let Some(saved) = saved_capabilities
            .iter()
            .find(|saved| saved.name == capability.name)
        {
            capability.success_rate = saved.success_rate;
            capability.median_latency_ms = saved.median_latency_ms;
        }
    }
}

fn load_capability_history(path: &PathBuf) -> BTreeMap<String, Vec<CapabilitySignal>> {
    let Ok(data) = std::fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    serde_json::from_str(&data).unwrap_or_default()
}

fn persist_capability_history(
    path: &Option<PathBuf>,
    history: &BTreeMap<String, Vec<CapabilitySignal>>,
) {
    let Some(path) = path else {
        return;
    };

    let Ok(json) = serde_json::to_string_pretty(history) else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, json);
}

fn blend_success_rate(current: f32, success: bool) -> f32 {
    let observed = if success { 1.0 } else { 0.0 };
    ((current * 4.0) + observed) / 5.0
}

fn blend_latency(current: u32, observed: u32) -> u32 {
    let current = u64::from(current.max(1));
    let observed = u64::from(observed.max(1));
    (((current * 3) + observed) / 4).min(u64::from(u32::MAX)) as u32
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::Utc;
    use platform_domain::{CapabilitySignal, Metadata, RuntimeHealth, RuntimeLoad};

    use super::*;

    #[test]
    fn record_stage_outcome_updates_capability_history() {
        let registry = RuntimeRegistry::new();
        registry
            .register(RuntimeDescriptor {
                runtime_id: "runtime-1".to_string(),
                agent_id: "agent-1".to_string(),
                display_name: "runtime-1".to_string(),
                endpoint: "http://runtime-1.example/a2a".to_string(),
                profile: "generalist".to_string(),
                trust_tier: 2,
                cost_per_task: 1.0,
                capabilities: vec![CapabilitySignal {
                    name: "implementation".to_string(),
                    level: 0.9,
                    success_rate: 0.8,
                    median_latency_ms: 1000,
                    max_parallelism: 4,
                    tags: vec![],
                }],
                health: RuntimeHealth {
                    status: HealthStatus::Healthy,
                    availability: 0.99,
                    last_heartbeat_at: Utc::now(),
                },
                load: RuntimeLoad::default(),
                metadata: Metadata::new(),
            })
            .expect("runtime registered");

        let updated = registry
            .record_stage_outcome("runtime-1", "implementation", 400, true)
            .expect("runtime updated");
        let capability = updated
            .capabilities
            .iter()
            .find(|capability| capability.name == "implementation")
            .expect("implementation capability");

        assert!(capability.success_rate > 0.8);
        assert!(capability.median_latency_ms < 1000);

        let updated = registry
            .record_stage_outcome("runtime-1", "implementation", 800, false)
            .expect("runtime updated again");
        let capability = updated
            .capabilities
            .iter()
            .find(|capability| capability.name == "implementation")
            .expect("implementation capability");

        assert!(capability.success_rate < 1.0);
        assert!(capability.median_latency_ms < 1000);
    }

    #[test]
    fn registry_restores_capability_history_on_register() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is monotonic")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("runtime-history-{unique}.json"));

        let registry = RuntimeRegistry::with_history_path(path.clone());
        registry
            .register(runtime_descriptor(0.8, 1000))
            .expect("runtime registered");
        registry
            .record_stage_outcome("runtime-1", "implementation", 250, true)
            .expect("stage outcome recorded");

        let restored = RuntimeRegistry::with_history_path(path.clone());
        let runtime = restored
            .register(runtime_descriptor(0.2, 9_000))
            .expect("runtime registered with restored history");
        let capability = runtime
            .capabilities
            .iter()
            .find(|capability| capability.name == "implementation")
            .expect("implementation capability");

        assert!(capability.success_rate > 0.8);
        assert!(capability.median_latency_ms < 1000);

        let _ = std::fs::remove_file(path);
    }

    fn runtime_descriptor(success_rate: f32, latency_ms: u32) -> RuntimeDescriptor {
        RuntimeDescriptor {
            runtime_id: "runtime-1".to_string(),
            agent_id: "agent-1".to_string(),
            display_name: "runtime-1".to_string(),
            endpoint: "http://runtime-1.example/a2a".to_string(),
            profile: "generalist".to_string(),
            trust_tier: 2,
            cost_per_task: 1.0,
            capabilities: vec![CapabilitySignal {
                name: "implementation".to_string(),
                level: 0.9,
                success_rate,
                median_latency_ms: latency_ms,
                max_parallelism: 4,
                tags: vec![],
            }],
            health: RuntimeHealth {
                status: HealthStatus::Healthy,
                availability: 0.99,
                last_heartbeat_at: Utc::now(),
            },
            load: RuntimeLoad::default(),
            metadata: Metadata::new(),
        }
    }
}
