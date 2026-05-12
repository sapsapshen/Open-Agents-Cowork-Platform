use chrono::Utc;
use platform_domain::{AssignmentDecision, HealthStatus, RuntimeDescriptor, TaskRequirements};

#[derive(Debug, Clone)]
pub struct SchedulerCandidate {
    pub runtime: RuntimeDescriptor,
    pub decision: AssignmentDecision,
}

#[derive(Debug, Clone, Default)]
pub struct Scheduler;

impl Scheduler {
    pub fn select_best(
        &self,
        runtimes: &[RuntimeDescriptor],
        requirements: &TaskRequirements,
        exclude_ids: &[String],
    ) -> Option<SchedulerCandidate> {
        let mut scored = runtimes
            .iter()
            .filter(|runtime| {
                !exclude_ids
                    .iter()
                    .any(|exclude| exclude == &runtime.runtime_id)
            })
            .filter_map(|runtime| self.score_runtime(runtime, requirements))
            .collect::<Vec<_>>();

        scored.sort_by(|left, right| {
            right
                .decision
                .score
                .partial_cmp(&left.decision.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        scored.into_iter().next()
    }

    fn score_runtime(
        &self,
        runtime: &RuntimeDescriptor,
        requirements: &TaskRequirements,
    ) -> Option<SchedulerCandidate> {
        if runtime.health.status == HealthStatus::Unavailable {
            return None;
        }
        if Utc::now()
            .signed_duration_since(runtime.health.last_heartbeat_at)
            .num_seconds()
            > 15
        {
            return None;
        }

        if runtime.load.max_tasks == 0 {
            return None;
        }
        if runtime.load.active_tasks >= runtime.load.max_tasks {
            return None;
        }

        if let Some(trust_tier) = requirements.trust_tier
            && runtime.trust_tier < trust_tier
        {
            return None;
        }

        if let Some(max_cost) = requirements.max_cost
            && runtime.cost_per_task > max_cost
        {
            return None;
        }

        let mut reasons = Vec::new();
        let mut required_score = 0.0;
        let mut success_signal_total = 0.0;
        let mut latency_signal_total = 0.0;
        let mut matched_required_capabilities = 0.0;
        for requirement in &requirements.required_capabilities {
            match runtime
                .capabilities
                .iter()
                .find(|cap| cap.name == requirement.capability)
            {
                Some(capability) => {
                    if capability.level < requirement.min_level {
                        return None;
                    }
                    if let Some(min_success_rate) = requirements.min_success_rate
                        && capability.success_rate < min_success_rate
                    {
                        return None;
                    }
                    if let Some(max_latency_ms) = requirements.max_latency_ms
                        && capability.median_latency_ms > max_latency_ms
                    {
                        return None;
                    }
                    required_score += capability.level * requirement.weight.max(0.1);
                    success_signal_total += capability.success_rate;
                    latency_signal_total += latency_signal(capability.median_latency_ms);
                    matched_required_capabilities += 1.0;
                    reasons.push(format!(
                        "required capability {} matched at {:.2}",
                        requirement.capability, capability.level
                    ));
                }
                None => {
                    if !supports_generic_runtime(runtime) {
                        return None;
                    }
                    reasons.push(format!(
                        "required capability {} satisfied by explicit generic runtime fallback",
                        requirement.capability
                    ));
                }
            }
        }

        let mut preferred_score = 0.0;
        for requirement in &requirements.preferred_capabilities {
            if let Some(capability) = runtime
                .capabilities
                .iter()
                .find(|capability| capability.name == requirement.capability)
            {
                preferred_score += capability.level * requirement.weight.max(0.1);
                reasons.push(format!(
                    "preferred capability {} boosted score to {:.2}",
                    requirement.capability, capability.level
                ));
            }
        }

        let load_penalty = runtime.load.active_tasks as f32 / runtime.load.max_tasks as f32;
        let queue_penalty = runtime.load.queued_tasks as f32 * 0.03;
        let availability_bonus = runtime.health.availability * 0.25;
        let cost_bonus = (1.0 / runtime.cost_per_task.max(1.0)) * 0.10;
        let quality_bonus = if matched_required_capabilities > 0.0 {
            (success_signal_total / matched_required_capabilities) * 0.15
                + (latency_signal_total / matched_required_capabilities) * 0.10
        } else {
            0.0
        };

        if quality_bonus > 0.0 {
            reasons.push(format!(
                "quality history bonus {:.3} from success rate and latency",
                quality_bonus
            ));
        }

        let score =
            required_score * 0.55
                + preferred_score * 0.20
                + availability_bonus
                + cost_bonus
                + quality_bonus
                - load_penalty * 0.20
                - queue_penalty;

        Some(SchedulerCandidate {
            runtime: runtime.clone(),
            decision: AssignmentDecision {
                runtime_id: runtime.runtime_id.clone(),
                score,
                reasons,
            },
        })
    }
}

fn latency_signal(latency_ms: u32) -> f32 {
    1.0 / (1.0 + (latency_ms.max(1) as f32 / 1000.0))
}

fn supports_generic_runtime(runtime: &RuntimeDescriptor) -> bool {
    runtime.capabilities.is_empty()
        && runtime
            .metadata
            .get("generic_runtime")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use platform_domain::{
        CapabilityRequirement, CapabilitySignal, RuntimeDescriptor, RuntimeHealth, RuntimeLoad,
        TaskRequirements,
    };

    use super::Scheduler;

    fn runtime(id: &str, capability: &str, level: f32) -> RuntimeDescriptor {
        RuntimeDescriptor {
            runtime_id: id.to_string(),
            agent_id: format!("agent-{id}"),
            display_name: id.to_string(),
            endpoint: format!("http://{id}.example/a2a"),
            profile: "generalist".to_string(),
            trust_tier: 2,
            cost_per_task: 1.0,
            capabilities: vec![CapabilitySignal {
                name: capability.to_string(),
                level,
                success_rate: 0.99,
                median_latency_ms: 120,
                max_parallelism: 4,
                tags: vec![],
            }],
            health: RuntimeHealth {
                status: platform_domain::HealthStatus::Healthy,
                availability: 0.99,
                last_heartbeat_at: Utc::now(),
            },
            load: RuntimeLoad::default(),
            metadata: Default::default(),
        }
    }

    fn generic_runtime(id: &str, explicit_opt_in: bool) -> RuntimeDescriptor {
        let mut runtime = RuntimeDescriptor {
            runtime_id: id.to_string(),
            agent_id: format!("agent-{id}"),
            display_name: id.to_string(),
            endpoint: format!("http://{id}.example/a2a"),
            profile: "bridge".to_string(),
            trust_tier: 2,
            cost_per_task: 1.0,
            capabilities: vec![],
            health: RuntimeHealth {
                status: platform_domain::HealthStatus::Healthy,
                availability: 0.99,
                last_heartbeat_at: Utc::now(),
            },
            load: RuntimeLoad::default(),
            metadata: Default::default(),
        };
        if explicit_opt_in {
            runtime
                .metadata
                .insert("generic_runtime".to_string(), "true".to_string());
        }
        runtime
    }

    #[test]
    fn scheduler_prefers_highest_matching_runtime() {
        let scheduler = Scheduler;
        let requirements = TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "review".to_string(),
                min_level: 0.8,
                weight: 1.0,
            }],
            preferred_capabilities: vec![],
            max_latency_ms: None,
            min_success_rate: None,
            max_cost: None,
            trust_tier: Some(1),
        };

        let selected = scheduler
            .select_best(
                &[runtime("r1", "review", 0.82), runtime("r2", "review", 0.95)],
                &requirements,
                &[],
            )
            .expect("expected a runtime");

        assert_eq!(selected.runtime.runtime_id, "r2");
    }

    #[test]
    fn scheduler_rejects_empty_capability_runtime_without_generic_opt_in() {
        let scheduler = Scheduler;
        let requirements = TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "implementation".to_string(),
                min_level: 0.5,
                weight: 1.0,
            }],
            preferred_capabilities: vec![],
            max_latency_ms: None,
            min_success_rate: None,
            max_cost: None,
            trust_tier: Some(1),
        };

        let selected = scheduler.select_best(&[generic_runtime("bridge", false)], &requirements, &[]);

        assert!(selected.is_none());
    }

    #[test]
    fn scheduler_allows_explicit_generic_runtime_fallback() {
        let scheduler = Scheduler;
        let requirements = TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "implementation".to_string(),
                min_level: 0.5,
                weight: 1.0,
            }],
            preferred_capabilities: vec![],
            max_latency_ms: None,
            min_success_rate: None,
            max_cost: None,
            trust_tier: Some(1),
        };

        let selected = scheduler
            .select_best(&[generic_runtime("bridge", true)], &requirements, &[])
            .expect("expected generic runtime fallback");

        assert_eq!(selected.runtime.runtime_id, "bridge");
    }

    #[test]
    fn scheduler_prefers_stronger_runtime_history_when_levels_tie() {
        let scheduler = Scheduler;
        let requirements = TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "implementation".to_string(),
                min_level: 0.8,
                weight: 1.0,
            }],
            preferred_capabilities: vec![],
            max_latency_ms: None,
            min_success_rate: None,
            max_cost: None,
            trust_tier: Some(1),
        };

        let mut fast_reliable = runtime("fast", "implementation", 0.9);
        fast_reliable.capabilities[0].success_rate = 0.99;
        fast_reliable.capabilities[0].median_latency_ms = 120;

        let mut slow_unreliable = runtime("slow", "implementation", 0.9);
        slow_unreliable.capabilities[0].success_rate = 0.65;
        slow_unreliable.capabilities[0].median_latency_ms = 2_500;

        let selected = scheduler
            .select_best(&[slow_unreliable, fast_reliable], &requirements, &[])
            .expect("expected a runtime");

        assert_eq!(selected.runtime.runtime_id, "fast");
    }
}
