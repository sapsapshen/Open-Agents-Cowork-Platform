use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use parking_lot::RwLock;
use platform_a2a::{A2aClient, MessageRole, text_message};
use platform_domain::{
    Artifact, CapabilityRequirement, Metadata, RuntimeDescriptor, TaskRequirements, WorkUnit,
    WorkUnitKind, WorkUnitStatus, WorkflowRecord, WorkflowRequest, WorkflowStatus, new_id,
};
use tokio::time::{Duration, timeout};
use tracing::info;

use crate::{RuntimeRegistry, Scheduler};

#[derive(Clone)]
pub struct WorkflowOrchestrator {
    registry: RuntimeRegistry,
    scheduler: Scheduler,
    client: A2aClient,
    workflows: Arc<RwLock<BTreeMap<String, WorkflowRecord>>>,
}

struct StageExecutionSpec<'a> {
    kind: WorkUnitKind,
    objective: &'a str,
    prompt: &'a str,
    requirements: TaskRequirements,
    inputs: &'a [Artifact],
    context_id: &'a str,
    collaborator_caps: Vec<CapabilityRequirement>,
}

impl WorkflowOrchestrator {
    pub fn new(registry: RuntimeRegistry, client: A2aClient) -> Self {
        Self {
            registry,
            scheduler: Scheduler,
            client,
            workflows: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn get_workflow(&self, workflow_id: &str) -> Option<WorkflowRecord> {
        self.workflows.read().get(workflow_id).cloned()
    }

    pub fn list_workflows(&self) -> Vec<WorkflowRecord> {
        self.workflows.read().values().cloned().collect()
    }

    pub async fn submit(&self, request: WorkflowRequest) -> Result<WorkflowRecord> {
        let now = Utc::now();
        let workflow_id = new_id("wf");
        let context_id = request.context_id.clone().unwrap_or_else(|| new_id("ctx"));
        let mut workflow = WorkflowRecord {
            workflow_id: workflow_id.clone(),
            request: request.clone(),
            status: WorkflowStatus::Running,
            created_at: now,
            updated_at: now,
            work_units: Vec::new(),
            final_report: None,
            audit_log: vec!["workflow accepted".to_string()],
        };
        self.persist(workflow.clone());

        let result: Result<Artifact> = timeout(Duration::from_secs(180), async {
            let plan_output = self
                .execute_stage(
                    &mut workflow,
                    StageExecutionSpec {
                        kind: WorkUnitKind::Plan,
                        objective: "Derive a capability-aware execution plan",
                        prompt: &build_plan_prompt(&request),
                        requirements: planning_requirements(),
                        inputs: &[],
                        context_id: &context_id,
                        collaborator_caps: vec![],
                    },
                )
                .await?;

            let mut current_output = self
                .execute_stage(&mut workflow, StageExecutionSpec {
                    kind: WorkUnitKind::Execute,
                    objective: "Produce the primary solution using direct runtime collaboration",
                    prompt: &build_execute_prompt(&request, &plan_output.content),
                    requirements: execution_requirements(),
                    inputs: std::slice::from_ref(&plan_output),
                    context_id: &context_id,
                    collaborator_caps: collaborator_requirements(),
                })
                .await?;

            let max_review_rounds = request.review_rounds.max(1);
            for round in 1..=max_review_rounds {
                let review_output = self
                    .execute_stage(
                        &mut workflow,
                        StageExecutionSpec {
                            kind: WorkUnitKind::Review,
                            objective: &format!("Review round {round}"),
                            prompt: &build_review_prompt(&request, &current_output.content, round),
                            requirements: review_requirements(),
                            inputs: std::slice::from_ref(&current_output),
                            context_id: &context_id,
                            collaborator_caps: vec![CapabilityRequirement {
                                capability: "architecture".to_string(),
                                min_level: 0.6,
                                weight: 0.4,
                            }],
                        },
                    )
                    .await?;

                let issues = extract_review_issues(&review_output.content);
                if issues.is_empty() {
                    workflow
                        .audit_log
                        .push(format!("review round {round} produced no material issues"));
                    self.persist(workflow.clone());
                    break;
                }

                workflow.audit_log.push(format!(
                    "review round {round} found {} issue(s): {}",
                    issues.len(),
                    issues.join("; ")
                ));
                self.persist(workflow.clone());

                if round == max_review_rounds {
                    bail!(
                        "review did not converge after {max_review_rounds} round(s): {}",
                        issues.join("; ")
                    );
                }

                current_output = self
                    .execute_stage(
                        &mut workflow,
                        StageExecutionSpec {
                            kind: WorkUnitKind::Revise,
                            objective: &format!("Revision round {round}"),
                            prompt: &build_revision_prompt(
                                &request,
                                &current_output.content,
                                &issues,
                            ),
                            requirements: execution_requirements(),
                            inputs: &[current_output.clone(), review_output.clone()],
                            context_id: &context_id,
                            collaborator_caps: collaborator_requirements(),
                        },
                    )
                    .await?;
            }

            self.execute_stage(
                &mut workflow,
                StageExecutionSpec {
                    kind: WorkUnitKind::Synthesize,
                    objective: "Produce the final executive-ready report",
                    prompt: &build_synthesis_prompt(
                        &request,
                        &plan_output.content,
                        &current_output.content,
                    ),
                    requirements: synthesis_requirements(),
                    inputs: &[plan_output, current_output],
                    context_id: &context_id,
                    collaborator_caps: vec![],
                },
            )
            .await
        })
        .await
        .map_err(|_| anyhow!("workflow exceeded the 180 second execution budget"))?;

        match result {
            Ok(synthesis) => {
                workflow.status = WorkflowStatus::Completed;
                workflow.updated_at = Utc::now();
                workflow.final_report = Some(synthesis);
                workflow.audit_log.push(
                    "workflow completed after capability-based dispatch and direct runtime collaboration"
                        .to_string(),
                );
                self.persist(workflow.clone());
                Ok(workflow)
            }
            Err(error) => {
                workflow.status = WorkflowStatus::Failed;
                workflow.updated_at = Utc::now();
                workflow.audit_log.push(format!("workflow failed: {error}"));
                self.persist(workflow.clone());
                Err(error)
            }
        }
    }

    async fn execute_stage(
        &self,
        workflow: &mut WorkflowRecord,
        spec: StageExecutionSpec<'_>,
    ) -> Result<Artifact> {
        let StageExecutionSpec {
            kind,
            objective,
            prompt,
            requirements,
            inputs,
            context_id,
            collaborator_caps,
        } = spec;
        let work_unit_id = new_id("wu");
        let exclude_ids = stage_exclusions(&kind, workflow);
        let scheduler_candidate = self
            .scheduler
            .select_best(&self.registry.list(), &requirements, &exclude_ids)
            .ok_or_else(|| anyhow!("no runtime satisfied stage {:?}", kind))?;

        let collaborators =
            self.select_collaborators(&scheduler_candidate.runtime, &collaborator_caps, 2, &[]);

        let mut metadata = Metadata::new();
        metadata.insert("workflow_id".to_string(), workflow.workflow_id.clone());
        metadata.insert(
            "workflow_role".to_string(),
            format!("{kind:?}").to_lowercase(),
        );
        metadata.insert("objective".to_string(), objective.to_string());
        metadata.insert(
            "constraints".to_string(),
            workflow.request.constraints.join(" | "),
        );
        let input_artifacts_json =
            serde_json::to_string(inputs).context("serializing stage input artifacts")?;
        if input_artifacts_json.len() > 128 * 1024 {
            bail!("input artifacts metadata exceeds 128 KiB");
        }
        metadata.insert("input_artifacts".to_string(), input_artifacts_json);

        let collaborators_json =
            serde_json::to_string(&collaborators).context("serializing collaborator metadata")?;
        if collaborators_json.len() > 64 * 1024 {
            bail!("collaborator metadata exceeds 64 KiB");
        }
        metadata.insert("collaborators".to_string(), collaborators_json);

        let message = text_message(
            new_id("msg"),
            MessageRole::User,
            None,
            Some(context_id.to_string()),
            prompt,
        );
        let mut message = message;
        message.metadata = metadata;

        workflow.audit_log.push(format!(
            "dispatching {:?} to {} with score {:.3}",
            kind, scheduler_candidate.runtime.runtime_id, scheduler_candidate.decision.score
        ));

        let timeout_ms = requirements
            .max_latency_ms
            .map(|value| (value as u64).saturating_mul(25))
            .unwrap_or(30_000)
            .clamp(5_000, 30_000);
        let work_unit = WorkUnit {
            work_unit_id: work_unit_id.clone(),
            kind: kind.clone(),
            objective: objective.to_string(),
            instructions: prompt.to_string(),
            requirements,
            assigned_runtime_id: Some(scheduler_candidate.runtime.runtime_id.clone()),
            status: WorkUnitStatus::Running,
            output: None,
            review_feedback: Vec::new(),
            transcript: Vec::new(),
        };
        workflow.work_units.push(work_unit.clone());
        workflow.updated_at = Utc::now();
        self.persist(workflow.clone());

        let stage_result: Result<Artifact> = async {
            let result = timeout(
                Duration::from_millis(timeout_ms),
                self.client
                    .send_message(&scheduler_candidate.runtime.endpoint, message),
            )
            .await
            .map_err(|_| {
                anyhow!(
                    "stage {:?} timed out for {}",
                    kind,
                    scheduler_candidate.runtime.runtime_id
                )
            })??;
            let task = result.task.ok_or_else(|| {
                anyhow!(
                    "runtime {} returned no task",
                    scheduler_candidate.runtime.runtime_id
                )
            })?;
            task.artifacts.first().cloned().ok_or_else(|| {
                anyhow!(
                    "runtime {} returned no artifact",
                    scheduler_candidate.runtime.runtime_id
                )
            })
        }
        .await;

        match stage_result {
            Ok(artifact) => {
                let stored_work_unit = workflow
                    .work_units
                    .iter_mut()
                    .find(|unit| unit.work_unit_id == work_unit_id)
                    .ok_or_else(|| anyhow!("workflow lost its current work unit {work_unit_id}"))?;
                stored_work_unit.status = WorkUnitStatus::Completed;
                stored_work_unit.output = Some(artifact.clone());

                workflow.updated_at = Utc::now();
                self.persist(workflow.clone());
                info!(
                    workflow_id = %workflow.workflow_id,
                    work_unit_kind = ?kind,
                    runtime_id = %scheduler_candidate.runtime.runtime_id,
                    "stage completed"
                );
                Ok(artifact)
            }
            Err(error) => {
                let stored_work_unit = workflow
                    .work_units
                    .iter_mut()
                    .find(|unit| unit.work_unit_id == work_unit_id)
                    .ok_or_else(|| anyhow!("workflow lost its current work unit {work_unit_id}"))?;
                stored_work_unit.status = WorkUnitStatus::Failed;
                stored_work_unit.review_feedback.push(error.to_string());
                workflow.updated_at = Utc::now();
                self.persist(workflow.clone());
                Err(error)
            }
        }
    }

    fn select_collaborators(
        &self,
        selected_runtime: &RuntimeDescriptor,
        requirements: &[CapabilityRequirement],
        limit: usize,
        exclude_ids: &[String],
    ) -> Vec<RuntimeDescriptor> {
        if requirements.is_empty() {
            return Vec::new();
        }

        let mut deny = exclude_ids.to_vec();
        deny.push(selected_runtime.runtime_id.clone());
        let mut collaborators = Vec::new();
        for requirement in requirements.iter().take(limit) {
            let task_requirements = TaskRequirements {
                required_capabilities: vec![requirement.clone()],
                preferred_capabilities: vec![],
                max_latency_ms: None,
                min_success_rate: Some(0.5),
                max_cost: None,
                trust_tier: Some(1),
            };
            if let Some(candidate) =
                self.scheduler
                    .select_best(&self.registry.list(), &task_requirements, &deny)
            {
                deny.push(candidate.runtime.runtime_id.clone());
                collaborators.push(candidate.runtime.clone());
            }
        }
        collaborators
    }

    fn persist(&self, workflow: WorkflowRecord) {
        self.workflows
            .write()
            .insert(workflow.workflow_id.clone(), workflow);
    }
}

fn stage_exclusions(kind: &WorkUnitKind, workflow: &WorkflowRecord) -> Vec<String> {
    match kind {
        WorkUnitKind::Review => workflow
            .work_units
            .iter()
            .filter(|unit| matches!(unit.kind, WorkUnitKind::Execute | WorkUnitKind::Revise))
            .filter_map(|unit| unit.assigned_runtime_id.clone())
            .collect(),
        _ => Vec::new(),
    }
}

fn build_plan_prompt(request: &WorkflowRequest) -> String {
    format!(
        "Design a Rust-first execution plan for this objective:\n{}\n\nConstraints:\n- {}\n\nFocus on task breakdown, capabilities needed, and direct A2A collaboration opportunities.",
        request.objective,
        request.constraints.join("\n- ")
    )
}

fn build_execute_prompt(request: &WorkflowRequest, plan: &str) -> String {
    format!(
        "Using this plan:\n{}\n\nDeliver the primary solution for:\n{}\n\nConstraints:\n- {}\n\nYou should actively consult peers through A2A when that improves quality.",
        plan,
        request.objective,
        request.constraints.join("\n- ")
    )
}

fn build_review_prompt(request: &WorkflowRequest, current_output: &str, round: u8) -> String {
    format!(
        "Review round {round} for objective:\n{}\n\nCurrent output:\n{}\n\nReturn a verdict and only material issues relevant to correctness, architecture, security, operability, and A2A capability-based orchestration.",
        request.objective, current_output
    )
}

fn build_revision_prompt(
    request: &WorkflowRequest,
    current_output: &str,
    issues: &[String],
) -> String {
    format!(
        "Revise the current output for objective:\n{}\n\nCurrent output:\n{}\n\nMaterial review issues:\n- {}\n\nPreserve what is good and close every issue.",
        request.objective,
        current_output,
        issues.join("\n- ")
    )
}

fn build_synthesis_prompt(request: &WorkflowRequest, plan: &str, final_output: &str) -> String {
    format!(
        "Synthesize a final platform report.\nObjective:\n{}\n\nPlan:\n{}\n\nFinal working content:\n{}\n\nSummarize the resulting platform, the capability-based assignment model, the A2A dialogue path, and the review closure.",
        request.objective, plan, final_output
    )
}

fn planning_requirements() -> TaskRequirements {
    TaskRequirements {
        required_capabilities: vec![CapabilityRequirement {
            capability: "planning".to_string(),
            min_level: 0.7,
            weight: 1.0,
        }],
        preferred_capabilities: vec![CapabilityRequirement {
            capability: "architecture".to_string(),
            min_level: 0.7,
            weight: 0.7,
        }],
        max_latency_ms: Some(800),
        min_success_rate: Some(0.5),
        max_cost: None,
        trust_tier: Some(1),
    }
}

fn execution_requirements() -> TaskRequirements {
    TaskRequirements {
        required_capabilities: vec![
            CapabilityRequirement {
                capability: "implementation".to_string(),
                min_level: 0.75,
                weight: 1.0,
            },
            CapabilityRequirement {
                capability: "rust".to_string(),
                min_level: 0.8,
                weight: 1.0,
            },
        ],
        preferred_capabilities: vec![CapabilityRequirement {
            capability: "a2a".to_string(),
            min_level: 0.6,
            weight: 0.8,
        }],
        max_latency_ms: Some(1200),
        min_success_rate: Some(0.5),
        max_cost: None,
        trust_tier: Some(1),
    }
}

fn review_requirements() -> TaskRequirements {
    TaskRequirements {
        required_capabilities: vec![CapabilityRequirement {
            capability: "review".to_string(),
            min_level: 0.8,
            weight: 1.0,
        }],
        preferred_capabilities: vec![
            CapabilityRequirement {
                capability: "security".to_string(),
                min_level: 0.6,
                weight: 0.5,
            },
            CapabilityRequirement {
                capability: "observability".to_string(),
                min_level: 0.6,
                weight: 0.4,
            },
        ],
        max_latency_ms: Some(800),
        min_success_rate: Some(0.5),
        max_cost: None,
        trust_tier: Some(1),
    }
}

fn synthesis_requirements() -> TaskRequirements {
    TaskRequirements {
        required_capabilities: vec![CapabilityRequirement {
            capability: "synthesis".to_string(),
            min_level: 0.6,
            weight: 1.0,
        }],
        preferred_capabilities: vec![CapabilityRequirement {
            capability: "reporting".to_string(),
            min_level: 0.6,
            weight: 0.5,
        }],
        max_latency_ms: Some(1200),
        min_success_rate: Some(0.5),
        max_cost: None,
        trust_tier: Some(1),
    }
}

fn collaborator_requirements() -> Vec<CapabilityRequirement> {
    vec![
        CapabilityRequirement {
            capability: "review".to_string(),
            min_level: 0.7,
            weight: 0.7,
        },
        CapabilityRequirement {
            capability: "planning".to_string(),
            min_level: 0.6,
            weight: 0.5,
        },
    ]
}

fn extract_review_issues(content: &str) -> Vec<String> {
    content
        .lines()
        .filter_map(|line| line.strip_prefix("- ISSUE: "))
        .map(str::trim)
        .filter(|issue| !issue.eq_ignore_ascii_case("none"))
        .map(ToString::to_string)
        .collect()
}
