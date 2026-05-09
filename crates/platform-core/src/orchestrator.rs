use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, anyhow};
use chrono::Utc;
use parking_lot::RwLock;
use platform_a2a::{A2aClient, MessageRole, text_message};
use platform_domain::{
    Artifact, CapabilityRequirement, Metadata, TaskRequirements, WorkUnit, WorkUnitKind,
    WorkUnitStatus, WorkflowRecord, WorkflowRequest, WorkflowStatus, new_id,
};
use tokio::time::{Duration, timeout};
use tracing::info;

use crate::{RuntimeRegistry, Scheduler};

/// Observer notified at key workflow lifecycle points.
#[async_trait::async_trait]
pub trait WorkflowObserver: Send + Sync {
    async fn on_stage_start(&self, _workflow_id: &str, _stage: &str, _runtime_id: &str) {}
    async fn on_stage_complete(&self, _workflow_id: &str, _stage: &str, _runtime_id: &str) {}
    async fn on_stage_failed(
        &self,
        _workflow_id: &str,
        _stage: &str,
        _runtime_id: &str,
        _error: &str,
    ) {
    }
    async fn on_workflow_complete(&self, _workflow_id: &str) {}
    async fn on_workflow_failed(&self, _workflow_id: &str, _error: &str) {}
}

#[derive(Clone)]
pub struct WorkflowOrchestrator {
    registry: RuntimeRegistry,
    scheduler: Scheduler,
    client: A2aClient,
    workflows: Arc<RwLock<BTreeMap<String, WorkflowRecord>>>,
    observers: Vec<Arc<dyn WorkflowObserver>>,
}

struct StageExecutionSpec<'a> {
    kind: WorkUnitKind,
    objective: &'a str,
    prompt: &'a str,
    context_id: &'a str,
}

impl WorkflowOrchestrator {
    pub fn new(registry: RuntimeRegistry, client: A2aClient) -> Self {
        Self {
            registry,
            scheduler: Scheduler,
            client,
            workflows: Arc::new(RwLock::new(BTreeMap::new())),
            observers: Vec::new(),
        }
    }

    pub fn add_observer(&mut self, observer: Arc<dyn WorkflowObserver>) {
        self.observers.push(observer);
    }

    async fn notify_stage_start(&self, workflow_id: &str, stage: &str, runtime_id: &str) {
        for o in &self.observers {
            o.on_stage_start(workflow_id, stage, runtime_id).await;
        }
    }

    async fn notify_stage_complete(&self, workflow_id: &str, stage: &str, runtime_id: &str) {
        for o in &self.observers {
            o.on_stage_complete(workflow_id, stage, runtime_id).await;
        }
    }

    async fn notify_stage_failed(
        &self,
        workflow_id: &str,
        stage: &str,
        runtime_id: &str,
        error: &str,
    ) {
        for o in &self.observers {
            o.on_stage_failed(workflow_id, stage, runtime_id, error)
                .await;
        }
    }

    async fn notify_workflow_complete(&self, workflow_id: &str) {
        for o in &self.observers {
            o.on_workflow_complete(workflow_id).await;
        }
    }

    async fn notify_workflow_failed(&self, workflow_id: &str, error: &str) {
        for o in &self.observers {
            o.on_workflow_failed(workflow_id, error).await;
        }
    }

    pub fn get_workflow(&self, workflow_id: &str) -> Option<WorkflowRecord> {
        self.workflows.read().get(workflow_id).cloned()
    }

    pub fn list_workflows(&self) -> Vec<WorkflowRecord> {
        self.workflows.read().values().cloned().collect()
    }

    /// Submit a workflow request. The orchestrator dispatches the objective
    /// to a capability-matched runtime via the configured stages. Each stage
    /// is executed sequentially, with the output of one stage available as
    /// context for the next. Real agent services connected via A2A process
    /// the prompts and return results.
    pub async fn submit(&self, request: WorkflowRequest) -> Result<WorkflowRecord> {
        let now = Utc::now();
        let workflow_id = new_id("wf");
        let context_id = request.context_id.clone().unwrap_or_else(|| new_id("ctx"));

        // Extract data needed for prompt building before moving request.
        let objective = request.objective.clone();
        let constraints = request.constraints.clone();

        let mut workflow = WorkflowRecord {
            workflow_id: workflow_id.clone(),
            request,
            status: WorkflowStatus::Running,
            created_at: now,
            updated_at: now,
            work_units: Vec::new(),
            final_report: None,
            audit_log: vec!["workflow accepted".to_string()],
        };
        self.persist(workflow.clone());

        // Build prompts that only need the objective/constraints (not the workflow record)
        let plan_prompt = build_plan_prompt(&objective, &constraints);

        let result: Result<Artifact> = timeout(Duration::from_secs(300), async {
            // Stage 1: Plan — dispatch to a runtime with the objective
            let plan_output = self
                .execute_stage(
                    &mut workflow,
                    StageExecutionSpec {
                        kind: WorkUnitKind::Plan,
                        objective: "Derive an execution plan from the objective",
                        prompt: &plan_prompt,
                        context_id: &context_id,
                    },
                )
                .await?;

            // Stage 2: Execute — dispatch the plan to a capable runtime
            let execute_output = self
                .execute_stage(
                    &mut workflow,
                    StageExecutionSpec {
                        kind: WorkUnitKind::Execute,
                        objective: "Execute the plan and produce results",
                        prompt: &build_execute_prompt(
                            &objective,
                            &constraints,
                            &plan_output.content,
                        ),
                        context_id: &context_id,
                    },
                )
                .await?;

            // Stage 3: Synthesize — produce a final report from all outputs
            self.execute_stage(
                &mut workflow,
                StageExecutionSpec {
                    kind: WorkUnitKind::Synthesize,
                    objective: "Produce a final report from all outputs",
                    prompt: &build_synthesis_prompt(
                        &objective,
                        &plan_output.content,
                        &execute_output.content,
                    ),
                    context_id: &context_id,
                },
            )
            .await
        })
        .await
        .map_err(|_| anyhow!("workflow exceeded the 300 second execution budget"))?;

        match result {
            Ok(synthesis) => {
                workflow.status = WorkflowStatus::Completed;
                workflow.updated_at = Utc::now();
                workflow.final_report = Some(synthesis);
                workflow.audit_log.push(
                    "workflow completed after stage dispatch and A2A runtime collaboration"
                        .to_string(),
                );
                self.persist(workflow.clone());
                self.notify_workflow_complete(&workflow_id).await;
                Ok(workflow)
            }
            Err(error) => {
                workflow.status = WorkflowStatus::Failed;
                workflow.updated_at = Utc::now();
                workflow.audit_log.push(format!("workflow failed: {error}"));
                self.persist(workflow.clone());
                self.notify_workflow_failed(&workflow_id, &error.to_string())
                    .await;
                Err(error)
            }
        }
    }

    /// Execute a single stage by dispatching the prompt to the best-fit runtime
    /// via the A2A protocol. The runtime's agent service processes the message
    /// and returns an artifact with the result.
    ///
    /// The stage kind determines capability requirements used for auto-scheduling:
    ///   - Plan       → requires `planning` capability for task decomposition
    ///   - Execute    → requires `implementation` capability for solution delivery
    ///   - Synthesize → requires `synthesis` capability for report generation
    async fn execute_stage(
        &self,
        workflow: &mut WorkflowRecord,
        spec: StageExecutionSpec<'_>,
    ) -> Result<Artifact> {
        let StageExecutionSpec {
            kind,
            objective,
            prompt,
            context_id,
        } = spec;
        // Auto-build capability requirements based on stage kind.
        // The scheduler matches these against each runtime's registered capabilities.
        // When a runtime has Vec::new() capabilities (like the generic nodes),
        // the scheduler skips capability filtering and scores on load/availability/cost.
        let requirements = stage_requirements(&kind);
        let mut excluded_runtime_ids = Vec::new();
        let mut attempt_errors = Vec::new();

        loop {
            let scheduler_candidate = self
                .scheduler
                .select_best(&self.registry.list(), &requirements, &excluded_runtime_ids)
                .ok_or_else(|| {
                    let registered = self.registry.list();
                    if registered.is_empty() {
                        anyhow!("no runtimes registered. Go to Settings > Add Agent to configure and launch an AI agent, or check that agent-adapter processes are running and have registered with the control-plane")
                    } else if attempt_errors.is_empty() {
                        anyhow!("no runtime available for stage {:?} — {} runtime(s) registered but none meet the requirements (capabilities, heartbeat, or capacity)", kind, registered.len())
                    } else {
                        anyhow!(
                            "all available runtimes failed for stage {:?}: {}",
                            kind,
                            attempt_errors.join(" | ")
                        )
                    }
                })?;

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

            let stage_name = format!("{kind:?}").to_lowercase();
            let runtime_id = scheduler_candidate.runtime.runtime_id.clone();
            self.notify_stage_start(&workflow.workflow_id, &stage_name, &runtime_id)
                .await;

            let work_unit_id = new_id("wu");
            let work_unit = WorkUnit {
                work_unit_id: work_unit_id.clone(),
                kind: kind.clone(),
                objective: objective.to_string(),
                instructions: prompt.to_string(),
                requirements: requirements.clone(),
                assigned_runtime_id: Some(runtime_id.clone()),
                status: WorkUnitStatus::Running,
                output: None,
                review_feedback: Vec::new(),
                transcript: Vec::new(),
            };
            workflow.work_units.push(work_unit);
            workflow.updated_at = Utc::now();
            self.persist(workflow.clone());

            let stage_result: Result<Artifact> = async {
                let result = timeout(
                    Duration::from_secs(60),
                    self.client
                        .send_message(&scheduler_candidate.runtime.endpoint, message),
                )
                .await
                .map_err(|_| anyhow!("stage {:?} timed out for {}", kind, runtime_id))??;
                let task = result
                    .task
                    .ok_or_else(|| anyhow!("runtime {} returned no task", runtime_id))?;
                task.artifacts
                    .first()
                    .cloned()
                    .ok_or_else(|| anyhow!("runtime {} returned no artifact", runtime_id))
            }
            .await;

            match stage_result {
                Ok(artifact) => {
                    if let Some(unit) = workflow
                        .work_units
                        .iter_mut()
                        .find(|u| u.work_unit_id == work_unit_id)
                    {
                        unit.status = WorkUnitStatus::Completed;
                        unit.output = Some(artifact.clone());
                    }
                    workflow.updated_at = Utc::now();
                    self.persist(workflow.clone());
                    info!(
                        workflow_id = %workflow.workflow_id,
                        work_unit_kind = ?kind,
                        runtime_id = %runtime_id,
                        "stage completed"
                    );
                    self.notify_stage_complete(&workflow.workflow_id, &stage_name, &runtime_id)
                        .await;
                    return Ok(artifact);
                }
                Err(error) => {
                    let error_message = format_error_chain(&error);
                    if let Some(unit) = workflow
                        .work_units
                        .iter_mut()
                        .find(|u| u.work_unit_id == work_unit_id)
                    {
                        unit.status = WorkUnitStatus::Failed;
                        unit.review_feedback.push(error_message.clone());
                    }
                    workflow.audit_log.push(format!(
                        "stage {:?} failed on {}: {}; trying another runtime",
                        kind, runtime_id, error_message
                    ));
                    workflow.updated_at = Utc::now();
                    self.persist(workflow.clone());
                    self.notify_stage_failed(
                        &workflow.workflow_id,
                        &stage_name,
                        &runtime_id,
                        &error_message,
                    )
                    .await;
                    attempt_errors.push(format!("{runtime_id}: {error_message}"));
                    excluded_runtime_ids.push(runtime_id);
                }
            }
        }
    }

    fn persist(&self, workflow: WorkflowRecord) {
        self.workflows
            .write()
            .insert(workflow.workflow_id.clone(), workflow);
    }
}

/// Returns capability requirements for a given workflow stage.
/// These let the scheduler auto-select the best-fit runtime.
fn stage_requirements(kind: &WorkUnitKind) -> TaskRequirements {
    let (capability, min_level) = match kind {
        WorkUnitKind::Plan => ("planning", 0.5),
        WorkUnitKind::Execute => ("implementation", 0.5),
        WorkUnitKind::Synthesize => ("synthesis", 0.5),
        WorkUnitKind::Review => ("review", 0.5),
        WorkUnitKind::Revise => ("implementation", 0.5),
        WorkUnitKind::Consult => ("planning", 0.3),
    };
    TaskRequirements {
        required_capabilities: vec![CapabilityRequirement {
            capability: capability.to_string(),
            min_level,
            weight: 1.0,
        }],
        preferred_capabilities: Vec::new(),
        max_latency_ms: Some(30_000),
        min_success_rate: None,
        max_cost: None,
        trust_tier: Some(1),
    }
}

fn build_plan_prompt(objective: &str, constraints: &[String]) -> String {
    format!(
        "You are a planning agent. Design an execution plan for this objective:\n{}\n\nConstraints:\n- {}\n\n\
        Return ONLY a clear step-by-step plan. Use plain text or markdown. Do NOT wrap your response in JSON.",
        objective,
        constraints.join("\n- ")
    )
}

fn build_execute_prompt(objective: &str, constraints: &[String], plan: &str) -> String {
    format!(
        "You are an execution agent working from this plan:\n\n---BEGIN PLAN---\n{}\n---END PLAN---\n\n\
        Deliver the primary solution for:\n{}\n\nConstraints:\n- {}\n\n\
        Return ONLY your solution output. Use plain text or markdown. Do NOT wrap your response in JSON.",
        plan,
        objective,
        constraints.join("\n- ")
    )
}

fn build_synthesis_prompt(objective: &str, plan: &str, final_output: &str) -> String {
    format!(
        "You are a synthesis agent. Produce a concise final report.\n\n\
        Objective:\n{}\n\n\
        ---BEGIN PLAN---\n{}\n---END PLAN---\n\n\
        ---BEGIN EXECUTION OUTPUT---\n{}\n---END EXECUTION OUTPUT---\n\n\
        Summarize the results clearly. Use plain text or markdown. Do NOT wrap your response in JSON.",
        objective, plan, final_output
    )
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}
