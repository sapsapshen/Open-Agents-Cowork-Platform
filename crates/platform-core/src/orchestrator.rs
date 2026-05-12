use std::{collections::BTreeMap, sync::Arc, time::Instant};

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
    async fn on_workflow_submitted(&self, _workflow_id: &str) {}
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
    input_artifacts: &'a [Artifact],
    collaborator_requirements: &'a [CapabilityRequirement],
}

#[derive(Debug)]
enum ReviewDisposition {
    Approved,
    RevisionRequired,
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

    async fn notify_workflow_submitted(&self, workflow_id: &str) {
        for o in &self.observers {
            o.on_workflow_submitted(workflow_id).await;
        }
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
        self.notify_workflow_submitted(&workflow_id).await;

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
                        input_artifacts: &[],
                        collaborator_requirements: &[],
                    },
                )
                .await?;

            let execution_steps = parse_plan_steps(&plan_output.content);
            workflow.audit_log.push(format!(
                "plan decomposed into {} execution step(s)",
                execution_steps.len()
            ));
            workflow.updated_at = Utc::now();
            self.persist(workflow.clone());

            let mut execute_outputs = Vec::new();
            for (index, step) in execution_steps.iter().enumerate() {
                let step_number = index + 1;
                let step_objective = format!(
                    "Execute plan step {step_number} of {}",
                    execution_steps.len()
                );
                let step_prompt = build_execute_step_prompt(
                    &objective,
                    &constraints,
                    &plan_output.content,
                    step_number,
                    execution_steps.len(),
                    step,
                    &execute_outputs,
                );
                let stage_inputs = build_execute_inputs(&plan_output, &execute_outputs);
                let collaborator_requirements = execute_collaborator_requirements();

                let step_output = self
                    .execute_stage(
                        &mut workflow,
                        StageExecutionSpec {
                            kind: WorkUnitKind::Execute,
                            objective: &step_objective,
                            prompt: &step_prompt,
                            context_id: &context_id,
                            input_artifacts: &stage_inputs,
                            collaborator_requirements: &collaborator_requirements,
                        },
                    )
                    .await?;

                workflow.audit_log.push(format!(
                    "execution step {step_number}/{} completed: {}",
                    execution_steps.len(),
                    step
                ));
                workflow.updated_at = Utc::now();
                self.persist(workflow.clone());
                execute_outputs.push(step_output);
            }

            let execute_output = combine_execution_outputs(&execute_outputs);

            let reviewed_output = self
                .review_execution_output(
                    &mut workflow,
                    &objective,
                    &constraints,
                    &plan_output.content,
                    execute_output,
                    &context_id,
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
                        &reviewed_output.content,
                    ),
                    context_id: &context_id,
                    input_artifacts: &[plan_output.clone(), reviewed_output.clone()],
                    collaborator_requirements: &[],
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
            input_artifacts,
            collaborator_requirements,
        } = spec;
        // Auto-build capability requirements based on stage kind.
        // The scheduler matches these against each runtime's registered capabilities.
        // When a runtime has Vec::new() capabilities (like the generic nodes),
        // the scheduler skips capability filtering and scores on load/availability/cost.
        let requirements = stage_requirements(&kind);
        let mut excluded_runtime_ids = stage_exclusions(&kind, workflow);
        let mut attempt_errors = Vec::new();

        loop {
            let scheduler_candidate = match self
                .scheduler
                .select_best(&self.registry.list(), &requirements, &excluded_runtime_ids)
            {
                Some(candidate) => candidate,
                None if matches!(kind, WorkUnitKind::Review)
                    && attempt_errors.is_empty()
                    && !excluded_runtime_ids.is_empty() =>
                {
                    workflow.audit_log.push(
                        "no dedicated reviewer available; falling back to a previously used runtime"
                            .to_string(),
                    );
                    excluded_runtime_ids.clear();
                    workflow.updated_at = Utc::now();
                    self.persist(workflow.clone());
                    continue;
                }
                None => {
                    let registered = self.registry.list();
                    return Err(if registered.is_empty() {
                        anyhow!("no runtimes registered. Go to Settings > Add Agent to configure and launch an AI agent, or check that agent-adapter processes are running and have registered with the control-plane")
                    } else if attempt_errors.is_empty() {
                        anyhow!("no runtime available for stage {:?} — {} runtime(s) registered but none meet the requirements (capabilities, heartbeat, or capacity)", kind, registered.len())
                    } else {
                        anyhow!(
                            "all available runtimes failed for stage {:?}: {}",
                            kind,
                            attempt_errors.join(" | ")
                        )
                    });
                }
            };

            let collaborators = self.select_collaborators(
                &scheduler_candidate.runtime,
                collaborator_requirements,
                2,
                &excluded_runtime_ids,
            );

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
            let input_artifacts_json = serde_json::to_string(input_artifacts)
                .map_err(|error| anyhow!("failed to serialize input artifacts: {error}"))?;
            if input_artifacts_json.len() > 128 * 1024 {
                return Err(anyhow!("input artifacts metadata exceeds 128 KiB"));
            }
            metadata.insert("input_artifacts".to_string(), input_artifacts_json);

            let collaborators_json = serde_json::to_string(&collaborators)
                .map_err(|error| anyhow!("failed to serialize collaborators: {error}"))?;
            if collaborators_json.len() > 64 * 1024 {
                return Err(anyhow!("collaborators metadata exceeds 64 KiB"));
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

            let stage_start = Instant::now();
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

            let elapsed_ms = stage_start.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;
            let capability_name = stage_capability_name(&kind);

            match stage_result {
                Ok(artifact) => {
                    let _ = self.registry.record_stage_outcome(
                        &runtime_id,
                        capability_name,
                        elapsed_ms,
                        true,
                    );
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
                    let _ = self.registry.record_stage_outcome(
                        &runtime_id,
                        capability_name,
                        elapsed_ms,
                        false,
                    );
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

    fn select_collaborators(
        &self,
        selected_runtime: &platform_domain::RuntimeDescriptor,
        requirements: &[CapabilityRequirement],
        limit: usize,
        exclude_ids: &[String],
    ) -> Vec<platform_domain::RuntimeDescriptor> {
        if requirements.is_empty() {
            return Vec::new();
        }

        let mut deny = exclude_ids.to_vec();
        deny.push(selected_runtime.runtime_id.clone());
        let mut collaborators = Vec::new();
        for requirement in requirements.iter().take(limit) {
            let task_requirements = TaskRequirements {
                required_capabilities: vec![requirement.clone()],
                preferred_capabilities: Vec::new(),
                max_latency_ms: None,
                min_success_rate: Some(0.4),
                max_cost: None,
                trust_tier: Some(1),
            };
            if let Some(candidate) = self
                .scheduler
                .select_best(&self.registry.list(), &task_requirements, &deny)
            {
                deny.push(candidate.runtime.runtime_id.clone());
                collaborators.push(candidate.runtime);
            }
        }
        collaborators
    }

    fn persist(&self, workflow: WorkflowRecord) {
        self.workflows
            .write()
            .insert(workflow.workflow_id.clone(), workflow);
    }

    async fn review_execution_output(
        &self,
        workflow: &mut WorkflowRecord,
        objective: &str,
        constraints: &[String],
        plan: &str,
        execute_output: Artifact,
        context_id: &str,
    ) -> Result<Artifact> {
        let review_output = self
            .execute_stage(
                workflow,
                StageExecutionSpec {
                    kind: WorkUnitKind::Review,
                    objective: "Review the execution output and decide whether revision is required",
                    prompt: &build_review_prompt(objective, constraints, plan, &execute_output.content),
                    context_id,
                    input_artifacts: std::slice::from_ref(&execute_output),
                    collaborator_requirements: &[],
                },
            )
            .await?;

        match parse_review_disposition(&review_output.content)? {
            ReviewDisposition::Approved => {
                workflow.audit_log.push(
                    "review approved execution output; continuing to synthesis".to_string(),
                );
                workflow.updated_at = Utc::now();
                self.persist(workflow.clone());
                Ok(execute_output)
            }
            ReviewDisposition::RevisionRequired => {
                workflow.audit_log.push(
                    "review requested revision; dispatching revise stage".to_string(),
                );
                workflow.updated_at = Utc::now();
                self.persist(workflow.clone());

                let revise_prompt = build_revise_prompt(
                    objective,
                    constraints,
                    plan,
                    &execute_output.content,
                    &review_output.content,
                );
                let revise_inputs = vec![execute_output.clone(), review_output.clone()];
                let collaborator_requirements = execute_collaborator_requirements();
                let revised_output = self
                    .execute_stage(
                        workflow,
                        StageExecutionSpec {
                            kind: WorkUnitKind::Revise,
                            objective: "Revise the execution output using the review feedback",
                            prompt: &revise_prompt,
                            context_id,
                            input_artifacts: &revise_inputs,
                            collaborator_requirements: &collaborator_requirements,
                        },
                    )
                    .await?;

                let second_review = self
                    .execute_stage(
                        workflow,
                        StageExecutionSpec {
                            kind: WorkUnitKind::Review,
                            objective: "Confirm whether the revised output is ready for synthesis",
                            prompt: &build_review_prompt(
                                objective,
                                constraints,
                                plan,
                                &revised_output.content,
                            ),
                            context_id,
                            input_artifacts: std::slice::from_ref(&revised_output),
                            collaborator_requirements: &[],
                        },
                    )
                    .await?;

                match parse_review_disposition(&second_review.content)? {
                    ReviewDisposition::Approved => {
                        workflow.audit_log.push(
                            "review approved revised output; continuing to synthesis"
                                .to_string(),
                        );
                        workflow.updated_at = Utc::now();
                        self.persist(workflow.clone());
                        Ok(revised_output)
                    }
                    ReviewDisposition::RevisionRequired => Err(anyhow!(
                        "review requested additional revision after revise stage: {}",
                        second_review.content.trim()
                    )),
                }
            }
        }
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

/// Returns capability requirements for a given workflow stage.
/// These let the scheduler auto-select the best-fit runtime.
fn stage_requirements(kind: &WorkUnitKind) -> TaskRequirements {
    match kind {
        WorkUnitKind::Plan => TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "planning".to_string(),
                min_level: 0.6,
                weight: 1.0,
            }],
            preferred_capabilities: vec![CapabilityRequirement {
                capability: "architecture".to_string(),
                min_level: 0.5,
                weight: 0.6,
            }],
            max_latency_ms: Some(20_000),
            min_success_rate: Some(0.4),
            max_cost: None,
            trust_tier: Some(1),
        },
        WorkUnitKind::Execute | WorkUnitKind::Revise => TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "implementation".to_string(),
                min_level: 0.6,
                weight: 1.0,
            }],
            preferred_capabilities: vec![CapabilityRequirement {
                capability: "review".to_string(),
                min_level: 0.5,
                weight: 0.3,
            }],
            max_latency_ms: Some(30_000),
            min_success_rate: Some(0.4),
            max_cost: None,
            trust_tier: Some(1),
        },
        WorkUnitKind::Synthesize => TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "synthesis".to_string(),
                min_level: 0.5,
                weight: 1.0,
            }],
            preferred_capabilities: vec![CapabilityRequirement {
                capability: "reporting".to_string(),
                min_level: 0.4,
                weight: 0.3,
            }],
            max_latency_ms: Some(30_000),
            min_success_rate: Some(0.4),
            max_cost: None,
            trust_tier: Some(1),
        },
        WorkUnitKind::Review => TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "review".to_string(),
                min_level: 0.6,
                weight: 1.0,
            }],
            preferred_capabilities: vec![CapabilityRequirement {
                capability: "security".to_string(),
                min_level: 0.4,
                weight: 0.4,
            }],
            max_latency_ms: Some(20_000),
            min_success_rate: Some(0.4),
            max_cost: None,
            trust_tier: Some(1),
        },
        WorkUnitKind::Consult => TaskRequirements {
            required_capabilities: vec![CapabilityRequirement {
                capability: "planning".to_string(),
                min_level: 0.3,
                weight: 1.0,
            }],
            preferred_capabilities: Vec::new(),
            max_latency_ms: Some(15_000),
            min_success_rate: Some(0.4),
            max_cost: None,
            trust_tier: Some(1),
        },
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

fn build_execute_step_prompt(
    objective: &str,
    constraints: &[String],
    plan: &str,
    step_number: usize,
    total_steps: usize,
    step: &str,
    prior_outputs: &[Artifact],
) -> String {
    let prior_output_summary = if prior_outputs.is_empty() {
        "None yet.".to_string()
    } else {
        prior_outputs
            .iter()
            .enumerate()
            .map(|(index, artifact)| {
                format!(
                    "Step {} output:\n{}",
                    index + 1,
                    artifact.content.trim()
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    };

    format!(
        "You are executing step {step_number} of {total_steps} for this objective.\n\n\
        Objective:\n{}\n\nConstraints:\n- {}\n\n\
        ---BEGIN PLAN---\n{}\n---END PLAN---\n\n\
        Current step:\n{}\n\n\
        Prior completed outputs:\n{}\n\n\
        You may consult collaborator runtimes through A2A if that improves correctness.\n\
        Return ONLY the output for this step. Use plain text or markdown. Do NOT wrap your response in JSON.",
        objective,
        constraints.join("\n- "),
        plan,
        step,
        prior_output_summary
    )
}

fn build_review_prompt(
    objective: &str,
    constraints: &[String],
    plan: &str,
    execution_output: &str,
) -> String {
    format!(
        "You are a review agent performing a quality gate on a workflow result.\n\n\
        Objective:\n{}\n\nConstraints:\n- {}\n\n\
        ---BEGIN PLAN---\n{}\n---END PLAN---\n\n\
        ---BEGIN EXECUTION OUTPUT---\n{}\n---END EXECUTION OUTPUT---\n\n\
        Decide whether the execution output is ready for final synthesis.\n\
        The FIRST LINE of your response MUST be exactly one of:\n\
        APPROVED\n\
        REVISION_REQUIRED\n\n\
        After the first line, provide concise review notes.",
        objective,
        constraints.join("\n- "),
        plan,
        execution_output
    )
}

fn build_revise_prompt(
    objective: &str,
    constraints: &[String],
    plan: &str,
    execution_output: &str,
    review_feedback: &str,
) -> String {
    format!(
        "You are a revision agent improving a prior execution result.\n\n\
        Objective:\n{}\n\nConstraints:\n- {}\n\n\
        ---BEGIN PLAN---\n{}\n---END PLAN---\n\n\
        ---BEGIN PRIOR EXECUTION OUTPUT---\n{}\n---END PRIOR EXECUTION OUTPUT---\n\n\
        ---BEGIN REVIEW FEEDBACK---\n{}\n---END REVIEW FEEDBACK---\n\n\
        Produce a revised result that addresses every review issue.\n\
        Return ONLY the revised output. Use plain text or markdown. Do NOT wrap your response in JSON.",
        objective,
        constraints.join("\n- "),
        plan,
        execution_output,
        review_feedback
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

fn execute_collaborator_requirements() -> Vec<CapabilityRequirement> {
    vec![
        CapabilityRequirement {
            capability: "review".to_string(),
            min_level: 0.5,
            weight: 0.6,
        },
        CapabilityRequirement {
            capability: "planning".to_string(),
            min_level: 0.5,
            weight: 0.4,
        },
    ]
}

fn build_execute_inputs(plan_output: &Artifact, previous_outputs: &[Artifact]) -> Vec<Artifact> {
    let mut inputs = Vec::with_capacity(previous_outputs.len() + 1);
    inputs.push(plan_output.clone());
    inputs.extend(previous_outputs.iter().cloned());
    inputs
}

fn combine_execution_outputs(outputs: &[Artifact]) -> Artifact {
    let content = outputs
        .iter()
        .enumerate()
        .map(|(index, artifact)| {
            format!(
                "## Execution Step {}\n\n{}",
                index + 1,
                artifact.content.trim()
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    Artifact {
        artifact_id: new_id("artifact"),
        name: "execution-output.md".to_string(),
        mime_type: "text/markdown".to_string(),
        content,
    }
}

fn parse_plan_steps(plan: &str) -> Vec<String> {
    let mut steps = plan
        .lines()
        .filter_map(normalize_plan_step)
        .take(8)
        .collect::<Vec<_>>();

    if steps.is_empty() {
        let compact = plan.trim();
        if !compact.is_empty() {
            steps.push(compact.to_string());
        }
    }

    if steps.is_empty() {
        steps.push("Produce the primary solution".to_string());
    }

    steps
}

fn normalize_plan_step(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| strip_numbered_step_prefix(trimmed))
        .map(str::trim)
        .filter(|step| !step.is_empty())
        .map(ToString::to_string)
}

fn strip_numbered_step_prefix(line: &str) -> Option<&str> {
    let digit_count = line.chars().take_while(|char| char.is_ascii_digit()).count();
    if digit_count == 0 || line.len() <= digit_count + 1 {
        return None;
    }

    let suffix = &line[digit_count..];
    if let Some(rest) = suffix.strip_prefix(". ") {
        Some(rest)
    } else if let Some(rest) = suffix.strip_prefix(") ") {
        Some(rest)
    } else {
        None
    }
}

fn stage_capability_name(kind: &WorkUnitKind) -> &'static str {
    match kind {
        WorkUnitKind::Plan => "planning",
        WorkUnitKind::Execute => "implementation",
        WorkUnitKind::Review => "review",
        WorkUnitKind::Revise => "implementation",
        WorkUnitKind::Synthesize => "synthesis",
        WorkUnitKind::Consult => "planning",
    }
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

fn parse_review_disposition(review_output: &str) -> Result<ReviewDisposition> {
    let normalized = review_output.trim().to_ascii_uppercase();
    let first_line = normalized.lines().next().unwrap_or_default().trim();

    if first_line.starts_with("REVISION_REQUIRED")
        || first_line.starts_with("NEEDS_REVISION")
        || first_line.starts_with("REQUIRES_REVISION")
        || normalized.contains("REVISION_REQUIRED")
        || normalized.contains("NEEDS REVISION")
        || normalized.contains("REQUIRES REVISION")
    {
        Ok(ReviewDisposition::RevisionRequired)
    } else if first_line == "APPROVED" {
        Ok(ReviewDisposition::Approved)
    } else {
        Err(anyhow!(
            "review output missing explicit verdict; expected first line to be APPROVED or REVISION_REQUIRED"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use axum::{Json, Router, routing::post};
    use chrono::Utc;
    use platform_a2a::{
        A2aClient, A2ATask, JsonRpcRequest, JsonRpcResponse, MessagePart, SendMessageParams,
        SendMessageResult, TaskState,
    };
    use platform_domain::{
        Artifact, CapabilitySignal, HealthStatus, Metadata, RuntimeDescriptor, RuntimeHealth,
        RuntimeLoad, WorkUnitKind, WorkflowRequest,
    };
    use super::{
        ReviewDisposition, WorkflowOrchestrator, parse_plan_steps, parse_review_disposition,
    };
    use crate::RuntimeRegistry;

    #[test]
    fn review_parser_detects_revision_requests() {
        assert!(matches!(
            parse_review_disposition("REVISION_REQUIRED\nMissing acceptance criteria"),
            Ok(ReviewDisposition::RevisionRequired)
        ));
        assert!(matches!(
            parse_review_disposition("APPROVED\nLooks good"),
            Ok(ReviewDisposition::Approved)
        ));
    }

    #[test]
    fn review_parser_rejects_ambiguous_verdicts() {
        let error = parse_review_disposition("Looks good overall")
            .expect_err("ambiguous review output must fail closed");

        assert!(error
            .to_string()
            .contains("review output missing explicit verdict"));
    }

    #[test]
    fn review_parser_accepts_only_explicit_approved_first_line() {
        assert!(matches!(
            parse_review_disposition("APPROVED\nLooks good"),
            Ok(ReviewDisposition::Approved)
        ));
        assert!(parse_review_disposition("Looks APPROVED to me").is_err());
    }

    #[test]
    fn plan_parser_extracts_numbered_and_bulleted_steps() {
        let steps = parse_plan_steps(
            "# Plan\n1. Gather requirements\n2. Implement workflow\n- Review quality\n* Synthesize report",
        );

        assert_eq!(
            steps,
            vec![
                "Gather requirements",
                "Implement workflow",
                "Review quality",
                "Synthesize report"
            ]
        );
    }

    #[tokio::test]
    async fn orchestrator_revises_after_review_feedback() -> Result<()> {
        unsafe {
            std::env::set_var("PLATFORM_API_TOKEN", "test-token");
        }

        let review_calls = Arc::new(AtomicUsize::new(0));
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/a2a",
            post({
                let review_calls = review_calls.clone();
                let execute_calls = execute_calls.clone();
                move |Json(request): Json<JsonRpcRequest>| {
                    let review_calls = review_calls.clone();
                    let execute_calls = execute_calls.clone();
                    async move {
                        let params: SendMessageParams = serde_json::from_value(request.params)
                            .expect("valid sendMessage params");
                        let role = params
                            .message
                            .metadata
                            .get("workflow_role")
                            .cloned()
                            .expect("workflow role metadata");
                        let artifact_content = match role.as_str() {
                            "plan" => "1. Prepare workflow\n2. Deliver implementation".to_string(),
                            "execute" => {
                                execute_calls.fetch_add(1, Ordering::SeqCst);
                                let collaborators = params
                                    .message
                                    .metadata
                                    .get("collaborators")
                                    .cloned()
                                    .expect("collaborators metadata");
                                assert_ne!(collaborators, "[]");
                                let input_artifacts = params
                                    .message
                                    .metadata
                                    .get("input_artifacts")
                                    .cloned()
                                    .expect("input artifact metadata");
                                assert!(input_artifacts.contains("artifact-plan") || input_artifacts.contains("execution-output"));
                                "draft implementation".to_string()
                            }
                            "review" => {
                                if review_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                                    "REVISION_REQUIRED\nAdd the missing validation and edge case handling".to_string()
                                } else {
                                    "APPROVED\nThe revised output addresses the issues".to_string()
                                }
                            }
                            "revise" => {
                                let prompt = params.message.parts.iter().find_map(|part| match part {
                                    MessagePart::Text { text } => Some(text.as_str()),
                                    _ => None,
                                }).unwrap_or_default();
                                assert!(prompt.contains("Add the missing validation and edge case handling"));
                                "revised implementation with validation".to_string()
                            }
                            "synthesize" => "final report".to_string(),
                            other => panic!("unexpected stage {other}"),
                        };

                        Json(JsonRpcResponse::success(
                            request.id,
                            serde_json::to_value(SendMessageResult {
                                task: Some(A2ATask {
                                    id: "task-1".to_string(),
                                    context_id: params.message.context_id,
                                    status: TaskState::Completed,
                                    history: vec![],
                                    artifacts: vec![Artifact {
                                        artifact_id: format!("artifact-{role}"),
                                        name: role.clone(),
                                        mime_type: "text/markdown".to_string(),
                                        content: artifact_content,
                                    }],
                                }),
                                message: None,
                            })
                            .expect("serializable sendMessage result"),
                        ))
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server runs");
        });

        let registry = RuntimeRegistry::new();
        registry.register(RuntimeDescriptor {
            runtime_id: "runtime-1".to_string(),
            agent_id: "agent-1".to_string(),
            display_name: "runtime-1".to_string(),
            endpoint: format!("http://{addr}/a2a"),
            profile: "generalist".to_string(),
            trust_tier: 2,
            cost_per_task: 1.0,
            capabilities: vec![
                capability("planning", 0.9),
                capability("implementation", 0.9),
                capability("review", 0.9),
                capability("synthesis", 0.9),
            ],
            health: RuntimeHealth {
                status: HealthStatus::Healthy,
                availability: 0.99,
                last_heartbeat_at: Utc::now(),
            },
            load: RuntimeLoad::default(),
            metadata: Metadata::new(),
        })?;
        registry.register(RuntimeDescriptor {
            runtime_id: "runtime-2".to_string(),
            agent_id: "agent-2".to_string(),
            display_name: "runtime-2".to_string(),
            endpoint: format!("http://{addr}/a2a"),
            profile: "reviewer".to_string(),
            trust_tier: 2,
            cost_per_task: 1.0,
            capabilities: vec![capability("review", 0.95), capability("planning", 0.92)],
            health: RuntimeHealth {
                status: HealthStatus::Healthy,
                availability: 0.99,
                last_heartbeat_at: Utc::now(),
            },
            load: RuntimeLoad::default(),
            metadata: Metadata::new(),
        })?;

        let client = A2aClient::new(reqwest::Client::new());
        let orchestrator = WorkflowOrchestrator::new(registry.clone(), client);
        let record = orchestrator
            .submit(WorkflowRequest {
                objective: "Ship a robust implementation".to_string(),
                constraints: vec!["Include validation".to_string()],
                context_id: Some("ctx-1".to_string()),
            })
            .await?;

        let stage_kinds = record
            .work_units
            .iter()
            .map(|unit| format!("{:?}", unit.kind))
            .collect::<Vec<_>>();
        assert_eq!(
            stage_kinds,
            vec!["Plan", "Execute", "Execute", "Review", "Revise", "Review", "Synthesize"]
        );
        assert_eq!(
            record.final_report.expect("final report").content,
            "final report"
        );
        assert_eq!(review_calls.load(Ordering::SeqCst), 2);
        assert_eq!(execute_calls.load(Ordering::SeqCst), 2);

        let runtime = registry.get("runtime-1").expect("runtime remains registered");
        let implementation = runtime
            .capabilities
            .iter()
            .find(|capability| capability.name == "implementation")
            .expect("implementation capability");
        assert!(implementation.success_rate > 0.85);

        Ok(())
    }

    #[tokio::test]
    async fn orchestrator_falls_back_to_single_runtime_for_review() -> Result<()> {
        unsafe {
            std::env::set_var("PLATFORM_API_TOKEN", "test-token");
        }

        let app = Router::new().route(
            "/a2a",
            post(|Json(request): Json<JsonRpcRequest>| async move {
                let params: SendMessageParams = serde_json::from_value(request.params)
                    .expect("valid sendMessage params");
                let role = params
                    .message
                    .metadata
                    .get("workflow_role")
                    .cloned()
                    .expect("workflow role metadata");
                let artifact_content = match role.as_str() {
                    "plan" => "1. Return OK".to_string(),
                    "execute" => "OK".to_string(),
                    "review" => "APPROVED\nLooks good".to_string(),
                    "synthesize" => "OK".to_string(),
                    other => panic!("unexpected stage {other}"),
                };

                Json(JsonRpcResponse::success(
                    request.id,
                    serde_json::to_value(SendMessageResult {
                        task: Some(A2ATask {
                            id: format!("task-{role}"),
                            context_id: params.message.context_id,
                            status: TaskState::Completed,
                            history: vec![],
                            artifacts: vec![Artifact {
                                artifact_id: format!("artifact-{role}"),
                                name: role,
                                mime_type: "text/markdown".to_string(),
                                content: artifact_content,
                            }],
                        }),
                        message: None,
                    })
                    .expect("serializable sendMessage result"),
                ))
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server runs");
        });

        let registry = RuntimeRegistry::new();
        registry.register(RuntimeDescriptor {
            runtime_id: "runtime-1".to_string(),
            agent_id: "agent-1".to_string(),
            display_name: "runtime-1".to_string(),
            endpoint: format!("http://{addr}/a2a"),
            profile: "generalist".to_string(),
            trust_tier: 2,
            cost_per_task: 1.0,
            capabilities: vec![
                capability("planning", 0.9),
                capability("implementation", 0.9),
                capability("review", 0.9),
                capability("synthesis", 0.9),
            ],
            health: RuntimeHealth {
                status: HealthStatus::Healthy,
                availability: 0.99,
                last_heartbeat_at: Utc::now(),
            },
            load: RuntimeLoad::default(),
            metadata: Metadata::new(),
        })?;

        let client = A2aClient::new(reqwest::Client::new());
        let orchestrator = WorkflowOrchestrator::new(registry, client);
        let record = orchestrator
            .submit(WorkflowRequest {
                objective: "Return OK".to_string(),
                constraints: vec![],
                context_id: Some("ctx-single-runtime".to_string()),
            })
            .await?;

        assert_eq!(record.status, platform_domain::WorkflowStatus::Completed);
        assert_eq!(record.final_report.expect("final report").content, "OK");
        assert!(record
            .audit_log
            .iter()
            .any(|entry| entry.contains("no dedicated reviewer available")));
        assert!(record.work_units.iter().any(|unit| {
            matches!(unit.kind, WorkUnitKind::Review)
                && unit.assigned_runtime_id.as_deref() == Some("runtime-1")
        }));

        Ok(())
    }

    fn capability(name: &str, level: f32) -> CapabilitySignal {
        CapabilitySignal {
            name: name.to_string(),
            level,
            success_rate: 0.99,
            median_latency_ms: 120,
            max_parallelism: 4,
            tags: vec![],
        }
    }
}
