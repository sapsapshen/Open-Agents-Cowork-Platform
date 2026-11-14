use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Metadata = BTreeMap<String, String>;

pub fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnitKind {
    Plan,
    Execute,
    Review,
    Revise,
    Synthesize,
    Consult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnitStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapabilitySignal {
    pub name: String,
    pub level: f32,
    pub success_rate: f32,
    pub median_latency_ms: u32,
    pub max_parallelism: u32,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapabilityRequirement {
    pub capability: String,
    pub min_level: f32,
    pub weight: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskRequirements {
    pub required_capabilities: Vec<CapabilityRequirement>,
    pub preferred_capabilities: Vec<CapabilityRequirement>,
    pub max_latency_ms: Option<u32>,
    pub min_success_rate: Option<f32>,
    pub max_cost: Option<f32>,
    pub trust_tier: Option<u8>,
}

impl Default for TaskRequirements {
    fn default() -> Self {
        Self {
            required_capabilities: Vec::new(),
            preferred_capabilities: Vec::new(),
            max_latency_ms: None,
            min_success_rate: None,
            max_cost: None,
            trust_tier: Some(1),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeLoad {
    pub active_tasks: u32,
    pub max_tasks: u32,
    pub queued_tasks: u32,
}

impl Default for RuntimeLoad {
    fn default() -> Self {
        Self {
            active_tasks: 0,
            max_tasks: 4,
            queued_tasks: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeHealth {
    pub status: HealthStatus,
    pub availability: f32,
    pub last_heartbeat_at: DateTime<Utc>,
}

impl Default for RuntimeHealth {
    fn default() -> Self {
        Self {
            status: HealthStatus::Healthy,
            availability: 0.99,
            last_heartbeat_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeDescriptor {
    pub runtime_id: String,
    pub agent_id: String,
    pub display_name: String,
    pub endpoint: String,
    pub profile: String,
    pub trust_tier: u8,
    pub cost_per_task: f32,
    pub capabilities: Vec<CapabilitySignal>,
    pub health: RuntimeHealth,
    pub load: RuntimeLoad,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AssignmentDecision {
    pub runtime_id: String,
    pub score: f32,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Artifact {
    pub artifact_id: String,
    pub name: String,
    pub mime_type: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConversationTurn {
    pub turn_id: String,
    pub from_runtime_id: String,
    pub to_runtime_id: Option<String>,
    pub prompt: String,
    pub response: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkUnit {
    pub work_unit_id: String,
    pub kind: WorkUnitKind,
    pub objective: String,
    pub instructions: String,
    pub requirements: TaskRequirements,
    pub assigned_runtime_id: Option<String>,
    pub status: WorkUnitStatus,
    pub output: Option<Artifact>,
    pub review_feedback: Vec<String>,
    pub transcript: Vec<ConversationTurn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRequest {
    pub objective: String,
    pub constraints: Vec<String>,
    pub context_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRecord {
    pub workflow_id: String,
    pub request: WorkflowRequest,
    pub status: WorkflowStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub work_units: Vec<WorkUnit>,
    pub final_report: Option<Artifact>,
    pub audit_log: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeRegistration {
    pub runtime: RuntimeDescriptor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeHeartbeat {
    pub runtime_id: String,
    pub active_tasks: u32,
    pub queued_tasks: u32,
    pub availability: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowSubmissionResponse {
    pub workflow_id: String,
    pub status: WorkflowStatus,
    pub final_report: Option<Artifact>,
    pub audit_log: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentConfig {
    pub id: String,
    pub name: String,
    pub backend_type: String,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    /// Environment variable to read the API key from at launch time.
    /// The secret itself is not stored in `agents.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub enabled: bool,
    pub auto_launch: bool,
}

impl AgentConfig {
    pub fn display_type(&self) -> &str {
        match self.backend_type.as_str() {
            "openai" => "OpenAI API",
            "stdio" => "CLI Process",
            "claude_cli" => "Claude Code",
            "codex_cli" => "Codex CLI",
            _ => "Unknown",
        }
    }
}

pub const MAX_OBJECTIVE_CHARS: usize = 10_000;
pub const MAX_CONSTRAINTS: usize = 50;
pub const MAX_CONSTRAINT_CHARS: usize = 4_000;

/// Reject workflow requests that cannot be scheduled safely.
pub fn validate_workflow_request(request: &WorkflowRequest) -> Result<(), String> {
    if request.objective.trim().is_empty() {
        return Err("objective must not be empty".to_string());
    }
    if request.objective.chars().count() > MAX_OBJECTIVE_CHARS {
        return Err(format!(
            "objective length must be <= {MAX_OBJECTIVE_CHARS} characters"
        ));
    }
    if request.constraints.len() > MAX_CONSTRAINTS {
        return Err(format!("constraint count must be <= {MAX_CONSTRAINTS}"));
    }
    if let Some(constraint) = request
        .constraints
        .iter()
        .find(|constraint| constraint.chars().count() > MAX_CONSTRAINT_CHARS)
    {
        return Err(format!(
            "constraint length must be <= {MAX_CONSTRAINT_CHARS} characters (got {})",
            constraint.chars().count()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_request_rejects_blank_objective() {
        let error = validate_workflow_request(&WorkflowRequest {
            objective: "   ".to_string(),
            constraints: vec![],
            context_id: None,
        })
        .expect_err("blank objective");
        assert!(error.contains("must not be empty"));
    }

    #[test]
    fn workflow_request_accepts_a_normal_objective() {
        validate_workflow_request(&WorkflowRequest {
            objective: "Ship the workflow".to_string(),
            constraints: vec!["rust-only".to_string()],
            context_id: None,
        })
        .expect("valid request");
    }

    #[test]
    fn display_type_names_known_backends() {
        let mut config = AgentConfig {
            id: "agent".to_string(),
            name: "Agent".to_string(),
            backend_type: "claude_cli".to_string(),
            base_url: None,
            model: None,
            api_key: None,
            api_key_env: None,
            command: None,
            args: vec![],
            enabled: true,
            auto_launch: false,
        };
        assert_eq!(config.display_type(), "Claude Code");
        config.backend_type = "codex_cli".to_string();
        assert_eq!(config.display_type(), "Codex CLI");
    }
}
