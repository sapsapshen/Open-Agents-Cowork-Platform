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
    pub review_rounds: u8,
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
