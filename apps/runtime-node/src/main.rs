use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use clap::Parser;
use platform_a2a::{
    A2ATask, A2aClient, AgentCard, AgentCardCapabilities, AgentSkill, JsonRpcRequest,
    JsonRpcResponse, METHOD_AGENT_GET_CARD, METHOD_MESSAGE_SEND, METHOD_TASKS_GET, Message,
    MessagePart, MessageRole, PLATFORM_TOKEN_HEADER, SendMessageParams, SendMessageResult,
    TaskState, platform_api_token, require_platform_api_token, text_message, validate_endpoint,
};
use platform_domain::{
    Artifact, CapabilitySignal, HealthStatus, Metadata, RuntimeDescriptor, RuntimeHealth,
    RuntimeLoad, RuntimeRegistration, new_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};
use tracing::info;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9101")]
    bind: SocketAddr,
    #[arg(long)]
    public_endpoint: Option<String>,
    #[arg(long)]
    control_plane: Option<String>,
    #[arg(long, default_value = "runtime-1")]
    runtime_id: String,
    #[arg(long, default_value = "agent-1")]
    agent_id: String,
    #[arg(long, default_value = "generalist")]
    profile: String,
    #[arg(long, default_value = "2")]
    trust_tier: u8,
    #[arg(long, default_value = "1.0")]
    cost_per_task: f32,
    #[arg(long, default_value_t = false)]
    auto_register: bool,
}

#[derive(Clone)]
struct AppState {
    runtime: RuntimeDescriptor,
    client: A2aClient,
    inflight_tasks: Arc<AtomicU32>,
    tasks: Arc<RwLock<BTreeMap<String, A2ATask>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsultSummary {
    runtime_id: String,
    response: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    require_platform_api_token()?;
    let public_endpoint = args
        .public_endpoint
        .clone()
        .unwrap_or_else(|| format!("http://{}/a2a", args.bind));

    let runtime = RuntimeDescriptor {
        runtime_id: args.runtime_id.clone(),
        agent_id: args.agent_id.clone(),
        display_name: format!("{}-{}", args.profile, args.runtime_id),
        endpoint: public_endpoint,
        profile: args.profile.clone(),
        trust_tier: args.trust_tier,
        cost_per_task: args.cost_per_task,
        capabilities: capabilities_for_profile(&args.profile),
        health: RuntimeHealth {
            status: HealthStatus::Healthy,
            availability: 0.99,
            last_heartbeat_at: chrono::Utc::now(),
        },
        load: RuntimeLoad::default(),
        metadata: Metadata::new(),
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;
    let state = AppState {
        runtime: runtime.clone(),
        client: A2aClient::new(client.clone()),
        inflight_tasks: Arc::new(AtomicU32::new(0)),
        tasks: Arc::new(RwLock::new(BTreeMap::new())),
    };

    let inflight_tasks = state.inflight_tasks.clone();
    let app = Router::new()
        .route("/health", get(health))
        .route("/agent-card", get(agent_card_endpoint))
        .route("/a2a", post(handle_a2a))
        .with_state(state);

    info!("runtime-node listening on {}", args.bind);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    if args.auto_register {
        let control_plane = args.control_plane.clone();
        let runtime = runtime.clone();
        let client = client.clone();
        let inflight_tasks = inflight_tasks.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut backoff = Duration::from_millis(500);
            loop {
                match auto_register(control_plane.as_deref(), &client, runtime.clone()).await {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            runtime_id = %runtime.runtime_id,
                            "runtime auto-registration failed; retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_secs(5));
                    }
                }
            }
            if let Some(control_plane) = control_plane {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let active_tasks = inflight_tasks.load(Ordering::SeqCst);
                    if let Err(error) =
                        send_heartbeat(&client, &control_plane, &runtime.runtime_id, active_tasks)
                            .await
                    {
                        tracing::warn!(
                            %error,
                            runtime_id = %runtime.runtime_id,
                            "runtime heartbeat failed"
                        );
                    }
                }
            }
        });
    }
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": "runtime-node" }))
}

async fn agent_card_endpoint(State(state): State<AppState>) -> Json<AgentCard> {
    Json(build_agent_card(&state.runtime))
}

async fn handle_a2a(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(request): Json<JsonRpcRequest>,
) -> (StatusCode, Json<JsonRpcResponse>) {
    if let Err(status) = require_token(&headers) {
        return (
            status,
            Json(JsonRpcResponse::error(
                json!(null),
                40101,
                "missing or invalid platform token",
                None,
            )),
        );
    }
    if let Some(version) = headers
        .get("A2A-Version")
        .and_then(|value| value.to_str().ok())
        && version != "1.0"
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse::error(
                request.id,
                40010,
                format!("unsupported A2A version {version}"),
                None,
            )),
        );
    }

    let response = match request.method.as_str() {
        METHOD_AGENT_GET_CARD => JsonRpcResponse::success(
            request.id,
            serde_json::to_value(build_agent_card(&state.runtime)).unwrap_or_else(
                |error| json!({ "error": format!("serialization failure: {error}") }),
            ),
        ),
        METHOD_MESSAGE_SEND => match serde_json::from_value::<SendMessageParams>(request.params) {
            Ok(params) => match process_message(&state, params.message).await {
                Ok(result) => JsonRpcResponse::success(
                    request.id,
                    serde_json::to_value(result).unwrap_or_else(
                        |error| json!({ "error": format!("serialization failure: {error}") }),
                    ),
                ),
                Err(error) => JsonRpcResponse::internal_error(request.id, error.to_string()),
            },
            Err(error) => JsonRpcResponse::invalid_params(request.id, error.to_string()),
        },
        METHOD_TASKS_GET => {
            let task_id = request
                .params
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string);
            match task_id {
                Some(task_id) => {
                    let tasks = state.tasks.read().await;
                    match tasks.get(&task_id) {
                        Some(task) => JsonRpcResponse::success(
                            request.id,
                            serde_json::to_value(task).unwrap_or_else(|error| {
                                json!({ "error": format!("serialization failure: {error}") })
                            }),
                        ),
                        None => JsonRpcResponse::error(
                            request.id,
                            40401,
                            format!("task {task_id} not found"),
                            None,
                        ),
                    }
                }
                None => JsonRpcResponse::invalid_params(request.id, "missing task id"),
            }
        }
        other => JsonRpcResponse::method_not_found(request.id, other),
    };

    (StatusCode::OK, Json(response))
}

async fn process_message(state: &AppState, message: Message) -> Result<SendMessageResult> {
    state.inflight_tasks.fetch_add(1, Ordering::SeqCst);
    let result = process_message_inner(state, message).await;
    state.inflight_tasks.fetch_sub(1, Ordering::SeqCst);
    result
}

async fn process_message_inner(state: &AppState, message: Message) -> Result<SendMessageResult> {
    let prompt = message
        .parts
        .iter()
        .filter_map(|part| match part {
            MessagePart::Text { text } => Some(text.clone()),
            MessagePart::Json { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let workflow_role = message
        .metadata
        .get("workflow_role")
        .cloned()
        .unwrap_or_else(|| "consult".to_string());
    let collaborators = parse_collaborators(&message.metadata)?;
    let consults = consult_peers(state, &message, &collaborators).await?;
    let input_artifacts = parse_input_artifacts(&message.metadata)?;

    let artifact = Artifact {
        artifact_id: new_id("artifact"),
        name: format!("{}-output.md", workflow_role),
        mime_type: "text/markdown".to_string(),
        content: render_output(
            &state.runtime,
            &workflow_role,
            &prompt,
            &input_artifacts,
            &consults,
            &message.metadata,
        ),
    };

    let response_message = text_message(
        new_id("msg"),
        MessageRole::Agent,
        None,
        message.context_id.clone(),
        format!("completed {}", workflow_role),
    );

    let context_id = message.context_id.clone();
    let task = A2ATask {
        id: new_id("task"),
        context_id,
        status: TaskState::Completed,
        history: vec![message, response_message],
        artifacts: vec![artifact],
    };

    state
        .tasks
        .write()
        .await
        .insert(task.id.clone(), task.clone());
    Ok(SendMessageResult {
        task: Some(task),
        message: None,
    })
}

async fn consult_peers(
    state: &AppState,
    message: &Message,
    collaborators: &[RuntimeDescriptor],
) -> Result<Vec<ConsultSummary>> {
    let consultation_depth = message
        .metadata
        .get("consultation_depth")
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(0);
    if consultation_depth >= 2
        || message
            .metadata
            .get("workflow_role")
            .is_some_and(|role| role == "consult")
    {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    for collaborator in collaborators {
        let consult_prompt = format!(
            "Collaborator request from {}. Objective: {}. Provide 3 concise recommendations.",
            state.runtime.runtime_id,
            message
                .metadata
                .get("objective")
                .cloned()
                .unwrap_or_else(|| "unspecified".to_string())
        );
        let mut consult_message = text_message(
            new_id("msg"),
            MessageRole::User,
            None,
            message.context_id.clone(),
            consult_prompt,
        );
        consult_message
            .metadata
            .insert("workflow_role".to_string(), "consult".to_string());
        consult_message.metadata.insert(
            "consultation_depth".to_string(),
            (consultation_depth + 1).to_string(),
        );
        let response = timeout(
            Duration::from_secs(15),
            state
                .client
                .send_message(&collaborator.endpoint, consult_message),
        )
        .await
        .map_err(|_| anyhow!("consultation timeout for {}", collaborator.runtime_id))?
        .with_context(|| format!("consulting collaborator {}", collaborator.runtime_id))?;
        let task = response
            .task
            .ok_or_else(|| anyhow!("collaborator {} returned no task", collaborator.runtime_id))?;
        let artifact = task.artifacts.first().ok_or_else(|| {
            anyhow!(
                "collaborator {} returned no artifact",
                collaborator.runtime_id
            )
        })?;
        results.push(ConsultSummary {
            runtime_id: collaborator.runtime_id.clone(),
            response: artifact.content.clone(),
        });
    }
    Ok(results)
}

fn parse_collaborators(metadata: &Metadata) -> Result<Vec<RuntimeDescriptor>> {
    match metadata.get("collaborators") {
        Some(value) if !value.is_empty() => {
            if value.len() > 64 * 1024 {
                return Err(anyhow!("collaborators metadata exceeds 64 KiB"));
            }
            let collaborators: Vec<RuntimeDescriptor> =
                serde_json::from_str(value).context("parsing collaborators")?;
            if collaborators.len() > 8 {
                return Err(anyhow!("collaborator fan-out exceeds 8 runtimes"));
            }
            Ok(collaborators)
        }
        _ => Ok(Vec::new()),
    }
}

fn parse_input_artifacts(metadata: &Metadata) -> Result<Vec<Artifact>> {
    match metadata.get("input_artifacts") {
        Some(value) if !value.is_empty() => {
            if value.len() > 128 * 1024 {
                return Err(anyhow!("input_artifacts metadata exceeds 128 KiB"));
            }
            serde_json::from_str(value).context("parsing input artifacts")
        }
        _ => Ok(Vec::new()),
    }
}

fn render_output(
    runtime: &RuntimeDescriptor,
    workflow_role: &str,
    prompt: &str,
    input_artifacts: &[Artifact],
    consults: &[ConsultSummary],
    metadata: &Metadata,
) -> String {
    match workflow_role {
        "plan" => render_plan(runtime, prompt, metadata),
        "review" => render_review(prompt),
        "synthesize" => render_synthesis(prompt, input_artifacts),
        "consult" => render_consult(runtime),
        "revise" => render_execution(runtime, prompt, consults, true),
        _ => render_execution(runtime, prompt, consults, false),
    }
}

fn render_plan(runtime: &RuntimeDescriptor, prompt: &str, metadata: &Metadata) -> String {
    format!(
        "# Capability-Aware Execution Plan\n\nGenerated by `{}`.\n\n## Objective\n{}\n\n## Recommended stages\n1. Planning and requirement extraction.\n2. Implementation on the strongest Rust/A2A runtime.\n3. Direct peer consultation over A2A for design critique.\n4. Independent review rounds.\n5. Revision until no material issues remain.\n6. Final synthesis.\n\n## Control data\n- Constraints: {}\n- Strategy: score by capability coverage, success rate, latency, and runtime load.\n",
        runtime.runtime_id,
        prompt,
        metadata
            .get("constraints")
            .cloned()
            .unwrap_or_else(|| "none".to_string())
    )
}

fn render_execution(
    runtime: &RuntimeDescriptor,
    prompt: &str,
    consults: &[ConsultSummary],
    is_revision: bool,
) -> String {
    let mut sections = vec![
        "# Rust Multi-Agent Platform".to_string(),
        format!("Generated by `{}`.", runtime.runtime_id),
        "## Workspace".to_string(),
        "- apps/control-plane for orchestration and scheduling".to_string(),
        "- apps/runtime-node for A2A-compliant runtime endpoints".to_string(),
        "- apps/cli for operator access".to_string(),
        "- crates/platform-domain, platform-a2a, platform-core for shared logic".to_string(),
        "## Capability-Based Scheduling".to_string(),
        "- Hard filters on required capabilities, trust tier, latency, and cost.".to_string(),
        "- Weighted score on quality fit, availability, cost, and load.".to_string(),
        "## A2A Dialogue Path".to_string(),
        "- Each runtime exposes an Agent Card and JSON-RPC A2A endpoint.".to_string(),
        "- Builder runtimes directly consult planner/reviewer peers through `message/send`."
            .to_string(),
        "- Control plane remains the orchestrator, not the message relay.".to_string(),
        "## Prompt Context".to_string(),
        prompt.to_string(),
    ];

    if !consults.is_empty() {
        sections.push("## Peer Consultations".to_string());
        for consult in consults {
            sections.push(format!(
                "- {} advised: {}",
                consult.runtime_id,
                consult.response.replace('\n', " ")
            ));
        }
    }

    if is_revision || prompt.contains("observability") {
        sections.push("## Observability & Audit".to_string());
        sections.push(
            "- Correlate workflow_id, work_unit_id, runtime_id, task_id, and context_id across logs."
                .to_string(),
        );
    }

    if is_revision || prompt.contains("security") || prompt.contains("policy") {
        sections.push("## Policy & Security".to_string());
        sections.push("- Apply trust tiers, runtime admission policy, and version validation on every A2A call.".to_string());
    }

    if is_revision || prompt.contains("failure") {
        sections.push("## Failure Handling".to_string());
        sections.push(
            "- Lease-based retries, explicit task terminal states, and revision loops close material issues."
                .to_string(),
        );
    }

    sections.join("\n")
}

fn render_review(prompt: &str) -> String {
    let output = prompt.to_lowercase();
    let mut issues = Vec::new();
    if !output.contains("observability") {
        issues.push("Add explicit observability and audit propagation.");
    }
    if !output.contains("policy") && !output.contains("security") {
        issues.push("Document policy or security controls for cross-runtime traffic.");
    }
    if !output.contains("failure handling") {
        issues.push("Describe failure handling, retries, and terminal task semantics.");
    }
    if !output.contains("a2a") {
        issues.push("The output must explicitly describe A2A-based dialogue.");
    }

    if issues.is_empty() {
        return "# Review Report\n\nVerdict: pass\n\n- ISSUE: none".to_string();
    }

    let issue_block = issues
        .into_iter()
        .map(|issue| format!("- ISSUE: {issue}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("# Review Report\n\nVerdict: needs_revision\n\n{issue_block}")
}

fn render_synthesis(prompt: &str, input_artifacts: &[Artifact]) -> String {
    let sources = input_artifacts
        .iter()
        .map(|artifact| format!("- {}", artifact.name))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# Final Synthesis\n\n{}\n\n## Inputs\n{}\n\n## Outcome\nThe platform is composed as a Rust workspace with capability-aware scheduling, direct A2A runtime dialogue, and iterative review closure.",
        prompt, sources
    )
}

fn render_consult(runtime: &RuntimeDescriptor) -> String {
    format!(
        "Use {} capability profile `{}` to strengthen capability scoring, explicit A2A role boundaries, and review completeness.",
        runtime.runtime_id, runtime.profile
    )
}

fn build_agent_card(runtime: &RuntimeDescriptor) -> AgentCard {
    AgentCard {
        protocol_version: "1.0".to_string(),
        name: runtime.display_name.clone(),
        description: format!(
            "{} runtime exposing A2A JSON-RPC and direct peer collaboration",
            runtime.profile
        ),
        endpoint: runtime.endpoint.clone(),
        skills: runtime
            .capabilities
            .iter()
            .map(|capability| AgentSkill {
                id: capability.name.clone(),
                name: capability.name.clone(),
                description: format!("{} capability at {:.2}", capability.name, capability.level),
                tags: capability.tags.clone(),
            })
            .collect(),
        capabilities: AgentCardCapabilities {
            streaming: false,
            push_notifications: false,
            multi_turn: true,
            direct_collaboration: true,
        },
        default_input_modes: vec!["text/plain".to_string(), "application/json".to_string()],
        default_output_modes: vec!["text/markdown".to_string(), "application/json".to_string()],
        metadata: Metadata::new(),
    }
}

fn capabilities_for_profile(profile: &str) -> Vec<CapabilitySignal> {
    match profile {
        "planner" => vec![
            capability("planning", 0.96, 180),
            capability("architecture", 0.91, 200),
            capability("synthesis", 0.74, 200),
        ],
        "builder" => vec![
            capability("implementation", 0.97, 220),
            capability("rust", 0.98, 190),
            capability("a2a", 0.88, 230),
            capability("planning", 0.62, 210),
        ],
        "reviewer" => vec![
            capability("review", 0.98, 150),
            capability("security", 0.92, 170),
            capability("observability", 0.90, 170),
            capability("architecture", 0.75, 180),
        ],
        "synthesizer" => vec![
            capability("synthesis", 0.94, 180),
            capability("reporting", 0.95, 160),
            capability("review", 0.70, 200),
        ],
        _ => vec![
            capability("implementation", 0.78, 220),
            capability("rust", 0.82, 210),
            capability("review", 0.80, 180),
            capability("planning", 0.78, 180),
            capability("synthesis", 0.80, 180),
            capability("a2a", 0.80, 200),
        ],
    }
}

fn capability(name: &str, level: f32, median_latency_ms: u32) -> CapabilitySignal {
    CapabilitySignal {
        name: name.to_string(),
        level,
        success_rate: 0.97,
        median_latency_ms,
        max_parallelism: 4,
        tags: vec![name.to_string()],
    }
}

async fn auto_register(
    control_plane: Option<&str>,
    client: &reqwest::Client,
    runtime: RuntimeDescriptor,
) -> Result<()> {
    let control_plane = control_plane
        .ok_or_else(|| anyhow!("--control-plane is required when --auto-register is enabled"))?;
    validate_endpoint(control_plane)?;
    let registration = RuntimeRegistration { runtime };
    client
        .post(format!("{control_plane}/runtimes/register"))
        .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
        .json(&registration)
        .send()
        .await
        .context("registering runtime with control plane")?
        .error_for_status()
        .context("control plane rejected runtime registration")?;
    Ok(())
}

async fn send_heartbeat(
    client: &reqwest::Client,
    control_plane: &str,
    runtime_id: &str,
    active_tasks: u32,
) -> Result<()> {
    validate_endpoint(control_plane)?;
    client
        .post(format!("{control_plane}/runtimes/heartbeat"))
        .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
        .json(&serde_json::json!({
            "runtime_id": runtime_id,
            "active_tasks": active_tasks,
            "queued_tasks": 0,
            "availability": 0.99
        }))
        .send()
        .await
        .context("sending runtime heartbeat")?
        .error_for_status()
        .context("control plane rejected runtime heartbeat")?;
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .try_init();
}

fn require_token(headers: &HeaderMap) -> Result<(), StatusCode> {
    let received = headers
        .get(PLATFORM_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok());
    if received == platform_api_token().as_deref() {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
