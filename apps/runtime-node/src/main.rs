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
    A2ATask, AgentCard, AgentCardCapabilities, AgentSkill, JsonRpcRequest, JsonRpcResponse,
    METHOD_AGENT_GET_CARD, METHOD_MESSAGE_SEND, METHOD_TASKS_GET, Message, MessageRole,
    PLATFORM_TOKEN_HEADER, SendMessageParams, SendMessageResult, TaskState, platform_api_token,
    require_platform_api_token, text_message, validate_endpoint,
};
use platform_domain::{
    HealthStatus, Metadata, RuntimeDescriptor, RuntimeHealth, RuntimeLoad, RuntimeRegistration,
    new_id,
};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::time::Duration;
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
    #[arg(long, default_value = "")]
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
    inflight_tasks: Arc<AtomicU32>,
    tasks: Arc<RwLock<BTreeMap<String, A2ATask>>>,
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
        capabilities: Vec::new(), // Real agents register their own capabilities
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
    // Extract the workflow metadata from the incoming message
    let workflow_role = message
        .metadata
        .get("workflow_role")
        .cloned()
        .unwrap_or_else(|| "task".to_string());
    let workflow_id = message
        .metadata
        .get("workflow_id")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    // Acknowledge receipt — real agents would process and produce meaningful output here.
    // The runtime node is a protocol bridge; actual AI processing happens
    // in external agent services connected via A2A.
    let content = format!(
        "# Task Acknowledged\n\n\
         Runtime `{}` received a `{}` request for workflow `{}`.\n\n\
         ## Message Received\n\
         {}",
        state.runtime.runtime_id,
        workflow_role,
        workflow_id,
        message
            .parts
            .iter()
            .filter_map(|part| match part {
                platform_a2a::MessagePart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    );

    let artifact = platform_domain::Artifact {
        artifact_id: new_id("artifact"),
        name: format!("{}-response.md", workflow_role),
        mime_type: "text/markdown".to_string(),
        content,
    };

    let response_message = text_message(
        new_id("msg"),
        MessageRole::Agent,
        None,
        message.context_id.clone(),
        format!("acknowledged {}", workflow_role),
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

fn build_agent_card(runtime: &RuntimeDescriptor) -> AgentCard {
    AgentCard {
        protocol_version: "1.0".to_string(),
        name: runtime.display_name.clone(),
        description: format!(
            "{} runtime — A2A-compliant agent node",
            if runtime.profile.is_empty() {
                "unconfigured"
            } else {
                &runtime.profile
            }
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
