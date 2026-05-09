use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{delete, get, post, put},
};
use clap::Parser;
use dashboard::{DashboardState, dashboard_routes};
use platform_a2a::{
    A2aClient, PLATFORM_TOKEN_HEADER, platform_api_token, require_platform_api_token,
    validate_endpoint,
};
use platform_core::{RuntimeRegistry, WorkflowOrchestrator};
use platform_domain::{
    AgentConfig, RuntimeHeartbeat, RuntimeRegistration, WorkflowRecord, WorkflowRequest,
    WorkflowSubmissionResponse,
};
use serde_json::{Value, json};
use tracing::info;

mod agent_manager;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9000")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    a2a_client: A2aClient,
    registry: RuntimeRegistry,
    orchestrator: WorkflowOrchestrator,
    agent_manager: agent_manager::AgentManager,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    require_platform_api_token()?;

    let registry = RuntimeRegistry::new();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;
    let a2a_client = A2aClient::new(client.clone());

    // Dashboard state + observer
    let dashboard_state = Arc::new(DashboardState::new(1000));
    let mut orchestrator = WorkflowOrchestrator::new(registry.clone(), a2a_client.clone());
    orchestrator.add_observer(dashboard_state.clone());

    // Agent manager for user-configured AI agents
    let run_root = std::env::current_dir()
        .unwrap_or_default()
        .join("target")
        .join("platform-runtime");
    let agent_manager = agent_manager::AgentManager::new(
        run_root,
        format!("http://{}", args.bind),
        registry.clone(),
    );

    let state = AppState {
        a2a_client,
        registry,
        orchestrator,
        agent_manager,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/runtimes", get(list_runtimes))
        .route("/runtimes/register", post(register_runtime))
        .route("/runtimes/heartbeat", post(heartbeat_runtime))
        .route("/workflows", get(list_workflows))
        .route("/workflows/submit", post(submit_workflow))
        .route("/workflows/{workflow_id}", get(get_workflow))
        // Agent config management (token required)
        .route("/api/agents", get(list_agents))
        .route("/api/agents", post(add_agent))
        .route("/api/agents/detect", get(detect_agents))
        .route("/api/agents/statuses", get(agent_statuses))
        .route("/api/agents/{id}", put(update_agent))
        .route("/api/agents/{id}", delete(delete_agent))
        .route("/api/agents/{id}/launch", post(launch_agent))
        .route("/api/agents/{id}/stop", post(stop_agent))
        // Dashboard routes (no auth required for the UI)
        .merge(dashboard_routes())
        .layer(Extension(dashboard_state))
        .with_state(state);

    info!("control-plane listening on {}", args.bind);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok", "service": "control-plane" }))
}

async fn index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Open Agents Control Plane</title>
  <style>
    body { font-family: -apple-system, sans-serif; background: #0d1117; color: #e6edf3; padding: 40px; }
    a { color: #58a6ff; }
    ul { line-height: 2; }
  </style>
</head>
<body>
  <h1>Open Agents Control Plane</h1>
  <p>The control plane is running.</p>
  <ul>
    <li><a href="/health">/health</a> - health check</li>
    <li><a href="/dashboard">/dashboard</a> - web dashboard</li>
    <li>/runtimes - list runtimes (requires <code>x-platform-token</code>)</li>
    <li>/workflows - list workflows (requires <code>x-platform-token</code>)</li>
  </ul>
</body>
</html>"#,
    )
}

async fn register_runtime(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(payload): Json<RuntimeRegistration>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    if let Err(error) = validate_endpoint(&payload.runtime.endpoint) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        );
    }
    match state
        .a2a_client
        .get_agent_card(&payload.runtime.endpoint)
        .await
    {
        Ok(card) if card.endpoint == payload.runtime.endpoint => {
            match state.registry.register(payload.runtime) {
                Ok(runtime) => (StatusCode::CREATED, Json(json!(runtime))),
                Err(error) => (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": error.to_string() })),
                ),
            }
        }
        Ok(_) => (
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "runtime agent card endpoint does not match registration endpoint" }),
            ),
        ),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("failed to verify runtime endpoint: {error}") })),
        ),
    }
}

async fn heartbeat_runtime(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(payload): Json<RuntimeHeartbeat>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    match state.registry.heartbeat(payload) {
        Some(runtime) => (
            StatusCode::OK,
            Json(json!({ "updated": true, "runtime": runtime })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "updated": false, "error": "runtime not found" })),
        ),
    }
}

async fn list_runtimes(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    (StatusCode::OK, Json(json!(state.registry.list())))
}

async fn submit_workflow(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(payload): Json<WorkflowRequest>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    if payload.objective.len() > 10_000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "objective length must be <= 10000" })),
        );
    }
    if payload.constraints.len() > 50 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "constraint count must be <= 50" })),
        );
    }
    let result = state.orchestrator.submit(payload).await;
    match result {
        Ok(record) => (StatusCode::OK, Json(json!(to_submission_response(record)))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format_error_chain(&error) })),
        ),
    }
}

async fn list_workflows(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    (
        StatusCode::OK,
        Json(json!(state.orchestrator.list_workflows())),
    )
}

async fn get_workflow(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    match state.orchestrator.get_workflow(&workflow_id) {
        Some(record) => (StatusCode::OK, Json(json!(record))),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "workflow not found" })),
        ),
    }
}

fn to_submission_response(record: WorkflowRecord) -> WorkflowSubmissionResponse {
    WorkflowSubmissionResponse {
        workflow_id: record.workflow_id,
        status: record.status,
        final_report: record.final_report,
        audit_log: record.audit_log,
    }
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

// ── Agent Management Handlers ──────────────────────────────

async fn list_agents(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    let agents = state.agent_manager.list_agents();
    let statuses = state.agent_manager.agent_statuses();
    let mut result: Vec<Value> = agents
        .into_iter()
        .map(|a| {
            let st = statuses
                .get(&a.id)
                .cloned()
                .unwrap_or(json!({"status":"stopped","pid":null}));
            json!({
                "config": a,
                "process": st,
            })
        })
        .collect();
    // Sort: running first, then by name
    result.sort_by(|a, b| {
        let a_running = a["process"]["status"].as_str() == Some("running");
        let b_running = b["process"]["status"].as_str() == Some("running");
        b_running.cmp(&a_running).then(
            a["config"]["name"]
                .as_str()
                .unwrap_or("")
                .cmp(b["config"]["name"].as_str().unwrap_or("")),
        )
    });
    (StatusCode::OK, Json(json!(result)))
}

async fn add_agent(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(payload): Json<AgentConfig>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    if payload.id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "agent id is required" })),
        );
    }
    match state.agent_manager.add_agent(payload) {
        Ok(config) => (
            StatusCode::CREATED,
            Json(json!({ "config": config, "process": {"status":"stopped","pid":null} })),
        ),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn update_agent(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<AgentConfig>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    match state.agent_manager.update_agent(&id, payload) {
        Ok(config) => (StatusCode::OK, Json(json!({ "config": config }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn delete_agent(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    match state.agent_manager.remove_agent(&id) {
        Ok(()) => (StatusCode::OK, Json(json!({ "deleted": true }))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn launch_agent(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    match state.agent_manager.launch_agent(&id).await {
        Ok(()) => {
            let status = state.agent_manager.agent_status(&id);
            (
                StatusCode::OK,
                Json(json!({ "launched": true, "status": status })),
            )
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn stop_agent(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    match state.agent_manager.stop_agent(&id) {
        Ok(()) => (StatusCode::OK, Json(json!({ "stopped": true }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn detect_agents(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    let detected = state.agent_manager.detect_agents();
    (StatusCode::OK, Json(json!(detected)))
}

async fn agent_statuses(headers: HeaderMap, State(state): State<AppState>) -> impl IntoResponse {
    if let Err(response) = require_token(&headers) {
        return response;
    }
    state.agent_manager.reap_zombies();
    let statuses = state.agent_manager.agent_statuses();
    (StatusCode::OK, Json(json!(statuses)))
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .try_init();
}

fn require_token(headers: &HeaderMap) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let received = headers
        .get(PLATFORM_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok());
    if received == platform_api_token().as_deref() {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or invalid platform token" })),
        ))
    }
}
