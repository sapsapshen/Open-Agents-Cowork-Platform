use std::net::SocketAddr;

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use clap::Parser;
use platform_a2a::{
    A2aClient, PLATFORM_TOKEN_HEADER, platform_api_token, require_platform_api_token,
    validate_endpoint,
};
use platform_core::{RuntimeRegistry, WorkflowOrchestrator};
use platform_domain::{
    RuntimeHeartbeat, RuntimeRegistration, WorkflowRecord, WorkflowRequest,
    WorkflowSubmissionResponse,
};
use serde_json::json;
use tracing::info;

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
    let orchestrator = WorkflowOrchestrator::new(registry.clone(), a2a_client.clone());
    let state = AppState {
        a2a_client,
        registry,
        orchestrator,
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
</head>
<body>
  <h1>Open Agents Control Plane</h1>
  <p>The control plane is running.</p>
  <ul>
    <li><a href="/health">/health</a> - health check</li>
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
    if payload.review_rounds > 10 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "review_rounds must be <= 10" })),
        );
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
            Json(json!({ "error": error.to_string() })),
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
