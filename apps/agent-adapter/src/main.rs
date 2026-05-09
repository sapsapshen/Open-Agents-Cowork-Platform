//! agent-adapter — A2A-compliant bridge to external AI agent runtimes.
//!
//! Start with:
//!   cargo run -p agent-adapter -- \
//!     --runtime-id my-agent \
//!     --backend '{"type":"claude_cli"}' \
//!     --control-plane http://127.0.0.1:9000 \
//!     --auto-register
//!
//! Backend config is JSON passed via --backend:
//!   type: "stdio"     → run a CLI process (claude, codex, etc.)
//!   type: "openai"    → call any OpenAI-compatible API
//!   type: "claude_cli" → run `claude` CLI
//!   type: "codex_cli"  → run `codex` CLI

use std::net::SocketAddr;

use agent_adapter::{
    AdapterState, BackendConfig, build_runtime_descriptor, handle_a2a_request,
    validate_a2a_version, validate_token,
};
use anyhow::{Context, Result, anyhow};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
};
use clap::Parser;
use platform_a2a::{
    JsonRpcResponse, PLATFORM_TOKEN_HEADER, require_platform_api_token, validate_endpoint,
};
use platform_domain::{Metadata, RuntimeDescriptor, RuntimeRegistration};
use serde_json::{Value, json};
use tokio::time::Duration;
use tracing::info;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9201")]
    bind: SocketAddr,
    #[arg(long)]
    public_endpoint: Option<String>,
    #[arg(long)]
    control_plane: Option<String>,
    #[arg(long, default_value = "agent-1")]
    runtime_id: String,
    #[arg(long, default_value = "agent-1")]
    agent_id: String,
    #[arg(long, default_value = "")]
    display_name: String,
    /// Backend configuration as JSON. See BackendConfig enum.
    #[arg(long, default_value = r#"{"type":"claude_cli"}"#)]
    backend: String,
    #[arg(long, default_value = "2")]
    trust_tier: u8,
    #[arg(long, default_value = "1.0")]
    cost_per_task: f32,
    #[arg(long, default_value_t = false)]
    auto_register: bool,
}

#[derive(Clone)]
struct AppState {
    adapter: std::sync::Arc<AdapterState>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    require_platform_api_token()?;

    let backend: BackendConfig = serde_json::from_str(&args.backend)
        .with_context(|| format!("invalid --backend JSON: {}", args.backend))?;

    let display_name = if args.display_name.is_empty() {
        format!("agent-{}", &args.runtime_id)
    } else {
        args.display_name.clone()
    };

    // Bind first so we know the actual port
    info!("agent-adapter listening on {}", args.bind);
    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    let local_addr = listener.local_addr()?;

    // Resolve the endpoint with the actual port (critical when bind is ":0")
    let endpoint = args
        .public_endpoint
        .clone()
        .unwrap_or_else(|| format!("http://{}:{}/a2a", local_addr.ip(), local_addr.port()));

    let runtime = build_runtime_descriptor(
        &args.runtime_id,
        &args.agent_id,
        &endpoint,
        &display_name,
        &backend,
        args.trust_tier,
        args.cost_per_task,
    );

    let adapter = std::sync::Arc::new(AdapterState {
        runtime: runtime.clone(),
        backend,
        tasks: tokio::sync::Mutex::new(std::collections::BTreeMap::new()),
    });

    let state = AppState {
        adapter: adapter.clone(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/agent-card", get(agent_card_endpoint))
        .route("/a2a", axum::routing::post(handle_a2a))
        .with_state(state);

    if args.auto_register {
        let control_plane = args.control_plane.clone();
        let runtime_clone = runtime.clone();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()?;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut backoff = Duration::from_millis(500);
            loop {
                match auto_register(control_plane.as_deref(), &client, runtime_clone.clone()).await
                {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::warn!(%error, runtime_id = %runtime_clone.runtime_id, "auto-registration failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_secs(5));
                    }
                }
            }
            // Heartbeat loop
            if let Some(cp) = control_plane {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    if let Err(e) = send_heartbeat(&client, &cp, &runtime_clone.runtime_id).await {
                        tracing::warn!(%e, "heartbeat failed");
                    }
                }
            }
        });
    }

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": "agent-adapter" }))
}

async fn agent_card_endpoint(State(state): State<AppState>) -> Json<platform_a2a::AgentCard> {
    Json(platform_a2a::AgentCard {
        protocol_version: "1.0".to_string(),
        name: state.adapter.runtime.display_name.clone(),
        description: format!("Agent adapter — {}", state.adapter.runtime.runtime_id),
        endpoint: state.adapter.runtime.endpoint.clone(),
        skills: state
            .adapter
            .runtime
            .capabilities
            .iter()
            .map(|c| platform_a2a::AgentSkill {
                id: c.name.clone(),
                name: c.name.clone(),
                description: format!("{} capability at {:.2}", c.name, c.level),
                tags: c.tags.clone(),
            })
            .collect(),
        capabilities: platform_a2a::AgentCardCapabilities {
            streaming: false,
            push_notifications: false,
            multi_turn: true,
            direct_collaboration: true,
        },
        default_input_modes: vec!["text/plain".to_string(), "application/json".to_string()],
        default_output_modes: vec!["text/markdown".to_string(), "application/json".to_string()],
        metadata: Metadata::new(),
    })
}

async fn handle_a2a(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(request): Json<platform_a2a::JsonRpcRequest>,
) -> (StatusCode, Json<JsonRpcResponse>) {
    // Validate token
    let received = headers
        .get(PLATFORM_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());
    if !validate_token(received) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(JsonRpcResponse::error(
                json!(null),
                40101,
                "missing or invalid platform token",
                None,
            )),
        );
    }

    // Validate A2A version
    let version = headers.get("A2A-Version").and_then(|v| v.to_str().ok());
    if let Err(msg) = validate_a2a_version(version) {
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse::error(request.id, 40010, msg, None)),
        );
    }

    let response = handle_a2a_request(&state.adapter, request).await;
    (StatusCode::OK, Json(response))
}

async fn auto_register(
    control_plane: Option<&str>,
    client: &reqwest::Client,
    runtime: RuntimeDescriptor,
) -> Result<()> {
    let cp = control_plane
        .ok_or_else(|| anyhow!("--control-plane is required when --auto-register is enabled"))?;
    validate_endpoint(cp)?;
    let registration = RuntimeRegistration { runtime };
    client
        .post(format!("{cp}/runtimes/register"))
        .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
        .json(&registration)
        .send()
        .await
        .context("registering with control plane")?
        .error_for_status()
        .context("control plane rejected registration")?;
    Ok(())
}

async fn send_heartbeat(
    client: &reqwest::Client,
    control_plane: &str,
    runtime_id: &str,
) -> Result<()> {
    validate_endpoint(control_plane)?;
    client
        .post(format!("{control_plane}/runtimes/heartbeat"))
        .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
        .json(&serde_json::json!({
            "runtime_id": runtime_id,
            "active_tasks": 0,
            "queued_tasks": 0,
            "availability": 0.99
        }))
        .send()
        .await
        .context("sending heartbeat")?
        .error_for_status()
        .context("control plane rejected heartbeat")?;
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
