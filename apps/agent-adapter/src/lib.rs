//! Agent Adapter — bridges A2A protocol to external AI agent runtimes.
//!
//! Supported backends:
//! - `stdio`: Launch any CLI process and exchange prompts/results via stdin/stdout
//!   (works with: Claude Code, Codex CLI, any LLM CLI tool)
//! - `openai`: Call any OpenAI-compatible HTTP API (works with: OpenAI, Anthropic
//!   via API, Zed AI, GitHub Copilot API, any OpenAI-compatible endpoint)
//! - `claude-cli`: Convenience wrapper for `claude` CLI (same as stdio with
//!   sensible defaults for Claude Code)
//! - `codex-cli`: Convenience wrapper for Codex CLI
//!
//! The adapter registers with the control plane and presents an AgentCard
//! advertising its capabilities. When a workflow stage message arrives via A2A,
//! the adapter dispatches it to the configured backend and returns the result.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use platform_a2a::{
    A2ATask, AgentCard, AgentCardCapabilities, AgentSkill, JsonRpcRequest, JsonRpcResponse,
    METHOD_AGENT_GET_CARD, METHOD_MESSAGE_SEND, METHOD_TASKS_GET, Message, MessageRole,
    SendMessageResult, TaskState, platform_api_token, text_message,
};
use platform_domain::{Artifact, CapabilitySignal, Metadata, RuntimeDescriptor, new_id};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendConfig {
    /// Launch a subprocess and exchange via stdin/stdout.
    /// The process receives the prompt line-by-line via stdin and writes
    /// the result to stdout. Example: claude, codex, any LLM CLI.
    /// When `message_on_cli` is true, the prompt is appended as a CLI argument
    /// instead of being written to stdin (useful for tools like `opencode run`).
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        /// Environment variables to set (in addition to inherited ones).
        #[serde(default)]
        env: BTreeMap<String, String>,
        /// Max seconds to wait for the process to complete.
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
        /// If true, pass the prompt as the last CLI argument instead of stdin.
        #[serde(default)]
        message_on_cli: bool,
    },
    /// Call an OpenAI-compatible HTTP API.
    Openai {
        /// API base URL (e.g. https://api.openai.com/v1)
        base_url: String,
        /// Model name (e.g. gpt-4o, claude-sonnet-4-20250514)
        model: String,
        /// API key. Falls back to OPENAI_API_KEY env var if not set.
        api_key: Option<String>,
        /// System prompt prepended to each request.
        #[serde(default)]
        system_prompt: String,
        /// Max seconds to wait for the API response.
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
    },
    /// Convenience: Claude Code CLI.
    ClaudeCli {
        /// Additional args passed to `claude`.
        #[serde(default)]
        args: Vec<String>,
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
    },
    /// Convenience: Codex CLI.
    CodexCli {
        /// Additional args passed to `codex`.
        #[serde(default)]
        args: Vec<String>,
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
    },
}

fn default_timeout() -> u64 {
    120
}

/// Adapter state shared across HTTP handlers.
pub struct AdapterState {
    pub runtime: RuntimeDescriptor,
    pub backend: BackendConfig,
    pub tasks: Mutex<BTreeMap<String, A2ATask>>,
}

/// Handle an A2A JSON-RPC request.
pub async fn handle_a2a_request(state: &AdapterState, request: JsonRpcRequest) -> JsonRpcResponse {
    let result = match request.method.as_str() {
        METHOD_AGENT_GET_CARD => JsonRpcResponse::success(
            request.id,
            serde_json::to_value(build_agent_card(&state.runtime)).unwrap_or_default(),
        ),
        METHOD_MESSAGE_SEND => {
            let params: platform_a2a::SendMessageParams =
                match serde_json::from_value(request.params) {
                    Ok(p) => p,
                    Err(e) => return JsonRpcResponse::invalid_params(request.id, e.to_string()),
                };
            match process_message(state, params.message).await {
                Ok(result) => JsonRpcResponse::success(
                    request.id,
                    serde_json::to_value(result).unwrap_or_default(),
                ),
                Err(e) => JsonRpcResponse::internal_error(request.id, e.to_string()),
            }
        }
        METHOD_TASKS_GET => {
            let task_id = request
                .params
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match task_id {
                Some(id) => {
                    let tasks = state.tasks.lock().await;
                    match tasks.get(&id) {
                        Some(task) => JsonRpcResponse::success(
                            request.id,
                            serde_json::to_value(task).unwrap_or_default(),
                        ),
                        None => JsonRpcResponse::error(
                            request.id,
                            40401,
                            format!("task {id} not found"),
                            None,
                        ),
                    }
                }
                None => JsonRpcResponse::invalid_params(request.id, "missing task id"),
            }
        }
        _ => JsonRpcResponse::method_not_found(request.id, &request.method),
    };
    result
}

/// Validate A2A version header.
pub fn validate_a2a_version(value: Option<&str>) -> Result<(), &'static str> {
    match value {
        Some("1.0") => Ok(()),
        Some(_) => Err("unsupported A2A version"),
        None => Ok(()), // version header is optional
    }
}

/// Validate platform token.
pub fn validate_token(received: Option<&str>) -> bool {
    received == platform_api_token().as_deref()
}

fn build_agent_card(runtime: &RuntimeDescriptor) -> AgentCard {
    AgentCard {
        protocol_version: "1.0".to_string(),
        name: runtime.display_name.clone(),
        description: format!("Agent adapter — {}", runtime.runtime_id),
        endpoint: runtime.endpoint.clone(),
        skills: runtime
            .capabilities
            .iter()
            .map(|c| AgentSkill {
                id: c.name.clone(),
                name: c.name.clone(),
                description: format!("{} capability at {:.2}", c.name, c.level),
                tags: c.tags.clone(),
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

async fn process_message(state: &AdapterState, message: Message) -> Result<SendMessageResult> {
    // Extract the prompt from the message parts
    let prompt: String = message
        .parts
        .iter()
        .filter_map(|part| match part {
            platform_a2a::MessagePart::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    let workflow_role = message
        .metadata
        .get("workflow_role")
        .cloned()
        .unwrap_or_default();

    // Dispatch to the configured backend
    let content = dispatch_to_backend(&state.backend, &prompt, &message.metadata).await?;

    let artifact = Artifact {
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
        .lock()
        .await
        .insert(task.id.clone(), task.clone());
    Ok(SendMessageResult {
        task: Some(task),
        message: None,
    })
}

/// Dispatch a prompt to the configured backend and return the response text.
async fn dispatch_to_backend(
    config: &BackendConfig,
    prompt: &str,
    _metadata: &Metadata,
) -> Result<String> {
    match config {
        BackendConfig::Stdio {
            command,
            args,
            env,
            timeout_secs,
            message_on_cli,
        } => {
            if *message_on_cli {
                run_stdio_with_cli_arg(command, args, env, prompt, *timeout_secs).await
            } else {
                run_stdio(command, args, env, prompt, *timeout_secs).await
            }
        }
        BackendConfig::Openai {
            base_url,
            model,
            api_key,
            system_prompt,
            timeout_secs,
        } => {
            call_openai(
                base_url,
                model,
                api_key,
                system_prompt,
                prompt,
                *timeout_secs,
            )
            .await
        }
        BackendConfig::ClaudeCli { args, timeout_secs } => {
            run_stdio("claude", args, &BTreeMap::new(), prompt, *timeout_secs).await
        }
        BackendConfig::CodexCli { args, timeout_secs } => {
            run_stdio("codex", args, &BTreeMap::new(), prompt, *timeout_secs).await
        }
    }
}

/// Strip common CLI/tool noise from output text.
/// Removes ANSI escape sequences, terminal control characters, and trailing prompt patterns.
fn strip_cli_noise(input: &str) -> String {
    // Remove ANSI escape sequences (colors, cursor movement)
    let mut cleaned = String::with_capacity(input.len());
    let mut in_escape = false;
    for ch in input.chars() {
        if in_escape {
            if ch == 'A' || matches!(ch, 'a'..='z' | 'A'..='Z') {
                in_escape = false;
            }
            continue;
        }
        if ch == '\x1B' {
            in_escape = true;
            continue;
        }
        cleaned.push(ch);
    }
    // Remove carriage returns
    let cleaned = cleaned.replace("\r\n", "\n").replace('\r', "\n");
    // Remove trailing ">" or "$ " prompt artifacts common in CLI output
    let cleaned = cleaned.trim().trim_end_matches('>').trim_end_matches('$');
    cleaned.to_string()
}

/// Run a CLI process, send prompt to stdin, collect stdout.
async fn run_stdio(
    command: &str,
    args: &[String],
    extra_env: &BTreeMap<String, String>,
    prompt: &str,
    timeout_secs: u64,
) -> Result<String> {
    let mut cmd = tokio::process::Command::new(command);
    cmd.args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    // Inherit current env + add extra env vars
    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn process: {command}"))?;

    // Write prompt to stdin in a separate task
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to open stdin for {command}"))?;
    let prompt_owned = prompt.to_string();
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let _ = stdin.write_all(prompt_owned.as_bytes()).await;
        let _ = stdin.flush().await;
        // Close stdin to signal EOF to the process
        drop(stdin);
    });

    // Wait with timeout
    let output = tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
        .await
        .map_err(|_| anyhow!("process {command} timed out after {timeout_secs}s"))??;

    if !output.status.success() {
        return Err(anyhow!(
            "process {command} failed: {}",
            process_failure_detail(output.status, &output.stdout, &output.stderr)
        ));
    }

    let result = String::from_utf8(output.stdout)
        .map_err(|e| anyhow!("process {command} output was not valid UTF-8: {e}"))?;

    // Clean up common CLI artifacts: ANSI escapes, control chars, trailing prompts
    let cleaned = strip_cli_noise(&result);
    Ok(cleaned)
}

/// Run a CLI process, pass prompt as the last CLI argument (not stdin).
/// Useful for tools like `opencode run <message>` that read from CLI args.
async fn run_stdio_with_cli_arg(
    command: &str,
    base_args: &[String],
    extra_env: &BTreeMap<String, String>,
    prompt: &str,
    timeout_secs: u64,
) -> Result<String> {
    let mut cmd = tokio::process::Command::new(command);
    // Append prompt as the last argument
    let all_args: Vec<&str> = base_args
        .iter()
        .map(|s| s.as_str())
        .chain(std::iter::once(prompt))
        .collect();
    cmd.args(&all_args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    for (k, v) in extra_env {
        cmd.env(k, v);
    }

    let child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn process: {command}"))?;

    let output = tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output())
        .await
        .map_err(|_| anyhow!("process {command} timed out after {timeout_secs}s"))??;

    if !output.status.success() {
        return Err(anyhow!(
            "process {command} failed: {}",
            process_failure_detail(output.status, &output.stdout, &output.stderr)
        ));
    }

    let result = String::from_utf8(output.stdout)
        .map_err(|e| anyhow!("process {command} output was not valid UTF-8: {e}"))?;

    Ok(strip_cli_noise(&result))
}

fn process_failure_detail(
    status: std::process::ExitStatus,
    stdout: &[u8],
    stderr: &[u8],
) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let stderr = stderr.trim();
    if !stderr.is_empty() {
        return stderr.to_string();
    }

    let stdout = String::from_utf8_lossy(stdout);
    let stdout = stdout.trim();
    if !stdout.is_empty() {
        return stdout.to_string();
    }

    status.to_string()
}

/// Call an OpenAI-compatible chat completions API.
async fn call_openai(
    base_url: &str,
    model: &str,
    api_key: &Option<String>,
    system_prompt: &str,
    user_prompt: &str,
    timeout_secs: u64,
) -> Result<String> {
    let key = api_key
        .clone()
        .unwrap_or_else(|| std::env::var("OPENAI_API_KEY").unwrap_or_default());

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()?;

    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let mut messages = Vec::new();
    if !system_prompt.is_empty() {
        messages.push(serde_json::json!({
            "role": "system",
            "content": system_prompt
        }));
    }
    messages.push(serde_json::json!({
        "role": "user",
        "content": user_prompt
    }));

    let body = serde_json::json!({
        "model": model,
        "messages": messages,
        "max_tokens": 16384,
    });

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .context("sending LLM API request")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("LLM API returned HTTP {status}: {text}"));
    }

    let json: serde_json::Value = resp.json().await.context("parsing LLM API response")?;
    let content = json["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| anyhow!("LLM API response did not contain expected completion"))?;

    Ok(content.trim().to_string())
}

/// Build a RuntimeDescriptor with auto-detected capabilities based on the backend type.
pub fn build_runtime_descriptor(
    runtime_id: &str,
    agent_id: &str,
    endpoint: &str,
    display_name: &str,
    backend: &BackendConfig,
    trust_tier: u8,
    cost_per_task: f32,
) -> RuntimeDescriptor {
    let capabilities = backend_capabilities(backend, runtime_id);
    RuntimeDescriptor {
        runtime_id: runtime_id.to_string(),
        agent_id: agent_id.to_string(),
        display_name: display_name.to_string(),
        endpoint: endpoint.to_string(),
        profile: backend_type_name(backend),
        trust_tier,
        cost_per_task,
        capabilities,
        health: platform_domain::RuntimeHealth {
            status: platform_domain::HealthStatus::Healthy,
            availability: 0.99,
            last_heartbeat_at: Utc::now(),
        },
        load: platform_domain::RuntimeLoad::default(),
        metadata: Metadata::new(),
    }
}

fn backend_type_name(backend: &BackendConfig) -> String {
    match backend {
        BackendConfig::Stdio { command, .. } => format!("stdio:{}", command),
        BackendConfig::Openai { model, .. } => format!("openai:{}", model),
        BackendConfig::ClaudeCli { .. } => "claude-cli".to_string(),
        BackendConfig::CodexCli { .. } => "codex-cli".to_string(),
    }
}

/// Derive capability signals from the backend configuration.
/// Each backend type advertises different capabilities at different levels.
fn backend_capabilities(backend: &BackendConfig, runtime_id: &str) -> Vec<CapabilitySignal> {
    let base = |name: &str, level: f32| CapabilitySignal {
        name: name.to_string(),
        level,
        success_rate: 0.95,
        median_latency_ms: 5000,
        max_parallelism: 1,
        tags: vec![runtime_id.to_string()],
    };

    match backend {
        BackendConfig::Stdio { .. }
        | BackendConfig::ClaudeCli { .. }
        | BackendConfig::CodexCli { .. } => {
            vec![
                base("planning", 0.85),
                base("implementation", 0.90),
                base("synthesis", 0.85),
                base("review", 0.80),
            ]
        }
        BackendConfig::Openai { model, .. } => {
            // OpenAI-compatible models — capability levels based on model tier
            let lower = model.to_lowercase();
            let (planning, implementation, synthesis) = if lower.contains("claude-4")
                || lower.contains("gpt-4")
                || lower.contains("o3")
                || lower.contains("o4")
            {
                (0.90, 0.92, 0.90)
            } else if lower.contains("claude-3.5") || lower.contains("gpt-4o") {
                (0.85, 0.88, 0.85)
            } else if lower.contains("claude-3") || lower.contains("gpt-4o-mini") {
                (0.75, 0.80, 0.75)
            } else {
                (0.65, 0.70, 0.65)
            };
            vec![
                base("planning", planning),
                base("implementation", implementation),
                base("synthesis", synthesis),
                base("review", implementation * 0.9),
            ]
        }
    }
}
