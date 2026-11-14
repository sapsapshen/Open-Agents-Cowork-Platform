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

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use platform_a2a::{
    A2ATask, A2aClient, AgentCard, AgentCardCapabilities, AgentSkill, JsonRpcRequest,
    JsonRpcResponse, METHOD_AGENT_GET_CARD, METHOD_MESSAGE_SEND, METHOD_TASKS_GET, Message,
    MessageRole, SendMessageResult, TaskState, platform_api_token, text_message,
};
use platform_domain::{
    Artifact, CapabilitySignal, Metadata, RuntimeDescriptor, RuntimeLoad, new_id,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const MAX_STORED_TASKS: usize = 128;
const CONSULT_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Bounded task history keyed by insertion order.
pub struct TaskBook {
    by_id: BTreeMap<String, A2ATask>,
    order: VecDeque<String>,
}

impl TaskBook {
    pub fn new() -> Self {
        Self {
            by_id: BTreeMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn insert(&mut self, task: A2ATask) {
        let id = task.id.clone();
        if self.by_id.insert(id.clone(), task).is_none() {
            self.order.push_back(id);
        }
        while self.order.len() > MAX_STORED_TASKS {
            if let Some(old) = self.order.pop_front() {
                self.by_id.remove(&old);
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<&A2ATask> {
        self.by_id.get(id)
    }
}

impl Default for TaskBook {
    fn default() -> Self {
        Self::new()
    }
}

/// Adapter state shared across HTTP handlers.
pub struct AdapterState {
    pub runtime: RuntimeDescriptor,
    pub backend: BackendConfig,
    pub tasks: Mutex<TaskBook>,
    pub inflight: AtomicU32,
}

impl AdapterState {
    pub fn active_tasks(&self) -> u32 {
        self.inflight.load(Ordering::SeqCst)
    }

    fn try_acquire(&self) -> bool {
        let max_tasks = self.runtime.load.max_tasks.max(1);
        self.inflight
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current >= max_tasks {
                    None
                } else {
                    Some(current + 1)
                }
            })
            .is_ok()
    }
}

struct InflightGuard<'a> {
    counter: &'a AtomicU32,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Handle an A2A JSON-RPC request.
pub async fn handle_a2a_request(state: &AdapterState, request: JsonRpcRequest) -> JsonRpcResponse {
    match request.method.as_str() {
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
    }
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
    if !state.try_acquire() {
        return Err(anyhow!(
            "runtime {} is at capacity ({} active task(s))",
            state.runtime.runtime_id,
            state.active_tasks()
        ));
    }
    let _guard = InflightGuard {
        counter: &state.inflight,
    };

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
    let prompt = augment_prompt_with_consults(&prompt, &message.metadata).await;

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
        history: vec![truncate_message_for_history(message), response_message],
        artifacts: vec![artifact],
    };

    let mut result = SendMessageResult {
        task: Some(task),
        message: None,
    };
    platform_a2a::constrain_send_result(&mut result);
    if let Some(task) = result.task.clone() {
        state.tasks.lock().await.insert(task);
    }
    Ok(result)
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
            let args = prepare_claude_args(args);
            run_stdio("claude", &args, &BTreeMap::new(), prompt, *timeout_secs).await
        }
        BackendConfig::CodexCli { args, timeout_secs } => {
            let args = prepare_codex_args(args);
            run_stdio("codex", &args, &BTreeMap::new(), prompt, *timeout_secs).await
        }
    }
}

/// Non-interactive Claude Code invocation. `-p` reads the prompt from stdin.
pub fn prepare_claude_args(extra: &[String]) -> Vec<String> {
    let mut args = Vec::new();
    if !extra.iter().any(|arg| arg == "-p" || arg == "--print") {
        args.push("-p".to_string());
    }
    if !extra.iter().any(|arg| arg == "--output-format") {
        args.push("--output-format".to_string());
        args.push("text".to_string());
    }
    args.extend(extra.iter().cloned());
    args
}

/// Non-interactive Codex CLI invocation. `exec` reads the prompt from stdin.
pub fn prepare_codex_args(extra: &[String]) -> Vec<String> {
    let mut args = Vec::new();
    if !extra.iter().any(|arg| arg == "exec") {
        args.push("exec".to_string());
    }
    args.extend(extra.iter().cloned());
    args
}

fn shared_http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client")
    })
}

/// Execute and revise stages ask the listed peers for advice and fold it into
/// the model prompt. A peer that is down does not fail the stage.
async fn augment_prompt_with_consults(prompt: &str, metadata: &Metadata) -> String {
    let role = metadata
        .get("workflow_role")
        .map(String::as_str)
        .unwrap_or("");
    if !matches!(role, "execute" | "revise") {
        return prompt.to_string();
    }
    let depth = metadata
        .get("consultation_depth")
        .and_then(|value| value.parse::<u8>().ok())
        .unwrap_or(0);
    if depth >= 1 {
        return prompt.to_string();
    }

    let collaborators = collaborator_targets(metadata);
    if collaborators.is_empty() {
        return prompt.to_string();
    }

    let client = A2aClient::new(shared_http().clone());
    let mut notes = Vec::new();
    for collaborator in collaborators.into_iter().take(2) {
        match consult_peer(&client, metadata, &collaborator).await {
            Ok(advice) => notes.push(format!("- {}: {}", collaborator.runtime_id, advice)),
            Err(error) => notes.push(format!(
                "- {} unavailable: {}",
                collaborator.runtime_id, error
            )),
        }
    }

    format!("{prompt}\n\nPeer consultation notes:\n{}", notes.join("\n"))
}

fn collaborator_targets(metadata: &Metadata) -> Vec<platform_domain::RuntimeDescriptor> {
    let Some(raw) = metadata.get("collaborators") else {
        return Vec::new();
    };
    if raw.is_empty() || raw.len() > 64 * 1024 {
        return Vec::new();
    }
    serde_json::from_str(raw).unwrap_or_default()
}

async fn consult_peer(
    client: &A2aClient,
    metadata: &Metadata,
    collaborator: &platform_domain::RuntimeDescriptor,
) -> Result<String> {
    let objective = metadata
        .get("objective")
        .cloned()
        .unwrap_or_else(|| "unspecified".to_string());
    let mut consult_message = text_message(
        new_id("msg"),
        MessageRole::User,
        None,
        metadata.get("workflow_id").cloned(),
        format!("Collaborator request. Objective: {objective}. Provide 3 concise recommendations."),
    );
    consult_message
        .metadata
        .insert("workflow_role".to_string(), "consult".to_string());
    consult_message
        .metadata
        .insert("consultation_depth".to_string(), "1".to_string());

    let response = client
        .send_message_with_timeout(&collaborator.endpoint, consult_message, CONSULT_TIMEOUT)
        .await?;
    let task = response
        .task
        .ok_or_else(|| anyhow!("collaborator {} returned no task", collaborator.runtime_id))?;
    let artifact = task.artifacts.first().ok_or_else(|| {
        anyhow!(
            "collaborator {} returned no artifact",
            collaborator.runtime_id
        )
    })?;
    let mut advice = artifact.content.trim().to_string();
    if advice.chars().count() > 2_000 {
        advice = advice.chars().take(2_000).collect();
        advice.push_str("...");
    }
    Ok(advice)
}

/// Strip common CLI/tool noise from output text.
/// Removes ANSI escape sequences, terminal control characters, and trailing prompt patterns.
fn strip_cli_noise(input: &str) -> String {
    // Remove ANSI escape sequences (colors, cursor movement)
    let mut cleaned = String::with_capacity(input.len());
    let mut in_escape = false;
    for ch in input.chars() {
        if in_escape {
            if ch.is_ascii_alphabetic() {
                in_escape = false;
            }
            continue;
        }
        if ch == '\u{1b}' {
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
        .kill_on_drop(true)
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
        .kill_on_drop(true)
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

    let primary_field = completion_limit_field(model);
    let resp = send_chat_completion(&client, &url, &key, model, &messages, primary_field).await?;
    if resp.status().as_u16() == 400 {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let alternate = if primary_field == "max_tokens" {
            "max_completion_tokens"
        } else {
            "max_tokens"
        };
        let lowered = text.to_ascii_lowercase();
        if lowered.contains("max_tokens") || lowered.contains("max_completion_tokens") {
            let retry =
                send_chat_completion(&client, &url, &key, model, &messages, alternate).await?;
            return completion_text(retry).await;
        }
        return Err(anyhow!(
            "LLM API returned HTTP {status}: {}",
            truncate_chars(&text, 500)
        ));
    }

    completion_text(resp).await
}

fn completion_limit_field(model: &str) -> &'static str {
    let lower = model.to_ascii_lowercase();
    if lower.starts_with("o1")
        || lower.starts_with("o3")
        || lower.starts_with("o4")
        || lower.contains("gpt-5")
        || lower.contains("gpt-4.1")
        || lower.contains("gpt-4.5")
    {
        "max_completion_tokens"
    } else {
        "max_tokens"
    }
}

async fn send_chat_completion(
    client: &reqwest::Client,
    url: &str,
    key: &str,
    model: &str,
    messages: &[serde_json::Value],
    token_field: &str,
) -> Result<reqwest::Response> {
    let mut body = serde_json::json!({
        "model": model,
        "messages": messages,
    });
    body[token_field] = serde_json::json!(16384);
    client
        .post(url)
        .header("Authorization", format!("Bearer {key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .context("sending LLM API request")
}

async fn completion_text(resp: reqwest::Response) -> Result<String> {
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "LLM API returned HTTP {status}: {}",
            truncate_chars(&text, 500)
        ));
    }

    let json: serde_json::Value = resp.json().await.context("parsing LLM API response")?;
    let content = json["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| anyhow!("LLM API response did not contain expected completion"))?;

    Ok(content.trim().to_string())
}

fn truncate_message_for_history(mut message: Message) -> Message {
    for part in &mut message.parts {
        if let platform_a2a::MessagePart::Text { text } = part {
            *text = truncate_chars(text, 4_000);
        }
    }
    message
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated: String = value.chars().take(max_chars).collect();
    truncated.push_str("...");
    truncated
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
    let max_tasks = capabilities
        .iter()
        .map(|capability| capability.max_parallelism)
        .max()
        .unwrap_or(1)
        .max(1);
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
        load: RuntimeLoad {
            max_tasks,
            ..RuntimeLoad::default()
        },
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
    let signal = |name: &str, level: f32, parallelism: u32| CapabilitySignal {
        name: name.to_string(),
        level,
        success_rate: 0.95,
        median_latency_ms: 5000,
        max_parallelism: parallelism,
        tags: vec![runtime_id.to_string()],
    };

    match backend {
        BackendConfig::Stdio { .. }
        | BackendConfig::ClaudeCli { .. }
        | BackendConfig::CodexCli { .. } => {
            vec![
                signal("planning", 0.85, 1),
                signal("implementation", 0.90, 1),
                signal("synthesis", 0.85, 1),
                signal("review", 0.80, 1),
            ]
        }
        BackendConfig::Openai { model, .. } => {
            let (planning, implementation, synthesis) = model_capability_levels(model);
            vec![
                signal("planning", planning, 4),
                signal("implementation", implementation, 4),
                signal("synthesis", synthesis, 4),
                signal("review", implementation * 0.9, 4),
            ]
        }
    }
}

/// Map a model name onto planning / implementation / synthesis levels.
/// Smaller model markers are checked first so names such as `gpt-4o-mini`
/// are not scored as frontier models.
pub fn model_capability_levels(model: &str) -> (f32, f32, f32) {
    let lower = model.to_ascii_lowercase();
    if contains_any(
        &lower,
        &["mini", "nano", "haiku", "flash", "-small", "small"],
    ) {
        (0.75, 0.80, 0.75)
    } else if contains_any(
        &lower,
        &[
            "gpt-5",
            "gpt-4.1",
            "gpt-4.5",
            "o1",
            "o3",
            "o4",
            "claude-opus",
            "claude-sonnet-4",
            "claude-4",
            "claude-3.7",
            "claude-3-7",
            "opus",
        ],
    ) {
        (0.90, 0.92, 0.90)
    } else if contains_any(
        &lower,
        &[
            "gpt-4o",
            "gpt-4",
            "claude-3.5",
            "claude-3-5",
            "claude-3",
            "sonnet",
        ],
    ) {
        (0.85, 0.88, 0.85)
    } else {
        (0.65, 0.70, 0.65)
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use platform_domain::Metadata;

    use super::*;

    #[test]
    fn claude_and_codex_args_are_non_interactive() {
        assert_eq!(
            prepare_claude_args(&[]),
            vec!["-p", "--output-format", "text"]
        );
        assert_eq!(
            prepare_claude_args(&["--verbose".to_string()]),
            vec!["-p", "--output-format", "text", "--verbose"]
        );
        assert_eq!(
            prepare_claude_args(&[
                "-p".to_string(),
                "--output-format".to_string(),
                "json".to_string()
            ]),
            vec!["-p", "--output-format", "json"]
        );
        assert_eq!(prepare_codex_args(&[]), vec!["exec"]);
        assert_eq!(
            prepare_codex_args(&[
                "exec".to_string(),
                "--sandbox".to_string(),
                "read-only".to_string()
            ]),
            vec!["exec", "--sandbox", "read-only"]
        );
    }

    #[test]
    fn model_tiers_do_not_promote_mini_models() {
        assert_eq!(model_capability_levels("gpt-4o-mini").1, 0.80);
        assert_eq!(model_capability_levels("gpt-4o").1, 0.88);
        assert_eq!(model_capability_levels("gpt-5").1, 0.92);
        assert_eq!(model_capability_levels("claude-sonnet-4-5").1, 0.92);
        assert_eq!(model_capability_levels("o3").1, 0.92);
        assert_eq!(model_capability_levels("llama3").1, 0.70);
    }

    #[test]
    fn openai_descriptor_allows_more_parallelism_than_cli() {
        let cli = build_runtime_descriptor(
            "cli",
            "cli",
            "http://127.0.0.1:1/a2a",
            "cli",
            &BackendConfig::ClaudeCli {
                args: vec![],
                timeout_secs: 30,
            },
            1,
            1.0,
        );
        let api = build_runtime_descriptor(
            "api",
            "api",
            "http://127.0.0.1:1/a2a",
            "api",
            &BackendConfig::Openai {
                base_url: "http://127.0.0.1:11434/v1".to_string(),
                model: "llama3".to_string(),
                api_key: None,
                system_prompt: String::new(),
                timeout_secs: 30,
            },
            1,
            1.0,
        );
        assert_eq!(cli.load.max_tasks, 1);
        assert_eq!(api.load.max_tasks, 4);
    }

    #[test]
    fn adapter_rejects_work_past_capacity() {
        let state = AdapterState {
            runtime: build_runtime_descriptor(
                "cli",
                "cli",
                "http://127.0.0.1:1/a2a",
                "cli",
                &BackendConfig::Stdio {
                    command: "cat".to_string(),
                    args: vec![],
                    env: BTreeMap::new(),
                    timeout_secs: 5,
                    message_on_cli: false,
                },
                1,
                1.0,
            ),
            backend: BackendConfig::Stdio {
                command: "cat".to_string(),
                args: vec![],
                env: BTreeMap::new(),
                timeout_secs: 5,
                message_on_cli: false,
            },
            tasks: Mutex::new(TaskBook::new()),
            inflight: AtomicU32::new(0),
        };
        assert!(state.try_acquire());
        assert!(!state.try_acquire());
        assert_eq!(state.active_tasks(), 1);
    }

    #[test]
    fn completion_limit_field_follows_current_model_apis() {
        assert_eq!(completion_limit_field("llama3"), "max_tokens");
        assert_eq!(completion_limit_field("gpt-4o"), "max_tokens");
        assert_eq!(completion_limit_field("gpt-5"), "max_completion_tokens");
        assert_eq!(completion_limit_field("o3"), "max_completion_tokens");
    }

    #[tokio::test]
    async fn execute_stage_includes_peer_advice() {
        unsafe {
            std::env::set_var("PLATFORM_API_TOKEN", "test-token");
        }

        let app = axum::Router::new().route(
            "/a2a",
            axum::routing::post(
                |axum::Json(request): axum::Json<JsonRpcRequest>| async move {
                    let params: platform_a2a::SendMessageParams =
                        serde_json::from_value(request.params).expect("params");
                    axum::Json(JsonRpcResponse::success(
                        request.id,
                        serde_json::to_value(SendMessageResult {
                            task: Some(A2ATask {
                                id: "task-peer".to_string(),
                                context_id: params.message.context_id,
                                status: TaskState::Completed,
                                history: vec![],
                                artifacts: vec![Artifact {
                                    artifact_id: "artifact-peer".to_string(),
                                    name: "consult.md".to_string(),
                                    mime_type: "text/markdown".to_string(),
                                    content: "Peer says validate the input.".to_string(),
                                }],
                            }),
                            message: None,
                        })
                        .expect("result"),
                    ))
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("peer server");
        });

        let peer = build_runtime_descriptor(
            "peer",
            "peer",
            &format!("http://{addr}/a2a"),
            "peer",
            &BackendConfig::ClaudeCli {
                args: vec![],
                timeout_secs: 5,
            },
            1,
            1.0,
        );
        let state = AdapterState {
            runtime: build_runtime_descriptor(
                "local",
                "local",
                "http://127.0.0.1:9/a2a",
                "local",
                &BackendConfig::Stdio {
                    command: "cat".to_string(),
                    args: vec![],
                    env: BTreeMap::new(),
                    timeout_secs: 5,
                    message_on_cli: false,
                },
                1,
                1.0,
            ),
            backend: BackendConfig::Stdio {
                command: "cat".to_string(),
                args: vec![],
                env: BTreeMap::new(),
                timeout_secs: 5,
                message_on_cli: false,
            },
            tasks: Mutex::new(TaskBook::new()),
            inflight: AtomicU32::new(0),
        };
        let mut metadata = Metadata::new();
        metadata.insert("workflow_role".to_string(), "execute".to_string());
        metadata.insert("objective".to_string(), "Ship it".to_string());
        metadata.insert(
            "collaborators".to_string(),
            serde_json::to_string(&vec![peer]).expect("collaborators"),
        );
        let mut message = text_message(
            "msg-1",
            MessageRole::User,
            None,
            Some("ctx-1".to_string()),
            "Do the work",
        );
        message.metadata = metadata;

        let result = process_message(&state, message).await.expect("processed");
        let content = result.task.expect("task").artifacts[0].content.clone();
        assert!(content.contains("Peer says validate the input."));
        assert!(content.contains("Do the work"));
        assert_eq!(state.active_tasks(), 0);
    }

    #[test]
    fn task_book_forgets_tasks_beyond_the_cap() {
        let mut book = TaskBook::new();
        for index in 0..=MAX_STORED_TASKS {
            book.insert(A2ATask {
                id: format!("task-{index:04}"),
                context_id: None,
                status: TaskState::Completed,
                history: vec![],
                artifacts: vec![],
            });
        }
        assert_eq!(book.order.len(), MAX_STORED_TASKS);
        assert!(book.get("task-0000").is_none());
        assert!(book.get(&format!("task-{MAX_STORED_TASKS:04}")).is_some());
    }
}
