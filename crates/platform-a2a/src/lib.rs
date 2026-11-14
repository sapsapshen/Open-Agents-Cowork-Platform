use std::{
    collections::BTreeMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use platform_domain::{Artifact, Metadata};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

pub const A2A_VERSION: &str = "1.0";
pub const METHOD_AGENT_GET_CARD: &str = "agent/getCard";
pub const METHOD_MESSAGE_SEND: &str = "message/send";
pub const METHOD_TASKS_GET: &str = "tasks/get";
pub const PLATFORM_TOKEN_HEADER: &str = "x-platform-token";
/// Upper bound for one JSON-RPC call. Stage orchestration uses the same budget.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard cap checked by the A2A client on a JSON-RPC response body.
pub const MAX_JSON_RPC_BODY_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentCardCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
    pub multi_turn: bool,
    pub direct_collaboration: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentCard {
    pub protocol_version: String,
    pub name: String,
    pub description: String,
    pub endpoint: String,
    pub skills: Vec<AgentSkill>,
    pub capabilities: AgentCardCapabilities,
    pub default_input_modes: Vec<String>,
    pub default_output_modes: Vec<String>,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Agent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessagePart {
    Text { text: String },
    Json { value: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub message_id: String,
    pub role: MessageRole,
    pub task_id: Option<String>,
    pub context_id: Option<String>,
    pub parts: Vec<MessagePart>,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct A2ATask {
    pub id: String,
    pub context_id: Option<String>,
    pub status: TaskState,
    pub history: Vec<Message>,
    pub artifacts: Vec<Artifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SendMessageParams {
    pub message: Message,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GetTaskParams {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SendMessageResult {
    pub task: Option<A2ATask>,
    pub message: Option<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn invalid_params(id: Value, message: impl Into<String>) -> Self {
        Self::error(id, -32602, message, None)
    }

    pub fn method_not_found(id: Value, method: impl Into<String>) -> Self {
        Self::error(
            id,
            -32601,
            format!("method {} is not supported", method.into()),
            None,
        )
    }

    pub fn internal_error(id: Value, message: impl Into<String>) -> Self {
        Self::error(id, -32603, message, None)
    }

    pub fn error(id: Value, code: i32, message: impl Into<String>, data: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct A2aClient {
    http: reqwest::Client,
    request_counter: Arc<AtomicU64>,
}

impl A2aClient {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            http,
            request_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn get_agent_card(&self, endpoint: &str) -> Result<AgentCard> {
        let response = self
            .rpc(
                endpoint,
                METHOD_AGENT_GET_CARD,
                json!({}),
                DEFAULT_RPC_TIMEOUT,
            )
            .await
            .with_context(|| format!("fetching agent card from {endpoint}"))?;
        serde_json::from_value(response).context("deserializing agent card")
    }

    pub async fn send_message(
        &self,
        endpoint: &str,
        message: Message,
    ) -> Result<SendMessageResult> {
        self.send_message_with_timeout(endpoint, message, DEFAULT_RPC_TIMEOUT)
            .await
    }

    pub async fn send_message_with_timeout(
        &self,
        endpoint: &str,
        message: Message,
        timeout: Duration,
    ) -> Result<SendMessageResult> {
        let response = self
            .rpc(
                endpoint,
                METHOD_MESSAGE_SEND,
                serde_json::to_value(SendMessageParams { message })?,
                timeout,
            )
            .await
            .with_context(|| format!("sending A2A message to {endpoint}"))?;
        serde_json::from_value(response).context("deserializing sendMessage result")
    }

    pub async fn get_task(&self, endpoint: &str, task_id: &str) -> Result<A2ATask> {
        let response = self
            .rpc(
                endpoint,
                METHOD_TASKS_GET,
                serde_json::to_value(GetTaskParams {
                    id: task_id.to_string(),
                })?,
                DEFAULT_RPC_TIMEOUT,
            )
            .await
            .with_context(|| format!("getting A2A task {task_id} from {endpoint}"))?;
        serde_json::from_value(response).context("deserializing A2A task")
    }

    async fn rpc(
        &self,
        endpoint: &str,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value> {
        validate_endpoint(endpoint)?;
        let payload = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(self.request_counter.fetch_add(1, Ordering::Relaxed)),
            method: method.to_string(),
            params,
        };
        let response = self
            .http
            .post(endpoint)
            .timeout(timeout)
            .header(PLATFORM_TOKEN_HEADER, require_platform_api_token()?)
            .header("A2A-Version", A2A_VERSION)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("sending JSON-RPC request to {endpoint}"))?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "unexpected HTTP status {} from {endpoint} calling method {method}",
                response.status()
            ));
        }

        let body_bytes = response
            .bytes()
            .await
            .context("reading JSON-RPC response body")?;
        if body_bytes.len() > MAX_JSON_RPC_BODY_BYTES {
            return Err(anyhow!(
                "JSON-RPC response from {endpoint} exceeded 256 KiB limit"
            ));
        }
        let body: JsonRpcResponse =
            serde_json::from_slice(&body_bytes).context("deserializing JSON-RPC response")?;
        if let Some(error) = body.error {
            return Err(anyhow!("rpc error {}: {}", error.code, error.message));
        }

        body.result
            .ok_or_else(|| anyhow!("missing JSON-RPC result in successful response"))
    }
}

pub fn platform_api_token() -> Option<String> {
    env::var("PLATFORM_API_TOKEN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub fn require_platform_api_token() -> Result<String> {
    platform_api_token().ok_or_else(|| anyhow!("PLATFORM_API_TOKEN must be set"))
}

pub fn validate_endpoint(endpoint: &str) -> Result<()> {
    let url = Url::parse(endpoint).context("invalid endpoint URL")?;
    match url.scheme() {
        "http" | "https" => {}
        other => return Err(anyhow!("unsupported endpoint scheme {other}")),
    }

    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("endpoint host is required"))?;
    let allowed_hosts_raw = env::var("PLATFORM_ALLOWED_HOSTS")
        .unwrap_or_else(|_| "127.0.0.1,localhost,::1".to_string());
    let allowed_hosts = allowed_hosts_raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();

    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    if !allowed_hosts.iter().any(|allowed| {
        let allowed = allowed
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(allowed);
        allowed.eq_ignore_ascii_case(host)
    }) {
        return Err(anyhow!(
            "endpoint host {host} is not allowed; configure PLATFORM_ALLOWED_HOSTS to permit it"
        ));
    }

    Ok(())
}

/// Drop bulky history metadata and shrink the largest text until a send
/// result serializes under the JSON-RPC body cap.
pub fn constrain_send_result(result: &mut SendMessageResult) {
    if let Some(task) = result.task.as_mut() {
        for message in &mut task.history {
            message.metadata.clear();
            for part in &mut message.parts {
                if let MessagePart::Json { .. } = part {
                    *part = MessagePart::Text {
                        text: "[json omitted]".to_string(),
                    };
                }
            }
        }
    }

    let budget = MAX_JSON_RPC_BODY_BYTES.saturating_sub(2 * 1024);
    for _ in 0..24 {
        let size = serde_json::to_vec(result)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        if size <= budget || !shrink_largest_text(result) {
            return;
        }
    }
}

fn shrink_largest_text(result: &mut SendMessageResult) -> bool {
    let Some(task) = result.task.as_mut() else {
        return false;
    };
    let mut best_len = 0usize;
    let mut best_artifact: Option<usize> = None;
    let mut best_history: Option<(usize, usize)> = None;
    for (index, artifact) in task.artifacts.iter().enumerate() {
        if artifact.content.len() > best_len {
            best_len = artifact.content.len();
            best_artifact = Some(index);
            best_history = None;
        }
    }
    for (message_index, message) in task.history.iter().enumerate() {
        for (part_index, part) in message.parts.iter().enumerate() {
            if let MessagePart::Text { text } = part
                && text.len() > best_len
            {
                best_len = text.len();
                best_artifact = None;
                best_history = Some((message_index, part_index));
            }
        }
    }
    if best_len <= 64 {
        return false;
    }
    let keep = best_len / 2;
    let shrink = |text: &mut String| {
        let mut end = keep.min(text.len());
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("\n...[truncated]");
    };
    if let Some(index) = best_artifact {
        shrink(&mut task.artifacts[index].content);
        return true;
    }
    if let Some((message_index, part_index)) = best_history
        && let MessagePart::Text { text } = &mut task.history[message_index].parts[part_index]
    {
        shrink(text);
        return true;
    }
    false
}

pub fn text_message(
    message_id: impl Into<String>,
    role: MessageRole,
    task_id: Option<String>,
    context_id: Option<String>,
    text: impl Into<String>,
) -> Message {
    Message {
        message_id: message_id.into(),
        role,
        task_id,
        context_id,
        parts: vec![MessagePart::Text { text: text.into() }],
        metadata: BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use platform_domain::Artifact;

    use super::{
        A2ATask, MAX_JSON_RPC_BODY_BYTES, MessagePart, MessageRole, SendMessageResult, TaskState,
        constrain_send_result, text_message, validate_endpoint,
    };

    #[test]
    fn endpoint_validation_allows_loopback_and_rejects_other_hosts() {
        validate_endpoint("http://127.0.0.1:9000/a2a").expect("ipv4 loopback");
        validate_endpoint("http://localhost:9201/a2a").expect("localhost");
        validate_endpoint("http://[::1]:9000/a2a").expect("ipv6 loopback");

        let error = validate_endpoint("http://example.com/a2a").expect_err("public host");
        assert!(error.to_string().contains("not allowed"));

        let error = validate_endpoint("ftp://127.0.0.1/a2a").expect_err("scheme");
        assert!(error.to_string().contains("unsupported endpoint scheme"));
    }

    #[test]
    fn send_result_stays_under_the_json_rpc_body_cap() {
        let mut history = text_message("msg-1", MessageRole::User, None, None, "prompt");
        history
            .metadata
            .insert("constraints".to_string(), "约束".repeat(80_000));
        history.parts.push(MessagePart::Json {
            value: serde_json::json!({"blob": "x".repeat(80_000)}),
        });
        let mut result = SendMessageResult {
            task: Some(A2ATask {
                id: "task-1".to_string(),
                context_id: None,
                status: TaskState::Completed,
                history: vec![history],
                artifacts: vec![Artifact {
                    artifact_id: "artifact-1".to_string(),
                    name: "out.md".to_string(),
                    mime_type: "text/markdown".to_string(),
                    content: "y".repeat(400_000),
                }],
            }),
            message: None,
        };

        constrain_send_result(&mut result);

        let encoded = serde_json::to_vec(&result).expect("result encodes");
        assert!(encoded.len() <= MAX_JSON_RPC_BODY_BYTES - 2 * 1024);
        let task = result.task.expect("task remains");
        assert!(task.history[0].metadata.is_empty());
        assert!(task.artifacts[0].content.contains("[truncated]"));
    }
}
