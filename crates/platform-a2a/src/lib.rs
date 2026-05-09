use std::{
    collections::BTreeMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
            .rpc(endpoint, METHOD_AGENT_GET_CARD, json!({}))
            .await
            .with_context(|| format!("fetching agent card from {endpoint}"))?;
        serde_json::from_value(response).context("deserializing agent card")
    }

    pub async fn send_message(
        &self,
        endpoint: &str,
        message: Message,
    ) -> Result<SendMessageResult> {
        let response = self
            .rpc(
                endpoint,
                METHOD_MESSAGE_SEND,
                serde_json::to_value(SendMessageParams { message })?,
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
            )
            .await
            .with_context(|| format!("getting A2A task {task_id} from {endpoint}"))?;
        serde_json::from_value(response).context("deserializing A2A task")
    }

    async fn rpc(&self, endpoint: &str, method: &str, params: Value) -> Result<Value> {
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
        if body_bytes.len() > 256 * 1024 {
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
    let allowed_hosts_raw =
        env::var("PLATFORM_ALLOWED_HOSTS").unwrap_or_else(|_| "127.0.0.1,localhost".to_string());
    let allowed_hosts = allowed_hosts_raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();

    if !allowed_hosts
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(host))
    {
        return Err(anyhow!(
            "endpoint host {host} is not allowed; configure PLATFORM_ALLOWED_HOSTS to permit it"
        ));
    }

    Ok(())
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
