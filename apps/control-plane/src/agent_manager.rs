use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use platform_core::RuntimeRegistry;
use platform_domain::AgentConfig;
use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};
use tracing::{error, info, warn};

/// Represents a system-detected AI tool that can be added as an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedAgent {
    pub name: String,
    pub backend_type: String,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub description: String,
    pub already_configured: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentState {
    config: AgentConfig,
    pid: Option<u32>,
    status: String, // "stopped", "running", "error"
}

#[derive(Clone)]
pub struct AgentManager {
    inner: Arc<RwLock<BTreeMap<String, AgentState>>>,
    registry: RuntimeRegistry,
    data_dir: PathBuf,
    control_plane_url: String,
}

impl AgentManager {
    pub fn new(data_dir: PathBuf, control_plane_url: String, registry: RuntimeRegistry) -> Self {
        let this = Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            registry,
            data_dir,
            control_plane_url,
        };
        let _ = this.load();
        this
    }

    // ── Persistence ─────────────────────────────────────────

    fn agents_path(&self) -> PathBuf {
        self.data_dir.join("agents.json")
    }

    fn save(&self) {
        let path = self.agents_path();
        let guard = self.inner.read();
        if let Ok(json) = serde_json::to_string_pretty(&*guard) {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(&path, &json) {
                warn!("Failed to persist agents.json: {e}");
            }
        }
    }

    fn load(&self) -> Result<()> {
        let path = self.agents_path();
        if !path.exists() {
            return Ok(());
        }
        let data = std::fs::read_to_string(&path)?;
        let map: BTreeMap<String, AgentState> = serde_json::from_str(&data)?;
        // Reset all to stopped since child processes don't survive restarts
        let map: BTreeMap<String, AgentState> = map
            .into_iter()
            .map(|(k, mut v)| {
                v.pid = None;
                if v.config.enabled {
                    v.status = "stopped".into();
                } else {
                    v.status = "stopped".into();
                }
                (k, v)
            })
            .collect();
        *self.inner.write() = map;
        Ok(())
    }

    // ── CRUD ────────────────────────────────────────────────

    pub fn list_agents(&self) -> Vec<AgentConfig> {
        self.inner
            .read()
            .values()
            .map(|s| s.config.clone())
            .collect()
    }

    #[allow(dead_code)]
    pub fn get_agent(&self, id: &str) -> Option<AgentConfig> {
        self.inner.read().get(id).map(|s| s.config.clone())
    }

    pub fn add_agent(&self, config: AgentConfig) -> Result<AgentConfig> {
        let mut guard = self.inner.write();
        if guard.contains_key(&config.id) {
            anyhow::bail!("agent id {} already exists", config.id);
        }
        let state = AgentState {
            config: config.clone(),
            pid: None,
            status: "stopped".into(),
        };
        guard.insert(config.id.clone(), state);
        drop(guard);
        self.save();
        Ok(config)
    }

    pub fn update_agent(&self, id: &str, config: AgentConfig) -> Result<AgentConfig> {
        let mut guard = self.inner.write();
        let entry = guard
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("agent {id} not found"))?;
        if entry.status == "running" {
            anyhow::bail!("cannot update a running agent; stop it first");
        }
        entry.config = config.clone();
        drop(guard);
        self.save();
        Ok(config)
    }

    pub fn remove_agent(&self, id: &str) -> Result<()> {
        // Stop if running
        let _ = self.stop_agent(id);
        let mut guard = self.inner.write();
        guard
            .remove(id)
            .ok_or_else(|| anyhow::anyhow!("agent {id} not found"))?;
        drop(guard);
        self.save();
        Ok(())
    }

    // ── Process Lifecycle ───────────────────────────────────

    pub fn agent_status(&self, id: &str) -> String {
        self.inner
            .read()
            .get(id)
            .map(|s| s.status.clone())
            .unwrap_or_else(|| "not_found".into())
    }

    pub async fn launch_agent(&self, id: &str) -> Result<()> {
        let config = {
            let guard = self.inner.read();
            let state = guard
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("agent {id} not found"))?;
            if state.status == "running" {
                anyhow::bail!("agent {id} is already running");
            }
            state.config.clone()
        };
        let child = spawn_agent_adapter(&config, &self.control_plane_url)
            .context("failed to spawn agent-adapter process")?;
        let pid = child
            .id()
            .ok_or_else(|| anyhow::anyhow!("child has no pid"))?;
        // Update state to "starting" before spawning background monitor
        {
            let mut guard = self.inner.write();
            if let Some(state) = guard.get_mut(id) {
                state.pid = Some(pid);
                state.status = "starting".into();
            }
        }
        self.save();
        // Monitor the child process in background
        let inner = self.inner.clone();
        let monitor_id = id.to_string();
        tokio::spawn(async move {
            let result = child.wait_with_output().await;
            {
                let mut guard = inner.write();
                if let Some(state) = guard.get_mut(&monitor_id) {
                    state.pid = None;
                    state.status = "stopped".into();
                }
            }
            match result {
                Ok(output) => {
                    if !output.status.success() {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        warn!("agent {monitor_id} exited with {}: {stderr}", output.status);
                    } else {
                        info!("agent {monitor_id} exited cleanly");
                    }
                }
                Err(e) => {
                    error!("agent {monitor_id} wait error: {e}");
                }
            }
        });
        // Wait for the agent to register with the control-plane (up to 10 seconds)
        let registry = self.registry.clone();
        let wait_id = id.to_string();
        for _ in 0..20 {
            if registry.get(&wait_id).is_some() {
                // Agent registered successfully
                let mut guard = self.inner.write();
                if let Some(state) = guard.get_mut(&wait_id) {
                    state.status = "running".into();
                }
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        // Registration timed out — set to error but keep process running (it may still register)
        {
            let mut guard = self.inner.write();
            if let Some(state) = guard.get_mut(&wait_id) {
                if state.status == "starting" {
                    state.status = "error".into();
                }
            }
        }
        Err(anyhow::anyhow!(
            "agent {id} failed to register within 10 seconds — check agent-adapter logs for registration errors"
        ))
    }

    pub fn stop_agent(&self, id: &str) -> Result<()> {
        let pid = {
            let mut guard = self.inner.write();
            let state = guard
                .get_mut(id)
                .ok_or_else(|| anyhow::anyhow!("agent {id} not found"))?;
            let pid = state
                .pid
                .take()
                .ok_or_else(|| anyhow::anyhow!("agent {id} is not running"))?;
            state.status = "stopped".into();
            pid
        };
        // Kill the process
        #[cfg(unix)]
        {
            use std::process::Command as StdCommand;
            let _ = StdCommand::new("kill").arg(pid.to_string()).output();
        }
        #[cfg(not(unix))]
        {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid as i32),
                nix::sys::signal::Signal::SIGTERM,
            );
        }
        self.save();
        info!("stopped agent {id} (pid {pid})");
        Ok(())
    }

    /// Check if a child process is still alive; if dead, update state.
    pub fn reap_zombies(&self) {
        let mut guard = self.inner.write();
        for (id, state) in guard.iter_mut() {
            if state.status != "running" {
                continue;
            }
            if let Some(pid) = state.pid {
                let alive = check_pid_alive(pid);
                if !alive {
                    info!("reaped zombie agent {id} (pid {pid})");
                    state.pid = None;
                    state.status = "stopped".into();
                }
            }
        }
    }

    /// Return status map for the API
    pub fn agent_statuses(&self) -> BTreeMap<String, serde_json::Value> {
        let guard = self.inner.read();
        guard
            .iter()
            .map(|(id, state)| {
                let val = serde_json::json!({
                    "pid": state.pid,
                    "status": state.status,
                });
                (id.clone(), val)
            })
            .collect()
    }

    /// Scan the system for installed AI software and return detected agents.
    pub fn detect_agents(&self) -> Vec<DetectedAgent> {
        let existing: std::collections::HashSet<String> = self
            .inner
            .read()
            .values()
            .map(|s| s.config.id.clone())
            .collect();
        detect_system_agents(existing)
    }
}

// ── System Detection ─────────────────────────────────────

fn detect_system_agents(existing: std::collections::HashSet<String>) -> Vec<DetectedAgent> {
    let mut agents: Vec<DetectedAgent> = Vec::new();

    // Helper: check if a binary exists on PATH
    let which = |name: &str| -> Option<String> {
        let output = std::process::Command::new("which")
            .arg(name)
            .output()
            .ok()?;
        if output.status.success() {
            let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !s.is_empty() { Some(s) } else { None }
        } else {
            None
        }
    };

    // Helper: read npm package manifest for version info
    let npm_version = |pkg: &str| -> String {
        let output = std::process::Command::new("npm")
            .args(["list", "-g", "--depth=0", "--json"])
            .output()
            .ok();
        if let Some(out) = output {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                if let Some(deps) = json.get("dependencies") {
                    if let Some(info) = deps.get(pkg) {
                        if let Some(ver) = info.get("version").and_then(|v| v.as_str()) {
                            return ver.to_string();
                        }
                    }
                }
            }
        }
        String::new()
    };

    let is_configured = |id: &str| -> bool { existing.contains(id) };

    // ── 1. Claude Code (npm package) ──
    if let Some(path) = which("claude") {
        let ver = npm_version("claude");
        agents.push(DetectedAgent {
            name: if ver.is_empty() { "Claude Code".into() } else { format!("Claude Code ({ver})") },
            backend_type: "claude_cli".into(),
            command: Some(path),
            args: vec![],
            base_url: None,
            model: None,
            description: "Anthropic Claude Code AI assistant (via npm). 支持 Planning、Implementation、Review 等多种能力。请确保已设置 ANTHROPIC_API_KEY 或已登录 Claude。".into(),
            already_configured: is_configured("claude-code"),
        });
    }

    // ── 2. OpenCode AI ──
    if let Some(path) = which("opencode") {
        let ver = npm_version("opencode-ai");
        agents.push(DetectedAgent {
            name: if ver.is_empty() {
                "OpenCode".into()
            } else {
                format!("OpenCode AI (v{ver})")
            },
            backend_type: "stdio".into(),
            command: Some(path),
            args: vec!["run".into(), "--format".into(), "json".into()],
            base_url: None,
            model: None,
            description:
                "OpenCode — 开源 AI 编码智能体。通过 opencode run 模式集成，使用 JSON 格式输出。"
                    .into(),
            already_configured: is_configured("opencode"),
        });
    }

    // ── 3. Ollama (local LLM server) ──
    let ollama_paths = [
        "/opt/homebrew/bin/ollama",
        "/usr/local/bin/ollama",
        "/usr/bin/ollama",
    ];
    let ollama_found = ollama_paths
        .iter()
        .any(|p| std::path::Path::new(p).exists());
    if ollama_found || which("ollama").is_some() {
        agents.push(DetectedAgent {
            name: "Ollama".into(),
            backend_type: "openai".into(),
            command: None,
            args: vec![],
            base_url: Some("http://localhost:11434/v1".into()),
            model: Some("llama3".into()),
            description: "Ollama — 本地运行的 LLM 服务器。兼容 OpenAI API 格式。连接后可在平台上使用本地模型（如 llama3、mistral、qwen2 等）。请确保已启动: ollama serve".into(),
            already_configured: is_configured("ollama"),
        });
    }

    // ── 4. GitHub Copilot CLI ──
    if let Some(path) = which("copilot") {
        agents.push(DetectedAgent {
            name: "GitHub Copilot CLI".into(),
            backend_type: "stdio".into(),
            command: Some(path),
            args: vec!["-p".into()],
            base_url: None,
            model: None,
            description:
                "GitHub Copilot CLI — 在终端中使用 GitHub Copilot。通过非交互式 prompt 模式集成。"
                    .into(),
            already_configured: is_configured("github-copilot"),
        });
    }

    // ── 5. agent-browser (npm) ──
    if let Some(path) = which("agent-browser") {
        let ver = npm_version("agent-browser");
        agents.push(DetectedAgent {
            name: if ver.is_empty() {
                "Agent Browser".into()
            } else {
                format!("Agent Browser (v{ver})")
            },
            backend_type: "stdio".into(),
            command: Some(path),
            args: vec!["--headless".into()],
            base_url: None,
            model: None,
            description: "Agent Browser — 浏览器自动化 AI 代理。可控制浏览器执行网页操作任务。"
                .into(),
            already_configured: is_configured("agent-browser"),
        });
    }

    // ── 6. ollama 运行检测（正在运行的进程中有 ollama） ──
    let ollama_running = std::process::Command::new("pgrep")
        .arg("-x")
        .arg("ollama")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if ollama_running {
        // If ollama is running but the agent was already added above, update the entry
        if let Some(entry) = agents.iter_mut().find(|a| a.name == "Ollama") {
            // Already present — just update the model list hint
            if entry.model.as_deref() == Some("llama3") {
                // Try to detect available models
                let models = std::process::Command::new("ollama")
                    .arg("list")
                    .output()
                    .ok();
                if let Some(m) = models {
                    let out = String::from_utf8_lossy(&m.stdout);
                    let first_model = out.lines().nth(1).and_then(|l| l.split_whitespace().next());
                    if let Some(mname) = first_model {
                        entry.model = Some(mname.to_string());
                    }
                }
            }
        }
    }

    // Sort: not configured first, then by name
    agents.sort_by(|a, b| {
        a.already_configured
            .cmp(&b.already_configured)
            .then(a.name.cmp(&b.name))
    });

    agents
}

// ── Spawning ─────────────────────────────────────────────

fn spawn_agent_adapter(config: &AgentConfig, control_plane_url: &str) -> Result<Child> {
    let backend_json = build_backend_json(config);
    let bin_path = find_agent_adapter_bin()?;

    let mut cmd = Command::new(&bin_path);
    cmd.args([
        "--bind",
        &format!("127.0.0.1:0"), // OS-assigned port
        "--control-plane",
        control_plane_url,
        "--runtime-id",
        &config.id,
        "--agent-id",
        &config.id,
        "--display-name",
        &config.name,
        "--backend",
        &backend_json,
        "--auto-register",
    ])
    .kill_on_drop(true)
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped());

    let child = cmd
        .spawn()
        .context("failed to spawn agent-adapter binary")?;
    info!(
        "spawned agent-adapter for {} (pid {})",
        config.name,
        child.id().unwrap_or(0)
    );
    Ok(child)
}

fn build_backend_json(config: &AgentConfig) -> String {
    match config.backend_type.as_str() {
        "openai" => {
            let mut map = serde_json::Map::new();
            map.insert("type".into(), "openai".into());
            if let Some(base_url) = &config.base_url {
                map.insert("base_url".into(), base_url.clone().into());
            }
            if let Some(model) = &config.model {
                map.insert("model".into(), model.clone().into());
            }
            if let Some(api_key) = &config.api_key {
                map.insert("api_key".into(), api_key.clone().into());
            }
            serde_json::Value::Object(map).to_string()
        }
        "stdio" => {
            let cmd = config
                .command
                .clone()
                .unwrap_or_else(|| "/bin/cat".to_string());
            let (args, message_on_cli) = normalize_stdio_args(&cmd, &config.args);
            let args: Vec<serde_json::Value> = args
                .iter()
                .map(|a| serde_json::Value::String(a.clone()))
                .collect();
            let mut backend = serde_json::json!({
                "type": "stdio",
                "command": cmd,
            });
            if !args.is_empty() {
                backend["args"] = serde_json::Value::Array(args);
            }
            // Prompt-mode CLIs expect the message as an argument rather than stdin.
            if message_on_cli {
                backend["message_on_cli"] = serde_json::Value::Bool(true);
            }
            backend.to_string()
        }
        "claude_cli" => serde_json::json!({ "type": "claude_cli" }).to_string(),
        "codex_cli" => serde_json::json!({ "type": "codex_cli" }).to_string(),
        _ => {
            // Fallback to stdio with the given command
            serde_json::json!({
                "type": "stdio",
                "command": config.command.clone().unwrap_or_else(|| "/bin/cat".to_string()),
            })
            .to_string()
        }
    }
}

fn normalize_stdio_args(command: &str, configured_args: &[String]) -> (Vec<String>, bool) {
    let binary_name = std::path::Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(command);

    match binary_name {
        "copilot" => {
            let mut args = configured_args.to_vec();
            if args.is_empty() || (args.len() == 2 && args[0] == "explain" && args[1] == "--") {
                args = vec!["-p".to_string()];
            }
            let message_on_cli = args
                .iter()
                .any(|arg| matches!(arg.as_str(), "-p" | "--prompt"));
            (args, message_on_cli)
        }
        "opencode" => {
            let mut args = configured_args
                .iter()
                .filter(|arg| arg.as_str() != "--non-interactive")
                .cloned()
                .collect::<Vec<_>>();
            if !args.iter().any(|arg| arg == "run") {
                args.insert(0, "run".to_string());
            }
            (args, true)
        }
        _ => {
            let args = configured_args.to_vec();
            let message_on_cli = args.iter().any(|arg| arg == "run");
            (args, message_on_cli)
        }
    }
}

fn find_agent_adapter_bin() -> Result<String> {
    // Try common locations relative to the control-plane binary
    let exe = std::env::current_exe().ok();
    if let Some(path) = exe {
        // Same directory (target/debug/)
        let sibling = path.parent().unwrap().join("agent-adapter");
        if sibling.exists() {
            return Ok(sibling.to_string_lossy().to_string());
        }
        // Workspace root / target/debug/
        let ws = path.ancestors().nth(3).unwrap_or(std::path::Path::new("."));
        let ws_bin = ws.join("target").join("debug").join("agent-adapter");
        if ws_bin.exists() {
            return Ok(ws_bin.to_string_lossy().to_string());
        }
    }
    // Fallback: check PATH
    if let Ok(path) = std::process::Command::new("which")
        .arg("agent-adapter")
        .output()
    {
        if path.status.success() {
            let s = String::from_utf8_lossy(&path.stdout).trim().to_string();
            if !s.is_empty() {
                return Ok(s);
            }
        }
    }
    anyhow::bail!(
        "agent-adapter binary not found. Build with `cargo build --workspace` or place it on PATH."
    );
}

fn check_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let output = std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .output();
        matches!(output, Ok(o) if o.status.success())
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true // fallback: assume alive
    }
}
