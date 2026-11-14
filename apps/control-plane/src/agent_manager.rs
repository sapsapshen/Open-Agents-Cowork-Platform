use anyhow::{Context, Result};
use parking_lot::RwLock;
use platform_core::RuntimeRegistry;
use platform_domain::AgentConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::process::{Child, Command};
use tracing::{error, info, warn};

/// Represents a system-detected AI tool that can be added as an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedAgent {
    pub id: String,
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
    status: String, // "stopped", "running", "starting", "error"
    #[serde(default)]
    launch_generation: u64,
}

#[derive(Clone)]
pub struct AgentManager {
    inner: Arc<RwLock<BTreeMap<String, AgentState>>>,
    registry: RuntimeRegistry,
    data_dir: PathBuf,
    control_plane_url: String,
    shutting_down: Arc<AtomicBool>,
}

impl AgentManager {
    pub fn new(data_dir: PathBuf, control_plane_url: String, registry: RuntimeRegistry) -> Self {
        let this = Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            registry,
            data_dir,
            control_plane_url,
            shutting_down: Arc::new(AtomicBool::new(false)),
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
        let persisted: BTreeMap<String, AgentState> = guard
            .iter()
            .map(|(id, state)| {
                (
                    id.clone(),
                    AgentState {
                        config: redact_agent_secret(&state.config),
                        pid: state.pid,
                        status: state.status.clone(),
                        launch_generation: state.launch_generation,
                    },
                )
            })
            .collect();
        if let Ok(json) = serde_json::to_string_pretty(&persisted) {
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
                v.status = "stopped".into();
                v.launch_generation = 0;
                (k, v)
            })
            .collect();
        *self.inner.write() = map;
        Ok(())
    }

    // ── CRUD ────────────────────────────────────────────────

    pub fn list_agents(&self) -> Vec<AgentConfig> {
        self.refresh_runtime_statuses();
        self.inner
            .read()
            .values()
            .map(|s| redact_agent_secret(&s.config))
            .collect()
    }

    #[allow(dead_code)]
    pub fn get_agent(&self, id: &str) -> Option<AgentConfig> {
        self.refresh_runtime_statuses();
        self.inner
            .read()
            .get(id)
            .map(|s| redact_agent_secret(&s.config))
    }

    pub fn add_agent(&self, config: AgentConfig) -> Result<AgentConfig> {
        validate_agent_config(&config)?;
        let mut guard = self.inner.write();
        if guard.contains_key(&config.id) {
            anyhow::bail!("agent id {} already exists", config.id);
        }
        let state = AgentState {
            config: config.clone(),
            pid: None,
            status: "stopped".into(),
            launch_generation: 0,
        };
        guard.insert(config.id.clone(), state);
        drop(guard);
        self.save();
        Ok(redact_agent_secret(&config))
    }

    pub fn update_agent(&self, id: &str, config: AgentConfig) -> Result<AgentConfig> {
        let mut guard = self.inner.write();
        let entry = guard
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("agent {id} not found"))?;
        if entry.status == "running" || entry.status == "starting" {
            anyhow::bail!("cannot update a running agent; stop it first");
        }
        let mut incoming = config;
        incoming.id = id.to_string();
        let merged = merge_agent_config(&entry.config, incoming);
        validate_agent_config(&merged)?;
        entry.config = merged.clone();
        drop(guard);
        self.save();
        Ok(redact_agent_secret(&merged))
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
        self.refresh_runtime_statuses();
        self.inner
            .read()
            .get(id)
            .map(|s| s.status.clone())
            .unwrap_or_else(|| "not_found".into())
    }

    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub async fn launch_agent(&self, id: &str) -> Result<()> {
        let (config, generation) = self.reserve_launch(id)?;
        let mut child = match spawn_agent_adapter(&config, &self.control_plane_url) {
            Ok(child) => child,
            Err(error) => {
                self.release_launch(id, generation);
                return Err(error).context("failed to spawn agent-adapter process");
            }
        };
        let Some(pid) = child.id() else {
            let _ = child.start_kill();
            self.release_launch(id, generation);
            anyhow::bail!("child has no pid");
        };
        if !self.record_launch_pid(id, generation, pid) {
            let _ = child.start_kill();
            self.release_launch(id, generation);
            anyhow::bail!("agent {id} launch was superseded");
        }
        self.save();
        let inner = self.inner.clone();
        let registry = self.registry.clone();
        let monitor_id = id.to_string();
        tokio::spawn(async move {
            let result = child.wait().await;
            registry.remove(&monitor_id);
            {
                let mut guard = inner.write();
                if let Some(state) = guard.get_mut(&monitor_id)
                    && state.launch_generation == generation
                {
                    state.pid = None;
                    state.status = "stopped".into();
                }
            }
            match result {
                Ok(status) => {
                    if !status.success() {
                        warn!("agent {monitor_id} exited with {status}");
                    } else {
                        info!("agent {monitor_id} exited cleanly");
                    }
                }
                Err(e) => {
                    error!("agent {monitor_id} wait error: {e}");
                }
            }
        });
        let registry = self.registry.clone();
        let wait_id = id.to_string();
        for _ in 0..20 {
            if self.is_shutting_down() {
                anyhow::bail!("control plane is shutting down");
            }
            if registry.get(&wait_id).is_some() {
                let mut guard = self.inner.write();
                if let Some(state) = guard.get_mut(&wait_id)
                    && state.launch_generation == generation
                {
                    state.status = "running".into();
                }
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        {
            let mut guard = self.inner.write();
            if let Some(state) = guard.get_mut(&wait_id)
                && state.launch_generation == generation
                && state.status == "starting"
            {
                state.status = "error".into();
            }
        }
        Err(anyhow::anyhow!(
            "agent {id} failed to register within 10 seconds — check agent-adapter logs for registration errors"
        ))
    }

    fn reserve_launch(&self, id: &str) -> Result<(AgentConfig, u64)> {
        let mut guard = self.inner.write();
        if self.is_shutting_down() {
            anyhow::bail!("control plane is shutting down");
        }
        let state = guard
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("agent {id} not found"))?;
        if state.status == "running" || state.status == "starting" {
            anyhow::bail!("agent {id} is already running");
        }
        if let Some(pid) = state.pid
            && check_pid_alive(pid)
        {
            anyhow::bail!("agent {id} is already running (pid {pid})");
        }
        state.launch_generation = state.launch_generation.wrapping_add(1);
        state.status = "starting".into();
        state.pid = None;
        Ok((state.config.clone(), state.launch_generation))
    }

    fn record_launch_pid(&self, id: &str, generation: u64, pid: u32) -> bool {
        let mut guard = self.inner.write();
        if self.is_shutting_down() {
            return false;
        }
        let Some(state) = guard.get_mut(id) else {
            return false;
        };
        if state.launch_generation != generation {
            return false;
        }
        state.pid = Some(pid);
        state.status = "starting".into();
        true
    }

    fn release_launch(&self, id: &str, generation: u64) {
        let mut guard = self.inner.write();
        if let Some(state) = guard.get_mut(id)
            && state.launch_generation == generation
            && state.status == "starting"
        {
            state.pid = None;
            state.status = "stopped".into();
        }
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
        terminate_process(pid);
        self.registry.remove(id);
        self.save();
        info!("stopped agent {id} (pid {pid})");
        Ok(())
    }

    /// Check if a child process is still alive; if dead, update state.
    pub fn reap_zombies(&self) {
        let mut changed = false;
        let mut guard = self.inner.write();
        for (id, state) in guard.iter_mut() {
            if state.status != "running" {
                continue;
            }
            if let Some(pid) = state.pid {
                let alive = check_pid_alive(pid);
                if !alive {
                    info!("reaped zombie agent {id} (pid {pid})");
                    self.registry.remove(id);
                    state.pid = None;
                    state.status = "stopped".into();
                    changed = true;
                }
            }
        }
        drop(guard);
        if changed {
            self.save();
        }
    }

    pub fn stop_all(&self) {
        let ids = self.inner.read().keys().cloned().collect::<Vec<_>>();
        for id in ids {
            let has_process = self
                .inner
                .read()
                .get(&id)
                .is_some_and(|state| state.pid.is_some());
            if has_process && let Err(error) = self.stop_agent(&id) {
                warn!("failed to stop agent {id} during shutdown: {error}");
            }
        }
    }

    pub async fn launch_auto_agents(&self) {
        let ids = self
            .inner
            .read()
            .iter()
            .filter(|(_, state)| state.config.enabled && state.config.auto_launch)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            if self.is_shutting_down() {
                break;
            }
            if let Err(error) = self.launch_agent(&id).await {
                warn!("auto-launch failed for {id}: {error}");
            }
        }
    }

    /// Return status map for the API
    pub fn agent_statuses(&self) -> BTreeMap<String, serde_json::Value> {
        self.refresh_runtime_statuses();
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

    fn refresh_runtime_statuses(&self) {
        let registered_ids = self
            .registry
            .list()
            .into_iter()
            .map(|runtime| runtime.runtime_id)
            .collect::<std::collections::HashSet<_>>();

        let mut changed = false;
        {
            let mut guard = self.inner.write();
            for (id, state) in guard.iter_mut() {
                if state.pid.is_some() && registered_ids.contains(id) && state.status != "running" {
                    state.status = "running".into();
                    changed = true;
                } else if state.pid.is_none()
                    && !registered_ids.contains(id)
                    && matches!(state.status.as_str(), "running" | "starting" | "error")
                {
                    state.status = "stopped".into();
                    changed = true;
                }
            }
        }
        if changed {
            self.save();
        }
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
        if let Some(out) = output
            && let Ok(json) = serde_json::from_slice::<serde_json::Value>(&out.stdout)
            && let Some(deps) = json.get("dependencies")
            && let Some(info) = deps.get(pkg)
            && let Some(ver) = info.get("version").and_then(|v| v.as_str())
        {
            return ver.to_string();
        }
        String::new()
    };

    let is_configured = |id: &str| -> bool { existing.contains(id) };

    // ── 1. Claude Code (npm package) ──
    if let Some(path) = which("claude") {
        let ver = npm_version("claude");
        agents.push(DetectedAgent {
            id: "claude-code".into(),
            name: if ver.is_empty() {
                "Claude Code".into()
            } else {
                format!("Claude Code ({ver})")
            },
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
            id: "opencode".into(),
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
            id: "ollama".into(),
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
            id: "github-copilot".into(),
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
            id: "agent-browser".into(),
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
        "127.0.0.1:0",
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
    .stdout(std::process::Stdio::inherit())
    .stderr(std::process::Stdio::inherit());

    apply_backend_env(&mut cmd, config);

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

fn apply_backend_env(cmd: &mut Command, config: &AgentConfig) {
    if config.backend_type != "openai" {
        return;
    }
    if let Some(api_key) = config
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        cmd.env("OPENAI_API_KEY", api_key);
        return;
    }
    let Some(var_name) = config
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    match std::env::var(var_name) {
        Ok(value) if !value.trim().is_empty() => {
            cmd.env("OPENAI_API_KEY", value);
        }
        _ => {
            cmd.env_remove("OPENAI_API_KEY");
            warn!(
                "agent {} api_key_env {var_name} is unset; OPENAI_API_KEY was removed from the child environment",
                config.id
            );
        }
    }
}

fn redact_agent_secret(config: &AgentConfig) -> AgentConfig {
    let mut sanitized = config.clone();
    sanitized.api_key = None;
    sanitized
}

fn merge_agent_config(existing: &AgentConfig, mut incoming: AgentConfig) -> AgentConfig {
    let keep_existing_secret = incoming
        .api_key
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty);
    if keep_existing_secret {
        incoming.api_key = existing.api_key.clone();
    }
    incoming
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
        "claude" => {
            let mut args = configured_args.to_vec();
            if !args.iter().any(|arg| arg == "-p" || arg == "--print") {
                args.insert(0, "-p".to_string());
            }
            if !args.iter().any(|arg| arg == "--output-format") {
                args.push("--output-format".to_string());
                args.push("text".to_string());
            }
            (args, false)
        }
        "codex" => {
            let mut args = configured_args.to_vec();
            if !args.iter().any(|arg| arg == "exec") {
                args.insert(0, "exec".to_string());
            }
            (args, false)
        }
        _ => {
            let args = configured_args.to_vec();
            let message_on_cli = args.iter().any(|arg| arg == "run");
            (args, message_on_cli)
        }
    }
}

fn adapter_file_name() -> String {
    format!("agent-adapter{}", std::env::consts::EXE_SUFFIX)
}

fn adapter_candidates(exe: &Path) -> Vec<PathBuf> {
    let file_name = adapter_file_name();
    let Some(directory) = exe.parent() else {
        return Vec::new();
    };
    let mut candidates = vec![directory.join(&file_name)];
    let workspace = exe.ancestors().nth(3).unwrap_or(Path::new("."));
    for profile in ["debug", "release"] {
        candidates.push(workspace.join("target").join(profile).join(&file_name));
    }
    candidates
}

fn find_on_path(file_name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths).find_map(|directory| {
        let candidate = directory.join(file_name);
        candidate.is_file().then_some(candidate)
    })
}

fn find_agent_adapter_bin() -> Result<String> {
    if let Ok(path) = std::env::current_exe() {
        for candidate in adapter_candidates(&path) {
            if candidate.is_file() {
                return Ok(candidate.to_string_lossy().into_owned());
            }
        }
    }
    if let Some(found) = find_on_path(&adapter_file_name()) {
        return Ok(found.to_string_lossy().into_owned());
    }
    anyhow::bail!(
        "agent-adapter binary not found. Build with `cargo build --workspace` or place it on PATH."
    );
}

fn terminate_process(pid: u32) {
    #[cfg(unix)]
    {
        let _ = std::process::Command::new("kill")
            .arg(pid.to_string())
            .output();
    }
    #[cfg(not(unix))]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string()])
            .output();
    }
}

fn validate_agent_config(config: &AgentConfig) -> Result<()> {
    if config.id.is_empty()
        || config.id.len() > 64
        || !config.id.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '-' || character == '_'
        })
    {
        anyhow::bail!("agent id must be 1-64 characters of ASCII letters, digits, '-' or '_'");
    }
    if config.name.trim().is_empty() {
        anyhow::bail!("agent name is required");
    }
    match config.backend_type.as_str() {
        "openai" => {
            let base_url = config.base_url.as_deref().unwrap_or("").trim();
            if !(base_url.starts_with("http://") || base_url.starts_with("https://")) {
                anyhow::bail!("openai base_url must start with http:// or https://");
            }
            if config.model.as_deref().unwrap_or("").trim().is_empty() {
                anyhow::bail!("openai model is required");
            }
            if let Some(env_name) = config
                .api_key_env
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                validate_env_var_name(env_name)?;
            }
        }
        "stdio" => {
            if config.command.as_deref().unwrap_or("").trim().is_empty() {
                anyhow::bail!("stdio command is required");
            }
        }
        "claude_cli" | "codex_cli" => {}
        other => anyhow::bail!("unsupported backend type {other}"),
    }
    Ok(())
}

fn validate_env_var_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("api_key_env must name an environment variable");
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        anyhow::bail!("api_key_env must be a valid environment variable name");
    }
    Ok(())
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
        let output = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output();
        match output {
            Ok(output) => String::from_utf8_lossy(&output.stdout).contains(&pid.to_string()),
            Err(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use platform_domain::{HealthStatus, Metadata, RuntimeDescriptor, RuntimeHealth, RuntimeLoad};

    use super::*;

    fn openai_config() -> AgentConfig {
        AgentConfig {
            id: "agent-1".to_string(),
            name: "Agent 1".to_string(),
            backend_type: "openai".to_string(),
            base_url: Some("http://localhost:11434/v1".to_string()),
            model: Some("llama3".to_string()),
            api_key: Some("secret-token".to_string()),
            api_key_env: None,
            command: None,
            args: vec![],
            enabled: true,
            auto_launch: false,
        }
    }

    #[test]
    fn backend_json_omits_api_key() {
        let json = build_backend_json(&openai_config());

        assert!(json.contains("\"type\":\"openai\""));
        assert!(!json.contains("secret-token"));
        assert!(!json.contains("api_key"));
    }

    #[test]
    fn save_redacts_api_keys_from_persistence() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is monotonic")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("agent-manager-test-{unique}"));
        let manager = AgentManager::new(
            dir.clone(),
            "http://127.0.0.1:9000".to_string(),
            RuntimeRegistry::new(),
        );

        manager.add_agent(openai_config()).expect("agent added");

        let data = std::fs::read_to_string(dir.join("agents.json")).expect("agents.json exists");
        assert!(!data.contains("secret-token"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn update_preserves_existing_secret_when_payload_omits_it() {
        let existing = openai_config();
        let mut incoming = existing.clone();
        incoming.model = Some("qwen2.5-coder".to_string());
        incoming.api_key = None;

        let merged = merge_agent_config(&existing, incoming);

        assert_eq!(merged.api_key.as_deref(), Some("secret-token"));
        assert_eq!(merged.model.as_deref(), Some("qwen2.5-coder"));
    }

    #[test]
    fn late_runtime_registration_recovers_agent_status() {
        let registry = RuntimeRegistry::new();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is monotonic")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("agent-manager-status-test-{unique}"));
        let manager = AgentManager::new(
            dir.clone(),
            "http://127.0.0.1:9000".to_string(),
            registry.clone(),
        );

        manager.add_agent(openai_config()).expect("agent added");
        {
            let mut guard = manager.inner.write();
            let state = guard.get_mut("agent-1").expect("agent state");
            state.pid = Some(4242);
            state.status = "error".to_string();
        }

        registry
            .register(RuntimeDescriptor {
                runtime_id: "agent-1".to_string(),
                agent_id: "agent-1".to_string(),
                display_name: "Agent 1".to_string(),
                endpoint: "http://127.0.0.1:9201/a2a".to_string(),
                profile: "openai".to_string(),
                trust_tier: 2,
                cost_per_task: 1.0,
                capabilities: vec![],
                health: RuntimeHealth {
                    status: HealthStatus::Healthy,
                    availability: 0.99,
                    ..RuntimeHealth::default()
                },
                load: RuntimeLoad::default(),
                metadata: Metadata::new(),
            })
            .expect("runtime registered");

        assert_eq!(manager.agent_status("agent-1"), "running");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn launch_refuses_a_second_process_while_the_pid_is_alive() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is monotonic")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("agent-manager-launch-test-{unique}"));
        let manager = AgentManager::new(
            dir.clone(),
            "http://127.0.0.1:9000".to_string(),
            RuntimeRegistry::new(),
        );
        manager.add_agent(openai_config()).expect("agent added");
        {
            let mut guard = manager.inner.write();
            let state = guard.get_mut("agent-1").expect("agent state");
            state.pid = Some(std::process::id());
            state.status = "error".to_string();
        }

        let error = manager
            .launch_agent("agent-1")
            .await
            .expect_err("live pid must block a second launch");
        assert!(error.to_string().contains("already running"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn stdio_normalization_uses_current_cli_modes() {
        let (args, message_on_cli) = normalize_stdio_args("claude", &[]);
        assert_eq!(args, vec!["-p", "--output-format", "text"]);
        assert!(!message_on_cli);

        let (args, message_on_cli) = normalize_stdio_args("/usr/local/bin/codex", &[]);
        assert_eq!(args, vec!["exec"]);
        assert!(!message_on_cli);
    }

    #[tokio::test]
    async fn launch_refuses_after_shutdown_starts() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is monotonic")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("agent-manager-shutdown-test-{unique}"));
        let manager = AgentManager::new(
            dir.clone(),
            "http://127.0.0.1:9000".to_string(),
            RuntimeRegistry::new(),
        );
        manager.add_agent(openai_config()).expect("agent added");
        manager.begin_shutdown();

        let error = manager
            .launch_agent("agent-1")
            .await
            .expect_err("shutdown must reject new launches");
        assert!(error.to_string().contains("shutting down"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_api_key_env_removes_inherited_openai_key() {
        let mut config = openai_config();
        config.api_key = None;
        config.api_key_env = Some("PLATFORM_TEST_MISSING_OPENAI_KEY".to_string());
        let mut command = Command::new("agent-adapter");
        apply_backend_env(&mut command, &config);

        let removed = command
            .as_std()
            .get_envs()
            .any(|(key, value)| key == "OPENAI_API_KEY" && value.is_none());
        assert!(removed);
    }

    #[test]
    fn adapter_candidates_include_the_platform_executable_suffix() {
        let exe = PathBuf::from("/tmp/workspace/target/debug/control-plane");
        let candidates = adapter_candidates(&exe);
        let expected = adapter_file_name();
        assert!(candidates.len() >= 3);
        assert!(
            candidates
                .iter()
                .all(|path| path.file_name().and_then(|name| name.to_str())
                    == Some(expected.as_str()))
        );
    }
}
