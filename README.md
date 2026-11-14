# Open Agents Cowork Platform

> **多智能体协同平台 · Multi-Agent Cowork Platform · Plataforma de Coworking Multi-Agente**

A Rust-first, capability-aware multi-agent orchestration platform built on a JSON-RPC subset of the [A2A (Agent-to-Agent) protocol](https://github.com/a2aproject/A2A). The platform discovers AI agents on your machine, scores their capabilities, decomposes complex tasks into stages, and dispatches each stage to the best-fit runtime. Execute and revise stages can also ask peer runtimes for advice over A2A.

---

## 项目特点 · Features · Características

| 特点 | Feature | Característica |
|---|---|---|
| **本地 Agent 自动识别** | Automatic local agent discovery | Descubrimiento automático de agentes locales |
| **能力得分感知调度** | Capability-aware scheduling | Programación consciente de capacidades |
| **Google A2A 协议通信** | Google A2A protocol | Protocolo Google A2A |
| **第三方 AI 接入** | Third-party AI integration | Integración de IA de terceros |
| **自动任务分解** | Automatic task decomposition | Descomposición automática de tareas |
| **三语界面** | Trilingual UI (EN/ZH/ES) | Interfaz trilingüe |

### 1. 本地 Agent Runtime 自动识别

平台启动后，自动扫描您的系统环境，发现已安装的 AI 工具：

| 自动发现 | Auto-detected | Detectado automáticamente |
|---|---|---|
| **Claude Code** — npm 全局安装的 Claude CLI | `which claude` + npm version | |
| **OpenCode AI** — 开源 AI 编码智能体 | `which opencode` | |
| **Ollama** — 本地大模型服务器（支持 llama3, mistral, qwen2 等） | `which ollama` + 常见安装路径 | |
| **GitHub Copilot CLI** — 终端 Copilot | `which copilot` | |
| **Agent Browser** — 浏览器自动化代理 | `which agent-browser` | |

> 扫描结果以表格形式展示在 Dashboard → Settings → Auto-Detect 中。您勾选需要的 Agent 后，一键添加并启动，无需手动配置命令行参数。

### 2. Agent Runtime 能力得分感知

每个 Agent 在注册时都会上报其 **能力维度** 与 **能力得分**（0.0 ~ 1.0）：

| 能力 | Capability | 默认 CLI 得分 | OpenAI 高端模型得分 |
|---|---|---|---|
| 规划 (Planning) | 任务分解与执行计划 | 0.85 | 0.90（frontier：gpt-5、claude-opus、claude-sonnet-4、o3 等） |
| 执行 (Implementation) | 编写代码、生成内容 | 0.90 | 0.92 |
| 综合 (Synthesis) | 汇总生成最终报告 | 0.85 | 0.90 |
| 审查 (Review) | 审查输出质量 | 0.80 | 约 0.83（实现分 × 0.9） |

名称里带 `mini`、`haiku`、`nano`、`flash` 或 `small` 的模型使用更低一档（规划/执行/综合约为 0.75 / 0.80 / 0.75）。`gpt-4o` 这类中档模型约为 0.85 / 0.88 / 0.85。本地模型名（如 `llama3`）使用默认档 0.65 / 0.70 / 0.65。CLI 后端把 `max_parallelism` 标成 1，OpenAI 兼容后端标成 4；调度器使用其中的最大值作为该 Runtime 的 `max_tasks`。适配器在达到上限后会拒绝新的 `message/send`，调度器再改派其他 Runtime。

调度器（Scheduler）根据多维度评分矩阵选择最优 Runtime：

**硬过滤条件**（不满足则排除）：
- 必须拥有当前阶段所需能力
- 能力最低水平达标
- 信任等级（trust tier）匹配
- 延迟 / 成本预算在阈值内
- 心跳在 15 秒窗口内（健康检查）
- 未过载

**加权评分**（总分最高者胜出）：
```
最终得分 = 必需能力水平加权和 × 0.55
         + 偏好能力水平加权和 × 0.20
         + 平均成功率 × 0.15
         + 平均延迟信号 × 0.10
         + 可用性 × 0.25
         + (1 / max(单任务成本, 1)) × 0.10
         - (活跃任务 / 最大任务) × 0.20
         - 排队任务数 × 0.03
```

延迟信号为 `1 / (1 + 中位延迟毫秒 / 1000)`。成功率和延迟只统计真正匹配到的必需能力；通用 Runtime 回退不加质量分。

历史质量奖励来自 Runtime 已注册能力上的成功率和延迟信号。每次阶段执行结束后，控制平面会把该次成功/失败与耗时回写到对应能力指标，后续调度会自动偏向近期更稳定、更低延迟的 Runtime。

### 3. Google A2A Agent 通信协议

平台实现 [A2A（Agent-to-Agent）](https://github.com/a2aproject/A2A) 的一个 JSON-RPC 子集，用于运行时之间的直接对话。当前规范里的 Agent Card 也可以放在 `/.well-known/agent-card.json`；本仓库仍通过 JSON-RPC `agent/getCard` 交换同一份卡片，以保持现有控制平面和适配器兼容。

| A2A 方法 | Method | 用途 |
|---|---|---|
| `agent/getCard` | GET Agent Card | 获取 Runtime 的技能声明与能力指标 |
| `message/send` | Send Message | 发送工作指令，携带对话上下文 |
| `tasks/get` | Get Task | 查询异步任务状态与结果 |

- **传输层**：HTTP + JSON-RPC 2.0
- **认证**：`x-platform-token` 请求头
- **版本协商**：`A2A-Version: 1.0`
- **上下文连续性**：`context_id` 贯穿工作流各阶段和 Runtime 间协作
- **响应大小限制**：256 KiB 上限
- **直接协作**：Execute / Revise 阶段可携带 `collaborators` 元数据。`runtime-node` 会直接 `message/send` 咨询协作者，咨询失败则本阶段失败。`agent-adapter` 会用最多 10 秒向最多 2 个协作者取建议，并把建议写进发给模型的提示；某个协作者不可达时本阶段继续，不因此失败。咨询深度到 1 即停止，避免互相调用。

### 4. 支持第三方 AI 接入

您可以通过 Dashboard → Settings 添加外部 AI 服务。支持两种接入模式：

#### OpenAI 兼容 API
适用于 OpenAI、Anthropic API、Ollama、Zed AI、任意兼容端点。

```json
{
  "type": "openai",
  "base_url": "http://localhost:11434/v1",
  "model": "llama3"
}
```

API 密钥不要写进上面的后端 JSON。在 Dashboard 里填写的 `api_key` 只留在控制平面进程内存中，保存到 `target/platform-runtime/agents.json` 时会被抹掉，接口响应里也不会返回。重启后需要重新填写，或者在 Agent 上设置 `api_key_env`（例如 `OPENAI_API_KEY`），启动时从控制平面的环境变量读出并传给适配器。适配器调用模型时使用 `OPENAI_API_KEY`，密钥不会出现在命令行参数里。

Claude Code 后端执行 `claude -p --output-format text`，提示从标准输入进入，避免交互式 REPL 不退出。Codex 后端执行 `codex exec`，同样从标准输入读提示。OpenAI 兼容调用里，`gpt-5`、`gpt-4.1`、`o1`、`o3`、`o4` 使用 `max_completion_tokens`；如果服务端拒绝该字段，会自动换 `max_tokens` 再试一次。Ollama 等本地端点默认仍使用 `max_tokens`。

`auto_launch: true` 且 `enabled: true` 的 Agent 会在控制平面开始监听之后自动启动。自动检测使用稳定 id（`claude-code`、`opencode`、`ollama`、`github-copilot`、`agent-browser`）；版本号只出现在显示名中，不会因为版本变化而重复添加。

#### CLI 进程
适用于任意可通过命令行调用的 AI 工具（Claude CLI、Codex CLI、opencode 等）。

```json
{
  "type": "stdio",
  "command": "opencode",
  "args": ["run", "--format", "json"],
  "timeout_secs": 120
}
```

添加后，在 Settings 页面点击 **Launch**（启动），平台自动 spawn agent-adapter 进程并注册到控制平面，即可参与工作流调度。

### 5. 自动划分复杂任务并分配 Agent Runtime 执行

工作流提交后，Orchestrator 会先执行规划，再按计划自动拆解成多个执行步骤，并在综合前插入质量门：

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌────────────────────┐
│   Plan      │ ──> │  Execute    │ ──> │   Review    │ ──> │     Synthesize     │
│  规划阶段    │     │  执行阶段   │     │  审查阶段   │     │      综合阶段      │
└─────────────┘     └─────────────┘     └─────────────┘     └────────────────────┘
                                         │
                                         └──── revision required ────> Revise -> Review
```

1. **Plan（规划）** — 将目标发送给得分最高的 Planner Runtime，生成执行计划
2. **Execute（执行）** — 控制平面从 Plan 中解析出多个步骤，逐步调度 Executor；每步可携带上游 Artifact，并按能力要求附带 1~2 个协作者 Runtime
3. **Direct A2A Consult（直接协作）** — Execute / Revise Runtime 可直接向协作者发起 `consult` 消息，收集建议后再继续本步骤产出
4. **Review（审查）** — 使用 Reviewer Runtime 对聚合后的执行结果做质量门判断，要求返回 `APPROVED` 或 `REVISION_REQUIRED`
5. **Revise（修订，可选）** — 如果审查要求返工，调度 Revision Runtime 按反馈修订一次，并再次进入审查
6. **Synthesize（综合）** — 汇总目标、计划和经审查通过的执行结果，生成最终报告

审查结果的**第一行**必须是 `APPROVED` 或 `REVISION_REQUIRED`（也接受同行的 `NEEDS_REVISION` / `REQUIRES_REVISION`，以及首尾标点）。正文里再出现这些词不会改判。

Review 阶段会优先选择与 Execute / Revise 不同的 Runtime；如果当前部署中只有一个满足条件的 Runtime，则会回退到同一 Runtime 完成自审，避免单节点部署在审查阶段卡死。

每个阶段的 A2A 调用超时是 60 秒，整条工作流预算是 300 秒。`POST /workflows/submit` 会等到工作流结束再返回最终报告，所以 CLI 和 Dashboard 的提交按钮会一直等到完成或失败。如果调用方中途断开，控制平面里的任务仍会跑完，之后可以用 Dashboard、`GET /workflows/{id}` 或 SSE 查看。失败响应的错误链里带有 `workflow <id> failed`，同一条记录也会留在 `/workflows`。

---

## Architecture · 架构 · Arquitectura

### Workspace Layout

```text
open-agents-cowork-platform/
├── apps/
│   ├── control-plane/        # 控制平面：注册、编排、API、Agent 管理器
│   ├── agent-adapter/        # A2A 适配器：桥接到外部 AI Runtime
│   ├── runtime-node/         # A2A Runtime 端点（旧版节点）
│   ├── dashboard/            # Web Dashboard SPA（单页应用）
│   └── cli/                  # 操作 CLI
├── crates/
│   ├── platform-domain/      # 共享领域模型
│   ├── platform-a2a/         # A2A 数据模型、JSON-RPC 信封、客户端
│   └── platform-core/        # 注册表、调度器、编排器
└── start-platform.sh         # 一键启动脚本
```

### Components · 组件 · Componentes

#### Control Plane
- 接收工作流提交，提供 REST API
- 管理 Runtime 注册与心跳（每 5 秒）
- 运行能力感知调度器（评分矩阵）
- 编排 Plan、Execute、Review、可选 Revise、Synthesize 工作流
- 在提交请求返回后继续执行长工作流，并通过 `/workflows` 与 SSE 暴露进度
- Agent 配置持久化（`target/platform-runtime/agents.json`）
- Launch/stop agent-adapter 子进程

#### Agent Adapter
- 对每个 Agent 启动一个独立 HTTP 服务
- 将 A2A 协议请求转发到后端 AI（CLI 进程或 OpenAI API）
- 自动注册到控制平面并维持心跳
- 支持 Stdio / OpenAI / Claude CLI / Codex CLI 四种后端

#### Dashboard
- 单页应用（SPA），Hash 路由
- 四个页面：Dashboard（提交工作流）、Workflows（查看列表与详情）、Runtimes（监控运行节点）、Settings（Agent 配置管理）
- SSE 实时推送事件刷新页面
- 显示从 `submitted` 到各阶段完成/失败的完整工作流时间线
- 支持三语界面：中文、English、Español

---

## Quick Start · 快速开始 · Inicio Rápido

### Prerequisites · 前置条件 · Requisitos

- Rust 1.88+（edition 2024，代码使用了已稳定的 let chains）。仓库里的 `rust-toolchain.toml` 会选择当前 stable。
- Node.js / npm（仅在自动检测全局 npm 包版本时需要；没有 npm 时检测会跳过版本号）

### One-Click Start · 一键启动 · Inicio con Un Click

```bash
# macOS / Linux
./start-platform.sh
```

Windows:

```powershell
.\start-platform.cmd
```

启动后打开：

```
http://127.0.0.1:9000/dashboard
```

会自动构建 workspace，启动 control-plane。Dashboard 中可通过 Settings → Auto-Detect 发现并启动本地 AI Agent。

Windows 的 `start-platform.cmd` 还会额外启动四个 `runtime-node`（planner、builder、reviewer、synthesizer）。它们按 `--profile` 注册对应能力，因此不开外部模型也能跑通演示工作流。演示节点会截断回显的提示和综合输入，避免多步产物把 A2A 响应撑过 256 KiB。Linux / macOS 脚本只启动控制平面。

### Manual Start · 手动启动 · Inicio Manual

```bash
# 1. 构建
cargo build --workspace

# 2. 启动控制平面
PLATFORM_API_TOKEN=local-review-token \
RUST_LOG=info \
cargo run -p control-plane -- --bind 127.0.0.1:9000

# 3. 打开 Dashboard
# http://127.0.0.1:9000/dashboard

# 4. 在 Settings 页面添加并启动 Agent
```

如果只启动了一个 Runtime，平台仍可完成 Review 阶段，但会在审查时回退到同一 Runtime。多 Runtime 部署下，Review 会优先交给独立 Reviewer。

### Stop · 停止 · Detener

```bash
./stop-platform.sh
```

### Environment Variables · 环境变量 · Variables de Entorno

| 变量 | Variable | 默认值 | 说明 |
|---|---|---|---|
| `PLATFORM_API_TOKEN` | API 认证令牌 | 无默认值，启动脚本会用 `local-review-token` | 所有受保护 API 必须携带 `x-platform-token`。Dashboard 右上角可以把它存进浏览器 |
| `PLATFORM_ALLOWED_HOSTS` | 允许的主机 | `127.0.0.1,localhost,::1` | 出站 Runtime 与控制平面地址白名单 |
| `RUST_LOG` | 日志级别 | `info` | 可选 `debug`, `warn`, `error` |

---

## Dashboard · 仪表盘 · Panel de Control

| 页面 | Tab | Descripción |
|---|---|---|
| **Dashboard** | 提交工作流目标，查看最近工作流 | Submit workflow objectives |
| **Workflows** | 查看所有工作流，点击查看详细执行时间线 | View execution timeline |
| **Runtimes** | 监控已注册的 Agent Runtime 状态和心跳 | Monitor registered runtimes |
| **Settings** | 添加/编辑/启动/停止 AI Agent，自动检测系统工具 | Manage AI agents |

### i18n · 国际化 · Internacionalización

右上角语言切换：English / 中文 / Español。所有 UI 文本通过 `data-i18n` 属性自动切换。

---

## Security · 安全 · Seguridad

- 控制平面和 Runtime A2A 端点均受 `x-platform-token` 保护。二进制本身不提供默认令牌；`./start-platform.sh` 和 `start-platform.cmd` 在未设置时使用 `local-review-token`
- 出站 Runtime 端点通过 `PLATFORM_ALLOWED_HOSTS` 白名单限制，默认只允许环回地址
- 工作负载有上限：目标最多 10000 个字符，最多 50 条约束，每条约束最多 4000 个字符，A2A 响应最多 256 KiB。空白目标会被拒绝
- 每次 A2A JSON-RPC 调用有 60 秒超时，不依赖更短的客户端默认超时
- 收到 SIGINT / SIGTERM 时，控制平面会停掉它启动的 agent-adapter；适配器超时或被取消时会杀掉自己拉起的 CLI 子进程
- API 密钥不会写入 `agents.json`，也不会出现在适配器命令行中

---

## Tests and CI · 测试与持续集成

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
```

GitHub Actions 会在 push 和 pull request 上运行同样的三步。工作流文件是 `.github/workflows/ci.yml`。

---

## Tech Stack · 技术栈 · Tecnología

| 技术 | Technology | Propósito |
|---|---|---|
| **Rust** (edition 2024) | 核心开发语言 | Performance, safety, concurrency |
| **Tokio** | 异步运行时 | Async I/O, process management |
| **Axum** | HTTP 框架 | REST API, routing, middleware |
| **Reqwest** | HTTP 客户端 | A2A JSON-RPC transport |
| **JSON-RPC 2.0** | 协议 | Agent-to-Agent communication |
| **SSE** | 实时推送 | Server-Sent Events for live updates |
| **Serde** | 序列化 | JSON serialization/deserialization |
| **Clap** | CLI 参数 | Argument parsing |

---

## License · 许可 · Licencia

MIT
