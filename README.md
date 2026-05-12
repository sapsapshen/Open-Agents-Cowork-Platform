# Open Agents Cowork Platform

> **多智能体协同平台 · Multi-Agent Cowork Platform · Plataforma de Coworking Multi-Agente**

A Rust-first, capability-aware multi-agent orchestration platform built on the [Google A2A (Agent-to-Agent) protocol](https://github.com/google/A2A). The platform automatically discovers AI agents on your machine, scores their capabilities, decomposes complex tasks into stages, and dispatches each stage to the best-fit runtime — all through direct A2A peer dialogue.

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
| 规划 (Planning) | 任务分解与执行计划 | 0.85 | 0.90 (claude-4/gpt-4/o3) |
| 执行 (Implementation) | 编写代码、生成内容 | 0.90 | 0.92 |
| 综合 (Synthesis) | 汇总生成最终报告 | 0.85 | 0.90 |
| 审查 (Review) | 审查输出质量 | 0.80 | 0.83 |

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
最终得分 = 必需能力匹配 × 0.55
         + 偏好能力加分 × 0.20
         + 历史质量奖励   × 0.25
         + 可用性奖励   × 0.25
         + 成本奖励     × 0.10
         - 负载惩罚     × 0.20
         - 队列惩罚     × 0.03
```

历史质量奖励来自 Runtime 已注册能力上的成功率和延迟信号。每次阶段执行结束后，控制平面会把该次成功/失败与耗时回写到对应能力指标，后续调度会自动偏向近期更稳定、更低延迟的 Runtime。

### 3. Google A2A Agent 通信协议

平台基于 [A2A（Agent-to-Agent）](https://github.com/google/A2A) 规范实现运行时之间的直接对话：

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
- **直接协作**：Execute / Revise 阶段可携带 `collaborators` 元数据，Runtime 在本地直接通过 `message/send` 咨询其他 Runtime，而不是回到控制平面中转

### 4. 支持第三方 AI 接入

您可以通过 Dashboard → Settings 添加外部 AI 服务。支持两种接入模式：

#### OpenAI 兼容 API
适用于 OpenAI、Anthropic API、Ollama、Zed AI、任意兼容端点。

```json
{
  "type": "openai",
  "base_url": "http://localhost:11434/v1",
  "model": "llama3",
  "api_key": "optional-key"
}
```

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

Review 阶段会优先选择与 Execute / Revise 不同的 Runtime；如果当前部署中只有一个满足条件的 Runtime，则会回退到同一 Runtime 完成自审，避免单节点部署在审查阶段卡死。

每个阶段默认 60 秒超时，整体工作流 300 秒超时。工作流在控制平面内持续执行，不依赖单次提交请求保持连接；即使提交方超时或断开连接，仍可通过 Dashboard、`/workflows` 或 SSE（Server-Sent Events）继续跟踪结果。

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
- 编排三阶段工作流执行
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

- Rust 1.85+ (edition 2024)
- Node.js / npm（用于自动检测 npm 全局包）

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
| `PLATFORM_API_TOKEN` | API 认证令牌 | `local-review-token` | 所有 API 请求必须携带 |
| `PLATFORM_ALLOWED_HOSTS` | 允许的主机 | `127.0.0.1,localhost` | 出站 Runtime 端点白名单 |
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

- 控制平面和 Runtime A2A 端点均受 `x-platform-token` 保护
- 默认 Token 来源：`PLATFORM_API_TOKEN` 环境变量
- 出站 Runtime 端点通过 `PLATFORM_ALLOWED_HOSTS` 白名单限制
- 工作负载和元数据载荷大小有限制（10000 字符 Objective、50 个约束、256 KiB A2A 响应）
- HTTP 客户端和阶段执行均设置显式超时
- Agent 子进程自动清理

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
