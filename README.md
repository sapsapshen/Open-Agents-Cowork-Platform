# Open Agents Cowork Platform

Rust-first multi-agent runtime platform for **capability-aware task orchestration** and **direct runtime-to-runtime dialogue** over **Google A2A / Agent2Agent-inspired JSON-RPC**.

## What this platform does

- Orchestrates complex work as staged workflows: **plan -> execute -> review -> revise -> synthesize**
- Assigns each stage to the best runtime using **actual runtime capabilities**, not static labels only
- Lets runtimes **consult each other directly** through A2A-style `message/send` calls
- Supports **review / fix / re-review loops** until no new material issues remain
- Keeps the entire implementation in **Rust**

## Architecture

### Workspace layout

```text
apps/
  control-plane/   # registration, orchestration, workflow APIs
  runtime-node/    # A2A runtime endpoint, direct peer collaboration
  cli/             # operator CLI for listing runtimes and submitting workflows

crates/
  platform-domain/ # shared domain model
  platform-a2a/    # A2A data model, JSON-RPC envelope, client
  platform-core/   # registry, scheduler, orchestrator
```

### Core design

1. **Control plane**
   - accepts workflow submissions
   - persists in-memory workflow state
   - scores runtimes against stage requirements
   - dispatches stages to the selected runtime

2. **Runtime node**
   - exposes an A2A-compatible JSON-RPC endpoint at `/a2a`
   - publishes an Agent Card
   - executes assigned work according to its profile
   - directly consults peer runtimes through A2A when collaborators are attached

3. **Capability-aware scheduler**
   - applies hard filters for capability level, latency, trust tier, cost, and health
   - then scores candidates with availability and load awareness

4. **Review loop**
   - independent reviewer runtime inspects the current artifact
   - material issues are fed back into a revise stage
   - orchestration repeats until review stops finding new problems

## A2A mapping

The implementation follows the A2A model with pragmatic Rust bindings:

- **Agent Card**: `agent/getCard`
- **Send Message**: `message/send`
- **Get Task**: `tasks/get`
- **Transport**: HTTP + JSON-RPC 2.0
- **Context continuity**: `context_id` is preserved through stages and peer consultations
- **Task output**: runtime returns an A2A task containing artifacts and history

This keeps the runtime boundary interoperable while leaving room for future extensions such as streaming and push notifications.

## Security defaults

- control-plane protected by `x-platform-token`
- runtime A2A endpoint protected by `x-platform-token`
- default token source: `PLATFORM_API_TOKEN`
- token is required at process start; there is no fallback secret
- outbound runtime endpoints are allow-listed by `PLATFORM_ALLOWED_HOSTS`
- default allowed hosts: `127.0.0.1,localhost`
- workflow and metadata payload sizes are bounded
- HTTP clients and stage execution use explicit timeouts

## Capability-based assignment

Each runtime advertises measured capability signals:

- capability name
- capability level
- success rate
- median latency
- max parallelism
- runtime health
- runtime load

The control plane uses:

1. **hard filters**
   - required capability present
   - minimum level satisfied
   - trust tier satisfied
   - latency and cost budget respected

2. **weighted scoring**
   - required capability fit
   - preferred capability boost
   - availability bonus
   - load penalty
   - queue penalty

This is intentionally modeled after strong open-source scheduling patterns rather than naive round-robin dispatch.

## Open-source reference baseline

The code is an original Rust implementation, but the module boundaries and operational patterns deliberately track proven open-source approaches:

- **A2A / Agent2Agent** for agent interoperability and direct dialogue semantics
- **Kubernetes scheduler** for capability- and constraint-aware placement thinking
- **Temporal / durable workflow engines** for staged orchestration and explicit state transitions
- **OpenTelemetry / structured tracing** for workflow correlation and audit propagation
- **Tokio + Axum + Reqwest** for high-quality async Rust service boundaries

## Run locally

### One-click startup on Windows

From the project root:

```powershell
.\start-platform.cmd
```

This keeps startup in a single launcher window, builds the workspace once, then starts the control-plane and the four demo runtimes as managed background processes. If `PLATFORM_API_TOKEN`, `PLATFORM_ALLOWED_HOSTS`, or `RUST_LOG` are not already set, the script provides local defaults for the demo session.

After startup, open:

```text
http://127.0.0.1:9000/
```

Logs are written to `target\platform-runtime\logs`.

### One-click stop on Windows

From the project root:

```powershell
.\stop-platform.cmd
```

This stops only the processes started by `.\start-platform.cmd`.

### 1. Start control plane

```powershell
cargo run -p control-plane -- --bind 127.0.0.1:9000
```

### 2. Start runtimes

```powershell
cargo run -p runtime-node -- --bind 127.0.0.1:9101 --public-endpoint http://127.0.0.1:9101/a2a --control-plane http://127.0.0.1:9000 --runtime-id planner-1 --agent-id planner-1 --profile planner --auto-register
cargo run -p runtime-node -- --bind 127.0.0.1:9102 --public-endpoint http://127.0.0.1:9102/a2a --control-plane http://127.0.0.1:9000 --runtime-id builder-1 --agent-id builder-1 --profile builder --auto-register
cargo run -p runtime-node -- --bind 127.0.0.1:9103 --public-endpoint http://127.0.0.1:9103/a2a --control-plane http://127.0.0.1:9000 --runtime-id reviewer-1 --agent-id reviewer-1 --profile reviewer --auto-register
cargo run -p runtime-node -- --bind 127.0.0.1:9104 --public-endpoint http://127.0.0.1:9104/a2a --control-plane http://127.0.0.1:9000 --runtime-id synthesizer-1 --agent-id synthesizer-1 --profile synthesizer --auto-register
```

### 3. Inspect runtimes

```powershell
cargo run -p cli -- list-runtimes --control-plane http://127.0.0.1:9000
```

### 4. Submit a workflow

```powershell
cargo run -p cli -- submit-workflow --control-plane http://127.0.0.1:9000 --objective "Build a Rust platform for capability-aware agent orchestration using A2A direct dialogue" --constraints rust-only,a2a-direct-dialogue,capability-based-assignment,review-loop
```

### Optional environment variables

```powershell
$env:PLATFORM_API_TOKEN="replace-with-your-own-local-token"
$env:PLATFORM_ALLOWED_HOSTS="127.0.0.1,localhost"
```

## Runtime profiles

- `planner`
- `builder`
- `reviewer`
- `synthesizer`
- `generalist`

Each profile maps to a different measured capability set, which changes scheduling outcomes.

## Current implementation scope

Implemented now:

- capability-aware runtime registry
- A2A-like JSON-RPC runtime boundary
- direct peer consultations between runtimes
- workflow orchestration with review loops
- CLI and runnable local demo

Planned extension points already preserved in the design:

- durable store adapter instead of in-memory workflow registry
- streaming / SSE task updates
- push notifications
- external LLM runtime adapters
- richer policy, mTLS, and audit pipelines

## Build and test

```powershell
cargo check --workspace
cargo test --workspace
```
