use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Router,
    extract::{Extension, Path as AxumPath},
    http::StatusCode,
    response::{
        Html, IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::get,
};
use chrono::{DateTime, Utc};
use futures::stream::{self, Stream, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::info;

// ── Types ────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DashboardEvent {
    pub seq: usize,
    pub event_type: DashboardEventType,
    pub workflow_id: String,
    pub stage: Option<String>,
    pub runtime_id: Option<String>,
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DashboardEventType {
    Submitted,
    StageStarted,
    StageCompleted,
    StageFailed,
    WorkflowCompleted,
    WorkflowFailed,
}

impl std::fmt::Display for DashboardEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Submitted => write!(f, "submitted"),
            Self::StageStarted => write!(f, "stage_started"),
            Self::StageCompleted => write!(f, "stage_completed"),
            Self::StageFailed => write!(f, "stage_failed"),
            Self::WorkflowCompleted => write!(f, "workflow_completed"),
            Self::WorkflowFailed => write!(f, "workflow_failed"),
        }
    }
}

// ── State ────────────────────────────────────────────────

pub struct DashboardState {
    event_log: RwLock<Vec<DashboardEvent>>,
    tx: broadcast::Sender<DashboardEvent>,
    next_seq: AtomicUsize,
    max_events: usize,
}

impl DashboardState {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            event_log: RwLock::new(Vec::with_capacity(capacity.min(64))),
            tx,
            next_seq: AtomicUsize::new(1),
            max_events: capacity,
        }
    }

    pub fn push_event(
        &self,
        event_type: DashboardEventType,
        workflow_id: String,
        stage: Option<String>,
        runtime_id: Option<String>,
        message: String,
    ) {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let event = DashboardEvent {
            seq,
            event_type,
            workflow_id,
            stage,
            runtime_id,
            message,
            timestamp: Utc::now(),
        };
        {
            let mut log = self.event_log.write();
            log.push(event.clone());
            if log.len() > self.max_events {
                log.remove(0);
            }
        }
        let _ = self.tx.send(event);
    }

    pub fn events_since(&self, since_seq: usize) -> Vec<DashboardEvent> {
        let log = self.event_log.read();
        if since_seq == 0 {
            return log.clone();
        }
        log.iter().filter(|e| e.seq > since_seq).cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DashboardEvent> {
        self.tx.subscribe()
    }
}

// ── Observer Implementation ─────────────────────────────

#[async_trait::async_trait]
impl platform_core::WorkflowObserver for DashboardState {
    async fn on_stage_start(&self, workflow_id: &str, stage: &str, runtime_id: &str) {
        self.push_event(
            DashboardEventType::StageStarted,
            workflow_id.to_string(),
            Some(stage.to_string()),
            Some(runtime_id.to_string()),
            format!("Stage '{stage}' started on runtime '{runtime_id}'"),
        );
    }

    async fn on_stage_complete(&self, workflow_id: &str, stage: &str, runtime_id: &str) {
        self.push_event(
            DashboardEventType::StageCompleted,
            workflow_id.to_string(),
            Some(stage.to_string()),
            Some(runtime_id.to_string()),
            format!("Stage '{stage}' completed on runtime '{runtime_id}'"),
        );
    }

    async fn on_stage_failed(&self, workflow_id: &str, stage: &str, runtime_id: &str, error: &str) {
        self.push_event(
            DashboardEventType::StageFailed,
            workflow_id.to_string(),
            Some(stage.to_string()),
            Some(runtime_id.to_string()),
            format!("Stage '{stage}' failed on runtime '{runtime_id}': {error}"),
        );
    }

    async fn on_workflow_complete(&self, workflow_id: &str) {
        self.push_event(
            DashboardEventType::WorkflowCompleted,
            workflow_id.to_string(),
            None,
            None,
            format!("Workflow '{workflow_id}' completed successfully"),
        );
    }

    async fn on_workflow_failed(&self, workflow_id: &str, error: &str) {
        self.push_event(
            DashboardEventType::WorkflowFailed,
            workflow_id.to_string(),
            None,
            None,
            format!("Workflow '{workflow_id}' failed: {error}"),
        );
    }
}

// ── Routes ───────────────────────────────────────────────

/// Returns routes that use Extension<Arc<DashboardState>>.
/// The state type parameter S matches whatever state the parent router uses.
pub fn dashboard_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/dashboard", get(dashboard_page))
        .route("/api/events", get(events_sse))
        .route("/api/events/since/{seq}", get(events_since))
}

async fn dashboard_page() -> impl IntoResponse {
    Html(include_str!("index.html"))
}

async fn events_since(
    Extension(dashboard): Extension<std::sync::Arc<DashboardState>>,
    AxumPath(seq): AxumPath<usize>,
) -> impl IntoResponse {
    let events = dashboard.events_since(seq);
    (StatusCode::OK, axum::Json(events))
}

async fn events_sse(
    Extension(dashboard): Extension<std::sync::Arc<DashboardState>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let existing = dashboard.events_since(0);
    let rx = dashboard.subscribe();

    // Existing events → stream of SSE events
    let existing_stream = futures::stream::iter(existing.into_iter().map(|event| {
        let json = serde_json::to_string(&event).unwrap_or_default();
        Ok(Event::default()
            .event(event.event_type.to_string())
            .id(event.seq.to_string())
            .data(json))
    }));

    // Live events → stream listening on broadcast channel
    let live_stream = stream::unfold(rx, |mut rx| async {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let json = serde_json::to_string(&event).unwrap_or_default();
                    let sse_event = Ok(Event::default()
                        .event(event.event_type.to_string())
                        .id(event.seq.to_string())
                        .data(json));
                    return Some((sse_event, rx));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    info!("SSE client lagged by {n} events");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return None;
                }
            }
        }
    });

    Sse::new(existing_stream.chain(live_stream)).keep_alive(KeepAlive::default())
}
