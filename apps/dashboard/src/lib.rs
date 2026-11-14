use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Router,
    extract::{Extension, Path as AxumPath},
    http::{HeaderMap, StatusCode},
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
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl DashboardState {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(256);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        Self {
            event_log: RwLock::new(Vec::with_capacity(capacity.min(64))),
            tx,
            next_seq: AtomicUsize::new(1),
            max_events: capacity,
            shutdown,
        }
    }

    pub fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub fn shutdown_requested(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub fn push_event(
        &self,
        event_type: DashboardEventType,
        workflow_id: String,
        stage: Option<String>,
        runtime_id: Option<String>,
        message: String,
    ) {
        let mut log = self.event_log.write();
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
        log.push(event.clone());
        if log.len() > self.max_events {
            log.remove(0);
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
    async fn on_workflow_submitted(&self, workflow_id: &str) {
        self.push_event(
            DashboardEventType::Submitted,
            workflow_id.to_string(),
            None,
            None,
            format!("Workflow '{workflow_id}' submitted"),
        );
    }

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
    headers: HeaderMap,
    Extension(dashboard): Extension<std::sync::Arc<DashboardState>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let since = last_event_seq(&headers);
    let rx = dashboard.subscribe();
    let shutdown = dashboard.shutdown.subscribe();
    let existing = dashboard.events_since(since);
    let replayed_through = existing.last().map(|event| event.seq).unwrap_or(since);

    let existing_stream = futures::stream::iter(existing.into_iter().map(|event| {
        let json = serde_json::to_string(&event).unwrap_or_default();
        Ok(Event::default()
            .event(event.event_type.to_string())
            .id(event.seq.to_string())
            .data(json))
    }));

    let live_stream = stream::unfold(
        (rx, shutdown, replayed_through),
        |(mut rx, mut shutdown, mut seen)| async move {
            loop {
                if *shutdown.borrow() {
                    return None;
                }
                tokio::select! {
                    biased;
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return None;
                        }
                    }
                    received = rx.recv() => {
                        match received {
                            Ok(event) => {
                                if !accept_live_event(event.seq, seen) {
                                    continue;
                                }
                                seen = event.seq;
                                let json = serde_json::to_string(&event).unwrap_or_default();
                                let sse_event = Ok(Event::default()
                                    .event(event.event_type.to_string())
                                    .id(event.seq.to_string())
                                    .data(json));
                                return Some((sse_event, (rx, shutdown, seen)));
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
                }
            }
        },
    );

    Sse::new(existing_stream.chain(live_stream)).keep_alive(KeepAlive::default())
}

fn last_event_seq(headers: &HeaderMap) -> usize {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn accept_live_event(seq: usize, replayed_through: usize) -> bool {
    seq > replayed_through
}

#[cfg(test)]
mod tests {
    use super::{DashboardEventType, DashboardState, accept_live_event, last_event_seq};
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn events_since_returns_only_newer_sequences() {
        let state = DashboardState::new(10);
        state.push_event(
            DashboardEventType::Submitted,
            "wf-1".to_string(),
            None,
            None,
            "submitted".to_string(),
        );
        state.push_event(
            DashboardEventType::WorkflowCompleted,
            "wf-1".to_string(),
            None,
            None,
            "done".to_string(),
        );
        let all = state.events_since(0);
        assert_eq!(all.len(), 2);
        let newer = state.events_since(all[0].seq);
        assert_eq!(newer.len(), 1);
        assert_eq!(newer[0].message, "done");
    }

    #[test]
    fn last_event_header_defaults_to_zero() {
        let mut headers = HeaderMap::new();
        assert_eq!(last_event_seq(&headers), 0);
        headers.insert("last-event-id", HeaderValue::from_static("4"));
        assert_eq!(last_event_seq(&headers), 4);
        headers.insert("last-event-id", HeaderValue::from_static("nope"));
        assert_eq!(last_event_seq(&headers), 0);
    }

    #[test]
    fn shutdown_flag_is_visible_to_event_streams() {
        let state = DashboardState::new(4);
        assert!(!state.shutdown_requested());
        state.request_shutdown();
        assert!(state.shutdown_requested());
    }

    #[tokio::test]
    async fn subscription_opened_before_the_snapshot_can_dedup_the_same_event() {
        let state = DashboardState::new(10);
        let mut live = state.subscribe();
        state.push_event(
            DashboardEventType::Submitted,
            "wf-1".to_string(),
            None,
            None,
            "submitted".to_string(),
        );
        let snapshot = state.events_since(0);
        let event = live.try_recv().expect("live event");
        let replayed_through = snapshot.last().expect("snapshot").seq;
        assert_eq!(event.seq, replayed_through);
        assert!(!accept_live_event(event.seq, replayed_through));
        assert!(accept_live_event(event.seq + 1, replayed_through));
    }
}
