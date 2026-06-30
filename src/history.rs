use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::harness::{AgentEvent, AgentEventPayload};

const DEFAULT_RECENT_RUNS_PER_NAMESPACE: usize = 25;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunHistoryState {
    pub runs: Vec<RunRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: Uuid,
    pub namespace: String,
    pub prompt: String,
    pub model: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
    pub exit_status: Option<String>,
    pub tool_calls: Vec<RunToolCall>,
    pub final_result: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunToolCall {
    pub id: Option<String>,
    pub label: String,
    pub kind: String,
    pub status: Option<String>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
}

#[async_trait]
pub trait RunHistoryStore: Send + Sync {
    async fn load(&self) -> anyhow::Result<RunHistoryState>;
    async fn save(&self, state: &RunHistoryState) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct FileRunHistoryStore {
    path: PathBuf,
}

impl FileRunHistoryStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl RunHistoryStore for FileRunHistoryStore {
    async fn load(&self) -> anyhow::Result<RunHistoryState> {
        if !self.path.exists() {
            return Ok(RunHistoryState::default());
        }
        let bytes = tokio::fs::read(&self.path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn save(&self, state: &RunHistoryState) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temp_path = temp_history_path(&self.path);
        tokio::fs::write(&temp_path, serde_json::to_vec_pretty(state)?).await?;
        tokio::fs::rename(&temp_path, &self.path).await?;
        Ok(())
    }
}

pub struct RunHistory {
    records: Mutex<Vec<RunRecord>>,
    active: Mutex<HashMap<String, Uuid>>,
    store: Option<Arc<dyn RunHistoryStore>>,
    max_per_namespace: usize,
}

impl RunHistory {
    pub fn in_memory() -> Self {
        Self::from_records(Vec::new(), None, DEFAULT_RECENT_RUNS_PER_NAMESPACE)
    }

    pub async fn from_store(store: Arc<dyn RunHistoryStore>) -> anyhow::Result<Self> {
        let state = store.load().await?;
        Ok(Self::from_records(
            state.runs,
            Some(store),
            DEFAULT_RECENT_RUNS_PER_NAMESPACE,
        ))
    }

    fn from_records(
        records: Vec<RunRecord>,
        store: Option<Arc<dyn RunHistoryStore>>,
        max_per_namespace: usize,
    ) -> Self {
        Self {
            records: Mutex::new(trim_records(records, max_per_namespace)),
            active: Mutex::new(HashMap::new()),
            store,
            max_per_namespace,
        }
    }

    pub async fn start_run(
        &self,
        namespace: &str,
        prompt: &str,
        model: Option<String>,
    ) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        self.active.lock().await.insert(namespace.to_string(), id);
        let snapshot = {
            let mut records = self.records.lock().await;
            records.push(RunRecord {
                id,
                namespace: namespace.to_string(),
                prompt: prompt.to_string(),
                model,
                started_at: Utc::now(),
                completed_at: None,
                duration_ms: None,
                exit_status: None,
                tool_calls: Vec::new(),
                final_result: None,
                errors: Vec::new(),
            });
            *records = trim_records(std::mem::take(&mut *records), self.max_per_namespace);
            records.clone()
        };
        self.persist(snapshot).await?;
        Ok(id)
    }

    pub async fn record_agent_event(&self, event: &AgentEvent) -> anyhow::Result<()> {
        let Some(payload) = &event.payload else {
            return Ok(());
        };
        match payload {
            AgentEventPayload::ToolStarted { id, label, kind } => {
                self.record_tool_start(&event.namespace, id.clone(), label, kind)
                    .await
            }
            AgentEventPayload::ToolCompleted {
                id,
                label,
                kind,
                status,
                exit_code,
                error,
                ..
            } => {
                self.record_tool_completion(
                    &event.namespace,
                    id.clone(),
                    label,
                    kind,
                    status.clone(),
                    *exit_code,
                    error.clone(),
                )
                .await
            }
            AgentEventPayload::TurnCompleted {
                final_text,
                duration_ms,
                exit_status,
                ..
            } => {
                self.finish_run(
                    &event.namespace,
                    Some(final_text.clone()),
                    *duration_ms,
                    exit_status.clone(),
                )
                .await
            }
            AgentEventPayload::Error { message } => {
                self.record_error(&event.namespace, message.clone()).await
            }
            AgentEventPayload::Text { .. } => Ok(()),
        }
    }

    pub async fn record_error(&self, namespace: &str, error: String) -> anyhow::Result<()> {
        let snapshot = {
            let mut records = self.records.lock().await;
            if let Some(record) = latest_mut(&mut records, namespace) {
                record.errors.push(error);
                record.completed_at = Some(Utc::now());
                if record.exit_status.is_none() {
                    record.exit_status = Some("error".to_string());
                }
            }
            records.clone()
        };
        self.active.lock().await.remove(namespace);
        self.persist(snapshot).await
    }

    pub async fn recent_runs(&self, namespace: &str, limit: usize) -> Vec<RunRecord> {
        self.records
            .lock()
            .await
            .iter()
            .rev()
            .filter(|record| record.namespace == namespace)
            .take(limit)
            .cloned()
            .collect()
    }

    pub async fn last_run(&self, namespace: &str) -> Option<RunRecord> {
        self.recent_runs(namespace, 1).await.into_iter().next()
    }

    async fn record_tool_start(
        &self,
        namespace: &str,
        id: Option<String>,
        label: &str,
        kind: &str,
    ) -> anyhow::Result<()> {
        let snapshot = {
            let mut records = self.records.lock().await;
            if let Some(record) = latest_mut(&mut records, namespace) {
                record.tool_calls.push(RunToolCall {
                    id,
                    label: label.to_string(),
                    kind: kind.to_string(),
                    status: Some("running".to_string()),
                    exit_code: None,
                    error: None,
                });
            }
            records.clone()
        };
        self.persist(snapshot).await
    }

    async fn record_tool_completion(
        &self,
        namespace: &str,
        id: Option<String>,
        label: &str,
        kind: &str,
        status: Option<String>,
        exit_code: Option<i64>,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let snapshot = {
            let mut records = self.records.lock().await;
            if let Some(record) = latest_mut(&mut records, namespace) {
                let maybe_tool = record.tool_calls.iter_mut().rev().find(|tool| {
                    tool.id == id
                        || (tool.label == label && tool.kind == kind && tool.exit_code.is_none())
                });
                if let Some(tool) = maybe_tool {
                    tool.status = status.or_else(|| Some("done".to_string()));
                    tool.exit_code = exit_code;
                    tool.error = error;
                } else {
                    record.tool_calls.push(RunToolCall {
                        id,
                        label: label.to_string(),
                        kind: kind.to_string(),
                        status: status.or_else(|| Some("done".to_string())),
                        exit_code,
                        error,
                    });
                }
            }
            records.clone()
        };
        self.persist(snapshot).await
    }

    async fn finish_run(
        &self,
        namespace: &str,
        final_result: Option<String>,
        duration_ms: Option<u64>,
        exit_status: Option<String>,
    ) -> anyhow::Result<()> {
        let snapshot = {
            let mut records = self.records.lock().await;
            if let Some(record) = latest_mut(&mut records, namespace) {
                let completed_at = Utc::now();
                record.completed_at = Some(completed_at);
                record.duration_ms = duration_ms.or_else(|| {
                    (completed_at - record.started_at)
                        .to_std()
                        .ok()
                        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
                });
                record.exit_status = exit_status.or_else(|| Some("completed".to_string()));
                if let Some(final_result) = final_result.filter(|body| !body.trim().is_empty()) {
                    record.final_result = Some(final_result);
                }
            }
            records.clone()
        };
        self.active.lock().await.remove(namespace);
        self.persist(snapshot).await
    }

    async fn persist(&self, records: Vec<RunRecord>) -> anyhow::Result<()> {
        if let Some(store) = &self.store {
            store.save(&RunHistoryState { runs: records }).await?;
        }
        Ok(())
    }
}

fn latest_mut<'a>(records: &'a mut [RunRecord], namespace: &str) -> Option<&'a mut RunRecord> {
    records
        .iter_mut()
        .rev()
        .find(|record| record.namespace == namespace)
}

fn trim_records(records: Vec<RunRecord>, max_per_namespace: usize) -> Vec<RunRecord> {
    let mut counts = HashMap::<String, usize>::new();
    let mut kept = Vec::new();
    for record in records.into_iter().rev() {
        let count = counts.entry(record.namespace.clone()).or_default();
        if *count < max_per_namespace {
            *count += 1;
            kept.push(record);
        }
    }
    kept.reverse();
    kept
}

fn temp_history_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "run-history.json".into());
    path.with_file_name(format!("{}.{}.tmp", file_name, std::process::id()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::engine::AgentEngine;
    use crate::harness::{EventStreamKind, TokenUsage};

    use super::*;

    #[tokio::test]
    async fn run_history_records_prompt_tools_final_and_persists() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(FileRunHistoryStore::new(dir.path().join("runs.json")));
        let history = RunHistory::from_store(store.clone()).await.unwrap();

        history
            .start_run("moni", "ship it", Some("gpt-5-codex".to_string()))
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "tool-start:cargo test".to_string(),
                payload: Some(AgentEventPayload::ToolStarted {
                    id: Some("tool-1".to_string()),
                    label: "cargo test".to_string(),
                    kind: "commandExecution".to_string(),
                }),
            })
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "tool-complete:cargo test".to_string(),
                payload: Some(AgentEventPayload::ToolCompleted {
                    id: Some("tool-1".to_string()),
                    label: "cargo test".to_string(),
                    kind: "commandExecution".to_string(),
                    status: Some("completed".to_string()),
                    exit_code: Some(0),
                    stdout: Some("ok".to_string()),
                    stderr: None,
                    error: None,
                }),
            })
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Final,
                line: "done".to_string(),
                payload: Some(AgentEventPayload::TurnCompleted {
                    final_text: "done".to_string(),
                    model: Some("gpt-5-codex".to_string()),
                    duration_ms: Some(42),
                    usage: Some(TokenUsage::default()),
                    exit_status: Some("completed".to_string()),
                }),
            })
            .await
            .unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.runs.len(), 1);
        assert_eq!(loaded.runs[0].prompt, "ship it");
        assert_eq!(loaded.runs[0].model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(loaded.runs[0].tool_calls[0].exit_code, Some(0));
        assert_eq!(loaded.runs[0].final_result.as_deref(), Some("done"));
        assert_eq!(loaded.runs[0].duration_ms, Some(42));
    }

    #[tokio::test]
    async fn run_history_records_errors_and_trims_per_namespace() {
        let history = RunHistory::from_records(Vec::new(), None, 2);

        history.start_run("moni", "one", None).await.unwrap();
        history
            .record_error("moni", "first failed".to_string())
            .await
            .unwrap();
        history.start_run("moni", "two", None).await.unwrap();
        history.start_run("moni", "three", None).await.unwrap();

        let recent = history.recent_runs("moni", 10).await;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].prompt, "three");
        assert_eq!(recent[1].prompt, "two");
        assert_eq!(history.last_run("moni").await.unwrap().prompt, "three");
    }

    #[tokio::test]
    async fn run_history_records_error_payload_and_unmatched_tool_completion() {
        let history = RunHistory::in_memory();

        history.start_run("moni", "ship", None).await.unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "tool-complete:cargo test".to_string(),
                payload: Some(AgentEventPayload::ToolCompleted {
                    id: Some("tool-x".to_string()),
                    label: "cargo test".to_string(),
                    kind: "commandExecution".to_string(),
                    status: None,
                    exit_code: Some(101),
                    stdout: None,
                    stderr: Some("failed".to_string()),
                    error: Some("boom".to_string()),
                }),
            })
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "codex-error:bad json".to_string(),
                payload: Some(AgentEventPayload::Error {
                    message: "bad json".to_string(),
                }),
            })
            .await
            .unwrap();

        let run = history.last_run("moni").await.unwrap();
        assert_eq!(run.tool_calls.len(), 1);
        assert_eq!(run.tool_calls[0].status.as_deref(), Some("done"));
        assert_eq!(run.tool_calls[0].exit_code, Some(101));
        assert_eq!(run.tool_calls[0].error.as_deref(), Some("boom"));
        assert_eq!(run.exit_status.as_deref(), Some("error"));
        assert_eq!(run.errors, vec!["bad json".to_string()]);
    }

    #[tokio::test]
    async fn run_history_handles_no_active_record_and_label_tool_match() {
        let history = RunHistory::in_memory();

        history
            .record_error("missing", "ignored".to_string())
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "missing".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "tool-complete:cargo test".to_string(),
                payload: Some(AgentEventPayload::ToolCompleted {
                    id: Some("tool-x".to_string()),
                    label: "cargo test".to_string(),
                    kind: "commandExecution".to_string(),
                    status: None,
                    exit_code: Some(1),
                    stdout: None,
                    stderr: None,
                    error: Some("ignored".to_string()),
                }),
            })
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "missing".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Final,
                line: String::new(),
                payload: Some(AgentEventPayload::TurnCompleted {
                    final_text: String::new(),
                    model: None,
                    duration_ms: None,
                    usage: None,
                    exit_status: None,
                }),
            })
            .await
            .unwrap();
        assert!(history.last_run("missing").await.is_none());

        history.start_run("moni", "ship", None).await.unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "tool-start:cargo test".to_string(),
                payload: Some(AgentEventPayload::ToolStarted {
                    id: Some("tool-start".to_string()),
                    label: "cargo test".to_string(),
                    kind: "commandExecution".to_string(),
                }),
            })
            .await
            .unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Status,
                line: "tool-complete:cargo test".to_string(),
                payload: Some(AgentEventPayload::ToolCompleted {
                    id: Some("tool-complete".to_string()),
                    label: "cargo test".to_string(),
                    kind: "commandExecution".to_string(),
                    status: Some("completed".to_string()),
                    exit_code: Some(0),
                    stdout: None,
                    stderr: None,
                    error: None,
                }),
            })
            .await
            .unwrap();

        let run = history.last_run("moni").await.unwrap();
        assert_eq!(run.tool_calls.len(), 1);
        assert_eq!(run.tool_calls[0].id.as_deref(), Some("tool-start"));
        assert_eq!(run.tool_calls[0].exit_code, Some(0));
    }

    #[tokio::test]
    async fn run_history_finishes_with_duration_fallback_and_empty_final() {
        let history = RunHistory::in_memory();

        history.start_run("moni", "ship", None).await.unwrap();
        history
            .record_agent_event(&AgentEvent {
                namespace: "moni".to_string(),
                engine: AgentEngine::Codex,
                stream: EventStreamKind::Final,
                line: String::new(),
                payload: Some(AgentEventPayload::TurnCompleted {
                    final_text: " ".to_string(),
                    model: None,
                    duration_ms: None,
                    usage: None,
                    exit_status: None,
                }),
            })
            .await
            .unwrap();

        let run = history.last_run("moni").await.unwrap();
        assert_eq!(run.exit_status.as_deref(), Some("completed"));
        assert!(run.completed_at.is_some());
        assert!(run.duration_ms.is_some());
        assert!(run.final_result.is_none());
    }

    #[tokio::test]
    async fn missing_history_file_loads_default() {
        let dir = TempDir::new().unwrap();
        let store = FileRunHistoryStore::new(dir.path().join("missing.json"));
        assert_eq!(store.load().await.unwrap(), RunHistoryState::default());
    }

    #[test]
    fn temp_history_path_uses_default_name_when_path_has_no_file_name() {
        let path = temp_history_path(Path::new("/"));

        assert!(path.to_string_lossy().contains("run-history.json."));
        assert!(path.to_string_lossy().ends_with(".tmp"));
    }
}
