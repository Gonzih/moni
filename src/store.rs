use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{cron::CronTask, discord::ChannelBinding};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoniState {
    pub bindings: Vec<ChannelBinding>,
    pub cron_tasks: Vec<CronTask>,
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn load(&self) -> anyhow::Result<MoniState>;
    async fn save(&self, state: &MoniState) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct FileStateStore {
    path: PathBuf,
}

impl FileStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl StateStore for FileStateStore {
    async fn load(&self) -> anyhow::Result<MoniState> {
        if !self.path.exists() {
            return Ok(MoniState::default());
        }
        let bytes = tokio::fs::read(&self.path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn save(&self, state: &MoniState) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temp_path = temp_state_path(&self.path);
        tokio::fs::write(&temp_path, serde_json::to_vec_pretty(state)?).await?;
        tokio::fs::rename(&temp_path, &self.path).await?;
        Ok(())
    }
}

fn temp_state_path(path: &std::path::Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "state.json".into());
    path.with_file_name(format!("{}.{}.tmp", file_name, std::process::id()))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn binding() -> ChannelBinding {
        ChannelBinding {
            channel_id: "1".to_string(),
            namespace: "moni".to_string(),
            repo_url: "repo".to_string(),
        }
    }

    #[tokio::test]
    async fn missing_state_file_loads_default() {
        let dir = TempDir::new().unwrap();
        let store = FileStateStore::new(dir.path().join("state.json"));
        assert_eq!(store.load().await.unwrap(), MoniState::default());
    }

    #[tokio::test]
    async fn state_round_trips_file() {
        let dir = TempDir::new().unwrap();
        let store = FileStateStore::new(dir.path().join("state.json"));
        let state = MoniState {
            bindings: vec![binding()],
            cron_tasks: Vec::new(),
        };
        store.save(&state).await.unwrap();
        assert_eq!(store.load().await.unwrap(), state);
    }

    #[tokio::test]
    async fn save_creates_parent_directories() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested/state.json");
        let store = FileStateStore::new(&path);
        store.save(&MoniState::default()).await.unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn save_does_not_leave_temp_file_after_success() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.json");
        let store = FileStateStore::new(&path);
        store.save(&MoniState::default()).await.unwrap();

        let temp_path = temp_state_path(&path);
        assert!(path.exists());
        assert!(!temp_path.exists());
    }
}
