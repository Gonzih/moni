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
        tokio::fs::write(&self.path, serde_json::to_vec_pretty(state)?).await?;
        Ok(())
    }
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
}
