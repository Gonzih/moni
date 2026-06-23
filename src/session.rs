use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    engine::EngineConfig,
    harness::{AgentEventStream, AgentHarness, ProcessAgentHarness, StopReason},
    output::{OutputSink, event_to_output_message},
    queue::QueuedPrompt,
};

#[async_trait]
pub trait EngineConfigResolver: Send + Sync {
    async fn config_for(&self, namespace: &str) -> anyhow::Result<EngineConfig>;
}

#[derive(Debug, Clone)]
pub struct StaticEngineConfigResolver {
    config: EngineConfig,
}

impl StaticEngineConfigResolver {
    pub fn new(config: EngineConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl EngineConfigResolver for StaticEngineConfigResolver {
    async fn config_for(&self, _namespace: &str) -> anyhow::Result<EngineConfig> {
        Ok(self.config.clone())
    }
}

struct Session {
    harness: ProcessAgentHarness,
    forward_task: JoinHandle<()>,
}

pub struct SessionManager {
    workspace_root: PathBuf,
    resolver: Arc<dyn EngineConfigResolver>,
    output: Arc<dyn OutputSink>,
    sessions: Mutex<HashMap<String, Session>>,
}

impl SessionManager {
    pub fn new(
        workspace_root: impl Into<PathBuf>,
        resolver: Arc<dyn EngineConfigResolver>,
        output: Arc<dyn OutputSink>,
    ) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            resolver,
            output,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn handle_prompt(&self, prompt: QueuedPrompt) -> anyhow::Result<()> {
        let mut sessions = self.sessions.lock().await;
        let session = self
            .ensure_session_locked(&mut sessions, &prompt.namespace)
            .await?;
        session.harness.reap_if_exited().await?;
        if !session.harness.status().running {
            session.harness.start().await?;
        }
        session.harness.send(&prompt.body).await?;
        Ok(())
    }

    pub async fn compact(&self, namespace: &str) -> anyhow::Result<()> {
        self.handle_prompt(QueuedPrompt::new(
            namespace,
            None,
            "/compact",
            "command:compact",
        ))
        .await
    }

    pub async fn stop_namespace(
        &self,
        namespace: &str,
        reason: StopReason,
    ) -> anyhow::Result<bool> {
        let mut sessions = self.sessions.lock().await;
        let Some(mut session) = sessions.remove(namespace) else {
            return Ok(false);
        };
        session.harness.stop(reason).await?;
        session.forward_task.abort();
        Ok(true)
    }

    pub async fn active_namespaces(&self) -> Vec<String> {
        self.sessions.lock().await.keys().cloned().collect()
    }

    fn workspace_path(&self, namespace: &str) -> PathBuf {
        self.workspace_root.join(namespace)
    }

    async fn ensure_session_locked<'a>(
        &self,
        sessions: &'a mut HashMap<String, Session>,
        namespace: &str,
    ) -> anyhow::Result<&'a mut Session> {
        if !sessions.contains_key(namespace) {
            let workspace_path = self.workspace_path(namespace);
            tokio::fs::create_dir_all(&workspace_path).await?;
            let config = self.resolver.config_for(namespace).await?;
            let (harness, events) =
                ProcessAgentHarness::new(namespace.to_string(), workspace_path, config);
            let forward_task = spawn_output_forwarder(events, self.output.clone());
            sessions.insert(
                namespace.to_string(),
                Session {
                    harness,
                    forward_task,
                },
            );
        }

        Ok(sessions.get_mut(namespace).expect("session inserted"))
    }
}

fn spawn_output_forwarder(
    mut events: AgentEventStream,
    output: Arc<dyn OutputSink>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Some(message) = event_to_output_message(event) {
                let _ = output.send(message).await;
            }
        }
    })
}

pub fn namespace_workspace(root: impl AsRef<Path>, namespace: &str) -> PathBuf {
    root.as_ref().join(namespace)
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};

    use crate::{
        engine::{AgentEngine, EngineConfig},
        output::InMemoryOutputSink,
    };

    use super::*;

    fn write_mock_agent(path: &Path) {
        fs::write(
            path,
            r#"#!/usr/bin/env bash
while IFS= read -r line; do
  echo "reply:$line"
done
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    async fn wait_for_messages(
        sink: &InMemoryOutputSink,
        count: usize,
    ) -> Vec<crate::output::OutputMessage> {
        timeout(Duration::from_secs(5), async {
            loop {
                let messages = sink.messages().await;
                if messages.len() >= count {
                    return messages;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }

    fn manager(dir: &TempDir, bin: PathBuf, output: InMemoryOutputSink) -> SessionManager {
        let resolver = Arc::new(StaticEngineConfigResolver::new(EngineConfig::new(
            AgentEngine::Claude,
            bin,
        )));
        SessionManager::new(dir.path().join("workspaces"), resolver, Arc::new(output))
    }

    #[test]
    fn namespace_workspace_joins_root_and_namespace() {
        assert_eq!(
            namespace_workspace("/tmp/root", "moni"),
            PathBuf::from("/tmp/root/moni")
        );
    }

    #[tokio::test]
    async fn handle_prompt_starts_session_and_forwards_output() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "hello", "test"))
            .await
            .unwrap();

        let messages = wait_for_messages(&output, 1).await;
        assert_eq!(messages[0].namespace, "moni");
        assert_eq!(messages[0].body, "reply:hello");
    }

    #[tokio::test]
    async fn handle_prompt_reuses_existing_session() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();
        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "two", "test"))
            .await
            .unwrap();

        let messages = wait_for_messages(&output, 2).await;
        assert!(messages.iter().any(|message| message.body == "reply:one"));
        assert!(messages.iter().any(|message| message.body == "reply:two"));
        assert_eq!(manager.active_namespaces().await, vec!["moni".to_string()]);
    }

    #[tokio::test]
    async fn stop_namespace_removes_active_session() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output);

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();

        assert!(
            manager
                .stop_namespace("moni", StopReason::Reset)
                .await
                .unwrap()
        );
        assert!(manager.active_namespaces().await.is_empty());
    }

    #[tokio::test]
    async fn stop_missing_namespace_returns_false() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output);

        assert!(
            !manager
                .stop_namespace("missing", StopReason::Reset)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn compact_sends_compact_prompt() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output.clone());

        manager.compact("moni").await.unwrap();

        let messages = wait_for_messages(&output, 1).await;
        assert_eq!(messages[0].body, "reply:/compact");
    }

    #[tokio::test]
    async fn handle_prompt_creates_namespace_workspace() {
        let dir = TempDir::new().unwrap();
        let bin = dir.path().join("mock-agent");
        write_mock_agent(&bin);
        let output = InMemoryOutputSink::default();
        let manager = manager(&dir, bin, output);

        manager
            .handle_prompt(QueuedPrompt::new("moni", None, "one", "test"))
            .await
            .unwrap();

        assert!(dir.path().join("workspaces/moni").is_dir());
    }
}
