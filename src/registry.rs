use std::{collections::HashMap, sync::Arc};

use serenity::model::id::ChannelId;
use tokio::sync::RwLock;

use crate::discord::ChannelBinding;

#[derive(Debug, Clone, Default)]
pub struct BindingRegistry {
    by_channel: Arc<RwLock<HashMap<ChannelId, ChannelBinding>>>,
}

impl BindingRegistry {
    pub fn new(bindings: impl IntoIterator<Item = ChannelBinding>) -> anyhow::Result<Self> {
        let mut by_channel = HashMap::new();
        for binding in bindings {
            let channel_id = binding.channel_id.parse::<u64>()?;
            by_channel.insert(ChannelId::new(channel_id), binding);
        }
        Ok(Self {
            by_channel: Arc::new(RwLock::new(by_channel)),
        })
    }

    pub async fn get_by_channel(&self, channel_id: ChannelId) -> Option<ChannelBinding> {
        self.by_channel.read().await.get(&channel_id).cloned()
    }

    pub async fn channel_for_namespace(&self, namespace: &str) -> Option<ChannelId> {
        self.by_channel
            .read()
            .await
            .iter()
            .find_map(|(channel_id, binding)| {
                (binding.namespace == namespace).then_some(*channel_id)
            })
    }

    pub async fn upsert(&self, binding: ChannelBinding) -> anyhow::Result<()> {
        let channel_id = binding.channel_id.parse::<u64>()?;
        self.by_channel
            .write()
            .await
            .insert(ChannelId::new(channel_id), binding);
        Ok(())
    }

    pub async fn all(&self) -> Vec<ChannelBinding> {
        self.by_channel.read().await.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(channel_id: &str, namespace: &str) -> ChannelBinding {
        ChannelBinding {
            channel_id: channel_id.to_string(),
            namespace: namespace.to_string(),
            repo_url: "repo".to_string(),
        }
    }

    #[tokio::test]
    async fn registry_gets_by_channel() {
        let registry = BindingRegistry::new([binding("1", "moni")]).unwrap();
        assert_eq!(
            registry
                .get_by_channel(ChannelId::new(1))
                .await
                .unwrap()
                .namespace,
            "moni"
        );
    }

    #[tokio::test]
    async fn registry_finds_channel_for_namespace() {
        let registry = BindingRegistry::new([binding("1", "moni")]).unwrap();
        assert_eq!(
            registry.channel_for_namespace("moni").await,
            Some(ChannelId::new(1))
        );
    }

    #[tokio::test]
    async fn registry_upsert_replaces_channel_binding() {
        let registry = BindingRegistry::new([binding("1", "old")]).unwrap();
        registry.upsert(binding("1", "new")).await.unwrap();
        assert_eq!(
            registry
                .get_by_channel(ChannelId::new(1))
                .await
                .unwrap()
                .namespace,
            "new"
        );
    }

    #[tokio::test]
    async fn registry_all_lists_bindings() {
        let registry = BindingRegistry::new([binding("1", "moni"), binding("2", "ops")]).unwrap();
        assert_eq!(registry.all().await.len(), 2);
    }

    #[test]
    fn registry_rejects_invalid_channel_id() {
        assert!(BindingRegistry::new([binding("bad", "moni")]).is_err());
    }
}
