use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentEngine {
    Claude,
    Codex,
}

impl AgentEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub engine: AgentEngine,
    pub command: PathBuf,
    pub args: Vec<String>,
}

impl EngineConfig {
    pub fn new(engine: AgentEngine, command: impl Into<PathBuf>) -> Self {
        Self {
            engine,
            command: command.into(),
            args: Vec::new(),
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_engine_string_is_stable() {
        assert_eq!(AgentEngine::Claude.as_str(), "claude");
    }

    #[test]
    fn codex_engine_string_is_stable() {
        assert_eq!(AgentEngine::Codex.as_str(), "codex");
    }

    #[test]
    fn engine_serializes_as_kebab_case() {
        assert_eq!(
            serde_json::to_string(&AgentEngine::Claude).unwrap(),
            "\"claude\""
        );
        assert_eq!(
            serde_json::to_string(&AgentEngine::Codex).unwrap(),
            "\"codex\""
        );
    }

    #[test]
    fn engine_deserializes_from_kebab_case() {
        assert_eq!(
            serde_json::from_str::<AgentEngine>("\"claude\"").unwrap(),
            AgentEngine::Claude
        );
        assert_eq!(
            serde_json::from_str::<AgentEngine>("\"codex\"").unwrap(),
            AgentEngine::Codex
        );
    }

    #[test]
    fn engine_config_defaults_to_no_args() {
        let config = EngineConfig::new(AgentEngine::Claude, "/bin/echo");
        assert_eq!(config.engine, AgentEngine::Claude);
        assert_eq!(config.command, PathBuf::from("/bin/echo"));
        assert!(config.args.is_empty());
    }

    #[test]
    fn engine_config_collects_args_in_order() {
        let config = EngineConfig::new(AgentEngine::Codex, "/bin/echo")
            .with_args(["--one", "two", "--three"]);
        assert_eq!(config.args, vec!["--one", "two", "--three"]);
    }
}
