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
