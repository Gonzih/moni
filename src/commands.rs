#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandAction {
    Register { namespace: String, repo_url: String },
    Reset,
    Clear,
    Compact,
    Status,
    SetModel { model: String },
    VoiceStatus,
    CronAdd { schedule: String, message: String },
    CronList,
    CronPause { id: String },
    CronResume { id: String },
    CronDelete { id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommand {
    pub namespace: String,
    pub action: CommandAction,
}

pub fn parse_command(
    namespace: impl Into<String>,
    body: &str,
) -> anyhow::Result<Option<ParsedCommand>> {
    let namespace = namespace.into();
    let trimmed = body.trim();
    if !trimmed.starts_with('/') {
        return Ok(None);
    }

    let mut parts = trimmed.split_whitespace();
    let command = parts.next().expect("trimmed command starts with slash");

    let action = match command {
        "/register" => {
            let namespace = parts
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing namespace"))?
                .to_string();
            let repo_url = parts
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing repo url"))?
                .to_string();
            CommandAction::Register {
                namespace,
                repo_url,
            }
        }
        "/reset" => CommandAction::Reset,
        "/clear" => CommandAction::Clear,
        "/compact" => CommandAction::Compact,
        "/status" => CommandAction::Status,
        "/voice" => parse_voice_command(parts.collect::<Vec<_>>())?,
        "/model" => CommandAction::SetModel {
            model: parts.collect::<Vec<_>>().join(" "),
        },
        "/cron" => parse_cron_command(parts.collect::<Vec<_>>())?,
        _ => return Ok(None),
    };
    if let CommandAction::SetModel { model } = &action {
        if model.trim().is_empty() {
            anyhow::bail!("missing model");
        }
    }

    Ok(Some(ParsedCommand { namespace, action }))
}

fn parse_voice_command(parts: Vec<&str>) -> anyhow::Result<CommandAction> {
    let Some(subcommand) = parts.first().copied() else {
        anyhow::bail!("missing voice subcommand");
    };

    match subcommand {
        "status" => Ok(CommandAction::VoiceStatus),
        other => anyhow::bail!("unknown voice subcommand `{other}`"),
    }
}

fn parse_cron_command(parts: Vec<&str>) -> anyhow::Result<CommandAction> {
    let Some(subcommand) = parts.first().copied() else {
        anyhow::bail!("missing cron subcommand");
    };

    match subcommand {
        "list" => Ok(CommandAction::CronList),
        "pause" => Ok(CommandAction::CronPause {
            id: required_arg(&parts, 1, "cron id")?.to_string(),
        }),
        "resume" => Ok(CommandAction::CronResume {
            id: required_arg(&parts, 1, "cron id")?.to_string(),
        }),
        "delete" => Ok(CommandAction::CronDelete {
            id: required_arg(&parts, 1, "cron id")?.to_string(),
        }),
        "add" => {
            if parts.len() < 7 {
                anyhow::bail!("cron add expects five schedule fields and a message");
            }
            let schedule = parts[1..6].join(" ");
            let message = parts[6..].join(" ");
            Ok(CommandAction::CronAdd { schedule, message })
        }
        other => anyhow::bail!("unknown cron subcommand `{other}`"),
    }
}

fn required_arg<'a>(parts: &'a [&str], index: usize, name: &str) -> anyhow::Result<&'a str> {
    parts
        .get(index)
        .copied()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_command_returns_none() {
        assert!(parse_command("moni", "hello").unwrap().is_none());
    }

    #[test]
    fn unknown_command_returns_none() {
        assert!(parse_command("moni", "/unknown").unwrap().is_none());
    }

    #[test]
    fn parses_reset() {
        assert_eq!(
            parse_command("moni", "/reset").unwrap().unwrap().action,
            CommandAction::Reset
        );
    }

    #[test]
    fn parses_register() {
        assert_eq!(
            parse_command("ignored", "/register moni https://example.com/repo")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::Register {
                namespace: "moni".to_string(),
                repo_url: "https://example.com/repo".to_string()
            }
        );
    }

    #[test]
    fn register_requires_namespace() {
        assert!(parse_command("moni", "/register").is_err());
    }

    #[test]
    fn register_requires_repo_url() {
        assert!(parse_command("moni", "/register moni").is_err());
    }

    #[test]
    fn parses_clear() {
        assert_eq!(
            parse_command("moni", "/clear").unwrap().unwrap().action,
            CommandAction::Clear
        );
    }

    #[test]
    fn parses_compact() {
        assert_eq!(
            parse_command("moni", "/compact").unwrap().unwrap().action,
            CommandAction::Compact
        );
    }

    #[test]
    fn parses_status() {
        assert_eq!(
            parse_command("moni", "/status").unwrap().unwrap().action,
            CommandAction::Status
        );
    }

    #[test]
    fn parses_voice_status() {
        assert_eq!(
            parse_command("moni", "/voice status")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::VoiceStatus
        );
    }

    #[test]
    fn voice_requires_known_subcommand() {
        assert!(parse_command("moni", "/voice").is_err());
        assert!(parse_command("moni", "/voice nope").is_err());
    }

    #[test]
    fn parses_model_selection() {
        assert_eq!(
            parse_command("moni", "/model prompt")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::SetModel {
                model: "prompt".to_string()
            }
        );
    }

    #[test]
    fn model_selection_requires_model() {
        assert!(parse_command("moni", "/model").is_err());
        assert!(parse_command("moni", "/model   ").is_err());
    }

    #[test]
    fn command_preserves_namespace() {
        assert_eq!(
            parse_command("ops", "/reset").unwrap().unwrap().namespace,
            "ops"
        );
    }

    #[test]
    fn parses_cron_list() {
        assert_eq!(
            parse_command("moni", "/cron list").unwrap().unwrap().action,
            CommandAction::CronList
        );
    }

    #[test]
    fn parses_cron_pause() {
        assert_eq!(
            parse_command("moni", "/cron pause c1")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::CronPause {
                id: "c1".to_string()
            }
        );
    }

    #[test]
    fn parses_cron_resume() {
        assert_eq!(
            parse_command("moni", "/cron resume c1")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::CronResume {
                id: "c1".to_string()
            }
        );
    }

    #[test]
    fn parses_cron_delete() {
        assert_eq!(
            parse_command("moni", "/cron delete c1")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::CronDelete {
                id: "c1".to_string()
            }
        );
    }

    #[test]
    fn parses_cron_add() {
        assert_eq!(
            parse_command("moni", "/cron add */5 * * * * run this now")
                .unwrap()
                .unwrap()
                .action,
            CommandAction::CronAdd {
                schedule: "*/5 * * * *".to_string(),
                message: "run this now".to_string()
            }
        );
    }

    #[test]
    fn cron_add_requires_message() {
        assert!(parse_command("moni", "/cron add * * * * *").is_err());
    }

    #[test]
    fn cron_pause_requires_id() {
        assert!(parse_command("moni", "/cron pause").is_err());
    }

    #[test]
    fn cron_requires_subcommand() {
        let err = parse_command("moni", "/cron").unwrap_err();
        assert!(err.to_string().contains("missing cron subcommand"));
    }

    #[test]
    fn cron_unknown_subcommand_errors() {
        assert!(parse_command("moni", "/cron nope").is_err());
    }

    #[test]
    fn trims_command_whitespace() {
        assert_eq!(
            parse_command("moni", "  /reset  ").unwrap().unwrap().action,
            CommandAction::Reset
        );
    }
}
