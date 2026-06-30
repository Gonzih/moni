use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

use tempfile::TempDir;

fn moni_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_moni"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MONI_")
            || matches!(
                key.as_str(),
                "WHISPER_BIN" | "FFMPEG_BIN" | "CURL_BIN" | "WHISPER_MODEL" | "WHISPER_MODEL_DIR"
            )
        {
            command.env_remove(key);
        }
    }
    command.env("RUST_LOG", "off");
    command
}

fn write_executable(path: &Path) {
    fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

#[test]
fn exits_cleanly_without_discord_token() {
    let output = moni_command().output().unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("set MONI_DISCORD_TOKEN"));
}

#[test]
fn exits_cleanly_with_invalid_rust_log_fallback() {
    let mut command = moni_command();
    command.env("RUST_LOG", "[");

    let output = command.output().unwrap();

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("set MONI_DISCORD_TOKEN"));
}

#[test]
fn rejects_unknown_engine_before_runtime_start() {
    let mut command = moni_command();
    command
        .env("MONI_DISCORD_TOKEN", "token")
        .env("MONI_ENGINE", "unknown");

    let output = command.output().unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown agent engine"));
}

#[test]
fn rejects_invalid_channel_binding_before_runtime_start() {
    let mut command = moni_command();
    command
        .env("MONI_DISCORD_TOKEN", "token")
        .env("MONI_CHANNELS", "not-a-binding");

    let output = command.output().unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid channel binding"));
}

#[test]
fn builds_runtime_config_then_fails_to_connect_to_invalid_nats_url() {
    let mut command = moni_command();
    command
        .env("MONI_DISCORD_TOKEN", "token")
        .env("MONI_CHANNELS", "1=moni=https://github.com/Gonzih/moni")
        .env("MONI_NATS_URL", "not-a-url")
        .env("MONI_CRON_TICK_SECONDS", "0")
        .env("MONI_WORKSPACE_ROOT", "/tmp/moni-workspace-test")
        .env("MONI_ENGINE", "codex")
        .env("MONI_CODEX_APP_SERVER", "true")
        .env("MONI_AGENT_ARGS", "")
        .env("MONI_ALLOWED_USER_IDS", "42")
        .env("MONI_DEFAULT_CATEGORY_ID", "")
        .env("WHISPER_BIN", "/missing/whisper")
        .env("FFMPEG_BIN", "/missing/ffmpeg")
        .env("CURL_BIN", "/missing/curl")
        .env("RUST_LOG", "off");

    let output = command.output().unwrap();

    assert!(!output.status.success());
}

#[test]
fn loads_state_and_configures_voice_before_nats_connect() {
    let dir = TempDir::new().unwrap();
    let state_path = dir.path().join("state.json");
    fs::write(
        &state_path,
        r#"{
  "bindings": [
    {
      "channel_id": "2",
      "namespace": "state",
      "repo_url": "https://github.com/Gonzih/moni"
    }
  ],
  "cron_tasks": []
}"#,
    )
    .unwrap();
    let whisper = dir.path().join("whisper-cli");
    let ffmpeg = dir.path().join("ffmpeg");
    let curl = dir.path().join("curl");
    let model = dir.path().join("ggml-small.en.bin");
    write_executable(&whisper);
    write_executable(&ffmpeg);
    write_executable(&curl);
    fs::write(&model, "model").unwrap();

    let mut command = moni_command();
    command
        .env("MONI_DISCORD_TOKEN", "token")
        .env("MONI_CHANNELS", "1=moni=https://github.com/Gonzih/moni")
        .env("MONI_STATE_PATH", &state_path)
        .env("MONI_NATS_URL", "not-a-url")
        .env("MONI_WORKSPACE_ROOT", dir.path().join("workspace"))
        .env("MONI_ENGINE", "codex")
        .env("MONI_CODEX_APP_SERVER", "false")
        .env("MONI_AGENT_ARGS", "--json")
        .env("WHISPER_BIN", &whisper)
        .env("FFMPEG_BIN", &ffmpeg)
        .env("CURL_BIN", &curl)
        .env("WHISPER_MODEL", &model);

    let output = command.output().unwrap();

    assert!(!output.status.success());
}
