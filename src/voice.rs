use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use tokio::process::Command;
use uuid::Uuid;

const WHISPER_MODEL_NAMES: &[&str] = &[
    "ggml-small.en.bin",
    "ggml-small.bin",
    "ggml-base.en.bin",
    "ggml-base.bin",
    "ggml-tiny.en.bin",
    "ggml-tiny.bin",
];
const DEFAULT_VOICE_PROMPT_TEMPLATE: &str =
    "[voice note - transcription may contain typos]: {content}";
const DEFAULT_MAX_AUDIO_BYTES: u64 = 25 * 1024 * 1024;
const DEFAULT_MAX_DURATION_SECONDS: u64 = 10 * 60;
const WAV_BYTES_PER_SECOND: u64 = 16_000 * 2;

#[derive(Debug, Clone)]
pub struct VoiceTranscriber {
    whisper_bin: PathBuf,
    ffmpeg_bin: PathBuf,
    curl_bin: PathBuf,
    model: PathBuf,
    temp_dir: PathBuf,
    prompt_template: String,
    max_audio_bytes: u64,
    max_duration_seconds: u64,
}

impl VoiceTranscriber {
    pub fn from_env() -> anyhow::Result<Self> {
        let home = dirs_next::home_dir();
        let whisper_bin = find_existing_path(env_candidates(
            "WHISPER_BIN",
            &[
                "/opt/homebrew/bin/whisper-cli",
                "/opt/homebrew/bin/whisper-cpp",
                "/usr/local/bin/whisper-cli",
                "/usr/local/bin/whisper-cpp",
                "/opt/homebrew/bin/whisper",
            ],
        ))
        .ok_or_else(|| {
            anyhow::anyhow!("whisper-cpp not found - install with: brew install whisper-cpp")
        })?;
        let ffmpeg_bin = find_existing_path(env_candidates(
            "FFMPEG_BIN",
            &[
                "/opt/homebrew/bin/ffmpeg",
                "/usr/local/bin/ffmpeg",
                "/usr/bin/ffmpeg",
            ],
        ))
        .ok_or_else(|| anyhow::anyhow!("ffmpeg not found - install with: brew install ffmpeg"))?;
        let curl_bin = find_existing_path(env_candidates(
            "CURL_BIN",
            &["/usr/bin/curl", "/opt/homebrew/bin/curl"],
        ))
        .ok_or_else(|| anyhow::anyhow!("curl not found"))?;

        let mut model_candidates = env_candidates(
            "WHISPER_MODEL",
            &[
                "/opt/homebrew/share/whisper-cpp/ggml-small.en.bin",
                "/opt/homebrew/share/whisper-cpp/ggml-small.bin",
                "/opt/homebrew/share/whisper-cpp/ggml-base.en.bin",
                "/opt/homebrew/share/whisper-cpp/ggml-base.bin",
                "/opt/homebrew/share/whisper-cpp",
                "/usr/local/share/whisper-cpp",
            ],
        );
        if let Ok(model_dir) = env::var("WHISPER_MODEL_DIR") {
            model_candidates.insert(0, PathBuf::from(model_dir));
        }
        if let Some(home) = home {
            model_candidates.extend([
                home.join(".local/share/whisper-cpp/ggml-small.en.bin"),
                home.join(".local/share/whisper-cpp/ggml-base.en.bin"),
                home.join(".local/share/whisper-cpp"),
                home.join("Library/Application Support/whisper-cpp"),
            ]);
        }
        let model = find_model(model_candidates).ok_or_else(|| {
            anyhow::anyhow!("No whisper model found - set WHISPER_MODEL or WHISPER_MODEL_DIR")
        })?;

        Ok(Self {
            whisper_bin,
            ffmpeg_bin,
            curl_bin,
            model,
            temp_dir: env::temp_dir(),
            prompt_template: env::var("MONI_VOICE_PROMPT_TEMPLATE")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_VOICE_PROMPT_TEMPLATE.to_string()),
            max_audio_bytes: env_positive_u64("MONI_VOICE_MAX_BYTES", DEFAULT_MAX_AUDIO_BYTES)?,
            max_duration_seconds: env_positive_u64(
                "MONI_VOICE_MAX_DURATION_SECONDS",
                DEFAULT_MAX_DURATION_SECONDS,
            )?,
        })
    }

    pub fn new(
        whisper_bin: impl Into<PathBuf>,
        ffmpeg_bin: impl Into<PathBuf>,
        curl_bin: impl Into<PathBuf>,
        model: impl Into<PathBuf>,
        temp_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            whisper_bin: whisper_bin.into(),
            ffmpeg_bin: ffmpeg_bin.into(),
            curl_bin: curl_bin.into(),
            model: model.into(),
            temp_dir: temp_dir.into(),
            prompt_template: DEFAULT_VOICE_PROMPT_TEMPLATE.to_string(),
            max_audio_bytes: DEFAULT_MAX_AUDIO_BYTES,
            max_duration_seconds: DEFAULT_MAX_DURATION_SECONDS,
        }
    }

    pub fn status_report(&self) -> String {
        format!(
            "voice transcription configured\nwhisper.cpp: {}\nffmpeg: {}\ncurl: {}\nmodel: {}\nmax bytes: {}\nmax duration: {}s",
            path_status(&self.whisper_bin),
            path_status(&self.ffmpeg_bin),
            path_status(&self.curl_bin),
            path_status(&self.model),
            self.max_audio_bytes,
            self.max_duration_seconds
        )
    }

    pub fn with_prompt_template(mut self, template: impl Into<String>) -> Self {
        self.prompt_template = template.into();
        self
    }

    pub fn with_guardrails(mut self, max_audio_bytes: u64, max_duration_seconds: u64) -> Self {
        self.max_audio_bytes = max_audio_bytes;
        self.max_duration_seconds = max_duration_seconds;
        self
    }

    pub fn validate_attachment_size(&self, size: Option<u64>) -> anyhow::Result<()> {
        if let Some(size) = size {
            validate_audio_size(size, self.max_audio_bytes)?;
        }
        Ok(())
    }

    pub fn build_prompt(&self, caption: &str, transcript: &str) -> String {
        let content = voice_prompt_content(caption, transcript);
        self.prompt_template.replace("{content}", &content)
    }

    pub async fn transcribe_url(&self, url: &str) -> anyhow::Result<String> {
        fs::create_dir_all(&self.temp_dir)?;
        let stem = self.temp_dir.join(format!("moni-voice-{}", Uuid::new_v4()));
        let audio_path = stem.with_extension(audio_extension_from_url(url));
        let wav_path = stem.with_extension("wav");

        let result = async {
            run_command(
                &self.curl_bin,
                &["-L", "-f", "-sS", "-o", path_arg(&audio_path).as_str(), url],
                Duration::from_secs(120),
            )
            .await
            .map_err(|err| anyhow::anyhow!("audio download failed: {err}"))?;
            validate_audio_size(fs::metadata(&audio_path)?.len(), self.max_audio_bytes)?;

            run_command(
                &self.ffmpeg_bin,
                &[
                    "-y",
                    "-i",
                    path_arg(&audio_path).as_str(),
                    "-ar",
                    "16000",
                    "-ac",
                    "1",
                    "-c:a",
                    "pcm_s16le",
                    path_arg(&wav_path).as_str(),
                ],
                Duration::from_secs(120),
            )
            .await
            .map_err(|err| anyhow::anyhow!("ffmpeg conversion failed: {err}"))?;
            validate_wav_duration(&wav_path, self.max_duration_seconds)?;

            let lang = if self.model.to_string_lossy().contains(".en.") {
                "en"
            } else {
                "auto"
            };
            run_command(
                &self.whisper_bin,
                &[
                    "-m",
                    path_arg(&self.model).as_str(),
                    "-f",
                    path_arg(&wav_path).as_str(),
                    "--no-timestamps",
                    "-l",
                    lang,
                    "--output-txt",
                ],
                Duration::from_secs(10 * 60),
            )
            .await
            .map_err(|err| anyhow::anyhow!("whisper-cpp failed: {err}"))?;

            let raw = read_whisper_text(&wav_path)?;
            Ok(clean_transcript(&raw))
        }
        .await;

        let _ = fs::remove_file(&audio_path);
        let _ = fs::remove_file(&wav_path);
        let _ = fs::remove_file(format!("{}.txt", wav_path.to_string_lossy()));
        let _ = fs::remove_file(wav_path.with_extension("txt"));
        result
    }
}

fn path_status(path: &Path) -> String {
    let state = if path.is_file() {
        "ok"
    } else if path.exists() {
        "not a file"
    } else {
        "missing"
    };
    format!("{state} ({})", path.display())
}

pub fn is_audio_attachment(filename: &str, content_type: Option<&str>) -> bool {
    if content_type
        .map(|content_type| content_type.to_ascii_lowercase().starts_with("audio/"))
        .unwrap_or(false)
    {
        return true;
    }

    let lower = filename.to_ascii_lowercase();
    [".ogg", ".oga", ".m4a", ".mp3", ".wav", ".webm", ".opus"]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
}

pub fn build_voice_prompt(caption: &str, transcript: &str) -> String {
    DEFAULT_VOICE_PROMPT_TEMPLATE.replace("{content}", &voice_prompt_content(caption, transcript))
}

fn voice_prompt_content(caption: &str, transcript: &str) -> String {
    let caption = caption.trim();
    let transcript = transcript.trim();
    if caption.is_empty() {
        transcript.to_string()
    } else {
        format!("{caption}\n\n{transcript}")
    }
}

fn env_positive_u64(key: &str, default: u64) -> anyhow::Result<u64> {
    let Ok(raw) = env::var(key) else {
        return Ok(default);
    };
    let parsed = raw
        .parse::<u64>()
        .map_err(|err| anyhow::anyhow!("{key} must be a positive integer: {err}"))?;
    if parsed == 0 {
        anyhow::bail!("{key} must be greater than zero");
    }
    Ok(parsed)
}

fn validate_audio_size(size: u64, max_audio_bytes: u64) -> anyhow::Result<()> {
    if size > max_audio_bytes {
        anyhow::bail!("voice attachment is too large: {size} bytes exceeds {max_audio_bytes}");
    }
    Ok(())
}

fn validate_wav_duration(path: &Path, max_duration_seconds: u64) -> anyhow::Result<()> {
    let size = fs::metadata(path)?.len();
    let max_wav_bytes = max_duration_seconds
        .saturating_mul(WAV_BYTES_PER_SECOND)
        .saturating_add(4096);
    if size > max_wav_bytes {
        anyhow::bail!(
            "voice attachment is too long: converted audio exceeds {max_duration_seconds}s"
        );
    }
    Ok(())
}

fn env_candidates(key: &str, defaults: &[&str]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(value) = env::var(key) {
        if !value.trim().is_empty() {
            paths.push(PathBuf::from(value));
            return paths;
        }
    }
    paths.extend(defaults.iter().map(PathBuf::from));
    paths
}

fn find_existing_path(paths: Vec<PathBuf>) -> Option<PathBuf> {
    paths.into_iter().find(|path| path.exists())
}

fn find_model(paths: Vec<PathBuf>) -> Option<PathBuf> {
    for path in paths {
        if !path.exists() {
            continue;
        }
        if path.is_file() {
            return Some(path);
        }
        if path.is_dir() {
            for name in WHISPER_MODEL_NAMES {
                let candidate = path.join(name);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
            if let Ok(entries) = fs::read_dir(&path) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if entry.path().is_file() && name.starts_with("ggml") && name.ends_with(".bin")
                    {
                        return Some(entry.path());
                    }
                }
            }
        }
    }
    None
}

fn audio_extension_from_url(url: &str) -> &'static str {
    let path = url.split('?').next().unwrap_or(url).to_ascii_lowercase();
    for ext in ["ogg", "m4a", "mp3", "wav", "webm", "opus"] {
        if path.ends_with(&format!(".{ext}")) {
            return ext;
        }
    }
    "ogg"
}

fn path_arg(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

async fn run_command(bin: &Path, args: &[&str], timeout: Duration) -> anyhow::Result<String> {
    let output = tokio::time::timeout(timeout, Command::new(bin).args(args).output())
        .await
        .map_err(|_| anyhow::anyhow!("timed out"))??;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).to_string());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!("exit status {}: {}", output.status, stderr.trim())
}

fn read_whisper_text(wav_path: &Path) -> anyhow::Result<String> {
    let full = PathBuf::from(format!("{}.txt", wav_path.to_string_lossy()));
    let stripped = wav_path.with_extension("txt");
    for path in [full, stripped] {
        if path.exists() {
            return Ok(fs::read_to_string(path)?);
        }
    }
    anyhow::bail!("whisper-cpp ran but produced no output text file")
}

fn clean_transcript(raw: &str) -> String {
    let mut cleaned = String::new();
    let mut in_brackets = false;
    for ch in raw.replace("[BLANK_AUDIO]", "").chars() {
        match ch {
            '[' => in_brackets = true,
            ']' => in_brackets = false,
            _ if !in_brackets => cleaned.push(ch),
            _ => {}
        }
    }
    let text = cleaned.trim();
    if text.is_empty() {
        "[empty transcription]".to_string()
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        os::unix::{fs::PermissionsExt, net::UnixListener},
        sync::Mutex,
    };

    use tempfile::TempDir;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    fn voice_env_snapshot() -> [(&'static str, Option<String>); 9] {
        [
            ("WHISPER_BIN", env::var("WHISPER_BIN").ok()),
            ("FFMPEG_BIN", env::var("FFMPEG_BIN").ok()),
            ("CURL_BIN", env::var("CURL_BIN").ok()),
            ("WHISPER_MODEL", env::var("WHISPER_MODEL").ok()),
            ("WHISPER_MODEL_DIR", env::var("WHISPER_MODEL_DIR").ok()),
            (
                "MONI_VOICE_PROMPT_TEMPLATE",
                env::var("MONI_VOICE_PROMPT_TEMPLATE").ok(),
            ),
            (
                "MONI_VOICE_MAX_BYTES",
                env::var("MONI_VOICE_MAX_BYTES").ok(),
            ),
            (
                "MONI_VOICE_MAX_DURATION_SECONDS",
                env::var("MONI_VOICE_MAX_DURATION_SECONDS").ok(),
            ),
            ("HOME", env::var("HOME").ok()),
        ]
    }

    fn restore_voice_env(old: [(&'static str, Option<String>); 9]) {
        unsafe {
            for (key, value) in old {
                if let Some(value) = value {
                    env::set_var(key, value);
                } else {
                    env::remove_var(key);
                }
            }
        }
    }

    fn restore_test_env(key: &str, value: Option<String>) {
        unsafe {
            if let Some(value) = value {
                env::set_var(key, value);
            } else {
                env::remove_var(key);
            }
        }
    }

    fn clear_voice_config_env() {
        unsafe {
            env::remove_var("MONI_VOICE_PROMPT_TEMPLATE");
            env::remove_var("MONI_VOICE_MAX_BYTES");
            env::remove_var("MONI_VOICE_MAX_DURATION_SECONDS");
        }
    }

    #[test]
    fn restore_test_env_sets_and_removes_values() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let original = env::var("MONI_TEST_RESTORE").ok();

        restore_test_env("MONI_TEST_RESTORE", Some("value".to_string()));
        assert_eq!(env::var("MONI_TEST_RESTORE").unwrap(), "value");

        restore_test_env("MONI_TEST_RESTORE", None);
        assert!(env::var("MONI_TEST_RESTORE").is_err());

        restore_test_env("MONI_TEST_RESTORE", original);
    }

    #[test]
    fn transcriber_status_report_checks_configured_paths() {
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg-dir");
        let curl = dir.path().join("curl");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&whisper, "bin").unwrap();
        fs::create_dir(&ffmpeg).unwrap();
        fs::write(&model, "model").unwrap();
        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        let report = transcriber.status_report();

        assert!(report.contains("whisper.cpp: ok"));
        assert!(report.contains("ffmpeg: not a file"));
        assert!(report.contains("curl: missing"));
        assert!(report.contains("model: ok"));
        assert!(report.contains("max bytes: 26214400"));
        assert!(report.contains("max duration: 600s"));
    }

    #[test]
    fn audio_detection_accepts_voice_mime() {
        assert!(is_audio_attachment("voice.dat", Some("audio/ogg")));
        assert!(is_audio_attachment("voice.m4a", None));
        assert!(!is_audio_attachment("photo.png", Some("image/png")));
    }

    #[test]
    fn voice_prompt_combines_caption_and_transcript() {
        assert_eq!(
            build_voice_prompt("caption", "hello"),
            "[voice note - transcription may contain typos]: caption\n\nhello"
        );
    }

    #[test]
    fn transcriber_prompt_uses_configured_template() {
        let transcriber = VoiceTranscriber::new("whisper", "ffmpeg", "curl", "model.bin", "tmp")
            .with_prompt_template("voice says:\n{content}");

        assert_eq!(
            transcriber.build_prompt("caption", "hello"),
            "voice says:\ncaption\n\nhello"
        );
    }

    #[test]
    fn attachment_size_guard_accepts_unknown_or_small_sizes() {
        let transcriber = VoiceTranscriber::new("whisper", "ffmpeg", "curl", "model.bin", "tmp")
            .with_guardrails(10, 60);

        transcriber.validate_attachment_size(None).unwrap();
        transcriber.validate_attachment_size(Some(10)).unwrap();
    }

    #[test]
    fn attachment_size_guard_rejects_large_sizes() {
        let transcriber = VoiceTranscriber::new("whisper", "ffmpeg", "curl", "model.bin", "tmp")
            .with_guardrails(10, 60);

        let err = transcriber.validate_attachment_size(Some(11)).unwrap_err();

        assert!(err.to_string().contains("voice attachment is too large"));
    }

    #[test]
    fn clean_transcript_removes_bracketed_noise() {
        assert_eq!(clean_transcript("[music] hello [BLANK_AUDIO]"), "hello");
    }

    #[test]
    fn voice_prompt_omits_empty_caption() {
        assert_eq!(
            build_voice_prompt("  ", "hello"),
            "[voice note - transcription may contain typos]: hello"
        );
    }

    #[test]
    fn model_discovery_accepts_file_named_model() {
        let dir = TempDir::new().unwrap();
        let model = dir.path().join("custom.bin");
        fs::write(&model, "model").unwrap();

        assert_eq!(find_model(vec![model.clone()]), Some(model));
    }

    #[test]
    fn model_discovery_prefers_known_names_in_directory() {
        let dir = TempDir::new().unwrap();
        let model = dir.path().join("ggml-base.en.bin");
        fs::write(&model, "model").unwrap();

        assert_eq!(find_model(vec![dir.path().to_path_buf()]), Some(model));
    }

    #[test]
    fn model_discovery_accepts_any_ggml_bin_in_directory() {
        let dir = TempDir::new().unwrap();
        let model = dir.path().join("ggml-custom.bin");
        fs::write(&model, "model").unwrap();

        assert_eq!(find_model(vec![dir.path().to_path_buf()]), Some(model));
    }

    #[test]
    fn model_discovery_ignores_non_model_files_in_directory() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("notes.txt"), "not a model").unwrap();

        assert!(find_model(vec![dir.path().to_path_buf()]).is_none());
    }

    #[test]
    fn model_discovery_ignores_unreadable_directories() {
        let dir = TempDir::new().unwrap();
        let model_dir = dir.path().join("models");
        fs::create_dir_all(&model_dir).unwrap();
        let mut permissions = fs::metadata(&model_dir).unwrap().permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&model_dir, permissions).unwrap();

        let found = find_model(vec![model_dir.clone()]);

        let mut permissions = fs::metadata(&model_dir).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&model_dir, permissions).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn model_discovery_ignores_existing_non_file_non_directory_paths() {
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("model.sock");
        let _listener = UnixListener::bind(&socket).unwrap();

        assert!(find_model(vec![socket]).is_none());
    }

    #[test]
    fn model_discovery_returns_none_for_missing_paths() {
        assert!(find_model(vec![PathBuf::from("/definitely/missing/model")]).is_none());
    }

    #[test]
    fn env_candidates_ignores_blank_env_value() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let original = env::var("MONI_TEST_BLANK").ok();
        unsafe {
            env::set_var("MONI_TEST_BLANK", " ");
        }

        let candidates = env_candidates("MONI_TEST_BLANK", &["fallback"]);

        restore_test_env("MONI_TEST_BLANK", original);
        assert_eq!(candidates, vec![PathBuf::from("fallback")]);
    }

    #[test]
    fn env_positive_u64_uses_default_and_accepts_positive_values() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let original = env::var("MONI_TEST_POSITIVE").ok();
        unsafe {
            env::remove_var("MONI_TEST_POSITIVE");
        }
        assert_eq!(env_positive_u64("MONI_TEST_POSITIVE", 7).unwrap(), 7);

        unsafe {
            env::set_var("MONI_TEST_POSITIVE", "12");
        }
        assert_eq!(env_positive_u64("MONI_TEST_POSITIVE", 7).unwrap(), 12);

        restore_test_env("MONI_TEST_POSITIVE", original);
    }

    #[test]
    fn env_positive_u64_rejects_zero_and_parse_errors() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let original = env::var("MONI_TEST_POSITIVE").ok();
        unsafe {
            env::set_var("MONI_TEST_POSITIVE", "0");
        }
        let zero = env_positive_u64("MONI_TEST_POSITIVE", 7).unwrap_err();
        assert!(zero.to_string().contains("greater than zero"));

        unsafe {
            env::set_var("MONI_TEST_POSITIVE", "nope");
        }
        let parse = env_positive_u64("MONI_TEST_POSITIVE", 7).unwrap_err();
        assert!(parse.to_string().contains("positive integer"));

        restore_test_env("MONI_TEST_POSITIVE", original);
    }

    #[test]
    fn audio_extension_is_detected_from_url() {
        assert_eq!(audio_extension_from_url("https://cdn/x.m4a?token=1"), "m4a");
        assert_eq!(audio_extension_from_url("https://cdn/no-extension"), "ogg");
    }

    #[test]
    fn missing_command_reports_spawn_error() {
        let err = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(run_command(
                Path::new("/definitely/missing/moni-voice-command"),
                &[],
                Duration::from_secs(5),
            ))
            .unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn failed_command_reports_stderr() {
        let dir = TempDir::new().unwrap();
        let fail = dir.path().join("fail");
        write_script(
            &fail,
            r#"#!/bin/sh
echo nope >&2
exit 7
"#,
        );

        let err = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(run_command(&fail, &[], Duration::from_secs(5)))
            .unwrap_err();

        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn timed_out_command_reports_timeout() {
        let dir = TempDir::new().unwrap();
        let slow = dir.path().join("slow");
        write_script(
            &slow,
            r#"#!/bin/sh
sleep 2
"#,
        );

        let err = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(run_command(&slow, &[], Duration::from_millis(10)))
            .unwrap_err();

        assert!(err.to_string().contains("timed out"));
    }

    #[test]
    fn from_env_uses_configured_paths() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg");
        let curl = dir.path().join("curl");
        let model_dir = dir.path().join("models");
        fs::create_dir_all(&model_dir).unwrap();
        let model = model_dir.join("ggml-small.en.bin");
        for bin in [&whisper, &ffmpeg, &curl] {
            write_script(bin, "#!/bin/sh\nexit 0\n");
        }
        fs::write(&model, "model").unwrap();

        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", &ffmpeg);
            env::set_var("CURL_BIN", &curl);
            env::remove_var("WHISPER_MODEL");
            env::set_var("WHISPER_MODEL_DIR", &model_dir);
            env::set_var("HOME", dir.path());
        }
        clear_voice_config_env();

        let transcriber = VoiceTranscriber::from_env().unwrap();
        assert_eq!(transcriber.whisper_bin, whisper);
        assert_eq!(transcriber.ffmpeg_bin, ffmpeg);
        assert_eq!(transcriber.curl_bin, curl);
        assert_eq!(transcriber.model, model);

        restore_voice_env(old);
    }

    #[test]
    fn from_env_uses_voice_prompt_template_and_guardrails() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg");
        let curl = dir.path().join("curl");
        let model = dir.path().join("ggml-small.en.bin");
        for bin in [&whisper, &ffmpeg, &curl] {
            write_script(bin, "#!/bin/sh\nexit 0\n");
        }
        fs::write(&model, "model").unwrap();
        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", &ffmpeg);
            env::set_var("CURL_BIN", &curl);
            env::set_var("WHISPER_MODEL", &model);
            env::remove_var("WHISPER_MODEL_DIR");
            env::set_var("MONI_VOICE_PROMPT_TEMPLATE", "voice:\n{content}");
            env::set_var("MONI_VOICE_MAX_BYTES", "3");
            env::set_var("MONI_VOICE_MAX_DURATION_SECONDS", "4");
            env::set_var("HOME", dir.path());
        }

        let transcriber = VoiceTranscriber::from_env().unwrap();

        restore_voice_env(old);
        assert_eq!(
            transcriber.build_prompt("caption", "hello"),
            "voice:\ncaption\n\nhello"
        );
        assert!(transcriber.validate_attachment_size(Some(4)).is_err());
        let report = transcriber.status_report();
        assert!(report.contains("max bytes: 3"));
        assert!(report.contains("max duration: 4s"));
    }

    #[test]
    fn from_env_reports_invalid_duration_guardrail() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg");
        let curl = dir.path().join("curl");
        let model = dir.path().join("ggml-small.en.bin");
        for bin in [&whisper, &ffmpeg, &curl] {
            write_script(bin, "#!/bin/sh\nexit 0\n");
        }
        fs::write(&model, "model").unwrap();
        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", &ffmpeg);
            env::set_var("CURL_BIN", &curl);
            env::set_var("WHISPER_MODEL", &model);
            env::remove_var("WHISPER_MODEL_DIR");
            env::remove_var("MONI_VOICE_PROMPT_TEMPLATE");
            env::set_var("MONI_VOICE_MAX_BYTES", "3");
            env::set_var("MONI_VOICE_MAX_DURATION_SECONDS", "nope");
            env::set_var("HOME", dir.path());
        }

        let err = VoiceTranscriber::from_env().unwrap_err();

        restore_voice_env(old);
        assert!(err.to_string().contains("MONI_VOICE_MAX_DURATION_SECONDS"));
    }

    #[test]
    fn from_env_reports_missing_model() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg");
        let curl = dir.path().join("curl");
        let empty_model_dir = dir.path().join("models");
        fs::create_dir_all(&empty_model_dir).unwrap();
        for bin in [&whisper, &ffmpeg, &curl] {
            write_script(bin, "#!/bin/sh\nexit 0\n");
        }

        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", &ffmpeg);
            env::set_var("CURL_BIN", &curl);
            env::remove_var("WHISPER_MODEL");
            env::set_var("WHISPER_MODEL_DIR", &empty_model_dir);
            env::set_var("HOME", dir.path());
        }
        clear_voice_config_env();

        let err = VoiceTranscriber::from_env().unwrap_err();
        assert!(err.to_string().contains("No whisper model found"));

        restore_voice_env(old);
    }

    #[test]
    fn from_env_reports_missing_ffmpeg() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let curl = dir.path().join("curl");
        let model = dir.path().join("ggml-small.en.bin");
        write_script(&whisper, "#!/bin/sh\nexit 0\n");
        write_script(&curl, "#!/bin/sh\nexit 0\n");
        fs::write(&model, "model").unwrap();
        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", dir.path().join("missing-ffmpeg"));
            env::set_var("CURL_BIN", &curl);
            env::set_var("WHISPER_MODEL", &model);
            env::remove_var("WHISPER_MODEL_DIR");
        }
        clear_voice_config_env();

        let err = VoiceTranscriber::from_env().unwrap_err();

        restore_voice_env(old);
        assert!(err.to_string().contains("ffmpeg not found"));
    }

    #[test]
    fn from_env_reports_missing_curl() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg");
        let model = dir.path().join("ggml-small.en.bin");
        write_script(&whisper, "#!/bin/sh\nexit 0\n");
        write_script(&ffmpeg, "#!/bin/sh\nexit 0\n");
        fs::write(&model, "model").unwrap();
        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", &ffmpeg);
            env::set_var("CURL_BIN", dir.path().join("missing-curl"));
            env::set_var("WHISPER_MODEL", &model);
            env::remove_var("WHISPER_MODEL_DIR");
        }
        clear_voice_config_env();

        let err = VoiceTranscriber::from_env().unwrap_err();

        restore_voice_env(old);
        assert!(err.to_string().contains("curl not found"));
    }

    #[test]
    fn from_env_accepts_explicit_model_without_home() {
        let _guard = ENV_LOCK.lock().expect("voice env lock poisoned");
        let dir = TempDir::new().unwrap();
        let whisper = dir.path().join("whisper-cli");
        let ffmpeg = dir.path().join("ffmpeg");
        let curl = dir.path().join("curl");
        let model = dir.path().join("ggml-small.en.bin");
        for bin in [&whisper, &ffmpeg, &curl] {
            write_script(bin, "#!/bin/sh\nexit 0\n");
        }
        fs::write(&model, "model").unwrap();
        let old = voice_env_snapshot();
        unsafe {
            env::set_var("WHISPER_BIN", &whisper);
            env::set_var("FFMPEG_BIN", &ffmpeg);
            env::set_var("CURL_BIN", &curl);
            env::set_var("WHISPER_MODEL", &model);
            env::remove_var("WHISPER_MODEL_DIR");
            env::remove_var("HOME");
        }
        clear_voice_config_env();

        let transcriber = VoiceTranscriber::from_env().unwrap();

        restore_voice_env(old);
        assert_eq!(transcriber.model, model);
    }

    #[tokio::test]
    async fn transcriber_shells_out_to_configured_tools() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            r#"#!/bin/sh
while [ "$1" != "-o" ]; do shift; done
shift
printf audio > "$1"
"#,
        );
        write_script(
            &ffmpeg,
            r#"#!/bin/sh
out=""
for arg in "$@"; do out="$arg"; done
printf wav > "$out"
"#,
        );
        write_script(
            &whisper,
            r#"#!/bin/sh
wav=""
while [ "$1" != "-f" ]; do shift; done
shift
wav="$1"
printf "hello from voice" > "$wav.txt"
"#,
        );

        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        let transcript = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap();

        assert_eq!(transcript, "hello from voice");
    }

    #[tokio::test]
    async fn transcriber_rejects_downloaded_audio_larger_than_guardrail() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            r#"#!/bin/sh
while [ "$1" != "-o" ]; do shift; done
shift
printf audio > "$1"
"#,
        );
        write_script(&ffmpeg, "#!/bin/sh\necho should not convert >&2\nexit 7\n");
        write_script(&whisper, "#!/bin/sh\nexit 0\n");

        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path())
            .with_guardrails(4, 60);

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("voice attachment is too large"));
    }

    #[tokio::test]
    async fn transcriber_rejects_converted_audio_longer_than_guardrail() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            r#"#!/bin/sh
while [ "$1" != "-o" ]; do shift; done
shift
printf audio > "$1"
"#,
        );
        write_script(
            &ffmpeg,
            r#"#!/bin/sh
out=""
for arg in "$@"; do out="$arg"; done
dd if=/dev/zero bs=40000 count=1 of="$out" 2>/dev/null
"#,
        );
        write_script(
            &whisper,
            "#!/bin/sh\necho should not transcribe >&2\nexit 8\n",
        );

        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path())
            .with_guardrails(100, 1);

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("voice attachment is too long"));
    }

    #[tokio::test]
    async fn transcriber_returns_empty_marker_for_blank_output() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            "#!/bin/sh\nwhile [ \"$1\" != \"-o\" ]; do shift; done\nshift\nprintf audio > \"$1\"\n",
        );
        write_script(
            &ffmpeg,
            "#!/bin/sh\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf wav > \"$out\"\n",
        );
        write_script(
            &whisper,
            "#!/bin/sh\nwhile [ \"$1\" != \"-f\" ]; do shift; done\nshift\nprintf \"[BLANK_AUDIO]\" > \"$1.txt\"\n",
        );

        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        assert_eq!(
            transcriber
                .transcribe_url("https://cdn.discordapp.com/voice.webm")
                .await
                .unwrap(),
            "[empty transcription]"
        );
    }

    #[tokio::test]
    async fn transcriber_cleans_up_after_download_failure() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(&curl, "#!/bin/sh\necho download failed >&2\nexit 2\n");
        write_script(&ffmpeg, "#!/bin/sh\nexit 0\n");
        write_script(&whisper, "#!/bin/sh\nexit 0\n");
        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.mp3")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("audio download failed"));
        assert!(fs::read_dir(dir.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("moni-voice-")
        }));
    }

    #[tokio::test]
    async fn transcriber_reports_ffmpeg_failure() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            "#!/bin/sh\nwhile [ \"$1\" != \"-o\" ]; do shift; done\nshift\nprintf audio > \"$1\"\n",
        );
        write_script(&ffmpeg, "#!/bin/sh\necho bad ffmpeg >&2\nexit 2\n");
        write_script(&whisper, "#!/bin/sh\nexit 0\n");
        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("ffmpeg conversion failed"));
    }

    #[tokio::test]
    async fn transcriber_reports_whisper_failure() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            "#!/bin/sh\nwhile [ \"$1\" != \"-o\" ]; do shift; done\nshift\nprintf audio > \"$1\"\n",
        );
        write_script(
            &ffmpeg,
            "#!/bin/sh\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf wav > \"$out\"\n",
        );
        write_script(&whisper, "#!/bin/sh\necho bad whisper >&2\nexit 3\n");
        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("whisper-cpp failed"));
    }

    #[tokio::test]
    async fn transcriber_reports_temp_dir_creation_error() {
        let dir = TempDir::new().unwrap();
        let temp_file = dir.path().join("not-a-directory");
        fs::write(&temp_file, "file").unwrap();
        let transcriber = VoiceTranscriber::new(
            dir.path().join("whisper"),
            dir.path().join("ffmpeg"),
            dir.path().join("curl"),
            dir.path().join("ggml-small.en.bin"),
            temp_file,
        );

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn read_whisper_text_reports_existing_unreadable_output() {
        let dir = TempDir::new().unwrap();
        let wav_path = dir.path().join("voice.wav");
        let output_path = PathBuf::from(format!("{}.txt", wav_path.to_string_lossy()));
        fs::create_dir(&output_path).unwrap();

        let err = read_whisper_text(&wav_path).unwrap_err();

        assert!(err.downcast_ref::<std::io::Error>().is_some());
    }

    #[tokio::test]
    async fn transcriber_reports_missing_whisper_output() {
        let dir = TempDir::new().unwrap();
        let curl = dir.path().join("curl");
        let ffmpeg = dir.path().join("ffmpeg");
        let whisper = dir.path().join("whisper-cli");
        let model = dir.path().join("ggml-small.en.bin");
        fs::write(&model, "model").unwrap();
        write_script(
            &curl,
            "#!/bin/sh\nwhile [ \"$1\" != \"-o\" ]; do shift; done\nshift\nprintf audio > \"$1\"\n",
        );
        write_script(
            &ffmpeg,
            "#!/bin/sh\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf wav > \"$out\"\n",
        );
        write_script(&whisper, "#!/bin/sh\nexit 0\n");
        let transcriber = VoiceTranscriber::new(&whisper, &ffmpeg, &curl, &model, dir.path());

        let err = transcriber
            .transcribe_url("https://cdn.discordapp.com/voice.ogg")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("produced no output text file"));
    }
}
