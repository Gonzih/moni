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

#[derive(Debug, Clone)]
pub struct VoiceTranscriber {
    whisper_bin: PathBuf,
    ffmpeg_bin: PathBuf,
    curl_bin: PathBuf,
    model: PathBuf,
    temp_dir: PathBuf,
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
        }
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
    let caption = caption.trim();
    let transcript = transcript.trim();
    let full = if caption.is_empty() {
        transcript.to_string()
    } else {
        format!("{caption}\n\n{transcript}")
    };
    format!("[voice note - transcription may contain typos]: {full}")
}

fn env_candidates(key: &str, defaults: &[&str]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(value) = env::var(key) {
        if !value.trim().is_empty() {
            paths.push(PathBuf::from(value));
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
    use std::{fs, os::unix::fs::PermissionsExt};

    use tempfile::TempDir;

    use super::*;

    fn write_script(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
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
    fn clean_transcript_removes_bracketed_noise() {
        assert_eq!(clean_transcript("[music] hello [BLANK_AUDIO]"), "hello");
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
}
