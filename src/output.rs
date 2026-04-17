use anyhow::{Context, Result};
use serde_json::{json, Map, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;

pub const JSON_SCHEMA_VERSION: u32 = 1;

static LOGGER: OnceLock<Option<Arc<StructuredLogger>>> = OnceLock::new();
static SETTINGS: OnceLock<OutputSettings> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputMode {
    Json,
    Human,
}

#[derive(Clone, Copy, Debug)]
pub struct OutputSettings {
    pub mode: OutputMode,
    pub quiet: bool,
    pub no_progress: bool,
}

pub fn init_output(settings: OutputSettings, path: Option<PathBuf>) -> Result<()> {
    let _ = SETTINGS.set(settings);
    let logger = match path {
        Some(path) => Some(Arc::new(StructuredLogger::new(path)?)),
        None => None,
    };

    let _ = LOGGER.set(logger);
    Ok(())
}

pub fn settings() -> OutputSettings {
    *SETTINGS.get().unwrap_or(&OutputSettings {
        mode: OutputMode::Json,
        quiet: false,
        no_progress: false,
    })
}

pub fn command_output(command: &str, data: Value) -> Value {
    json!({
        "schema_version": JSON_SCHEMA_VERSION,
        "command": command,
        "data": data,
    })
}

pub fn event_output(command: &str, event: &str, data: Value) -> Value {
    json!({
        "schema_version": JSON_SCHEMA_VERSION,
        "command": command,
        "event": event,
        "timestamp_unix": unix_timestamp(),
        "data": data,
    })
}

pub fn annotate_payload(payload: Value) -> Value {
    match payload {
        Value::Object(mut map) => {
            map.insert(
                "schema_version".to_string(),
                Value::Number(JSON_SCHEMA_VERSION.into()),
            );
            Value::Object(map)
        }
        other => other,
    }
}

pub fn object_with_optional_fields(
    mut base: Map<String, Value>,
    optional: impl IntoIterator<Item = (&'static str, Option<Value>)>,
) -> Value {
    for (key, value) in optional {
        if let Some(value) = value {
            base.insert(key.to_string(), value);
        }
    }
    Value::Object(base)
}

pub fn write_json_pretty(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub fn write_command_output(command: &str, data: Value) -> Result<()> {
    match settings().mode {
        OutputMode::Json => write_json_pretty(&command_output(command, data)),
        OutputMode::Human => write_human(command, &data),
    }
}

pub fn stderrln(message: impl AsRef<str>) {
    if !settings().quiet {
        eprintln!("{}", message.as_ref());
    }
}

pub fn progress_enabled() -> bool {
    let settings = settings();
    !settings.no_progress && !settings.quiet
}

pub fn log_event(command: &str, event: &str, data: &Value) -> Result<()> {
    let Some(Some(logger)) = LOGGER.get() else {
        return Ok(());
    };

    logger.write(&event_output(command, event, data.clone()))
}

pub async fn run_hook(
    hook: Option<&str>,
    command: &str,
    event: &str,
    payload: &Value,
) -> Result<()> {
    let Some(hook) = hook else {
        return Ok(());
    };

    let payload_json = serde_json::to_string(payload)?;
    let mut process = if cfg!(windows) {
        let mut cmd = Command::new("cmd");
        cmd.arg("/C").arg(hook);
        cmd
    } else {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(hook);
        cmd
    };

    process
        .env("TW_DL_COMMAND", command)
        .env("TW_DL_EVENT", event)
        .env("TW_DL_SCHEMA_VERSION", JSON_SCHEMA_VERSION.to_string())
        .env("TW_DL_EVENT_JSON", &payload_json)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let mut child = process
        .spawn()
        .with_context(|| format!("Failed to start hook '{}'", hook))?;

    if let Some(stdin) = child.stdin.as_mut() {
        use tokio::io::AsyncWriteExt;
        stdin.write_all(payload_json.as_bytes()).await?;
    }

    let status = child
        .wait()
        .await
        .with_context(|| format!("Failed to wait for hook '{}'", hook))?;
    if !status.success() {
        anyhow::bail!("Hook '{}' exited with status {}", hook, status);
    }

    Ok(())
}

fn write_human(command: &str, data: &Value) -> Result<()> {
    match command {
        "download" => write_human_download(data),
        "inspect" => write_human_inspect(data),
        "list-chats" => write_human_list_chats(data),
        "resolve" => write_human_resolve(data),
        "whoami" => write_human_whoami(data),
        "doctor" => write_human_doctor(data),
        "manifest-export" | "manifest-import" => write_human_manifest(command, data),
        _ => write_json_pretty(&command_output(command, data.clone())),
    }
}

fn write_human_download(data: &Value) -> Result<()> {
    if let Some(status) = data.get("status").and_then(Value::as_str) {
        match status {
            "downloaded" | "skipped" | "planned" => {
                let path = data.get("file").and_then(Value::as_str).unwrap_or("-");
                let source = data
                    .get("canonical_source_link")
                    .and_then(Value::as_str)
                    .unwrap_or("-");
                println!("{} {} {}", status, path, source);
            }
            "completed" => {
                println!(
                    "completed: {} succeeded, {} skipped, {} failed",
                    data.get("succeeded").and_then(Value::as_u64).unwrap_or(0),
                    data.get("skipped").and_then(Value::as_u64).unwrap_or(0),
                    data.get("failed").and_then(Value::as_u64).unwrap_or(0)
                );
            }
            _ => println!("{}", serde_json::to_string_pretty(data)?),
        }
    } else {
        println!("{}", serde_json::to_string_pretty(data)?);
    }
    Ok(())
}

fn write_human_inspect(data: &Value) -> Result<()> {
    println!(
        "message {} in {}",
        data.get("id").and_then(Value::as_i64).unwrap_or_default(),
        data.get("peer_id")
            .and_then(Value::as_i64)
            .unwrap_or_default()
    );
    if let Some(text) = data.get("text").and_then(Value::as_str) {
        if !text.is_empty() {
            println!("{}", text);
        }
    }
    Ok(())
}

fn write_human_list_chats(data: &Value) -> Result<()> {
    if let Some(chats) = data.get("chats").and_then(Value::as_array) {
        for chat in chats {
            println!(
                "{}\t{}\t{}",
                chat.get("id").and_then(Value::as_i64).unwrap_or_default(),
                chat.get("kind").and_then(Value::as_str).unwrap_or("-"),
                chat.get("name").and_then(Value::as_str).unwrap_or("-")
            );
        }
    }
    Ok(())
}

fn write_human_resolve(data: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(data)?);
    Ok(())
}

fn write_human_whoami(data: &Value) -> Result<()> {
    println!(
        "{} {}",
        data.get("username").and_then(Value::as_str).unwrap_or("-"),
        data.get("phone").and_then(Value::as_str).unwrap_or("-")
    );
    Ok(())
}

fn write_human_doctor(data: &Value) -> Result<()> {
    println!(
        "healthy: {}",
        data.get("healthy")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    );
    Ok(())
}

fn write_human_manifest(command: &str, data: &Value) -> Result<()> {
    println!(
        "{} {} -> {} ({})",
        command,
        data.get("input").and_then(Value::as_str).unwrap_or("-"),
        data.get("output").and_then(Value::as_str).unwrap_or("-"),
        data.get("records").and_then(Value::as_u64).unwrap_or(0)
    );
    Ok(())
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

struct StructuredLogger {
    path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl StructuredLogger {
    fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create log directory '{}'", parent.display())
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to open log file '{}'", path.display()))?;

        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    fn write(&self, value: &Value) -> Result<()> {
        let mut guard = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire log file lock"))?;
        serde_json::to_writer(&mut *guard, value)
            .with_context(|| format!("Failed to write log file '{}'", self.path.display()))?;
        guard.write_all(b"\n")?;
        guard.flush()?;
        Ok(())
    }
}

pub fn archive_append(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create archive directory '{}'", parent.display())
        })?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open archive '{}'", path.display()))?;
    serde_json::to_writer(&mut file, value)
        .with_context(|| format!("Failed to serialize archive entry to '{}'", path.display()))?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_output_pretty_json_golden() {
        let value = command_output(
            "download",
            json!({
                "status": "downloaded",
                "file": "downloads/video.mp4",
            }),
        );

        let rendered = serde_json::to_string_pretty(&value).unwrap();
        let expected = r#"{
  "command": "download",
  "data": {
    "file": "downloads/video.mp4",
    "status": "downloaded"
  },
  "schema_version": 1
}"#;

        assert_eq!(rendered, expected);
    }

    #[test]
    fn test_event_output_json_shape_golden() {
        let value = event_output(
            "download",
            "item_completed",
            json!({
                "status": "downloaded",
                "canonical_source_link": "https://t.me/example/42",
            }),
        );

        let timestamp = value
            .get("timestamp_unix")
            .and_then(Value::as_u64)
            .expect("timestamp_unix");
        assert!(timestamp > 0);

        assert_eq!(
            value,
            json!({
                "schema_version": JSON_SCHEMA_VERSION,
                "command": "download",
                "event": "item_completed",
                "timestamp_unix": timestamp,
                "data": {
                    "status": "downloaded",
                    "canonical_source_link": "https://t.me/example/42",
                }
            })
        );
    }

    #[test]
    fn test_archive_append_jsonl_golden() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-archive-{}-{}.jsonl",
            std::process::id(),
            unix_timestamp()
        ));
        let value = json!({
            "schema_version": JSON_SCHEMA_VERSION,
            "command": "download",
            "event": "archive_item",
            "timestamp_unix": 123456789,
            "data": {
                "status": "downloaded",
                "file": "downloads/video.mp4",
            }
        });

        archive_append(&path, &value).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        let expected = "{\"command\":\"download\",\"data\":{\"file\":\"downloads/video.mp4\",\"status\":\"downloaded\"},\"event\":\"archive_item\",\"schema_version\":1,\"timestamp_unix\":123456789}\n";

        assert_eq!(written, expected);

        let _ = std::fs::remove_file(path);
    }
}
