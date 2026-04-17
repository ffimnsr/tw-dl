use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub index: usize,
    pub link: String,
    pub status: String,
    pub file: Option<String>,
    pub error: Option<String>,
    pub timestamp_unix: u64,
    #[serde(default)]
    pub canonical_source_link: Option<String>,
}

pub async fn read_manifest_records(path: &Path) -> Result<Vec<ManifestRecord>> {
    if !fs::try_exists(path)
        .await
        .with_context(|| format!("Failed to inspect manifest '{}'", path.display()))?
    {
        bail!("Manifest '{}' does not exist", path.display());
    }

    let text = fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read manifest '{}'", path.display()))?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if trimmed.starts_with('[') {
        let records: Vec<ManifestRecord> = serde_json::from_str(trimmed)
            .with_context(|| format!("Failed to parse JSON manifest '{}'", path.display()))?;
        return Ok(records);
    }

    let file = fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open manifest '{}'", path.display()))?;
    let mut lines = BufReader::new(file).lines();
    let mut records = Vec::new();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<ManifestRecord>(&line)
            .with_context(|| format!("Failed to parse manifest entry in '{}'", path.display()))?;
        records.push(record);
    }

    Ok(records)
}

pub async fn open_manifest_writer(path: &Path) -> Result<fs::File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.with_context(|| {
            format!("Failed to create manifest directory '{}'", parent.display())
        })?;
    }

    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await
        .with_context(|| format!("Failed to open manifest '{}'", path.display()))
}

pub async fn append_manifest_record(writer: &mut fs::File, record: &ManifestRecord) -> Result<()> {
    let line = serde_json::to_string(record).context("Failed to serialize manifest record")?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

pub async fn export_manifest(input: &Path, output: &Path) -> Result<()> {
    let records = read_manifest_records(input).await?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create export directory '{}'", parent.display()))?;
    }

    let serialized = if output
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    {
        serde_json::to_string_pretty(&records)?
    } else {
        let mut data = String::new();
        for record in &records {
            data.push_str(&serde_json::to_string(record)?);
            data.push('\n');
        }
        data
    };

    fs::write(output, serialized)
        .await
        .with_context(|| format!("Failed to write export '{}'", output.display()))?;
    Ok(())
}

pub async fn import_manifest(input: &Path, output: &Path) -> Result<()> {
    let records = read_manifest_records(input).await?;
    let mut writer = open_manifest_writer(output).await?;
    for record in records {
        append_manifest_record(&mut writer, &record).await?;
    }
    Ok(())
}

pub fn manifest_export_result(input: &Path, output: &Path, count: usize) -> Value {
    serde_json::json!({
        "input": input.display().to_string(),
        "output": output.display().to_string(),
        "records": count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> ManifestRecord {
        ManifestRecord {
            index: 1,
            link: "https://t.me/example/42".to_string(),
            status: "downloaded".to_string(),
            file: Some("downloads/video.mp4".to_string()),
            error: None,
            timestamp_unix: 123456789,
            canonical_source_link: Some("https://t.me/example/42".to_string()),
        }
    }

    #[tokio::test]
    async fn test_append_manifest_record_jsonl_golden() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-manifest-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let mut writer = open_manifest_writer(&path).await.unwrap();
        append_manifest_record(&mut writer, &sample_record())
            .await
            .unwrap();

        let written = fs::read_to_string(&path).await.unwrap();
        let expected = "{\"index\":1,\"link\":\"https://t.me/example/42\",\"status\":\"downloaded\",\"file\":\"downloads/video.mp4\",\"error\":null,\"timestamp_unix\":123456789,\"canonical_source_link\":\"https://t.me/example/42\"}\n";

        assert_eq!(written, expected);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_export_manifest_json_golden() {
        let input = std::env::temp_dir().join(format!(
            "tw-dl-export-input-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let output = std::env::temp_dir().join(format!(
            "tw-dl-export-output-{}-{}.json",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let mut writer = open_manifest_writer(&input).await.unwrap();
        append_manifest_record(&mut writer, &sample_record())
            .await
            .unwrap();

        export_manifest(&input, &output).await.unwrap();

        let written = fs::read_to_string(&output).await.unwrap();
        let expected = r#"[
  {
    "index": 1,
    "link": "https://t.me/example/42",
    "status": "downloaded",
    "file": "downloads/video.mp4",
    "error": null,
    "timestamp_unix": 123456789,
    "canonical_source_link": "https://t.me/example/42"
  }
]"#;

        assert_eq!(written, expected);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_file(output);
    }

    #[tokio::test]
    async fn test_import_manifest_from_json_array_golden() {
        let input = std::env::temp_dir().join(format!(
            "tw-dl-import-input-{}-{}.json",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let output = std::env::temp_dir().join(format!(
            "tw-dl-import-output-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let json_input = r#"[
  {
    "index": 1,
    "link": "https://t.me/example/42",
    "status": "downloaded",
    "file": "downloads/video.mp4",
    "error": null,
    "timestamp_unix": 123456789,
    "canonical_source_link": "https://t.me/example/42"
  }
]"#;
        fs::write(&input, json_input).await.unwrap();

        import_manifest(&input, &output).await.unwrap();

        let written = fs::read_to_string(&output).await.unwrap();
        let expected = "{\"index\":1,\"link\":\"https://t.me/example/42\",\"status\":\"downloaded\",\"file\":\"downloads/video.mp4\",\"error\":null,\"timestamp_unix\":123456789,\"canonical_source_link\":\"https://t.me/example/42\"}\n";

        assert_eq!(written, expected);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_file(output);
    }
}
