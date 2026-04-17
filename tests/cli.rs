use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tw_dl::output::JSON_SCHEMA_VERSION;

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_tw-dl")
}

fn temp_path(name: &str, extension: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "tw-dl-{}-{}-{}.{}",
        name,
        std::process::id(),
        tw_dl::output::unix_timestamp(),
        extension
    ))
}

#[test]
fn test_manifest_export_cli() {
    let input = temp_path("manifest-export-input", "jsonl");
    let output = temp_path("manifest-export-output", "json");
    let input_data = concat!(
        "{\"index\":1,\"link\":\"https://t.me/example/42\",\"status\":\"downloaded\",",
        "\"file\":\"downloads/video.mp4\",\"error\":null,\"timestamp_unix\":123456789,",
        "\"canonical_source_link\":\"https://t.me/example/42\"}\n"
    );
    fs::write(&input, input_data).unwrap();

    let result = Command::new(bin_path())
        .args([
            "manifest",
            "export",
            "--input",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(result.status.success(), "{:?}", result);

    let stdout: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(stdout["schema_version"], JSON_SCHEMA_VERSION);
    assert_eq!(stdout["command"], "manifest-export");
    assert_eq!(stdout["data"]["input"], input.display().to_string());
    assert_eq!(stdout["data"]["output"], output.display().to_string());
    assert_eq!(stdout["data"]["records"], 1);

    let exported = fs::read_to_string(&output).unwrap();
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
    assert_eq!(exported, expected);

    let _ = fs::remove_file(input);
    let _ = fs::remove_file(output);
}

#[test]
fn test_manifest_import_cli() {
    let input = temp_path("manifest-import-input", "json");
    let output = temp_path("manifest-import-output", "jsonl");
    let input_data = r#"[
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
    fs::write(&input, input_data).unwrap();

    let result = Command::new(bin_path())
        .args([
            "manifest",
            "import",
            "--input",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(result.status.success(), "{:?}", result);

    let stdout: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(stdout["schema_version"], JSON_SCHEMA_VERSION);
    assert_eq!(stdout["command"], "manifest-import");
    assert_eq!(stdout["data"]["input"], input.display().to_string());
    assert_eq!(stdout["data"]["output"], output.display().to_string());
    assert_eq!(stdout["data"]["records"], 1);

    let imported = fs::read_to_string(&output).unwrap();
    let expected = concat!(
        "{\"index\":1,\"link\":\"https://t.me/example/42\",\"status\":\"downloaded\",",
        "\"file\":\"downloads/video.mp4\",\"error\":null,\"timestamp_unix\":123456789,",
        "\"canonical_source_link\":\"https://t.me/example/42\"}\n"
    );
    assert_eq!(imported, expected);

    let _ = fs::remove_file(input);
    let _ = fs::remove_file(output);
}

#[test]
fn test_completions_bash_cli() {
    let result = Command::new(bin_path())
        .args(["completions", "bash"])
        .output()
        .unwrap();

    assert!(result.status.success(), "{:?}", result);

    let stdout = String::from_utf8(result.stdout).unwrap();
    assert!(stdout.contains("_tw-dl()"));
    assert!(stdout.contains("complete -F _tw-dl"));
    assert!(stdout.contains("tw-dl"));
}

#[test]
fn test_completions_zsh_cli() {
    let result = Command::new(bin_path())
        .args(["completions", "zsh"])
        .output()
        .unwrap();

    assert!(result.status.success(), "{:?}", result);

    let stdout = String::from_utf8(result.stdout).unwrap();
    assert!(stdout.contains("#compdef tw-dl"));
    assert!(stdout.contains("_tw-dl()"));
}

#[test]
fn test_completions_fish_cli() {
    let result = Command::new(bin_path())
        .args(["completions", "fish"])
        .output()
        .unwrap();

    assert!(result.status.success(), "{:?}", result);

    let stdout = String::from_utf8(result.stdout).unwrap();
    assert!(stdout.contains("complete -c tw-dl"));
    assert!(stdout.contains("__fish_tw_dl_needs_command"));
}

#[test]
fn test_completions_powershell_cli() {
    let result = Command::new(bin_path())
        .args(["completions", "powershell"])
        .output()
        .unwrap();

    assert!(result.status.success(), "{:?}", result);

    let stdout = String::from_utf8(result.stdout).unwrap();
    assert!(stdout.contains("Register-ArgumentCompleter"));
    assert!(stdout.contains("-CommandName 'tw-dl'"));
}
