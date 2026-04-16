use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::IsTerminal;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub telegram_api_id: i32,
    pub telegram_api_hash: String,
}

pub fn load_config(path: &Path) -> Result<Option<AppConfig>> {
    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file '{}'", path.display()))?;
    let config = toml::from_str(&contents)
        .with_context(|| format!("Failed to parse config file '{}'", path.display()))?;
    Ok(Some(config))
}

pub fn cmd_init(config_path: PathBuf, force: bool, yes: bool) -> Result<()> {
    crate::session::ensure_parent_dir(&config_path)?;

    if config_path.exists() && !force {
        bail!(
            "Config file '{}' already exists. Re-run with --force to overwrite it.",
            config_path.display()
        );
    }

    if config_path.exists() && force && !yes {
        confirm_action(format!(
            "Overwrite existing config at '{}'?",
            config_path.display()
        ))?;
    }

    let api_id = prompt("Telegram API ID: ")?;
    let api_id = api_id
        .parse::<i32>()
        .context("Telegram API ID must be a valid integer")?;

    let api_hash = prompt("Telegram API hash: ")?;
    if api_hash.is_empty() {
        bail!("Telegram API hash cannot be empty");
    }

    let config = AppConfig {
        telegram_api_id: api_id,
        telegram_api_hash: api_hash,
    };
    let rendered = toml::to_string_pretty(&config).context("Failed to render config.toml")?;
    fs::write(&config_path, rendered)
        .with_context(|| format!("Failed to write config file '{}'", config_path.display()))?;

    println!("Wrote config to '{}'.", config_path.display());
    Ok(())
}

fn prompt(message: &str) -> Result<String> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    write!(handle, "{}", message)?;
    handle.flush()?;

    let stdin = io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .context("Failed to read input")?;
    Ok(line.trim().to_string())
}

fn confirm_action(message: String) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        bail!("Confirmation required. Re-run with `--yes` in non-interactive mode.");
    }

    let answer = prompt(&format!("{} [y/N]: ", message))?;
    if matches!(answer.to_ascii_lowercase().as_str(), "y" | "yes") {
        return Ok(());
    }

    bail!("Aborted by user.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config_missing_file() {
        let path = PathBuf::from("/tmp/tw-dl-config-missing.toml");
        let loaded = load_config(&path).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_config_parses_toml() {
        let base = std::env::temp_dir().join(format!("tw-dl-config-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        let path = base.join("config.toml");
        std::fs::write(
            &path,
            "telegram_api_id = 12345\ntelegram_api_hash = \"abcdef\"\n",
        )
        .unwrap();

        let loaded = load_config(&path).unwrap().unwrap();
        assert_eq!(
            loaded,
            AppConfig {
                telegram_api_id: 12345,
                telegram_api_hash: "abcdef".to_string(),
            }
        );

        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(&base).unwrap();
    }
}
