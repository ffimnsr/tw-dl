use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Return the default session directory path: `~/.config/tw-dl/`
fn default_session_dir() -> Result<PathBuf> {
    let config_dir = dirs::config_dir().context("Could not determine the user config directory")?;
    Ok(config_dir.join("tw-dl"))
}

/// Resolve the session file path from an optional override.
///
/// If `override_path` is given it is used as-is (file path).
/// Otherwise returns `~/.config/tw-dl/session`.
pub fn resolve_session_path(override_path: Option<PathBuf>) -> Result<PathBuf> {
    match override_path {
        Some(p) => Ok(p),
        None => {
            let dir = default_session_dir()?;
            Ok(dir.join("session"))
        }
    }
}

/// Ensure that the parent directory of `path` exists, creating it if needed.
pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory '{}'", parent.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_session_path_override() {
        let p = PathBuf::from("/tmp/my-session");
        let result = resolve_session_path(Some(p.clone())).unwrap();
        assert_eq!(result, p);
    }

    #[test]
    fn test_resolve_session_path_default() {
        let result = resolve_session_path(None).unwrap();
        assert!(result.ends_with("tw-dl/session"));
    }
}
