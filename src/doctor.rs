use anyhow::Result;
use serde_json::json;
use std::path::{Path, PathBuf};

pub async fn cmd_doctor(config_path: PathBuf, session_path: PathBuf) -> Result<()> {
    let env_api_id = std::env::var("TELEGRAM_API_ID").ok();
    let env_api_hash = std::env::var("TELEGRAM_API_HASH").ok();
    let loaded_config = crate::config::load_config(&config_path);

    let config_report = match &loaded_config {
        Ok(Some(config)) => json!({
            "present": true,
            "loadable": true,
            "path": config_path.display().to_string(),
            "telegram_api_id_present": config.telegram_api_id > 0,
            "telegram_api_hash_present": !config.telegram_api_hash.is_empty(),
        }),
        Ok(None) => json!({
            "present": false,
            "loadable": true,
            "path": config_path.display().to_string(),
        }),
        Err(error) => json!({
            "present": true,
            "loadable": false,
            "path": config_path.display().to_string(),
            "error": format!("{:#}", error),
        }),
    };

    let api_id = env_api_id
        .as_deref()
        .and_then(|value| value.parse::<i32>().ok())
        .or_else(|| {
            loaded_config
                .as_ref()
                .ok()
                .and_then(|value| value.as_ref().map(|cfg| cfg.telegram_api_id))
        });
    let api_hash_present = env_api_hash
        .as_ref()
        .map(|value| !value.is_empty())
        .or_else(|| {
            loaded_config
                .as_ref()
                .ok()
                .and_then(|value| value.as_ref().map(|cfg| !cfg.telegram_api_hash.is_empty()))
        })
        .unwrap_or(false);

    let session_exists = session_path.exists();
    let lock_path = session_path.with_extension("lock");
    let lock_exists = lock_path.exists();

    let auth_report = if let Some(api_id) = api_id {
        if session_exists && !lock_exists {
            match crate::auth::build_client(
                api_id,
                &session_path,
                crate::auth::SessionOptions::default(),
            )
            .await
            {
                Ok(client) => match client.is_authorized().await {
                    Ok(authorized) => json!({
                        "checked": true,
                        "authorized": authorized,
                    }),
                    Err(error) => json!({
                        "checked": true,
                        "authorized": false,
                        "error": format!("{:#}", error),
                    }),
                },
                Err(error) => json!({
                    "checked": true,
                    "authorized": false,
                    "error": format!("{:#}", error),
                }),
            }
        } else if lock_exists {
            json!({
                "checked": false,
                "authorized": null,
                "error": format!("Session lock '{}' exists", lock_path.display()),
            })
        } else {
            json!({
                "checked": false,
                "authorized": null,
                "error": "Session file is missing",
            })
        }
    } else {
        json!({
            "checked": false,
            "authorized": null,
            "error": "TELEGRAM_API_ID is not configured",
        })
    };

    let api_id_source = if env_api_id.is_some() {
        "env"
    } else if loaded_config
        .as_ref()
        .ok()
        .and_then(|cfg| cfg.as_ref())
        .is_some()
    {
        "config"
    } else {
        "missing"
    };
    let api_hash_source = if env_api_hash.is_some() {
        "env"
    } else if loaded_config
        .as_ref()
        .ok()
        .and_then(|cfg| cfg.as_ref())
        .is_some()
    {
        "config"
    } else {
        "missing"
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "config": config_report,
            "environment": {
                "telegram_api_id_source": api_id_source,
                "telegram_api_hash_source": api_hash_source,
            },
            "session": {
                "path": session_path.display().to_string(),
                "exists": session_exists,
                "lock_path": lock_path.display().to_string(),
                "lock_exists": lock_exists,
                "parent_exists": session_parent_exists(&session_path),
            },
            "credentials": {
                "api_id_present": api_id.is_some(),
                "api_hash_present": api_hash_present,
            },
            "authorization": auth_report,
            "healthy": loaded_config.is_ok() && api_id.is_some() && api_hash_present,
        }))?
    );

    Ok(())
}

fn session_parent_exists(path: &Path) -> bool {
    path.parent().map(Path::exists).unwrap_or(false)
}
