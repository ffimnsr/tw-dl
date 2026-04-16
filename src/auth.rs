use anyhow::{Context, Result};
use grammers_client::{Client, SenderPool, SignInError};
use grammers_session::storages::SqliteSession;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Build a Telegram `Client` connected to the network, loading or creating a
/// session at `session_path`.
///
/// Also spawns the network runner task on the Tokio runtime (a background task
/// that processes all low-level MTProto I/O).
pub async fn build_client(api_id: i32, session_path: &PathBuf) -> Result<AppClient> {
    crate::session::ensure_parent_dir(session_path)?;
    let session_lock = SessionLock::acquire(session_path)?;

    let session = Arc::new(
        SqliteSession::open(session_path)
            .await
            .with_context(|| format!("Failed to open session at '{}'", session_path.display()))?,
    );

    let SenderPool { runner, handle, .. } = SenderPool::new(Arc::clone(&session), api_id);
    let client = Client::new(handle);

    // Spawn the sender-pool runner; it shuts down automatically when all Client clones are
    // dropped or when the Tokio runtime exits.
    tokio::spawn(runner.run());

    Ok(AppClient {
        client,
        _session_lock: session_lock,
    })
}

/// Interactive `login` command.
///
/// Prompts for phone number and the SMS/app code, handles optional 2FA, and
/// saves the session.
pub async fn cmd_login(api_id: i32, api_hash: &str, session_path: PathBuf) -> Result<()> {
    let client = build_client(api_id, &session_path).await?;

    if client.is_authorized().await? {
        println!("Already logged in. Use `tw-dl whoami` to see current user.");
        return Ok(());
    }

    let phone = prompt("Phone number (international format, e.g. +15550001234): ")?;
    let login_token = client
        .request_login_code(&phone, api_hash)
        .await
        .context("Failed to request login code")?;

    let code = prompt("Enter the code you received: ")?;

    match client.sign_in(&login_token, &code).await {
        Ok(user) => {
            println!("Signed in as {} (id: {})", display_name(&user), user.id());
        }
        Err(SignInError::PasswordRequired(password_token)) => {
            let hint = password_token.hint().unwrap_or("none");
            let password = rpassword::prompt_password(format!(
                "Two-factor authentication password (hint: {}): ",
                hint
            ))
            .context("Failed to read password")?;
            client
                .check_password(password_token, password.trim())
                .await
                .context("Two-factor authentication failed")?;
            println!("Signed in successfully with 2FA.");
        }
        Err(e) => {
            return Err(e).context("Sign-in failed");
        }
    }

    println!("Session saved to '{}'.", session_path.display());
    Ok(())
}

/// `whoami` command – prints the currently logged-in user's details as JSON.
pub async fn cmd_whoami(api_id: i32, session_path: PathBuf) -> Result<()> {
    let client = build_client(api_id, &session_path).await?;

    if !client.is_authorized().await? {
        anyhow::bail!("Not logged in. Run `tw-dl login` first.");
    }

    let me = client
        .get_me()
        .await
        .context("Failed to fetch current user")?;

    let output = serde_json::json!({
        "id": me.id().bot_api_dialog_id(),
        "username": me.username(),
        "first_name": me.first_name(),
        "last_name": me.last_name(),
        "phone": me.phone(),
    });

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub fn cmd_logout(session_path: &Path) -> Result<()> {
    if crate::session::clear_session(session_path)? {
        println!("Cleared session at '{}'.", session_path.display());
    } else {
        println!("No session found at '{}'.", session_path.display());
    }

    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

fn display_name(user: &grammers_client::peer::User) -> String {
    let first = user.first_name().unwrap_or("");
    let last = user.last_name().unwrap_or("");
    match (first.is_empty(), last.is_empty()) {
        (false, false) => format!("{} {}", first, last),
        (false, true) => first.to_string(),
        (true, false) => last.to_string(),
        (true, true) => user
            .username()
            .map(|u| format!("@{}", u))
            .unwrap_or_else(|| format!("id:{}", user.id().bot_api_dialog_id())),
    }
}

pub struct AppClient {
    client: Client,
    _session_lock: SessionLock,
}

impl Deref for AppClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

struct SessionLock {
    path: PathBuf,
    _file: std::fs::File,
}

impl SessionLock {
    fn acquire(session_path: &Path) -> Result<Self> {
        let lock_path = session_path.with_extension("lock");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .with_context(|| {
                format!(
                    "Session '{}' is already in use by another tw-dl process. Wait for it to finish or remove stale lock file '{}'.",
                    session_path.display(),
                    lock_path.display()
                )
            })?;

        Ok(Self {
            path: lock_path,
            _file: file,
        })
    }
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
