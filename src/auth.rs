use anyhow::{Context, Result};
use grammers_client::{Client, SenderPool, SignInError};
use grammers_session::storages::SqliteSession;
use grammers_session::Session;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::IsTerminal;
use std::io::{self, BufRead, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
pub struct SessionOptions {
    pub force_unlock: bool,
    pub stale_lock_timeout: Duration,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            force_unlock: false,
            stale_lock_timeout: Duration::from_secs(30 * 60),
        }
    }
}

/// Build a Telegram `Client` connected to the network, loading or creating a
/// session at `session_path`.
///
/// Also spawns the network runner task on the Tokio runtime (a background task
/// that processes all low-level MTProto I/O).
pub async fn build_client(
    api_id: i32,
    session_path: &Path,
    session_options: SessionOptions,
) -> Result<AppClient> {
    crate::session::ensure_parent_dir(session_path)?;
    let session_lock = SessionLock::acquire(session_path, session_options)?;
    build_client_with_lock(api_id, session_path, session_lock).await
}

/// Build a client while the caller transfers ownership of an already-acquired
/// session lock. Reconnecting uses this to avoid tripping over its own lock file.
async fn build_client_with_lock(
    api_id: i32,
    session_path: &Path,
    session_lock: SessionLock,
) -> Result<AppClient> {
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
        session,
        session_lock: Some(session_lock),
    })
}

/// Drop auth keys cached for secondary datacenters.
///
/// Exported per-DC authorizations are tied to the account's login session. After
/// Telegram revokes a session (for example from another device or via support),
/// the keys cached for other DCs are dead while the home DC may still work. Media
/// requests then fail with `SESSION_REVOKED`. Clearing the cached keys makes
/// grammers export a fresh authorization the next time that DC is used.
async fn discard_stale_dc_auth_keys(session: &dyn Session) -> usize {
    let home_dc = session.home_dc_id();
    let mut cleared = 0;

    for dc_id in 1..=5 {
        if dc_id == home_dc {
            continue;
        }
        let Some(mut option) = session.dc_option(dc_id) else {
            continue;
        };
        if option.auth_key.is_none() {
            continue;
        }
        option.auth_key = None;
        session.set_dc_option(&option).await;
        cleared += 1;
    }

    cleared
}

/// Interactive `login` command.
///
/// Prompts for phone number and the SMS/app code, handles optional 2FA, and
/// saves the session.
pub async fn cmd_login(
    api_id: i32,
    api_hash: &str,
    session_path: PathBuf,
    session_options: SessionOptions,
) -> Result<()> {
    let client = build_client(api_id, &session_path, session_options).await?;

    if client.is_authorized().await? {
        println!("Already logged in. Use `tw-dl whoami` to see current user.");
        report_discarded_dc_auth_keys(&client).await;
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

    report_discarded_dc_auth_keys(&client).await;
    println!("Session saved to '{}'.", session_path.display());
    Ok(())
}

async fn report_discarded_dc_auth_keys(client: &AppClient) {
    let cleared = discard_stale_dc_auth_keys(client.session.as_ref()).await;
    if cleared > 0 {
        println!(
            "Cleared {} cached datacenter session key(s); they will be re-exported on next use.",
            cleared
        );
    }
}

/// `whoami` command – prints the currently logged-in user's details as JSON.
pub async fn cmd_whoami(
    api_id: i32,
    session_path: PathBuf,
    session_options: SessionOptions,
) -> Result<()> {
    let client = build_client(api_id, &session_path, session_options).await?;

    if !client.is_authorized().await? {
        anyhow::bail!("Not logged in. Run `tw-dl login` first.");
    }

    let me = client
        .get_me()
        .await
        .context("Failed to fetch current user")?;

    let output = crate::output::command_output(
        "whoami",
        serde_json::json!({
            "id": me.id().bot_api_dialog_id(),
            "username": me.username(),
            "first_name": me.first_name(),
            "last_name": me.last_name(),
            "phone": me.phone(),
        }),
    );

    crate::output::write_command_output("whoami", output["data"].clone())?;
    Ok(())
}

/// `session repair` command – drop cached per-datacenter auth keys while keeping
/// the account authorization intact.
///
/// Useful after Telegram revokes the sessions: the home datacenter may still be
/// authorized while keys cached for media datacenters are dead.
pub async fn cmd_session_repair(
    api_id: i32,
    session_path: PathBuf,
    session_options: SessionOptions,
) -> Result<()> {
    if !session_path.exists() {
        anyhow::bail!(
            "No session found at '{}'. Run `tw-dl login` first.",
            session_path.display()
        );
    }

    let client = build_client(api_id, &session_path, session_options).await?;

    if !client.is_authorized().await? {
        anyhow::bail!(
            "Session at '{}' is not authorized. Run `tw-dl login` first.",
            session_path.display()
        );
    }

    let cleared = discard_stale_dc_auth_keys(client.session.as_ref()).await;
    crate::output::write_command_output(
        "session-repair",
        serde_json::json!({
            "authorized": true,
            "cleared_dc_keys": cleared,
            "session_path": session_path.display().to_string(),
        }),
    )?;

    Ok(())
}

pub fn cmd_logout(session_path: &Path, yes: bool) -> Result<()> {
    if (session_path.exists() || session_path.with_extension("lock").exists()) && !yes {
        confirm_action(format!(
            "Remove the saved Telegram session at '{}'?",
            session_path.display()
        ))?;
    }

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

fn confirm_action(message: String) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("Confirmation required. Re-run with `--yes` in non-interactive mode.");
    }

    let answer = prompt(&format!("{} [y/N]: ", message))?;
    if matches!(answer.to_ascii_lowercase().as_str(), "y" | "yes") {
        return Ok(());
    }

    anyhow::bail!("Aborted by user.");
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
    session: Arc<SqliteSession>,
    session_lock: Option<SessionLock>,
}

impl AppClient {
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Replace the underlying client with a fresh connection while keeping the
    /// existing session lock. The old sender pool is dropped once the swap is done.
    pub(crate) async fn reconnect(&mut self, api_id: i32, session_path: &Path) -> Result<()> {
        let session_lock = self
            .session_lock
            .take()
            .context("Session lock was already released")?;

        let mut rebuilt = build_client_with_lock(api_id, session_path, session_lock).await?;
        std::mem::swap(self, &mut rebuilt);
        Ok(())
    }
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
    fn acquire(session_path: &Path, options: SessionOptions) -> Result<Self> {
        let lock_path = session_path.with_extension("lock");
        if options.force_unlock && lock_path.exists() {
            std::fs::remove_file(&lock_path).with_context(|| {
                format!(
                    "Failed to remove session lock file '{}' during force unlock",
                    lock_path.display()
                )
            })?;
        } else if let Some(age) = lock_age(&lock_path)? {
            if age >= options.stale_lock_timeout {
                std::fs::remove_file(&lock_path).with_context(|| {
                    format!(
                        "Failed to remove stale session lock file '{}'",
                        lock_path.display()
                    )
                })?;
            }
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .with_context(|| {
                format!(
                    "Session '{}' is already in use by another tw-dl process. Wait for it to finish, allow stale-lock expiry, or re-run with `--force-unlock` for lock file '{}'.",
                    session_path.display(),
                    lock_path.display()
                )
            })?;

        let metadata = LockMetadata {
            pid: std::process::id(),
            created_unix: unix_timestamp(),
        };
        serde_json::to_writer(&file, &metadata).context("Failed to write session lock metadata")?;

        Ok(Self {
            path: lock_path,
            _file: file,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct LockMetadata {
    pid: u32,
    created_unix: u64,
}

fn lock_age(lock_path: &Path) -> Result<Option<Duration>> {
    if !lock_path.exists() {
        return Ok(None);
    }

    let modified = std::fs::metadata(lock_path)
        .with_context(|| format!("Failed to inspect lock file '{}'", lock_path.display()))?
        .modified()
        .with_context(|| format!("Failed to read lock mtime '{}'", lock_path.display()))?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default();
    Ok(Some(age))
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grammers_session::storages::MemorySession;
    use grammers_session::types::DcOption;

    fn dc_option(id: i32) -> DcOption {
        DcOption {
            id,
            ipv4: "149.154.167.92:443".parse().unwrap(),
            ipv6: "[2001:67c:4e8:f004::a]:443".parse().unwrap(),
            auth_key: Some([7u8; 256]),
        }
    }

    #[tokio::test]
    async fn test_discard_stale_dc_auth_keys_keeps_home_dc() {
        let session = MemorySession::default();
        session.set_home_dc_id(5).await;
        session.set_dc_option(&dc_option(4)).await;
        session.set_dc_option(&dc_option(5)).await;

        let cleared = discard_stale_dc_auth_keys(&session).await;

        assert_eq!(cleared, 1);
        assert!(session.dc_option(4).unwrap().auth_key.is_none());
        assert!(session.dc_option(5).unwrap().auth_key.is_some());
    }

    #[tokio::test]
    async fn test_discard_stale_dc_auth_keys_is_idempotent() {
        let session = MemorySession::default();
        session.set_home_dc_id(5).await;
        session.set_dc_option(&dc_option(4)).await;

        assert_eq!(discard_stale_dc_auth_keys(&session).await, 1);
        assert_eq!(discard_stale_dc_auth_keys(&session).await, 0);
    }

    #[tokio::test]
    async fn test_reconnect_reuses_session_lock() {
        let dir = std::env::temp_dir().join(format!(
            "tw-dl-reconnect-{}-{}",
            std::process::id(),
            unix_timestamp()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let session_path = dir.join("session");
        let lock_path = session_path.with_extension("lock");

        let mut client = build_client(12345, &session_path, SessionOptions::default())
            .await
            .unwrap();
        assert!(lock_path.exists());

        client.reconnect(12345, &session_path).await.unwrap();
        assert!(lock_path.exists());

        drop(client);
        assert!(!lock_path.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
