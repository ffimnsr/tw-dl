mod auth;
mod config;
mod doctor;
mod download;
mod link;
mod session;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "tw-dl",
    version,
    about = "Download Telegram media via MTProto",
    long_about = "A CLI tool for downloading videos, documents and photos from Telegram \
                  channels and groups that your account has access to.\n\n\
                  Configure TELEGRAM_API_ID and TELEGRAM_API_HASH in environment variables, \
                  a local .env file, or ~/.config/tw-dl/config.toml before use."
)]
struct Cli {
    /// Path to the session file (default: ~/.config/tw-dl/session)
    #[arg(long, global = true, value_name = "FILE")]
    session_path: Option<PathBuf>,

    /// Assume yes for confirmation prompts.
    #[arg(long, global = true)]
    yes: bool,

    /// Remove an existing session lock file before starting.
    #[arg(long, global = true)]
    force_unlock: bool,

    /// Consider a session lock stale after this many seconds.
    #[arg(long, global = true, default_value_t = 1800)]
    stale_lock_age_secs: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize ~/.config/tw-dl/config.toml with Telegram API credentials.
    Init {
        /// Overwrite an existing config.toml file.
        #[arg(long)]
        force: bool,
    },

    /// Interactive login: authenticate with your Telegram account and save the session.
    Login,

    /// Remove the saved Telegram session.
    Logout,

    /// Print the currently authenticated user's info (JSON).
    Whoami,

    /// List accessible chats, groups, and channels as JSON.
    ListChats,

    /// Resolve a Telegram link, username, or numeric id and print details as JSON.
    Resolve {
        /// Telegram message link, @username, bare username, or numeric chat id.
        target: String,
    },

    /// Download media from a Telegram message.
    ///
    /// Examples:
    ///   tw-dl download https://t.me/mychannel/42
    ///   tw-dl download https://t.me/c/1234567890/10
    ///   tw-dl download --peer mychannel --msg 42 --out ./videos
    ///   tw-dl download --file links.txt --out ./videos
    Download {
        /// Telegram message link (https://t.me/... or https://t.me/c/...)
        #[arg(conflicts_with_all = ["peer", "msg", "file"])]
        link: Option<String>,

        /// Peer username or numeric channel id (alternative to a link)
        #[arg(
            long,
            value_name = "USERNAME_OR_ID",
            requires = "msg",
            conflicts_with = "file"
        )]
        peer: Option<String>,

        /// Message id (used together with --peer)
        #[arg(long, value_name = "ID", requires = "peer", conflicts_with = "file")]
        msg: Option<i32>,

        /// File containing one Telegram link per line (batch download)
        #[arg(long, short = 'f', value_name = "FILE", conflicts_with_all = ["link", "peer", "msg"])]
        file: Option<PathBuf>,

        /// Output directory (default: ./downloads)
        #[arg(long, short = 'o', value_name = "DIR", default_value = "downloads")]
        out: PathBuf,

        /// Skip downloads when the destination file already exists.
        #[arg(long, conflicts_with_all = ["overwrite", "resume"])]
        skip_existing: bool,

        /// Overwrite the destination file or partial download if present.
        #[arg(long, conflicts_with_all = ["skip_existing", "resume"])]
        overwrite: bool,

        /// Resume from an existing .part file when present.
        #[arg(long, conflicts_with_all = ["skip_existing", "overwrite"])]
        resume: bool,

        /// Number of retry attempts for transient network failures.
        #[arg(long, default_value_t = 3)]
        retries: u32,

        /// Initial retry delay in milliseconds.
        #[arg(long, default_value_t = 1_000)]
        retry_delay_ms: u64,

        /// Maximum retry delay in milliseconds.
        #[arg(long, default_value_t = 30_000)]
        max_retry_delay_ms: u64,

        /// Concurrent jobs for batch downloads.
        #[arg(long, default_value_t = 1)]
        jobs: usize,

        /// Path to the batch checkpoint manifest (JSONL). Defaults next to --file.
        #[arg(long, value_name = "FILE", requires = "file")]
        checkpoint: Option<PathBuf>,

        /// Preview what would be downloaded without writing files.
        #[arg(long)]
        dry_run: bool,

        /// Replay only failed links from a previous checkpoint/manifest JSONL file.
        #[arg(long, value_name = "FILE", conflicts_with_all = ["link", "peer", "msg", "file"])]
        retry_from: Option<PathBuf>,

        /// Number of parallel chunk workers for large-file downloads.
        #[arg(long, default_value_t = 1)]
        parallel_chunks: usize,

        /// Keep partial .part files on timeout or download failure.
        #[arg(long)]
        keep_partial: bool,

        /// Timeout for individual network operations in milliseconds.
        #[arg(long)]
        request_timeout_ms: Option<u64>,

        /// Timeout for a single item download/inspect in milliseconds.
        #[arg(long)]
        item_timeout_ms: Option<u64>,

        /// Timeout for the entire batch run in milliseconds.
        #[arg(long)]
        batch_timeout_ms: Option<u64>,
    },

    /// Inspect a Telegram message and print its metadata as JSON.
    Inspect {
        /// Telegram message link (https://t.me/... or https://t.me/c/...)
        #[arg(conflicts_with_all = ["peer", "msg"])]
        link: Option<String>,

        /// Peer username or numeric channel id (alternative to a link)
        #[arg(long, value_name = "USERNAME_OR_ID", requires = "msg")]
        peer: Option<String>,

        /// Message id (used together with --peer)
        #[arg(long, value_name = "ID", requires = "peer")]
        msg: Option<i32>,
    },

    /// Validate config, session, and authorization state.
    Doctor,
}

#[tokio::main]
async fn main() -> ExitCode {
    // Load local .env configuration first; shell environment still takes precedence.
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", render_error(&error));
            ExitCode::from(classify_exit_code(&error))
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let session_path = session::resolve_session_path(cli.session_path)?;
    let config_path = session::resolve_config_path()?;
    let session_options = auth::SessionOptions {
        force_unlock: cli.force_unlock,
        stale_lock_timeout: std::time::Duration::from_secs(cli.stale_lock_age_secs),
    };

    match cli.command {
        Commands::Init { force } => {
            config::cmd_init(config_path, force, cli.yes)?;
        }
        Commands::Login => {
            let api_id = load_api_id(&config_path)?;
            let api_hash = load_api_hash(&config_path)?;
            auth::cmd_login(api_id, &api_hash, session_path, session_options).await?;
        }
        Commands::Logout => {
            auth::cmd_logout(&session_path, cli.yes)?;
        }
        Commands::Whoami => {
            let api_id = load_api_id(&config_path)?;
            auth::cmd_whoami(api_id, session_path, session_options).await?;
        }
        Commands::ListChats => {
            let api_id = load_api_id(&config_path)?;
            download::cmd_list_chats(api_id, session_path, session_options).await?;
        }
        Commands::Resolve { target } => {
            let api_id = load_api_id(&config_path)?;
            download::cmd_resolve(api_id, session_path, target, session_options).await?;
        }
        Commands::Download {
            link,
            peer,
            msg,
            file,
            out,
            skip_existing,
            overwrite,
            resume,
            retries,
            retry_delay_ms,
            max_retry_delay_ms,
            jobs,
            checkpoint,
            dry_run,
            retry_from,
            parallel_chunks,
            keep_partial,
            request_timeout_ms,
            item_timeout_ms,
            batch_timeout_ms,
        } => {
            let api_id = load_api_id(&config_path)?;
            download::cmd_download(
                api_id,
                session_path,
                session_options,
                download::DownloadArgs {
                    link,
                    peer,
                    msg_id: msg,
                    out_dir: out,
                    file_list: file,
                    collision: download::CollisionPolicy::from_flags(
                        skip_existing,
                        overwrite,
                        resume,
                    ),
                    retry: download::RetryConfig::new(
                        retries,
                        std::time::Duration::from_millis(retry_delay_ms),
                        std::time::Duration::from_millis(max_retry_delay_ms),
                    )?,
                    jobs,
                    checkpoint,
                    dry_run,
                    retry_from,
                    parallel_chunks,
                    keep_partial,
                    request_timeout: request_timeout_ms.map(std::time::Duration::from_millis),
                    item_timeout: item_timeout_ms.map(std::time::Duration::from_millis),
                    batch_timeout: batch_timeout_ms.map(std::time::Duration::from_millis),
                },
            )
            .await?;
        }
        Commands::Inspect { link, peer, msg } => {
            let api_id = load_api_id(&config_path)?;
            download::cmd_inspect(
                api_id,
                session_path,
                session_options,
                download::InspectArgs {
                    link,
                    peer,
                    msg_id: msg,
                },
            )
            .await?;
        }
        Commands::Doctor => {
            doctor::cmd_doctor(config_path, session_path).await?;
        }
    }

    Ok(())
}

fn load_api_id(config_path: &std::path::Path) -> Result<i32> {
    if let Ok(value) = std::env::var("TELEGRAM_API_ID") {
        return value
            .parse()
            .context("TELEGRAM_API_ID must be a valid integer");
    }

    let config = config::load_config(config_path)?;
    config
        .map(|cfg| cfg.telegram_api_id)
        .context(
            "TELEGRAM_API_ID is not set and no telegram_api_id was found in ~/.config/tw-dl/config.toml",
        )
}

fn load_api_hash(config_path: &std::path::Path) -> Result<String> {
    if let Ok(value) = std::env::var("TELEGRAM_API_HASH") {
        return Ok(value);
    }

    let config = config::load_config(config_path)?;
    config
        .map(|cfg| cfg.telegram_api_hash)
        .context(
            "TELEGRAM_API_HASH is not set and no telegram_api_hash was found in ~/.config/tw-dl/config.toml",
        )
}

fn render_error(error: &anyhow::Error) -> String {
    let rendered = format!("{:#}", error);
    let normalized = rendered.to_ascii_lowercase();

    if normalized.contains("channel private")
        || normalized.contains("chat admin required")
        || normalized.contains("invite request sent")
        || normalized.contains("forbidden")
        || normalized.contains("access denied")
    {
        return format!(
            "Access denied: your account cannot access the target chat or message. \
Join the chat first or use an account that already has permission.\n\n{}",
            rendered
        );
    }

    if normalized.contains("could not find a chat with id=") {
        return format!(
            "Chat lookup failed: the numeric peer id was not found in your dialogs. \
Numeric ids are account-relative and easy to misuse; prefer a full Telegram link or @username when possible.\n\n{}",
            rendered
        );
    }

    if normalized.contains("provide a link or --peer / --msg")
        || normalized.contains("could not parse link")
        || normalized.contains("temporary download file")
        || normalized.contains("refusing to overwrite existing file")
    {
        return format!("Usage error: {}", rendered);
    }

    rendered
}

fn classify_exit_code(error: &anyhow::Error) -> u8 {
    let normalized = format!("{:#}", error).to_ascii_lowercase();

    if normalized.contains("provide a link or --peer / --msg")
        || normalized.contains("could not parse link")
        || normalized.contains("must be")
        || normalized.contains("conflicts")
        || normalized.contains("usage error:")
    {
        return 2;
    }

    if normalized.contains("not logged in")
        || normalized.contains("phone code invalid")
        || normalized.contains("phone code expired")
        || normalized.contains("two-factor authentication failed")
        || normalized.contains("already in use by another tw-dl process")
    {
        return 3;
    }

    if normalized.contains("access denied")
        || normalized.contains("channel private")
        || normalized.contains("chat admin required")
        || normalized.contains("invite request sent")
        || normalized.contains("forbidden")
    {
        return 4;
    }

    if normalized.contains("temporary download file")
        || normalized.contains("refusing to overwrite existing file")
    {
        return 5;
    }

    1
}
