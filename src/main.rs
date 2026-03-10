mod auth;
mod download;
mod link;
mod session;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "tw-dl",
    version,
    about = "Download Telegram media via MTProto",
    long_about = "A CLI tool for downloading videos, documents and photos from Telegram \
                  channels and groups that your account has access to.\n\n\
                  Configure TELEGRAM_API_ID and TELEGRAM_API_HASH in a .env file or your shell environment before use."
)]
struct Cli {
    /// Path to the session file (default: ~/.config/tw-dl/session)
    #[arg(long, global = true, value_name = "FILE")]
    session_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Interactive login: authenticate with your Telegram account and save the session.
    Login,

    /// Print the currently authenticated user's info (JSON).
    Whoami,

    /// Download media from a Telegram message.
    ///
    /// Examples:
    ///   tw-dl download https://t.me/mychannel/42
    ///   tw-dl download https://t.me/c/1234567890/10
    ///   tw-dl download --peer mychannel --msg 42 --out ./videos
    ///   tw-dl download --file links.txt --out ./videos
    Download {
        /// Telegram message link (https://t.me/... or https://t.me/c/...)
        link: Option<String>,

        /// Peer username or numeric channel id (alternative to a link)
        #[arg(long, value_name = "USERNAME_OR_ID")]
        peer: Option<String>,

        /// Message id (used together with --peer)
        #[arg(long, value_name = "ID")]
        msg: Option<i32>,

        /// File containing one Telegram link per line (batch download)
        #[arg(long, short = 'f', value_name = "FILE", conflicts_with_all = ["link", "peer", "msg"])]
        file: Option<PathBuf>,

        /// Output directory (default: ./downloads)
        #[arg(long, short = 'o', value_name = "DIR", default_value = "downloads")]
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load local .env configuration first; shell environment still takes precedence.
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    let session_path = session::resolve_session_path(cli.session_path)?;

    let api_id: i32 = std::env::var("TELEGRAM_API_ID")
        .context("TELEGRAM_API_ID environment variable is not set")?
        .parse()
        .context("TELEGRAM_API_ID must be a valid integer")?;

    let api_hash = std::env::var("TELEGRAM_API_HASH")
        .context("TELEGRAM_API_HASH environment variable is not set")?;

    match cli.command {
        Commands::Login => {
            auth::cmd_login(api_id, &api_hash, session_path).await?;
        }

        Commands::Whoami => {
            auth::cmd_whoami(api_id, session_path).await?;
        }

        Commands::Download { link, peer, msg, file, out } => {
            download::cmd_download(
                api_id,
                session_path,
                download::DownloadArgs {
                    link,
                    peer,
                    msg_id: msg,
                    out_dir: out,
                    file_list: file,
                },
            )
            .await?;
        }
    }

    Ok(())
}
