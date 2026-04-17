use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Generator, Shell};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use tw_dl::{auth, config, doctor, download, manifest, output, session};

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

    /// Append structured JSON logs to this file.
    #[arg(long, global = true, value_name = "FILE")]
    log_file: Option<PathBuf>,

    /// Emit machine-readable JSON output.
    #[arg(long, global = true, conflicts_with = "human")]
    json: bool,

    /// Emit human-readable output.
    #[arg(long, global = true, conflicts_with = "json")]
    human: bool,

    /// Reduce non-essential stderr output.
    #[arg(long, global = true)]
    quiet: bool,

    /// Disable progress bars during downloads.
    #[arg(long, global = true)]
    no_progress: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BatchInputFormatArg {
    Auto,
    Text,
    Csv,
    Jsonl,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MediaVariantArg {
    Auto,
    LargestPhoto,
    OriginalDocument,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputLayoutArg {
    Flat,
    Chat,
    Date,
    MediaType,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CaptionSidecarArg {
    Txt,
    Json,
}

#[allow(clippy::large_enum_variant)]
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

        /// File containing batch input. Use "-" to read from stdin.
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

        /// Batch input format for --file or stdin.
        #[arg(long, value_enum, default_value_t = BatchInputFormatArg::Auto)]
        input_format: BatchInputFormatArg,

        /// Continue processing remaining batch items after a failure.
        #[arg(long, conflicts_with = "fail_fast")]
        continue_on_error: bool,

        /// Stop scheduling new batch items after the first failure.
        #[arg(long, conflicts_with = "continue_on_error")]
        fail_fast: bool,

        /// Stop scheduling new batch items after this many failures.
        #[arg(long, value_name = "N")]
        max_failures: Option<usize>,

        /// Start processing at this 1-based input line number.
        #[arg(long, value_name = "N")]
        from_line: Option<usize>,

        /// Stop processing after this 1-based input line number.
        #[arg(long, value_name = "N")]
        to_line: Option<usize>,

        /// Path to the batch checkpoint manifest (JSONL). Defaults next to --file or ./stdin.checkpoint.jsonl for stdin.
        #[arg(long, value_name = "FILE")]
        checkpoint: Option<PathBuf>,

        /// Preview what would be downloaded without writing files.
        #[arg(long)]
        dry_run: bool,

        /// Replay only failed links from a previous checkpoint/manifest JSONL file.
        #[arg(long, value_name = "FILE", conflicts_with_all = ["link", "peer", "msg", "file"])]
        retry_from: Option<PathBuf>,

        /// Run this shell command after each successful item, with JSON in `TW_DL_EVENT_JSON` and stdin.
        #[arg(long, value_name = "COMMAND")]
        success_hook: Option<String>,

        /// Run this shell command after each failed item, with JSON in `TW_DL_EVENT_JSON` and stdin.
        #[arg(long, value_name = "COMMAND")]
        failure_hook: Option<String>,

        /// Append processed item metadata to this JSONL archive file.
        #[arg(long, value_name = "FILE")]
        archive: Option<PathBuf>,

        /// Media selection strategy for supported messages and grouped media.
        #[arg(long, value_enum, default_value_t = MediaVariantArg::Auto)]
        media_variant: MediaVariantArg,

        /// File naming template, for example "{chat}_{msg_id}_{filename}".
        #[arg(long, value_name = "TEMPLATE")]
        name_template: Option<String>,

        /// Organize downloads into subdirectories by chat, date, or media type.
        #[arg(long, value_enum, default_value_t = OutputLayoutArg::Flat)]
        output_layout: OutputLayoutArg,

        /// Suffix the filename when the destination already exists.
        #[arg(long, conflicts_with_all = ["skip_existing", "overwrite", "resume"])]
        suffix_existing: bool,

        /// Write a JSON metadata sidecar next to each downloaded item.
        #[arg(long)]
        metadata_sidecar: bool,

        /// Write the message caption as a sidecar file.
        #[arg(long, value_enum, value_name = "FORMAT")]
        caption_sidecar: Option<CaptionSidecarArg>,

        /// Compute a SHA-256 hash for each completed download.
        #[arg(long)]
        hash: bool,

        /// Redownload from scratch if the final file size does not match the expected Telegram size.
        #[arg(long)]
        redownload_on_mismatch: bool,

        /// Print only the output path(s) for successful or planned items.
        #[arg(long)]
        print_path_only: bool,

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

    /// Generate shell completion scripts.
    Completions {
        /// Shell to generate completions for.
        #[arg(value_enum)]
        shell: Shell,
    },

    /// Import or export checkpoint/manifest files.
    Manifest {
        #[command(subcommand)]
        command: ManifestCommands,
    },
}

#[derive(Subcommand)]
enum ManifestCommands {
    /// Export a checkpoint or manifest to JSON or JSONL.
    Export {
        #[arg(long, value_name = "FILE")]
        input: PathBuf,
        #[arg(long, value_name = "FILE")]
        output: PathBuf,
    },

    /// Import a JSON or JSONL manifest into normalized JSONL format.
    Import {
        #[arg(long, value_name = "FILE")]
        input: PathBuf,
        #[arg(long, value_name = "FILE")]
        output: PathBuf,
    },
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
    output::init_output(
        output::OutputSettings {
            mode: if cli.human {
                output::OutputMode::Human
            } else if cli.json {
                output::OutputMode::Json
            } else if std::io::stdout().is_terminal() {
                output::OutputMode::Human
            } else {
                output::OutputMode::Json
            },
            quiet: cli.quiet,
            no_progress: cli.no_progress,
        },
        cli.log_file.clone(),
    )?;
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
            input_format,
            continue_on_error,
            fail_fast,
            max_failures,
            from_line,
            to_line,
            checkpoint,
            dry_run,
            retry_from,
            success_hook,
            failure_hook,
            archive,
            media_variant,
            name_template,
            output_layout,
            suffix_existing,
            metadata_sidecar,
            caption_sidecar,
            hash,
            redownload_on_mismatch,
            print_path_only,
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
                        suffix_existing,
                    ),
                    retry: download::RetryConfig::new(
                        retries,
                        std::time::Duration::from_millis(retry_delay_ms),
                        std::time::Duration::from_millis(max_retry_delay_ms),
                    )?,
                    jobs,
                    input_format: match input_format {
                        BatchInputFormatArg::Auto => download::BatchInputFormat::Auto,
                        BatchInputFormatArg::Text => download::BatchInputFormat::Text,
                        BatchInputFormatArg::Csv => download::BatchInputFormat::Csv,
                        BatchInputFormatArg::Jsonl => download::BatchInputFormat::Jsonl,
                    },
                    failure_mode: if fail_fast {
                        download::BatchFailureMode::FailFast
                    } else {
                        let _ = continue_on_error;
                        download::BatchFailureMode::ContinueOnError
                    },
                    max_failures,
                    from_line,
                    to_line,
                    checkpoint,
                    dry_run,
                    retry_from,
                    log_file: cli.log_file.clone(),
                    success_hook,
                    failure_hook,
                    archive_path: archive,
                    media_variant: match media_variant {
                        MediaVariantArg::Auto => download::MediaVariant::Auto,
                        MediaVariantArg::LargestPhoto => download::MediaVariant::LargestPhoto,
                        MediaVariantArg::OriginalDocument => {
                            download::MediaVariant::OriginalDocument
                        }
                    },
                    name_template,
                    output_layout: match output_layout {
                        OutputLayoutArg::Flat => download::OutputLayout::Flat,
                        OutputLayoutArg::Chat => download::OutputLayout::Chat,
                        OutputLayoutArg::Date => download::OutputLayout::Date,
                        OutputLayoutArg::MediaType => download::OutputLayout::MediaType,
                    },
                    metadata_sidecar,
                    caption_sidecar: caption_sidecar.map(|format| match format {
                        CaptionSidecarArg::Txt => download::CaptionSidecarFormat::Txt,
                        CaptionSidecarArg::Json => download::CaptionSidecarFormat::Json,
                    }),
                    hash,
                    redownload_on_mismatch,
                    print_path_only,
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
        Commands::Completions { shell } => {
            print_completions(shell, &mut Cli::command());
        }
        Commands::Manifest { command } => match command {
            ManifestCommands::Export { input, output } => {
                manifest::export_manifest(&input, &output).await?;
                let records = manifest::read_manifest_records(&output).await?;
                output::write_command_output(
                    "manifest-export",
                    manifest::manifest_export_result(&input, &output, records.len()),
                )?;
            }
            ManifestCommands::Import { input, output } => {
                manifest::import_manifest(&input, &output).await?;
                let records = manifest::read_manifest_records(&output).await?;
                output::write_command_output(
                    "manifest-import",
                    manifest::manifest_export_result(&input, &output, records.len()),
                )?;
            }
        },
    }

    Ok(())
}

fn print_completions<G: Generator>(generator: G, cmd: &mut clap::Command) {
    generate(
        generator,
        cmd,
        cmd.get_name().to_string(),
        &mut std::io::stdout(),
    );
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
