//! Download command implementation.
//!
//! Public surface: [`cmd_download`], [`cmd_inspect`], [`cmd_list_chats`],
//! [`cmd_resolve`], and the argument / option types.
//!
//! The module is split into focused sub-modules:
//!
//! | Module | Responsibility |
//! |--------|---------------|
//! | [`types`] | Argument structs, enums, RetryConfig, TimeoutConfig |
//! | [`client`] | ResilientClient, DownloadCaches |
//! | [`retry`] | Retry helpers |
//! | [`resolver`] | Peer / message resolution |
//! | [`describe`] | Value serialisation of Telegram objects |
//! | [`transfer`] | File download I/O |
//! | [`batch`] | Batch input streaming, job loop, checkpointing |

pub(crate) mod batch;
pub(crate) mod client;
pub(crate) mod describe;
pub(crate) mod resolver;
pub(crate) mod retry;
pub(crate) mod transfer;
pub(crate) mod types;

// ── re-exports of the public API ───────────────────────────────────────────────

pub use types::{
    BatchFailureMode, BatchInputFormat, CaptionSidecarFormat, CollisionPolicy, DownloadArgs,
    InspectArgs, MediaVariant, OutputLayout, RetryConfig,
};

// ── internal imports ────────────────────────────────────────────────────────────

use anyhow::{bail, Context, Result};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use batch::{
    BatchContext, ManifestReplayContext, BatchLineRange, checkpoint_path_for,
    checkpoint_path_for_batch_source, emit_download_result, maybe_archive_result,
    resolve_batch_source, run_batch_downloads, run_manifest_replay,
};
use client::{DownloadCaches, ResilientClient};
use describe::{describe_message, peer_kind_name};
use resolver::{fetch_message_with_retry, resolve_target, MessageSelectorArgs};
use transfer::{download_one, SingleDownloadRequest};
use types::TimeoutConfig;

// ── commands ───────────────────────────────────────────────────────────────────

/// Entry-point for the `download` subcommand.
pub async fn cmd_download(
    api_id: i32,
    session_path: PathBuf,
    session_options: crate::auth::SessionOptions,
    args: DownloadArgs,
) -> Result<()> {
    if args.jobs == 0 {
        bail!("--jobs must be at least 1");
    }
    if args.parallel_chunks == 0 {
        bail!("--parallel-chunks must be at least 1");
    }
    if args.file_list.is_some() && args.retry_from.is_some() {
        bail!("--file and --retry-from cannot be used together");
    }
    if matches!(args.failure_mode, BatchFailureMode::FailFast) && args.max_failures == Some(0) {
        bail!("--max-failures must be at least 1 when provided");
    }
    if let Some(0) = args.max_failures {
        bail!("--max-failures must be at least 1");
    }
    if let (Some(from_line), Some(to_line)) = (args.from_line, args.to_line) {
        if from_line > to_line {
            bail!("--from-line must be less than or equal to --to-line");
        }
    }
    if args.retry_from.is_some() && (args.from_line.is_some() || args.to_line.is_some()) {
        bail!("--from-line and --to-line can only be used with --file or stdin batch input");
    }
    if args.retry_from.is_some() && !matches!(args.input_format, BatchInputFormat::Auto) {
        bail!("--input-format applies to --file or stdin batch input, not --retry-from");
    }

    let client = Arc::new(ResilientClient::new(api_id, session_path, session_options).await?);
    ensure_authorized(&client).await?;
    let caches = Arc::new(DownloadCaches::default());
    let timeouts = TimeoutConfig::from_args(&args);
    let batch_source = resolve_batch_source(&args)?;
    let is_batch_mode = batch_source.is_some() || args.retry_from.is_some();
    let _ = &args.log_file;

    if !is_batch_mode {
        if args.max_failures.is_some() {
            bail!("--max-failures can only be used with --file, stdin, or --retry-from");
        }
        if args.from_line.is_some() || args.to_line.is_some() {
            bail!("--from-line and --to-line can only be used with --file or stdin batch input");
        }
        if !matches!(args.failure_mode, BatchFailureMode::ContinueOnError) {
            bail!("--fail-fast can only be used with --file, stdin, or --retry-from");
        }
    }

    let shutdown = install_shutdown_handler();
    crate::output::log_event(
        "download",
        "command_started",
        &json!({
            "batch_mode": is_batch_mode,
            "dry_run": args.dry_run,
            "jobs": args.jobs,
        }),
    )?;

    let run = async {
        if let Some(retry_from) = &args.retry_from {
            run_manifest_replay(
                &client,
                ManifestReplayContext {
                    manifest_path: retry_from.clone(),
                    out_dir: args.out_dir.clone(),
                    collision: args.collision,
                    retry: args.retry,
                    jobs: args.jobs,
                    failure_mode: args.failure_mode,
                    max_failures: args.max_failures,
                    checkpoint: checkpoint_path_for(retry_from, args.checkpoint.clone()),
                    dry_run: args.dry_run,
                    success_hook: args.success_hook.clone(),
                    failure_hook: args.failure_hook.clone(),
                    archive_path: args.archive_path.clone(),
                    media_variant: args.media_variant,
                    name_template: args.name_template.clone(),
                    output_layout: args.output_layout,
                    metadata_sidecar: args.metadata_sidecar,
                    caption_sidecar: args.caption_sidecar,
                    hash: args.hash,
                    redownload_on_mismatch: args.redownload_on_mismatch,
                    print_path_only: args.print_path_only,
                    parallel_chunks: args.parallel_chunks,
                    keep_partial: args.keep_partial,
                    timeouts,
                },
                shutdown.clone(),
                Arc::clone(&caches),
            )
            .await
        } else if let Some(source) = batch_source.clone() {
            run_batch_downloads(
                &client,
                BatchContext {
                    source,
                    out_dir: args.out_dir.clone(),
                    collision: args.collision,
                    retry: args.retry,
                    jobs: args.jobs,
                    input_format: args.input_format,
                    failure_mode: args.failure_mode,
                    max_failures: args.max_failures,
                    line_range: BatchLineRange::new(args.from_line, args.to_line)?,
                    checkpoint: checkpoint_path_for_batch_source(
                        batch_source.as_ref().expect("checked above"),
                        args.checkpoint.clone(),
                    ),
                    dry_run: args.dry_run,
                    success_hook: args.success_hook.clone(),
                    failure_hook: args.failure_hook.clone(),
                    archive_path: args.archive_path.clone(),
                    media_variant: args.media_variant,
                    name_template: args.name_template.clone(),
                    output_layout: args.output_layout,
                    metadata_sidecar: args.metadata_sidecar,
                    caption_sidecar: args.caption_sidecar,
                    hash: args.hash,
                    redownload_on_mismatch: args.redownload_on_mismatch,
                    print_path_only: args.print_path_only,
                    parallel_chunks: args.parallel_chunks,
                    keep_partial: args.keep_partial,
                    timeouts,
                },
                shutdown.clone(),
                Arc::clone(&caches),
            )
            .await
        } else {
            let request = SingleDownloadRequest {
                selector: MessageSelectorArgs {
                    link: args.link,
                    peer: args.peer,
                    msg_id: args.msg_id,
                },
                out_dir: args.out_dir,
                collision: args.collision,
                retry: args.retry,
                dry_run: args.dry_run,
                media_variant: args.media_variant,
                name_template: args.name_template.clone(),
                output_layout: args.output_layout,
                metadata_sidecar: args.metadata_sidecar,
                caption_sidecar: args.caption_sidecar,
                hash: args.hash,
                redownload_on_mismatch: args.redownload_on_mismatch,
                print_path_only: args.print_path_only,
                parallel_chunks: args.parallel_chunks,
                keep_partial: args.keep_partial,
                timeouts,
            };
            match download_one(&client, &request, &shutdown, &caches).await {
                Ok(result) => {
                    maybe_archive_result(args.archive_path.as_deref(), &result)?;
                    crate::output::log_event("download", "item_completed", &result)?;
                    crate::output::run_hook(
                        args.success_hook.as_deref(),
                        "download",
                        "item_completed",
                        &result,
                    )
                    .await?;
                    emit_download_result(&result)?;
                    Ok(())
                }
                Err(error) => {
                    let failure = json!({
                        "status": "failed",
                        "error": format!("{:#}", error),
                    });
                    crate::output::log_event("download", "item_failed", &failure)?;
                    crate::output::run_hook(
                        args.failure_hook.as_deref(),
                        "download",
                        "item_failed",
                        &failure,
                    )
                    .await?;
                    Err(error)
                }
            }
        }
    };

    if let Some(timeout) = timeouts.batch_timeout {
        tokio::time::timeout(timeout, run)
            .await
            .context("Batch timeout exceeded")?
    } else {
        run.await
    }
}

/// Entry-point for the `inspect` subcommand.
pub async fn cmd_inspect(
    api_id: i32,
    session_path: PathBuf,
    session_options: crate::auth::SessionOptions,
    args: InspectArgs,
) -> Result<()> {
    let client = Arc::new(ResilientClient::new(api_id, session_path, session_options).await?);
    ensure_authorized(&client).await?;
    let caches = Arc::new(DownloadCaches::default());

    let message = fetch_message_with_retry(
        &client,
        &MessageSelectorArgs {
            link: args.link,
            peer: args.peer,
            msg_id: args.msg_id,
        },
        RetryConfig::default(),
        TimeoutConfig::default(),
        &caches,
    )
    .await?;

    crate::output::write_command_output("inspect", describe_message(&message))?;
    Ok(())
}

/// Entry-point for the `list-chats` subcommand.
pub async fn cmd_list_chats(
    api_id: i32,
    session_path: PathBuf,
    session_options: crate::auth::SessionOptions,
) -> Result<()> {
    let client = Arc::new(ResilientClient::new(api_id, session_path, session_options).await?);
    ensure_authorized(&client).await?;

    let current_client = client.client().await;
    let mut dialogs = current_client.iter_dialogs();
    let mut chats = Vec::new();
    while let Some(dialog) = dialogs.next().await? {
        let last_message = dialog.last_message.as_ref().map(|message| {
            json!({
                "id": message.id(),
                "date": message.date().to_rfc3339(),
                "text": message.text(),
                "has_media": message.media().is_some(),
            })
        });

        chats.push(json!({
            "id": dialog.peer_id().bare_id(),
            "peer_id": dialog.peer_id().bot_api_dialog_id(),
            "kind": peer_kind_name(dialog.peer()),
            "name": dialog.peer().name(),
            "username": dialog.peer().username(),
            "usernames": dialog.peer().usernames(),
            "last_message": last_message,
        }));
    }

    crate::output::write_command_output("list-chats", json!({ "chats": chats }))?;
    Ok(())
}

/// Entry-point for the `resolve` subcommand.
pub async fn cmd_resolve(
    api_id: i32,
    session_path: PathBuf,
    target: String,
    session_options: crate::auth::SessionOptions,
) -> Result<()> {
    let client = Arc::new(ResilientClient::new(api_id, session_path, session_options).await?);
    ensure_authorized(&client).await?;
    let caches = Arc::new(DownloadCaches::default());

    let resolved = resolve_target(
        &client,
        &target,
        RetryConfig::default(),
        TimeoutConfig::default(),
        &caches,
    )
    .await?;
    crate::output::write_command_output("resolve", resolved)?;
    Ok(())
}

// ── internal helpers ───────────────────────────────────────────────────────────

async fn ensure_authorized(client: &ResilientClient) -> Result<()> {
    let current_client = client.client().await;
    if !current_client.is_authorized().await? {
        bail!("Not logged in. Run `tw-dl login` first.");
    }
    Ok(())
}

fn install_shutdown_handler() -> Arc<AtomicBool> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_flag = shutdown.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let already_set = signal_flag.swap(true, Ordering::SeqCst);
            if !already_set {
                crate::output::stderrln(
                    "Interrupt received. Stopping new work and leaving partial downloads resumable.",
                );
            }
        }
    });

    shutdown
}
