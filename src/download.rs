use anyhow::{bail, Context, Result};
use grammers_client::media::{Document, Downloadable, Media};
use grammers_client::message::Message;
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::IsTerminal;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{
    self, AsyncBufReadExt, AsyncRead, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use crate::link::{parse_link, ParsedLink};
use crate::manifest::{
    append_manifest_record, open_manifest_writer, read_manifest_records, ManifestRecord,
};

const DOWNLOAD_CHUNK_SIZE: i32 = 128 * 1024;
const MAX_PARALLEL_CHUNK_SIZE: i32 = 512 * 1024;
const PARALLEL_DOWNLOAD_THRESHOLD: u64 = 10 * 1024 * 1024;
const GROUP_FETCH_WINDOW: i32 = 64;

/// Options for the `download` command.
pub struct DownloadArgs {
    pub link: Option<String>,
    pub peer: Option<String>,
    pub msg_id: Option<i32>,
    pub out_dir: PathBuf,
    pub file_list: Option<PathBuf>,
    pub collision: CollisionPolicy,
    pub retry: RetryConfig,
    pub jobs: usize,
    pub input_format: BatchInputFormat,
    pub failure_mode: BatchFailureMode,
    pub max_failures: Option<usize>,
    pub from_line: Option<usize>,
    pub to_line: Option<usize>,
    pub checkpoint: Option<PathBuf>,
    pub dry_run: bool,
    pub retry_from: Option<PathBuf>,
    pub log_file: Option<PathBuf>,
    pub success_hook: Option<String>,
    pub failure_hook: Option<String>,
    pub archive_path: Option<PathBuf>,
    pub media_variant: MediaVariant,
    pub name_template: Option<String>,
    pub output_layout: OutputLayout,
    pub metadata_sidecar: bool,
    pub caption_sidecar: Option<CaptionSidecarFormat>,
    pub hash: bool,
    pub redownload_on_mismatch: bool,
    pub print_path_only: bool,
    pub parallel_chunks: usize,
    pub keep_partial: bool,
    pub request_timeout: Option<Duration>,
    pub item_timeout: Option<Duration>,
    pub batch_timeout: Option<Duration>,
}

/// Options for the `inspect` command.
pub struct InspectArgs {
    pub link: Option<String>,
    pub peer: Option<String>,
    pub msg_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchInputFormat {
    Auto,
    Text,
    Csv,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFailureMode {
    ContinueOnError,
    FailFast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollisionPolicy {
    Error,
    SkipExisting,
    Overwrite,
    Resume,
    SuffixExisting,
}

impl CollisionPolicy {
    pub fn from_flags(
        skip_existing: bool,
        overwrite: bool,
        resume: bool,
        suffix_existing: bool,
    ) -> Self {
        if skip_existing {
            Self::SkipExisting
        } else if overwrite {
            Self::Overwrite
        } else if resume {
            Self::Resume
        } else if suffix_existing {
            Self::SuffixExisting
        } else {
            Self::Error
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaVariant {
    Auto,
    LargestPhoto,
    OriginalDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputLayout {
    Flat,
    Chat,
    Date,
    MediaType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptionSidecarFormat {
    Txt,
    Json,
}

#[derive(Default)]
struct DownloadCaches {
    username_peer_refs: RwLock<HashMap<String, PeerRef>>,
    id_peer_refs: RwLock<HashMap<i64, PeerRef>>,
    id_peers: RwLock<HashMap<i64, Peer>>,
    messages: RwLock<HashMap<MessageCacheKey, Message>>,
    dialog_cache_loaded: AtomicBool,
}

#[derive(Clone, Copy, Default)]
struct TimeoutConfig {
    request_timeout: Option<Duration>,
    item_timeout: Option<Duration>,
    batch_timeout: Option<Duration>,
}

impl TimeoutConfig {
    fn from_args(args: &DownloadArgs) -> Self {
        Self {
            request_timeout: args.request_timeout,
            item_timeout: args.item_timeout,
            batch_timeout: args.batch_timeout,
        }
    }
}

async fn run_request<T, F>(timeout: Option<Duration>, timeout_message: &str, future: F) -> Result<T>
where
    F: Future<Output = T>,
{
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, future)
            .await
            .with_context(|| timeout_message.to_string())
    } else {
        Ok(future.await)
    }
}

struct ResilientClient {
    api_id: i32,
    session_path: PathBuf,
    session_options: crate::auth::SessionOptions,
    inner: tokio::sync::Mutex<crate::auth::AppClient>,
    pacing_until: tokio::sync::Mutex<Option<tokio::time::Instant>>,
}

impl ResilientClient {
    async fn new(
        api_id: i32,
        session_path: PathBuf,
        session_options: crate::auth::SessionOptions,
    ) -> Result<Self> {
        let inner = crate::auth::build_client(api_id, &session_path, session_options).await?;
        Ok(Self {
            api_id,
            session_path,
            session_options,
            inner: tokio::sync::Mutex::new(inner),
            pacing_until: tokio::sync::Mutex::new(None),
        })
    }

    async fn client(&self) -> Client {
        self.inner.lock().await.client()
    }

    async fn reconnect(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        *guard = crate::auth::build_client(self.api_id, &self.session_path, self.session_options)
            .await?;
        Ok(())
    }

    async fn wait_for_pacing(&self) {
        let until = *self.pacing_until.lock().await;
        if let Some(until) = until {
            let now = tokio::time::Instant::now();
            if until > now {
                tokio::time::sleep_until(until).await;
            }
        }
    }

    async fn apply_pacing(&self, delay: Duration) {
        let until = tokio::time::Instant::now() + delay;
        let mut guard = self.pacing_until.lock().await;
        match *guard {
            Some(existing) if existing > until => {}
            _ => *guard = Some(until),
        }
    }

    async fn clear_pacing_if_elapsed(&self) {
        let now = tokio::time::Instant::now();
        let mut guard = self.pacing_until.lock().await;
        if guard.is_some_and(|until| until <= now) {
            *guard = None;
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MessageCacheKey {
    peer_spec: PeerSpec,
    msg_id: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryConfig {
    pub fn new(retries: u32, initial_delay: Duration, max_delay: Duration) -> Result<Self> {
        if initial_delay.is_zero() {
            bail!("retry_delay_ms must be greater than zero");
        }
        if max_delay < initial_delay {
            bail!("max_retry_delay_ms must be greater than or equal to retry_delay_ms");
        }

        Ok(Self {
            retries,
            initial_delay,
            max_delay,
        })
    }

    fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        let base = self.initial_delay.saturating_mul(factor);
        let capped = base.min(self.max_delay);
        capped.saturating_add(Duration::from_millis(((attempt as u64) * 137) % 251))
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retries: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

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

async fn ensure_authorized(client: &ResilientClient) -> Result<()> {
    let current_client = client.client().await;
    if !current_client.is_authorized().await? {
        bail!("Not logged in. Run `tw-dl login` first.");
    }

    Ok(())
}

fn install_shutdown_handler() -> std::sync::Arc<AtomicBool> {
    let shutdown = std::sync::Arc::new(AtomicBool::new(false));
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

#[derive(Clone)]
struct MessageSelectorArgs {
    link: Option<String>,
    peer: Option<String>,
    msg_id: Option<i32>,
}

struct SingleDownloadRequest {
    selector: MessageSelectorArgs,
    out_dir: PathBuf,
    collision: CollisionPolicy,
    retry: RetryConfig,
    dry_run: bool,
    media_variant: MediaVariant,
    name_template: Option<String>,
    output_layout: OutputLayout,
    metadata_sidecar: bool,
    caption_sidecar: Option<CaptionSidecarFormat>,
    hash: bool,
    redownload_on_mismatch: bool,
    print_path_only: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
}

async fn download_one(
    client: &ResilientClient,
    request: &SingleDownloadRequest,
    shutdown: &AtomicBool,
    caches: &DownloadCaches,
) -> Result<Value> {
    let operation = async {
        warn_about_ambiguous_selector(&request.selector);
        let messages = fetch_messages_with_retry(
            client,
            &request.selector,
            request.retry,
            request.timeouts,
            caches,
        )
        .await?;
        if request.dry_run {
            plan_download(&messages, request).await
        } else {
            download_media(client, &messages, request, shutdown).await
        }
    };

    if let Some(timeout) = request.timeouts.item_timeout {
        tokio::time::timeout(timeout, operation)
            .await
            .context("Per-item timeout exceeded")?
    } else {
        operation.await
    }
}

async fn fetch_message_with_retry(
    client: &ResilientClient,
    selector: &MessageSelectorArgs,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Message> {
    let (peer_spec, msg_id) = resolve_peer_msg(selector)?;
    retry_fetch_message(client, &peer_spec, msg_id, retry, timeouts, caches).await
}

async fn fetch_messages_with_retry(
    client: &ResilientClient,
    selector: &MessageSelectorArgs,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Vec<Message>> {
    let (peer_spec, msg_id) = resolve_peer_msg(selector)?;
    let message = retry_fetch_message(client, &peer_spec, msg_id, retry, timeouts, caches).await?;
    if let Some(grouped_id) = message.grouped_id() {
        return fetch_grouped_messages(client, &peer_spec, msg_id, grouped_id, timeouts, caches)
            .await;
    }

    Ok(vec![message])
}

async fn fetch_grouped_messages(
    client: &ResilientClient,
    peer_spec: &PeerSpec,
    anchor_msg_id: i32,
    grouped_id: i64,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Vec<Message>> {
    let peer_ref = resolve_peer_ref(client, peer_spec, timeouts, caches).await?;
    let current_client = client.client().await;
    let upper_bound = anchor_msg_id.saturating_add(GROUP_FETCH_WINDOW);
    let lower_bound = anchor_msg_id.saturating_sub(GROUP_FETCH_WINDOW);
    let mut iter = current_client
        .iter_messages(peer_ref)
        .offset_id(upper_bound);
    let mut messages = Vec::new();

    while let Some(message) = run_request(
        timeouts.request_timeout,
        "Grouped message request timed out",
        iter.next(),
    )
    .await??
    {
        if message.id() < lower_bound {
            break;
        }
        if message.grouped_id() == Some(grouped_id) {
            caches.messages.write().await.insert(
                MessageCacheKey {
                    peer_spec: peer_spec.clone(),
                    msg_id: message.id(),
                },
                message.clone(),
            );
            messages.push(message);
        }
    }

    messages.sort_by_key(Message::id);
    messages.dedup_by_key(|message| message.id());

    if messages.is_empty() {
        bail!(
            "Grouped media {} was not found around message {}",
            grouped_id,
            anchor_msg_id
        );
    }

    Ok(messages)
}

async fn retry_fetch_message(
    client: &ResilientClient,
    spec: &PeerSpec,
    msg_id: i32,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Message> {
    let cache_key = MessageCacheKey {
        peer_spec: spec.clone(),
        msg_id,
    };
    if let Some(message) = caches.messages.read().await.get(&cache_key).cloned() {
        return Ok(message);
    }

    let mut attempt = 0u32;

    loop {
        match fetch_message_once(client, spec, msg_id, timeouts, caches).await {
            Ok(message) => {
                caches
                    .messages
                    .write()
                    .await
                    .insert(cache_key.clone(), message.clone());
                return Ok(message);
            }
            Err(error) => {
                attempt += 1;
                if let Some(delay) = retryable_delay(&error, attempt, retry) {
                    prepare_retry(client, &error, delay).await?;
                    crate::output::stderrln(format!(
                        "Retrying message fetch after error (attempt {}/{}): {}",
                        attempt, retry.retries, error
                    ));
                    continue;
                }
                return Err(error);
            }
        }
    }
}

// ── batch download ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum BatchSource {
    File(PathBuf),
    Stdin,
}

#[derive(Debug, Clone, Copy)]
struct BatchLineRange {
    from_line: Option<usize>,
    to_line: Option<usize>,
}

impl BatchLineRange {
    fn new(from_line: Option<usize>, to_line: Option<usize>) -> Result<Self> {
        if matches!(from_line, Some(0)) {
            bail!("--from-line must be at least 1");
        }
        if matches!(to_line, Some(0)) {
            bail!("--to-line must be at least 1");
        }

        Ok(Self { from_line, to_line })
    }

    fn contains(self, line_number: usize) -> bool {
        if self.from_line.is_some_and(|from| line_number < from) {
            return false;
        }
        if self.to_line.is_some_and(|to| line_number > to) {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone)]
struct BatchEntry {
    line_number: usize,
    link: String,
}

struct BatchContext {
    source: BatchSource,
    out_dir: PathBuf,
    collision: CollisionPolicy,
    retry: RetryConfig,
    jobs: usize,
    input_format: BatchInputFormat,
    failure_mode: BatchFailureMode,
    max_failures: Option<usize>,
    line_range: BatchLineRange,
    checkpoint: PathBuf,
    dry_run: bool,
    success_hook: Option<String>,
    failure_hook: Option<String>,
    archive_path: Option<PathBuf>,
    media_variant: MediaVariant,
    name_template: Option<String>,
    output_layout: OutputLayout,
    metadata_sidecar: bool,
    caption_sidecar: Option<CaptionSidecarFormat>,
    hash: bool,
    redownload_on_mismatch: bool,
    print_path_only: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
}

struct ManifestReplayContext {
    manifest_path: PathBuf,
    out_dir: PathBuf,
    collision: CollisionPolicy,
    retry: RetryConfig,
    jobs: usize,
    failure_mode: BatchFailureMode,
    max_failures: Option<usize>,
    checkpoint: PathBuf,
    dry_run: bool,
    success_hook: Option<String>,
    failure_hook: Option<String>,
    archive_path: Option<PathBuf>,
    media_variant: MediaVariant,
    name_template: Option<String>,
    output_layout: OutputLayout,
    metadata_sidecar: bool,
    caption_sidecar: Option<CaptionSidecarFormat>,
    hash: bool,
    redownload_on_mismatch: bool,
    print_path_only: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
}

struct LinkJobContext<'a> {
    client: &'a Arc<ResilientClient>,
    entries: Vec<BatchEntry>,
    out_dir: &'a Path,
    collision: CollisionPolicy,
    retry: RetryConfig,
    jobs: usize,
    failure_mode: BatchFailureMode,
    max_failures: Option<usize>,
    checkpoint_path: &'a Path,
    dry_run: bool,
    success_hook: Option<&'a str>,
    failure_hook: Option<&'a str>,
    archive_path: Option<&'a Path>,
    media_variant: MediaVariant,
    name_template: Option<&'a str>,
    output_layout: OutputLayout,
    metadata_sidecar: bool,
    caption_sidecar: Option<CaptionSidecarFormat>,
    hash: bool,
    redownload_on_mismatch: bool,
    print_path_only: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
    caches: Arc<DownloadCaches>,
    shutdown: std::sync::Arc<AtomicBool>,
}

#[derive(Debug)]
struct BatchTaskResult {
    line_number: usize,
    link: String,
    result: Result<Value>,
}

struct BatchResultContext<'a> {
    checkpoint_path: &'a Path,
    writer: &'a mut fs::File,
    completed_links: &'a mut HashSet<String>,
    success_count: &'a mut usize,
    failure_count: &'a mut usize,
    skipped_count: &'a mut usize,
    success_hook: Option<&'a str>,
    failure_hook: Option<&'a str>,
    archive_path: Option<&'a Path>,
}

fn resolve_batch_source(args: &DownloadArgs) -> Result<Option<BatchSource>> {
    if let Some(path) = &args.file_list {
        return Ok(Some(if path == Path::new("-") {
            BatchSource::Stdin
        } else {
            BatchSource::File(path.clone())
        }));
    }

    if args.link.is_none()
        && args.peer.is_none()
        && args.msg_id.is_none()
        && args.retry_from.is_none()
        && !std::io::stdin().is_terminal()
    {
        return Ok(Some(BatchSource::Stdin));
    }

    Ok(None)
}

fn checkpoint_path_for_batch_source(
    source: &BatchSource,
    override_path: Option<PathBuf>,
) -> PathBuf {
    if let Some(path) = override_path {
        return path;
    }

    match source {
        BatchSource::File(path) => checkpoint_path_for(path, None),
        BatchSource::Stdin => PathBuf::from("stdin.checkpoint.jsonl"),
    }
}

fn normalize_link_key(link: &str) -> String {
    match parse_link(link.trim()) {
        Ok(ParsedLink::Username { username, msg_id }) => {
            format!("username:{}:{}", username.to_ascii_lowercase(), msg_id)
        }
        Ok(ParsedLink::Channel { channel_id, msg_id }) => {
            format!("channel:{}:{}", channel_id, msg_id)
        }
        Err(_) => link.trim().to_string(),
    }
}

fn infer_batch_input_format(
    source: &BatchSource,
    requested: BatchInputFormat,
    lines: &[SourceLine],
) -> BatchInputFormat {
    if !matches!(requested, BatchInputFormat::Auto) {
        return requested;
    }

    if let BatchSource::File(path) = source {
        if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
            match ext.to_ascii_lowercase().as_str() {
                "csv" => return BatchInputFormat::Csv,
                "jsonl" | "ndjson" => return BatchInputFormat::Jsonl,
                _ => {}
            }
        }
    }

    let Some(first_non_empty) = lines.iter().find(|line| !line.text.trim().is_empty()) else {
        return BatchInputFormat::Text;
    };
    let trimmed = first_non_empty.text.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('"') {
        BatchInputFormat::Jsonl
    } else if trimmed.contains(',') {
        BatchInputFormat::Csv
    } else {
        BatchInputFormat::Text
    }
}

#[derive(Debug, Clone)]
struct SourceLine {
    line_number: usize,
    text: String,
}

async fn read_batch_source_lines(source: &BatchSource) -> Result<Vec<SourceLine>> {
    let reader: Box<dyn AsyncRead + Unpin + Send> = match source {
        BatchSource::File(path) => Box::new(
            fs::File::open(path)
                .await
                .with_context(|| format!("Cannot open file list '{}'", path.display()))?,
        ),
        BatchSource::Stdin => Box::new(io::stdin()),
    };

    let mut lines = BufReader::new(reader).lines();
    let mut out = Vec::new();
    let mut line_number = 0usize;
    while let Some(line) = lines.next_line().await? {
        line_number += 1;
        out.push(SourceLine {
            line_number,
            text: line,
        });
    }
    Ok(out)
}

fn parse_text_batch_entries(lines: &[SourceLine], line_range: BatchLineRange) -> Vec<BatchEntry> {
    lines
        .iter()
        .filter(|line| line_range.contains(line.line_number))
        .filter_map(|line| {
            let link = line.text.trim();
            if link.is_empty() || link.starts_with('#') {
                return None;
            }
            Some(BatchEntry {
                line_number: line.line_number,
                link: link.to_string(),
            })
        })
        .collect()
}

fn parse_csv_record(line: &str) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(field.trim().to_string());
                field.clear();
            }
            _ => field.push(ch),
        }
    }

    if in_quotes {
        bail!("Unterminated quoted CSV field");
    }

    fields.push(field.trim().to_string());
    Ok(fields)
}

fn is_probably_link(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with("https://t.me/")
        || trimmed.starts_with("http://t.me/")
        || parse_link(trimmed).is_ok()
}

fn extract_csv_link(row: &[String], selected_column: Option<usize>) -> Option<String> {
    if let Some(index) = selected_column {
        return row
            .get(index)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    }

    row.iter()
        .find(|value| is_probably_link(value))
        .or_else(|| row.iter().find(|value| !value.trim().is_empty()))
        .map(|value| value.trim().to_string())
}

fn parse_csv_batch_entries(
    lines: &[SourceLine],
    line_range: BatchLineRange,
) -> Result<Vec<BatchEntry>> {
    let mut entries = Vec::new();
    let mut selected_column = None;
    let mut header_processed = false;

    for line in lines
        .iter()
        .filter(|line| line_range.contains(line.line_number))
    {
        if line.text.trim().is_empty() {
            continue;
        }

        let row = parse_csv_record(&line.text)
            .with_context(|| format!("Invalid CSV row at line {}", line.line_number))?;
        if !header_processed {
            header_processed = true;
            if let Some((index, _)) = row.iter().enumerate().find(|(_, cell)| {
                matches!(
                    cell.trim().to_ascii_lowercase().as_str(),
                    "link" | "url" | "message_link" | "message_url"
                )
            }) {
                selected_column = Some(index);
                continue;
            }
        }

        if let Some(link) = extract_csv_link(&row, selected_column) {
            entries.push(BatchEntry {
                line_number: line.line_number,
                link,
            });
        }
    }

    Ok(entries)
}

fn parse_jsonl_batch_entries(
    lines: &[SourceLine],
    line_range: BatchLineRange,
) -> Result<Vec<BatchEntry>> {
    let mut entries = Vec::new();

    for line in lines
        .iter()
        .filter(|line| line_range.contains(line.line_number))
    {
        let trimmed = line.text.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("Invalid JSONL entry at line {}", line.line_number))?;
        let link = match value {
            Value::String(link) => link,
            Value::Object(map) => ["link", "url", "message_link", "message_url"]
                .into_iter()
                .find_map(|key| map.get(key).and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!(
                        "JSONL entry at line {} must be a string or object containing link/url/message_link/message_url",
                        line.line_number
                    )
                })?,
            _ => {
                bail!(
                    "JSONL entry at line {} must be a string or object",
                    line.line_number
                )
            }
        };

        entries.push(BatchEntry {
            line_number: line.line_number,
            link: link.trim().to_string(),
        });
    }

    Ok(entries)
}

async fn load_batch_entries(
    source: &BatchSource,
    input_format: BatchInputFormat,
    line_range: BatchLineRange,
) -> Result<Vec<BatchEntry>> {
    let lines = read_batch_source_lines(source).await?;
    let inferred_format = infer_batch_input_format(source, input_format, &lines);

    match inferred_format {
        BatchInputFormat::Auto => unreachable!("auto format should be resolved before parsing"),
        BatchInputFormat::Text => Ok(parse_text_batch_entries(&lines, line_range)),
        BatchInputFormat::Csv => parse_csv_batch_entries(&lines, line_range),
        BatchInputFormat::Jsonl => parse_jsonl_batch_entries(&lines, line_range),
    }
}

async fn run_batch_downloads(
    client: &Arc<ResilientClient>,
    ctx: BatchContext,
    shutdown: std::sync::Arc<AtomicBool>,
    caches: Arc<DownloadCaches>,
) -> Result<()> {
    let entries = load_batch_entries(&ctx.source, ctx.input_format, ctx.line_range).await?;

    prefetch_messages_for_entries(client, &entries, ctx.retry, ctx.timeouts, &caches).await?;

    run_link_jobs(LinkJobContext {
        client,
        entries,
        out_dir: &ctx.out_dir,
        collision: ctx.collision,
        retry: ctx.retry,
        jobs: ctx.jobs,
        failure_mode: ctx.failure_mode,
        max_failures: ctx.max_failures,
        checkpoint_path: &ctx.checkpoint,
        dry_run: ctx.dry_run,
        success_hook: ctx.success_hook.as_deref(),
        failure_hook: ctx.failure_hook.as_deref(),
        archive_path: ctx.archive_path.as_deref(),
        media_variant: ctx.media_variant,
        name_template: ctx.name_template.as_deref(),
        output_layout: ctx.output_layout,
        metadata_sidecar: ctx.metadata_sidecar,
        caption_sidecar: ctx.caption_sidecar,
        hash: ctx.hash,
        redownload_on_mismatch: ctx.redownload_on_mismatch,
        print_path_only: ctx.print_path_only,
        parallel_chunks: ctx.parallel_chunks,
        keep_partial: ctx.keep_partial,
        timeouts: ctx.timeouts,
        caches,
        shutdown,
    })
    .await
}

async fn run_manifest_replay(
    client: &Arc<ResilientClient>,
    ctx: ManifestReplayContext,
    shutdown: std::sync::Arc<AtomicBool>,
    caches: Arc<DownloadCaches>,
) -> Result<()> {
    let entries = load_failed_links(&ctx.manifest_path).await?;
    if entries.is_empty() {
        crate::output::write_command_output(
            "download",
            json!({
                "status": "completed",
                "mode": "retry-from",
                "succeeded": 0,
                "skipped": 0,
                "checkpoint_skipped": 0,
                "failed": 0,
                "checkpoint": ctx.checkpoint.display().to_string(),
                "source_manifest": ctx.manifest_path.display().to_string(),
                "message": "No failed links found in manifest",
            }),
        )?;
        return Ok(());
    }

    prefetch_messages_for_entries(client, &entries, ctx.retry, ctx.timeouts, &caches).await?;

    run_link_jobs(LinkJobContext {
        client,
        entries,
        out_dir: &ctx.out_dir,
        collision: ctx.collision,
        retry: ctx.retry,
        jobs: ctx.jobs,
        failure_mode: ctx.failure_mode,
        max_failures: ctx.max_failures,
        checkpoint_path: &ctx.checkpoint,
        dry_run: ctx.dry_run,
        success_hook: ctx.success_hook.as_deref(),
        failure_hook: ctx.failure_hook.as_deref(),
        archive_path: ctx.archive_path.as_deref(),
        media_variant: ctx.media_variant,
        name_template: ctx.name_template.as_deref(),
        output_layout: ctx.output_layout,
        metadata_sidecar: ctx.metadata_sidecar,
        caption_sidecar: ctx.caption_sidecar,
        hash: ctx.hash,
        redownload_on_mismatch: ctx.redownload_on_mismatch,
        print_path_only: ctx.print_path_only,
        parallel_chunks: ctx.parallel_chunks,
        keep_partial: ctx.keep_partial,
        timeouts: ctx.timeouts,
        caches,
        shutdown,
    })
    .await
}

async fn run_link_jobs(ctx: LinkJobContext<'_>) -> Result<()> {
    let LinkJobContext {
        client,
        entries,
        out_dir,
        collision,
        retry,
        jobs,
        failure_mode,
        max_failures,
        checkpoint_path,
        dry_run,
        success_hook,
        failure_hook,
        archive_path,
        media_variant,
        name_template,
        output_layout,
        metadata_sidecar,
        caption_sidecar,
        hash,
        redownload_on_mismatch,
        print_path_only,
        parallel_chunks,
        keep_partial,
        timeouts,
        caches,
        shutdown,
    } = ctx;

    let mut completed_links = load_completed_links(checkpoint_path).await?;
    let mut writer = open_manifest_writer(checkpoint_path).await?;
    let mut join_set = JoinSet::new();
    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    let mut skipped_count = 0usize;
    let mut checkpoint_skip_count = 0usize;
    let mut duplicate_skip_count = 0usize;
    let mut seen_links = HashSet::new();
    let mut stop_scheduling = false;
    let client_handle = client.clone();

    for entry in entries {
        let line_number = entry.line_number;
        let link = entry.link;
        let link_key = normalize_link_key(&link);

        if completed_links.contains(&link_key) {
            checkpoint_skip_count += 1;
            crate::output::stderrln(format!(
                "[{}] Skipping completed checkpoint entry {}",
                line_number, link
            ));
            continue;
        }

        if !seen_links.insert(link_key.clone()) {
            duplicate_skip_count += 1;
            let value = json!({
                "status": "skipped",
                "reason": "duplicate-input",
                "input_line": line_number,
                "link": link,
            });
            append_manifest_record(
                &mut writer,
                &ManifestRecord {
                    index: line_number,
                    link,
                    status: "skipped".to_string(),
                    file: None,
                    error: Some("duplicate input".to_string()),
                    timestamp_unix: crate::output::unix_timestamp(),
                    canonical_source_link: None,
                },
            )
            .await?;
            skipped_count += 1;
            crate::output::log_event("download", "item_skipped", &value)?;
            emit_download_result(&value)?;
            continue;
        }

        while join_set.len() >= jobs {
            let finished = join_set
                .join_next()
                .await
                .expect("join set length checked")?;
            handle_batch_result(
                finished,
                BatchResultContext {
                    checkpoint_path,
                    writer: &mut writer,
                    completed_links: &mut completed_links,
                    success_count: &mut success_count,
                    failure_count: &mut failure_count,
                    skipped_count: &mut skipped_count,
                    success_hook,
                    failure_hook,
                    archive_path,
                },
            )
            .await?;
            if should_stop_batch(failure_mode, max_failures, failure_count) {
                stop_scheduling = true;
                break;
            }
        }

        if shutdown.load(Ordering::SeqCst) || stop_scheduling {
            break;
        }

        let request = SingleDownloadRequest {
            selector: MessageSelectorArgs {
                link: Some(link.clone()),
                peer: None,
                msg_id: None,
            },
            out_dir: out_dir.to_path_buf(),
            collision,
            retry,
            dry_run,
            media_variant,
            name_template: name_template.map(ToOwned::to_owned),
            output_layout,
            metadata_sidecar,
            caption_sidecar,
            hash,
            redownload_on_mismatch,
            print_path_only,
            parallel_chunks,
            keep_partial,
            timeouts,
        };
        let task_client = client_handle.clone();
        let task_shutdown = shutdown.clone();
        let task_caches = Arc::clone(&caches);
        join_set.spawn(async move {
            BatchTaskResult {
                line_number,
                link,
                result: download_one(&task_client, &request, &task_shutdown, &task_caches).await,
            }
        });
    }

    while let Some(task_result) = join_set.join_next().await {
        let finished = task_result?;
        handle_batch_result(
            finished,
            BatchResultContext {
                checkpoint_path,
                writer: &mut writer,
                completed_links: &mut completed_links,
                success_count: &mut success_count,
                failure_count: &mut failure_count,
                skipped_count: &mut skipped_count,
                success_hook,
                failure_hook,
                archive_path,
            },
        )
        .await?;
    }

    if shutdown.load(Ordering::SeqCst) {
        bail!(
            "Batch interrupted with {} succeeded, {} skipped, {} failed, {} restored from checkpoint.",
            success_count,
            skipped_count,
            failure_count,
            checkpoint_skip_count + duplicate_skip_count
        );
    }

    if failure_count > 0 {
        bail!(
            "Batch download completed with {} succeeded, {} skipped, {} restored from checkpoint or dedupe, and {} failed.",
            success_count,
            skipped_count,
            checkpoint_skip_count + duplicate_skip_count,
            failure_count
        );
    }

    let summary = json!({
        "status": "completed",
        "succeeded": success_count,
        "skipped": skipped_count,
        "checkpoint_skipped": checkpoint_skip_count,
        "dedupe_skipped": duplicate_skip_count,
        "failed": failure_count,
        "checkpoint": checkpoint_path.display().to_string(),
        "dry_run": dry_run,
    });
    crate::output::log_event("download", "batch_completed", &summary)?;
    emit_download_result(&summary)?;
    Ok(())
}

async fn handle_batch_result(result: BatchTaskResult, ctx: BatchResultContext<'_>) -> Result<()> {
    let BatchResultContext {
        checkpoint_path,
        writer,
        completed_links,
        success_count,
        failure_count,
        skipped_count,
        success_hook,
        failure_hook,
        archive_path,
    } = ctx;

    match result.result {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("downloaded");
            if status == "skipped" {
                *skipped_count += 1;
            } else {
                *success_count += 1;
                completed_links.insert(normalize_link_key(&result.link));
            }

            append_manifest_record(
                writer,
                &ManifestRecord {
                    index: result.line_number,
                    link: result.link,
                    status: status.to_string(),
                    file: value
                        .get("file")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    error: None,
                    timestamp_unix: crate::output::unix_timestamp(),
                    canonical_source_link: value
                        .get("canonical_source_link")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                },
            )
            .await?;

            maybe_archive_result(archive_path, &value)?;
            crate::output::log_event("download", "item_completed", &value)?;
            crate::output::run_hook(success_hook, "download", "item_completed", &value).await?;
            emit_download_result(&value)?;
        }
        Err(error) => {
            *failure_count += 1;
            let rendered = format!("{:#}", error);
            let failure_value = json!({
                "status": "failed",
                "input_line": result.line_number,
                "link": result.link.clone(),
                "error": rendered,
            });
            append_manifest_record(
                writer,
                &ManifestRecord {
                    index: result.line_number,
                    link: result.link.clone(),
                    status: "failed".to_string(),
                    file: None,
                    error: Some(
                        failure_value
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    ),
                    timestamp_unix: crate::output::unix_timestamp(),
                    canonical_source_link: None,
                },
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to update checkpoint '{}'",
                    checkpoint_path.display()
                )
            })?;

            crate::output::log_event("download", "item_failed", &failure_value)?;
            crate::output::run_hook(failure_hook, "download", "item_failed", &failure_value)
                .await?;
            crate::output::stderrln(format!(
                "[{}] ERROR {}: {}",
                result.line_number,
                result.link,
                failure_value["error"].as_str().unwrap_or_default()
            ));
        }
    }

    Ok(())
}

fn should_stop_batch(
    failure_mode: BatchFailureMode,
    max_failures: Option<usize>,
    failure_count: usize,
) -> bool {
    if matches!(failure_mode, BatchFailureMode::FailFast) && failure_count > 0 {
        return true;
    }

    max_failures.is_some_and(|limit| failure_count >= limit)
}

fn emit_download_result(value: &Value) -> Result<()> {
    if value
        .get("print_path_only")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(items) = value.get("items").and_then(Value::as_array) {
            for item in items {
                if let Some(path) = item.get("file").and_then(Value::as_str) {
                    println!("{}", path);
                }
            }
            return Ok(());
        }
        if let Some(path) = value.get("file").and_then(Value::as_str) {
            println!("{}", path);
            return Ok(());
        }
    }

    crate::output::write_command_output("download", value.clone())
}

fn maybe_archive_result(path: Option<&Path>, value: &Value) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    crate::output::archive_append(
        path,
        &crate::output::event_output("download", "archive_item", value.clone()),
    )
}

async fn load_completed_links(path: &Path) -> Result<HashSet<String>> {
    let mut completed = HashSet::new();
    let records = match read_manifest_records(path).await {
        Ok(records) => records,
        Err(error) if format!("{:#}", error).contains("does not exist") => return Ok(completed),
        Err(error) => return Err(error),
    };
    for entry in records {
        if entry.status == "downloaded" {
            completed.insert(normalize_link_key(&entry.link));
        }
    }

    Ok(completed)
}

async fn load_failed_links(path: &Path) -> Result<Vec<BatchEntry>> {
    let records = read_manifest_records(path).await?;
    let mut failed = Vec::new();
    let mut index = 0usize;

    for entry in records {
        if entry.status == "failed" {
            index += 1;
            failed.push(BatchEntry {
                line_number: index,
                link: entry.link,
            });
        }
    }

    Ok(failed)
}
fn checkpoint_path_for(list_path: &Path, override_path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = override_path {
        return path;
    }

    let base_name = list_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "batch".to_string());
    list_path.with_file_name(format!("{}.checkpoint.jsonl", base_name))
}

// ── peer / message resolution ─────────────────────────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum PeerSpec {
    Username(String),
    ChannelId(i64),
}

async fn resolve_target(
    client: &ResilientClient,
    target: &str,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Value> {
    let trimmed = target.trim();

    if parse_link(trimmed).is_ok() {
        let selector = MessageSelectorArgs {
            link: Some(trimmed.to_string()),
            peer: None,
            msg_id: None,
        };
        let (peer_spec, msg_id) = resolve_peer_msg(&selector)?;
        let message =
            retry_fetch_message(client, &peer_spec, msg_id, retry, timeouts, caches).await?;
        return Ok(json!({
            "input": trimmed,
            "input_type": "message_link",
            "peer_spec": describe_peer_spec(&peer_spec),
            "message_id": msg_id,
            "message": describe_message(&message),
        }));
    }

    if let Ok(channel_id) = trimmed.parse::<i64>() {
        warn_about_numeric_peer_input(trimmed);
        let peer = find_peer_by_id(client, channel_id, timeouts, caches).await?;
        return Ok(json!({
            "input": trimmed,
            "input_type": "numeric_id",
            "peer": describe_peer(&peer),
        }));
    }

    let username = trimmed.trim_start_matches('@');
    let peer_ref = resolve_peer_ref(
        client,
        &PeerSpec::Username(username.to_string()),
        timeouts,
        caches,
    )
    .await?;
    let peer = resolve_peer_from_ref(client, peer_ref, timeouts, caches).await?;

    Ok(json!({
        "input": trimmed,
        "input_type": "username",
        "peer": describe_peer(&peer),
    }))
}

fn warn_about_ambiguous_selector(selector: &MessageSelectorArgs) {
    if let Some(peer) = selector.peer.as_deref() {
        warn_about_numeric_peer_input(peer);
    }
}

fn warn_about_numeric_peer_input(input: &str) {
    if input.parse::<i64>().is_ok() {
        crate::output::stderrln(
            "Warning: numeric peer ids are Telegram bare ids and can be ambiguous. \
Prefer a full Telegram message link or @username when possible.",
        );
    }
}

fn resolve_peer_msg(args: &MessageSelectorArgs) -> Result<(PeerSpec, i32)> {
    if let (Some(peer), Some(msg)) = (&args.peer, args.msg_id) {
        let spec = if let Ok(id) = peer.parse::<i64>() {
            PeerSpec::ChannelId(id)
        } else {
            PeerSpec::Username(peer.trim_start_matches('@').to_string())
        };
        return Ok((spec, msg));
    }

    let link_str = args
        .link
        .as_deref()
        .context("Provide a link or --peer / --msg")?;

    match parse_link(link_str)? {
        ParsedLink::Username { username, msg_id } => Ok((PeerSpec::Username(username), msg_id)),
        ParsedLink::Channel { channel_id, msg_id } => Ok((PeerSpec::ChannelId(channel_id), msg_id)),
    }
}

async fn fetch_message_once(
    client: &ResilientClient,
    spec: &PeerSpec,
    msg_id: i32,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Message> {
    let peer_ref = resolve_peer_ref(client, spec, timeouts, caches).await?;
    client.wait_for_pacing().await;
    let current_client = client.client().await;
    let mut messages = run_request(
        timeouts.request_timeout,
        "Message request timed out",
        current_client.get_messages_by_id(peer_ref, &[msg_id]),
    )
    .await
    .with_context(|| format!("Failed to fetch message id={}", msg_id))??;

    messages
        .pop()
        .flatten()
        .with_context(|| format!("Message id={} not found in chat", msg_id))
}

async fn find_peer_by_id(
    client: &ResilientClient,
    target_id: i64,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Peer> {
    if let Some(peer) = caches.id_peers.read().await.get(&target_id).cloned() {
        return Ok(peer);
    }

    populate_dialog_caches(client, timeouts, caches).await?;
    if let Some(peer) = caches.id_peers.read().await.get(&target_id).cloned() {
        return Ok(peer);
    }

    bail!(
        "Could not find a chat with id={} in your dialogs. Make sure your account has joined that channel/group.",
        target_id
    );
}

async fn find_peer_ref_by_id(
    client: &ResilientClient,
    target_id: i64,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<PeerRef> {
    if let Some(peer_ref) = caches.id_peer_refs.read().await.get(&target_id).copied() {
        return Ok(peer_ref);
    }

    populate_dialog_caches(client, timeouts, caches).await?;
    if let Some(peer_ref) = caches.id_peer_refs.read().await.get(&target_id).copied() {
        return Ok(peer_ref);
    }

    bail!(
        "Could not find a chat with id={} in your dialogs. Make sure your account has joined that channel/group.",
        target_id
    );
}

async fn resolve_peer_ref(
    client: &ResilientClient,
    spec: &PeerSpec,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<PeerRef> {
    match spec {
        PeerSpec::Username(username) => {
            if let Some(peer_ref) = caches
                .username_peer_refs
                .read()
                .await
                .get(username)
                .copied()
            {
                return Ok(peer_ref);
            }

            client.wait_for_pacing().await;
            let current_client = client.client().await;
            let peer = run_request(
                timeouts.request_timeout,
                "Username resolution timed out",
                current_client.resolve_username(username),
            )
            .await
            .with_context(|| format!("Failed to resolve username '{}'", username))??
            .with_context(|| format!("Username '{}' not found", username))?;
            let peer_ref = peer.to_ref().await.with_context(|| {
                format!("Could not obtain a usable reference for '@{}'", username)
            })?;
            caches
                .username_peer_refs
                .write()
                .await
                .insert(username.clone(), peer_ref);
            caches
                .id_peer_refs
                .write()
                .await
                .insert(peer.id().bare_id(), peer_ref);
            caches
                .id_peers
                .write()
                .await
                .insert(peer.id().bare_id(), peer);
            Ok(peer_ref)
        }
        PeerSpec::ChannelId(channel_id) => {
            find_peer_ref_by_id(client, *channel_id, timeouts, caches).await
        }
    }
}

async fn resolve_peer_from_ref(
    client: &ResilientClient,
    peer_ref: PeerRef,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Peer> {
    if let Some(peer) = caches
        .id_peers
        .read()
        .await
        .get(&peer_ref.id.bare_id())
        .cloned()
    {
        return Ok(peer);
    }

    populate_dialog_caches(client, timeouts, caches).await?;
    if let Some(peer) = caches
        .id_peers
        .read()
        .await
        .get(&peer_ref.id.bare_id())
        .cloned()
    {
        return Ok(peer);
    }

    find_peer_by_id(client, peer_ref.id.bare_id(), timeouts, caches).await
}

async fn populate_dialog_caches(
    client: &ResilientClient,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<()> {
    if caches.dialog_cache_loaded.load(Ordering::SeqCst) {
        return Ok(());
    }

    let current_client = client.client().await;
    let mut dialogs = current_client.iter_dialogs();
    loop {
        client.wait_for_pacing().await;
        let next_dialog = run_request(
            timeouts.request_timeout,
            "Dialog iteration timed out",
            dialogs.next(),
        )
        .await?;
        let Some(dialog) = next_dialog? else {
            break;
        };
        let bare_id = dialog.peer_id().bare_id();
        let peer_ref = dialog.peer_ref();
        let peer = dialog.peer().clone();
        caches.id_peer_refs.write().await.insert(bare_id, peer_ref);
        caches.id_peers.write().await.insert(bare_id, peer.clone());
        if let Some(username) = peer.username() {
            caches
                .username_peer_refs
                .write()
                .await
                .insert(username.to_string(), peer_ref);
        }
    }
    caches.dialog_cache_loaded.store(true, Ordering::SeqCst);
    Ok(())
}

async fn prefetch_messages_for_entries(
    client: &Arc<ResilientClient>,
    entries: &[BatchEntry],
    _retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<()> {
    let mut grouped: HashMap<PeerSpec, Vec<i32>> = HashMap::new();
    for entry in entries {
        let selector = MessageSelectorArgs {
            link: Some(entry.link.clone()),
            peer: None,
            msg_id: None,
        };
        if let Ok((peer_spec, msg_id)) = resolve_peer_msg(&selector) {
            grouped.entry(peer_spec).or_default().push(msg_id);
        }
    }

    for (peer_spec, msg_ids) in grouped {
        let peer_ref = match resolve_peer_ref(client, &peer_spec, timeouts, caches).await {
            Ok(peer_ref) => peer_ref,
            Err(_) => continue,
        };
        let unique_ids: HashSet<i32> = msg_ids.into_iter().collect();
        let ids: Vec<i32> = unique_ids.into_iter().collect();
        for chunk in ids.chunks(100) {
            client.wait_for_pacing().await;
            let current_client = client.client().await;
            let messages = match run_request(
                timeouts.request_timeout,
                "Prefetch message request timed out",
                current_client.get_messages_by_id(peer_ref, chunk),
            )
            .await
            {
                Ok(Ok(messages)) => messages,
                Err(_) => continue,
                Ok(Err(_)) => continue,
            };
            for message in messages.into_iter().flatten() {
                caches.messages.write().await.insert(
                    MessageCacheKey {
                        peer_spec: peer_spec.clone(),
                        msg_id: message.id(),
                    },
                    message,
                );
            }
        }
    }

    Ok(())
}

// ── media download and inspect ────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ResolvedDownloadTarget {
    out_path: PathBuf,
    filename: String,
    media_type: String,
    mime_type: String,
    expected_size: u64,
    canonical_source_link: Option<String>,
    grouped_id: Option<i64>,
    caption: String,
    metadata: Value,
}

#[derive(Debug, Clone)]
enum DownloadSource {
    Document(Document),
    Photo(grammers_client::media::Photo),
}

impl DownloadSource {
    fn expected_size(&self) -> u64 {
        match self {
            Self::Document(doc) => doc.size().unwrap_or(0) as u64,
            Self::Photo(photo) => photo.size().unwrap_or(0) as u64,
        }
    }

    fn mime_type(&self) -> String {
        match self {
            Self::Document(doc) => doc
                .mime_type()
                .unwrap_or("application/octet-stream")
                .to_string(),
            Self::Photo(_) => "image/jpeg".to_string(),
        }
    }

    fn media_type(&self) -> &'static str {
        match self {
            Self::Document(_) => "document",
            Self::Photo(_) => "photo",
        }
    }
}

async fn download_media(
    client: &ResilientClient,
    messages: &[Message],
    request: &SingleDownloadRequest,
    shutdown: &AtomicBool,
) -> Result<Value> {
    let mut items = Vec::new();
    for message in messages {
        let Some((target, source)) = resolve_download_target(request, message).await? else {
            continue;
        };
        let outcome = match &source {
            DownloadSource::Document(doc) => {
                stream_download_verified(client, doc, &target, request, shutdown).await?
            }
            DownloadSource::Photo(photo) => {
                stream_download_verified(client, photo, &target, request, shutdown).await?
            }
        };
        let mut item = download_result_value(
            message,
            &target.out_path,
            target.filename.clone(),
            &target.mime_type,
            target.expected_size,
            outcome,
        );
        augment_item_result(&mut item, &target, message, request).await?;
        items.push(item);
    }

    summarize_download_items(items, request)
}

async fn plan_download(messages: &[Message], request: &SingleDownloadRequest) -> Result<Value> {
    let mut items = Vec::new();
    for message in messages {
        let Some((target, _source)) = resolve_download_target(request, message).await? else {
            continue;
        };
        let collision_result =
            inspect_output_collision(&target.out_path, request.collision).await?;
        items.push(json!({
            "status": "planned",
            "message_id": message.id(),
            "peer_id": message.peer_id().bot_api_dialog_id(),
            "canonical_source_link": target.canonical_source_link,
            "file": target.out_path.display().to_string(),
            "filename": target.filename,
            "mime_type": target.mime_type,
            "size": target.expected_size,
            "media_type": target.media_type,
            "grouped_id": target.grouped_id,
            "collision_policy": collision_policy_name(request.collision),
            "collision_result": collision_result,
            "dry_run": true,
        }));
    }

    summarize_download_items(items, request)
}

async fn stream_download_verified<D: Downloadable + Clone + Send + Sync + 'static>(
    client: &ResilientClient,
    downloadable: &D,
    target: &ResolvedDownloadTarget,
    request: &SingleDownloadRequest,
    shutdown: &AtomicBool,
) -> Result<DownloadOutcome> {
    let mut redownloaded = false;
    loop {
        let outcome = stream_download(
            client,
            downloadable,
            &target.out_path,
            StreamDownloadConfig {
                total_bytes: target.expected_size,
                collision: if redownloaded {
                    CollisionPolicy::Overwrite
                } else {
                    request.collision
                },
                retry: request.retry,
                parallel_chunks: request.parallel_chunks,
                keep_partial: request.keep_partial,
                request_timeout: request.timeouts.request_timeout,
            },
            shutdown,
        )
        .await?;
        match verify_size_match(&target.out_path, target.expected_size) {
            Ok(()) => return Ok(outcome),
            Err(_error) if request.redownload_on_mismatch && !redownloaded => {
                redownloaded = true;
                if fs::try_exists(&target.out_path).await.unwrap_or(false) {
                    let _ = fs::remove_file(&target.out_path).await;
                }
                let partial = partial_download_path(&target.out_path);
                if fs::try_exists(&partial).await.unwrap_or(false) {
                    let _ = fs::remove_file(&partial).await;
                }
                crate::output::stderrln(format!(
                    "Size mismatch for '{}', redownloading from scratch.",
                    target.out_path.display()
                ));
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}

fn verify_size_match(out_path: &Path, expected_size: u64) -> Result<()> {
    if expected_size == 0 {
        return Ok(());
    }

    let actual_size = std::fs::metadata(out_path)
        .with_context(|| format!("Failed to inspect '{}'", out_path.display()))?
        .len();
    if actual_size != expected_size {
        bail!(
            "Downloaded file '{}' has size {} but Telegram reported {}",
            out_path.display(),
            actual_size,
            expected_size
        );
    }

    Ok(())
}

async fn augment_item_result(
    item: &mut Value,
    target: &ResolvedDownloadTarget,
    message: &Message,
    request: &SingleDownloadRequest,
) -> Result<()> {
    if let Some(object) = item.as_object_mut() {
        object.insert(
            "media_type".to_string(),
            Value::String(target.media_type.clone()),
        );
        object.insert(
            "grouped_id".to_string(),
            target.grouped_id.map(Value::from).unwrap_or(Value::Null),
        );
    }

    if request.hash && item.get("status").and_then(Value::as_str) == Some("downloaded") {
        let sha256 = sha256_file(&target.out_path)?;
        item.as_object_mut()
            .context("download result must be an object")?
            .insert("sha256".to_string(), Value::String(sha256));
    }

    if request.metadata_sidecar {
        let metadata_path = write_metadata_sidecar(target, message, item).await?;
        item.as_object_mut()
            .context("download result must be an object")?
            .insert(
                "metadata_sidecar".to_string(),
                Value::String(metadata_path.display().to_string()),
            );
    }

    if let Some(format) = request.caption_sidecar {
        if !target.caption.is_empty() {
            let caption_path = write_caption_sidecar(target, format).await?;
            item.as_object_mut()
                .context("download result must be an object")?
                .insert(
                    "caption_sidecar".to_string(),
                    Value::String(caption_path.display().to_string()),
                );
        }
    }
    Ok(())
}

fn summarize_download_items(items: Vec<Value>, request: &SingleDownloadRequest) -> Result<Value> {
    if items.is_empty() {
        bail!("No downloadable media matched the selected media variant");
    }

    let first = items[0].clone();
    let all_statuses: Vec<&str> = items
        .iter()
        .filter_map(|item| item.get("status").and_then(Value::as_str))
        .collect();
    let status = if all_statuses.iter().all(|status| *status == "planned") {
        "planned"
    } else if all_statuses.iter().all(|status| *status == "skipped") {
        "skipped"
    } else {
        "downloaded"
    };
    let files: Vec<Value> = items
        .iter()
        .filter_map(|item| item.get("file").cloned())
        .collect();

    let mut summary = json!({
        "status": status,
        "item_count": items.len(),
        "files": files,
        "items": items,
        "print_path_only": request.print_path_only,
    });

    if let Some(object) = summary.as_object_mut() {
        for key in [
            "file",
            "filename",
            "mime_type",
            "message_id",
            "peer_id",
            "canonical_source_link",
            "grouped_id",
        ] {
            if let Some(value) = first.get(key).cloned() {
                object.insert(key.to_string(), value);
            }
        }
    }

    Ok(summary)
}

async fn write_metadata_sidecar(
    target: &ResolvedDownloadTarget,
    message: &Message,
    item: &Value,
) -> Result<PathBuf> {
    let path = sidecar_path(&target.out_path, "json");
    let metadata = json!({
        "message_id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "grouped_id": message.grouped_id(),
        "chat": message.peer().map(describe_peer),
        "sender": message.sender().map(describe_peer),
        "date": message.date().to_rfc3339(),
        "caption": message.text(),
        "mime_type": target.mime_type,
        "media_type": target.media_type,
        "source_link": target.canonical_source_link,
        "file": target.out_path.display().to_string(),
        "filename": target.filename,
        "result": item,
        "message": target.metadata.clone(),
    });
    fs::write(&path, serde_json::to_vec_pretty(&metadata)?)
        .await
        .with_context(|| format!("Failed to write metadata sidecar '{}'", path.display()))?;
    Ok(path)
}

async fn write_caption_sidecar(
    target: &ResolvedDownloadTarget,
    format: CaptionSidecarFormat,
) -> Result<PathBuf> {
    let (path, bytes) = match format {
        CaptionSidecarFormat::Txt => (
            sidecar_path(&target.out_path, "txt"),
            target.caption.as_bytes().to_vec(),
        ),
        CaptionSidecarFormat::Json => (
            sidecar_path(&target.out_path, "caption.json"),
            serde_json::to_vec_pretty(&json!({
                "caption": target.caption,
                "file": target.out_path.display().to_string(),
                "source_link": target.canonical_source_link,
            }))?,
        ),
    };
    fs::write(&path, bytes)
        .await
        .with_context(|| format!("Failed to write caption sidecar '{}'", path.display()))?;
    Ok(path)
}

fn sidecar_path(out_path: &Path, suffix: &str) -> PathBuf {
    let file_name = out_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    out_path.with_file_name(format!("{}.{}", file_name, suffix))
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("Failed to read '{}' for hashing", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn describe_message(message: &Message) -> Value {
    let peer = message.peer().map(describe_peer);
    let sender = message.sender().map(describe_peer);

    json!({
        "id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "canonical_source_link": canonical_source_link(message),
        "date": message.date().to_rfc3339(),
        "text": message.text(),
        "has_media": message.media().is_some(),
        "peer": peer,
        "sender": sender,
        "media": message.media().map(|media| describe_media(&media)),
    })
}

fn describe_peer_spec(spec: &PeerSpec) -> Value {
    match spec {
        PeerSpec::Username(username) => json!({
            "type": "username",
            "value": username,
        }),
        PeerSpec::ChannelId(channel_id) => json!({
            "type": "channel_id",
            "value": channel_id,
        }),
    }
}

fn describe_peer(peer: &Peer) -> Value {
    json!({
        "bare_id": peer.id().bare_id(),
        "id": peer.id().bot_api_dialog_id(),
        "type": peer_kind_name(peer),
        "name": peer.name(),
        "username": peer.username(),
        "usernames": peer.usernames(),
    })
}

fn peer_kind_name(peer: &Peer) -> &'static str {
    match peer {
        Peer::User(_) => "user",
        Peer::Group(_) => "group",
        Peer::Channel(_) => "channel",
    }
}

fn describe_media(media: &Media) -> Value {
    match media {
        Media::Document(doc) => json!({
            "type": "document",
            "id": doc.id(),
            "filename": doc.name(),
            "mime_type": doc.mime_type(),
            "size": doc.size(),
            "creation_date": doc.creation_date().map(|date| date.to_rfc3339()),
        }),
        Media::Photo(photo) => json!({
            "type": "photo",
            "id": photo.id(),
            "size": photo.size(),
            "ttl_seconds": photo.ttl_seconds(),
            "thumb_count": photo.thumbs().len(),
        }),
        other => json!({
            "type": "unsupported",
            "debug": format!("{:?}", other),
        }),
    }
}

fn download_result_value(
    message: &Message,
    out_path: &Path,
    filename: String,
    mime_type: &str,
    expected_size: u64,
    outcome: DownloadOutcome,
) -> Value {
    let actual_size = std::fs::metadata(out_path)
        .map(|meta| meta.len())
        .unwrap_or(expected_size);
    let status = match outcome {
        DownloadOutcome::Downloaded { .. } => "downloaded",
        DownloadOutcome::SkippedExisting => "skipped",
    };
    let resumed = matches!(outcome, DownloadOutcome::Downloaded { resumed: true });

    json!({
        "status": status,
        "file": out_path.display().to_string(),
        "filename": filename,
        "size": actual_size,
        "mime_type": mime_type,
        "message_id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "canonical_source_link": canonical_source_link(message),
        "resumed": resumed,
    })
}

fn canonical_source_link(message: &Message) -> Option<String> {
    if let Some(peer) = message.peer() {
        if let Some(username) = peer.username() {
            return Some(format!("https://t.me/{}/{}", username, message.id()));
        }
    }

    let dialog_id = message.peer_id().bot_api_dialog_id().to_string();
    dialog_id
        .strip_prefix("-100")
        .map(|channel_id| format!("https://t.me/c/{}/{}", channel_id, message.id()))
}

fn resolve_download_source(
    message: &Message,
    media_variant: MediaVariant,
) -> Option<DownloadSource> {
    match message.media()? {
        Media::Document(doc) => {
            if matches!(media_variant, MediaVariant::LargestPhoto) {
                None
            } else {
                Some(DownloadSource::Document(doc.clone()))
            }
        }
        Media::Photo(photo) => {
            if matches!(media_variant, MediaVariant::OriginalDocument) {
                None
            } else {
                Some(DownloadSource::Photo(photo.clone()))
            }
        }
        _ => None,
    }
}

fn peer_label(message: &Message) -> String {
    message
        .peer()
        .and_then(|peer| peer.username().map(ToOwned::to_owned))
        .or_else(|| {
            message
                .peer()
                .and_then(|peer| peer.name().map(sanitize_filename))
        })
        .unwrap_or_else(|| message.peer_id().bot_api_dialog_id().to_string())
}

fn message_date_slug(message: &Message) -> String {
    message.date().format("%Y-%m-%d").to_string()
}

fn output_dir_for_layout(
    base: &Path,
    layout: OutputLayout,
    message: &Message,
    media_type: &str,
) -> PathBuf {
    match layout {
        OutputLayout::Flat => base.to_path_buf(),
        OutputLayout::Chat => base.join(sanitize_filename(&peer_label(message))),
        OutputLayout::Date => base.join(message_date_slug(message)),
        OutputLayout::MediaType => base.join(media_type),
    }
}

fn render_name_template(
    template: Option<&str>,
    base_filename: &str,
    message: &Message,
    media_type: &str,
) -> String {
    let Some(template) = template else {
        return base_filename.to_string();
    };
    let rendered = template
        .replace("{chat}", &peer_label(message))
        .replace(
            "{chat_id}",
            &message.peer_id().bot_api_dialog_id().to_string(),
        )
        .replace("{msg_id}", &message.id().to_string())
        .replace("{filename}", base_filename)
        .replace("{media_type}", media_type)
        .replace("{date}", &message_date_slug(message));

    if Path::new(&rendered).extension().is_none() {
        if let Some(ext) = Path::new(base_filename)
            .extension()
            .and_then(|ext| ext.to_str())
        {
            return format!("{}.{}", rendered, ext);
        }
    }

    rendered
}

fn choose_filename_for_source(source: &DownloadSource, message: &Message) -> String {
    match source {
        DownloadSource::Document(doc) => choose_filename_document(doc, message),
        DownloadSource::Photo(_) => format!(
            "photo_{}_{}.jpg",
            message.peer_id().bot_api_dialog_id(),
            message.id()
        ),
    }
}

async fn resolve_target_path(
    request: &SingleDownloadRequest,
    message: &Message,
    source: &DownloadSource,
) -> Result<(PathBuf, String)> {
    let media_type = source.media_type();
    let base_dir =
        output_dir_for_layout(&request.out_dir, request.output_layout, message, media_type);
    fs::create_dir_all(&base_dir)
        .await
        .with_context(|| format!("Failed to create output directory '{}'", base_dir.display()))?;

    let base_filename = choose_filename_for_source(source, message);
    let rendered = render_name_template(
        request.name_template.as_deref(),
        &base_filename,
        message,
        media_type,
    );
    let filename = sanitize_filename(&rendered);
    let out_path = safe_output_path(&base_dir, &filename)?;
    if matches!(request.collision, CollisionPolicy::SuffixExisting) {
        let suffixed = choose_suffix_output_path(&out_path).await?;
        return Ok((
            suffixed.clone(),
            suffixed
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(&filename)
                .to_string(),
        ));
    }

    Ok((out_path, filename))
}

async fn choose_suffix_output_path(base_path: &Path) -> Result<PathBuf> {
    if !fs::try_exists(base_path).await?
        && !fs::try_exists(&partial_download_path(base_path)).await?
    {
        return Ok(base_path.to_path_buf());
    }

    let stem = base_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("file");
    let ext = base_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext))
        .unwrap_or_default();

    for index in 1..10_000usize {
        let candidate = base_path.with_file_name(format!("{} ({}){}", stem, index, ext));
        if !fs::try_exists(&candidate).await?
            && !fs::try_exists(&partial_download_path(&candidate)).await?
        {
            return Ok(candidate);
        }
    }

    bail!(
        "Could not find a free suffixed filename for '{}'",
        base_path.display()
    )
}

async fn resolve_download_target(
    request: &SingleDownloadRequest,
    message: &Message,
) -> Result<Option<(ResolvedDownloadTarget, DownloadSource)>> {
    let Some(source) = resolve_download_source(message, request.media_variant) else {
        return Ok(None);
    };
    let (out_path, filename) = resolve_target_path(request, message, &source).await?;
    let mime_type = source.mime_type();
    let media_type = source.media_type().to_string();
    let expected_size = source.expected_size();
    let canonical_source_link = canonical_source_link(message);

    Ok(Some((
        ResolvedDownloadTarget {
            out_path,
            filename,
            media_type,
            mime_type: mime_type.clone(),
            expected_size,
            canonical_source_link: canonical_source_link.clone(),
            grouped_id: message.grouped_id(),
            caption: message.text().to_string(),
            metadata: describe_message(message),
        },
        source,
    )))
}

/// Choose a filename for a document message.
fn choose_filename_document(doc: &Document, message: &Message) -> String {
    if let Some(name) = doc.name() {
        if !name.is_empty() {
            return sanitize_filename(name);
        }
    }

    let ext = mime_to_ext(doc.mime_type().unwrap_or(""));
    format!(
        "file_{}_{}{}",
        message.peer_id().bot_api_dialog_id(),
        message.id(),
        ext
    )
}

enum DownloadOutcome {
    Downloaded { resumed: bool },
    SkippedExisting,
}

enum PreparedDownload {
    Ready {
        file: fs::File,
        initial_bytes: u64,
        resumed: bool,
    },
    SkippedExisting,
}

struct StreamDownloadConfig {
    total_bytes: u64,
    collision: CollisionPolicy,
    retry: RetryConfig,
    parallel_chunks: usize,
    keep_partial: bool,
    request_timeout: Option<Duration>,
}

struct ParallelDownloadContext<'a, D> {
    client: Client,
    downloadable: D,
    file: fs::File,
    out_path: &'a Path,
    temp_path: &'a Path,
    total_bytes: u64,
    retry: RetryConfig,
    parallel_chunks: usize,
    keep_partial: bool,
    request_timeout: Option<Duration>,
    pb: ProgressBar,
    shutdown: &'a AtomicBool,
}

async fn stream_download<D: Downloadable + Clone + Send + Sync + 'static>(
    client: &ResilientClient,
    downloadable: &D,
    out_path: &Path,
    config: StreamDownloadConfig,
    shutdown: &AtomicBool,
) -> Result<DownloadOutcome> {
    let temp_path = partial_download_path(out_path);

    let prepared = prepare_output_file(out_path, &temp_path, config.collision).await?;
    let (file, initial_bytes, resumed) = match prepared {
        PreparedDownload::Ready {
            file,
            initial_bytes,
            resumed,
        } => (file, initial_bytes, resumed),
        PreparedDownload::SkippedExisting => return Ok(DownloadOutcome::SkippedExisting),
    };
    if resumed && initial_bytes > config.total_bytes && config.total_bytes > 0 {
        bail!(
            "Partial file '{}' is larger than the expected download size.",
            temp_path.display()
        );
    }

    let pb = make_progress_bar(config.total_bytes, initial_bytes, config.parallel_chunks);
    update_progress_status(&pb, 0, None, config.parallel_chunks);

    if config.parallel_chunks > 1
        && config.total_bytes >= PARALLEL_DOWNLOAD_THRESHOLD
        && initial_bytes == 0
        && downloadable.to_raw_input_location().is_some()
    {
        let parallel_client = client.client().await;
        return parallel_stream_download(ParallelDownloadContext {
            client: parallel_client,
            downloadable: downloadable.clone(),
            file,
            out_path,
            temp_path: &temp_path,
            total_bytes: config.total_bytes,
            retry: config.retry,
            parallel_chunks: config.parallel_chunks,
            keep_partial: config.keep_partial,
            request_timeout: config.request_timeout,
            pb,
            shutdown,
        })
        .await;
    }

    let mut written_bytes = initial_bytes;
    let mut attempt = 0u32;
    let mut total_retry_count = 0u32;
    let mut writer = BufWriter::with_capacity(MAX_PARALLEL_CHUNK_SIZE as usize, file);
    loop {
        if shutdown.load(Ordering::SeqCst) {
            pb.abandon_with_message(format!(
                "Interrupted; kept partial download '{}'",
                temp_path.display()
            ));
            writer.flush().await.ok();
            bail!("Interrupted by Ctrl+C");
        }

        let current_client = client.client().await;
        let mut download = current_client
            .iter_download(downloadable)
            .chunk_size(DOWNLOAD_CHUNK_SIZE);
        if written_bytes > 0 {
            if written_bytes % (DOWNLOAD_CHUNK_SIZE as u64) != 0 {
                pb.abandon_with_message(format!(
                    "Partial download '{}' is misaligned and cannot be resumed safely.",
                    temp_path.display()
                ));
                bail!(
                    "Partial download '{}' is misaligned and cannot be resumed safely.",
                    temp_path.display()
                );
            }
            download = download.skip_chunks((written_bytes / DOWNLOAD_CHUNK_SIZE as u64) as i32);
        }

        loop {
            if shutdown.load(Ordering::SeqCst) {
                pb.abandon_with_message(format!(
                    "Interrupted; kept partial download '{}'",
                    temp_path.display()
                ));
                writer.flush().await.ok();
                bail!("Interrupted by Ctrl+C");
            }

            client.wait_for_pacing().await;
            match run_request(config.request_timeout, "Chunk download timed out", async {
                download
                    .next()
                    .await
                    .map_err(|error| anyhow::anyhow!("Download error: {}", error))
            })
            .await?
            {
                Ok(Some(chunk)) => {
                    writer
                        .write_all(&chunk)
                        .await
                        .with_context(|| format!("Failed to write to '{}'", temp_path.display()))?;
                    written_bytes += chunk.len() as u64;
                    pb.inc(chunk.len() as u64);
                    attempt = 0;
                    update_progress_status(&pb, total_retry_count, None, config.parallel_chunks);
                }
                Ok(None) => {
                    writer
                        .flush()
                        .await
                        .with_context(|| format!("Failed to flush '{}'", temp_path.display()))?;
                    let mut file = writer.into_inner();
                    file.flush()
                        .await
                        .with_context(|| format!("Failed to flush '{}'", temp_path.display()))?;
                    drop(file);
                    fs::rename(&temp_path, out_path).await.with_context(|| {
                        format!(
                            "Failed to move completed download from '{}' to '{}'",
                            temp_path.display(),
                            out_path.display()
                        )
                    })?;
                    pb.finish_with_message(format!("Saved to '{}'", out_path.display()));
                    return Ok(DownloadOutcome::Downloaded { resumed });
                }
                Err(error) => {
                    attempt += 1;
                    total_retry_count += 1;
                    let error = anyhow::anyhow!("Download error: {}", error);
                    if let Some(delay) = retryable_delay(&error, attempt, config.retry) {
                        writer.flush().await.ok();
                        prepare_retry(client, &error, delay).await?;
                        update_progress_status(
                            &pb,
                            total_retry_count,
                            Some(delay),
                            config.parallel_chunks,
                        );
                        if crate::output::progress_enabled() {
                            pb.println(format!(
                                "Transient download error for '{}', retrying in {} ms (attempt {}/{}).",
                                out_path.display(),
                                delay.as_millis(),
                                attempt,
                                config.retry.retries
                            ));
                        }
                        break;
                    }

                    finalize_partial_failure(&pb, &temp_path, config.keep_partial).await;
                    writer.flush().await.ok();
                    return Err(error);
                }
            }
        }
    }
}

fn make_progress_bar(total_bytes: u64, initial_bytes: u64, parallel_chunks: usize) -> ProgressBar {
    if !crate::output::progress_enabled() {
        return ProgressBar::hidden();
    }

    if total_bytes > 0 {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} @ {bytes_per_sec} ({eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        pb.set_position(initial_bytes);
        update_progress_status(&pb, 0, None, parallel_chunks);
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bytes} downloaded @ {bytes_per_sec} {msg}",
            )
            .unwrap(),
        );
        pb.inc(initial_bytes);
        update_progress_status(&pb, 0, None, parallel_chunks);
        pb
    }
}

fn update_progress_status(
    pb: &ProgressBar,
    total_retries: u32,
    backoff: Option<Duration>,
    parallel_chunks: usize,
) {
    let mode = if parallel_chunks > 1 {
        format!("parallel={}", parallel_chunks)
    } else {
        "parallel=off".to_string()
    };
    let backoff_text = backoff
        .map(|delay| format!(" backoff={}ms", delay.as_millis()))
        .unwrap_or_default();
    pb.set_message(format!(
        "retries={}{} {}",
        total_retries, backoff_text, mode
    ));
}

async fn parallel_stream_download<D: Downloadable + Clone + Send + Sync + 'static>(
    ctx: ParallelDownloadContext<'_, D>,
) -> Result<DownloadOutcome> {
    let ParallelDownloadContext {
        client,
        downloadable,
        mut file,
        out_path,
        temp_path,
        total_bytes,
        retry,
        parallel_chunks,
        keep_partial,
        request_timeout,
        pb,
        shutdown,
    } = ctx;
    file.set_len(total_bytes)
        .await
        .with_context(|| format!("Failed to preallocate '{}'", temp_path.display()))?;
    file.seek(SeekFrom::Start(0))
        .await
        .with_context(|| format!("Failed to seek '{}'", temp_path.display()))?;

    let chunk_size = MAX_PARALLEL_CHUNK_SIZE as u64;
    let total_parts = total_bytes.div_ceil(chunk_size);
    let mut join_set = JoinSet::new();
    let mut next_part = 0u64;
    let mut completed_chunks: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut next_write_offset = 0u64;
    let mut total_retry_count = 0u32;

    while next_write_offset < total_bytes {
        while join_set.len() < parallel_chunks && next_part < total_parts {
            let offset = next_part * chunk_size;
            let task_client = client.clone();
            let task_downloadable = downloadable.clone();
            let task_retry = retry;
            let chunk_index = next_part as i32;
            join_set.spawn(async move {
                fetch_parallel_chunk(
                    task_client,
                    task_downloadable,
                    offset,
                    chunk_index,
                    task_retry,
                    request_timeout,
                )
                .await
            });
            next_part += 1;
        }

        if shutdown.load(Ordering::SeqCst) {
            pb.abandon_with_message(format!(
                "Interrupted; kept partial download '{}'",
                temp_path.display()
            ));
            file.flush().await.ok();
            bail!("Interrupted by Ctrl+C");
        }

        let Some(result) = join_set.join_next().await else {
            break;
        };
        let fetched = match result {
            Ok(Ok(fetched)) => fetched,
            Ok(Err(error)) => {
                finalize_partial_failure(&pb, temp_path, keep_partial).await;
                file.flush().await.ok();
                return Err(error);
            }
            Err(error) => {
                finalize_partial_failure(&pb, temp_path, keep_partial).await;
                file.flush().await.ok();
                return Err(error.into());
            }
        };
        total_retry_count += fetched.retry_count;
        update_progress_status(
            &pb,
            total_retry_count,
            fetched.last_backoff,
            parallel_chunks,
        );
        completed_chunks.insert(fetched.offset, fetched.bytes);

        while let Some(bytes) = completed_chunks.remove(&next_write_offset) {
            if file.stream_position().await? != next_write_offset {
                file.seek(SeekFrom::Start(next_write_offset)).await?;
            }
            file.write_all(&bytes)
                .await
                .with_context(|| format!("Failed to write to '{}'", temp_path.display()))?;
            next_write_offset += bytes.len() as u64;
            pb.inc(bytes.len() as u64);
        }
    }

    file.flush()
        .await
        .with_context(|| format!("Failed to flush '{}'", temp_path.display()))?;
    fs::rename(temp_path, out_path).await.with_context(|| {
        format!(
            "Failed to move completed download from '{}' to '{}'",
            temp_path.display(),
            out_path.display()
        )
    })?;
    pb.finish_with_message(format!("Saved to '{}'", out_path.display()));
    Ok(DownloadOutcome::Downloaded { resumed: false })
}

struct ParallelChunkResult {
    offset: u64,
    bytes: Vec<u8>,
    retry_count: u32,
    last_backoff: Option<Duration>,
}

async fn fetch_parallel_chunk<D: Downloadable + Clone + Send + Sync + 'static>(
    client: Client,
    downloadable: D,
    offset: u64,
    chunk_index: i32,
    retry: RetryConfig,
    request_timeout: Option<Duration>,
) -> Result<ParallelChunkResult> {
    let mut attempt = 0u32;
    let mut retry_count = 0u32;
    let mut last_backoff = None;

    loop {
        let mut download = client
            .iter_download(&downloadable)
            .chunk_size(MAX_PARALLEL_CHUNK_SIZE)
            .skip_chunks(chunk_index);
        match run_request(request_timeout, "Parallel chunk request timed out", async {
            download
                .next()
                .await
                .map_err(|error| anyhow::anyhow!("Download error: {}", error))
        })
        .await
        {
            Ok(Ok(Some(bytes))) => {
                return Ok(ParallelChunkResult {
                    offset,
                    bytes,
                    retry_count,
                    last_backoff,
                });
            }
            Ok(Ok(None)) => {
                return Ok(ParallelChunkResult {
                    offset,
                    bytes: Vec::new(),
                    retry_count,
                    last_backoff,
                });
            }
            Ok(Err(anyhow_error)) | Err(anyhow_error) => {
                attempt += 1;
                retry_count += 1;
                if let Some(delay) = retryable_delay(&anyhow_error, attempt, retry) {
                    last_backoff = Some(delay);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(anyhow_error);
            }
        }
    }
}

async fn prepare_output_file(
    out_path: &Path,
    temp_path: &Path,
    collision: CollisionPolicy,
) -> Result<PreparedDownload> {
    let out_exists = fs::try_exists(out_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", out_path.display()))?;
    let temp_exists = fs::try_exists(temp_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", temp_path.display()))?;

    if out_exists {
        match collision {
            CollisionPolicy::Error => {
                bail!(
                    "Refusing to overwrite existing file '{}'. Use --skip-existing, --overwrite, or --resume.",
                    out_path.display()
                );
            }
            CollisionPolicy::SkipExisting | CollisionPolicy::Resume => {
                return Ok(PreparedDownload::SkippedExisting);
            }
            CollisionPolicy::Overwrite => {
                fs::remove_file(out_path)
                    .await
                    .with_context(|| format!("Failed to remove '{}'", out_path.display()))?;
            }
            CollisionPolicy::SuffixExisting => {}
        }
    }

    if temp_exists {
        match collision {
            CollisionPolicy::Error => {
                bail!(
                    "Temporary download file '{}' already exists. Use --resume or --overwrite.",
                    temp_path.display()
                );
            }
            CollisionPolicy::SkipExisting => {
                return Ok(PreparedDownload::SkippedExisting);
            }
            CollisionPolicy::Overwrite => {
                fs::remove_file(temp_path)
                    .await
                    .with_context(|| format!("Failed to remove '{}'", temp_path.display()))?;
            }
            CollisionPolicy::Resume => {
                let meta = fs::metadata(temp_path)
                    .await
                    .with_context(|| format!("Failed to inspect '{}'", temp_path.display()))?;
                let file = OpenOptions::new()
                    .append(true)
                    .open(temp_path)
                    .await
                    .with_context(|| format!("Failed to reopen '{}'", temp_path.display()))?;
                return Ok(PreparedDownload::Ready {
                    file,
                    initial_bytes: meta.len(),
                    resumed: true,
                });
            }
            CollisionPolicy::SuffixExisting => {}
        }
    }

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)
        .await
        .with_context(|| format!("Failed to create file '{}'", temp_path.display()))?;

    Ok(PreparedDownload::Ready {
        file,
        initial_bytes: 0,
        resumed: false,
    })
}

fn retryable_delay(error: &anyhow::Error, attempt: u32, retry: RetryConfig) -> Option<Duration> {
    if attempt > retry.retries {
        return None;
    }

    let message = error.to_string();
    if let Some(wait_seconds) = flood_wait_seconds(&message) {
        return Some(Duration::from_secs(wait_seconds.max(1)));
    }

    if is_retryable_error(&message) {
        return Some(retry.delay_for_attempt(attempt));
    }

    None
}

async fn prepare_retry(
    client: &ResilientClient,
    error: &anyhow::Error,
    delay: Duration,
) -> Result<()> {
    let message = error.to_string();
    if flood_wait_seconds(&message).is_some() {
        client.apply_pacing(delay).await;
    }
    if should_reconnect(&message) {
        client.reconnect().await?;
    }
    tokio::time::sleep(delay).await;
    client.clear_pacing_if_elapsed().await;
    Ok(())
}

async fn finalize_partial_failure(pb: &ProgressBar, temp_path: &Path, keep_partial: bool) {
    if keep_partial {
        pb.abandon_with_message(format!(
            "Failed; kept partial download '{}'",
            temp_path.display()
        ));
        return;
    }

    match fs::remove_file(temp_path).await {
        Ok(()) => pb.abandon_with_message(format!(
            "Failed; removed partial download '{}'",
            temp_path.display()
        )),
        Err(_) => pb.abandon_with_message(format!(
            "Failed; partial download may remain at '{}'",
            temp_path.display()
        )),
    }
}

fn flood_wait_seconds(message: &str) -> Option<u64> {
    static FLOOD_WAIT_RE: OnceLock<Regex> = OnceLock::new();
    let regex = FLOOD_WAIT_RE
        .get_or_init(|| Regex::new(r"(?i)(?:FLOOD_WAIT_|wait of )(\d+)(?: seconds?)?").unwrap());
    regex
        .captures(message)
        .and_then(|captures| captures.get(1))
        .and_then(|matched| matched.as_str().parse::<u64>().ok())
}

fn is_retryable_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    if is_permanent_error(&normalized) {
        return false;
    }

    [
        "flood wait",
        "rpc call fail",
        "timeout",
        "timed out",
        "connection reset",
        "connection aborted",
        "connection refused",
        "network",
        "temporarily unavailable",
        "transport",
        "broken pipe",
        "unexpected eof",
        "download error",
        "disconnected",
        "interrupted by peer",
    ]
    .iter()
    .any(|pattern| normalized.contains(pattern))
}

fn should_reconnect(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    [
        "connection reset",
        "connection aborted",
        "connection refused",
        "broken pipe",
        "transport",
        "unexpected eof",
        "disconnected",
        "interrupted by peer",
        "network",
    ]
    .iter()
    .any(|pattern| normalized.contains(pattern))
}

fn is_permanent_error(normalized_message: &str) -> bool {
    [
        "not logged in",
        "username '",
        "username not found",
        "message id=",
        "this message does not contain downloadable media",
        "unsupported media type",
        "could not find a chat with id=",
        "could not parse link",
        "phone code invalid",
        "phone code expired",
        "password hash invalid",
        "session password needed",
        "unauthorized",
        "forbidden",
        "access denied",
        "invalid peer",
        "peer id invalid",
        "chat admin required",
        "channel private",
        "invite request sent",
        "auth key",
    ]
    .iter()
    .any(|pattern| normalized_message.contains(pattern))
}

// ── utilities ─────────────────────────────────────────────────────────────────

async fn inspect_output_collision(
    out_path: &Path,
    collision: CollisionPolicy,
) -> Result<&'static str> {
    let temp_path = partial_download_path(out_path);
    let out_exists = fs::try_exists(out_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", out_path.display()))?;
    let temp_exists = fs::try_exists(&temp_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", temp_path.display()))?;

    let result = match (out_exists, temp_exists, collision) {
        (true, _, CollisionPolicy::SkipExisting) => "skip-existing-file",
        (true, _, CollisionPolicy::Resume) => "skip-complete-file",
        (true, _, CollisionPolicy::Overwrite) => "overwrite-file",
        (true, _, CollisionPolicy::SuffixExisting) => "suffix-existing-file",
        (true, _, CollisionPolicy::Error) => "error-existing-file",
        (false, true, CollisionPolicy::SkipExisting) => "skip-partial-file",
        (false, true, CollisionPolicy::Resume) => "resume-partial-file",
        (false, true, CollisionPolicy::Overwrite) => "overwrite-partial-file",
        (false, true, CollisionPolicy::SuffixExisting) => "suffix-partial-file",
        (false, true, CollisionPolicy::Error) => "error-partial-file",
        (false, false, _) => "write-new-file",
    };

    Ok(result)
}

fn collision_policy_name(policy: CollisionPolicy) -> &'static str {
    match policy {
        CollisionPolicy::Error => "error",
        CollisionPolicy::SkipExisting => "skip-existing",
        CollisionPolicy::Overwrite => "overwrite",
        CollisionPolicy::Resume => "resume",
        CollisionPolicy::SuffixExisting => "suffix-existing",
    }
}

fn sanitize_filename(name: &str) -> String {
    let normalized_input = name.replace('\\', "/");
    let basename = std::path::Path::new(&normalized_input)
        .file_name()
        .and_then(|part| part.to_str())
        .unwrap_or(name);
    let mut sanitized: String = basename
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    sanitized = sanitized.trim_matches(['.', ' ']).to_string();
    sanitized = sanitized.split_whitespace().collect::<Vec<_>>().join(" ");

    if sanitized.is_empty() {
        sanitized = "download".to_string();
    }

    let (stem, ext) = split_extension(&sanitized);
    let stem = stem.trim_end_matches(['.', ' ']);
    let ext = ext.trim_matches(['.', ' ']);
    let normalized_stem = if is_windows_reserved_name(stem) {
        format!("_{}", stem)
    } else {
        stem.to_string()
    };
    sanitized = if ext.is_empty() {
        normalized_stem
    } else {
        format!("{}.{}", normalized_stem, ext)
    };

    sanitized
}

fn safe_output_path(out_dir: &Path, filename: &str) -> Result<PathBuf> {
    let sanitized = sanitize_filename(filename);
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        bail!("Resolved filename was empty after sanitization");
    }

    let path = out_dir.join(&sanitized);
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("Refusing to write outside of the output directory");
    }

    Ok(path)
}

fn split_extension(name: &str) -> (&str, &str) {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => (stem, ext),
        _ => (name, ""),
    }
}

fn is_windows_reserved_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn partial_download_path(out_path: &Path) -> PathBuf {
    let mut filename = out_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    filename.push(".part");
    out_path.with_file_name(filename)
}

fn mime_to_ext(mime: &str) -> &str {
    match mime {
        "video/mp4" => ".mp4",
        "video/x-matroska" => ".mkv",
        "video/webm" => ".webm",
        "video/quicktime" => ".mov",
        "audio/mpeg" => ".mp3",
        "audio/ogg" => ".ogg",
        "audio/opus" => ".opus",
        "audio/aac" => ".aac",
        "audio/flac" => ".flac",
        "image/jpeg" => ".jpg",
        "image/png" => ".png",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "application/pdf" => ".pdf",
        "application/zip" => ".zip",
        "application/x-tar" => ".tar",
        "application/gzip" => ".gz",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("foo/bar.mp4"), "bar.mp4");
        assert_eq!(sanitize_filename("normal.mkv"), "normal.mkv");
        assert_eq!(sanitize_filename("a:b*c?d"), "a_b_c_d");
        assert_eq!(sanitize_filename("..\\..\\CON .txt"), "_CON.txt");
        assert_eq!(sanitize_filename(" trailing . "), "trailing");
    }

    #[test]
    fn test_mime_to_ext() {
        assert_eq!(mime_to_ext("video/mp4"), ".mp4");
        assert_eq!(mime_to_ext("audio/mpeg"), ".mp3");
        assert_eq!(mime_to_ext("unknown/type"), "");
    }

    #[test]
    fn test_partial_download_path() {
        let path = Path::new("/tmp/video.mp4");
        assert_eq!(
            partial_download_path(path),
            PathBuf::from("/tmp/video.mp4.part")
        );
    }

    #[test]
    fn test_collision_policy_from_flags() {
        assert_eq!(
            CollisionPolicy::from_flags(true, false, false, false),
            CollisionPolicy::SkipExisting
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, true, false, false),
            CollisionPolicy::Overwrite
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, true, false),
            CollisionPolicy::Resume
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, false, true),
            CollisionPolicy::SuffixExisting
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, false, false),
            CollisionPolicy::Error
        );
    }

    #[test]
    fn test_retry_delay_validation() {
        assert!(RetryConfig::new(3, Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(RetryConfig::new(3, Duration::from_secs(2), Duration::from_secs(1)).is_err());
    }

    #[test]
    fn test_retryable_error_detection() {
        assert!(is_retryable_error("network timeout while downloading"));
        assert!(!is_retryable_error("username not found"));
        assert!(!is_retryable_error("CHANNEL_PRIVATE"));
    }

    #[test]
    fn test_flood_wait_parsing() {
        assert_eq!(flood_wait_seconds("FLOOD_WAIT_12"), Some(12));
        assert_eq!(
            flood_wait_seconds("A wait of 9 seconds is required"),
            Some(9)
        );
        assert_eq!(flood_wait_seconds("username not found"), None);
    }

    #[test]
    fn test_checkpoint_path_default() {
        let list = Path::new("/tmp/links.txt");
        assert_eq!(
            checkpoint_path_for(list, None),
            PathBuf::from("/tmp/links.txt.checkpoint.jsonl")
        );
    }

    #[test]
    fn test_parse_csv_batch_entries_with_header() {
        let entries = parse_csv_batch_entries(
            &[
                SourceLine {
                    line_number: 1,
                    text: "link,label".to_string(),
                },
                SourceLine {
                    line_number: 2,
                    text: "https://t.me/example/1,first".to_string(),
                },
                SourceLine {
                    line_number: 3,
                    text: "https://t.me/example/2,second".to_string(),
                },
            ],
            BatchLineRange::new(None, None).unwrap(),
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].line_number, 2);
        assert_eq!(entries[0].link, "https://t.me/example/1");
        assert_eq!(entries[1].line_number, 3);
        assert_eq!(entries[1].link, "https://t.me/example/2");
    }

    #[test]
    fn test_parse_jsonl_batch_entries() {
        let entries = parse_jsonl_batch_entries(
            &[
                SourceLine {
                    line_number: 1,
                    text: "\"https://t.me/example/1\"".to_string(),
                },
                SourceLine {
                    line_number: 2,
                    text: "{\"link\":\"https://t.me/example/2\"}".to_string(),
                },
            ],
            BatchLineRange::new(None, None).unwrap(),
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].link, "https://t.me/example/1");
        assert_eq!(entries[1].link, "https://t.me/example/2");
    }

    #[test]
    fn test_parse_text_batch_entries_respects_line_range() {
        let entries = parse_text_batch_entries(
            &[
                SourceLine {
                    line_number: 1,
                    text: "# comment".to_string(),
                },
                SourceLine {
                    line_number: 2,
                    text: "https://t.me/example/1".to_string(),
                },
                SourceLine {
                    line_number: 3,
                    text: "https://t.me/example/2".to_string(),
                },
            ],
            BatchLineRange::new(Some(3), Some(3)).unwrap(),
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line_number, 3);
        assert_eq!(entries[0].link, "https://t.me/example/2");
    }

    #[test]
    fn test_should_stop_batch() {
        assert!(should_stop_batch(BatchFailureMode::FailFast, None, 1));
        assert!(should_stop_batch(
            BatchFailureMode::ContinueOnError,
            Some(2),
            2
        ));
        assert!(!should_stop_batch(
            BatchFailureMode::ContinueOnError,
            Some(2),
            1
        ));
    }

    #[tokio::test]
    async fn test_load_completed_links() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-checkpoint-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let mut file = open_manifest_writer(&path).await.unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 1,
                link: "https://t.me/example/1".to_string(),
                status: "downloaded".to_string(),
                file: Some("/tmp/out.mp4".to_string()),
                error: None,
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 2,
                link: "https://t.me/example/2".to_string(),
                status: "failed".to_string(),
                file: None,
                error: Some("boom".to_string()),
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();

        let links = load_completed_links(&path).await.unwrap();
        assert!(links.contains(&normalize_link_key("https://t.me/example/1")));
        assert!(!links.contains(&normalize_link_key("https://t.me/example/2")));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_load_failed_links() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-failed-links-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let mut file = open_manifest_writer(&path).await.unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 1,
                link: "https://t.me/example/1".to_string(),
                status: "downloaded".to_string(),
                file: Some("/tmp/out.mp4".to_string()),
                error: None,
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 2,
                link: "https://t.me/example/2".to_string(),
                status: "failed".to_string(),
                file: None,
                error: Some("boom".to_string()),
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();

        let links = load_failed_links(&path).await.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].line_number, 1);
        assert_eq!(links[0].link, "https://t.me/example/2");

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_inspect_output_collision() {
        let base = std::env::temp_dir().join(format!(
            "tw-dl-collision-{}-{}",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        std::fs::create_dir_all(&base).unwrap();
        let out_path = base.join("video.mp4");
        let part_path = partial_download_path(&out_path);

        std::fs::write(&part_path, b"partial").unwrap();
        assert_eq!(
            inspect_output_collision(&out_path, CollisionPolicy::Resume)
                .await
                .unwrap(),
            "resume-partial-file"
        );

        std::fs::remove_file(&part_path).unwrap();
        std::fs::write(&out_path, b"done").unwrap();
        assert_eq!(
            inspect_output_collision(&out_path, CollisionPolicy::SkipExisting)
                .await
                .unwrap(),
            "skip-existing-file"
        );
        assert_eq!(
            inspect_output_collision(&out_path, CollisionPolicy::SuffixExisting)
                .await
                .unwrap(),
            "suffix-existing-file"
        );

        let _ = std::fs::remove_file(out_path);
        let _ = std::fs::remove_dir(base);
    }

    #[test]
    fn test_sidecar_path_appends_suffix_to_filename() {
        let path = Path::new("/tmp/video.mp4");
        assert_eq!(
            sidecar_path(path, "json"),
            PathBuf::from("/tmp/video.mp4.json")
        );
        assert_eq!(
            sidecar_path(path, "caption.json"),
            PathBuf::from("/tmp/video.mp4.caption.json")
        );
    }

    #[tokio::test]
    async fn test_choose_suffix_output_path_picks_next_available_name() {
        let base = std::env::temp_dir().join(format!(
            "tw-dl-suffix-{}-{}",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        std::fs::create_dir_all(&base).unwrap();

        let path = base.join("video.mp4");
        let path_1 = base.join("video (1).mp4");
        let path_2_part = partial_download_path(&base.join("video (2).mp4"));
        std::fs::write(&path, b"done").unwrap();
        std::fs::write(&path_1, b"done").unwrap();
        std::fs::write(&path_2_part, b"partial").unwrap();

        let candidate = choose_suffix_output_path(&path).await.unwrap();
        assert_eq!(candidate, base.join("video (3).mp4"));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path_1);
        let _ = std::fs::remove_file(path_2_part);
        let _ = std::fs::remove_dir(base);
    }

    #[test]
    fn test_safe_output_path_stays_in_output_dir() {
        let out_dir = Path::new("/tmp/downloads");
        let path = safe_output_path(out_dir, "../../secret.txt").unwrap();
        assert_eq!(path, PathBuf::from("/tmp/downloads/secret.txt"));
    }

    #[test]
    fn test_windows_reserved_name_detection() {
        assert!(is_windows_reserved_name("CON"));
        assert!(is_windows_reserved_name("lpt1"));
        assert!(!is_windows_reserved_name("video"));
    }
}
