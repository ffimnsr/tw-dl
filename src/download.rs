use anyhow::{bail, Context, Result};
use grammers_client::media::{Document, Downloadable, Media};
use grammers_client::message::Message;
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use crate::link::{parse_link, ParsedLink};

const DOWNLOAD_CHUNK_SIZE: i32 = 128 * 1024;
const MAX_PARALLEL_CHUNK_SIZE: i32 = 512 * 1024;
const PARALLEL_DOWNLOAD_THRESHOLD: u64 = 10 * 1024 * 1024;

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
    pub checkpoint: Option<PathBuf>,
    pub dry_run: bool,
    pub retry_from: Option<PathBuf>,
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
pub enum CollisionPolicy {
    Error,
    SkipExisting,
    Overwrite,
    Resume,
}

impl CollisionPolicy {
    pub fn from_flags(skip_existing: bool, overwrite: bool, resume: bool) -> Self {
        if skip_existing {
            Self::SkipExisting
        } else if overwrite {
            Self::Overwrite
        } else if resume {
            Self::Resume
        } else {
            Self::Error
        }
    }
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

    let client = Arc::new(ResilientClient::new(api_id, session_path, session_options).await?);
    ensure_authorized(&client).await?;
    let caches = Arc::new(DownloadCaches::default());
    let timeouts = TimeoutConfig::from_args(&args);

    let shutdown = install_shutdown_handler();
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
                    checkpoint: checkpoint_path_for(retry_from, args.checkpoint.clone()),
                    dry_run: args.dry_run,
                    parallel_chunks: args.parallel_chunks,
                    keep_partial: args.keep_partial,
                    timeouts,
                },
                shutdown.clone(),
                Arc::clone(&caches),
            )
            .await
        } else if let Some(list_path) = &args.file_list {
            run_batch_downloads(
                &client,
                BatchContext {
                    list_path: list_path.clone(),
                    out_dir: args.out_dir.clone(),
                    collision: args.collision,
                    retry: args.retry,
                    jobs: args.jobs,
                    checkpoint: checkpoint_path_for(list_path, args.checkpoint.clone()),
                    dry_run: args.dry_run,
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
                parallel_chunks: args.parallel_chunks,
                keep_partial: args.keep_partial,
                timeouts,
            };
            let result = download_one(&client, &request, &shutdown, &caches).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
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

    println!(
        "{}",
        serde_json::to_string_pretty(&describe_message(&message))?
    );
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

    println!("{}", serde_json::to_string_pretty(&Value::Array(chats))?);
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
    println!("{}", serde_json::to_string_pretty(&resolved)?);
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
                eprintln!("Interrupt received. Stopping new work and leaving partial downloads resumable.");
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
        let message = fetch_message_with_retry(
            client,
            &request.selector,
            request.retry,
            request.timeouts,
            caches,
        )
        .await?;
        if request.dry_run {
            plan_download(&message, &request.out_dir, request.collision).await
        } else {
            download_media(
                client,
                &message,
                &request.out_dir,
                collision_config(request),
                shutdown,
            )
            .await
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
                    eprintln!(
                        "Retrying message fetch after error (attempt {}/{}): {}",
                        attempt, retry.retries, error
                    );
                    continue;
                }
                return Err(error);
            }
        }
    }
}

// ── batch download ────────────────────────────────────────────────────────────

struct BatchContext {
    list_path: PathBuf,
    out_dir: PathBuf,
    collision: CollisionPolicy,
    retry: RetryConfig,
    jobs: usize,
    checkpoint: PathBuf,
    dry_run: bool,
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
    checkpoint: PathBuf,
    dry_run: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
}

struct LinkJobContext<'a> {
    client: &'a Arc<ResilientClient>,
    entries: Vec<(usize, String)>,
    out_dir: &'a Path,
    collision: CollisionPolicy,
    retry: RetryConfig,
    jobs: usize,
    checkpoint_path: &'a Path,
    dry_run: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
    caches: Arc<DownloadCaches>,
    shutdown: std::sync::Arc<AtomicBool>,
}

#[derive(Debug)]
struct BatchTaskResult {
    index: usize,
    link: String,
    result: Result<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckpointEntry {
    index: usize,
    link: String,
    status: String,
    file: Option<String>,
    error: Option<String>,
    timestamp_unix: u64,
}

async fn run_batch_downloads(
    client: &Arc<ResilientClient>,
    ctx: BatchContext,
    shutdown: std::sync::Arc<AtomicBool>,
    caches: Arc<DownloadCaches>,
) -> Result<()> {
    let file = fs::File::open(&ctx.list_path)
        .await
        .with_context(|| format!("Cannot open file list '{}'", ctx.list_path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut entries = Vec::new();
    let mut index = 0usize;
    while let Some(line) = lines.next_line().await? {
        let link = line.trim().to_string();
        if link.is_empty() || link.starts_with('#') {
            continue;
        }
        index += 1;
        entries.push((index, link));
    }

    prefetch_messages_for_entries(client, &entries, ctx.retry, ctx.timeouts, &caches).await?;

    run_link_jobs(LinkJobContext {
        client,
        entries,
        out_dir: &ctx.out_dir,
        collision: ctx.collision,
        retry: ctx.retry,
        jobs: ctx.jobs,
        checkpoint_path: &ctx.checkpoint,
        dry_run: ctx.dry_run,
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
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "status": "completed",
                "mode": "retry-from",
                "succeeded": 0,
                "skipped": 0,
                "checkpoint_skipped": 0,
                "failed": 0,
                "checkpoint": ctx.checkpoint.display().to_string(),
                "source_manifest": ctx.manifest_path.display().to_string(),
                "message": "No failed links found in manifest",
            }))?
        );
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
        checkpoint_path: &ctx.checkpoint,
        dry_run: ctx.dry_run,
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
        checkpoint_path,
        dry_run,
        parallel_chunks,
        keep_partial,
        timeouts,
        caches,
        shutdown,
    } = ctx;

    let mut completed_links = load_completed_links(checkpoint_path).await?;
    let mut writer = open_checkpoint_writer(checkpoint_path).await?;
    let mut join_set = JoinSet::new();
    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    let mut skipped_count = 0usize;
    let mut checkpoint_skip_count = 0usize;
    let client_handle = client.clone();

    for (index, link) in entries {
        if completed_links.contains(&link) {
            checkpoint_skip_count += 1;
            eprintln!("[{}] Skipping completed checkpoint entry {}", index, link);
            continue;
        }

        while join_set.len() >= jobs {
            let finished = join_set
                .join_next()
                .await
                .expect("join set length checked")?;
            handle_batch_result(
                finished,
                checkpoint_path,
                &mut writer,
                &mut completed_links,
                &mut success_count,
                &mut failure_count,
                &mut skipped_count,
            )
            .await?;
        }

        if shutdown.load(Ordering::SeqCst) {
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
            parallel_chunks,
            keep_partial,
            timeouts,
        };
        let task_client = client_handle.clone();
        let task_shutdown = shutdown.clone();
        let task_caches = Arc::clone(&caches);
        join_set.spawn(async move {
            BatchTaskResult {
                index,
                link,
                result: download_one(&task_client, &request, &task_shutdown, &task_caches).await,
            }
        });
    }

    while let Some(task_result) = join_set.join_next().await {
        let finished = task_result?;
        handle_batch_result(
            finished,
            checkpoint_path,
            &mut writer,
            &mut completed_links,
            &mut success_count,
            &mut failure_count,
            &mut skipped_count,
        )
        .await?;
    }

    if shutdown.load(Ordering::SeqCst) {
        bail!(
            "Batch interrupted with {} succeeded, {} skipped, {} failed, {} restored from checkpoint.",
            success_count,
            skipped_count,
            failure_count,
            checkpoint_skip_count
        );
    }

    if failure_count > 0 {
        bail!(
            "Batch download completed with {} succeeded, {} skipped, {} restored from checkpoint, and {} failed.",
            success_count,
            skipped_count,
            checkpoint_skip_count,
            failure_count
        );
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "status": "completed",
            "succeeded": success_count,
            "skipped": skipped_count,
            "checkpoint_skipped": checkpoint_skip_count,
            "failed": failure_count,
            "checkpoint": checkpoint_path.display().to_string(),
            "dry_run": dry_run,
        }))?
    );
    Ok(())
}

async fn handle_batch_result(
    result: BatchTaskResult,
    checkpoint_path: &Path,
    writer: &mut fs::File,
    completed_links: &mut HashSet<String>,
    success_count: &mut usize,
    failure_count: &mut usize,
    skipped_count: &mut usize,
) -> Result<()> {
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
                completed_links.insert(result.link.clone());
            }

            append_checkpoint_entry(
                writer,
                CheckpointEntry {
                    index: result.index,
                    link: result.link,
                    status: status.to_string(),
                    file: value
                        .get("file")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    error: None,
                    timestamp_unix: unix_timestamp(),
                },
            )
            .await?;

            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Err(error) => {
            *failure_count += 1;
            let rendered = format!("{:#}", error);
            append_checkpoint_entry(
                writer,
                CheckpointEntry {
                    index: result.index,
                    link: result.link.clone(),
                    status: "failed".to_string(),
                    file: None,
                    error: Some(rendered.clone()),
                    timestamp_unix: unix_timestamp(),
                },
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to update checkpoint '{}'",
                    checkpoint_path.display()
                )
            })?;

            eprintln!("[{}] ERROR {}: {}", result.index, result.link, rendered);
        }
    }

    Ok(())
}

async fn load_completed_links(path: &Path) -> Result<HashSet<String>> {
    if !fs::try_exists(path)
        .await
        .with_context(|| format!("Failed to inspect checkpoint '{}'", path.display()))?
    {
        return Ok(HashSet::new());
    }

    let file = fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open checkpoint '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut completed = HashSet::new();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let entry: CheckpointEntry = serde_json::from_str(&line)
            .with_context(|| format!("Failed to parse checkpoint entry in '{}'", path.display()))?;
        if entry.status == "downloaded" {
            completed.insert(entry.link);
        }
    }

    Ok(completed)
}

async fn load_failed_links(path: &Path) -> Result<Vec<(usize, String)>> {
    if !fs::try_exists(path)
        .await
        .with_context(|| format!("Failed to inspect manifest '{}'", path.display()))?
    {
        bail!("Manifest '{}' does not exist", path.display());
    }

    let file = fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open manifest '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut failed = Vec::new();
    let mut index = 0usize;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let entry: CheckpointEntry = serde_json::from_str(&line)
            .with_context(|| format!("Failed to parse manifest entry in '{}'", path.display()))?;
        if entry.status == "failed" {
            index += 1;
            failed.push((index, entry.link));
        }
    }

    Ok(failed)
}

async fn open_checkpoint_writer(path: &Path) -> Result<fs::File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "Failed to create checkpoint directory '{}'",
                parent.display()
            )
        })?;
    }

    OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await
        .with_context(|| format!("Failed to open checkpoint '{}'", path.display()))
}

async fn append_checkpoint_entry(writer: &mut fs::File, entry: CheckpointEntry) -> Result<()> {
    let line = serde_json::to_string(&entry).context("Failed to serialize checkpoint entry")?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
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
        eprintln!(
            "Warning: numeric peer ids are Telegram bare ids and can be ambiguous. \
Prefer a full Telegram message link or @username when possible."
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
    entries: &[(usize, String)],
    _retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<()> {
    let mut grouped: HashMap<PeerSpec, Vec<i32>> = HashMap::new();
    for (_, link) in entries {
        let selector = MessageSelectorArgs {
            link: Some(link.clone()),
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

async fn download_media(
    client: &ResilientClient,
    message: &Message,
    out_dir: &Path,
    config: StreamDownloadConfig,
    shutdown: &AtomicBool,
) -> Result<Value> {
    let media = message
        .media()
        .context("This message does not contain downloadable media")?;

    fs::create_dir_all(out_dir)
        .await
        .with_context(|| format!("Failed to create output directory '{}'", out_dir.display()))?;

    match media {
        Media::Document(ref doc) => {
            let filename = choose_filename_document(doc, message);
            let out_path = safe_output_path(out_dir, &filename)?;
            let total = doc.size().unwrap_or(0) as u64;
            let mime = doc
                .mime_type()
                .unwrap_or("application/octet-stream")
                .to_string();

            let outcome = stream_download(
                client,
                doc,
                &out_path,
                StreamDownloadConfig {
                    total_bytes: total,
                    ..config
                },
                shutdown,
            )
            .await?;

            Ok(download_result_value(
                message, &out_path, filename, &mime, total, outcome,
            ))
        }
        Media::Photo(ref photo) => {
            let filename = format!(
                "photo_{}_{}.jpg",
                message.peer_id().bot_api_dialog_id(),
                message.id()
            );
            let out_path = safe_output_path(out_dir, &filename)?;
            let total = photo.size().unwrap_or(0) as u64;

            let outcome = stream_download(
                client,
                photo,
                &out_path,
                StreamDownloadConfig {
                    total_bytes: total,
                    ..config
                },
                shutdown,
            )
            .await?;

            Ok(download_result_value(
                message,
                &out_path,
                filename,
                "image/jpeg",
                total,
                outcome,
            ))
        }
        other => {
            bail!(
                "Unsupported media type: {:?}. Only documents and photos are supported.",
                other
            );
        }
    }
}

async fn plan_download(
    message: &Message,
    out_dir: &Path,
    collision: CollisionPolicy,
) -> Result<Value> {
    let media = message
        .media()
        .context("This message does not contain downloadable media")?;

    fs::create_dir_all(out_dir)
        .await
        .with_context(|| format!("Failed to create output directory '{}'", out_dir.display()))?;

    let (filename, mime_type, size, out_path) = match media {
        Media::Document(ref doc) => {
            let filename = choose_filename_document(doc, message);
            let out_path = safe_output_path(out_dir, &filename)?;
            (
                filename,
                doc.mime_type()
                    .unwrap_or("application/octet-stream")
                    .to_string(),
                doc.size().unwrap_or(0) as u64,
                out_path,
            )
        }
        Media::Photo(ref photo) => {
            let filename = format!(
                "photo_{}_{}.jpg",
                message.peer_id().bot_api_dialog_id(),
                message.id()
            );
            let out_path = safe_output_path(out_dir, &filename)?;
            (
                filename,
                "image/jpeg".to_string(),
                photo.size().unwrap_or(0) as u64,
                out_path,
            )
        }
        other => {
            bail!(
                "Unsupported media type: {:?}. Only documents and photos are supported.",
                other
            );
        }
    };

    let collision_result = inspect_output_collision(&out_path, collision).await?;
    Ok(json!({
        "status": "planned",
        "message_id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "file": out_path.display().to_string(),
        "filename": filename,
        "mime_type": mime_type,
        "size": size,
        "collision_policy": collision_policy_name(collision),
        "collision_result": collision_result,
        "dry_run": true,
    }))
}

fn describe_message(message: &Message) -> Value {
    let peer = message.peer().map(describe_peer);
    let sender = message.sender().map(describe_peer);

    json!({
        "id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
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
        "resumed": resumed,
    })
}

fn collision_config(request: &SingleDownloadRequest) -> StreamDownloadConfig {
    StreamDownloadConfig {
        total_bytes: 0,
        collision: request.collision,
        retry: request.retry,
        parallel_chunks: request.parallel_chunks,
        keep_partial: request.keep_partial,
        request_timeout: request.timeouts.request_timeout,
    }
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
                        pb.println(format!(
                            "Transient download error for '{}', retrying in {} ms (attempt {}/{}).",
                            out_path.display(),
                            delay.as_millis(),
                            attempt,
                            config.retry.retries
                        ));
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
        (true, _, CollisionPolicy::Error) => "error-existing-file",
        (false, true, CollisionPolicy::SkipExisting) => "skip-partial-file",
        (false, true, CollisionPolicy::Resume) => "resume-partial-file",
        (false, true, CollisionPolicy::Overwrite) => "overwrite-partial-file",
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

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
            CollisionPolicy::from_flags(true, false, false),
            CollisionPolicy::SkipExisting
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, true, false),
            CollisionPolicy::Overwrite
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, true),
            CollisionPolicy::Resume
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, false),
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

    #[tokio::test]
    async fn test_load_completed_links() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-checkpoint-{}-{}.jsonl",
            std::process::id(),
            unix_timestamp()
        ));
        let mut file = open_checkpoint_writer(&path).await.unwrap();
        append_checkpoint_entry(
            &mut file,
            CheckpointEntry {
                index: 1,
                link: "https://t.me/example/1".to_string(),
                status: "downloaded".to_string(),
                file: Some("/tmp/out.mp4".to_string()),
                error: None,
                timestamp_unix: unix_timestamp(),
            },
        )
        .await
        .unwrap();
        append_checkpoint_entry(
            &mut file,
            CheckpointEntry {
                index: 2,
                link: "https://t.me/example/2".to_string(),
                status: "failed".to_string(),
                file: None,
                error: Some("boom".to_string()),
                timestamp_unix: unix_timestamp(),
            },
        )
        .await
        .unwrap();

        let links = load_completed_links(&path).await.unwrap();
        assert!(links.contains("https://t.me/example/1"));
        assert!(!links.contains("https://t.me/example/2"));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_load_failed_links() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-failed-links-{}-{}.jsonl",
            std::process::id(),
            unix_timestamp()
        ));
        let mut file = open_checkpoint_writer(&path).await.unwrap();
        append_checkpoint_entry(
            &mut file,
            CheckpointEntry {
                index: 1,
                link: "https://t.me/example/1".to_string(),
                status: "downloaded".to_string(),
                file: Some("/tmp/out.mp4".to_string()),
                error: None,
                timestamp_unix: unix_timestamp(),
            },
        )
        .await
        .unwrap();
        append_checkpoint_entry(
            &mut file,
            CheckpointEntry {
                index: 2,
                link: "https://t.me/example/2".to_string(),
                status: "failed".to_string(),
                file: None,
                error: Some("boom".to_string()),
                timestamp_unix: unix_timestamp(),
            },
        )
        .await
        .unwrap();

        let links = load_failed_links(&path).await.unwrap();
        assert_eq!(links, vec![(1, "https://t.me/example/2".to_string())]);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_inspect_output_collision() {
        let base = std::env::temp_dir().join(format!(
            "tw-dl-collision-{}-{}",
            std::process::id(),
            unix_timestamp()
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

        let _ = std::fs::remove_file(out_path);
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
